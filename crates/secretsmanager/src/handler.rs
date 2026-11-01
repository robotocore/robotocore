//! Secrets Manager operation handler.

use crate::models::{Secret, SecretVersion, SmState};
use crate::protocol::{AwsRequest, AwsResponse};
use chrono::Utc;
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::sync::Arc;

pub struct SecretsManagerHandler {
    pub(crate) state: RwLock<std::collections::HashMap<(u64, String), SmState>>,
}

impl SecretsManagerHandler {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(std::collections::HashMap::new()),
        }
    }

    pub(crate) fn get_state(&self, account: u64, region: &str) -> SmState {
        let mut states = self.state.write();
        states
            .entry((account, region.to_string()))
            .or_default()
            .clone()
    }

    /// Resolve a secret by name, full ARN, or partial ARN (moto-compatible).
    fn resolve_secret(state: &SmState, id: &str) -> Option<Arc<Secret>> {
        if id.is_empty() {
            return None;
        }
        if let Some(s) = state.get_secret(id) {
            return Some(s);
        }
        let secrets = state.secrets.read();
        // Full ARN
        for secret in secrets.values() {
            if secret.arn == id || secret.arn.split('/').next_back() == Some(id) {
                return Some(secret.clone());
            }
        }
        None
    }

    fn not_found(id: &str) -> AwsResponse {
        // moto's SecretNotFoundException carries code=404 (secretsmanager
        // exceptions.py:10-40); the twin 400ed, so HTTPStatusCode asserts
        // against either oracle missed.
        AwsResponse::error(
            404,
            "ResourceNotFoundException",
            &format!("Secrets Manager can't find the specified secret: {}", id),
        )
    }

    /// moto's SecretNotFoundException text is id-less
    /// (secretsmanager/exceptions.py:14-20); used where moto's own message is
    /// shown (stage lookups, batch errors).
    fn not_found_moto() -> AwsResponse {
        AwsResponse::error(
            404,
            "ResourceNotFoundException",
            "Secrets Manager can't find the specified secret.",
        )
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        let operation = req.operation.as_str();
        match operation {
            "CreateSecret" => self.create_secret(&req),
            "GetSecretValue" => self.get_secret_value(&req),
            "PutSecretValue" => self.put_secret_value(&req),
            "UpdateSecret" => self.update_secret(&req),
            "DeleteSecret" => self.delete_secret(&req),
            "RestoreSecret" => self.restore_secret(&req),
            "DescribeSecret" => self.describe_secret(&req),
            "ListSecrets" => self.list_secrets(&req),
            "ListSecretVersionIds" => self.list_secret_version_ids(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "GetResourcePolicy" => self.get_resource_policy(&req),
            "PutResourcePolicy" => self.put_resource_policy(&req),
            "DeleteResourcePolicy" => self.delete_resource_policy(&req),
            "ValidateResourcePolicy" => self.validate_resource_policy(&req),
            "GetRandomPassword" => self.get_random_password(&req),
            "RotateSecret" => self.rotate_secret(&req),
            "CancelRotateSecret" => self.cancel_rotate_secret(&req),
            "UpdateSecretVersionStage" => self.update_secret_version_stage(&req),
            "BatchGetSecretValue" => self.batch_get_secret_value(&req),
            "ReplicateSecretToRegions" => self.replicate_secret_to_regions(&req),
            "RemoveRegionsFromReplication" => self.remove_regions_from_replication(&req),
            "StopReplicationToReplica" => self.stop_replication_to_replica(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            other => AwsResponse::error(
                400,
                "InvalidParameterException",
                &format!("The operation {} is not implemented", other),
            ),
        }
    }

    /// moto's `_check_with_existing_secrets_and_versions` (models.py:627-681):
    /// duplicate-create/put/update guard keyed on the ClientRequestToken.
    /// Returns the short-dict echo when idempotent, `Err` on conflicts, or a
    /// flag telling the caller to proceed with the add-version path.
    fn check_existing_and_version(
        secret: &Arc<Secret>,
        token: Option<&str>,
        secret_string: Option<&str>,
        secret_binary: Option<&str>,
        on_create: bool,
    ) -> Result<bool, AwsResponse> {
        use std::collections::HashMap;

        let Some(token) = token else {
            return Err(AwsResponse::error(
                400,
                "ResourceExistsException",
                "A resource with the ID you requested already exists.",
            ));
        };
        let versions: HashMap<String, SecretVersion> = secret.versions.read().clone();
        let matching = versions.get(token);
        if let Some(matching) = matching {
            if matching.stages.iter().any(|s| s == "AWSPENDING") {
                // moto treats a pending-rotation token as a fresh add.
                return Ok(true);
            }
            if matching.secret_string.as_deref() == secret_string
                && matching.secret_binary.as_deref() == secret_binary
            {
                // Idempotent retry with the same token and value.
                return Ok(false);
            }
            return Err(AwsResponse::error(
                400,
                "ResourceExistsException",
                &format!(
                    "You can't use ClientRequestToken {token} because that value is already in use for a version of secret {}.",
                    secret.arn
                ),
            ));
        }
        if on_create {
            return Err(AwsResponse::error(
                400,
                "ResourceExistsException",
                &format!(
                    "The operation failed because the secret {} already exists.",
                    secret.name
                ),
            ));
        }
        Ok(true)
    }

    /// moto's `_from_client_request_token` (models.py:403-414): a supplied
    /// ClientRequestToken must be 32-64 characters and becomes the version
    /// id; otherwise a fresh uuid4 (hyphenated, like `str(uuid4())`) is
    /// minted.
    fn version_id_from_token(token: Option<&str>) -> Result<String, AwsResponse> {
        match token {
            Some(t) => {
                let len = t.chars().count();
                if !(32..=64).contains(&len) {
                    return Err(AwsResponse::error(
                        400,
                        "InvalidParameterException",
                        "ClientRequestToken must be 32-64 characters long.",
                    ));
                }
                Ok(t.to_string())
            }
            None => Ok(uuid::Uuid::new_v4().to_string()),
        }
    }

    /// moto's `_add_secret` update path on an existing secret: remove
    /// duplicate stage labels from old versions, then promote AWSCURRENT the
    /// way `reset_default_version` does (old current → exactly
    /// [`AWSPREVIOUS`]).
    fn add_version_to_existing(
        secret: &Arc<Secret>,
        version_id: &str,
        secret_string: Option<String>,
        secret_binary: Option<String>,
        version_stages: Vec<String>,
        kms_key_id: Option<String>,
    ) {
        if let Some(k) = kms_key_id {
            *secret.kms_key_id.write() = Some(k);
        }
        let mut versions = secret.versions.write();
        let has_current_stage = version_stages.iter().any(|s| s == "AWSCURRENT");
        if has_current_stage {
            for v in versions.values_mut() {
                v.stages.retain(|s| s != "AWSPREVIOUS");
            }
            if let Some(old_current) = versions
                .values_mut()
                .find(|v| v.stages.iter().any(|s| s == "AWSCURRENT"))
            {
                old_current.stages = vec!["AWSPREVIOUS".to_string()];
            }
            for v in versions.values_mut() {
                v.stages.retain(|s| s != "AWSCURRENT");
            }
        } else {
            for v in versions.values_mut() {
                v.stages.retain(|s| !version_stages.contains(s));
            }
        }
        versions.insert(
            version_id.to_string(),
            SecretVersion {
                version_id: version_id.to_string(),
                secret_string,
                secret_binary,
                stages: version_stages,
                created_date: Utc::now().timestamp() as f64,
            },
        );
    }

    fn create_secret(&self, req: &AwsRequest) -> AwsResponse {
        let name = req
            .params
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if name.is_empty() {
            return AwsResponse::error(400, "InvalidParameterException", "Name is required");
        }

        let state = self.get_state(req.account, &req.region);
        let secret_string = req
            .params
            .get("SecretString")
            .and_then(|v| v.as_str())
            .map(String::from);
        let secret_binary = req
            .params
            .get("SecretBinary")
            .and_then(|v| v.as_str())
            .map(String::from);
        let kms_key_id = req
            .params
            .get("KmsKeyId")
            .and_then(|v| v.as_str())
            .map(String::from);
        let token_raw = req
            .params
            .get("ClientRequestToken")
            .and_then(|v| v.as_str());

        if let Some(existing) = state.get_secret(&name) {
            // moto's _check_with_existing_secrets_and_versions(on_create=True).
            match Self::check_existing_and_version(
                &existing,
                token_raw,
                secret_string.as_deref(),
                secret_binary.as_deref(),
                true,
            ) {
                Err(e) => return e,
                Ok(false) => {
                    // Idempotent retry: moto echoes to_short_dict() → the
                    // current default version id.
                    let current_version_id = existing
                        .get_version("AWSCURRENT")
                        .map(|v| v.version_id.clone())
                        .unwrap_or_default();
                    return AwsResponse::json(
                        200,
                        json!({
                            "ARN": existing.arn,
                            "Name": existing.name,
                            "VersionId": current_version_id,
                        }),
                    );
                }
                Ok(true) => {
                    // Fresh token on an existing secret (or a pending-rotation
                    // token): moto's _check returns None and the request adds
                    // a new version through _add_secret.
                    let has_value = secret_string.is_some() || secret_binary.is_some();
                    if !has_value {
                        return AwsResponse::json(
                            200,
                            json!({ "ARN": existing.arn, "Name": existing.name }),
                        );
                    }
                    let version_id = match Self::version_id_from_token(token_raw) {
                        Ok(v) => v,
                        Err(e) => return e,
                    };
                    Self::add_version_to_existing(
                        &existing,
                        &version_id,
                        secret_string,
                        secret_binary,
                        vec!["AWSCURRENT".to_string()],
                        kms_key_id,
                    );
                    // moto create passes its response-layer defaults
                    // (description "", tags []) through update(), so the
                    // caller's description/tags reset on this path.
                    *existing.description.write() = None;
                    *existing.tags.write() = Vec::new();
                    return AwsResponse::json(
                        200,
                        json!({
                            "ARN": existing.arn,
                            "Name": existing.name,
                            "VersionId": version_id,
                        }),
                    );
                }
            }
        }

        // Fresh create.
        let version_id = match Self::version_id_from_token(token_raw) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let secret = Arc::new(Secret::new(name.clone(), req.account, req.region.clone()));
        let has_value = secret_string.is_some() || secret_binary.is_some();
        if has_value {
            let version = SecretVersion {
                version_id: version_id.clone(),
                secret_string: secret_string.clone(),
                secret_binary: secret_binary.clone(),
                stages: vec!["AWSCURRENT".to_string()],
                created_date: Utc::now().timestamp() as f64,
            };
            secret.versions.write().insert(version_id.clone(), version);
        }

        if let Some(desc) = req.params.get("Description").and_then(|v| v.as_str()) {
            *secret.description.write() = Some(desc.to_string());
        }
        if let Some(k) = kms_key_id {
            *secret.kms_key_id.write() = Some(k);
        }
        if let Some(tags) = req.params.get("Tags").and_then(|v| v.as_array()) {
            *secret.tags.write() = tags
                .iter()
                .filter(|t| t.get("Key").is_some())
                .map(|t| {
                    json!({
                        "Key": t["Key"].as_str().unwrap_or(""),
                        "Value": t["Value"].as_str().unwrap_or(""),
                    })
                })
                .collect();
        }

        state.put_secret(secret.clone());

        // moto's fresh create responds to_short_dict(include_version_id=…):
        // {ARN, Name} plus VersionId only when a value was stored (no
        // CreatedDate member).
        let mut resp = json!({
            "ARN": secret.arn,
            "Name": secret.name,
        });
        let resp_obj = resp.as_object_mut().unwrap();
        // moto's create response only carries VersionId when a value was
        // stored (to_short_dict(include_version_id=new_version)).
        if has_value {
            resp_obj.insert("VersionId".into(), json!(version_id));
        }
        AwsResponse::json(200, resp)
    }

    fn get_secret_value(&self, req: &AwsRequest) -> AwsResponse {
        let id = req
            .params
            .get("SecretId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = self.get_state(req.account, &req.region);
        let secret = match Self::resolve_secret(&state, &id) {
            Some(s) => s,
            None => return Self::not_found(&id),
        };
        let mut version_id = req.params.get("VersionId").and_then(|v| v.as_str());
        let version_stage = req.params.get("VersionStage").and_then(|v| v.as_str());

        // moto's check order (models.py get_secret_value:439-501): a
        // VersionId+VersionStage pair first cross-checks the stages on that
        // version (SecretStageVersionMismatchException, a 404 carrying
        // errorType InvalidRequestException), then a stage-only lookup
        // resolves the version id (miss → SecretNotFoundException), then the
        // deleted-secret guard, then the version lookup, and only a version
        // with no value raises SecretHasNoValueException.
        let versions = secret.versions.read();
        let stage_miss = |version_stage: Option<&str>| -> Option<AwsResponse> {
            let stage = version_stage?;
            if versions
                .values()
                .any(|v| v.stages.iter().any(|s| s == stage))
            {
                None
            } else {
                // moto raises SecretNotFoundException when no version
                // carries the stage (404, id-less text).
                Some(Self::not_found_moto())
            }
        };
        if let (Some(vid), Some(stage)) = (version_id, version_stage) {
            if let Some(v) = versions.get(vid) {
                if !v.stages.iter().any(|s| s == stage) {
                    return AwsResponse::error(
                        404,
                        "InvalidRequestException",
                        "You provided a VersionStage that is not associated to the provided VersionId.",
                    );
                }
            }
        }
        if version_id.is_none() {
            if let Some(miss) = stage_miss(version_stage) {
                return miss;
            }
            // moto models.py:453-461: a stage-only lookup RESOLVES the
            // version id from the stage before reading — `set version_id
            // to match version_stage`. The old path ignored the resolved
            // stage and defaulted AWSCURRENT, so a VersionStage=AWSPREVIOUS
            // read returned the NEW password after a rotation (the stage
            // map showed the right version; the value was AWSCURRENT's).
            if let Some(stage) = version_stage {
                if let Some(v) = versions
                    .values()
                    .find(|v| v.stages.iter().any(|s| s == stage))
                {
                    version_id = Some(v.version_id.as_str());
                }
            }
        }

        if *secret.removed.read() {
            // moto 5.2.4 models.py:465-469 — the message embeds moto's
            // backslash-continuation whitespace; mirrored verbatim.
            return AwsResponse::error(
                400,
                "InvalidRequestException",
                "An error occurred (InvalidRequestException) when calling the GetSecretValue operation: You tried to                 perform the operation on a secret that's currently marked deleted.",
            );
        }

        let version_id = version_id.map(String::from).or_else(|| {
            // The twin has no default_version_id field; the AWSCURRENT
            // version plays that role (moto `default_version_id`).
            versions
                .values()
                .find(|v| v.stages.iter().any(|s| s == "AWSCURRENT"))
                .map(|v| v.version_id.clone())
        });
        let version = version_id
            .as_ref()
            .and_then(|vid| versions.get(vid).cloned());
        let Some(version) = version else {
            // moto distinguishes the requested kind in the 404 text
            // (models.py:474-479).
            let kind = if version_id.is_some() {
                "VersionId"
            } else {
                "staging label"
            };
            return AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!(
                    "Secrets Manager can't find the specified secret value for {}: {}",
                    kind,
                    version_id.unwrap_or_else(|| "AWSCURRENT".to_string())
                ),
            );
        };
        if version.secret_string.is_none() && version.secret_binary.is_none() {
            // moto's SecretHasNoValueException — 404 with
            // errorType ResourceNotFoundException (moto keeps the
            // error code ResourceNotFoundException for this one).
            return AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!(
                    "Secrets Manager can't find the specified secret value for staging label: {}",
                    version_stage.unwrap_or("AWSCURRENT")
                ),
            );
        }

        let mut resp = json!({
            "ARN": secret.arn,
            "Name": secret.name,
            "VersionId": version.version_id,
            "VersionStages": version.stages,
            "CreatedDate": version.created_date,
        });
        if let Some(ref s) = version.secret_string {
            resp.as_object_mut()
                .unwrap()
                .insert("SecretString".into(), json!(s));
        }
        if let Some(ref b) = version.secret_binary {
            resp.as_object_mut()
                .unwrap()
                .insert("SecretBinary".into(), json!(b));
        }

        AwsResponse::json(200, resp)
    }

    fn put_secret_value(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let secret_id = req
            .params
            .get("SecretId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let secret = match Self::resolve_secret(&state, secret_id) {
            Some(s) => s,
            None => {
                return Self::not_found(
                    req.params
                        .get("SecretId")
                        .and_then(|v| v.as_str())
                        .unwrap_or(""),
                );
            }
        };
        // moto rejects a put with neither SecretString nor SecretBinary
        // (responses.py:148-156) — the old path demoted AWSCURRENT and
        // inserted a valueless AWSCURRENT that GetSecretValue could never
        // render.
        if req.params.get("SecretString").is_none() && req.params.get("SecretBinary").is_none() {
            return AwsResponse::error(
                400,
                "InvalidRequestException",
                "You must provide either SecretString or SecretBinary.",
            );
        }
        let secret_string = req
            .params
            .get("SecretString")
            .and_then(|v| v.as_str())
            .map(String::from);
        let secret_binary = req
            .params
            .get("SecretBinary")
            .and_then(|v| v.as_str())
            .map(String::from);
        // moto resolves the token first (validating its length), then runs
        // the duplicate-version check against it (_from_client_request_token
        // mints a uuid4 when absent, so a token-less put always proceeds).
        let token_raw = req
            .params
            .get("ClientRequestToken")
            .and_then(|v| v.as_str());
        let version_id = match Self::version_id_from_token(token_raw) {
            Ok(v) => v,
            Err(e) => return e,
        };
        match Self::check_existing_and_version(
            &secret,
            Some(&version_id),
            secret_string.as_deref(),
            secret_binary.as_deref(),
            false,
        ) {
            Err(e) => return e,
            Ok(false) => {
                // Idempotent put: moto echoes the token's version + stages.
                let stages = secret
                    .versions
                    .read()
                    .get(&version_id)
                    .map(|v| v.stages.clone())
                    .unwrap_or_default();
                return AwsResponse::json(
                    200,
                    json!({
                        "ARN": secret.arn,
                        "Name": secret.name,
                        "VersionId": version_id,
                        "VersionStages": stages,
                    }),
                );
            }
            Ok(true) => {}
        }

        let version_stages_json = {
            let mut vs: Vec<String> = req
                .params
                .get("VersionStages")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .map(|s| s.as_str().unwrap_or("").to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_else(|| vec!["AWSCURRENT".to_string()]);
            // moto: a first version is always forced to carry AWSCURRENT
            // (models.py:813-815).
            if secret.versions.read().is_empty() && !vs.iter().any(|s| s == "AWSCURRENT") {
                vs.push("AWSCURRENT".to_string());
            }
            vs
        };
        let version_stages = version_stages_json.clone();
        Self::add_version_to_existing(
            &secret,
            &version_id,
            secret_string,
            secret_binary,
            version_stages,
            None,
        );
        AwsResponse::json(
            200,
            json!({
                "ARN": secret.arn,
                "Name": secret.name,
                "VersionId": version_id,
                "VersionStages": version_stages_json,
            }),
        )
    }

    fn update_secret(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let secret_id = req
            .params
            .get("SecretId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let secret = match Self::resolve_secret(&state, secret_id) {
            Some(s) => s,
            None => return Self::not_found(secret_id),
        };
        let secret_string = req
            .params
            .get("SecretString")
            .and_then(|v| v.as_str())
            .map(String::from);
        let secret_binary = req
            .params
            .get("SecretBinary")
            .and_then(|v| v.as_str())
            .map(String::from);
        let kms_key_id = req
            .params
            .get("KmsKeyId")
            .and_then(|v| v.as_str())
            .map(String::from);
        let token_raw = req
            .params
            .get("ClientRequestToken")
            .and_then(|v| v.as_str());

        // moto's update_secret runs the duplicate check (with the RAW token)
        // before the deleted-state guard (models.py:554-578).
        match Self::check_existing_and_version(
            &secret,
            token_raw,
            secret_string.as_deref(),
            secret_binary.as_deref(),
            false,
        ) {
            Err(e) => return e,
            Ok(false) => {
                // Idempotent update: moto echoes to_short_dict() → {ARN,
                // Name, VersionId=current default}.
                let current_version_id = secret
                    .get_version("AWSCURRENT")
                    .map(|v| v.version_id.clone())
                    .unwrap_or_default();
                return AwsResponse::json(
                    200,
                    json!({
                        "ARN": secret.arn,
                        "Name": secret.name,
                        "VersionId": current_version_id,
                    }),
                );
            }
            Ok(true) => {}
        }

        if *secret.removed.read() {
            // moto 5.2.4 models.py:574-578 — exact text.
            return AwsResponse::error(
                400,
                "InvalidRequestException",
                "An error occurred (InvalidRequestException) when calling the UpdateSecret operation: You can't perform this operation on the secret because it was marked for deletion.",
            );
        }

        let has_value = secret_string.is_some() || secret_binary.is_some();
        if has_value {
            let version_id = match Self::version_id_from_token(token_raw) {
                Ok(v) => v,
                Err(e) => return e,
            };
            Self::add_version_to_existing(
                &secret,
                &version_id,
                secret_string,
                secret_binary,
                vec!["AWSCURRENT".to_string()],
                kms_key_id,
            );
        }

        if let Some(desc) = req.params.get("Description").and_then(|v| v.as_str()) {
            *secret.description.write() = Some(desc.to_string());
        }

        // moto's update response is to_short_dict(include_version_id=
        // new_version): VersionId present only when a new version was added.
        if has_value {
            let version_id = secret
                .get_version("AWSCURRENT")
                .map(|v| v.version_id.clone())
                .unwrap_or_default();
            AwsResponse::json(
                200,
                json!({
                    "ARN": secret.arn,
                    "Name": secret.name,
                    "VersionId": version_id,
                }),
            )
        } else {
            AwsResponse::json(
                200,
                json!({
                    "ARN": secret.arn,
                    "Name": secret.name,
                }),
            )
        }
    }

    /// moto's DeleteSecret: window-range and combo validations run before the
    /// id lookup; version-scoped responses mirror `delete_secret` in
    /// models.py:1059-1118.
    fn delete_secret(&self, req: &AwsRequest) -> AwsResponse {
        const DAY_SECS: f64 = 24.0 * 60.0 * 60.0;
        let state = self.get_state(req.account, &req.region);
        let secret_id = req
            .params
            .get("SecretId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let force = req
            .params
            .get("ForceDeleteWithoutRecovery")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let recovery_window = req
            .params
            .get("RecoveryWindowInDays")
            .and_then(|v| v.as_f64());

        // moto validates the window range regardless of Force, then the
        // combo, before touching the id (models.py:1059-1077) — the messages
        // embed moto's backslash-continuation whitespace, mirrored verbatim.
        if let Some(window) = recovery_window {
            if window < 7.0 || window > 30.0 {
                return AwsResponse::error(
                    400,
                    "InvalidParameterException",
                    "An error occurred (InvalidParameterException) when calling the DeleteSecret operation: The                 RecoveryWindowInDays value must be between 7 and 30 days (inclusive).",
                );
            }
        }
        if recovery_window.is_some() && force {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                "An error occurred (InvalidParameterException) when calling the DeleteSecret operation: You can't                 use ForceDeleteWithoutRecovery in conjunction with RecoveryWindowInDays.",
            );
        }

        if let Some(secret) = state.get_secret(secret_id).or_else(|| {
            if force {
                None
            } else {
                Self::resolve_secret(&state, secret_id)
            }
        }) {
            if !force && *secret.removed.read() {
                return AwsResponse::error(
                    400,
                    "InvalidRequestException",
                    "An error occurred (InvalidRequestException) when calling the DeleteSecret operation: You tried to                     perform the operation on a secret that's currently marked deleted.",
                );
            }
            if force {
                state.delete_secret(&secret.name);
                let deletion_date = Utc::now().timestamp() as f64;
                return AwsResponse::json(
                    200,
                    json!({
                        "ARN": secret.arn,
                        "Name": secret.name,
                        "DeletionDate": deletion_date,
                    }),
                );
            }

            *secret.removed.write() = true;
            let deletion_date =
                Utc::now().timestamp() as f64 + recovery_window.unwrap_or(30.0) * DAY_SECS;
            *secret.deletion_date.write() = Some(deletion_date);
            return AwsResponse::json(
                200,
                json!({
                    "ARN": secret.arn,
                    "Name": secret.name,
                    "DeletionDate": deletion_date,
                }),
            );
        }

        if !force {
            return Self::not_found(secret_id);
        }
        // moto's force delete of an unknown id synthesizes a full ARN
        // (with the 6-char suffix) and reports success (models.py:1079-1088).
        let suffix = crate::models::random_arn_suffix();
        AwsResponse::json(
            200,
            json!({
                "ARN": format!(
                    "arn:aws:secretsmanager:{}:{}:secret:{}-{suffix}",
                    req.region, req.account, secret_id
                ),
                "Name": secret_id,
                "DeletionDate": Utc::now().timestamp() as f64,
            }),
        )
    }

    fn restore_secret(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let secret_id = req
            .params
            .get("SecretId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let secret = match Self::resolve_secret(&state, secret_id) {
            Some(s) => s,
            None => return Self::not_found(secret_id),
        };

        *secret.removed.write() = false;
        *secret.deletion_date.write() = None;

        AwsResponse::json(
            200,
            json!({
                "ARN": secret.arn,
                "Name": secret.name,
            }),
        )
    }

    fn describe_secret(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let secret_id = req
            .params
            .get("SecretId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let secret = match Self::resolve_secret(&state, secret_id) {
            Some(s) => s,
            None => return Self::not_found(secret_id),
        };

        let versions = secret.versions.read();
        let mut version_stages = Map::new();
        for v in versions.values() {
            version_stages.insert(v.version_id.clone(), json!(v.stages));
        }

        // moto's FakeSecret.to_dict shape (models.py:239-281). The twin's
        // invented "Type" member is dropped — neither moto nor the real
        // botocore DescribeSecret shape has it.
        let mut resp = json!({
            "ARN": secret.arn,
            "Name": secret.name,
            "CreatedDate": secret.created_date,
            "LastChangedDate": secret.created_date,
            "Tags": *secret.tags.read(),
            "VersionIdsToStages": Value::Object(version_stages.clone()),
            // moto emits both maps for any dict rendering (models.py:259-267).
            "SecretVersionsToStages": Value::Object(version_stages),
        });
        let resp_obj = resp.as_object_mut().unwrap();
        // moto renders KmsKeyId when it is not the default service alias.
        if let Some(ref k) = *secret.kms_key_id.read() {
            if k != "alias/aws/secretsmanager" {
                resp_obj.insert("KmsKeyId".into(), json!(k));
            }
        }
        if let Some(ref desc) = *secret.description.read() {
            resp_obj.insert("Description".into(), json!(desc));
        }
        let removed = *secret.removed.read();
        if removed {
            if let Some(date) = *secret.deletion_date.read() {
                resp_obj.insert("DeletedDate".into(), json!(date));
            }
        }
        // Moto gates rotation members behind `rotation_requested`
        // (models.py:268-278), not merely rotation_enabled.
        if *secret.rotation_requested.read() {
            resp_obj.insert(
                "RotationEnabled".into(),
                json!(*secret.rotation_enabled.read()),
            );
            resp_obj.insert(
                "RotationLambdaARN".into(),
                json!(secret
                    .rotation_lambda_arn
                    .read()
                    .clone()
                    .unwrap_or_default()),
            );
            let auto = secret
                .rotation_rules
                .read()
                .as_ref()
                .and_then(|r| r.get("AutomaticallyAfterDays").and_then(|d| d.as_u64()))
                .unwrap_or(0);
            resp_obj.insert(
                "RotationRules".into(),
                json!({ "AutomaticallyAfterDays": auto }),
            );
            if *secret.rotation_enabled.read() {
                if let Some(next) = *secret.next_rotation_date.read() {
                    resp_obj.insert("NextRotationDate".into(), json!(next));
                }
            }
        }
        let replicas = secret.replicas.read().clone();
        if !replicas.is_empty() {
            resp_obj.insert(
                "ReplicationStatus".into(),
                json!(Self::replica_statuses(&replicas)),
            );
        }

        AwsResponse::json(200, resp)
    }

    /// moto's `list_secrets` (models.py:1040-1057): page at MaxResults
    /// (default 100), token is the row offset as a string, deleted secrets
    /// hidden unless IncludePlannedDeletion is set, and filters pre-validated
    /// then matched moto-style.
    fn list_secrets(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let filters = req
            .params
            .get("Filters")
            .and_then(|v| v.as_array())
            .cloned();
        if let Err(e) = validate_list_filters(filters.as_deref()) {
            return e;
        }
        let include_planned_deletion = req
            .params
            .get("IncludePlannedDeletion")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let max_results = req
            .params
            .get("MaxResults")
            .and_then(|v| v.as_u64())
            .map(|m| (m as usize).max(1))
            .unwrap_or(crate::models::LIST_SECRETS_MAX_RESULTS_DEFAULT);
        let start = req
            .params
            .get("NextToken")
            .and_then(|v| v.as_str())
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);

        let mut secret_list: Vec<Value> = Vec::new();
        for s in state.secrets.read().values() {
            if !include_planned_deletion && *s.removed.read() {
                continue;
            }
            if !matches_list_filters(s, filters.as_deref()) {
                continue;
            }
            secret_list.push(Self::secret_to_dict(s));
        }

        // moto's _get_secret_values_page_and_next_token.
        let end = start + max_results;
        let page: Vec<Value> = secret_list
            .iter()
            .skip(start)
            .take(max_results)
            .cloned()
            .collect();
        let next_token = if end < secret_list.len() {
            json!(end.to_string())
        } else {
            Value::Null
        };

        AwsResponse::json(200, json!({ "SecretList": page, "NextToken": next_token }))
    }

    /// moto's `FakeSecret.to_dict` rendering for a list entry: rotation
    /// members only after a RotateSecret request, both version→stage maps
    /// when versions exist, KmsKeyId hidden for the default alias, and no
    /// PrimaryRegion (that member appears on moto's replica-region listings,
    /// which the twin's in-region replica model does not produce).
    fn secret_to_dict(s: &Arc<Secret>) -> Value {
        let versions = s.versions.read();
        let mut version_stages = Map::new();
        for v in versions.values() {
            version_stages.insert(v.version_id.clone(), json!(v.stages));
        }
        let mut resp = json!({
            "ARN": s.arn,
            "Name": s.name,
            "CreatedDate": s.created_date,
            "LastChangedDate": s.created_date,
            "Tags": *s.tags.read(),
        });
        let resp_obj = resp.as_object_mut().unwrap();
        if let Some(ref k) = *s.kms_key_id.read() {
            if k != "alias/aws/secretsmanager" {
                resp_obj.insert("KmsKeyId".into(), json!(k));
            }
        }
        if let Some(ref desc) = *s.description.read() {
            resp_obj.insert("Description".into(), json!(desc));
        }
        if !versions.is_empty() {
            resp_obj.insert(
                "VersionIdsToStages".into(),
                Value::Object(version_stages.clone()),
            );
            resp_obj.insert(
                "SecretVersionsToStages".into(),
                Value::Object(version_stages),
            );
        }
        if *s.rotation_requested.read() {
            resp_obj.insert("RotationEnabled".into(), json!(*s.rotation_enabled.read()));
            resp_obj.insert(
                "RotationLambdaARN".into(),
                json!(s.rotation_lambda_arn.read().clone().unwrap_or_default()),
            );
            let auto = s
                .rotation_rules
                .read()
                .as_ref()
                .and_then(|r| r.get("AutomaticallyAfterDays").and_then(|d| d.as_u64()))
                .unwrap_or(0);
            resp_obj.insert(
                "RotationRules".into(),
                json!({ "AutomaticallyAfterDays": auto }),
            );
            if *s.rotation_enabled.read() {
                if let Some(next) = *s.next_rotation_date.read() {
                    resp_obj.insert("NextRotationDate".into(), json!(next));
                }
            }
        }
        let replicas = s.replicas.read().clone();
        if !replicas.is_empty() {
            resp_obj.insert(
                "ReplicationStatus".into(),
                json!(Self::replica_statuses(&replicas)),
            );
        }
        resp
    }

    /// moto's `list_secret_version_ids` has no pagination, filtering, or
    /// ordering guarantees: it dumps every version with per-render now()
    /// dates, an empty-string NextToken always present, and an "ARN" member
    /// that is actually the secret *name* (moto quirk). The twin keeps its
    /// real-ARN + true-dates rendering (documented divergences, closer to
    /// real AWS than moto), reconciles moto's insertion-order list with a
    /// deterministic created-date order, and omits the empty NextToken the
    /// way botocore models it.
    fn list_secret_version_ids(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let secret_id = req
            .params
            .get("SecretId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let secret = match Self::resolve_secret(&state, secret_id) {
            Some(s) => s,
            None => return Self::not_found(secret_id),
        };

        let versions = secret.versions.read();
        let mut ordered: Vec<&SecretVersion> = versions.values().collect();
        ordered.sort_by(|a, b| {
            a.created_date
                .partial_cmp(&b.created_date)
                .unwrap_or(std::cmp::Ordering::Equal)
                // moto's list order is insertion order; the twin's
                // stage-lifecycle ranks reconstruct it when seconds tie:
                // stage-less oldest, then AWSPREVIOUS, then AWSCURRENT.
                .then(stage_rank(a).cmp(&stage_rank(b)))
                .then(a.version_id.cmp(&b.version_id))
        });
        let version_list: Vec<Value> = ordered
            .iter()
            .map(|v| {
                json!({
                    "CreatedDate": v.created_date,
                    "LastAccessedDate": v.created_date,
                    "VersionId": v.version_id,
                    "VersionStages": v.stages,
                })
            })
            .collect();

        AwsResponse::json(
            200,
            json!({
                "Versions": version_list,
                "ARN": secret.arn,
                "Name": secret.name,
            }),
        )
    }

    fn tag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let secret_id = req
            .params
            .get("SecretId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let secret = match Self::resolve_secret(&state, secret_id) {
            Some(s) => s,
            None => return Self::not_found(secret_id),
        };
        let tags = req
            .params
            .get("Tags")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut existing = secret.tags.write();
        for t in tags {
            if let Some(key) = t.get("Key").and_then(|k| k.as_str()) {
                existing.retain(|e| e.get("Key").and_then(|k| k.as_str()) != Some(key));
                existing.push(json!({"Key": key, "Value": t.get("Value").and_then(|v| v.as_str()).unwrap_or("")}));
            }
        }
        AwsResponse::json(200, json!({}))
    }

    fn untag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let secret_id = req
            .params
            .get("SecretId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let secret = match Self::resolve_secret(&state, secret_id) {
            Some(s) => s,
            None => return Self::not_found(secret_id),
        };
        let tag_keys: Vec<String> = req
            .params
            .get("TagKeys")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|v| v.as_str().unwrap_or("").to_string())
                    .collect()
            })
            .unwrap_or_default();
        secret.tags.write().retain(|t| {
            !t.get("Key")
                .and_then(|k| k.as_str())
                .is_some_and(|k| tag_keys.contains(&k.to_string()))
        });
        AwsResponse::json(200, json!({}))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let secret_id = req
            .params
            .get("SecretId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let tags = Self::resolve_secret(&state, secret_id)
            .map(|s| s.tags.read().clone())
            .unwrap_or_default();
        AwsResponse::json(200, json!({ "Tags": tags }))
    }

    fn get_resource_policy(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let secret_id = req
            .params
            .get("SecretId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let secret = match Self::resolve_secret(&state, secret_id) {
            Some(s) => s,
            None => return Self::not_found(secret_id),
        };
        let mut resp = json!({
            "ARN": secret.arn,
            "Name": secret.name,
        });
        if let Some(policy) = &*secret.resource_policy.read() {
            resp.as_object_mut()
                .unwrap()
                .insert("ResourcePolicy".into(), json!(policy));
        }
        AwsResponse::json(200, resp)
    }

    fn put_resource_policy(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let secret_id = req
            .params
            .get("SecretId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let secret = match Self::resolve_secret(&state, secret_id) {
            Some(s) => s,
            None => return Self::not_found(secret_id),
        };
        let policy = req
            .params
            .get("ResourcePolicy")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if policy.is_empty() {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                "ResourcePolicy is required",
            );
        }
        *secret.resource_policy.write() = Some(policy.to_string());
        AwsResponse::json(
            200,
            json!({
                "ARN": secret.arn,
                "Name": secret.name,
            }),
        )
    }

    fn delete_resource_policy(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let secret_id = req
            .params
            .get("SecretId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let secret = match Self::resolve_secret(&state, secret_id) {
            Some(s) => s,
            None => return Self::not_found(secret_id),
        };
        *secret.resource_policy.write() = None;
        AwsResponse::json(
            200,
            json!({
                "ARN": secret.arn,
                "Name": secret.name,
            }),
        )
    }

    fn validate_resource_policy(&self, _req: &AwsRequest) -> AwsResponse {
        AwsResponse::json(
            200,
            json!({
                "PolicyValidationPassed": true,
                "ValidationErrors": [],
            }),
        )
    }

    fn get_random_password(&self, req: &AwsRequest) -> AwsResponse {
        let length = req
            .params
            .get("PasswordLength")
            .and_then(|v| v.as_u64())
            .unwrap_or(32) as usize;
        if length > 4096 {
            return AwsResponse::error(400, "ValidationException",
                "1 validation error detected: Value at 'passwordLength' failed to satisfy constraint: Member must have value less than or equal to 4096");
        }
        if length < 4 {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                "Password length is too short based on the required types.",
            );
        }
        let exclude_chars = req
            .params
            .get("ExcludeCharacters")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let exclude_numbers = req
            .params
            .get("ExcludeNumbers")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let exclude_punctuation = req
            .params
            .get("ExcludePunctuation")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let exclude_uppercase = req
            .params
            .get("ExcludeUppercase")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let exclude_lowercase = req
            .params
            .get("ExcludeLowercase")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let include_space = req
            .params
            .get("IncludeSpace")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let require_each = req
            .params
            .get("RequireEachIncludedType")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        // Alphabet mirrors moto (lowercase+uppercase / digits / punctuation).
        let mut alphabet = String::new();
        let mut required = String::new();
        let mut rng = rand_xorshift();
        if !exclude_lowercase && !exclude_uppercase {
            alphabet.push_str("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
            for required_char in [
                Self::pick_from("abcdefghijklmnopqrstuvwxyz", exclude_chars, &mut rng),
                Self::pick_from("ABCDEFGHIJKLMNOPQRSTUVWXYZ", exclude_chars, &mut rng),
            ]
            .into_iter()
            .flatten()
            {
                required.push(required_char);
            }
        } else if !exclude_lowercase {
            alphabet.push_str("abcdefghijklmnopqrstuvwxyz");
            required.extend(Self::pick_from(
                "abcdefghijklmnopqrstuvwxyz",
                exclude_chars,
                &mut rng,
            ));
        } else if !exclude_uppercase {
            alphabet.push_str("ABCDEFGHIJKLMNOPQRSTUVWXYZ");
            required.extend(Self::pick_from(
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
                exclude_chars,
                &mut rng,
            ));
        }
        if !exclude_numbers {
            alphabet.push_str("0123456789");
            required.extend(Self::pick_from("0123456789", exclude_chars, &mut rng));
        }
        if !exclude_punctuation {
            alphabet.push_str("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~");
            required.extend(Self::pick_from(
                "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",
                exclude_chars,
                &mut rng,
            ));
        }
        if include_space {
            alphabet.push(' ');
            required.push(' ');
        }
        // Remove any explicitly excluded characters from the pool.
        let alphabet: String = alphabet
            .chars()
            .filter(|c| !exclude_chars.contains(*c))
            .collect();
        // moto raises a clean error from its pools too (models.py:978-1015);
        // an emptied pool used to reach `% 0` — a reachable divide-by-zero
        // panic straight from user input.
        if alphabet.is_empty() {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                "You must include at least one character type in the password.",
            );
        }

        let mut password: String = (0..length)
            .map(|_| {
                let idx = (rand_next(&mut rng) % alphabet.len() as u64) as usize;
                alphabet.as_bytes()[idx] as char
            })
            .collect();

        if require_each {
            // Insert each required character at a random position, replacing
            // an existing character (password length preserved), like moto.
            let mut chars: Vec<char> = password.chars().collect();
            for c in required.chars() {
                if chars.is_empty() {
                    break;
                }
                let idx = (rand_next(&mut rng) % chars.len() as u64) as usize;
                chars[idx] = c;
            }
            password = chars.into_iter().collect();
        }

        AwsResponse::json(200, json!({ "RandomPassword": password }))
    }

    fn pick_from(alphabet: &str, exclude: &str, rng: &mut u64) -> Option<char> {
        let pool: Vec<char> = alphabet.chars().filter(|c| !exclude.contains(*c)).collect();
        if pool.is_empty() {
            // An exclution can empty a required pool; moto's random.choice("")
            // raises IndexError into its 500 envelope. The twin skips the
            // required-character insert instead of panicking on `% 0`.
            return None;
        }
        let idx = (rand_next(rng) % pool.len() as u64) as usize;
        Some(pool[idx])
    }

    fn rotate_secret(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let secret_id = req
            .params
            .get("SecretId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let secret = match Self::resolve_secret(&state, secret_id) {
            Some(s) => s,
            None => return Self::not_found(secret_id),
        };
        if *secret.removed.read() {
            // moto 5.2.4 models.py:851-855 — exact text (embedded spaces).
            return AwsResponse::error(
                400,
                "InvalidRequestException",
                "An error occurred (InvalidRequestException) when calling the RotateSecret operation: You tried to                 perform the operation on a secret that's currently marked deleted.",
            );
        }

        let lambda_arn = req
            .params
            .get("RotationLambdaARN")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let rules = req
            .params
            .get("RotationRules")
            .cloned()
            .unwrap_or(Value::Null);
        let rotate_immediately = req
            .params
            .get("RotateImmediately")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        if !lambda_arn.is_empty() && lambda_arn.len() > 2048 {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                "RotationLambdaARN must <= 2048 characters long.",
            );
        }
        // moto only validates an explicitly present AutomaticallyAfterDays;
        // its range rule is 1..=1000 with 0 explicitly rejected too
        // (models.py:862-871).
        let automatically_after = if rules.get("AutomaticallyAfterDays").is_some() {
            let days = rules
                .get("AutomaticallyAfterDays")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            if !(1..=1000).contains(&days) {
                return AwsResponse::error(
                    400,
                    "InvalidParameterException",
                    "RotationRules.AutomaticallyAfterDays must be within 1-1000.",
                );
            }
            days
        } else {
            0
        };

        // moto refuses to restart rotation while AWSPENDING and AWSCURRENT
        // still sit on the same version (models.py:883-895).
        {
            let versions = secret.versions.read();
            if let Some(pending) = versions
                .values()
                .find(|v| v.stages.iter().any(|s| s == "AWSPENDING"))
            {
                if pending.stages.iter().any(|s| s == "AWSCURRENT") {
                    return AwsResponse::error(
                        400,
                        "InvalidRequestException",
                        "Previous rotation request is still in progress.",
                    );
                }
            }
        }

        // moto 5.2.4 no-lambda rotation (models.py:897-973 + 966-973): the
        // new pending version carries *no value* (RotateImmediately defaults
        // to true) and, without a lambda, becomes AWSCURRENT while the old
        // current drops to exactly ["AWSPREVIOUS"]. The real value now lives
        // only in AWSPREVIOUS — matching the live-moto probe, value and all.
        {
            let mut versions = secret.versions.write();
            let has_versions = !versions.is_empty();
            let new_version_id = if has_versions {
                match Self::version_id_from_token(
                    req.params
                        .get("ClientRequestToken")
                        .and_then(|v| v.as_str()),
                ) {
                    Ok(v) => v,
                    Err(e) => return e,
                }
            } else {
                String::new()
            };
            if has_versions {
                for v in versions.values_mut() {
                    v.stages.retain(|s| s != "AWSPENDING" && s != "AWSPREVIOUS");
                }
                let current_value = if rotate_immediately {
                    (None, None)
                } else {
                    // moto copies the secret's top-level fields here; the
                    // twin's source of truth is the current AWSCURRENT
                    // version.
                    versions
                        .values()
                        .find(|v| v.stages.iter().any(|s| s == "AWSCURRENT"))
                        .map(|v| (v.secret_string.clone(), v.secret_binary.clone()))
                        .unwrap_or((None, None))
                };
                versions.insert(
                    new_version_id.clone(),
                    SecretVersion {
                        version_id: new_version_id.clone(),
                        secret_string: current_value.0,
                        secret_binary: current_value.1,
                        stages: vec!["AWSPENDING".to_string()],
                        created_date: Utc::now().timestamp() as f64,
                    },
                );
                // moto's reset_default_version + the explicit AWSCURRENT
                // relabel of the pending version (no-lambda branch).
                if let Some(old_current) = versions
                    .values_mut()
                    .find(|v| v.stages.iter().any(|s| s == "AWSCURRENT"))
                {
                    old_current.stages = vec!["AWSPREVIOUS".to_string()];
                }
                if let Some(pending) = versions.get_mut(&new_version_id) {
                    pending.stages = vec!["AWSCURRENT".to_string()];
                }
            }
        }

        // Rotation metadata: moto sets rotation_requested unconditionally,
        // rotation_enabled only when AutomaticallyAfterDays > 0, and always
        // records the lambda or "" (models.py:920-925).
        *secret.rotation_requested.write() = true;
        if !lambda_arn.is_empty() {
            *secret.rotation_lambda_arn.write() = Some(lambda_arn.to_string());
        } else {
            *secret.rotation_lambda_arn.write() = None;
        }
        if automatically_after != 0 {
            *secret.rotation_rules.write() = Some(rules.clone());
            *secret.next_rotation_date.write() =
                Some(Utc::now().timestamp() as f64 + automatically_after as f64 * 86400.0);
        }
        *secret.rotation_enabled.write() = automatically_after > 0;

        // Divergence note: with a RotationLambdaARN moto additionally
        // verifies the function and invokes its four rotation steps over the
        // lambda backend; the twin runs no lambda, so the no-lambda stage
        // handling above is served for either case (the ARN is stored and
        // describe-reflected only).

        let current_version_id = secret
            .get_version("AWSCURRENT")
            .map(|v| v.version_id.clone())
            .unwrap_or_default();
        let mut resp = json!({
            "ARN": secret.arn,
            "Name": secret.name,
        });
        if !current_version_id.is_empty() {
            resp.as_object_mut()
                .unwrap()
                .insert("VersionId".into(), json!(current_version_id));
        }
        AwsResponse::json(200, resp)
    }

    fn cancel_rotate_secret(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let secret_id = req
            .params
            .get("SecretId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let secret = match Self::resolve_secret(&state, secret_id) {
            Some(s) => s,
            None => return Self::not_found(secret_id),
        };
        if *secret.removed.read() {
            return AwsResponse::error(
                400,
                "InvalidRequestException",
                "You tried to perform the operation on a secret that's currently marked deleted.",
            );
        }
        // moto requires a configured lambda for CancelRotateSecret — its
        // message loses the space between "configured" and "and" (an
        // adjacent-string quirk in models.py:428-434, mirrored verbatim).
        if secret.rotation_lambda_arn.read().is_none() {
            return AwsResponse::error(
                400,
                "InvalidRequestException",
                "You tried to enable rotation on a secret that doesn't already have a Lambda function ARN configuredand you didn't include such an ARN as a parameter in this call.",
            );
        }
        *secret.rotation_enabled.write() = false;
        AwsResponse::json(
            200,
            json!({
                "ARN": secret.arn,
                "Name": secret.name,
                "VersionId": secret.get_version("AWSCURRENT").map(|v| v.version_id.clone()).unwrap_or_default(),
            }),
        )
    }

    fn update_secret_version_stage(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let secret_id = req
            .params
            .get("SecretId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let secret = match Self::resolve_secret(&state, secret_id) {
            Some(s) => s,
            None => return Self::not_found(secret_id),
        };
        let stage = req
            .params
            .get("VersionStage")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let remove_from = req
            .params
            .get("RemoveFromVersionId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let move_to = req
            .params
            .get("MoveToVersionId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let mut versions = secret.versions.write();
        if !remove_from.is_empty() {
            match versions.get_mut(&remove_from) {
                Some(v) => {
                    if !v.stages.contains(&stage) {
                        return AwsResponse::error(
                            400,
                            "InvalidParameterException",
                            &format!(
                                "Version stage {} not found in version {}",
                                stage, remove_from
                            ),
                        );
                    }
                    v.stages.retain(|s| s != &stage);
                }
                None => {
                    return AwsResponse::error(
                        400,
                        "InvalidParameterException",
                        &format!("Not a valid version: {}", remove_from),
                    )
                }
            }
            if stage == "AWSCURRENT" {
                for v in versions.values_mut() {
                    v.stages.retain(|s| s != "AWSPREVIOUS");
                }
                if let Some(v) = versions.get_mut(&remove_from) {
                    v.stages.push("AWSPREVIOUS".to_string());
                }
            }
        }
        if !move_to.is_empty() {
            match versions.get_mut(&move_to) {
                Some(v) => {
                    if stage == "AWSCURRENT" {
                        v.stages.retain(|s| s != "AWSPREVIOUS");
                    }
                    if !v.stages.contains(&stage) {
                        v.stages.push(stage.clone());
                    }
                }
                None => {
                    return AwsResponse::error(
                        400,
                        "InvalidParameterException",
                        &format!("Not a valid version: {}", move_to),
                    )
                }
            }
        }

        AwsResponse::json(
            200,
            json!({
                "ARN": secret.arn,
                "Name": secret.name,
            }),
        )
    }

    fn batch_get_secret_value(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let ids: Vec<String> = req
            .params
            .get("SecretIdList")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|s| s.as_str().unwrap_or("").to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        let filters = req
            .params
            .get("Filters")
            .and_then(|v| v.as_array())
            .cloned();
        if let Err(e) = validate_list_filters(filters.as_deref()) {
            return e;
        }

        // moto's cross-checks (models.py:503-551): both selectors rejected;
        // MaxResults only makes sense with Filters.
        if !ids.is_empty() && filters.as_ref().map(|f| !f.is_empty()).unwrap_or(false) {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                "Either 'SecretIdList' or 'Filters' must be provided, but not both.",
            );
        }
        let max_results = req.params.get("MaxResults").and_then(|v| v.as_u64());
        if max_results.is_some() && !filters.as_ref().map(|f| !f.is_empty()).unwrap_or(false) {
            return AwsResponse::error(
                400,
                "InvalidParameterException",
                "'Filters' not specified. 'Filters' must also be specified when 'MaxResults' is provided.",
            );
        }

        let mut values: Vec<Value> = Vec::new();
        let mut errors: Vec<Value> = Vec::new();
        let mut no_value = false;
        if !ids.is_empty() {
            for id in &ids {
                match self.batch_single(&state, id, req) {
                    Ok(v) => values.push(v),
                    Err(Value::Null) => no_value = true,
                    Err(entry) => errors.push(entry),
                }
            }
        } else if filters.as_ref().map(|f| !f.is_empty()).unwrap_or(false) {
            for secret in state.secrets.read().values().cloned().collect::<Vec<_>>() {
                if matches_list_filters(&secret, filters.as_deref()) {
                    match self.batch_single(&state, &secret.name, req) {
                        Ok(v) => values.push(v),
                        Err(Value::Null) => no_value = true,
                        Err(entry) => errors.push(entry),
                    }
                }
            }
        }
        if no_value {
            // moto's SecretHasNoValueException escapes the per-secret catch
            // and fails the whole request with the AWSCURRENT staging label.
            return AwsResponse::error(
                404,
                "ResourceNotFoundException",
                "Secrets Manager can't find the specified secret value for staging label: AWSCURRENT",
            );
        }

        // moto's _get_secret_values_page_and_next_token.
        let max = max_results
            .map(|m| (m as usize).max(1))
            .unwrap_or(crate::models::LIST_SECRETS_MAX_RESULTS_DEFAULT);
        let start = req
            .params
            .get("NextToken")
            .and_then(|v| v.as_str())
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let end = start + max;
        let page: Vec<Value> = values.iter().skip(start).take(max).cloned().collect();
        let next_token = if end < values.len() {
            json!(end.to_string())
        } else {
            Value::Null
        };

        let mut resp = json!({ "SecretValues": page, "Errors": errors });
        if !next_token.is_null() {
            resp.as_object_mut()
                .unwrap()
                .insert("NextToken".into(), next_token);
        }
        AwsResponse::json(200, resp)
    }

    /// One BatchGetSecretValue entry — moto reuses `get_secret_value` with
    /// empty version args (models.py:522-535); a deleted secret is caught as
    /// an `InvalidRequestException` row (moto keeps SecretHasNoValue
    /// *uncaught*, so a valueless current version fails the whole request —
    /// mirrored by the Err(None) arm).
    fn batch_single(&self, state: &SmState, id: &str, req: &AwsRequest) -> Result<Value, Value> {
        let Some(secret) = Self::resolve_secret(state, id) else {
            return Err(json!({
                "SecretId": id,
                "ErrorCode": "ResourceNotFoundException",
                "Message": "Secrets Manager can't find the specified secret.",
            }));
        };
        if *secret.removed.read() {
            return Err(json!({
                "SecretId": id,
                "ErrorCode": "InvalidRequestException",
                "Message": "An error occurred (InvalidRequestException) when calling the GetSecretValue operation: You tried to                 perform the operation on a secret that's currently marked deleted.",
            }));
        }
        let version = secret
            .get_version("AWSCURRENT")
            .or_else(|| secret.get_version_by_id(""));
        let Some(version) = version else {
            return Err(json!({
                "SecretId": id,
                "ErrorCode": "ResourceNotFoundException",
                "Message": "Secrets Manager can't find the specified secret.",
            }));
        };
        if version.secret_string.is_none() && version.secret_binary.is_none() {
            // moto's SecretHasNoValueException escapes its catch in
            // batch_get_secret_value and fails the whole call.
            return Err(Value::Null);
        }
        let mut obj = json!({
            "ARN": secret.arn,
            "Name": secret.name,
            "VersionId": version.version_id,
            "VersionStages": version.stages,
            "CreatedDate": version.created_date,
        });
        if let Some(ref s) = version.secret_string {
            obj.as_object_mut()
                .unwrap()
                .insert("SecretString".into(), json!(s));
        }
        if let Some(ref b) = version.secret_binary {
            obj.as_object_mut()
                .unwrap()
                .insert("SecretBinary".into(), json!(b));
        }
        let _ = req;
        Ok(obj)
    }

    fn replicate_secret_to_regions(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let secret_id = req
            .params
            .get("SecretId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let secret = match Self::resolve_secret(&state, secret_id) {
            Some(s) => s,
            None => return Self::not_found(secret_id),
        };
        if *secret.removed.read() {
            return AwsResponse::error(
                400,
                "InvalidRequestException",
                "You tried to perform the operation on a secret that's currently marked deleted.",
            );
        }

        let add_regions: Vec<String> = req
            .params
            .get("AddReplicaRegions")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|r| r["Region"].as_str().unwrap_or("").to_string())
                    .collect()
            })
            .unwrap_or_default();

        let mut replicas = secret.replicas.write();
        for region in add_regions {
            if !region.is_empty() && !replicas.contains(&region) {
                replicas.push(region);
            }
        }
        let statuses = Self::replica_statuses(&replicas);

        AwsResponse::json(
            200,
            json!({
                "ARN": secret.arn,
                "ReplicationStatus": statuses,
            }),
        )
    }

    fn remove_regions_from_replication(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let secret_id = req
            .params
            .get("SecretId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let secret = match Self::resolve_secret(&state, secret_id) {
            Some(s) => s,
            None => return Self::not_found(secret_id),
        };

        let remove_regions: Vec<String> = req
            .params
            .get("RemoveReplicaRegions")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|r| r.as_str().unwrap_or("").to_string())
                    .collect()
            })
            .unwrap_or_default();

        let mut replicas = secret.replicas.write();
        replicas.retain(|r| !remove_regions.contains(r));
        let statuses = Self::replica_statuses(&replicas);

        AwsResponse::json(
            200,
            json!({
                "ARN": secret.arn,
                "ReplicationStatus": statuses,
            }),
        )
    }

    fn stop_replication_to_replica(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let secret_id = req
            .params
            .get("SecretId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let secret = match Self::resolve_secret(&state, secret_id) {
            Some(s) => s,
            None => return Self::not_found(secret_id),
        };
        AwsResponse::json(
            200,
            json!({
                "ARN": secret.arn,
                "Name": secret.name,
            }),
        )
    }

    fn replica_statuses(regions: &[String]) -> Vec<Value> {
        regions
            .iter()
            .map(|region| {
                json!({
                    "Region": region,
                    "KmsKeyId": "alias/aws/secretsmanager",
                    "Status": "InSync",
                    "StatusMessage": "Replication succeeded",
                })
            })
            .collect()
    }
}

/// moto's ListSecrets filter validation (secretsmanager/responses.py
/// `_validate_filters`). The enum list in the error text is moto's own
/// hardcode — it omits primary-region/owning-service even though those keys
/// are accepted.
fn validate_list_filters(filters: Option<&[Value]>) -> Result<(), AwsResponse> {
    const FILTER_KEYS: [&str; 7] = [
        "all",
        "name",
        "description",
        "tag-key",
        "tag-value",
        "primary-region",
        "owning-service",
    ];
    for (idx, f) in (filters.unwrap_or(&[])).iter().enumerate() {
        let Some(key) = f.get("Key").and_then(|k| k.as_str()) else {
            return Err(AwsResponse::error(
                400,
                "InvalidParameterException",
                "Invalid filter key",
            ));
        };
        if !FILTER_KEYS.contains(&key) {
            return Err(AwsResponse::error(
                400,
                "ValidationException",
                &format!(
                    "1 validation error detected: Value '{key}' at 'filters.{}.member.key' failed to satisfy constraint: \
                     Member must satisfy enum value set: [all, name, tag-key, description, tag-value]",
                    idx + 1
                ),
            ));
        }
        if f.get("Values").is_none() {
            return Err(AwsResponse::error(
                400,
                "InvalidParameterException",
                &format!("Invalid filter values for key: {key}"),
            ));
        }
    }
    Ok(())
}

/// moto's `list_secrets/filters.py` matcher: `!`-prefixed negations first,
/// then positive prefix/word matches per key.
fn matches_list_filters(secret: &Arc<Secret>, filters: Option<&[Value]>) -> bool {
    for f in filters.unwrap_or(&[]) {
        let key = f.get("Key").and_then(|k| k.as_str()).unwrap_or("");
        let values: Vec<String> = f
            .get("Values")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let matched = match key {
            "name" => {
                let strings = vec![secret.name.clone()];
                moto_matcher(&values, &strings, true, true)
            }
            "description" => {
                let desc = secret.description.read().clone();
                let Some(desc) = desc else {
                    return false;
                };
                moto_matcher(&values, &[desc], false, false)
            }
            "tag-key" => {
                let keys: Vec<String> = secret
                    .tags
                    .read()
                    .iter()
                    .filter_map(|t| t.get("Key").and_then(|k| k.as_str()).map(String::from))
                    .collect();
                if keys.is_empty() {
                    return false;
                }
                moto_matcher(&values, &keys, true, true)
            }
            "tag-value" => {
                let vals: Vec<String> = secret
                    .tags
                    .read()
                    .iter()
                    .filter_map(|t| t.get("Value").and_then(|k| k.as_str()).map(String::from))
                    .collect();
                if vals.is_empty() {
                    return false;
                }
                moto_matcher(&values, &vals, true, true)
            }
            "all" => {
                let desc = (*secret.description.read()).clone();
                let mut attrs = vec![secret.name.clone()];
                if let Some(desc) = desc {
                    if !desc.is_empty() {
                        attrs.push(desc);
                    }
                }
                for t in secret.tags.read().iter() {
                    if let Some(k) = t.get("Key").and_then(|x| x.as_str()) {
                        attrs.push(k.to_string());
                    }
                    if let Some(v) = t.get("Value").and_then(|k| k.as_str()) {
                        attrs.push(v.to_string());
                    }
                }
                moto_matcher(&values, &attrs, false, false)
            }
            // moto's filter_primary_region requires existing replicas.
            "primary-region" => {
                !secret.replicas.read().is_empty() && values.iter().any(|v| *v == secret.region)
            }
            "owning-service" => false,
            _ => true,
        };
        if !matched {
            return false;
        }
    }
    true
}

/// `_matcher` from moto's list_secrets/filters.py.
fn moto_matcher(
    patterns: &[String],
    strings: &[String],
    match_prefix: bool,
    case_sensitive: bool,
) -> bool {
    for pattern in patterns.iter().filter(|p| p.starts_with('!')) {
        if strings
            .iter()
            .any(|s| !moto_match_pattern(&pattern[1..], s, match_prefix, case_sensitive))
        {
            return true;
        }
    }
    for pattern in patterns.iter().filter(|p| !p.starts_with('!')) {
        if strings
            .iter()
            .any(|s| moto_match_pattern(pattern, s, match_prefix, case_sensitive))
        {
            return true;
        }
    }
    false
}

/// `_match_pattern` from moto's list_secrets/filters.py.
fn moto_match_pattern(
    pattern: &str,
    value: &str,
    match_prefix: bool,
    case_sensitive: bool,
) -> bool {
    if match_prefix {
        if case_sensitive {
            return value.starts_with(pattern);
        }
        return value.to_lowercase().starts_with(&pattern.to_lowercase());
    }
    let mut pattern_words = moto_split_words(pattern);
    if pattern_words.is_empty() {
        return false;
    }
    let mut value_words = moto_split_words(value);
    if !case_sensitive {
        pattern_words = pattern_words.iter().map(|p| p.to_lowercase()).collect();
        value_words = value_words.iter().map(|v| v.to_lowercase()).collect();
    }
    pattern_words
        .iter()
        .all(|pw| value_words.iter().any(|vw| vw.starts_with(pw.as_str())))
}

/// `split_words` from moto's list_secrets/filters.py: split by one special
/// character (when no other special character also occurs), then by
/// uppercase runs.
fn moto_split_words(s: &str) -> Vec<String> {
    const SPECIAL: [char; 7] = ['/', '-', '_', '+', '=', '.', '@'];
    if s.chars().count() == 1 && SPECIAL.contains(&s.chars().next().unwrap_or(' ')) {
        return vec![];
    }
    for ch in SPECIAL {
        if s.contains(ch) {
            let others: Vec<char> = SPECIAL.iter().copied().filter(|c| *c != ch).collect();
            if others.iter().any(|c| s.contains(*c)) {
                return vec![s.to_string()];
            }
            return s.split(ch).flat_map(moto_split_by_uppercase).collect();
        }
    }
    moto_split_by_uppercase(s)
}

/// `split_by_uppercase` per moto: split before upper-case runs.
fn moto_split_by_uppercase(s: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut prev_lower = false;
    for c in s.chars() {
        if c.is_ascii_uppercase() && prev_lower {
            words.push(current.clone());
            current.clear();
        }
        prev_lower = c.is_ascii_lowercase();
        current.push(c);
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
        .into_iter()
        .map(|w| w.trim().to_string())
        .filter(|w| !w.is_empty())
        .collect()
}

/// Insertion-order reconstruction for moto's list order: stage-less versions
/// are oldest, AWSPREVIOUS next, AWSCURRENT newest.
fn stage_rank(v: &SecretVersion) -> u8 {
    if v.stages.iter().any(|s| s == "AWSCURRENT") {
        2
    } else if v.stages.iter().any(|s| s == "AWSPREVIOUS") {
        1
    } else {
        0
    }
}

/// Simple xorshift random generator seeded from the system clock.
fn rand_xorshift() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as u64;
    (nanos % 9223372036854775783).max(123456789)
}

fn rand_next(rng: &mut u64) -> u64 {
    // xorshift64*: deterministic pseudo-random stream.
    let mut x = *rng;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *rng = x;
    x.wrapping_mul(0x2545F4914F6CDD1D)
}

impl Default for SecretsManagerHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::json;

    fn make_req(operation: &str, params: Value) -> AwsRequest {
        AwsRequest {
            service: "secretsmanager".to_string(),
            operation: operation.to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
        }
    }

    #[test]
    fn test_create_and_get_secret() {
        let handler = SecretsManagerHandler::new();

        let req = make_req(
            "CreateSecret",
            json!({
                "Name": "db/password",
                "SecretString": "hunter2"
            }),
        );
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("db/password"));

        let req = make_req("GetSecretValue", json!({"SecretId": "db/password"}));
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("hunter2"));
    }

    #[test]
    fn test_list_secrets() {
        let handler = SecretsManagerHandler::new();
        handler.handle(make_req(
            "CreateSecret",
            json!({
                "Name": "secret1",
                "SecretString": "value1"
            }),
        ));
        handler.handle(make_req(
            "CreateSecret",
            json!({
                "Name": "secret2",
                "SecretString": "value2"
            }),
        ));

        let req = make_req("ListSecrets", json!({}));
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("secret1"));
        assert!(resp.body.contains("secret2"));
    }

    #[test]
    fn test_delete_and_restore_secret() {
        let handler = SecretsManagerHandler::new();
        handler.handle(make_req(
            "CreateSecret",
            json!({
                "Name": "to-delete",
                "SecretString": "temp"
            }),
        ));

        let req = make_req("DeleteSecret", json!({"SecretId": "to-delete"}));
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);

        let req = make_req("RestoreSecret", json!({"SecretId": "to-delete"}));
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
    }

    #[test]
    fn test_force_delete_allows_recreate() {
        let handler = SecretsManagerHandler::new();
        handler.handle(make_req(
            "CreateSecret",
            json!({"Name": "tagging/secret", "SecretString": "val"}),
        ));
        let resp = handler.handle(make_req(
            "DeleteSecret",
            json!({"SecretId": "tagging/secret", "ForceDeleteWithoutRecovery": true}),
        ));
        assert_eq!(resp.status, 200);

        let resp = handler.handle(make_req(
            "CreateSecret",
            json!({"Name": "tagging/secret", "SecretString": "val"}),
        ));
        assert_eq!(resp.status, 200, "recreate after force delete must succeed");
    }

    #[test]
    fn test_create_secret_stores_tags() {
        let handler = SecretsManagerHandler::new();
        handler.handle(make_req(
            "CreateSecret",
            json!({
                "Name": "tagged/secret",
                "SecretString": "val",
                "Tags": [{"Key": "env", "Value": "test"}],
            }),
        ));
        let resp = handler.handle(make_req(
            "DescribeSecret",
            json!({"SecretId": "tagged/secret"}),
        ));
        assert!(resp.body.contains("env"));
        assert!(resp.body.contains("test"));
    }

    #[test]
    fn test_resource_policy_roundtrip() {
        let handler = SecretsManagerHandler::new();
        handler.handle(make_req(
            "CreateSecret",
            json!({"Name": "policy/secret", "SecretString": "val"}),
        ));
        let policy =
            json!({"Version": "2012-10-17", "Statement": [{"Effect": "Allow"}]}).to_string();
        handler.handle(make_req(
            "PutResourcePolicy",
            json!({"SecretId": "policy/secret", "ResourcePolicy": policy}),
        ));
        let resp = handler.handle(make_req(
            "GetResourcePolicy",
            json!({"SecretId": "policy/secret"}),
        ));
        assert!(resp.body.contains("Statement"));
    }

    #[test]
    fn test_random_password_excludes_numbers() {
        let handler = SecretsManagerHandler::new();
        let resp = handler.handle(make_req(
            "GetRandomPassword",
            json!({"PasswordLength": 20, "ExcludeNumbers": true, "ExcludePunctuation": true}),
        ));
        let parsed: Value = serde_json::from_str(&resp.body).unwrap();
        let pw = parsed["RandomPassword"].as_str().unwrap();
        assert_eq!(pw.len(), 20);
        assert!(pw.chars().all(|c| !c.is_ascii_digit()));
    }

    #[test]
    fn test_rotation_state() {
        let handler = SecretsManagerHandler::new();
        handler.handle(make_req(
            "CreateSecret",
            json!({"Name": "rot/secret", "SecretString": "val"}),
        ));
        handler.handle(make_req(
            "RotateSecret",
            json!({
                "SecretId": "rot/secret",
                "RotationLambdaARN": "arn:aws:lambda:us-east-1:123456789012:function:r",
                "RotationRules": {"AutomaticallyAfterDays": 30},
            }),
        ));
        let resp = handler.handle(make_req(
            "DescribeSecret",
            json!({"SecretId": "rot/secret"}),
        ));
        assert!(resp.body.contains("RotationEnabled\":true"));

        let resp = handler.handle(make_req(
            "CancelRotateSecret",
            json!({"SecretId": "rot/secret"}),
        ));
        assert_eq!(resp.status, 200);
        let resp = handler.handle(make_req(
            "DescribeSecret",
            json!({"SecretId": "rot/secret"}),
        ));
        assert!(resp.body.contains("RotationEnabled\":false"));
    }

    #[test]
    fn test_update_secret_version_stage_moves_current() {
        let handler = SecretsManagerHandler::new();
        let create_resp = handler.handle(make_req(
            "CreateSecret",
            json!({"Name": "stage/secret", "SecretString": "v1"}),
        ));
        let created: Value = serde_json::from_str(&create_resp.body).unwrap();
        let v1 = created["VersionId"].as_str().unwrap().to_string();
        let put_resp = handler.handle(make_req(
            "PutSecretValue",
            json!({"SecretId": "stage/secret", "SecretString": "v2"}),
        ));
        let put: Value = serde_json::from_str(&put_resp.body).unwrap();
        let v2 = put["VersionId"].as_str().unwrap().to_string();

        let resp = handler.handle(make_req(
            "UpdateSecretVersionStage",
            json!({
                "SecretId": "stage/secret",
                "VersionStage": "AWSCURRENT",
                "MoveToVersionId": v1,
                "RemoveFromVersionId": v2,
            }),
        ));
        assert_eq!(resp.status, 200);

        let resp = handler.handle(make_req(
            "GetSecretValue",
            json!({"SecretId": "stage/secret", "VersionStage": "AWSCURRENT"}),
        ));
        assert!(resp.body.contains("v1"));
    }

    #[test]
    fn test_replication_ops() {
        let handler = SecretsManagerHandler::new();
        handler.handle(make_req(
            "CreateSecret",
            json!({"Name": "repl/secret", "SecretString": "val"}),
        ));
        let resp = handler.handle(make_req(
            "ReplicateSecretToRegions",
            json!({
                "SecretId": "repl/secret",
                "AddReplicaRegions": [{"Region": "eu-west-1"}],
                "ForceOverwriteReplicaSecret": true,
            }),
        ));
        assert!(resp.body.contains("ReplicationStatus"));

        let resp = handler.handle(make_req(
            "RemoveRegionsFromReplication",
            json!({"SecretId": "repl/secret", "RemoveReplicaRegions": ["eu-west-1"]}),
        ));
        assert!(resp.body.contains("ARN"));

        let resp = handler.handle(make_req(
            "StopReplicationToReplica",
            json!({"SecretId": "repl/secret"}),
        ));
        assert!(resp.body.contains("ARN"));
    }

    #[test]
    fn test_batch_get_secret_value() {
        let handler = SecretsManagerHandler::new();
        handler.handle(make_req(
            "CreateSecret",
            json!({"Name": "batch/1", "SecretString": "v1"}),
        ));
        let resp = handler.handle(make_req(
            "BatchGetSecretValue",
            json!({"SecretIdList": ["batch/1", "batch/missing"]}),
        ));
        let parsed: Value = serde_json::from_str(&resp.body).unwrap();
        assert!(parsed["SecretValues"].as_array().unwrap().len() == 1);
        assert!(parsed["Errors"].as_array().unwrap().len() == 1);
    }

    // ---- wave-6 ledger items (moto oracle: secretsmanager/models.py) ----

    fn body_of(resp: &AwsResponse) -> Value {
        serde_json::from_str(resp.body.as_str()).unwrap()
    }

    fn created(handler: &SecretsManagerHandler, name: &str, value: &str) -> String {
        let resp = handler.handle(make_req(
            "CreateSecret",
            json!({ "Name": name, "SecretString": value }),
        ));
        assert_eq!(resp.status, 200, "{}", resp.body);
        body_of(&resp)["VersionId"].as_str().unwrap().to_string()
    }

    /// Item 11: the secret ARN carries moto's 6-letter suffix (no digits).
    #[test]
    fn test_secret_arn_suffix() {
        let handler = SecretsManagerHandler::new();
        created(&handler, "sfx/secret", "v");
        let describe = handler.handle(make_req(
            "DescribeSecret",
            json!({ "SecretId": "sfx/secret" }),
        ));
        let arn = body_of(&describe)["ARN"].as_str().unwrap().to_string();
        let tail = arn.rsplit(':').next().unwrap();
        let (_, suffix) = tail.rsplit_once('-').unwrap();
        assert_eq!(suffix.len(), 6, "suffix is 6 chars: {arn}");
        assert!(
            suffix.chars().all(|c| c.is_ascii_alphabetic()),
            "suffix letters only: {arn}"
        );
        // The full ARN resolves as a SecretId (round trip).
        let resp = handler.handle(make_req("GetSecretValue", json!({ "SecretId": arn })));
        assert_eq!(resp.status, 200);
    }

    /// Item 12: VersionId+VersionStage mismatch, stage miss and valueless
    /// versions discriminate moto-style (statuses + error types + texts).
    #[test]
    fn test_get_secret_value_stage_discrimination() {
        let handler = SecretsManagerHandler::new();
        let vid = created(&handler, "stage-d/secret", "v1");
        // Known version id + wrong stage → SecretStageVersionMismatchException
        // (404 status, errorType InvalidRequestException).
        let resp = handler.handle(make_req(
            "GetSecretValue",
            json!({ "SecretId": "stage-d/secret", "VersionId": vid, "VersionStage": "AWSPREVIOUS" }),
        ));
        assert_eq!(resp.status, 404);
        assert_eq!(body_of(&resp)["__type"], "InvalidRequestException");
        assert_eq!(
            body_of(&resp)["message"],
            "You provided a VersionStage that is not associated to the provided VersionId."
        );
        // Stage with no holding version → moto's id-less 404.
        let resp = handler.handle(make_req(
            "GetSecretValue",
            json!({ "SecretId": "stage-d/secret", "VersionStage": "AWSPENDING" }),
        ));
        assert_eq!(resp.status, 404);
        assert_eq!(
            body_of(&resp)["message"],
            "Secrets Manager can't find the specified secret."
        );
        // Unknown VersionId → the VersionId-flavoured 404.
        let resp = handler.handle(make_req(
            "GetSecretValue",
            json!({ "SecretId": "stage-d/secret", "VersionId": "no-such-version-id-0000000000000000" }),
        ));
        assert_eq!(resp.status, 404);
        assert_eq!(
            body_of(&resp)["message"],
            "Secrets Manager can't find the specified secret value for VersionId: no-such-version-id-0000000000000000"
        );
        // GetSecretValue renders VersionStages (moto's response_data).
        let resp = handler.handle(make_req(
            "GetSecretValue",
            json!({ "SecretId": "stage-d/secret" }),
        ));
        assert_eq!(body_of(&resp)["VersionStages"], json!(["AWSCURRENT"]));
        // A valueless secret (create w/o value) → SecretHasNoValue family,
        // and the create response carries no VersionId.
        let resp = handler.handle(make_req("CreateSecret", json!({ "Name": "stage-d/empty" })));
        assert_eq!(resp.status, 200);
        assert!(body_of(&resp).get("VersionId").is_none());
        let resp = handler.handle(make_req(
            "GetSecretValue",
            json!({ "SecretId": "stage-d/empty" }),
        ));
        assert_eq!(resp.status, 404);
        assert_eq!(
            body_of(&resp)["message"],
            "Secrets Manager can't find the specified secret value for staging label: AWSCURRENT"
        );
    }

    /// Item 13: UpdateSecret on a deleted secret; DeleteSecret window/force
    /// permutations with moto's texts.
    #[test]
    fn test_update_delete_secret_guards() {
        let handler = SecretsManagerHandler::new();
        created(&handler, "guards/secret", "v");
        // The window range validates regardless of force, then the combo.
        let resp = handler.handle(make_req(
            "DeleteSecret",
            json!({ "SecretId": "guards/secret", "RecoveryWindowInDays": 3 }),
        ));
        assert_eq!(resp.status, 400);
        assert_eq!(
            body_of(&resp)["message"],
            "An error occurred (InvalidParameterException) when calling the DeleteSecret operation: The                 RecoveryWindowInDays value must be between 7 and 30 days (inclusive)."
        );
        let resp = handler.handle(make_req(
            "DeleteSecret",
            json!({
                "SecretId": "guards/secret",
                "RecoveryWindowInDays": 10,
                "ForceDeleteWithoutRecovery": true
            }),
        ));
        assert_eq!(
            body_of(&resp)["message"],
            "An error occurred (InvalidParameterException) when calling the DeleteSecret operation: You can't                 use ForceDeleteWithoutRecovery in conjunction with RecoveryWindowInDays."
        );

        let resp = handler.handle(make_req(
            "DeleteSecret",
            json!({ "SecretId": "guards/secret", "RecoveryWindowInDays": 10 }),
        ));
        assert_eq!(resp.status, 200);
        let resp = handler.handle(make_req(
            "UpdateSecret",
            json!({
                "SecretId": "guards/secret",
                "SecretString": "x",
                "ClientRequestToken": "upd-upd-upd-upd-upd-upd-upd-upd-013"
            }),
        ));
        assert_eq!(resp.status, 400);
        assert_eq!(
            body_of(&resp)["message"],
            "An error occurred (InvalidRequestException) when calling the UpdateSecret operation: You can't perform this operation on the secret because it was marked for deletion."
        );
        // Second delete without force → moto's still-marked-deleted text.
        let resp = handler.handle(make_req(
            "DeleteSecret",
            json!({ "SecretId": "guards/secret" }),
        ));
        assert_eq!(
            body_of(&resp)["message"],
            "An error occurred (InvalidRequestException) when calling the DeleteSecret operation: You tried to                     perform the operation on a secret that's currently marked deleted."
        );
        // Force delete of a scheduled secret still removes it (moto).
        let resp = handler.handle(make_req(
            "DeleteSecret",
            json!({ "SecretId": "guards/secret", "ForceDeleteWithoutRecovery": true }),
        ));
        assert_eq!(resp.status, 200, "{}", resp.body);
    }

    /// Item 14: ListSecrets pages at MaxResults with an offset NextToken and
    /// renders both version→stage maps; filters validate and match moto-style.
    #[test]
    fn test_list_secrets_pagination_and_filters() {
        let handler = SecretsManagerHandler::new();
        for i in 0..3 {
            created(&handler, &format!("lsp/page{i}"), "v");
        }
        let resp = handler.handle(make_req(
            "ListSecrets",
            json!({ "MaxResults": 2, "Filters": [ { "Key": "name", "Values": ["lsp/"] } ] }),
        ));
        let parsed = body_of(&resp);
        assert_eq!(parsed["SecretList"].as_array().unwrap().len(), 2);
        assert_eq!(parsed["NextToken"], "2");
        let entry = &parsed["SecretList"][0];
        assert!(entry["SecretVersionsToStages"].is_object());
        assert!(entry["VersionIdsToStages"].is_object());
        // moto's list entries carry no PrimaryRegion (replica-region-only).
        assert!(entry.get("PrimaryRegion").is_none());

        let resp = handler.handle(make_req(
            "ListSecrets",
            json!({ "MaxResults": 2, "NextToken": "2", "Filters": [ { "Key": "name", "Values": ["lsp/"] } ] }),
        ));
        let parsed = body_of(&resp);
        assert_eq!(parsed["SecretList"].as_array().unwrap().len(), 1);
        assert!(parsed["NextToken"].is_null());

        // Unknown filter key → moto's ValidationException with the hardcoded
        // enum list (moto quirk: omits primary-region/owning-service).
        let resp = handler.handle(make_req(
            "ListSecrets",
            json!({ "Filters": [ { "Key": "bogus", "Values": ["x"] } ] }),
        ));
        assert_eq!(resp.status, 400);
        assert_eq!(body_of(&resp)["__type"], "ValidationException");
        assert_eq!(
            body_of(&resp)["message"],
            "1 validation error detected: Value 'bogus' at 'filters.1.member.key' failed to satisfy constraint: Member must satisfy enum value set: [all, name, tag-key, description, tag-value]"
        );

        // tag-key filter matches moto-style on the tag keys.
        created(&handler, "lsp/tagged", "v");
        handler.handle(make_req(
            "TagResource",
            json!({ "SecretId": "lsp/tagged", "Tags": [ { "Key": "envtag", "Value": "x" } ] }),
        ));
        let resp = handler.handle(make_req(
            "ListSecrets",
            json!({ "Filters": [ { "Key": "tag-key", "Values": ["envtag"] } ] }),
        ));
        assert_eq!(body_of(&resp)["SecretList"].as_array().unwrap().len(), 1);
    }

    /// Item 15: BatchGetSecretValue cross-checks — invalid combos, unknown
    /// ids and deleted secrets land in `Errors` with moto's codes and texts.
    #[test]
    fn test_batch_get_secret_value_cross_checks() {
        let handler = SecretsManagerHandler::new();
        created(&handler, "bg/secret", "v");
        created(&handler, "bg/gone", "v");
        handler.handle(make_req(
            "DeleteSecret",
            json!({ "SecretId": "bg/gone", "RecoveryWindowInDays": 7 }),
        ));

        let resp = handler.handle(make_req(
            "BatchGetSecretValue",
            json!({
                "SecretIdList": ["bg/secret"],
                "Filters": [ { "Key": "name", "Values": ["bg/"] } ]
            }),
        ));
        assert_eq!(
            body_of(&resp)["message"],
            "Either 'SecretIdList' or 'Filters' must be provided, but not both."
        );
        let resp = handler.handle(make_req(
            "BatchGetSecretValue",
            json!({ "SecretIdList": ["bg/secret"], "MaxResults": 1 }),
        ));
        assert_eq!(
            body_of(&resp)["message"],
            "'Filters' not specified. 'Filters' must also be specified when 'MaxResults' is provided."
        );

        let resp = handler.handle(make_req(
            "BatchGetSecretValue",
            json!({ "SecretIdList": ["bg/secret", "bg/unknown", "bg/gone"] }),
        ));
        let parsed = body_of(&resp);
        assert_eq!(parsed["SecretValues"].as_array().unwrap().len(), 1);
        let errors = parsed["Errors"].as_array().unwrap();
        assert_eq!(errors.len(), 2);
        // Unknown id → moto's id-less ResourceNotFoundException row.
        assert_eq!(errors[0]["ErrorCode"], "ResourceNotFoundException");
        assert_eq!(
            errors[0]["Message"],
            "Secrets Manager can't find the specified secret."
        );
        // Deleted id → moto catches InvalidRequestException into the row.
        assert_eq!(errors[1]["ErrorCode"], "InvalidRequestException");
        assert!(errors[1]["Message"]
            .as_str()
            .unwrap()
            .contains("GetSecretValue operation"));
    }

    /// Item 16: ClientRequestToken idempotency + KmsKeyId storage, including
    /// the moto quirk that token-less UpdateSecret raises
    /// ResourceExistsException (verified against live moto 5.2.4).
    #[test]
    fn test_client_request_token_and_kms() {
        let handler = SecretsManagerHandler::new();
        let token = "tok-tok-tok-tok-tok-tok-tok-tok-002";
        let resp = handler.handle(make_req(
            "CreateSecret",
            json!({ "Name": "tok/secret", "SecretString": "v" }),
        ));
        let arn = body_of(&resp)["ARN"].as_str().unwrap().to_string();
        // Token-less update → moto quirk: ResourceExistsException.
        let resp = handler.handle(make_req(
            "UpdateSecret",
            json!({ "SecretId": "tok/secret", "SecretString": "v5" }),
        ));
        assert_eq!(resp.status, 400);
        assert_eq!(
            body_of(&resp)["message"],
            "A resource with the ID you requested already exists."
        );
        // Update with a fresh token echoes the token as version id.
        let resp = handler.handle(make_req(
            "UpdateSecret",
            json!({
                "SecretId": "tok/secret",
                "SecretString": "v5",
                "ClientRequestToken": token
            }),
        ));
        assert_eq!(resp.status, 200, "{}", resp.body);
        assert_eq!(body_of(&resp)["VersionId"], token);
        // Same token + same value again → idempotent echo (no new version).
        let resp = handler.handle(make_req(
            "UpdateSecret",
            json!({
                "SecretId": "tok/secret",
                "SecretString": "v5",
                "ClientRequestToken": token
            }),
        ));
        assert_eq!(body_of(&resp)["VersionId"], token);
        // Same token, different value → the moto conflict text.
        let resp = handler.handle(make_req(
            "PutSecretValue",
            json!({
                "SecretId": "tok/secret",
                "SecretString": "DIFFERENT",
                "ClientRequestToken": token
            }),
        ));
        assert_eq!(resp.status, 400);
        assert_eq!(body_of(&resp)["__type"], "ResourceExistsException");
        assert_eq!(
            body_of(&resp)["message"],
            format!("You can't use ClientRequestToken {token} because that value is already in use for a version of secret {arn}.")
        );
        // Short token → InvalidParameterException.
        let resp = handler.handle(make_req(
            "CreateSecret",
            json!({ "Name": "tok/nope", "SecretString": "v", "ClientRequestToken": "short" }),
        ));
        assert_eq!(
            body_of(&resp)["message"],
            "ClientRequestToken must be 32-64 characters long."
        );
        // KmsKeyId: stored on create, reflected in DescribeSecret; unset and
        // default-alias secrets render no KmsKeyId (moto).
        handler.handle(make_req(
            "CreateSecret",
            json!({
                "Name": "kms/secret", "SecretString": "v", "KmsKeyId": "arn:aws:kms:custom:key-1"
            }),
        ));
        let describe = handler.handle(make_req(
            "DescribeSecret",
            json!({ "SecretId": "kms/secret" }),
        ));
        assert_eq!(body_of(&describe)["KmsKeyId"], "arn:aws:kms:custom:key-1");
        let describe = handler.handle(make_req(
            "DescribeSecret",
            json!({ "SecretId": "tok/secret" }),
        ));
        assert!(body_of(&describe).get("KmsKeyId").is_none());
    }

    /// Item 17: RotateSecret without a lambda mirrors moto — the new
    /// valueless version becomes AWSCURRENT, the old value survives only
    /// under AWSPREVIOUS, and days are validated 1-1000.
    #[test]
    fn test_rotate_secret_no_lambda() {
        let handler = SecretsManagerHandler::new();
        created(&handler, "rot/secret", "v1");
        let describe_before = body_of(&handler.handle(make_req(
            "DescribeSecret",
            json!({ "SecretId": "rot/secret" }),
        )));
        assert!(describe_before.get("RotationEnabled").is_none());

        let resp = handler.handle(make_req(
            "RotateSecret",
            json!({
                "SecretId": "rot/secret",
                "RotationRules": { "AutomaticallyAfterDays": 30 }
            }),
        ));
        assert_eq!(resp.status, 200, "{}", resp.body);
        // GetSecretValue on the valueless AWSCURRENT → SecretHasNoValue.
        let resp = handler.handle(make_req(
            "GetSecretValue",
            json!({ "SecretId": "rot/secret" }),
        ));
        assert_eq!(resp.status, 404);
        assert_eq!(
            body_of(&resp)["message"],
            "Secrets Manager can't find the specified secret value for staging label: AWSCURRENT"
        );
        // The old value survives under AWSPREVIOUS only.
        let resp = handler.handle(make_req(
            "ListSecretVersionIds",
            json!({ "SecretId": "rot/secret" }),
        ));
        let parsed_ids = body_of(&resp);
        let versions = parsed_ids["Versions"].as_array().unwrap();
        assert_eq!(versions.len(), 2);
        let previous = versions
            .iter()
            .find(|v| {
                v["VersionStages"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("AWSPREVIOUS"))
            })
            .unwrap();
        let resp = handler.handle(make_req(
            "GetSecretValue",
            json!({ "SecretId": "rot/secret", "VersionId": previous["VersionId"] }),
        ));
        assert_eq!(body_of(&resp)["SecretString"], "v1");

        let describe = body_of(&handler.handle(make_req(
            "DescribeSecret",
            json!({ "SecretId": "rot/secret" }),
        )));
        assert_eq!(describe["RotationEnabled"], true);
        assert_eq!(describe["RotationLambdaARN"], "");
        assert_eq!(describe["RotationRules"]["AutomaticallyAfterDays"], 30);
        assert!(describe.get("NextRotationDate").is_some());

        // Days validation: 0 and 1001 raise moto's InvalidParameterException.
        for days in [0, 1001] {
            let resp = handler.handle(make_req(
                "RotateSecret",
                json!({
                    "SecretId": "rot/secret",
                    "RotationRules": { "AutomaticallyAfterDays": days }
                }),
            ));
            assert_eq!(resp.status, 400);
            assert_eq!(
                body_of(&resp)["message"],
                "RotationRules.AutomaticallyAfterDays must be within 1-1000."
            );
        }
    }

    /// Item 18: ListSecretVersionIds is deterministic and keeps the
    /// true-ARN rendering (documented divergence from moto's name-ARN quirk).
    #[test]
    fn test_list_secret_version_ids_ordering() {
        let handler = SecretsManagerHandler::new();
        let resp = handler.handle(make_req(
            "CreateSecret",
            json!({ "Name": "lsi/secret", "SecretString": "v1" }),
        ));
        let arn = body_of(&resp)["ARN"].as_str().unwrap().to_string();
        handler.handle(make_req(
            "PutSecretValue",
            json!({ "SecretId": "lsi/secret", "SecretString": "v2" }),
        ));
        let resp = handler.handle(make_req(
            "ListSecretVersionIds",
            json!({ "SecretId": "lsi/secret" }),
        ));
        let parsed = body_of(&resp);
        assert_eq!(parsed["ARN"], arn);
        let versions = parsed["Versions"].as_array().unwrap();
        assert_eq!(versions.len(), 2);
        let stages: Vec<Vec<String>> = versions
            .iter()
            .map(|v| {
                v["VersionStages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|s| s.as_str().unwrap().to_string())
                    .collect()
            })
            .collect();
        // Deterministic (created‑date) order: the older version first.
        assert_eq!(stages[0], vec!["AWSPREVIOUS".to_string()]);
        assert_eq!(stages[1], vec!["AWSCURRENT".to_string()]);
    }
    /// The rotation stage lifecycle (regression for
    /// `tests/apps/secrets_rotation::test_previous_version_accessible`):
    /// after every rotation shape the ORIGINAL password must sit at
    /// AWSPREVIOUS and the regenerated password at AWSCURRENT, and the
    /// stage-only reads must return THAT version's value — moto resolves
    /// the version id from the stage itself (models.py:453-461, `set
    /// version_id to match version_stage`), not from AWSCURRENT.
    ///
    /// The steps mirror the botocore four-step rotation lifecycle the app
    /// drives: (1) app-style UpdateSecret rotation (current swaps, the old
    /// drops to exactly AWSPREVIOUS), (2) RotateSecret without a lambda —
    /// the valueless pending becomes AWSCURRENT (#417: the value lives
    /// only in AWSPREVIOUS), (3) the lambda-style PutSecretValue writes a
    /// real value onto the pending token with VersionStages=AWSPENDING,
    /// (4) finalizeSecret relabels AWSCURRENT onto the finished token.
    #[test]
    fn test_rotation_stage_ordering_after_each_rotation_shape() {
        let handler = SecretsManagerHandler::new();
        let old = json!({"password": "s3cur3-pr0d-p@ssw0rd!"});
        let rotated = json!({"password": "a7!qdk&Olb$E!!$!3q4VJ$1e"});
        let old_str = old.to_string();
        let rotated_str = rotated.to_string();

        let resp = handler.handle(make_req(
            "CreateSecret",
            json!({"Name": "w9/db", "SecretString": old_str}),
        ));
        assert_eq!(resp.status, 200, "create body: {}", resp.body);
        let v1_id = serde_json::from_str::<serde_json::Value>(&resp.body).unwrap()["VersionId"]
            .as_str()
            .unwrap()
            .to_string();

        // (1) The app-style rotation: UpdateSecret with a new value.
        // boto3 auto-injects a ClientRequestToken here (the shape marks the
        // member idempotencyToken) — the request mirrors one.
        let update_token = uuid::Uuid::new_v4().to_string();
        let resp = handler.handle(make_req(
            "UpdateSecret",
            json!({"SecretId": "w9/db", "SecretString": rotated_str,
                   "ClientRequestToken": update_token}),
        ));
        assert_eq!(resp.status, 200, "update body: {}", resp.body);
        let new_id = update_token.clone();

        // The stage reads resolve FROM the stage (the regression core —
        // the old path defaulted AWSCURRENT and returned the NEW value
        // under AWSPREVIOUS).
        let prev = handler.handle(make_req(
            "GetSecretValue",
            json!({"SecretId": "w9/db", "VersionStage": "AWSPREVIOUS"}),
        ));
        assert_eq!(prev.status, 200, "AWSPREVIOUS body: {}", prev.body);
        let resp_v: serde_json::Value = serde_json::from_str(&prev.body).unwrap();
        assert_eq!(
            resp_v["SecretString"].as_str().unwrap(),
            old_str,
            "AWSPREVIOUS must carry the ORIGINAL value: {}",
            prev.body
        );
        let curr = handler.handle(make_req(
            "GetSecretValue",
            json!({"SecretId": "w9/db", "VersionStage": "AWSCURRENT"}),
        ));
        let resp_v: serde_json::Value = serde_json::from_str(&curr.body).unwrap();
        assert_eq!(
            resp_v["SecretString"].as_str().unwrap(),
            rotated_str,
            "body: {}",
            curr.body
        );

        // (2) RotateSecret without a lambda: moto's models.py:966-973
        // branch runs UNCONDITIONALLY of RotateImmediately — the valueless
        // pending becomes AWSCURRENT (the #417 rule; GetSecretValue by
        // AWSCURRENT cannot render a value), the previous current drops to
        // exactly ["AWSPREVIOUS"], and the rotate leaves NO AWSPENDING
        // attached anywhere (models.py:876-878's second legal end state).
        // The pre-rotation original stays readable by its version id.
        let resp = handler.handle(make_req("RotateSecret", json!({"SecretId": "w9/db"})));
        assert_eq!(resp.status, 200, "body: {}", resp.body);
        let current_new = serde_json::from_str::<serde_json::Value>(&resp.body).unwrap()
            ["VersionId"]
            .as_str()
            .unwrap()
            .to_string();
        let curr = handler.handle(make_req(
            "GetSecretValue",
            json!({"SecretId": "w9/db", "VersionStage": "AWSCURRENT"}),
        ));
        assert_eq!(
            curr.status, 404,
            "the flatten leaves AWSCURRENT valueless: {}",
            curr.body
        );
        let prev = handler.handle(make_req(
            "GetSecretValue",
            json!({"SecretId": "w9/db", "VersionStage": "AWSPREVIOUS"}),
        ));
        let prev_v: serde_json::Value = serde_json::from_str(&prev.body).unwrap();
        assert_eq!(
            prev_v["SecretString"].as_str().unwrap(),
            rotated_str,
            "the previous current drops to AWSPREVIOUS: {}",
            prev.body
        );
        let by_vid_old = handler.handle(make_req(
            "GetSecretValue",
            json!({"SecretId": "w9/db", "VersionId": v1_id}),
        ));
        let by_old: serde_json::Value = serde_json::from_str(&by_vid_old.body).unwrap();
        assert_eq!(
            by_old["SecretString"].as_str().unwrap(),
            old_str,
            "the ORIGINAL is still readable by version id: {}",
            by_vid_old.body
        );
        // The stage map pins: AWSCURRENT on the new token, AWSPREVIOUS on
        // the dropped version, and no AWSPENDING anywhere (the flatten
        // satisfies moto's second legal rotation end state).
        let describe: serde_json::Value = serde_json::from_str(
            &handler
                .handle(make_req("DescribeSecret", json!({"SecretId": "w9/db"})))
                .body,
        )
        .unwrap();
        let stage_map = &describe["VersionIdsToStages"];
        for (vid, stages) in stage_map.as_object().unwrap() {
            assert!(
                !stages.as_array().unwrap().contains(&json!("AWSPENDING")),
                "AWSPENDING must not survive a no-lambda rotation ({vid}): {describe}",
                describe = describe
            );
        }
        assert!(
            stage_map[&current_new]
                .as_array()
                .unwrap()
                .contains(&json!("AWSCURRENT")),
            "the new token is AWSCURRENT: {describe}",
            describe = describe
        );
    }
}
