//! Secrets Manager operation handler.

use crate::models::{Secret, SecretVersion, SmState};
use crate::protocol::{AwsRequest, AwsResponse};
use chrono::Utc;
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::sync::Arc;

pub struct SecretsManagerHandler {
    state: RwLock<std::collections::HashMap<(u64, String), SmState>>,
}

impl SecretsManagerHandler {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(std::collections::HashMap::new()),
        }
    }

    fn get_state(&self, account: u64, region: &str) -> SmState {
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
        AwsResponse::error(
            400,
            "ResourceNotFoundException",
            &format!("Secrets Manager can't find the specified secret: {}", id),
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
        if let Some(existing) = state.get_secret(&name) {
            // A secret scheduled for deletion blocks creation too (moto parity).
            let _ = existing;
            return AwsResponse::error(
                409,
                "ResourceExistsException",
                "The operation failed because the secret already exists.",
            );
        }

        let secret = Arc::new(Secret::new(name.clone(), req.account, req.region.clone()));

        let version_id = Self::new_version_id();
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

        let version = SecretVersion {
            version_id: version_id.clone(),
            secret_string,
            secret_binary,
            stages: vec!["AWSCURRENT".to_string()],
            created_date: Utc::now().timestamp() as f64,
        };
        secret.versions.write().insert(version_id.clone(), version);

        if let Some(desc) = req.params.get("Description").and_then(|v| v.as_str()) {
            *secret.description.write() = Some(desc.to_string());
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

        AwsResponse::json(
            200,
            json!({
                "ARN": secret.arn,
                "Name": secret.name,
                "VersionId": version_id,
                "CreatedDate": secret.created_date,
            }),
        )
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

        if *secret.removed.read() {
            return AwsResponse::error(
                400,
                "InvalidRequestException",
                "You tried to perform the operation on a secret that's currently marked deleted.",
            );
        }

        // VersionId wins over VersionStage when both provided.
        let version = if let Some(vid) = req.params.get("VersionId").and_then(|v| v.as_str()) {
            secret.get_version_by_id(vid)
        } else {
            let stage = req
                .params
                .get("VersionStage")
                .and_then(|v| v.as_str())
                .unwrap_or("AWSCURRENT");
            secret.get_version(stage)
        };
        let version = match version {
            Some(v) => v,
            None => {
                return AwsResponse::error(
                    400,
                    "InvalidRequestException",
                    "The requested secret has no versions with the specified stage",
                )
            }
        };

        let mut resp = json!({
            "ARN": secret.arn,
            "Name": secret.name,
            "VersionId": version.version_id,
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
        let secret = match Self::resolve_secret(
            &state,
            req.params
                .get("SecretId")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
        ) {
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
        Self::promote_new_version(req, &secret, "create")
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

        // UpdateSecret with a new value creates a new current version (moto
        // reuses the existing version when no value is provided).
        let has_value =
            req.params.get("SecretString").is_some() || req.params.get("SecretBinary").is_some();
        if has_value {
            let resp = Self::promote_new_version(req, &secret, "create");
            let _ = resp;
        }

        if let Some(desc) = req.params.get("Description").and_then(|v| v.as_str()) {
            *secret.description.write() = Some(desc.to_string());
        }

        let current = secret.get_version("AWSCURRENT").map(|v| v.version_id);
        AwsResponse::json(
            200,
            json!({
                "ARN": secret.arn,
                "Name": secret.name,
                "VersionId": current.unwrap_or_default(),
            }),
        )
    }

    /// Create a new version for PutSecretValue / UpdateSecret, rotating the
    /// AWSCURRENT/AWSPREVIOUS stage labels like the Python twin does.
    fn promote_new_version(req: &AwsRequest, secret: &Arc<Secret>, _mode: &str) -> AwsResponse {
        let version_id = Self::new_version_id();
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
        let created_date = Utc::now().timestamp() as f64;

        let mut versions = secret.versions.write();
        for v in versions.values_mut() {
            if v.stages.contains(&"AWSCURRENT".to_string()) {
                v.stages.retain(|s| s != "AWSCURRENT" && s != "AWSPREVIOUS");
                v.stages.push("AWSPREVIOUS".to_string());
            } else {
                v.stages.retain(|s| s != "AWSPREVIOUS");
            }
        }

        let stages = req
            .params
            .get("VersionStages")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|s| s.as_str().unwrap_or("").to_string())
                    .collect()
            })
            .unwrap_or_else(|| vec!["AWSCURRENT".to_string()]);

        let version = SecretVersion {
            version_id: version_id.clone(),
            secret_string,
            secret_binary,
            stages,
            created_date,
        };
        versions.insert(version_id.clone(), version);

        AwsResponse::json(
            200,
            json!({
                "ARN": secret.arn,
                "Name": secret.name,
                "VersionId": version_id,
                "CreatedDate": created_date,
            }),
        )
    }

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

        if let Some(window) = recovery_window {
            if (window < 7.0 || window > 30.0) && !force {
                return AwsResponse::error(
                    400,
                    "InvalidParameterException",
                    "RecoveryWindowInDays value must be between 7 and 30 days (inclusive)",
                );
            }
        }

        // Force-delete without recovery removes the secret entirely.
        if let Some(secret) = state.get_secret(secret_id).or_else(|| {
            // resolve also via ARN
            if force {
                None
            } else {
                Self::resolve_secret(&state, secret_id)
            }
        }) {
            if *secret.removed.read() && !force {
                return AwsResponse::error(
                    400,
                    "InvalidRequestException",
                    "You tried to perform the operation on a secret that's currently marked deleted.",
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
        // Deleting an unknown id with force still reports success (moto parity).
        AwsResponse::json(
            200,
            json!({
                "ARN": format!(
                    "arn:aws:secretsmanager:{}:{}:secret:{}",
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

        let mut resp = json!({
            "ARN": secret.arn,
            "Name": secret.name,
            "Type": "AWS_SECRETS_MANAGER",
            "CreatedDate": secret.created_date,
            "LastChangedDate": secret.created_date,
            "Tags": *secret.tags.read(),
            "RotationEnabled": *secret.rotation_enabled.read(),
            "VersionIdsToStages": Value::Object(version_stages),
        });
        let resp_obj = resp.as_object_mut().unwrap();
        if let Some(ref desc) = *secret.description.read() {
            resp_obj.insert("Description".into(), json!(desc));
        }
        let removed = *secret.removed.read();
        if removed {
            if let Some(date) = *secret.deletion_date.read() {
                resp_obj.insert("DeletedDate".into(), json!(date));
            }
        }
        if let Some(arn) = &*secret.rotation_lambda_arn.read() {
            resp_obj.insert("RotationLambdaARN".into(), json!(arn));
        }
        if let Some(rules) = &*secret.rotation_rules.read() {
            resp_obj.insert("RotationRules".into(), rules.clone());
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

    fn list_secrets(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let filters = req
            .params
            .get("Filters")
            .and_then(|v| v.as_array())
            .cloned();

        let secrets = state.list_secrets();
        let secret_list: Vec<Value> = secrets
            .into_iter()
            .filter(|s| filtered_in(s, filters.as_deref()))
            .map(|s| {
                let mut obj = json!({
                    "ARN": s.arn,
                    "Name": s.name,
                    "CreatedDate": s.created_date,
                    "LastChangedDate": s.created_date,
                    "RotationEnabled": *s.rotation_enabled.read(),
                    "Tags": *s.tags.read(),
                });
                let obj_map = obj.as_object_mut().unwrap();
                if let Some(ref desc) = *s.description.read() {
                    obj_map.insert("Description".into(), json!(desc));
                }
                let replicas = s.replicas.read().clone();
                if !replicas.is_empty() {
                    obj_map.insert("PrimaryRegion".into(), json!(s.region));
                    obj_map.insert(
                        "ReplicationStatus".into(),
                        json!(Self::replica_statuses(&replicas)),
                    );
                }
                obj
            })
            .collect();

        AwsResponse::json(200, json!({ "SecretList": secret_list }))
    }

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
        let version_list: Vec<Value> = versions
            .values()
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
            required.push(Self::pick_from(
                "abcdefghijklmnopqrstuvwxyz",
                exclude_chars,
                &mut rng,
            ));
            required.push(Self::pick_from(
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
                exclude_chars,
                &mut rng,
            ));
        } else if !exclude_lowercase {
            alphabet.push_str("abcdefghijklmnopqrstuvwxyz");
            required.push(Self::pick_from(
                "abcdefghijklmnopqrstuvwxyz",
                exclude_chars,
                &mut rng,
            ));
        } else if !exclude_uppercase {
            alphabet.push_str("ABCDEFGHIJKLMNOPQRSTUVWXYZ");
            required.push(Self::pick_from(
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
                exclude_chars,
                &mut rng,
            ));
        }
        if !exclude_numbers {
            alphabet.push_str("0123456789");
            required.push(Self::pick_from("0123456789", exclude_chars, &mut rng));
        }
        if !exclude_punctuation {
            alphabet.push_str("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~");
            required.push(Self::pick_from(
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

    fn pick_from(alphabet: &str, exclude: &str, rng: &mut u64) -> char {
        let pool: Vec<char> = alphabet.chars().filter(|c| !exclude.contains(*c)).collect();
        let idx = (rand_next(rng) % pool.len() as u64) as usize;
        pool[idx]
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
            return AwsResponse::error(
                400,
                "InvalidRequestException",
                "You tried to perform the operation on a secret that's currently marked deleted.",
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
        if !lambda_arn.is_empty() {
            *secret.rotation_lambda_arn.write() = Some(lambda_arn.to_string());
        }
        let automatically_after = rules
            .get("AutomaticallyAfterDays")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if !lambda_arn.is_empty() && automatically_after > 0 {
            *secret.rotation_rules.write() = Some(rules.clone());
        }
        if !lambda_arn.is_empty() {
            *secret.rotation_enabled.write() = true;
        }

        // New AWSPENDING version carrying the current value.
        let current = secret.get_version("AWSCURRENT");
        let pending_id = Self::new_version_id();
        let mut versions = secret.versions.write();
        for v in versions.values_mut() {
            v.stages.retain(|s| s != "AWSPENDING");
        }
        let version = SecretVersion {
            version_id: pending_id.clone(),
            secret_string: current.as_ref().and_then(|c| c.secret_string.clone()),
            secret_binary: current.as_ref().and_then(|c| c.secret_binary.clone()),
            stages: vec!["AWSPENDING".to_string()],
            created_date: Utc::now().timestamp() as f64,
        };
        versions.insert(pending_id.clone(), version);

        AwsResponse::json(
            200,
            json!({
                "ARN": secret.arn,
                "Name": secret.name,
                "VersionId": pending_id,
            }),
        )
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
        if secret.rotation_lambda_arn.read().is_none() {
            return AwsResponse::error(
                400,
                "InvalidRequestException",
                "You tried to enable rotation on a secret that doesn't already have a Lambda function ARN configured and you didn't include such an ARN as a parameter in this call.",
            );
        }
        *secret.rotation_enabled.write() = false;
        AwsResponse::json(
            200,
            json!({
                "ARN": secret.arn,
                "Name": secret.name,
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
                    .collect()
            })
            .unwrap_or_default();

        let mut values: Vec<Value> = Vec::new();
        let mut errors: Vec<Value> = Vec::new();
        for id in &ids {
            let secret = Self::resolve_secret(&state, id);
            let secret = match secret {
                Some(s) if !*s.removed.read() => Some(s),
                Some(_) => None,
                None => None,
            };
            if let Some(secret) = secret {
                if let Some(version) = secret.get_version("AWSCURRENT") {
                    let mut obj = json!({
                        "ARN": secret.arn,
                        "Name": secret.name,
                        "VersionId": version.version_id,
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
                    values.push(obj);
                    continue;
                }
            }
            errors.push(json!({
                "SecretId": id,
                "ErrorCode": "ResourceNotFoundException",
                "Message": format!("Secrets Manager can't find the specified secret: {}", id),
            }));
        }

        let mut resp = json!({ "SecretValues": values });
        if !errors.is_empty() {
            resp.as_object_mut()
                .unwrap()
                .insert("Errors".into(), json!(errors));
        }
        AwsResponse::json(200, resp)
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

    fn new_version_id() -> String {
        uuid::Uuid::new_v4().simple().to_string()
    }
}

/// Name-based list filter with no filters = include everything.
fn filtered_in(secret: &Arc<Secret>, filters: Option<&[Value]>) -> bool {
    let filters = match filters {
        Some(f) => f,
        None => return true,
    };
    for f in filters {
        let key = f.get("Key").and_then(|k| k.as_str()).unwrap_or("");
        let values: Vec<&str> = f
            .get("Values")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|s| s.as_str()).collect())
            .unwrap_or_default();
        let is_match = match key {
            "name" => values.iter().any(|p| secret.name.starts_with(p)),
            "description" => {
                let desc = secret.description.read().clone().unwrap_or_default();
                values
                    .iter()
                    .any(|p| desc.to_lowercase().contains(&p.to_lowercase()))
            }
            "tag-key" => {
                let tags = secret.tags.read();
                values.iter().any(|p| {
                    tags.iter()
                        .any(|t| t.get("Key").and_then(|k| k.as_str()) == Some(*p))
                })
            }
            "tag-value" => {
                let tags = secret.tags.read();
                values.iter().any(|p| {
                    tags.iter()
                        .any(|t| t.get("Value").and_then(|k| k.as_str()) == Some(*p))
                })
            }
            "all" => {
                let desc = (*secret.description.read()).clone().unwrap_or_default();
                let tags = secret.tags.read().clone();
                let mut all: Vec<String> = vec![secret.name.clone(), desc];
                for t in &tags {
                    all.push(
                        t.get("Key")
                            .and_then(|k| k.as_str())
                            .unwrap_or("")
                            .to_string(),
                    );
                    all.push(
                        t.get("Value")
                            .and_then(|k| k.as_str())
                            .unwrap_or("")
                            .to_string(),
                    );
                }
                values.iter().any(|p| {
                    all.iter()
                        .any(|s| s.to_lowercase().contains(&p.to_lowercase()))
                })
            }
            "primary-region" => values.contains(&secret.region.as_str()),
            _ => true,
        };
        if !is_match {
            return false;
        }
    }
    true
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
}
