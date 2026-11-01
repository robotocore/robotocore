//! KMS operation handler.
//!
//! JSON protocol (x-amz-json-1.1). Response shapes mirror the Moto ground truth.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use sha2::{Sha224, Sha256, Sha384, Sha512};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use crate::cipher::{self, context_entries};
use crate::models::{CustomKeyStore, Grant, Key, KmsState};
use crate::protocol::{AwsRequest, AwsResponse};

pub struct KmsHandler {
    pub(crate) state: RwLock<HashMap<(u64, String), KmsState>>,
}

/// Default key policy (mirrors the Moto default policy shape closely enough
/// for consumers that parse it as JSON).
fn default_policy(arn: &str, account: u64) -> Value {
    json!({
        "Version": "2012-10-17",
        "Id": "key-default-1",
        "Statement": [
            {
                "Sid": "Enable IAM User Permissions",
                "Effect": "Allow",
                "Principal": {"AWS": format!("arn:aws:iam::{}:root", account)},
                "Action": "kms:*",
                "Resource": arn,
            }
        ]
    })
}

/// Ephemeral RSA-2048 keypair used as the process-wide import-wrapping key.
fn wrapping_keypair() -> &'static rsa::RsaPrivateKey {
    static WRAP: OnceLock<rsa::RsaPrivateKey> = OnceLock::new();
    WRAP.get_or_init(|| {
        let mut rng = rand::thread_rng();
        rsa::RsaPrivateKey::new(&mut rng, 2048).expect("RSA keygen failed")
    })
}

/// Epoch-seconds timestamp for `now`, as KMS timestamps are serialized.
fn now_epoch_f64() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

impl KmsHandler {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn get_state(&self, account: u64, region: &str) -> KmsState {
        let mut states = self.state.write();
        states
            .entry((account, region.to_string()))
            .or_default()
            .clone()
    }

    fn not_found() -> AwsResponse {
        AwsResponse::error(
            400,
            "NotFoundException",
            "The operation references a key that does not exist",
        )
    }

    fn custom_ks_not_found() -> AwsResponse {
        AwsResponse::error(
            400,
            "CustomKeyStoreNotFoundException",
            "The custom key store does not exist",
        )
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        let op = req.operation.as_str();
        match op {
            "CreateKey" => self.create_key(&req),
            "DescribeKey" => self.describe_key(&req),
            "ListKeys" => self.list_keys(&req),
            "ScheduleKeyDeletion" => self.schedule_key_deletion(&req),
            "CancelKeyDeletion" => self.cancel_key_deletion(&req),
            "EnableKey" => self.set_key_state(&req, "Enabled", true),
            "DisableKey" => self.set_key_state(&req, "Disabled", false),
            "UpdateKeyDescription" => self.update_key_description(&req),
            "EnableKeyRotation" => self.set_key_rotation(&req, true),
            "DisableKeyRotation" => self.set_key_rotation(&req, false),
            "GetKeyRotationStatus" => self.get_key_rotation_status(&req),
            "ListKeyRotations" => AwsResponse::json(200, json!({"Rotations": []})),
            "RotateKeyOnDemand" => self.rotate_key_on_demand(&req),
            "ReplicateKey" => self.replicate_key(&req),
            "UpdatePrimaryRegion" => AwsResponse::json(200, json!({})),
            "CreateAlias" => self.create_alias(&req),
            "DeleteAlias" => self.delete_alias(&req),
            "UpdateAlias" => self.update_alias(&req),
            "ListAliases" => self.list_aliases(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListResourceTags" => self.list_resource_tags(&req),
            "Encrypt" => self.encrypt(&req),
            "Decrypt" => self.decrypt(&req),
            "ReEncrypt" => self.re_encrypt(&req),
            "GenerateDataKey" => self.generate_data_key(&req, true),
            "GenerateDataKeyWithoutPlaintext" => self.generate_data_key(&req, false),
            "GenerateDataKeyPair" => self.generate_data_key_pair(&req, true),
            "GenerateDataKeyPairWithoutPlaintext" => self.generate_data_key_pair(&req, false),
            "GenerateRandom" => self.generate_random(&req),
            "GenerateMac" => self.generate_mac(&req),
            "VerifyMac" => self.verify_mac(&req),
            "Sign" => self.sign(&req),
            "Verify" => self.verify(&req),
            "GetPublicKey" => self.get_public_key(&req),
            "DeriveSharedSecret" => self.derive_shared_secret(&req),
            "CreateGrant" => self.create_grant(&req),
            "ListGrants" => self.list_grants(&req),
            "RetireGrant" => self.retire_grant(&req),
            "RevokeGrant" => self.revoke_grant(&req),
            "ListRetirableGrants" => self.list_retirable_grants(&req),
            "PutKeyPolicy" => self.put_key_policy(&req),
            "GetKeyPolicy" => self.get_key_policy(&req),
            "ListKeyPolicies" => AwsResponse::json(
                200,
                json!({"PolicyNames": ["default"], "NextMarker": null, "Truncated": false}),
            ),
            "CreateCustomKeyStore" => self.create_custom_key_store(&req),
            "DescribeCustomKeyStores" => self.describe_custom_key_stores(&req),
            "ConnectCustomKeyStore" => self.set_custom_key_store_state(&req, "CONNECTED"),
            "DisconnectCustomKeyStore" => self.set_custom_key_store_state(&req, "DISCONNECTED"),
            "UpdateCustomKeyStore" => self.update_custom_key_store(&req),
            "DeleteCustomKeyStore" => self.delete_custom_key_store(&req),
            "GetParametersForImport" => self.get_parameters_for_import(&req),
            "ImportKeyMaterial" => self.import_key_material(&req),
            "DeleteImportedKeyMaterial" => self.delete_imported_key_material(&req),
            other => AwsResponse::error(
                400,
                "InvalidException",
                &format!("The operation {} is not implemented", other),
            ),
        }
    }

    // ---------- key lifecycle ----------

    fn create_key(&self, req: &AwsRequest) -> AwsResponse {
        let key_usage = pstr(req, "KeyUsage").unwrap_or_else(|| "ENCRYPT_DECRYPT".into());
        let key_spec = pstr(req, "KeySpec").unwrap_or_else(|| "SYMMETRIC_DEFAULT".into());
        let origin = pstr(req, "Origin").unwrap_or_else(|| "AWS_KMS".into());
        let multi_region = req
            .params
            .get("MultiRegion")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let key = Arc::new(if multi_region {
            Key::new_multi_region(req.account, req.region.clone(), &key_usage, &key_spec)
        } else {
            Key::new(req.account, req.region.clone(), &key_usage, &key_spec)
        });
        *key.origin.write() = origin;
        *key.multi_region.write() = multi_region;
        if let Some(desc) = pstr(req, "Description") {
            *key.description.write() = desc;
        }
        key.generate_key_material();
        let state = self.get_state(req.account, &req.region);
        state.put_key(key.clone());

        if let Some(tags) = req.params.get("Tags").cloned() {
            if let Some(arr) = tags.as_array() {
                // Tags store is keyed by key id (shared with TagResource).
                let mut all_tags = state.tags.write();
                let entry = all_tags.entry(key.key_id.clone()).or_default();
                for tag in arr {
                    let k = tag
                        .get("TagKey")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();
                    let v = tag
                        .get("TagValue")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();
                    entry.insert(k.to_string(), json!(v));
                }
            }
        }

        let metadata = self.key_metadata(&key, req);
        AwsResponse::json(200, json!({ "KeyMetadata": metadata }))
    }

    /// Standard KeyMetadata body for a key.
    fn key_metadata(&self, key: &Key, req: &AwsRequest) -> Value {
        let _ = req;
        let (enc, sign, mac) = key.algorithms();
        let creation = key.created_ms as f64 / 1000.0;
        let mut meta = Map::new();
        meta.insert("AWSAccountId".into(), json!(key.owner.to_string()));
        meta.insert("KeyId".into(), json!(key.key_id));
        meta.insert("Arn".into(), json!(key.arn));
        meta.insert("CreationDate".into(), json!(creation));
        meta.insert("Enabled".into(), json!(*key.enabled.read()));
        meta.insert("Description".into(), json!(key.description.read().clone()));
        meta.insert("KeyUsage".into(), json!(key.key_usage));
        meta.insert("KeyState".into(), json!(key.key_state.read().clone()));
        meta.insert("KeyManager".into(), json!("CUSTOMER"));
        meta.insert("Origin".into(), json!(*key.origin.read()));
        meta.insert("KeySpec".into(), json!(key.key_spec.clone()));
        meta.insert("CustomerMasterKeySpec".into(), json!(key.key_spec.clone()));
        if !enc.is_empty() {
            meta.insert(
                "EncryptionAlgorithms".into(),
                Value::Array(enc.iter().map(|s| json!(s)).collect()),
            );
        }
        if !sign.is_empty() {
            meta.insert(
                "SigningAlgorithms".into(),
                Value::Array(sign.iter().map(|s| json!(s)).collect()),
            );
        }
        if !mac.is_empty() {
            meta.insert(
                "MacAlgorithms".into(),
                Value::Array(mac.iter().map(|s| json!(s)).collect()),
            );
        }
        meta.insert("MultiRegion".into(), json!(*key.multi_region.read()));
        if *key.multi_region.read() {
            meta.insert(
                "MultiRegionConfiguration".into(),
                json!({
                    "MultiRegionKeyType": "PRIMARY",
                    "PrimaryKey": {"Arn": key.arn, "Region": key.region},
                    "ReplicaKeys": [],
                }),
            );
        }
        meta.insert(
            "KeyRotationEnabled".into(),
            json!(*key.rotation_enabled.read()),
        );
        meta.insert("RotationPeriodInDays".into(), json!(365));
        if let Some(ts) = *key.last_rotation_ms.read() {
            meta.insert(
                "OnDemandRotationStartDate".into(),
                json!(ts as f64 / 1000.0),
            );
        }
        if key.key_state.read().as_str() == "PendingDeletion" {
            let (deletion, window) = (*key.deletion_ms.read(), *key.pending_window_days.read());
            // moto Describes from the stored schedule (models.py:294-295).
            meta.insert(
                "DeletionDate".into(),
                json!(deletion
                    .map(|ms| ms as f64 / 1000.0)
                    .unwrap_or_else(|| now_epoch_f64() + 7.0 * 24.0 * 3600.0)),
            );
            // botocore's KeyMetadata member is PendingDeletionWindowInDays;
            // the ScheduleKeyDeletion RESPONSE keeps its modeled
            // PendingWindowInDays extra (the schedule arm).
            meta.insert(
                "PendingDeletionWindowInDays".into(),
                json!(window.unwrap_or(30)),
            );
        }
        if *key.origin.read() == "EXTERNAL" && *key.has_imported_material.read() {
            meta.insert(
                "ExpirationModel".into(),
                json!("KEY_MATERIAL_DOES_NOT_EXPIRE"),
            );
        }
        Value::Object(meta)
    }

    fn describe_key(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        match state.get_key(&key_id) {
            Some(key) => {
                AwsResponse::json(200, json!({ "KeyMetadata": self.key_metadata(&key, req) }))
            }
            None => Self::not_found(),
        }
    }

    fn list_keys(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        // botocore's KeyListEntry members are KeyId + KeyArn (motorily
        // responses.py:200-206) — an `Arn` member is a plain KeyError for
        // every ARN read.
        let key_list: Vec<Value> = state
            .list_keys()
            .iter()
            .map(|k| json!({"KeyId": k.key_id, "KeyArn": k.arn}))
            .collect();
        AwsResponse::json(
            200,
            json!({ "Keys": key_list, "NextMarker": null, "Truncated": false }),
        )
    }

    fn set_key_state(&self, req: &AwsRequest, state_name: &str, enabled: bool) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let kms = self.get_state(req.account, &req.region);
        match kms.get_key(&key_id) {
            Some(key) => {
                *key.key_state.write() = state_name.to_string();
                *key.enabled.write() = enabled;
                AwsResponse::json(200, json!({}))
            }
            None => Self::not_found(),
        }
    }

    fn schedule_key_deletion(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let window_days = req
            .params
            .get("PendingWindowInDays")
            .and_then(|v| v.as_i64());
        // moto (models.py:580-587): only 7..=30 schedules a deletion — any
        // other window is a no-op that answers DeletionDate: null with the
        // key still Enabled. (Real AWS ValExes; moto 200s, so the 200 is
        // the pinned shape.)
        let window_days = match window_days {
            None => 30u64,
            Some(d) if (7..=30).contains(&d) => d as u64,
            Some(_) => {
                let state = self.get_state(req.account, &req.region);
                return match state.get_key(&key_id) {
                    Some(key) => AwsResponse::json(
                        200,
                        json!({"KeyId": key.key_id, "KeyState": "Enabled", "DeletionDate": Value::Null}),
                    ),
                    None => Self::not_found(),
                };
            }
        };
        let state = self.get_state(req.account, &req.region);
        match state.get_key(&key_id) {
            Some(key) => {
                let deletion_ms =
                    chrono::Utc::now().timestamp_millis() as u64 + window_days * 24 * 3600 * 1000;
                *key.key_state.write() = "PendingDeletion".to_string();
                *key.enabled.write() = false;
                // moto persists the schedule (models.py:580-587) and Describes
                // it from the store — not a per-call recompute.
                *key.deletion_ms.write() = Some(deletion_ms);
                *key.pending_window_days.write() = Some(window_days);
                AwsResponse::json(
                    200,
                    json!({
                        "KeyId": key.key_id,
                        "KeyState": "PendingDeletion",
                        "DeletionDate": deletion_ms as f64 / 1000.0,
                        "PendingWindowInDays": window_days,
                    }),
                )
            }
            None => Self::not_found(),
        }
    }

    fn cancel_key_deletion(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        match state.get_key(&key_id) {
            Some(key) => {
                *key.key_state.write() = "Disabled".to_string();
                *key.enabled.write() = false;
                // moto's cancel clears the recorded schedule.
                *key.deletion_ms.write() = None;
                *key.pending_window_days.write() = None;
                AwsResponse::json(200, json!({"KeyId": key.key_id, "KeyState": "Disabled"}))
            }
            None => Self::not_found(),
        }
    }

    fn update_key_description(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let desc = pstr(req, "Description").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        match state.get_key(&key_id) {
            Some(key) => {
                *key.description.write() = desc;
                AwsResponse::json(200, json!({}))
            }
            None => Self::not_found(),
        }
    }

    fn set_key_rotation(&self, req: &AwsRequest, enabled: bool) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        match state.get_key(&key_id) {
            Some(key) => {
                if !(key.key_spec == "SYMMETRIC_DEFAULT" && key.key_usage == "ENCRYPT_DECRYPT") {
                    return AwsResponse::error(
                        400,
                        "UnsupportedOperationException",
                        "Key rotation is only supported for symmetric keys",
                    );
                }
                *key.rotation_enabled.write() = enabled;
                AwsResponse::json(200, json!({}))
            }
            None => Self::not_found(),
        }
    }

    fn get_key_rotation_status(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        match state.get_key(&key_id) {
            Some(key) => AwsResponse::json(
                200,
                json!({
                    "KeyRotationEnabled": *key.rotation_enabled.read(),
                }),
            ),
            None => Self::not_found(),
        }
    }

    fn rotate_key_on_demand(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        match state.get_key(&key_id) {
            Some(key) => {
                *key.last_rotation_ms.write() = Some(chrono::Utc::now().timestamp_millis() as u64);
                AwsResponse::json(200, json!({"KeyId": key.arn}))
            }
            None => Self::not_found(),
        }
    }

    fn replicate_key(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let replica_region = pstr(req, "ReplicaRegion").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let key = match state.get_key(&key_id) {
            Some(k) => k,
            None => return Self::not_found(),
        };
        if !*key.multi_region.read() {
            return AwsResponse::error(
                400,
                "ValidationException",
                "The specified key is not a multi-region key",
            );
        }

        let replica_state = self.get_state(req.account, &replica_region);
        let existing = replica_state.get_key(&key.key_id);
        let replica = match existing {
            Some(k) => k,
            None => {
                let replica = Arc::new(Key::with_id(
                    key.key_id.clone(),
                    req.account,
                    replica_region.clone(),
                    &key.key_usage,
                    &key.key_spec,
                ));
                *replica.description.write() = key.description.read().clone();
                *replica.multi_region.write() = true;
                *replica.rotation_enabled.write() = *key.rotation_enabled.read();
                replica_state.put_key(Arc::clone(&replica));
                replica
            }
        };

        let meta = self.key_metadata(&replica, req);
        let mut obj = meta.as_object().cloned().unwrap_or_default();
        obj.insert(
            "MultiRegionConfiguration".into(),
            json!({
                "MultiRegionKeyType": "REPLICA",
                "PrimaryKey": {"Arn": key.arn, "Region": key.region},
                "ReplicaKeys": [{"Arn": replica.arn, "Region": replica_region}],
            }),
        );
        AwsResponse::json(200, json!({"ReplicaKeyMetadata": Value::Object(obj)}))
    }

    // ---------- aliases / tags ----------

    fn create_alias(&self, req: &AwsRequest) -> AwsResponse {
        let alias_name = pstr(req, "AliasName").unwrap_or_default();
        let key_id = pstr(req, "TargetKeyId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        if state.get_key(&key_id).is_none() {
            return Self::not_found();
        }
        if !alias_name.starts_with("alias/") {
            return AwsResponse::error(
                400,
                "ValidationException",
                "The alias name must start with alias/",
            );
        }
        state.create_alias(&alias_name, &key_id);
        AwsResponse::json(200, json!({}))
    }

    fn delete_alias(&self, req: &AwsRequest) -> AwsResponse {
        let alias_name = pstr(req, "AliasName").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        state.delete_alias(&alias_name);
        AwsResponse::json(200, json!({}))
    }

    fn update_alias(&self, req: &AwsRequest) -> AwsResponse {
        let alias_name = pstr(req, "AliasName").unwrap_or_default();
        let key_id = pstr(req, "TargetKeyId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        if state.get_key(&key_id).is_none() {
            return Self::not_found();
        }
        state.update_alias(&alias_name, &key_id);
        AwsResponse::json(200, json!({}))
    }

    fn list_aliases(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let alias_list: Vec<Value> = state
            .list_aliases()
            .iter()
            .map(|(name, key_id)| {
                json!({
                    "AliasName": name,
                    "AliasArn": format!("arn:aws:kms:{}:{}:{}", req.region, req.account, name),
                    "TargetKeyId": key_id,
                    "CreationDate": now_epoch_f64(),
                })
            })
            .collect();
        AwsResponse::json(
            200,
            json!({ "Aliases": alias_list, "NextMarker": null, "Truncated": false }),
        )
    }

    fn tag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let tags = req.params.get("Tags").cloned().unwrap_or(Value::Null);
        let mut all_tags = state.tags.write();
        let entry = all_tags.entry(key_id.clone()).or_default();
        if let Some(arr) = tags.as_array() {
            for tag in arr {
                let key = tag.get("TagKey").and_then(|v| v.as_str()).unwrap_or("");
                let value = tag.get("TagValue").and_then(|v| v.as_str()).unwrap_or("");
                entry.insert(key.to_string(), json!(value));
            }
        }
        AwsResponse::json(200, json!({}))
    }

    fn untag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let tag_keys: Vec<String> = req
            .params
            .get("TagKeys")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut all_tags = state.tags.write();
        if let Some(entry) = all_tags.get_mut(&key_id) {
            for key in &tag_keys {
                entry.remove(key);
            }
        }
        AwsResponse::json(200, json!({}))
    }

    fn list_resource_tags(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let tags = state.tags.read().get(&key_id).cloned().unwrap_or_default();
        let tags_list: Vec<Value> = tags
            .iter()
            .map(|(k, v)| json!({ "TagKey": k, "TagValue": v.as_str().unwrap_or("") }))
            .collect();
        AwsResponse::json(
            200,
            json!({ "Tags": tags_list, "NextMarker": null, "Truncated": false }),
        )
    }

    // ---------- crypto: encrypt / decrypt / data keys ----------

    /// HTTPS 400 "not valid" error for ciphertext blobs (moto body style: the
    /// `__type` alone, no message).
    fn invalid_ciphertext() -> AwsResponse {
        AwsResponse::error(400, "InvalidCiphertextException", "")
    }

    /// Error for a ciphertext whose blob header names a key this state does
    /// not know (moto mirrors real KMS with AccessDeniedException here).
    fn ciphertext_key_missing() -> AwsResponse {
        AwsResponse::error(
            400,
            "AccessDeniedException",
            "The ciphertext refers to a customer master key that does not exist, \
             does not exist in this region, or you are not allowed to access.",
        )
    }

    fn encrypt(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let key = match state.get_key(&key_id) {
            Some(k) => k,
            None => return Self::not_found(),
        };
        if !*key.enabled.read() {
            return AwsResponse::error(400, "DisabledException", "Key is disabled");
        }
        let plaintext = decode_param_bytes(req, "Plaintext");
        // Moto validates the plaintext against the real KMS 4 KiB input limit.
        if plaintext.is_empty() {
            return AwsResponse::error(
                400,
                "ValidationException",
                "1 validation error detected: Value at 'plaintext' failed to satisfy \
                 constraint: Member must have length greater than or equal to 1",
            );
        }
        if plaintext.len() > 4096 {
            return AwsResponse::error(
                400,
                "ValidationException",
                "1 validation error detected: Value at 'plaintext' failed to satisfy \
                 constraint: Member must have length less than or equal to 4096",
            );
        }
        let entries = context_entries(req.params.get("EncryptionContext"));
        let blob = cipher::encrypt(&key.key_material.read(), &key.key_id, &plaintext, &entries);
        AwsResponse::json(
            200,
            json!({
                "CiphertextBlob": B64.encode(blob),
                "KeyId": key.arn,
                "EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
            }),
        )
    }

    fn decrypt(&self, req: &AwsRequest) -> AwsResponse {
        let blob = decode_param_bytes(req, "CiphertextBlob");
        let ciphertext = match cipher::Ciphertext::parse(&blob) {
            Some(ct) => ct,
            None => return Self::invalid_ciphertext(),
        };
        // The blob header names the key that encrypted it; moto (like real
        // KMS) ignores any requested KeyId on Decrypt.
        let state = self.get_state(req.account, &req.region);
        let key = match state.keys.read().get(&ciphertext.key_id).cloned() {
            Some(k) => k,
            None => return Self::ciphertext_key_missing(),
        };
        let entries = context_entries(req.params.get("EncryptionContext"));
        let plaintext = match cipher::decrypt(&key.key_material.read(), &ciphertext, &entries) {
            Some(pt) => pt,
            None => return Self::invalid_ciphertext(),
        };
        AwsResponse::json(
            200,
            json!({
                "Plaintext": B64.encode(plaintext),
                "KeyId": key.arn,
                "EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
            }),
        )
    }

    fn re_encrypt(&self, req: &AwsRequest) -> AwsResponse {
        let destination = pstr(req, "DestinationKeyId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let dest_key = match state.get_key(&destination) {
            Some(k) => k,
            None => return Self::not_found(),
        };
        let blob = decode_param_bytes(req, "CiphertextBlob");
        let ciphertext = match cipher::Ciphertext::parse(&blob) {
            Some(ct) => ct,
            None => return Self::invalid_ciphertext(),
        };
        let source_key = match state.keys.read().get(&ciphertext.key_id).cloned() {
            Some(k) => k,
            None => return Self::ciphertext_key_missing(),
        };
        // Moto decrypts against SourceEncryptionContext (any mismatch fails
        // with InvalidCiphertextException) and re-encrypts the recovered
        // plaintext under the destination key bound to
        // DestinationEncryptionContext.
        let source_ctx = context_entries(req.params.get("SourceEncryptionContext"));
        let plaintext =
            match cipher::decrypt(&source_key.key_material.read(), &ciphertext, &source_ctx) {
                Some(pt) => pt,
                None => return Self::invalid_ciphertext(),
            };
        let dest_ctx = context_entries(req.params.get("DestinationEncryptionContext"));
        let new_blob = cipher::encrypt(
            &dest_key.key_material.read(),
            &dest_key.key_id,
            &plaintext,
            &dest_ctx,
        );
        AwsResponse::json(
            200,
            json!({
                "CiphertextBlob": B64.encode(new_blob),
                "KeyId": dest_key.arn,
                "SourceKeyId": source_key.arn,
                "SourceEncryptionAlgorithm": "SYMMETRIC_DEFAULT",
                "DestinationEncryptionAlgorithm": "SYMMETRIC_DEFAULT",
            }),
        )
    }

    /// GenerateDataKey and GenerateDataKeyWithoutPlaintext.
    fn generate_data_key(&self, req: &AwsRequest, include_plaintext: bool) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let key = match state.get_key(&key_id) {
            Some(k) => k,
            None => return Self::not_found(),
        };
        let has_bytes = req.params.get("NumberOfBytes").is_some();
        let has_spec = req.params.get("KeySpec").is_some();
        if has_bytes && has_spec {
            // moto responses.py:585-588 — mutual exclusion.
            return AwsResponse::error(
                400,
                "ValidationException",
                "Please specify either number of bytes or key spec.",
            );
        }
        let length = match req.params.get("NumberOfBytes").and_then(|v| v.as_u64()) {
            Some(n) if (1..=1024).contains(&n) => n as usize,
            Some(_) => {
                return AwsResponse::error(
                    400,
                    "ValidationException",
                    "The NumberOfBytes parameter must be between 1 and 1024",
                )
            }
            None => {
                if !has_spec {
                    // moto responses.py:580-583 — neither parameter is an
                    // error, not a silent default.
                    return AwsResponse::error(
                        400,
                        "ValidationException",
                        "Please specify either number of bytes or key spec.",
                    );
                }
                match pstr(req, "KeySpec").as_deref() {
                    Some("AES_128") => 16,
                    Some("AES_256") => 32,
                    // moto responses.py:574-579 — unknown KeySpec values are
                    // a member-enum validation error, not a silent default.
                    _ => {
                        return AwsResponse::error(
                            400,
                            "ValidationException",
                            "1 validation error detected: Value at 'keySpec' failed to satisfy constraint: Member must satisfy enum value set: [AES_256, AES_128]",
                        )
                    }
                }
            }
        };
        let plaintext: Vec<u8> = (0..length).map(|_| rand::random::<u8>()).collect();
        let entries = context_entries(req.params.get("EncryptionContext"));
        let blob = cipher::encrypt(&key.key_material.read(), &key.key_id, &plaintext, &entries);
        let mut body = json!({
            "CiphertextBlob": B64.encode(blob),
            "KeyId": key.arn,
            "EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
        });
        if include_plaintext {
            body["Plaintext"] = json!(B64.encode(&plaintext));
        }
        AwsResponse::json(200, body)
    }

    /// GenerateDataKeyPair and GenerateDataKeyPairWithoutPlaintext.
    fn generate_data_key_pair(&self, req: &AwsRequest, include_plaintext: bool) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let key = match state.get_key(&key_id) {
            Some(k) => k,
            None => return Self::not_found(),
        };
        let pair_spec = pstr(req, "KeyPairSpec").unwrap_or_default();

        let (private_key_der, public_key_der) = match pair_spec.as_str() {
            "RSA_2048" | "RSA_3072" | "RSA_4096" => {
                let bits = match pair_spec.as_str() {
                    "RSA_3072" => 3072,
                    "RSA_4096" => 4096,
                    _ => 2048,
                };
                let priv_key = match rsa::RsaPrivateKey::new(&mut rand::thread_rng(), bits) {
                    Ok(k) => k,
                    Err(e) => {
                        return AwsResponse::error(
                            500,
                            "InternalFailure",
                            &format!("RSA keygen failed: {}", e),
                        )
                    }
                };
                use rsa::pkcs8::{EncodePrivateKey, EncodePublicKey};
                let priv_der = priv_key
                    .to_pkcs8_der()
                    .map(|d| d.as_bytes().to_vec())
                    .unwrap_or_default();
                let pub_der = rsa::RsaPublicKey::from(&priv_key)
                    .to_public_key_der()
                    .map(|d| d.as_bytes().to_vec())
                    .unwrap_or_default();
                (priv_der, pub_der)
            }
            "ECC_NIST_P256" | "ECC_SECG_P256K1" => {
                let mut secret_bytes = [0u8; 32];
                for b in secret_bytes.iter_mut() {
                    *b = rand::random::<u8>();
                }
                let secret = match p256::SecretKey::from_slice(&secret_bytes) {
                    Ok(s) => s,
                    Err(e) => {
                        return AwsResponse::error(
                            500,
                            "InternalFailure",
                            &format!("EC keygen failed: {}", e),
                        )
                    }
                };
                use p256::pkcs8::{EncodePrivateKey, EncodePublicKey};
                let priv_der = secret
                    .to_pkcs8_der()
                    .map(|d| d.as_bytes().to_vec())
                    .unwrap_or_default();
                let pub_der = secret
                    .public_key()
                    .to_public_key_der()
                    .map(|d| d.as_bytes().to_vec())
                    .unwrap_or_default();
                (priv_der, pub_der)
            }
            other => {
                return AwsResponse::error(
                    400,
                    "UnsupportedOperationException",
                    &format!("KeyPairSpec {} is not supported", other),
                )
            }
        };

        let priv_plain = B64.encode(&private_key_der);
        let entries = context_entries(req.params.get("EncryptionContext"));
        let blob = cipher::encrypt(
            &key.key_material.read(),
            &key.key_id,
            &private_key_der,
            &entries,
        );
        let mut body = json!({
            "PrivateKeyCiphertextBlob": B64.encode(blob),
            "PublicKey": B64.encode(&public_key_der),
            "KeyId": key.arn,
            "KeyPairSpec": pair_spec,
        });
        if include_plaintext {
            body["PrivateKeyPlaintext"] = json!(priv_plain);
        }
        AwsResponse::json(200, body)
    }

    fn generate_random(&self, req: &AwsRequest) -> AwsResponse {
        let len = req
            .params
            .get("NumberOfBytes")
            .and_then(|v| v.as_u64())
            .unwrap_or(16);
        if len == 0 || len > 1024 {
            return AwsResponse::error(
                400,
                "ValidationException",
                "The NumberOfBytes parameter must be between 1 and 1024",
            );
        }
        let data: Vec<u8> = (0..len).map(|_| rand::random::<u8>()).collect();
        AwsResponse::json(200, json!({ "Plaintext": B64.encode(data) }))
    }

    // ---------- MAC / sign ----------

    fn hmac_output(algo: &str, key: &[u8], message: &[u8]) -> Option<Vec<u8>> {
        let mac_for = |algo: &str, key: &[u8], message: &[u8]| -> Option<Vec<u8>> {
            match algo {
                "HMAC_SHA_224" => {
                    let mut mac = Hmac::<Sha224>::new_from_slice(key).ok()?;
                    mac.update(message);
                    Some(mac.finalize().into_bytes().to_vec())
                }
                "HMAC_SHA_256" => {
                    let mut mac = Hmac::<Sha256>::new_from_slice(key).ok()?;
                    mac.update(message);
                    Some(mac.finalize().into_bytes().to_vec())
                }
                "HMAC_SHA_384" => {
                    let mut mac = Hmac::<Sha384>::new_from_slice(key).ok()?;
                    mac.update(message);
                    Some(mac.finalize().into_bytes().to_vec())
                }
                "HMAC_SHA_512" => {
                    let mut mac = Hmac::<Sha512>::new_from_slice(key).ok()?;
                    mac.update(message);
                    Some(mac.finalize().into_bytes().to_vec())
                }
                _ => None,
            }
        };
        mac_for(algo, key, message)
    }

    fn generate_mac(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let algo = pstr(req, "MacAlgorithm").unwrap_or_else(|| "HMAC_SHA_256".into());
        let state = self.get_state(req.account, &req.region);
        let key = match state.get_key(&key_id) {
            Some(k) => k,
            None => return Self::not_found(),
        };
        if key.key_usage != "GENERATE_VERIFY_MAC" {
            return AwsResponse::error(
                400,
                "InvalidKeyUsageException",
                "The key is not capable to generate mac",
            );
        }
        let message = decode_param_bytes(req, "Message");
        let mac = match Self::hmac_output(&algo, &key.key_material.read(), &message) {
            Some(m) => m,
            None => {
                return AwsResponse::error(
                    400,
                    "ValidationException",
                    &format!("The MacAlgorithm {} is not supported", algo),
                )
            }
        };
        AwsResponse::json(
            200,
            json!({
                "Mac": B64.encode(mac),
                "MacAlgorithm": algo,
                "KeyId": key.key_id,
            }),
        )
    }

    fn verify_mac(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let algo = pstr(req, "MacAlgorithm").unwrap_or_else(|| "HMAC_SHA_256".into());
        let state = self.get_state(req.account, &req.region);
        let key = match state.get_key(&key_id) {
            Some(k) => k,
            None => return Self::not_found(),
        };
        let message = decode_param_bytes(req, "Message");
        let expect_mac = decode_param_bytes(req, "Mac");
        let mac = match Self::hmac_output(&algo, &key.key_material.read(), &message) {
            Some(m) => m,
            None => {
                return AwsResponse::error(
                    400,
                    "ValidationException",
                    &format!("The MacAlgorithm {} is not supported", algo),
                )
            }
        };
        AwsResponse::json(
            200,
            json!({
                "KeyId": key.key_id,
                "MacValid": mac == expect_mac,
                "MacAlgorithm": algo,
            }),
        )
    }

    fn sign(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let algo = pstr(req, "SigningAlgorithm").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let key = match state.get_key(&key_id) {
            Some(k) => k,
            None => return Self::not_found(),
        };
        let message = decode_param_bytes(req, "Message");
        match sign_raw(&key, &algo, &message) {
            Ok(sig) => AwsResponse::json(
                200,
                json!({
                    "KeyId": key.arn,
                    "Signature": B64.encode(sig),
                    "SigningAlgorithm": algo,
                }),
            ),
            Err(resp) => resp,
        }
    }

    fn verify(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let algo = pstr(req, "SigningAlgorithm").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let key = match state.get_key(&key_id) {
            Some(k) => k,
            None => return Self::not_found(),
        };
        let message = decode_param_bytes(req, "Message");
        let signature = decode_param_bytes(req, "Signature");
        match verify_raw(&key, &algo, &message, &signature) {
            Ok(valid) => AwsResponse::json(
                200,
                json!({
                    "KeyId": key.arn,
                    "SignatureValid": valid,
                    "SigningAlgorithm": algo,
                }),
            ),
            Err(resp) => resp,
        }
    }

    fn get_public_key(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let key = match state.get_key(&key_id) {
            Some(k) => k,
            None => return Self::not_found(),
        };
        if !key.is_rsa() && !key.is_ecc() {
            return AwsResponse::error(
                400,
                "UnsupportedOperationException",
                "GetPublicKey is not supported for symmetric keys",
            );
        }
        let public_key_der = if key.is_rsa() {
            let priv_key = key.rsa_key.read();
            let priv_key = match priv_key.as_ref() {
                Some(k) => k,
                None => return Self::not_found(),
            };
            rsa_public_der(priv_key)
        } else {
            let ec_key = key.ec_key.read();
            let ec_key = match ec_key.as_ref() {
                Some(k) => k,
                None => return Self::not_found(),
            };
            ec_public_der(ec_key)
        };
        let (_, sign, _) = key.algorithms();
        AwsResponse::json(
            200,
            json!({
                "KeyId": key.key_id,
                "PublicKey": B64.encode(public_key_der),
                "KeySpec": key.key_spec,
                "KeyUsage": key.key_usage,
                "CustomerMasterKeySpec": key.key_spec,
                "SigningAlgorithms": sign,
            }),
        )
    }

    fn derive_shared_secret(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let algo = pstr(req, "KeyAgreementAlgorithm").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let key = match state.get_key(&key_id) {
            Some(k) => k,
            None => return Self::not_found(),
        };
        if !key.is_ecc() {
            return AwsResponse::error(
                400,
                "UnsupportedOperationException",
                "The key ID must reference an ECC key spec",
            );
        }
        if key.key_usage != "KEY_AGREEMENT" {
            return AwsResponse::error(
                400,
                "InvalidKeyUsageException",
                "The key usage KEY_AGREEMENT is required",
            );
        }
        if algo != "ECDH" {
            return AwsResponse::error(
                400,
                "ValidationException",
                &format!("The KeyAgreementAlgorithm {} is not supported", algo),
            );
        }
        use p256::pkcs8::DecodePublicKey as _;
        let their_bytes = decode_param_bytes(req, "PublicKey");
        let their_public = match p256::PublicKey::from_public_key_der(&their_bytes)
            .or_else(|_| p256::PublicKey::from_sec1_bytes(&their_bytes))
        {
            Ok(pk) => pk,
            Err(e) => {
                return AwsResponse::error(
                    400,
                    "InvalidGrantIdException",
                    &format!("Invalid public key: {}", e),
                )
            }
        };
        let secret = key.ec_key.read().clone();
        let secret = match secret {
            Some(s) => s,
            None => return Self::not_found(),
        };
        let shared =
            p256::ecdh::diffie_hellman(secret.to_nonzero_scalar(), their_public.as_affine());
        AwsResponse::json(
            200,
            json!({
                "SharedSecret": B64.encode(shared.raw_secret_bytes()),
                "KeyId": key.key_id,
                "KeyAgreementAlgorithm": algo,
            }),
        )
    }

    // ---------- grants ----------

    fn create_grant(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let key = match state.get_key(&key_id) {
            Some(k) => k,
            None => return Self::not_found(),
        };
        let grant_id = uuid::Uuid::new_v4().simple().to_string();
        let grant_token = format!("{}:{}", grant_id, uuid::Uuid::new_v4().simple());
        let grant = Grant {
            grant_id: grant_id.clone(),
            grant_token: grant_token.clone(),
            key_id: key.key_id.clone(),
            grantee_principal: pstr(req, "GranteePrincipal").unwrap_or_default(),
            retiring_principal: pstr(req, "RetiringPrincipal").unwrap_or_default(),
            operations: string_list(req, "Operations"),
            constraints: req.params.get("Constraints").cloned(),
            name: pstr(req, "Name").unwrap_or_default(),
            issuing_account: format!("arn:aws:iam::{}:root", req.account),
            creation_date_ms: chrono::Utc::now().timestamp_millis() as u64,
        };
        state.grants.write().push(grant);
        AwsResponse::json(200, json!({"GrantId": grant_id, "GrantToken": grant_token}))
    }

    fn grant_json(grant: &Grant) -> Value {
        let mut obj = json!({
            "GrantId": grant.grant_id,
            "GranteePrincipal": grant.grantee_principal,
            "IssuingAccount": grant.issuing_account,
            "Name": grant.name,
            "Operations": grant.operations,
            "RetiringPrincipal": grant.retiring_principal,
            "CreationDate": grant.creation_date_ms as f64 / 1000.0,
        });
        if let Some(c) = &grant.constraints {
            obj["Constraints"] = c.clone();
        }
        obj
    }

    fn list_grants(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let key = match state.get_key(&key_id) {
            Some(k) => k,
            None => return Self::not_found(),
        };
        let grants: Vec<Value> = state
            .grants
            .read()
            .iter()
            .filter(|g| g.key_id == key.key_id)
            .map(Self::grant_json)
            .collect();
        AwsResponse::json(
            200,
            json!({"Grants": grants, "NextMarker": null, "Truncated": false}),
        )
    }

    fn list_retirable_grants(&self, req: &AwsRequest) -> AwsResponse {
        let retiring = pstr(req, "RetiringPrincipal").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let grants: Vec<Value> = state
            .grants
            .read()
            .iter()
            .filter(|g| g.retiring_principal == retiring)
            .map(Self::grant_json)
            .collect();
        AwsResponse::json(
            200,
            json!({"Grants": grants, "NextMarker": null, "Truncated": false}),
        )
    }

    fn retire_grant(&self, req: &AwsRequest) -> AwsResponse {
        let token = pstr(req, "GrantToken").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut grants = state.grants.write();
        let removed = if !token.is_empty() {
            let before = grants.len();
            grants.retain(|g| g.grant_token != token);
            grants.len() != before
        } else {
            let key_id = pstr(req, "KeyId").unwrap_or_default();
            let grant_id = pstr(req, "GrantId").unwrap_or_default();
            if let Some(key) = state.get_key(&key_id) {
                let key_id = key.key_id.clone();
                let before = grants.len();
                grants.retain(|g| !(g.key_id == key_id && g.grant_id == grant_id));
                grants.len() != before
            } else {
                false
            }
        };
        drop(grants);
        if removed {
            AwsResponse::json(200, json!({}))
        } else {
            Self::not_found()
        }
    }

    fn revoke_grant(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let grant_id = pstr(req, "GrantId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let key = match state.get_key(&key_id) {
            Some(k) => k,
            None => return Self::not_found(),
        };
        let mut grants = state.grants.write();
        let before = grants.len();
        grants.retain(|g| !(g.key_id == key.key_id && g.grant_id == grant_id));
        if grants.len() == before {
            return Self::not_found();
        }
        AwsResponse::json(200, json!({}))
    }

    // ---------- key policies ----------

    fn put_key_policy(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let policy_name = pstr(req, "PolicyName").unwrap_or_else(|| "default".into());
        let policy = pstr(req, "Policy").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        match state.get_key(&key_id) {
            Some(key) => {
                key.policies.write().insert(policy_name, policy);
                AwsResponse::json(200, json!({}))
            }
            None => Self::not_found(),
        }
    }

    fn get_key_policy(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let policy_name = pstr(req, "PolicyName").unwrap_or_else(|| "default".into());
        let state = self.get_state(req.account, &req.region);
        match state.get_key(&key_id) {
            Some(key) => {
                let policy = key
                    .policies
                    .read()
                    .get(&policy_name)
                    .cloned()
                    .unwrap_or_else(|| default_policy(&key.arn, req.account).to_string());
                AwsResponse::json(200, json!({"Policy": policy}))
            }
            None => Self::not_found(),
        }
    }

    // ---------- custom key stores ----------

    fn create_custom_key_store(&self, req: &AwsRequest) -> AwsResponse {
        let name = pstr(req, "CustomKeyStoreName").unwrap_or_default();
        let id = format!("cks-{}", hex_block());
        let store = Arc::new(CustomKeyStore {
            id: id.clone(),
            name: RwLock::new(name),
            cloud_hsm_cluster_id: RwLock::new(pstr(req, "CloudHsmClusterId").unwrap_or_default()),
            trust_anchor_certificate: pstr(req, "TrustAnchorCertificate").unwrap_or_default(),
            key_store_password: pstr(req, "KeyStorePassword").unwrap_or_default(),
            connection_state: RwLock::new("DISCONNECTED".into()),
            creation_date_ms: chrono::Utc::now().timestamp_millis() as u64,
        });
        self.get_state(req.account, &req.region)
            .custom_key_stores
            .write()
            .insert(id.clone(), store);
        AwsResponse::json(200, json!({"CustomKeyStoreId": id}))
    }

    fn describe_custom_key_stores(&self, req: &AwsRequest) -> AwsResponse {
        let filter_id = pstr(req, "CustomKeyStoreId");
        let filter_name = pstr(req, "CustomKeyStoreName");
        let state = self.get_state(req.account, &req.region);
        let stores: Vec<Value> = state
            .custom_key_stores
            .read()
            .values()
            .filter(|s| match (&filter_id, &filter_name) {
                (Some(id), _) => s.id == *id,
                (None, Some(name)) => *s.name.read() == *name,
                _ => true,
            })
            .map(|s| {
                json!({
                    "ConnectionState": *s.connection_state.read(),
                    "CreationDate": s.creation_date_ms as f64 / 1000.0,
                    "CustomKeyStoreId": s.id,
                    "CustomKeyStoreName": *s.name.read(),
                    "CustomKeyStoreType": "AWS_CLOUDHSM",
                })
            })
            .collect();
        AwsResponse::json(200, json!({"CustomKeyStores": stores}))
    }

    fn set_custom_key_store_state(&self, req: &AwsRequest, connection_state: &str) -> AwsResponse {
        let id = pstr(req, "CustomKeyStoreId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let found = state.custom_key_stores.read().get(&id).cloned();
        match found {
            Some(store) => {
                *store.connection_state.write() = connection_state.to_string();
                AwsResponse::json(200, json!({}))
            }
            None => Self::custom_ks_not_found(),
        }
    }

    fn update_custom_key_store(&self, req: &AwsRequest) -> AwsResponse {
        let id = pstr(req, "CustomKeyStoreId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let found = state.custom_key_stores.read().get(&id).cloned();
        match found {
            Some(store) => {
                if let Some(name) = pstr(req, "NewCustomKeyStoreName") {
                    *store.name.write() = name;
                }
                if let Some(cluster) = pstr(req, "NewCloudHsmClusterId") {
                    *store.cloud_hsm_cluster_id.write() = cluster;
                }
                AwsResponse::json(200, json!({}))
            }
            None => Self::custom_ks_not_found(),
        }
    }

    fn delete_custom_key_store(&self, req: &AwsRequest) -> AwsResponse {
        let id = pstr(req, "CustomKeyStoreId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let existed = state.custom_key_stores.write().remove(&id).is_some();
        if existed {
            AwsResponse::json(200, json!({}))
        } else {
            Self::custom_ks_not_found()
        }
    }

    // ---------- import key material ----------

    fn get_parameters_for_import(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let key = match state.get_key(&key_id) {
            Some(k) => k,
            None => return Self::not_found(),
        };
        if *key.origin.read() != "EXTERNAL" {
            return AwsResponse::error(
                400,
                "UnsupportedOperationException",
                "The key is not available for import",
            );
        }
        let pub_der = rsa_public_der(wrapping_keypair());
        let import_token = uuid::Uuid::new_v4().simple().to_string();
        AwsResponse::json(
            200,
            json!({
                "KeyId": key.key_id,
                "ImportToken": import_token,
                "PublicKey": B64.encode(pub_der),
                "ParametersValidTo": now_epoch_f64() + 24.0 * 3600.0,
            }),
        )
    }

    fn import_key_material(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        match state.get_key(&key_id) {
            Some(key) => {
                if *key.origin.read() != "EXTERNAL" {
                    return AwsResponse::error(
                        400,
                        "UnsupportedOperationException",
                        "The key is not a key that can be imported",
                    );
                }
                *key.has_imported_material.write() = true;
                *key.key_state.write() = "Enabled".to_string();
                AwsResponse::json(200, json!({}))
            }
            None => Self::not_found(),
        }
    }

    fn delete_imported_key_material(&self, req: &AwsRequest) -> AwsResponse {
        let key_id = pstr(req, "KeyId").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        match state.get_key(&key_id) {
            Some(key) => {
                *key.has_imported_material.write() = false;
                *key.key_state.write() = "PendingImport".to_string();
                AwsResponse::json(200, json!({}))
            }
            None => Self::not_found(),
        }
    }
}

impl Default for KmsHandler {
    fn default() -> Self {
        Self::new()
    }
}

// ---------- param helpers ----------

fn pstr(req: &AwsRequest, key: &str) -> Option<String> {
    req.params
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn string_list(req: &AwsRequest, key: &str) -> Vec<String> {
    req.params
        .get(key)
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

/// Decode a blob request parameter (base64-encoded in the JSON protocol).
fn decode_param_bytes(req: &AwsRequest, key: &str) -> Vec<u8> {
    pstr(req, key)
        .and_then(|v| B64.decode(v.as_bytes()).ok())
        .unwrap_or_default()
}

fn hex_block() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..12].to_string()
}

// ---------- crypto helpers ----------

fn rsa_public_der(priv_key: &rsa::RsaPrivateKey) -> Vec<u8> {
    use rsa::pkcs8::EncodePublicKey;
    rsa::RsaPublicKey::from(priv_key)
        .to_public_key_der()
        .map(|d| d.as_bytes().to_vec())
        .unwrap_or_default()
}

fn ec_public_der(secret: &p256::SecretKey) -> Vec<u8> {
    use p256::pkcs8::EncodePublicKey;
    secret
        .public_key()
        .to_public_key_der()
        .map(|d| d.as_bytes().to_vec())
        .unwrap_or_default()
}

/// Sign `message` with the key's material using the requested algorithm.
fn sign_raw(key: &Key, algo: &str, message: &[u8]) -> Result<Vec<u8>, AwsResponse> {
    use rsa::signature::{SignatureEncoding, Signer};
    if key.key_usage != "SIGN_VERIFY" {
        return Err(AwsResponse::error(
            400,
            "InvalidKeyUsageException",
            "The key is not capable to sign",
        ));
    }
    if key.is_rsa() {
        let priv_key = key.rsa_key.read();
        let priv_key = match priv_key.as_ref() {
            Some(k) => k,
            None => return Err(KmsHandler::not_found()),
        };
        let sig = match algo {
            "RSASSA_PKCS1_V1_5_SHA_256" => {
                rsa::pkcs1v15::SigningKey::<Sha256>::new(priv_key.clone())
                    .sign(message)
                    .to_bytes()
            }
            "RSASSA_PKCS1_V1_5_SHA_384" => {
                rsa::pkcs1v15::SigningKey::<Sha384>::new(priv_key.clone())
                    .sign(message)
                    .to_bytes()
            }
            "RSASSA_PKCS1_V1_5_SHA_512" => {
                rsa::pkcs1v15::SigningKey::<Sha512>::new(priv_key.clone())
                    .sign(message)
                    .to_bytes()
            }
            "RSASSA_PSS_SHA_256" => rsa::pss::SigningKey::<Sha256>::new(priv_key.clone())
                .sign(message)
                .to_bytes(),
            "RSASSA_PSS_SHA_384" => rsa::pss::SigningKey::<Sha384>::new(priv_key.clone())
                .sign(message)
                .to_bytes(),
            "RSASSA_PSS_SHA_512" => rsa::pss::SigningKey::<Sha512>::new(priv_key.clone())
                .sign(message)
                .to_bytes(),
            other => {
                return Err(AwsResponse::error(
                    400,
                    "UnsupportedOperationException",
                    &format!("The SigningAlgorithm {} is not supported", other),
                ))
            }
        };
        return Ok(sig.into());
    }
    if key.is_ecc() {
        if algo != "ECDSA_SHA_256" {
            return Err(AwsResponse::error(
                400,
                "UnsupportedOperationException",
                &format!("The SigningAlgorithm {} is not supported", algo),
            ));
        }
        let ec_key = key.ec_key.read();
        let ec_key = match ec_key.as_ref() {
            Some(k) => k,
            None => return Err(KmsHandler::not_found()),
        };
        let signing = p256::ecdsa::SigningKey::from(ec_key.clone());
        let sig: p256::ecdsa::Signature = signing.sign(message);
        return Ok(sig.to_bytes().to_vec());
    }
    Err(AwsResponse::error(
        400,
        "UnsupportedOperationException",
        "The key is not capable to sign",
    ))
}

/// Verify a signature; returns Ok(valid) or an error response for bad algorithms.
fn verify_raw(
    key: &Key,
    algo: &str,
    message: &[u8],
    signature: &[u8],
) -> Result<bool, AwsResponse> {
    use rsa::signature::Verifier;
    if key.key_usage != "SIGN_VERIFY" {
        return Err(AwsResponse::error(
            400,
            "InvalidKeyUsageException",
            "The key is not capable to verify",
        ));
    }
    if key.is_rsa() {
        let priv_key = key.rsa_key.read();
        let priv_key = match priv_key.as_ref() {
            Some(k) => k,
            None => return Err(KmsHandler::not_found()),
        };
        let public = rsa::RsaPublicKey::from(priv_key);
        macro_rules! verify_pkcs1 {
            ($digest:ty) => {{
                let vk = rsa::pkcs1v15::VerifyingKey::<$digest>::new(public.clone());
                match rsa::pkcs1v15::Signature::try_from(signature) {
                    Ok(sig) => vk.verify(message, &sig).is_ok(),
                    Err(_) => false,
                }
            }};
        }
        let valid = match algo {
            "RSASSA_PKCS1_V1_5_SHA_256" => verify_pkcs1!(Sha256),
            "RSASSA_PKCS1_V1_5_SHA_384" => verify_pkcs1!(Sha384),
            "RSASSA_PKCS1_V1_5_SHA_512" => verify_pkcs1!(Sha512),
            "RSASSA_PSS_SHA_256" => {
                let vk = rsa::pss::VerifyingKey::<Sha256>::new(public.clone());
                match rsa::pss::Signature::try_from(signature) {
                    Ok(sig) => vk.verify(message, &sig).is_ok(),
                    Err(_) => false,
                }
            }
            "RSASSA_PSS_SHA_384" => {
                let vk = rsa::pss::VerifyingKey::<Sha384>::new(public.clone());
                match rsa::pss::Signature::try_from(signature) {
                    Ok(sig) => vk.verify(message, &sig).is_ok(),
                    Err(_) => false,
                }
            }
            "RSASSA_PSS_SHA_512" => {
                let vk = rsa::pss::VerifyingKey::<Sha512>::new(public.clone());
                match rsa::pss::Signature::try_from(signature) {
                    Ok(sig) => vk.verify(message, &sig).is_ok(),
                    Err(_) => false,
                }
            }
            other => {
                return Err(AwsResponse::error(
                    400,
                    "UnsupportedOperationException",
                    &format!("The SigningAlgorithm {} is not supported", other),
                ))
            }
        };
        return Ok(valid);
    }
    if key.is_ecc() {
        if algo != "ECDSA_SHA_256" {
            return Err(AwsResponse::error(
                400,
                "UnsupportedOperationException",
                &format!("The SigningAlgorithm {} is not supported", algo),
            ));
        }
        let ec_key = key.ec_key.read();
        let ec_key = match ec_key.as_ref() {
            Some(k) => k,
            None => return Err(KmsHandler::not_found()),
        };
        let vk = p256::ecdsa::VerifyingKey::from(ec_key.public_key());
        let Ok(sig) = p256::ecdsa::Signature::from_slice(signature) else {
            return Ok(false);
        };
        return Ok(vk.verify(message, &sig).is_ok());
    }
    Err(AwsResponse::error(
        400,
        "UnsupportedOperationException",
        "The key is not capable to verify",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::json;

    fn make_req(operation: &str, params: Value) -> AwsRequest {
        AwsRequest {
            service: "kms".to_string(),
            operation: operation.to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
        }
    }

    #[test]
    fn test_create_and_describe_key() {
        let handler = KmsHandler::new();
        let resp = handler.handle(make_req("CreateKey", json!({})));
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("KeyId"));

        let key_id = serde_json::from_str::<Value>(&resp.body).unwrap()["KeyMetadata"]["KeyId"]
            .as_str()
            .unwrap()
            .to_string();

        let resp = handler.handle(make_req("DescribeKey", json!({"KeyId": key_id})));
        assert_eq!(resp.status, 200);
        let meta: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(meta["KeyMetadata"]["KeyState"], "Enabled");
    }

    #[test]
    fn test_list_keys() {
        let handler = KmsHandler::new();
        handler.handle(make_req("CreateKey", json!({})));
        handler.handle(make_req("CreateKey", json!({})));
        let resp = handler.handle(make_req("ListKeys", json!({})));
        assert_eq!(resp.status, 200);
        let keys: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(keys["Keys"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_enable_disable_key() {
        let handler = KmsHandler::new();
        let resp = handler.handle(make_req("CreateKey", json!({})));
        let key_id = serde_json::from_str::<Value>(&resp.body).unwrap()["KeyMetadata"]["KeyId"]
            .as_str()
            .unwrap()
            .to_string();

        handler.handle(make_req("DisableKey", json!({"KeyId": key_id})));
        let resp = handler.handle(make_req("DescribeKey", json!({"KeyId": key_id})));
        assert!(resp.body.contains("\"KeyState\":\"Disabled\""));

        handler.handle(make_req("EnableKey", json!({"KeyId": key_id})));
        let resp = handler.handle(make_req("DescribeKey", json!({"KeyId": key_id})));
        assert!(resp.body.contains("\"KeyState\":\"Enabled\""));
    }

    #[test]
    fn test_schedule_and_cancel_deletion() {
        let handler = KmsHandler::new();
        let resp = handler.handle(make_req("CreateKey", json!({})));
        let key_id = serde_json::from_str::<Value>(&resp.body).unwrap()["KeyMetadata"]["KeyId"]
            .as_str()
            .unwrap()
            .to_string();

        let resp = handler.handle(make_req(
            "ScheduleKeyDeletion",
            json!({"KeyId": key_id, "PendingWindowInDays": 7}),
        ));
        assert!(resp.body.contains("PendingDeletion"));

        let resp = handler.handle(make_req("DescribeKey", json!({"KeyId": key_id})));
        assert!(resp.body.contains("PendingDeletion"));

        handler.handle(make_req("CancelKeyDeletion", json!({"KeyId": key_id})));
        let resp = handler.handle(make_req("DescribeKey", json!({"KeyId": key_id})));
        assert!(resp.body.contains("Disabled"));
    }

    #[test]
    fn test_generate_data_key_sizes() {
        let handler = KmsHandler::new();
        let resp = handler.handle(make_req("CreateKey", json!({})));
        let key_id = serde_json::from_str::<Value>(&resp.body).unwrap()["KeyMetadata"]["KeyId"]
            .as_str()
            .unwrap()
            .to_string();

        let resp = handler.handle(make_req(
            "GenerateDataKey",
            json!({"KeyId": key_id, "KeySpec": "AES_256"}),
        ));
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        let pt = B64.decode(body["Plaintext"].as_str().unwrap()).unwrap();
        assert_eq!(pt.len(), 32);

        // moto responses.py:574-588 — spec/bytes trio validations.
        let resp = handler.handle(make_req(
            "GenerateDataKey",
            json!({"KeyId": key_id, "KeySpec": "AES_9"}),
        ));
        assert_eq!(resp.status, 400);
        assert!(resp.body.contains("enum value set"), "{}", resp.body);

        let resp = handler.handle(make_req("GenerateDataKey", json!({"KeyId": key_id})));
        assert_eq!(resp.status, 400);
        assert!(resp.body.contains("either number of bytes or key spec"));

        let resp = handler.handle(make_req(
            "GenerateDataKey",
            json!({"KeyId": key_id, "KeySpec": "AES_256", "NumberOfBytes": 5}),
        ));
        assert_eq!(resp.status, 400);
        assert!(resp.body.contains("either number of bytes or key spec"));

        let resp = handler.handle(make_req(
            "GenerateDataKey",
            json!({"KeyId": key_id, "NumberOfBytes": 64}),
        ));
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        let pt = B64.decode(body["Plaintext"].as_str().unwrap()).unwrap();
        assert_eq!(pt.len(), 64);
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let handler = KmsHandler::new();
        let resp = handler.handle(make_req("CreateKey", json!({})));
        let key_id = serde_json::from_str::<Value>(&resp.body).unwrap()["KeyMetadata"]["KeyId"]
            .as_str()
            .unwrap()
            .to_string();

        let enc = handler.handle(make_req(
            "Encrypt",
            json!({"KeyId": key_id, "Plaintext": B64.encode(b"cipher me")}),
        ));
        let body: Value = serde_json::from_str(&enc.body).unwrap();
        let blob = body["CiphertextBlob"].as_str().unwrap().to_string();

        let dec = handler.handle(make_req("Decrypt", json!({"CiphertextBlob": blob})));
        let body: Value = serde_json::from_str(&dec.body).unwrap();
        assert_eq!(
            body["Plaintext"].as_str().unwrap(),
            B64.encode(b"cipher me")
        );
        // Decrypt reports the ARN of the key that encrypted the blob even
        // when no KeyId was requested.
        assert!(body["KeyId"].as_str().unwrap().contains(":key/"));
    }

    #[test]
    fn test_decrypt_roundtrip_binary_and_4k() {
        let handler = KmsHandler::new();
        let resp = handler.handle(make_req("CreateKey", json!({})));
        let key_id = serde_json::from_str::<Value>(&resp.body).unwrap()["KeyMetadata"]["KeyId"]
            .as_str()
            .unwrap()
            .to_string();

        for plaintext in [b"\x00\x01\x02\xff".as_slice(), &[b'x'; 4096][..]] {
            let enc = handler.handle(make_req(
                "Encrypt",
                json!({"KeyId": key_id, "Plaintext": B64.encode(plaintext)}),
            ));
            let body: Value = serde_json::from_str(&enc.body).unwrap();
            let blob = body["CiphertextBlob"].as_str().unwrap().to_string();

            // moto blob layout: 36-byte key id + 12-byte IV + 16-byte tag + ct
            let blob = B64.decode(&blob).unwrap();
            assert_eq!(blob.len(), 64 + plaintext.len());

            let dec = handler.handle(make_req(
                "Decrypt",
                json!({"CiphertextBlob": B64.encode(&blob)}),
            ));
            let body: Value = serde_json::from_str(&dec.body).unwrap();
            assert_eq!(dec.status, 200);
            assert_eq!(
                B64.decode(body["Plaintext"].as_str().unwrap()).unwrap(),
                plaintext
            );
        }
    }

    #[test]
    fn test_decrypt_garbage_blob_is_invalid_ciphertext() {
        let handler = KmsHandler::new();
        handler.handle(make_req("CreateKey", json!({})));

        for garbage in [
            "",
            &B64.encode(b"too short"),
            &B64.encode(b"\x00\x01\x02\x03"),
        ] {
            let resp = handler.handle(make_req("Decrypt", json!({"CiphertextBlob": garbage})));
            assert_eq!(resp.status, 400);
            assert!(resp.body.contains("InvalidCiphertextException"));
        }
    }

    #[test]
    fn test_decrypt_tampered_blob_is_invalid_ciphertext() {
        let handler = KmsHandler::new();
        let resp = handler.handle(make_req("CreateKey", json!({})));
        let key_id = serde_json::from_str::<Value>(&resp.body).unwrap()["KeyMetadata"]["KeyId"]
            .as_str()
            .unwrap()
            .to_string();

        let enc = handler.handle(make_req(
            "Encrypt",
            json!({"KeyId": key_id, "Plaintext": B64.encode(b"tamper me")}),
        ));
        let mut blob = B64
            .decode(
                serde_json::from_str::<Value>(&enc.body).unwrap()["CiphertextBlob"]
                    .as_str()
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
        assert!(blob.len() > 64);
        blob[64] ^= 0xff;

        let dec = handler.handle(make_req(
            "Decrypt",
            json!({"CiphertextBlob": B64.encode(&blob)}),
        ));
        assert_eq!(dec.status, 400);
        assert!(dec.body.contains("InvalidCiphertextException"));
    }

    #[test]
    fn test_decrypt_blob_of_unknown_key_is_access_denied() {
        // A structurally valid blob whose header key id is unknown to this
        // state: moto (mirroring real KMS) answers AccessDeniedException.
        let handler = KmsHandler::new();
        let mut blob = vec![0u8; 128];
        blob[..36].copy_from_slice(b"00000000-1111-2222-3333-444444444444");
        let resp = handler.handle(make_req(
            "Decrypt",
            json!({"CiphertextBlob": B64.encode(&blob)}),
        ));
        assert_eq!(resp.status, 400);
        assert!(resp.body.contains("AccessDeniedException"));
        assert!(resp
            .body
            .contains("The ciphertext refers to a customer master key that does not exist"));
    }

    #[test]
    fn test_decrypt_state_isolated_across_regions() {
        let handler = KmsHandler::new();
        let enc = handler.handle(make_req("CreateKey", json!({})));
        let key_id = serde_json::from_str::<Value>(&enc.body).unwrap()["KeyMetadata"]["KeyId"]
            .as_str()
            .unwrap()
            .to_string();
        let enc = handler.handle(make_req(
            "Encrypt",
            json!({"KeyId": key_id, "Plaintext": B64.encode(b"regional")}),
        ));
        let blob = serde_json::from_str::<Value>(&enc.body).unwrap()["CiphertextBlob"]
            .as_str()
            .unwrap()
            .to_string();

        // The same request aimed at a different region cannot see the key.
        let mut other = make_req("Decrypt", json!({"CiphertextBlob": blob}));
        other.region = "eu-west-1".to_string();
        let resp = handler.handle(other);
        assert_eq!(resp.status, 400);
        assert!(resp.body.contains("AccessDeniedException"));
    }

    #[test]
    fn test_encrypt_rejects_empty_and_oversize_plaintext() {
        let handler = KmsHandler::new();
        let resp = handler.handle(make_req("CreateKey", json!({})));
        let key_id = serde_json::from_str::<Value>(&resp.body).unwrap()["KeyMetadata"]["KeyId"]
            .as_str()
            .unwrap()
            .to_string();

        let resp = handler.handle(make_req(
            "Encrypt",
            json!({"KeyId": key_id, "Plaintext": ""}),
        ));
        assert_eq!(resp.status, 400);
        assert!(resp.body.contains("ValidationException"));

        let resp = handler.handle(make_req(
            "Encrypt",
            json!({"KeyId": key_id, "Plaintext": B64.encode(vec![0u8; 4097])}),
        ));
        assert_eq!(resp.status, 400);
        assert!(resp.body.contains("ValidationException"));

        // Exactly 4096 bytes is the accepted maximum (moto allows it).
        let resp = handler.handle(make_req(
            "Encrypt",
            json!({"KeyId": key_id, "Plaintext": B64.encode(vec![0u8; 4096])}),
        ));
        assert_eq!(resp.status, 200);
    }

    #[test]
    fn test_data_key_blob_decrypts_back() {
        let handler = KmsHandler::new();
        let resp = handler.handle(make_req("CreateKey", json!({})));
        let key_id = serde_json::from_str::<Value>(&resp.body).unwrap()["KeyMetadata"]["KeyId"]
            .as_str()
            .unwrap()
            .to_string();

        let dk = handler.handle(make_req(
            "GenerateDataKey",
            json!({"KeyId": key_id, "KeySpec": "AES_256"}),
        ));
        let body: Value = serde_json::from_str(&dk.body).unwrap();
        let blob = body["CiphertextBlob"].as_str().unwrap().to_string();
        let dk_plain = body["Plaintext"].as_str().unwrap().to_string();

        // moto can decrypt a data key ciphertext back to the raw data key.
        let dec = handler.handle(make_req("Decrypt", json!({"CiphertextBlob": blob})));
        let body: Value = serde_json::from_str(&dec.body).unwrap();
        assert_eq!(dec.status, 200);
        assert_eq!(body["Plaintext"].as_str().unwrap(), dk_plain);
    }

    /// Create a key and return `(key_id, arn)`.
    fn created_key(handler: &KmsHandler) -> (String, String) {
        let resp = handler.handle(make_req("CreateKey", json!({})));
        let meta = &serde_json::from_str::<Value>(&resp.body).unwrap()["KeyMetadata"];
        (
            meta["KeyId"].as_str().unwrap().to_string(),
            meta["Arn"].as_str().unwrap().to_string(),
        )
    }

    #[test]
    fn test_re_encrypt_rebinds_key_and_context() {
        let handler = KmsHandler::new();
        let (src_id, src_arn) = created_key(&handler);
        let (dst_id, dst_arn) = created_key(&handler);

        let enc = handler.handle(make_req(
            "Encrypt",
            json!({"KeyId": src_id, "Plaintext": B64.encode(b"reencrypt me")}),
        ));
        let blob = serde_json::from_str::<Value>(&enc.body).unwrap()["CiphertextBlob"]
            .as_str()
            .unwrap()
            .to_string();

        let re = handler.handle(make_req(
            "ReEncrypt",
            json!({
                "CiphertextBlob": blob.clone(),
                "DestinationKeyId": dst_id,
                "DestinationEncryptionContext": {"env": "dev"},
            }),
        ));
        assert_eq!(re.status, 200);
        let body: Value = serde_json::from_str(&re.body).unwrap();
        // moto response shape: KeyId = destination arn, SourceKeyId = source arn.
        assert_eq!(body["KeyId"].as_str().unwrap(), dst_arn);
        assert_eq!(body["SourceKeyId"].as_str().unwrap(), src_arn);

        let new_blob = body["CiphertextBlob"].as_str().unwrap().to_string();
        // moto re-encrypts: a fresh ciphertext of the recovered plaintext
        // under the destination key (the header names the destination key),
        // decryptable only with the bound destination context.
        assert_ne!(new_blob, blob);
        let new_ct = cipher::Ciphertext::parse(&B64.decode(&new_blob).unwrap()).unwrap();
        assert_eq!(new_ct.key_id, dst_id);

        let dec = handler.handle(make_req(
            "Decrypt",
            json!({"CiphertextBlob": new_blob, "EncryptionContext": {"env": "dev"}}),
        ));
        assert_eq!(dec.status, 200);
        let body: Value = serde_json::from_str(&dec.body).unwrap();
        assert_eq!(
            body["Plaintext"].as_str().unwrap(),
            B64.encode(b"reencrypt me")
        );
        assert_eq!(body["KeyId"].as_str().unwrap(), dst_arn);
    }

    #[test]
    fn test_re_encrypt_rejects_wrong_source_context() {
        let handler = KmsHandler::new();
        let (src_id, _) = created_key(&handler);
        let (dst_id, _) = created_key(&handler);

        let enc = handler.handle(make_req(
            "Encrypt",
            json!({"KeyId": src_id, "Plaintext": B64.encode(b"ctx"), "EncryptionContext": {"env": "dev"}}),
        ));
        let blob = serde_json::from_str::<Value>(&enc.body).unwrap()["CiphertextBlob"]
            .as_str()
            .unwrap()
            .to_string();

        // moto decrypts against SourceEncryptionContext first; any mismatch
        // raises InvalidCiphertextException (not a context-free success).
        let re = handler.handle(make_req(
            "ReEncrypt",
            json!({"CiphertextBlob": blob, "SourceEncryptionContext": {"env": "prod"}, "DestinationKeyId": dst_id}),
        ));
        assert_eq!(re.status, 400);
        assert!(re.body.contains("InvalidCiphertextException"));

        // An empty source context against a bound blob fails the same way.
        let re = handler.handle(make_req(
            "ReEncrypt",
            json!({"CiphertextBlob": blob, "DestinationKeyId": dst_id}),
        ));
        assert_eq!(re.status, 400);
        assert!(re.body.contains("InvalidCiphertextException"));
    }

    #[test]
    fn test_re_encrypt_garbage_blob_is_invalid_ciphertext() {
        let handler = KmsHandler::new();
        let (_, dst_arn) = created_key(&handler);
        let re = handler.handle(make_req(
            "ReEncrypt",
            json!({"CiphertextBlob": B64.encode(b"junk"), "DestinationKeyId": dst_arn}),
        ));
        assert_eq!(re.status, 400);
        assert!(re.body.contains("InvalidCiphertextException"));
    }

    #[test]
    fn test_alias_name_and_arn_resolution() {
        let handler = KmsHandler::new();
        let resp = handler.handle(make_req("CreateKey", json!({})));
        let key_id = serde_json::from_str::<Value>(&resp.body).unwrap()["KeyMetadata"]["KeyId"]
            .as_str()
            .unwrap()
            .to_string();

        handler.handle(make_req(
            "CreateAlias",
            json!({"AliasName": "alias/arn-resolve", "TargetKeyId": key_id.clone()}),
        ));

        // Plain alias names keep resolving through DescribeKey.
        let by_name = handler.handle(make_req(
            "DescribeKey",
            json!({"KeyId": "alias/arn-resolve"}),
        ));
        assert_eq!(by_name.status, 200);
        assert_eq!(
            serde_json::from_str::<Value>(&by_name.body).unwrap()["KeyMetadata"]["KeyId"],
            key_id
        );

        // The full alias ARN resolves the same way (moto get_alias_name).
        let arn = format!(
            "arn:aws:kms:{}:{}:alias/arn-resolve",
            "us-east-1", 123456789012u64
        );
        let by_arn = handler.handle(make_req("DescribeKey", json!({"KeyId": arn.clone()})));
        assert_eq!(by_arn.status, 200);
        assert_eq!(
            serde_json::from_str::<Value>(&by_arn.body).unwrap()["KeyMetadata"]["KeyId"],
            key_id
        );

        // Operations keyed by the alias ARN behave like the plain alias.
        let enc = handler.handle(make_req(
            "Encrypt",
            json!({"KeyId": arn.clone(), "Plaintext": B64.encode(b"via arn")}),
        ));
        assert_eq!(enc.status, 200);
        let blob = serde_json::from_str::<Value>(&enc.body).unwrap()["CiphertextBlob"]
            .as_str()
            .unwrap()
            .to_string();
        let dec = handler.handle(make_req("Decrypt", json!({"CiphertextBlob": blob})));
        assert_eq!(dec.status, 200);
        assert_eq!(
            serde_json::from_str::<Value>(&dec.body).unwrap()["Plaintext"],
            B64.encode(b"via arn")
        );

        let dk = handler.handle(make_req(
            "GenerateDataKey",
            json!({"KeyId": arn, "KeySpec": "AES_256"}),
        ));
        assert_eq!(dk.status, 200);
    }

    #[test]
    fn test_grants_can_be_retired() {
        let handler = KmsHandler::new();
        let resp = handler.handle(make_req("CreateKey", json!({})));
        let key_id = serde_json::from_str::<Value>(&resp.body).unwrap()["KeyMetadata"]["KeyId"]
            .as_str()
            .unwrap()
            .to_string();

        let grant = handler.handle(make_req(
            "CreateGrant",
            json!({
                "KeyId": key_id,
                "GranteePrincipal": "arn:aws:iam::123456789012:root",
                "Operations": ["Encrypt", "Decrypt"],
            }),
        ));
        let body: Value = serde_json::from_str(&grant.body).unwrap();
        assert!(body["GrantId"].is_string());
        let token = body["GrantToken"].as_str().unwrap().to_string();

        let list = handler.handle(make_req("ListGrants", json!({"KeyId": key_id.clone()})));
        let body: Value = serde_json::from_str(&list.body).unwrap();
        assert_eq!(body["Grants"].as_array().unwrap().len(), 1);

        handler.handle(make_req("RetireGrant", json!({"GrantToken": token})));
        let list = handler.handle(make_req("ListGrants", json!({"KeyId": key_id})));
        let body: Value = serde_json::from_str(&list.body).unwrap();
        assert_eq!(body["Grants"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_sign_verify_rsa() {
        let handler = KmsHandler::new();
        let resp = handler.handle(make_req(
            "CreateKey",
            json!({"KeySpec": "RSA_2048", "KeyUsage": "SIGN_VERIFY"}),
        ));
        let key_id = serde_json::from_str::<Value>(&resp.body).unwrap()["KeyMetadata"]["KeyId"]
            .as_str()
            .unwrap()
            .to_string();

        let msg = B64.encode(b"message to sign");
        let sign = handler.handle(make_req(
            "Sign",
            json!({
                "KeyId": key_id,
                "Message": msg,
                "MessageType": "RAW",
                "SigningAlgorithm": "RSASSA_PKCS1_V1_5_SHA_256",
            }),
        ));
        let body: Value = serde_json::from_str(&sign.body).unwrap();
        assert!(body["Signature"].is_string());
        let sig = body["Signature"].as_str().unwrap().to_string();

        let verify = handler.handle(make_req(
            "Verify",
            json!({
                "KeyId": key_id,
                "Message": msg,
                "MessageType": "RAW",
                "Signature": sig,
                "SigningAlgorithm": "RSASSA_PKCS1_V1_5_SHA_256",
            }),
        ));
        let vbody: Value = serde_json::from_str(&verify.body).unwrap();
        assert_eq!(vbody["SignatureValid"], true);
    }

    #[test]
    fn test_generate_mac_drift() {
        let handler = KmsHandler::new();
        let resp = handler.handle(make_req(
            "CreateKey",
            json!({"KeySpec": "HMAC_256", "KeyUsage": "GENERATE_VERIFY_MAC"}),
        ));
        let key_id = serde_json::from_str::<Value>(&resp.body).unwrap()["KeyMetadata"]["KeyId"]
            .as_str()
            .unwrap()
            .to_string();

        let msg = B64.encode(b"hello");
        let mac = handler.handle(make_req(
            "GenerateMac",
            json!({"KeyId": key_id, "Message": msg, "MacAlgorithm": "HMAC_SHA_256"}),
        ));
        let body: Value = serde_json::from_str(&mac.body).unwrap();
        assert!(body["Mac"].is_string());
        let mac_value = body["Mac"].as_str().unwrap().to_string();

        let verify = handler.handle(make_req(
            "VerifyMac",
            json!({
                "KeyId": key_id,
                "Message": msg,
                "MacAlgorithm": "HMAC_SHA_256",
                "Mac": mac_value,
            }),
        ));
        let vbody: Value = serde_json::from_str(&verify.body).unwrap();
        assert_eq!(vbody["MacValid"], true);
    }

    #[test]
    fn test_custom_key_store_lifecycle() {
        let handler = KmsHandler::new();
        let create = handler.handle(make_req(
            "CreateCustomKeyStore",
            json!({"CustomKeyStoreName": "test-cks"}),
        ));
        let body: Value = serde_json::from_str(&create.body).unwrap();
        assert!(body["CustomKeyStoreId"].is_string());
        let id = body["CustomKeyStoreId"].as_str().unwrap().to_string();

        let desc = handler.handle(make_req(
            "DescribeCustomKeyStores",
            json!({"CustomKeyStoreId": id.clone()}),
        ));
        let dbody: Value = serde_json::from_str(&desc.body).unwrap();
        let stores = dbody["CustomKeyStores"].as_array().unwrap();
        assert_eq!(stores.len(), 1);
        assert_eq!(stores[0]["CustomKeyStoreName"], "test-cks");

        let upd = handler.handle(make_req(
            "UpdateCustomKeyStore",
            json!({"CustomKeyStoreId": id.clone(), "NewCustomKeyStoreName": "renamed"}),
        ));
        assert_eq!(upd.status, 200);

        handler.handle(make_req(
            "DeleteCustomKeyStore",
            json!({"CustomKeyStoreId": id}),
        ));
        let desc2 = handler.handle(make_req(
            "DescribeCustomKeyStores",
            json!({"CustomKeyStoreId": id}),
        ));
        let d2: Value = serde_json::from_str(&desc2.body).unwrap();
        assert_eq!(d2["CustomKeyStores"].as_array().unwrap().len(), 0);

        let again = handler.handle(make_req(
            "DeleteCustomKeyStore",
            json!({"CustomKeyStoreId": id}),
        ));
        assert!(again.body.contains("CustomKeyStoreNotFoundException"));
    }

    #[test]
    fn test_import_flow() {
        let handler = KmsHandler::new();
        let create = handler.handle(make_req("CreateKey", json!({"Origin": "EXTERNAL"})));
        let key_id = serde_json::from_str::<Value>(&create.body).unwrap()["KeyMetadata"]["KeyId"]
            .as_str()
            .unwrap()
            .to_string();

        let pfi = handler.handle(make_req(
            "GetParametersForImport",
            json!({"KeyId": key_id, "WrappingAlgorithm": "RSAES_OAEP_SHA_256", "WrappingKeySpec": "RSA_2048"}),
        ));
        let body: Value = serde_json::from_str(&pfi.body).unwrap();
        assert!(body["PublicKey"].is_string());
        assert!(body["ImportToken"].is_string());

        // Wrapping public key must be a valid RSA SPKI DER structure.
        use rsa::pkcs8::DecodePublicKey as _;
        let pub_der = B64.decode(body["PublicKey"].as_str().unwrap()).unwrap();
        assert!(rsa::RsaPublicKey::from_public_key_der(&pub_der).is_ok());

        let imp = handler.handle(make_req(
            "ImportKeyMaterial",
            json!({
                "KeyId": key_id.clone(),
                "ImportToken": "tok",
                "EncryptedKeyMaterial": B64.encode(vec![1u8; 32]),
                "ExpirationModel": "KEY_MATERIAL_DOES_NOT_EXPIRE",
            }),
        ));
        assert_eq!(imp.status, 200);

        handler.handle(make_req(
            "DeleteImportedKeyMaterial",
            json!({"KeyId": key_id}),
        ));
        let desc = handler.handle(make_req("DescribeKey", json!({"KeyId": key_id})));
        assert!(desc.body.contains("PendingImport"));
    }
}
