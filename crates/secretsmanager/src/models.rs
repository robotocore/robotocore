//! Secrets Manager in-memory state models.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// A secret version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretVersion {
    pub version_id: String,
    pub secret_string: Option<String>,
    pub secret_binary: Option<String>,
    pub stages: Vec<String>,
    pub created_date: f64,
}

/// A secret.
#[derive(Debug, Serialize, Deserialize)]
pub struct Secret {
    pub name: String,
    pub arn: String,
    pub owner: u64,
    pub region: String,
    pub created_date: f64,
    #[serde(with = "crate::snapshot::rw")]
    pub versions: RwLock<HashMap<String, SecretVersion>>,
    #[serde(with = "crate::snapshot::rw")]
    pub tags: RwLock<Vec<Value>>,
    #[serde(with = "crate::snapshot::rw")]
    pub description: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub deletion_date: RwLock<Option<f64>>,
    /// Set when the secret is scheduled for deletion (recoverable).
    #[serde(with = "crate::snapshot::rw")]
    pub removed: RwLock<bool>,
    /// KMS key the secret is encrypted with (CreateSecret/UpdateSecret),
    /// mirrored in DescribeSecret unless it is the default AWS alias.
    #[serde(with = "crate::snapshot::rw", default)]
    pub kms_key_id: RwLock<Option<String>>,
    /// Resource policy JSON (PutResourcePolicy / GetResourcePolicy).
    #[serde(with = "crate::snapshot::rw")]
    pub resource_policy: RwLock<Option<String>>,
    /// Rotation state (RotateSecret / CancelRotateSecret / DescribeSecret).
    #[serde(with = "crate::snapshot::rw")]
    pub rotation_enabled: RwLock<bool>,
    /// moto gates rotation members behind `rotation_requested` — set once
    /// RotateSecret has been called, not merely when rotation is enabled.
    #[serde(with = "crate::snapshot::rw", default)]
    pub rotation_requested: RwLock<bool>,
    /// Schedule committed by a RotateSecret carrying AutomaticallyAfterDays
    /// (moto `secret.next_rotation_date`, epoch seconds).
    #[serde(with = "crate::snapshot::rw", default)]
    pub next_rotation_date: RwLock<Option<f64>>,
    #[serde(with = "crate::snapshot::rw")]
    pub rotation_lambda_arn: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub rotation_rules: RwLock<Option<Value>>,
    /// Replica region names registered via ReplicateSecretToRegions.
    #[serde(with = "crate::snapshot::rw")]
    pub replicas: RwLock<Vec<String>>,
}

/// 6 random letters for the ARN suffix (moto ids use `[A-Za-z]{6}`,
/// utils.py SecretsManagerSecretIdentifier.generate → get_random_string(6,
/// include_digits=False)).
pub fn random_arn_suffix() -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut seed =
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default() as u64 ^ 0x9E3779B97F4A7C15;
    (0..6)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            ALPHABET[(seed % ALPHABET.len() as u64) as usize] as char
        })
        .collect()
}

impl Secret {
    pub fn new(name: String, account: u64, region: String) -> Self {
        let arn = format!(
            "arn:aws:secretsmanager:{}:{}:secret:{}-{}",
            region,
            account,
            name,
            random_arn_suffix()
        );
        Self {
            name,
            arn,
            owner: account,
            region,
            created_date: chrono::Utc::now().timestamp() as f64,
            versions: RwLock::new(HashMap::new()),
            tags: RwLock::new(Vec::new()),
            description: RwLock::new(None),
            deletion_date: RwLock::new(None),
            removed: RwLock::new(false),
            kms_key_id: RwLock::new(None),
            resource_policy: RwLock::new(None),
            rotation_enabled: RwLock::new(false),
            rotation_requested: RwLock::new(false),
            next_rotation_date: RwLock::new(None),
            rotation_lambda_arn: RwLock::new(None),
            rotation_rules: RwLock::new(None),
            replicas: RwLock::new(Vec::new()),
        }
    }

    pub fn get_version(&self, stage: &str) -> Option<SecretVersion> {
        deterministic_by_stage(&self.versions.read(), stage)
    }

    /// Version looked up by version id.
    pub fn get_version_by_id(&self, version_id: &str) -> Option<SecretVersion> {
        self.versions.read().get(version_id).cloned()
    }
}

/// The Secrets Manager state store (per account+region).
#[derive(Clone)]
pub struct SmState {
    pub secrets: Arc<RwLock<HashMap<String, Arc<Secret>>>>,
}

impl SmState {
    pub fn new() -> Self {
        Self {
            secrets: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get_secret(&self, name: &str) -> Option<Arc<Secret>> {
        self.secrets.read().get(name).cloned()
    }

    pub fn put_secret(&self, secret: Arc<Secret>) {
        self.secrets.write().insert(secret.name.clone(), secret);
    }

    pub fn delete_secret(&self, name: &str) -> Option<Arc<Secret>> {
        self.secrets.write().remove(name)
    }

    pub fn list_secrets(&self) -> Vec<Arc<Secret>> {
        self.secrets
            .read()
            .values()
            .filter(|s| !*s.removed.read())
            .cloned()
            .collect()
    }
}

/// moto's `MAX_RESULTS_DEFAULT` for ListSecrets / BatchGetSecretValue.
pub const LIST_SECRETS_MAX_RESULTS_DEFAULT: usize = 100;

impl Default for SmState {
    fn default() -> Self {
        Self::new()
    }
}

/// Deterministic stage→version pick. moto resolves the first version
/// carrying a stage in dict-INSERTION order (models.py:457-460 iterates the
/// dict); a stacked stage is only reachable via an empty
/// `RemoveFromVersionId` on `update_secret_version_stage`
/// (models.py:1165-1172). To mirror the oracle's kind of determinism
/// (stable, oldest-first) the twin orders candidates by creation date and
/// version id — HashMap order is neither insertion-stable nor
/// reproducible.
pub(crate) fn deterministic_by_stage(
    versions: &std::collections::HashMap<String, SecretVersion>,
    stage: &str,
) -> Option<SecretVersion> {
    versions
        .values()
        .filter(|v| v.stages.iter().any(|s| s == stage))
        .min_by(|a, b| {
            a.created_date
                .partial_cmp(&b.created_date)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.version_id.cmp(&b.version_id))
        })
        .cloned()
}
