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
    /// Resource policy JSON (PutResourcePolicy / GetResourcePolicy).
    #[serde(with = "crate::snapshot::rw")]
    pub resource_policy: RwLock<Option<String>>,
    /// Rotation state (RotateSecret / CancelRotateSecret / DescribeSecret).
    #[serde(with = "crate::snapshot::rw")]
    pub rotation_enabled: RwLock<bool>,
    #[serde(with = "crate::snapshot::rw")]
    pub rotation_lambda_arn: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub rotation_rules: RwLock<Option<Value>>,
    /// Replica region names registered via ReplicateSecretToRegions.
    #[serde(with = "crate::snapshot::rw")]
    pub replicas: RwLock<Vec<String>>,
}

impl Secret {
    pub fn new(name: String, account: u64, region: String) -> Self {
        let arn = format!(
            "arn:aws:secretsmanager:{}:{}:secret:{}",
            region, account, name
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
            resource_policy: RwLock::new(None),
            rotation_enabled: RwLock::new(false),
            rotation_lambda_arn: RwLock::new(None),
            rotation_rules: RwLock::new(None),
            replicas: RwLock::new(Vec::new()),
        }
    }

    pub fn get_version(&self, stage: &str) -> Option<SecretVersion> {
        self.versions
            .read()
            .values()
            .find(|v| v.stages.iter().any(|s| s == stage))
            .cloned()
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

impl Default for SmState {
    fn default() -> Self {
        Self::new()
    }
}
