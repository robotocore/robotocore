//! ECR in-memory state models.
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// An in-progress image-layer upload (moto `LayerUpload`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EcrLayerUpload {
    pub upload_id: String,
    pub repository_name: String,
    pub registry_id: String,
    /// Bytes received so far, in arrival order; moto keeps a `bytearray`
    /// and computes the layer digest over the concatenation.
    #[serde(with = "crate::snapshot::b64")]
    pub layer_parts: Vec<u8>,
}

/// A pushed image inside a repository.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EcrImage {
    pub digest: String,
    pub tags: Vec<String>,
    pub manifest: String,
    pub manifest_media_type: String,
    pub size_in_bytes: Option<i64>,
    /// Epoch seconds; published as `imagePushedAt`.
    pub pushed_at: i64,
    /// Epoch seconds of the last scan start, used for the once-per-day limit.
    pub scanned_at: Option<i64>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EcrState {
    #[serde(with = "crate::snapshot::arc_rw")]
    pub repositories: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub images: Arc<RwLock<HashMap<String, Vec<EcrImage>>>>,
    /// In-progress layer uploads per repository, keyed by uploadId
    /// (moto `Repository.uploads`).
    #[serde(with = "crate::snapshot::arc_rw")]
    pub uploads: Arc<RwLock<HashMap<String, HashMap<String, EcrLayerUpload>>>>,
    /// Completed layer sizes per repository, keyed by digest
    /// (moto `Repository.layers`: digest -> size in bytes).
    #[serde(with = "crate::snapshot::arc_rw")]
    pub layers: Arc<RwLock<HashMap<String, HashMap<String, i64>>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub repository_policies: Arc<RwLock<HashMap<String, String>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub lifecycle_policies: Arc<RwLock<HashMap<String, String>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub tags: Arc<RwLock<HashMap<String, Vec<serde_json::Value>>>>,
    /// Per-repository scanning configuration
    /// (`{repositoryArn, repositoryName, scanOnPush, scanFrequency, appliedScanFilters}`).
    #[serde(with = "crate::snapshot::arc_rw")]
    pub scanning_configs: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub registry_policy: Arc<RwLock<Option<String>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub replication_configuration: Arc<RwLock<serde_json::Value>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub registry_scanning_configuration: Arc<RwLock<serde_json::Value>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub pull_through_cache_rules: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub repository_creation_templates: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub account_settings: Arc<RwLock<HashMap<String, String>>>,
}

impl EcrState {
    pub fn new() -> Self {
        Self {
            repositories: Arc::new(RwLock::new(HashMap::new())),
            images: Arc::new(RwLock::new(HashMap::new())),
            uploads: Arc::new(RwLock::new(HashMap::new())),
            layers: Arc::new(RwLock::new(HashMap::new())),
            repository_policies: Arc::new(RwLock::new(HashMap::new())),
            lifecycle_policies: Arc::new(RwLock::new(HashMap::new())),
            tags: Arc::new(RwLock::new(HashMap::new())),
            scanning_configs: Arc::new(RwLock::new(HashMap::new())),
            registry_policy: Arc::new(RwLock::new(None)),
            replication_configuration: Arc::new(RwLock::new(serde_json::json!({"rules": []}))),
            registry_scanning_configuration: Arc::new(RwLock::new(
                serde_json::json!({"scanType": "BASIC", "rules": []}),
            )),
            pull_through_cache_rules: Arc::new(RwLock::new(HashMap::new())),
            repository_creation_templates: Arc::new(RwLock::new(HashMap::new())),
            account_settings: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for EcrState {
    fn default() -> Self {
        Self::new()
    }
}
