//! ECR in-memory state models.
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// A pushed image inside a repository.
#[derive(Clone, Debug)]
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

#[derive(Clone)]
pub struct EcrState {
    pub repositories: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    pub images: Arc<RwLock<HashMap<String, Vec<EcrImage>>>>,
    pub repository_policies: Arc<RwLock<HashMap<String, String>>>,
    pub lifecycle_policies: Arc<RwLock<HashMap<String, String>>>,
    pub tags: Arc<RwLock<HashMap<String, Vec<serde_json::Value>>>>,
    /// Per-repository scanning configuration
    /// (`{repositoryArn, repositoryName, scanOnPush, scanFrequency, appliedScanFilters}`).
    pub scanning_configs: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    pub registry_policy: Arc<RwLock<Option<String>>>,
    pub replication_configuration: Arc<RwLock<serde_json::Value>>,
    pub registry_scanning_configuration: Arc<RwLock<serde_json::Value>>,
    pub pull_through_cache_rules: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    pub repository_creation_templates: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    pub account_settings: Arc<RwLock<HashMap<String, String>>>,
}

impl EcrState {
    pub fn new() -> Self {
        Self {
            repositories: Arc::new(RwLock::new(HashMap::new())),
            images: Arc::new(RwLock::new(HashMap::new())),
            repository_policies: Arc::new(RwLock::new(HashMap::new())),
            lifecycle_policies: Arc::new(RwLock::new(HashMap::new())),
            tags: Arc::new(RwLock::new(HashMap::new())),
            scanning_configs: Arc::new(RwLock::new(HashMap::new())),
            registry_policy: Arc::new(RwLock::new(None)),
            replication_configuration: Arc::new(RwLock::new(
                serde_json::json!({"rules": []}),
            )),
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
    fn default() -> Self { Self::new() }
}
