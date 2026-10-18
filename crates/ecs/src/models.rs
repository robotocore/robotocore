//! ECS in-memory state models.
//!
//! Shapes mirror `src/robotocore/services/ecs/provider.py` (the Python ground
//! truth) with resource maps nested per cluster where AWS scopes them so.
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Task definitions grouped by family, ordered by revision (index 0 = rev 1).
pub type TaskDefRevisions = Vec<serde_json::Value>;

#[derive(Clone)]
pub struct EcsState {
    pub clusters: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    /// cluster name -> service name -> service
    pub services: Arc<RwLock<HashMap<String, HashMap<String, serde_json::Value>>>>,
    /// cluster name -> task id -> task
    pub tasks: Arc<RwLock<HashMap<String, HashMap<String, serde_json::Value>>>>,
    /// cluster name -> container instance id -> container instance
    pub container_instances: Arc<RwLock<HashMap<String, HashMap<String, serde_json::Value>>>>,
    /// family -> revisions
    pub task_definitions: Arc<RwLock<HashMap<String, TaskDefRevisions>>>,
    /// service ARN -> task set id -> task set
    pub task_sets: Arc<RwLock<HashMap<String, HashMap<String, serde_json::Value>>>>,
    /// cluster name -> attributes
    pub attributes: Arc<RwLock<HashMap<String, Vec<serde_json::Value>>>>,
    /// resource ARN -> tags (JSON array of `{key, value}`)
    pub tags: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    pub account_settings: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    /// name -> capacity provider
    pub capacity_providers: Arc<RwLock<HashMap<String, serde_json::Value>>>,
}

impl EcsState {
    pub fn new() -> Self {
        Self {
            clusters: Arc::new(RwLock::new(HashMap::new())),
            services: Arc::new(RwLock::new(HashMap::new())),
            tasks: Arc::new(RwLock::new(HashMap::new())),
            container_instances: Arc::new(RwLock::new(HashMap::new())),
            task_definitions: Arc::new(RwLock::new(HashMap::new())),
            task_sets: Arc::new(RwLock::new(HashMap::new())),
            attributes: Arc::new(RwLock::new(HashMap::new())),
            tags: Arc::new(RwLock::new(HashMap::new())),
            account_settings: Arc::new(RwLock::new(HashMap::new())),
            capacity_providers: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for EcsState {
    fn default() -> Self {
        Self::new()
    }
}
