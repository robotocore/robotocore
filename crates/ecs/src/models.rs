//! ECS in-memory state models.
//!
//! Shapes mirror `src/robotocore/services/ecs/provider.py` (the Python ground
//! truth) with resource maps nested per cluster where AWS scopes them so.
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Task definitions grouped by family, ordered by revision (index 0 = rev 1).
pub type TaskDefRevisions = Vec<serde_json::Value>;

#[derive(Clone, Serialize, Deserialize)]
pub struct EcsState {
    #[serde(with = "crate::snapshot::arc_rw")]
    pub clusters: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    /// cluster name -> service name -> service
    #[serde(with = "crate::snapshot::arc_rw")]
    pub services: Arc<RwLock<HashMap<String, HashMap<String, serde_json::Value>>>>,
    /// cluster name -> task id -> task
    #[serde(with = "crate::snapshot::arc_rw")]
    pub tasks: Arc<RwLock<HashMap<String, HashMap<String, serde_json::Value>>>>,
    /// cluster name -> container instance id -> container instance
    #[serde(with = "crate::snapshot::arc_rw")]
    pub container_instances: Arc<RwLock<HashMap<String, HashMap<String, serde_json::Value>>>>,
    /// family -> revisions
    #[serde(with = "crate::snapshot::arc_rw")]
    pub task_definitions: Arc<RwLock<HashMap<String, TaskDefRevisions>>>,
    /// service ARN -> task set id -> task set
    #[serde(with = "crate::snapshot::arc_rw")]
    pub task_sets: Arc<RwLock<HashMap<String, HashMap<String, serde_json::Value>>>>,
    /// cluster name -> attributes
    #[serde(with = "crate::snapshot::arc_rw")]
    pub attributes: Arc<RwLock<HashMap<String, Vec<serde_json::Value>>>>,
    /// resource ARN -> tags (JSON array of `{key, value}`)
    #[serde(with = "crate::snapshot::arc_rw")]
    pub tags: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    #[serde(with = "crate::snapshot::arc_rw")]
    pub account_settings: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    /// name -> capacity provider
    #[serde(with = "crate::snapshot::arc_rw")]
    pub capacity_providers: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    /// task ARN -> protection enabled (UpdateTaskProtection/GetTaskProtection
    /// store: the flag lives with the task, real-AWS semantics — the old
    /// pair echoed the request and always answered false).
    #[serde(with = "crate::snapshot::arc_rw", default = "Default::default")]
    pub task_protection: Arc<RwLock<HashMap<String, serde_json::Value>>>,
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
            task_protection: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for EcsState {
    fn default() -> Self {
        Self::new()
    }
}
