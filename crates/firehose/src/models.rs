//! Firehose in-memory state models.
use parking_lot::RwLock;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// One delivery stream with its destination configuration.
#[derive(Clone, Debug)]
pub struct DeliveryStream {
    pub name: String,
    pub arn: String,
    pub status: String,
    pub stream_type: String,
    pub created: f64,
    pub version_id: u64,
    pub tags: BTreeMap<String, String>,
    pub s3_config: Map<String, Value>,
    pub encryption: Option<Value>,
}

/// The Firehose state store for one (account, region).
#[derive(Clone)]
pub struct FirehoseState {
    pub resources: Arc<RwLock<HashMap<String, DeliveryStream>>>,
}

impl FirehoseState {
    pub fn new() -> Self {
        Self {
            resources: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get(&self, name: &str) -> Option<DeliveryStream> {
        self.resources.read().get(name).cloned()
    }

    pub fn put(&self, stream: DeliveryStream) {
        self.resources.write().insert(stream.name.clone(), stream);
    }

    pub fn remove(&self, name: &str) -> Option<DeliveryStream> {
        self.resources.write().remove(name)
    }

    pub fn list_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.resources.read().keys().cloned().collect();
        names.sort();
        names
    }
}

impl Default for FirehoseState {
    fn default() -> Self {
        Self::new()
    }
}
