//! Firehose in-memory state models.
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// One delivery stream with its destination configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeliveryStream {
    pub name: String,
    pub arn: String,
    pub status: String,
    pub stream_type: String,
    pub created: f64,
    pub version_id: u64,
    pub tags: BTreeMap<String, String>,
    pub s3_config: Map<String, Value>,
    /// Which botocore configuration member created the S3 destination:
    /// ExtendedS3DestinationConfiguration renders ExtendedS3Destination-
    /// Description, plain S3DestinationConfiguration the minimal
    /// S3DestinationDescription (distinct botocore members; moto keeps the
    /// two flavors apart, models.py:146-164).
    #[serde(default)]
    pub s3_config_is_extended: bool,
    pub encryption: Option<Value>,
}

/// The Firehose state store for one (account, region).
#[derive(Clone, Serialize, Deserialize)]
pub struct FirehoseState {
    #[serde(with = "crate::snapshot::arc_rw")]
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
