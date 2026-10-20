//! DynamoDB in-memory state models.

use chrono::Utc;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// An AttributeValue in DynamoDB format.
/// DynamoDB uses a specific JSON format for attribute values:
/// { "S": "string" }, { "N": "number" }, { "B": "base64" },
/// { "BOOL": true }, { "NULL": true }, { "M": {...} }, { "L": [...] },
/// { "SS": [...] }, { "NS": [...] }, { "BS": [...] }
pub type AttrValue = Value;

/// A single DynamoDB item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub attributes: HashMap<String, AttrValue>,
}

impl Item {
    pub fn new(attributes: HashMap<String, AttrValue>) -> Self {
        Self { attributes }
    }
}

/// A DynamoDB table definition.
#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub table_id: String,
    pub name: String,
    pub account: u64,
    pub region: String,
    #[serde(with = "crate::snapshot::rw")]
    pub status: RwLock<String>,
    pub created_at: u64,
    pub key_schema: Vec<KeySchema>,
    #[serde(with = "crate::snapshot::rw")]
    pub attribute_definitions: RwLock<Vec<AttributeDefinition>>,
    pub billing_mode: String,
    #[serde(with = "crate::snapshot::rw")]
    pub provisioned_rcu: RwLock<i64>,
    #[serde(with = "crate::snapshot::rw")]
    pub provisioned_wcu: RwLock<i64>,
    #[serde(with = "crate::snapshot::rw")]
    pub items: RwLock<HashMap<String, Arc<Item>>>,
    #[serde(with = "crate::snapshot::rw")]
    pub global_secondary_indexes: RwLock<Vec<IndexDefinition>>,
    #[serde(with = "crate::snapshot::rw")]
    pub local_secondary_indexes: RwLock<Vec<IndexDefinition>>,
    #[serde(with = "crate::snapshot::rw")]
    pub stream_enabled: RwLock<bool>,
    #[serde(with = "crate::snapshot::rw")]
    pub stream_view_type: RwLock<Option<String>>,
    /// Stream label (moto's `latest_stream_label`) set when the stream is
    /// enabled; drives the LatestStreamArn (`{table_arn}/stream/{label}`).
    #[serde(with = "crate::snapshot::rw")]
    pub stream_label: RwLock<Option<String>>,
    /// The single stream shard carrying captured mutation records.
    #[serde(with = "crate::snapshot::rw")]
    pub stream_shard: RwLock<Option<StreamShard>>,
    #[serde(with = "crate::snapshot::rw")]
    pub tags: RwLock<HashMap<String, String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub ttl_enabled: RwLock<bool>,
    #[serde(with = "crate::snapshot::rw")]
    pub ttl_attribute: RwLock<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub pitr_enabled: RwLock<bool>,
    #[serde(with = "crate::snapshot::rw")]
    pub policy: RwLock<Option<ResourcePolicy>>,
    #[serde(with = "crate::snapshot::rw")]
    pub kinesis_destinations: RwLock<Vec<KinesisDestination>>,
    #[serde(with = "crate::snapshot::rw")]
    pub contributor_insights: RwLock<Option<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub table_arn: RwLock<String>,
}

impl Table {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        account: u64,
        region: String,
        key_schema: Vec<KeySchema>,
        attribute_definitions: Vec<AttributeDefinition>,
        billing_mode: String,
    ) -> Self {
        let arn = format!("arn:aws:dynamodb:{}:{}:table/{}", region, account, name);
        Self {
            table_id: uuid::Uuid::new_v4().simple().to_string(),
            name,
            account,
            region,
            status: RwLock::new("ACTIVE".to_string()),
            created_at: Utc::now().timestamp() as u64,
            key_schema,
            attribute_definitions: RwLock::new(attribute_definitions),
            billing_mode: billing_mode.clone(),
            provisioned_rcu: RwLock::new(0),
            provisioned_wcu: RwLock::new(0),
            items: RwLock::new(HashMap::new()),
            global_secondary_indexes: RwLock::new(Vec::new()),
            local_secondary_indexes: RwLock::new(Vec::new()),
            stream_enabled: RwLock::new(false),
            stream_view_type: RwLock::new(None),
            stream_label: RwLock::new(None),
            stream_shard: RwLock::new(None),
            tags: RwLock::new(HashMap::new()),
            ttl_enabled: RwLock::new(false),
            ttl_attribute: RwLock::new(String::new()),
            pitr_enabled: RwLock::new(false),
            policy: RwLock::new(None),
            kinesis_destinations: RwLock::new(Vec::new()),
            contributor_insights: RwLock::new(None),
            table_arn: RwLock::new(arn),
        }
    }

    pub fn hash_key(&self) -> Option<&KeySchema> {
        self.key_schema.iter().find(|k| k.key_type == "HASH")
    }

    pub fn range_key(&self) -> Option<&KeySchema> {
        self.key_schema.iter().find(|k| k.key_type == "RANGE")
    }
}

/// A resource-based policy attached to a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourcePolicy {
    pub document: String,
    pub revision_id: String,
}

/// A Kinesis streaming destination registered on a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KinesisDestination {
    pub stream_arn: String,
    pub status: String,
    #[serde(default)]
    pub precision: Option<String>,
}

/// The single per-table stream shard (mirrors moto's `StreamShard`).
///
/// Records are stored as wire-ready JSON objects so the dynamodbstreams
/// backend can serve them verbatim, moto-style.
#[derive(Debug, Serialize, Deserialize)]
pub struct StreamShard {
    pub shard_id: String,
    /// Kept as moto's string literal: the 22-digit constant exceeds u64 and
    /// botocore requires SequenceNumber strings of at least 21 chars.
    pub starting_sequence_number: String,
    #[serde(with = "crate::snapshot::rw")]
    pub records: RwLock<Vec<Value>>,
}

impl StreamShard {
    pub fn new() -> Self {
        Self {
            shard_id: crate::streaming::SHARD_ID.to_string(),
            starting_sequence_number: crate::streaming::STARTING_SEQUENCE_NUMBER.to_string(),
            records: RwLock::new(Vec::new()),
        }
    }

    /// Moto's `StreamShard.to_json()` shape.
    pub fn to_json(&self) -> Value {
        json!({
            "ShardId": self.shard_id,
            "SequenceNumberRange": {
                "StartingSequenceNumber": self.starting_sequence_number.clone(),
            },
        })
    }
}

impl Default for StreamShard {
    fn default() -> Self {
        Self::new()
    }
}

/// A stored backup of a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Backup {
    pub arn: String,
    pub name: String,
    pub table_name: String,
    pub status: String,
    pub backup_type: String,
    pub created_at: String,
    pub size_bytes: i64,
    pub source_arn: String,
    pub source_created_at: String,
    pub source_item_count: i64,
    pub key_schema: Vec<KeySchema>,
    pub attribute_definitions: Vec<AttributeDefinition>,
    pub items: Vec<HashMap<String, AttrValue>>,
}

/// A table export record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Export {
    pub arn: String,
    pub table_arn: String,
    pub table_name: String,
    pub status: String,
    pub format: String,
    pub bucket: String,
    pub prefix: String,
}

/// A global table (multi-region replication) record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalTable {
    pub name: String,
    pub arn: String,
    pub replication_group: Vec<String>,
    pub status: String,
    pub created_at: String,
}

/// Compute the primary key string for an item.
pub fn compute_key_from_attrs(
    key_schema: &[KeySchema],
    attrs: &HashMap<String, AttrValue>,
) -> Option<String> {
    let mut parts = Vec::new();
    for key in key_schema {
        let val = attrs.get(&key.attribute_name)?;
        parts.push(format!("{}={}", key.attribute_name, val));
    }
    Some(parts.join(","))
}

/// A DynamoDB table index definition (GSI or LSI).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub name: String,
    pub key_schema: Vec<KeySchema>,
    pub projection: String,
    #[serde(default)]
    pub non_key_attributes: Vec<String>,
}

impl IndexDefinition {
    /// The index hash (partition) key attribute name.
    pub fn hash_attr(&self) -> Option<&str> {
        self.key_schema
            .iter()
            .find(|k| k.key_type == "HASH")
            .map(|k| k.attribute_name.as_str())
    }

    /// The index range (sort) key attribute name.
    pub fn range_attr(&self) -> Option<&str> {
        self.key_schema
            .iter()
            .find(|k| k.key_type == "RANGE")
            .map(|k| k.attribute_name.as_str())
    }
}

/// The DynamoDB state store (per account+region).
#[derive(Clone)]
pub struct DynamoDbState {
    pub tables: Arc<RwLock<HashMap<String, Arc<Table>>>>,
    pub backups: Arc<RwLock<Vec<Arc<Backup>>>>,
    pub exports: Arc<RwLock<Vec<Arc<Export>>>>,
    pub global_tables: Arc<RwLock<HashMap<String, Arc<GlobalTable>>>>,
}

impl DynamoDbState {
    pub fn new() -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            backups: Arc::new(RwLock::new(Vec::new())),
            exports: Arc::new(RwLock::new(Vec::new())),
            global_tables: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get_table(&self, name: &str) -> Option<Arc<Table>> {
        self.tables.read().get(name).cloned()
    }

    pub fn put_table(&self, table: Arc<Table>) {
        self.tables.write().insert(table.name.clone(), table);
    }

    pub fn delete_table(&self, name: &str) -> Option<Arc<Table>> {
        self.tables.write().remove(name)
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
    }
}

impl Default for DynamoDbState {
    fn default() -> Self {
        Self::new()
    }
}

/// Key schema element.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeySchema {
    pub attribute_name: String,
    pub key_type: String, // HASH or RANGE
}

/// Attribute definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeDefinition {
    pub attribute_name: String,
    pub attribute_type: String, // S, N, or B
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_clear() {
        let state = DynamoDbState::new();
        assert!(state.list_tables().is_empty());
    }
}
