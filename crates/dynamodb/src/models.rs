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
    #[serde(with = "crate::snapshot::rw")]
    pub billing_mode: RwLock<String>,
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
    /// moto `Table.deletion_protection_enabled` — set by CreateTable
    /// (default False) and mutable through UpdateTable; describe always
    /// renders it (moto/dynamodb/models/table.py describe).
    #[serde(with = "crate::snapshot::rw", default)]
    pub deletion_protection_enabled: RwLock<bool>,
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
            billing_mode: RwLock::new(billing_mode),
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
            deletion_protection_enabled: RwLock::new(false),
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

/// Real AWS's per-item size limit: 400 KB. moto's `LimitedSizeDict` guards
/// the same limit at a defensive 405,000 bytes (getmoto/moto#1874); we
/// enforce the documented 409,600-byte limit, sizing numbers with AWS's own
/// formula per getmoto/moto#10176.
pub const MAX_ITEM_SIZE: u64 = 400 * 1024;

/// Number of significant digits in a DynamoDB Number literal, per the AWS
/// item-size rules: digit characters only (the leading minus sign and the
/// decimal point are never counted), leading and trailing zeroes trimmed,
/// exponent digits excluded (they are not significant digits).
///
/// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html
fn number_significant_digits(n: &str) -> u64 {
    let digits: Vec<u8> = n
        .split(['e', 'E'])
        .next()
        .unwrap_or(n)
        .bytes()
        .filter(|b| b.is_ascii_digit())
        .collect();
    match digits.iter().position(|b| *b != b'0') {
        Some(first) => {
            let last = digits.iter().rposition(|b| *b != b'0').unwrap();
            (last - first + 1) as u64
        }
        None => 0, // "0", "-0", "0e+0", "0.00", or no digits at all
    }
}

/// Size of a Number value in bytes, per the AWS item-size docs:
/// "(1 byte per two significant digits) + (1 byte)", i.e.
/// `ceil(significant_digits / 2) + 1`. moto's `DynamoType.size()` uses
/// `len(str(value))` instead, which over-counts ("1786461547" is 10 bytes
/// there, 6 in AWS); getmoto/moto#10176 recommends implementing the AWS
/// formula, which is what this does.
pub fn number_value_size(n: &str) -> u64 {
    number_significant_digits(n).div_ceil(2) + 1
}

/// Size in bytes of a single (typed) AttributeValue, mirroring moto's
/// `DynamoType.size()` (except for Number values, see
/// `number_value_size`): strings are UTF-8 byte lengths, binary values raw
/// byte lengths, booleans/nulls 1 byte, lists sum their elements (index
/// names are not counted), maps count each nested attribute name, sets sum
/// element sizes without attribute names.
///
/// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html
pub fn attr_value_size(value: &AttrValue) -> u64 {
    let Some(fields) = value.as_object() else {
        return 0;
    };
    let Some((attr_type, val)) = fields.iter().next() else {
        return 0;
    };
    match attr_type.as_str() {
        "S" => val.as_str().map(str_size).unwrap_or(0),
        "N" => val.as_str().map(number_value_size).unwrap_or(0),
        "B" => val.as_str().map(binary_size).unwrap_or(0),
        "BOOL" | "NULL" => 1,
        "L" => val
            .as_array()
            .map(|a| a.iter().map(attr_value_size).sum())
            .unwrap_or(0),
        "M" => val
            .as_object()
            .map(|o| {
                o.iter()
                    .map(|(name, v)| str_size(name) + attr_value_size(v))
                    .sum()
            })
            .unwrap_or(0),
        "SS" => val
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str()).map(str_size).sum())
            .unwrap_or(0),
        "NS" => val
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str())
                    .map(number_value_size)
                    .sum()
            })
            .unwrap_or(0),
        "BS" => val
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str()).map(binary_size).sum())
            .unwrap_or(0),
        _ => 0,
    }
}

/// Size in bytes of a full item: for each attribute, the UTF-8 length of
/// the attribute name plus the value size. Checked against
/// `MAX_ITEM_SIZE` wherever an item is written, like moto's
/// `LimitedSizeDict` (getmoto/moto#1874).
pub fn item_size(attrs: &serde_json::Map<String, AttrValue>) -> u64 {
    attrs
        .iter()
        .map(|(name, value)| str_size(name) + attr_value_size(value))
        .sum()
}

/// Number of UTF-8-encoded bytes of a string attribute (AWS sizing).
fn str_size(s: &str) -> u64 {
    s.len() as u64
}

/// Size in bytes of a binary value: its raw length after base64 decoding.
fn binary_size(encoded: &str) -> u64 {
    use base64::Engine;
    match base64::engine::general_purpose::STANDARD.decode(encoded) {
        Ok(raw) => raw.len() as u64,
        // Not decodable wire data is stored verbatim; size the encoded form.
        Err(_) => encoded.len() as u64,
    }
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
    /// Per-index ProvisionedThroughput, absent for an on-demand index (LSI
    /// specs never carry one). Drives the GSI WarmThroughput value selection
    /// mirroring moto's `GlobalSecondaryIndex.__init__`: a provisioned GSI
    /// advertises its own units, an on-demand GSI moto's 12000/4000 defaults.
    #[serde(default)]
    pub provisioned_rcu: Option<i64>,
    #[serde(default)]
    pub provisioned_wcu: Option<i64>,
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

    // AWS item-size rules for Number values: ceil(digits / 2) + 1, with
    // leading and trailing zeroes trimmed and the sign, decimal point and
    // exponent not counting as digits (getmoto/moto#10176).
    #[test]
    fn item_size_number_bytes_aws_formula() {
        assert_eq!(number_value_size("0"), 1);
        assert_eq!(number_value_size("-0"), 1);
        assert_eq!(number_value_size(""), 1);
        assert_eq!(number_value_size("1"), 2);
        assert_eq!(number_value_size("42"), 2);
        assert_eq!(number_value_size("-42"), 2); // sign is not a digit
        assert_eq!(number_value_size("123"), 3);
        assert_eq!(number_value_size("1786461547"), 6); // ceil(10/2)+1
        assert_eq!(number_value_size("1.5"), 2);
        assert_eq!(number_value_size("300000.1"), 5); // 7 significant digits
    }

    #[test]
    fn item_size_number_bytes_trims_zeroes() {
        assert_eq!(number_significant_digits("0078000"), 2);
        assert_eq!(number_value_size("0078000"), 2); // "78" after trimming
        assert_eq!(number_value_size("0.5"), 2); // 1 digit
        assert_eq!(number_value_size("100000.000"), 2); // trimmed to 1 digit
        assert_eq!(number_value_size("-0.0"), 1);
        // Exponent digits are not significant digits.
        assert_eq!(number_value_size("1e30"), 2);
        assert_eq!(number_value_size("0e+0"), 1);
    }

    #[test]
    fn item_size_counts_names_and_values_like_moto() {
        // moto's Item accounting: name bytes + value size per attribute.
        let item = serde_json::from_str::<serde_json::Map<String, AttrValue>>(
            r#"{"pk": {"S": "abc"}, "N1": {"N": "123"}, "B": {"B": "aGVsbG8="}}"#,
        )
        .unwrap();
        // "pk"(2)+3 + "N1"(2)+3 + "B"(1)+5 = 16
        assert_eq!(item_size(&item), 16);
        // Lists sum element sizes without index names; maps count names.
        let nested: AttrValue =
            serde_json::from_str(r#"{"L": [{"N": "1786461547"}, {"M": {"inner": {"S": "xy"}}}]}"#)
                .unwrap();
        // number(6) + {"inner" name(5) + S value(2)}
        assert_eq!(attr_value_size(&nested), 13);
        let attr: AttrValue = serde_json::from_str(r#"{"M": {"inner": {"S": "xy"}}}"#).unwrap();
        assert_eq!(attr_value_size(&attr), 7); // "inner"(5) + 2
                                               // Bool and null cost one byte.
        assert_eq!(
            attr_value_size(&serde_json::from_str::<AttrValue>(r#"{"BOOL": true}"#).unwrap()),
            1
        );
        assert_eq!(
            attr_value_size(&serde_json::from_str::<AttrValue>(r#"{"NULL": true}"#).unwrap()),
            1
        );
    }
}
