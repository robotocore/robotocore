//! Kinesis in-memory state models (mirrors src/robotocore/services/kinesis/models.py).

use base64::Engine as _;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// Maximum hash key value for shard range partitioning (2^128 - 1).
pub const MAX_HASH_KEY: u128 = u128::MAX;

/// A stored Kinesis record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KinesisRecord {
    pub partition_key: String,
    #[serde(with = "crate::snapshot::b64")]
    pub data: Vec<u8>,
    /// Zero-padded 20-digit sequence number.
    pub sequence_number: String,
    pub timestamp: f64,
    pub shard_id: String,
    /// The record's `ExplicitHashKey` when the writer pinned it (moto's
    /// `KinesisRecord.explicit_hash_key`; carried on the record — merge-ups
    /// re-store it so a future reshard keeps the pin).
    #[serde(default)]
    pub explicit_hash_key: Option<String>,
}

impl KinesisRecord {
    pub fn to_output(&self) -> Value {
        serde_json::json!({
            "SequenceNumber": self.sequence_number,
            "ApproximateArrivalTimestamp": self.timestamp,
            "Data": base64::engine::general_purpose::STANDARD.encode(&self.data),
            "PartitionKey": self.partition_key,
        })
    }
}

/// Stream shard with record storage.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Shard {
    pub shard_id: String,
    #[serde(with = "crate::snapshot::u128_str")]
    pub hash_key_start: u128,
    #[serde(with = "crate::snapshot::u128_str")]
    pub hash_key_end: u128,
    pub starting_sequence_number: String,
    #[serde(with = "crate::snapshot::rw")]
    pub parent_id: RwLock<Option<String>>,
    /// The adjacency partner of a merged pair (moto's merged shard carries
    /// ParentShardId + AdjacentParentShardId).
    #[serde(with = "crate::snapshot::rw")]
    pub adjacent_parent_id: RwLock<Option<String>>,
    /// Set when a reshard closes this shard: the last sequence number ever
    /// assigned inside it. Open shards stay None.
    #[serde(with = "crate::snapshot::rw")]
    pub ending_sequence_number: RwLock<Option<u64>>,
    #[serde(with = "crate::snapshot::mutex_serde")]
    pub(crate) records: Mutex<Vec<KinesisRecord>>,
    #[serde(with = "crate::snapshot::mutex_u64")]
    pub(crate) sequence_counter: Mutex<u64>,
}

impl Shard {
    pub fn new(shard_id: String, hash_key_start: u128, hash_key_end: u128) -> Self {
        Self {
            shard_id,
            hash_key_start,
            hash_key_end,
            starting_sequence_number: "00000000000000000000".to_string(),
            parent_id: RwLock::new(None),
            adjacent_parent_id: RwLock::new(None),
            ending_sequence_number: RwLock::new(None),
            records: Mutex::new(Vec::new()),
            sequence_counter: Mutex::new(0),
        }
    }

    /// Closed by a reshard (moto keeps closed shards in the stream's shard
    /// list with an EndingSequenceNumber; consumers may still page their
    /// history until retention expiry).
    pub fn is_open(&self) -> bool {
        self.ending_sequence_number.read().is_none()
    }

    /// The last sequence number assigned inside this shard (its counter) —
    /// what a reshard's EndingSequenceNumber renders.
    pub fn last_sequence_number(&self) -> String {
        format!("{:020}", *self.sequence_counter.lock())
    }

    /// Store a record on this shard, returning the assigned sequence number.
    pub fn put_record(
        &self,
        shard_id: &str,
        partition_key: String,
        data: Vec<u8>,
        explicit_hash_key: Option<String>,
    ) -> KinesisRecord {
        let mut counter = self.sequence_counter.lock();
        *counter += 1;
        let seq = format!("{:020}", *counter);
        let record = KinesisRecord {
            partition_key,
            data,
            sequence_number: seq,
            timestamp: chrono::Utc::now().timestamp_millis() as f64 / 1000.0,
            shard_id: shard_id.to_string(),
            explicit_hash_key,
        };
        self.records.lock().push(record.clone());
        record
    }

    /// A copy of this shard's stored records — the re-fire set a reshard
    /// copies into its successors WITHOUT draining the shard (moto's
    /// merge/update copy; the closed parent keeps its records so the paging
    /// history contract stays real).
    pub(crate) fn records_snapshot(&self) -> Vec<KinesisRecord> {
        self.records.lock().clone()
    }

    /// Records at or after `start_sequence` plus the sequence to continue from
    /// (None when the shard has nothing at or after that sequence).
    pub fn get_records(
        &self,
        start_sequence: &str,
        limit: usize,
    ) -> (Vec<KinesisRecord>, Option<String>) {
        let start = parse_sequence(start_sequence);
        let records = self.records.lock();
        let start_idx = match records
            .iter()
            .position(|r| parse_sequence(&r.sequence_number) >= start)
        {
            Some(i) => i,
            None => return (Vec::new(), None),
        };
        let end = (start_idx + limit).min(records.len());
        if start_idx >= end {
            return (Vec::new(), None);
        }
        let batch = records[start_idx..end].to_vec();
        let next_seq = batch
            .last()
            .map(|r| parse_sequence(&r.sequence_number))
            .map(|seq| seq.saturating_add(1))
            .map(format_sequence);
        (batch, next_seq)
    }

    /// Sequence that would be assigned to the next record on this shard.
    pub fn get_latest_sequence(&self) -> String {
        let counter = self.sequence_counter.lock();
        format!("{:020}", *counter + 1)
    }
}

/// Zero-padded 20-digit sequence format.
pub fn format_sequence(seq: u128) -> String {
    format!("{:020}", seq)
}

/// Parse a (possibly huge) decimal sequence number; invalid input becomes 0.
pub fn parse_sequence(seq: &str) -> u128 {
    seq.parse::<u128>().unwrap_or(0)
}

/// A registered (enhanced fan-out) consumer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerInfo {
    pub name: String,
    pub arn: String,
    pub status: String,
    pub created: f64,
}

impl ConsumerInfo {
    pub fn to_value(&self) -> Value {
        serde_json::json!({
            "ConsumerName": self.name,
            "ConsumerARN": self.arn,
            "ConsumerStatus": self.status,
            "ConsumerCreationTimestamp": self.created,
        })
    }
}

/// A Kinesis stream.
#[derive(Debug, Serialize, Deserialize)]
pub struct KinesisStream {
    pub name: String,
    pub arn: String,
    pub status: String,
    pub shard_count: u32,
    #[serde(with = "crate::snapshot::rw")]
    pub retention_hours: RwLock<u32>,
    #[serde(with = "crate::snapshot::rw")]
    pub tags: RwLock<BTreeMap<String, String>>,
    pub created: f64,
    #[serde(with = "crate::snapshot::rw")]
    pub encryption_type: RwLock<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub key_id: RwLock<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub consumers: RwLock<BTreeMap<String, ConsumerInfo>>,
    #[serde(with = "crate::snapshot::rw")]
    pub shard_level_metrics: RwLock<Vec<String>>,
    #[serde(with = "crate::snapshot::rw")]
    pub stream_mode: RwLock<String>,
    #[serde(with = "crate::snapshot::rw")]
    pub shards: RwLock<Vec<Arc<Shard>>>,
}

impl KinesisStream {
    pub fn new(account: u64, region: &str, name: String, shard_count: u32) -> Self {
        let arn = format!("arn:aws:kinesis:{region}:{account}:stream/{name}");
        let shards: Vec<Arc<Shard>> = build_shards(shard_count)
            .into_iter()
            .map(Arc::new)
            .collect();
        Self {
            name,
            arn,
            status: "ACTIVE".to_string(),
            shard_count,
            retention_hours: RwLock::new(24),
            tags: RwLock::new(BTreeMap::new()),
            created: chrono::Utc::now().timestamp_millis() as f64 / 1000.0,
            encryption_type: RwLock::new("NONE".to_string()),
            key_id: RwLock::new(String::new()),
            consumers: RwLock::new(BTreeMap::new()),
            shard_level_metrics: RwLock::new(Vec::new()),
            stream_mode: RwLock::new("PROVISIONED".to_string()),
            shards: RwLock::new(shards),
        }
    }

    /// Hash a partition key onto the 128-bit space.
    pub fn hash_key(key: &str) -> u128 {
        // AWS uses MD5 of the partition key: the full 128-bit digest read as
        // one big-endian integer. Matches moto and the Python twin.
        let digest = md5::compute(key.as_bytes());
        u128::from_be_bytes(digest.into())
    }

    /// Select the shard covering a hash value (falls back to the first shard).
    fn select_shard(&self, hash: u128) -> Option<Arc<Shard>> {
        let shards = self.shards.read();
        shards
            .iter()
            // Only open shards accept new records: a reshard's closed
            // parents keep their (overlapping) ranges in the list for every
            // consumer of history, and a first-match on that range would
            // otherwise route new records into the closed shard.
            .find(|s| s.is_open() && s.hash_key_start <= hash && hash <= s.hash_key_end)
            .cloned()
            .or_else(|| shards.iter().find(|s| s.is_open()).cloned())
    }

    /// Write a record; picks the shard via explicit hash key or partition key.
    pub fn put_record(
        &self,
        partition_key: &str,
        data: Vec<u8>,
        explicit_hash_key: Option<&str>,
    ) -> Option<(Arc<Shard>, KinesisRecord)> {
        let hash = match explicit_hash_key.and_then(|k| k.parse::<u128>().ok()) {
            Some(h) => h,
            None => Self::hash_key(partition_key),
        };
        let shard = self.select_shard(hash)?;
        let record = shard.put_record(
            &shard.shard_id,
            partition_key.to_string(),
            data,
            explicit_hash_key.map(String::from),
        );
        Some((shard.clone(), record))
    }

    pub fn shard_by_id(&self, shard_id: &str) -> Option<Arc<Shard>> {
        self.shards
            .read()
            .iter()
            .find(|s| s.shard_id == shard_id)
            .cloned()
    }
}

/// Rebuild N evenly-spaced shards over the full hash key range.
pub fn build_shards(count: u32) -> Vec<Shard> {
    let count = count.max(1);
    (0..count)
        .map(|i| {
            let start = if i == 0 { 0u128 } else { shard_start(i, count) };
            let end = if i + 1 == count {
                MAX_HASH_KEY
            } else {
                shard_start(i + 1, count) - 1
            };
            Shard::new(format!("shardId-{i:012}"), start, end)
        })
        .collect()
}

/// Start of the hash range for shard `i` of `count` (i >= 1).
fn shard_start(i: u32, count: u32) -> u128 {
    // range_size = 2^128 // count, without a u256 intermediate:
    // 2^128 // count == u128::MAX // count + 1 only when count divides 2^128,
    // otherwise (q, r) split decides; equivalently use ceil-division:
    let q = MAX_HASH_KEY / u128::from(count);
    let r = MAX_HASH_KEY % u128::from(count);
    let range = if r + 1 == u128::from(count) { q + 1 } else { q };
    range * u128::from(i)
}

/// The Kinesis state store for one (account, region).
#[derive(Clone)]
pub struct KinesisState {
    pub streams: Arc<RwLock<BTreeMap<String, Arc<KinesisStream>>>>,
    pub resource_policies: Arc<Mutex<HashMap<String, String>>>,
}

impl KinesisState {
    pub fn new() -> Self {
        Self {
            streams: Arc::new(RwLock::new(BTreeMap::new())),
            resource_policies: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn get_stream(&self, name: &str) -> Option<Arc<KinesisStream>> {
        self.streams.read().get(name).cloned()
    }

    pub fn find_stream_by_arn(&self, arn: &str) -> Option<Arc<KinesisStream>> {
        self.streams.read().values().find(|s| s.arn == arn).cloned()
    }

    pub fn list_stream_names(&self) -> Vec<String> {
        self.streams.read().keys().cloned().collect()
    }
}

impl Default for KinesisState {
    fn default() -> Self {
        Self::new()
    }
}
