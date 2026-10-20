//! Stream capture for stream-enabled tables (moto parity).
//!
//! Mirrors moto's `Table.stream_shard.add(old, new)` hook: item mutations
//! append a wire-ready stream record shaped exactly like
//! `moto.dynamodb.models.table.StreamRecord.record`, and the
//! dynamodbstreams backend reads them back verbatim.

use crate::models::{StreamShard, Table};
use serde_json::{json, Map, Value};

/// moto's hardcoded shard id for stream-enabled tables.
pub const SHARD_ID: &str = "shardId-00000001541626099285-f35f62ef";
/// moto's fixed starting sequence number for a table's shard. The 22-digit
/// literal is kept verbatim (moto/dynamodb/models/table.py); it exceeds u64,
/// so sequence math carries it as a string parsed to u128 on demand.
pub const STARTING_SEQUENCE_NUMBER: &str = "1100000000017454423009";

/// Attach the shard and stream label (moto sets both once, when streams
/// are configured on the table).
/// Cheap predicate mirroring `record_mutation`'s gate so callers can skip
/// the serde deep-copies for non-stream tables entirely.
pub fn stream_enabled(table: &Table) -> bool {
    *table.stream_enabled.read()
}

pub fn enable_stream(table: &Table) {
    {
        let mut shard = table.stream_shard.write();
        if shard.is_none() {
            *shard = Some(StreamShard::new());
        }
    }
    let mut label = table.stream_label.write();
    if label.is_none() {
        // moto: naive `datetime.utcnow().isoformat()` — no UTC offset.
        *label = Some(
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6f")
                .to_string(),
        );
    }
}

/// Capture one item mutation on `table` (stream-enabled tables only).
///
/// `old`/`new` are the pre-/post-image attribute maps; exactly one may be
/// `None` (INSERT/REMOVE) or both present (MODIFY). Sequence numbers are
/// dense per shard: `starting_sequence_number + records.len()`, ascending
/// as moto's are.
pub fn record_mutation(
    table: &Table,
    old: Option<&Map<String, Value>>,
    new: Option<&Map<String, Value>>,
) {
    if !*table.stream_enabled.read() {
        return;
    }
    let shard_guard = table.stream_shard.read();
    let Some(shard) = shard_guard.as_ref() else {
        return;
    };

    let event_name = match (old.is_some(), new.is_some()) {
        (false, true) => "INSERT",
        (true, false) => "REMOVE",
        _ => "MODIFY",
    };

    // The record judged "current" carries the key attributes (moto takes
    // rec.hash_key / rec.range_key from whichever side produced it).
    let Some(rec_attrs) = old.or(new) else {
        return;
    };
    let mut keys = Map::new();
    for key in &table.key_schema {
        if let Some(v) = rec_attrs.get(&key.attribute_name) {
            keys.insert(key.attribute_name.clone(), v.clone());
        }
    }

    let view_type = table
        .stream_view_type
        .read()
        .clone()
        .unwrap_or_else(|| "NEW_AND_OLD_IMAGES".to_string());

    let sequence: u128 =
        shard.starting_sequence_number.parse().unwrap_or(0) + shard.records.read().len() as u128;
    let mut dynamodb = json!({
        "StreamViewType": view_type,
        "ApproximateCreationDateTime": chrono::Utc::now().timestamp_millis() as f64 / 1000.0,
        "SequenceNumber": sequence.to_string(),
        "SizeBytes": 1,
        "Keys": Value::Object(keys),
    });
    if matches!(view_type.as_str(), "NEW_IMAGE" | "NEW_AND_OLD_IMAGES") {
        if let Some(new_map) = new {
            dynamodb["NewImage"] = Value::Object(new_map.clone());
        }
    }
    if matches!(view_type.as_str(), "OLD_IMAGE" | "NEW_AND_OLD_IMAGES") {
        if let Some(old_map) = old {
            dynamodb["OldImage"] = Value::Object(old_map.clone());
        }
    }
    // moto sizes the record from the serialized `dynamodb` member.
    let size = serde_json::to_string(&dynamodb)
        .map(|s| s.len())
        .unwrap_or(0);
    dynamodb["SizeBytes"] = json!(size);

    let record = json!({
        "eventID": uuid::Uuid::new_v4().to_string().replace('-', ""),
        "eventName": event_name,
        "eventSource": "aws:dynamodb",
        "eventVersion": "1.0",
        // moto hardcodes the record region (`us-east-1`) regardless of the
        // table's actual region (moto/dynamodb/models/table.py StreamRecord).
        "awsRegion": "us-east-1",
        "dynamodb": dynamodb,
    });

    shard.records.write().push(record);
}
