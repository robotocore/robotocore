//! Event source mapping engine: polls SQS / Kinesis / DynamoDB streams for
//! Lambda ESMs and invokes the mapped functions in this process.
//!
//! Mirrors the moto/recorded semantics for the observable behavior:
//! - SQS: a poll receives up to BatchSize messages, invokes the function once
//!   per batch, deletes on success and makes messages visible on failure
//!   (moto's add_message hook). `ReportBatchItemFailures` mappings delete
//!   only the messages the function did NOT report.
//! - Filters (`FilterCriteria`) drop non-matching records: they are consumed
//!   from the source without invoking (recorded suite behavior).
//! - Kinesis: per-shard GetShardIterator/GetRecords polling with continuation
//!   iterators kept in engine memory; delivery advances only on success.
//! - DynamoDB streams: reads the dynamodb crate's stream capture directly and
//!   tracks the last delivered SequenceNumber per mapping.
//!
//! The engine runs on a dedicated thread started by the server binary; the
//! service crates stay decoupled (they only expose operation handlers and
//! accessor hooks).

use crate::core::server::NativeHandlers;
use bytes::Bytes;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// Poll cadence. The trigger suites sleep 5-15s and need sub-second delivery;
/// 300ms is fast enough and cheap on an idle process.
const POLL_INTERVAL_MS: u64 = 300;

pub fn start(native: Arc<NativeHandlers>) {
    std::thread::Builder::new()
        .name("esm-engine".into())
        .spawn(move || run(native))
        .ok();
}

fn run(native: Arc<NativeHandlers>) {
    let mut engine = Engine::default();
    loop {
        let snapshot = native.lambda.esm_snapshot();
        for entry in snapshot {
            let mapping = &entry.mapping;
            if !mapping.enabled || mapping.state != "Enabled" {
                continue;
            }
            match classify_arn(&mapping.event_source_arn) {
                Some(Source::Sqs { queue_arn }) => poll_sqs(&native, &entry, &queue_arn),
                Some(Source::Kinesis { stream_name }) => {
                    engine.poll_kinesis(&native, &entry, &stream_name)
                }
                Some(Source::DynamoDb { table_name }) => {
                    engine.poll_ddb_stream(&native, &entry, &table_name)
                }
                None => continue,
            }
        }
        std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
    }
}

enum Source {
    Sqs { queue_arn: String },
    Kinesis { stream_name: String },
    DynamoDb { table_name: String },
}

/// ARN → source descriptor (moto accepts exactly these three families).
fn classify_arn(arn: &str) -> Option<Source> {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() < 6 || parts[0] != "arn" {
        return None;
    }
    match parts[2] {
        "sqs" => Some(Source::Sqs {
            queue_arn: arn.to_string(),
        }),
        "kinesis" => Some(Source::Kinesis {
            stream_name: parts[5].strip_prefix("stream/")?.to_string(),
        }),
        "dynamodb" => {
            // arn:aws:dynamodb:{region}:{account}:table/{name}/stream/{label}
            let rest = parts[5].strip_prefix("table/")?;
            let table_name = rest.split("/stream/").next()?;
            Some(Source::DynamoDb {
                table_name: table_name.to_string(),
            })
        }
        _ => None,
    }
}

/// Parse `arn:aws:{svc}:{region}:{account}:{…}` into (account, region).
fn arn_scope(arn: &str) -> Option<(u64, String)> {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() < 6 {
        return None;
    }
    Some((parts[4].parse().ok()?, parts[3].to_string()))
}

/// The dynamodb SequenceNumber as a number for ordering (22-digit decimals
/// exceed u64 but fit u128; unparseable sorts lowest).
fn seq_num(s: &str) -> u128 {
    s.parse().unwrap_or(0)
}

fn vec_of_shards(arr: &[Value]) -> Vec<String> {
    arr.iter()
        .filter_map(|s| s["ShardId"].as_str().map(String::from))
        .collect()
}

fn seq_of(r: &Value) -> Option<String> {
    r.get("dynamodb")
        .and_then(|d| d.get("SequenceNumber"))
        .and_then(Value::as_str)
        .map(String::from)
}

/// Engine cursor state (in-memory; restart re-polls from the map's starting
/// position, like moto's backends which lose nothing durable).
#[derive(Default)]
struct Engine {
    /// Continuation iterators keyed by (account, region, esm-uuid, shard id).
    kinesis_iterators: HashMap<(u64, String, String, String), String>,
    /// Last delivered SequenceNumber per (account, region, esm-uuid).
    ddb_last_seq: HashMap<(u64, String, String), String>,
}

impl Engine {
    fn starting_position(mapping: &lambda::models::EventSourceMapping) -> String {
        mapping
            .starting_position
            .clone()
            .unwrap_or_else(|| "TRIM_HORIZON".to_string())
    }

    // -- Kinesis ------------------------------------------------------------

    fn poll_kinesis(
        &mut self,
        native: &Arc<NativeHandlers>,
        entry: &lambda::models::EsmMappingEntry,
        stream_name: &str,
    ) {
        let mapping = &entry.mapping;
        let describe = native.kinesis.handle(kinesis::AwsRequest {
            service: "kinesis".into(),
            operation: "DescribeStream".into(),
            account: entry.account,
            region: entry.region.clone(),
            params: json!({"StreamName": stream_name}),
            body: Bytes::new(),
        });
        if describe.status != 200 {
            return;
        }
        let Ok(body) = serde_json::from_str::<Value>(&describe.body) else {
            return;
        };
        let shards: Vec<String> = body["StreamDescription"]["Shards"]
            .as_array()
            .map(|arr| vec_of_shards(arr))
            .unwrap_or_default();
        for shard in shards {
            let key = (
                entry.account,
                entry.region.clone(),
                mapping.uuid.clone(),
                shard.clone(),
            );
            let iterator = match self.kinesis_iterators.get(&key) {
                Some(it) => it.clone(),
                None => {
                    let resp = native.kinesis.handle(kinesis::AwsRequest {
                        service: "kinesis".into(),
                        operation: "GetShardIterator".into(),
                        account: entry.account,
                        region: entry.region.clone(),
                        params: json!({
                            "StreamName": stream_name,
                            "ShardId": shard,
                            "ShardIteratorType": Self::starting_position(mapping),
                        }),
                        body: Bytes::new(),
                    });
                    if resp.status != 200 {
                        continue;
                    }
                    let Ok(body) = serde_json::from_str::<Value>(&resp.body) else {
                        continue;
                    };
                    let Some(iterator) = body["ShardIterator"].as_str().map(String::from) else {
                        continue;
                    };
                    iterator
                }
            };

            let records_resp = native.kinesis.handle(kinesis::AwsRequest {
                service: "kinesis".into(),
                operation: "GetRecords".into(),
                account: entry.account,
                region: entry.region.clone(),
                params: json!({"ShardIterator": iterator}),
                body: Bytes::new(),
            });
            if records_resp.status != 200 {
                // Iterator likely expired; drop the cursor so the next poll
                // re-creates it from the mapping's starting position.
                self.kinesis_iterators.remove(&key);
                continue;
            }
            let Ok(body) = serde_json::from_str::<Value>(&records_resp.body) else {
                continue;
            };
            let records = body["Records"].as_array().cloned().unwrap_or_default();
            let next = body["NextShardIterator"].as_str().map(String::from);
            if records.is_empty() {
                match next {
                    Some(next) => {
                        self.kinesis_iterators.insert(key, next);
                    }
                    None => {
                        // Shard exhausted; recreate from start-position tomorrow.
                        self.kinesis_iterators.remove(&key);
                    }
                }
                continue;
            }

            // Filter, build the event, invoke — the delivered cursor advances
            // only when the invocation succeeded (retry on failure).
            let delivered: Vec<Value> = records
                .iter()
                .filter(|r| filter_matches(mapping.filter_criteria.as_ref(), r))
                .map(|r| {
                    json!({
                        "kinesis": {
                            "kinesisSchemaVersion": "1.0",
                            "partitionKey": r.get("PartitionKey"),
                            "sequenceNumber": r.get("SequenceNumber"),
                            "data": r.get("Data"),
                            "approximateArrivalTimestamp": r.get("ApproximateArrivalTimestamp"),
                        },
                        "eventSource": "aws:kinesis",
                        "eventVersion": "1.0",
                        "eventID": format!(
                            "{}:{}",
                            shard,
                            r.get("SequenceNumber").cloned().unwrap_or_default()
                        ),
                        "eventName": "aws:kinesis:record",
                        "invokeIdentityArn": mapping.function_arn,
                        "awsRegion": entry.region,
                        "eventSourceARN": mapping.event_source_arn,
                    })
                })
                .collect();
            if delivered.is_empty() {
                if let Some(next) = next {
                    self.kinesis_iterators.insert(key, next);
                }
                continue;
            }
            let result = native.lambda.invoke_engine_event(
                entry.account,
                &entry.region,
                &mapping.function_arn,
                &json!({"Records": delivered}),
            );
            if result.function_error.is_none() {
                match next {
                    Some(next) => {
                        self.kinesis_iterators.insert(key, next);
                    }
                    None => {
                        self.kinesis_iterators.remove(&key);
                    }
                }
            }
            // On failure: keep the old iterator so the next poll re-delivers.
        }
    }

    // -- DynamoDB streams ----------------------------------------------------

    fn poll_ddb_stream(
        &mut self,
        native: &Arc<NativeHandlers>,
        entry: &lambda::models::EsmMappingEntry,
        table_name: &str,
    ) {
        let mapping = &entry.mapping;
        let key = (entry.account, entry.region.clone(), mapping.uuid.clone());
        let last = self.ddb_last_seq.get(&key).cloned();
        let Some(records) = native.dynamodb.stream_records_after(
            entry.account,
            &entry.region,
            table_name,
            last.clone(),
        ) else {
            return;
        };
        if records.is_empty() {
            return;
        }
        let matching = self.build_ddb_event(mapping, entry, &records);
        // Filters may have dropped every record; the cursor still advances.
        let all_delivered_seq = records.iter().filter_map(seq_of).max_by_key(|s| seq_num(s));
        if matching.is_empty() {
            if let Some(seq) = all_delivered_seq {
                self.ddb_last_seq.insert(key, seq);
            }
            return;
        }
        let result = native.lambda.invoke_engine_event(
            entry.account,
            &entry.region,
            &mapping.function_arn,
            &json!({"Records": matching}),
        );
        if result.function_error.is_none() {
            if let Some(seq) = all_delivered_seq {
                self.ddb_last_seq.insert(key, seq);
            }
        }
    }

    fn build_ddb_event(
        &self,
        mapping: &lambda::models::EventSourceMapping,
        entry: &lambda::models::EsmMappingEntry,
        records: &[Value],
    ) -> Vec<Value> {
        records
            .iter()
            .filter(|r| filter_matches(mapping.filter_criteria.as_ref(), r))
            .map(|r| {
                json!({
                    "eventID": r.get("eventID"),
                    "eventName": r.get("eventName"),
                    "eventVersion": r.get("eventVersion"),
                    "eventSource": r.get("eventSource"),
                    "awsRegion": entry.region,
                    "dynamodb": r.get("dynamodb"),
                    "eventSourceARN": mapping.event_source_arn,
                })
            })
            .collect()
    }
}

// -- SQS -----------------------------------------------------------------------

/// SQS mappings poll on every cycle (recorded suites poll steadily; moto
/// delivers synchronously per send_message, a timer gives the same visible
/// ordering without cross-crate shipping plumbing).
fn poll_sqs(
    native: &Arc<NativeHandlers>,
    entry: &lambda::models::EsmMappingEntry,
    queue_arn: &str,
) {
    let Some((account, region)) = arn_scope(queue_arn) else {
        return;
    };
    let mapping = &entry.mapping;
    let Some(queue_name) = queue_arn.rsplit(':').next().map(String::from) else {
        return;
    };
    // The SQS handler derives the queue from the URL's last segment.
    let queue_url = format!("http://localhost:4566/{account}/{queue_name}");
    let resp = sqs::SqsHandler::handle(
        &*native.sqs,
        sqs::AwsRequest {
            service: "sqs".into(),
            operation: "ReceiveMessage".into(),
            account,
            region: region.clone(),
            params: json!({
                "QueueUrl": queue_url,
                "MaxNumberOfMessages": mapping.batch_size,
                "WaitTimeSeconds": 0,
            }),
            body: Bytes::new(),
        },
    );
    if resp.status != 200 {
        return;
    }
    let Ok(body) = serde_json::from_str::<Value>(&resp.body) else {
        return;
    };
    let messages: Vec<Value> = body["Messages"].as_array().cloned().unwrap_or_default();
    if messages.is_empty() {
        return;
    }

    // Split into matching and non-matching records per FilterCriteria.
    let mut matched: Vec<&Value> = Vec::new();
    for msg in &messages {
        if filter_matches(mapping.filter_criteria.as_ref(), &sqs_filter_doc(msg)) {
            matched.push(msg);
        } else {
            // Filtered-out messages are consumed without invocation.
            delete_sqs_message(native, account, &region, &queue_url, msg);
        }
    }
    if matched.is_empty() {
        return;
    }

    let event = json!({
        "Records": matched
            .iter()
            .map(|msg| sqs_record(mapping, msg, queue_arn, &region))
            .collect::<Vec<Value>>(),
    });
    let result = native.lambda.invoke_engine_event(
        entry.account,
        &entry.region,
        &mapping.function_arn,
        &event,
    );

    let succeeded = result.function_error.is_none();
    let reports_failures = mapping
        .function_response_types
        .as_ref()
        .is_some_and(|types| types.iter().any(|t| t == "ReportBatchItemFailures"));
    let mut failures: HashSet<String> = HashSet::new();
    if reports_failures && succeeded {
        if let Ok(parsed) = serde_json::from_str::<Value>(&result.payload) {
            if let Some(items) = parsed["batchItemFailures"].as_array() {
                for item in items {
                    if let Some(id) = item["itemIdentifier"].as_str() {
                        failures.insert(id.to_string());
                    }
                }
            }
        }
    }

    if !succeeded && mapping.bisect_batch_on_function_error && matched.len() > 1 {
        // BisectBatchOnFunctionError: split the failed batch in half and
        // retry each half once; a half that succeeds deletes its messages,
        // a failed half returns to the queue at visibility 0 so further
        // bisection can continue on the next poll.
        let mid = matched.len() / 2;
        for (start, end) in [(0, mid), (mid, matched.len())] {
            let half = &matched[start..end];
            let half_event = json!({
                "Records": half
                    .iter()
                    .map(|msg| sqs_record(mapping, msg, queue_arn, &region))
                    .collect::<Vec<Value>>(),
            });
            let half_result = native.lambda.invoke_engine_event(
                entry.account,
                &entry.region,
                &mapping.function_arn,
                &half_event,
            );
            for msg in half {
                if half_result.function_error.is_none() {
                    delete_sqs_message(native, account, &region, &queue_url, msg);
                } else {
                    change_sqs_visibility(
                        native,
                        account,
                        &region,
                        &queue_url,
                        msg["ReceiptHandle"].as_str().unwrap_or_default(),
                        0,
                    );
                }
            }
        }
        return;
    }

    for msg in &matched {
        let message_id = msg["MessageId"].as_str().unwrap_or_default();
        let keep = !succeeded || (reports_failures && failures.contains(message_id));
        if keep {
            // Failed messages come back immediately (moto: visibility 0).
            change_sqs_visibility(
                native,
                account,
                &region,
                &queue_url,
                msg["ReceiptHandle"].as_str().unwrap_or_default(),
                0,
            );
        } else {
            delete_sqs_message(native, account, &region, &queue_url, msg);
        }
    }
}

/// The filter document AWS builds for an SQS message: JSON body (when it
/// parses) plus message metadata. Unparseable bodies stay a raw string.
fn sqs_filter_doc(msg: &Value) -> Value {
    let body = msg["Body"].as_str().unwrap_or_default();
    let parsed = serde_json::from_str::<Value>(body).unwrap_or(Value::String(body.to_string()));
    json!({
        "body": parsed,
        "messageId": msg["MessageId"],
        "md5OfBody": msg["MD5OfBody"],
    })
}

/// One SQS record in a lambda event (moto's `_send_sqs_message` record).
fn sqs_record(
    _mapping: &lambda::models::EventSourceMapping,
    msg: &Value,
    queue_arn: &str,
    region: &str,
) -> Value {
    let mut attributes = json!({});
    if let Some(attrs) = msg["Attributes"].as_object() {
        attributes = Value::Object(attrs.clone());
    }
    if let Some(obj) = attributes.as_object_mut() {
        // ApproximateReceiveCount is emitted on delivery; default to 1 like
        // moto's hardcoded record when absent.
        obj.entry("ApproximateReceiveCount")
            .or_insert_with(|| json!("1"));
    }
    let mut record = json!({
        "messageId": msg["MessageId"],
        "receiptHandle": msg["ReceiptHandle"],
        "body": msg["Body"],
        "attributes": attributes,
        "messageAttributes": {},
        "md5OfBody": msg["MD5OfBody"],
        "eventSource": "aws:sqs",
        "eventSourceARN": queue_arn,
        "awsRegion": region,
    });
    if queue_arn.ends_with(".fifo") {
        if let Some(attrs) = msg["Attributes"].as_object() {
            if let Some(obj) = record["attributes"].as_object_mut() {
                for key in ["MessageGroupId", "MessageDeduplicationId"] {
                    if let Some(v) = attrs.get(key) {
                        obj.insert(key.to_string(), v.clone());
                    }
                }
            }
        }
    }
    record
}

fn delete_sqs_message(
    native: &Arc<NativeHandlers>,
    account: u64,
    region: &str,
    queue_url: &str,
    msg: &Value,
) {
    let Some(handle) = msg["ReceiptHandle"].as_str() else {
        return;
    };
    let _ = sqs::SqsHandler::handle(
        &*native.sqs,
        sqs::AwsRequest {
            service: "sqs".into(),
            operation: "DeleteMessage".into(),
            account,
            region: region.to_string(),
            params: json!({
                "QueueUrl": queue_url,
                "ReceiptHandle": handle,
            }),
            body: Bytes::new(),
        },
    );
}

fn change_sqs_visibility(
    native: &Arc<NativeHandlers>,
    account: u64,
    region: &str,
    queue_url: &str,
    handle: &str,
    visibility: u64,
) {
    if handle.is_empty() {
        return;
    }
    let _ = sqs::SqsHandler::handle(
        &*native.sqs,
        sqs::AwsRequest {
            service: "sqs".into(),
            operation: "ChangeMessageVisibility".into(),
            account,
            region: region.to_string(),
            params: json!({
                "QueueUrl": queue_url,
                "ReceiptHandle": handle,
                "VisibilityTimeout": visibility,
            }),
            body: Bytes::new(),
        },
    );
}

// -- Filter matching -----------------------------------------------------------

/// ESM filter match: the pattern is an AND of keys; value arrays mean "one
/// of" (membership), objects recurse (subset match), everything else
/// compares for equality. `None` pattern matches everything.
fn filter_matches(criteria: Option<&Value>, doc: &Value) -> bool {
    let Some(criteria) = criteria else {
        return true;
    };
    let Some(filters) = criteria.get("Filters").and_then(Value::as_array) else {
        return true;
    };
    if filters.is_empty() {
        return true;
    }
    // Filters are OR'd; each Pattern is itself an AND of keys.
    filters.iter().any(|f| match f.get("Pattern") {
        Some(Value::String(pattern)) => serde_json::from_str::<Value>(pattern)
            .map(|rules| filter_match(&rules, doc))
            .unwrap_or(false),
        Some(Value::Object(rules)) => {
            let rules = Value::Object(rules.clone());
            filter_match(&rules, doc)
        }
        _ => true,
    })
}

/// Subset match with ESM semantics: `"key": [...]` lists match any listed
/// value, nested objects recur, missing keys never match.
fn filter_match(pattern: &Value, doc: &Value) -> bool {
    match (pattern, doc) {
        (Value::Object(rules), Value::Object(obj)) => rules.iter().all(|(k, sub)| {
            doc_lookup(obj, k)
                .as_ref()
                .map(|target| filter_match_leaf(sub, target))
                .unwrap_or(false)
        }),
        _ => filter_match_leaf(pattern, doc),
    }
}

/// Object key lookup with dot-notation nesting (`a.b.c` walks objects).
fn doc_lookup(obj: &serde_json::Map<String, Value>, key: &str) -> Option<Value> {
    if let Some(v) = obj.get(key) {
        return Some(v.clone());
    }
    let (head, rest) = match key.split_once('.') {
        Some((h, r)) => (h, r),
        None => return None,
    };
    match obj.get(head) {
        Some(Value::Object(m)) => doc_lookup(m, rest),
        _ => None,
    }
}

fn filter_match_leaf(pattern: &Value, target: &Value) -> bool {
    match (pattern, target) {
        (Value::Array(list), _) => list.iter().any(|v| v == target),
        (Value::Object(_), Value::Object(_)) => filter_match(pattern, target),
        _ => pattern == target,
    }
}
