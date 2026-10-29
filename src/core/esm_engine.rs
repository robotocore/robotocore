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
//! - Failed deliveries are budgeted: after MAX_CONSECUTIVE_FAILURES failed
//!   deliveries of the same batch the mapping's redelivery is parked (no
//!   invokes, cursor pinned) until the source produces new content beyond
//!   the failed watermark, the invocation succeeds again, or the mapping
//!   config changes (an operator update resets the budget).
//!
//! The engine runs on a dedicated thread started by the server binary; the
//! service crates stay decoupled (they only expose operation handlers and
//! accessor hooks).

use crate::core::server::NativeHandlers;
use bytes::Bytes;
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// Poll cadence. The trigger suites sleep 5-15s and need sub-second delivery;
/// 300ms is fast enough and cheap on an idle process.
const POLL_INTERVAL_MS: u64 = 300;

/// Failed redelivery budget: a permanently failing function re-invoked every
/// poll would flood downstream services forever; after this many consecutive
/// failed deliveries of the SAME batch, park until new content shows up.
const MAX_CONSECUTIVE_FAILURES: u32 = 5;

/// (account, region, esm-uuid, source scope) — the scope narrows the budget
/// to a shard for Kinesis and is empty for per-mapping sources.
type BudgetKey = (u64, String, String, String);

/// (account, region, esm-uuid) — identifies one mapping's engine state.
type MappingKey = (u64, String, String);

/// Consecutive-failure budget for one mapping cursor. Redelivering the same
/// batch is allowed while the budget lasts, then parked until the delivery
/// content changes (new records beyond the failed watermark) or the mapping
/// config changes (operator update — the enable/disable lever).
#[derive(Default)]
struct RedeliveryGate {
    consecutive_failures: u32,
    /// Identity of the last failed delivery (record fingerprint).
    failed_fingerprint: Option<String>,
    /// Salient mapping config as last seen; a change resets the budget.
    config_state: Option<String>,
}

impl RedeliveryGate {
    /// Whether this poll may deliver the batch named by `fingerprint`.
    fn may_deliver(&mut self, fingerprint: &str, config_state: &str) -> bool {
        if self.config_state.as_deref() != Some(config_state) {
            // ESM update — the operator's enable/disable lever. Reset.
            *self = Self {
                config_state: Some(config_state.to_string()),
                ..Self::default()
            };
            return true;
        }
        if self.failed_fingerprint.as_deref() != Some(fingerprint) {
            return true;
        }
        self.consecutive_failures < MAX_CONSECUTIVE_FAILURES
    }

    /// One failed delivery cycle of `fingerprint`: same batch again nudges
    /// the budget; a new batch restarts the count (one retry per new record).
    fn record_failure(&mut self, fingerprint: &str) {
        if self.failed_fingerprint.as_deref() != Some(fingerprint) {
            self.consecutive_failures = 1;
        } else {
            self.consecutive_failures += 1;
        }
        self.failed_fingerprint = Some(fingerprint.to_string());
    }

    fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.failed_fingerprint = None;
    }
}

pub fn start(native: Arc<NativeHandlers>) {
    // Attach hook: `CreateEventSourceMapping` runs on a request thread and
    // freezes a kinesis mapping's LATEST position here — before we even
    // spawn the poller thread, matching moto's synchronous source
    // registration (moto/kinesis/models.py KinesisStream.
    // lambda_event_source_mappings).
    lambda::bridge::register_esm_attach_hook(Arc::new({
        let native = native.clone();
        move |account, region, arn, uuid| bind_kinesis_at_attach(&native, account, region, arn, uuid)
    }));
    std::thread::Builder::new()
        .name("esm-engine".into())
        .spawn(move || run(native.clone()))
        .ok();
}

/// Run one guarded poll cycle: a panic in any handler (queue state races,
/// malformed queue URLs, runtime crashes) must never kill the engine
/// thread — the poll loop survives and retries on the next tick.
fn poll_guarded<F: FnOnce()>(f: F) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
}

fn run(native: Arc<NativeHandlers>) {
    let mut engine = Engine::default();
    loop {
        poll_guarded(|| poll_snapshot(&native, &mut engine));
        std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
    }
}

fn poll_snapshot(native: &Arc<NativeHandlers>, engine: &mut Engine) {
    let snapshot = native.lambda.esm_snapshot();
    tracing::debug!(mappings = snapshot.len(), "esm: snapshot");
    let mut active: HashSet<MappingKey> = HashSet::new();
    for entry in snapshot {
        let mapping = &entry.mapping;
        if !mapping.enabled || mapping.state != "Enabled" {
            continue;
        }
        active.insert((entry.account, entry.region.clone(), mapping.uuid.clone()));
        let source = classify_arn(&mapping.event_source_arn);
        if source.is_none() && !mapping.event_source_arn.is_empty() {
            // Only log once per unmapped ARN family: an unknown ARN means the
            // mapping silently never triggers, which is what we most need to
            // see in the logs.
            tracing::warn!(
                arn = %mapping.event_source_arn,
                "esm: unmapped event-source ARN — mapping will not trigger"
            );
        }
        match source {
            Some(Source::Sqs { queue_arn }) => engine.poll_sqs(native, &entry, &queue_arn),
            Some(Source::Kinesis { stream_name }) => {
                engine.poll_kinesis(native, &entry, &stream_name)
            }
            Some(Source::DynamoDb { table_name }) => {
                engine.poll_ddb_stream(native, &entry, &table_name)
            }
            None => continue,
        }
    }
    // Drop cursor state of mappings that vanished from the snapshot.
    engine.prune(&active);
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

/// Attach-time cursor binding, key (account, region, esm-uuid) →
/// shard id → sequence bound at CreateEventSourceMapping time. Mirrors
/// moto's push registration (`moto/kinesis/models.py`):
/// `KinesisStream.lambda_event_source_mappings` only ever picks up records
/// *assigned after* registration, so a `LATEST` mapping freezes its
/// position when it is created — never at the poller's first tick, which
/// may run after a producer's first puts and would silently skip them.
fn kinesis_bound() -> &'static RwLock<HashMap<MappingKey, HashMap<String, String>>> {
    static BOUND: OnceLock<RwLock<HashMap<MappingKey, HashMap<String, String>>>> = OnceLock::new();
    BOUND.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Record the per-shard `LATEST` watermarks for a kinesis-ARN mapping at
/// attach time (called from the handler's create hook). No-op for other
/// ARN families and when the stream does not exist yet (shards created
/// later still bind at first poll sight).
fn bind_kinesis_at_attach(native: &Arc<NativeHandlers>, account: u64, region: &str, arn: &str, uuid: &str) {
    let Some(Source::Kinesis { stream_name }) = classify_arn(arn) else {
        return;
    };
    let marks = native.kinesis.attach_watermarks(account, region, &stream_name);
    tracing::debug!(stream = stream_name, marks = marks.len(), "esm: attach binding");
    if marks.is_empty() {
        return;
    }
    kinesis_bound()
        .write()
        .insert((account, region.to_string(), uuid.to_string()), marks.into_iter().collect());
}

/// Engine cursor state (in-memory; restart re-polls from the map's starting
/// position, like moto's backends which lose nothing durable).
#[derive(Default)]
struct Engine {
    /// Continuation iterators keyed by (account, region, esm-uuid, shard id).
    kinesis_iterators: HashMap<(u64, String, String, String), String>,
    /// Last delivered SequenceNumber per (account, region, esm-uuid).
    ddb_last_seq: HashMap<(u64, String, String), String>,
    /// Redelivery budgets per (mapping, source scope).
    gates: HashMap<BudgetKey, RedeliveryGate>,
    /// SQS bisect frontier per mapping: failed halves awaiting the next
    /// poll's invocation. Held messages stay in-flight — nothing is
    /// re-queued into SQS until a bisection ends at a single message.
    sqs_frontier: HashMap<MappingKey, SqsFrontier>,
}

/// The pending bisect work of one mapping. `fingerprint` is the content
/// identity of the cycle that produced the queue (the failure-budget key).
struct SqsFrontier {
    fingerprint: String,
    layers: Vec<Vec<Value>>,
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
                    tracing::debug!(
                        stream = stream_name,
                        shard = %shard,
                        position = mapping.starting_position.as_deref().unwrap_or("TRIM_HORIZON"),
                        "esm: creating kinesis iterator"
                    );
                    // Bind at the attach-time watermark when this mapping
                    // registered with the source before its first poll tick
                    // (moto semantics); a restart or a shard created after
                    // attach falls back to first-sight LATEST.
                    let bound_seq = kinesis_bound()
                        .read()
                        .get(&(
                            entry.account,
                            entry.region.clone(),
                            mapping.uuid.clone(),
                        ))
                        .and_then(|m| m.get(&shard))
                        .cloned();
                    let (params_type, params_seq): (Value, Option<Value>) = match bound_seq {
                        Some(seq) => (
                            json!("AT_SEQUENCE_NUMBER"),
                            Some(json!(seq)),
                        ),
                        None => (json!(Self::starting_position(mapping)), None),
                    };
                    let mut params = json!({
                        "StreamName": stream_name,
                        "ShardId": shard,
                        "ShardIteratorType": params_type,
                    });
                    if let Some(seq) = params_seq {
                        params["StartingSequenceNumber"] = seq;
                    }
                    let resp = native.kinesis.handle(kinesis::AwsRequest {
                        service: "kinesis".into(),
                        operation: "GetShardIterator".into(),
                        account: entry.account,
                        region: entry.region.clone(),
                        params,
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
            // Redelivery budget: same batch after the budget is spent only
            // parks (no invoke, cursor pinned) until new records arrive.
            let fingerprint = delivered
                .last()
                .and_then(|r| r["eventID"].as_str())
                .unwrap_or_default()
                .to_string();
            let gate = self
                .gates
                .entry((
                    entry.account,
                    entry.region.clone(),
                    mapping.uuid.clone(),
                    shard,
                ))
                .or_default();
            if !gate.may_deliver(&fingerprint, &config_signature(mapping)) {
                continue;
            }
            let result = native.lambda.invoke_engine_event(
                entry.account,
                &entry.region,
                &mapping.function_arn,
                &json!({"Records": delivered}),
            );
            if result.function_error.is_none() {
                tracing::debug!(stream = stream_name, records = delivered.len(), "esm: kinesis batch delivered");
                gate.record_success();
                match next {
                    Some(next) => {
                        self.kinesis_iterators.insert(key, next);
                    }
                    None => {
                        self.kinesis_iterators.remove(&key);
                    }
                }
            } else {
                tracing::debug!(
                    stream = stream_name,
                    error = %result.function_error.as_deref().unwrap_or(""),
                    payload = result.payload.get(0..300).unwrap_or(&result.payload),
                    "esm: kinesis batch invoke FAILED"
                );
                gate.record_failure(&fingerprint);
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
        // Batch capacity per moto's EventSourceMapping.batch_size: each
        // invocation window carries at most that many matching records, and
        // the watermark advances only through records actually delivered
        // (a failed window re-delivers on the next poll).
        let batch_cap = (mapping.batch_size).max(1) as usize;
        let matching = self.build_ddb_event(mapping, entry, &records);
        if matching.is_empty() {
            // Filters dropped everything; the cursor still advances past
            // these records.
            if let Some(seq) = records.iter().filter_map(seq_of).max_by_key(|s| seq_num(s)) {
                self.ddb_last_seq.insert(key, seq);
            }
            return;
        }
        let mut last_seq: Option<String> = None;
        // Redelivery budget for the failed windows of this scan: parked once
        // the budget is spent (no invoke, watermark pinned) until new writes
        // arrive beyond the failed watermark.
        let gate_fingerprint = records
            .iter()
            .filter_map(seq_of)
            .max_by(|a, b| seq_num(a).cmp(&seq_num(b)))
            .unwrap_or_default();
        let gate = self
            .gates
            .entry((
                entry.account,
                entry.region.clone(),
                mapping.uuid.clone(),
                String::new(),
            ))
            .or_default();
        if !gate.may_deliver(&gate_fingerprint, &config_signature(mapping)) {
            return;
        }
        let mut delivered_any = false;
        let mut delivery_failed = false;
        for window in matching.chunks(batch_cap) {
            let result = native.lambda.invoke_engine_event(
                entry.account,
                &entry.region,
                &mapping.function_arn,
                &json!({"Records": window}),
            );
            if result.function_error.is_some() {
                delivery_failed = true;
                break;
            }
            delivered_any = true;
            if let Some(seq) = window.iter().filter_map(seq_of).max_by_key(|s| seq_num(s)) {
                last_seq = Some(seq);
            }
        }
        if delivery_failed {
            gate.record_failure(&gate_fingerprint);
        } else if delivered_any {
            gate.record_success();
        }
        if let Some(seq) = last_seq {
            self.ddb_last_seq.insert(key, seq);
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

/// Next bisection spans for a failed batch of `len` records: the two halves
/// `(start, end)`, or `None` at a single message — bisection ends there and
/// the normal failure path (visibility 0) takes the record back.
fn bisect_halves(len: usize) -> Option<[(usize, usize); 2]> {
    (len > 1).then_some([(0, len / 2), (len / 2, len)])
}

/// The salient operator-visible config of a mapping: `RedeliveryGate` resets
/// its budget whenever this signature changes (an UpdateEventSourceMapping
/// enable/disable is the lever an operator has against a parked mapping).
fn config_signature(mapping: &lambda::models::EventSourceMapping) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}",
        mapping.enabled,
        mapping.batch_size,
        mapping.function_arn,
        mapping.starting_position.as_deref().unwrap_or_default(),
        mapping.bisect_batch_on_function_error,
        mapping.maximum_retry_attempts,
        serde_json::to_string(&mapping.function_response_types).unwrap_or_default(),
        serde_json::to_string(&mapping.filter_criteria).unwrap_or_default(),
    )
}

/// Sorted MessageIds of a message batch — the delivery fingerprint the
/// failure budget keys on (same set again = same failed redelivery; a new
/// record anywhere in the batch starts a fresh retry cycle).
fn message_fingerprint(messages: &[&Value]) -> String {
    let mut ids: Vec<&str> = messages
        .iter()
        .filter_map(|m| m["MessageId"].as_str())
        .collect();
    ids.sort_unstable();
    ids.join(",")
}

impl Engine {
    /// Is this mapping's SQS delivery parked? Bid the gate notice config
    /// changes and decide about `fingerprint` in one go.
    fn sqs_gate(&mut self, entry: &lambda::models::EsmMappingEntry) -> &mut RedeliveryGate {
        self.gates
            .entry((
                entry.account,
                entry.region.clone(),
                entry.mapping.uuid.clone(),
                String::new(),
            ))
            .or_default()
    }

    /// One SQS poll cycle: frontier first (bisection re-invokes only the
    /// failed fraction), then a fresh receive.
    ///
    /// Bisection (`BisectBatchOnFunctionError`) keeps failed halves in
    /// memory: successful halves are deleted, failed halves are held
    /// in-flight (never re-queued into SQS), and the next poll splits them
    /// again until single-message batches, which fall back to the normal
    /// failure path (visible again at visibility 0). The recursion therefore
    /// deepens per poll instead of re-receiving the whole original batch.
    fn poll_sqs(
        &mut self,
        native: &Arc<NativeHandlers>,
        entry: &lambda::models::EsmMappingEntry,
        queue_arn: &str,
    ) {
        let mapping = &entry.mapping;
        let mapping_key = (entry.account, entry.region.clone(), mapping.uuid.clone());
        let config = config_signature(mapping);

        if let Some(frontier) = self.sqs_frontier.remove(&mapping_key) {
            let gate = self.sqs_gate(entry);
            if !gate.may_deliver(&frontier.fingerprint, &config) {
                // Parked: release the held half-batches back to the queue
                // (visibility 0) and skip invoking anything this poll.
                release_frontier(native, queue_arn, &frontier);
                return;
            }
            let (stuck, resolved_any, failed_any) =
                drain_frontier_layer(native, entry, mapping, queue_arn, &frontier.layers);
            // One failure-poll regardless of partial suite successes: the
            // budget must keep ticking while any fraction stays unresolved.
            let gate = self.sqs_gate(entry);
            if failed_any {
                gate.record_failure(&frontier.fingerprint);
            } else if resolved_any {
                gate.record_success();
            }
            if !stuck.is_empty() {
                self.sqs_frontier.insert(
                    mapping_key,
                    SqsFrontier {
                        fingerprint: frontier.fingerprint,
                        layers: stuck,
                    },
                );
            }
            return;
        }

        self.sqs_receive_cycle(native, entry, mapping, queue_arn, &config);
    }

    /// Fresh receive → filter → single invoke → outcome handling.
    fn sqs_receive_cycle(
        &mut self,
        native: &Arc<NativeHandlers>,
        entry: &lambda::models::EsmMappingEntry,
        mapping: &lambda::models::EventSourceMapping,
        queue_arn: &str,
        config: &str,
    ) {
        let Some((account, region)) = arn_scope(queue_arn) else {
            return;
        };
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
                    // Real delivery: carry the system attributes and the
                    // message attributes into the polled records.
                    "AttributeNames": ["All"],
                    "MessageAttributeNames": ["All"],
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

        // Failure budget: parked mappings skip the invoke and release the
        // messages untouched, until new content (a different fingerprint)
        // or a config change (recorded by `may_deliver`) reopens delivery.
        let fingerprint = message_fingerprint(&matched);
        if !self.sqs_gate(entry).may_deliver(&fingerprint, config) {
            for msg in &matched {
                change_sqs_visibility(
                    native,
                    account,
                    &region,
                    &queue_url,
                    msg["ReceiptHandle"].as_str().unwrap_or_default(),
                    0,
                );
            }
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
            // BisectBatchOnFunctionError: hold the failed halves for the
            // next poll instead of re-queueing them — the queue only ever
            // re-receives what the bifurcation genuinely gave up.
            let mut halves: Vec<Vec<Value>> = Vec::new();
            for (start, end) in bisect_halves(matched.len()).unwrap_or([(0, 0), (0, 0)]) {
                halves.push(matched[start..end].iter().map(|m| (*m).clone()).collect());
            }
            self.sqs_frontier.insert(
                (entry.account, entry.region.clone(), mapping.uuid.clone()),
                SqsFrontier {
                    fingerprint: fingerprint.clone(),
                    layers: halves,
                },
            );
            self.sqs_gate(entry).record_failure(&fingerprint);
            return;
        }

        // Everything the outcome leaves unresolved (whole-batch error or
        // reported item failures) keeps its messages in the queue.
        let mut unresolved: Vec<&Value> = Vec::new();
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
                unresolved.push(msg);
            } else {
                delete_sqs_message(native, account, &region, &queue_url, msg);
            }
        }
        let gate = self.sqs_gate(entry);
        if unresolved.is_empty() {
            gate.record_success();
        } else {
            // One failure-poll for the unresolved identity; parked once the
            // budget is spent (next polls release it untouched).
            gate.record_failure(&message_fingerprint(&unresolved));
        }
    }
}

/// Invoke every held half of one frontier layer; return the next layer
/// (failed halves down to their own bisection or singles re-queued at
/// visibility 0), plus whether anything resolved or failed.
fn drain_frontier_layer(
    native: &Arc<NativeHandlers>,
    entry: &lambda::models::EsmMappingEntry,
    mapping: &lambda::models::EventSourceMapping,
    queue_arn: &str,
    layers: &[Vec<Value>],
) -> (Vec<Vec<Value>>, bool, bool) {
    let Some((account, region)) = arn_scope(queue_arn) else {
        return (Vec::new(), false, false);
    };
    let Some(queue_name) = queue_arn.rsplit(':').next().map(String::from) else {
        return (Vec::new(), false, false);
    };
    let queue_url = format!("http://localhost:4566/{account}/{queue_name}");
    let mut next: Vec<Vec<Value>> = Vec::new();
    let mut resolved_any = false;
    let mut failed_any = false;
    for batch in layers {
        let event = json!({
            "Records": batch
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
        if result.function_error.is_none() {
            resolved_any = true;
            for msg in batch {
                delete_sqs_message(native, account, &region, &queue_url, msg);
            }
        } else {
            failed_any = true;
            match bisect_halves(batch.len()) {
                Some(halves) => {
                    for (start, end) in halves {
                        next.push(batch[start..end].to_vec());
                    }
                }
                None => {
                    // Single message: bisection ends; the record goes
                    // back through the normal failure path.
                    for msg in batch {
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
        }
    }
    (next, resolved_any, failed_any)
}

/// Release every held message of a parked frontier back to the queue at
/// visibility 0 (their receipt handles are still the receiving ones).
fn release_frontier(native: &Arc<NativeHandlers>, queue_arn: &str, frontier: &SqsFrontier) {
    let Some((account, region)) = arn_scope(queue_arn) else {
        return;
    };
    let Some(queue_name) = queue_arn.rsplit(':').next().map(String::from) else {
        return;
    };
    let queue_url = format!("http://localhost:4566/{account}/{queue_name}");
    for batch in &frontier.layers {
        for msg in batch {
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

impl Engine {
    /// Drop engine state of mappings that vanished from the snapshot.
    fn prune(&mut self, active: &HashSet<MappingKey>) {
        self.gates
            .retain(|k, _| active.contains(&(k.0, k.1.clone(), k.2.clone())));
        self.sqs_frontier
            .retain(|k, _| active.contains(&(k.0, k.1.clone(), k.2.clone())));
        kinesis_bound().write().retain(|k, _| active.contains(k));
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

/// One SQS record in a lambda event (era `event_source.py` `_poll_sqs`
/// record row, including the real attributes and converted message
/// attributes).
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
        // ApproximateReceiveCount comes from the wire per received message.
        // Keep the auto-fill for bridged messages that skip the receive path.
        obj.entry("ApproximateReceiveCount")
            .or_insert_with(|| json!("1"));
    }
    let mut record = json!({
        "messageId": msg["MessageId"],
        "receiptHandle": msg["ReceiptHandle"],
        "body": msg["Body"],
        "attributes": attributes,
        "messageAttributes": convert_message_attributes(msg.get("MessageAttributes")),
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

/// Convert SQS wire message attributes into the lambda event row (era
/// `event_source.py::_convert_message_attributes`): structured wire objects
/// (`{DataType, StringValue, ...}`) pass through, bare scalars are wrapped
/// into a String-typed row.
fn convert_message_attributes(attrs: Option<&Value>) -> Value {
    let mut out = serde_json::Map::new();
    let Some(map) = attrs.and_then(Value::as_object) else {
        return Value::Object(out);
    };
    for (key, value) in map {
        if let Value::Object(_) = value {
            out.insert(key.clone(), value.clone());
        } else {
            out.insert(
                key.clone(),
                json!({"stringValue": value.to_string(), "dataType": "String"}),
            );
        }
    }
    Value::Object(out)
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

/// ESM filter match: the pattern is an AND of keys; value arrays run
/// EventBridge operator semantics (membership, prefix/suffix, numeric,
/// exists, anything-but — identical to rule patterns via the shared
/// `events::pattern` matcher); nested objects recurse. `None` pattern
/// matches everything.
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

/// Subset match with ESM semantics: `"key": [...]` lists carry the
/// EventBridge operators (shared matcher), `{"exists": false}` lists match
/// absent keys (era `event_source.py::_match_pattern`), nested objects
/// recur, otherwise-present keys never match absent targets.
fn filter_match(pattern: &Value, doc: &Value) -> bool {
    match (pattern, doc) {
        (Value::Object(rules), Value::Object(obj)) => rules.iter().all(|(k, sub)| {
            match doc_lookup(obj, k) {
                Some(target) => filter_match_leaf(sub, &target),
                // Absent key: only an "exists": false list passes (the era
                // filter never matched other shapes against missing keys).
                None => sub.as_array().is_some_and(|list| {
                    list.iter()
                        .any(|pv| pv.get("exists").and_then(Value::as_bool) == Some(false))
                }),
            }
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

/// Leaf comparison with the EventBridge operator semantics. `{"exists":
/// true}` is satisfied by the key resolving at all (era filters should not
/// reject present keys); the rest of the list run the shared matcher.
fn filter_match_leaf(pattern: &Value, target: &Value) -> bool {
    match (pattern, target) {
        (Value::Array(list), _) => {
            if list
                .iter()
                .any(|pv| pv.get("exists").and_then(Value::as_bool) == Some(true))
            {
                return true;
            }
            // Shared EventBridge operators (prefix/suffix/numeric/exists/
            // anything-but) plus era's equality fallbacks: string patterns
            // match numeric fields ("42" == 42) and bare object elements
            // compare for equality.
            let target_repr = target.to_string();
            events::pattern::list_match(list, target)
                || list.iter().any(|pv| {
                    (pv.as_object().is_some() && pv == target)
                        || (target.as_f64().is_some() && pv.as_str() == Some(target_repr.as_str()))
                })
        }
        (Value::Object(_), Value::Object(_)) => filter_match(pattern, target),
        _ => pattern == target,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(id: &str) -> Value {
        json!({"MessageId": id, "ReceiptHandle": format!("rh-{id}"), "Body": id})
    }

    // -- bisect frontier ----------------------------------------------------

    #[test]
    fn bisect_halves_end_at_single_message() {
        assert!(bisect_halves(1).is_none(), "size 1 cannot bisect further");
        assert_eq!(bisect_halves(4), Some([(0, 2), (2, 4)]));
        assert_eq!(bisect_halves(5), Some([(0, 2), (2, 5)]));
    }

    #[test]
    fn bisect_frontier_deepens_to_batch_size_one() {
        // Drive the exact structure the drain uses: a permanently failing
        // function bisects a 4-message batch; every poll drains the current
        // layer, successes delete, failures split into halves. Deepening
        // must reach single-message deliveries instead of re-receiving the
        // whole original batch each poll.
        let mut layers: Vec<Vec<Value>> = vec![(0..4).map(|i| msg(&format!("m{i}"))).collect()];
        let mut invoked_sizes: Vec<usize> = Vec::new();
        // Far more polls than bisect depth needs — a wedged recursion would
        // keep growing this sequence.
        for _ in 0..8 {
            let drained = std::mem::take(&mut layers);
            let mut next: Vec<Vec<Value>> = Vec::new();
            for batch in &drained {
                invoked_sizes.push(batch.len());
                match bisect_halves(batch.len()) {
                    Some([(l, lm), (r, re)]) => {
                        next.push(batch[l..lm].to_vec());
                        next.push(batch[r..re].to_vec());
                    }
                    None => continue, // single → normal failure path
                }
            }
            layers = next;
            if layers.is_empty() {
                break;
            }
        }
        assert_eq!(invoked_sizes, vec![4, 2, 2, 1, 1, 1, 1]);
        assert!(
            layers.is_empty(),
            "the frontier must drain: no half ever re-joins a full batch"
        );
    }

    // -- redelivery budget --------------------------------------------------

    #[test]
    fn budget_parks_after_five_consecutive_failures() {
        let mut gate = RedeliveryGate::default();
        let cfg = "cfg";
        // Five failed delivery cycles of the same batch: each is allowed.
        for cycle in 1..=MAX_CONSECUTIVE_FAILURES {
            assert!(
                gate.may_deliver("batch-a", cfg),
                "cycle {cycle} is inside the budget"
            );
            gate.record_failure("batch-a");
        }
        // Then the same batch parks; other content still delivers.
        assert!(!gate.may_deliver("batch-a", cfg), "parked");
        assert!(
            gate.may_deliver("batch-b", cfg),
            "different content is always allowed"
        );
    }

    #[test]
    fn budget_restarts_the_count_per_new_record_batch() {
        let mut gate = RedeliveryGate::default();
        for _ in 0..MAX_CONSECUTIVE_FAILURES {
            assert!(gate.may_deliver("batch-a", "cfg"));
            gate.record_failure("batch-a");
        }
        assert!(!gate.may_deliver("batch-a", "cfg"));
        // New records beyond the failed watermark unpark for one cycle.
        assert!(gate.may_deliver("batch-ab", "cfg"));
        gate.record_failure("batch-ab");
        // The count restarted for the new content: only the remaining
        // budget cycles deliver before the parked state returns.
        for cycle in 1..MAX_CONSECUTIVE_FAILURES {
            assert!(
                gate.may_deliver("batch-ab", "cfg"),
                "cycle {cycle} of the restarted budget"
            );
            gate.record_failure("batch-ab");
        }
        assert!(!gate.may_deliver("batch-ab", "cfg"));
    }

    #[test]
    fn budget_resets_on_success() {
        let mut gate = RedeliveryGate::default();
        for _ in 0..MAX_CONSECUTIVE_FAILURES {
            assert!(gate.may_deliver("batch-a", "cfg"));
            gate.record_failure("batch-a");
        }
        assert!(!gate.may_deliver("batch-a", "cfg"));
        gate.record_success();
        assert!(
            gate.may_deliver("batch-a", "cfg"),
            "success reopens delivery"
        );
    }

    #[test]
    fn budget_resets_when_the_mapping_is_updated() {
        let mut gate = RedeliveryGate::default();
        for _ in 0..MAX_CONSECUTIVE_FAILURES {
            assert!(gate.may_deliver("batch-a", "old-config"));
            gate.record_failure("batch-a");
        }
        assert!(!gate.may_deliver("batch-a", "old-config"));
        // The operator's enable/disable lever: any salient config change
        // resets the budget in the same may_deliver check.
        assert!(gate.may_deliver("batch-a", "new-config"));
    }

    // -- engine bookkeeping --------------------------------------------------

    // Park keeps the SQS receive accounting alive (visibility 0 releases),
    // which is what lets the queue's redrive policy move exhausted
    // messages to the DLQ instead of parking them forever; on the wire
    // side redriving preserves the original MessageId.

    #[test]
    fn engine_prune_drops_state_of_deleted_mappings() {
        let mut engine = Engine::default();
        let live = (1u64, "us-east-1".to_string(), "live-uuid".to_string());
        let dead = (2u64, "us-east-1".to_string(), "dead-uuid".to_string());
        for key in [live.clone(), dead.clone()] {
            engine.gates.insert(
                (key.0, key.1.clone(), key.2.clone(), String::new()),
                RedeliveryGate::default(),
            );
            engine.sqs_frontier.insert(
                key.clone(),
                SqsFrontier {
                    fingerprint: "f".into(),
                    layers: vec![vec![msg("m")]],
                },
            );
        }
        engine.prune(&HashSet::from([live.clone()]));
        assert!(engine
            .gates
            .contains_key(&(live.0, live.1.clone(), live.2.clone(), "".into())));
        assert!(!engine
            .gates
            .contains_key(&(dead.0, dead.1.clone(), dead.2.clone(), "".into())));
        assert!(engine.sqs_frontier.contains_key(&live));
        assert!(!engine.sqs_frontier.contains_key(&dead));
    }

    // -- SQS record payload (F1) --------------------------------------------

    fn test_mapping() -> lambda::models::EventSourceMapping {
        lambda::models::EventSourceMapping {
            uuid: "u".into(),
            function_arn: String::new(),
            event_source_arn: String::new(),
            state: "Enabled".into(),
            state_transition_reason: String::new(),
            last_modified: 0.0,
            batch_size: 10,
            enabled: true,
            starting_position: None,
            filter_criteria: None,
            function_response_types: None,
            bisect_batch_on_function_error: false,
            maximum_retry_attempts: -1,
        }
    }

    #[test]
    fn sqs_record_carries_message_attributes_and_real_receive_count() {
        let msg = serde_json::json!({
            "MessageId": "m1",
            "ReceiptHandle": "rh",
            "Body": "{}",
            "MD5OfBody": "d41d8cd98f00b204e9800998ecf8427e",
            "Attributes": {
                "SentTimestamp": "1545082649183",
                "ApproximateReceiveCount": "3",
                "ApproximateFirstReceiveTimestamp": "1545082649185",
            },
            "MessageAttributes": {
                "color": {"dataType": "String", "stringValue": "red"},
                "count": {"DataType": "Number", "StringValue": "7"},
            },
        });
        let record = sqs_record(
            &test_mapping(),
            &msg,
            "arn:aws:sqs:us-east-1:123456789012:q",
            "us-east-1",
        );
        assert_eq!(
            record["attributes"]["ApproximateReceiveCount"],
            json!("3"),
            "the wire receive count must flow into the event verbatim"
        );
        // Wire rows pass through; crudely-typed scalars wrap into a String row
        // (era `_convert_message_attributes`).
        assert_eq!(
            record["messageAttributes"]["count"],
            json!({"DataType": "Number", "StringValue": "7"})
        );
        assert_eq!(
            record["messageAttributes"]["color"],
            json!({"dataType": "String", "stringValue": "red"})
        );
    }

    #[test]
    fn sqs_record_defaults_receive_count_without_wire_attributes() {
        // Bridged messages (no receive path) keep moto's default of 1.
        let msg = msg("m2");
        let record = sqs_record(
            &test_mapping(),
            &msg,
            "arn:aws:sqs:us-east-1:123456789012:q",
            "us-east-1",
        );
        assert_eq!(record["attributes"]["ApproximateReceiveCount"], json!("1"));
        assert_eq!(record["messageAttributes"], json!({}));
    }

    // -- filter operators (F2) -----------------------------------------------

    fn filter_apply(pattern: &str, msg_doc: &Value) -> bool {
        let criteria = json!({"Filters": [{"Pattern": pattern}]});
        filter_matches(Some(&criteria), msg_doc)
    }

    #[test]
    fn esm_filter_operators_prefix_numeric_exists() {
        let doc_with_body = sqs_filter_doc(&serde_json::json!({
            "MessageId": "m", "Body": "{\"statusCode\": \"200 OK\", \"amount\": 42, \"kind\": \"x\"}"
        }));

        // prefix operator against a nested body field
        assert!(filter_apply(
            "{\"body\": {\"statusCode\": [{\"prefix\": \"200\"}]}}",
            &doc_with_body
        ));
        assert!(!filter_apply(
            "{\"body\": {\"statusCode\": [{\"prefix\": \"404\"}]}}",
            &doc_with_body
        ));

        // numeric operator
        assert!(filter_apply(
            "{\"body\": {\"amount\": [{\"numeric\": [\">\", 10]}]}}",
            &doc_with_body
        ));
        assert!(!filter_apply(
            "{\"body\": {\"amount\": [{\"numeric\": [\">\", 42]}]}}",
            &doc_with_body
        ));

        // exists operator: present keys match with exists:true; an absent
        // key passes only through an exists:false list (era filter rule).
        assert!(filter_apply(
            "{\"body\": {\"kind\": [{\"exists\": true}]}}",
            &doc_with_body
        ));
        assert!(filter_apply(
            "{\"body\": {\"missing\": [{\"exists\": false}]}}",
            &doc_with_body
        ));
        assert!(!filter_apply(
            "{\"body\": {\"missing\": [{\"exists\": true}]}}",
            &doc_with_body
        ));

        // plain membership still works after the operator port.
        assert!(filter_apply(
            "{\"body\": {\"kind\": [\"x\"]}}",
            &doc_with_body
        ));
        assert!(!filter_apply(
            "{\"body\": {\"kind\": [\"y\"]}}",
            &doc_with_body
        ));
    }

    #[test]
    fn esm_filter_exists_false_matches_absent_keys() {
        let doc = sqs_filter_doc(&serde_json::json!({
            "MessageId": "m", "Body": "{\"a\": 1}"
        }));
        // Absent key + exists:false list → match (era _match_pattern).
        assert!(filter_apply(
            "{\"body\": {\"b\": [{\"exists\": false}]}}",
            &doc
        ));
        // Present key + exists:false → no match.
        assert!(!filter_apply(
            "{\"body\": {\"a\": [{\"exists\": false}]}}",
            &doc
        ));
    }

    // -- guarded poll loop (F10) ----------------------------------------------

    #[test]
    fn poll_guard_survives_a_panicking_cycle() {
        // A panic inside a poll cycle is contained: the engine loop keeps
        // running on the next tick instead of dying with the thread.
        let mut cycles = 0;
        poll_guarded(|| panic!("engine poll boom"));
        cycles += 1;
        poll_guarded(|| ());
        cycles += 1;
        assert_eq!(cycles, 2, "execution continues past a panicking poll");
        let escaped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            poll_guarded(|| panic!("guarded"));
        }))
        .is_err();
        assert!(!escaped, "poll_guarded must swallow the unwind itself");
    }
}
