//! SQS request handler implementation

use crate::error::SqsError;
use crate::models::{MoveTask, Queue, SqsMessage, SqsStore};
use crate::protocol::{AwsRequest, AwsResponse, SqsHandler};
use chrono::Utc;
use md5;
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use uuid::Uuid;

type StoreMap = Arc<RwLock<HashMap<(u64, String), Arc<SqsStore>>>>;

/// Process-wide store map shared by every `DefaultSqsHandler` instance.
///
/// Cross-service callers (SNS delivery, S3 notifications, EventBridge
/// targets) construct their own handler instance; sharing the store map
/// lets those instances see the same queues as the server's registered
/// SQS handler, mirroring Moto's in-process backend semantics.
fn global_stores() -> StoreMap {
    use std::sync::OnceLock;
    static STORES: OnceLock<StoreMap> = OnceLock::new();
    STORES
        .get_or_init(|| Arc::new(RwLock::new(HashMap::new())))
        .clone()
}

/// (account, region, store) for every process-global store, for the
/// cross-service bridge (Resource Groups Tagging API walks live queue tags).
pub(crate) fn bridge_stores() -> Vec<(u64, String, Arc<SqsStore>)> {
    global_stores()
        .read()
        .iter()
        .map(|((a, r), s)| (*a, r.clone(), s.clone()))
        .collect()
}

/// Outcome of preparing a standard/FIFO send.
enum PreparedMessage {
    /// Message accepted; carries the message to enqueue.
    Accepted(Box<crate::models::SqsMessage>),
    /// Deduplicated: respond with the original send's fields.
    Duplicate {
        message_id: String,
        md5_of_body: String,
        sequence_number: Option<String>,
    },
}

/// A VisibilityTimeout-family member (`VisibilityTimeout`,
/// `MaxNumberOfMessages`, `WaitTimeSeconds`) as moto's automated parse
/// layer sees it (`moto/core/parse.py`). The doctrine it encodes:
///
/// - **absent-or-null → absent.** The key missing and the member carrying
///   a JSON null are the same thing: moto's request parser drops null
///   members outright (`BaseJSONParser._parse_structure` skips a member
///   whose wire value parses to None — live-verified), so the response
///   layer falls back to the queue's attribute or its defaults (receive),
///   or crashes on the MISSING value where moto's own code does
///   (`None > 43200` TypeError in top-level ChangeMessageVisibility —
///   the dropped null reproduces exactly, live-verified).
/// - **""/junk → the moto-crash mirror.** `_parse_integer` int()s every
///   surviving member (`int("")` → ValueError; live-verified on receive,
///   ChangeMessageVisibility, batch rows and the receive bounds), and a
///   non-string non-numeric shape (`{"x": 1}`, `[1]`) raises TypeError
///   there too: the request dies as a plain werkzeug 500 before any
///   response method runs — the batch row arm's broad `except:` only
///   produces a Failed entry when the KEY is absent, never for junk (the
///   parse layer has already discarded it).
/// - **python int() coercions are honored** (see `python_int_str`):
///   surrounding whitespace and unicode Nd digits strip/map
///   (`int(" 30 ")` = 30, `int("٣٠")` = 30 — live-verified), PEP 515
///   underscores parse only between digits (`int("1_0")` = 10,
///   `int("٣_٠")` = 30, `int("_30")` = ValueError → the moto crash),
///   booleans coerce (`int(True)` = 1 — live-verified), a leading sign
///   is honored and negatives apply (moto's `Message.change_visibility`
///   stores `visible_at` in the past; the call sites clamp negatives to
///   0 because the u64-typed store cannot represent a past point), and
///   float members truncate (`int(3.5)` = 3).
/// - **saturation only for beyond-i64 magnitudes.** Python's int() is
///   unbounded, but the API only ever compares the value against small
///   bounds or the 43200 cap, so the comparison value saturates at the
///   i64 range: a positive overflow stays cap/bounds-breaching and a
///   negative overflow behaves like every negative. The *echo* is a
///   different story — moto interpolates the real digits into its error
///   messages (live-verified: `Value 999999999999999999999999999999 for
///   parameter MaxNumberOfMessages is invalid`), so `Parsed` carries
///   `echo: str(int(value))` for the message text, saturated
///   comparisons aside.
enum MotoInt {
    /// The key is missing or carries a JSON null (moto's parse drops
    /// both — the response layer takes its fallback).
    Absent,
    /// int() accepted. `value` is the i64-saturated comparison value;
    /// `echo` is python's str(int(member)) — the canonical digits moto
    /// interpolates into its error messages instead of the saturated
    /// value (the receive Max/Wait bounds wordings).
    Parsed { value: i64, echo: String },
    /// Present but int()-raising: moto's parse layer kills the request
    /// as a plain werkzeug 500 before the response method runs.
    Junk,
}

/// The parse-layer view of a `param` (see `MotoInt` for the doctrine):
/// any numeral text runs through python's int() (`python_int_str`), so
/// the value surface is identical across the native JSON numbers and the
/// gateway's query-protocol strings. A JSON number beyond the u64 range
/// arrives in serde as a float that already lost the literal's digit
/// fidelity — python's json keeps the exact integer — so those echoes
/// are a recorded divergence (the digits come back rounded); every
/// integer ≤ u64::MAX and every string carries moto's exact bytes.
/// (Documented on the enum: its implementation is the associated
/// `moto_int` below.)
///
/// The actual SQS handler implementation
pub struct DefaultSqsHandler {
    // Store per (account, region), shared process-wide
    pub(crate) stores: StoreMap,
}

impl DefaultSqsHandler {
    pub fn new() -> Self {
        Self {
            stores: global_stores(),
        }
    }

    pub(crate) fn get_store(&self, account: u64, region: &str) -> Arc<SqsStore> {
        let mut stores = self.stores.write();
        stores
            .entry((account, region.to_string()))
            .or_insert_with(|| Arc::new(SqsStore::new()))
            .clone()
    }

    /// CreateEventSourceMapping existence probe (lambda bridge): does this
    /// account's region know the queue name? FIFO names resolve bare.
    pub fn has_queue(&self, account: u64, region: &str, name: &str) -> bool {
        let bare = name.strip_suffix(".fifo").unwrap_or(name);
        self.get_store(account, region)
            .get_queue_any(bare)
            .is_some()
    }

    fn validate_queue_name(name: &str) -> Result<(), SqsError> {
        // moto's Queue.name setter (moto/sqs/models.py) answers every
        // malformed name with the same single InvalidParameterValue message:
        // plain names `^[a-zA-Z0-9_-]{1,80}$` or FIFO names
        // `^[a-zA-Z0-9_-]{1,75}\.fifo$`. A `.` is legal ONLY as the
        // `.fifo` suffix — one anywhere else is moto-invalid, as is a
        // non-ASCII (unicode) alphanumeric. Length counts python str
        // chars, not bytes. Live-verified against the pinned fork.
        let plain = name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
        if !name.is_empty() && name.chars().count() <= 80 && plain {
            return Ok(());
        }
        if let Some(body) = name.strip_suffix(".fifo") {
            if (1..=75).contains(&body.chars().count())
                && body
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            {
                return Ok(());
            }
        }
        Err(SqsError::InvalidParameterValue(
            "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length"
                .to_string(),
        ))
    }

    fn md5_hash(data: &str) -> String {
        format!("{:x}", md5::compute(data.as_bytes()))
    }

    /// The AWS MD5-of-message-attributes algorithm.
    fn md5_of_message_attributes(attrs: &HashMap<String, serde_json::Value>) -> Option<String> {
        if attrs.is_empty() {
            return None;
        }
        let mut encoded: Vec<u8> = Vec::new();
        let mut names: Vec<&String> = attrs.keys().collect();
        names.sort();
        for name in names {
            let details = &attrs[name.as_str()];
            let data_type = details
                .get("DataType")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            Self::encode_len_prefixed(&mut encoded, name.as_bytes());
            Self::encode_len_prefixed(&mut encoded, data_type.as_bytes());
            if data_type.starts_with("Binary") {
                encoded.push(0x02);
                let value = Self::binary_attr_bytes(details);
                Self::encode_len_prefixed(&mut encoded, &value);
            } else {
                encoded.push(0x01);
                let value = details
                    .get("StringValue")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .as_bytes();
                Self::encode_len_prefixed(&mut encoded, value);
            }
        }
        Some(Self::md5_hash_bytes(&encoded))
    }

    fn encode_len_prefixed(encoded: &mut Vec<u8>, data: &[u8]) {
        encoded.extend_from_slice(&(data.len() as u32).to_be_bytes());
        encoded.extend_from_slice(data);
    }

    /// Decode the BinaryValue of a message attribute (base64 string or byte array).
    fn binary_attr_bytes(details: &serde_json::Value) -> Vec<u8> {
        use base64::Engine;
        match details.get("BinaryValue") {
            Some(serde_json::Value::String(s)) => base64::engine::general_purpose::STANDARD
                .decode(s.as_bytes())
                .unwrap_or_else(|_| s.as_bytes().to_vec()),
            Some(serde_json::Value::Array(arr)) => arr
                .iter()
                .filter_map(|v| v.as_u64().map(|b| b as u8))
                .collect(),
            _ => Vec::new(),
        }
    }

    fn md5_hash_bytes(data: &[u8]) -> String {
        format!("{:x}", md5::compute(data))
    }

    /// Mint a receipt handle for `message_id` on `queue_url`. Also used by the
    /// bridge so cross-service messages get real handles when drained.
    pub(crate) fn generate_receipt_handle(message_id: &str, queue_url: &str) -> String {
        // Encode as base64: {random_uuid} {queue_arn} {message_id} {timestamp}
        let data = format!(
            "{} {} {} {}",
            Uuid::new_v4(),
            queue_url,
            message_id,
            Utc::now().timestamp_millis()
        );
        {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.encode(&data)
        }
    }

    /// Host port for minted QueueUrls. moto derives the URL from the
    /// request's netloc (`moto/sqs/models.py:535 def url`); the gateway
    /// strips the Host header before the crate sees the request, so the
    /// port follows `SQS_PUBLIC_BASE`'s explicit port, then `RUST_PORT`,
    /// then the documented canonical 4566 — logged once per process
    /// (portability rule: AGENTS.md merge protocol item 5).
    fn queue_host_port() -> u16 {
        use std::sync::atomic::{AtomicBool, Ordering};
        static FALLBACK_LOGGED: AtomicBool = AtomicBool::new(false);
        if let Ok(base) = std::env::var("SQS_PUBLIC_BASE") {
            if let Some(p) = base
                .split("://")
                .last()
                .and_then(|h| h.split('/').next())
                .and_then(|h| h.rsplit(':').next())
                .and_then(|p| p.parse().ok())
            {
                return p;
            }
        }
        if let Ok(p) = std::env::var("RUST_PORT") {
            if let Ok(port) = p.parse() {
                return port;
            }
        }
        if !FALLBACK_LOGGED.swap(true, Ordering::Relaxed) {
            eprintln!(
                "[sqs] QueueUrls minted with the canonical :4566 fallback (set SQS_PUBLIC_BASE or RUST_PORT to override)"
            );
        }
        4566
    }

    fn queue_url_for(queue_name: &str, account: u64, region: &str) -> String {
        format!(
            "http://sqs.{}.localhost.robotocore.cloud:{}/{}/{}",
            region,
            Self::queue_host_port(),
            account,
            queue_name
        )
    }

    fn json_headers() -> Vec<(String, String)> {
        vec![(
            "Content-Type".to_string(),
            "application/x-amz-json-1.0".to_string(),
        )]
    }

    fn handle_create_queue(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        // moto feeds a missing QueueName straight into the Queue.name setter
        // where the regex match on None escapes as a werkzeug 500 crash
        // (live-verified against the pinned fork).
        let queue_name = params
            .get("QueueName")
            .and_then(|v| v.as_str())
            .ok_or(SqsError::MotoCrash)?;

        Self::validate_queue_name(queue_name)?;

        let store = self.get_store(account, region);

        // Resolve-or-create: the store is insert-if-absent by name, so two
        // racing CreateQueue calls for the same name converge on one shared
        // queue instead of the second insert discarding the first (with any
        // sends racing through the replaced Arc lost). moto returns the
        // existing queue idempotently on either path (`moto/sqs/models.py`
        // `create_queue`).
        let queue_arc = match store.get_queue(queue_name) {
            Some(existing) => existing,
            None => {
                // Create new queue
                let url = Self::queue_url_for(queue_name, account, region);
                let arn = format!("arn:aws:sqs:{}:{}:{}", region, account, queue_name);

                let mut queue = Queue::new(
                    queue_name.to_string(),
                    region.to_string(),
                    account.to_string(),
                    url,
                    arn,
                );
                if queue_name.ends_with(".fifo") {
                    queue.attributes.fifo_queue = true;
                }
                queue.last_modified = queue.created;
                store.create_queue(queue)
            }
        };
        let url = Self::queue_url_for(queue_name, account, region);

        // moto re-applies attributes and tags on idempotent create; boto3's
        // SQS json protocol serializes create-queue tags lower-case, the
        // query protocol uses `Tags`.
        {
            let mut q = queue_arc.write();
            q.attributes
                .apply(params.get("Attributes").unwrap_or(&Value::Null));
            let mut stored = q.tags.write();
            for (k, v) in Self::create_tags_of(params) {
                stored.insert(k, v);
            }
        }

        // Return the queue URL (idempotent, like Python _create_queue)
        let body = json!({
            "QueueUrl": url
        })
        .to_string();

        Ok(AwsResponse {
            status: 200,
            headers: Self::json_headers(),
            body,
        })
    }

    /// boto3's SQS json protocol serializes create-queue tags lower-case;
    /// the query protocol uses `Tags`. Both land as string maps here.
    fn create_tags_of(params: &Value) -> Vec<(String, String)> {
        params
            .get("tags")
            .or_else(|| params.get("Tags"))
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Prepare a message for sending on `queue`, applying FIFO dedup, group id,
    /// sequence numbers and delay. Returns Ok(None) when the message is a
    /// duplicate (deduplicated send), Ok(Some(response fields)) otherwise.
    /// The `params` are the send parameters (SendMessage or a batch entry).
    fn prepare_send(
        &self,
        queue: &mut Queue,
        queue_url: &str,
        body: &str,
        params: &Value,
    ) -> Result<Option<PreparedMessage>, SqsError> {
        let now_ms = Utc::now().timestamp_millis() as u64;

        let delay: u32 = if queue.attributes.fifo_queue {
            0 // FIFO queues reject per-message delay; queue delay still applies
        } else {
            params
                .get("DelaySeconds")
                .and_then(|v| v.as_u64())
                .unwrap_or(queue.attributes.delay_seconds as u64)
                .min(900) as u32
        };
        let effective_delay = if params.get("DelaySeconds").is_some() {
            delay
        } else {
            queue.attributes.delay_seconds
        };

        let message_attributes: HashMap<String, serde_json::Value> = params
            .get("MessageAttributes")
            .and_then(|v| v.as_object())
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        let md5_of_body = Self::md5_hash(body);

        let message = if queue.attributes.fifo_queue {
            let group_id = params
                .get("MessageGroupId")
                .and_then(|v| v.as_str())
                .map(String::from);
            let gid = group_id.ok_or_else(|| {
                SqsError::MissingParameter(
                    "The request must contain the parameter MessageGroupId.".to_string(),
                )
            })?;
            let dedup_id = params
                .get("MessageDeduplicationId")
                .and_then(|v| v.as_str())
                .map(String::from)
                // Content-based dedup only when no explicit id was given; a
                // missing id is not a dedup key (an empty-string key would
                // wrongly dedup every message on a FIFO queue without
                // ContentBasedDeduplication).
                .or_else(|| {
                    if queue.attributes.content_based_deduplication {
                        Some(format!("{:x}", md5::compute(body.as_bytes())))
                    } else {
                        None
                    }
                });

            if let Some(dedup_id) = dedup_id {
                // Expire dedup entries older than 5 minutes, then check for a
                // duplicate. Entries are keyed by the first send and are NOT
                // refreshed by re-sends: within the window the original
                // message's response fields are returned, even if the message
                // itself was already received and deleted (retract-pending).
                queue.dedup_ids.retain(|_, entry| {
                    now_ms.saturating_sub(entry.sent_at) < Self::DEDUP_INTERVAL_MS
                });
                if let Some(entry) = queue.dedup_ids.get(&dedup_id) {
                    return Ok(Some(PreparedMessage::Duplicate {
                        message_id: entry.message_id.clone(),
                        md5_of_body: entry.md5_of_body.clone(),
                        sequence_number: entry.sequence_number.clone(),
                    }));
                }

                let sequence_number = queue
                    .next_sequence_number
                    .checked_add(1)
                    .unwrap_or(1)
                    .to_string();
                queue.next_sequence_number += 1;
                let message_id = Uuid::new_v4().to_string();
                queue.dedup_ids.insert(
                    dedup_id,
                    crate::models::DedupEntry {
                        message_id: message_id.clone(),
                        md5_of_body: md5_of_body.clone(),
                        sequence_number: Some(sequence_number.clone()),
                        sent_at: now_ms,
                    },
                );

                let mut attrs_map = HashMap::new();
                attrs_map.insert("MessageGroupId".to_string(), gid);
                attrs_map.insert("SequenceNumber".to_string(), sequence_number);
                let receipt_handle = Self::generate_receipt_handle(&message_id, queue_url);
                SqsMessage {
                    message_id,
                    body: body.to_string(),
                    md5_of_body,
                    receipt_handle,
                    sent_timestamp: now_ms,
                    visibility_until: None,
                    receive_count: 0,
                    first_receive_timestamp: None,
                    attributes: attrs_map,
                    message_attributes,
                    available_at: if effective_delay > 0 {
                        Some(now_ms + (effective_delay as u64 * 1000))
                    } else {
                        None
                    },
                    deduplication_id: None,
                }
            } else {
                // No dedup key: enqueue without consulting the dedup map.
                let sequence_number = queue
                    .next_sequence_number
                    .checked_add(1)
                    .unwrap_or(1)
                    .to_string();
                queue.next_sequence_number += 1;
                let message_id = Uuid::new_v4().to_string();

                let mut attrs_map = HashMap::new();
                attrs_map.insert("MessageGroupId".to_string(), gid);
                attrs_map.insert("SequenceNumber".to_string(), sequence_number);
                let receipt_handle = Self::generate_receipt_handle(&message_id, queue_url);
                SqsMessage {
                    message_id,
                    body: body.to_string(),
                    md5_of_body,
                    receipt_handle,
                    sent_timestamp: now_ms,
                    visibility_until: None,
                    receive_count: 0,
                    first_receive_timestamp: None,
                    attributes: attrs_map,
                    message_attributes,
                    available_at: if effective_delay > 0 {
                        Some(now_ms + (effective_delay as u64 * 1000))
                    } else {
                        None
                    },
                    deduplication_id: None,
                }
            }
        } else {
            let message_id = Uuid::new_v4().to_string();
            let attrs_map = HashMap::new();
            SqsMessage {
                message_id: message_id.clone(),
                body: body.to_string(),
                md5_of_body: md5_of_body.clone(),
                receipt_handle: Self::generate_receipt_handle(&message_id, queue_url),
                sent_timestamp: now_ms,
                visibility_until: None,
                receive_count: 0,
                first_receive_timestamp: None,
                attributes: attrs_map,
                message_attributes,
                available_at: if effective_delay > 0 {
                    Some(now_ms + (effective_delay as u64 * 1000))
                } else {
                    None
                },
                deduplication_id: None,
            }
        };
        Ok(Some(PreparedMessage::Accepted(Box::new(message))))
    }

    /// 5-minute deduplication interval in milliseconds.
    const DEDUP_INTERVAL_MS: u64 = 300_000;

    fn handle_send_message(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let queue_url = params
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
            })?;

        // moto passes the missing MessageBody into Message() where the md5
        // computation escapes as a werkzeug 500 crash (live-verified).
        let message_body = params
            .get("MessageBody")
            .and_then(|v| v.as_str())
            .ok_or(SqsError::MotoCrash)?;

        let queue_name = self.extract_queue_name_from_url(queue_url)?;
        let store = self.get_store(account, region);
        let queue_arc = store.get_queue(&queue_name).ok_or_else(|| {
            SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
        })?;

        let mut body = serde_json::Map::new();
        {
            let mut q = queue_arc.write();
            match self.prepare_send(&mut q, queue_url, message_body, params)? {
                Some(PreparedMessage::Accepted(message)) => {
                    let md5_attrs = Self::md5_of_message_attributes(&message.message_attributes);
                    let sequence_number = message.attributes.get("SequenceNumber").cloned();
                    let message_id = message.message_id.clone();
                    let md5_body = message.md5_of_body.clone();
                    q.send_message(*message);
                    body.insert("MessageId".into(), json!(message_id));
                    body.insert("MD5OfMessageBody".into(), json!(md5_body));
                    if let Some(seq) = sequence_number {
                        body.insert("SequenceNumber".into(), json!(seq));
                    }
                    if let Some(md5_attrs) = md5_attrs {
                        body.insert("MD5OfMessageAttributes".into(), json!(md5_attrs));
                    }
                }
                Some(PreparedMessage::Duplicate {
                    message_id,
                    md5_of_body,
                    sequence_number,
                }) => {
                    body.insert("MessageId".into(), json!(message_id));
                    body.insert("MD5OfMessageBody".into(), json!(md5_of_body));
                    if let Some(seq) = sequence_number {
                        body.insert("SequenceNumber".into(), json!(seq));
                    }
                }
                None => {}
            }
        }

        Ok(AwsResponse {
            status: 200,
            headers: Self::json_headers(),
            body: Value::Object(body).to_string(),
        })
    }

    fn handle_change_visibility_batch(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let queue_url = params
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        // moto's automated parameter parse int()s EVERY batch row's
        // VisibilityTimeout (moto/core/responses.py parse_parameters, the
        // entries carry an integer shape) BEFORE the response method runs:
        // one present-but-unparseable value anywhere in Entries crashes the
        // whole request as a werkzeug 500 — the backend's broad `except:`
        // around its own parse never gets to see that value (live-verified:
        // a lone "abc" row answers 500 HTML, and a [valid, junk] mix
        // crashes too, discarding the valid row). A MISSING key no longer
        // crashes here: the response-layer bare except catches its lookup
        // failure and answers the per-row Failed arm (live-verified shape).
        // A JSON null takes the same missing route — moto's parse drops
        // null members entirely (`BaseJSONParser._parse_structure` skips a
        // member whose value came off the wire as None; live-verified: a
        // batch row with VisibilityTimeout:null answers 200 with the same
        // Failed row, not the parse crash).
        // The scan sits before the batch-Id validation because the parse
        // precedes the whole response layer.
        if let Some(entries) = params.get("Entries").and_then(|v| v.as_array()) {
            for entry in entries {
                if matches!(
                    Self::moto_int(entry.get("VisibilityTimeout")),
                    MotoInt::Junk
                ) {
                    return Err(SqsError::MotoCrash);
                }
            }
        }

        // moto validates the batch Entries in the response layer before the
        // backend resolves the queue: EmptyBatchRequest is moto 5.2.4
        // response-layer behavior, and the batch-Id errors ride the same
        // layer (UNMERGED PR getmoto/moto#10346 at 5.2.4, ported
        // deliberately for real-AWS parity), so a malformed batch beats
        // QueueDoesNotExist.
        let entries =
            Self::validate_batch_entries(params, "ChangeMessageVisibilityBatchRequestEntry")?;

        let queue_name = self.extract_queue_name_from_url(&queue_url)?;
        let store = self.get_store(account, region);

        let mut successful: Vec<Value> = Vec::new();
        let mut failed: Vec<Value> = Vec::new();

        for entry in &entries {
            let id = entry
                .get("Id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            // moto's change_message_visibility_batch backend wraps the
            // per-entry `int(entry["VisibilityTimeout"])` parse and the
            // MAXIMUM_VISIBILITY_TIMEOUT assert in one bare except: a row
            // whose VisibilityTimeout is MISSING (KeyError inside the
            // arm) or above the cap answers a per-entry Failed row
            // (live-verified shape: SenderFault true / Code
            // InvalidParameterValue / Message "Visibility timeout
            // invalid"; a row carrying NEITHER key fails this arm first,
            // live-verified: Entries=[{"Id":"m1"}] answers 200 with the
            // Failed row), and a u64-overflowing value takes the same
            // row. Negatives parse and PASS the assert (moto 5.2.4, live
            // verified: a "-5" row succeeds on a valid handle and the
            // message becomes visible immediately). This arm runs BEFORE
            // the ReceiptHandle lookup below is ever touched.
            // The batch's parse-layer pre-scan above guarantees the row's
            // VisibilityTimeout is either absent or int()-parseable by
            // the time this loop runs, so the Junk variant cannot appear
            // here: a row whose key is missing (or a dropped null) fails
            // moto's broad `except:` arm like an over-cap value, and a
            // u64-overflowing value takes the same row.
            let visibility = match Self::moto_int(entry.get("VisibilityTimeout")) {
                MotoInt::Parsed { value, .. } if value <= Self::MAX_VISIBILITY_TIMEOUT as i64 => {
                    value
                }
                _ => {
                    failed.push(json!({
                        "Id": id,
                        "SenderFault": true,
                        "Code": "InvalidParameterValue",
                        "Message": "Visibility timeout invalid",
                    }));
                    continue;
                }
            };
            // moto's backend reads entry["ReceiptHandle"] STRICTLY, after
            // the per-row visibility arm: a row without the key raises
            // KeyError inside the batch loop — OUTSIDE the broad `except:`
            // guarding only the visibility parse — and crashes the whole
            // request as a werkzeug 500 (live-verified, and it precedes
            // the per-row queue lookup so a ghost queue keeps the crash).
            // A null member never reaches that lookup as a value either:
            // moto's parse layer drops null members outright, so a row
            // carrying ReceiptHandle:null answers the SAME KeyError crash
            // (live-verified; the twin previously steered the null into
            // moto's None-mode match, mutating the first message's
            // visibility — a state change moto never performs here).
            // Other non-string values can never match moto's membership
            // test and take the per-row ReceiptHandleIsInvalid arm.
            let receipt_handle: Option<&str> = match entry.get("ReceiptHandle") {
                Some(Value::String(s)) => Some(s.as_str()),
                None | Some(Value::Null) => return Err(SqsError::MotoCrash),
                Some(_) => Some(""),
            };
            // moto resolves the queue only inside the per-row backend call
            // (`change_message_visibility` -> `get_queue`,
            // moto/sqs/models.py:1082): QueueDoesNotExist propagates out of
            // the WHOLE batch as the global NonExistentQueue error and
            // discards the Failed rows already collected for earlier
            // parse-failing entries (live-verified: a [over-cap, valid]
            // ghost-queue batch answers 400 NonExistentQueue, not the
            // earlier row). Rows that fail the per-entry parse or the cap
            // assert never reach that lookup, so they answer 200 Failed
            // rows even on a nonexistent queue (live-verified: an
            // over-cap-only / missing-VisibilityTimeout ghost-queue batch
            // answers 200 — the queue is never resolved; the twin's
            // previous eager resolution answered NonExistentQueue there).
            let queue_arc = store.get_queue(&queue_name).ok_or_else(|| {
                SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
            })?;

            let mut q = queue_arc.write();
            // moto's inner change_message_visibility stores visible_at in
            // the past for negative timeouts (live-verified: a "-5" row
            // succeeds and the message is re-receivable immediately); the
            // store cannot represent a past point, so the checked apply
            // clamps negatives to 0 — the same outcome.
            //
            // The TOTAL-VisibilityTimeout rule inside the inner call is
            // NOT caught by moto's batch loop (it catches only
            // ReceiptHandleIsInvalid): a rule breach raises out of
            // change_message_visibility_batch and fails the WHOLE request
            // with the global 400 InvalidParameterValue envelope
            // (live-verified; already-collected Failed rows are
            // discarded).
            match q.change_message_visibility_checked(receipt_handle, visibility) {
                Ok(()) => successful.push(json!({ "Id": id })),
                // moto rows per-entry ReceiptHandleIsInvalid with
                // SenderFault true and the ServiceException message
                // `getattr(e, "message", "")` yields
                // (moto/sqs/models.py:1128-1136; live-verified).
                Err(SqsError::ReceiptHandleIsInvalid(_)) => {
                    failed.push(json!({
                        "Id": id,
                        "SenderFault": true,
                        "Code": "ReceiptHandleIsInvalid",
                        "Message": "The input receipt handle is invalid."
                    }));
                }
                // Total-rule InvalidParameterValue: propagate as the
                // whole-request 400, like moto's un-caught raise.
                Err(err) => return Err(err),
            }
        }

        let body = json!({
            "Successful": successful,
            "Failed": failed
        })
        .to_string();

        Ok(AwsResponse {
            status: 200,
            headers: vec![(
                "Content-Type".to_string(),
                "application/x-amz-json-1.0".to_string(),
            )],
            body,
        })
    }

    fn handle_delete_message_batch(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let queue_url = params
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        // moto answers an empty Entries list with EmptyBatchRequest before
        // touching the queue (live-verified wording), then the shared helper
        // runs the batch-Id validation (UNMERGED PR getmoto/moto#10346 at
        // moto 5.2.4; real-AWS-better for DELETE/VIS) — resolve the queue
        // only after those batch errors could fire.
        let entries = Self::validate_batch_entries(params, "DeleteMessageBatchRequestEntry")?;

        // moto responses.py delete_message_batch (pinned 5.2.4): a
        // ReceiptHandle seen twice across entries answers
        // BatchEntryIdsNotDistinct carrying the SECOND entry's Id ("Id b
        // repeated."), still before the backend resolves the queue
        // (live-verified: a repeated handle on a nonexistent queue answers
        // the batch error, not NonExistentQueue). PR #10346's Id-distinct
        // check above stays on top of this receipt-distinct check.
        let mut seen_handles: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for entry in &entries {
            let receipt_handle = entry
                .get("ReceiptHandle")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if !seen_handles.insert(receipt_handle) {
                let id = entry.get("Id").and_then(|v| v.as_str()).unwrap_or("");
                return Err(SqsError::BatchEntryIdsNotDistinct(format!(
                    "Id {id} repeated."
                )));
            }
        }

        let queue_name = self.extract_queue_name_from_url(&queue_url)?;
        let store = self.get_store(account, region);
        let queue_opt = store.get_queue(&queue_name);
        let queue_arc = queue_opt.ok_or_else(|| {
            SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
        })?;

        let mut successful: Vec<Value> = Vec::new();
        let mut failed: Vec<Value> = Vec::new();

        for entry in &entries {
            let id = entry
                .get("Id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let receipt_handle = entry
                .get("ReceiptHandle")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let mut q = queue_arc.write();
            if q.delete_message(&receipt_handle).is_ok() {
                successful.push(json!({ "Id": id }));
            } else {
                failed.push(json!({
                    "Id": id,
                    "Code": "ReceiptHandleIsInvalid",
                    "Message": "The specified receipt handle is not valid."
                }));
            }
        }

        let body = json!({
            "Successful": successful,
            "Failed": failed
        })
        .to_string();

        Ok(AwsResponse {
            status: 200,
            headers: vec![(
                "Content-Type".to_string(),
                "application/x-amz-json-1.0".to_string(),
            )],
            body,
        })
    }

    fn handle_receive_message(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        // A PRESENT-but-unparseable VisibilityTimeout crashes the receive:
        // moto's automated parse layer int()s the model-declared integer at
        // setup — before the response method touches the queue at all — so
        // the ValueError escapes as a plain werkzeug 500 (live-verified:
        // "abc" on an EXISTING queue and on a nonexistent one both answer
        // the 500 HTML page instead of NonExistentQueue). A MISSING value
        // does not crash: the response layer catches the fallback TypeError
        // and applies the queue's attribute (live-verified: absent on a
        // nonexistent queue answers NonExistentQueue like every resolve).
        // A JSON null takes the same absent path — moto's parse layer drops
        // null members entirely (`BaseJSONParser._parse_structure` skips a
        // member whose value came off the wire as None, live-verified:
        // receive with VisibilityTimeout:null answers 200 with the queue
        // default). Numeric strings keep the int() coercion of both
        // protocols.
        if matches!(
            Self::moto_int(params.get("VisibilityTimeout")),
            MotoInt::Junk
        ) || matches!(
            Self::moto_int(params.get("MaxNumberOfMessages")),
            MotoInt::Junk
        ) || matches!(Self::moto_int(params.get("WaitTimeSeconds")), MotoInt::Junk)
        {
            // moto's automated parse layer int()s EVERY model-declared
            // Integer member in one pass before the response method runs,
            // so junk in any of the three crashes ahead of the queue
            // resolution (live-verified: junk Max/Wait on a ghost queue
            // answers the 500, not NonExistentQueue).
            return Err(SqsError::MotoCrash);
        }

        let queue_url = params
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
            })?;

        // Extract queue name from URL
        let queue_name = self.extract_queue_name_from_url(queue_url)?;

        let store = self.get_store(account, region);
        let queue_arc = store.get_queue(&queue_name).ok_or_else(|| {
            SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
        })?;

        // moto validates the bounds in its RESPONSE layer, after the queue
        // resolution (responses.py receive_message calls get_queue before
        // touching the parameters): an out-of-range MaxNumberOfMessages on
        // a nonexistent queue answers NonExistentQueue first (live-
        // verified), as does WaitTimeSeconds=25. The 1..=10 bounds fire
        // before the wait bounds, both before the VisibilityTimeout cap
        // (live-verified: Max=0 with Wait=25 answers the Max error; Wait=25
        // with an over-cap VisibilityTimeout answers the Wait error). The
        // message wordings are moto's exact strings (live-verified), the
        // wait one including its html-escaped oddity, and the parsed
        // numeric shapes are the model-parser ones (numbers, numeric
        // strings; a JUNK value crashes ahead of the queue resolution in
        // the parse layer — pinned separately — which is why `_parse_
        // integer`'s TypeError/ValueError surfaces before NonExistentQueue
        // here, live-verified).
        let max_requested = match Self::moto_int(params.get("MaxNumberOfMessages")) {
            MotoInt::Junk => return Err(SqsError::MotoCrash),
            MotoInt::Parsed { value, echo } => Some((value, echo)),
            MotoInt::Absent => None,
        };
        // moto interpolates the parsed python integer into this wording —
        // the canonical str(int()) digits, NOT an i64-saturated value
        // (live-verified: "Value 999999999999999999999999999999 ..." with
        // a 30-digit member; "Value 18446744073709551616" through the u64
        // range; " -3 " echoes the canonical -3; "٣٠" echoes ASCII 30).
        if let Some((v, echo)) = &max_requested {
            if !(1..=10).contains(v) {
                return Err(SqsError::InvalidParameterValue(format!(
                    "An error occurred (InvalidParameterValue) when calling the \
                     ReceiveMessage operation: Value {echo} for parameter \
                     MaxNumberOfMessages is invalid. Reason: must be between 1 and \
                     10, if provided."
                )));
            }
        }
        let max_messages = max_requested.map_or(1, |(v, _)| v).min(10) as usize;

        // Long poll: WaitTimeSeconds wins, else the queue-level attribute.
        // The bounds apply to whichever wins — moto validates the queue
        // attribute too (live-verified: ReceiveMessageWaitTimeSeconds=25 on
        // the queue makes a plain receive answer the WaitTimeSeconds error
        // with Value 25). Junk crashes at the top with its sibling, and
        // the wording carries the same canonical echo (live-verified:
        // a 21-digit member echoes its full digits).
        let wait_requested = match Self::moto_int(params.get("WaitTimeSeconds")) {
            MotoInt::Junk => return Err(SqsError::MotoCrash),
            MotoInt::Parsed { value, echo } => Some((value, echo)),
            MotoInt::Absent => None,
        };
        let queue_wait = queue_arc.read().attributes.receive_message_wait_time as i64;
        let wait_seconds = wait_requested.as_ref().map_or(queue_wait, |(v, _)| *v);
        if !(0..=20).contains(&wait_seconds) {
            let echo = wait_requested
                .as_ref()
                .map(|(_, echo)| echo.clone())
                .unwrap_or_else(|| queue_wait.to_string());
            return Err(SqsError::InvalidParameterValue(format!(
                "An error occurred (InvalidParameterValue) when calling the \
                 ReceiveMessage operation: Value {echo} for parameter \
                 WaitTimeSeconds is invalid. Reason: must be &lt;= 0 and &gt;= 20 \
                 if provided."
            )));
        }
        let wait_seconds = wait_seconds as u64;

        // moto validates the requested VisibilityTimeout in its response
        // layer (`_get_validated_visibility_timeout` → MaxVisibilityTimeout,
        // moto/sqs/responses.py receive_message) AFTER the queue lookup AND
        // after the MaxNumberOfMessages/WaitTimeSeconds bounds, but
        // BEFORE the backend receive/long-poll: an over-cap value answers
        // 400 InvalidParameterValue "Invalid request, maximum visibility
        // timeout is 43200" (live-verified: receive_message with
        // VisibilityTimeout=50000 → 400 with that exact wording; 43200
        // passes at the boundary, 43201 does not), while the same request
        // on a NONEXISTENT queue answers NonExistentQueue first
        // (live-verified) — so this guard sits after the queue resolution.
        // moto accepts negative timeouts on this path (they pass the cap
        // comparison and Message.change_visibility applies them); the
        // twin's application below keeps treating a negative as the
        // parameter being absent (queue default), a recorded divergence.
        match Self::moto_int(params.get("VisibilityTimeout")) {
            MotoInt::Junk => return Err(SqsError::MotoCrash), // unreachable: crashed above
            MotoInt::Parsed { value, .. } if value > Self::MAX_VISIBILITY_TIMEOUT as i64 => {
                return Err(SqsError::InvalidParameterValue(
                    Self::MAX_VISIBILITY_TIMEOUT_MESSAGE.to_string(),
                ));
            }
            _ => {}
        }

        let mut messages;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(wait_seconds);
        // Visibility override from the request, if any; otherwise the
        // queue attribute, read under the queue lock during the receive.
        // The same numeric shapes moto's parser coerces count here
        // (numbers, numeric strings, booleans as int(True/False)); with
        // the parse-layer crash above, only numeric values reach this
        // point. Negatives take the recorded divergence (the parameter
        // counts as absent) once again.
        let params_visibility = match Self::moto_int(params.get("VisibilityTimeout")) {
            MotoInt::Parsed { value, .. } if value >= 0 => Some(value as u32),
            _ => None,
        };
        loop {
            messages = {
                // Redrive policy and DLQ handle are re-resolved on every
                // poll so attribute changes apply without restarting.
                let (max_receive, dlq_arn) = {
                    let q = queue_arc.read();
                    (q.redrive_max_receive_count(), q.redrive_target_arn())
                };
                // Resolve the DLQ handle before taking any queue lock: the
                // store lookup reads every queue's lock while held, so it
                // must complete before the source queue is locked and the
                // pair taken in canonical name order.
                let dlq_arc = dlq_arn
                    .as_deref()
                    .and_then(|arn| store.get_queue_by_arn(arn));
                // A self-target RedrivePolicy has same queue on both ends;
                // skipping the nested same-queue lock keeps it a no-op
                // instead of a dequeue wedge.
                let redrive = dlq_arc.and_then(|dl| {
                    if Arc::ptr_eq(&dl, &queue_arc) {
                        return None;
                    }
                    max_receive.map(|max| (max, dl))
                });
                if let Some((max_receive, dlq)) = redrive {
                    // Receive and the DLQ redrive move happen under ONE
                    // critical section on both queues: with
                    // VisibilityTimeout=0 the message is visible again
                    // inside this same lock, so redriving it out of scope
                    // lets a concurrent poll receive the bumped count too
                    // and double-move it (the second removal would
                    // silently no-op). Both write locks are taken in
                    // canonical queue-name order so a mirrored receive on
                    // the same pair cannot deadlock on nested acquisition.
                    let src_first = queue_arc.read().name.clone() <= dlq.read().name.clone();
                    let (mut src, mut dl) = if src_first {
                        let src = queue_arc.write();
                        let dl = dlq.write();
                        (src, dl)
                    } else {
                        let dl = dlq.write();
                        let src = queue_arc.write();
                        (src, dl)
                    };
                    super::bridge::drain_pending(account, region, &queue_name, &mut src);
                    let visibility_timeout =
                        params_visibility.unwrap_or(src.attributes.visibility_timeout);
                    let received = src.receive_messages(max_messages, visibility_timeout);
                    let mut stayed = Vec::with_capacity(received.len());
                    for msg in received {
                        if msg.receive_count > max_receive {
                            // Real SQS preserves the original MessageId
                            // across the dead-letter move; only the receipt
                            // handle is re-minted for the DLQ scope.
                            let mut moved = msg.clone();
                            moved.receipt_handle =
                                Self::generate_receipt_handle(&moved.message_id, &dl.url);
                            moved.receive_count = 0;
                            moved.first_receive_timestamp = None;
                            moved.visibility_until = None;
                            moved.available_at = None;
                            dl.send_message(moved);
                            src.remove_by_receipt_handle(&msg.receipt_handle);
                        } else {
                            stayed.push(msg);
                        }
                    }
                    stayed
                } else {
                    // No usable redrive target, or policy absent: plain
                    // receive. Exhausted messages still leave the source
                    // when a dead-letter move is configured, matching the
                    // no-DLQ-queue case (they are dropped, not delivered).
                    let mut q = queue_arc.write();
                    // Deliver events queued by cross-service callers (SNS/EventBridge)
                    // before each receive attempt so messages arrive during long polls.
                    super::bridge::drain_pending(account, region, &queue_name, &mut q);
                    let visibility_timeout =
                        params_visibility.unwrap_or(q.attributes.visibility_timeout);
                    let received = q.receive_messages(max_messages, visibility_timeout);
                    if let Some(max_receive) = max_receive {
                        let mut stayed = Vec::with_capacity(received.len());
                        for msg in received {
                            if msg.receive_count > max_receive {
                                q.remove_by_receipt_handle(&msg.receipt_handle);
                            } else {
                                stayed.push(msg);
                            }
                        }
                        stayed
                    } else {
                        received
                    }
                }
            };
            if !messages.is_empty() || std::time::Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        let msg_objs: Vec<Value> = messages
            .into_iter()
            .map(|msg| {
                let mut obj = json!({
                    "MessageId": msg.message_id,
                    "ReceiptHandle": msg.receipt_handle,
                    "MD5OfBody": msg.md5_of_body,
                    "Body": msg.body,
                    "Attributes": msg.attributes,
                });
                let map = obj.as_object_mut().unwrap();
                if !msg.message_attributes.is_empty() {
                    map.insert(
                        "MD5OfMessageAttributes".into(),
                        json!(Self::md5_of_message_attributes(&msg.message_attributes)
                            .unwrap_or_default()),
                    );
                    map.insert(
                        "MessageAttributes".into(),
                        Value::Object(msg.message_attributes.clone().into_iter().collect()),
                    );
                }
                obj
            })
            .collect();

        let body = if msg_objs.is_empty() {
            Value::Object(serde_json::Map::new()).to_string()
        } else {
            json!({
                "Messages": msg_objs
            })
            .to_string()
        };

        Ok(AwsResponse {
            status: 200,
            headers: Self::json_headers(),
            body,
        })
    }

    fn handle_delete_message(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let queue_url = params
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
            })?;

        // moto: parsing a None ReceiptHandle inside the werkzeug handler
        // escapes as an unhandled error (werkzeug plain 500 crash).
        let receipt_handle = params
            .get("ReceiptHandle")
            .and_then(|v| v.as_str())
            .ok_or(SqsError::MotoCrash)?;

        // Extract queue name from URL
        let queue_name = self.extract_queue_name_from_url(queue_url)?;

        let store = self.get_store(account, region);
        let queue_opt = store.get_queue(&queue_name);
        let queue_arc = queue_opt.ok_or_else(|| {
            SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
        })?;

        {
            let mut q = queue_arc.write();
            q.delete_message(receipt_handle)
                .map_err(SqsError::ReceiptHandleIsInvalid)?;
        }

        let body = json!({}).to_string();

        Ok(AwsResponse {
            status: 200,
            headers: vec![(
                "Content-Type".to_string(),
                "application/x-amz-json-1.0".to_string(),
            )],
            body,
        })
    }

    fn handle_delete_queue(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let queue_url = params
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
            })?;

        // Extract queue name from URL
        let queue_name = self.extract_queue_name_from_url(queue_url)?;

        let store = self.get_store(account, region);
        // Remove the queue entry and drop its pending cross-service
        // deliveries inside the same map-write critical section: a bridge
        // producer buffering in between the two steps would either be wiped
        // after the entry is already gone or leave a stale delivery behind
        // for a re-created queue of the same name.
        store.with_queues_write(|queues| {
            queues.remove(&queue_name);
            super::bridge::clear_pending(account, region, &queue_name);
        });

        let body = json!({}).to_string();

        Ok(AwsResponse {
            status: 200,
            headers: vec![(
                "Content-Type".to_string(),
                "application/x-amz-json-1.0".to_string(),
            )],
            body,
        })
    }

    fn handle_list_queues(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let store = self.get_store(account, region);
        let queue_names = store.list_queues();

        let prefix = params.get("QueueNamePrefix").and_then(|v| v.as_str());

        let urls: Vec<String> = queue_names
            .into_iter()
            .filter(|name| prefix.is_none_or(|p| name.starts_with(p)))
            .map(|name| {
                format!(
                    "http://sqs.{}.localhost.robotocore.cloud:{}/{}/{}",
                    region,
                    Self::queue_host_port(),
                    account,
                    name
                )
            })
            .collect();

        let body = json!({
            "QueueUrls": urls
        })
        .to_string();

        Ok(AwsResponse {
            status: 200,
            headers: vec![(
                "Content-Type".to_string(),
                "application/x-amz-json-1.0".to_string(),
            )],
            body,
        })
    }

    fn handle_get_queue_url(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let queue_name = params
            .get("QueueName")
            .and_then(|v| v.as_str())
            // moto's get_queue_url(None) resolves to a missing queue.
            .ok_or_else(|| {
                SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
            })?;

        let store = self.get_store(account, region);
        store.get_queue(queue_name).ok_or_else(|| {
            SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
        })?;

        let url = format!(
            "http://sqs.{}.localhost.robotocore.cloud:{}/{}/{}",
            region,
            Self::queue_host_port(),
            account,
            queue_name
        );

        let body = json!({
            "QueueUrl": url
        })
        .to_string();

        Ok(AwsResponse {
            status: 200,
            headers: vec![(
                "Content-Type".to_string(),
                "application/x-amz-json-1.0".to_string(),
            )],
            body,
        })
    }

    fn handle_get_queue_attributes(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let queue_url = params
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
            })?;

        // Extract queue name from URL
        let queue_name = self.extract_queue_name_from_url(queue_url)?;

        let store = self.get_store(account, region);
        let queue_opt = store.get_queue(&queue_name);
        let queue_arc = queue_opt.ok_or_else(|| {
            SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
        })?;

        let mut attrs = serde_json::Map::new();
        let q = queue_arc.read();
        let (visible, inflight, delayed) = q.message_counts();

        attrs.insert(
            "VisibilityTimeout".to_string(),
            Value::String(q.attributes.visibility_timeout.to_string()),
        );
        attrs.insert(
            "MessageRetentionPeriod".to_string(),
            Value::String(q.attributes.message_retention_period.to_string()),
        );
        attrs.insert(
            "MaximumMessageSize".to_string(),
            Value::String(q.attributes.maximum_message_size.to_string()),
        );
        attrs.insert(
            "DelaySeconds".to_string(),
            Value::String(q.attributes.delay_seconds.to_string()),
        );
        attrs.insert(
            "ReceiveMessageWaitTimeSeconds".to_string(),
            Value::String(q.attributes.receive_message_wait_time.to_string()),
        );
        attrs.insert(
            "CreatedTimestamp".to_string(),
            Value::String((q.created / 1000).to_string()),
        );
        attrs.insert(
            "LastModifiedTimestamp".to_string(),
            Value::String((q.last_modified / 1000).to_string()),
        );
        attrs.insert("QueueArn".to_string(), Value::String(q.arn.clone()));
        attrs.insert(
            "ApproximateNumberOfMessages".to_string(),
            Value::String(visible.to_string()),
        );
        attrs.insert(
            "ApproximateNumberOfMessagesNotVisible".to_string(),
            Value::String(inflight.to_string()),
        );
        attrs.insert(
            "ApproximateNumberOfMessagesDelayed".to_string(),
            Value::String(delayed.to_string()),
        );
        if let Some(policy) = &q.attributes.policy {
            attrs.insert("Policy".to_string(), Value::String(policy.clone()));
        }
        if let Some(redrive) = &q.attributes.redrive_policy {
            attrs.insert("RedrivePolicy".to_string(), Value::String(redrive.clone()));
        }
        for (key, value) in &q.attributes.extra {
            attrs.insert(key.clone(), Value::String(value.clone()));
        }
        if q.attributes.fifo_queue {
            attrs.insert("FifoQueue".to_string(), Value::String("true".to_string()));
            attrs.insert(
                "ContentBasedDeduplication".to_string(),
                Value::String(
                    if q.attributes.content_based_deduplication {
                        "true"
                    } else {
                        "false"
                    }
                    .to_string(),
                ),
            );
        }

        let body = json!({
            "Attributes": attrs
        })
        .to_string();

        Ok(AwsResponse {
            status: 200,
            headers: Self::json_headers(),
            body,
        })
    }

    fn extract_queue_name_from_url(&self, url: &str) -> Result<String, SqsError> {
        // URL format: http://sqs.{region}.localhost.robotocore.cloud:4566/{account_id}/{queue_name}
        // Or: http://sqs.{region}.localhost.robotocore.cloud:4566/{account_id}/{queue_name}.fifo

        if let Some(last_slash) = url.rfind('/') {
            let queue_part = &url[last_slash + 1..];
            if !queue_part.is_empty() {
                return Ok(queue_part.to_string());
            }
        }

        // moto never fails during QueueUrl parsing: it always resolves the
        // queue_name (however malformed) and answers a missing queue.
        Err(SqsError::NonExistentQueue(
            "The specified queue does not exist.".to_string(),
        ))
    }

    fn handle_set_queue_attributes(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let queue_url = params
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
            })?;

        let queue_name = self.extract_queue_name_from_url(queue_url)?;
        let store = self.get_store(account, region);
        let queue_arc = store.get_queue(&queue_name).ok_or_else(|| {
            SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
        })?;

        if let Some(attrs) = params.get("Attributes") {
            let mut q = queue_arc.write();
            q.attributes.apply(attrs);
            q.last_modified = (Utc::now().timestamp_millis()) as u64;
        }

        let body = json!({}).to_string();
        Ok(AwsResponse {
            status: 200,
            headers: Self::json_headers(),
            body,
        })
    }

    fn handle_change_message_visibility(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        // moto crashes on BOTH a missing and a present-but-unparseable
        // VisibilityTimeout in this op (live-verified against the pinned
        // fork: `int("abc")` ValueError inside the automated parse layer
        // and the `None > 43200` TypeError inside
        // `_get_validated_visibility_timeout` both escape as a plain
        // werkzeug 500 HTML page). The crash precedes the entire response
        // layer — no queue resolution has happened yet: a junk value on a
        // nonexistent queue answers 500 instead of NonExistentQueue, and a
        // missing QueueUrl with a missing VisibilityTimeout answers 500
        // too — so the parse sits before any queue/URL handling here.
        // Numeric strings are honored (moto's model parser coerces the
        // query-protocol form values the same way int() does).
        // Numeric strings are honored (moto's model parser coerces the
        // query-protocol form values the same way int() does); a dropped
        // null reaches the op as a missing value and takes the same moto
        // 500 (live-verified).
        let timeout = match Self::moto_int(params.get("VisibilityTimeout")) {
            MotoInt::Parsed { value, .. } => value,
            _ => return Err(SqsError::MotoCrash),
        };
        // moto validates the visibility timeout in the RESPONSE layer
        // (`_get_validated_visibility_timeout` → MaxVisibilityTimeout)
        // before the backend resolves the queue or matches the receipt
        // handle, so an over-cap value beats even a nonexistent queue
        // (live-verified). Negative values pass the response-layer cap in
        // moto too; the Total-VisibilityTimeout rule below takes them
        // signed.
        if timeout > Self::MAX_VISIBILITY_TIMEOUT as i64 {
            return Err(SqsError::InvalidParameterValue(
                Self::MAX_VISIBILITY_TIMEOUT_MESSAGE.to_string(),
            ));
        }

        let queue_url = params
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
            })?;

        // The ReceiptHandle resolution mirrors moto's split (re-adjudicated
        // live against the pinned fork, replacing the round-1 MotoCrash
        // pin): a MISSING handle travels to the backend as None, and
        // `None in all_receipt_handles` matches EVERY extant moto Message
        // (the pre-receive handle slot is None), so the FIRST message in
        // queue order wins and the bump applies to it — 200 OK
        // (live-verified, including the total rule on that message and the
        // empty queue answering ReceiptHandleIsInvalid). A non-string
        // handle value (a raw-JSON corner) can never match either.
        let receipt_handle: Option<&str> = match params.get("ReceiptHandle") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(s.as_str()),
            Some(_) => Some(""),
        };

        let queue_name = self.extract_queue_name_from_url(queue_url)?;
        let store = self.get_store(account, region);
        let queue_arc = store.get_queue(&queue_name).ok_or_else(|| {
            SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
        })?;

        // moto's backend change_message_visibility runs the
        // Total-VisibilityTimeout rule on the matched message before
        // applying (live-verified: a fresh message at the 43200 boundary
        // answers the Total rule, not the static cap, because the
        // message age alone puts the total beyond the limit; the
        // "handle resolved but rule breached" answer carries the raw
        // timeout value in moto's exact framing). ReceiptHandleIsInvalid
        // (no message matches) serializes through the normal error table.
        {
            let mut q = queue_arc.write();
            q.change_message_visibility_checked(receipt_handle, timeout)?;
        }

        let body = json!({}).to_string();
        Ok(AwsResponse {
            status: 200,
            headers: vec![(
                "Content-Type".to_string(),
                "application/x-amz-json-1.0".to_string(),
            )],
            body,
        })
    }

    fn handle_purge_queue(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let queue_url = params
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
            })?;

        let queue_name = self.extract_queue_name_from_url(queue_url)?;
        let store = self.get_store(account, region);

        // Purge under the same map-write critical section as DeleteQueue:
        // no bridged producer can re-buffer a pending delivery between the
        // pending clear and the stored-message purge. The 60-second
        // re-purge throttle (moto's purge_queue) rejects the whole
        // operation, leaving messages and pending buffer untouched.
        let purged = store.with_queues_write(|queues| match queues.get(&queue_name) {
            Some(queue) => {
                queue.write().purge_with_throttle()?;
                super::bridge::clear_pending(account, region, &queue_name);
                Ok(())
            }
            None => Err(SqsError::NonExistentQueue(
                "The specified queue does not exist.".to_string(),
            )),
        });
        purged?;

        let body = json!({}).to_string();
        Ok(AwsResponse {
            status: 200,
            headers: Self::json_headers(),
            body,
        })
    }

    /// `MAXIMUM_VISIBILITY_TIMEOUT` (moto/sqs/constants.py): the cap the
    /// response layer applies to every ChangeMessageVisibility input.
    /// A value above it answers InvalidParameterValue, not the queue or
    /// handle checks (live-verified: a nonexistent queue with an
    /// over-cap timeout still answers the cap error).
    const MAX_VISIBILITY_TIMEOUT: u64 = 43200;

    /// `MAXIMUM_MESSAGE_LENGTH` (moto/sqs/constants.py): the aggregate
    /// entry-body ceiling the send_message_batch backend enforces
    /// (`BatchRequestTooLong`). moto also reports this same constant as
    /// the default `MaximumMessageSize` queue attribute
    /// (`moto/sqs/models.py` Queue defaults, live-verified: a fresh
    /// GetQueueAttributes answers 1048576).
    const MAXIMUM_BATCH_BODY_LENGTH: usize = 1_048_576;

    /// moto exceptions.MaxVisibilityTimeout.message (live-verified).
    const MAX_VISIBILITY_TIMEOUT_MESSAGE: &str =
        "Invalid request, maximum visibility timeout is 43200";

    /// A VisibilityTimeout-family member (`VisibilityTimeout`,
    /// `MaxNumberOfMessages`, `WaitTimeSeconds`) as moto's automated parse
    /// layer sees it. The doctrine it encodes:
    ///
    /// - **absent-or-null → absent.** The key missing and the member
    ///   carrying a JSON null are the same thing: moto's request parser
    ///   drops null members outright (`BaseJSONParser._parse_structure`
    ///   skips a member whose wire value parses to None — live-verified),
    ///   so the response layer falls back to the queue's attribute or its
    ///   defaults (receive), or crashes on the MISSING value where moto's
    ///   own code does (`None > 43200` TypeError in top-level
    ///   ChangeMessageVisibility — the dropped null reproduces exactly).
    /// - **""/junk → the moto-crash mirror.** `_parse_integer` int()s every
    ///   surviving member (`int("")` → ValueError; live-verified on
    ///   receive, CMV, batch rows and the receive bounds) and a non-string
    ///   non-numeric shape (`{"x": 1}`, `[1]`) raises TypeError/ValueError
    ///   there too: the request dies as a plain werkzeug 500 before any
    ///   response method runs — the batch row arm's broad `except:` only
    ///   produces a Failed entry when the KEY is absent, never for junk
    ///   (the parse layer has already discarded it).
    /// - **python int() coercions are honored** (see `python_int_str`):
    ///   surrounding whitespace and unicode Nd digits strip/map
    ///   (`int(" 30 ")` = 30, `int("٣٠")` = 30 — live-verified), PEP 515
    ///   underscores parse only between digits (`int("1_0")` = 10,
    ///   `int("٣_٠")` = 30, `int("_30")` = ValueError → the moto crash),
    ///   booleans coerce (`int(True)` = 1 — live-verified), a leading sign
    ///   is honored and negatives apply (moto's `Message.change_visibility`
    ///   stores `visible_at` in the past; the call sites clamp negatives
    ///   to 0 because the u64-typed store cannot represent a past point),
    ///   and float members truncate (`int(3.5)` = 3).
    /// - **saturation only for beyond-i64 magnitudes.** Python's int() is
    ///   unbounded, but the API only ever compares the value against small
    ///   bounds or the 43200 cap, so the comparison value saturates at the
    ///   i64 range: a positive overflow stays cap/bounds-breaching and a
    ///   negative overflow behaves like every negative. The *echo* — the
    ///   digits moto interpolates into its error messages (live-verified:
    ///   `Value 999999999999999999999999999999 for parameter
    ///   MaxNumberOfMessages is invalid`) — is not the saturated value:
    ///   `Parsed` captures `echo: str(int(value))`,
    ///   saturated comparisons aside.
    ///
    /// Any numeral text runs through python's int() (`python_int_str`),
    /// so the value surface is identical across the native JSON numbers
    /// and the gateway's query-protocol strings. A JSON number beyond the
    /// u64 range arrives in serde as a float that already lost the
    /// literal's digit fidelity — python's json keeps the exact integer —
    /// so those echoes are a recorded divergence (the digits come back
    /// rounded); every integer ≤ u64::MAX and every string carries moto's
    /// exact bytes.
    fn moto_int(param: Option<&Value>) -> MotoInt {
        match param {
            None | Some(Value::Null) => MotoInt::Absent,
            Some(Value::Number(n)) => {
                if n.is_f64() {
                    // python int() truncates floats (int(3.5) == 3); the
                    // echo backs off to the decimal expansion of the
                    // truncated integer while it fits in 128 bits.
                    let t = n.as_f64().unwrap_or(0.0).trunc();
                    if t.abs() < 1.0e38 {
                        let expanded = t as i128;
                        MotoInt::Parsed {
                            value: expanded.clamp(i64::MIN as i128, i64::MAX as i128) as i64,
                            echo: expanded.to_string(),
                        }
                    } else {
                        // beyond the 128-bit expansion neither serde nor
                        // this mirror can reproduce python's digits.
                        MotoInt::Parsed {
                            value: if t < 0.0 { i64::MIN } else { i64::MAX },
                            echo: n.to_string(),
                        }
                    }
                } else if let Some(i) = n.as_i64() {
                    MotoInt::Parsed {
                        value: i,
                        echo: i.to_string(),
                    }
                } else {
                    // Positive beyond i64::MAX (serde kept the u64): an
                    // unbounded python int() — saturate the comparison,
                    // echo the real digits (live-verified through the u64
                    // range: Value 18446744073709551615).
                    MotoInt::Parsed {
                        value: i64::MAX,
                        echo: n.to_string(),
                    }
                }
            }
            // Python's int(True) == 1 / int(False) == 0: moto's model
            // parser runs the same coercion on a boolean JSON member
            // (live-verified: ReceiveMessage with VisibilityTimeout=true
            // answers 200, ChangeMessageVisibility with true passes the
            // cap as vis=1 and reaches the backend).
            Some(Value::Bool(b)) => {
                let value = i64::from(*b);
                MotoInt::Parsed {
                    value,
                    echo: value.to_string(),
                }
            }
            Some(Value::String(s)) => match Self::python_int_str(s) {
                Some((value, echo)) => MotoInt::Parsed { value, echo },
                None => MotoInt::Junk,
            },
            // Objects and arrays raise TypeError inside python int(): the
            // same parse-layer 500 (live-verified on receive and
            // ChangeMessageVisibility).
            Some(_) => MotoInt::Junk,
        }
    }

    /// Unicode decimal-digit runs (category Nd): CPython's
    /// `_PyUnicode_TransformDecimalAndSpaceToASCII` maps every Nd code
    /// point to the ASCII digit of its position inside the run, so
    /// python's int() reads "٣٠" as 30. The 68 ten-wide runs of the
    /// pinned fork's interpreter (Python 3.13, its Unicode database;
    /// generated with unicodedata over the code points).
    const PYTHON_ND_RANGES: &[(u32, u32)] = &[
        (0x30, 0x39),
        (0x660, 0x669),
        (0x6F0, 0x6F9),
        (0x7C0, 0x7C9),
        (0x966, 0x96F),
        (0x9E6, 0x9EF),
        (0xA66, 0xA6F),
        (0xAE6, 0xAEF),
        (0xB66, 0xB6F),
        (0xBE6, 0xBEF),
        (0xC66, 0xC6F),
        (0xCE6, 0xCEF),
        (0xD66, 0xD6F),
        (0xDE6, 0xDEF),
        (0xE50, 0xE59),
        (0xED0, 0xED9),
        (0xF20, 0xF29),
        (0x1040, 0x1049),
        (0x1090, 0x1099),
        (0x17E0, 0x17E9),
        (0x1810, 0x1819),
        (0x1946, 0x194F),
        (0x19D0, 0x19D9),
        (0x1A80, 0x1A89),
        (0x1A90, 0x1A99),
        (0x1B50, 0x1B59),
        (0x1BB0, 0x1BB9),
        (0x1C40, 0x1C49),
        (0x1C50, 0x1C59),
        (0xA620, 0xA629),
        (0xA8D0, 0xA8D9),
        (0xA900, 0xA909),
        (0xA9D0, 0xA9D9),
        (0xA9F0, 0xA9F9),
        (0xAA50, 0xAA59),
        (0xABF0, 0xABF9),
        (0xFF10, 0xFF19),
        (0x104A0, 0x104A9),
        (0x10D30, 0x10D39),
        (0x11066, 0x1106F),
        (0x110F0, 0x110F9),
        (0x11136, 0x1113F),
        (0x111D0, 0x111D9),
        (0x112F0, 0x112F9),
        (0x11450, 0x11459),
        (0x114D0, 0x114D9),
        (0x11650, 0x11659),
        (0x116C0, 0x116C9),
        (0x11730, 0x11739),
        (0x118E0, 0x118E9),
        (0x11950, 0x11959),
        (0x11C50, 0x11C59),
        (0x11D50, 0x11D59),
        (0x11DA0, 0x11DA9),
        (0x11F50, 0x11F59),
        (0x16A60, 0x16A69),
        (0x16AC0, 0x16AC9),
        (0x16B50, 0x16B59),
        (0x1D7CE, 0x1D7D7),
        (0x1D7D8, 0x1D7E1),
        (0x1D7E2, 0x1D7EB),
        (0x1D7EC, 0x1D7F5),
        (0x1D7F6, 0x1D7FF),
        (0x1E140, 0x1E149),
        (0x1E2F0, 0x1E2F9),
        (0x1E4F0, 0x1E4F9),
        (0x1E950, 0x1E959),
        (0x1FBF0, 0x1FBF9),
    ];

    /// CPython's Py_UNICODE_ISSPACE: the Unicode White_Space set plus the
    /// legacy ASCII file-separator run \x1c..\x1f that int()'s whitespace
    /// transliteration also strips (Rust's is_whitespace is White_Space
    /// only).
    fn py_isspace(c: char) -> bool {
        c.is_whitespace() || matches!(c, '\u{1c}'..='\u{1f}')
    }

    /// The ASCII digit a python int() would see for `c`, if int() accepts
    /// it at all: ASCII digits pass through, any Unicode Nd code point
    /// maps to the digit of its position in its run (live-verified:
    /// "٣٠" parses to 30), and everything else is ValueError.
    fn python_digit(c: char) -> Option<char> {
        if c.is_ascii_digit() {
            return Some(c);
        }
        let ch = c as u32;
        for &(lo, hi) in Self::PYTHON_ND_RANGES {
            if ch >= lo && ch <= hi {
                return char::from_digit(ch - lo, 10);
            }
        }
        None
    }

    /// python int(str) for the string shapes the gateway delivers
    /// (moto/core/parse.py `_parse_integer` and the model parse layer run
    /// the same coercion — live-verified: " 30 " parses to 30 with the
    /// surrounding whitespace stripped, "٣٠" to 30 via the Nd mapping,
    /// "1_0" to 10 and "٣_٠" to 30 via PEP 515, while "_30" raises).
    /// Returns None when python would raise — trailing/leading/doubled
    /// underscores, any non-digit, and the empty string included.
    ///
    /// The tuple carries the value saturated into i64 (python's int() is
    /// unbounded, and the API only ever compares it against small bounds
    /// or the 43200 cap) plus `echo`: python's str(int(s)) — the exact
    /// digits moto interpolates into its bounds error messages, canonical
    /// (sign, ASCII digits, no leading zeros — str(int("٣٠")) is "30",
    /// str(int("00005")) is "5", str(int(" -3 ")) is "-3"; live-verified).
    fn python_int_str(s: &str) -> Option<(i64, String)> {
        let trimmed = s.trim_matches(Self::py_isspace);
        let (negative, rest) = match trimmed.strip_prefix('-') {
            Some(r) => (true, r),
            None => (false, trimmed.strip_prefix('+').unwrap_or(trimmed)),
        };
        // PEP 515: underscores are legal only BETWEEN digits — never at
        // either end of the signed digit run.
        let mut digits = String::with_capacity(rest.len());
        let mut prev_digit = false;
        for c in rest.chars() {
            if c == '_' {
                if !prev_digit {
                    return None;
                }
                prev_digit = false;
                continue;
            }
            let d = Self::python_digit(c)?;
            digits.push(d);
            prev_digit = true;
        }
        if !prev_digit {
            return None;
        }
        // Canonical magnitude for the echo: str(int("00005")) is "5".
        let mag = digits.trim_start_matches('0');
        let echo = if mag.is_empty() {
            "0".to_string() // str(int("-0")) loses the sign
        } else if negative {
            format!("-{mag}")
        } else {
            mag.to_string()
        };
        // The comparison value saturates beyond the i64 range: a positive
        // overflow stays cap/bounds-breaching, a negative overflow behaves
        // like every negative.
        let value = if mag.is_empty() {
            0
        } else if mag.len() > 39 {
            if negative {
                i64::MIN
            } else {
                i64::MAX
            }
        } else {
            let m: u128 = mag.parse().ok()?;
            if negative {
                if m > i64::MAX as u128 + 1 {
                    i64::MIN
                } else {
                    -(m as i128) as i64
                }
            } else if m > i64::MAX as u128 {
                i64::MAX
            } else {
                m as i64
            }
        };
        Some((value, echo))
    }

    /// Validate a batch operation's `Entries` list, raising moto's batch
    /// errors before any entry is processed. Entries can arrive rebuilt from
    /// the flattened query wire (see `query_flatten_members`) — both shapes
    /// reach here as the JSON array.
    ///
    /// `EmptyBatchRequest` (per-operation entry noun), then the batch-Id
    /// checks — InvalidBatchEntryId when any Id fails python's
    /// `^[\w-]{1,80}$` regex, BatchEntryIdsNotDistinct for the first
    /// repeated Id, and TooManyEntriesInBatchRequest above 10 entries —
    /// each a top-level 400.
    fn validate_batch_entries(
        params: &Value,
        entry_noun: &'static str,
    ) -> Result<Vec<Value>, SqsError> {
        let entries = params
            .get("Entries")
            .and_then(|v| v.as_array())
            .cloned()
            .filter(|entries| !entries.is_empty())
            .ok_or_else(|| {
                SqsError::EmptyBatchRequest(format!(
                    "There should be at least one {} in the request.",
                    entry_noun
                ))
            })?;

        // moto: `re.match(r"^[\w-]{1,80}$", entry["Id"])` — Python \w is
        // unicode word characters, so alphanumeric code points, '_' and
        // '-', within 1..=80 characters (chars, not bytes).
        let invalid_id = entries.iter().any(|entry| {
            let id = entry.get("Id").and_then(|v| v.as_str()).unwrap_or("");
            !(1..=80).contains(&id.chars().count())
                || !id
                    .chars()
                    .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        });
        if invalid_id {
            return Err(SqsError::InvalidBatchEntryId);
        }

        // moto models.py send_message_batch backend (live-verified):
        // InvalidBatchEntryId, then BatchRequestTooLong when the summed
        // entry bodies exceed MAXIMUM_MESSAGE_LENGTH (1048576,
        // moto/sqs/constants.py) — strictly greater: exactly 1048576
        // sends (live-verified). Python len(str) counts characters, so
        // the same char count is used here. The sum only ever computes on
        // Send entries (delete/visibility entries carry no MessageBody),
        // which is the only op whose moto backend enforces it; it fires
        // before the Id-distinct and TooManyEntries checks, matching the
        // backend order (live-verified: an 11-entry over-cap batch
        // answers BatchRequestTooLong, not TooManyEntries).
        if entry_noun == "SendMessageBatchRequestEntry" {
            let body_chars: usize = entries
                .iter()
                .map(|entry| {
                    entry
                        .get("MessageBody")
                        .and_then(|v| v.as_str())
                        .map(|body| body.chars().count())
                        .unwrap_or(0)
                })
                .sum();
            if body_chars > Self::MAXIMUM_BATCH_BODY_LENGTH {
                return Err(SqsError::BatchRequestTooLong(format!(
                    "Batch requests cannot be longer than {} bytes. You have sent {} bytes.",
                    Self::MAXIMUM_BATCH_BODY_LENGTH,
                    body_chars
                )));
            }
        }

        // moto's _get_first_duplicate_id: the first Id seen twice wins.
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut duplicate: Option<String> = None;
        for entry in &entries {
            let id = entry
                .get("Id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if !seen.insert(id.clone()) {
                duplicate = Some(id);
                break;
            }
        }
        if let Some(id) = duplicate {
            return Err(SqsError::BatchEntryIdsNotDistinct(format!(
                "Id {} repeated.",
                id
            )));
        }

        if entries.len() > 10 {
            return Err(SqsError::TooManyEntriesInBatchRequest(format!(
                "Maximum number of entries per request are 10. You have sent {}.",
                entries.len()
            )));
        }

        Ok(entries)
    }

    fn handle_send_message_batch(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let queue_url = params
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
            })?;

        // moto's send_message_batch differs from its delete/visibility
        // batch siblings: the response layer resolves the queue
        // (`self.sqs_backend.get_queue`) BEFORE inspecting Entries at all,
        // so a missing queue answers NonExistentQueue even when the batch
        // itself is malformed — empty entries, invalid or duplicate Ids
        // all lose to QueueDoesNotExist (live-verified against the pinned
        // fork). DeleteMessageBatch/ChangeMessageVisibilityBatch keep
        // entries-first below; moto checks those in the response layer
        // and resolves the queue only inside the backend.
        let queue_name = self.extract_queue_name_from_url(queue_url)?;
        let store = self.get_store(account, region);
        let queue_arc = store.get_queue(&queue_name).ok_or_else(|| {
            SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
        })?;

        // moto's EmptyBatchRequest wording (live-verified on the pinned
        // fork); the shared helper also runs moto's batch-Id validation.
        let entries = Self::validate_batch_entries(params, "SendMessageBatchRequestEntry")?;

        let mut successful: Vec<Value> = Vec::new();
        let mut failed: Vec<Value> = Vec::new();

        for entry in &entries {
            let id = entry
                .get("Id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let body = entry
                .get("MessageBody")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let mut success = serde_json::Map::new();
            {
                let mut q = queue_arc.write();
                // A bad entry becomes a BatchResultErrorEntry, not a
                // top-level 400: valid entries in the request still send.
                match self.prepare_send(&mut q, queue_url, &body, entry) {
                    Ok(Some(PreparedMessage::Accepted(message))) => {
                        let md5_attrs =
                            Self::md5_of_message_attributes(&message.message_attributes);
                        let sequence_number = message.attributes.get("SequenceNumber").cloned();
                        let message_id = message.message_id.clone();
                        let md5_body = message.md5_of_body.clone();
                        q.send_message(*message);
                        success.insert("Id".into(), json!(id));
                        success.insert("MessageId".into(), json!(message_id));
                        success.insert("MD5OfMessageBody".into(), json!(md5_body));
                        if let Some(seq) = sequence_number {
                            success.insert("SequenceNumber".into(), json!(seq));
                        }
                        if let Some(md5_attrs) = md5_attrs {
                            success.insert("MD5OfMessageAttributes".into(), json!(md5_attrs));
                        }
                    }
                    Ok(Some(PreparedMessage::Duplicate {
                        message_id,
                        md5_of_body,
                        sequence_number,
                    })) => {
                        success.insert("Id".into(), json!(id));
                        success.insert("MessageId".into(), json!(message_id));
                        success.insert("MD5OfMessageBody".into(), json!(md5_of_body));
                        if let Some(seq) = sequence_number {
                            success.insert("SequenceNumber".into(), json!(seq));
                        }
                    }
                    Ok(None) => {}
                    Err(err) => {
                        failed.push(json!({
                            "Id": id,
                            "SenderFault": true,
                            "Code": err.code(),
                            "Message": err.message(),
                        }));
                        continue;
                    }
                }
            }
            if success.is_empty() {
                continue;
            }
            successful.push(Value::Object(success));
        }

        let body = json!({
            "Successful": successful,
            "Failed": failed
        })
        .to_string();

        Ok(AwsResponse {
            status: 200,
            headers: Self::json_headers(),
            body,
        })
    }

    fn handle_tag_queue(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let queue_url = params
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let queue_name = self.extract_queue_name_from_url(queue_url)?;
        let tags_val = params.get("Tags").cloned().unwrap_or(Value::Null);
        let mut tags: HashMap<String, String> = match tags_val {
            Value::Object(m) => m
                .into_iter()
                .map(|(k, v)| (k, v.as_str().unwrap_or("").to_string()))
                .collect(),
            _ => {
                // Legacy Tag.N.Key/Value flattened form
                let mut result = HashMap::new();
                for i in 1.. {
                    let key_name = format!("Tag.{}.Key", i);
                    let val_name = format!("Tag.{}.Value", i);
                    match (params.get(&key_name), params.get(&val_name)) {
                        (Some(k), Some(v)) => {
                            result.insert(
                                k.as_str().unwrap_or("").to_string(),
                                v.as_str().unwrap_or("").to_string(),
                            );
                        }
                        _ => break,
                    }
                }
                result
            }
        };
        let store = self.get_store(account, region);
        if let Some(q) = store.get_queue(&queue_name) {
            let queue = q.write();
            let mut stored = queue.tags.write();
            for (k, v) in tags.drain() {
                stored.insert(k, v);
            }
        }
        let body = json!({}).to_string();
        Ok(AwsResponse {
            status: 200,
            headers: Self::json_headers(),
            body,
        })
    }

    fn handle_untag_queue(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let queue_url = params
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let queue_name = self.extract_queue_name_from_url(queue_url)?;
        let tag_keys: Vec<String> = params
            .get("TagKeys")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let store = self.get_store(account, region);
        if let Some(q) = store.get_queue(&queue_name) {
            let queue = q.write();
            let mut stored = queue.tags.write();
            for k in &tag_keys {
                stored.remove(k);
            }
        }
        let body = json!({}).to_string();
        Ok(AwsResponse {
            status: 200,
            headers: Self::json_headers(),
            body,
        })
    }

    fn handle_list_queue_tags(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let queue_url = params
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let queue_name = self.extract_queue_name_from_url(queue_url)?;
        let store = self.get_store(account, region);
        let has_tags = store
            .get_queue(&queue_name)
            .map(|q| !q.read().tags.read().is_empty())
            .unwrap_or(false);
        // Tags are a JSON map; omit the member entirely when no tags exist.
        let body = if has_tags {
            let tags = store
                .get_queue(&queue_name)
                .map(|q| q.read().tags.read().clone())
                .unwrap_or_default();
            let tags_map: serde_json::Map<String, Value> = tags
                .into_iter()
                .map(|(k, v)| (k, Value::String(v)))
                .collect();
            json!({ "Tags": tags_map }).to_string()
        } else {
            "{}".to_string()
        };
        Ok(AwsResponse {
            status: 200,
            headers: Self::json_headers(),
            body,
        })
    }

    fn handle_add_permission(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let queue_url = params
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let queue_name = self.extract_queue_name_from_url(queue_url)?;
        let store = self.get_store(account, region);
        let queue_arc = store.get_queue(&queue_name).ok_or_else(|| {
            SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
        })?;

        let label = params
            .get("Label")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let account_ids: Vec<String> = params
            .get("AWSAccountIds")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let actions: Vec<String> = params
            .get("Actions")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let mut q = queue_arc.write();
        let arn = q.arn.clone();
        let mut policy: Value = q
            .attributes
            .policy
            .as_ref()
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or_else(|| {
                json!({
                    "Version": "2012-10-17",
                    "Id": format!("{}/SQSDefaultPolicy", arn),
                    "Statement": []
                })
            });
        if actions.is_empty() {
            // moto/sqs/models.py delivers MissingParameter("Actions") which
            // serializes with this exact wording (live-verified).
            return Err(SqsError::MissingParameter(
                "The request must contain the parameter Actions.".to_string(),
            ));
        }
        let action_list: Vec<Value> = actions
            .iter()
            .map(|a| json!(format!("SQS:{}", a)))
            .collect();
        let principals: Vec<Value> = account_ids
            .iter()
            .map(|aid| json!(format!("arn:aws:iam::{}:root", aid)))
            .collect();
        let statement = json!({
            "Sid": label,
            "Effect": "Allow",
            "Principal": { "AWS": principals },
            "Action": if action_list.len() > 1 { json!(action_list) } else { action_list[0].clone() },
            "Resource": arn,
        });
        if let Some(obj) = policy.as_object_mut() {
            let mut statements: Vec<Value> = obj
                .get("Statement")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            statements.retain(|s| s.get("Sid").and_then(|v| v.as_str()) != Some(label.as_str()));
            statements.push(statement);
            obj.insert("Statement".into(), json!(statements));
        }
        q.attributes.policy = Some(policy.to_string());
        q.last_modified = Utc::now().timestamp_millis() as u64;

        Ok(AwsResponse {
            status: 200,
            headers: Self::json_headers(),
            body: "{}".to_string(),
        })
    }

    fn handle_remove_permission(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let queue_url = params
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let queue_name = self.extract_queue_name_from_url(queue_url)?;
        let store = self.get_store(account, region);
        let queue_arc = store.get_queue(&queue_name).ok_or_else(|| {
            SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
        })?;

        let label = params
            .get("Label")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let mut q = queue_arc.write();
        if let Some(raw) = &q.attributes.policy {
            if let Ok(mut policy) = serde_json::from_str::<Value>(raw) {
                if let Some(obj) = policy.as_object_mut() {
                    let mut statements: Vec<Value> = obj
                        .get("Statement")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    statements
                        .retain(|s| s.get("Sid").and_then(|v| v.as_str()) != Some(label.as_str()));
                    obj.insert("Statement".into(), json!(statements));
                }
                q.attributes.policy = Some(policy.to_string());
            }
        }
        q.last_modified = Utc::now().timestamp_millis() as u64;

        Ok(AwsResponse {
            status: 200,
            headers: Self::json_headers(),
            body: "{}".to_string(),
        })
    }

    fn handle_list_dead_letter_source_queues(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let queue_url = params
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let queue_name = self.extract_queue_name_from_url(queue_url)?;
        let store = self.get_store(account, region);
        let queue_arc = store.get_queue(&queue_name).ok_or_else(|| {
            SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
        })?;
        let dlq_arn = queue_arc.read().arn.clone();
        let sources = store.dead_letter_sources(&dlq_arn);
        let urls: Vec<String> = sources
            .iter()
            .map(|q| {
                let q = q.read();
                Self::queue_url_for(&q.name, account, region)
            })
            .collect();
        // The key is lowercase "queueUrls", matching the wire format.
        let body = json!({ "queueUrls": urls }).to_string();
        Ok(AwsResponse {
            status: 200,
            headers: Self::json_headers(),
            body,
        })
    }

    fn handle_start_message_move_task(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let source_arn = params
            .get("SourceArn")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let destination_arn = params
            .get("DestinationArn")
            .and_then(|v| v.as_str())
            .map(String::from);
        let max_per_second = params
            .get("MaxNumberOfMessagesPerSecond")
            .and_then(|v| v.as_u64())
            .unwrap_or(500)
            .min(500) as u32;

        let store = self.get_store(account, region);
        let source = store.get_queue_by_arn(&source_arn).ok_or_else(|| {
            // botocore's sqs model documents ResourceNotFoundException as the
            // move-task op error (moto does not implement these ops); the
            // invalid-SourceArn wording is preserved verbatim.
            SqsError::ResourceNotFound(format!(
                "Value {} for parameter SourceArn is invalid. Reason: Queue not found.",
                source_arn
            ))
        })?;

        // Resolve the target queue: explicit DestinationArn, else the queue
        // whose RedrivePolicy points at this DLQ. Both handles must exist
        // before any queue lock is taken, so the pair below can be locked
        // in canonical queue-name order.
        let dest = if let Some(dest) = &destination_arn {
            store.get_queue_by_arn(dest)
        } else {
            store.dead_letter_sources(&source_arn).into_iter().next()
        };

        let mut moved: u64 = 0;
        match dest.as_ref() {
            // Hold both write locks for the transfer, in canonical
            // queue-name order: a mirrored move task (X->Y racing Y->X)
            // holds one queue while acquiring the other, and only a total
            // order on queue names prevents the two from deadlocking.
            Some(dest) if !Arc::ptr_eq(&source, dest) => {
                let src_first = source.read().name.clone() <= dest.read().name.clone();
                let (mut src_q, mut dest_q) = if src_first {
                    let src_q = source.write();
                    let dest_q = dest.write();
                    (src_q, dest_q)
                } else {
                    let dest_q = dest.write();
                    let src_q = source.write();
                    (src_q, dest_q)
                };
                let mut kept = VecDeque::new();
                for msg in src_q.messages.drain(..) {
                    if msg.is_visible() {
                        moved += 1;
                        let mut new_msg = msg.clone();
                        new_msg.message_id = Uuid::new_v4().to_string();
                        new_msg.receive_count = 0;
                        new_msg.first_receive_timestamp = None;
                        new_msg.visibility_until = None;
                        new_msg.available_at = None;
                        dest_q.send_message(new_msg);
                    } else {
                        kept.push_back(msg);
                    }
                }
                src_q.messages = kept;
                src_q.last_modified = Utc::now().timestamp_millis() as u64;
            }
            // No target (FAILED), or a target resolving to the source queue
            // itself (nothing to move; the nested same-queue lock would
            // wedge): touch nothing but the source's last_modified.
            _ => {
                let mut dl = source.write();
                dl.last_modified = Utc::now().timestamp_millis() as u64;
            }
        }
        let to_move = moved;

        let status = if dest.is_none() {
            "FAILED".to_string()
        } else {
            "COMPLETED".to_string()
        };

        let task = MoveTask {
            task_handle: format!("w11-task-{}", Utc::now().timestamp_micros()),
            source_arn: source_arn.clone(),
            destination_arn: destination_arn.clone(),
            status,
            max_per_second,
            moved,
            to_move,
            started_at: Utc::now().timestamp_millis() as u64,
            failure_reason: None,
        };
        store.record_move_task(task.clone());

        Ok(AwsResponse {
            status: 200,
            headers: Self::json_headers(),
            body: json!({ "TaskHandle": task.task_handle }).to_string(),
        })
    }

    fn handle_cancel_message_move_task(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let task_handle = params
            .get("TaskHandle")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let store = self.get_store(account, region);
        match store.get_move_task(&task_handle) {
            Some(task) => Ok(AwsResponse {
                status: 200,
                headers: Self::json_headers(),
                body: json!({
                    "ApproximateNumberOfMessagesMoved": task.moved
                })
                .to_string(),
            }),
            None => Err(SqsError::ResourceNotFound(
                "The task handle provided is not valid.".to_string(),
            )),
        }
    }

    fn handle_list_message_move_tasks(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let source_arn = params
            .get("SourceArn")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let store = self.get_store(account, region);
        let results: Vec<Value> = store
            .move_tasks_for_source(&source_arn)
            .iter()
            .map(|task| {
                let mut obj = json!({
                    "TaskHandle": task.task_handle,
                    "SourceArn": task.source_arn,
                    "Status": task.status,
                    "MaxNumberOfMessagesPerSecond": task.max_per_second,
                    "ApproximateNumberOfMessagesMoved": task.moved,
                    "ApproximateNumberOfMessagesToMove": task.to_move,
                    "StartedTimestamp": task.started_at,
                });
                if let Some(dest) = &task.destination_arn {
                    obj.as_object_mut()
                        .unwrap()
                        .insert("DestinationArn".into(), json!(dest));
                }
                if let Some(reason) = &task.failure_reason {
                    obj.as_object_mut()
                        .unwrap()
                        .insert("FailureReason".into(), json!(reason));
                }
                obj
            })
            .collect();
        Ok(AwsResponse {
            status: 200,
            headers: Self::json_headers(),
            body: json!({ "Results": results }).to_string(),
        })
    }
}

impl Default for DefaultSqsHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl SqsHandler for DefaultSqsHandler {
    fn handle(&self, req: AwsRequest) -> AwsResponse {
        // Error envelope follows the request protocol: query-protocol callers
        // send the `Action=` form body and get the AWS query `<ErrorResponse>`
        // XML; JSON-protocol callers (botocore's sqs json service model,
        // cross-crate bridges with an empty body) get the JSON envelope. The
        // sniff rides the gateway-parsed params (`Action` exists only for a
        // form body) instead of a raw body substring — a JSON SendMessage
        // whose MessageBody literally contains "Action=" was previously
        // misrouted to XML errors.
        let is_query_protocol = req
            .params
            .get("Action")
            .and_then(|v| v.as_str())
            .map(|a| !a.is_empty())
            .unwrap_or(false);
        let result = {
            let mut req = req;
            query_flatten_members(&mut req.params);
            self.handle_request(&req)
        };
        match result {
            Ok(resp) => resp,
            Err(err) => {
                if is_query_protocol {
                    err.to_xml_response()
                } else {
                    err.to_json_response()
                }
            }
        }
    }
}

/// Rebuild structured batch members from a query-protocol flattened map:
/// gateway param flattening leaves `SendMessageBatchRequestEntry.1.Id` /
/// `SendMessageBatchRequestEntry.1.MessageBody` dotted keys (AWS SDK Go v1,
/// old botocore), so `Entries` (the array the JSON shape expects) never
/// arrives and the batch ops answer a phantom EmptyBatchRequest. Rebuild the
/// member lists from the `.N.` groups the same shape the JSON protocol
/// carries (moto flattens query POSTs with the same semantics).
fn query_flatten_members(params: &mut serde_json::Value) {
    let Some(obj) = params.as_object_mut() else {
        return;
    };
    // The three batch entry families the query wire flattens as
    // `<Family>.<N>.<Member>`.
    for prefix in [
        "SendMessageBatchRequestEntry",
        "DeleteMessageBatchRequestEntry",
        "ChangeMessageVisibilityBatchRequestEntry",
    ] {
        let family_prefix = format!("{prefix}.");
        if !obj.keys().any(|k| k.starts_with(&family_prefix)) {
            continue;
        }
        let flats: Vec<String> = obj
            .keys()
            .filter(|k| k.starts_with(&family_prefix))
            .cloned()
            .collect();
        let mut rows: Vec<(u32, serde_json::Map<String, Value>)> = Vec::new();
        for flat in &flats {
            let mut segs = flat
                .strip_prefix(&family_prefix)
                .expect("starts_with checked")
                .splitn(2, '.');
            let Some(idx) = segs.next().and_then(|s| s.parse::<u32>().ok()) else {
                continue;
            };
            let Some(field) = segs.next() else {
                continue;
            };
            let value = obj.get(flat).cloned().unwrap_or(Value::Null);
            let row = match rows.iter_mut().find(|(i, _)| *i == idx) {
                Some((_, row)) => row,
                None => {
                    rows.push((idx, serde_json::Map::new()));
                    let last = rows.last_mut().expect("just pushed");
                    &mut last.1
                }
            };
            row.insert(field.to_string(), value);
            obj.remove(flat);
        }
        rows.sort_by_key(|(i, _)| *i);
        obj.insert(
            "Entries".to_string(),
            Value::Array(
                rows.into_iter()
                    .map(|(_, r)| Value::Object(r))
                    .collect(),
            ),
        );
    }
}

impl DefaultSqsHandler {
    fn handle_request(&self, req: &AwsRequest) -> Result<AwsResponse, SqsError> {
        match req.operation.as_str() {
            "CreateQueue" => self.handle_create_queue(&req.params, req.account, &req.region),
            "SendMessage" => self.handle_send_message(&req.params, req.account, &req.region),
            "ReceiveMessage" => self.handle_receive_message(&req.params, req.account, &req.region),
            "DeleteMessage" => self.handle_delete_message(&req.params, req.account, &req.region),
            "DeleteMessageBatch" => {
                self.handle_delete_message_batch(&req.params, req.account, &req.region)
            }
            "ChangeMessageVisibilityBatch" => {
                self.handle_change_visibility_batch(&req.params, req.account, &req.region)
            }
            "DeleteQueue" => self.handle_delete_queue(&req.params, req.account, &req.region),
            "ListQueues" => self.handle_list_queues(&req.params, req.account, &req.region),
            "GetQueueUrl" => self.handle_get_queue_url(&req.params, req.account, &req.region),
            "GetQueueAttributes" => {
                self.handle_get_queue_attributes(&req.params, req.account, &req.region)
            }
            "SetQueueAttributes" => {
                self.handle_set_queue_attributes(&req.params, req.account, &req.region)
            }
            "ChangeMessageVisibility" => {
                self.handle_change_message_visibility(&req.params, req.account, &req.region)
            }
            "PurgeQueue" => self.handle_purge_queue(&req.params, req.account, &req.region),
            "SendMessageBatch" => {
                self.handle_send_message_batch(&req.params, req.account, &req.region)
            }
            "TagQueue" => self.handle_tag_queue(&req.params, req.account, &req.region),
            "UntagQueue" => self.handle_untag_queue(&req.params, req.account, &req.region),
            "ListQueueTags" => self.handle_list_queue_tags(&req.params, req.account, &req.region),
            "AddPermission" => self.handle_add_permission(&req.params, req.account, &req.region),
            "RemovePermission" => {
                self.handle_remove_permission(&req.params, req.account, &req.region)
            }
            "ListDeadLetterSourceQueues" => {
                self.handle_list_dead_letter_source_queues(&req.params, req.account, &req.region)
            }
            "StartMessageMoveTask" => {
                self.handle_start_message_move_task(&req.params, req.account, &req.region)
            }
            "CancelMessageMoveTask" => {
                self.handle_cancel_message_move_task(&req.params, req.account, &req.region)
            }
            "ListMessageMoveTasks" => {
                self.handle_list_message_move_tasks(&req.params, req.account, &req.region)
            }
            // moto's call_action raises NotImplementedError for unknown
            // actions — a plain werkzeug 500 crash (live-verified), never an
            // AWS error envelope.
            _ => Err(SqsError::MotoCrash),
        }
    }
}
