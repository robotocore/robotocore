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

/// The actual SQS handler implementation
pub struct DefaultSqsHandler {
    // Store per (account, region), shared process-wide
    stores: StoreMap,
}

impl DefaultSqsHandler {
    pub fn new() -> Self {
        Self {
            stores: global_stores(),
        }
    }

    fn get_store(&self, account: u64, region: &str) -> Arc<SqsStore> {
        let mut stores = self.stores.write();
        stores
            .entry((account, region.to_string()))
            .or_insert_with(|| Arc::new(SqsStore::new()))
            .clone()
    }

    fn validate_queue_name(name: &str) -> Result<(), SqsError> {
        if name.is_empty() || name.len() > 80 {
            return Err(SqsError::InvalidQueueName(
                "Queue name must be between 1 and 80 characters".to_string(),
            ));
        }

        // Allow alphanumeric, _, -, .
        if !name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.')
        {
            return Err(SqsError::InvalidQueueName(
                "Queue name can only contain alphanumeric characters, underscores, hyphens, and periods"
                    .to_string(),
            ));
        }

        Ok(())
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

    fn queue_url_for(queue_name: &str, account: u64, region: &str) -> String {
        format!(
            "http://sqs.{}.localhost.robotocore.cloud:4566/{}/{}",
            region, account, queue_name
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
        let queue_name = params
            .get("QueueName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SqsError::MissingParameter("QueueName is required".to_string()))?;

        Self::validate_queue_name(queue_name)?;

        let store = self.get_store(account, region);

        // Check if queue already exists
        if let Some(existing) = store.get_queue(queue_name) {
            existing
                .write()
                .attributes
                .apply(params.get("Attributes").unwrap_or(&Value::Null));
            // Return the existing queue URL (idempotent, like Python _create_queue)
            let url = Self::queue_url_for(queue_name, account, region);
            let body = json!({
                "QueueUrl": url
            })
            .to_string();
            return Ok(AwsResponse {
                status: 200,
                headers: Self::json_headers(),
                body,
            });
        }

        // Create new queue
        let url = Self::queue_url_for(queue_name, account, region);
        let arn = format!("arn:aws:sqs:{}:{}:{}", region, account, queue_name);

        let mut queue = Queue::new(
            queue_name.to_string(),
            region.to_string(),
            account.to_string(),
            url.clone(),
            arn,
        );
        if queue_name.ends_with(".fifo") {
            queue.attributes.fifo_queue = true;
        }
        queue.last_modified = queue.created;
        queue
            .attributes
            .apply(params.get("Attributes").unwrap_or(&Value::Null));

        if let Some(tags) = params.get("Tags").and_then(|v| v.as_object()) {
            let mut stored = queue.tags.write();
            for (k, v) in tags {
                stored.insert(k.clone(), v.as_str().unwrap_or("").to_string());
            }
        }

        store.create_queue(queue);

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
            let explicit_dedup = params
                .get("MessageDeduplicationId")
                .and_then(|v| v.as_str())
                .map(String::from);
            let dedup_id = explicit_dedup.unwrap_or_else(|| {
                if queue.attributes.content_based_deduplication {
                    format!("{:x}", md5::compute(body.as_bytes()))
                } else {
                    String::new()
                }
            });

            // Expire dedup entries older than 5 minutes, then check for a duplicate.
            queue
                .dedup_ids
                .retain(|_, (_, ts)| now_ms.saturating_sub(*ts) < Self::DEDUP_INTERVAL_MS);
            if let Some((original_id, _)) = queue.dedup_ids.get(&dedup_id) {
                let original_id = original_id.clone();
                // Duplicate: respond like the original send.
                let original = queue
                    .messages
                    .iter()
                    .find(|m| m.message_id == original_id)
                    .cloned();
                if let Some(orig) = original {
                    return Ok(Some(PreparedMessage::Duplicate {
                        message_id: orig.message_id,
                        md5_of_body: orig.md5_of_body,
                        sequence_number: orig.attributes.get("SequenceNumber").cloned(),
                    }));
                }
                // Original already delivered/removed: accept as a fresh message.
            }
            let sequence_number = queue
                .next_sequence_number
                .checked_add(1)
                .unwrap_or(1)
                .to_string();
            queue.next_sequence_number += 1;
            let message_id = Uuid::new_v4().to_string();
            queue
                .dedup_ids
                .insert(dedup_id, (message_id.clone(), now_ms));

            let mut attrs_map = HashMap::new();
            attrs_map.insert("MessageGroupId".to_string(), gid);
            attrs_map.insert("SequenceNumber".to_string(), sequence_number);
            SqsMessage {
                message_id: message_id.clone(),
                body: body.to_string(),
                md5_of_body: Self::md5_hash(body),
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
        } else {
            let message_id = Uuid::new_v4().to_string();
            let attrs_map = HashMap::new();
            SqsMessage {
                message_id: message_id.clone(),
                body: body.to_string(),
                md5_of_body: Self::md5_hash(body),
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
            .ok_or_else(|| SqsError::MissingParameter("QueueUrl is required".to_string()))?;

        let message_body = params
            .get("MessageBody")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SqsError::MissingParameter("MessageBody is required".to_string()))?;

        let queue_name = self.extract_queue_name_from_url(queue_url)?;
        let store = self.get_store(account, region);
        let queue_arc = store.get_queue(&queue_name).ok_or_else(|| {
            SqsError::NonExistentQueue(format!("Queue {} does not exist", queue_url))
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
        let queue_name = self.extract_queue_name_from_url(&queue_url)?;
        let store = self.get_store(account, region);
        let queue_opt = store.get_queue(&queue_name);
        let queue_arc = queue_opt.ok_or_else(|| {
            SqsError::NonExistentQueue(format!("Queue {} does not exist", queue_url))
        })?;

        let entries = params
            .get("Entries")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

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
            let visibility = entry
                .get("VisibilityTimeout")
                .and_then(|v| v.as_u64())
                .unwrap_or(30);
            let mut q = queue_arc.write();
            if q.change_message_visibility(&receipt_handle, visibility as u32) {
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
        let queue_name = self.extract_queue_name_from_url(&queue_url)?;
        let store = self.get_store(account, region);
        let queue_opt = store.get_queue(&queue_name);
        let queue_arc = queue_opt.ok_or_else(|| {
            SqsError::NonExistentQueue(format!("Queue {} does not exist", queue_url))
        })?;

        let entries = params
            .get("Entries")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

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
        let queue_url = params
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SqsError::MissingParameter("QueueUrl is required".to_string()))?;

        let max_messages = params
            .get("MaxNumberOfMessages")
            .and_then(|v| v.as_u64())
            .unwrap_or(1)
            .min(10) as usize;

        // Extract queue name from URL
        let queue_name = self.extract_queue_name_from_url(queue_url)?;

        let store = self.get_store(account, region);
        let queue_arc = store.get_queue(&queue_name).ok_or_else(|| {
            SqsError::NonExistentQueue(format!("Queue {} does not exist", queue_url))
        })?;

        // Long poll: WaitTimeSeconds wins, else the queue-level attribute.
        let queue_wait = queue_arc.read().attributes.receive_message_wait_time;
        let wait_seconds = params
            .get("WaitTimeSeconds")
            .and_then(|v| v.as_u64())
            .map(|v| v.min(20))
            .unwrap_or(queue_wait.min(20) as u64);

        let mut messages;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(wait_seconds);
        loop {
            messages = {
                let mut q = queue_arc.write();
                // Deliver events queued by cross-service callers (SNS/EventBridge)
                // before each receive attempt so messages arrive during long polls.
                super::bridge::drain_pending(account, region, &queue_name, &mut q);
                let visibility_timeout = params
                    .get("VisibilityTimeout")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as u32)
                    .unwrap_or(q.attributes.visibility_timeout);
                q.receive_messages(max_messages, visibility_timeout)
            };
            // DLQ redrive: messages received more times than maxReceiveCount move.
            let (max_receive, dlq_arn) = {
                let q = queue_arc.read();
                (q.redrive_max_receive_count(), q.redrive_target_arn())
            };
            if let (Some(max_receive), Some(dlq_arn)) = (max_receive, dlq_arn) {
                let mut stayed = Vec::new();
                for msg in messages {
                    if msg.receive_count > max_receive {
                        if let Some(dlq) = store.get_queue_by_arn(&dlq_arn) {
                            let mut moved = msg.clone();
                            moved.message_id = Uuid::new_v4().to_string();
                            moved.receipt_handle =
                                Self::generate_receipt_handle(&moved.message_id, queue_url);
                            moved.receive_count = 0;
                            moved.first_receive_timestamp = None;
                            moved.visibility_until = None;
                            moved.available_at = None;
                            {
                                let mut dl = dlq.write();
                                dl.send_message(moved);
                            }
                        }
                        let mut q = queue_arc.write();
                        q.remove_by_receipt_handle(&msg.receipt_handle);
                    } else {
                        stayed.push(msg);
                    }
                }
                messages = stayed;
            }
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
            .ok_or_else(|| SqsError::MissingParameter("QueueUrl is required".to_string()))?;

        let receipt_handle = params
            .get("ReceiptHandle")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SqsError::MissingParameter("ReceiptHandle is required".to_string()))?;

        // Extract queue name from URL
        let queue_name = self.extract_queue_name_from_url(queue_url)?;

        let store = self.get_store(account, region);
        let queue_opt = store.get_queue(&queue_name);
        let queue_arc = queue_opt.ok_or_else(|| {
            SqsError::NonExistentQueue(format!("Queue {} does not exist", queue_url))
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
            .ok_or_else(|| SqsError::MissingParameter("QueueUrl is required".to_string()))?;

        // Extract queue name from URL
        let queue_name = self.extract_queue_name_from_url(queue_url)?;

        let store = self.get_store(account, region);
        store.delete_queue(&queue_name);
        super::bridge::clear_pending(account, region, &queue_name);

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
                    "http://sqs.{}.localhost.robotocore.cloud:4566/{}/{}",
                    region, account, name
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
            .ok_or_else(|| SqsError::MissingParameter("QueueName is required".to_string()))?;

        let store = self.get_store(account, region);
        store.get_queue(queue_name).ok_or_else(|| {
            SqsError::NonExistentQueue(format!("Queue {} does not exist", queue_name))
        })?;

        let url = format!(
            "http://sqs.{}.localhost.robotocore.cloud:4566/{}/{}",
            region, account, queue_name
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
            .ok_or_else(|| SqsError::MissingParameter("QueueUrl is required".to_string()))?;

        // Extract queue name from URL
        let queue_name = self.extract_queue_name_from_url(queue_url)?;

        let store = self.get_store(account, region);
        let queue_opt = store.get_queue(&queue_name);
        let queue_arc = queue_opt.ok_or_else(|| {
            SqsError::NonExistentQueue(format!("Queue {} does not exist", queue_url))
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

        Err(SqsError::InvalidQueueName(format!(
            "Could not extract queue name from URL: {}",
            url
        )))
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
            .ok_or_else(|| SqsError::MissingParameter("QueueUrl is required".to_string()))?;

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
        let queue_url = params
            .get("QueueUrl")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SqsError::MissingParameter("QueueUrl is required".to_string()))?;

        let receipt_handle = params
            .get("ReceiptHandle")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SqsError::MissingParameter("ReceiptHandle is required".to_string()))?;

        let timeout = params
            .get("VisibilityTimeout")
            .and_then(|v| v.as_u64())
            .unwrap_or(30) as u32;

        let queue_name = self.extract_queue_name_from_url(queue_url)?;
        let store = self.get_store(account, region);
        let queue_arc = store.get_queue(&queue_name).ok_or_else(|| {
            SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
        })?;

        let ok = {
            let mut q = queue_arc.write();
            q.change_message_visibility(receipt_handle, timeout)
        };

        if !ok {
            return Err(SqsError::ReceiptHandleIsInvalid(
                "The input receipt handle is invalid.".to_string(),
            ));
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
            .ok_or_else(|| SqsError::MissingParameter("QueueUrl is required".to_string()))?;

        let queue_name = self.extract_queue_name_from_url(queue_url)?;
        let store = self.get_store(account, region);
        let queue_arc = store.get_queue(&queue_name).ok_or_else(|| {
            SqsError::NonExistentQueue("The specified queue does not exist.".to_string())
        })?;

        {
            let mut q = queue_arc.write();
            super::bridge::drain_pending(account, region, &queue_name, &mut q);
            super::bridge::clear_pending(account, region, &queue_name);
            q.purge();
        }

        let body = json!({}).to_string();
        Ok(AwsResponse {
            status: 200,
            headers: Self::json_headers(),
            body,
        })
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
            .ok_or_else(|| SqsError::MissingParameter("QueueUrl is required".to_string()))?;
        let entries = params
            .get("Entries")
            .and_then(|v| v.as_array())
            .cloned()
            .ok_or_else(|| SqsError::MissingParameter("Entries is required".to_string()))?;

        let queue_name = self.extract_queue_name_from_url(queue_url)?;
        let store = self.get_store(account, region);
        let queue_arc = store.get_queue(&queue_name).ok_or_else(|| {
            SqsError::NonExistentQueue(format!("Queue {} does not exist", queue_url))
        })?;

        let mut successful: Vec<Value> = Vec::new();

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
                match self.prepare_send(&mut q, queue_url, &body, entry)? {
                    Some(PreparedMessage::Accepted(message)) => {
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
                    Some(PreparedMessage::Duplicate {
                        message_id,
                        md5_of_body,
                        sequence_number,
                    }) => {
                        success.insert("Id".into(), json!(id));
                        success.insert("MessageId".into(), json!(message_id));
                        success.insert("MD5OfMessageBody".into(), json!(md5_of_body));
                        if let Some(seq) = sequence_number {
                            success.insert("SequenceNumber".into(), json!(seq));
                        }
                    }
                    None => {}
                }
            }
            if success.is_empty() {
                continue;
            }
            successful.push(Value::Object(success));
        }

        let body = json!({
            "Successful": successful,
            "Failed": Vec::<Value>::new()
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
            return Err(SqsError::MissingParameter(
                "Actions must contain at least one entry.".to_string(),
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
            SqsError::ValidationError(format!(
                "Value {} for parameter SourceArn is invalid. Reason: Queue not found.",
                source_arn
            ))
        })?;

        // Resolve the target queue: explicit DestinationArn, else the queue
        // whose RedrivePolicy points at this DLQ.
        let target_name = if let Some(dest) = &destination_arn {
            store.get_queue_by_arn(dest).map(|q| q.read().name.clone())
        } else {
            store
                .dead_letter_sources(&source_arn)
                .first()
                .map(|q| q.read().name.clone())
        };

        let mut moved: u64 = 0;
        {
            let mut dl = source.write();
            if let Some(dest_name) = &target_name {
                if let Some(dest) = store.get_queue(dest_name) {
                    let mut kept = VecDeque::new();
                    let mut dest_queue = dest.write();
                    for msg in dl.messages.drain(..) {
                        if msg.is_visible() {
                            moved += 1;
                            let mut new_msg = msg.clone();
                            new_msg.message_id = Uuid::new_v4().to_string();
                            new_msg.receive_count = 0;
                            new_msg.first_receive_timestamp = None;
                            new_msg.visibility_until = None;
                            new_msg.available_at = None;
                            dest_queue.send_message(new_msg);
                        } else {
                            kept.push_back(msg);
                        }
                    }
                    dl.messages = kept;
                }
            }
            dl.last_modified = Utc::now().timestamp_millis() as u64;
        }
        let to_move = moved;

        let status = if target_name.is_none() {
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
        let _ = (account, region);
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
            None => Err(SqsError::ValidationError(
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
        let _ = (account, region);
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
        let result = self.handle_request(&req);
        result.unwrap_or_else(|err| err.to_json_response())
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
            _ => Err(SqsError::ValidationError(format!(
                "Unknown operation: {}",
                req.operation
            ))),
        }
    }
}
