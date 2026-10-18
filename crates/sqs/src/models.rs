//! SQS data models: messages, queues, and storage

use chrono::Utc;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// A single SQS message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsMessage {
    pub message_id: String,
    pub body: String,
    pub md5_of_body: String,
    pub receipt_handle: String,
    pub sent_timestamp: u64,
    pub visibility_until: Option<u64>, // Unix timestamp in milliseconds when message becomes visible again
    pub receive_count: u32,
    pub first_receive_timestamp: Option<u64>,
    pub attributes: HashMap<String, String>,
    pub message_attributes: HashMap<String, serde_json::Value>,
    /// When set, the message is not deliverable before this time (send delay).
    #[serde(default)]
    pub available_at: Option<u64>,
    /// MessageDeduplicationId (or computed content-based id) for FIFO messages.
    #[serde(default)]
    pub deduplication_id: Option<String>,
}

impl SqsMessage {
    /// Check if message is currently visible (delay passed, not in flight)
    pub fn is_visible(&self) -> bool {
        let now_ms = (Utc::now().timestamp_millis()) as u64;
        if let Some(at) = self.available_at {
            if now_ms < at {
                return false;
            }
        }
        match self.visibility_until {
            Some(until) => now_ms >= until,
            None => true,
        }
    }

    /// Message is delayed (never delivered, waiting for the delay to pass).
    pub fn is_delayed(&self) -> bool {
        match self.available_at {
            Some(at) => {
                let now_ms = (Utc::now().timestamp_millis()) as u64;
                now_ms < at
            }
            None => false,
        }
    }

    /// Message was received before and its visibility window has not expired.
    pub fn is_inflight(&self) -> bool {
        if self.first_receive_timestamp.is_none() {
            return false;
        }
        match self.visibility_until {
            Some(until) => ((Utc::now().timestamp_millis()) as u64) < until,
            None => false,
        }
    }

    /// Set visibility timeout in seconds from now
    pub fn set_visibility_timeout(&mut self, timeout_seconds: u32) {
        let now_ms = (Utc::now().timestamp_millis()) as u64;
        self.visibility_until = Some(now_ms + (timeout_seconds as u64 * 1000));
    }
}

/// SQS Queue attributes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueAttributes {
    pub visibility_timeout: u32,        // default 30
    pub message_retention_period: u32,  // default 345600 (4 days)
    pub maximum_message_size: u32,      // default 262144 (256 KB)
    pub delay_seconds: u32,             // default 0
    pub receive_message_wait_time: u32, // default 0
    pub policy: Option<String>,
    pub fifo_queue: bool,                  // FifoQueue attribute
    pub content_based_deduplication: bool, // ContentBasedDeduplication attribute
    pub redrive_policy: Option<String>,    // RedrivePolicy JSON attribute
    #[serde(default)]
    pub extra: std::collections::HashMap<String, String>, // RedriveAllowPolicy, Kms*, etc.
}

impl Default for QueueAttributes {
    fn default() -> Self {
        Self {
            visibility_timeout: 30,
            message_retention_period: 345600,
            maximum_message_size: 262144,
            delay_seconds: 0,
            receive_message_wait_time: 0,
            policy: None,
            fifo_queue: false,
            content_based_deduplication: false,
            redrive_policy: None,
            extra: std::collections::HashMap::new(),
        }
    }
}

impl QueueAttributes {
    /// Apply a JSON-protocol Attributes map (attr name -> string value).
    pub fn apply(&mut self, attrs: &serde_json::Value) {
        if let Some(map) = attrs.as_object() {
            for (key, value) in map {
                let val = value.as_str().unwrap_or("");
                match key.as_str() {
                    "VisibilityTimeout" => {
                        if let Ok(v) = val.parse::<u32>() {
                            self.visibility_timeout = v;
                        }
                    }
                    "DelaySeconds" => {
                        if let Ok(v) = val.parse::<u32>() {
                            self.delay_seconds = v;
                        }
                    }
                    "ReceiveMessageWaitTimeSeconds" => {
                        if let Ok(v) = val.parse::<u32>() {
                            self.receive_message_wait_time = v;
                        }
                    }
                    "MaximumMessageSize" => {
                        if let Ok(v) = val.parse::<u32>() {
                            self.maximum_message_size = v;
                        }
                    }
                    "MessageRetentionPeriod" => {
                        if let Ok(v) = val.parse::<u32>() {
                            self.message_retention_period = v;
                        }
                    }
                    "Policy" => self.policy = Some(val.to_string()),
                    "RedrivePolicy" => self.redrive_policy = Some(val.to_string()),
                    "FifoQueue" => self.fifo_queue = val == "true",
                    "ContentBasedDeduplication" => self.content_based_deduplication = val == "true",
                    _ => {
                        self.extra.insert(key.clone(), val.to_string());
                    }
                }
            }
        }
    }
}

/// FIFO deduplication record: the original send's response fields. Kept for
/// the 5-minute dedup window even after the original message is deleted, so
/// a re-send with the same dedup id is suppressed (retract-pending) until
/// the window expires.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupEntry {
    pub message_id: String,
    pub md5_of_body: String,
    pub sequence_number: Option<String>,
    pub sent_at: u64, // timestamp of the first send, not refreshed on re-sends
}

/// A message move task (DeadLetterQueue redrive reversal).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MoveTask {
    pub task_handle: String,
    pub source_arn: String,
    pub destination_arn: Option<String>,
    pub status: String,
    pub max_per_second: u32,
    pub moved: u64,
    pub to_move: u64,
    pub started_at: u64,
    pub failure_reason: Option<String>,
}

/// A queue in SQS (behind RwLock for thread-safe mutation)
#[derive(Debug, Serialize, Deserialize)]
pub struct Queue {
    pub name: String,
    pub arn: String,
    pub url: String,
    pub region: String,
    pub account_id: String,
    pub created: u64,
    pub last_modified: u64,
    pub attributes: QueueAttributes,
    pub messages: VecDeque<SqsMessage>,
    #[serde(with = "crate::snapshot::tags_lock")]
    pub tags: parking_lot::RwLock<std::collections::HashMap<String, String>>,
    /// Deduplication IDs seen on this FIFO queue -> original send's record.
    /// Entries live for the 5-minute dedup window from the first send.
    #[serde(default)]
    pub dedup_ids: std::collections::HashMap<String, DedupEntry>,
    /// Monotonic sequence number counter for FIFO queues.
    #[serde(default)]
    pub next_sequence_number: u64,
}

impl Queue {
    pub fn new(name: String, region: String, account_id: String, url: String, arn: String) -> Self {
        let now_ms = (Utc::now().timestamp_millis()) as u64;
        Self {
            name,
            arn,
            url,
            region,
            account_id,
            created: now_ms,
            last_modified: now_ms,
            attributes: QueueAttributes::default(),
            messages: VecDeque::new(),
            tags: parking_lot::RwLock::new(std::collections::HashMap::new()),
            dedup_ids: std::collections::HashMap::new(),
            next_sequence_number: 1_854_250_000_000_000,
        }
    }

    /// deadLetterTargetArn parsed from the RedrivePolicy attribute.
    pub fn redrive_target_arn(&self) -> Option<String> {
        self.attributes.redrive_policy.as_ref().and_then(|raw| {
            serde_json::from_str::<serde_json::Value>(raw)
                .ok()
                .and_then(|v| {
                    v.get("deadLetterTargetArn")
                        .and_then(|t| t.as_str().map(String::from))
                })
        })
    }

    /// maxReceiveCount parsed from the RedrivePolicy attribute.
    pub fn redrive_max_receive_count(&self) -> Option<u32> {
        self.attributes.redrive_policy.as_ref().and_then(|raw| {
            serde_json::from_str::<serde_json::Value>(raw)
                .ok()
                .and_then(|v| v.get("maxReceiveCount").cloned())
                .and_then(|c| {
                    c.as_u64()
                        .or_else(|| c.as_str().and_then(|s| s.parse::<u64>().ok()))
                })
                .map(|c| c as u32)
        })
    }

    /// Add a message to the queue
    pub fn send_message(&mut self, message: SqsMessage) {
        self.messages.push_back(message);
    }

    /// Get visible messages (respecting visibility timeout), queue-order.
    ///
    /// Per-group ordering is preserved by keeping messages in their slots;
    /// VisibilityTimeout=0 leaves the message available immediately.
    ///
    /// FIFO queues: a group with a message in flight is blocked, so the
    /// group's later messages are skipped (head-of-line) while messages of
    /// other groups may still be delivered (AWS semantics). A group whose
    /// head is delayed (send delay) is likewise skipped. Long polls release
    /// naturally: the caller's poll loop re-enters receive_messages every
    /// 50ms, picking the group up once its message becomes visible again.
    pub fn receive_messages(
        &mut self,
        max_count: usize,
        visibility_timeout: u32,
    ) -> Vec<SqsMessage> {
        let mut result = Vec::new();
        let now_ms = (Utc::now().timestamp_millis()) as u64;

        let fifo = self.attributes.fifo_queue;
        let mut blocked_groups: std::collections::HashSet<String> = if fifo {
            self.messages
                .iter()
                .filter(|m| m.is_inflight())
                // Bridged messages may lack a group id; they behave as their
                // own (unblocked) stream.
                .filter_map(|m| m.attributes.get("MessageGroupId").cloned())
                .collect()
        } else {
            std::collections::HashSet::new()
        };

        for msg in self.messages.iter_mut() {
            if result.len() >= max_count {
                break;
            }
            if fifo {
                if let Some(group) = msg.attributes.get("MessageGroupId") {
                    if blocked_groups.contains(group) {
                        continue;
                    }
                }
            }
            if !msg.is_visible() {
                if fifo {
                    if let Some(group) = msg.attributes.get("MessageGroupId") {
                        blocked_groups.insert(group.clone());
                    }
                }
                continue;
            }

            msg.receive_count += 1;
            if msg.first_receive_timestamp.is_none() {
                msg.first_receive_timestamp = Some(now_ms);
            }
            msg.set_visibility_timeout(visibility_timeout);

            msg.attributes
                .insert("SenderId".to_string(), self.account_id.clone());
            msg.attributes
                .insert("SentTimestamp".to_string(), msg.sent_timestamp.to_string());
            msg.attributes.insert(
                "ApproximateReceiveCount".to_string(),
                msg.receive_count.to_string(),
            );
            msg.attributes.insert(
                "ApproximateFirstReceiveTimestamp".to_string(),
                msg.first_receive_timestamp.unwrap_or(now_ms).to_string(),
            );

            result.push(msg.clone());
            if visibility_timeout == 0 {
                // Immediately re-available.
                msg.visibility_until = None;
            }
        }

        result
    }

    /// Message counts: (visible, inflight, delayed).
    pub fn message_counts(&self) -> (usize, usize, usize) {
        let inflight = self.messages.iter().filter(|m| m.is_inflight()).count();
        let delayed = self.messages.iter().filter(|m| m.is_delayed()).count();
        let visible = self.messages.len() - inflight - delayed;
        (visible, inflight, delayed)
    }

    /// Remove a message by receipt handle (delete already matches this way).
    pub fn remove_by_receipt_handle(&mut self, receipt_handle: &str) -> bool {
        self.delete_message(receipt_handle).is_ok()
    }

    /// Remove message by receipt handle
    pub fn delete_message(&mut self, receipt_handle: &str) -> Result<(), String> {
        for (idx, msg) in self.messages.iter().enumerate() {
            if msg.receipt_handle == receipt_handle {
                self.messages.remove(idx);
                return Ok(());
            }
        }
        Err(format!(
            "Message not found with receipt handle: {}",
            receipt_handle
        ))
    }

    /// Change the visibility timeout of a message (identified by receipt handle).
    /// Returns true if the message was found and updated.
    pub fn change_message_visibility(
        &mut self,
        receipt_handle: &str,
        timeout_seconds: u32,
    ) -> bool {
        if let Some(msg) = self
            .messages
            .iter_mut()
            .find(|m| m.receipt_handle == receipt_handle)
        {
            msg.set_visibility_timeout(timeout_seconds);
            self.last_modified = (Utc::now().timestamp_millis()) as u64;
            true
        } else {
            false
        }
    }

    /// Remove all messages from the queue.
    pub fn purge(&mut self) {
        self.messages.clear();
        self.last_modified = (Utc::now().timestamp_millis()) as u64;
    }
}

/// Thread-safe SQS storage for a single account+region
pub struct SqsStore {
    pub(crate) queues: Arc<RwLock<HashMap<String, Arc<RwLock<Queue>>>>>,
    /// Dead-letter queue redrive (message move) tasks by handle.
    pub(crate) move_tasks: Arc<parking_lot::RwLock<HashMap<String, MoveTask>>>,
}

impl SqsStore {
    pub fn new() -> Self {
        Self {
            queues: Arc::new(RwLock::new(HashMap::new())),
            move_tasks: Arc::new(parking_lot::RwLock::new(HashMap::new())),
        }
    }

    pub fn get_queue_by_arn(&self, arn: &str) -> Option<Arc<RwLock<Queue>>> {
        let q = self.queues.read();
        q.values().find(|queue| queue.read().arn == arn).cloned()
    }

    /// Queues whose RedrivePolicy targets the given DLQ ARN (in name order).
    pub fn dead_letter_sources(&self, dlq_arn: &str) -> Vec<Arc<RwLock<Queue>>> {
        let q = self.queues.read();
        let mut sources: Vec<Arc<RwLock<Queue>>> = q
            .values()
            .filter(|queue| {
                let attr = queue.read();
                attr.attributes
                    .redrive_policy
                    .as_ref()
                    .map(|raw| {
                        serde_json::from_str::<serde_json::Value>(raw)
                            .ok()
                            .and_then(|v| {
                                v.get("deadLetterTargetArn")
                                    .and_then(|t| t.as_str().map(String::from))
                            })
                            .as_deref()
                            == Some(dlq_arn)
                    })
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        sources.sort_by_key(|queue| queue.read().name.clone());
        sources
    }

    /// Record a completed/cancelled message move task.
    pub fn record_move_task(&self, task: MoveTask) {
        self.move_tasks
            .write()
            .insert(task.task_handle.clone(), task);
    }

    pub fn get_move_task(&self, handle: &str) -> Option<MoveTask> {
        self.move_tasks.read().get(handle).cloned()
    }

    /// Tasks whose source is the given ARN, ordered by start time.
    pub fn move_tasks_for_source(&self, source_arn: &str) -> Vec<MoveTask> {
        let mut tasks: Vec<MoveTask> = self
            .move_tasks
            .read()
            .values()
            .filter(|t| t.source_arn == source_arn)
            .cloned()
            .collect();
        tasks.sort_by_key(|t| t.started_at);
        tasks
    }

    pub fn create_queue(&self, queue: Queue) {
        let mut q = self.queues.write();
        q.insert(queue.name.clone(), Arc::new(RwLock::new(queue)));
    }

    pub fn get_queue(&self, name: &str) -> Option<Arc<RwLock<Queue>>> {
        let q = self.queues.read();
        q.get(name).cloned()
    }

    pub fn delete_queue(&self, name: &str) -> bool {
        let mut q = self.queues.write();
        q.remove(name).is_some()
    }

    pub fn list_queues(&self) -> Vec<String> {
        let q = self.queues.read();
        q.keys().cloned().collect()
    }
}

impl Default for SqsStore {
    fn default() -> Self {
        Self::new()
    }
}
