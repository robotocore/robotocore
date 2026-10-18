//! Cross-service message bridge.
//!
//! lets other service crates (EventBridge, SNS, ...) deliver SQS messages
//! without owning a `DefaultSqsHandler` instance. Pending messages are held in
//! a process-global buffer keyed by (account, region, queue name) and drained
//! into the real queue when the SQS handler reads the queue, so per-instance
//! unit-test isolation in the sqs crate is unaffected.

use crate::models::{SqsMessage, Queue};
use chrono::Utc;
use md5 as md5sum;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::OnceLock;

#[derive(Debug)]
struct PendingMessage {
    account: u64,
    region: String,
    queue_name: String,
    message: SqsMessage,
}

fn pending() -> &'static RwLock<Vec<PendingMessage>> {
    static PENDING: OnceLock<RwLock<Vec<PendingMessage>>> = OnceLock::new();
    PENDING.get_or_init(|| RwLock::new(Vec::new()))
}

fn new_message(body: &str) -> SqsMessage {
    SqsMessage {
        message_id: uuid::Uuid::new_v4().to_string(),
        body: body.to_string(),
        md5_of_body: format!("{:x}", md5sum::compute(body.as_bytes())),
        receipt_handle: String::new(),
        sent_timestamp: (Utc::now().timestamp_millis()) as u64,
        visibility_until: None,
        receive_count: 0,
        first_receive_timestamp: None,
        attributes: HashMap::new(),
        message_attributes: HashMap::new(),
        available_at: None,
        deduplication_id: None,
    }
}

/// Deliver a message body to a queue identified by SQS ARN
/// (`arn:aws:sqs:<region>:<account>:<queue>`). Returns false if the ARN is
/// malformed; returns true even if the queue was not created yet (the message
/// is buffered until the queue is read).
pub fn deliver_to_arn(queue_arn: &str, body: &str) -> bool {
    let parts: Vec<&str> = queue_arn.split(':').collect();
    if parts.len() < 6 || parts[2] != "sqs" {
        return false;
    }
    let region = parts[3].to_string();
    let account = parts[4].parse::<u64>().unwrap_or(0);
    // FIFO queue ARNs: QueueName matches the non-.fifo part of the ARN as-is.
    let name = parts[5].to_string();
    deliver_to_queue(account, &region, &name, body)
}

/// Deliver a message body to a queue identified by (account, region, name).
pub fn deliver_to_queue(account: u64, region: &str, queue_name: &str, body: &str) -> bool {
    let msg = new_message(body);
    pending().write().push(PendingMessage {
        account,
        region: region.to_string(),
        queue_name: queue_name.to_string(),
        message: msg,
    });
    true
}

/// Drain any pending cross-service messages for a queue into the queue's
/// message list so they are returned by the next receive.
pub fn drain_pending(account: u64, region: &str, queue_name: &str, queue: &mut Queue) {
    let drained: Vec<SqsMessage> = {
        let mut pend = pending().write();
        let mut drained = Vec::new();
        // idx always decreasing so remove keeps indices consistent.
        for idx in (0..pend.len()).rev() {
            let p = &pend[idx];
            if p.account == account
                && p.region == region
                && p.queue_name == queue_name
            {
                drained.push(p.message.clone());
                pend.remove(idx);
            }
        }
        drained
    };
    for msg in drained {
        queue.send_message(msg);
    }
}

/// Drop pending cross-service messages for a queue (PurgeQueue / DeleteQueue).
pub fn clear_pending(account: u64, region: &str, queue_name: &str) {
    pending().write().retain(|p| {
        !(p.account == account && p.region == region && p.queue_name == queue_name)
    });
}
