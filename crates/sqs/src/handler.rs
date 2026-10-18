//! SQS request handler implementation

use crate::error::SqsError;
use crate::models::{Queue, SqsMessage, SqsStore};
use crate::protocol::{AwsRequest, AwsResponse, SqsHandler};
use chrono::Utc;
use md5;
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

type StoreMap = Arc<RwLock<HashMap<(u64, String), Arc<SqsStore>>>>;

/// Process-wide store map shared by every `DefaultSqsHandler` instance.
///
/// Cross-service callers (SNS message delivery) construct their own handler
/// instance; sharing the store map lets those instances see the same queues
/// as the server's registered SQS handler, mirroring Moto's in-process
/// backend semantics.
fn global_stores() -> StoreMap {
    use std::sync::OnceLock;
    static STORES: OnceLock<StoreMap> = OnceLock::new();
    STORES
        .get_or_init(|| Arc::new(RwLock::new(HashMap::new())))
        .clone()
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
        let key = (account, region.to_string());
        stores
            .entry(key)
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

    fn generate_receipt_handle(message_id: &str, queue_url: &str) -> String {
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
        if store.get_queue(queue_name).is_some() {
            // Return the existing queue URL (idempotent)
            let url = format!(
                "http://sqs.{}.localhost.robotocore.cloud:4566/{}/{}",
                region, account, queue_name
            );
            let body = json!({
                "QueueUrl": url
            })
            .to_string();
            return Ok(AwsResponse {
                status: 200,
                headers: vec![(
                    "Content-Type".to_string(),
                    "application/x-amz-json-1.0".to_string(),
                )],
                body,
            });
        }

        // Create new queue
        let url = format!(
            "http://sqs.{}.localhost.robotocore.cloud:4566/{}/{}",
            region, account, queue_name
        );
        let arn = format!("arn:aws:sqs:{}:{}:{}", region, account, queue_name);

        let mut queue = Queue::new(
            queue_name.to_string(),
            region.to_string(),
            account.to_string(),
            url.clone(),
            arn,
        );

        // Apply attributes if provided
        if let Some(attrs) = params.get("Attributes").and_then(|v| v.as_object()) {
            for (key, value) in attrs {
                let val = value.as_str().unwrap_or("");
                match key.as_str() {
                    "VisibilityTimeout" => {
                        if let Ok(v) = val.parse::<u32>() {
                            queue.attributes.visibility_timeout = v;
                        }
                    }
                    "DelaySeconds" => {
                        if let Ok(v) = val.parse::<u32>() {
                            queue.attributes.delay_seconds = v;
                        }
                    }
                    "ReceiveMessageWaitTimeSeconds" => {
                        if let Ok(v) = val.parse::<u32>() {
                            queue.attributes.receive_message_wait_time = v;
                        }
                    }
                    "MaximumMessageSize" => {
                        if let Ok(v) = val.parse::<u32>() {
                            queue.attributes.maximum_message_size = v;
                        }
                    }
                    "MessageRetentionPeriod" => {
                        if let Ok(v) = val.parse::<u32>() {
                            queue.attributes.message_retention_period = v;
                        }
                    }
                    "Policy" => {
                        queue.attributes.policy = Some(val.to_string());
                    }
                    _ => {}
                }
            }
        }

        store.create_queue(queue);

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

        // Extract queue name from URL
        let queue_name = self.extract_queue_name_from_url(queue_url)?;

        let store = self.get_store(account, region);
        let queue_opt = store.get_queue(&queue_name);
        let queue_arc = queue_opt.ok_or_else(|| {
            SqsError::NonExistentQueue(format!("Queue {} does not exist", queue_url))
        })?;

        let message_id = Uuid::new_v4().to_string();
        let md5_body = Self::md5_hash(message_body);
        let receipt_handle = Self::generate_receipt_handle(&message_id, queue_url);
        let sent_timestamp = (Utc::now().timestamp_millis()) as u64;

        let message_attributes: HashMap<String, serde_json::Value> = params.get("MessageAttributes")
            .and_then(|v| v.as_object())
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        let message = SqsMessage {
            message_id: message_id.clone(),
            body: message_body.to_string(),
            md5_of_body: md5_body.clone(),
            receipt_handle,
            sent_timestamp,
            visibility_until: None,
            receive_count: 0,
            first_receive_timestamp: None,
            attributes: HashMap::new(),
            message_attributes,
        };

        {
            let mut q = queue_arc.write();
            q.send_message(message);
        }

        let body = json!({
            "MessageId": message_id,
            "MD5OfMessageBody": md5_body,
            "MD5OfMessageAttributes": Self::md5_hash("")
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

    fn handle_change_visibility_batch(
        &self,
        params: &Value,
        account: u64,
        region: &str,
    ) -> Result<AwsResponse, SqsError> {
        let queue_url = params.get("QueueUrl")
            .and_then(|v| v.as_str()).unwrap_or_default().to_string();
        let queue_name = self.extract_queue_name_from_url(&queue_url)?;
        let store = self.get_store(account, region);
        let queue_opt = store.get_queue(&queue_name);
        let queue_arc = queue_opt.ok_or_else(|| {
            SqsError::NonExistentQueue(format!("Queue {} does not exist", queue_url))
        })?;

        let entries = params.get("Entries")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut successful: Vec<Value> = Vec::new();
        let mut failed: Vec<Value> = Vec::new();

        for entry in &entries {
            let id = entry.get("Id").and_then(|v| v.as_str()).unwrap_or_default().to_string();
            let receipt_handle = entry.get("ReceiptHandle")
                .and_then(|v| v.as_str()).unwrap_or_default().to_string();
            let visibility = entry.get("VisibilityTimeout")
                .and_then(|v| v.as_u64())
                .unwrap_or(30) as u64;
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
        let queue_url = params.get("QueueUrl")
            .and_then(|v| v.as_str()).unwrap_or_default().to_string();
        let queue_name = self.extract_queue_name_from_url(&queue_url)?;
        let store = self.get_store(account, region);
        let queue_opt = store.get_queue(&queue_name);
        let queue_arc = queue_opt.ok_or_else(|| {
            SqsError::NonExistentQueue(format!("Queue {} does not exist", queue_url))
        })?;

        let entries = params.get("Entries")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut successful: Vec<Value> = Vec::new();
        let mut failed: Vec<Value> = Vec::new();

        for entry in &entries {
            let id = entry.get("Id").and_then(|v| v.as_str()).unwrap_or_default().to_string();
            let receipt_handle = entry.get("ReceiptHandle")
                .and_then(|v| v.as_str()).unwrap_or_default().to_string();
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

        let visibility_timeout = params
            .get("VisibilityTimeout")
            .and_then(|v| v.as_u64())
            .unwrap_or(30) as u32;

        // Extract queue name from URL
        let queue_name = self.extract_queue_name_from_url(queue_url)?;

        let store = self.get_store(account, region);
        let queue_opt = store.get_queue(&queue_name);
        let queue_arc = queue_opt.ok_or_else(|| {
            SqsError::NonExistentQueue(format!("Queue {} does not exist", queue_url))
        })?;

        let messages = {
            let mut q = queue_arc.write();
            q.receive_messages(max_messages, visibility_timeout)
        };

        let msg_objs: Vec<Value> = messages
            .into_iter()
            .map(|msg| {
                json!({
                    "MessageId": msg.message_id,
                    "ReceiptHandle": msg.receipt_handle,
                    "MD5OfBody": msg.md5_of_body,
                    "Body": msg.body,
                    "Attributes": msg.attributes,
                    "MessageAttributes": msg.message_attributes,
                    "MD5OfMessageAttributes": Self::md5_hash("")
                })
            })
            .collect();

        let body = if msg_objs.is_empty() {
            json!({
                "Messages": []
            })
            .to_string()
        } else {
            json!({
                "Messages": msg_objs
            })
            .to_string()
        };

        Ok(AwsResponse {
            status: 200,
            headers: vec![(
                "Content-Type".to_string(),
                "application/x-amz-json-1.0".to_string(),
            )],
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
            Value::String(q.messages.len().to_string()),
        );
        attrs.insert(
            "ApproximateNumberOfMessagesNotVisible".to_string(),
            Value::String(
                q.messages
                    .iter()
                    .filter(|m| !m.is_visible())
                    .count()
                    .to_string(),
            ),
        );
        attrs.insert(
            "ApproximateNumberOfMessagesDelayed".to_string(),
            Value::String("0".to_string()),
        );

        let body = json!({
            "Attributes": attrs
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

        let attrs = params.get("Attributes").and_then(|v| v.as_object());
        if let Some(attrs) = attrs {
            let mut q = queue_arc.write();
            for (key, value) in attrs {
                let val = value.as_str().unwrap_or("");
                match key.as_str() {
                    "VisibilityTimeout" => {
                        if let Ok(v) = val.parse::<u32>() {
                            q.attributes.visibility_timeout = v;
                        }
                    }
                    "DelaySeconds" => {
                        if let Ok(v) = val.parse::<u32>() {
                            q.attributes.delay_seconds = v;
                        }
                    }
                    "ReceiveMessageWaitTimeSeconds" => {
                        if let Ok(v) = val.parse::<u32>() {
                            q.attributes.receive_message_wait_time = v;
                        }
                    }
                    "MaximumMessageSize" => {
                        if let Ok(v) = val.parse::<u32>() {
                            q.attributes.maximum_message_size = v;
                        }
                    }
                    "MessageRetentionPeriod" => {
                        if let Ok(v) = val.parse::<u32>() {
                            q.attributes.message_retention_period = v;
                        }
                    }
                    "Policy" => {
                        q.attributes.policy = Some(val.to_string());
                    }
                    _ => {}
                }
                q.last_modified = (Utc::now().timestamp_millis()) as u64;
            }
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
            q.purge();
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
    fn json_stub_batch(&self, params: &Value) -> AwsResponse {
        let entries: Vec<Value> = params.get("Entries")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let successful: Vec<Value> = entries.iter()
            .map(|e| {
                let id = e.get("Id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                json!({ "Id": id })
            })
            .collect();
        let body = json!({ "Successful": successful, "Failed": Vec::<Value>::new() }).to_string();
        AwsResponse {
            status: 200,
            headers: vec![("Content-Type".to_string(), "application/x-amz-json-1.0".to_string())],
            body,
        }
    }

    fn json_success(&self) -> AwsResponse {
        AwsResponse {
            status: 200,
            headers: vec![("Content-Type".to_string(), "application/x-amz-json-1.0".to_string())],
            body: "{}".to_string(),
        }
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
        let queue_arc = store
            .get_queue(&queue_name)
            .ok_or_else(|| SqsError::NonExistentQueue(format!("Queue {} does not exist", queue_url)))?;

        let mut successful: Vec<Value> = Vec::new();

        for entry in &entries {
            let id = entry.get("Id").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let body = entry.get("MessageBody").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let message_id = Uuid::new_v4().to_string();
            let md5_body = Self::md5_hash(&body);
            let receipt_handle = Self::generate_receipt_handle(&message_id, queue_url);
            let sent_timestamp = Utc::now().timestamp_millis() as u64;

            let message_attributes: HashMap<String, serde_json::Value> = entry.get("MessageAttributes")
                .and_then(|v| v.as_object())
                .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or_default();
            let message = SqsMessage {
                message_id: message_id.clone(),
                body,
                md5_of_body: md5_body.clone(),
                receipt_handle,
                sent_timestamp,
                visibility_until: None,
                receive_count: 0,
                first_receive_timestamp: None,
                attributes: HashMap::new(),
                message_attributes,
            };

            {
                let mut q = queue_arc.write();
                q.send_message(message);
            }

            successful.push(json!({
                "Id": id,
                "MessageId": message_id,
                "MD5OfMessageBody": md5_body,
            }));
        }

        let body = json!({
            "Successful": successful,
            "Failed": Vec::<Value>::new()
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


    fn handle_tag_queue(&self, params: &Value, account: u64, region: &str) -> Result<AwsResponse, SqsError> {
        let queue_url = params.get("QueueUrl").and_then(|v| v.as_str()).unwrap_or_default();
        let tags_val = params.get("Tags").cloned().unwrap_or(Value::Null);
        let tags: HashMap<String, String> = match tags_val {
            Value::Object(m) => m.into_iter().map(|(k, v)| (k, v.as_str().unwrap_or("").to_string())).collect(),
            _ => {
                // Parse from query protocol format (Tags.member.N.Key/Value)
                let mut result = HashMap::new();
                let mut i = 1;
                loop {
                    let key_name = format!("Tags.member.{}.Key", i);
                    let val_name = format!("Tags.member.{}.Value", i);
                    match (params.get(&key_name), params.get(&val_name)) {
                        (Some(k), Some(v)) => {
                            result.insert(k.as_str().unwrap_or("").to_string(), v.as_str().unwrap_or("").to_string());
                            i += 1;
                        }
                        _ => break,
                    }
                }
                result
            }
        };
        let store = self.get_store(account, region);
        if let Some(q) = store.get_queue(queue_url) {
            let mut queue = q.write();
            for (k, v) in tags {
                queue.tags.write().insert(k, v);
            }
        }
        let body = json!({}).to_string();
        Ok(AwsResponse {
            status: 200,
            headers: vec![("Content-Type".to_string(), "application/x-amz-json-1.0".to_string())],
            body,
        })
    }

    fn handle_untag_queue(&self, params: &Value, account: u64, region: &str) -> Result<AwsResponse, SqsError> {
        let queue_url = params.get("QueueUrl").and_then(|v| v.as_str()).unwrap_or_default();
        let tag_keys: Vec<String> = params.get("TagKeys")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default();
        let store = self.get_store(account, region);
        if let Some(q) = store.get_queue(queue_url) {
            let mut queue = q.write();
            for k in &tag_keys {
                queue.tags.write().remove(k);
            }
        }
        let body = json!({}).to_string();
        Ok(AwsResponse {
            status: 200,
            headers: vec![("Content-Type".to_string(), "application/x-amz-json-1.0".to_string())],
            body,
        })
    }

    fn handle_list_queue_tags(&self, params: &Value, account: u64, region: &str) -> Result<AwsResponse, SqsError> {
        let queue_url = params.get("QueueUrl").and_then(|v| v.as_str()).unwrap_or_default();
        let store = self.get_store(account, region);
        let tags = store.get_queue(queue_url).map(|q| q.read().tags.read().clone()).unwrap_or_default();
        let tag_list: Vec<Value> = tags.iter()
            .map(|(k, v)| json!({"Key": k, "Value": v}))
            .collect();
        let body = json!({ "Tags": tag_list }).to_string();
        Ok(AwsResponse {
            status: 200,
            headers: vec![("Content-Type".to_string(), "application/x-amz-json-1.0".to_string())],
            body,
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
            "DeleteMessageBatch" => self.handle_delete_message_batch(&req.params, req.account, &req.region),
            "ChangeMessageVisibilityBatch" => self.handle_change_visibility_batch(&req.params, req.account, &req.region),
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
            "SendMessageBatch" => self.handle_send_message_batch(&req.params, req.account, &req.region),
            "TagQueue" => self.handle_tag_queue(&req.params, req.account, &req.region),
            "UntagQueue" => self.handle_untag_queue(&req.params, req.account, &req.region),
            "ListQueueTags" => self.handle_list_queue_tags(&req.params, req.account, &req.region),
            _ => Err(SqsError::ValidationError(format!(
                "Unknown operation: {}",
                req.operation
            ))),
        }
    }
}
