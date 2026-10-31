//! DynamoDB request/response types for the json protocol.

use bytes::Bytes;
use serde_json::Value;

/// A parsed DynamoDB request.
#[derive(Debug, Clone)]
pub struct AwsRequest {
    pub service: String,
    pub operation: String,
    pub account: u64,
    pub region: String,
    /// The parsed JSON body as a Value.
    pub params: Value,
    /// The raw request body.
    pub body: Bytes,
}

/// A DynamoDB response to be serialized to HTTP.
#[derive(Debug, Clone)]
pub struct AwsResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl AwsResponse {
    pub fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            headers: vec![
                (
                    "Content-Type".to_string(),
                    "application/x-amz-json-1.0".to_string(),
                ),
                (
                    "x-amzn-RequestId".to_string(),
                    uuid::Uuid::new_v4().to_string(),
                ),
                (
                    "x-amzn-RequestId".to_string(),
                    uuid::Uuid::new_v4().to_string(),
                ),
            ],
            body: serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string()),
        }
    }

    pub fn error(status: u16, code: &str, message: &str) -> Self {
        let body = serde_json::json!({
            "__type": code,
            "message": message
        });
        Self {
            status,
            headers: vec![
                (
                    "Content-Type".to_string(),
                    "application/x-amz-json-1.0".to_string(),
                ),
                (
                    "x-amzn-RequestId".to_string(),
                    uuid::Uuid::new_v4().to_string(),
                ),
            ],
            body: serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string()),
        }
    }

    /// An error response carrying the caller-built JSON body — same headers
    /// as `error`, for codes that need extra members beyond `__type`/`message`
    /// (e.g. moto's TransactionCanceledException with per-row CancellationReasons).
    pub fn error_with_body(status: u16, body: Value) -> Self {
        Self {
            status,
            headers: vec![
                (
                    "Content-Type".to_string(),
                    "application/x-amz-json-1.0".to_string(),
                ),
                (
                    "x-amzn-RequestId".to_string(),
                    uuid::Uuid::new_v4().to_string(),
                ),
            ],
            body: serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string()),
        }
    }

}
