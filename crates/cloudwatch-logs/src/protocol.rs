//! CloudWatch Logs request/response types (json protocol).

use bytes::Bytes;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct AwsRequest {
    pub service: String,
    pub operation: String,
    pub account: u64,
    pub region: String,
    pub params: Value,
    pub body: Bytes,
}

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
                    "application/x-amz-json-1.1".to_string(),
                ),
                (
                    "x-amzn-RequestId".to_string(),
                    uuid::Uuid::new_v4().to_string(),
                ),
                ("server".to_string(), "robotocore".to_string()),
            ],
            body: serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string()),
        }
    }

    pub fn error(status: u16, code: &str, message: &str) -> Self {
        let body = serde_json::json!({ "__type": code, "message": message });
        Self {
            status,
            headers: vec![
                (
                    "Content-Type".to_string(),
                    "application/x-amz-json-1.1".to_string(),
                ),
                (
                    "x-amzn-RequestId".to_string(),
                    uuid::Uuid::new_v4().to_string(),
                ),
                ("server".to_string(), "robotocore".to_string()),
            ],
            body: serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string()),
        }
    }
}
