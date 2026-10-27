//! Kinesis request/response types (json protocol).

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
                    "application/x-amz-json-1.0".to_string(),
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
                    "application/x-amz-json-1.0".to_string(),
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

    /// AWS query-protocol error envelope (`<ErrorResponse><Error>`), for
    /// form-encoded `Action=` requests (the same call shape reads as the
    /// JSON protocol via `X-Amz-Target`).
    pub fn query_error(status: u16, code: &str, message: &str) -> Self {
        let esc = |s: &str| {
            s.replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;")
        };
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <ErrorResponse xmlns=\"http://monitoring.amazonaws.com/doc/2010-08-01/\">\
             <Error><Type>Sender</Type><Code>{code}</Code>\
             <Message>{message}</Message></Error>\
             <RequestId>{rid}</RequestId></ErrorResponse>",
            code = esc(code),
            message = esc(message),
            rid = uuid::Uuid::new_v4()
        );
        Self {
            status,
            headers: vec![
                ("Content-Type".to_string(), "text/xml".to_string()),
                (
                    "x-amzn-RequestId".to_string(),
                    uuid::Uuid::new_v4().to_string(),
                ),
                ("server".to_string(), "robotocore".to_string()),
            ],
            body,
        }
    }
}
