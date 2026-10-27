//! IAM request/response types (query protocol, XML).

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
    /// Raw query string for query-protocol services.
    pub query: String,
}

#[derive(Debug, Clone)]
pub struct AwsResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl AwsResponse {
    /// Build an XML response for a successful IAM operation.
    pub fn xml(status: u16, root: &str, body_xml: String) -> Self {
        let full_body = format!(
            "<{root}Response xmlns=\"https://iam.amazonaws.com/doc/2010-05-08/\"><{root}Result>{body_xml}</{root}Result></{root}Response>"
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
            body: full_body,
        }
    }

    pub fn error(status: u16, code: &str, message: &str) -> Self {
        let body = format!(
            "<ErrorResponse xmlns=\"https://iam.amazonaws.com/doc/2010-05-08/\">\
             <Error>\
             <Type>Sender</Type>\
             <Code>{}</Code>\
             <Message>{}</Message>\
             </Error>\
             <RequestId>{}</RequestId>\
             </ErrorResponse>",
            code,
            message,
            uuid::Uuid::new_v4()
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

/// Extract a parameter from query string or JSON params.
pub fn get_param(req: &AwsRequest, key: &str) -> Option<String> {
    // Try JSON params first
    if let Some(v) = req.params.get(key).and_then(|v| v.as_str()) {
        return Some(v.to_string());
    }
    // Fall back to query string
    if !req.query.is_empty() {
        for pair in req.query.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                if k == key {
                    return Some(v.to_string());
                }
            }
        }
    }
    None
}
