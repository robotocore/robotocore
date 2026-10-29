//! SNS request/response types for the query protocol.

use bytes::Bytes;
use serde_json::Value;

/// A parsed SNS request.
#[derive(Debug, Clone)]
pub struct AwsRequest {
    pub service: String,
    pub operation: String,
    pub account: u64,
    pub region: String,
    pub params: Value,
    pub body: Bytes,
}

/// An SNS response to be serialized to HTTP (query protocol XML).
#[derive(Debug, Clone)]
pub struct AwsResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl AwsResponse {
    /// Create a successful query-protocol XML response.
    pub fn query_success(operation: &str, body_xml: String) -> Self {
        let request_id = uuid::Uuid::new_v4().to_string();
        let full_body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<{op}Response xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <{op}Result>
{body}
  </{op}Result>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</{op}Response>"#,
            op = operation,
            body = body_xml,
            request_id = request_id,
        );
        Self {
            status: 200,
            headers: vec![
                ("Content-Type".to_string(), "text/xml".to_string()),
                ("server".to_string(), "robotocore".to_string()),
            ],
            body: full_body,
        }
    }

    /// Create an error response in query-protocol XML format.
    pub fn error(status: u16, code: &str, message: &str) -> Self {
        let request_id = uuid::Uuid::new_v4().to_string();
        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <Error>
    <Type>Sender</Type>
    <Code>{code}</Code>
    <Message>{message}</Message>
  </Error>
  <RequestId>{request_id}</RequestId>
</ErrorResponse>"#,
            code = code,
            message = message,
            request_id = request_id,
        );
        Self {
            status,
            headers: vec![
                ("Content-Type".to_string(), "text/xml".to_string()),
                ("server".to_string(), "robotocore".to_string()),
            ],
            body,
        }
    }
}
