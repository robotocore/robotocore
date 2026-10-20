//! S3 request/response types for the rest-xml protocol.

use bytes::Bytes;
use serde_json::Value;

/// A parsed S3 request.
#[derive(Debug, Clone)]
pub struct AwsRequest {
    pub service: String,
    pub operation: String,
    pub account: u64,
    pub region: String,
    /// The bucket name (extracted from path or virtual host).
    pub bucket: Option<String>,
    /// The object key (for object-level operations).
    pub key: Option<String>,
    /// Query parameters from the URL.
    pub query_params: std::collections::HashMap<String, String>,
    /// Request headers.
    pub headers: std::collections::HashMap<String, String>,
    /// The HTTP method (GET, PUT, POST, DELETE, HEAD).
    pub method: String,
    /// The request body (for PUT/POST).
    pub body: Bytes,
    /// The request parameters parsed from the body.
    pub params: Value,
}

/// An S3 response to be serialized to HTTP.
#[derive(Debug, Clone)]
pub struct AwsResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    /// The raw response body (XML for most S3 operations, binary for GetObject).
    pub body: Vec<u8>,
}

impl AwsResponse {
    pub fn xml(status: u16, body: String) -> Self {
        Self {
            status,
            headers: vec![
                ("Content-Type".to_string(), "application/xml".to_string()),
                (
                    "x-amz-request-id".to_string(),
                    uuid::Uuid::new_v4().to_string(),
                ),
            ],
            body: body.into_bytes(),
        }
    }

    pub fn binary(status: u16, body: Vec<u8>, content_type: &str) -> Self {
        Self {
            status,
            headers: vec![
                ("Content-Type".to_string(), content_type.to_string()),
                (
                    "x-amz-request-id".to_string(),
                    uuid::Uuid::new_v4().to_string(),
                ),
            ],
            body,
        }
    }

    pub fn no_content(status: u16) -> Self {
        Self {
            status,
            headers: vec![(
                "x-amz-request-id".to_string(),
                uuid::Uuid::new_v4().to_string(),
            )],
            body: vec![],
        }
    }

    pub fn error(status: u16, code: &str, message: &str) -> Self {
        let body = crate::xml::error_response(code, message);
        Self {
            status,
            headers: vec![
                ("Content-Type".to_string(), "application/xml".to_string()),
                (
                    "x-amz-request-id".to_string(),
                    uuid::Uuid::new_v4().to_string(),
                ),
                ("server".to_string(), "robotocore".to_string()),
            ],
            body: body.into_bytes(),
        }
    }

    /// Bucket-scoped error (moto S3 `bucket_error` template): adds the
    /// `<BucketName>` element that AWS S3 and moto include on bucket errors.
    pub fn bucket_error(status: u16, code: &str, message: &str, bucket: &str) -> Self {
        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>{code}</Code>
  <Message>{message}</Message>
  <BucketName>{name}</BucketName>
</Error>"#,
            code = code,
            message = message,
            name = bucket
        );
        Self {
            status,
            headers: vec![
                ("Content-Type".to_string(), "application/xml".to_string()),
                (
                    "x-amz-request-id".to_string(),
                    uuid::Uuid::new_v4().to_string(),
                ),
                ("server".to_string(), "robotocore".to_string()),
            ],
            body: body.into_bytes(),
        }
    }
}
