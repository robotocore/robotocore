//! Protocol definitions for AWS request/response handling
//!
//! Minimal stable interface that W2 can integrate with ~30 lines of glue.

use bytes::Bytes;
use serde_json::Value;

/// Incoming AWS API request
#[derive(Debug, Clone)]
pub struct AwsRequest {
    /// AWS service name (e.g. "sqs")
    pub service: String,
    /// Operation name (e.g. "SendMessage")
    pub operation: String,
    /// AWS account ID (12-digit number)
    pub account: u64,
    /// AWS region (e.g. "us-east-1")
    pub region: String,
    /// Parsed parameters/body as JSON value
    pub params: Value,
    /// Raw body bytes
    pub body: Bytes,
}

/// Outgoing AWS API response
#[derive(Debug, Clone)]
pub struct AwsResponse {
    /// HTTP status code
    pub status: u16,
    /// Response headers
    pub headers: Vec<(String, String)>,
    /// Response body (JSON or XML)
    pub body: String,
}

/// SQS handler trait for processing requests
pub trait SqsHandler: Send + Sync {
    /// Handle an incoming AWS request and return a response
    fn handle(&self, req: AwsRequest) -> AwsResponse;
}
