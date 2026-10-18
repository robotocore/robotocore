//! Native DynamoDB service implementation for robotocore.
//!
//! Implements the core DynamoDB operations using the json protocol (x-amz-json-1.0).

pub mod error;
pub mod expr;
pub mod handler;
pub mod models;
pub mod protocol;
pub mod snapshot;

pub use protocol::{AwsRequest, AwsResponse};

/// Default DynamoDB handler backed by in-memory state.
pub struct DefaultDynamoDbHandler {
    pub(crate) inner: handler::DynamoDbHandler,
}

impl DefaultDynamoDbHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::DynamoDbHandler::new(),
        }
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultDynamoDbHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultDynamoDbHandler {
    /// Serialize the full service state to JSON (state snapshot channel).
    pub fn snapshot_json(&self) -> serde_json::Value {
        self.inner.snapshot_json()
    }

    /// Replace the service state from JSON (state snapshot channel).
    pub fn restore_json(&self, v: &serde_json::Value) {
        self.inner.restore_json(v)
    }

    /// Clear all service state (state snapshot channel).
    pub fn reset(&self) {
        self.inner.reset()
    }
}
