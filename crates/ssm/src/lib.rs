//! Native SSM service for robotocore.
//!
//! JSON protocol (x-amz-json-1.0) with target prefix "AmazonSSM".

pub mod handler;
pub mod models;
pub mod protocol;
pub mod snapshot;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultSsmHandler {
    pub(crate) inner: handler::SsmHandler,
}

impl DefaultSsmHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::SsmHandler::new(),
        }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultSsmHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultSsmHandler {
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
