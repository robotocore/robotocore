//! Native KMS service for robotocore.
//!
//! JSON protocol (x-amz-json-1.1) with target prefix "TrentService".

pub mod handler;
pub mod models;
pub mod protocol;
pub mod snapshot;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultKmsHandler {
    pub(crate) inner: handler::KmsHandler,
}

impl DefaultKmsHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::KmsHandler::new(),
        }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultKmsHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultKmsHandler {
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
