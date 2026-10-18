//! Native S3 service implementation for robotocore.
//!
//! Implements the core S3 operations using the rest-xml protocol.

pub mod bridge;
pub mod error;
pub mod handler;
pub mod models;
pub mod protocol;
pub mod snapshot;
pub mod xml;

pub use handler::S3Handler;
pub use protocol::{AwsRequest, AwsResponse};

/// Default S3 handler backed by in-memory state.
pub struct DefaultS3Handler {
    pub(crate) inner: handler::S3Handler,
}

impl DefaultS3Handler {
    pub fn new() -> Self {
        Self {
            inner: handler::S3Handler::new(),
        }
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultS3Handler {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultS3Handler {
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
