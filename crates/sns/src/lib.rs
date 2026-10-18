//! Native SNS service implementation for robotocore.
//!
//! Implements the core SNS operations using the query protocol (XML).

pub mod error;
pub mod handler;
pub mod models;
pub mod protocol;
pub mod snapshot;

pub use protocol::{AwsRequest, AwsResponse};

/// Default SNS handler backed by in-memory state.
pub struct DefaultSnsHandler {
    pub(crate) inner: handler::SnsHandler,
}

impl DefaultSnsHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::SnsHandler::new(),
        }
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultSnsHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultSnsHandler {
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
