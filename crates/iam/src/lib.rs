//! Native IAM service for robotocore.
//!
//! Query protocol (XML) — the largest IAM operation surface.

pub mod handler;
pub mod models;
pub mod policy;
pub mod protocol;
pub mod snapshot;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultIamHandler {
    pub(crate) inner: handler::IamHandler,
}

impl DefaultIamHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::IamHandler::new(),
        }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultIamHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultIamHandler {
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
