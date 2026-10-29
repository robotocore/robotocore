//! Native CloudWatch Logs service for robotocore.
//!
//! JSON protocol (x-amz-json-1.1) with target prefix "Logs_20140328".

pub mod handler;
pub mod models;
pub mod protocol;
pub mod snapshot;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultLogsHandler {
    pub(crate) inner: handler::LogsHandler,
}

impl DefaultLogsHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::LogsHandler::new(),
        }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultLogsHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultLogsHandler {
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
