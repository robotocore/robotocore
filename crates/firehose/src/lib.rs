//! Native Firehose service for robotocore.
//! Protocol: json, Target: Firehose_20150804

pub mod handler;
pub mod models;
pub mod protocol;
pub mod snapshot;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultFirehoseHandler {
    pub(crate) inner: handler::FirehoseHandler,
}

impl DefaultFirehoseHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::FirehoseHandler::new(),
        }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultFirehoseHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultFirehoseHandler {
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
