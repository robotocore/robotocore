//! Native EventBridge service for robotocore.
//!
//! JSON protocol (x-amz-json-1.0) with target prefix "AWSEvents".

pub mod handler;
pub mod models;
pub mod pattern;
pub mod protocol;
pub mod snapshot;

pub use handler::publish_event_to_bus;
pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultEventsHandler {
    pub(crate) inner: handler::EventsHandler,
}

impl DefaultEventsHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::EventsHandler::new(),
        }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultEventsHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultEventsHandler {
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
