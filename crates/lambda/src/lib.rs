//! Native Lambda service for robotocore.
//!
//! rest-json protocol — functions, aliases, event source mappings, layers.

pub mod handler;
pub mod models;
pub mod protocol;
pub mod runtime;
pub mod snapshot;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultLambdaHandler {
    pub(crate) inner: handler::LambdaHandler,
}

impl DefaultLambdaHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::LambdaHandler::new(),
        }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultLambdaHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultLambdaHandler {
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
