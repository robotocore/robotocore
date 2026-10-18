//! Native Cloudwatch service for robotocore.
//! Protocol: smithy-rpc-v2-cbor, Target: GraniteServiceVersion20100801

pub mod handler;
pub mod models;
pub mod protocol;
pub mod snapshot;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultCloudwatchHandler {
    pub(crate) inner: handler::CloudwatchHandler,
}

impl DefaultCloudwatchHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::CloudwatchHandler::new(),
        }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultCloudwatchHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultCloudwatchHandler {
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
