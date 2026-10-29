//! Native Secrets Manager service for robotocore.
//!
//! JSON protocol (x-amz-json-1.1) with target prefix "secretsmanager".

pub mod handler;
pub mod models;
pub mod protocol;
pub mod snapshot;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultSecretsManagerHandler {
    pub(crate) inner: handler::SecretsManagerHandler,
}

impl DefaultSecretsManagerHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::SecretsManagerHandler::new(),
        }
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultSecretsManagerHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultSecretsManagerHandler {
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
