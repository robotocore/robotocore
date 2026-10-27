//! Native Stepfunctions service for robotocore.
//! Protocol: json, Target: AWSStepFunctions

pub mod asl;
pub mod handler;
pub mod jsonata;
pub mod jsonpath;
pub mod models;
pub mod protocol;
pub mod snapshot;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultStepfunctionsHandler {
    pub(crate) inner: handler::StepfunctionsHandler,
}

impl DefaultStepfunctionsHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::StepfunctionsHandler::new(),
        }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultStepfunctionsHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultStepfunctionsHandler {
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
