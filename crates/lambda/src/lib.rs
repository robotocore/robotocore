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

    /// Engine hook: snapshot of every stored ESM (see `LambdaHandler`).
    pub fn esm_snapshot(&self) -> Vec<crate::models::EsmMappingEntry> {
        self.inner.esm_snapshot()
    }

    /// Engine hook: run one ESM-driven invocation (see `LambdaHandler`).
    pub fn invoke_engine_event(
        &self,
        account: u64,
        region: &str,
        function_name: &str,
        event: &serde_json::Value,
    ) -> crate::models::EngineInvokeResult {
        self.inner
            .invoke_engine_event(account, region, function_name, event)
    }

    /// Function-URL lookup by url-id (see `LambdaHandler`).
    pub fn find_url_config(&self, url_id: &str) -> Option<(u64, String, serde_json::Value)> {
        self.inner.find_url_config(url_id)
    }
}
