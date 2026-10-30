//! Native Kinesis service for robotocore.
//!
//! JSON protocol (x-amz-json-1.1) with target prefix "Kinesis_20131202".

pub mod handler;
pub mod models;
pub mod protocol;
pub mod snapshot;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultKinesisHandler {
    pub(crate) inner: handler::KinesisHandler,
}

impl DefaultKinesisHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::KinesisHandler::new(),
        }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultKinesisHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultKinesisHandler {
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

    /// Attach-time cursor binding for the ESM engine — see
    /// `handler::KinesisHandler::attach_watermarks` for the moto-semantics
    /// rationale.
    pub fn attach_watermarks(
        &self,
        account: u64,
        region: &str,
        stream_name: &str,
    ) -> Vec<(String, String)> {
        self.inner.attach_watermarks(account, region, stream_name)
    }

    /// CreateEventSourceMapping existence probe — see
    /// `handler::KinesisHandler::has_stream`.
    pub fn has_stream(&self, account: u64, region: &str, stream_name: &str) -> bool {
        self.inner.has_stream(account, region, stream_name)
    }

    /// The newest sequence number that would be assigned on each shard of
    /// the stream (`None` when the stream does not exist) — the engine's
    /// parked-cursor "new content" signal (see `attach_watermarks` for the
    /// watermarks channel).
    pub fn shard_frontiers(
        &self,
        account: u64,
        region: &str,
        stream_name: &str,
    ) -> Vec<(String, String)> {
        self.inner.attach_watermarks(account, region, stream_name)
    }
}
