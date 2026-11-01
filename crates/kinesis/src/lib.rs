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

    /// Last-assigned sequence number per shard of the stream (`None` when
    /// the stream does not exist) — the engine's parked-cursor "new
    /// content" frontier: a frontier strictly greater than the failed
    /// batch's last-ASSIGNED watermark means a record actually landed after
    /// the failure. (`attach_watermarks` dishes up the next-to-assign form
    /// instead — iterator bindings need `counter+1`.)
    pub fn shard_frontiers(
        &self,
        account: u64,
        region: &str,
        stream_name: &str,
    ) -> Vec<(String, String)> {
        let state = self.inner.get_state(account, region);
        let Some(stream) = state.get_stream(stream_name) else {
            return Vec::new();
        };
        let shards = stream.shards.read();
        shards
            .iter()
            .map(|sh| (sh.shard_id.clone(), sh.get_last_assigned_sequence()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make(op: &str, params: serde_json::Value) -> AwsRequest {
        AwsRequest {
            service: "kinesis".to_string(),
            operation: op.to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            params,
            body: bytes::Bytes::new(),
        }
    }

    /// The ESM parked-cursor frontier contract:
    /// `shard_frontiers` reports the last ASSIGNED sequence (so it stays
    /// put when nothing lands — a closed shard's frontier is static
    /// forever, which is what stops phantom new-content flips), while
    /// `attach_watermarks` keeps the next-to-assign form the iterator
    /// bindings need.
    #[test]
    fn shard_frontiers_are_last_assigned_not_next_to_assign() {
        let svc = DefaultKinesisHandler::new();
        svc.handle(make(
            "CreateStream",
            serde_json::json!({"StreamName": "frontier-stream", "ShardCount": 1}),
        ));
        svc.handle(make(
            "PutRecord",
            serde_json::json!({"StreamName": "frontier-stream", "Data": "a", "PartitionKey": "k"}),
        ));
        svc.handle(make(
            "PutRecord",
            serde_json::json!({"StreamName": "frontier-stream", "Data": "b", "PartitionKey": "k"}),
        ));

        let frontiers = svc.shard_frontiers(123456789012, "us-east-1", "frontier-stream");
        assert_eq!(frontiers.len(), 1);
        assert_eq!(
            frontiers[0].1,
            format!("{:020}", 2),
            "two puts → frontier is the last ASSIGNED sequence, not counter+1"
        );
        let marks = svc.attach_watermarks(123456789012, "us-east-1", "frontier-stream");
        assert_eq!(marks.len(), 1,);
        assert_eq!(
            marks[0].1,
            format!("{:020}", 3),
            "attach watermark stays the next-to-assign form for bindings"
        );

        // Reshard: the split parent closes and its frontier freezes —
        // puts can only land on the open children.
        svc.handle(make(
            "UpdateShardCount",
            serde_json::json!({
                "StreamName": "frontier-stream",
                "TargetShardCount": 2,
                "ScalingType": "UNIFORM_SCALING"
            }),
        ));
        let closed = svc
            .shard_frontiers(123456789012, "us-east-1", "frontier-stream")
            .into_iter()
            .find(|(id, _)| id == "shardId-000000000000")
            .expect("split parent is still listed");
        assert_eq!(
            closed.1,
            format!("{:020}", 2),
            "parent keeps its ending sequence"
        );
        svc.handle(make(
            "PutRecord",
            serde_json::json!({"StreamName": "frontier-stream", "Data": "c", "PartitionKey": "k"}),
        ));
        let after = svc.shard_frontiers(123456789012, "us-east-1", "frontier-stream");
        let after_closed = &after
            .iter()
            .find(|(id, _)| id == "shardId-000000000000")
            .unwrap()
            .1;
        assert_eq!(
            after_closed, &closed.1,
            "closed shard's frontier must be static (no phantom new content)"
        );
    }
}
