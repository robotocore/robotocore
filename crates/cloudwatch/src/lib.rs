//! Native Cloudwatch service for robotocore.
//! Protocol: smithy-rpc-v2-cbor, Target: GraniteServiceVersion20100801

pub mod bridge;
pub mod cbor;
pub mod handler;
pub mod models;
pub mod protocol;
pub mod snapshot;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultCloudwatchHandler {
    /// The handler inside an `Arc` so `bridge` can keep a live handle for
    /// cross-service ingests without its own dependency on the callers.
    pub(crate) inner: std::sync::Arc<handler::CloudwatchHandler>,
}

impl DefaultCloudwatchHandler {
    pub fn new() -> Self {
        let inner = std::sync::Arc::new(handler::CloudwatchHandler::new());
        // Arm the cross-service bridge (kinesis put_record/put_records push
        // their IncomingBytes/IncomingRecords datapoints through it).
        bridge::register_handler(&inner);
        Self { inner }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }

    /// Serve a Smithy RPC v2 CBOR request: `req.body` is the CBOR payload
    /// (its `params` are ignored) and the response is CBOR-encoded.
    pub fn handle_cbor(&self, req: AwsRequest) -> cbor::CborResponse {
        let operation = req.operation.clone();
        let resp = match cbor::decode_request(&req.body) {
            Ok(params) => self.inner.handle(AwsRequest { params, ..req }),
            Err(e) => AwsResponse::error(
                400,
                "SerializationException",
                &format!("Unable to parse the CBOR request body: {e}"),
            ),
        };
        cbor::encode_response(&operation, resp)
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
