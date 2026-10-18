//! Native Cloudwatch service for robotocore.
//! Protocol: smithy-rpc-v2-cbor, Target: GraniteServiceVersion20100801

pub mod handler;
pub mod models;
pub mod protocol;

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
