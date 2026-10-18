//! Native Kinesis service for robotocore.
//!
//! JSON protocol (x-amz-json-1.1) with target prefix "Kinesis_20131202".

pub mod handler;
pub mod models;
pub mod protocol;

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
