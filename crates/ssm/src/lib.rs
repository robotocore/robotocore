//! Native SSM service for robotocore.
//!
//! JSON protocol (x-amz-json-1.0) with target prefix "AmazonSSM".

pub mod handler;
pub mod models;
pub mod protocol;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultSsmHandler {
    pub(crate) inner: handler::SsmHandler,
}

impl DefaultSsmHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::SsmHandler::new(),
        }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultSsmHandler {
    fn default() -> Self {
        Self::new()
    }
}
