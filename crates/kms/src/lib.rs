//! Native KMS service for robotocore.
//!
//! JSON protocol (x-amz-json-1.1) with target prefix "TrentService".

pub mod handler;
pub mod models;
pub mod protocol;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultKmsHandler {
    pub(crate) inner: handler::KmsHandler,
}

impl DefaultKmsHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::KmsHandler::new(),
        }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultKmsHandler {
    fn default() -> Self {
        Self::new()
    }
}
