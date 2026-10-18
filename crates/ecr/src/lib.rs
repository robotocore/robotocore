//! Native Ecr service for robotocore.
//! Protocol: json, Target: AmazonEC2ContainerRegistry_V20150921

pub mod handler;
pub mod models;
pub mod protocol;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultEcrHandler {
    pub(crate) inner: handler::EcrHandler,
}

impl DefaultEcrHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::EcrHandler::new(),
        }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultEcrHandler {
    fn default() -> Self {
        Self::new()
    }
}
