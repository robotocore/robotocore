//! Native Ecs service for robotocore.
//! Protocol: json, Target: AmazonEC2ContainerServiceV20141113

pub mod handler;
pub mod models;
pub mod protocol;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultEcsHandler {
    pub(crate) inner: handler::EcsHandler,
}

impl DefaultEcsHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::EcsHandler::new(),
        }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultEcsHandler {
    fn default() -> Self {
        Self::new()
    }
}
