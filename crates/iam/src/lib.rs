//! Native IAM service for robotocore.
//!
//! Query protocol (XML) — the largest IAM operation surface.

pub mod handler;
pub mod models;
pub mod protocol;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultIamHandler {
    pub(crate) inner: handler::IamHandler,
}

impl DefaultIamHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::IamHandler::new(),
        }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultIamHandler {
    fn default() -> Self {
        Self::new()
    }
}
