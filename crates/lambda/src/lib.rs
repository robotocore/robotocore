//! Native Lambda service for robotocore.
//!
//! rest-json protocol — functions, aliases, event source mappings, layers.

pub mod handler;
pub mod models;
pub mod protocol;
pub mod runtime;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultLambdaHandler {
    pub(crate) inner: handler::LambdaHandler,
}

impl DefaultLambdaHandler {
    pub fn new() -> Self {
        Self { inner: handler::LambdaHandler::new() }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultLambdaHandler {
    fn default() -> Self { Self::new() }
}
