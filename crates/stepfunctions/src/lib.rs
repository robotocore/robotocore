//! Native Stepfunctions service for robotocore.
//! Protocol: json, Target: AWSStepFunctions

pub mod asl;
pub mod handler;
pub mod models;
pub mod protocol;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultStepfunctionsHandler {
    pub(crate) inner: handler::StepfunctionsHandler,
}

impl DefaultStepfunctionsHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::StepfunctionsHandler::new(),
        }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultStepfunctionsHandler {
    fn default() -> Self {
        Self::new()
    }
}
