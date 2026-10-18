//! Native CloudWatch Logs service for robotocore.
//!
//! JSON protocol (x-amz-json-1.1) with target prefix "Logs_20140328".

pub mod handler;
pub mod models;
pub mod protocol;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultLogsHandler {
    pub(crate) inner: handler::LogsHandler,
}

impl DefaultLogsHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::LogsHandler::new(),
        }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultLogsHandler {
    fn default() -> Self {
        Self::new()
    }
}
