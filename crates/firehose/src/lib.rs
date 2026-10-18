//! Native Firehose service for robotocore.
//! Protocol: json, Target: Firehose_20150804

pub mod handler;
pub mod models;
pub mod protocol;

pub use protocol::{AwsRequest, AwsResponse};

pub struct DefaultFirehoseHandler {
    pub(crate) inner: handler::FirehoseHandler,
}

impl DefaultFirehoseHandler {
    pub fn new() -> Self {
        Self {
            inner: handler::FirehoseHandler::new(),
        }
    }
    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        self.inner.handle(req)
    }
}

impl Default for DefaultFirehoseHandler {
    fn default() -> Self {
        Self::new()
    }
}
