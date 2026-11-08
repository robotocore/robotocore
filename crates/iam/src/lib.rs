//! Native IAM service for robotocore.
//!
//! Query protocol (XML) — the largest IAM operation surface.

pub mod handler;
pub mod models;
pub mod policy;
pub mod policy_validation;
pub mod protocol;
pub mod snapshot;

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

impl DefaultIamHandler {
    /// Gateway `ENFORCE_IAM` hook: every identity-policy document that
    /// applies to the principal holding `access_key` (see
    /// `handler::IamHandler::policy_docs_for_access_key`).
    pub fn policy_docs_for_access_key(
        &self,
        account: u64,
        access_key: &str,
    ) -> Option<Vec<(String, String)>> {
        self.inner.policy_docs_for_access_key(account, access_key)
    }

    /// Serialize the full service state to JSON (state snapshot channel).
    pub fn snapshot_json(&self) -> serde_json::Value {
        self.inner.snapshot_json()
    }

    /// Replace the service state from JSON (state snapshot channel).
    pub fn restore_json(&self, v: &serde_json::Value) {
        self.inner.restore_json(v)
    }

    /// Clear all service state (state snapshot channel).
    pub fn reset(&self) {
        self.inner.reset()
    }
}
