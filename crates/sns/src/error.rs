//! SNS error types.

use thiserror::Error;

/// SNS error codes.
#[derive(Debug, Error)]
pub enum SnsError {
    #[error("{0}")]
    Other(String),
}

impl SnsError {
    pub fn not_found(_resource: &str) -> Self {
        SnsError::Other("InvalidParameterValue: The specified topic does not exist".to_string())
    }
}
