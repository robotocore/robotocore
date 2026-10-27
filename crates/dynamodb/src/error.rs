//! DynamoDB error types.

use thiserror::Error;

/// DynamoDB error codes.
#[derive(Debug, Error)]
pub enum DynamoDbError {
    #[error("ResourceNotFoundException: {0}")]
    ResourceNotFound(String),

    #[error("ItemCollectionSizeLimitExceededException: {0}")]
    ItemCollectionSizeLimitExceeded(String),

    #[error("ConditionalCheckFailedException: {0}")]
    ConditionalCheckFailed(String),

    #[error("ValidationException: {0}")]
    Validation(String),

    #[error("TableAlreadyExistsException: {0}")]
    TableAlreadyExists(String),

    #[error("LimitExceededException: {0}")]
    LimitExceeded(String),

    #[error("{0}")]
    Other(String),
}

impl DynamoDbError {
    pub fn status_code(&self) -> u16 {
        match self {
            DynamoDbError::ResourceNotFound(_) => 400,
            DynamoDbError::ConditionalCheckFailed(_) => 400,
            DynamoDbError::Validation(_) => 400,
            DynamoDbError::TableAlreadyExists(_) => 400,
            DynamoDbError::LimitExceeded(_) => 400,
            DynamoDbError::ItemCollectionSizeLimitExceeded(_) => 400,
            DynamoDbError::Other(_) => 400,
        }
    }

    pub fn code(&self) -> &str {
        match self {
            DynamoDbError::ResourceNotFound(_) => "ResourceNotFoundException",
            DynamoDbError::ConditionalCheckFailed(_) => "ConditionalCheckFailedException",
            DynamoDbError::Validation(_) => "ValidationException",
            DynamoDbError::TableAlreadyExists(_) => "TableAlreadyExistsException",
            DynamoDbError::LimitExceeded(_) => "LimitExceededException",
            DynamoDbError::ItemCollectionSizeLimitExceeded(_) => {
                "ItemCollectionSizeLimitExceededException"
            }
            DynamoDbError::Other(code) => code,
        }
    }

    pub fn message(&self) -> &str {
        match self {
            DynamoDbError::ResourceNotFound(msg) => msg,
            DynamoDbError::ConditionalCheckFailed(msg) => msg,
            DynamoDbError::Validation(msg) => msg,
            DynamoDbError::TableAlreadyExists(msg) => msg,
            DynamoDbError::LimitExceeded(msg) => msg,
            DynamoDbError::ItemCollectionSizeLimitExceeded(msg) => msg,
            DynamoDbError::Other(msg) => msg,
        }
    }
}
