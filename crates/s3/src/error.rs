//! S3 error types.

use thiserror::Error;

/// S3 error codes and messages.
#[derive(Debug, Error)]
pub enum S3Error {
    #[error("NoSuchBucket: The specified bucket does not exist")]
    NoSuchBucket,

    #[error("BucketAlreadyExists: A bucket with the same name already exists")]
    BucketAlreadyExists,

    #[error("BucketAlreadyOwnedByYou: A bucket with the same name already exists, owned by you")]
    BucketAlreadyOwnedByYou,

    #[error("NoSuchKey: The specified key does not exist")]
    NoSuchKey,

    #[error("NoSuchUpload: The specified multipart upload does not exist")]
    NoSuchUpload,

    #[error("NoSuchBucketPolicy: The bucket policy does not exist")]
    NoSuchBucketPolicy,

    #[error("NoSuchCORSConfiguration: The CORS configuration does not exist")]
    NoSuchCORSConfiguration,

    #[error("InvalidBucketName: The specified bucket is not valid")]
    InvalidBucketName,

    #[error("InvalidPart: One or more of the specified parts could not be found")]
    InvalidPart,

    #[error("EntityTooSmall: Part is too small")]
    EntityTooSmall,

    #[error("AccessDenied: Access Denied")]
    AccessDenied,

    #[error("MethodNotAllowed: The specified method is not allowed against this resource")]
    MethodNotAllowed,

    #[error("{0}")]
    Other(String),
}

impl S3Error {
    /// Return the HTTP status code for this error.
    pub fn status_code(&self) -> u16 {
        match self {
            S3Error::NoSuchBucket
            | S3Error::NoSuchKey
            | S3Error::NoSuchUpload
            | S3Error::NoSuchBucketPolicy
            | S3Error::NoSuchCORSConfiguration => 404,
            S3Error::BucketAlreadyExists | S3Error::BucketAlreadyOwnedByYou => 409,
            S3Error::InvalidBucketName
            | S3Error::InvalidPart
            | S3Error::EntityTooSmall => 400,
            S3Error::AccessDenied => 403,
            S3Error::MethodNotAllowed => 405,
            S3Error::Other(_) => 400,
        }
    }

    /// Return the error code string.
    pub fn code(&self) -> &str {
        match self {
            S3Error::NoSuchBucket => "NoSuchBucket",
            S3Error::BucketAlreadyExists => "BucketAlreadyExists",
            S3Error::BucketAlreadyOwnedByYou => "BucketAlreadyOwnedByYou",
            S3Error::NoSuchKey => "NoSuchKey",
            S3Error::NoSuchUpload => "NoSuchUpload",
            S3Error::NoSuchBucketPolicy => "NoSuchBucketPolicy",
            S3Error::NoSuchCORSConfiguration => "NoSuchCORSConfiguration",
            S3Error::InvalidBucketName => "InvalidBucketName",
            S3Error::InvalidPart => "InvalidPart",
            S3Error::EntityTooSmall => "EntityTooSmall",
            S3Error::AccessDenied => "AccessDenied",
            S3Error::MethodNotAllowed => "MethodNotAllowed",
            S3Error::Other(code) => code,
        }
    }

    /// Return the error message.
    pub fn message(&self) -> &str {
        match self {
            S3Error::NoSuchBucket => "The specified bucket does not exist",
            S3Error::BucketAlreadyExists => "A bucket with the same name already exists",
            S3Error::BucketAlreadyOwnedByYou => {
                "A bucket with the same name already exists, owned by you"
            }
            S3Error::NoSuchKey => "The specified key does not exist",
            S3Error::NoSuchUpload => "The specified multipart upload does not exist",
            S3Error::NoSuchBucketPolicy => "The bucket policy does not exist",
            S3Error::NoSuchCORSConfiguration => "The CORS configuration does not exist",
            S3Error::InvalidBucketName => "The specified bucket is not valid",
            S3Error::InvalidPart => "One or more of the specified parts could not be found",
            S3Error::EntityTooSmall => "Part is too small",
            S3Error::AccessDenied => "Access Denied",
            S3Error::MethodNotAllowed => {
                "The specified method is not allowed against this resource"
            }
            S3Error::Other(msg) => msg,
        }
    }
}
