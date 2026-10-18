//! SQS service implementation for robotocore-rust
//!
//! Provides in-memory SQS with full message lifecycle support.

pub mod bridge;
pub mod error;
pub mod handler;
pub mod models;
pub mod protocol;

pub use handler::DefaultSqsHandler;
pub use protocol::{AwsRequest, AwsResponse, SqsHandler};

#[cfg(test)]
mod tests;
