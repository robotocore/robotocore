//! SQS service implementation for robotocore-rust
//!
//! Provides in-memory SQS with full message lifecycle support.

pub mod error;
pub mod handler;
pub mod models;
pub mod protocol;

pub use handler::{deliver_to_queue, DefaultSqsHandler};
pub use protocol::{AwsRequest, AwsResponse, SqsHandler};

#[cfg(test)]
mod tests;
