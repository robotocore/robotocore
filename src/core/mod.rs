//! Robotocore Rust core implementation.
//!
//! Provides:
//! - Account and region management
//! - In-memory state storage
//! - AWS service specification loading
//! - Wire protocol parsing and serialization (query, json, ec2, rest-*)
//! - SigV4 signature validation
//! - HTTP server and service routing

pub mod account;
pub mod introspection;
pub mod protocol;
pub mod proxy;
pub mod server;
pub mod service_registry;
pub mod services;
pub mod signing;
pub mod spec;
pub mod state;

pub use account::{parse_account_from_key, AccountRegion};
pub use server::{build_router, ServiceRegistry};
pub use state::StateStore;

pub mod rest_ops;
