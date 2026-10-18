//! rotocore — the moto-equivalent AWS service engine in pure Rust.
//!
//! Implementations of the services robotocore's Python bridge used to
//! serve through moto. Every service module exposes a `Backend` holding
//! per-(account, region) state and a `dispatch` that turns parsed requests
//! into raw wire responses.
//!
//! Layout:
//! - [`proto`]: shared wire helpers (query-XML serializer, JSON, errors)
//! - [`store`]: process-global registry keyed (account, region)
//! - [`services`]: one module per AWS service
//!
//! Contract: same request/response shapes the moto-backed bridge produced
//! (the pytest compat suites are the spec).

pub mod proto;
pub mod services;
pub mod store;

use bytes::Bytes;

/// A request routed into rotocore (mirrors the moto sidecar's inputs).
#[derive(Debug, Clone)]
pub struct Request {
    pub service: String,
    pub operation: String,
    pub method: String,
    pub path: String,
    pub query_string: String,
    /// Lowercased header names.
    pub headers: std::collections::HashMap<String, String>,
    pub body: Bytes,
    pub account: u64,
    pub region: String,
}

/// A wire-ready response.
#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn new(status: u16, content_type: &str, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            headers: vec![("Content-Type".to_string(), content_type.to_string())],
            body: body.into(),
        }
    }

    pub fn xml(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self::new(status, "text/xml", body)
    }

    pub fn json(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self::new(status, "application/x-amz-json-1.1", body)
    }

    pub fn json_status(status: u16, value: &serde_json::Value) -> Self {
        Self::json(status, serde_json::to_vec(value).unwrap_or_default())
    }
}

/// Dispatch a routed request to its service backend.
///
/// Returns `None` when no ported backend exists for `service` (bin routes
/// the request to its own crate table).
pub fn dispatch(req: &Request) -> Option<Response> {
    services::dispatch(req)
}
