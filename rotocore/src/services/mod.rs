//! Service backends. Each module owns one AWS service and a `dispatch`.
//!
//! Only PORTED services appear here — a module replaces the moto sidecar
//! for its service the moment it publishes real behavior. Until then the
//! gateway falls back to the sidecar, so the wire contract stays intact.
//!
//! Ported:
//! - batch (moto-equivalent, docker-free)
//! - backup (moto-equivalent rest-json)
//! - eks (moto-equivalent rest-json)
//!
//! Still moto-backed via the sidecar (no module): athena, glue, emr.

mod backup;
mod batch;
mod eks;

use crate::{Request, Response};

// Pending ports (stubs live in their modules but are not yet wired):
// - acm (stub)
// - ec2 (stub)
// - cloudformation (stub)

/// Dispatch a routed request to its service backend.
///
/// Returns `None` when no ported backend exists for `service` (bin routes
/// the request to its own crate table).
pub fn dispatch(req: &Request) -> Option<Response> {
    batch::dispatch(req)
        .or_else(|| backup::dispatch(req))
        .or_else(|| eks::dispatch(req))
}
