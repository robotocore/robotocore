//! Service backends. Each module owns one AWS service and a `dispatch`.
//!
//! Only PORTED services appear here — a module replaces the moto sidecar
//! for its service the moment it publishes real behavior. Until then the
//! gateway falls back to the sidecar, so the wire contract stays intact.

use crate::{Request, Response};

pub mod inspector2;
pub mod macie2;
pub mod securityhub;

// Pending ports (stubs live in their modules but are not yet wired):
// - acm (stub)
// - ec2 (stub)
// - cloudformation (stub)

pub fn dispatch(req: &Request) -> Option<Response> {
    match req.service.as_str() {
        "inspector2" => Some(inspector2::dispatch(req)),
        "macie2" => Some(macie2::dispatch(req)),
        "securityhub" => Some(securityhub::dispatch(req)),
        _ => None,
    }
}
