//! Service backends. Each module owns one AWS service and a `dispatch`.
//!
//! Only PORTED services appear here — a module replaces the moto sidecar
//! for its service the moment it publishes real behavior. Until then the
//! gateway falls back to the sidecar, so the wire contract stays intact.

use crate::{Request, Response};

// Pending ports (stubs live in their modules but are not yet wired):
// - acm (stub)
// - ec2 (stub)

// Modules under active port; dispatch arms land in a final commit when the
// compatibility suites pass (mod.rs arms are centrally owned).
pub mod cloudformation;

pub fn dispatch(req: &Request) -> Option<Response> {
    match req.service.as_str() {
        // TEMPORARY local-test wiring; the committed mod.rs stays arm-less
        // until the final commit (arms are centrally owned).
        "cloudformation" => Some(cloudformation::dispatch(req)),
        _ => None,
    }
}
