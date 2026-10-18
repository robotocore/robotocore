//! Service backends. Each module owns one AWS service and a `dispatch`.
//!
//! Only PORTED services appear here — a module replaces the moto sidecar
//! for its service the moment it publishes real behavior. Until then the
//! gateway falls back to the sidecar, so the wire contract stays intact.

use crate::{Request, Response};

// Pending ports (stubs live in their modules but are not yet wired):
// - acm (stub)
// - ec2 (stub)
// - elbv2 / elb (not yet ported; fall back to the moto sidecar)

// Ported service modules (declared here; each owns one service backend).
pub mod cloudformation;

/// Ported service backends. An arm here routes that service to the native
/// rotocore implementation the moment it reaches suite parity.
pub fn dispatch(req: &Request) -> Option<Response> {
    match req.service.as_str() {
        // cloudformation: 150/150 on tests/compatibility/
        // test_cloudformation_compat.py (parity with the moto bridge).
        "cloudformation" => Some(cloudformation::dispatch(req)),
        _ => None,
    }
}
