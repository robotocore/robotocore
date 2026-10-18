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
// - elbv2 / elb (not yet ported; fall back to the moto sidecar)

pub mod cloudformation;
pub mod inspector2;
pub mod lakeformation;
pub mod macie2;
mod rds;
pub mod securityhub;

/// Ported service backends. An arm here routes that service to the native
/// rotocore implementation the moment it reaches suite parity.
pub fn dispatch(req: &Request) -> Option<Response> {
    match req.service.as_str() {
        // cloudformation: 150/150 on tests/compatibility/test_cloudformation_compat.py
        "cloudformation" => Some(cloudformation::dispatch(req)),
        "inspector2" => Some(inspector2::dispatch(req)),
        "lakeformation" => Some(lakeformation::dispatch(req)),
        "macie2" => Some(macie2::dispatch(req)),
        // rds: 326/326 on tests/compatibility/test_rds_compat.py
        "rds" => Some(self::rds::dispatch(req)),
        "securityhub" => Some(securityhub::dispatch(req)),
        _ => None,
    }
/// Dispatch a routed request to its service backend.
///
/// Returns `None` when no ported backend exists for `service` (bin routes
/// the request to its own crate table).
pub fn dispatch(req: &Request) -> Option<Response> {
    batch::dispatch(req)
        .or_else(|| backup::dispatch(req))
        .or_else(|| eks::dispatch(req))
}
