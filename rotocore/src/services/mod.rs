//! Service backends. Each module owns one AWS service and a `dispatch`.
//!
//! Only PORTED services appear here — a module replaces the moto sidecar
//! for its service the moment it publishes real behavior. Until then the
//! gateway falls back to the sidecar, so the wire contract stays intact.

use crate::{Request, Response};

// Pending ports (stubs live in their modules but are not yet wired):
// - ec2 (stub)
// - elbv2 / elb (not yet ported; fall back to the moto sidecar)

pub mod acm;
pub mod amp;
pub mod appmesh;
pub mod appsync;
pub mod backup;
pub mod batch;
pub mod eks;
pub mod greengrass;
pub mod inspector2;
pub mod kinesisanalyticsv2;
pub mod lakeformation;
pub mod macie2;
pub mod rds;
pub mod securityhub;

/// Ported service backends. An arm here routes that service to the native
/// rotocore implementation the moment it reaches suite parity.
pub fn dispatch(req: &Request) -> Option<Response> {
    match req.service.as_str() {
        "acm" => Some(acm::dispatch(req)),
        "amp" => Some(amp::dispatch(req)),
        "appmesh" => Some(appmesh::dispatch(req)),
        "appsync" => Some(appsync::dispatch(req)),
        "backup" => backup::dispatch(req),
        "batch" => batch::dispatch(req),
        "eks" => eks::dispatch(req),
        "greengrass" => Some(greengrass::dispatch(req)),        "inspector2" => Some(inspector2::dispatch(req)),        "kinesisanalyticsv2" => Some(kinesisanalyticsv2::dispatch(req)),
        "lakeformation" => Some(lakeformation::dispatch(req)),
        "macie2" => Some(macie2::dispatch(req)),        "rds" => Some(self::rds::dispatch(req)),
        "securityhub" => Some(securityhub::dispatch(req)),
        _ => None,
    }
}
