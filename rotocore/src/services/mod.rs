//! Service backends. Each module owns one AWS service and a `dispatch`.
//!
//! Only PORTED services appear here — a module replaces the moto sidecar
//! for its service the moment it publishes real behavior. Until then the
//! gateway falls back to the sidecar, so the wire contract stays intact.

use crate::{Request, Response};

// Pending ports (stubs live in their modules but are not yet wired):
// - ec2 (stub)
// - cloudformation (stub)

mod acm;
mod amp;
mod appmesh;
mod greengrass;
mod kinesisanalyticsv2;

pub fn dispatch(req: &Request) -> Option<Response> {
    match req.service.as_str() {
        "acm" => Some(acm::dispatch(req)),
        "kinesisanalyticsv2" => Some(kinesisanalyticsv2::dispatch(req)),
        "amp" => Some(amp::dispatch(req)),
        "appmesh" => Some(appmesh::dispatch(req)),
        "greengrass" => Some(greengrass::dispatch(req)),
        _ => None,
    }
}
