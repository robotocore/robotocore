//! Service backends. Each module owns one AWS service and a `dispatch`.
//!
//! Only PORTED services appear here — a module replaces the moto sidecar
//! for its service the moment it publishes real behavior. Until then the
//! gateway falls back to the sidecar, so the wire contract stays intact.

use crate::{Request, Response};

// Pending ports (stubs live in their modules but are not yet wired):
// - acm (stub)
// - ec2 (stub)
// - cloudformation (stub)

pub mod cognito_idp;
pub mod databrew;
pub mod datasync;
pub mod identitystore;
pub mod ivs;
pub mod mediapackage;
pub mod organizations;

pub fn dispatch(req: &Request) -> Option<Response> {
    if let Some(resp) = mediapackage::dispatch(req) {
        return Some(resp);
    }
    if let Some(resp) = identitystore::dispatch(req) {
        return Some(resp);
    }
    if let Some(resp) = ivs::dispatch(req) {
        return Some(resp);
    }
    if let Some(resp) = cognito_idp::dispatch(req) {
        return Some(resp);
    }
    if let Some(resp) = databrew::dispatch(req) {
        return Some(resp);
    }
    if let Some(resp) = datasync::dispatch(req) {
        return Some(resp);
    }
    if let Some(resp) = organizations::dispatch(req) {
        return Some(resp);
    }
    // No other service fully ported yet.
    None
}
