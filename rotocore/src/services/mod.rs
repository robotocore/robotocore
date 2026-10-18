//! Service backends. Each module owns one AWS service and a `dispatch`.
//!
//! Only PORTED services appear here — a module replaces the moto sidecar
//! for its service the moment it publishes real behavior. Until then the
//! gateway falls back to the sidecar, so the wire contract stays intact.

use crate::{Request, Response};

mod cloudtrail;
mod managedblockchain;
mod elasticache;
mod opensearchserverless;
mod resourcegroupstaggingapi;
mod route53resolver;

// Pending ports (stubs live in their modules but are not yet wired):
// - acm (stub)
// - ec2 (stub)
// - cloudformation (stub)

pub fn dispatch(req: &Request) -> Option<Response> {
    match req.service.as_str() {
        "cloudtrail" => Some(cloudtrail::dispatch(req)),
        "elasticache" => Some(elasticache::dispatch(req)),
        "opensearchserverless" => Some(opensearchserverless::dispatch(req)),
        "route53resolver" => Some(route53resolver::dispatch(req)),
        "managedblockchain" => Some(managedblockchain::dispatch(req)),
        "resourcegroupstaggingapi" => Some(resourcegroupstaggingapi::dispatch(req)),
        _ => None,
    }
}
