//! Service backends. Each module owns one AWS service and a `dispatch`.
//!
//! Only PORTED services appear here — a module replaces the moto sidecar
//! for its service the moment it publishes real behavior. Until then the
//! gateway falls back to the sidecar, so the wire contract stays intact.

use crate::{Request, Response};

mod cloudtrail;
mod config;
mod elasticache;
mod managedblockchain;
mod opensearch;
mod opensearchserverless;
mod resourcegroupstaggingapi;
mod route53resolver;
mod sesv2;

// Pending ports (stubs live in their modules but are not yet wired):
// - ec2 (stub)
// - elbv2 / elb (not yet ported; fall back to the moto sidecar)

pub mod acm;
pub mod amp;
pub mod appmesh;
pub mod appsync;
pub mod backup;
pub mod batch;
pub mod cognito_idp;
pub mod databrew;
pub mod datasync;
pub mod eks;
pub mod greengrass;
pub mod identitystore;
pub mod inspector2;
pub mod ivs;
pub mod kinesisanalyticsv2;
pub mod lakeformation;
pub mod macie2;
pub mod mediapackage;
pub mod organizations;
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
        "cognito-idp" => cognito_idp::dispatch(req),
        "databrew" => databrew::dispatch(req),
        "datasync" => datasync::dispatch(req),
        "eks" => eks::dispatch(req),
        "greengrass" => Some(greengrass::dispatch(req)),
        "identitystore" => identitystore::dispatch(req),
        "inspector2" => Some(inspector2::dispatch(req)),
        "ivs" => ivs::dispatch(req),
        "kinesisanalyticsv2" => Some(kinesisanalyticsv2::dispatch(req)),
        "lakeformation" => Some(lakeformation::dispatch(req)),
        "macie2" => Some(macie2::dispatch(req)),
        "mediapackage" => mediapackage::dispatch(req),
        "organizations" => organizations::dispatch(req),
        "rds" => Some(self::rds::dispatch(req)),
        "securityhub" => Some(securityhub::dispatch(req)),
        "cloudtrail" => Some(cloudtrail::dispatch(req)),
        "config" => Some(config::dispatch(req)),
        "opensearch" => Some(opensearch::dispatch(req)),
        "es" => Some(opensearch::dispatch(req)),
        "elasticache" => Some(elasticache::dispatch(req)),
        "opensearchserverless" => Some(opensearchserverless::dispatch(req)),
        "route53resolver" => Some(route53resolver::dispatch(req)),
        "managedblockchain" => Some(managedblockchain::dispatch(req)),
        "resourcegroupstaggingapi" => Some(resourcegroupstaggingapi::dispatch(req)),
        "sesv2" => Some(sesv2::dispatch(req)),
        _ => None,
    }
}
