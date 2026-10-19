//! Service backends. Each module owns one AWS service and a `dispatch`.
//!
//! Only PORTED services appear here — a module replaces the moto sidecar
//! for its service the moment it publishes real behavior. Until then the
//! gateway falls back to the sidecar, so the wire contract stays intact.

use crate::{Request, Response};

pub mod acm;
pub mod amp;
pub mod apigw_management;
pub mod appmesh;
pub mod appsync;
pub mod athena;
pub mod backup;
pub mod batch;
pub mod cloudformation;
pub mod cloudfront;
pub mod cloudtrail;
pub mod cognito_idp;
pub mod comprehend;
pub mod config;
pub mod databrew;
pub mod datasync;
mod ec2;
pub mod eks;
pub mod elasticache;
pub mod glue;
pub mod emr;
pub mod greengrass;
pub mod identitystore;
pub mod inspector2;
pub mod iot;
pub mod iot_data;
pub mod ivs;
pub mod kinesisanalyticsv2;
pub mod lakeformation;
pub mod macie2;
pub mod managedblockchain;
pub mod mediapackage;
pub mod opensearch;
pub mod opensearchserverless;
pub mod organizations;
pub mod pipes;
pub mod polly;
mod rds;
pub mod redshift;
pub mod redshiftdata;
pub mod resourcegroupstaggingapi;
pub mod route53resolver;
pub mod sagemaker;
pub mod securityhub;
pub mod sesv2;
pub mod support;
pub mod transcribe;

// Pending ports (fall back to the moto sidecar): elbv2/elb, glue,
// Pending ports (fall back to the moto sidecar): elbv2/elb, athena,
// emr, cloudfront, iot, connect, apigateway(v1/v2), redshift and the long
// Pending ports (fall back to the moto sidecar): elbv2/elb, athena, glue,
// cloudfront, iot, connect, apigateway(v1/v2), redshift and the long
// tail the ML-tail agent re-baselined through the sidecar.

/// Ported service backends. An arm here routes that service to the native
/// rotocore implementation the moment it reaches suite parity.
pub fn dispatch(req: &Request) -> Option<Response> {
    match req.service.as_str() {
        "acm" => Some(acm::dispatch(req)),
        "amp" => Some(amp::dispatch(req)),
        "apigatewaymanagementapi" => Some(apigw_management::dispatch(req)),
        "appmesh" => Some(appmesh::dispatch(req)),
        "appsync" => Some(appsync::dispatch(req)),
        "backup" => backup::dispatch(req),
        "athena" => Some(athena::dispatch(req)),
        "batch" => batch::dispatch(req),
        "cloudformation" => Some(self::cloudformation::dispatch(req)),
        "cloudfront" => Some(cloudfront::dispatch(req)),
        "cloudtrail" => Some(cloudtrail::dispatch(req)),
        "cognito-idp" => cognito_idp::dispatch(req),
        "comprehend" => Some(comprehend::dispatch(req)),
        "config" => Some(config::dispatch(req)),
        "databrew" => databrew::dispatch(req),
        "datasync" => datasync::dispatch(req),
        "ec2" => Some(self::ec2::dispatch(req)),
        "eks" => eks::dispatch(req),
        "elasticache" => Some(elasticache::dispatch(req)),
        "emr" => Some(emr::dispatch(req)),
        "es" => Some(opensearch::dispatch(req)),
        "greengrass" => Some(greengrass::dispatch(req)),
        "identitystore" => identitystore::dispatch(req),
        "inspector2" => Some(inspector2::dispatch(req)),
        "iot" => iot::dispatch(req),
        "iotdata" => iot_data::dispatch(req),
        "ivs" => ivs::dispatch(req),
        "kinesisanalyticsv2" => Some(kinesisanalyticsv2::dispatch(req)),
        "lakeformation" => Some(lakeformation::dispatch(req)),
        "macie2" => Some(macie2::dispatch(req)),
        "managedblockchain" => Some(managedblockchain::dispatch(req)),
        "mediapackage" => mediapackage::dispatch(req),
        "opensearch" => Some(opensearch::dispatch(req)),
        "opensearchserverless" => Some(opensearchserverless::dispatch(req)),
        "organizations" => organizations::dispatch(req),
        "pipes" => Some(pipes::dispatch(req)),
        "polly" => Some(polly::dispatch(req)),
        "redshift" => Some(redshift::dispatch(req)),
        "redshiftdata" => Some(redshiftdata::dispatch(req)),
        "resourcegroupstaggingapi" => Some(resourcegroupstaggingapi::dispatch(req)),
        "rds" => Some(self::rds::dispatch(req)),
        "route53resolver" => Some(route53resolver::dispatch(req)),
        "sagemaker" => Some(sagemaker::dispatch(req)),
        "securityhub" => Some(securityhub::dispatch(req)),
        "sesv2" => Some(sesv2::dispatch(req)),
        "support" => Some(support::dispatch(req)),
        "transcribe" => Some(transcribe::dispatch(req)),
        "glue" => glue::dispatch(req),
        _ => None,
    }
}
