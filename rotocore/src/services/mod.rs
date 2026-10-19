//! Service backends. Each module owns one AWS service and a `dispatch`.
//!
//! Only ported services appear here — a module takes over its service the
//! moment it publishes real behavior. Requests for services without a
//! backend answer the gateway's generic `501 NotImplemented`.

pub mod ec2;
pub mod kafka;

use crate::{Request, Response};

pub mod acm;
pub mod acmpca;
pub mod amp;
pub mod apigateway;
pub mod apigatewayv2;
pub mod apigw_management;
pub mod appmesh;
pub mod appsync;
pub mod athena;
pub mod backup;
pub mod batch;
pub mod bedrock;
pub mod budgets;
pub mod ce;
pub mod cloudformation;
pub mod cloudfront;
pub mod cloudtrail;
pub mod codepipeline;
pub mod cognito_identity;
pub mod cognito_idp;
pub mod comprehend;
pub mod config;
pub mod connect;
pub mod databrew;
pub mod datapipeline;
pub mod datasync;
pub mod dsql;
pub mod ebs;
pub mod ec2instanceconnect;
pub mod efs;
pub mod eks;
pub mod elasticache;
pub mod elb;
pub mod elbv2;
pub mod emr;
pub mod fsx;
pub mod glacier;
pub mod glue;
pub mod greengrass;
pub mod guardduty;
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
pub mod mediastore;
pub mod mq;
pub mod networkfirewall;
pub mod opensearch;
pub mod opensearchserverless;
pub mod organizations;
pub mod osis;
pub mod personalize;
pub mod pipes;
pub mod polly;
pub mod ram;
pub mod rds;
pub mod redshift;
pub mod redshiftdata;
pub mod rekognition;
pub mod resiliencehub;
pub mod resource_groups;
pub mod resourcegroupstaggingapi;
pub mod route53resolver;
pub mod s3control;
pub mod sagemaker;
pub mod securityhub;
pub mod servicecatalog;
pub mod sesv2;
pub mod shield;
pub mod signer;
pub mod ssoadmin;
pub mod support;
pub mod textract;
pub mod timestreaminfluxdb;
pub mod timestreamquery;
pub mod timestreamwrite;
pub mod transcribe;
pub mod wafv2;
pub mod workspaces;
pub mod xray;

/// Ported service backends. An arm here routes that service to the native
/// rotocore implementation the moment it reaches suite parity.
pub fn dispatch(req: &Request) -> Option<Response> {
    match req.service.as_str() {
        "acm" => Some(acm::dispatch(req)),
        "acmpca" => acmpca::dispatch(req),
        "amp" => Some(amp::dispatch(req)),
        "apigateway" => Some(apigateway::dispatch(req)),
        "apigatewaymanagementapi" => Some(apigw_management::dispatch(req)),
        "apigatewayv2" => Some(apigatewayv2::dispatch(req)),
        "appmesh" => Some(appmesh::dispatch(req)),
        "appsync" => Some(appsync::dispatch(req)),
        "athena" => Some(athena::dispatch(req)),
        "backup" => backup::dispatch(req),
        "batch" => batch::dispatch(req),
        "bedrock" => bedrock::dispatch(req),
        "budgets" => budgets::dispatch(req),
        "ce" => ce::dispatch(req),
        "cloudformation" => Some(self::cloudformation::dispatch(req)),
        "cloudfront" => Some(cloudfront::dispatch(req)),
        "cloudtrail" => Some(cloudtrail::dispatch(req)),
        "cognito-idp" => cognito_idp::dispatch(req),
        "cognitoidentity" => cognito_identity::dispatch(req),
        "comprehend" => Some(comprehend::dispatch(req)),
        "config" => Some(config::dispatch(req)),
        "connect" => Some(connect::dispatch(req)),
        "databrew" => databrew::dispatch(req),
        "datapipeline" => datapipeline::dispatch(req),
        "datasync" => datasync::dispatch(req),
        "dsql" => dsql::dispatch(req),
        "ebs" => ebs::dispatch(req),
        "ec2" => Some(ec2::dispatch(req)),
        "ec2instanceconnect" => ec2instanceconnect::dispatch(req),
        "eks" => eks::dispatch(req),
        "elasticache" => Some(elasticache::dispatch(req)),
        "elb" => self::elb::dispatch(req),
        "elbv2" => self::elbv2::dispatch(req),
        "emr" => Some(emr::dispatch(req)),
        "es" => Some(opensearch::dispatch(req)),
        "fsx" => fsx::dispatch(req),
        "glacier" => Some(glacier::dispatch(req)),
        "glue" => glue::dispatch(req),
        "greengrass" => Some(greengrass::dispatch(req)),
        "guardduty" => Some(guardduty::dispatch(req)),
        "identitystore" => identitystore::dispatch(req),
        "inspector2" => Some(inspector2::dispatch(req)),
        "iot" => iot::dispatch(req),
        "iotdata" => iot_data::dispatch(req),
        "ivs" => ivs::dispatch(req),
        "kafka" => Some(kafka::dispatch(req)),

        "kinesisanalyticsv2" => Some(kinesisanalyticsv2::dispatch(req)),
        "lakeformation" => Some(lakeformation::dispatch(req)),
        "macie2" => Some(macie2::dispatch(req)),
        "managedblockchain" => Some(managedblockchain::dispatch(req)),
        "mediapackage" => mediapackage::dispatch(req),
        "mediastore" => mediastore::dispatch(req),
        "mq" => mq::dispatch(req),
        "networkfirewall" => networkfirewall::dispatch(req),
        "osis" => osis::dispatch(req),
        "opensearch" => Some(opensearch::dispatch(req)),
        "opensearchserverless" => Some(opensearchserverless::dispatch(req)),
        "organizations" => organizations::dispatch(req),
        "personalize" => personalize::dispatch(req),
        "pipes" => Some(pipes::dispatch(req)),
        "polly" => Some(polly::dispatch(req)),
        "ram" => ram::dispatch(req),
        "rds" => Some(rds::dispatch(req)),
        "redshift" => Some(redshift::dispatch(req)),
        "redshiftdata" => Some(redshiftdata::dispatch(req)),
        "resiliencehub" => Some(resiliencehub::dispatch(req)),
        "codepipeline" => Some(codepipeline::dispatch(req)),
        "efs" => Some(efs::dispatch(req)),
        "rekognition" => Some(rekognition::dispatch(req)),
        "resource-groups" => Some(resource_groups::dispatch(req)),
        "resourcegroupstaggingapi" => Some(resourcegroupstaggingapi::dispatch(req)),
        "route53resolver" => Some(route53resolver::dispatch(req)),
        "s3control" => s3control::dispatch(req),
        "sagemaker" => Some(sagemaker::dispatch(req)),
        "securityhub" => Some(securityhub::dispatch(req)),
        "servicecatalog" => servicecatalog::dispatch(req),
        "sesv2" => Some(sesv2::dispatch(req)),
        "shield" => shield::dispatch(req),
        "signer" => signer::dispatch(req),
        "ssoadmin" => ssoadmin::dispatch(req),
        "support" => Some(support::dispatch(req)),
        "textract" => textract::dispatch(req),
        "timestreaminfluxdb" => timestreaminfluxdb::dispatch(req),
        "timestreamquery" => timestreamquery::dispatch(req),
        "timestreamwrite" => timestreamwrite::dispatch(req),
        "transcribe" => Some(transcribe::dispatch(req)),
        "wafv2" => wafv2::dispatch(req),
        "workspaces" => workspaces::dispatch(req),
        "xray" => Some(xray::dispatch(req)),

        _ => None,
    }
}

/// True when a native rotocore backend is armed for `service`.
///
/// Mirrors the arms of `dispatch` — keep the two lists in sync. The
/// gateway exposes this so introspection endpoints can report weights
/// truthfully instead of carrying stale sidecar-era labels.
pub fn handles_service(service: &str) -> bool {
    matches!(
        service,
        "acm"
            | "acmpca"
            | "amp"
            | "apigateway"
            | "apigatewaymanagementapi"
            | "apigatewayv2"
            | "appmesh"
            | "appsync"
            | "athena"
            | "backup"
            | "batch"
            | "bedrock"
            | "budgets"
            | "ce"
            | "cloudformation"
            | "cloudfront"
            | "cloudtrail"
            | "codepipeline"
            | "cognito-idp"
            | "cognitoidentity"
            | "comprehend"
            | "config"
            | "connect"
            | "databrew"
            | "datapipeline"
            | "datasync"
            | "dsql"
            | "ebs"
            | "ec2"
            | "ec2instanceconnect"
            | "efs"
            | "eks"
            | "elasticache"
            | "elb"
            | "elbv2"
            | "emr"
            | "es"
            | "fsx"
            | "glacier"
            | "glue"
            | "greengrass"
            | "guardduty"
            | "identitystore"
            | "inspector2"
            | "iot"
            | "iotdata"
            | "ivs"
            | "kafka"
            | "kinesisanalyticsv2"
            | "lakeformation"
            | "macie2"
            | "managedblockchain"
            | "mediapackage"
            | "mediastore"
            | "mq"
            | "networkfirewall"
            | "opensearch"
            | "opensearchserverless"
            | "organizations"
            | "osis"
            | "personalize"
            | "pipes"
            | "polly"
            | "ram"
            | "rds"
            | "redshift"
            | "redshiftdata"
            | "rekognition"
            | "resiliencehub"
            | "resource-groups"
            | "resourcegroupstaggingapi"
            | "route53resolver"
            | "s3control"
            | "sagemaker"
            | "securityhub"
            | "servicecatalog"
            | "sesv2"
            | "shield"
            | "signer"
            | "ssoadmin"
            | "support"
            | "textract"
            | "timestreaminfluxdb"
            | "timestreamquery"
            | "timestreamwrite"
            | "transcribe"
            | "wafv2"
            | "workspaces"
            | "xray"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_service_matches_dispatch_arms() {
        // Spot-check both directions: armed services report true, services
        // with no backend report false.
        for service in [
            "acm",
            "connect",
            "glue",
            "glacier",
            "elasticache",
            "codepipeline",
            "xray",
        ] {
            assert!(handles_service(service), "{service} should be armed");
        }
        for service in ["account", "cloudwatch", "athena-x-nothing", ""] {
            assert!(!handles_service(service), "{service} should not be armed");
        }
    }
}
