//! Service backends. Each module owns one AWS service and a `dispatch`.
//!
//! Only ported services appear here — a module takes over its service the
//! moment it publishes real behavior. Requests for services without a
//! backend answer the gateway's generic `501 NotImplemented`.

pub mod ec2;
pub mod kafka;

use crate::{Request, Response};

pub mod account;
pub mod acm;
pub mod acmpca;
pub mod amp;
pub mod apigateway;
pub mod apigatewayv2;
pub mod apigw_management;
pub mod appconfig;
pub mod appmesh;
pub mod appsync;
pub mod athena;
pub mod autoscaling;
pub mod backup;
pub mod batch;
pub mod bedrock;
pub mod bedrock_runtime;
pub mod bedrockagent;
pub mod budgets;
pub mod ce;
pub mod clouddirectory;
pub mod cloudformation;
pub mod cloudfront;
pub mod cloudhsmv2;
pub mod cloudtrail;
pub mod codebuild;
pub mod codecommit;
pub mod codedeploy;
pub mod codepipeline;
pub mod cognito_identity;
pub mod cognito_idp;
pub mod comprehend;
pub mod config;
pub mod connect;
pub mod connectcampaigns;
pub mod databrew;
pub mod datapipeline;
pub mod datasync;
pub mod dax;
pub mod directconnect;
pub mod dms;
pub mod ds;
pub mod dsql;
pub mod ebs;
pub mod ec2instanceconnect;
pub mod efs;
pub mod eks;
pub mod elasticache;
pub mod elasticbeanstalk;
pub mod elb;
pub mod elbv2;
pub mod emr;
pub mod emrcontainers;
pub mod forecast;
pub mod emrserverless;
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
pub mod kinesis_video_archived_media;
pub mod kinesisanalyticsv2;
pub mod kinesisvideo;
pub mod lakeformation;
pub mod lexv2models;
pub mod macie2;
pub mod managedblockchain;
pub mod mediaconnect;
pub mod medialive;
pub mod mediapackage;
pub mod mediastore;
pub mod memorydb;
pub mod mq;
pub mod networkfirewall;
pub mod networkmanager;
pub mod opensearch;
pub mod opensearchserverless;
pub mod organizations;
pub mod osis;
pub mod panorama;
pub mod personalize;
pub mod pinpoint;
pub mod pipes;
pub mod polly;
pub mod quicksight;
pub mod ram;
pub mod rds;
pub mod rdsdata;
pub mod redshift;
pub mod redshiftdata;
pub mod rekognition;
pub mod resiliencehub;
pub mod resource_groups;
pub mod resourcegroupstaggingapi;
pub mod route53;
pub mod route53domains;
pub mod route53resolver;
pub mod s3control;
pub mod s3tables;
pub mod s3vectors;
pub mod sagemaker;
pub mod sdb;
pub mod securityhub;
pub mod servicecatalog;
pub mod servicecatalogappregistry;
pub mod servicequotas;
pub mod ses;
pub mod sesv2;
pub mod shield;
pub mod signer;
pub mod ssoadmin;
pub mod support;
pub mod swf;
pub mod synthetics;
pub mod textract;
pub mod timestreaminfluxdb;
pub mod timestreamquery;
pub mod timestreamwrite;
pub mod transcribe;
pub mod transfer;
pub mod vpclattice;
pub mod wafv2;
pub mod workspaces;
pub mod workspacesweb;
pub mod xray;

/// Ported service backends. An arm here routes that service to the native
/// rotocore implementation the moment it reaches suite parity.
pub fn dispatch(req: &Request) -> Option<Response> {
    match req.service.as_str() {
        "acm" => Some(acm::dispatch(req)),
        "account" => account::dispatch(req),
        "acmpca" => acmpca::dispatch(req),
        "amp" => Some(amp::dispatch(req)),
        "apigateway" => Some(apigateway::dispatch(req)),
        "apigatewaymanagementapi" => Some(apigw_management::dispatch(req)),
        "apigatewayv2" => Some(apigatewayv2::dispatch(req)),
        "appconfig" => appconfig::dispatch(req),
        "appmesh" => Some(appmesh::dispatch(req)),
        "appsync" => Some(appsync::dispatch(req)),
        "athena" => Some(athena::dispatch(req)),
        "autoscaling" => autoscaling::dispatch(req),
        "backup" => backup::dispatch(req),
        "batch" => batch::dispatch(req),
        // bedrock shares one signing scope with its runtime/agent siblings;
        // route agent families, then runtime URIs, then the model-management
        // dispatch — the three URI families are disjoint.
        "bedrock" => bedrockagent::dispatch(req)
            .or_else(|| bedrock_runtime::dispatch(req))
            .or_else(|| bedrock::dispatch(req)),
        "bedrock-agent" => bedrockagent::dispatch(req),
        "bedrock-runtime" => bedrock_runtime::dispatch(req),
        "budgets" => budgets::dispatch(req),
        "ce" => ce::dispatch(req),
        "cloudformation" => Some(self::cloudformation::dispatch(req)),
        "clouddirectory" => clouddirectory::dispatch(req),
        "cloudfront" => Some(cloudfront::dispatch(req)),
        "cloudtrail" => Some(cloudtrail::dispatch(req)),
        "cloudhsmv2" => cloudhsmv2::dispatch(req),
        "codebuild" => codebuild::dispatch(req),
        "codecommit" => codecommit::dispatch(req),
        "cognito-idp" => cognito_idp::dispatch(req),
        "cognitoidentity" => cognito_identity::dispatch(req),
        "comprehend" => Some(comprehend::dispatch(req)),
        "config" => Some(config::dispatch(req)),
        "codedeploy" => codedeploy::dispatch(req),
        "connect" => Some(connect::dispatch(req)),
        "connectcampaigns" => connectcampaigns::dispatch(req),
        "databrew" => databrew::dispatch(req),
        "datapipeline" => datapipeline::dispatch(req),
        "datasync" => datasync::dispatch(req),
        "dax" => dax::dispatch(req),
        "directconnect" => directconnect::dispatch(req),
        "dms" => dms::dispatch(req),
        "ds" => ds::dispatch(req),
        "dsql" => dsql::dispatch(req),
        "ebs" => ebs::dispatch(req),
        "ec2" => Some(ec2::dispatch(req)),
        "ec2instanceconnect" => ec2instanceconnect::dispatch(req),
        "eks" => eks::dispatch(req),
        "elasticache" => Some(elasticache::dispatch(req)),
        "elasticbeanstalk" => elasticbeanstalk::dispatch(req),
        "elb" => self::elb::dispatch(req),
        "elbv2" => self::elbv2::dispatch(req),
        "emr" => Some(emr::dispatch(req)),
        "emrcontainers" => emrcontainers::dispatch(req),
        "emrserverless" => emrserverless::dispatch(req),
        "es" => Some(opensearch::dispatch(req)),
        "forecast" => forecast::dispatch(req),
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
        // kinesis-video-archived-media signs with the same `kinesisvideo`
        // credential scope as the control plane, so the gateway labels
        // those requests `kinesisvideo`. The URI families are disjoint;
        // the archived-media backend declines first and the control
        // plane keeps everything else.
        "kinesis-video-archived-media" | "kinesisvideo" => {
            kinesis_video_archived_media::dispatch(req).or_else(|| kinesisvideo::dispatch(req))
        }
        "lakeformation" => Some(lakeformation::dispatch(req)),
        "lexv2models" => lexv2models::dispatch(req),
        "macie2" => Some(macie2::dispatch(req)),
        "managedblockchain" => Some(managedblockchain::dispatch(req)),
        "mediaconnect" => mediaconnect::dispatch(req),
        "medialive" => medialive::dispatch(req),
        "mediapackage" => mediapackage::dispatch(req),
        "mediastore" => mediastore::dispatch(req),
        "memorydb" => memorydb::dispatch(req),
        "mq" => mq::dispatch(req),
        "networkfirewall" => networkfirewall::dispatch(req),
        "networkmanager" => networkmanager::dispatch(req),
        "osis" => osis::dispatch(req),
        "opensearch" => Some(opensearch::dispatch(req)),
        "opensearchserverless" => Some(opensearchserverless::dispatch(req)),
        "organizations" => organizations::dispatch(req),
        "panorama" => panorama::dispatch(req),
        "personalize" => personalize::dispatch(req),
        "pipes" => Some(pipes::dispatch(req)),
        "pinpoint" => pinpoint::dispatch(req),
        "polly" => Some(polly::dispatch(req)),
        "quicksight" => quicksight::dispatch(req),
        "ram" => ram::dispatch(req),
        "rds" => Some(rds::dispatch(req)),
        "rdsdata" => Some(rdsdata::dispatch(req)),
        "redshift" => Some(redshift::dispatch(req)),
        "redshiftdata" => Some(redshiftdata::dispatch(req)),
        "resiliencehub" => Some(resiliencehub::dispatch(req)),
        "codepipeline" => Some(codepipeline::dispatch(req)),
        "efs" => Some(efs::dispatch(req)),
        "rekognition" => Some(rekognition::dispatch(req)),
        "resource-groups" => Some(resource_groups::dispatch(req)),
        "resourcegroupstaggingapi" => Some(resourcegroupstaggingapi::dispatch(req)),
        "route53" => Some(route53::dispatch(req)),
        "route53domains" => route53domains::dispatch(req),
        "route53resolver" => Some(route53resolver::dispatch(req)),
        "s3control" => s3control::dispatch(req),
        "s3tables" => s3tables::dispatch(req),
        "s3vectors" => s3vectors::dispatch(req),
        "sagemaker" => Some(sagemaker::dispatch(req)),
        "sdb" => sdb::dispatch(req),
        "securityhub" => Some(securityhub::dispatch(req)),
        "servicecatalog" => servicecatalogappregistry::dispatch(req)
            // AppRegistry shares the Service Catalog signing name: requests
            // arrive labeled `servicecatalog` (rest-json URIs), while the
            // JSON Service Catalog API dispatches on X-Amz-Target. The two
            // URI families are disjoint, so route appregistry paths through
            // its backend and everything else to Service Catalog.
            .or_else(|| servicecatalog::dispatch(req)),
        "servicecatalogappregistry" => servicecatalogappregistry::dispatch(req),
        "servicequotas" | "service-quotas" => servicequotas::dispatch(req),
        "ses" => ses::dispatch(req),
        "sesv2" => Some(sesv2::dispatch(req)),
        "shield" => shield::dispatch(req),
        "signer" => signer::dispatch(req),
        "ssoadmin" => ssoadmin::dispatch(req),
        // Direct Connect arrives labeled `support`: the gateway router maps
        // directconnect's botocore targetPrefix `OvertureService` to
        // `support` (a python-era row predating the rotocore backend). The
        // two op surfaces are disjoint, so route directconnect operations
        // through the directconnect backend from the `support` arm.
        "support" => {
            if directconnect::handles_operation(req.operation.as_str()) {
                directconnect::dispatch(req)
            } else {
                Some(support::dispatch(req))
            }
        }
        "swf" => swf::dispatch(req),
        "synthetics" => synthetics::dispatch(req),
        "textract" => textract::dispatch(req),
        "timestreaminfluxdb" => timestreaminfluxdb::dispatch(req),
        "timestreamquery" => timestreamquery::dispatch(req),
        "timestreamwrite" => timestreamwrite::dispatch(req),
        "transcribe" => Some(transcribe::dispatch(req)),
        "transfer" => transfer::dispatch(req),
        "vpclattice" => vpclattice::dispatch(req),
        "wafv2" => wafv2::dispatch(req),
        "workspaces" => workspaces::dispatch(req),
        "workspacesweb" => workspacesweb::dispatch(req),
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
            | "account"
            | "acmpca"
            | "amp"
            | "apigateway"
            | "apigatewaymanagementapi"
            | "apigatewayv2"
            | "appconfig"
            | "appmesh"
            | "appsync"
            | "athena"
            | "autoscaling"
            | "backup"
            | "batch"
            | "bedrock"
            | "bedrock-agent"
            | "bedrock-runtime"
            | "bedrockagent"
            | "budgets"
            | "ce"
            | "cloudformation"
            | "clouddirectory"
            | "cloudfront"
            | "cloudtrail"
            | "cloudhsmv2"
            | "codebuild"
            | "codecommit"
            | "codepipeline"
            | "cognito-idp"
            | "cognitoidentity"
            | "comprehend"
            | "config"
            | "codedeploy"
            | "connect"
            | "connectcampaigns"
            | "databrew"
            | "datapipeline"
            | "datasync"
            | "dax"
            | "directconnect"
            | "dms"
            | "ds"
            | "dsql"
            | "ebs"
            | "ec2"
            | "ec2instanceconnect"
            | "efs"
            | "eks"
            | "elasticache"
            | "elasticbeanstalk"
            | "elb"
            | "elbv2"
            | "emr"
            | "emrcontainers"
            | "emrserverless"
            | "es"
            | "forecast"
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
            | "kinesis-video-archived-media"
            | "kinesisanalyticsv2"
            | "kinesisvideo"
            | "lakeformation"
            | "lexv2models"
            | "macie2"
            | "managedblockchain"
            | "mediaconnect"
            | "medialive"
            | "mediapackage"
            | "mediastore"
            | "memorydb"
            | "mq"
            | "networkfirewall"
            | "networkmanager"
            | "opensearch"
            | "opensearchserverless"
            | "organizations"
            | "osis"
            | "panorama"
            | "personalize"
            | "pipes"
            | "pinpoint"
            | "polly"
            | "quicksight"
            | "ram"
            | "rds"
            | "rdsdata"
            | "redshift"
            | "redshiftdata"
            | "rekognition"
            | "resiliencehub"
            | "resource-groups"
            | "resourcegroupstaggingapi"
            | "route53"
            | "route53domains"
            | "route53resolver"
            | "s3control"
            | "s3tables"
            | "s3vectors"
            | "sagemaker"
            | "sdb"
            | "securityhub"
            | "servicecatalog"
            | "servicecatalogappregistry"
            | "service-quotas"
            | "servicequotas"
            | "ses"
            | "sesv2"
            | "shield"
            | "signer"
            | "ssoadmin"
            | "support"
            | "swf"
            | "synthetics"
            | "textract"
            | "timestreaminfluxdb"
            | "timestreamquery"
            | "timestreamwrite"
            | "transcribe"
            | "transfer"
            | "vpclattice"
            | "wafv2"
            | "workspaces"
            | "workspacesweb"
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
            "connectcampaigns",
            "forecast",
            "emrcontainers",
            "glue",
            "glacier",
            "elasticache",
            "emrserverless",
            "kinesisvideo",
            "kinesis-video-archived-media",
            "codepipeline",
            "rdsdata",
            "lexv2models",
            "medialive",
            "xray",
            "servicecatalogappregistry",
        ] {
            assert!(handles_service(service), "{service} should be armed");
        }
        for service in ["cloudwatch", "athena-x-nothing", ""] {
            assert!(!handles_service(service), "{service} should not be armed");
        }
    }
}
