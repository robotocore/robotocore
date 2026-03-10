"""Service registry tracking which AWS services are enabled and their status."""

from dataclasses import dataclass
from enum import Enum


class ServiceStatus(Enum):
    MOTO_BACKED = "moto_backed"
    NATIVE = "native"
    EXTERNAL = "external"
    PLANNED = "planned"


@dataclass
class ServiceInfo:
    name: str
    status: ServiceStatus
    protocol: str
    description: str = ""


# All services with their implementation status
SERVICE_REGISTRY: dict[str, ServiceInfo] = {
    # Phase 1 - Core (Moto-backed)
    "s3": ServiceInfo("s3", ServiceStatus.NATIVE, "rest-xml", "S3 with event notifications"),
    "sqs": ServiceInfo("sqs", ServiceStatus.NATIVE, "json", "SQS with full message lifecycle"),
    "sns": ServiceInfo("sns", ServiceStatus.NATIVE, "query", "SNS with cross-service delivery"),
    "dynamodb": ServiceInfo(
        "dynamodb", ServiceStatus.NATIVE, "json", "DynamoDB with stream mutation hooks"
    ),
    "dynamodbstreams": ServiceInfo(
        "dynamodbstreams",
        ServiceStatus.NATIVE,
        "json",
        "DynamoDB Streams with native stream reading",
    ),
    "iam": ServiceInfo("iam", ServiceStatus.NATIVE, "query", "Identity and Access Management"),
    "sts": ServiceInfo("sts", ServiceStatus.NATIVE, "query", "Security Token Service"),
    "cloudformation": ServiceInfo(
        "cloudformation", ServiceStatus.NATIVE, "query", "CloudFormation engine"
    ),
    "cloudwatch": ServiceInfo(
        "cloudwatch", ServiceStatus.NATIVE, "query", "CloudWatch Metrics with alarm evaluation"
    ),
    "logs": ServiceInfo("logs", ServiceStatus.NATIVE, "json", "CloudWatch Logs"),
    "kms": ServiceInfo("kms", ServiceStatus.MOTO_BACKED, "json", "Key Management Service"),
    "lambda": ServiceInfo(
        "lambda", ServiceStatus.NATIVE, "rest-json", "Lambda with in-process Python execution"
    ),
    "events": ServiceInfo(
        "events", ServiceStatus.NATIVE, "json", "EventBridge with cross-service target invocation"
    ),
    "kinesis": ServiceInfo(
        "kinesis", ServiceStatus.NATIVE, "json", "Kinesis Streams with shard-based storage"
    ),
    "firehose": ServiceInfo("firehose", ServiceStatus.NATIVE, "json", "Firehose with S3 delivery"),
    "stepfunctions": ServiceInfo(
        "stepfunctions", ServiceStatus.NATIVE, "json", "Step Functions with ASL execution"
    ),
    # Phase 2 - Integration (Moto-backed)
    "apigateway": ServiceInfo("apigateway", ServiceStatus.NATIVE, "rest-json", "API Gateway"),
    "apigatewayv2": ServiceInfo(
        "apigatewayv2",
        ServiceStatus.NATIVE,
        "rest-json",
        "API Gateway V2 (HTTP APIs + WebSocket)",
    ),
    "secretsmanager": ServiceInfo(
        "secretsmanager", ServiceStatus.NATIVE, "json", "Secrets Manager"
    ),
    "ssm": ServiceInfo("ssm", ServiceStatus.NATIVE, "json", "Systems Manager"),
    "scheduler": ServiceInfo(
        "scheduler", ServiceStatus.NATIVE, "rest-json", "EventBridge Scheduler with schedule CRUD"
    ),
    "s3control": ServiceInfo("s3control", ServiceStatus.MOTO_BACKED, "rest-xml", "S3 Control"),
    # Phase 3 - Remaining (Moto-backed)
    "acm": ServiceInfo("acm", ServiceStatus.NATIVE, "json", "Certificate Manager"),
    "config": ServiceInfo("config", ServiceStatus.NATIVE, "json", "Config"),
    "ec2": ServiceInfo("ec2", ServiceStatus.NATIVE, "ec2", "Elastic Compute Cloud"),
    "redshift": ServiceInfo("redshift", ServiceStatus.MOTO_BACKED, "query", "Redshift"),
    "resource-groups": ServiceInfo(
        "resource-groups", ServiceStatus.NATIVE, "rest-json", "Resource Groups"
    ),
    "resourcegroupstaggingapi": ServiceInfo(
        "resourcegroupstaggingapi", ServiceStatus.NATIVE, "json", "Resource Groups Tagging API"
    ),
    "route53": ServiceInfo("route53", ServiceStatus.NATIVE, "rest-xml", "Route 53"),
    "route53resolver": ServiceInfo(
        "route53resolver", ServiceStatus.MOTO_BACKED, "json", "Route 53 Resolver"
    ),
    "ses": ServiceInfo("ses", ServiceStatus.NATIVE, "query", "Simple Email Service"),
    "sesv2": ServiceInfo("sesv2", ServiceStatus.NATIVE, "rest-json", "Simple Email Service v2"),
    "support": ServiceInfo("support", ServiceStatus.NATIVE, "json", "Support"),
    "swf": ServiceInfo("swf", ServiceStatus.MOTO_BACKED, "json", "Simple Workflow"),
    "transcribe": ServiceInfo("transcribe", ServiceStatus.MOTO_BACKED, "json", "Transcribe"),
    "es": ServiceInfo("es", ServiceStatus.NATIVE, "rest-json", "Elasticsearch Service"),
    "opensearch": ServiceInfo(
        "opensearch", ServiceStatus.NATIVE, "rest-json", "OpenSearch Service"
    ),
    "cognito-idp": ServiceInfo(
        "cognito-idp", ServiceStatus.NATIVE, "json", "Cognito Identity Provider"
    ),
    "appsync": ServiceInfo("appsync", ServiceStatus.NATIVE, "rest-json", "AppSync GraphQL"),
    "ecs": ServiceInfo("ecs", ServiceStatus.NATIVE, "json", "Elastic Container Service"),
    "batch": ServiceInfo("batch", ServiceStatus.NATIVE, "rest-json", "AWS Batch"),
    "ecr": ServiceInfo("ecr", ServiceStatus.NATIVE, "json", "Elastic Container Registry"),
    # Auto-registered Moto-backed services
    "account": ServiceInfo(
        "account", ServiceStatus.MOTO_BACKED, "rest-json", "AWS Account Management"
    ),
    "acmpca": ServiceInfo("acmpca", ServiceStatus.MOTO_BACKED, "json", "ACM Private CA"),
    "amp": ServiceInfo("amp", ServiceStatus.MOTO_BACKED, "rest-json", "Managed Prometheus"),
    "apigatewaymanagementapi": ServiceInfo(
        "apigatewaymanagementapi",
        ServiceStatus.MOTO_BACKED,
        "rest-json",
        "API Gateway Management API",
    ),
    "appconfig": ServiceInfo("appconfig", ServiceStatus.MOTO_BACKED, "rest-json", "AppConfig"),
    "applicationautoscaling": ServiceInfo(
        "applicationautoscaling", ServiceStatus.MOTO_BACKED, "json", "Application Auto Scaling"
    ),
    "appmesh": ServiceInfo("appmesh", ServiceStatus.MOTO_BACKED, "rest-json", "App Mesh"),
    "athena": ServiceInfo("athena", ServiceStatus.MOTO_BACKED, "json", "Athena SQL Analytics"),
    "autoscaling": ServiceInfo(
        "autoscaling", ServiceStatus.MOTO_BACKED, "query", "Auto Scaling Groups"
    ),
    "backup": ServiceInfo("backup", ServiceStatus.MOTO_BACKED, "rest-json", "AWS Backup"),
    "bedrock": ServiceInfo("bedrock", ServiceStatus.MOTO_BACKED, "rest-json", "Bedrock AI"),
    "bedrockagent": ServiceInfo(
        "bedrockagent", ServiceStatus.MOTO_BACKED, "rest-json", "Bedrock Agent"
    ),
    "budgets": ServiceInfo("budgets", ServiceStatus.MOTO_BACKED, "json", "AWS Budgets"),
    "ce": ServiceInfo("ce", ServiceStatus.MOTO_BACKED, "json", "Cost Explorer"),
    "clouddirectory": ServiceInfo(
        "clouddirectory", ServiceStatus.MOTO_BACKED, "rest-json", "Cloud Directory"
    ),
    "cloudfront": ServiceInfo(
        "cloudfront", ServiceStatus.MOTO_BACKED, "rest-xml", "CloudFront CDN"
    ),
    "cloudhsmv2": ServiceInfo("cloudhsmv2", ServiceStatus.MOTO_BACKED, "json", "CloudHSM v2"),
    "cloudtrail": ServiceInfo(
        "cloudtrail", ServiceStatus.MOTO_BACKED, "json", "CloudTrail Audit Logging"
    ),
    "codebuild": ServiceInfo("codebuild", ServiceStatus.MOTO_BACKED, "json", "CodeBuild CI"),
    "codecommit": ServiceInfo(
        "codecommit", ServiceStatus.MOTO_BACKED, "json", "CodeCommit Git Repos"
    ),
    "codedeploy": ServiceInfo(
        "codedeploy", ServiceStatus.MOTO_BACKED, "json", "CodeDeploy Deployment"
    ),
    "codepipeline": ServiceInfo(
        "codepipeline", ServiceStatus.MOTO_BACKED, "json", "CodePipeline CI/CD"
    ),
    "cognitoidentity": ServiceInfo(
        "cognitoidentity", ServiceStatus.MOTO_BACKED, "json", "Cognito Identity Pools"
    ),
    "comprehend": ServiceInfo("comprehend", ServiceStatus.MOTO_BACKED, "json", "Comprehend NLP"),
    "connect": ServiceInfo("connect", ServiceStatus.MOTO_BACKED, "rest-json", "Amazon Connect"),
    "connectcampaigns": ServiceInfo(
        "connectcampaigns", ServiceStatus.MOTO_BACKED, "rest-json", "Connect Campaigns"
    ),
    "databrew": ServiceInfo("databrew", ServiceStatus.MOTO_BACKED, "rest-json", "DataBrew"),
    "datapipeline": ServiceInfo("datapipeline", ServiceStatus.MOTO_BACKED, "json", "Data Pipeline"),
    "datasync": ServiceInfo("datasync", ServiceStatus.MOTO_BACKED, "json", "DataSync"),
    "dax": ServiceInfo("dax", ServiceStatus.MOTO_BACKED, "json", "DynamoDB Accelerator"),
    # directconnect: deregistered — all Moto ops return 500
    "dms": ServiceInfo("dms", ServiceStatus.MOTO_BACKED, "json", "Database Migration Service"),
    "ds": ServiceInfo("ds", ServiceStatus.MOTO_BACKED, "json", "Directory Service"),
    "dsql": ServiceInfo("dsql", ServiceStatus.MOTO_BACKED, "rest-json", "Aurora DSQL"),
    # ebs: deregistered — all Moto ops return 500
    "ec2instanceconnect": ServiceInfo(
        "ec2instanceconnect", ServiceStatus.MOTO_BACKED, "json", "EC2 Instance Connect"
    ),
    "efs": ServiceInfo("efs", ServiceStatus.MOTO_BACKED, "rest-json", "Elastic File System"),
    "eks": ServiceInfo(
        "eks", ServiceStatus.NATIVE, "rest-json", "Elastic Kubernetes Service with mock K8s API"
    ),
    "elasticache": ServiceInfo(
        "elasticache", ServiceStatus.MOTO_BACKED, "query", "ElastiCache (Redis/Memcached)"
    ),
    "elasticbeanstalk": ServiceInfo(
        "elasticbeanstalk", ServiceStatus.MOTO_BACKED, "query", "Elastic Beanstalk"
    ),
    "elb": ServiceInfo("elb", ServiceStatus.MOTO_BACKED, "query", "Classic Load Balancer"),
    "elbv2": ServiceInfo(
        "elbv2", ServiceStatus.MOTO_BACKED, "query", "Application/Network Load Balancer"
    ),
    "emr": ServiceInfo("emr", ServiceStatus.MOTO_BACKED, "json", "Elastic MapReduce"),
    "emrcontainers": ServiceInfo(
        "emrcontainers", ServiceStatus.MOTO_BACKED, "rest-json", "EMR on EKS"
    ),
    "emrserverless": ServiceInfo(
        "emrserverless", ServiceStatus.MOTO_BACKED, "rest-json", "EMR Serverless"
    ),
    # forecast: deregistered — deprecated by AWS, all Moto ops return 500
    "fsx": ServiceInfo("fsx", ServiceStatus.MOTO_BACKED, "json", "FSx File Systems"),
    "glacier": ServiceInfo(
        "glacier", ServiceStatus.MOTO_BACKED, "rest-json", "Glacier Archive Storage"
    ),
    "glue": ServiceInfo("glue", ServiceStatus.MOTO_BACKED, "json", "Glue ETL"),
    "greengrass": ServiceInfo(
        "greengrass", ServiceStatus.MOTO_BACKED, "rest-json", "IoT Greengrass"
    ),
    "guardduty": ServiceInfo(
        "guardduty", ServiceStatus.MOTO_BACKED, "rest-json", "GuardDuty Threat Detection"
    ),
    "identitystore": ServiceInfo(
        "identitystore", ServiceStatus.MOTO_BACKED, "json", "Identity Store"
    ),
    "inspector2": ServiceInfo(
        "inspector2", ServiceStatus.MOTO_BACKED, "rest-json", "Inspector Vulnerability Scanning"
    ),
    "iot": ServiceInfo("iot", ServiceStatus.MOTO_BACKED, "rest-json", "IoT Core"),
    "iotdata": ServiceInfo("iotdata", ServiceStatus.MOTO_BACKED, "rest-json", "IoT Data Plane"),
    "ivs": ServiceInfo("ivs", ServiceStatus.MOTO_BACKED, "rest-json", "Interactive Video Service"),
    "kafka": ServiceInfo(
        "kafka", ServiceStatus.MOTO_BACKED, "rest-json", "Managed Streaming for Kafka"
    ),
    "kinesisanalyticsv2": ServiceInfo(
        "kinesisanalyticsv2", ServiceStatus.MOTO_BACKED, "json", "Kinesis Analytics v2"
    ),
    "kinesisvideo": ServiceInfo(
        "kinesisvideo", ServiceStatus.MOTO_BACKED, "rest-json", "Kinesis Video Streams"
    ),
    # kinesisvideoarchivedmedia: deregistered — shares signing name with kinesisvideo, not routable
    "lakeformation": ServiceInfo(
        "lakeformation", ServiceStatus.MOTO_BACKED, "rest-json", "Lake Formation"
    ),
    "lexv2models": ServiceInfo(
        "lexv2models", ServiceStatus.MOTO_BACKED, "rest-json", "Lex v2 Models"
    ),
    "macie2": ServiceInfo("macie2", ServiceStatus.MOTO_BACKED, "rest-json", "Macie v2"),
    "managedblockchain": ServiceInfo(
        "managedblockchain", ServiceStatus.MOTO_BACKED, "rest-json", "Managed Blockchain"
    ),
    "mediaconnect": ServiceInfo(
        "mediaconnect", ServiceStatus.MOTO_BACKED, "rest-json", "MediaConnect"
    ),
    "medialive": ServiceInfo("medialive", ServiceStatus.MOTO_BACKED, "rest-json", "MediaLive"),
    "mediapackage": ServiceInfo(
        "mediapackage", ServiceStatus.MOTO_BACKED, "rest-json", "MediaPackage"
    ),
    "mediapackagev2": ServiceInfo(
        "mediapackagev2", ServiceStatus.MOTO_BACKED, "rest-json", "MediaPackage v2"
    ),
    "mediastore": ServiceInfo("mediastore", ServiceStatus.MOTO_BACKED, "json", "MediaStore"),
    # mediastoredata: deregistered — shares signing name with mediastore, not routable
    "memorydb": ServiceInfo("memorydb", ServiceStatus.MOTO_BACKED, "json", "MemoryDB for Redis"),
    # meteringmarketplace: deregistered — requires marketplace context, not useful locally
    "mq": ServiceInfo("mq", ServiceStatus.MOTO_BACKED, "rest-json", "Amazon MQ Message Brokers"),
    "networkfirewall": ServiceInfo(
        "networkfirewall", ServiceStatus.MOTO_BACKED, "json", "Network Firewall"
    ),
    "networkmanager": ServiceInfo(
        "networkmanager", ServiceStatus.MOTO_BACKED, "rest-json", "Network Manager"
    ),
    "opensearchserverless": ServiceInfo(
        "opensearchserverless", ServiceStatus.MOTO_BACKED, "json", "OpenSearch Serverless"
    ),
    "organizations": ServiceInfo(
        "organizations", ServiceStatus.MOTO_BACKED, "json", "AWS Organizations"
    ),
    "osis": ServiceInfo("osis", ServiceStatus.MOTO_BACKED, "rest-json", "OpenSearch Ingestion"),
    "panorama": ServiceInfo("panorama", ServiceStatus.MOTO_BACKED, "rest-json", "Panorama"),
    # personalize: deregistered — all Moto ops return 500
    "pinpoint": ServiceInfo("pinpoint", ServiceStatus.MOTO_BACKED, "rest-json", "Pinpoint"),
    "pipes": ServiceInfo("pipes", ServiceStatus.MOTO_BACKED, "rest-json", "EventBridge Pipes"),
    "polly": ServiceInfo("polly", ServiceStatus.MOTO_BACKED, "rest-json", "Polly Text-to-Speech"),
    "quicksight": ServiceInfo("quicksight", ServiceStatus.MOTO_BACKED, "rest-json", "QuickSight"),
    "ram": ServiceInfo("ram", ServiceStatus.MOTO_BACKED, "rest-json", "Resource Access Manager"),
    "rds": ServiceInfo("rds", ServiceStatus.MOTO_BACKED, "query", "Relational Database Service"),
    "rdsdata": ServiceInfo("rdsdata", ServiceStatus.MOTO_BACKED, "rest-json", "RDS Data API"),
    "redshiftdata": ServiceInfo(
        "redshiftdata", ServiceStatus.MOTO_BACKED, "json", "Redshift Data API"
    ),
    "rekognition": ServiceInfo("rekognition", ServiceStatus.NATIVE, "json", "Rekognition"),
    "resiliencehub": ServiceInfo(
        "resiliencehub", ServiceStatus.MOTO_BACKED, "rest-json", "Resilience Hub"
    ),
    "route53domains": ServiceInfo(
        "route53domains", ServiceStatus.MOTO_BACKED, "json", "Route 53 Domains"
    ),
    "s3tables": ServiceInfo("s3tables", ServiceStatus.MOTO_BACKED, "rest-json", "S3 Tables"),
    "s3vectors": ServiceInfo("s3vectors", ServiceStatus.MOTO_BACKED, "rest-json", "S3 Vectors"),
    "sagemaker": ServiceInfo(
        "sagemaker", ServiceStatus.MOTO_BACKED, "json", "SageMaker ML Platform"
    ),
    # sagemakermetrics: deregistered — shares signing name with sagemaker, not routable
    # sagemakerruntime: deregistered — shares signing name with sagemaker, not routable
    # sdb: deregistered — all Moto ops return InternalError
    "securityhub": ServiceInfo(
        "securityhub", ServiceStatus.MOTO_BACKED, "rest-json", "Security Hub"
    ),
    "servicecatalog": ServiceInfo(
        "servicecatalog", ServiceStatus.MOTO_BACKED, "json", "Service Catalog"
    ),
    "servicecatalogappregistry": ServiceInfo(
        "servicecatalogappregistry",
        ServiceStatus.MOTO_BACKED,
        "rest-json",
        "Service Catalog App Registry",
    ),
    "servicediscovery": ServiceInfo(
        "servicediscovery", ServiceStatus.MOTO_BACKED, "json", "Cloud Map Service Discovery"
    ),
    # servicequotas: deregistered — all Moto ops return 500
    "shield": ServiceInfo("shield", ServiceStatus.MOTO_BACKED, "json", "Shield DDoS Protection"),
    "signer": ServiceInfo("signer", ServiceStatus.MOTO_BACKED, "rest-json", "Signer"),
    "ssoadmin": ServiceInfo("ssoadmin", ServiceStatus.MOTO_BACKED, "json", "SSO Admin"),
    "synthetics": ServiceInfo(
        "synthetics", ServiceStatus.MOTO_BACKED, "rest-json", "CloudWatch Synthetics"
    ),
    "textract": ServiceInfo("textract", ServiceStatus.MOTO_BACKED, "json", "Textract"),
    "timestreaminfluxdb": ServiceInfo(
        "timestreaminfluxdb", ServiceStatus.MOTO_BACKED, "json", "Timestream InfluxDB"
    ),
    "timestreamquery": ServiceInfo(
        "timestreamquery", ServiceStatus.MOTO_BACKED, "json", "Timestream Query"
    ),
    "timestreamwrite": ServiceInfo(
        "timestreamwrite", ServiceStatus.MOTO_BACKED, "json", "Timestream Write"
    ),
    "transfer": ServiceInfo("transfer", ServiceStatus.MOTO_BACKED, "json", "Transfer Family"),
    "vpclattice": ServiceInfo("vpclattice", ServiceStatus.MOTO_BACKED, "rest-json", "VPC Lattice"),
    "wafv2": ServiceInfo("wafv2", ServiceStatus.MOTO_BACKED, "json", "WAF v2"),
    "workspaces": ServiceInfo("workspaces", ServiceStatus.MOTO_BACKED, "json", "WorkSpaces"),
    "workspacesweb": ServiceInfo(
        "workspacesweb", ServiceStatus.MOTO_BACKED, "rest-json", "WorkSpaces Web"
    ),
    "xray": ServiceInfo("xray", ServiceStatus.NATIVE, "rest-json", "X-Ray Distributed Tracing"),
}


def get_enabled_services() -> list[str]:
    """Return names of all enabled services."""
    return sorted(SERVICE_REGISTRY.keys())


def is_service_enabled(service_name: str) -> bool:
    return service_name in SERVICE_REGISTRY
