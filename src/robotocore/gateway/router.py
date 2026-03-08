"""AWS service detection from incoming HTTP requests.

Determines which AWS service a request targets by inspecting:
1. Authorization header (credential scope contains service name)
2. X-Amz-Target header (used by JSON protocol services)
3. URL path patterns (e.g., /2015-03-31/functions for Lambda)
4. Host header (e.g., sqs.us-east-1.amazonaws.com)
"""

import re

from starlette.requests import Request

# Map of X-Amz-Target prefixes to service names
TARGET_PREFIX_MAP: dict[str, str] = {
    "AWSCognitoIdentityProviderService": "cognito-idp",
    "AWSCognitoIdentityService": "cognitoidentity",
    "AWSStepFunctions": "stepfunctions",
    "AWSSupport": "support",
    "AmazonSSM": "ssm",
    "CertificateManager": "acm",
    "AmazonEC2ContainerRegistry": "ecr",
    "AmazonEC2ContainerServiceV20141113": "ecs",
    "CloudWatchEvents": "events",
    "DynamoDB": "dynamodb",
    "DynamoDBStreams": "dynamodbstreams",
    "Firehose": "firehose",
    "Kinesis": "kinesis",
    "Logs": "logs",
    "monitoring": "cloudwatch",
    "OvertureService": "support",
    "Route53Domains": "route53domains",
    "SageMaker": "sagemaker",
    "SecretManager": "secretsmanager",
    "secretsmanager": "secretsmanager",
    "StarlingDoveService": "config",
    "TrentService": "kms",
    "WorkspacesService": "workspaces",
    "ACMPrivateCA": "acmpca",
    "AWS242ServiceCatalogService": "servicecatalog",
    "AWSBudgetServiceGateway": "budgets",
    "AWSEC2InstanceConnectService": "ec2instanceconnect",
    "AWSGlue": "glue",
    "AWSIdentityStore": "identitystore",
    "AWSInsightsIndexService": "ce",
    "AWSMPMeteringService": "meteringmarketplace",
    "AWSOrganizationsV20161128": "organizations",
    "AWSShield_20160616": "shield",
    "AWSSimbaAPIService_v20180301": "fsx",
    "AWSWAF_20190729": "wafv2",
    "AmazonAthena": "athena",
    "AmazonDAXV3": "dax",
    "AmazonDMSv20160101": "dms",
    "AmazonForecast": "forecast",
    "AmazonMemoryDB": "memorydb",
    "AmazonPersonalize": "personalize",
    "AmazonTimestreamInfluxDB": "timestreaminfluxdb",
    "AnyScaleFrontendService": "applicationautoscaling",
    "BaldrApiService": "cloudhsmv2",
    "CodeBuild_20161006": "codebuild",
    "CodeCommit_20150413": "codecommit",
    "CodeDeploy_20141006": "codedeploy",
    "CodePipeline_20150709": "codepipeline",
    "Comprehend_20171127": "comprehend",
    "DataPipeline": "datapipeline",
    "DirectoryService_20150416": "ds",
    "ElasticMapReduce": "emr",
    "FmrsService": "datasync",
    "KinesisAnalytics_20180523": "kinesisanalyticsv2",
    "MediaStore_20170901": "mediastore",
    "NetworkFirewall_20201112": "networkfirewall",
    "OpenSearchServerless": "opensearchserverless",
    "RedshiftData": "redshiftdata",
    "RekognitionService": "rekognition",
    "Route53AutoNaming_v20170314": "servicediscovery",
    "Route53Domains_v20140515": "route53domains",
    "SWBExternalService": "ssoadmin",
    "ServiceQuotasV20190624": "servicequotas",
    "Textract": "textract",
    # Note: Timestream query and write share the same target prefix.
    # We route to timestreamwrite by default; query ops are handled below.
    "Timestream_20181101": "timestreamwrite",
    "TransferService": "transfer",
    "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101": "cloudtrail",
    "AmazonSQS": "sqs",
    "AWSEvents": "events",
    "GraniteServiceVersion20100801": "cloudwatch",
    "SimpleWorkflowService": "swf",
    "Route53Resolver": "route53resolver",
    "Transcribe": "transcribe",
    "ResourceGroupsTaggingAPI_20170126": "resourcegroupstaggingapi",
}

# URL path patterns to service names
PATH_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^/2014-11-13/functions"), "lambda"),
    (re.compile(r"^/2015-03-31/functions"), "lambda"),
    (re.compile(r"^/2021-\d{2}-\d{2}/functions/"), "lambda"),
    (re.compile(r"^/2021-01-01/"), "opensearch"),
    (re.compile(r"^/restapis"), "apigateway"),
    (re.compile(r"^/v2/email/"), "sesv2"),
    (re.compile(r"^/v2/"), "apigatewayv2"),
    (re.compile(r"^/v20180820/"), "s3control"),
    (re.compile(r"^/2015-01-01/es/"), "es"),
    (re.compile(r"^/2013-04-01/"), "route53"),
    (re.compile(r"^/2014-11-13/"), "logs"),
    (re.compile(r"^/tags$"), "resourcegroupstaggingapi"),
    (re.compile(r"^/prod/channels"), "medialive"),
    (re.compile(r"^/prod/inputs"), "medialive"),
    (re.compile(r"^/prod/input-security-groups"), "medialive"),
    (re.compile(r"^/prod/offerings"), "medialive"),
    (re.compile(r"^/prod/reservations"), "medialive"),
    (re.compile(r"^/prod/"), "kafka"),
    (re.compile(r"^/v1/apis"), "appsync"),
    (re.compile(r"^/v1/create"), "batch"),
    (re.compile(r"^/v1/describe"), "batch"),
    (re.compile(r"^/v1/update"), "batch"),
    (re.compile(r"^/v1/delete"), "batch"),
    (re.compile(r"^/v1/register"), "batch"),
    (re.compile(r"^/v1/deregister"), "batch"),
    (re.compile(r"^/v1/submit"), "batch"),
    (re.compile(r"^/v1/list"), "batch"),
    (re.compile(r"^/v1/terminate"), "batch"),
    (re.compile(r"^/v1/cancel"), "batch"),
    (re.compile(r"^/v1/tags"), "batch"),
    (re.compile(r"^/v1/untag"), "batch"),
]

# Service name extracted from credential scope in Authorization header
AUTH_SERVICE_RE = re.compile(r"Credential=[^/]+/\d{8}/[^/]+/([^/]+)/aws4_request")

# AWS credential scope service names that differ from Moto backend names
SERVICE_NAME_ALIASES: dict[str, str] = {
    "monitoring": "cloudwatch",
    "email": "ses",
    "states": "stepfunctions",
    "elasticmapreduce": "emr",
    "tagging": "resourcegroupstaggingapi",
    "acm-pca": "acmpca",
    "aoss": "opensearchserverless",
    "application-autoscaling": "applicationautoscaling",
    "aps": "amp",
    "aws-marketplace": "meteringmarketplace",
    "cloudhsm": "cloudhsmv2",
    "connect-campaigns": "connectcampaigns",
    "ec2-instance-connect": "ec2instanceconnect",
    "elasticfilesystem": "efs",
    "elasticloadbalancing": "elbv2",
    "emr-containers": "emrcontainers",
    "emr-serverless": "emrserverless",
    "kinesisanalytics": "kinesisanalyticsv2",
    "lex": "lexv2models",
    "mobiletargeting": "pinpoint",
    "network-firewall": "networkfirewall",
    "rds-data": "rdsdata",
    "redshift-data": "redshiftdata",
    "timestream": "timestreamwrite",
    "timestream-influxdb": "timestreaminfluxdb",
    "s3express": "s3",
    "vpc-lattice": "vpclattice",
    "workspaces-web": "workspacesweb",
    "sso": "ssoadmin",
    "execute-api": "apigatewaymanagementapi",
}


# Timestream Query operations (vs Write ops which are the default)
_TIMESTREAM_QUERY_OPS = frozenset(
    {
        "CancelQuery",
        "CreateScheduledQuery",
        "DeleteScheduledQuery",
        "DescribeAccountSettings",
        "DescribeScheduledQuery",
        "ExecuteScheduledQuery",
        "ListScheduledQueries",
        "PrepareQuery",
        "Query",
        "UpdateAccountSettings",
        "UpdateScheduledQuery",
    }
)


def route_to_service(request: Request) -> str | None:
    """Determine the target AWS service from request attributes."""

    # 1. Check X-Amz-Target header (JSON protocol services like DynamoDB, KMS, etc.)
    target = request.headers.get("x-amz-target", "")
    if target:
        # Target format is "ServiceName.Operation" or "ServiceName_Version.Operation"
        prefix = target.split(".")[0]
        operation = target.split(".")[-1] if "." in target else ""
        # Strip version suffix (e.g., "DynamoDB_20120810" -> "DynamoDB")
        base_prefix = prefix.split("_")[0]

        # Timestream query and write share the same target prefix — disambiguate by op
        if prefix == "Timestream_20181101" and operation in _TIMESTREAM_QUERY_OPS:
            return "timestreamquery"

        if prefix in TARGET_PREFIX_MAP:
            return TARGET_PREFIX_MAP[prefix]
        if base_prefix in TARGET_PREFIX_MAP:
            return TARGET_PREFIX_MAP[base_prefix]

    # 2. Check URL path patterns (before auth, since some services share signing names)
    path = request.url.path

    # /v2/apis is shared by appsync and apigatewayv2 — disambiguate by auth header
    if path.startswith("/v2/apis"):
        auth = request.headers.get("authorization", "")
        match = AUTH_SERVICE_RE.search(auth)
        if match and match.group(1) == "appsync":
            return "appsync"
        return "apigatewayv2"

    for pattern, service in PATH_PATTERNS:
        if pattern.match(path):
            # /v1/tags and /v1/untag are shared by Batch, AppSync, Kafka, MQ, and Pinpoint
            # — disambiguate via the service name in the auth credential scope
            if service == "batch" and (path.startswith("/v1/tags") or path.startswith("/v1/untag")):
                auth = request.headers.get("authorization", "")
                auth_match = AUTH_SERVICE_RE.search(auth)
                if auth_match:
                    auth_service = auth_match.group(1)
                    resolved = SERVICE_NAME_ALIASES.get(auth_service, auth_service)
                    if resolved in ("appsync", "kafka", "mq", "pinpoint"):
                        return resolved
            return service

    # 3. Check Authorization header for service name in credential scope
    auth = request.headers.get("authorization", "")
    match = AUTH_SERVICE_RE.search(auth)
    if match:
        service = match.group(1)
        resolved = SERVICE_NAME_ALIASES.get(service, service)
        # ELB Classic and ELBv2 share the signing name 'elasticloadbalancing'.
        # Disambiguate by the API Version query parameter.
        if resolved == "elbv2":
            version = request.query_params.get("Version", "")
            if version == "2012-06-01":
                return "elb"
        return resolved

    # 4. Check X-Amz-Credential query parameter (SigV4 presigned URLs)
    credential = request.query_params.get("X-Amz-Credential", "")
    if credential:
        # Format: <access-key>/<date>/<region>/<service>/aws4_request
        parts = credential.split("/")
        if len(parts) >= 4:
            service = parts[3]
            return SERVICE_NAME_ALIASES.get(service, service)

    # 4b. Check for SigV2 presigned URLs (AWSAccessKeyId + Signature)
    if request.query_params.get("AWSAccessKeyId") and request.query_params.get("Signature"):
        # SigV2 presigned URLs don't encode the service name.
        # Infer from path — S3 is the only service that commonly uses SigV2 presigned URLs.
        return "s3"

    # 5. Check Host header
    host = request.headers.get("host", "")
    if ".s3." in host or host.startswith("s3.") or host.startswith("s3-"):
        return "s3"

    # 6. Query string action parameter (used by EC2, SQS, SNS, etc.)
    action = request.query_params.get("Action")
    if action:
        # These services use query protocol with Action parameter
        # The service is in the auth header which we already checked,
        # but as a fallback we can try common patterns
        if "Queue" in path or "queue" in path:
            return "sqs"
        if "Topic" in path or "topic" in path:
            return "sns"

    # 7. Body-based Action detection for unsigned requests
    # Some STS operations (AssumeRoleWithWebIdentity, AssumeRoleWithSAML)
    # don't include an Authorization header.
    content_type = request.headers.get("content-type", "")
    if "x-www-form-urlencoded" in content_type and not auth:
        return "sts"

    return None
