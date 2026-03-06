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
    "AWSCognitoIdentityService": "cognito-identity",
    "AWSStepFunctions": "stepfunctions",
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
    "StarlingDoveService": "config",
    "TrentService": "kms",
    "WorkspacesService": "workspaces",
}

# URL path patterns to service names
PATH_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^/2015-03-31/functions"), "lambda"),
    (re.compile(r"^/2021-01-01/"), "opensearch"),
    (re.compile(r"^/2021-\d{2}-\d{2}/functions/"), "lambda"),
    (re.compile(r"^/restapis"), "apigateway"),
    (re.compile(r"^/v2/"), "apigatewayv2"),
    (re.compile(r"^/v20180820/"), "s3control"),
    (re.compile(r"^/2013-04-01/"), "route53"),
    (re.compile(r"^/2014-11-13/"), "logs"),
    (re.compile(r"^/tags"), "resourcegroupstaggingapi"),
    (re.compile(r"^/prod/"), "kafka"),
]

# Service name extracted from credential scope in Authorization header
AUTH_SERVICE_RE = re.compile(
    r"Credential=[^/]+/\d{8}/[^/]+/([^/]+)/aws4_request"
)

# AWS credential scope service names that differ from Moto backend names
SERVICE_NAME_ALIASES: dict[str, str] = {
    "monitoring": "cloudwatch",
    "email": "ses",
    "states": "stepfunctions",
    "elasticmapreduce": "emr",
    "tagging": "resourcegroupstaggingapi",
}


def route_to_service(request: Request) -> str | None:
    """Determine the target AWS service from request attributes."""

    # 1. Check X-Amz-Target header (JSON protocol services like DynamoDB, KMS, etc.)
    target = request.headers.get("x-amz-target", "")
    if target:
        # Target format is "ServiceName.Operation" or "ServiceName_Version.Operation"
        prefix = target.split(".")[0]
        # Strip version suffix (e.g., "DynamoDB_20120810" -> "DynamoDB")
        base_prefix = prefix.split("_")[0]
        if prefix in TARGET_PREFIX_MAP:
            return TARGET_PREFIX_MAP[prefix]
        if base_prefix in TARGET_PREFIX_MAP:
            return TARGET_PREFIX_MAP[base_prefix]

    # 2. Check URL path patterns (before auth, since some services share signing names)
    path = request.url.path
    for pattern, service in PATH_PATTERNS:
        if pattern.match(path):
            return service

    # 3. Check Authorization header for service name in credential scope
    auth = request.headers.get("authorization", "")
    match = AUTH_SERVICE_RE.search(auth)
    if match:
        service = match.group(1)
        return SERVICE_NAME_ALIASES.get(service, service)

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

    return None
