"""Failing tests for known routing bugs.

Each test documents a real correctness bug in the service routing layer.
All tests in this file are expected to FAIL until the corresponding bug is fixed.
"""

from unittest.mock import MagicMock

from robotocore.gateway.router import route_to_service
from robotocore.services.registry import SERVICE_REGISTRY


def _make_request(
    path: str = "/",
    headers: dict | None = None,
    query_params: dict | None = None,
) -> MagicMock:
    """Create a mock Starlette Request."""
    req = MagicMock()
    req.url.path = path
    req.headers = headers or {}
    req.query_params = query_params or {}
    return req


def _make_auth_header(service: str) -> str:
    return (
        "AWS4-HMAC-SHA256 "
        f"Credential=AKID/20260305/us-east-1/{service}/aws4_request, "
        "SignedHeaders=host, Signature=abc123"
    )


# ---------------------------------------------------------------------------
# Bug 1: cognito-identity TARGET_PREFIX_MAP returns a name not in the registry
# TARGET_PREFIX_MAP maps "AWSCognitoIdentityService" -> "cognito-identity"
# but SERVICE_REGISTRY key is "cognitoidentity" (no hyphen).
# ---------------------------------------------------------------------------
class TestCognitoIdentityNameMismatch:
    def test_target_prefix_returns_registry_name(self):
        """X-Amz-Target routing for Cognito Identity should return a name
        that exists in SERVICE_REGISTRY."""
        req = _make_request(headers={"x-amz-target": "AWSCognitoIdentityService.GetId"})
        result = route_to_service(req)
        assert result in SERVICE_REGISTRY, (
            f"route_to_service returned '{result}' which is not a key in SERVICE_REGISTRY. "
            f"Expected 'cognitoidentity'."
        )


# ---------------------------------------------------------------------------
# Bug 2: Missing TARGET_PREFIX_MAP entry for SQS (AmazonSQS)
# Modern boto3 uses JSON protocol for SQS, sending X-Amz-Target: AmazonSQS.SendMessage
# ---------------------------------------------------------------------------
class TestSqsTargetPrefix:
    def test_sqs_json_protocol_target(self):
        """SQS JSON protocol requests use X-Amz-Target: AmazonSQS.SendMessage."""
        req = _make_request(headers={"x-amz-target": "AmazonSQS.SendMessage"})
        assert route_to_service(req) == "sqs"


# ---------------------------------------------------------------------------
# Bug 3: Missing TARGET_PREFIX_MAP entry for Events (AWSEvents)
# Botocore's actual targetPrefix for EventBridge is "AWSEvents", not "CloudWatchEvents".
# "CloudWatchEvents" works for some older/alternative paths but the canonical prefix
# sent by boto3 is "AWSEvents".
# ---------------------------------------------------------------------------
class TestEventsTargetPrefix:
    def test_events_botocore_target_prefix(self):
        """EventBridge's botocore targetPrefix is 'AWSEvents'."""
        req = _make_request(headers={"x-amz-target": "AWSEvents.PutEvents"})
        assert route_to_service(req) == "events"


# ---------------------------------------------------------------------------
# Bug 4: Missing TARGET_PREFIX_MAP entry for CloudWatch (GraniteServiceVersion20100801)
# Botocore's targetPrefix for cloudwatch is "GraniteServiceVersion20100801".
# The router has "monitoring" which is the endpointPrefix, not the targetPrefix.
# Modern boto3 sends X-Amz-Target: GraniteServiceVersion20100801.PutMetricData
# ---------------------------------------------------------------------------
class TestCloudWatchTargetPrefix:
    def test_cloudwatch_botocore_target_prefix(self):
        """CloudWatch's botocore targetPrefix is 'GraniteServiceVersion20100801'."""
        req = _make_request(headers={"x-amz-target": "GraniteServiceVersion20100801.PutMetricData"})
        assert route_to_service(req) == "cloudwatch"


# ---------------------------------------------------------------------------
# Bug 5: Missing TARGET_PREFIX_MAP entry for SWF (SimpleWorkflowService)
# ---------------------------------------------------------------------------
class TestSwfTargetPrefix:
    def test_swf_target_prefix(self):
        """SWF's botocore targetPrefix is 'SimpleWorkflowService'."""
        req = _make_request(headers={"x-amz-target": "SimpleWorkflowService.PollForDecisionTask"})
        assert route_to_service(req) == "swf"


# ---------------------------------------------------------------------------
# Bug 6: Missing TARGET_PREFIX_MAP entry for Route53Resolver
# ---------------------------------------------------------------------------
class TestRoute53ResolverTargetPrefix:
    def test_route53resolver_target_prefix(self):
        """Route53Resolver's botocore targetPrefix is 'Route53Resolver'."""
        req = _make_request(headers={"x-amz-target": "Route53Resolver.CreateResolverEndpoint"})
        assert route_to_service(req) == "route53resolver"


# ---------------------------------------------------------------------------
# Bug 7: Missing TARGET_PREFIX_MAP entry for Transcribe
# ---------------------------------------------------------------------------
class TestTranscribeTargetPrefix:
    def test_transcribe_target_prefix(self):
        """Transcribe's botocore targetPrefix is 'Transcribe'."""
        req = _make_request(headers={"x-amz-target": "Transcribe.StartTranscriptionJob"})
        assert route_to_service(req) == "transcribe"


# ---------------------------------------------------------------------------
# Bug 8: Missing TARGET_PREFIX_MAP entry for ResourceGroupsTaggingAPI
# ---------------------------------------------------------------------------
class TestResourceGroupsTaggingApiTargetPrefix:
    def test_tagging_target_prefix(self):
        """ResourceGroupsTaggingAPI's botocore targetPrefix is
        'ResourceGroupsTaggingAPI_20170126'."""
        req = _make_request(
            headers={"x-amz-target": "ResourceGroupsTaggingAPI_20170126.GetResources"}
        )
        assert route_to_service(req) == "resourcegroupstaggingapi"


# ---------------------------------------------------------------------------
# Bug 9: Missing SERVICE_NAME_ALIASES for "sso" -> "ssoadmin"
# sso-admin's botocore signingName is "sso". When an SSO Admin API call
# goes through auth header routing, it would return "sso" which isn't
# in SERVICE_REGISTRY.
# ---------------------------------------------------------------------------
class TestSsoAdminSigningName:
    def test_sso_signing_name_routes_to_ssoadmin(self):
        """Auth header with signingName 'sso' should route to 'ssoadmin'."""
        req = _make_request(headers={"authorization": _make_auth_header("sso")})
        result = route_to_service(req)
        assert result in SERVICE_REGISTRY, (
            f"route_to_service returned '{result}' which is not in SERVICE_REGISTRY. "
            f"Expected 'ssoadmin'."
        )


# ---------------------------------------------------------------------------
# Bug 10: Missing SERVICE_NAME_ALIASES for "execute-api" -> "apigatewaymanagementapi"
# apigatewaymanagementapi's botocore signingName is "execute-api".
# ---------------------------------------------------------------------------
class TestApiGatewayManagementSigningName:
    def test_execute_api_signing_name(self):
        """Auth header with signingName 'execute-api' should route to
        'apigatewaymanagementapi'."""
        req = _make_request(headers={"authorization": _make_auth_header("execute-api")})
        result = route_to_service(req)
        assert result in SERVICE_REGISTRY, (
            f"route_to_service returned '{result}' which is not in SERVICE_REGISTRY. "
            f"Expected 'apigatewaymanagementapi'."
        )


# ---------------------------------------------------------------------------
# Bug 11: PATH_PATTERNS shadowing — opensearch pattern shadows lambda
# The pattern ^/2021-01-01/ for opensearch comes before ^/2021-\d{2}-\d{2}/functions/
# for lambda. A Lambda function URL path like /2021-01-01/functions/my-func
# incorrectly routes to opensearch.
# ---------------------------------------------------------------------------
class TestPathPatternShadowing:
    def test_lambda_2021_path_not_shadowed_by_opensearch(self):
        """Lambda paths under /2021-01-01/functions/ should route to lambda,
        not be shadowed by the opensearch /2021-01-01/ pattern."""
        req = _make_request(path="/2021-01-01/functions/my-func/invocations")
        assert route_to_service(req) == "lambda"
