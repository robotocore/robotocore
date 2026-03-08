"""Tests for AWS service routing logic."""

from unittest.mock import MagicMock

from robotocore.gateway.router import route_to_service


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


class TestRouteFromXAmzTarget:
    def test_dynamodb(self):
        req = _make_request(headers={"x-amz-target": "DynamoDB_20120810.GetItem"})
        assert route_to_service(req) == "dynamodb"

    def test_kinesis(self):
        req = _make_request(headers={"x-amz-target": "Kinesis_20131202.PutRecord"})
        assert route_to_service(req) == "kinesis"

    def test_kms(self):
        req = _make_request(headers={"x-amz-target": "TrentService.Encrypt"})
        assert route_to_service(req) == "kms"

    def test_stepfunctions(self):
        req = _make_request(headers={"x-amz-target": "AWSStepFunctions.StartExecution"})
        assert route_to_service(req) == "stepfunctions"

    def test_cloudwatch_logs(self):
        req = _make_request(headers={"x-amz-target": "Logs_20140328.PutLogEvents"})
        assert route_to_service(req) == "logs"

    def test_events(self):
        req = _make_request(headers={"x-amz-target": "CloudWatchEvents.PutEvents"})
        assert route_to_service(req) == "events"

    def test_secretsmanager(self):
        req = _make_request(headers={"x-amz-target": "SecretManager.GetSecretValue"})
        assert route_to_service(req) == "secretsmanager"


class TestRouteFromAuthHeader:
    def test_s3(self):
        req = _make_request(
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=AKID/20260305/us-east-1/s3/aws4_request, "
                    "SignedHeaders=host, Signature=abc123"
                )
            }
        )
        assert route_to_service(req) == "s3"

    def test_sqs(self):
        req = _make_request(
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=AKID/20260305/us-east-1/sqs/aws4_request, "
                    "SignedHeaders=host, Signature=abc123"
                )
            }
        )
        assert route_to_service(req) == "sqs"


class TestRouteFromPath:
    def test_lambda_functions(self):
        req = _make_request(path="/2015-03-31/functions")
        assert route_to_service(req) == "lambda"

    def test_apigateway(self):
        req = _make_request(path="/restapis/abc123")
        assert route_to_service(req) == "apigateway"

    def test_route53(self):
        req = _make_request(path="/2013-04-01/hostedzone")
        assert route_to_service(req) == "route53"


class TestRouteFromHost:
    def test_s3_virtual_hosted(self):
        req = _make_request(headers={"host": "mybucket.s3.us-east-1.amazonaws.com"})
        assert route_to_service(req) == "s3"

    def test_s3_path_style(self):
        req = _make_request(headers={"host": "s3.us-east-1.amazonaws.com"})
        assert route_to_service(req) == "s3"


class TestV1PathDisambiguation:
    """Bug: /v1/tags and other /v1/ Batch patterns are too greedy.

    AppSync uses /v1/tags/<arn> for tagging. The Batch patterns like
    ^/v1/tags steal these requests. The router should use the auth header
    to disambiguate /v1/ paths shared between services.
    """

    def test_v1_tags_with_appsync_auth_routes_to_appsync(self):
        """AppSync tag requests should NOT be misrouted to Batch."""
        req = _make_request(
            path="/v1/tags/arn:aws:appsync:us-east-1:123456789012:apis/abc123",
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=AKID/20260305/us-east-1/appsync/aws4_request, "
                    "SignedHeaders=host, Signature=abc123"
                )
            },
        )
        assert route_to_service(req) == "appsync"

    def test_v1_tags_with_batch_auth_routes_to_batch(self):
        """Batch tag requests should still route to Batch."""
        req = _make_request(
            path="/v1/tags/arn:aws:batch:us-east-1:123456789012:job/abc123",
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=AKID/20260305/us-east-1/batch/aws4_request, "
                    "SignedHeaders=host, Signature=abc123"
                )
            },
        )
        assert route_to_service(req) == "batch"

    def test_v1_tags_without_auth_defaults_to_batch(self):
        """Without auth, /v1/tags should default to Batch (most common)."""
        req = _make_request(path="/v1/tags/some-arn")
        assert route_to_service(req) == "batch"

    def test_v1_list_with_batch_auth(self):
        """Batch /v1/list paths should still work."""
        req = _make_request(
            path="/v1/listjobs",
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=AKID/20260305/us-east-1/batch/aws4_request, "
                    "SignedHeaders=host, Signature=abc123"
                )
            },
        )
        assert route_to_service(req) == "batch"


class TestELBv1vsELBv2Disambiguation:
    """Bug: elasticloadbalancing alias always maps to elbv2.

    Both ELB Classic (API version 2012-06-01) and ELBv2 (API version
    2015-12-01) use 'elasticloadbalancing' as their signing service name.
    The router should use the Version query param to disambiguate.
    """

    def test_elb_classic_version_routes_to_elb(self):
        """ELB Classic requests (Version=2012-06-01) should route to elb."""
        req = _make_request(
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=AKID/20260305/us-east-1/elasticloadbalancing/aws4_request, "
                    "SignedHeaders=host, Signature=abc123"
                )
            },
            query_params={"Action": "DescribeLoadBalancers", "Version": "2012-06-01"},
        )
        assert route_to_service(req) == "elb"

    def test_elbv2_version_routes_to_elbv2(self):
        """ELBv2 requests (Version=2015-12-01) should route to elbv2."""
        req = _make_request(
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=AKID/20260305/us-east-1/elasticloadbalancing/aws4_request, "
                    "SignedHeaders=host, Signature=abc123"
                )
            },
            query_params={"Action": "DescribeLoadBalancers", "Version": "2015-12-01"},
        )
        assert route_to_service(req) == "elbv2"

    def test_elb_no_version_defaults_to_elbv2(self):
        """Without Version param, elasticloadbalancing defaults to elbv2."""
        req = _make_request(
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=AKID/20260305/us-east-1/elasticloadbalancing/aws4_request, "
                    "SignedHeaders=host, Signature=abc123"
                )
            },
        )
        assert route_to_service(req) == "elbv2"


class TestV2ApisDisambiguation:
    """The /v2/apis path is shared by AppSync and API Gateway v2.

    Disambiguation relies on the auth header's credential scope containing
    'appsync'. Edge cases need to be tested.
    """

    def test_v2_apis_with_appsync_auth(self):
        req = _make_request(
            path="/v2/apis",
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=AKID/20260305/us-east-1/appsync/aws4_request, "
                    "SignedHeaders=host, Signature=abc123"
                )
            },
        )
        assert route_to_service(req) == "appsync"

    def test_v2_apis_with_apigateway_auth(self):
        req = _make_request(
            path="/v2/apis",
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=AKID/20260305/us-east-1/apigateway/aws4_request, "
                    "SignedHeaders=host, Signature=abc123"
                )
            },
        )
        assert route_to_service(req) == "apigatewayv2"

    def test_v2_apis_with_no_auth_defaults_to_apigatewayv2(self):
        req = _make_request(path="/v2/apis")
        assert route_to_service(req) == "apigatewayv2"

    def test_v2_apis_subpath_with_appsync_auth(self):
        """Subpaths like /v2/apis/<id>/channelNamespaces should also disambiguate."""
        req = _make_request(
            path="/v2/apis/abc123/channelNamespaces",
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=AKID/20260305/us-east-1/appsync/aws4_request, "
                    "SignedHeaders=host, Signature=abc123"
                )
            },
        )
        assert route_to_service(req) == "appsync"

    def test_v2_apis_presigned_url_with_appsync_credential(self):
        """Presigned URLs use X-Amz-Credential query param, not auth header."""
        req = _make_request(
            path="/v2/apis",
            query_params={
                "X-Amz-Credential": "AKID/20260305/us-east-1/appsync/aws4_request",
            },
        )
        # Currently this would default to apigatewayv2 because the disambiguation
        # only checks the Authorization header, not query params.
        # This test documents the current behavior.
        assert route_to_service(req) == "apigatewayv2"


class TestTimestreamDisambiguation:
    """Timestream Write and Query share the same X-Amz-Target prefix."""

    def test_timestream_write_op(self):
        req = _make_request(headers={"x-amz-target": "Timestream_20181101.WriteRecords"})
        assert route_to_service(req) == "timestreamwrite"

    def test_timestream_query_op(self):
        req = _make_request(headers={"x-amz-target": "Timestream_20181101.Query"})
        assert route_to_service(req) == "timestreamquery"

    def test_timestream_describe_account_settings_is_query(self):
        req = _make_request(headers={"x-amz-target": "Timestream_20181101.DescribeAccountSettings"})
        assert route_to_service(req) == "timestreamquery"


class TestPresignedURLRouting:
    def test_sigv4_presigned_url(self):
        req = _make_request(
            path="/my-bucket/my-key",
            query_params={
                "X-Amz-Credential": "AKID/20260305/us-east-1/s3/aws4_request",
            },
        )
        assert route_to_service(req) == "s3"

    def test_sigv4_presigned_url_with_alias(self):
        """Service aliases should work for presigned URLs too."""
        req = _make_request(
            path="/",
            query_params={
                "X-Amz-Credential": "AKID/20260305/us-east-1/monitoring/aws4_request",
            },
        )
        assert route_to_service(req) == "cloudwatch"

    def test_sigv2_presigned_url(self):
        req = _make_request(
            path="/my-bucket/my-key",
            query_params={
                "AWSAccessKeyId": "AKID",
                "Signature": "abc123",
            },
        )
        assert route_to_service(req) == "s3"


class TestFormUrlEncodedFallback:
    """Unsigned form-urlencoded requests should route to STS."""

    def test_sts_unsigned_request(self):
        req = _make_request(
            headers={"content-type": "application/x-www-form-urlencoded"},
        )
        assert route_to_service(req) == "sts"

    def test_form_urlencoded_with_auth_does_not_default_to_sts(self):
        """If auth header is present, don't default to STS."""
        req = _make_request(
            headers={
                "content-type": "application/x-www-form-urlencoded",
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=AKID/20260305/us-east-1/sqs/aws4_request, "
                    "SignedHeaders=host, Signature=abc123"
                ),
            },
        )
        assert route_to_service(req) == "sqs"


class TestServiceNameAliases:
    """Auth-based routing should resolve signing name aliases."""

    def test_monitoring_alias(self):
        req = _make_request(
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=AKID/20260305/us-east-1/monitoring/aws4_request, "
                    "SignedHeaders=host, Signature=abc123"
                )
            },
        )
        assert route_to_service(req) == "cloudwatch"

    def test_states_alias(self):
        req = _make_request(
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=AKID/20260305/us-east-1/states/aws4_request, "
                    "SignedHeaders=host, Signature=abc123"
                )
            },
        )
        assert route_to_service(req) == "stepfunctions"

    def test_elasticloadbalancing_alias(self):
        """Default (no Version param) should resolve to elbv2."""
        req = _make_request(
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=AKID/20260305/us-east-1/elasticloadbalancing/aws4_request, "
                    "SignedHeaders=host, Signature=abc123"
                )
            },
        )
        assert route_to_service(req) == "elbv2"


class TestUnknownService:
    def test_returns_none(self):
        req = _make_request(path="/unknown")
        assert route_to_service(req) is None
