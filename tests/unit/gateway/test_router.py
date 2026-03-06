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


class TestUnknownService:
    def test_returns_none(self):
        req = _make_request(path="/unknown")
        assert route_to_service(req) is None
