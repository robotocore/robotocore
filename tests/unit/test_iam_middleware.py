"""Unit tests for the IAM enforcement middleware."""

from unittest.mock import MagicMock

from robotocore.gateway.iam_middleware import (
    _SERVICE_ACTION_PREFIX,
    build_iam_action,
    build_resource_arn,
    clear_sts_sessions,
    extract_credentials,
    register_sts_session,
)


class TestExtractCredentials:
    def test_sigv4_authorization_header(self):
        request = MagicMock()
        request.headers = {
            "authorization": (
                "AWS4-HMAC-SHA256 "
                "Credential=AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request, "
                "SignedHeaders=host;x-amz-date, Signature=abc123"
            )
        }
        creds = extract_credentials(request)
        assert creds is not None
        assert creds["access_key_id"] == "AKIAIOSFODNN7EXAMPLE"
        assert creds["date"] == "20230101"
        assert creds["region"] == "us-east-1"
        assert creds["service"] == "s3"

    def test_presigned_url_query_params(self):
        request = MagicMock()
        request.headers = {}
        request.query_params = {
            "X-Amz-Credential": "AKIAIOSFODNN7EXAMPLE/20230101/eu-west-1/s3/aws4_request"
        }
        creds = extract_credentials(request)
        assert creds is not None
        assert creds["access_key_id"] == "AKIAIOSFODNN7EXAMPLE"
        assert creds["region"] == "eu-west-1"

    def test_no_credentials(self):
        request = MagicMock()
        request.headers = {}
        request.query_params = {}
        assert extract_credentials(request) is None

    def test_empty_authorization(self):
        request = MagicMock()
        request.headers = {"authorization": ""}
        request.query_params = {}
        assert extract_credentials(request) is None

    def test_no_headers_attribute(self):
        request = MagicMock(spec=[])
        assert extract_credentials(request) is None


class TestBuildIamAction:
    def test_known_service(self):
        assert build_iam_action("s3", "PutObject") == "s3:PutObject"
        assert build_iam_action("dynamodb", "PutItem") == "dynamodb:PutItem"
        assert build_iam_action("monitoring", "PutMetricData") == "cloudwatch:PutMetricData"
        assert build_iam_action("states", "StartExecution") == "states:StartExecution"

    def test_unknown_service_uses_name(self):
        assert build_iam_action("custom-service", "DoThing") == "custom-service:DoThing"

    def test_no_operation(self):
        assert build_iam_action("s3", None) == "s3:*"

    def test_service_action_prefix_map_coverage(self):
        # Verify all mapped services produce correct prefixes
        for signing_name, prefix in _SERVICE_ACTION_PREFIX.items():
            action = build_iam_action(signing_name, "Test")
            assert action == f"{prefix}:Test"


class TestBuildResourceArn:
    def test_s3_bucket_only(self):
        request = MagicMock()
        request.url.path = "/my-bucket"
        request.query_params = {}
        arn = build_resource_arn("s3", "us-east-1", "123456789012", request)
        assert arn == "arn:aws:s3:::my-bucket"

    def test_s3_bucket_and_key(self):
        request = MagicMock()
        request.url.path = "/my-bucket/path/to/key.txt"
        request.query_params = {}
        arn = build_resource_arn("s3", "us-east-1", "123456789012", request)
        assert arn == "arn:aws:s3:::my-bucket/path/to/key.txt"

    def test_sqs_from_queue_url(self):
        request = MagicMock()
        request.url.path = "/"
        request.query_params = {"QueueUrl": "http://localhost:4566/123456789012/my-queue"}
        arn = build_resource_arn("sqs", "us-east-1", "123456789012", request)
        assert arn == "arn:aws:sqs:us-east-1:123456789012:my-queue"

    def test_sqs_from_path(self):
        request = MagicMock()
        request.url.path = "/123456789012/my-queue"
        request.query_params = {}
        arn = build_resource_arn("sqs", "us-east-1", "123456789012", request)
        assert arn == "arn:aws:sqs:us-east-1:123456789012:my-queue"

    def test_sns_from_topic_arn(self):
        request = MagicMock()
        request.url.path = "/"
        request.query_params = {"TopicArn": "arn:aws:sns:us-east-1:123456789012:my-topic"}
        arn = build_resource_arn("sns", "us-east-1", "123456789012", request)
        assert arn == "arn:aws:sns:us-east-1:123456789012:my-topic"

    def test_lambda_from_path(self):
        request = MagicMock()
        request.url.path = "/2015-03-31/functions/my-function/invocations"
        request.query_params = {}
        arn = build_resource_arn("lambda", "us-east-1", "123456789012", request)
        assert arn == "arn:aws:lambda:us-east-1:123456789012:function:my-function"

    def test_dynamodb_wildcard(self):
        request = MagicMock()
        request.url.path = "/"
        request.query_params = {}
        arn = build_resource_arn("dynamodb", "us-east-1", "123456789012", request)
        assert arn == "arn:aws:dynamodb:us-east-1:123456789012:table/*"

    def test_generic_fallback(self):
        request = MagicMock()
        request.url.path = "/"
        request.query_params = {}
        arn = build_resource_arn("custom", "eu-west-1", "999999999999", request)
        assert arn == "arn:aws:custom:eu-west-1:999999999999:*"


class TestStsSessionRegistration:
    def setup_method(self):
        clear_sts_sessions()

    def test_register_and_clear(self):
        register_sts_session("ASIA123", "arn:aws:iam::123:role/test", "123456789012")
        # Just verify no error; internal state tested via gather_policies integration
        clear_sts_sessions()
