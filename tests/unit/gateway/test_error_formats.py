"""Tests for error response format consistency across protocols.

Phase 2C: Verify error responses use the correct format per protocol:
- JSON: {"__type": "ErrorCode", "message": "..."}
- query XML: <ErrorResponse><Error><Code>...</Code><Message>...</Message></Error></ErrorResponse>
- EC2: <Response><Errors><Error><Code>...</Code><Message>...</Message></Error></Errors></Response>
- S3: <Error><Code>...</Code><Message>...</Message></Error>
"""

import json
import xml.etree.ElementTree as ET
from unittest.mock import MagicMock

from robotocore.gateway.handler_chain import RequestContext
from robotocore.gateway.handlers import error_normalizer


def _make_context(service_name: str, protocol: str) -> RequestContext:
    request = MagicMock()
    request.method = "POST"
    request.url.path = "/"
    request.headers = {}
    request.query_params = {}
    ctx = RequestContext(request=request, service_name=service_name)
    ctx.protocol = protocol
    return ctx


class TestJsonErrorFormat:
    """JSON protocol services should return {"__type": "...", "message": "..."}."""

    def test_dynamodb_json_error(self):
        ctx = _make_context("dynamodb", "json")
        error_normalizer(ctx, ValueError("bad value"))
        body = json.loads(ctx.response.body.decode())
        assert "__type" in body
        assert "message" in body
        assert body["__type"] == "ValueError"
        assert body["message"] == "bad value"

    def test_kinesis_json_error(self):
        ctx = _make_context("kinesis", "json")
        error_normalizer(ctx, RuntimeError("stream error"))
        body = json.loads(ctx.response.body.decode())
        assert "__type" in body
        assert "message" in body

    def test_events_json_error(self):
        ctx = _make_context("events", "json")
        error_normalizer(ctx, Exception("events error"))
        body = json.loads(ctx.response.body.decode())
        assert "__type" in body

    def test_stepfunctions_json_error(self):
        ctx = _make_context("stepfunctions", "json")
        error_normalizer(ctx, Exception("sfn error"))
        body = json.loads(ctx.response.body.decode())
        assert "__type" in body


class TestRestJsonErrorFormat:
    """REST-JSON services should also return {"__type": "...", "message": "..."}."""

    def test_lambda_rest_json_error(self):
        ctx = _make_context("lambda", "rest-json")
        error_normalizer(ctx, ValueError("lambda error"))
        body = json.loads(ctx.response.body.decode())
        assert "__type" in body
        assert "message" in body

    def test_apigatewayv2_rest_json_error(self):
        ctx = _make_context("apigatewayv2", "rest-json")
        error_normalizer(ctx, ValueError("apigw error"))
        body = json.loads(ctx.response.body.decode())
        assert "__type" in body


class TestQueryXmlErrorFormat:
    """Query protocol services should return XML ErrorResponse."""

    def test_sts_query_error(self):
        ctx = _make_context("sts", "query")
        error_normalizer(ctx, ValueError("sts error"))
        body = ctx.response.body.decode()
        root = ET.fromstring(body)
        assert root.tag == "ErrorResponse"
        error_elem = root.find("Error")
        assert error_elem is not None
        assert error_elem.find("Code") is not None
        assert error_elem.find("Message") is not None
        assert error_elem.find("Code").text == "ValueError"

    def test_sqs_query_error(self):
        # SQS now uses JSON protocol in newer boto3, but query path still works
        ctx = _make_context("sqs", "query")
        error_normalizer(ctx, ValueError("sqs error"))
        body = ctx.response.body.decode()
        root = ET.fromstring(body)
        assert root.tag == "ErrorResponse"

    def test_sns_query_error(self):
        ctx = _make_context("sns", "query")
        error_normalizer(ctx, ValueError("sns error"))
        body = ctx.response.body.decode()
        root = ET.fromstring(body)
        assert root.tag == "ErrorResponse"


class TestRestXmlErrorFormat:
    """REST-XML services should return XML ErrorResponse."""

    def test_s3_rest_xml_error(self):
        ctx = _make_context("s3", "rest-xml")
        error_normalizer(ctx, ValueError("s3 error"))
        body = ctx.response.body.decode()
        root = ET.fromstring(body)
        # S3 uses bare <Error> root, not <ErrorResponse>
        assert root.tag == "Error"
        assert root.find("Code") is not None

    def test_route53_rest_xml_error(self):
        ctx = _make_context("route53", "rest-xml")
        error_normalizer(ctx, ValueError("route53 error"))
        body = ctx.response.body.decode()
        root = ET.fromstring(body)
        assert root.tag == "ErrorResponse"


class TestEc2ProtocolErrorFormat:
    """EC2 protocol should return <Response><Errors><Error> format."""

    def test_ec2_error(self):
        ctx = _make_context("ec2", "ec2")
        error_normalizer(ctx, ValueError("ec2 error"))
        body = ctx.response.body.decode()
        root = ET.fromstring(body)
        assert root.tag == "Response"
        errors_elem = root.find("Errors")
        assert errors_elem is not None
        error_elem = errors_elem.find("Error")
        assert error_elem is not None
        assert error_elem.find("Code") is not None
        assert error_elem.find("Message") is not None


class TestNotImplementedErrors:
    """NotImplementedError should use 501 status code, not 500."""

    def test_not_implemented_json_uses_501(self):
        ctx = _make_context("dynamodb", "json")
        error_normalizer(ctx, NotImplementedError("not yet"))
        assert ctx.response.status_code == 501
        body = json.loads(ctx.response.body.decode())
        assert body["__type"] == "NotImplemented"
        assert body["message"] == "not yet"

    def test_not_implemented_xml_uses_501(self):
        ctx = _make_context("s3", "rest-xml")
        error_normalizer(ctx, NotImplementedError("not yet"))
        assert ctx.response.status_code == 501
        root = ET.fromstring(ctx.response.body.decode())
        # S3 uses bare <Error> root
        assert root.find("Code").text == "NotImplemented"

    def test_regular_error_uses_500(self):
        ctx = _make_context("dynamodb", "json")
        error_normalizer(ctx, RuntimeError("crash"))
        assert ctx.response.status_code == 500
        body = json.loads(ctx.response.body.decode())
        assert body["__type"] == "RuntimeError"

    def test_regular_error_xml_uses_500(self):
        ctx = _make_context("sts", "query")
        error_normalizer(ctx, RuntimeError("crash"))
        assert ctx.response.status_code == 500
        root = ET.fromstring(ctx.response.body.decode())
        assert root.find("Error/Code").text == "RuntimeError"


class TestSpecialCharactersInErrors:
    """Error messages with special characters must be safe in all protocols."""

    def test_xml_special_chars(self):
        ctx = _make_context("s3", "rest-xml")
        error_normalizer(ctx, ValueError('key "foo" has <invalid> chars & more'))
        body = ctx.response.body.decode()
        # Raw XML must have escaped chars
        assert "&lt;invalid&gt;" in body
        assert "&amp;" in body
        # Must be valid XML (parseable)
        root = ET.fromstring(body)
        # After parsing, ET decodes the entities back
        # S3 uses bare <Error> root, so Message is a direct child
        msg = root.find("Message").text
        assert "<invalid>" in msg  # ET decodes entities

    def test_json_special_chars(self):
        ctx = _make_context("dynamodb", "json")
        error_normalizer(ctx, ValueError('key "foo" has <invalid> chars'))
        body = json.loads(ctx.response.body.decode())
        # JSON handles these natively
        assert "<invalid>" in body["message"]
