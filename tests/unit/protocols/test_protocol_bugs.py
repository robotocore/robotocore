"""Failing tests for protocol parsing/serialization bugs.

Each test documents a specific correctness bug in the protocol layer.
These tests are expected to FAIL until the bugs are fixed.
"""

import json
import xml.etree.ElementTree as ET
from unittest.mock import MagicMock

from robotocore.gateway.handler_chain import RequestContext
from robotocore.gateway.handlers import error_normalizer


def _make_context(service_name: str, protocol: str | None = None) -> RequestContext:
    request = MagicMock()
    request.method = "POST"
    request.url.path = "/"
    request.headers = {}
    request.query_params = {}
    ctx = RequestContext(request=request, service_name=service_name)
    if protocol is not None:
        ctx.protocol = protocol
    return ctx


class TestEc2ErrorFormatBug:
    """BUG: EC2 protocol errors use <ErrorResponse> but AWS EC2 uses <Response><Errors><Error>.

    The EC2 API error format is documented at:
    https://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html

    EC2 errors look like:
        <Response>
          <Errors>
            <Error>
              <Code>...</Code>
              <Message>...</Message>
            </Error>
          </Errors>
          <RequestId>...</RequestId>
        </Response>

    But error_normalizer produces:
        <ErrorResponse>
          <Error>
            <Code>...</Code>
            <Message>...</Message>
          </Error>
        </ErrorResponse>
    """

    def test_ec2_error_uses_response_errors_wrapper(self):
        ctx = _make_context("ec2", "ec2")
        error_normalizer(ctx, ValueError("ec2 error"))
        body = ctx.response.body.decode()
        root = ET.fromstring(body)
        # EC2 errors should use <Response> root with <Errors> wrapper
        assert root.tag == "Response", (
            f"EC2 errors must use <Response> root, got <{root.tag}>. "
            f"AWS EC2 API uses <Response><Errors><Error> format."
        )
        errors_elem = root.find("Errors")
        assert errors_elem is not None, "EC2 errors must have <Errors> wrapper element"
        error_elem = errors_elem.find("Error")
        assert error_elem is not None
        assert error_elem.find("Code") is not None
        assert error_elem.find("Message") is not None


class TestS3ErrorFormatBug:
    """BUG: S3 REST-XML errors use <ErrorResponse> but AWS S3 uses bare <Error>.

    The S3 API error format is documented at:
    https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html

    S3 errors look like:
        <Error>
          <Code>NoSuchKey</Code>
          <Message>The specified key does not exist.</Message>
          <Key>example-key</Key>
          <RequestId>...</RequestId>
        </Error>

    But error_normalizer produces:
        <ErrorResponse>
          <Error>
            <Code>...</Code>
            <Message>...</Message>
          </Error>
        </ErrorResponse>
    """

    def test_s3_error_uses_bare_error_root(self):
        ctx = _make_context("s3", "rest-xml")
        error_normalizer(ctx, ValueError("s3 error"))
        body = ctx.response.body.decode()
        root = ET.fromstring(body)
        # S3 errors should use <Error> as root, not <ErrorResponse>
        assert root.tag == "Error", (
            f"S3 errors must use <Error> root element, got <{root.tag}>. "
            f"AWS S3 API returns bare <Error> without an <ErrorResponse> wrapper."
        )
        assert root.find("Code") is not None
        assert root.find("Message") is not None


class TestCloudWatchProtocolBug:
    """BUG: CloudWatch protocol is now 'smithy-rpc-v2-cbor' in botocore but
    error_normalizer only matches 'json' and 'rest-json'.

    This means CloudWatch errors get XML format instead of JSON format,
    which breaks boto3 clients that expect JSON error responses.
    """

    def test_cloudwatch_error_is_json_not_xml(self):
        from robotocore.protocols.service_info import get_service_protocol

        protocol = get_service_protocol("cloudwatch")
        ctx = _make_context("cloudwatch", protocol)
        error_normalizer(ctx, ValueError("cw error"))
        body = ctx.response.body.decode()
        # CloudWatch errors should be JSON (it's a JSON-based service)
        parsed = json.loads(body)
        assert "__type" in parsed, (
            f"CloudWatch errors must be JSON with __type field, "
            f"but got XML-like response. Protocol was '{protocol}' which "
            f"doesn't match 'json' or 'rest-json' in error_normalizer."
        )
        assert parsed["__type"] == "ValueError"
        assert "cw error" in parsed["message"]


class TestMotoBridgeErrorContentTypeBug:
    """BUG: forward_to_moto always returns XML error responses even for JSON services.

    When Moto raises an exception for a JSON-protocol service (DynamoDB, Kinesis, etc.),
    the error response is wrapped in XML (<ErrorResponse>) with media_type='application/xml'.

    A boto3 client talking to DynamoDB expects JSON errors like:
        {"__type": "InternalError", "message": "..."}

    But gets:
        <ErrorResponse><Error><Code>InternalError</Code>...</Error></ErrorResponse>

    This causes boto3 to fail parsing the error response.
    """

    def test_moto_bridge_error_format_should_be_protocol_aware(self):
        """forward_to_moto error responses should use JSON format for JSON services."""
        import inspect

        from robotocore.providers.moto_bridge import forward_to_moto

        source = inspect.getsource(forward_to_moto)
        # The function should produce JSON errors for JSON services
        assert "application/x-amz-json" in source, (
            "forward_to_moto must produce JSON-format errors for JSON-protocol services "
            "(DynamoDB, Kinesis, etc.). Currently all error paths produce XML "
            "(<ErrorResponse>) which breaks boto3 clients expecting JSON error responses."
        )


class TestSqsXmlEscapingBug:
    """BUG: SQS _xml_response / dict_to_xml doesn't XML-escape values.

    If a message body or queue name contains XML special characters like
    < > & " ', the XML response will be malformed/invalid.
    """

    def test_sqs_xml_response_escapes_special_chars(self):
        from robotocore.services.sqs.provider import _xml_response

        # A response containing XML-special characters
        data = {"MessageBody": '<script>alert("xss")</script>'}
        response = _xml_response("SendMessageResponse", data)
        body = response.body.decode()
        # The raw XML must have escaped the angle brackets
        assert "<script>" not in body or "&lt;script&gt;" in body, (
            "SQS XML response must escape XML special characters in values. "
            f"Got unescaped XML: {body[:200]}"
        )
        # The XML must be parseable
        try:
            ET.fromstring(body)
        except ET.ParseError as e:
            raise AssertionError(
                f"SQS XML response with special chars produces invalid XML: {e}"
            ) from e


class TestEc2ProviderXmlEscapingBug:
    """BUG: EC2 provider error responses don't XML-escape the exception message.

    If an exception message contains < > & characters, the XML response
    will be malformed. The f-string interpolation doesn't escape.
    """

    def test_ec2_error_escapes_special_chars(self):
        import inspect

        from robotocore.services.ec2.provider import handle_ec2_request

        source = inspect.getsource(handle_ec2_request)
        # The error handling uses f-string with {e} directly in XML
        # This doesn't escape XML special characters
        assert "xml_escape" in source or "escape" in source or "cgi.escape" in source, (
            "EC2 provider error handling must XML-escape exception messages. "
            "Currently uses raw f-string interpolation like <Message>{e}</Message> "
            "which produces invalid XML for messages containing < > & characters."
        )
