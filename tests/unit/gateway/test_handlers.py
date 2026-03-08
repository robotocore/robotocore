"""Tests for individual request/response handlers."""

import json
from unittest.mock import MagicMock

import pytest
from starlette.responses import Response

from robotocore.gateway.handler_chain import RequestContext
from robotocore.gateway.handlers import (
    cors_handler,
    cors_response_handler,
    error_normalizer,
    populate_context_handler,
)


def _make_context(
    service_name: str = "sts",
    method: str = "POST",
    path: str = "/",
    headers: dict | None = None,
    query_params: dict | None = None,
) -> RequestContext:
    request = MagicMock()
    request.method = method
    request.url.path = path
    request.headers = headers or {}
    request.query_params = query_params or {}
    return RequestContext(request=request, service_name=service_name)


class TestPopulateContextHandler:
    def test_extracts_region(self):
        ctx = _make_context(
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=AKID/20260305/ap-southeast-1/sts/aws4_request, "
                    "SignedHeaders=host, Signature=abc"
                )
            }
        )
        populate_context_handler(ctx)
        assert ctx.region == "ap-southeast-1"

    def test_default_region(self):
        ctx = _make_context()
        populate_context_handler(ctx)
        assert ctx.region == "us-east-1"

    def test_detects_protocol_for_sts(self):
        ctx = _make_context(service_name="sts")
        populate_context_handler(ctx)
        assert ctx.protocol == "query"

    def test_detects_protocol_for_dynamodb(self):
        ctx = _make_context(service_name="dynamodb")
        populate_context_handler(ctx)
        assert ctx.protocol == "json"

    def test_detects_protocol_for_s3(self):
        ctx = _make_context(service_name="s3")
        populate_context_handler(ctx)
        assert ctx.protocol == "rest-xml"

    def test_detects_protocol_for_lambda(self):
        ctx = _make_context(service_name="lambda")
        populate_context_handler(ctx)
        assert ctx.protocol == "rest-json"

    def test_detects_protocol_for_ec2(self):
        ctx = _make_context(service_name="ec2")
        populate_context_handler(ctx)
        assert ctx.protocol == "ec2"

    def test_operation_from_x_amz_target(self):
        ctx = _make_context(headers={"x-amz-target": "DynamoDB_20120810.GetItem"})
        populate_context_handler(ctx)
        assert ctx.operation == "GetItem"

    def test_operation_from_action_param(self):
        ctx = _make_context(query_params={"Action": "GetCallerIdentity"})
        populate_context_handler(ctx)
        assert ctx.operation == "GetCallerIdentity"

    @pytest.mark.parametrize(
        "service,form_data,expected_op",
        [
            ("sts", "Action=GetCallerIdentity&Version=2011-06-15", "GetCallerIdentity"),
            ("sqs", "Action=SendMessage&QueueUrl=http://localhost/q&MessageBody=hi", "SendMessage"),
            ("iam", "Action=ListRoles&Version=2010-05-08", "ListRoles"),
            ("ec2", "Action=DescribeInstances&Version=2016-11-15", "DescribeInstances"),
            ("cloudformation", "Action=ListStacks&Version=2010-05-15", "ListStacks"),
            ("sns", "Action=ListTopics", "ListTopics"),
            (
                "autoscaling",
                "Action=DescribeAutoScalingGroups&Version=2011-01-01",
                "DescribeAutoScalingGroups",
            ),
            ("elb", "Action=DescribeLoadBalancers&Version=2012-06-01", "DescribeLoadBalancers"),
        ],
    )
    def test_operation_from_form_body(self, service, form_data, expected_op):
        """Query-protocol services send Action in the POST form body, not the
        URL query string. The handler must parse the form body to extract the
        operation name, otherwise audit logs and diagnostics show operation=None."""
        from starlette.applications import Starlette
        from starlette.responses import PlainTextResponse
        from starlette.routing import Route
        from starlette.testclient import TestClient

        captured = {}

        async def handler(request):
            # Pre-read body so sync handler can access request._body
            await request.body()
            ctx = RequestContext(request=request, service_name=service)
            populate_context_handler(ctx)
            captured["operation"] = ctx.operation
            return PlainTextResponse("ok")

        app = Starlette(routes=[Route("/", handler, methods=["POST"])])
        client = TestClient(app)
        client.post(
            "/",
            data=form_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        assert captured["operation"] == expected_op

    def test_operation_target_takes_precedence(self):
        ctx = _make_context(
            headers={"x-amz-target": "TrentService.Encrypt"},
            query_params={"Action": "ShouldNotBeUsed"},
        )
        populate_context_handler(ctx)
        assert ctx.operation == "Encrypt"


class TestCorsHandler:
    def test_options_gets_response(self):
        ctx = _make_context(method="OPTIONS")
        cors_handler(ctx)
        assert ctx.response is not None
        assert ctx.response.status_code == 200
        assert "Access-Control-Allow-Origin" in ctx.response.headers

    def test_non_options_no_response(self):
        ctx = _make_context(method="POST")
        cors_handler(ctx)
        assert ctx.response is None


class TestCorsResponseHandler:
    def test_adds_cors_headers_to_response(self):
        ctx = _make_context()
        ctx.response = Response(status_code=200)
        cors_response_handler(ctx)
        assert ctx.response.headers["Access-Control-Allow-Origin"] == "*"

    def test_skips_if_no_response(self):
        ctx = _make_context()
        cors_response_handler(ctx)  # Should not raise


class TestAuditDoubleExecution:
    """Response handlers run twice for normal requests: once inside
    _handler_chain.handle() (where context.response is still None → status=0)
    and again in handle_aws_request() after the provider response is set.
    This produces phantom audit entries and double-counts every request."""

    def test_single_request_produces_one_audit_entry(self):
        """A single STS request should produce exactly one audit log entry."""
        from starlette.testclient import TestClient

        from robotocore.audit.log import get_audit_log
        from robotocore.gateway.app import app

        get_audit_log()._entries.clear()
        client = TestClient(app)

        client.post(
            "/",
            data="Action=GetCallerIdentity&Version=2011-06-15",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=testing/20260305/us-east-1/sts/aws4_request, "
                    "SignedHeaders=host, Signature=abc"
                ),
            },
        )

        entries = get_audit_log().recent(100)
        sts_entries = [e for e in entries if e.get("service") == "sts"]
        assert len(sts_entries) == 1, (
            f"Expected 1 audit entry per request, got {len(sts_entries)}: {sts_entries}"
        )

    def test_no_audit_entries_with_status_zero(self):
        """Audit entries should never have status_code=0 — that means
        the response handler ran before any response was set."""
        from starlette.testclient import TestClient

        from robotocore.audit.log import get_audit_log
        from robotocore.gateway.app import app

        get_audit_log()._entries.clear()
        client = TestClient(app)

        # Make requests to several services
        for svc in ("sts", "sqs", "iam"):
            client.post(
                "/",
                data="Action=ListQueues&Version=2012-11-05",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Authorization": (
                        f"AWS4-HMAC-SHA256 "
                        f"Credential=testing/20260305/us-east-1/{svc}/aws4_request, "
                        f"SignedHeaders=host, Signature=abc"
                    ),
                },
            )

        entries = get_audit_log().recent(100)
        zero_entries = [e for e in entries if e.get("status_code") == 0]
        assert len(zero_entries) == 0, (
            f"Found {len(zero_entries)} audit entries with status=0 (phantom entries): "
            f"{zero_entries}"
        )

    def test_three_requests_produce_three_audit_entries(self):
        """N requests should produce exactly N audit log entries, not 2N."""
        from starlette.testclient import TestClient

        from robotocore.audit.log import get_audit_log
        from robotocore.gateway.app import app

        get_audit_log()._entries.clear()
        client = TestClient(app)

        for _ in range(3):
            client.post(
                "/",
                data="Action=GetCallerIdentity&Version=2011-06-15",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Authorization": (
                        "AWS4-HMAC-SHA256 "
                        "Credential=testing/20260305/us-east-1/sts/aws4_request, "
                        "SignedHeaders=host, Signature=abc"
                    ),
                },
            )

        entries = get_audit_log().recent(100)
        sts_entries = [e for e in entries if e.get("service") == "sts"]
        assert len(sts_entries) == 3, (
            f"Expected 3 audit entries for 3 requests, got {len(sts_entries)}"
        )

    def test_audit_entry_has_real_status_code(self):
        """The audit entry for a successful request should have status 200, not 0."""
        from starlette.testclient import TestClient

        from robotocore.audit.log import get_audit_log
        from robotocore.gateway.app import app

        get_audit_log()._entries.clear()
        client = TestClient(app)

        resp = client.post(
            "/",
            data="Action=GetCallerIdentity&Version=2011-06-15",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=testing/20260305/us-east-1/sts/aws4_request, "
                    "SignedHeaders=host, Signature=abc"
                ),
            },
        )
        assert resp.status_code == 200

        entries = get_audit_log().recent(100)
        sts_entries = [e for e in entries if e.get("service") == "sts"]
        # Every entry should have the real status code
        for entry in sts_entries:
            assert entry["status_code"] == 200, (
                f"Audit entry has status_code={entry['status_code']}, expected 200"
            )


class TestErrorNormalizer:
    def test_json_protocol_error(self):
        ctx = _make_context(service_name="dynamodb")
        ctx.protocol = "json"
        error_normalizer(ctx, ValueError("bad value"))
        assert ctx.response.status_code == 500
        body = json.loads(ctx.response.body.decode())
        assert body["__type"] == "ValueError"
        assert "bad value" in body["message"]

    def test_rest_json_protocol_error(self):
        ctx = _make_context(service_name="lambda")
        ctx.protocol = "rest-json"
        error_normalizer(ctx, RuntimeError("runtime fail"))
        assert ctx.response.status_code == 500
        body = json.loads(ctx.response.body.decode())
        assert body["__type"] == "RuntimeError"

    def test_query_protocol_error(self):
        ctx = _make_context(service_name="sts")
        ctx.protocol = "query"
        error_normalizer(ctx, ValueError("query fail"))
        assert ctx.response.status_code == 500
        assert b"<ErrorResponse>" in ctx.response.body

    def test_rest_xml_protocol_error(self):
        ctx = _make_context(service_name="s3")
        ctx.protocol = "rest-xml"
        error_normalizer(ctx, ValueError("xml fail"))
        assert ctx.response.status_code == 500
        # S3 uses bare <Error> root, not <ErrorResponse>
        assert b"<Error>" in ctx.response.body
        assert b"<ErrorResponse>" not in ctx.response.body

    def test_default_protocol_is_xml(self):
        ctx = _make_context()
        ctx.protocol = None
        error_normalizer(ctx, ValueError("default"))
        assert b"<ErrorResponse>" in ctx.response.body

    def test_json_11_services_get_correct_content_type(self):
        """Bug fix 1D: Kinesis, Logs, ECS use JSON 1.1, not 1.0."""
        for service in ("kinesis", "logs", "ecs"):
            ctx = _make_context(service_name=service)
            ctx.protocol = "json"
            error_normalizer(ctx, ValueError("test"))
            ct = ctx.response.headers.get("content-type", "")
            assert "1.1" in ct, f"{service} should use x-amz-json-1.1, got {ct}"

    def test_json_10_services_get_correct_content_type(self):
        """Bug fix 1D: DynamoDB uses JSON 1.0."""
        ctx = _make_context(service_name="dynamodb")
        ctx.protocol = "json"
        error_normalizer(ctx, ValueError("test"))
        ct = ctx.response.headers.get("content-type", "")
        assert "1.0" in ct, f"dynamodb should use x-amz-json-1.0, got {ct}"

    def test_xml_escaping_in_error_message(self):
        """Bug fix 1E: XML special chars in exception messages must be escaped."""
        ctx = _make_context(service_name="s3")
        ctx.protocol = "rest-xml"
        error_normalizer(ctx, ValueError('<script>alert("xss")</script>'))
        body = ctx.response.body.decode()
        assert "<script>" not in body
        assert "&lt;script&gt;" in body
        # Verify the XML is well-formed
        import xml.etree.ElementTree as ET

        ET.fromstring(body)
