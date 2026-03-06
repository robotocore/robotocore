"""Tests for individual request/response handlers."""

import json
from unittest.mock import MagicMock

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
        assert b"<ErrorResponse>" in ctx.response.body

    def test_default_protocol_is_xml(self):
        ctx = _make_context()
        ctx.protocol = None
        error_normalizer(ctx, ValueError("default"))
        assert b"<ErrorResponse>" in ctx.response.body
