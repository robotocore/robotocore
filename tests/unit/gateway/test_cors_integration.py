"""Semantic integration tests for CORS handling in the handler chain.

These tests exercise the cors_handler and cors_response_handler functions
with mock Starlette Request objects, verifying end-to-end behavior.
"""

import os
from unittest import mock

import pytest
from starlette.datastructures import Headers
from starlette.responses import Response

from robotocore.gateway.cors import reset_cors_config
from robotocore.gateway.handler_chain import RequestContext
from robotocore.gateway.handlers import cors_handler, cors_response_handler


@pytest.fixture(autouse=True)
def _reset_config():
    reset_cors_config()
    yield
    reset_cors_config()


def _make_request(method: str = "GET", headers: dict | None = None, path: str = "/"):
    """Build a minimal mock request for handler testing."""
    req = mock.MagicMock()
    req.method = method
    req.url = mock.MagicMock()
    req.url.path = path
    hdr = headers or {}
    req.headers = Headers(headers=hdr)
    req.query_params = {}
    return req


class TestEndToEndCORSHeaders:
    def test_request_with_origin_gets_cors_headers(self):
        request = _make_request(headers={"origin": "http://example.com"})
        context = RequestContext(request=request, service_name="sts")
        context.response = Response(status_code=200, content="ok")

        cors_response_handler(context)

        assert "Access-Control-Allow-Origin" in context.response.headers

    def test_options_preflight_returns_200(self):
        request = _make_request(method="OPTIONS", headers={"origin": "http://example.com"})
        context = RequestContext(request=request, service_name="sts")

        cors_handler(context)

        assert context.response is not None
        assert context.response.status_code == 200
        assert "Access-Control-Allow-Origin" in context.response.headers
        assert "Access-Control-Allow-Methods" in context.response.headers

    @mock.patch.dict(os.environ, {"DISABLE_CORS_HEADERS": "1"})
    def test_disable_cors_headers_no_cors_on_response(self):
        reset_cors_config()
        request = _make_request(headers={"origin": "http://example.com"})
        context = RequestContext(request=request, service_name="sts")
        context.response = Response(status_code=200, content="ok")

        cors_response_handler(context)

        assert "Access-Control-Allow-Origin" not in context.response.headers

    def test_s3_request_with_bucket_cors_applied(self):
        """When S3 bucket has CORS config, those headers are used."""
        request = _make_request(
            headers={"origin": "http://mybucket.example.com"},
            path="/my-bucket/key",
        )
        context = RequestContext(request=request, service_name="s3")
        context.response = Response(status_code=200, content="ok")

        # Set up bucket CORS
        from robotocore.services.s3.provider import set_bucket_cors

        set_bucket_cors(
            "my-bucket",
            [
                {
                    "AllowedOrigins": ["http://mybucket.example.com"],
                    "AllowedMethods": ["GET", "PUT"],
                    "AllowedHeaders": ["*"],
                    "ExposeHeaders": ["ETag"],
                    "MaxAgeSeconds": 1800,
                }
            ],
        )

        try:
            cors_response_handler(context)

            assert (
                context.response.headers["Access-Control-Allow-Origin"]
                == "http://mybucket.example.com"
            )
            assert "ETag" in context.response.headers.get("Access-Control-Expose-Headers", "")
            assert context.response.headers.get("Access-Control-Max-Age") == "1800"
        finally:
            from robotocore.services.s3.provider import delete_bucket_cors

            delete_bucket_cors("my-bucket")

    @mock.patch.dict(os.environ, {"DISABLE_CUSTOM_CORS_S3": "1"})
    def test_disable_custom_cors_s3_uses_default(self):
        """When DISABLE_CUSTOM_CORS_S3=1, bucket CORS is ignored."""
        reset_cors_config()
        request = _make_request(
            headers={"origin": "http://mybucket.example.com"},
            path="/my-bucket/key",
        )
        context = RequestContext(request=request, service_name="s3")
        context.response = Response(status_code=200, content="ok")

        from robotocore.services.s3.provider import set_bucket_cors

        set_bucket_cors(
            "my-bucket",
            [
                {
                    "AllowedOrigins": ["http://mybucket.example.com"],
                    "AllowedMethods": ["GET"],
                    "MaxAgeSeconds": 999,
                }
            ],
        )

        try:
            cors_response_handler(context)

            # Should get default wildcard, not bucket-specific origin
            assert context.response.headers["Access-Control-Allow-Origin"] == "*"
        finally:
            from robotocore.services.s3.provider import delete_bucket_cors

            delete_bucket_cors("my-bucket")

    def test_s3_bucket_without_cors_gets_default(self):
        """S3 request for bucket with no CORS config gets default headers."""
        request = _make_request(
            headers={"origin": "http://example.com"},
            path="/no-cors-bucket/key",
        )
        context = RequestContext(request=request, service_name="s3")
        context.response = Response(status_code=200, content="ok")

        cors_response_handler(context)

        # Should fall through to default CORS
        assert context.response.headers["Access-Control-Allow-Origin"] == "*"
