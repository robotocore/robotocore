"""Failing tests for gateway/routing edge cases.

Each test documents correct behavior that is currently missing or broken.
All tests in this file are expected to FAIL until the corresponding issue is fixed.
"""

from unittest.mock import MagicMock

from robotocore.gateway.app import _extract_account_id
from robotocore.gateway.cors import (
    CORSConfig,
    build_cors_headers,
)
from robotocore.gateway.handler_chain import RequestContext
from robotocore.gateway.router import route_to_service

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(
    path: str = "/",
    headers: dict | None = None,
    query_params: dict | None = None,
    method: str = "GET",
) -> MagicMock:
    """Create a mock Starlette Request."""
    req = MagicMock()
    req.url.path = path
    req.headers = headers or {}
    req.query_params = query_params or {}
    req.method = method
    return req


def _make_context(**kwargs) -> RequestContext:
    request = MagicMock()
    request.headers = kwargs.pop("headers", {})
    request.url.path = kwargs.pop("path", "/")
    request.query_params = kwargs.pop("query_params", {})
    request.method = kwargs.pop("method", "POST")
    request._body = kwargs.pop("body", b"")
    return RequestContext(
        request=request,
        service_name=kwargs.pop("service_name", "sts"),
        **kwargs,
    )


# ===========================================================================
# Router: malformed X-Amz-Target header
# ===========================================================================


class TestRouterMalformedXAmzTarget:
    """X-Amz-Target header edge cases that should be handled gracefully."""

    def test_x_amz_target_no_dot_returns_none(self):
        """An X-Amz-Target without a dot separator (just a bare prefix with
        no operation) should not match any service.

        Bug: The router splits on "." and takes [0], then looks up the whole
        string in TARGET_PREFIX_MAP. "DynamoDB" alone (no ".Operation")
        matches and returns "dynamodb", but this is a malformed header."""
        req = _make_request(headers={"x-amz-target": "DynamoDB"})
        result = route_to_service(req)
        assert result is None, (
            "A bare X-Amz-Target without '.Operation' suffix is malformed and should not route"
        )

    def test_x_amz_target_empty_operation(self):
        """X-Amz-Target with trailing dot but no operation name
        (e.g. 'DynamoDB_20120810.') is malformed.

        Bug: split(".")[0] extracts "DynamoDB_20120810", then
        split("_")[0] gives "DynamoDB" which matches TARGET_PREFIX_MAP.
        The empty operation string is silently accepted."""
        req = _make_request(headers={"x-amz-target": "DynamoDB_20120810."})
        result = route_to_service(req)
        assert result is None, "X-Amz-Target with empty operation (trailing dot) is malformed"

    def test_x_amz_target_multiple_dots(self):
        """X-Amz-Target with dotted prefixes like CloudTrail's full target:
        com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.LookupEvents

        Bug: split('.')[0] extracts only 'com' which doesn't match
        anything in TARGET_PREFIX_MAP, so the request falls through to
        path/auth routing and returns None. The correct behavior is to
        handle this known dotted-prefix pattern."""
        target = "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.LookupEvents"
        req = _make_request(headers={"x-amz-target": target})
        result = route_to_service(req)
        assert result == "cloudtrail", (
            "Dotted X-Amz-Target prefixes (CloudTrail) should route correctly"
        )


# ===========================================================================
# Router: SigV2 Authorization header
# ===========================================================================


class TestRouterSigV2AuthHeader:
    """SigV2-style Authorization header routing."""

    def test_sigv2_auth_header_for_s3_path(self):
        """A SigV2-style Authorization header (AWS AKID:signature) combined
        with an S3-like path should still route somewhere.

        Bug: The SigV2 Auth header doesn't match any regex, and the path
        /my-bucket/my-key doesn't match any PATH_PATTERN, and there's no
        Host header with s3 in it. So the request returns None even though
        a SigV2 auth header with an S3-style path is clearly an S3 request.
        The SigV2 query-string path (AWSAccessKeyId=) works, but the header
        path doesn't."""
        req = _make_request(
            path="/my-bucket/my-key",
            headers={"authorization": ("AWS AKIAIOSFODNN7EXAMPLE:frJIUN8DYpKDtOLCwoXXX=")},
        )
        result = route_to_service(req)
        assert result is not None, (
            "SigV2 Authorization header for S3 should still route to a service"
        )


# ===========================================================================
# Router: presigned URL empty service
# ===========================================================================


class TestRouterPresignedURLEmptyService:
    """Edge case with empty service in presigned URL credential."""

    def test_presigned_url_empty_service(self):
        """X-Amz-Credential with empty service field should return None.

        Bug: With credential "AKID/20260305/us-east-1//aws4_request",
        parts[3] is an empty string "". The router returns "" instead
        of None, and "" is not a valid service name."""
        req = _make_request(
            query_params={"X-Amz-Credential": "AKID/20260305/us-east-1//aws4_request"}
        )
        result = route_to_service(req)
        assert result is None, "Empty service name in X-Amz-Credential should return None"


# ===========================================================================
# Account ID extraction: numeric access key
# ===========================================================================


class TestAccountIdNumericAccessKey:
    """Account ID extraction from short numeric access keys."""

    def test_short_numeric_access_key_not_extracted_as_account(self):
        """If the Credential starts with a short numeric string like
        "12345", it should not be treated as an AWS account ID.

        Bug: _CREDENTIAL_RE is r"Credential=(\\d+)/" which matches any
        run of digits. "12345" is 5 digits but still matches, so
        _extract_account_id returns "12345" instead of the default
        "123456789012". AWS account IDs are always exactly 12 digits."""
        req = _make_request(
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 Credential=12345/20260305/us-east-1/s3/aws4_request"
                )
            }
        )
        result = _extract_account_id(req)
        assert result == "123456789012", (
            "Short numeric string in Credential should not be extracted as account ID"
        )


# ===========================================================================
# CORS: no Origin with specific allowed origins
# ===========================================================================


class TestCORSNoOriginWithAllowedOrigins:
    """CORS behavior when no Origin header is sent but specific
    origins are configured."""

    def test_no_origin_returns_no_cors_headers(self):
        """When specific origins are configured but no Origin header is
        sent, the correct behavior is to return no CORS headers.

        Bug: _resolve_origin() returns config.allowed_origins[0] when
        request_origin is None and allowed_origins is non-empty. This
        means a non-CORS request (no Origin header) gets CORS headers
        set to an arbitrary allowed origin, which is incorrect per the
        CORS spec. Only requests with an Origin header are CORS requests."""
        config = CORSConfig(
            allowed_origins=["https://allowed.com"],
            allowed_headers=[],
            expose_headers=[],
            allowed_methods=["GET"],
        )
        headers = build_cors_headers(config, request_origin=None)
        assert headers == {}, (
            "No Origin header in request should result in no CORS headers "
            "when specific origins are configured"
        )
