"""Unit tests for the CORS configuration and header building."""

import os
from unittest import mock

import pytest

from robotocore.gateway.cors import (
    CORSConfig,
    build_cors_headers,
    build_preflight_response,
    reset_cors_config,
)


@pytest.fixture(autouse=True)
def _reset_config():
    """Reset the singleton config before each test."""
    reset_cors_config()
    yield
    reset_cors_config()


# ---------------------------------------------------------------------------
# Default CORS config
# ---------------------------------------------------------------------------


class TestDefaultCORSConfig:
    def test_default_config_adds_all_headers(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config)
        assert "Access-Control-Allow-Origin" in headers
        assert "Access-Control-Allow-Methods" in headers
        assert "Access-Control-Allow-Headers" in headers
        assert "Access-Control-Expose-Headers" in headers
        assert "Access-Control-Max-Age" in headers

    def test_default_origin_is_wildcard(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config)
        assert headers["Access-Control-Allow-Origin"] == "*"

    def test_default_methods_include_standard_verbs(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config)
        methods = headers["Access-Control-Allow-Methods"]
        for verb in ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"]:
            assert verb in methods

    def test_default_headers_include_aws_headers(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config)
        allowed = headers["Access-Control-Allow-Headers"]
        assert "Authorization" in allowed
        assert "X-Amz-Target" in allowed
        assert "X-Amz-Date" in allowed

    def test_default_max_age(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config)
        assert headers["Access-Control-Max-Age"] == "86400"


# ---------------------------------------------------------------------------
# DISABLE_CORS_HEADERS
# ---------------------------------------------------------------------------


class TestDisableCORSHeaders:
    @mock.patch.dict(os.environ, {"DISABLE_CORS_HEADERS": "1"})
    def test_no_cors_headers_when_disabled(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config)
        assert headers == {}

    @mock.patch.dict(os.environ, {"DISABLE_CORS_HEADERS": "1"})
    def test_no_cors_headers_with_origin(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config, request_origin="http://example.com")
        assert headers == {}


# ---------------------------------------------------------------------------
# DISABLE_CORS_CHECKS
# ---------------------------------------------------------------------------


class TestDisableCORSChecks:
    @mock.patch.dict(
        os.environ,
        {"DISABLE_CORS_CHECKS": "1", "EXTRA_CORS_ALLOWED_ORIGINS": "http://allowed.com"},
    )
    def test_all_origins_accepted(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config, request_origin="http://not-in-list.com")
        assert headers["Access-Control-Allow-Origin"] == "http://not-in-list.com"


# ---------------------------------------------------------------------------
# EXTRA_CORS_ALLOWED_HEADERS
# ---------------------------------------------------------------------------


class TestExtraCORSAllowedHeaders:
    @mock.patch.dict(os.environ, {"EXTRA_CORS_ALLOWED_HEADERS": "X-Custom-One, X-Custom-Two"})
    def test_custom_headers_in_allowed(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config)
        allowed = headers["Access-Control-Allow-Headers"]
        assert "X-Custom-One" in allowed
        assert "X-Custom-Two" in allowed
        # Default headers still present
        assert "Authorization" in allowed


# ---------------------------------------------------------------------------
# EXTRA_CORS_EXPOSE_HEADERS
# ---------------------------------------------------------------------------


class TestExtraCORSExposeHeaders:
    @mock.patch.dict(os.environ, {"EXTRA_CORS_EXPOSE_HEADERS": "X-Exposed-One, X-Exposed-Two"})
    def test_custom_headers_in_exposed(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config)
        exposed = headers["Access-Control-Expose-Headers"]
        assert "X-Exposed-One" in exposed
        assert "X-Exposed-Two" in exposed
        # Default exposed headers still present
        assert "x-amz-request-id" in exposed


# ---------------------------------------------------------------------------
# EXTRA_CORS_ALLOWED_ORIGINS
# ---------------------------------------------------------------------------


class TestExtraCORSAllowedOrigins:
    @mock.patch.dict(
        os.environ,
        {"EXTRA_CORS_ALLOWED_ORIGINS": "http://allowed.com, http://other.com"},
    )
    def test_allowed_origin_reflected(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config, request_origin="http://allowed.com")
        assert headers["Access-Control-Allow-Origin"] == "http://allowed.com"

    @mock.patch.dict(
        os.environ,
        {"EXTRA_CORS_ALLOWED_ORIGINS": "http://allowed.com"},
    )
    def test_disallowed_origin_returns_no_headers(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config, request_origin="http://evil.com")
        assert headers == {}

    @mock.patch.dict(
        os.environ,
        {"EXTRA_CORS_ALLOWED_ORIGINS": "http://one.com, http://two.com, http://three.com"},
    )
    def test_multiple_origins_correct_one_reflected(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config, request_origin="http://two.com")
        assert headers["Access-Control-Allow-Origin"] == "http://two.com"


# ---------------------------------------------------------------------------
# Wildcard origin
# ---------------------------------------------------------------------------


class TestWildcardOrigin:
    def test_wildcard_returned_when_no_specific_origins(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config, request_origin="http://anything.com")
        assert headers["Access-Control-Allow-Origin"] == "*"

    @mock.patch.dict(os.environ, {"EXTRA_CORS_ALLOWED_ORIGINS": "*"})
    def test_explicit_wildcard_origin(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config, request_origin="http://anything.com")
        assert headers["Access-Control-Allow-Origin"] == "*"


# ---------------------------------------------------------------------------
# CORS_ALLOWED_METHODS
# ---------------------------------------------------------------------------


class TestCORSAllowedMethods:
    @mock.patch.dict(os.environ, {"CORS_ALLOWED_METHODS": "GET, POST"})
    def test_custom_methods(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config)
        assert headers["Access-Control-Allow-Methods"] == "GET, POST"
        assert "DELETE" not in headers["Access-Control-Allow-Methods"]


# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------


class TestPreflight:
    def test_preflight_response_includes_cors_headers(self):
        config = CORSConfig.from_environment()
        response = build_preflight_response(config)
        assert response is not None
        assert response.status_code == 200
        assert "Access-Control-Allow-Origin" in response.headers
        assert "Access-Control-Allow-Methods" in response.headers

    def test_preflight_response_includes_max_age(self):
        config = CORSConfig.from_environment()
        response = build_preflight_response(config)
        assert response is not None
        assert "Access-Control-Max-Age" in response.headers


# ---------------------------------------------------------------------------
# DISABLE_PREFLIGHT_PROCESSING
# ---------------------------------------------------------------------------


class TestDisablePreflightProcessing:
    @mock.patch.dict(os.environ, {"DISABLE_PREFLIGHT_PROCESSING": "1"})
    def test_options_not_handled(self):
        config = CORSConfig.from_environment()
        response = build_preflight_response(config)
        assert response is None


# ---------------------------------------------------------------------------
# Origin header validation
# ---------------------------------------------------------------------------


class TestOriginValidation:
    @mock.patch.dict(
        os.environ,
        {"EXTRA_CORS_ALLOWED_ORIGINS": "http://trusted.com"},
    )
    def test_allowed_origin_reflected(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config, request_origin="http://trusted.com")
        assert headers["Access-Control-Allow-Origin"] == "http://trusted.com"

    @mock.patch.dict(
        os.environ,
        {"EXTRA_CORS_ALLOWED_ORIGINS": "http://trusted.com"},
    )
    def test_disallowed_origin_no_cors_headers(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config, request_origin="http://evil.com")
        assert headers == {}

    @mock.patch.dict(
        os.environ,
        {"EXTRA_CORS_ALLOWED_ORIGINS": "http://trusted.com"},
    )
    def test_vary_header_set_for_specific_origin(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config, request_origin="http://trusted.com")
        assert headers.get("Vary") == "Origin"

    def test_no_vary_header_for_wildcard(self):
        config = CORSConfig.from_environment()
        headers = build_cors_headers(config)
        assert "Vary" not in headers
