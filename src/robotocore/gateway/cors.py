"""Configurable CORS handling for the gateway.

Environment variables:
    DISABLE_CORS_HEADERS        — disable ALL CORS headers entirely
    DISABLE_CORS_CHECKS         — accept all origins (don't validate Origin header)
    DISABLE_CUSTOM_CORS_S3      — disable S3-specific CORS (bucket CORS config)
    DISABLE_CUSTOM_CORS_APIGATEWAY — disable API Gateway CORS
    EXTRA_CORS_ALLOWED_HEADERS  — comma-separated additional allowed headers
    EXTRA_CORS_EXPOSE_HEADERS   — comma-separated additional exposed headers
    EXTRA_CORS_ALLOWED_ORIGINS  — comma-separated additional allowed origins
    DISABLE_PREFLIGHT_PROCESSING — don't handle OPTIONS preflight requests
    CORS_ALLOWED_METHODS        — override allowed methods
"""

from __future__ import annotations

import fnmatch
import os
from dataclasses import dataclass, field

from starlette.responses import Response

# Standard AWS request headers that should always be allowed
DEFAULT_ALLOWED_HEADERS = [
    "Authorization",
    "Content-Type",
    "Content-MD5",
    "Cache-Control",
    "X-Amz-Content-Sha256",
    "X-Amz-Date",
    "X-Amz-Security-Token",
    "X-Amz-Target",
    "X-Amz-User-Agent",
    "X-Amzn-Authorization",
    "x-localstack-tgt",
]

# Standard AWS response headers that should be exposed
DEFAULT_EXPOSE_HEADERS = [
    "x-amz-request-id",
    "x-amz-id-2",
    "x-amz-version-id",
    "x-amz-delete-marker",
    "ETag",
    "x-amz-server-side-encryption",
    "x-amzn-RequestId",
    "x-amz-bucket-region",
]

DEFAULT_ALLOWED_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"]

DEFAULT_MAX_AGE = "86400"


@dataclass
class CORSConfig:
    """CORS configuration loaded from environment variables."""

    disable_cors_headers: bool = False
    disable_cors_checks: bool = False
    disable_custom_cors_s3: bool = False
    disable_custom_cors_apigateway: bool = False
    disable_preflight_processing: bool = False
    allowed_headers: list[str] = field(default_factory=list)
    expose_headers: list[str] = field(default_factory=list)
    allowed_origins: list[str] = field(default_factory=list)
    allowed_methods: list[str] = field(default_factory=list)

    @classmethod
    def from_environment(cls) -> CORSConfig:
        """Build config from environment variables."""
        extra_headers = _parse_csv(os.environ.get("EXTRA_CORS_ALLOWED_HEADERS", ""))
        extra_expose = _parse_csv(os.environ.get("EXTRA_CORS_EXPOSE_HEADERS", ""))
        extra_origins = _parse_csv(os.environ.get("EXTRA_CORS_ALLOWED_ORIGINS", ""))
        methods_override = _parse_csv(os.environ.get("CORS_ALLOWED_METHODS", ""))

        return cls(
            disable_cors_headers=os.environ.get("DISABLE_CORS_HEADERS") == "1",
            disable_cors_checks=os.environ.get("DISABLE_CORS_CHECKS") == "1",
            disable_custom_cors_s3=os.environ.get("DISABLE_CUSTOM_CORS_S3") == "1",
            disable_custom_cors_apigateway=os.environ.get("DISABLE_CUSTOM_CORS_APIGATEWAY") == "1",
            disable_preflight_processing=os.environ.get("DISABLE_PREFLIGHT_PROCESSING") == "1",
            allowed_headers=list(DEFAULT_ALLOWED_HEADERS) + extra_headers,
            expose_headers=list(DEFAULT_EXPOSE_HEADERS) + extra_expose,
            allowed_origins=extra_origins,
            allowed_methods=methods_override or list(DEFAULT_ALLOWED_METHODS),
        )


# Singleton config — reloaded via get_cors_config()
_config: CORSConfig | None = None


def get_cors_config() -> CORSConfig:
    """Return the current CORS config (singleton, lazy-loaded)."""
    global _config
    if _config is None:
        _config = CORSConfig.from_environment()
    return _config


def reset_cors_config() -> None:
    """Reset the singleton so it reloads from env on next access."""
    global _config
    _config = None


def build_cors_headers(
    config: CORSConfig,
    request_origin: str | None = None,
) -> dict[str, str]:
    """Build CORS response headers based on config and request origin.

    Returns an empty dict if CORS headers are disabled.
    """
    if config.disable_cors_headers:
        return {}

    headers: dict[str, str] = {}

    # Determine the origin to return
    origin_value = _resolve_origin(config, request_origin)
    if origin_value is None:
        # Origin not allowed — return no CORS headers
        return {}

    headers["Access-Control-Allow-Origin"] = origin_value
    headers["Access-Control-Allow-Methods"] = ", ".join(config.allowed_methods)
    headers["Access-Control-Allow-Headers"] = ", ".join(config.allowed_headers)
    headers["Access-Control-Expose-Headers"] = ", ".join(config.expose_headers)
    headers["Access-Control-Max-Age"] = DEFAULT_MAX_AGE

    # If we reflected a specific origin, add Vary: Origin
    if origin_value != "*":
        headers["Vary"] = "Origin"

    return headers


def build_preflight_response(
    config: CORSConfig,
    request_origin: str | None = None,
) -> Response | None:
    """Build a preflight (OPTIONS) response, or None if preflight is disabled."""
    if config.disable_preflight_processing:
        return None

    cors_headers = build_cors_headers(config, request_origin)
    return Response(status_code=200, headers=cors_headers)


def build_s3_cors_headers(
    cors_rules: list[dict],
    request_origin: str | None,
    request_method: str | None = None,
    request_headers: str | None = None,
) -> dict[str, str]:
    """Apply S3 bucket CORS rules instead of default CORS.

    Args:
        cors_rules: List of S3 CORS rule dicts with keys like
            AllowedOrigins, AllowedMethods, AllowedHeaders, ExposeHeaders, MaxAgeSeconds.
        request_origin: The Origin header from the request.
        request_method: The Access-Control-Request-Method header (preflight).
        request_headers: The Access-Control-Request-Headers header (preflight).

    Returns:
        CORS headers dict if a matching rule is found, empty dict otherwise.
    """
    if not request_origin:
        return {}

    for rule in cors_rules:
        allowed_origins = rule.get("AllowedOrigins", [])
        allowed_methods = rule.get("AllowedMethods", [])
        allowed_headers = rule.get("AllowedHeaders", [])
        expose_headers = rule.get("ExposeHeaders", [])
        max_age = rule.get("MaxAgeSeconds")

        # Check origin match
        if not _origin_matches(request_origin, allowed_origins):
            continue

        # Check method match (if this is a preflight or a regular request)
        if request_method and not _method_matches(request_method, allowed_methods):
            continue

        # Build response headers
        headers: dict[str, str] = {}
        headers["Access-Control-Allow-Origin"] = request_origin
        if allowed_methods:
            headers["Access-Control-Allow-Methods"] = ", ".join(allowed_methods)
        if allowed_headers:
            headers["Access-Control-Allow-Headers"] = ", ".join(allowed_headers)
        if expose_headers:
            headers["Access-Control-Expose-Headers"] = ", ".join(expose_headers)
        if max_age is not None:
            headers["Access-Control-Max-Age"] = str(max_age)
        headers["Vary"] = "Origin"
        return headers

    return {}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_csv(value: str) -> list[str]:
    """Parse a comma-separated string into a list of trimmed, non-empty strings."""
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _resolve_origin(config: CORSConfig, request_origin: str | None) -> str | None:
    """Determine which origin value to return in Access-Control-Allow-Origin.

    Returns:
        - "*" if no specific origins are configured (wildcard mode)
        - The request_origin if it matches the allowed list
        - "*" if DISABLE_CORS_CHECKS is set
        - None if the origin is not allowed
    """
    # No specific origins configured → wildcard
    if not config.allowed_origins:
        return "*"

    # DISABLE_CORS_CHECKS → accept anything
    if config.disable_cors_checks:
        return request_origin or "*"

    # Check if wildcard is in allowed origins
    if "*" in config.allowed_origins:
        return "*"

    # No origin in request → use wildcard if allowed, else first origin
    if not request_origin:
        return config.allowed_origins[0] if config.allowed_origins else "*"

    # Check if the request origin matches any allowed origin
    for allowed in config.allowed_origins:
        if _origin_matches(request_origin, [allowed]):
            return request_origin

    # Origin not in allow list
    return None


def _origin_matches(origin: str, patterns: list[str]) -> bool:
    """Check if an origin matches any of the allowed origin patterns."""
    for pattern in patterns:
        if pattern == "*":
            return True
        if pattern == origin:
            return True
        # Support wildcard patterns like *.example.com
        if fnmatch.fnmatch(origin, pattern):
            return True
    return False


def _method_matches(method: str, allowed_methods: list[str]) -> bool:
    """Check if a method matches any of the allowed methods."""
    method_upper = method.upper()
    for m in allowed_methods:
        if m == "*" or m.upper() == method_upper:
            return True
    return False
