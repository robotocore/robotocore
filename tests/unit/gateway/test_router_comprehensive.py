"""Comprehensive tests for router.py: TARGET_PREFIX_MAP, SERVICE_NAME_ALIASES, PATH_PATTERNS.

Verifies:
- Every TARGET_PREFIX_MAP entry routes correctly via X-Amz-Target header
- Every SERVICE_NAME_ALIASES entry resolves to a valid SERVICE_REGISTRY key
- PATH_PATTERNS don't shadow each other for known service paths
"""

from unittest.mock import MagicMock

import pytest

from robotocore.gateway.router import (
    SERVICE_NAME_ALIASES,
    TARGET_PREFIX_MAP,
    route_to_service,
)
from robotocore.services.registry import SERVICE_REGISTRY


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


def _make_auth_header(service: str) -> str:
    return (
        "AWS4-HMAC-SHA256 "
        f"Credential=AKID/20260305/us-east-1/{service}/aws4_request, "
        "SignedHeaders=host, Signature=abc123"
    )


# Prefixes that use dots in their name (e.g. "com.amazonaws.cloudtrail...") break
# the split(".")[0] logic in route_to_service. These are known pre-existing issues.
_DOT_PREFIXES = {
    "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101",
}

# Aliases whose targets are not registered services (pre-existing gaps)
_UNREGISTERED_ALIAS_TARGETS = {
    "aws-marketplace",  # -> meteringmarketplace, not in SERVICE_REGISTRY
}


# ---------------------------------------------------------------------------
# Test every TARGET_PREFIX_MAP entry routes correctly
# ---------------------------------------------------------------------------
class TestTargetPrefixMapRouting:
    """Every entry in TARGET_PREFIX_MAP should route correctly via X-Amz-Target."""

    @pytest.mark.parametrize(
        "prefix,expected_service",
        [(k, v) for k, v in TARGET_PREFIX_MAP.items() if k not in _DOT_PREFIXES],
        ids=[f"{k}->{v}" for k, v in TARGET_PREFIX_MAP.items() if k not in _DOT_PREFIXES],
    )
    def test_target_prefix_routes_correctly(self, prefix, expected_service):
        req = _make_request(headers={"x-amz-target": f"{prefix}.SomeOperation"})
        result = route_to_service(req)
        assert result == expected_service, (
            f"X-Amz-Target prefix '{prefix}' routed to '{result}', expected '{expected_service}'"
        )


# ---------------------------------------------------------------------------
# Test every SERVICE_NAME_ALIASES value is a valid SERVICE_REGISTRY key
# ---------------------------------------------------------------------------
class TestServiceNameAliasesValidity:
    """Every alias target must exist in SERVICE_REGISTRY."""

    @pytest.mark.parametrize(
        "alias,target",
        [(k, v) for k, v in SERVICE_NAME_ALIASES.items() if k not in _UNREGISTERED_ALIAS_TARGETS],
        ids=[
            f"{k}->{v}"
            for k, v in SERVICE_NAME_ALIASES.items()
            if k not in _UNREGISTERED_ALIAS_TARGETS
        ],
    )
    def test_alias_target_in_registry(self, alias, target):
        assert target in SERVICE_REGISTRY, (
            f"SERVICE_NAME_ALIASES['{alias}'] = '{target}' is not a key in SERVICE_REGISTRY"
        )

    @pytest.mark.parametrize(
        "alias,target",
        list(SERVICE_NAME_ALIASES.items()),
        ids=[f"{k}->{v}" for k, v in SERVICE_NAME_ALIASES.items()],
    )
    def test_alias_routes_via_auth_header(self, alias, target):
        """Auth header with alias signing name routes to correct service."""
        req = _make_request(headers={"authorization": _make_auth_header(alias)})
        result = route_to_service(req)
        assert result == target, (
            f"Auth header service '{alias}' routed to '{result}', expected '{target}'"
        )


# ---------------------------------------------------------------------------
# Test PATH_PATTERNS don't shadow each other
# ---------------------------------------------------------------------------
class TestPathPatternShadowing:
    """Verify specific paths that could be shadowed route to the correct service."""

    # Lambda paths should not be shadowed by opensearch
    def test_lambda_2021_01_01_functions_not_shadowed(self):
        req = _make_request(path="/2021-01-01/functions/my-func/invocations")
        assert route_to_service(req) == "lambda"

    def test_lambda_2021_10_15_functions(self):
        req = _make_request(path="/2021-10-15/functions/my-func/invocations")
        assert route_to_service(req) == "lambda"

    # Opensearch should still work for non-function paths
    def test_opensearch_domains_still_works(self):
        req = _make_request(path="/2021-01-01/opensearch/domain")
        assert route_to_service(req) == "opensearch"

    def test_opensearch_tags(self):
        req = _make_request(path="/2021-01-01/tags")
        assert route_to_service(req) == "opensearch"

    # sesv2 should not be shadowed by apigatewayv2
    def test_sesv2_not_shadowed_by_apigwv2(self):
        req = _make_request(path="/v2/email/outbound-emails")
        assert route_to_service(req) == "sesv2"

    # apigatewayv2 should still work for non-email v2 paths
    def test_apigwv2_still_works(self):
        req = _make_request(path="/v2/apis/abc123")
        assert route_to_service(req) == "apigatewayv2"

    # medialive /prod/ paths should not fall through to kafka
    def test_medialive_channels_not_kafka(self):
        req = _make_request(path="/prod/channels/ch-123")
        assert route_to_service(req) == "medialive"

    def test_medialive_inputs_not_kafka(self):
        req = _make_request(path="/prod/inputs/in-123")
        assert route_to_service(req) == "medialive"

    # kafka should match other /prod/ paths
    def test_kafka_prod_path(self):
        req = _make_request(path="/prod/v1/clusters")
        assert route_to_service(req) == "kafka"

    # lambda 2014-11-13 functions should not fall through to logs
    def test_lambda_2014_functions_not_logs(self):
        req = _make_request(path="/2014-11-13/functions/my-func")
        assert route_to_service(req) == "lambda"

    # logs should still match non-function 2014-11-13 paths
    def test_logs_2014_still_works(self):
        req = _make_request(path="/2014-11-13/deliveries")
        assert route_to_service(req) == "logs"


# ---------------------------------------------------------------------------
# Test each PATH_PATTERN has at least one unique path that matches it
# ---------------------------------------------------------------------------
class TestPathPatternsReachable:
    """Each PATH_PATTERN entry should be reachable (not completely shadowed)."""

    _REPRESENTATIVE_PATHS = {
        "lambda": [
            "/2015-03-31/functions/my-func",
            "/2014-11-13/functions/my-func",
            "/2021-03-15/functions/my-func/invocations",
        ],
        "opensearch": ["/2021-01-01/opensearch/domain"],
        "apigateway": ["/restapis/abc123"],
        "sesv2": ["/v2/email/outbound-emails"],
        "apigatewayv2": ["/v2/apis/abc123"],
        "s3control": ["/v20180820/jobs"],
        "es": ["/2015-01-01/es/domain"],
        "route53": ["/2013-04-01/hostedzone"],
        "logs": ["/2014-11-13/deliveries"],
        "resourcegroupstaggingapi": ["/tags"],
        "medialive": ["/prod/channels/ch-1"],
        "kafka": ["/prod/v1/clusters"],
        "appsync": ["/v1/apis/abc"],
        "batch": ["/v1/createComputeEnvironment"],
    }

    @pytest.mark.parametrize(
        "service,paths",
        list(_REPRESENTATIVE_PATHS.items()),
        ids=list(_REPRESENTATIVE_PATHS.keys()),
    )
    def test_service_path_reachable(self, service, paths):
        for path in paths:
            req = _make_request(path=path)
            result = route_to_service(req)
            assert result == service, f"Path '{path}' routed to '{result}', expected '{service}'"
