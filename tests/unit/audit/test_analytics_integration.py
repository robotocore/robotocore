"""Semantic integration tests for usage analytics management endpoints."""

import time

import pytest
from starlette.testclient import TestClient

from robotocore.audit.analytics import (
    _reset_singleton,
    get_usage_analytics,
)


def _rr(analytics, **kwargs):
    """Shorthand for record_request with defaults."""
    defaults = {
        "service": "s3",
        "operation": "Get",
        "status_code": 200,
        "duration_ms": 1.0,
    }
    defaults.update(kwargs)
    analytics.record_request(**defaults)


@pytest.fixture(autouse=True)
def reset_analytics(monkeypatch):
    """Reset the analytics singleton before each test."""
    monkeypatch.setenv("USAGE_ANALYTICS", "1")
    _reset_singleton()
    yield
    _reset_singleton()


@pytest.fixture
def client():
    """Create a Starlette test client with the management routes."""
    from robotocore.gateway.app import app

    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def analytics():
    """Get the current analytics singleton."""
    return get_usage_analytics()


class TestUsageSummaryEndpoint:
    def test_record_requests_then_get_summary(self, client, analytics):
        _rr(analytics, operation="PutObject", duration_ms=5.0)
        _rr(analytics, service="sqs", operation="SendMessage", duration_ms=3.0)
        _rr(analytics, operation="GetObject", status_code=404)

        resp = client.get("/_robotocore/usage")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_requests"] == 3
        assert data["total_errors"] == 1
        assert data["services_used"] == 2


class TestUsageErrorsEndpoint:
    def test_record_errors_then_get_breakdown(self, client, analytics):
        _rr(analytics, operation="GetObject", status_code=404, error_type="NoSuchKey")
        _rr(analytics, operation="PutObject", status_code=500, error_type="InternalError")
        _rr(
            analytics,
            service="sqs",
            operation="SendMessage",
            status_code=400,
            error_type="InvalidParameterValue",
        )

        resp = client.get("/_robotocore/usage/errors")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_errors"] == 3
        assert data["by_status_code"]["404"] == 1
        assert data["by_status_code"]["500"] == 1
        assert data["by_error_type"]["NoSuchKey"] == 1


class TestUsageServicesEndpoint:
    def test_record_across_services_then_get_breakdown(self, client, analytics):
        for _ in range(3):
            _rr(analytics, operation="GetObject", duration_ms=5.0)
        _rr(analytics, service="sqs", operation="SendMessage", duration_ms=10.0)

        resp = client.get("/_robotocore/usage/services")
        assert resp.status_code == 200
        data = resp.json()
        services = data["services"]
        assert "s3" in services
        assert "sqs" in services
        assert services["s3"]["total_requests"] == 3
        assert services["sqs"]["total_requests"] == 1

    def test_get_single_service_detail(self, client, analytics):
        _rr(analytics, operation="PutObject", duration_ms=5.0)
        _rr(analytics, operation="GetObject", duration_ms=3.0)

        resp = client.get("/_robotocore/usage/services/s3")
        assert resp.status_code == 200
        data = resp.json()
        assert data["service"] == "s3"
        assert data["total_requests"] == 2
        assert "PutObject" in data["operations"]
        assert "GetObject" in data["operations"]


class TestUsageTimelineEndpoint:
    def test_record_over_time_then_get_timeline(self, client, analytics):
        now = time.time()
        _rr(analytics, timestamp=now)
        _rr(analytics, timestamp=now - 60)

        resp = client.get("/_robotocore/usage/timeline")
        assert resp.status_code == 200
        data = resp.json()
        assert "timeline" in data
        assert isinstance(data["timeline"], list)
        total = sum(e["count"] for e in data["timeline"])
        assert total == 2


class TestEndpointsReturnValidJSON:
    def test_all_usage_endpoints_return_json(self, client):
        endpoints = [
            "/_robotocore/usage",
            "/_robotocore/usage/services",
            "/_robotocore/usage/errors",
            "/_robotocore/usage/timeline",
        ]
        for endpoint in endpoints:
            resp = client.get(endpoint)
            assert resp.status_code == 200, f"Failed for {endpoint}: {resp.status_code}"
            data = resp.json()
            assert isinstance(data, dict), f"Expected dict for {endpoint}"
