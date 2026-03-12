"""Tests for the SERVICES env var service filtering feature.

Verifies that:
- When SERVICES is not set, all services are accessible
- When SERVICES is set, only listed services work; others return 501
- Service names are case-insensitive
- Internal /_robotocore/* endpoints always work regardless of SERVICES
- Health endpoint reflects which services are enabled
"""

import pytest
from starlette.testclient import TestClient

from robotocore.gateway.app import app
from robotocore.services.loader import init_loader, reset_loader


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture(autouse=True)
def _reset_loader():
    """Reset loader state before and after each test."""
    reset_loader()
    yield
    reset_loader()


def _sts_headers():
    """Return headers that route to STS."""
    return {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": (
            "AWS4-HMAC-SHA256 "
            "Credential=testing/20260305/us-east-1/sts/aws4_request, "
            "SignedHeaders=host, Signature=abc"
        ),
    }


def _s3_headers():
    """Return headers that route to S3."""
    return {
        "Authorization": (
            "AWS4-HMAC-SHA256 "
            "Credential=testing/20260305/us-east-1/s3/aws4_request, "
            "SignedHeaders=host, Signature=abc"
        ),
    }


def _dynamodb_headers():
    """Return headers that route to DynamoDB."""
    return {
        "Content-Type": "application/x-amz-json-1.0",
        "X-Amz-Target": "DynamoDB_20120810.ListTables",
        "Authorization": (
            "AWS4-HMAC-SHA256 "
            "Credential=testing/20260305/us-east-1/dynamodb/aws4_request, "
            "SignedHeaders=host, Signature=abc"
        ),
    }


class TestServicesNotSet:
    """When SERVICES env var is not set, all services should be accessible."""

    def test_sts_accessible(self, client, monkeypatch):
        monkeypatch.delenv("SERVICES", raising=False)
        init_loader()
        response = client.post(
            "/",
            data="Action=GetCallerIdentity&Version=2011-06-15",
            headers=_sts_headers(),
        )
        assert response.status_code == 200

    def test_dynamodb_accessible(self, client, monkeypatch):
        monkeypatch.delenv("SERVICES", raising=False)
        init_loader()
        response = client.post(
            "/",
            content=b'{"ExclusiveStartTableName": null, "Limit": 100}',
            headers=_dynamodb_headers(),
        )
        assert response.status_code == 200

    def test_health_shows_all_services(self, client, monkeypatch):
        monkeypatch.delenv("SERVICES", raising=False)
        init_loader()
        response = client.get("/_robotocore/health")
        assert response.status_code == 200
        data = response.json()
        assert data["services_filter"] == "all"
        assert "enabled_services" not in data
        # Should have many services (all registered)
        assert len(data["services"]) > 100


class TestServicesFilterActive:
    """When SERVICES is set, only listed services should work."""

    def test_allowed_service_works(self, client, monkeypatch):
        monkeypatch.setenv("SERVICES", "sts,s3")
        init_loader()
        response = client.post(
            "/",
            data="Action=GetCallerIdentity&Version=2011-06-15",
            headers=_sts_headers(),
        )
        assert response.status_code == 200

    def test_disallowed_service_returns_501(self, client, monkeypatch):
        monkeypatch.setenv("SERVICES", "s3,sqs")
        init_loader()
        response = client.post(
            "/",
            content=b'{"ExclusiveStartTableName": null, "Limit": 100}',
            headers=_dynamodb_headers(),
        )
        assert response.status_code == 501
        data = response.json()
        assert "not enabled" in data["error"]
        assert "dynamodb" in data["error"]

    def test_multiple_services(self, client, monkeypatch):
        monkeypatch.setenv("SERVICES", "s3,sqs,dynamodb")
        init_loader()
        # DynamoDB should work now
        response = client.post(
            "/",
            content=b'{"ExclusiveStartTableName": null, "Limit": 100}',
            headers=_dynamodb_headers(),
        )
        assert response.status_code == 200


class TestServicesFilterCaseInsensitive:
    """Service names in SERVICES should be case-insensitive."""

    def test_uppercase(self, client, monkeypatch):
        monkeypatch.setenv("SERVICES", "STS,S3")
        init_loader()
        response = client.post(
            "/",
            data="Action=GetCallerIdentity&Version=2011-06-15",
            headers=_sts_headers(),
        )
        assert response.status_code == 200

    def test_mixed_case(self, client, monkeypatch):
        monkeypatch.setenv("SERVICES", "Sts,s3,DynamoDB")
        init_loader()
        response = client.post(
            "/",
            data="Action=GetCallerIdentity&Version=2011-06-15",
            headers=_sts_headers(),
        )
        assert response.status_code == 200

    def test_spaces_around_names(self, client, monkeypatch):
        monkeypatch.setenv("SERVICES", " sts , s3 ")
        init_loader()
        response = client.post(
            "/",
            data="Action=GetCallerIdentity&Version=2011-06-15",
            headers=_sts_headers(),
        )
        assert response.status_code == 200


class TestInternalEndpointsAlwaysWork:
    """/_robotocore/* endpoints must work regardless of SERVICES setting."""

    def test_health_works_with_services_filter(self, client, monkeypatch):
        monkeypatch.setenv("SERVICES", "s3")
        init_loader()
        response = client.get("/_robotocore/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "running"

    def test_services_endpoint_works_with_filter(self, client, monkeypatch):
        monkeypatch.setenv("SERVICES", "s3")
        init_loader()
        response = client.get("/_robotocore/services")
        assert response.status_code == 200

    def test_config_endpoint_works_with_filter(self, client, monkeypatch):
        monkeypatch.setenv("SERVICES", "s3")
        init_loader()
        response = client.get("/_robotocore/config")
        assert response.status_code == 200


class TestHealthReflectsFilter:
    """Health endpoint should show only enabled services when SERVICES is set."""

    def test_health_shows_filter_value(self, client, monkeypatch):
        monkeypatch.setenv("SERVICES", "s3,sqs")
        init_loader()
        response = client.get("/_robotocore/health")
        data = response.json()
        assert data["services_filter"] == "s3,sqs"

    def test_health_shows_enabled_services_list(self, client, monkeypatch):
        monkeypatch.setenv("SERVICES", "s3,sqs,dynamodb")
        init_loader()
        response = client.get("/_robotocore/health")
        data = response.json()
        assert "enabled_services" in data
        assert sorted(data["enabled_services"]) == ["dynamodb", "s3", "sqs"]

    def test_health_only_shows_enabled_in_services_map(self, client, monkeypatch):
        monkeypatch.setenv("SERVICES", "s3,sqs")
        init_loader()
        response = client.get("/_robotocore/health")
        data = response.json()
        services = data["services"]
        assert "s3" in services
        assert "sqs" in services
        assert "dynamodb" not in services
        assert "lambda" not in services

    def test_health_no_filter_shows_all(self, client, monkeypatch):
        monkeypatch.delenv("SERVICES", raising=False)
        init_loader()
        response = client.get("/_robotocore/health")
        data = response.json()
        assert data["services_filter"] == "all"
        assert "enabled_services" not in data
        assert "dynamodb" in data["services"]
        assert "s3" in data["services"]
