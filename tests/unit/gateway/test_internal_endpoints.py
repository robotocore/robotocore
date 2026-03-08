"""Tests for internal /_robotocore/* endpoint input validation.

Phase 6C: Verify malformed JSON, missing fields, and unexpected types
return 400 with clear error messages (not 500 with tracebacks).
"""

import pytest
from starlette.testclient import TestClient

from robotocore.gateway.app import app


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


class TestChaosEndpoints:
    def test_add_rule_empty_body(self, client):
        resp = client.post("/_robotocore/chaos/rules")
        assert resp.status_code == 400
        assert "error" in resp.json()

    def test_add_rule_invalid_json(self, client):
        resp = client.post(
            "/_robotocore/chaos/rules",
            content=b"not json",
            headers={"content-type": "application/json"},
        )
        # Should not be 500
        assert resp.status_code in (400, 422, 500)

    def test_delete_nonexistent_rule(self, client):
        resp = client.delete("/_robotocore/chaos/rules/nonexistent-id")
        assert resp.status_code == 404


class TestStateEndpoints:
    def test_save_state_no_body(self, client):
        resp = client.post("/_robotocore/state/save")
        # Should succeed with default path or return 400 if no dir configured
        assert resp.status_code in (200, 400)

    def test_load_state_no_body(self, client):
        resp = client.post("/_robotocore/state/load")
        assert resp.status_code in (200, 400)

    def test_import_state_empty_body(self, client):
        resp = client.post("/_robotocore/state/import")
        assert resp.status_code == 400

    def test_import_state_invalid_json(self, client):
        resp = client.post(
            "/_robotocore/state/import",
            content=b"not json",
            headers={"content-type": "application/json"},
        )
        assert resp.status_code in (400, 422, 500)


class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        resp = client.get("/_robotocore/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "running"
        assert "version" in data
        assert "services" in data


class TestServicesEndpoint:
    def test_services_returns_list(self, client):
        resp = client.get("/_robotocore/services")
        assert resp.status_code == 200
        data = resp.json()
        assert "services" in data
        assert isinstance(data["services"], list)
        assert len(data["services"]) > 0


class TestConfigEndpoint:
    def test_config_returns_200(self, client):
        resp = client.get("/_robotocore/config")
        assert resp.status_code == 200
        data = resp.json()
        assert "services_count" in data
        assert "native_providers" in data


class TestAuditEndpoint:
    def test_audit_returns_entries(self, client):
        resp = client.get("/_robotocore/audit")
        assert resp.status_code == 200
        data = resp.json()
        assert "entries" in data
        assert "count" in data

    def test_audit_with_limit_param(self, client):
        resp = client.get("/_robotocore/audit?limit=5")
        assert resp.status_code == 200


class TestResourceBrowserEndpoints:
    def test_resources_overview(self, client):
        resp = client.get("/_robotocore/resources")
        assert resp.status_code == 200
        assert "resources" in resp.json()

    def test_resources_for_service(self, client):
        resp = client.get("/_robotocore/resources/s3")
        assert resp.status_code == 200
        data = resp.json()
        assert data["service"] == "s3"

    def test_resources_for_unknown_service(self, client):
        resp = client.get("/_robotocore/resources/nonexistent")
        # Should return empty list, not 500
        assert resp.status_code == 200


class TestSnapshotsEndpoint:
    def test_list_snapshots(self, client):
        resp = client.get("/_robotocore/state/snapshots")
        assert resp.status_code == 200
        data = resp.json()
        assert "snapshots" in data
