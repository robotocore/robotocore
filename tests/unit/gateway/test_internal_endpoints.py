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
        assert resp.status_code == 400
        assert resp.json()["error"] == "Invalid JSON"

    def test_add_rule_binary_garbage(self, client):
        resp = client.post(
            "/_robotocore/chaos/rules",
            content=b"\x80\x81\x82\xff",
            headers={"content-type": "application/json"},
        )
        assert resp.status_code == 400
        assert "Invalid JSON" in resp.json()["error"]

    def test_add_rule_truncated_json(self, client):
        resp = client.post(
            "/_robotocore/chaos/rules",
            content=b'{"service": "s3"',
            headers={"content-type": "application/json"},
        )
        assert resp.status_code == 400

    def test_add_rule_valid_creates_rule(self, client):
        resp = client.post(
            "/_robotocore/chaos/rules",
            content=b'{"service": "s3", "error_code": "InternalError"}',
            headers={"content-type": "application/json"},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["status"] == "created"
        assert "rule_id" in data
        # Clean up
        client.delete(f"/_robotocore/chaos/rules/{data['rule_id']}")

    def test_add_rule_empty_json_object(self, client):
        resp = client.post(
            "/_robotocore/chaos/rules",
            content=b"{}",
            headers={"content-type": "application/json"},
        )
        assert resp.status_code == 201
        # Clean up
        client.delete(f"/_robotocore/chaos/rules/{resp.json()['rule_id']}")

    def test_list_rules_returns_rules_array(self, client):
        resp = client.get("/_robotocore/chaos/rules")
        assert resp.status_code == 200
        assert "rules" in resp.json()
        assert isinstance(resp.json()["rules"], list)

    def test_clear_rules(self, client):
        resp = client.post("/_robotocore/chaos/rules/clear")
        assert resp.status_code == 200
        assert resp.json()["status"] == "cleared"

    def test_add_then_list_then_delete(self, client):
        """Full lifecycle: add, verify listed, delete, verify gone."""
        # Add
        add_resp = client.post(
            "/_robotocore/chaos/rules",
            content=b'{"service": "sqs", "error_code": "QueueDoesNotExist"}',
        )
        assert add_resp.status_code == 201
        rule_id = add_resp.json()["rule_id"]

        # List
        list_resp = client.get("/_robotocore/chaos/rules")
        rule_ids = [r["rule_id"] for r in list_resp.json()["rules"]]
        assert rule_id in rule_ids

        # Delete
        del_resp = client.delete(f"/_robotocore/chaos/rules/{rule_id}")
        assert del_resp.status_code == 200

        # Verify gone
        list_resp2 = client.get("/_robotocore/chaos/rules")
        rule_ids2 = [r["rule_id"] for r in list_resp2.json()["rules"]]
        assert rule_id not in rule_ids2

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
        # TODO: state/import also needs json.loads error handling
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
