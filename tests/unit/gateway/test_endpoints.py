"""Tests for management endpoints: health, services, config, state export/import."""

import json


class TestHealthEndpoint:
    def test_returns_200(self, client):
        response = client.get("/_robotocore/health")
        assert response.status_code == 200

    def test_status_running(self, client):
        response = client.get("/_robotocore/health")
        data = response.json()
        assert data["status"] == "running"

    def test_has_version(self, client):
        response = client.get("/_robotocore/health")
        data = response.json()
        assert isinstance(data["version"], str)
        assert len(data["version"]) > 0

    def test_has_uptime(self, client):
        response = client.get("/_robotocore/health")
        data = response.json()
        assert "uptime_seconds" in data
        assert isinstance(data["uptime_seconds"], (int, float))
        assert data["uptime_seconds"] >= 0

    def test_has_services_dict(self, client):
        response = client.get("/_robotocore/health")
        data = response.json()
        assert "services" in data
        services = data["services"]
        assert isinstance(services, dict)
        # Should have at least some services
        assert len(services) > 0

    def test_service_entry_structure(self, client):
        response = client.get("/_robotocore/health")
        data = response.json()
        # Check one known service
        assert "s3" in data["services"]
        s3 = data["services"]["s3"]
        assert s3["status"] == "running"
        assert s3["type"] in ("native", "moto")
        assert isinstance(s3["requests"], int)

    def test_native_vs_moto_types(self, client):
        response = client.get("/_robotocore/health")
        data = response.json()
        # SQS is native
        assert data["services"]["sqs"]["type"] == "native"
        # IAM is native (has interceptors for simulate, permissions boundary, etc.)
        assert data["services"]["iam"]["type"] == "native"


class TestServicesEndpoint:
    def test_returns_200(self, client):
        response = client.get("/_robotocore/services")
        assert response.status_code == 200

    def test_has_services_list(self, client):
        response = client.get("/_robotocore/services")
        data = response.json()
        assert "services" in data
        assert isinstance(data["services"], list)
        assert len(data["services"]) > 0

    def test_service_entry_fields(self, client):
        response = client.get("/_robotocore/services")
        data = response.json()
        first = data["services"][0]
        assert "name" in first
        assert "status" in first
        assert "protocol" in first
        assert first["status"] in ("native", "moto")

    def test_services_sorted_by_name(self, client):
        response = client.get("/_robotocore/services")
        data = response.json()
        names = [s["name"] for s in data["services"]]
        assert names == sorted(names)

    def test_contains_known_services(self, client):
        response = client.get("/_robotocore/services")
        data = response.json()
        names = {s["name"] for s in data["services"]}
        assert "s3" in names
        assert "sqs" in names
        assert "lambda" in names
        assert "iam" in names


class TestConfigEndpoint:
    def test_returns_200(self, client):
        response = client.get("/_robotocore/config")
        assert response.status_code == 200

    def test_has_expected_fields(self, client):
        response = client.get("/_robotocore/config")
        data = response.json()
        assert "enforce_iam" in data
        assert "persistence" in data
        assert "log_level" in data
        assert "debug" in data
        assert "region" in data
        assert "services_count" in data
        assert "native_providers" in data

    def test_default_values(self, client):
        response = client.get("/_robotocore/config")
        data = response.json()
        assert data["enforce_iam"] is False
        assert isinstance(data["services_count"], int)
        assert data["services_count"] > 0
        assert isinstance(data["native_providers"], int)
        assert data["native_providers"] > 0

    def test_log_level_is_string(self, client):
        response = client.get("/_robotocore/config")
        data = response.json()
        assert isinstance(data["log_level"], str)
        assert data["log_level"] in ("DEBUG", "INFO", "WARNING", "ERROR")


class TestStateExportImport:
    def test_export_returns_200(self, client):
        response = client.get("/_robotocore/state/export")
        assert response.status_code == 200

    def test_export_structure(self, client):
        response = client.get("/_robotocore/state/export")
        data = response.json()
        assert "version" in data
        assert "exported_at" in data
        assert "native_state" in data

    def test_import_returns_200(self, client):
        payload = {
            "version": "1.0",
            "exported_at": 0,
            "native_state": {},
        }
        response = client.post(
            "/_robotocore/state/import",
            content=json.dumps(payload),
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "imported"

    def test_import_empty_body_returns_400(self, client):
        response = client.post("/_robotocore/state/import", content=b"")
        assert response.status_code == 400
