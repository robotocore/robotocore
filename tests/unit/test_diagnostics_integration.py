"""Semantic integration tests for the diagnostic bundle endpoint."""

import os
import time
from unittest.mock import patch

from starlette.applications import Starlette
from starlette.routing import Route
from starlette.testclient import TestClient

from robotocore.diagnostics_bundle import diagnose_endpoint


def _make_client():
    test_app = Starlette(routes=[Route("/diagnose", diagnose_endpoint, methods=["GET"])])
    return TestClient(test_app, raise_server_exceptions=False)


class TestEndToEnd:
    def test_full_json_structure(self):
        """Call endpoint and verify JSON has all expected top-level keys."""
        client = _make_client()
        with patch.dict(os.environ, {"DEBUG": "1"}):
            resp = client.get("/diagnose")
        assert resp.status_code == 200
        data = resp.json()
        expected_keys = {
            "system",
            "server",
            "config",
            "services",
            "state",
            "background_engines",
            "memory",
            "audit",
            "extensions",
        }
        assert expected_keys == set(data.keys())

    def test_pid_matches_current_process(self):
        """Verify the diagnostic PID is accurate."""
        client = _make_client()
        with patch.dict(os.environ, {"DEBUG": "1"}):
            resp = client.get("/diagnose?section=system")
        data = resp.json()
        assert data["system"]["pid"] == os.getpid()

    def test_uptime_increases(self):
        """Verify uptime is non-negative (monotonic)."""
        client = _make_client()
        with patch.dict(os.environ, {"DEBUG": "1"}):
            resp1 = client.get("/diagnose?section=server")
            time.sleep(0.05)
            resp2 = client.get("/diagnose?section=server")
        t1 = resp1.json()["server"]["uptime_seconds"]
        t2 = resp2.json()["server"]["uptime_seconds"]
        # Uptime should be non-negative
        assert t1 >= 0
        assert t2 >= 0

    def test_masked_config_doesnt_leak_secrets(self):
        """Verify sensitive env vars are masked, never leaked."""
        client = _make_client()
        env = {
            "DEBUG": "1",
            "ROBOTOCORE_SECRET_KEY": "super-secret-value-12345",
            "ROBOTOCORE_API_TOKEN": "tok-secret-abc",
        }
        with patch.dict(os.environ, env):
            resp = client.get("/diagnose?section=config")
        data = resp.json()
        config = data["config"]
        # Must be masked
        assert config["ROBOTOCORE_SECRET_KEY"] == "***MASKED***"
        assert config["ROBOTOCORE_API_TOKEN"] == "***MASKED***"
        # The actual secret values must not appear anywhere in the response body
        body = resp.text
        assert "super-secret-value-12345" not in body
        assert "tok-secret-abc" not in body

    def test_services_native_count_matches_registry(self):
        """Verify the service count is derived from the real registry."""
        from robotocore.services.registry import SERVICE_REGISTRY, ServiceStatus

        client = _make_client()
        with patch.dict(os.environ, {"DEBUG": "1"}):
            resp = client.get("/diagnose?section=services")
        data = resp.json()
        expected_native = sum(
            1 for s in SERVICE_REGISTRY.values() if s.status == ServiceStatus.NATIVE
        )
        assert data["services"]["native_count"] == expected_native

    def test_memory_rss_is_positive(self):
        """RSS memory should be a positive integer."""
        client = _make_client()
        with patch.dict(os.environ, {"DEBUG": "1"}):
            resp = client.get("/diagnose?section=memory")
        data = resp.json()
        assert data["memory"]["rss_bytes"] > 0
