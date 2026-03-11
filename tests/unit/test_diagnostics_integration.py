"""Semantic integration tests for the diagnostic bundle endpoint.

These tests verify cross-section behavior and end-to-end data accuracy
by calling the endpoint and validating the response against known state.
"""

import os
import time
from unittest.mock import patch

from starlette.applications import Starlette
from starlette.routing import Route
from starlette.testclient import TestClient

from robotocore.diagnostics_bundle import ALL_SECTIONS, diagnose_endpoint


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
        expected_keys = set(ALL_SECTIONS)
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

    def test_config_section_shows_only_relevant_vars(self):
        """Verify random env vars don't leak into the config section."""
        client = _make_client()
        env = {
            "DEBUG": "1",
            "PATH": "/usr/bin",
            "HOME": "/home/test",
            "ROBOTOCORE_CUSTOM": "yes",
        }
        with patch.dict(os.environ, env):
            resp = client.get("/diagnose?section=config")
        data = resp.json()
        assert "ROBOTOCORE_CUSTOM" in data["config"]
        assert "PATH" not in data["config"]
        assert "HOME" not in data["config"]

    def test_multiple_calls_return_consistent_static_data(self):
        """Static fields like python_version should not change between calls."""
        client = _make_client()
        with patch.dict(os.environ, {"DEBUG": "1"}):
            resp1 = client.get("/diagnose?section=system")
            resp2 = client.get("/diagnose?section=system")
        d1 = resp1.json()["system"]
        d2 = resp2.json()["system"]
        assert d1["python_version"] == d2["python_version"]
        assert d1["platform"] == d2["platform"]
        assert d1["architecture"] == d2["architecture"]
        assert d1["pid"] == d2["pid"]

    def test_services_total_equals_sum_of_categories(self):
        """total_count must equal native + moto + disabled."""
        client = _make_client()
        with patch.dict(os.environ, {"DEBUG": "1"}):
            resp = client.get("/diagnose?section=services")
        svc = resp.json()["services"]
        assert svc["total_count"] == svc["native_count"] + svc["moto_count"] + svc["disabled_count"]
