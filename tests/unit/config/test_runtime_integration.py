"""Semantic integration tests for runtime config endpoints.

These test the HTTP endpoints via Starlette's test client, verifying
the full request/response cycle for config management.
"""

import os
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient


@pytest.fixture
def client_enabled():
    """Create a test client with config updates enabled."""
    with patch.dict(os.environ, {"ENABLE_CONFIG_UPDATES": "1"}):
        # Reset the singleton so it picks up the env var
        import robotocore.config.runtime as rt_mod

        old = rt_mod._runtime_config
        rt_mod._runtime_config = None
        try:
            from robotocore.gateway.app import app

            with TestClient(app, raise_server_exceptions=False) as client:
                yield client
        finally:
            rt_mod._runtime_config = old


@pytest.fixture
def client_disabled():
    """Create a test client with config updates disabled."""
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("ENABLE_CONFIG_UPDATES", None)
        import robotocore.config.runtime as rt_mod

        old = rt_mod._runtime_config
        rt_mod._runtime_config = None
        try:
            from robotocore.gateway.app import app

            with TestClient(app, raise_server_exceptions=False) as client:
                yield client
        finally:
            rt_mod._runtime_config = old


class TestPostConfig:
    def test_post_updates_setting_and_get_shows_new_value(self, client_enabled):
        # Update LOG_LEVEL
        resp = client_enabled.post(
            "/_robotocore/config",
            json={"LOG_LEVEL": "DEBUG"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["updated"]["LOG_LEVEL"] == "DEBUG"

        # GET should show the new value
        resp = client_enabled.get("/_robotocore/config")
        assert resp.status_code == 200
        data = resp.json()
        assert data["log_level"] == "DEBUG"

    def test_post_without_enable_returns_403(self, client_disabled):
        resp = client_disabled.post(
            "/_robotocore/config",
            json={"LOG_LEVEL": "DEBUG"},
        )
        assert resp.status_code == 403
        assert "disabled" in resp.json()["error"].lower()

    def test_post_non_whitelisted_setting_returns_400(self, client_enabled):
        resp = client_enabled.post(
            "/_robotocore/config",
            json={"PORT": "5000"},
        )
        assert resp.status_code == 400
        assert "cannot be changed at runtime" in resp.json()["error"]

    def test_multiple_settings_updated_in_one_post(self, client_enabled):
        resp = client_enabled.post(
            "/_robotocore/config",
            json={"LOG_LEVEL": "DEBUG", "ENFORCE_IAM": "1"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["updated"]["LOG_LEVEL"] == "DEBUG"
        assert data["updated"]["ENFORCE_IAM"] == "1"


class TestDeleteConfig:
    def test_delete_resets_to_original(self, client_enabled):
        # Set override
        client_enabled.post(
            "/_robotocore/config",
            json={"LOG_LEVEL": "DEBUG"},
        )
        # Delete it
        resp = client_enabled.delete("/_robotocore/config/LOG_LEVEL")
        assert resp.status_code == 200
        assert resp.json()["status"] == "reset"

    def test_delete_nonexistent_returns_404(self, client_enabled):
        resp = client_enabled.delete("/_robotocore/config/LOG_LEVEL")
        assert resp.status_code == 404


class TestGetConfig:
    def test_config_shows_source_env_vs_runtime(self, client_enabled):
        with patch.dict(os.environ, {"DEBUG": "0"}):
            # Set runtime override for LOG_LEVEL
            client_enabled.post(
                "/_robotocore/config",
                json={"LOG_LEVEL": "DEBUG"},
            )
            resp = client_enabled.get("/_robotocore/config")
            data = resp.json()
            # Should still have standard config fields
            assert "log_level" in data
            assert "debug" in data
            # Should also have detailed settings with sources
            assert "settings" in data
            log_setting = next((s for s in data["settings"] if s["key"] == "LOG_LEVEL"), None)
            assert log_setting is not None
            assert log_setting["source"] == "runtime"
