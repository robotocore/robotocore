"""Tests for the /_robotocore/init and /_robotocore/plugins endpoint handlers."""

import pytest
from starlette.testclient import TestClient

import robotocore.extensions.plugin_status as plugin_status_mod
import robotocore.init.tracker as tracker_mod
from robotocore.extensions.base import RobotocorePlugin


@pytest.fixture(autouse=True)
def _reset_singletons():
    """Reset global singletons before each test."""
    tracker_mod._tracker = None
    plugin_status_mod._collector = None
    yield
    tracker_mod._tracker = None
    plugin_status_mod._collector = None


@pytest.fixture()
def client():
    """Create a Starlette test client for management routes only."""
    from starlette.applications import Starlette
    from starlette.routing import Route

    from robotocore.gateway.app import (
        init_stage,
        init_summary,
        plugin_detail,
        plugins_list,
    )

    app = Starlette(
        routes=[
            Route("/_robotocore/init", init_summary, methods=["GET"]),
            Route("/_robotocore/init/{stage}", init_stage, methods=["GET"]),
            Route("/_robotocore/plugins", plugins_list, methods=["GET"]),
            Route("/_robotocore/plugins/{name}", plugin_detail, methods=["GET"]),
        ]
    )
    return TestClient(app)


# ---------------------------------------------------------------------------
# /_robotocore/init endpoint tests
# ---------------------------------------------------------------------------


class TestInitSummaryEndpoint:
    def test_empty_summary(self, client):
        resp = client.get("/_robotocore/init")
        assert resp.status_code == 200
        data = resp.json()
        assert data == {"stages": {}}

    def test_summary_with_scripts(self, client):
        tracker = tracker_mod.get_init_tracker()
        tracker.record_complete("a.sh", "boot", duration=0.1)
        tracker.record_failure("b.sh", "boot", error="err", duration=0.2)
        tracker.record_complete("c.sh", "ready", duration=0.3)

        resp = client.get("/_robotocore/init")
        assert resp.status_code == 200
        data = resp.json()
        assert "boot" in data["stages"]
        assert data["stages"]["boot"]["total"] == 2
        assert data["stages"]["boot"]["completed"] == 1
        assert data["stages"]["boot"]["failed"] == 1
        assert "ready" in data["stages"]
        assert data["stages"]["ready"]["total"] == 1
        assert data["stages"]["ready"]["completed"] == 1


class TestInitStageEndpoint:
    def test_empty_stage(self, client):
        resp = client.get("/_robotocore/init/boot")
        assert resp.status_code == 200
        data = resp.json()
        assert data == {"stage": "boot", "scripts": []}

    def test_stage_with_scripts(self, client):
        tracker = tracker_mod.get_init_tracker()
        tracker.record_complete("01-setup.sh", "boot", duration=0.5)
        tracker.record_failure("02-db.sh", "boot", error="timeout", duration=1.0)

        resp = client.get("/_robotocore/init/boot")
        assert resp.status_code == 200
        data = resp.json()
        assert data["stage"] == "boot"
        assert len(data["scripts"]) == 2
        assert data["scripts"][0]["filename"] == "01-setup.sh"
        assert data["scripts"][0]["status"] == "completed"
        assert data["scripts"][0]["duration"] == 0.5
        assert data["scripts"][1]["filename"] == "02-db.sh"
        assert data["scripts"][1]["status"] == "failed"
        assert data["scripts"][1]["error"] == "timeout"

    def test_different_stages_are_isolated(self, client):
        tracker = tracker_mod.get_init_tracker()
        tracker.record_complete("boot.sh", "boot", duration=0.1)
        tracker.record_complete("ready.sh", "ready", duration=0.2)

        resp_boot = client.get("/_robotocore/init/boot")
        resp_ready = client.get("/_robotocore/init/ready")
        assert len(resp_boot.json()["scripts"]) == 1
        assert resp_boot.json()["scripts"][0]["filename"] == "boot.sh"
        assert len(resp_ready.json()["scripts"]) == 1
        assert resp_ready.json()["scripts"][0]["filename"] == "ready.sh"

    def test_nonexistent_stage_returns_empty(self, client):
        resp = client.get("/_robotocore/init/nonexistent")
        assert resp.status_code == 200
        data = resp.json()
        assert data["scripts"] == []


# ---------------------------------------------------------------------------
# /_robotocore/plugins endpoint tests
# ---------------------------------------------------------------------------


class _SamplePlugin(RobotocorePlugin):
    name = "sample"
    version = "2.0.0"
    description = "Sample plugin"

    def on_startup(self):
        pass

    def get_service_overrides(self):
        return {"s3": lambda r, reg, acc: None}

    def get_custom_routes(self):
        return [("/_ext/sample/health", "GET", lambda r: None)]


class TestPluginsListEndpoint:
    def test_empty_plugins_list(self, client):
        resp = client.get("/_robotocore/plugins")
        assert resp.status_code == 200
        data = resp.json()
        assert data == {"plugins": []}

    def test_list_with_loaded_plugin(self, client):
        collector = plugin_status_mod.get_plugin_status_collector()
        plugin = _SamplePlugin()
        collector.record_loaded(plugin, source="entrypoint", load_time=0.05)

        resp = client.get("/_robotocore/plugins")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["plugins"]) == 1
        p = data["plugins"][0]
        assert p["name"] == "sample"
        assert p["version"] == "2.0.0"
        assert p["source"] == "entrypoint"
        assert p["state"] == "active"
        assert "on_startup" in p["hooks"]
        assert "s3" in p["service_overrides"]
        assert "/_ext/sample/health" in p["custom_routes"]

    def test_list_with_failed_plugin(self, client):
        collector = plugin_status_mod.get_plugin_status_collector()
        plugin = RobotocorePlugin()
        plugin.name = "broken"
        plugin.version = "0.1"
        collector.record_failed(plugin, source="directory", error="ImportError")

        resp = client.get("/_robotocore/plugins")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["plugins"]) == 1
        p = data["plugins"][0]
        assert p["name"] == "broken"
        assert p["state"] == "failed"
        assert p["error"] == "ImportError"

    def test_list_with_multiple_plugins(self, client):
        collector = plugin_status_mod.get_plugin_status_collector()
        p1 = _SamplePlugin()
        p2 = RobotocorePlugin()
        p2.name = "basic"
        p2.version = "1.0"
        collector.record_loaded(p1, source="entrypoint")
        collector.record_loaded(p2, source="env_var")

        resp = client.get("/_robotocore/plugins")
        data = resp.json()
        assert len(data["plugins"]) == 2
        names = {p["name"] for p in data["plugins"]}
        assert names == {"sample", "basic"}


class TestPluginDetailEndpoint:
    def test_detail_for_existing_plugin(self, client):
        collector = plugin_status_mod.get_plugin_status_collector()
        plugin = _SamplePlugin()
        collector.record_loaded(plugin, source="entrypoint", load_time=0.123)

        resp = client.get("/_robotocore/plugins/sample")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "sample"
        assert data["version"] == "2.0.0"
        assert data["description"] == "Sample plugin"
        assert data["load_time"] == 0.123
        assert data["state"] == "active"

    def test_detail_for_nonexistent_plugin(self, client):
        resp = client.get("/_robotocore/plugins/nonexistent")
        assert resp.status_code == 404
        data = resp.json()
        assert "error" in data
        assert "nonexistent" in data["error"]

    def test_detail_for_failed_plugin(self, client):
        collector = plugin_status_mod.get_plugin_status_collector()
        plugin = RobotocorePlugin()
        plugin.name = "broke"
        plugin.version = "0.0.1"
        collector.record_failed(plugin, source="directory", error="segfault")

        resp = client.get("/_robotocore/plugins/broke")
        assert resp.status_code == 200
        data = resp.json()
        assert data["state"] == "failed"
        assert data["error"] == "segfault"
