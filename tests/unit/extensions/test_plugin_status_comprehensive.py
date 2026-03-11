"""Comprehensive unit tests for plugin status collection and hook detection."""

import json

import robotocore.extensions.plugin_status as plugin_status_mod
from robotocore.extensions.base import RobotocorePlugin
from robotocore.extensions.plugin_status import (
    PluginInfo,
    PluginStatusCollector,
    _detect_hooks,
    get_plugin_status_collector,
)

# ---------------------------------------------------------------------------
# Test plugin subclasses
# ---------------------------------------------------------------------------


class _NoHooksPlugin(RobotocorePlugin):
    """Plugin that overrides nothing."""

    name = "no-hooks"
    version = "1.0.0"
    description = "Does nothing"


class _AllHooksPlugin(RobotocorePlugin):
    """Plugin that overrides all lifecycle hooks."""

    name = "all-hooks"
    version = "2.0.0"
    description = "Overrides everything"

    def on_load(self):
        pass

    def on_startup(self):
        pass

    def on_shutdown(self):
        pass

    def on_request(self, request, context):
        return None

    def on_response(self, request, response, context):
        return None

    def on_error(self, request, error, context):
        return None


class _PartialHooksPlugin(RobotocorePlugin):
    """Plugin that overrides only on_startup and on_shutdown."""

    name = "partial"
    version = "0.5.0"
    description = "Startup/shutdown only"

    def on_startup(self):
        pass

    def on_shutdown(self):
        pass


class _OverridesPlugin(RobotocorePlugin):
    """Plugin with service overrides and custom routes."""

    name = "overrides"
    version = "3.0.0"
    description = "Has overrides and routes"

    def get_service_overrides(self):
        return {
            "s3": lambda r, reg, acc: None,
            "dynamodb": lambda r, reg, acc: None,
        }

    def get_custom_routes(self):
        return [
            ("/_ext/overrides/status", "GET", lambda r: None),
            ("/_ext/overrides/config", "POST", lambda r: None),
        ]


# ---------------------------------------------------------------------------
# Tests for _detect_hooks
# ---------------------------------------------------------------------------


class TestDetectHooks:
    def test_no_overrides_returns_empty(self):
        plugin = _NoHooksPlugin()
        hooks = _detect_hooks(plugin)
        assert hooks == []

    def test_all_overrides_detected(self):
        plugin = _AllHooksPlugin()
        hooks = _detect_hooks(plugin)
        assert set(hooks) == {
            "on_load",
            "on_startup",
            "on_shutdown",
            "on_request",
            "on_response",
            "on_error",
        }

    def test_partial_overrides_detected(self):
        plugin = _PartialHooksPlugin()
        hooks = _detect_hooks(plugin)
        assert set(hooks) == {"on_startup", "on_shutdown"}

    def test_overrides_plugin_detects_no_lifecycle_hooks(self):
        """_OverridesPlugin doesn't override lifecycle hooks, only get_service_overrides."""
        plugin = _OverridesPlugin()
        hooks = _detect_hooks(plugin)
        # get_service_overrides and get_custom_routes are NOT lifecycle hooks
        assert hooks == []


# ---------------------------------------------------------------------------
# Tests for PluginInfo
# ---------------------------------------------------------------------------


class TestPluginInfo:
    def test_to_dict_minimal(self):
        info = PluginInfo(
            name="test",
            version="1.0",
            description="desc",
            source="entrypoint",
            state="active",
        )
        d = info.to_dict()
        assert d["name"] == "test"
        assert d["version"] == "1.0"
        assert d["description"] == "desc"
        assert d["source"] == "entrypoint"
        assert d["state"] == "active"
        assert d["hooks"] == []
        assert d["load_time"] == 0.0
        assert d["service_overrides"] == []
        assert d["custom_routes"] == []
        # error and config should NOT be present when unset
        assert "error" not in d
        assert "config" not in d

    def test_to_dict_with_error(self):
        info = PluginInfo(
            name="broken",
            version="0.1",
            description="",
            source="env_var",
            state="failed",
            error="ImportError: no module named foo",
        )
        d = info.to_dict()
        assert d["state"] == "failed"
        assert d["error"] == "ImportError: no module named foo"

    def test_to_dict_with_config(self):
        info = PluginInfo(
            name="cfg",
            version="1.0",
            description="",
            source="directory",
            state="active",
            config={"key": "value", "nested": {"a": 1}},
        )
        d = info.to_dict()
        assert d["config"] == {"key": "value", "nested": {"a": 1}}

    def test_to_dict_empty_config_not_included(self):
        info = PluginInfo(
            name="cfg",
            version="1.0",
            description="",
            source="directory",
            state="active",
            config={},
        )
        d = info.to_dict()
        assert "config" not in d

    def test_to_dict_with_hooks_and_overrides(self):
        info = PluginInfo(
            name="full",
            version="1.0",
            description="Full plugin",
            source="entrypoint",
            state="active",
            hooks=["on_load", "on_request"],
            service_overrides=["s3", "dynamodb"],
            custom_routes=["/_ext/full/status"],
            load_time=0.123,
        )
        d = info.to_dict()
        assert d["hooks"] == ["on_load", "on_request"]
        assert d["service_overrides"] == ["s3", "dynamodb"]
        assert d["custom_routes"] == ["/_ext/full/status"]
        assert d["load_time"] == 0.123

    def test_to_dict_is_json_serializable(self):
        info = PluginInfo(
            name="json-test",
            version="1.0",
            description="test",
            source="entrypoint",
            state="active",
            hooks=["on_load"],
            error="some error",
            config={"a": 1},
        )
        serialized = json.dumps(info.to_dict())
        parsed = json.loads(serialized)
        assert parsed["name"] == "json-test"
        assert parsed["error"] == "some error"
        assert parsed["config"]["a"] == 1


# ---------------------------------------------------------------------------
# Tests for PluginStatusCollector
# ---------------------------------------------------------------------------


class TestPluginStatusCollectorComprehensive:
    def setup_method(self):
        self.collector = PluginStatusCollector()

    def test_record_loaded_captures_hooks(self):
        plugin = _AllHooksPlugin()
        self.collector.record_loaded(plugin, source="entrypoint", load_time=0.01)
        result = self.collector.list_plugins()
        assert len(result) == 1
        hooks = result[0]["hooks"]
        assert "on_load" in hooks
        assert "on_startup" in hooks
        assert "on_shutdown" in hooks
        assert "on_request" in hooks
        assert "on_response" in hooks
        assert "on_error" in hooks

    def test_record_loaded_captures_service_overrides(self):
        plugin = _OverridesPlugin()
        self.collector.record_loaded(plugin, source="directory", load_time=0.02)
        result = self.collector.list_plugins()
        overrides = result[0]["service_overrides"]
        assert set(overrides) == {"s3", "dynamodb"}

    def test_record_loaded_captures_custom_routes(self):
        plugin = _OverridesPlugin()
        self.collector.record_loaded(plugin, source="directory", load_time=0.02)
        result = self.collector.list_plugins()
        routes = result[0]["custom_routes"]
        assert "/_ext/overrides/status" in routes
        assert "/_ext/overrides/config" in routes

    def test_record_loaded_default_load_time(self):
        plugin = _NoHooksPlugin()
        self.collector.record_loaded(plugin, source="entrypoint")
        result = self.collector.list_plugins()
        assert result[0]["load_time"] == 0.0

    def test_multiple_plugins_listed(self):
        p1 = _NoHooksPlugin()
        p2 = _AllHooksPlugin()
        p3 = _OverridesPlugin()
        self.collector.record_loaded(p1, source="entrypoint")
        self.collector.record_loaded(p2, source="env_var")
        self.collector.record_loaded(p3, source="directory")
        result = self.collector.list_plugins()
        assert len(result) == 3
        names = {p["name"] for p in result}
        assert names == {"no-hooks", "all-hooks", "overrides"}

    def test_mixed_loaded_and_failed_plugins(self):
        good = _NoHooksPlugin()
        bad = _PartialHooksPlugin()
        bad.name = "bad-plugin"
        self.collector.record_loaded(good, source="entrypoint")
        self.collector.record_failed(bad, source="env_var", error="load failed")
        result = self.collector.list_plugins()
        assert len(result) == 2
        states = {p["name"]: p["state"] for p in result}
        assert states["no-hooks"] == "active"
        assert states["bad-plugin"] == "failed"

    def test_record_loaded_overwrites_previous_entry(self):
        """If a plugin with the same name is recorded again, it replaces."""
        plugin = _NoHooksPlugin()
        self.collector.record_loaded(plugin, source="entrypoint", load_time=0.01)
        self.collector.record_loaded(plugin, source="directory", load_time=0.05)
        result = self.collector.list_plugins()
        assert len(result) == 1
        assert result[0]["source"] == "directory"
        assert result[0]["load_time"] == 0.05

    def test_get_plugin_detail_returns_full_info(self):
        plugin = _OverridesPlugin()
        self.collector.record_loaded(plugin, source="entrypoint", load_time=0.03)
        detail = self.collector.get_plugin_detail("overrides")
        assert detail is not None
        assert detail["name"] == "overrides"
        assert detail["version"] == "3.0.0"
        assert detail["description"] == "Has overrides and routes"
        assert detail["source"] == "entrypoint"
        assert detail["state"] == "active"
        assert "s3" in detail["service_overrides"]

    def test_get_plugin_detail_for_failed_plugin(self):
        plugin = _NoHooksPlugin()
        self.collector.record_failed(plugin, source="directory", error="Import error")
        detail = self.collector.get_plugin_detail("no-hooks")
        assert detail is not None
        assert detail["state"] == "failed"
        assert detail["error"] == "Import error"

    def test_get_plugin_detail_not_found(self):
        detail = self.collector.get_plugin_detail("nonexistent")
        assert detail is None

    def test_record_failed_captures_all_info(self):
        plugin = _AllHooksPlugin()
        self.collector.record_failed(plugin, source="env_var", error="timeout during load")
        result = self.collector.list_plugins()
        assert len(result) == 1
        p = result[0]
        assert p["name"] == "all-hooks"
        assert p["version"] == "2.0.0"
        assert p["description"] == "Overrides everything"
        assert p["source"] == "env_var"
        assert p["state"] == "failed"
        assert p["error"] == "timeout during load"
        # Failed plugins don't have hooks detected
        assert p["hooks"] == []

    def test_record_failed_default_empty_error(self):
        plugin = _NoHooksPlugin()
        self.collector.record_failed(plugin, source="directory")
        result = self.collector.list_plugins()
        assert result[0]["error"] == ""


# ---------------------------------------------------------------------------
# Tests for get_plugin_status_collector singleton
# ---------------------------------------------------------------------------


class TestGetPluginStatusCollectorSingleton:
    def setup_method(self):
        plugin_status_mod._collector = None

    def teardown_method(self):
        plugin_status_mod._collector = None

    def test_returns_same_instance(self):
        c1 = get_plugin_status_collector()
        c2 = get_plugin_status_collector()
        assert c1 is c2

    def test_creates_instance_on_first_call(self):
        assert plugin_status_mod._collector is None
        c = get_plugin_status_collector()
        assert c is not None
        assert isinstance(c, PluginStatusCollector)

    def test_state_persists_across_calls(self):
        c1 = get_plugin_status_collector()
        plugin = _NoHooksPlugin()
        c1.record_loaded(plugin, source="test")
        c2 = get_plugin_status_collector()
        assert len(c2.list_plugins()) == 1
