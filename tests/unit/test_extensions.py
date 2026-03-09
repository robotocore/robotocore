"""Tests for the extension/plugin system."""

import json
from unittest.mock import MagicMock, patch

import pytest
from starlette.responses import JSONResponse, Response

from robotocore.extensions.base import RobotocorePlugin
from robotocore.extensions.compat import (
    LocalStackExtensionAdapter,
)
from robotocore.extensions.registry import (
    ExtensionRegistry,
    _discover_from_env,
    discover_extensions,
    get_extension_registry,
)

# --- Test plugins ---


class SamplePlugin(RobotocorePlugin):
    name = "sample"
    version = "1.0.0"
    description = "A sample plugin for testing"
    priority = 50

    def __init__(self):
        super().__init__()
        self.loaded = False
        self.started = False
        self.shut_down = False
        self.request_count = 0
        self.response_count = 0

    def on_load(self):
        self.loaded = True

    def on_startup(self):
        self.started = True

    def on_shutdown(self):
        self.shut_down = True

    def on_request(self, request, context):
        self.request_count += 1
        return None

    def on_response(self, request, response, context):
        self.response_count += 1
        return None


class ShortCircuitPlugin(RobotocorePlugin):
    name = "short-circuit"
    priority = 10

    def on_request(self, request, context):
        return Response(content=b"intercepted", status_code=200)


class ResponseModifierPlugin(RobotocorePlugin):
    name = "response-modifier"

    def on_response(self, request, response, context):
        return Response(
            content=b"modified",
            status_code=200,
            headers={"X-Modified": "true"},
        )


class ServiceOverridePlugin(RobotocorePlugin):
    name = "s3-override"

    def get_service_overrides(self):
        async def custom_s3(request, region, account_id):
            return JSONResponse({"custom": True})

        return {"s3": custom_s3}


class CustomRoutePlugin(RobotocorePlugin):
    name = "custom-routes"

    def get_custom_routes(self):
        async def status_handler(request):
            return JSONResponse({"status": "ok"})

        return [("/_ext/status", "GET", status_handler)]


class ErrorPlugin(RobotocorePlugin):
    name = "error-handler"

    def on_error(self, request, error, context):
        return Response(
            content=json.dumps({"error": str(error)}).encode(),
            status_code=500,
        )


class BrokenPlugin(RobotocorePlugin):
    name = "broken"

    def on_request(self, request, context):
        raise RuntimeError("Plugin error")


# --- Registry tests ---


class TestExtensionRegistry:
    def setup_method(self):
        self.registry = ExtensionRegistry()

    def test_register_plugin(self):
        plugin = SamplePlugin()
        self.registry.register(plugin)
        assert len(self.registry.plugins) == 1
        assert self.registry.plugins[0].name == "sample"

    def test_register_sets_name_from_class(self):
        plugin = RobotocorePlugin()
        self.registry.register(plugin)
        assert plugin.name == "RobotocorePlugin"

    def test_register_rejects_non_plugin(self):
        with pytest.raises(TypeError, match="Expected RobotocorePlugin"):
            self.registry.register("not a plugin")

    def test_register_rejects_duplicate(self):
        p1 = SamplePlugin()
        p2 = SamplePlugin()
        self.registry.register(p1)
        self.registry.register(p2)
        assert len(self.registry.plugins) == 1

    def test_plugins_sorted_by_priority(self):
        p1 = SamplePlugin()  # priority 50
        p2 = ShortCircuitPlugin()  # priority 10
        self.registry.register(p1)
        self.registry.register(p2)
        assert self.registry.plugins[0].name == "short-circuit"
        assert self.registry.plugins[1].name == "sample"

    def test_unregister_plugin(self):
        plugin = SamplePlugin()
        self.registry.register(plugin)
        assert self.registry.unregister("sample")
        assert len(self.registry.plugins) == 0

    def test_unregister_nonexistent(self):
        assert not self.registry.unregister("nonexistent")

    def test_unregister_clears_overrides(self):
        plugin = ServiceOverridePlugin()
        self.registry.register(plugin)
        assert self.registry.get_service_override("s3") is not None
        self.registry.unregister("s3-override")
        assert self.registry.get_service_override("s3") is None

    def test_service_override(self):
        plugin = ServiceOverridePlugin()
        self.registry.register(plugin)
        handler = self.registry.get_service_override("s3")
        assert handler is not None
        assert self.registry.get_service_override("sqs") is None

    def test_on_startup(self):
        plugin = SamplePlugin()
        self.registry.register(plugin)
        self.registry.on_startup()
        assert plugin.started

    def test_on_shutdown(self):
        plugin = SamplePlugin()
        self.registry.register(plugin)
        self.registry.on_shutdown()
        assert plugin.shut_down

    def test_on_request_passthrough(self):
        plugin = SamplePlugin()
        self.registry.register(plugin)
        req = MagicMock()
        result = self.registry.on_request(req, {})
        assert result is req  # Returns original request when plugin returns None
        assert plugin.request_count == 1

    def test_on_request_short_circuit(self):
        p1 = ShortCircuitPlugin()
        p2 = SamplePlugin()
        self.registry.register(p1)
        self.registry.register(p2)
        result = self.registry.on_request(MagicMock(), {})
        assert isinstance(result, Response)
        assert result.body == b"intercepted"
        # p2 should not have been called (p1 short-circuited)
        assert p2.request_count == 0

    def test_on_response_passthrough(self):
        plugin = SamplePlugin()
        self.registry.register(plugin)
        original = Response(content=b"original")
        result = self.registry.on_response(MagicMock(), original, {})
        assert result is original
        assert plugin.response_count == 1

    def test_on_response_modifier(self):
        plugin = ResponseModifierPlugin()
        self.registry.register(plugin)
        original = Response(content=b"original")
        result = self.registry.on_response(MagicMock(), original, {})
        assert result.body == b"modified"

    def test_on_error_handler(self):
        plugin = ErrorPlugin()
        self.registry.register(plugin)
        error = ValueError("test error")
        result = self.registry.on_error(MagicMock(), error, {})
        assert isinstance(result, Response)
        body = json.loads(result.body)
        assert body["error"] == "test error"

    def test_on_error_no_handler(self):
        plugin = SamplePlugin()
        self.registry.register(plugin)
        result = self.registry.on_error(MagicMock(), ValueError("test"), {})
        assert result is None

    def test_broken_plugin_doesnt_crash(self):
        plugin = BrokenPlugin()
        self.registry.register(plugin)
        # Should not raise, just log the error
        req = MagicMock()
        result = self.registry.on_request(req, {})
        assert result is req  # Returns original request even when plugin errors

    def test_custom_routes(self):
        plugin = CustomRoutePlugin()
        self.registry.register(plugin)
        routes = self.registry.get_custom_routes()
        assert len(routes) == 1
        assert routes[0][0] == "/_ext/status"
        assert routes[0][1] == "GET"

    def test_list_plugins(self):
        p1 = SamplePlugin()
        p2 = ServiceOverridePlugin()
        self.registry.register(p1)
        self.registry.register(p2)
        info = self.registry.list_plugins()
        assert len(info) == 2
        assert info[0]["name"] == "sample"
        assert info[0]["version"] == "1.0.0"
        assert info[1]["name"] == "s3-override"
        assert info[1]["service_overrides"] == ["s3"]


# --- Plugin base class tests ---


class TestRobotocorePlugin:
    def test_default_values(self):
        p = RobotocorePlugin()
        assert p.name == ""
        assert p.version == "0.0.0"
        assert p.priority == 100

    def test_lifecycle_methods_are_noops(self):
        p = RobotocorePlugin()
        p.on_load()
        p.on_startup()
        p.on_shutdown()
        assert p.on_request(MagicMock(), {}) is None
        assert p.on_response(MagicMock(), MagicMock(), {}) is None
        assert p.on_error(MagicMock(), Exception(), {}) is None

    def test_default_no_overrides(self):
        p = RobotocorePlugin()
        assert p.get_service_overrides() == {}
        assert p.get_custom_routes() == []

    def test_repr(self):
        p = SamplePlugin()
        assert "SamplePlugin" in repr(p)
        assert "sample" in repr(p)
        assert "1.0.0" in repr(p)


# --- LocalStack compatibility tests ---


class TestLocalStackExtensionAdapter:
    def test_wraps_extension(self):
        mock_ext = MagicMock()
        mock_ext.name = "ls-test"
        mock_ext.version = "2.0"
        adapter = LocalStackExtensionAdapter(mock_ext)
        assert adapter.name == "ls-test"
        assert "External extension" in adapter.description

    def test_on_startup_calls_platform_start_and_ready(self):
        mock_ext = MagicMock()
        adapter = LocalStackExtensionAdapter(mock_ext)
        adapter.on_startup()
        mock_ext.on_platform_start.assert_called_once()
        mock_ext.on_platform_ready.assert_called_once()

    def test_on_shutdown_calls_platform_shutdown(self):
        mock_ext = MagicMock()
        adapter = LocalStackExtensionAdapter(mock_ext)
        adapter.on_shutdown()
        mock_ext.on_platform_shutdown.assert_called_once()

    def test_on_request_delegates(self):
        mock_ext = MagicMock()
        mock_ext.on_request.return_value = None
        adapter = LocalStackExtensionAdapter(mock_ext)
        adapter.on_request(MagicMock(), {})
        mock_ext.on_request.assert_called_once()

    def test_on_response_delegates(self):
        mock_ext = MagicMock()
        mock_ext.on_response.return_value = None
        adapter = LocalStackExtensionAdapter(mock_ext)
        adapter.on_response(MagicMock(), MagicMock(), {})
        mock_ext.on_response.assert_called_once()

    def test_handles_missing_methods(self):
        ext = type("SimpleExt", (), {"name": "simple"})()
        adapter = LocalStackExtensionAdapter(ext)
        adapter.on_startup()  # no on_platform_start, should not crash
        adapter.on_shutdown()  # no on_platform_shutdown
        assert adapter.on_request(MagicMock(), {}) is None
        assert adapter.on_response(MagicMock(), MagicMock(), {}) is None

    def test_error_in_ls_extension_doesnt_crash(self):
        mock_ext = MagicMock()
        mock_ext.on_platform_start.side_effect = RuntimeError("boom")
        adapter = LocalStackExtensionAdapter(mock_ext)
        # Should not raise
        adapter.on_startup()

    def test_priority_is_lower(self):
        adapter = LocalStackExtensionAdapter(MagicMock())
        assert adapter.priority == 200  # Higher number = lower priority


# --- Discovery tests ---


class TestDiscovery:
    @patch("robotocore.extensions.registry._registry", None)
    def test_get_extension_registry_creates_singleton(self):
        r1 = get_extension_registry()
        r2 = get_extension_registry()
        assert r1 is r2

    @patch("robotocore.extensions.registry._registry", None)
    def test_discover_extensions_loads_once(self):
        registry = get_extension_registry()
        registry._loaded = True
        result = discover_extensions()
        assert isinstance(result, list)

    @patch.dict("os.environ", {"ROBOTOCORE_EXTENSIONS": ""})
    def test_discover_from_env_empty(self):
        result = _discover_from_env()
        assert result == []

    @patch.dict(
        "os.environ",
        {"ROBOTOCORE_EXTENSIONS": "nonexistent.module.path"},
    )
    def test_discover_from_env_bad_module(self):
        result = _discover_from_env()
        assert result == []
