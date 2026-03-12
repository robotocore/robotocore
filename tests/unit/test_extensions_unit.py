"""Unit tests for the extension system."""

from unittest.mock import MagicMock

from starlette.responses import Response

from robotocore.extensions.base import RobotocorePlugin
from robotocore.extensions.compat import LocalStackExtensionAdapter
from robotocore.extensions.registry import ExtensionRegistry


class DummyPlugin(RobotocorePlugin):
    name = "dummy"
    version = "1.0.0"
    description = "A test plugin"
    priority = 50


class OverridePlugin(RobotocorePlugin):
    name = "override"
    version = "0.1.0"

    def get_service_overrides(self):
        return {"custom-svc": lambda: "handled"}


class RequestInterceptPlugin(RobotocorePlugin):
    name = "interceptor"

    def on_request(self, request, context):
        return Response(content="intercepted", status_code=200)


class ResponseModPlugin(RobotocorePlugin):
    name = "response-mod"

    def on_response(self, request, response, context):
        return Response(content="modified", status_code=200)


class ErrorPlugin(RobotocorePlugin):
    name = "error-handler"

    def on_error(self, request, error, context):
        return Response(content="handled error", status_code=500)


class RoutePlugin(RobotocorePlugin):
    name = "routes"

    def get_custom_routes(self):
        return [("/_ext/test", "GET", lambda: "ok")]


# ─── RobotocorePlugin base class ─────────────────────────────────────────────


class TestRobotocorePlugin:
    def test_defaults(self):
        p = RobotocorePlugin()
        assert p.name == ""
        assert p.version == "0.0.0"
        assert p.priority == 100
        assert p.get_service_overrides() == {}
        assert p.get_custom_routes() == []

    def test_on_request_returns_none(self):
        p = RobotocorePlugin()
        assert p.on_request(MagicMock(), {}) is None

    def test_on_response_returns_none(self):
        p = RobotocorePlugin()
        assert p.on_response(MagicMock(), MagicMock(), {}) is None

    def test_on_error_returns_none(self):
        p = RobotocorePlugin()
        assert p.on_error(MagicMock(), Exception(), {}) is None

    def test_repr(self):
        p = DummyPlugin()
        r = repr(p)
        assert "DummyPlugin" in r
        assert "dummy" in r


# ─── ExtensionRegistry ───────────────────────────────────────────────────────


class TestExtensionRegistry:
    def test_register_plugin(self):
        reg = ExtensionRegistry()
        p = DummyPlugin()
        reg.register(p)
        assert len(reg.plugins) == 1
        assert reg.plugins[0].name == "dummy"

    def test_register_rejects_non_plugin(self):
        reg = ExtensionRegistry()
        try:
            reg.register("not a plugin")
            assert False, "Should raise TypeError"
        except TypeError:
            pass

    def test_register_auto_names_unnamed(self):
        reg = ExtensionRegistry()
        p = RobotocorePlugin()
        reg.register(p)
        assert p.name == "RobotocorePlugin"

    def test_duplicate_name_rejected(self):
        reg = ExtensionRegistry()
        reg.register(DummyPlugin())
        reg.register(DummyPlugin())  # should warn, not crash
        assert len(reg.plugins) == 1

    def test_priority_ordering(self):
        reg = ExtensionRegistry()
        high = DummyPlugin()
        high.name = "high"
        high.priority = 10
        low = DummyPlugin()
        low.name = "low"
        low.priority = 200
        reg.register(low)
        reg.register(high)
        assert reg.plugins[0].name == "high"
        assert reg.plugins[1].name == "low"

    def test_unregister(self):
        reg = ExtensionRegistry()
        reg.register(DummyPlugin())
        assert reg.unregister("dummy") is True
        assert len(reg.plugins) == 0

    def test_unregister_nonexistent(self):
        reg = ExtensionRegistry()
        assert reg.unregister("nonexistent") is False

    def test_service_override(self):
        reg = ExtensionRegistry()
        reg.register(OverridePlugin())
        handler = reg.get_service_override("custom-svc")
        assert handler is not None
        assert handler() == "handled"

    def test_service_override_none(self):
        reg = ExtensionRegistry()
        assert reg.get_service_override("s3") is None

    def test_unregister_clears_overrides(self):
        reg = ExtensionRegistry()
        reg.register(OverridePlugin())
        reg.unregister("override")
        assert reg.get_service_override("custom-svc") is None

    def test_on_startup_calls_all(self):
        reg = ExtensionRegistry()
        p1 = MagicMock(spec=RobotocorePlugin)
        p1.name = "p1"
        p1.priority = 100
        p1.api_version = "1.0"
        p1.get_service_overrides.return_value = {}
        p2 = MagicMock(spec=RobotocorePlugin)
        p2.name = "p2"
        p2.priority = 100
        p2.api_version = "1.0"
        p2.get_service_overrides.return_value = {}
        reg.register(p1)
        reg.register(p2)
        reg.on_startup()
        p1.on_startup.assert_called_once()
        p2.on_startup.assert_called_once()

    def test_on_shutdown_reverse_order(self):
        reg = ExtensionRegistry()
        order = []
        p1 = DummyPlugin()
        p1.name = "first"
        p1.priority = 10
        p1.on_shutdown = lambda: order.append("first")
        p2 = DummyPlugin()
        p2.name = "second"
        p2.priority = 20
        p2.on_shutdown = lambda: order.append("second")
        reg.register(p1)
        reg.register(p2)
        reg.on_shutdown()
        assert order == ["second", "first"]

    def test_on_request_short_circuit(self):
        reg = ExtensionRegistry()
        reg.register(RequestInterceptPlugin())
        result = reg.on_request(MagicMock(), {})
        assert isinstance(result, Response)
        assert result.body == b"intercepted"

    def test_on_request_no_plugins(self):
        reg = ExtensionRegistry()
        req = MagicMock()
        assert reg.on_request(req, {}) is req  # Returns original request

    def test_on_response_modifies(self):
        reg = ExtensionRegistry()
        reg.register(ResponseModPlugin())
        result = reg.on_response(MagicMock(), MagicMock(), {})
        assert result.body == b"modified"

    def test_on_error_returns_response(self):
        reg = ExtensionRegistry()
        reg.register(ErrorPlugin())
        result = reg.on_error(MagicMock(), Exception(), {})
        assert isinstance(result, Response)

    def test_on_error_no_handler(self):
        reg = ExtensionRegistry()
        assert reg.on_error(MagicMock(), Exception(), {}) is None

    def test_get_custom_routes(self):
        reg = ExtensionRegistry()
        reg.register(RoutePlugin())
        routes = reg.get_custom_routes()
        assert len(routes) == 1
        assert routes[0][0] == "/_ext/test"

    def test_list_plugins(self):
        reg = ExtensionRegistry()
        reg.register(DummyPlugin())
        plugins = reg.list_plugins()
        assert len(plugins) == 1
        assert plugins[0]["name"] == "dummy"
        assert plugins[0]["version"] == "1.0.0"

    def test_on_startup_error_doesnt_crash(self):
        reg = ExtensionRegistry()
        p = DummyPlugin()
        p.on_startup = MagicMock(side_effect=RuntimeError("boom"))
        reg.plugins = [p]
        # Should not raise
        reg.on_startup()


# ─── LocalStack compat adapter ───────────────────────────────────────────────


class TestLocalStackAdapter:
    def test_wraps_ls_extension(self):
        ls_ext = MagicMock()
        ls_ext.name = "ls-test"
        ls_ext.version = "2.0"
        adapter = LocalStackExtensionAdapter(ls_ext)
        assert adapter.name == "ls-test"
        assert adapter.priority == 200

    def test_on_startup_calls_platform_methods(self):
        ls_ext = MagicMock()
        ls_ext.name = "ls-test"
        adapter = LocalStackExtensionAdapter(ls_ext)
        adapter.on_startup()
        ls_ext.on_platform_start.assert_called_once()
        ls_ext.on_platform_ready.assert_called_once()

    def test_on_shutdown_calls_platform_shutdown(self):
        ls_ext = MagicMock()
        ls_ext.name = "ls-test"
        adapter = LocalStackExtensionAdapter(ls_ext)
        adapter.on_shutdown()
        ls_ext.on_platform_shutdown.assert_called_once()

    def test_on_request_delegates(self):
        ls_ext = MagicMock()
        ls_ext.name = "ls-test"
        ls_ext.on_request.return_value = Response(content="ok")
        adapter = LocalStackExtensionAdapter(ls_ext)
        result = adapter.on_request(MagicMock(), {})
        assert isinstance(result, Response)

    def test_on_request_no_method(self):
        ls_ext = MagicMock(spec=[])
        ls_ext.name = "ls-basic"
        adapter = LocalStackExtensionAdapter(ls_ext)
        assert adapter.on_request(MagicMock(), {}) is None

    def test_on_response_delegates(self):
        ls_ext = MagicMock()
        ls_ext.name = "ls-test"
        ls_ext.on_response.return_value = Response(content="modified")
        adapter = LocalStackExtensionAdapter(ls_ext)
        result = adapter.on_response(MagicMock(), MagicMock(), {})
        assert isinstance(result, Response)

    def test_on_startup_handles_error(self):
        ls_ext = MagicMock()
        ls_ext.name = "ls-err"
        ls_ext.on_platform_start.side_effect = RuntimeError("boom")
        adapter = LocalStackExtensionAdapter(ls_ext)
        # Should not raise
        adapter.on_startup()
