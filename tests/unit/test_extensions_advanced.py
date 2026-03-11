"""Advanced tests for the extension system: directory discovery,
env var discovery, error handling, lifecycle hooks."""

import os
from unittest.mock import MagicMock, patch

from starlette.responses import Response

from robotocore.extensions.base import RobotocorePlugin
from robotocore.extensions.registry import (
    ExtensionRegistry,
    _discover_from_directory,
    _discover_from_env,
)


class TestDiscoverFromDirectory:
    def test_directory_with_plugin_file(self, tmp_path):
        plugin_file = tmp_path / "my_plugin.py"
        plugin_file.write_text(
            """
from robotocore.extensions.base import RobotocorePlugin

class MyPlugin(RobotocorePlugin):
    name = "my-dir-plugin"
    version = "1.0"
"""
        )
        plugins = _discover_from_directory(str(tmp_path))
        assert len(plugins) == 1
        assert plugins[0].name == "my-dir-plugin"

    def test_directory_nonexistent(self):
        plugins = _discover_from_directory("/nonexistent/path/xyz")
        assert plugins == []

    def test_directory_skips_underscored_files(self, tmp_path):
        (tmp_path / "__init__.py").write_text("")
        (tmp_path / "_helper.py").write_text(
            """
from robotocore.extensions.base import RobotocorePlugin
class Helper(RobotocorePlugin):
    name = "helper"
"""
        )
        plugins = _discover_from_directory(str(tmp_path))
        assert len(plugins) == 0

    def test_directory_handles_import_error(self, tmp_path):
        bad_file = tmp_path / "bad_plugin.py"
        bad_file.write_text("raise ImportError('broken')")
        # Should not crash
        plugins = _discover_from_directory(str(tmp_path))
        assert plugins == []

    def test_directory_with_multiple_plugins(self, tmp_path):
        for i in range(3):
            (tmp_path / f"plugin_{i}.py").write_text(
                f"""
from robotocore.extensions.base import RobotocorePlugin
class Plugin{i}(RobotocorePlugin):
    name = "plugin-{i}"
"""
            )
        plugins = _discover_from_directory(str(tmp_path))
        assert len(plugins) == 3
        names = {p.name for p in plugins}
        assert names == {"plugin-0", "plugin-1", "plugin-2"}


class TestDiscoverFromEnv:
    def test_empty_env_returns_empty(self):
        with patch.dict(os.environ, {"ROBOTOCORE_EXTENSIONS": ""}):
            plugins = _discover_from_env()
        assert plugins == []

    def test_no_env_returns_empty(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("ROBOTOCORE_EXTENSIONS", None)
            plugins = _discover_from_env()
        assert plugins == []

    def test_invalid_module_path_handled(self):
        with patch.dict(os.environ, {"ROBOTOCORE_EXTENSIONS": "nonexistent.module.path"}):
            # Should not crash
            plugins = _discover_from_env()
        assert plugins == []

    def test_whitespace_module_paths_handled(self):
        with patch.dict(os.environ, {"ROBOTOCORE_EXTENSIONS": " , , "}):
            plugins = _discover_from_env()
        assert plugins == []


class TestExtensionRegistryOnErrorHook:
    def test_on_error_first_plugin_handles(self):
        reg = ExtensionRegistry()

        class ErrorHandler(RobotocorePlugin):
            name = "err-handler"

            def on_error(self, request, error, context):
                return Response(content="handled", status_code=500)

        reg.register(ErrorHandler())
        result = reg.on_error(MagicMock(), RuntimeError("boom"), {})
        assert isinstance(result, Response)
        assert result.body == b"handled"

    def test_on_error_plugin_exception_swallowed(self):
        reg = ExtensionRegistry()

        class BadErrorHandler(RobotocorePlugin):
            name = "bad-err"

            def on_error(self, request, error, context):
                raise RuntimeError("handler itself crashed")

        reg.register(BadErrorHandler())
        # Should not raise
        result = reg.on_error(MagicMock(), ValueError("original"), {})
        assert result is None

    def test_on_error_multiple_plugins_first_wins(self):
        reg = ExtensionRegistry()

        class First(RobotocorePlugin):
            name = "first"
            priority = 10

            def on_error(self, request, error, context):
                return Response(content="first handler", status_code=500)

        class Second(RobotocorePlugin):
            name = "second"
            priority = 20

            def on_error(self, request, error, context):
                return Response(content="second handler", status_code=500)

        reg.register(First())
        reg.register(Second())
        result = reg.on_error(MagicMock(), ValueError("err"), {})
        assert result.body == b"first handler"


class TestExtensionRegistryOnRequestHook:
    def test_on_request_returns_modified_request(self):
        reg = ExtensionRegistry()

        class Modifier(RobotocorePlugin):
            name = "modifier"

            def on_request(self, request, context):
                modified = MagicMock()
                modified.modified = True
                return modified

        reg.register(Modifier())
        result = reg.on_request(MagicMock(), {})
        assert result.modified is True

    def test_on_request_plugin_error_swallowed(self):
        reg = ExtensionRegistry()

        class Crasher(RobotocorePlugin):
            name = "crasher"

            def on_request(self, request, context):
                raise RuntimeError("crash!")

        reg.register(Crasher())
        req = MagicMock()
        # Should not raise
        result = reg.on_request(req, {})
        assert result is req  # Original request returned


class TestExtensionRegistryOnResponseHook:
    def test_on_response_multiple_plugins_chain(self):
        """Multiple plugins can each modify the response."""
        reg = ExtensionRegistry()

        class AddHeader1(RobotocorePlugin):
            name = "header1"
            priority = 10

            def on_response(self, request, response, context):
                response.headers["X-Plugin-1"] = "true"
                return response

        class AddHeader2(RobotocorePlugin):
            name = "header2"
            priority = 20

            def on_response(self, request, response, context):
                response.headers["X-Plugin-2"] = "true"
                return response

        reg.register(AddHeader1())
        reg.register(AddHeader2())

        response = Response(content="ok")
        result = reg.on_response(MagicMock(), response, {})
        assert result.headers.get("X-Plugin-1") == "true"
        assert result.headers.get("X-Plugin-2") == "true"


class TestExtensionRegistryOnShutdownError:
    def test_on_shutdown_error_doesnt_crash(self):
        reg = ExtensionRegistry()

        class Crasher(RobotocorePlugin):
            name = "crasher"

            def on_shutdown(self):
                raise RuntimeError("shutdown crash")

        reg.register(Crasher())
        # Should not raise
        reg.on_shutdown()

    def test_on_shutdown_error_doesnt_prevent_other_plugins(self):
        order = []
        reg = ExtensionRegistry()

        class Good(RobotocorePlugin):
            name = "good"
            priority = 20

            def on_shutdown(self):
                order.append("good")

        class Bad(RobotocorePlugin):
            name = "bad"
            priority = 10  # runs second in reverse order since priority=10 < 20

            def on_shutdown(self):
                order.append("bad")
                raise RuntimeError("crash")

        reg.register(Good())
        reg.register(Bad())
        reg.on_shutdown()
        # Both should have been called (reverse order: good first since higher priority)
        assert "good" in order
        assert "bad" in order


class TestExtensionRegistryGetCustomRoutes:
    def test_empty_registry_returns_no_routes(self):
        reg = ExtensionRegistry()
        assert reg.get_custom_routes() == []

    def test_multiple_plugins_routes_merged(self):
        reg = ExtensionRegistry()

        class Plugin1(RobotocorePlugin):
            name = "p1"

            def get_custom_routes(self):
                return [("/_ext/p1/status", "GET", lambda: "ok")]

        class Plugin2(RobotocorePlugin):
            name = "p2"

            def get_custom_routes(self):
                return [
                    ("/_ext/p2/a", "GET", lambda: "a"),
                    ("/_ext/p2/b", "POST", lambda: "b"),
                ]

        reg.register(Plugin1())
        reg.register(Plugin2())
        routes = reg.get_custom_routes()
        assert len(routes) == 3
        paths = [r[0] for r in routes]
        assert "/_ext/p1/status" in paths
        assert "/_ext/p2/a" in paths
        assert "/_ext/p2/b" in paths

    def test_plugin_route_error_handled(self):
        reg = ExtensionRegistry()

        class BadRoutes(RobotocorePlugin):
            name = "bad-routes"

            def get_custom_routes(self):
                raise RuntimeError("route error")

        reg.register(BadRoutes())
        # Should not raise, just returns empty
        routes = reg.get_custom_routes()
        assert routes == []
