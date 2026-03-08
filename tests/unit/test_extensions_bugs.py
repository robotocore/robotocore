"""Failing tests for bugs found in the extension/plugin system.

Each test documents a specific bug. All tests are expected to FAIL against the
current codebase.
"""

from __future__ import annotations

import os
import sys
import textwrap
from unittest.mock import MagicMock, patch

from starlette.requests import Request

from robotocore.extensions.base import RobotocorePlugin
from robotocore.extensions.compat import (
    LocalStackExtensionAdapter,
    load_localstack_extension_module,
)
from robotocore.extensions.registry import (
    ExtensionRegistry,
    _discover_from_env,
)

# ---------------------------------------------------------------------------
# Helper plugins
# ---------------------------------------------------------------------------


class RequestModifierPlugin(RobotocorePlugin):
    """Plugin that returns a modified Request (not a Response)."""

    name = "request-modifier"

    def on_request(self, request, context):
        # Simulate modifying the request by attaching extra state
        request.state.modified_by = "request-modifier"
        return request


# ---------------------------------------------------------------------------
# Bug 1: _discover_from_env only finds the FIRST RobotocorePlugin subclass
#         when scanning a module (it breaks after the first match).
#         _discover_from_directory finds ALL subclasses. This is inconsistent
#         and means env-var-based discovery silently drops plugins.
# ---------------------------------------------------------------------------


class TestEnvDiscoveryFindsAllPlugins:
    def test_env_discovery_finds_multiple_plugins_in_one_module(self, tmp_path):
        """A module with two RobotocorePlugin subclasses should yield both."""
        mod_file = tmp_path / "multi_plugin.py"
        mod_file.write_text(
            textwrap.dedent("""\
            from robotocore.extensions.base import RobotocorePlugin

            class FirstPlugin(RobotocorePlugin):
                name = "first"

            class SecondPlugin(RobotocorePlugin):
                name = "second"
            """)
        )

        # Add tmp_path to sys.path so importlib can find it
        sys.path.insert(0, str(tmp_path))
        try:
            with patch.dict(os.environ, {"ROBOTOCORE_EXTENSIONS": "multi_plugin"}):
                plugins = _discover_from_env()
                names = {p.name for p in plugins}
                # BUG: only "first" (or whichever dir() yields first) is found
                # because of `break` on line 252 of registry.py
                assert "first" in names
                assert "second" in names, (
                    "env discovery should find ALL plugin subclasses, not just the first"
                )
        finally:
            sys.path.pop(0)
            sys.modules.pop("multi_plugin", None)


# ---------------------------------------------------------------------------
# Bug 4: on_request loses modified Request objects.
#         When a plugin returns a modified Request (not a Response), the
#         registry updates its local `request` variable but then returns None
#         at the end. The caller never sees the modified request.
# ---------------------------------------------------------------------------


class TestOnRequestReturnsModifiedRequest:
    def test_modified_request_is_returned_to_caller(self):
        """If a plugin modifies and returns the request, the registry should
        return it so the caller can use the modified version."""
        registry = ExtensionRegistry()
        modifier = RequestModifierPlugin()
        modifier.priority = 10
        registry.register(modifier)

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [],
            "query_string": b"",
        }
        request = Request(scope)
        request.state.modified_by = None

        result = registry.on_request(request, {})

        # BUG: result is None because registry.on_request always returns None
        # when no plugin returns a Response. The modified request is lost.
        assert result is not None, "on_request should return the modified request, not None"


# ---------------------------------------------------------------------------
# Bug 6: load_localstack_extension_module skips classes named "Extension".
#         The filter `attr_name != "Extension"` on line 104 of compat.py
#         means a user who names their class "Extension" (the most natural
#         name when subclassing localstack.extensions.api.Extension) will
#         have it silently ignored.
# ---------------------------------------------------------------------------


class TestLocalStackExtensionNamedExtension:
    def test_class_named_extension_is_loaded(self, tmp_path):
        """A class literally named 'Extension' should still be loadable."""
        mod_file = tmp_path / "my_ls_ext.py"
        mod_file.write_text(
            textwrap.dedent("""\
            class Extension:
                name = "my-extension"

                def on_platform_start(self):
                    pass

                def on_platform_ready(self):
                    pass
            """)
        )

        sys.path.insert(0, str(tmp_path))
        try:
            result = load_localstack_extension_module("my_ls_ext")
            # BUG: result is None because the class is named "Extension"
            assert result is not None, (
                "A class named 'Extension' with LocalStack lifecycle methods "
                "should be loaded, not skipped"
            )
        finally:
            sys.path.pop(0)
            sys.modules.pop("my_ls_ext", None)


# ---------------------------------------------------------------------------
# Bug 7: LocalStackExtensionAdapter.on_startup calls on_platform_ready even
#         when on_platform_start raises. If the extension's ready logic
#         depends on start having succeeded, this causes cascading errors.
#         The adapter should skip on_platform_ready if on_platform_start
#         failed.
# ---------------------------------------------------------------------------


class TestLocalStackAdapterStartupOrdering:
    def test_platform_ready_not_called_if_platform_start_fails(self):
        """If on_platform_start() raises, on_platform_ready() should NOT
        be called since the platform isn't actually started."""
        mock_ext = MagicMock()
        mock_ext.name = "fragile"
        mock_ext.on_platform_start.side_effect = RuntimeError("start failed")

        adapter = LocalStackExtensionAdapter(mock_ext)
        adapter.on_startup()

        mock_ext.on_platform_start.assert_called_once()
        # BUG: on_platform_ready IS called even though start failed
        mock_ext.on_platform_ready.assert_not_called()


# ---------------------------------------------------------------------------
# Bug 8: _discover_from_env with `plugin` attribute that is not a
#         RobotocorePlugin. If a module has `plugin = "not a plugin"`,
#         the code blindly appends it without type-checking. This will
#         cause a TypeError later when registry.register() is called.
# ---------------------------------------------------------------------------


class TestEnvDiscoveryPluginAttributeTypeCheck:
    def test_non_plugin_plugin_attribute_is_rejected(self, tmp_path):
        """If mod.plugin is not a RobotocorePlugin, it should be skipped."""
        mod_file = tmp_path / "bad_plugin_attr.py"
        mod_file.write_text(
            textwrap.dedent("""\
            plugin = "I am not a plugin"
            """)
        )

        sys.path.insert(0, str(tmp_path))
        try:
            with patch.dict(os.environ, {"ROBOTOCORE_EXTENSIONS": "bad_plugin_attr"}):
                plugins = _discover_from_env()
                # BUG: the string "I am not a plugin" gets added to the list
                for p in plugins:
                    assert isinstance(p, RobotocorePlugin), (
                        f"Non-RobotocorePlugin object should not be in results: {p!r}"
                    )
        finally:
            sys.path.pop(0)
            sys.modules.pop("bad_plugin_attr", None)
