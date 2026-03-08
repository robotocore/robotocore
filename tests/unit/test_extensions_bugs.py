"""Tests for extension system bug fixes.

Validates fixes for real bugs found during code audit:
- _discover_from_env only found first plugin subclass (break after first match)
- mod.plugin attribute not type-checked before appending
- Class named "Extension" was skipped by load_localstack_extension_module
- on_platform_ready called even when on_platform_start raised
- on_request dropped modified request objects (returned None instead)
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


class RequestModifierPlugin(RobotocorePlugin):
    """Plugin that returns a modified Request (not a Response)."""

    name = "request-modifier"

    def on_request(self, request, context):
        request.state.modified_by = "request-modifier"
        return request


class TestEnvDiscoveryFindsAllPlugins:
    """Fixed: _discover_from_env now finds ALL plugin subclasses, not just the first."""

    def test_env_discovery_finds_multiple_plugins_in_one_module(self, tmp_path):
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

        sys.path.insert(0, str(tmp_path))
        try:
            with patch.dict(os.environ, {"ROBOTOCORE_EXTENSIONS": "multi_plugin"}):
                plugins = _discover_from_env()
                names = {p.name for p in plugins}
                assert "first" in names
                assert "second" in names
        finally:
            sys.path.pop(0)
            sys.modules.pop("multi_plugin", None)


class TestEnvDiscoveryPluginAttributeTypeCheck:
    """Fixed: mod.plugin is now type-checked before being added to results."""

    def test_non_plugin_plugin_attribute_is_rejected(self, tmp_path):
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
                for p in plugins:
                    assert isinstance(p, RobotocorePlugin)
        finally:
            sys.path.pop(0)
            sys.modules.pop("bad_plugin_attr", None)


class TestOnRequestReturnsModifiedRequest:
    """Fixed: on_request now returns the (possibly modified) request instead of None."""

    def test_modified_request_is_returned_to_caller(self):
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
        assert result is not None


class TestLocalStackExtensionNamedExtension:
    """Fixed: Classes named 'Extension' are no longer skipped."""

    def test_class_named_extension_is_loaded(self, tmp_path):
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
            assert result is not None
        finally:
            sys.path.pop(0)
            sys.modules.pop("my_ls_ext", None)


class TestLocalStackAdapterStartupOrdering:
    """Fixed: on_platform_ready is not called if on_platform_start raises."""

    def test_platform_ready_not_called_if_platform_start_fails(self):
        mock_ext = MagicMock()
        mock_ext.name = "fragile"
        mock_ext.on_platform_start.side_effect = RuntimeError("start failed")

        adapter = LocalStackExtensionAdapter(mock_ext)
        adapter.on_startup()

        mock_ext.on_platform_start.assert_called_once()
        mock_ext.on_platform_ready.assert_not_called()
