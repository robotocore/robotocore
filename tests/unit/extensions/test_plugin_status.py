"""Unit tests for plugin status collection."""

from robotocore.extensions.base import RobotocorePlugin
from robotocore.extensions.plugin_status import PluginStatusCollector


class _TestPlugin(RobotocorePlugin):
    name = "test-plugin"
    version = "1.2.3"
    description = "A test plugin"

    def on_load(self):
        pass

    def on_startup(self):
        pass

    def on_request(self, request, context):
        return None

    def on_response(self, request, response, context):
        return None


class _FailedPlugin(RobotocorePlugin):
    name = "broken-plugin"
    version = "0.0.1"


class TestPluginStatusCollector:
    def setup_method(self):
        self.collector = PluginStatusCollector()

    def test_list_plugins_empty(self):
        result = self.collector.list_plugins()
        assert result == []

    def test_list_plugins_returns_loaded_plugin(self):
        plugin = _TestPlugin()
        self.collector.record_loaded(plugin, source="entrypoint", load_time=0.05)
        result = self.collector.list_plugins()
        assert len(result) == 1
        assert result[0]["name"] == "test-plugin"

    def test_plugin_info_includes_name_and_source(self):
        plugin = _TestPlugin()
        self.collector.record_loaded(plugin, source="env_var", load_time=0.01)
        result = self.collector.list_plugins()
        assert result[0]["name"] == "test-plugin"
        assert result[0]["source"] == "env_var"

    def test_plugin_info_includes_registered_hooks(self):
        plugin = _TestPlugin()
        self.collector.record_loaded(plugin, source="entrypoint", load_time=0.01)
        result = self.collector.list_plugins()
        hooks = result[0]["hooks"]
        assert "on_load" in hooks
        assert "on_startup" in hooks
        assert "on_request" in hooks
        assert "on_response" in hooks

    def test_plugin_info_includes_load_state(self):
        plugin = _TestPlugin()
        self.collector.record_loaded(plugin, source="entrypoint", load_time=0.01)
        result = self.collector.list_plugins()
        assert result[0]["state"] == "active"

    def test_failed_plugin_shows_error_message(self):
        plugin = _FailedPlugin()
        self.collector.record_failed(plugin, source="directory", error="Import error: foo")
        result = self.collector.list_plugins()
        assert result[0]["state"] == "failed"
        assert result[0]["error"] == "Import error: foo"

    def test_plugin_detail_for_specific_plugin(self):
        plugin = _TestPlugin()
        self.collector.record_loaded(plugin, source="entrypoint", load_time=0.05)
        detail = self.collector.get_plugin_detail("test-plugin")
        assert detail is not None
        assert detail["name"] == "test-plugin"
        assert detail["version"] == "1.2.3"
        assert detail["description"] == "A test plugin"
        assert detail["load_time"] == 0.05

    def test_plugin_detail_nonexistent_returns_none(self):
        detail = self.collector.get_plugin_detail("nonexistent")
        assert detail is None
