"""Semantic tests for plugin status — end-to-end with endpoint JSON structure."""

import json

from robotocore.extensions.base import RobotocorePlugin
from robotocore.extensions.plugin_status import get_plugin_status_collector


class _SamplePlugin(RobotocorePlugin):
    name = "sample"
    version = "2.0.0"
    description = "Sample plugin"

    def on_startup(self):
        pass

    def get_service_overrides(self):
        return {"s3": lambda r, reg, acc: None}


class TestPluginStatusIntegration:
    def setup_method(self):
        import robotocore.extensions.plugin_status as mod

        mod._collector = None

    def test_register_plugin_then_list(self):
        collector = get_plugin_status_collector()
        plugin = _SamplePlugin()
        collector.record_loaded(plugin, source="entrypoint", load_time=0.02)

        plugins = collector.list_plugins()
        assert len(plugins) == 1
        assert plugins[0]["name"] == "sample"
        assert plugins[0]["version"] == "2.0.0"
        assert plugins[0]["source"] == "entrypoint"
        assert plugins[0]["state"] == "active"

    def test_management_endpoint_json_structure(self):
        collector = get_plugin_status_collector()
        plugin = _SamplePlugin()
        collector.record_loaded(plugin, source="directory", load_time=0.05)

        plugins = collector.list_plugins()
        json_str = json.dumps({"plugins": plugins})
        parsed = json.loads(json_str)
        assert "plugins" in parsed
        p = parsed["plugins"][0]
        assert "name" in p
        assert "version" in p
        assert "source" in p
        assert "state" in p
        assert "hooks" in p
        assert "load_time" in p
        assert isinstance(p["hooks"], list)
        assert "service_overrides" in p
