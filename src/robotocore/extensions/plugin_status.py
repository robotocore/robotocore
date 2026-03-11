"""Plugin status collector for Robotocore extensions.

Tracks plugin discovery, loading, and runtime state. Provides data for
the /_robotocore/plugins endpoints.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from robotocore.extensions.base import RobotocorePlugin

# Hooks that are considered "overridden" if the plugin's method differs from the base class
_LIFECYCLE_HOOKS = ("on_load", "on_startup", "on_shutdown", "on_request", "on_response", "on_error")


def _detect_hooks(plugin: RobotocorePlugin) -> list[str]:
    """Detect which lifecycle hooks a plugin has overridden."""
    hooks = []
    for hook_name in _LIFECYCLE_HOOKS:
        plugin_method = getattr(type(plugin), hook_name, None)
        base_method = getattr(RobotocorePlugin, hook_name, None)
        if plugin_method is not None and plugin_method is not base_method:
            hooks.append(hook_name)
    return hooks


@dataclass
class PluginInfo:
    name: str
    version: str
    description: str
    source: str  # "entrypoint", "env_var", "directory"
    state: str  # "active", "failed", "disabled"
    hooks: list[str] = field(default_factory=list)
    load_time: float = 0.0
    error: str | None = None
    service_overrides: list[str] = field(default_factory=list)
    custom_routes: list[str] = field(default_factory=list)
    config: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        d: dict = {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "source": self.source,
            "state": self.state,
            "hooks": self.hooks,
            "load_time": self.load_time,
            "service_overrides": self.service_overrides,
            "custom_routes": self.custom_routes,
        }
        if self.error is not None:
            d["error"] = self.error
        if self.config:
            d["config"] = self.config
        return d


class PluginStatusCollector:
    """Collects and serves plugin status information."""

    def __init__(self) -> None:
        self._plugins: dict[str, PluginInfo] = {}

    def record_loaded(
        self,
        plugin: RobotocorePlugin,
        source: str,
        load_time: float = 0.0,
    ) -> None:
        """Record a successfully loaded plugin."""
        hooks = _detect_hooks(plugin)
        overrides = list(plugin.get_service_overrides().keys())
        routes = [r[0] for r in plugin.get_custom_routes()]

        self._plugins[plugin.name] = PluginInfo(
            name=plugin.name,
            version=plugin.version,
            description=plugin.description,
            source=source,
            state="active",
            hooks=hooks,
            load_time=load_time,
            service_overrides=overrides,
            custom_routes=routes,
        )

    def record_failed(
        self,
        plugin: RobotocorePlugin,
        source: str,
        error: str = "",
    ) -> None:
        """Record a plugin that failed to load."""
        self._plugins[plugin.name] = PluginInfo(
            name=plugin.name,
            version=plugin.version,
            description=plugin.description,
            source=source,
            state="failed",
            error=error,
        )

    def list_plugins(self) -> list[dict]:
        """Return info about all tracked plugins."""
        return [info.to_dict() for info in self._plugins.values()]

    def get_plugin_detail(self, name: str) -> dict | None:
        """Return detailed info for a specific plugin, or None if not found."""
        info = self._plugins.get(name)
        if info is None:
            return None
        return info.to_dict()


# Global singleton
_collector: PluginStatusCollector | None = None


def get_plugin_status_collector() -> PluginStatusCollector:
    """Get or create the global plugin status collector."""
    global _collector
    if _collector is None:
        _collector = PluginStatusCollector()
    return _collector
