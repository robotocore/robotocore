"""Extension discovery, registration, and lifecycle management."""

from __future__ import annotations

import importlib
import logging
import os
from pathlib import Path

from robotocore.extensions.base import RobotocorePlugin

logger = logging.getLogger(__name__)

_registry: ExtensionRegistry | None = None


class ExtensionRegistry:
    """Manages all loaded extensions."""

    def __init__(self):
        self.plugins: list[RobotocorePlugin] = []
        self._service_overrides: dict[str, callable] = {}
        self._loaded = False

    def register(self, plugin: RobotocorePlugin) -> None:
        """Register a plugin instance."""
        if not isinstance(plugin, RobotocorePlugin):
            raise TypeError(f"Expected RobotocorePlugin, got {type(plugin).__name__}")
        if not plugin.name:
            plugin.name = type(plugin).__name__

        # Check for duplicate names
        existing = [p for p in self.plugins if p.name == plugin.name]
        if existing:
            logger.warning(f"Plugin '{plugin.name}' already registered, skipping")
            return

        self.plugins.append(plugin)
        self.plugins.sort(key=lambda p: p.priority)

        # Merge service overrides
        for service, handler in plugin.get_service_overrides().items():
            if service in self._service_overrides:
                logger.warning(
                    f"Service '{service}' override from '{plugin.name}' replaces existing override"
                )
            self._service_overrides[service] = handler

        logger.info(f"Registered extension: {plugin}")

    def unregister(self, name: str) -> bool:
        """Unregister a plugin by name."""
        for i, plugin in enumerate(self.plugins):
            if plugin.name == name:
                # Remove service overrides from this plugin
                for service in plugin.get_service_overrides():
                    self._service_overrides.pop(service, None)
                self.plugins.pop(i)
                logger.info(f"Unregistered extension: {name}")
                return True
        return False

    def get_service_override(self, service_name: str) -> callable | None:
        """Get a service handler override, if any plugin provides one."""
        return self._service_overrides.get(service_name)

    def on_startup(self) -> None:
        """Call on_startup for all plugins."""
        for plugin in self.plugins:
            try:
                plugin.on_startup()
            except Exception:
                logger.exception(f"Error in {plugin.name}.on_startup()")

    def on_shutdown(self) -> None:
        """Call on_shutdown for all plugins."""
        for plugin in reversed(self.plugins):
            try:
                plugin.on_shutdown()
            except Exception:
                logger.exception(f"Error in {plugin.name}.on_shutdown()")

    def on_request(self, request, context: dict):
        """Run all plugin request hooks. Returns Response to short-circuit."""
        for plugin in self.plugins:
            try:
                result = plugin.on_request(request, context)
                if result is not None:
                    from starlette.responses import Response

                    if isinstance(result, Response):
                        return result
                    request = result
            except Exception:
                logger.exception(f"Error in {plugin.name}.on_request()")
        return request

    def on_response(self, request, response, context: dict):
        """Run all plugin response hooks."""
        for plugin in self.plugins:
            try:
                result = plugin.on_response(request, response, context)
                if result is not None:
                    response = result
            except Exception:
                logger.exception(f"Error in {plugin.name}.on_response()")
        return response

    def on_error(self, request, error, context: dict):
        """Run all plugin error hooks. Returns Response to override."""
        for plugin in self.plugins:
            try:
                result = plugin.on_error(request, error, context)
                if result is not None:
                    return result
            except Exception:
                logger.exception(f"Error in {plugin.name}.on_error()")
        return None

    def get_custom_routes(self) -> list[tuple[str, str, callable]]:
        """Collect all custom routes from plugins."""
        routes = []
        for plugin in self.plugins:
            try:
                routes.extend(plugin.get_custom_routes())
            except Exception:
                logger.exception(f"Error getting routes from {plugin.name}")
        return routes

    def list_plugins(self) -> list[dict]:
        """Return info about all loaded plugins."""
        return [
            {
                "name": p.name,
                "version": p.version,
                "description": p.description,
                "priority": p.priority,
                "service_overrides": list(p.get_service_overrides().keys()),
            }
            for p in self.plugins
        ]


def get_extension_registry() -> ExtensionRegistry:
    """Get or create the global extension registry."""
    global _registry
    if _registry is None:
        _registry = ExtensionRegistry()
    return _registry


def register_extension(plugin: RobotocorePlugin) -> None:
    """Register a plugin with the global registry."""
    get_extension_registry().register(plugin)


def discover_extensions() -> list[RobotocorePlugin]:
    """Discover and load extensions from all sources.

    Sources (in order):
    1. Python entry points: `robotocore.extensions`
    2. ROBOTOCORE_EXTENSIONS env var (comma-separated module paths)
    3. /etc/robotocore/extensions/ directory
    4. ~/.robotocore/extensions/ directory
    """
    registry = get_extension_registry()
    if registry._loaded:
        return registry.plugins

    plugins = []

    # 1. Entry points
    plugins.extend(_discover_entry_points())

    # 2. Environment variable
    plugins.extend(_discover_from_env())

    # 3. System directory
    plugins.extend(_discover_from_directory("/etc/robotocore/extensions"))

    # 4. User directory
    home = Path.home() / ".robotocore" / "extensions"
    plugins.extend(_discover_from_directory(str(home)))

    # Register all discovered plugins
    for plugin in plugins:
        try:
            plugin.on_load()
            registry.register(plugin)
        except Exception:
            logger.exception(f"Failed to load plugin: {plugin}")

    registry._loaded = True
    return registry.plugins


def _discover_entry_points() -> list[RobotocorePlugin]:
    """Discover plugins via Python entry points."""
    plugins = []
    try:
        from importlib.metadata import entry_points

        eps = entry_points(group="robotocore.extensions")

        for ep in eps:
            try:
                plugin_cls = ep.load()
                if isinstance(plugin_cls, type) and issubclass(plugin_cls, RobotocorePlugin):
                    plugins.append(plugin_cls())
                elif isinstance(plugin_cls, RobotocorePlugin):
                    plugins.append(plugin_cls)
                else:
                    logger.warning(f"Entry point {ep.name} is not a RobotocorePlugin: {plugin_cls}")
            except Exception:
                logger.exception(f"Failed to load entry point: {ep.name}")
    except Exception:
        logger.debug("No entry points found for robotocore.extensions")

    return plugins


def _discover_from_env() -> list[RobotocorePlugin]:
    """Discover plugins from ROBOTOCORE_EXTENSIONS env var."""
    env = os.environ.get("ROBOTOCORE_EXTENSIONS", "")
    if not env:
        return []

    plugins = []
    for module_path in env.split(","):
        module_path = module_path.strip()
        if not module_path:
            continue
        try:
            mod = importlib.import_module(module_path)
            # Look for a `plugin` attribute or RobotocorePlugin subclass
            if hasattr(mod, "plugin"):
                obj = mod.plugin
                if isinstance(obj, type) and issubclass(obj, RobotocorePlugin):
                    plugins.append(obj())
                elif isinstance(obj, RobotocorePlugin):
                    plugins.append(obj)
                else:
                    logger.warning(
                        f"Module {module_path} has 'plugin' attribute "
                        f"that is not a RobotocorePlugin: {type(obj)}"
                    )
            else:
                # Scan module for RobotocorePlugin subclasses
                for attr_name in dir(mod):
                    attr = getattr(mod, attr_name)
                    if (
                        isinstance(attr, type)
                        and issubclass(attr, RobotocorePlugin)
                        and attr is not RobotocorePlugin
                    ):
                        plugins.append(attr())
        except Exception:
            logger.exception(f"Failed to load extension module: {module_path}")

    return plugins


def _discover_from_directory(dir_path: str) -> list[RobotocorePlugin]:
    """Discover plugins from a directory of Python files."""
    d = Path(dir_path)
    if not d.exists():
        return []

    plugins = []
    for py_file in sorted(d.glob("*.py")):
        if py_file.name.startswith("_"):
            continue
        try:
            spec = importlib.util.spec_from_file_location(f"robotocore_ext_{py_file.stem}", py_file)
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)

                for attr_name in dir(mod):
                    attr = getattr(mod, attr_name)
                    if (
                        isinstance(attr, type)
                        and issubclass(attr, RobotocorePlugin)
                        and attr is not RobotocorePlugin
                    ):
                        plugins.append(attr())
        except Exception:
            logger.exception(f"Failed to load extension file: {py_file}")

    return plugins
