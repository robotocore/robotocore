"""Extension discovery, registration, and lifecycle management."""

from __future__ import annotations

import importlib
import logging
import os
from pathlib import Path

from robotocore.extensions.api_version import CompatResult, PluginAPIVersion
from robotocore.extensions.base import RobotocorePlugin

logger = logging.getLogger(__name__)

_registry: ExtensionRegistry | None = None


class ExtensionRegistry:
    """Manages all loaded extensions."""

    def __init__(self):
        self.plugins: list[RobotocorePlugin] = []
        self._service_overrides: dict[str, callable] = {}
        self._loaded = False
        # Track version compatibility results per plugin name
        self._compat_results: dict[str, CompatResult] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, plugin: RobotocorePlugin) -> None:
        """Register a plugin instance.

        Checks API version compatibility before accepting the plugin.
        Incompatible plugins are rejected with a warning.
        """
        if not isinstance(plugin, RobotocorePlugin):
            raise TypeError(f"Expected RobotocorePlugin, got {type(plugin).__name__}")
        if not plugin.name:
            plugin.name = type(plugin).__name__

        # Check for duplicate names
        existing = [p for p in self.plugins if p.name == plugin.name]
        if existing:
            logger.warning(f"Plugin '{plugin.name}' already registered, skipping")
            return

        # API version compatibility check
        compat = PluginAPIVersion.check_compatibility(plugin.api_version)
        self._compat_results[plugin.name] = compat

        if not compat.compatible:
            logger.warning(
                f"Plugin '{plugin.name}' has incompatible API version "
                f"'{plugin.api_version}': {compat.errors}"
            )
            return

        for warning in compat.warnings:
            logger.warning(f"Plugin '{plugin.name}': {warning}")

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
                self._compat_results.pop(name, None)
                logger.info(f"Unregistered extension: {name}")
                return True
        return False

    def get_service_override(self, service_name: str) -> callable | None:
        """Get a service handler override, if any plugin provides one."""
        return self._service_overrides.get(service_name)

    # ------------------------------------------------------------------
    # Lifecycle hooks
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Capability queries
    # ------------------------------------------------------------------

    def plugins_with_capability(self, capability: str) -> list[RobotocorePlugin]:
        """Return all plugins that declare *capability*."""
        return [p for p in self.plugins if capability in p.get_capabilities()]

    # ------------------------------------------------------------------
    # Dependency resolution
    # ------------------------------------------------------------------

    @staticmethod
    def resolve_load_order(
        plugins: list[RobotocorePlugin],
    ) -> tuple[list[RobotocorePlugin], list[tuple[str, str]]]:
        """Sort *plugins* so that dependencies are loaded first.

        Returns ``(ordered_plugins, warnings)`` where *warnings* is a list of
        ``(plugin_name, message)`` tuples for missing dependencies or cycles.
        Plugins with unsatisfied dependencies are included at the end with a
        warning (not silently dropped).
        """
        by_name: dict[str, RobotocorePlugin] = {}
        for p in plugins:
            name = p.name or type(p).__name__
            by_name[name] = p

        warnings: list[tuple[str, str]] = []
        ordered: list[str] = []
        visited: set[str] = set()
        in_stack: set[str] = set()  # for cycle detection

        def visit(name: str) -> None:
            if name in visited:
                return
            if name in in_stack:
                warnings.append((name, f"Circular dependency detected involving '{name}'"))
                return
            in_stack.add(name)

            plugin = by_name.get(name)
            if plugin is None:
                return

            deps = getattr(plugin, "dependencies", [])
            for dep in deps:
                if dep not in by_name:
                    warnings.append((name, f"Missing dependency '{dep}' for plugin '{name}'"))
                else:
                    visit(dep)

            in_stack.discard(name)
            if name not in visited:
                visited.add(name)
                ordered.append(name)

        for name in by_name:
            visit(name)

        result = [by_name[n] for n in ordered if n in by_name]
        return result, warnings

    # ------------------------------------------------------------------
    # Info / status
    # ------------------------------------------------------------------

    def list_plugins(self) -> list[dict]:
        """Return info about all loaded plugins."""
        result = []
        for p in self.plugins:
            info: dict = {
                "name": p.name,
                "version": p.version,
                "api_version": p.api_version,
                "description": p.description,
                "priority": p.priority,
                "capabilities": sorted(p.get_capabilities()),
                "dependencies": getattr(p, "dependencies", []),
                "service_overrides": list(p.get_service_overrides().keys()),
            }
            compat = self._compat_results.get(p.name)
            if compat:
                info["api_compat"] = {
                    "compatible": compat.compatible,
                    "warnings": compat.warnings,
                    "errors": compat.errors,
                }
            result.append(info)
        return result

    def get_dependency_graph(self) -> dict[str, list[str]]:
        """Return a mapping of plugin name -> list of dependency names."""
        return {p.name: getattr(p, "dependencies", []) for p in self.plugins}


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
    ep_plugins = _discover_entry_points()
    for p in ep_plugins:
        p._discovery_source = "entrypoint"
    plugins.extend(ep_plugins)

    # 2. Environment variable
    env_plugins = _discover_from_env()
    for p in env_plugins:
        p._discovery_source = "env_var"
    plugins.extend(env_plugins)

    # 3. System directory
    sys_plugins = _discover_from_directory("/etc/robotocore/extensions")
    for p in sys_plugins:
        p._discovery_source = "directory"
    plugins.extend(sys_plugins)

    # 4. User directory
    home = Path.home() / ".robotocore" / "extensions"
    user_plugins = _discover_from_directory(str(home))
    for p in user_plugins:
        p._discovery_source = "directory"
    plugins.extend(user_plugins)

    # Resolve dependency order
    ordered, dep_warnings = ExtensionRegistry.resolve_load_order(plugins)
    for plugin_name, msg in dep_warnings:
        logger.warning(f"Dependency issue for '{plugin_name}': {msg}")

    # Register all discovered plugins and track status
    from robotocore.extensions.plugin_status import get_plugin_status_collector

    collector = get_plugin_status_collector()

    for plugin in ordered:
        source = getattr(plugin, "_discovery_source", "unknown")
        try:
            start = __import__("time").monotonic()
            plugin.on_load()
            registry.register(plugin)
            load_time = __import__("time").monotonic() - start
            collector.record_loaded(plugin, source=source, load_time=load_time)
        except Exception:
            logger.exception(f"Failed to load plugin: {plugin}")
            collector.record_failed(plugin, source=source, error=str(plugin))

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
    except Exception:  # noqa: BLE001
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
