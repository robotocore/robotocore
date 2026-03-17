"""Third-party extension compatibility layer.

Provides a basic compatibility shim so that extensions written for other
AWS emulators can be loaded in Robotocore without modification.

Compatible extensions typically:
1. Register via entry points under `localstack.extensions`
2. Subclass an Extension base class
3. Override methods like `on_platform_start()`, `on_platform_ready()`
"""

from __future__ import annotations

import importlib
import logging

from robotocore.extensions.base import RobotocorePlugin

logger = logging.getLogger(__name__)


class ExternalExtensionAdapter(RobotocorePlugin):
    """Adapter that wraps a third-party extension as a RobotocorePlugin."""

    def __init__(self, ext):
        self._ext = ext
        self.name = getattr(ext, "name", type(ext).__name__)
        self.version = getattr(ext, "version", "0.0.0")
        self.description = f"External extension: {self.name}"
        self.priority = 200  # Lower priority than native plugins

    def on_startup(self) -> None:
        if hasattr(self._ext, "on_platform_start"):
            try:
                self._ext.on_platform_start()
            except Exception:
                logger.exception(f"Error in extension {self.name}.on_platform_start()")
                return  # Don't call on_platform_ready if start failed
        if hasattr(self._ext, "on_platform_ready"):
            try:
                self._ext.on_platform_ready()
            except Exception:
                logger.exception(f"Error in extension {self.name}.on_platform_ready()")

    def on_shutdown(self) -> None:
        if hasattr(self._ext, "on_platform_shutdown"):
            try:
                self._ext.on_platform_shutdown()
            except Exception:
                logger.exception(f"Error in extension {self.name}.on_platform_shutdown()")

    def on_request(self, request, context):
        if hasattr(self._ext, "on_request"):
            try:
                return self._ext.on_request(request, context)
            except Exception:
                logger.exception(f"Error in extension {self.name}.on_request()")
        return None

    def on_response(self, request, response, context):
        if hasattr(self._ext, "on_response"):
            try:
                return self._ext.on_response(request, response, context)
            except Exception:
                logger.exception(f"Error in extension {self.name}.on_response()")
        return None


# Keep old name as alias for backwards compatibility
LocalStackExtensionAdapter = ExternalExtensionAdapter


def discover_external_extensions() -> list[RobotocorePlugin]:
    """Discover third-party extensions from entry points."""
    plugins = []

    try:
        from importlib.metadata import entry_points

        # Scan the `localstack.extensions` entry point group for compat
        eps = entry_points(group="localstack.extensions")

        for ep in eps:
            try:
                ext_cls = ep.load()
                if isinstance(ext_cls, type):
                    ext = ext_cls()
                else:
                    ext = ext_cls

                adapter = ExternalExtensionAdapter(ext)
                plugins.append(adapter)
                logger.info(f"Loaded external extension: {ep.name} as {adapter}")
            except Exception:  # noqa: BLE001
                logger.debug(f"Could not load extension: {ep.name}")
    except Exception:  # noqa: BLE001
        logger.debug("No external extensions found")

    return plugins


# Keep old name as alias for backwards compatibility
discover_localstack_extensions = discover_external_extensions


def load_external_extension_module(
    module_path: str,
) -> RobotocorePlugin | None:
    """Load a specific third-party extension by module path."""
    try:
        mod = importlib.import_module(module_path)
        for attr_name in dir(mod):
            attr = getattr(mod, attr_name)
            if isinstance(attr, type) and attr_name not in ("__class__",):
                # Check if it looks like a platform extension
                if hasattr(attr, "on_platform_start") or hasattr(attr, "on_platform_ready"):
                    ext = attr()
                    return ExternalExtensionAdapter(ext)
    except Exception:
        logger.exception(f"Failed to load extension: {module_path}")

    return None


# Keep old name as alias for backwards compatibility
load_localstack_extension_module = load_external_extension_module
