"""LocalStack extension compatibility layer.

Provides a basic compatibility shim so that simple LocalStack extensions
can be loaded in Robotocore without modification.

LocalStack extensions typically:
1. Register via entry points under `localstack.extensions`
2. Subclass `localstack.extensions.api.Extension`
3. Override methods like `on_platform_start()`, `on_platform_ready()`
"""

from __future__ import annotations

import importlib
import logging

from robotocore.extensions.base import RobotocorePlugin

logger = logging.getLogger(__name__)


class LocalStackExtensionAdapter(RobotocorePlugin):
    """Adapter that wraps a LocalStack Extension as a RobotocorePlugin."""

    def __init__(self, ls_extension):
        self._ext = ls_extension
        self.name = getattr(ls_extension, "name", type(ls_extension).__name__)
        self.version = getattr(ls_extension, "version", "0.0.0")
        self.description = f"LocalStack extension: {self.name}"
        self.priority = 200  # Lower priority than native plugins

    def on_startup(self) -> None:
        if hasattr(self._ext, "on_platform_start"):
            try:
                self._ext.on_platform_start()
            except Exception:
                logger.exception(f"Error in LS extension {self.name}.on_platform_start()")
                return  # Don't call on_platform_ready if start failed
        if hasattr(self._ext, "on_platform_ready"):
            try:
                self._ext.on_platform_ready()
            except Exception:
                logger.exception(f"Error in LS extension {self.name}.on_platform_ready()")

    def on_shutdown(self) -> None:
        if hasattr(self._ext, "on_platform_shutdown"):
            try:
                self._ext.on_platform_shutdown()
            except Exception:
                logger.exception(f"Error in LS extension {self.name}.on_platform_shutdown()")

    def on_request(self, request, context):
        if hasattr(self._ext, "on_request"):
            try:
                return self._ext.on_request(request, context)
            except Exception:
                logger.exception(f"Error in LS extension {self.name}.on_request()")
        return None

    def on_response(self, request, response, context):
        if hasattr(self._ext, "on_response"):
            try:
                return self._ext.on_response(request, response, context)
            except Exception:
                logger.exception(f"Error in LS extension {self.name}.on_response()")
        return None


def discover_localstack_extensions() -> list[RobotocorePlugin]:
    """Discover LocalStack extensions from entry points."""
    plugins = []

    try:
        from importlib.metadata import entry_points

        eps = entry_points(group="localstack.extensions")

        for ep in eps:
            try:
                ext_cls = ep.load()
                if isinstance(ext_cls, type):
                    ext = ext_cls()
                else:
                    ext = ext_cls

                adapter = LocalStackExtensionAdapter(ext)
                plugins.append(adapter)
                logger.info(f"Loaded LocalStack extension: {ep.name} as {adapter}")
            except Exception:
                logger.debug(f"Could not load LocalStack extension: {ep.name}")
    except Exception:
        logger.debug("No LocalStack extensions found")

    return plugins


def load_localstack_extension_module(
    module_path: str,
) -> RobotocorePlugin | None:
    """Load a specific LocalStack extension by module path."""
    try:
        mod = importlib.import_module(module_path)
        for attr_name in dir(mod):
            attr = getattr(mod, attr_name)
            if isinstance(attr, type) and attr_name not in ("__class__",):
                # Check if it looks like a LocalStack Extension
                if hasattr(attr, "on_platform_start") or hasattr(attr, "on_platform_ready"):
                    ext = attr()
                    return LocalStackExtensionAdapter(ext)
    except Exception:
        logger.exception(f"Failed to load LS extension: {module_path}")

    return None
