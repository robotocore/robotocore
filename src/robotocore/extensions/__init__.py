"""Robotocore extension system — third-party plugins and service overrides."""

from robotocore.extensions.api_version import (
    CURRENT_API_VERSION,
    SUPPORTED_VERSIONS,
    CompatResult,
    PluginAPIVersion,
)
from robotocore.extensions.base import PluginManifest, RobotocorePlugin
from robotocore.extensions.registry import (
    discover_extensions,
    get_extension_registry,
    register_extension,
)

__all__ = [
    "CURRENT_API_VERSION",
    "SUPPORTED_VERSIONS",
    "CompatResult",
    "PluginAPIVersion",
    "PluginManifest",
    "RobotocorePlugin",
    "discover_extensions",
    "get_extension_registry",
    "register_extension",
]
