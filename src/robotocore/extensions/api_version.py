"""Plugin API versioning for Robotocore extensions.

Provides version management so that internal API changes don't break plugins
silently. Plugins declare which API version they target; the registry checks
compatibility at load time and surfaces warnings/errors.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# The current plugin API version. Bump this when the plugin contract changes.
CURRENT_API_VERSION = "1.0"

# Set of all API versions that can still be loaded (may include deprecated ones).
SUPPORTED_VERSIONS: set[str] = {"1.0"}

# Deprecated versions map to a human-readable migration message.
DEPRECATED_VERSIONS: dict[str, str] = {}


@dataclass
class CompatResult:
    """Result of a plugin API version compatibility check."""

    compatible: bool
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class PluginAPIVersion:
    """Declares and validates plugin API compatibility."""

    @staticmethod
    def check_compatibility(plugin_version: str) -> CompatResult:
        """Check whether *plugin_version* is compatible with the running host.

        Returns a ``CompatResult`` with ``compatible=True`` when the version is
        in ``SUPPORTED_VERSIONS``.  Deprecated versions are compatible but carry
        a warning.  Unknown/unsupported versions are incompatible.
        """
        if not plugin_version:
            return CompatResult(
                compatible=False,
                errors=["Plugin did not declare an api_version"],
            )

        if plugin_version not in SUPPORTED_VERSIONS:
            return CompatResult(
                compatible=False,
                errors=[
                    f"Plugin API version '{plugin_version}' is not supported. "
                    f"Supported versions: {sorted(SUPPORTED_VERSIONS)}"
                ],
            )

        warnings: list[str] = []
        if plugin_version in DEPRECATED_VERSIONS:
            warnings.append(
                f"Plugin API version '{plugin_version}' is deprecated: "
                f"{DEPRECATED_VERSIONS[plugin_version]}"
            )

        return CompatResult(compatible=True, warnings=warnings)

    @staticmethod
    def get_migration_guide() -> list[dict]:
        """Return a list of migration entries for deprecated API versions."""
        entries: list[dict] = []
        for version, message in sorted(DEPRECATED_VERSIONS.items()):
            entries.append(
                {
                    "from_version": version,
                    "to_version": CURRENT_API_VERSION,
                    "message": message,
                }
            )
        return entries
