"""Base plugin class for Robotocore extensions."""

from __future__ import annotations

from dataclasses import dataclass, field

from starlette.requests import Request
from starlette.responses import Response


@dataclass
class PluginManifest:
    """Standardized metadata for a Robotocore plugin."""

    name: str
    version: str
    api_version: str
    description: str = ""
    author: str = ""
    capabilities: set[str] = field(default_factory=set)
    dependencies: list[str] = field(default_factory=list)
    config_schema: dict | None = None

    def validate(self) -> list[str]:
        """Return a list of validation errors (empty if valid)."""
        errors: list[str] = []
        if not self.name:
            errors.append("manifest.name is required")
        if not self.version:
            errors.append("manifest.version is required")
        if not self.api_version:
            errors.append("manifest.api_version is required")
        if not isinstance(self.capabilities, set):
            errors.append("manifest.capabilities must be a set")
        if not isinstance(self.dependencies, list):
            errors.append("manifest.dependencies must be a list")
        if self.config_schema is not None and not isinstance(self.config_schema, dict):
            errors.append("manifest.config_schema must be a dict or None")
        return errors

    def to_dict(self) -> dict:
        d: dict = {
            "name": self.name,
            "version": self.version,
            "api_version": self.api_version,
            "description": self.description,
            "author": self.author,
            "capabilities": sorted(self.capabilities),
            "dependencies": list(self.dependencies),
        }
        if self.config_schema is not None:
            d["config_schema"] = self.config_schema
        return d


class RobotocorePlugin:
    """Base class for all Robotocore extensions.

    Subclass this to create plugins that intercept requests/responses,
    override service providers, or add custom endpoints.

    Lifecycle:
        1. on_load() — called when the plugin is discovered and loaded
        2. on_startup() — called when the server starts
        3. on_request() / on_response() — called for each AWS request
        4. on_shutdown() — called when the server stops

    Example::

        class MyPlugin(RobotocorePlugin):
            name = "my-plugin"
            api_version = "1.0"

            def get_capabilities(self):
                return {"custom_routes", "state_hooks"}

            def on_request(self, request, context):
                # Log or modify requests
                pass

            def get_service_overrides(self):
                return {"s3": my_custom_s3_handler}
    """

    name: str = ""
    version: str = "0.0.0"
    api_version: str = "1.0"
    description: str = ""
    priority: int = 100  # Lower = earlier execution

    def on_load(self) -> None:
        """Called when the plugin is first loaded. Initialize state here."""

    def on_startup(self) -> None:
        """Called when the server starts (after all plugins are loaded)."""

    def on_shutdown(self) -> None:
        """Called when the server is shutting down."""

    def on_request(self, request: Request, context: dict) -> Request | Response | None:
        """Called before each AWS request is processed.

        Args:
            request: The incoming Starlette request.
            context: Dict with service_name, region, account_id, operation.

        Returns:
            - None: continue processing normally
            - Request: use modified request
            - Response: short-circuit and return this response
        """
        return None

    def on_response(self, request: Request, response: Response, context: dict) -> Response | None:
        """Called after each AWS request is processed.

        Args:
            request: The original request.
            response: The response about to be returned.
            context: Dict with service_name, region, account_id, operation.

        Returns:
            - None: use original response
            - Response: use modified response
        """
        return None

    def on_error(self, request: Request, error: Exception, context: dict) -> Response | None:
        """Called when request processing raises an exception.

        Returns:
            - None: use default error handling
            - Response: use this response instead
        """
        return None

    def get_service_overrides(self) -> dict:
        """Return a dict of service_name -> handler_function overrides.

        The handler function should have the signature:
            async def handler(request: Request, region: str, account_id: str)
                -> Response

        Returns:
            Dict mapping service names to handler functions.
        """
        return {}

    # ------------------------------------------------------------------
    # State lifecycle hooks (optional)
    # ------------------------------------------------------------------

    def on_before_state_save(self, context: dict) -> None:
        """Called before state is saved. Raise to abort the save."""

    def on_after_state_save(self, context: dict) -> None:
        """Called after state is saved successfully."""

    def on_before_state_load(self, context: dict) -> None:
        """Called before state is loaded. Raise to abort the load."""

    def on_after_state_load(self, context: dict) -> None:
        """Called after state is loaded successfully."""

    def on_before_state_reset(self, context: dict) -> None:
        """Called before state is reset. Raise to abort the reset."""

    def on_after_state_reset(self, context: dict) -> None:
        """Called after state is reset successfully."""

    def get_custom_routes(self) -> list[tuple[str, str, callable]]:
        """Return custom HTTP routes to register.

        Returns:
            List of (path, method, handler) tuples.
            Example: [("/_ext/my-plugin/status", "GET", my_status_handler)]
        """
        return []

    # ------------------------------------------------------------------
    # Versioned API hooks (new)
    # ------------------------------------------------------------------

    def on_api_version_change(self, old_version: str, new_version: str) -> None:
        """Called when the host API version changes (e.g. after a live upgrade).

        Plugins can use this to adapt behaviour or log a warning.
        """

    def get_capabilities(self) -> set[str]:
        """Return the set of capabilities this plugin provides.

        Well-known capabilities:
            - ``"custom_routes"``   — plugin adds HTTP routes
            - ``"state_hooks"``     — plugin hooks into state save/load/reset
            - ``"service_overrides"`` — plugin overrides AWS service handlers
            - ``"request_hooks"``   — plugin intercepts requests/responses

        Plugins may also declare custom capability strings.
        """
        return set()

    def get_config_schema(self) -> dict | None:
        """Return a JSON Schema dict describing this plugin's configuration.

        Return ``None`` if the plugin has no configurable options.
        """
        return None

    def validate_config(self, config: dict) -> list[str]:
        """Validate *config* against this plugin's schema.

        Returns a list of human-readable error strings (empty = valid).
        Default implementation does a basic check against ``get_config_schema()``
        if one is provided.
        """
        schema = self.get_config_schema()
        if schema is None:
            return []
        errors: list[str] = []
        required = schema.get("required", [])
        properties = schema.get("properties", {})
        for key in required:
            if key not in config:
                errors.append(f"Missing required config key: {key}")
        for key, value in config.items():
            if key in properties:
                prop = properties[key]
                expected = prop.get("type")
                if expected and not _json_type_matches(value, expected):
                    errors.append(
                        f"Config key '{key}': expected type '{expected}', "
                        f"got '{type(value).__name__}'"
                    )
        return errors

    def get_manifest(self) -> PluginManifest:
        """Build a ``PluginManifest`` from this plugin's attributes."""
        return PluginManifest(
            name=self.name,
            version=self.version,
            api_version=self.api_version,
            description=self.description,
            capabilities=self.get_capabilities(),
            dependencies=getattr(self, "dependencies", []),
            config_schema=self.get_config_schema(),
        )

    def __repr__(self) -> str:
        return f"<{type(self).__name__} name={self.name!r} v{self.version}>"


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

_JSON_TYPE_MAP: dict[str, type | tuple[type, ...]] = {
    "string": str,
    "number": (int, float),
    "integer": int,
    "boolean": bool,
    "array": list,
    "object": dict,
}


def _json_type_matches(value: object, json_type: str) -> bool:
    """Return True if *value* matches the JSON Schema *json_type*."""
    py_types = _JSON_TYPE_MAP.get(json_type)
    if py_types is None:
        return True  # unknown type — don't reject
    return isinstance(value, py_types)
