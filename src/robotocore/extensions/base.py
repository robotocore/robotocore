"""Base plugin class for Robotocore extensions."""

from __future__ import annotations

from starlette.requests import Request
from starlette.responses import Response


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

            def on_request(self, request, context):
                # Log or modify requests
                pass

            def get_service_overrides(self):
                return {"s3": my_custom_s3_handler}
    """

    name: str = ""
    version: str = "0.0.0"
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

    def __repr__(self) -> str:
        return f"<{type(self).__name__} name={self.name!r} v{self.version}>"
