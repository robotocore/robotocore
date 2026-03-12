"""Dashboard request handler.

Serves the single-page dashboard HTML at GET /_robotocore/dashboard.
The actual HTML content and endpoint logic live in app.py; this module
re-exports the handler for convenience and provides a thin wrapper.
"""

from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse

from robotocore.dashboard.app import dashboard_endpoint

__all__ = ["handle_dashboard_request", "dashboard_endpoint"]


async def handle_dashboard_request(request: Request) -> HTMLResponse | JSONResponse:
    """Handle a dashboard request -- delegates to dashboard_endpoint."""
    return await dashboard_endpoint(request)
