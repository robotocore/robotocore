"""Unified timeline combining audit log and chaos events."""

import time

from starlette.requests import Request
from starlette.responses import JSONResponse


async def handle_timeline(request: Request) -> JSONResponse:
    """GET /_robotocore/timeline -- unified view of recent API calls + chaos events."""
    from robotocore.audit.log import get_audit_log

    limit = int(request.query_params.get("limit", "50"))
    service_filter = request.query_params.get("service")

    audit = get_audit_log()
    entries = audit.recent(limit=limit * 2, service=service_filter)

    # Annotate with category
    for entry in entries:
        error = entry.get("error") or ""
        if error.startswith("chaos_injection:"):
            entry["_category"] = "chaos"
        else:
            entry["_category"] = "api_call"

    entries = entries[:limit]

    return JSONResponse(
        {
            "entries": entries,
            "count": len(entries),
            "server_time": time.time(),
        }
    )
