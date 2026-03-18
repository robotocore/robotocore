"""Native CloudWatch Synthetics provider.

Intercepts CreateCanary, StartCanary, StopCanary, and GetCanaryRuns to provide
actual canary script execution. All other operations are forwarded to Moto.
"""

import json
import logging
import re
from urllib.parse import unquote

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto
from robotocore.services.synthetics.executor import get_runs
from robotocore.services.synthetics.scheduler import get_canary_scheduler

logger = logging.getLogger(__name__)

# REST-JSON path patterns for synthetics
_CANARY_START = re.compile(r"^/canary/([^/]+)/start$")
_CANARY_STOP = re.compile(r"^/canary/([^/]+)/stop$")
_CANARY_RUNS = re.compile(r"^/canary/([^/]+)/runs$")


async def handle_synthetics_request(request: Request, region: str, account_id: str) -> Response:
    """Handle a CloudWatch Synthetics API request.

    Intercepts key operations for canary execution, forwards everything else to Moto.
    """
    path = request.url.path
    method = request.method.upper()

    # StartCanary: POST /canary/{name}/start
    m = _CANARY_START.match(path)
    if m and method == "POST":
        canary_name = unquote(m.group(1))
        return await _handle_start_canary(request, canary_name, region, account_id)

    # StopCanary: POST /canary/{name}/stop
    m = _CANARY_STOP.match(path)
    if m and method == "POST":
        canary_name = unquote(m.group(1))
        return await _handle_stop_canary(request, canary_name, region, account_id)

    # GetCanaryRuns: POST /canary/{name}/runs
    m = _CANARY_RUNS.match(path)
    if m and method == "POST":
        canary_name = unquote(m.group(1))
        return _handle_get_canary_runs(canary_name, region, account_id)

    # All other operations: forward to Moto
    return await forward_to_moto(request, "synthetics", account_id=account_id)


async def _handle_start_canary(
    request: Request, canary_name: str, region: str, account_id: str
) -> Response:
    """Start a canary: forward to Moto, then trigger immediate execution."""
    # Forward to Moto to update state
    moto_response = await forward_to_moto(request, "synthetics", account_id=account_id)

    # If Moto returned an error, pass it through
    if moto_response.status_code >= 400:
        return moto_response

    # Trigger immediate execution
    try:
        from moto.backends import get_backend

        backend = get_backend("synthetics")[account_id][region]
        canary = backend.canaries.get(canary_name)
        if canary:
            scheduler = get_canary_scheduler()
            result = scheduler.trigger_immediate(canary, account_id, region)
            logger.info(
                "Canary %s started: %s (%.1fms)",
                canary_name,
                result.status,
                result.duration_ms,
            )
    except Exception:
        logger.exception("Failed to trigger canary %s after start", canary_name)

    return moto_response


async def _handle_stop_canary(
    request: Request, canary_name: str, region: str, account_id: str
) -> Response:
    """Stop a canary: forward to Moto to update state."""
    return await forward_to_moto(request, "synthetics", account_id=account_id)


def _handle_get_canary_runs(canary_name: str, region: str, account_id: str) -> Response:
    """Return canary runs from our execution store."""
    # First check if canary exists via Moto
    try:
        from moto.backends import get_backend

        backend = get_backend("synthetics")[account_id][region]
        if canary_name not in backend.canaries:
            body = json.dumps(
                {
                    "__type": "ResourceNotFoundException",
                    "Message": f"Canary {canary_name} not found",
                }
            )
            return Response(content=body, status_code=404, media_type="application/json")
    except Exception as e:  # noqa: BLE001
        logger.debug("Canary lookup skipped (best-effort): %s", e)

    # Get runs from our executor store
    runs = get_runs(account_id, region, canary_name)
    run_dicts = [r.to_dict() for r in reversed(runs)]  # newest first

    body = json.dumps({"CanaryRuns": run_dicts})
    return Response(content=body, status_code=200, media_type="application/json")
