"""Request/response handlers for the AWS gateway pipeline."""

import json
import logging
import re

from starlette.responses import Response

from robotocore.gateway.handler_chain import RequestContext
from robotocore.gateway.router import route_to_service
from robotocore.protocols.service_info import get_service_protocol

log = logging.getLogger(__name__)

_REGION_RE = re.compile(r"Credential=[^/]+/\d{8}/([^/]+)/")


# -- Request Handlers --


def parse_service_handler(context: RequestContext) -> None:
    """Populate service_name from the incoming request if not already set."""
    if not context.service_name:
        service = route_to_service(context.request)
        if service:
            context.service_name = service


def populate_context_handler(context: RequestContext) -> None:
    """Extract region, account_id, and protocol from the request."""
    headers = context.request.headers

    # Region from Authorization header or X-Amz-Credential query param (presigned URLs)
    auth = headers.get("authorization", "")
    match = _REGION_RE.search(auth)
    if match:
        context.region = match.group(1)
    else:
        credential = context.request.query_params.get("X-Amz-Credential", "")
        if credential:
            parts = credential.split("/")
            if len(parts) >= 3:
                context.region = parts[2]

    # Protocol from botocore service specs
    if context.service_name:
        protocol = get_service_protocol(context.service_name)
        if protocol:
            context.protocol = protocol

    # Operation from X-Amz-Target or Action param
    target = headers.get("x-amz-target", "")
    if target and "." in target:
        context.operation = target.split(".")[-1]
    else:
        action = context.request.query_params.get("Action")
        if action:
            context.operation = action


def cors_handler(context: RequestContext) -> None:
    """Handle CORS preflight and set CORS headers on OPTIONS requests."""
    if context.request.method == "OPTIONS":
        context.response = Response(
            status_code=200,
            headers=_cors_headers(),
        )


def _cors_headers() -> dict[str, str]:
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": (
            "Authorization, Content-Type, X-Amz-Target, X-Amz-Date, "
            "X-Amz-Security-Token, X-Amz-Content-Sha256"
        ),
        "Access-Control-Max-Age": "86400",
    }


# -- Response Handlers --


def cors_response_handler(context: RequestContext) -> None:
    """Add CORS headers to all responses."""
    if context.response is not None:
        for key, value in _cors_headers().items():
            context.response.headers.setdefault(key, value)


def logging_response_handler(context: RequestContext) -> None:
    """Log completed request details."""
    if context.response is not None:
        status = context.response.status_code
        level = logging.DEBUG if status < 400 else logging.WARNING
        log.log(
            level,
            "%s %s → %s %s (%d)",
            context.request.method,
            context.request.url.path,
            context.service_name,
            context.operation or "?",
            status,
        )


# -- Exception Handlers --


def error_normalizer(context: RequestContext, exc: Exception) -> None:
    """Convert exceptions to properly formatted AWS error responses."""
    protocol = context.protocol or "query"

    if protocol in ("json", "rest-json"):
        body = json.dumps(
            {
                "__type": type(exc).__name__,
                "message": str(exc),
            }
        )
        context.response = Response(
            content=body,
            status_code=500,
            media_type="application/x-amz-json-1.0",
        )
    else:
        # XML format for query, rest-xml, ec2
        body = (
            f"<ErrorResponse><Error>"
            f"<Code>InternalError</Code>"
            f"<Message>{exc}</Message>"
            f"</Error></ErrorResponse>"
        )
        context.response = Response(
            content=body,
            status_code=500,
            media_type="application/xml",
        )
