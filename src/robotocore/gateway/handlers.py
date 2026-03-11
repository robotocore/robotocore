"""Request/response handlers for the AWS gateway pipeline."""

import json
import logging
import re

from starlette.responses import Response

from robotocore.gateway.handler_chain import RequestContext
from robotocore.gateway.router import route_to_service
from robotocore.protocols.service_info import get_service_json_version, get_service_protocol

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
        if not action:
            # Query-protocol services (STS, SQS, IAM, EC2, etc.) send Action
            # in the POST form body, not URL query params.
            content_type = headers.get("content-type", "")
            if "x-www-form-urlencoded" in content_type:
                from urllib.parse import parse_qs

                body = getattr(context.request, "_body", b"")
                if body:
                    form = parse_qs(body.decode("utf-8", errors="replace"))
                    action = form.get("Action", [None])[0]
        if action:
            context.operation = action


def cors_handler(context: RequestContext) -> None:
    """Handle CORS preflight and set CORS headers on OPTIONS requests."""
    from robotocore.gateway.cors import build_preflight_response, get_cors_config

    if context.request.method == "OPTIONS":
        config = get_cors_config()
        request_origin = context.request.headers.get("origin")
        preflight = build_preflight_response(config, request_origin)
        if preflight is not None:
            context.response = preflight


# -- Response Handlers --


def cors_response_handler(context: RequestContext) -> None:
    """Add CORS headers to all responses."""
    from robotocore.gateway.cors import (
        build_cors_headers,
        build_s3_cors_headers,
        get_cors_config,
    )

    if context.response is None:
        return

    config = get_cors_config()
    if config.disable_cors_headers:
        return

    request_origin = context.request.headers.get("origin")

    # S3 bucket-specific CORS: apply bucket CORS rules if available
    if context.service_name == "s3" and not config.disable_custom_cors_s3 and request_origin:
        bucket_cors = _get_s3_bucket_cors(context)
        if bucket_cors is not None:
            request_method = context.request.headers.get("access-control-request-method")
            s3_headers = build_s3_cors_headers(bucket_cors, request_origin, request_method)
            for key, value in s3_headers.items():
                context.response.headers.setdefault(key, value)
            return

    # Default CORS headers
    cors_headers = build_cors_headers(config, request_origin)
    for key, value in cors_headers.items():
        context.response.headers.setdefault(key, value)


def _get_s3_bucket_cors(context: RequestContext) -> list[dict] | None:
    """Extract bucket name from the request and return its CORS config, if any."""
    from robotocore.services.s3.provider import get_bucket_cors

    path = context.request.url.path
    # Path-style: /bucket-name/...
    parts = path.strip("/").split("/")
    if parts and parts[0]:
        return get_bucket_cors(parts[0])
    return None


def audit_response_handler(context: RequestContext) -> None:
    """Record the request in the audit log and usage analytics."""
    from robotocore.audit.analytics import get_usage_analytics
    from robotocore.audit.log import get_audit_log

    status = context.response.status_code if context.response else 0
    get_audit_log().record(
        service=context.service_name,
        operation=context.operation,
        method=context.request.method,
        path=context.request.url.path,
        status_code=status,
        account_id=context.account_id,
        region=context.region,
    )

    # Record in usage analytics
    # Extract access key from Authorization header
    auth = context.request.headers.get("authorization", "")
    access_key = None
    if "Credential=" in auth:
        cred_part = auth.split("Credential=")[1].split("/")[0]
        if cred_part:
            access_key = cred_part

    get_usage_analytics().record_request(
        service=context.service_name,
        operation=context.operation,
        status_code=status,
        access_key_id=access_key,
    )


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
    """Convert exceptions to properly formatted AWS error responses.

    Uses 501 for NotImplementedError (operation not supported) and 500 for
    everything else (genuine internal errors). This distinction lets clients
    and probe scripts differentiate "not yet built" from "broken".
    """
    from xml.sax.saxutils import escape as xml_escape

    from robotocore.diagnostics import header_value as diag_header
    from robotocore.diagnostics import record as diag_record

    protocol = context.protocol or "query"
    is_not_implemented = isinstance(exc, NotImplementedError)
    status_code = 501 if is_not_implemented else 500
    error_code = "NotImplemented" if is_not_implemented else type(exc).__name__

    diag_record(
        exc=exc,
        service=context.service_name,
        operation=context.operation or "",
        method=context.request.method,
        path=context.request.url.path,
        status=status_code,
    )
    diag_hdr = {"x-robotocore-diag": diag_header(exc)}

    if protocol in ("json", "rest-json", "smithy-rpc-v2-cbor"):
        body = json.dumps(
            {
                "__type": error_code,
                "message": str(exc),
            }
        )
        # Use the correct JSON version from botocore metadata (1.0 or 1.1)
        json_version = get_service_json_version(context.service_name) or "1.0"
        context.response = Response(
            content=body,
            status_code=status_code,
            media_type=f"application/x-amz-json-{json_version}",
            headers=diag_hdr,
        )
    else:
        # XML format varies by protocol:
        # - EC2: <Response><Errors><Error>...</Error></Errors><RequestId>...</RequestId></Response>
        # - S3 (rest-xml): bare <Error>...</Error>
        # - query/rest-xml (non-S3): <ErrorResponse><Error>...</Error></ErrorResponse>
        safe_message = xml_escape(str(exc))
        if protocol == "ec2":
            body = (
                f"<Response><Errors><Error>"
                f"<Code>{error_code}</Code>"
                f"<Message>{safe_message}</Message>"
                f"</Error></Errors>"
                f"<RequestId>00000000-0000-0000-0000-000000000000</RequestId>"
                f"</Response>"
            )
        elif context.service_name == "s3":
            body = f"<Error><Code>{error_code}</Code><Message>{safe_message}</Message></Error>"
        else:
            body = (
                f"<ErrorResponse><Error>"
                f"<Code>{error_code}</Code>"
                f"<Message>{safe_message}</Message>"
                f"</Error></ErrorResponse>"
            )
        context.response = Response(
            content=body,
            status_code=status_code,
            media_type="application/xml",
            headers=diag_hdr,
        )
