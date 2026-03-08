"""Request tracing middleware for Robotocore.

Assigns a unique request ID (X-Amz-Request-Id) to every request,
tracks request timing, and optionally exports traces via OpenTelemetry.
"""

import logging
import os
import time
import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from robotocore.observability.logging import log_request, log_response

logger = logging.getLogger(__name__)

# Optional OpenTelemetry tracer
_tracer = None


def _get_tracer():
    """Lazily initialize OpenTelemetry tracer if OTEL endpoint is configured."""
    global _tracer
    if _tracer is not None:
        return _tracer

    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        return None

    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = Resource.create({"service.name": "robotocore"})
        provider = TracerProvider(resource=resource)
        exporter = OTLPSpanExporter(endpoint=endpoint)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        _tracer = trace.get_tracer("robotocore")
        logger.info("OpenTelemetry tracing enabled, exporting to %s", endpoint)
        return _tracer
    except ImportError:
        logger.debug("OpenTelemetry packages not installed; tracing disabled")
        return None


def generate_request_id() -> str:
    """Generate a unique request ID in AWS format."""
    return str(uuid.uuid4())


class TracingMiddleware(BaseHTTPMiddleware):
    """Middleware that adds request tracing and timing."""

    async def dispatch(self, request: Request, call_next) -> Response:
        request_id = generate_request_id()
        start_time = time.monotonic()

        # Store request_id on request state for downstream use
        request.state.request_id = request_id
        request.state.start_time = start_time

        # Log request
        body = await request.body()
        log_request(
            logger,
            method=request.method,
            path=request.url.path,
            headers=dict(request.headers),
            body_size=len(body),
            request_id=request_id,
        )

        # Optional OTel span
        tracer = _get_tracer()
        if tracer:
            with tracer.start_as_current_span(
                f"{request.method} {request.url.path}",
                attributes={
                    "http.method": request.method,
                    "http.url": str(request.url),
                    "robotocore.request_id": request_id,
                },
            ):
                response = await call_next(request)
        else:
            response = await call_next(request)

        duration_ms = (time.monotonic() - start_time) * 1000

        # Add standard headers
        response.headers["X-Amz-Request-Id"] = request_id
        response.headers["X-Robotocore-Request-Id"] = request_id

        # Determine body size from content-length or 0
        body_size = int(response.headers.get("content-length", "0"))

        log_response(
            logger,
            status_code=response.status_code,
            body_size=body_size,
            duration_ms=duration_ms,
            request_id=request_id,
        )

        return response
