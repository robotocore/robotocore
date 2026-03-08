"""Structured JSON logging for Robotocore.

Configuration via environment variables:
    LOG_LEVEL=INFO          Logging level (DEBUG, INFO, WARNING, ERROR)
    LOG_FORMAT=text         Log format: 'text' (default) or 'json'
    DEBUG=1                 Enable debug-level request/response logging
"""

import json
import logging
import os
from datetime import UTC, datetime


class JsonFormatter(logging.Formatter):
    """Formats log records as JSON lines."""

    def format(self, record: logging.LogRecord) -> str:
        entry: dict = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Include extra fields if present
        for field in ("service", "operation", "request_id", "duration_ms"):
            val = getattr(record, field, None)
            if val is not None:
                entry[field] = val
        if record.exc_info and record.exc_info[1]:
            entry["exception"] = str(record.exc_info[1])
        return json.dumps(entry, default=str)


def setup_logging() -> None:
    """Configure logging based on environment variables.

    Call once at startup from main.py.
    """
    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    fmt = os.environ.get("LOG_FORMAT", "text")

    # Remove existing handlers on root logger to avoid duplicates
    root = logging.getLogger()
    root.handlers.clear()

    handler = logging.StreamHandler()
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )

    root.addHandler(handler)
    root.setLevel(getattr(logging, level_name, logging.INFO))


def log_request(
    logger: logging.Logger,
    *,
    method: str,
    path: str,
    headers: dict,
    body_size: int,
    request_id: str,
) -> None:
    """Log an incoming request (only when DEBUG=1)."""
    if not os.environ.get("DEBUG") == "1":
        return
    # Sanitize sensitive headers
    sanitized = {
        k: ("***" if k.lower() in ("authorization", "x-amz-security-token") else v)
        for k, v in headers.items()
    }
    logger.debug(
        "Request %s %s headers=%s body_size=%d request_id=%s",
        method,
        path,
        sanitized,
        body_size,
        request_id,
    )


def log_response(
    logger: logging.Logger,
    *,
    status_code: int,
    body_size: int,
    duration_ms: float,
    request_id: str,
) -> None:
    """Log a response (only when DEBUG=1)."""
    if not os.environ.get("DEBUG") == "1":
        return
    logger.debug(
        "Response status=%d body_size=%d duration_ms=%.1f request_id=%s",
        status_code,
        body_size,
        duration_ms,
        request_id,
    )
