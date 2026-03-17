"""Diagnostic logging for internal errors.

When an AWS operation fails inside Moto or a native provider, the gateway
returns a clean HTTP error to the client.  The original exception — the thing
that tells you *why* — is normally invisible to the caller.

This module makes it visible in two ways:

1. **Response header** ``x-robotocore-diag``
   Always present on 4xx/5xx responses originating from caught exceptions.
   Contains ``ExceptionType: message`` (truncated to 512 bytes).
   Accessible via ``e.response["ResponseMetadata"]["HTTPHeaders"]`` in boto3.

2. **Diagnostic log file** (opt-in via ``ROBOTOCORE_DIAG``)
   Set ``ROBOTOCORE_DIAG=1`` to write ``.robotocore-diag.log`` in the project
   root.  Set ``ROBOTOCORE_DIAG=/path/to/file.log`` for a custom path.

   Each line is a JSON object::

       {"ts": "...", "level": "ERROR", "service": "sqs", "operation": "SendMessage",
        "method": "POST", "path": "/", "status": 500,
        "exc_type": "KeyError", "exc_msg": "'foo'",
        "traceback": "Traceback (most recent call last):\\n  ..."}

   Tail it while running tests::

       ROBOTOCORE_DIAG=1 make compat-test &
       tail -f .robotocore-diag.log | grep '"level": "ERROR"'

Usage from error-handling code::

    from robotocore.diagnostics import diag

    diag.record(
        exc=e,
        service="sqs",
        operation="SendMessage",
        method="POST",
        path="/",
        status=500,
    )
    # Returns the header value to attach to the response:
    header_val = diag.header_value(e)
"""

import json
import logging
import os
import traceback
from datetime import UTC, datetime

# ---------------------------------------------------------------------------
# Module-level logger (separate from the application logger)
# ---------------------------------------------------------------------------

_diag_logger = logging.getLogger("robotocore.diag")
_diag_logger.propagate = False  # Don't spam the root logger
_configured = False

# Maximum length of the x-robotocore-diag header value
_MAX_HEADER_LEN = 512


def _ensure_configured() -> None:
    """Lazy-configure the diagnostic logger on first use."""
    global _configured
    if _configured:
        return
    _configured = True

    diag_setting = os.environ.get("ROBOTOCORE_DIAG", "")
    if not diag_setting:
        # No env var → logger stays at WARNING with no handlers (silent)
        _diag_logger.setLevel(logging.WARNING)
        return

    # Determine file path
    if diag_setting == "1":
        # Default location: project root
        log_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            ".robotocore-diag.log",
        )
    else:
        log_path = diag_setting

    handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(message)s"))
    _diag_logger.addHandler(handler)
    _diag_logger.setLevel(logging.DEBUG)

    # Write a startup marker so you can tell when a new run starts
    _diag_logger.info(
        json.dumps(
            {
                "ts": datetime.now(tz=UTC).isoformat(),
                "level": "INFO",
                "event": "diag_start",
                "message": f"Diagnostic logging to {log_path}",
            }
        )
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def header_value(exc: BaseException) -> str:
    """Build the ``x-robotocore-diag`` header value from an exception.

    Format: ``ExceptionType: message`` truncated to 512 bytes.
    Always safe to call — returns a string even if exc is weird.
    """
    try:
        val = f"{type(exc).__name__}: {exc}"
    except Exception:  # noqa: BLE001
        val = "UnknownError"
    if len(val) > _MAX_HEADER_LEN:
        val = val[: _MAX_HEADER_LEN - 3] + "..."
    # Headers must not contain newlines
    return val.replace("\n", " ").replace("\r", "")


def record(
    *,
    exc: BaseException,
    service: str = "",
    operation: str = "",
    method: str = "",
    path: str = "",
    status: int = 0,
) -> None:
    """Log a diagnostic record for a caught exception.

    Cheap no-op when ``ROBOTOCORE_DIAG`` is not set.
    """
    _ensure_configured()

    # Even without the file handler, log at WARNING so it appears in
    # the normal server log (stderr / .robotocore.log) for 500s.
    is_internal_error = status >= 500 and status != 501
    level = logging.WARNING if is_internal_error else logging.DEBUG
    if not _diag_logger.isEnabledFor(level):
        return

    tb = traceback.format_exception(type(exc), exc, exc.__traceback__)
    entry = {
        "ts": datetime.now(tz=UTC).isoformat(),
        "level": "ERROR" if is_internal_error else "WARN",
        "service": service,
        "operation": operation,
        "method": method,
        "path": path,
        "status": status,
        "exc_type": type(exc).__name__,
        "exc_msg": str(exc),
        "traceback": "".join(tb),
    }
    _diag_logger.log(level, json.dumps(entry, default=str))


def reset() -> None:
    """Reset configuration state (for testing)."""
    global _configured
    _configured = False
    _diag_logger.handlers.clear()
