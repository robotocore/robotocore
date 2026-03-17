"""Bridge between chaos middleware and audit log for unified observability."""

import logging

from robotocore.observability.request_context import get_current_context

logger = logging.getLogger(__name__)


def record_chaos_event(rule_name: str, fault_type: str, details: dict) -> None:
    """Record a chaos fault injection in both the request context and audit log."""
    ctx = get_current_context()
    if ctx:
        ctx.chaos_applied.append(
            {
                "rule": rule_name,
                "type": fault_type,
                **details,
            }
        )

    # Also record in audit log
    try:
        from robotocore.audit.log import get_audit_log

        audit = get_audit_log()
        audit.record(
            service=ctx.service if ctx else "unknown",
            operation=f"chaos:{fault_type}",
            status_code=details.get("status_code", 0),
            error=f"chaos_injection:{rule_name}",
        )
    except Exception:  # noqa: BLE001
        logger.debug("Could not record chaos event in audit log", exc_info=True)
