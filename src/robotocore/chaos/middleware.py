"""Chaos engineering handler for the request pipeline.

Integrates with the handler chain to inject faults before requests
reach service providers. Records chaos events in the audit log and
observability hub for end-to-end request tracing.
"""

import asyncio
import json
import uuid

from starlette.responses import Response

from robotocore.chaos.fault_rules import get_fault_store
from robotocore.gateway.handler_chain import RequestContext


def chaos_handler(context: RequestContext) -> None:
    """Handler chain integration: check for matching fault rules and inject faults."""
    store = get_fault_store()
    rule = store.find_matching(
        service=context.service_name,
        operation=context.operation,
        region=context.region,
    )

    if rule is None:
        return

    # Use asyncio.sleep via asyncio.to_thread-compatible approach to avoid
    # blocking the event loop.
    if rule.latency_ms > 0:
        try:
            loop = asyncio.get_running_loop()
            # We're in an async context; schedule a non-blocking sleep
            loop.create_task(asyncio.sleep(rule.latency_ms / 1000.0))
        except RuntimeError:
            # No event loop running; fall back to asyncio.run for sync contexts
            asyncio.run(asyncio.sleep(rule.latency_ms / 1000.0))

    # Determine what action was taken
    action_taken = None
    if rule.error_code and rule.latency_ms > 0:
        action_taken = "error_injected+latency_added"
    elif rule.error_code:
        action_taken = "error_injected"
    elif rule.latency_ms > 0:
        action_taken = "latency_added"

    # Record chaos event in the observability hub
    if action_taken:
        _record_chaos_event(context, rule, action_taken)

    # Apply error injection
    if rule.error_code:
        request_id = context.request_id or uuid.uuid4().hex
        error_body = json.dumps(
            {
                "__type": rule.error_code,
                "message": rule.error_message,
                "Message": rule.error_message,
                "RequestId": request_id,
            }
        )
        context.response = Response(
            content=error_body,
            status_code=rule.status_code,
            media_type="application/json",
            headers={"x-robotocore-chaos": rule.rule_id},
        )


def _record_chaos_event(context: RequestContext, rule, action_taken: str) -> None:
    """Record chaos fault injection in the observability hub and audit log."""
    from robotocore.audit.log import get_audit_log
    from robotocore.observability.unified import get_observability_hub

    request_id = context.request_id or ""

    # Record in observability hub
    hub = get_observability_hub()
    hub.record_chaos_event(
        request_id=request_id,
        service=context.service_name,
        operation=context.operation,
        rule=rule.to_dict(),
        action_taken=action_taken,
    )

    # Record in audit log with chaos prefix on error field
    get_audit_log().record(
        service=context.service_name,
        operation=context.operation,
        method=context.request.method,
        path=context.request.url.path,
        status_code=rule.status_code if rule.error_code else 200,
        account_id=context.account_id,
        region=context.region,
        error=f"chaos:{rule.rule_id}:{action_taken}",
    )
