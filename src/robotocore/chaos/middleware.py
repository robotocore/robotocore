"""Chaos engineering handler for the request pipeline.

Integrates with the handler chain to inject faults before requests
reach service providers.
"""

import json
import time
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

    from robotocore.observability.chaos_audit_bridge import record_chaos_event

    # Use time.sleep() — the handler chain runs synchronously inside
    # asyncio.to_thread(), so this blocks only the current request thread
    # without blocking the event loop.
    if rule.latency_ms > 0:
        record_chaos_event(rule.rule_id, "latency", {"latency_ms": rule.latency_ms})
        time.sleep(rule.latency_ms / 1000.0)

    # Apply error injection
    if rule.error_code:
        request_id = uuid.uuid4().hex
        error_body = json.dumps(
            {
                "__type": rule.error_code,
                "message": rule.error_message,
                "Message": rule.error_message,
                "RequestId": request_id,
            }
        )
        record_chaos_event(
            rule.rule_id,
            "error",
            {"status_code": rule.status_code, "error_code": rule.error_code},
        )
        context.response = Response(
            content=error_body,
            status_code=rule.status_code,
            media_type="application/json",
            headers={"x-robotocore-chaos": rule.rule_id},
        )
