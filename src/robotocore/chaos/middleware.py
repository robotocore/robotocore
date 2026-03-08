"""Chaos engineering handler for the request pipeline.

Integrates with the handler chain to inject faults before requests
reach service providers.
"""

import json
import time

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

    # NOTE: time.sleep blocks the event loop since the handler chain runs
    # synchronously inside an async request handler. To fix properly, the
    # handler chain needs async support (asyncio.to_thread or async handlers).
    if rule.latency_ms > 0:
        time.sleep(rule.latency_ms / 1000.0)

    # Apply error injection
    if rule.error_code:
        error_body = json.dumps(
            {
                "__type": rule.error_code,
                "message": rule.error_message,
                "Message": rule.error_message,
            }
        )
        context.response = Response(
            content=error_body,
            status_code=rule.status_code,
            media_type="application/json",
            headers={"x-robotocore-chaos": rule.rule_id},
        )
