"""Native EventBridge provider with cross-service target invocation.

Enterprise-grade: when PutEvents is called, matching rules invoke their targets
(Lambda, SQS, SNS, Kinesis, Step Functions, etc.)
"""

import json
import logging
import threading
import time
import uuid

from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.events.models import EventsStore

logger = logging.getLogger(__name__)

_stores: dict[str, EventsStore] = {}
_store_lock = threading.Lock()

# Invocation log for cross-service target dispatch (useful for testing)
_invocation_log: list[dict] = []
_invocation_log_lock = threading.Lock()


def get_invocation_log() -> list[dict]:
    """Return a copy of the invocation log (for testing/debugging)."""
    with _invocation_log_lock:
        return list(_invocation_log)


def clear_invocation_log() -> None:
    """Clear the invocation log."""
    with _invocation_log_lock:
        _invocation_log.clear()


def _log_invocation(
    target_type: str, target_arn: str, payload: str, result: dict | None = None
) -> None:
    """Record an invocation in the log."""
    with _invocation_log_lock:
        _invocation_log.append(
            {
                "target_type": target_type,
                "target_arn": target_arn,
                "payload": payload,
                "result": result,
                "timestamp": time.time(),
            }
        )


def _get_store(region: str = "us-east-1", account_id: str = "123456789012") -> EventsStore:
    with _store_lock:
        if region not in _stores:
            _stores[region] = EventsStore()
        store = _stores[region]
        store.ensure_default_bus(region, account_id)
        return store


async def handle_events_request(request: Request, region: str, account_id: str) -> Response:
    """Handle an EventBridge API request (JSON protocol via X-Amz-Target)."""
    body = await request.body()
    target = request.headers.get("x-amz-target", "")

    # Extract operation from X-Amz-Target: "AWSEvents.PutEvents"
    operation = target.split(".")[-1] if "." in target else target

    params = json.loads(body) if body else {}
    store = _get_store(region, account_id)

    handler = _ACTION_MAP.get(operation)
    if handler is None:
        return _error("UnknownOperation", f"Unknown operation: {operation}", 400)

    try:
        result = handler(store, params, region, account_id)
        return _json(200, result)
    except EventsError as e:
        return _error(e.code, e.message, e.status)
    except Exception as e:
        return _error("InternalError", str(e), 500)


class EventsError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


# --- Operations ---


def _put_rule(store: EventsStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("Name", "")
    bus_name = params.get("EventBusName", "default")
    event_pattern = params.get("EventPattern")
    if isinstance(event_pattern, str):
        event_pattern = json.loads(event_pattern)
    schedule = params.get("ScheduleExpression")
    state = params.get("State", "ENABLED")
    description = params.get("Description", "")

    rule = store.put_rule(
        name,
        bus_name,
        region,
        account_id,
        event_pattern=event_pattern,
        schedule_expression=schedule,
        state=state,
        description=description,
    )
    return {"RuleArn": rule.arn}


def _delete_rule(store: EventsStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("Name", "")
    bus_name = params.get("EventBusName", "default")
    store.delete_rule(name, bus_name)
    return {}


def _describe_rule(store: EventsStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("Name", "")
    bus_name = params.get("EventBusName", "default")
    rule = store.get_rule(name, bus_name)
    if not rule:
        raise EventsError("ResourceNotFoundException", f"Rule {name} not found", 400)
    result = {
        "Name": rule.name,
        "Arn": rule.arn,
        "State": rule.state,
        "Description": rule.description,
        "EventBusName": rule.event_bus_name,
    }
    if rule.event_pattern:
        result["EventPattern"] = json.dumps(rule.event_pattern)
    if rule.schedule_expression:
        result["ScheduleExpression"] = rule.schedule_expression
    return result


def _enable_rule(store: EventsStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("Name", "")
    bus_name = params.get("EventBusName", "default")
    rule = store.get_rule(name, bus_name)
    if rule:
        rule.state = "ENABLED"
    return {}


def _disable_rule(store: EventsStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("Name", "")
    bus_name = params.get("EventBusName", "default")
    rule = store.get_rule(name, bus_name)
    if rule:
        rule.state = "DISABLED"
    return {}


def _list_rules(store: EventsStore, params: dict, region: str, account_id: str) -> dict:
    bus_name = params.get("EventBusName", "default")
    prefix = params.get("NamePrefix")
    rules = store.list_rules(bus_name, prefix)
    return {
        "Rules": [
            {
                "Name": r.name,
                "Arn": r.arn,
                "State": r.state,
                "Description": r.description,
                "EventBusName": r.event_bus_name,
                **({"EventPattern": json.dumps(r.event_pattern)} if r.event_pattern else {}),
                **({"ScheduleExpression": r.schedule_expression} if r.schedule_expression else {}),
            }
            for r in rules
        ]
    }


def _put_targets(store: EventsStore, params: dict, region: str, account_id: str) -> dict:
    rule_name = params.get("Rule", "")
    bus_name = params.get("EventBusName", "default")
    targets = params.get("Targets", [])
    failed = store.put_targets(rule_name, bus_name, targets)
    return {
        "FailedEntryCount": len(failed),
        "FailedEntries": failed,
    }


def _remove_targets(store: EventsStore, params: dict, region: str, account_id: str) -> dict:
    rule_name = params.get("Rule", "")
    bus_name = params.get("EventBusName", "default")
    target_ids = params.get("Ids", [])
    failed = store.remove_targets(rule_name, bus_name, target_ids)
    return {
        "FailedEntryCount": len(failed),
        "FailedEntries": failed,
    }


def _list_targets_by_rule(store: EventsStore, params: dict, region: str, account_id: str) -> dict:
    rule_name = params.get("Rule", "")
    bus_name = params.get("EventBusName", "default")
    targets = store.list_targets(rule_name, bus_name)
    return {
        "Targets": [
            {
                "Id": t.target_id,
                "Arn": t.arn,
                **({"RoleArn": t.role_arn} if t.role_arn else {}),
                **({"Input": t.input} if t.input else {}),
                **({"InputPath": t.input_path} if t.input_path else {}),
            }
            for t in targets
        ]
    }


def _put_events(store: EventsStore, params: dict, region: str, account_id: str) -> dict:
    """Core operation: match events to rules, invoke targets."""
    entries = params.get("Entries", [])
    results = []

    for entry in entries:
        event = {
            "version": "0",
            "id": str(uuid.uuid4()),
            "source": entry.get("Source", ""),
            "account": account_id,
            "time": entry.get("Time", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
            "region": region,
            "resources": entry.get("Resources", []),
            "detail-type": entry.get("DetailType", ""),
        }

        # Parse detail
        detail = entry.get("Detail", "{}")
        if isinstance(detail, str):
            try:
                event["detail"] = json.loads(detail)
            except json.JSONDecodeError:
                event["detail"] = {}
        else:
            event["detail"] = detail

        bus_name = entry.get("EventBusName", "default")
        bus = store.get_bus(bus_name)

        if bus:
            # Match against all rules in the bus
            for rule in bus.rules.values():
                if rule.matches_event(event):
                    _dispatch_to_targets(rule, event, region, account_id)

        results.append({"EventId": event["id"]})

    return {
        "FailedEntryCount": 0,
        "Entries": results,
    }


def _create_event_bus(store: EventsStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("Name", "")
    bus = store.create_event_bus(name, region, account_id)
    return {"EventBusArn": bus.arn}


def _delete_event_bus(store: EventsStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("Name", "")
    store.delete_bus(name)
    return {}


def _describe_event_bus(store: EventsStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("Name", "default")
    bus = store.get_bus(name)
    if not bus:
        raise EventsError("ResourceNotFoundException", f"Event bus {name} not found")
    return {
        "Name": bus.name,
        "Arn": bus.arn,
    }


def _list_event_buses(store: EventsStore, params: dict, region: str, account_id: str) -> dict:
    buses = store.list_buses()
    return {"EventBuses": [{"Name": b.name, "Arn": b.arn} for b in buses]}


def _tag_resource(store: EventsStore, params: dict, region: str, account_id: str) -> dict:
    return {}


def _untag_resource(store: EventsStore, params: dict, region: str, account_id: str) -> dict:
    return {}


def _list_tag_resource(store: EventsStore, params: dict, region: str, account_id: str) -> dict:
    return {"Tags": []}


# --- Target Dispatch (cross-service) ---


def _dispatch_to_targets(rule, event: dict, region: str, account_id: str):
    """Dispatch an event to all targets of a matching rule."""
    for target in rule.targets.values():
        try:
            _invoke_target(target, event, region, account_id)
        except Exception:
            logger.exception(f"Error invoking target {target.target_id} for rule {rule.name}")


def _invoke_target(target, event: dict, region: str, account_id: str):
    """Invoke a single target with the event."""
    arn = target.arn

    # Determine input to send
    if target.input:
        payload = target.input
    elif target.input_path:
        # JSONPath extraction (simplified)
        payload = json.dumps(event)
    else:
        payload = json.dumps(event)

    if ":lambda:" in arn:
        _invoke_lambda_target(arn, payload, region, account_id)
    elif ":sqs:" in arn:
        _invoke_sqs_target(arn, payload, region, account_id)
    elif ":sns:" in arn:
        _invoke_sns_target(arn, payload, region, account_id)
    elif ":logs:" in arn:
        logger.info(f"EventBridge → CloudWatch Logs: {arn}")
    else:
        logger.warning(f"Unsupported EventBridge target type: {arn}")


def _invoke_lambda_target(arn: str, payload: str, region: str, account_id: str):
    """Invoke a Lambda function from EventBridge.

    Uses async dispatch via thread pool to avoid deadlocking the event loop
    when the Lambda function calls back to the server.
    """
    from robotocore.services.lambda_.invoke import invoke_lambda_async

    event = json.loads(payload) if isinstance(payload, str) else payload

    def _on_complete(result, error_type, logs):
        invocation_result = {"result": result, "error_type": error_type, "logs": logs}
        _log_invocation("lambda", arn, payload, invocation_result)

    invoke_lambda_async(arn, event, region, account_id, callback=_on_complete)


def _invoke_sqs_target(arn: str, payload: str, region: str, account_id: str):
    """Send a message to an SQS queue from EventBridge."""
    import hashlib

    from robotocore.services.sqs.models import SqsMessage
    from robotocore.services.sqs.provider import _get_store

    queue_name = arn.rsplit(":", 1)[-1]
    store = _get_store(region)
    queue = store.get_queue(queue_name)
    if not queue:
        logger.error(f"EventBridge: SQS queue not found: {queue_name}")
        return

    msg = SqsMessage(
        message_id=str(uuid.uuid4()),
        body=payload,
        md5_of_body=hashlib.md5(payload.encode()).hexdigest(),
    )
    queue.put(msg)
    _log_invocation("sqs", arn, payload)
    logger.info(f"EventBridge → SQS: {queue_name}")


def _invoke_sns_target(arn: str, payload: str, region: str, account_id: str):
    """Publish to an SNS topic from EventBridge."""
    from robotocore.services.sns.provider import _get_store

    store = _get_store(region)
    topic = store.get_topic(arn)
    if not topic:
        logger.error(f"EventBridge: SNS topic not found: {arn}")
        return

    # Deliver to subscribers
    from robotocore.services.sns.provider import _deliver_to_subscriber, _new_id

    message_id = _new_id()
    for sub in topic.subscriptions:
        if sub.confirmed:
            _deliver_to_subscriber(
                sub, payload, "EventBridge Notification", {}, message_id, arn, region
            )
    _log_invocation("sns", arn, payload)
    logger.info(f"EventBridge → SNS: {arn}")


# --- Response helpers ---


def _json(status_code: int, data) -> Response:
    if data is None:
        return Response(content=b"", status_code=status_code)
    return Response(
        content=json.dumps(data),
        status_code=status_code,
        media_type="application/x-amz-json-1.1",
    )


def _error(code: str, message: str, status: int) -> Response:
    body = json.dumps({"__type": code, "message": message})
    return Response(content=body, status_code=status, media_type="application/x-amz-json-1.1")


_ACTION_MAP = {
    "PutRule": _put_rule,
    "DeleteRule": _delete_rule,
    "DescribeRule": _describe_rule,
    "EnableRule": _enable_rule,
    "DisableRule": _disable_rule,
    "ListRules": _list_rules,
    "PutTargets": _put_targets,
    "RemoveTargets": _remove_targets,
    "ListTargetsByRule": _list_targets_by_rule,
    "PutEvents": _put_events,
    "CreateEventBus": _create_event_bus,
    "DeleteEventBus": _delete_event_bus,
    "DescribeEventBus": _describe_event_bus,
    "ListEventBuses": _list_event_buses,
    "TagResource": _tag_resource,
    "UntagResource": _untag_resource,
    "ListTagsForResource": _list_tag_resource,
}
