"""Native EventBridge provider with cross-service target invocation.

Enterprise-grade: when PutEvents is called, matching rules invoke their targets
(Lambda, SQS, SNS, Kinesis, Step Functions, etc.)

Supports 17 target types, InputTransformer, dead-letter queues, and
event archives with replay.
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
                **(
                    {"InputTransformer": t.input_transformer}
                    if t.input_transformer
                    else {}
                ),
                **(
                    {"DeadLetterConfig": t.dead_letter_config}
                    if t.dead_letter_config
                    else {}
                ),
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
            # Archive the event
            store.archive_event(event, bus_name)
            # Match against all rules in the bus
            for rule in bus.rules.values():
                if rule.matches_event(event):
                    _dispatch_to_targets(
                        rule, event, region, account_id, store
                    )

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
    resource_arn = params.get("ResourceARN", "")
    tags = params.get("Tags", [])
    store.tag_resource(resource_arn, tags)
    return {}


def _untag_resource(store: EventsStore, params: dict, region: str, account_id: str) -> dict:
    resource_arn = params.get("ResourceARN", "")
    tag_keys = params.get("TagKeys", [])
    store.untag_resource(resource_arn, tag_keys)
    return {}


def _list_tag_resource(store: EventsStore, params: dict, region: str, account_id: str) -> dict:
    resource_arn = params.get("ResourceARN", "")
    tags = store.list_tags_for_resource(resource_arn)
    return {"Tags": tags}


# --- Archive operations ---


def _create_archive(
    store: EventsStore, params: dict, region: str, account_id: str
) -> dict:
    name = params.get("ArchiveName", "")
    source_arn = params.get("EventSourceArn", "")
    description = params.get("Description", "")
    event_pattern = params.get("EventPattern")
    if isinstance(event_pattern, str):
        event_pattern = json.loads(event_pattern)
    retention = params.get("RetentionDays", 0)

    if store.get_archive(name):
        raise EventsError(
            "ResourceAlreadyExistsException",
            f"Archive {name} already exists",
        )

    archive = store.create_archive(
        name,
        source_arn,
        region,
        account_id,
        description=description,
        event_pattern=event_pattern,
        retention_days=retention,
    )
    return {
        "ArchiveArn": archive.arn,
        "State": archive.state,
        "CreationTime": archive.created,
    }


def _describe_archive(
    store: EventsStore, params: dict, region: str, account_id: str
) -> dict:
    name = params.get("ArchiveName", "")
    archive = store.get_archive(name)
    if not archive:
        raise EventsError(
            "ResourceNotFoundException",
            f"Archive {name} does not exist.",
        )
    result = {
        "ArchiveArn": archive.arn,
        "ArchiveName": archive.name,
        "EventSourceArn": archive.source_arn,
        "State": archive.state,
        "RetentionDays": archive.retention_days,
        "SizeBytes": archive.size_bytes,
        "EventCount": archive.event_count,
        "CreationTime": archive.created,
        "Description": archive.description,
    }
    if archive.event_pattern:
        result["EventPattern"] = json.dumps(archive.event_pattern)
    return result


def _list_archives(
    store: EventsStore, params: dict, region: str, account_id: str
) -> dict:
    prefix = params.get("NamePrefix")
    archives = store.list_archives(prefix)
    return {
        "Archives": [
            {
                "ArchiveName": a.name,
                "ArchiveArn": a.arn,
                "EventSourceArn": a.source_arn,
                "State": a.state,
                "RetentionDays": a.retention_days,
                "SizeBytes": a.size_bytes,
                "EventCount": a.event_count,
                "CreationTime": a.created,
            }
            for a in archives
        ]
    }


def _delete_archive(
    store: EventsStore, params: dict, region: str, account_id: str
) -> dict:
    name = params.get("ArchiveName", "")
    if not store.delete_archive(name):
        raise EventsError(
            "ResourceNotFoundException",
            f"Archive {name} does not exist.",
        )
    return {}


def _start_replay(
    store: EventsStore, params: dict, region: str, account_id: str
) -> dict:
    name = params.get("ReplayName", "")
    archive_arn = params.get("EventSourceArn", "")
    destination = params.get("Destination", {})
    destination_arn = destination.get("Arn", "")
    start_time = params.get("EventStartTime", 0)
    end_time = params.get("EventEndTime", time.time())

    # Find the archive
    archive_name = archive_arn.rsplit("/", 1)[-1]
    archive = store.get_archive(archive_name)
    if not archive:
        raise EventsError(
            "ResourceNotFoundException",
            f"Archive {archive_arn} does not exist.",
        )

    replay = store.create_replay(
        name,
        archive_arn,
        region,
        account_id,
        destination_arn=destination_arn,
        start_time=start_time,
        end_time=end_time,
    )

    # Replay matching events to the destination bus
    bus_name = destination_arn.rsplit("/", 1)[-1] if "/" in destination_arn else "default"
    replayed = 0
    for evt in archive.events:
        # Replay all archived events (simplified — no time filtering here
        # because stored events may not have numeric timestamps)
        bus = store.get_bus(bus_name)
        if bus:
            for rule in bus.rules.values():
                if rule.matches_event(evt):
                    _dispatch_to_targets(
                        rule, evt, region, account_id, store
                    )
            replayed += 1

    replay.events_replayed = replayed
    replay.state = "COMPLETED"

    return {
        "ReplayArn": replay.arn,
        "State": replay.state,
        "StateReason": "Replay completed successfully",
    }


def _describe_replay(
    store: EventsStore, params: dict, region: str, account_id: str
) -> dict:
    name = params.get("ReplayName", "")
    replay = store.get_replay(name)
    if not replay:
        raise EventsError(
            "ResourceNotFoundException",
            f"Replay {name} does not exist.",
        )
    return {
        "ReplayName": replay.name,
        "ReplayArn": replay.arn,
        "EventSourceArn": replay.archive_arn,
        "State": replay.state,
        "EventStartTime": replay.start_time,
        "EventEndTime": replay.end_time,
        "EventsReplayed": replay.events_replayed,
    }


# --- Input Transformer ---


def _apply_input_transformer(
    transformer: dict, event: dict
) -> str:
    """Apply an InputTransformer to an event.

    The transformer has:
      InputPathsMap: {"key": "$.detail.field"} — simple JSONPath
      InputTemplate: "The <key> happened" — placeholder replacement
    """
    paths_map = transformer.get("InputPathsMap", {})
    template = transformer.get("InputTemplate", "")

    # Resolve each JSONPath
    resolved: dict[str, str] = {}
    for key, path in paths_map.items():
        resolved[key] = _resolve_jsonpath(path, event)

    # Replace <key> placeholders in template
    result = template
    for key, value in resolved.items():
        result = result.replace(f"<{key}>", value)

    return result


def _resolve_jsonpath(path: str, event: dict) -> str:
    """Resolve a simple JSONPath like $.detail.field against an event.

    Supports:
      $ — the whole event
      $.field — top-level field
      $.field.subfield — nested field
    """
    if path == "$":
        return json.dumps(event)

    # Strip leading "$."
    if path.startswith("$."):
        parts = path[2:].split(".")
    else:
        return json.dumps(event)

    current = event
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return ""

    if isinstance(current, str):
        return current
    return json.dumps(current)


# --- Target Dispatch (cross-service) ---


def _dispatch_to_targets(
    rule, event: dict, region: str, account_id: str,
    store: EventsStore | None = None,
):
    """Dispatch an event to all targets of a matching rule."""
    for target in rule.targets.values():
        try:
            _invoke_target(target, event, region, account_id)
        except Exception as exc:
            logger.exception(
                "Error invoking target %s for rule %s",
                target.target_id, rule.name,
            )
            # Dead-letter queue: send failed event to DLQ
            dlq_config = (
                target.dead_letter_config
                or rule.dead_letter_config
            )
            if dlq_config:
                _send_to_dlq(
                    dlq_config, event, target, rule, exc,
                    region, account_id,
                )


def _invoke_target(target, event: dict, region: str, account_id: str):
    """Invoke a single target with the event."""
    arn = target.arn

    # Determine input to send
    if target.input_transformer:
        payload = _apply_input_transformer(
            target.input_transformer, event
        )
    elif target.input:
        payload = target.input
    elif target.input_path:
        # Simple JSONPath extraction
        payload = _resolve_jsonpath(target.input_path, event)
    else:
        payload = json.dumps(event)

    if ":lambda:" in arn:
        _invoke_lambda_target(arn, payload, region, account_id)
    elif ":sqs:" in arn:
        _invoke_sqs_target(arn, payload, region, account_id)
    elif ":sns:" in arn:
        _invoke_sns_target(arn, payload, region, account_id)
    elif ":kinesis:" in arn:
        _invoke_kinesis_target(arn, payload, region, account_id)
    elif ":firehose:" in arn:
        _invoke_firehose_target(arn, payload, region, account_id)
    elif ":states:" in arn:
        _invoke_stepfunctions_target(
            arn, payload, region, account_id
        )
    elif ":logs:" in arn:
        _invoke_logs_target(arn, payload, region, account_id)
    elif ":ecs:" in arn:
        _invoke_ecs_target(arn, payload, region, account_id)
    elif ":events:" in arn:
        _invoke_eventbridge_target(
            arn, payload, region, account_id
        )
    elif ":execute-api:" in arn:
        _invoke_apigateway_target(
            arn, payload, region, account_id
        )
    elif ":codebuild:" in arn:
        _invoke_simulated_target(
            "codebuild", arn, payload, region, account_id
        )
    elif ":codepipeline:" in arn:
        _invoke_simulated_target(
            "codepipeline", arn, payload, region, account_id
        )
    elif ":batch:" in arn:
        _invoke_simulated_target(
            "batch", arn, payload, region, account_id
        )
    elif ":ssm:" in arn:
        _invoke_simulated_target(
            "ssm", arn, payload, region, account_id
        )
    elif ":redshift:" in arn:
        _invoke_simulated_target(
            "redshift", arn, payload, region, account_id
        )
    elif ":sagemaker:" in arn:
        _invoke_simulated_target(
            "sagemaker", arn, payload, region, account_id
        )
    elif ":inspector:" in arn:
        _invoke_simulated_target(
            "inspector", arn, payload, region, account_id
        )
    else:
        logger.warning(
            "Unsupported EventBridge target type: %s", arn
        )
        _log_invocation("unsupported", arn, payload)


def _invoke_lambda_target(
    arn: str, payload: str, region: str, account_id: str
):
    """Invoke a Lambda function from EventBridge.

    Uses async dispatch via thread pool to avoid deadlocking the
    event loop when the Lambda function calls back to the server.
    """
    from robotocore.services.lambda_.invoke import (
        invoke_lambda_async,
    )

    event = (
        json.loads(payload) if isinstance(payload, str) else payload
    )

    def _on_complete(result, error_type, logs):
        invocation_result = {
            "result": result,
            "error_type": error_type,
            "logs": logs,
        }
        _log_invocation("lambda", arn, payload, invocation_result)

    invoke_lambda_async(
        arn, event, region, account_id, callback=_on_complete
    )


def _invoke_sqs_target(
    arn: str, payload: str, region: str, account_id: str
):
    """Send a message to an SQS queue from EventBridge."""
    import hashlib

    from robotocore.services.sqs.models import SqsMessage
    from robotocore.services.sqs.provider import _get_store

    queue_name = arn.rsplit(":", 1)[-1]
    store = _get_store(region)
    queue = store.get_queue(queue_name)
    if not queue:
        logger.error(
            "EventBridge: SQS queue not found: %s", queue_name
        )
        return

    msg = SqsMessage(
        message_id=str(uuid.uuid4()),
        body=payload,
        md5_of_body=hashlib.md5(payload.encode()).hexdigest(),
    )
    queue.put(msg)
    _log_invocation("sqs", arn, payload)
    logger.info("EventBridge -> SQS: %s", queue_name)


def _invoke_sns_target(
    arn: str, payload: str, region: str, account_id: str
):
    """Publish to an SNS topic from EventBridge."""
    from robotocore.services.sns.provider import _get_store

    store = _get_store(region)
    topic = store.get_topic(arn)
    if not topic:
        logger.error("EventBridge: SNS topic not found: %s", arn)
        return

    from robotocore.services.sns.provider import (
        _deliver_to_subscriber,
        _new_id,
    )

    message_id = _new_id()
    for sub in topic.subscriptions:
        if sub.confirmed:
            _deliver_to_subscriber(
                sub,
                payload,
                "EventBridge Notification",
                {},
                message_id,
                arn,
                region,
            )
    _log_invocation("sns", arn, payload)
    logger.info("EventBridge -> SNS: %s", arn)


def _invoke_kinesis_target(
    arn: str, payload: str, region: str, account_id: str
):
    """Put a record to a Kinesis stream from EventBridge."""
    from robotocore.services.kinesis.models import _get_store

    stream_name = arn.rsplit("/", 1)[-1] if "/" in arn else arn.rsplit(":", 1)[-1]
    store = _get_store(region)
    stream = store.streams.get(stream_name)
    if not stream:
        logger.error(
            "EventBridge: Kinesis stream not found: %s",
            stream_name,
        )
        _log_invocation("kinesis", arn, payload, {"error": "stream_not_found"})
        return

    data = payload.encode("utf-8")
    partition_key = f"eventbridge-{uuid.uuid4().hex[:8]}"
    stream.put_record(partition_key, data)
    _log_invocation("kinesis", arn, payload)
    logger.info("EventBridge -> Kinesis: %s", stream_name)


def _invoke_firehose_target(
    arn: str, payload: str, region: str, account_id: str
):
    """Put a record to a Firehose delivery stream."""
    from robotocore.services.firehose import provider as fh

    stream_name = arn.rsplit("/", 1)[-1] if "/" in arn else arn.rsplit(":", 1)[-1]

    with fh._lock:
        if stream_name not in fh._delivery_streams:
            logger.error(
                "EventBridge: Firehose stream not found: %s",
                stream_name,
            )
            _log_invocation(
                "firehose", arn, payload,
                {"error": "stream_not_found"},
            )
            return
        fh._stream_buffers.setdefault(stream_name, []).append(
            payload.encode("utf-8")
        )

    _log_invocation("firehose", arn, payload)
    logger.info("EventBridge -> Firehose: %s", stream_name)


def _invoke_stepfunctions_target(
    arn: str, payload: str, region: str, account_id: str
):
    """Start a Step Functions execution from EventBridge."""
    from robotocore.services.stepfunctions import provider as sfn

    with sfn._exec_lock:
        sm = sfn._state_machines.get(arn)
    if not sm:
        logger.error(
            "EventBridge: State machine not found: %s", arn
        )
        _log_invocation(
            "stepfunctions", arn, payload,
            {"error": "state_machine_not_found"},
        )
        return

    # Start execution via the provider
    try:
        result = sfn._start_execution(
            {
                "stateMachineArn": arn,
                "name": f"eb-{uuid.uuid4().hex[:8]}",
                "input": payload,
            },
            region,
            account_id,
        )
        _log_invocation("stepfunctions", arn, payload, result)
    except Exception:
        logger.exception(
            "EventBridge: Failed to start execution: %s", arn
        )
        _log_invocation(
            "stepfunctions", arn, payload,
            {"error": "execution_failed"},
        )
        raise
    logger.info("EventBridge -> Step Functions: %s", arn)


def _invoke_logs_target(
    arn: str, payload: str, region: str, account_id: str
):
    """Put a log event to CloudWatch Logs from EventBridge."""
    try:
        from moto.backends import get_backend

        logs_backend = get_backend("logs")[account_id][region]
        # Extract log group from ARN:
        # arn:aws:logs:region:account:log-group:name
        parts = arn.split(":")
        log_group_name = ""
        for i, part in enumerate(parts):
            if part == "log-group" and i + 1 < len(parts):
                log_group_name = ":".join(parts[i + 1 :])
                # Strip trailing :* if present
                if log_group_name.endswith(":*"):
                    log_group_name = log_group_name[:-2]
                break

        if not log_group_name:
            log_group_name = "/aws/events/default"

        # Create log group if not exists (best effort)
        try:
            logs_backend.create_log_group(
                log_group_name, {}
            )
        except Exception:
            pass  # already exists

        stream_name = f"eventbridge-{uuid.uuid4().hex[:8]}"
        try:
            logs_backend.create_log_stream(
                log_group_name, stream_name
            )
        except Exception:
            pass

        logs_backend.put_log_events(
            log_group_name,
            stream_name,
            [
                {
                    "timestamp": int(time.time() * 1000),
                    "message": payload,
                }
            ],
        )
        _log_invocation("logs", arn, payload)
    except Exception:
        logger.exception(
            "EventBridge: Failed to put log events: %s", arn
        )
        _log_invocation(
            "logs", arn, payload, {"error": "log_put_failed"}
        )
        raise
    logger.info("EventBridge -> CloudWatch Logs: %s", arn)


def _invoke_ecs_target(
    arn: str, payload: str, region: str, account_id: str
):
    """Simulate an ECS RunTask from EventBridge (log only)."""
    _log_invocation("ecs", arn, payload)
    logger.info("EventBridge -> ECS RunTask (simulated): %s", arn)


def _invoke_eventbridge_target(
    arn: str, payload: str, region: str, account_id: str
):
    """Forward event to another EventBridge bus."""
    # Extract bus name from ARN
    bus_name = arn.rsplit("/", 1)[-1] if "/" in arn else "default"
    store = _get_store(region, account_id)
    bus = store.get_bus(bus_name)
    if not bus:
        logger.error(
            "EventBridge: Target bus not found: %s", bus_name
        )
        _log_invocation(
            "events", arn, payload,
            {"error": "bus_not_found"},
        )
        return

    event = (
        json.loads(payload) if isinstance(payload, str) else payload
    )
    for rule in bus.rules.values():
        if rule.matches_event(event):
            _dispatch_to_targets(
                rule, event, region, account_id, store
            )
    _log_invocation("events", arn, payload)
    logger.info(
        "EventBridge -> EventBridge bus: %s", bus_name
    )


def _invoke_apigateway_target(
    arn: str, payload: str, region: str, account_id: str
):
    """Simulate an API Gateway invocation (log only)."""
    _log_invocation("apigateway", arn, payload)
    logger.info(
        "EventBridge -> API Gateway (simulated): %s", arn
    )


def _invoke_simulated_target(
    service: str,
    arn: str,
    payload: str,
    region: str,
    account_id: str,
):
    """Simulated target invocation — log only."""
    _log_invocation(service, arn, payload)
    logger.info(
        "EventBridge -> %s (simulated): %s", service, arn
    )


# --- Dead Letter Queue ---


def _send_to_dlq(
    dlq_config: dict,
    event: dict,
    target,
    rule,
    exception: Exception,
    region: str,
    account_id: str,
):
    """Send failed event to DLQ (SQS queue)."""
    import hashlib

    dlq_arn = dlq_config.get("Arn", "")
    if not dlq_arn or ":sqs:" not in dlq_arn:
        return

    from robotocore.services.sqs.models import SqsMessage
    from robotocore.services.sqs.provider import _get_store

    queue_name = dlq_arn.rsplit(":", 1)[-1]
    sqs_store = _get_store(region)
    queue = sqs_store.get_queue(queue_name)
    if not queue:
        logger.error(
            "EventBridge DLQ: queue not found: %s", queue_name
        )
        return

    dlq_body = json.dumps(
        {
            "event": event,
            "rule": rule.name,
            "target": target.target_id,
            "error": str(exception),
        }
    )
    msg = SqsMessage(
        message_id=str(uuid.uuid4()),
        body=dlq_body,
        md5_of_body=hashlib.md5(dlq_body.encode()).hexdigest(),
    )
    queue.put(msg)
    _log_invocation("dlq", dlq_arn, dlq_body)
    logger.info("EventBridge -> DLQ: %s", queue_name)


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
    return Response(
        content=body,
        status_code=status,
        media_type="application/x-amz-json-1.1",
    )


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
    "CreateArchive": _create_archive,
    "DescribeArchive": _describe_archive,
    "ListArchives": _list_archives,
    "DeleteArchive": _delete_archive,
    "StartReplay": _start_replay,
    "DescribeReplay": _describe_replay,
}
