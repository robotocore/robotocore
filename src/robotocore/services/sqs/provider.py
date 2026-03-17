"""Native SQS provider with full message lifecycle support.

Uses JSON protocol (application/x-amz-json-1.0) as used by modern boto3.
Falls back to query protocol parsing for legacy clients.
"""

import hashlib
import json
import logging
import threading
import time
import uuid
from collections.abc import Callable

from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.sqs.behavioral import (
    PurgeQueueInProgressError,
    PurgeTracker,
    QueueDeletedRecentlyError,
    QueueDeletedTracker,
    RetentionScanner,
)
from robotocore.services.sqs.models import SqsMessage, SqsStore, StandardQueue

DEFAULT_ACCOUNT_ID = "123456789012"

_stores: dict[tuple[str, str], SqsStore] = {}
_store_lock = threading.Lock()
_worker_started = False
_worker_lock = threading.Lock()

# Behavioral fidelity singletons
_purge_tracker = PurgeTracker()
_delete_tracker = QueueDeletedTracker()
_retention_scanner = RetentionScanner()


logger = logging.getLogger(__name__)


def _get_store(region: str = "us-east-1", account_id: str = DEFAULT_ACCOUNT_ID) -> SqsStore:
    key = (account_id, region)
    with _store_lock:
        if key not in _stores:
            _stores[key] = SqsStore()
        return _stores[key]


def _ensure_worker():
    global _worker_started
    with _worker_lock:
        if _worker_started:
            return
        _worker_started = True
        t = threading.Thread(target=_background_worker, daemon=True)
        t.start()
        _retention_scanner.start(_stores)


def _background_worker():
    while True:
        time.sleep(1)
        for store in list(_stores.values()):
            try:
                store.requeue_all()
            except Exception as exc:  # noqa: BLE001
                logger.debug("_background_worker: requeue_all failed (non-fatal): %s", exc)


def _md5(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()


def _new_id() -> str:
    return str(uuid.uuid4())


async def handle_sqs_request(request: Request, region: str, account_id: str) -> Response:
    """Handle an SQS API request."""
    _ensure_worker()
    body = await request.body()
    content_type = request.headers.get("content-type", "")
    target = request.headers.get("x-amz-target", "")

    if target and "application/x-amz-json" in content_type:
        # JSON protocol
        action = target.split(".")[-1]
        params = json.loads(body) if body else {}
        use_json = True
    else:
        # Query protocol (legacy)
        from urllib.parse import parse_qs

        if "x-www-form-urlencoded" in content_type:
            parsed = parse_qs(body.decode(), keep_blank_values=True)
        else:
            parsed = parse_qs(str(request.url.query), keep_blank_values=True)
        params = {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}
        action = params.get("Action", "")
        use_json = False

    store = _get_store(region, account_id)
    handler = _ACTION_MAP.get(action)
    if handler is None:
        from robotocore.providers.moto_bridge import forward_to_moto

        return await forward_to_moto(request, "sqs", account_id=account_id)

    try:
        # ReceiveMessage may long-poll (block), so run in a thread to avoid
        # blocking the event loop and deadlocking cross-service callbacks.
        if action == "ReceiveMessage":
            import asyncio

            result = await asyncio.to_thread(handler, store, params, region, account_id, request)
        else:
            result = handler(store, params, region, account_id, request)
        if use_json:
            return _json_response(result)
        else:
            return _xml_response(action + "Response", result)
    except SqsError as e:
        return _error(e.code, e.message, e.status, use_json)
    except PurgeQueueInProgressError:
        return _error(
            "AWS.SimpleQueueService.PurgeQueueInProgress",
            "Only one PurgeQueue operation on the same queue is allowed every 60 seconds.",
            403,
            use_json,
        )
    except QueueDeletedRecentlyError as e:
        return _error(
            "AWS.SimpleQueueService.QueueDeletedRecently",
            str(e),
            400,
            use_json,
        )
    except Exception as e:  # noqa: BLE001
        return _error("InternalError", str(e), 500, use_json)


class SqsError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


def _resolve_queue(store: SqsStore, params: dict, request: Request) -> StandardQueue:
    """Find queue from QueueUrl param or request path."""
    url = params.get("QueueUrl", "")
    if url:
        queue = store.get_queue_by_url(url)
        if queue:
            return queue
    # Try request path: /account_id/queue_name
    path = request.url.path.strip("/")
    parts = path.split("/")
    if len(parts) >= 2:
        queue = store.get_queue(parts[-1])
        if queue:
            return queue
    raise SqsError("AWS.SimpleQueueService.NonExistentQueue", "The specified queue does not exist.")


# --- Actions (return dicts, serialized by caller) ---


def _create_queue(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    name = params.get("QueueName", "")
    _delete_tracker.check_create(name)
    attributes = params.get("Attributes", {})
    tags = params.get("Tags", params.get("tags", {}))
    # Query protocol: Attribute.N.Name/Value
    for key, value in params.items():
        if key.startswith("Attribute.") and key.endswith(".Name"):
            idx = key.split(".")[1]
            attributes[value] = params.get(f"Attribute.{idx}.Value", "")
    queue = store.create_queue(name, region, account_id, attributes)
    if tags:
        queue.tags.update(tags)
    return {"QueueUrl": queue.url}


def _delete_queue(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    url = params.get("QueueUrl", "")
    queue = store.get_queue_by_url(url)
    if not queue:
        raise SqsError(
            "AWS.SimpleQueueService.NonExistentQueue",
            "The specified queue does not exist.",
        )
    store.delete_queue(queue.name)
    _delete_tracker.record_deletion(queue.name)
    _purge_tracker.remove(queue.name)
    return {}


def _get_queue_url(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    name = params.get("QueueName", "")
    queue = store.get_queue(name)
    if not queue:
        raise SqsError("AWS.SimpleQueueService.NonExistentQueue", f"Queue {name} does not exist.")
    return {"QueueUrl": queue.url}


def _list_queues(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    prefix = params.get("QueueNamePrefix")
    queues = store.list_queues(prefix)
    return {"QueueUrls": [q.url for q in queues]}


def _send_message(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    queue = _resolve_queue(store, params, request)
    body_text = params.get("MessageBody", "")
    message_id = _new_id()
    md5_body = _md5(body_text)
    delay = int(params.get("DelaySeconds", "0"))

    # Track metrics
    from robotocore.services.sqs.metrics import increment_sent

    increment_sent(queue.name, len(body_text.encode("utf-8")))

    msg = SqsMessage(
        message_id=message_id,
        body=body_text,
        md5_of_body=md5_body,
        delay_seconds=delay,
        message_group_id=params.get("MessageGroupId"),
        message_deduplication_id=params.get("MessageDeduplicationId"),
    )
    _parse_message_attributes(params, msg)
    _parse_system_attributes(params, msg)

    if queue.is_fifo:
        if not params.get("MessageGroupId"):
            raise SqsError(
                "MissingParameter",
                "The request must contain the parameter MessageGroupId.",
            )
        if not params.get("MessageDeduplicationId") and not getattr(
            queue, "content_based_dedup", False
        ):
            raise SqsError(
                "InvalidParameterValue",
                "The queue should either have ContentBasedDeduplication enabled or "
                "MessageDeduplicationId provided explicitly.",
            )
        result = queue.put(msg)
        message_id = result.message_id
        md5_body = result.md5_of_body
    else:
        queue.put(msg)

    resp = {"MessageId": message_id, "MD5OfMessageBody": md5_body}
    if msg.sequence_number:
        resp["SequenceNumber"] = msg.sequence_number
    return resp


def _receive_message(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    queue = _resolve_queue(store, params, request)
    max_msgs = int(params.get("MaxNumberOfMessages", "1"))
    if max_msgs < 1 or max_msgs > 10:
        raise SqsError(
            "InvalidParameterValue",
            f"Value {max_msgs} for parameter MaxNumberOfMessages is invalid. "
            "Reason: Must be between 1 and 10, if provided.",
        )
    vis_timeout = params.get("VisibilityTimeout")
    if vis_timeout is not None:
        vis_timeout = int(vis_timeout)
    wait_time = params.get("WaitTimeSeconds")
    if wait_time is not None:
        wait_time = int(wait_time)

    results = queue.receive(
        max_messages=max_msgs, visibility_timeout=vis_timeout, wait_time_seconds=wait_time
    )

    # DLQ redrive
    valid = []
    for msg, receipt in results:
        if queue.max_receive_count and msg.receive_count > queue.max_receive_count:
            _move_to_dlq(store, queue, msg)
        else:
            valid.append((msg, receipt))

    # Track metrics
    from robotocore.services.sqs.metrics import (
        increment_empty_receives,
        increment_received,
    )

    if valid:
        increment_received(queue.name, len(valid))
    else:
        increment_empty_receives(queue.name)

    messages = []
    for msg, receipt in valid:
        m = {
            "MessageId": msg.message_id,
            "ReceiptHandle": receipt,
            "MD5OfBody": msg.md5_of_body,
            "Body": msg.body,
            "Attributes": {
                "SenderId": account_id,
                "SentTimestamp": str(int(msg.created * 1000)),
                "ApproximateReceiveCount": str(msg.receive_count),
                "ApproximateFirstReceiveTimestamp": str(int((msg.first_received or 0) * 1000)),
            },
        }
        if msg.message_group_id:
            m["Attributes"]["MessageGroupId"] = msg.message_group_id
        if msg.sequence_number:
            m["Attributes"]["SequenceNumber"] = msg.sequence_number
        if msg.system_attributes:
            for k, v in msg.system_attributes.items():
                m["Attributes"][k] = v.get("StringValue", "") if isinstance(v, dict) else str(v)
        if msg.message_attributes:
            m["MessageAttributes"] = msg.message_attributes
        messages.append(m)

    return {"Messages": messages} if messages else {}


def _delete_message(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    queue = _resolve_queue(store, params, request)
    receipt = params.get("ReceiptHandle", "")
    queue.delete_message(receipt)

    from robotocore.services.sqs.metrics import increment_deleted

    increment_deleted(queue.name)
    return {}


def _get_queue_attributes(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    queue = _resolve_queue(store, params, request)
    return {"Attributes": queue.get_attributes()}


def _set_queue_attributes(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    queue = _resolve_queue(store, params, request)
    attrs = params.get("Attributes", {})
    for key, value in params.items():
        if key.startswith("Attribute.") and key.endswith(".Name"):
            idx = key.split(".")[1]
            attrs[value] = params.get(f"Attribute.{idx}.Value", "")
    queue.attributes.update(attrs)
    return {}


def _purge_queue(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    queue = _resolve_queue(store, params, request)
    _purge_tracker.check_and_record(queue.name)
    queue.purge()
    return {}


def _change_message_visibility(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    queue = _resolve_queue(store, params, request)
    receipt = params.get("ReceiptHandle", "")
    timeout = int(params.get("VisibilityTimeout", "30"))
    ok = queue.change_visibility(receipt, timeout)
    if not ok:
        raise SqsError(
            "ReceiptHandleIsInvalid",
            "The input receipt handle is invalid.",
        )
    return {}


def _send_message_batch(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    queue = _resolve_queue(store, params, request)
    successful = []
    entries = params.get("Entries", [])
    # Query protocol
    i = 1
    while f"SendMessageBatchRequestEntry.{i}.Id" in params:
        entries.append(
            {
                "Id": params[f"SendMessageBatchRequestEntry.{i}.Id"],
                "MessageBody": params.get(f"SendMessageBatchRequestEntry.{i}.MessageBody", ""),
                "DelaySeconds": params.get(f"SendMessageBatchRequestEntry.{i}.DelaySeconds", "0"),
                "MessageGroupId": params.get(f"SendMessageBatchRequestEntry.{i}.MessageGroupId"),
                "MessageDeduplicationId": params.get(
                    f"SendMessageBatchRequestEntry.{i}.MessageDeduplicationId"
                ),
            }
        )
        i += 1

    # Validate batch size <= 10
    if len(entries) > 10:
        raise SqsError(
            "TooManyEntriesInBatchRequest",
            "Maximum number of entries per request are 10. You have sent 11.",
        )

    # Validate no duplicate IDs
    seen_ids = set()
    for entry in entries:
        entry_id = entry.get("Id", "")
        if entry_id in seen_ids:
            raise SqsError(
                "BatchEntryIdsNotDistinct",
                f"Id {entry_id} repeated.",
            )
        seen_ids.add(entry_id)

    from robotocore.services.sqs.metrics import increment_sent

    for entry in entries:
        msg_id = _new_id()
        body_text = entry.get("MessageBody", "")
        md5_body = _md5(body_text)
        increment_sent(queue.name, len(body_text.encode("utf-8")))
        msg = SqsMessage(
            message_id=msg_id,
            body=body_text,
            md5_of_body=md5_body,
            delay_seconds=int(entry.get("DelaySeconds", "0") or "0"),
            message_group_id=entry.get("MessageGroupId"),
            message_deduplication_id=entry.get("MessageDeduplicationId"),
        )
        result = queue.put(msg)
        if queue.is_fifo and result is not None:
            msg_id = result.message_id
            md5_body = result.md5_of_body
        successful.append(
            {
                "Id": entry.get("Id", ""),
                "MessageId": msg_id,
                "MD5OfMessageBody": md5_body,
            }
        )

    return {"Successful": successful, "Failed": []}


def _delete_message_batch(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    queue = _resolve_queue(store, params, request)
    successful = []
    entries = params.get("Entries", [])
    i = 1
    while f"DeleteMessageBatchRequestEntry.{i}.Id" in params:
        entries.append(
            {
                "Id": params[f"DeleteMessageBatchRequestEntry.{i}.Id"],
                "ReceiptHandle": params.get(
                    f"DeleteMessageBatchRequestEntry.{i}.ReceiptHandle", ""
                ),
            }
        )
        i += 1

    failed = []
    for entry in entries:
        ok = queue.delete_message(entry.get("ReceiptHandle", ""))
        if ok:
            successful.append({"Id": entry.get("Id", "")})
        else:
            failed.append(
                {
                    "Id": entry.get("Id", ""),
                    "Code": "ReceiptHandleIsInvalid",
                    "Message": "The input receipt handle is invalid.",
                    "SenderFault": True,
                }
            )

    return {"Successful": successful, "Failed": failed}


def _change_message_visibility_batch(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    queue = _resolve_queue(store, params, request)
    successful = []
    failed = []
    entries = params.get("Entries", [])
    # Query protocol
    i = 1
    while f"ChangeMessageVisibilityBatchRequestEntry.{i}.Id" in params:
        entries.append(
            {
                "Id": params[f"ChangeMessageVisibilityBatchRequestEntry.{i}.Id"],
                "ReceiptHandle": params.get(
                    f"ChangeMessageVisibilityBatchRequestEntry.{i}.ReceiptHandle", ""
                ),
                "VisibilityTimeout": params.get(
                    f"ChangeMessageVisibilityBatchRequestEntry.{i}.VisibilityTimeout", "30"
                ),
            }
        )
        i += 1

    for entry in entries:
        receipt = entry.get("ReceiptHandle", "")
        timeout = int(entry.get("VisibilityTimeout", "30"))
        ok = queue.change_visibility(receipt, timeout)
        if ok:
            successful.append({"Id": entry.get("Id", "")})
        else:
            failed.append(
                {
                    "Id": entry.get("Id", ""),
                    "Code": "ReceiptHandleIsInvalid",
                    "Message": "The input receipt handle is invalid.",
                    "SenderFault": True,
                }
            )

    return {"Successful": successful, "Failed": failed}


def _add_permission(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    queue = _resolve_queue(store, params, request)
    label = params.get("Label", "")
    aws_account_ids = params.get("AWSAccountIds", [])
    actions = params.get("Actions", [])
    # Query protocol
    i = 1
    while f"AWSAccountId.{i}" in params:
        aws_account_ids.append(params[f"AWSAccountId.{i}"])
        i += 1
    i = 1
    while f"ActionName.{i}" in params:
        actions.append(params[f"ActionName.{i}"])
        i += 1

    # Build or update the policy
    policy_str = queue.attributes.get("Policy")
    if policy_str:
        policy = json.loads(policy_str) if isinstance(policy_str, str) else policy_str
    else:
        policy = {
            "Version": "2012-10-17",
            "Id": f"{queue.arn}/SQSDefaultPolicy",
            "Statement": [],
        }

    if not actions:
        raise SqsError("MissingParameter", "Actions must contain at least one entry.")
    action_list = [f"SQS:{a}" for a in actions]
    statement = {
        "Sid": label,
        "Effect": "Allow",
        "Principal": {"AWS": [f"arn:aws:iam::{aid}:root" for aid in aws_account_ids]},
        "Action": action_list if len(action_list) > 1 else action_list[0],
        "Resource": queue.arn,
    }
    # Remove any existing statement with the same label
    policy["Statement"] = [s for s in policy["Statement"] if s.get("Sid") != label]
    policy["Statement"].append(statement)
    queue.attributes["Policy"] = json.dumps(policy)
    return {}


def _remove_permission(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    queue = _resolve_queue(store, params, request)
    label = params.get("Label", "")
    policy_str = queue.attributes.get("Policy")
    if policy_str:
        policy = json.loads(policy_str) if isinstance(policy_str, str) else policy_str
        policy["Statement"] = [s for s in policy["Statement"] if s.get("Sid") != label]
        queue.attributes["Policy"] = json.dumps(policy)
    return {}


def _list_dead_letter_source_queues(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    queue = _resolve_queue(store, params, request)
    target_arn = queue.arn
    source_urls = []
    for q in store.list_queues():
        rp = q.redrive_policy
        if rp and rp.get("deadLetterTargetArn") == target_arn:
            source_urls.append(q.url)
    return {"queueUrls": source_urls}


def _tag_queue(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    queue = _resolve_queue(store, params, request)
    tags = params.get("Tags", {})
    # Query protocol: Tag.N.Key / Tag.N.Value
    i = 1
    while f"Tag.{i}.Key" in params:
        key = params[f"Tag.{i}.Key"]
        value = params.get(f"Tag.{i}.Value", "")
        tags[key] = value
        i += 1
    queue.tags.update(tags)
    return {}


def _untag_queue(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    queue = _resolve_queue(store, params, request)
    tag_keys = params.get("TagKeys", [])
    # Query protocol: TagKey.N
    i = 1
    while f"TagKey.{i}" in params:
        tag_keys.append(params[f"TagKey.{i}"])
        i += 1
    for key in tag_keys:
        queue.tags.pop(key, None)
    return {}


def _list_queue_tags(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    queue = _resolve_queue(store, params, request)
    return {"Tags": dict(queue.tags)}


def _start_message_move_task(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    source_arn = params.get("SourceArn", "")
    destination_arn = params.get("DestinationArn")
    max_per_second = int(params.get("MaxNumberOfMessagesPerSecond", "500"))

    try:
        task = store.start_message_move_task(source_arn, destination_arn, max_per_second)
    except ValueError as e:
        raise SqsError("ResourceNotFoundException", str(e)) from e

    return {"TaskHandle": task.task_handle}


def _cancel_message_move_task(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    task_handle = params.get("TaskHandle", "")
    task = store.cancel_message_move_task(task_handle)
    if not task:
        raise SqsError("ResourceNotFoundException", "Task not found")
    return {
        "ApproximateNumberOfMessagesMoved": task.approximate_number_of_messages_moved,
    }


def _list_message_move_tasks(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    source_arn = params.get("SourceArn", "")
    tasks = store.list_message_move_tasks(source_arn)
    results = []
    for task in tasks:
        t = {
            "TaskHandle": task.task_handle,
            "SourceArn": task.source_arn,
            "Status": task.status,
            "MaxNumberOfMessagesPerSecond": task.max_number_of_messages_per_second,
            "ApproximateNumberOfMessagesMoved": (task.approximate_number_of_messages_moved),
            "ApproximateNumberOfMessagesToMove": (task.approximate_number_of_messages_to_move),
            "StartedTimestamp": int(task.started_timestamp * 1000),
        }
        if task.destination_arn:
            t["DestinationArn"] = task.destination_arn
        if task.failure_reason:
            t["FailureReason"] = task.failure_reason
        results.append(t)
    return {"Results": results}


# --- Helpers ---


def _move_to_dlq(store: SqsStore, queue: StandardQueue, msg: SqsMessage) -> None:
    policy = queue.redrive_policy
    if not policy:
        return
    dl_arn = policy.get("deadLetterTargetArn", "")
    dl_queue = store.get_queue_by_arn(dl_arn)
    if dl_queue:
        dl_msg = SqsMessage(
            message_id=_new_id(),
            body=msg.body,
            md5_of_body=msg.md5_of_body,
            message_attributes=msg.message_attributes,
        )
        dl_queue.put(dl_msg)
    msg.deleted = True


def _parse_message_attributes(params: dict, msg: SqsMessage) -> None:
    i = 1
    while f"MessageAttribute.{i}.Name" in params:
        name = params[f"MessageAttribute.{i}.Name"]
        data_type = params.get(f"MessageAttribute.{i}.Value.DataType", "String")
        string_value = params.get(f"MessageAttribute.{i}.Value.StringValue", "")
        msg.message_attributes[name] = {"DataType": data_type, "StringValue": string_value}
        i += 1
    if "MessageAttributes" in params:
        msg.message_attributes.update(params["MessageAttributes"])


def _parse_system_attributes(params: dict, msg: SqsMessage) -> None:
    """Parse MessageSystemAttributes from request params."""
    i = 1
    while f"MessageSystemAttribute.{i}.Name" in params:
        name = params[f"MessageSystemAttribute.{i}.Name"]
        data_type = params.get(f"MessageSystemAttribute.{i}.Value.DataType", "String")
        string_value = params.get(f"MessageSystemAttribute.{i}.Value.StringValue", "")
        msg.system_attributes[name] = {
            "DataType": data_type,
            "StringValue": string_value,
        }
        i += 1
    if "MessageSystemAttributes" in params:
        msg.system_attributes.update(params["MessageSystemAttributes"])


def _json_response(data: dict) -> Response:
    return Response(
        content=json.dumps(data),
        status_code=200,
        media_type="application/x-amz-json-1.0",
    )


def _xml_response(action: str, data: dict) -> Response:
    """Build XML response from dict for legacy query protocol."""
    from xml.sax.saxutils import escape as xml_escape

    # Simple XML serialization for basic responses
    def dict_to_xml(d: dict, indent: int = 2) -> str:
        parts = []
        for k, v in d.items():
            if isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        parts.append(f"<{k}>{dict_to_xml(item, indent + 1)}</{k}>")
                    else:
                        parts.append(f"<{k}>{xml_escape(str(item))}</{k}>")
            elif isinstance(v, dict):
                parts.append(f"<{k}>{dict_to_xml(v, indent + 1)}</{k}>")
            else:
                parts.append(f"<{k}>{xml_escape(str(v))}</{k}>")
        return "".join(parts)

    result_name = action.replace("Response", "Result")
    body_xml = dict_to_xml(data)
    xml = (
        f'<?xml version="1.0"?>'
        f'<{action} xmlns="http://queue.amazonaws.com/doc/2012-11-05/">'
        f"<{result_name}>{body_xml}</{result_name}>"
        f"<ResponseMetadata><RequestId>{_new_id()}</RequestId></ResponseMetadata>"
        f"</{action}>"
    )
    return Response(content=xml, status_code=200, media_type="text/xml")


def _error(code: str, message: str, status: int, use_json: bool) -> Response:
    if use_json:
        body = json.dumps({"__type": code, "message": message})
        return Response(content=body, status_code=status, media_type="application/x-amz-json-1.0")
    xml = (
        f'<?xml version="1.0"?>'
        f'<ErrorResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">'
        f"<Error><Type>Sender</Type><Code>{code}</Code>"
        f"<Message>{message}</Message></Error>"
        f"<RequestId>{_new_id()}</RequestId>"
        f"</ErrorResponse>"
    )
    return Response(content=xml, status_code=status, media_type="text/xml")


_ACTION_MAP: dict[str, Callable] = {
    "CreateQueue": _create_queue,
    "DeleteQueue": _delete_queue,
    "SendMessage": _send_message,
    "ReceiveMessage": _receive_message,
    "DeleteMessage": _delete_message,
    "GetQueueAttributes": _get_queue_attributes,
    "SetQueueAttributes": _set_queue_attributes,
    "ListQueues": _list_queues,
    "GetQueueUrl": _get_queue_url,
    "PurgeQueue": _purge_queue,
    "ChangeMessageVisibility": _change_message_visibility,
    "ChangeMessageVisibilityBatch": _change_message_visibility_batch,
    "AddPermission": _add_permission,
    "RemovePermission": _remove_permission,
    "ListDeadLetterSourceQueues": _list_dead_letter_source_queues,
    "SendMessageBatch": _send_message_batch,
    "DeleteMessageBatch": _delete_message_batch,
    "TagQueue": _tag_queue,
    "UntagQueue": _untag_queue,
    "ListQueueTags": _list_queue_tags,
    "StartMessageMoveTask": _start_message_move_task,
    "CancelMessageMoveTask": _cancel_message_move_task,
    "ListMessageMoveTasks": _list_message_move_tasks,
}
