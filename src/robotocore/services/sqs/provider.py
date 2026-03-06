"""Native SQS provider with full message lifecycle support.

Uses JSON protocol (application/x-amz-json-1.0) as used by modern boto3.
Falls back to query protocol parsing for legacy clients.
"""

import hashlib
import json
import threading
import time
import uuid
from collections.abc import Callable

from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.sqs.models import SqsMessage, SqsStore, StandardQueue

_stores: dict[str, SqsStore] = {}
_store_lock = threading.Lock()
_worker_started = False
_worker_lock = threading.Lock()


def _get_store(region: str = "us-east-1") -> SqsStore:
    with _store_lock:
        if region not in _stores:
            _stores[region] = SqsStore()
        return _stores[region]


def _ensure_worker():
    global _worker_started
    with _worker_lock:
        if _worker_started:
            return
        _worker_started = True
        t = threading.Thread(target=_background_worker, daemon=True)
        t.start()


def _background_worker():
    while True:
        time.sleep(1)
        for store in list(_stores.values()):
            try:
                store.requeue_all()
            except Exception:
                pass


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

    store = _get_store(region)
    handler = _ACTION_MAP.get(action)
    if handler is None:
        return _error("InvalidAction", f"Unknown action: {action}", 400, use_json)

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
    except Exception as e:
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
    attributes = params.get("Attributes", {})
    # Query protocol: Attribute.N.Name/Value
    for key, value in params.items():
        if key.startswith("Attribute.") and key.endswith(".Name"):
            idx = key.split(".")[1]
            attributes[value] = params.get(f"Attribute.{idx}.Value", "")
    queue = store.create_queue(name, region, account_id, attributes)
    return {"QueueUrl": queue.url}


def _delete_queue(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    url = params.get("QueueUrl", "")
    queue = store.get_queue_by_url(url)
    if queue:
        store.delete_queue(queue.name)
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

    msg = SqsMessage(
        message_id=message_id,
        body=body_text,
        md5_of_body=md5_body,
        delay_seconds=delay,
        message_group_id=params.get("MessageGroupId"),
        message_deduplication_id=params.get("MessageDeduplicationId"),
    )
    _parse_message_attributes(params, msg)

    if queue.is_fifo:
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
    max_msgs = min(int(params.get("MaxNumberOfMessages", "1")), 10)
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
    queue.purge()
    return {}


def _change_message_visibility(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    queue = _resolve_queue(store, params, request)
    receipt = params.get("ReceiptHandle", "")
    timeout = int(params.get("VisibilityTimeout", "30"))
    queue.change_visibility(receipt, timeout)
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

    for entry in entries:
        msg_id = _new_id()
        body_text = entry.get("MessageBody", "")
        md5_body = _md5(body_text)
        msg = SqsMessage(
            message_id=msg_id,
            body=body_text,
            md5_of_body=md5_body,
            delay_seconds=int(entry.get("DelaySeconds", "0") or "0"),
            message_group_id=entry.get("MessageGroupId"),
            message_deduplication_id=entry.get("MessageDeduplicationId"),
        )
        queue.put(msg)
        successful.append(
            {"Id": entry.get("Id", ""), "MessageId": msg_id, "MD5OfMessageBody": md5_body}
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

    for entry in entries:
        queue.delete_message(entry.get("ReceiptHandle", ""))
        successful.append({"Id": entry.get("Id", "")})

    return {"Successful": successful, "Failed": []}


def _tag_queue(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    return {}


def _untag_queue(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    return {}


def _list_queue_tags(
    store: SqsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    return {"Tags": {}}


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


def _json_response(data: dict) -> Response:
    return Response(
        content=json.dumps(data),
        status_code=200,
        media_type="application/x-amz-json-1.0",
    )


def _xml_response(action: str, data: dict) -> Response:
    """Build XML response from dict for legacy query protocol."""

    # Simple XML serialization for basic responses
    def dict_to_xml(d: dict, indent: int = 2) -> str:
        parts = []
        for k, v in d.items():
            if isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        parts.append(f"<{k}>{dict_to_xml(item, indent + 1)}</{k}>")
                    else:
                        parts.append(f"<{k}>{item}</{k}>")
            elif isinstance(v, dict):
                parts.append(f"<{k}>{dict_to_xml(v, indent + 1)}</{k}>")
            else:
                parts.append(f"<{k}>{v}</{k}>")
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
        f"<Error><Type>Sender</Type><Code>{code}</Code><Message>{message}</Message></Error>"
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
    "SendMessageBatch": _send_message_batch,
    "DeleteMessageBatch": _delete_message_batch,
    "TagQueue": _tag_queue,
    "UntagQueue": _untag_queue,
    "ListQueueTags": _list_queue_tags,
}
