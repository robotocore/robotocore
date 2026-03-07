"""Native Firehose provider with S3 delivery."""

import base64
import json
import threading
import time
import uuid
from collections.abc import Callable

from starlette.requests import Request
from starlette.responses import Response

_delivery_streams: dict[str, dict] = {}
_stream_buffers: dict[str, list[bytes]] = {}
_stream_tags: dict[str, list[dict[str, str]]] = {}
_lock = threading.Lock()
_worker_started = False
_worker_lock = threading.Lock()

BUFFER_SIZE = 1 * 1024 * 1024  # 1MB buffer before flushing
BUFFER_INTERVAL = 60  # seconds


class FirehoseError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


def _ensure_worker():
    global _worker_started
    with _worker_lock:
        if _worker_started:
            return
        _worker_started = True
        t = threading.Thread(target=_flush_worker, daemon=True)
        t.start()


def _flush_worker():
    while True:
        time.sleep(5)
        with _lock:
            for name in list(_stream_buffers.keys()):
                _flush_buffer(name)


def _flush_buffer(stream_name: str) -> None:
    """Flush buffered records to S3. Must be called with _lock held."""
    records = _stream_buffers.get(stream_name, [])
    if not records:
        return

    stream = _delivery_streams.get(stream_name)
    if not stream:
        return

    s3_config = stream.get("s3_config")
    if not s3_config:
        return

    bucket = s3_config.get("BucketARN", "").rsplit(":::", 1)[-1]
    prefix = s3_config.get("Prefix", "")
    if not bucket:
        return

    # Build the S3 key with timestamp-based partitioning
    now = time.gmtime()
    uid = uuid.uuid4().hex[:8]
    key = (
        f"{prefix}{now.tm_year}/{now.tm_mon:02d}/"
        f"{now.tm_mday:02d}/{now.tm_hour:02d}/{stream_name}-{uid}"
    )

    # Concatenate all records
    data = b"".join(records)
    _stream_buffers[stream_name] = []

    # Write to S3 via Moto's internal API
    _write_to_s3(bucket, key, data, stream.get("region", "us-east-1"))


def _write_to_s3(bucket: str, key: str, data: bytes, region: str) -> None:
    """Write data to S3 using Moto's backend directly."""
    try:
        from moto.backends import get_backend
        from moto.core import DEFAULT_ACCOUNT_ID

        s3_backend = get_backend("s3")[DEFAULT_ACCOUNT_ID]["global"]
        s3_backend.put_object(bucket, key, data)
    except Exception:
        pass


async def handle_firehose_request(request: Request, region: str, account_id: str) -> Response:
    """Handle a Firehose API request."""
    _ensure_worker()
    body = await request.body()
    target = request.headers.get("x-amz-target", "")

    if not target:
        return _error("InvalidAction", "Missing X-Amz-Target", 400)

    action = target.split(".")[-1]
    params = json.loads(body) if body else {}

    handler = _ACTION_MAP.get(action)
    if handler is None:
        return _error("InvalidAction", f"Unknown action: {action}", 400)

    try:
        result = handler(params, region, account_id)
        return Response(
            content=json.dumps(result),
            status_code=200,
            media_type="application/x-amz-json-1.1",
        )
    except FirehoseError as e:
        return _error(e.code, e.message, e.status)
    except Exception as e:
        return _error("InternalError", str(e), 500)


def _create_delivery_stream(params: dict, region: str, account_id: str) -> dict:
    name = params.get("DeliveryStreamName", "")
    if not name:
        raise FirehoseError("ValidationException", "DeliveryStreamName is required")

    with _lock:
        if name in _delivery_streams:
            raise FirehoseError("ResourceInUseException", f"Stream {name} already exists")

        s3_config = (
            params.get("ExtendedS3DestinationConfiguration")
            or params.get("S3DestinationConfiguration")
            or {}
        )

        stream = {
            "name": name,
            "arn": f"arn:aws:firehose:{region}:{account_id}:deliverystream/{name}",
            "status": "ACTIVE",
            "type": params.get("DeliveryStreamType", "DirectPut"),
            "s3_config": s3_config,
            "region": region,
            "account_id": account_id,
            "created": time.time(),
        }
        _delivery_streams[name] = stream
        _stream_buffers[name] = []
        _stream_tags[name] = list(params.get("Tags", []))

    return {"DeliveryStreamARN": stream["arn"]}


def _delete_delivery_stream(params: dict, region: str, account_id: str) -> dict:
    name = params.get("DeliveryStreamName", "")
    with _lock:
        if name not in _delivery_streams:
            raise FirehoseError("ResourceNotFoundException", f"Stream {name} not found")
        del _delivery_streams[name]
        _stream_buffers.pop(name, None)
        _stream_tags.pop(name, None)
    return {}


def _describe_delivery_stream(params: dict, region: str, account_id: str) -> dict:
    name = params.get("DeliveryStreamName", "")
    with _lock:
        stream = _delivery_streams.get(name)
        if not stream:
            raise FirehoseError("ResourceNotFoundException", f"Stream {name} not found")

    destinations = []
    if stream.get("s3_config"):
        destinations.append(
            {
                "DestinationId": "dest-1",
                "ExtendedS3DestinationDescription": {
                    "BucketARN": stream["s3_config"].get("BucketARN", ""),
                    "Prefix": stream["s3_config"].get("Prefix", ""),
                    "RoleARN": stream["s3_config"].get("RoleARN", ""),
                    "BufferingHints": stream["s3_config"].get("BufferingHints", {}),
                },
            }
        )

    return {
        "DeliveryStreamDescription": {
            "DeliveryStreamName": name,
            "DeliveryStreamARN": stream["arn"],
            "DeliveryStreamStatus": stream["status"],
            "DeliveryStreamType": stream["type"],
            "Destinations": destinations,
            "HasMoreDestinations": False,
            "CreateTimestamp": stream["created"],
        }
    }


def _list_delivery_streams(params: dict, region: str, account_id: str) -> dict:
    with _lock:
        names = sorted(_delivery_streams.keys())
    limit = params.get("Limit", 100)
    start = params.get("ExclusiveStartDeliveryStreamName")
    if start:
        try:
            idx = names.index(start) + 1
            names = names[idx:]
        except ValueError:
            pass
    names = names[:limit]
    return {
        "DeliveryStreamNames": names,
        "HasMoreDeliveryStreams": False,
    }


def _put_record(params: dict, region: str, account_id: str) -> dict:
    name = params.get("DeliveryStreamName", "")
    record = params.get("Record", {})
    data_b64 = record.get("Data", "")

    with _lock:
        if name not in _delivery_streams:
            raise FirehoseError("ResourceNotFoundException", f"Stream {name} not found")
        data = base64.b64decode(data_b64) if data_b64 else b""
        _stream_buffers.setdefault(name, []).append(data)

        # Flush if buffer exceeds threshold
        total = sum(len(r) for r in _stream_buffers[name])
        if total >= BUFFER_SIZE:
            _flush_buffer(name)

    return {
        "RecordId": uuid.uuid4().hex,
        "Encrypted": False,
    }


def _put_record_batch(params: dict, region: str, account_id: str) -> dict:
    name = params.get("DeliveryStreamName", "")
    records = params.get("Records", [])

    with _lock:
        if name not in _delivery_streams:
            raise FirehoseError("ResourceNotFoundException", f"Stream {name} not found")

        request_responses = []
        for rec in records:
            data_b64 = rec.get("Data", "")
            data = base64.b64decode(data_b64) if data_b64 else b""
            _stream_buffers.setdefault(name, []).append(data)
            request_responses.append({"RecordId": uuid.uuid4().hex})

        total = sum(len(r) for r in _stream_buffers[name])
        if total >= BUFFER_SIZE:
            _flush_buffer(name)

    return {
        "FailedPutCount": 0,
        "Encrypted": False,
        "RequestResponses": request_responses,
    }


def _tag_delivery_stream(params: dict, region: str, account_id: str) -> dict:
    name = params.get("DeliveryStreamName", "")
    tags = params.get("Tags", [])
    with _lock:
        if name not in _delivery_streams:
            raise FirehoseError("ResourceNotFoundException", f"Stream {name} not found")
        existing = _stream_tags.setdefault(name, [])
        # Merge: update existing keys, add new ones
        existing_keys = {t["Key"]: i for i, t in enumerate(existing)}
        for tag in tags:
            if tag["Key"] in existing_keys:
                existing[existing_keys[tag["Key"]]] = tag
            else:
                existing.append(tag)
    return {}


def _untag_delivery_stream(params: dict, region: str, account_id: str) -> dict:
    name = params.get("DeliveryStreamName", "")
    tag_keys = params.get("TagKeys", [])
    with _lock:
        if name not in _delivery_streams:
            raise FirehoseError("ResourceNotFoundException", f"Stream {name} not found")
        existing = _stream_tags.get(name, [])
        _stream_tags[name] = [t for t in existing if t["Key"] not in tag_keys]
    return {}


def _list_tags_for_delivery_stream(params: dict, region: str, account_id: str) -> dict:
    name = params.get("DeliveryStreamName", "")
    with _lock:
        if name not in _delivery_streams:
            raise FirehoseError("ResourceNotFoundException", f"Stream {name} not found")
        tags = list(_stream_tags.get(name, []))
    return {"Tags": tags, "HasMoreTags": False}


def _error(code: str, message: str, status: int) -> Response:
    body = json.dumps({"__type": code, "message": message})
    return Response(content=body, status_code=status, media_type="application/x-amz-json-1.1")


_ACTION_MAP: dict[str, Callable] = {
    "CreateDeliveryStream": _create_delivery_stream,
    "DeleteDeliveryStream": _delete_delivery_stream,
    "DescribeDeliveryStream": _describe_delivery_stream,
    "ListDeliveryStreams": _list_delivery_streams,
    "PutRecord": _put_record,
    "PutRecordBatch": _put_record_batch,
    "TagDeliveryStream": _tag_delivery_stream,
    "UntagDeliveryStream": _untag_delivery_stream,
    "ListTagsForDeliveryStream": _list_tags_for_delivery_stream,
}
