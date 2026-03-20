"""Native Firehose provider with S3 delivery."""

import base64
import json
import logging
import threading
import time
import uuid
from collections.abc import Callable

from starlette.requests import Request
from starlette.responses import Response

_delivery_streams: dict[tuple[str, str, str], dict] = {}  # (account_id, region, name) -> stream
_stream_buffers: dict[tuple[str, str, str], list[bytes]] = {}  # same key -> records
_lock = threading.Lock()
_worker_started = False
_worker_lock = threading.Lock()

BUFFER_SIZE = 1 * 1024 * 1024  # 1MB buffer before flushing
BUFFER_INTERVAL = 60  # seconds


logger = logging.getLogger(__name__)


def _key(name: str, region: str, account_id: str) -> tuple[str, str, str]:
    """Build a scoped key for the global dicts."""
    return (account_id, region, name)


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
            for k in list(_stream_buffers.keys()):
                _flush_buffer(k)


def _flush_buffer(stream_key: tuple[str, str, str]) -> None:
    """Flush buffered records to S3. Must be called with _lock held."""
    records = _stream_buffers.get(stream_key, [])
    if not records:
        return

    stream = _delivery_streams.get(stream_key)
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
    stream_name = stream["name"]
    s3_key = (
        f"{prefix}{now.tm_year}/{now.tm_mon:02d}/"
        f"{now.tm_mday:02d}/{now.tm_hour:02d}/{stream_name}-{uid}"
    )

    # Concatenate all records
    data = b"".join(records)
    _stream_buffers[stream_key] = []

    # Write to S3 via Moto's internal API
    _write_to_s3(bucket, s3_key, data, stream.get("region", "us-east-1"))


def _write_to_s3(bucket: str, key: str, data: bytes, region: str) -> None:
    """Write data to S3 using Moto's backend directly."""
    try:
        from moto.backends import get_backend  # noqa: I001
        from moto.core import DEFAULT_ACCOUNT_ID

        s3_backend = get_backend("s3")[DEFAULT_ACCOUNT_ID]["global"]
        s3_backend.put_object(bucket, key, data)
    except Exception as exc:  # noqa: BLE001
        logger.debug("_write_to_s3: put_object failed (non-fatal): %s", exc)


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
        from robotocore.providers.moto_bridge import forward_to_moto

        return await forward_to_moto(request, "firehose", account_id=account_id)

    try:
        result = handler(params, region, account_id)
        return Response(
            content=json.dumps(result),
            status_code=200,
            media_type="application/x-amz-json-1.1",
        )
    except FirehoseError as e:
        return _error(e.code, e.message, e.status)
    except Exception as e:  # noqa: BLE001
        return _error("InternalError", str(e), 500)


def _create_delivery_stream(params: dict, region: str, account_id: str) -> dict:
    name = params.get("DeliveryStreamName", "")
    if not name:
        raise FirehoseError("ValidationException", "DeliveryStreamName is required")

    k = _key(name, region, account_id)
    with _lock:
        if k in _delivery_streams:
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
            "version_id": 1,
            "tags": {},
        }
        # Store initial tags if provided
        initial_tags = params.get("Tags", [])
        for tag in initial_tags:
            stream["tags"][tag["Key"]] = tag.get("Value", "")
        _delivery_streams[k] = stream
        _stream_buffers[k] = []

    return {"DeliveryStreamARN": stream["arn"]}


def _delete_delivery_stream(params: dict, region: str, account_id: str) -> dict:
    name = params.get("DeliveryStreamName", "")
    k = _key(name, region, account_id)
    with _lock:
        if k not in _delivery_streams:
            raise FirehoseError("ResourceNotFoundException", f"Stream {name} not found")
        del _delivery_streams[k]
        _stream_buffers.pop(k, None)
    return {}


def _describe_delivery_stream(params: dict, region: str, account_id: str) -> dict:
    name = params.get("DeliveryStreamName", "")
    k = _key(name, region, account_id)
    with _lock:
        stream = _delivery_streams.get(k)
        if not stream:
            raise FirehoseError("ResourceNotFoundException", f"Stream {name} not found")

        # Build the full response inside the lock to avoid TOCTOU races
        destinations = []
        if stream.get("s3_config"):
            s3_desc = {
                "BucketARN": stream["s3_config"].get("BucketARN", ""),
                "Prefix": stream["s3_config"].get("Prefix", ""),
                "RoleARN": stream["s3_config"].get("RoleARN", ""),
                "BufferingHints": stream["s3_config"].get("BufferingHints", {}),
                "CompressionFormat": stream["s3_config"].get("CompressionFormat", "UNCOMPRESSED"),
            }
            if "ErrorOutputPrefix" in stream["s3_config"]:
                s3_desc["ErrorOutputPrefix"] = stream["s3_config"]["ErrorOutputPrefix"]
            destinations.append(
                {
                    "DestinationId": "dest-1",
                    "ExtendedS3DestinationDescription": s3_desc,
                }
            )

        desc = {
            "DeliveryStreamName": name,
            "DeliveryStreamARN": stream["arn"],
            "DeliveryStreamStatus": stream["status"],
            "DeliveryStreamType": stream["type"],
            "VersionId": str(stream.get("version_id", 1)),
            "Destinations": destinations,
            "HasMoreDestinations": False,
            "CreateTimestamp": stream["created"],
        }

        encryption = stream.get("encryption")
        if encryption:
            desc["DeliveryStreamEncryptionConfiguration"] = encryption

    return {"DeliveryStreamDescription": desc}


def _list_delivery_streams(params: dict, region: str, account_id: str) -> dict:
    with _lock:
        names = sorted(
            stream_key[2]
            for stream_key in _delivery_streams
            if stream_key[0] == account_id and stream_key[1] == region
        )
    limit = params.get("Limit", 100)
    start = params.get("ExclusiveStartDeliveryStreamName")
    if start:
        try:
            idx = names.index(start) + 1
            names = names[idx:]
        except ValueError as exc:
            logger.debug("_list_delivery_streams: index failed (non-fatal): %s", exc)
    has_more = len(names) > limit
    names = names[:limit]
    return {
        "DeliveryStreamNames": names,
        "HasMoreDeliveryStreams": has_more,
    }


def _put_record(params: dict, region: str, account_id: str) -> dict:
    name = params.get("DeliveryStreamName", "")
    record = params.get("Record", {})
    data_b64 = record.get("Data", "")

    k = _key(name, region, account_id)
    with _lock:
        if k not in _delivery_streams:
            raise FirehoseError("ResourceNotFoundException", f"Stream {name} not found")
        data = base64.b64decode(data_b64) if data_b64 else b""
        _stream_buffers.setdefault(k, []).append(data)

        # Flush if buffer exceeds threshold
        total = sum(len(r) for r in _stream_buffers[k])
        if total >= BUFFER_SIZE:
            _flush_buffer(k)

    return {
        "RecordId": uuid.uuid4().hex,
        "Encrypted": False,
    }


def _put_record_batch(params: dict, region: str, account_id: str) -> dict:
    name = params.get("DeliveryStreamName", "")
    records = params.get("Records", [])

    k = _key(name, region, account_id)
    with _lock:
        if k not in _delivery_streams:
            raise FirehoseError("ResourceNotFoundException", f"Stream {name} not found")

        request_responses = []
        for rec in records:
            data_b64 = rec.get("Data", "")
            data = base64.b64decode(data_b64) if data_b64 else b""
            _stream_buffers.setdefault(k, []).append(data)
            request_responses.append({"RecordId": uuid.uuid4().hex})

        total = sum(len(r) for r in _stream_buffers[k])
        if total >= BUFFER_SIZE:
            _flush_buffer(k)

    return {
        "FailedPutCount": 0,
        "Encrypted": False,
        "RequestResponses": request_responses,
    }


def _update_destination(params: dict, region: str, account_id: str) -> dict:
    name = params.get("DeliveryStreamName", "")
    destination_id = params.get("DestinationId", "")

    current_version = params.get("CurrentDeliveryStreamVersionId")

    k = _key(name, region, account_id)
    with _lock:
        stream = _delivery_streams.get(k)
        if not stream:
            raise FirehoseError("ResourceNotFoundException", f"Stream {name} not found")

        if not destination_id:
            raise FirehoseError("ValidationException", "DestinationId is required")

        # Validate version ID if provided
        if current_version is not None:
            expected = str(stream.get("version_id", 1))
            if str(current_version) != expected:
                raise FirehoseError(
                    "InvalidArgumentException",
                    f"Version mismatch: expected {expected}, got {current_version}",
                )

        # Merge updates into existing s3_config
        s3_update = (
            params.get("ExtendedS3DestinationUpdate") or params.get("S3DestinationUpdate") or {}
        )
        if s3_update:
            s3_config = stream.get("s3_config", {})
            for s3_field, value in s3_update.items():
                if isinstance(value, dict) and isinstance(s3_config.get(s3_field), dict):
                    s3_config[s3_field].update(value)
                else:
                    s3_config[s3_field] = value
            stream["s3_config"] = s3_config

        stream["version_id"] = stream.get("version_id", 1) + 1

    return {}


def _start_delivery_stream_encryption(params: dict, region: str, account_id: str) -> dict:
    name = params.get("DeliveryStreamName", "")

    k = _key(name, region, account_id)
    with _lock:
        stream = _delivery_streams.get(k)
        if not stream:
            raise FirehoseError("ResourceNotFoundException", f"Stream {name} not found")

        encryption_config = (
            params.get("DeliveryStreamEncryptionConfigurationInput")
            or params.get("DeliveryStreamEncryptionInput")
            or {}
        )
        stream["encryption"] = {
            "KeyType": encryption_config.get("KeyType", "AWS_OWNED_CMK"),
            "KeyARN": encryption_config.get("KeyARN"),
            "Status": "ENABLED",
        }

    return {}


def _stop_delivery_stream_encryption(params: dict, region: str, account_id: str) -> dict:
    name = params.get("DeliveryStreamName", "")

    k = _key(name, region, account_id)
    with _lock:
        stream = _delivery_streams.get(k)
        if not stream:
            raise FirehoseError("ResourceNotFoundException", f"Stream {name} not found")

        stream["encryption"] = {
            "Status": "DISABLED",
        }

    return {}


def _tag_delivery_stream(params: dict, region: str, account_id: str) -> dict:
    name = params.get("DeliveryStreamName", "")
    tags = params.get("Tags", [])
    k = _key(name, region, account_id)
    with _lock:
        stream = _delivery_streams.get(k)
        if not stream:
            raise FirehoseError("ResourceNotFoundException", f"Stream {name} not found")
        for tag in tags:
            stream["tags"][tag["Key"]] = tag.get("Value", "")
    return {}


def _untag_delivery_stream(params: dict, region: str, account_id: str) -> dict:
    name = params.get("DeliveryStreamName", "")
    tag_keys = params.get("TagKeys", [])
    k = _key(name, region, account_id)
    with _lock:
        stream = _delivery_streams.get(k)
        if not stream:
            raise FirehoseError("ResourceNotFoundException", f"Stream {name} not found")
        for tag_key in tag_keys:
            stream["tags"].pop(tag_key, None)
    return {}


def _list_tags_for_delivery_stream(params: dict, region: str, account_id: str) -> dict:
    name = params.get("DeliveryStreamName", "")
    limit = params.get("Limit", 50)
    exclusive_start = params.get("ExclusiveStartTagKey")
    k = _key(name, region, account_id)
    with _lock:
        stream = _delivery_streams.get(k)
        if not stream:
            raise FirehoseError("ResourceNotFoundException", f"Stream {name} not found")
        all_tags = [{"Key": tag_k, "Value": v} for tag_k, v in sorted(stream["tags"].items())]

    # Apply ExclusiveStartTagKey filter
    if exclusive_start:
        start_idx = 0
        for i, tag in enumerate(all_tags):
            if tag["Key"] == exclusive_start:
                start_idx = i + 1
                break
        all_tags = all_tags[start_idx:]

    has_more = len(all_tags) > limit
    tags = all_tags[:limit]
    return {"Tags": tags, "HasMoreTags": has_more}


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
    "UpdateDestination": _update_destination,
    "StartDeliveryStreamEncryption": _start_delivery_stream_encryption,
    "StopDeliveryStreamEncryption": _stop_delivery_stream_encryption,
    "TagDeliveryStream": _tag_delivery_stream,
    "UntagDeliveryStream": _untag_delivery_stream,
    "ListTagsForDeliveryStream": _list_tags_for_delivery_stream,
}
