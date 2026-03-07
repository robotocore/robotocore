"""Native Kinesis Streams provider with shard-based message storage.

Uses JSON protocol (X-Amz-Target: Kinesis_20131202.{Action}).
"""

import base64
import json
import threading
import time
from collections.abc import Callable

from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.kinesis.models import KinesisStore, _get_store


class KinesisError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


# Shard iterator storage: maps iterator token -> iterator state
_iterators: dict[str, dict] = {}
_iterator_lock = threading.Lock()


def _encode_iterator(
    stream_name: str, shard_id: str, iterator_type: str, sequence: str, region: str
) -> str:
    """Encode shard iterator as a base64 JSON blob."""
    payload = json.dumps(
        {
            "stream": stream_name,
            "shard": shard_id,
            "type": iterator_type,
            "seq": sequence,
            "region": region,
            "ts": time.time(),
        }
    )
    return base64.b64encode(payload.encode()).decode()


def _decode_iterator(token: str) -> dict:
    """Decode a shard iterator token."""
    try:
        payload = base64.b64decode(token)
        return json.loads(payload)
    except Exception:
        raise KinesisError("InvalidArgumentException", "Invalid ShardIterator.")


async def handle_kinesis_request(request: Request, region: str, account_id: str) -> Response:
    """Handle a Kinesis API request."""
    body = await request.body()
    target = request.headers.get("x-amz-target", "")

    if not target:
        return _error("InvalidAction", "Missing X-Amz-Target", 400)

    action = target.split(".")[-1]
    params = json.loads(body) if body else {}

    handler = _ACTION_MAP.get(action)
    if handler is None:
        return _error("InvalidAction", f"Unknown action: {action}", 400)

    store = _get_store(region)

    try:
        result = handler(store, params, region, account_id)
        return Response(
            content=json.dumps(result),
            status_code=200,
            media_type="application/x-amz-json-1.1",
        )
    except KinesisError as e:
        return _error(e.code, e.message, e.status)
    except Exception as e:
        return _error("InternalError", str(e), 500)


# --- Actions ---


def _create_stream(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StreamName", "")
    if not name:
        raise KinesisError("ValidationException", "StreamName is required")
    shard_count = params.get("ShardCount", 1)

    try:
        store.create_stream(name, shard_count, region, account_id)
    except ValueError:
        raise KinesisError(
            "ResourceInUseException", f"Stream {name} under account {account_id} already exists."
        )

    return {}


def _delete_stream(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StreamName", "")
    if not store.delete_stream(name):
        raise KinesisError(
            "ResourceNotFoundException", f"Stream {name} under account {account_id} not found."
        )
    return {}


def _describe_stream(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StreamName", "")
    stream = store.get_stream(name)
    if not stream:
        raise KinesisError(
            "ResourceNotFoundException", f"Stream {name} under account {account_id} not found."
        )

    shards = []
    for shard in stream.shards:
        shards.append(
            {
                "ShardId": shard.shard_id,
                "HashKeyRange": {
                    "StartingHashKey": str(shard.hash_key_start),
                    "EndingHashKey": str(shard.hash_key_end),
                },
                "SequenceNumberRange": {
                    "StartingSequenceNumber": shard.starting_sequence_number,
                },
            }
        )

    return {
        "StreamDescription": {
            "StreamName": stream.name,
            "StreamARN": stream.arn,
            "StreamStatus": stream.status,
            "StreamModeDetails": {"StreamMode": "PROVISIONED"},
            "Shards": shards,
            "HasMoreShards": False,
            "RetentionPeriodHours": stream.retention_hours,
            "EnhancedMonitoring": [{"ShardLevelMetrics": []}],
            "StreamCreationTimestamp": stream.created,
        }
    }


def _list_streams(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    names = store.list_streams()
    limit = params.get("Limit", 100)
    start = params.get("ExclusiveStartStreamName")
    if start:
        try:
            idx = names.index(start) + 1
            names = names[idx:]
        except ValueError:
            pass
    names = names[:limit]
    return {
        "StreamNames": names,
        "HasMoreStreams": False,
    }


def _put_record(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StreamName", "")
    stream = store.get_stream(name)
    if not stream:
        raise KinesisError(
            "ResourceNotFoundException", f"Stream {name} under account {account_id} not found."
        )

    partition_key = params.get("PartitionKey", "")
    data_b64 = params.get("Data", "")
    explicit_hash_key = params.get("ExplicitHashKey")

    data = base64.b64decode(data_b64) if data_b64 else b""
    record = stream.put_record(partition_key, data, explicit_hash_key)

    return {
        "ShardId": record.shard_id,
        "SequenceNumber": record.sequence_number,
    }


def _put_records(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StreamName", "")
    stream = store.get_stream(name)
    if not stream:
        raise KinesisError(
            "ResourceNotFoundException", f"Stream {name} under account {account_id} not found."
        )

    records_input = params.get("Records", [])
    result_records = []
    failed_count = 0

    for rec in records_input:
        partition_key = rec.get("PartitionKey", "")
        data_b64 = rec.get("Data", "")
        explicit_hash_key = rec.get("ExplicitHashKey")

        data = base64.b64decode(data_b64) if data_b64 else b""
        record = stream.put_record(partition_key, data, explicit_hash_key)
        result_records.append(
            {
                "ShardId": record.shard_id,
                "SequenceNumber": record.sequence_number,
            }
        )

    return {
        "FailedRecordCount": failed_count,
        "Records": result_records,
    }


def _get_shard_iterator(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StreamName", "")
    stream = store.get_stream(name)
    if not stream:
        raise KinesisError(
            "ResourceNotFoundException", f"Stream {name} under account {account_id} not found."
        )

    shard_id = params.get("ShardId", "")
    iterator_type = params.get("ShardIteratorType", "TRIM_HORIZON")
    starting_seq = params.get("StartingSequenceNumber", "00000000000000000000")

    # Validate shard exists
    shard = None
    for s in stream.shards:
        if s.shard_id == shard_id:
            shard = s
            break
    if shard is None:
        raise KinesisError(
            "ResourceNotFoundException", f"Shard {shard_id} in stream {name} not found."
        )

    # Determine the starting sequence based on iterator type
    if iterator_type == "TRIM_HORIZON":
        seq = "00000000000000000000"
    elif iterator_type == "LATEST":
        seq = shard.get_latest_sequence()
    elif iterator_type == "AT_SEQUENCE_NUMBER":
        seq = starting_seq
    elif iterator_type == "AFTER_SEQUENCE_NUMBER":
        seq = f"{int(starting_seq) + 1:020d}"
    else:
        raise KinesisError(
            "InvalidArgumentException", f"Invalid ShardIteratorType: {iterator_type}"
        )

    token = _encode_iterator(name, shard_id, iterator_type, seq, region)
    return {"ShardIterator": token}


def _get_records(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    token = params.get("ShardIterator", "")
    limit = min(params.get("Limit", 10000), 10000)

    iterator_info = _decode_iterator(token)
    stream_name = iterator_info["stream"]
    shard_id = iterator_info["shard"]
    seq = iterator_info["seq"]
    iter_region = iterator_info.get("region", region)

    iter_store = _get_store(iter_region)
    stream = iter_store.get_stream(stream_name)
    if not stream:
        raise KinesisError("ResourceNotFoundException", f"Stream {stream_name} not found.")

    shard = None
    for s in stream.shards:
        if s.shard_id == shard_id:
            shard = s
            break
    if shard is None:
        raise KinesisError("ResourceNotFoundException", f"Shard {shard_id} not found.")

    records, next_seq = shard.get_records(seq, limit)

    # Build the next iterator
    new_seq = next_seq if next_seq else seq
    next_token = _encode_iterator(stream_name, shard_id, "AT_SEQUENCE_NUMBER", new_seq, iter_region)

    output_records = []
    for rec in records:
        output_records.append(
            {
                "SequenceNumber": rec.sequence_number,
                "ApproximateArrivalTimestamp": rec.timestamp,
                "Data": base64.b64encode(rec.data).decode(),
                "PartitionKey": rec.partition_key,
            }
        )

    return {
        "Records": output_records,
        "NextShardIterator": next_token,
        "MillisBehindLatest": 0,
    }


def _list_shards(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StreamName", "")
    stream = store.get_stream(name)
    if not stream:
        raise KinesisError(
            "ResourceNotFoundException", f"Stream {name} under account {account_id} not found."
        )

    shards = []
    for shard in stream.shards:
        shards.append(
            {
                "ShardId": shard.shard_id,
                "HashKeyRange": {
                    "StartingHashKey": str(shard.hash_key_start),
                    "EndingHashKey": str(shard.hash_key_end),
                },
                "SequenceNumberRange": {
                    "StartingSequenceNumber": shard.starting_sequence_number,
                },
            }
        )

    return {"Shards": shards}


def _increase_retention(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StreamName", "")
    stream = store.get_stream(name)
    if not stream:
        raise KinesisError(
            "ResourceNotFoundException", f"Stream {name} under account {account_id} not found."
        )

    hours = params.get("RetentionPeriodHours", 48)
    if hours <= stream.retention_hours:
        raise KinesisError(
            "InvalidArgumentException",
            f"Retention period {hours} must be greater than current {stream.retention_hours}.",
        )
    stream.retention_hours = hours
    return {}


def _decrease_retention(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StreamName", "")
    stream = store.get_stream(name)
    if not stream:
        raise KinesisError(
            "ResourceNotFoundException", f"Stream {name} under account {account_id} not found."
        )

    hours = params.get("RetentionPeriodHours", 24)
    if hours >= stream.retention_hours:
        raise KinesisError(
            "InvalidArgumentException",
            f"Retention period {hours} must be less than current {stream.retention_hours}.",
        )
    if hours < 24:
        raise KinesisError("InvalidArgumentException", "Minimum retention period is 24 hours.")
    stream.retention_hours = hours
    return {}


def _add_tags(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StreamName", "")
    stream = store.get_stream(name)
    if not stream:
        raise KinesisError(
            "ResourceNotFoundException", f"Stream {name} under account {account_id} not found."
        )

    tags = params.get("Tags", {})
    stream.tags.update(tags)
    return {}


def _remove_tags(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StreamName", "")
    stream = store.get_stream(name)
    if not stream:
        raise KinesisError(
            "ResourceNotFoundException", f"Stream {name} under account {account_id} not found."
        )

    tag_keys = params.get("TagKeys", [])
    for key in tag_keys:
        stream.tags.pop(key, None)
    return {}


def _describe_stream_summary(
    store: KinesisStore, params: dict, region: str, account_id: str
) -> dict:
    name = params.get("StreamName", "")
    stream = store.get_stream(name)
    if not stream:
        raise KinesisError(
            "ResourceNotFoundException", f"Stream {name} under account {account_id} not found."
        )

    open_shard_count = len(stream.shards)

    return {
        "StreamDescriptionSummary": {
            "StreamName": stream.name,
            "StreamARN": stream.arn,
            "StreamStatus": stream.status,
            "StreamModeDetails": {"StreamMode": "PROVISIONED"},
            "RetentionPeriodHours": stream.retention_hours,
            "StreamCreationTimestamp": stream.created,
            "EnhancedMonitoring": [{"ShardLevelMetrics": []}],
            "OpenShardCount": open_shard_count,
            "ConsumerCount": 0,
        }
    }


def _update_shard_count(
    store: KinesisStore, params: dict, region: str, account_id: str
) -> dict:
    name = params.get("StreamName", "")
    stream = store.get_stream(name)
    if not stream:
        raise KinesisError(
            "ResourceNotFoundException", f"Stream {name} under account {account_id} not found."
        )

    target_shard_count = params.get("TargetShardCount")
    scaling_type = params.get("ScalingType", "UNIFORM_SCALING")

    if target_shard_count is None:
        raise KinesisError("ValidationException", "TargetShardCount is required")
    if scaling_type != "UNIFORM_SCALING":
        raise KinesisError(
            "ValidationException",
            f"ScalingType {scaling_type} is not supported. Only UNIFORM_SCALING is supported.",
        )
    if target_shard_count < 1:
        raise KinesisError(
            "InvalidArgumentException", "TargetShardCount must be at least 1."
        )

    current_shard_count = len(stream.shards)

    # Rebuild shards to match the new target count
    from robotocore.services.kinesis.models import MAX_HASH_KEY, Shard

    new_shards = []
    for i in range(target_shard_count):
        range_size = (MAX_HASH_KEY + 1) // target_shard_count
        start = i * range_size
        end = (i + 1) * range_size - 1 if i < target_shard_count - 1 else MAX_HASH_KEY
        shard = Shard(
            shard_id=f"shardId-{i:012d}",
            hash_key_start=start,
            hash_key_end=end,
            starting_sequence_number="00000000000000000000",
        )
        new_shards.append(shard)

    stream.shards = new_shards
    stream.shard_count = target_shard_count

    return {
        "StreamName": stream.name,
        "CurrentShardCount": current_shard_count,
        "TargetShardCount": target_shard_count,
        "StreamARN": stream.arn,
    }


def _list_tags(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StreamName", "")
    stream = store.get_stream(name)
    if not stream:
        raise KinesisError(
            "ResourceNotFoundException", f"Stream {name} under account {account_id} not found."
        )

    tags = [{"Key": k, "Value": v} for k, v in stream.tags.items()]
    return {"Tags": tags, "HasMoreTags": False}


def _error(code: str, message: str, status: int) -> Response:
    body = json.dumps({"__type": code, "message": message})
    return Response(content=body, status_code=status, media_type="application/x-amz-json-1.1")


_ACTION_MAP: dict[str, Callable] = {
    "CreateStream": _create_stream,
    "DeleteStream": _delete_stream,
    "DescribeStream": _describe_stream,
    "ListStreams": _list_streams,
    "PutRecord": _put_record,
    "PutRecords": _put_records,
    "GetShardIterator": _get_shard_iterator,
    "GetRecords": _get_records,
    "ListShards": _list_shards,
    "IncreaseStreamRetentionPeriod": _increase_retention,
    "DecreaseStreamRetentionPeriod": _decrease_retention,
    "AddTagsToStream": _add_tags,
    "RemoveTagsFromStream": _remove_tags,
    "ListTagsForStream": _list_tags,
    "DescribeStreamSummary": _describe_stream_summary,
    "UpdateShardCount": _update_shard_count,
}
