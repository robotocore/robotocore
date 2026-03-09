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
        from robotocore.providers.moto_bridge import forward_to_moto

        return await forward_to_moto(request, "kinesis")

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
    # Look up stream first to get ARN for resource policy cleanup
    stream = store.get_stream(name)
    if not stream:
        raise KinesisError(
            "ResourceNotFoundException", f"Stream {name} under account {account_id} not found."
        )
    # Clean up resource policies for this stream and its consumers
    stream_arn = stream.arn
    keys_to_remove = [k for k in store.resource_policies if k.startswith(stream_arn)]
    for k in keys_to_remove:
        del store.resource_policies[k]
    store.delete_stream(name)
    return {}


def _describe_stream(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StreamName", "")
    limit = params.get("Limit")
    exclusive_start = params.get("ExclusiveStartShardId")
    stream = store.get_stream(name)
    if not stream:
        raise KinesisError(
            "ResourceNotFoundException", f"Stream {name} under account {account_id} not found."
        )

    all_shards = []
    for shard in stream.shards:
        all_shards.append(
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

    # Apply ExclusiveStartShardId
    if exclusive_start:
        start_idx = 0
        for i, s in enumerate(all_shards):
            if s["ShardId"] == exclusive_start:
                start_idx = i + 1
                break
        all_shards = all_shards[start_idx:]

    # Apply Limit
    has_more = False
    if limit and len(all_shards) > limit:
        all_shards = all_shards[:limit]
        has_more = True

    return {
        "StreamDescription": {
            "StreamName": stream.name,
            "StreamARN": stream.arn,
            "StreamStatus": stream.status,
            "StreamModeDetails": {"StreamMode": "PROVISIONED"},
            "Shards": all_shards,
            "HasMoreShards": has_more,
            "RetentionPeriodHours": stream.retention_hours,
            "EnhancedMonitoring": [{"ShardLevelMetrics": []}],
            "EncryptionType": stream.encryption_type,
            "KeyId": stream.key_id if stream.key_id else None,
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
    has_more = len(names) > limit
    names = names[:limit]
    return {
        "StreamNames": names,
        "HasMoreStreams": has_more,
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
        "EncryptionType": "NONE",
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
        "EncryptionType": "NONE",
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
        try:
            seq = f"{int(starting_seq) + 1:020d}"
        except (ValueError, TypeError):
            raise KinesisError(
                "InvalidArgumentException",
                f"Invalid StartingSequenceNumber: {starting_seq}",
            )
    elif iterator_type == "AT_TIMESTAMP":
        # AT_TIMESTAMP starts from the beginning and filters by timestamp
        seq = "00000000000000000000"
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
    next_token = params.get("NextToken")
    max_results = params.get("MaxResults", 10000)

    # Support NextToken-based pagination (token encodes stream name + start index)
    start_idx = 0
    if next_token:
        parts = next_token.split(":", 1)
        if len(parts) == 2:
            name = parts[0]
            start_idx = int(parts[1])

    stream = store.get_stream(name)
    if not stream:
        raise KinesisError(
            "ResourceNotFoundException", f"Stream {name} under account {account_id} not found."
        )

    all_shards = []
    for shard in stream.shards:
        all_shards.append(
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

    # Apply pagination
    paged = all_shards[start_idx : start_idx + max_results]
    result: dict = {"Shards": paged}
    if start_idx + max_results < len(all_shards):
        result["NextToken"] = f"{name}:{start_idx + max_results}"
    return result


def _increase_retention(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StreamName", "")
    stream = store.get_stream(name)
    if not stream:
        raise KinesisError(
            "ResourceNotFoundException", f"Stream {name} under account {account_id} not found."
        )

    hours = params.get("RetentionPeriodHours", 48)
    if hours < stream.retention_hours:
        raise KinesisError(
            "InvalidArgumentException",
            f"Retention period {hours} must be >= current {stream.retention_hours}.",
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
            "ConsumerCount": len(stream.consumers),
        }
    }


def _update_shard_count(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
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
        raise KinesisError("InvalidArgumentException", "TargetShardCount must be at least 1.")

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

    # Sort tags by key for deterministic pagination (AWS returns sorted by key)
    all_tags = sorted(stream.tags.items(), key=lambda x: x[0])

    # Apply ExclusiveStartTagKey — skip tags up to and including this key
    exclusive_start = params.get("ExclusiveStartTagKey")
    if exclusive_start:
        all_tags = [(k, v) for k, v in all_tags if k > exclusive_start]

    # Apply Limit (AWS default is 10)
    limit = params.get("Limit", 10)
    has_more = len(all_tags) > limit
    all_tags = all_tags[:limit]

    tags = [{"Key": k, "Value": v} for k, v in all_tags]
    return {"Tags": tags, "HasMoreTags": has_more}


def _start_stream_encryption(
    store: KinesisStore, params: dict, region: str, account_id: str
) -> dict:
    name = params.get("StreamName", "")
    key_id = params.get("KeyId", "")
    encryption_type = params.get("EncryptionType", "KMS")
    stream = store.get_stream(name)
    if not stream:
        raise KinesisError("ResourceNotFoundException", f"Stream {name} not found")
    stream.encryption_type = encryption_type
    stream.key_id = key_id
    return {}


def _stop_stream_encryption(
    store: KinesisStore, params: dict, region: str, account_id: str
) -> dict:
    name = params.get("StreamName", "")
    stream = store.get_stream(name)
    if not stream:
        raise KinesisError("ResourceNotFoundException", f"Stream {name} not found")
    stream.encryption_type = "NONE"
    stream.key_id = ""
    return {}


def _register_stream_consumer(
    store: KinesisStore, params: dict, region: str, account_id: str
) -> dict:
    stream_arn = params.get("StreamARN", "")
    consumer_name = params.get("ConsumerName", "")
    # Find stream by ARN
    stream = None
    for s in store.streams.values():
        if s.arn == stream_arn:
            stream = s
            break
    if not stream:
        raise KinesisError("ResourceNotFoundException", f"Stream not found: {stream_arn}")

    # Check for duplicate consumer name
    if consumer_name in stream.consumers:
        raise KinesisError(
            "ResourceInUseException",
            f"Consumer {consumer_name} already exists on stream {stream_arn}",
        )

    import time as _time

    consumer_arn = f"{stream_arn}/consumer/{consumer_name}:{int(_time.time())}"
    consumer = {
        "ConsumerName": consumer_name,
        "ConsumerARN": consumer_arn,
        "ConsumerStatus": "ACTIVE",
        "ConsumerCreationTimestamp": _time.time(),
    }
    stream.consumers[consumer_name] = consumer
    return {"Consumer": consumer}


def _describe_stream_consumer(
    store: KinesisStore, params: dict, region: str, account_id: str
) -> dict:
    stream_arn = params.get("StreamARN", "")
    consumer_name = params.get("ConsumerName", "")
    consumer_arn = params.get("ConsumerARN", "")

    # Lookup by ARN (can find without StreamARN/ConsumerName)
    if consumer_arn:
        for s in store.streams.values():
            for c in s.consumers.values():
                if c["ConsumerARN"] == consumer_arn:
                    return {"ConsumerDescription": c}
        raise KinesisError("ResourceNotFoundException", f"Consumer {consumer_arn} not found")

    # Lookup by StreamARN + ConsumerName
    for s in store.streams.values():
        if s.arn == stream_arn and consumer_name in s.consumers:
            return {"ConsumerDescription": s.consumers[consumer_name]}
    raise KinesisError("ResourceNotFoundException", f"Consumer {consumer_name} not found")


def _list_stream_consumers(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    stream_arn = params.get("StreamARN", "")
    for s in store.streams.values():
        if s.arn == stream_arn:
            return {"Consumers": list(s.consumers.values())}
    raise KinesisError("ResourceNotFoundException", f"Stream not found: {stream_arn}")


def _deregister_stream_consumer(
    store: KinesisStore, params: dict, region: str, account_id: str
) -> dict:
    stream_arn = params.get("StreamARN", "")
    consumer_name = params.get("ConsumerName", "")
    for s in store.streams.values():
        if s.arn == stream_arn:
            if consumer_name not in s.consumers:
                raise KinesisError(
                    "ResourceNotFoundException",
                    f"Consumer {consumer_name} not found on stream {stream_arn}",
                )
            del s.consumers[consumer_name]
            return {}
    raise KinesisError("ResourceNotFoundException", f"Stream not found: {stream_arn}")


def _enable_enhanced_monitoring(
    store: KinesisStore, params: dict, region: str, account_id: str
) -> dict:
    name = params.get("StreamName", "")
    stream_arn = params.get("StreamARN", "")
    stream = None
    if name:
        stream = store.get_stream(name)
    elif stream_arn:
        for s in store.streams.values():
            if s.arn == stream_arn:
                stream = s
                break
    if not stream:
        lookup = name or stream_arn
        raise KinesisError(
            "ResourceNotFoundException",
            f"Stream {lookup} under account {account_id} not found.",
        )
    shard_level_metrics = params.get("ShardLevelMetrics", [])
    current = list(stream.shard_level_metrics)
    if "ALL" in shard_level_metrics:
        desired = [
            "IncomingBytes",
            "IncomingRecords",
            "OutgoingBytes",
            "OutgoingRecords",
            "WriteProvisionedThroughputExceeded",
            "ReadProvisionedThroughputExceeded",
            "IteratorAgeMilliseconds",
            "ALL",
        ]
    else:
        desired = list(set(current + shard_level_metrics))
    stream.shard_level_metrics = desired
    return {
        "StreamName": stream.name,
        "StreamARN": stream.arn,
        "CurrentShardLevelMetrics": current,
        "DesiredShardLevelMetrics": desired,
    }


def _disable_enhanced_monitoring(
    store: KinesisStore, params: dict, region: str, account_id: str
) -> dict:
    name = params.get("StreamName", "")
    stream_arn = params.get("StreamARN", "")
    stream = None
    if name:
        stream = store.get_stream(name)
    elif stream_arn:
        for s in store.streams.values():
            if s.arn == stream_arn:
                stream = s
                break
    if not stream:
        lookup = name or stream_arn
        raise KinesisError(
            "ResourceNotFoundException",
            f"Stream {lookup} under account {account_id} not found.",
        )
    to_disable = params.get("ShardLevelMetrics", [])
    current = list(stream.shard_level_metrics)
    if "ALL" in to_disable:
        desired: list[str] = []
    else:
        desired = [m for m in current if m not in to_disable]
    stream.shard_level_metrics = desired
    return {
        "StreamName": stream.name,
        "StreamARN": stream.arn,
        "CurrentShardLevelMetrics": current,
        "DesiredShardLevelMetrics": desired,
    }


def _split_shard(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StreamName", "")
    stream = store.get_stream(name)
    if not stream:
        raise KinesisError("ResourceNotFoundException", f"Stream {name} not found")
    shard_id = params.get("ShardToSplit", "")
    new_hash = params.get("NewStartingHashKey", "")
    # Find the shard
    target = None
    for shard in stream.shards:
        if shard.shard_id == shard_id:
            target = shard
            break
    if not target:
        raise KinesisError("ResourceNotFoundException", f"Shard {shard_id} not found")
    # Create two new shards from the split
    mid = int(new_hash) if new_hash else (target.hash_key_start + target.hash_key_end) // 2
    from robotocore.services.kinesis.models import Shard

    new_id1 = f"shardId-{len(stream.shards):012d}"
    new_id2 = f"shardId-{len(stream.shards) + 1:012d}"
    shard1 = Shard(shard_id=new_id1, hash_key_start=target.hash_key_start, hash_key_end=mid - 1)
    shard2 = Shard(shard_id=new_id2, hash_key_start=mid, hash_key_end=target.hash_key_end)
    stream.shards.remove(target)
    stream.shards.extend([shard1, shard2])
    stream.shard_count = len(stream.shards)
    return {}


def _merge_shards(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StreamName", "")
    stream = store.get_stream(name)
    if not stream:
        raise KinesisError("ResourceNotFoundException", f"Stream {name} not found")
    shard1_id = params.get("ShardToMerge", "")
    shard2_id = params.get("AdjacentShardToMerge", "")
    s1 = s2 = None
    for shard in stream.shards:
        if shard.shard_id == shard1_id:
            s1 = shard
        if shard.shard_id == shard2_id:
            s2 = shard
    if not s1 or not s2:
        raise KinesisError("ResourceNotFoundException", "Shard not found")
    from robotocore.services.kinesis.models import Shard

    new_id = f"shardId-{len(stream.shards):012d}"
    merged = Shard(
        shard_id=new_id,
        hash_key_start=min(s1.hash_key_start, s2.hash_key_start),
        hash_key_end=max(s1.hash_key_end, s2.hash_key_end),
    )
    stream.shards.remove(s1)
    stream.shards.remove(s2)
    stream.shards.append(merged)
    stream.shard_count = len(stream.shards)
    return {}


def _extract_stream_name_from_arn(resource_arn: str) -> str:
    """Extract stream name from a stream or consumer ARN.

    Stream ARN:   arn:aws:kinesis:us-east-1:123:stream/mystream
    Consumer ARN: arn:aws:kinesis:us-east-1:123:stream/mystream/consumer/myconsumer:12345
    """
    # Find "stream/" and extract the next path segment
    parts = resource_arn.split("/")
    for i, part in enumerate(parts):
        if part.endswith(":stream") or part == "stream":
            if i + 1 < len(parts):
                return parts[i + 1]
    # Fallback: last segment (original behavior)
    return parts[-1]


def _put_resource_policy(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    resource_arn = params.get("ResourceARN", "")
    policy = params.get("Policy", "")
    # Validate stream exists (works for both stream and consumer ARNs)
    stream_name = _extract_stream_name_from_arn(resource_arn)
    stream = store.get_stream(stream_name)
    if not stream:
        raise KinesisError(
            "ResourceNotFoundException",
            f"Stream {resource_arn} under account {account_id} not found.",
        )
    store.resource_policies[resource_arn] = policy
    return {}


def _get_resource_policy(store: KinesisStore, params: dict, region: str, account_id: str) -> dict:
    resource_arn = params.get("ResourceARN", "")
    stream_name = _extract_stream_name_from_arn(resource_arn)
    stream = store.get_stream(stream_name)
    if not stream:
        raise KinesisError(
            "ResourceNotFoundException",
            f"Stream {resource_arn} under account {account_id} not found.",
        )
    policy = store.resource_policies.get(resource_arn, "{}")
    return {"Policy": policy}


def _delete_resource_policy(
    store: KinesisStore, params: dict, region: str, account_id: str
) -> dict:
    resource_arn = params.get("ResourceARN", "")
    stream_name = _extract_stream_name_from_arn(resource_arn)
    stream = store.get_stream(stream_name)
    if not stream:
        raise KinesisError(
            "ResourceNotFoundException",
            f"Stream {resource_arn} under account {account_id} not found.",
        )
    if resource_arn not in store.resource_policies:
        raise KinesisError(
            "ResourceNotFoundException",
            f"No resource policy found for resource ARN {resource_arn}.",
        )
    del store.resource_policies[resource_arn]
    return {}


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
    "StartStreamEncryption": _start_stream_encryption,
    "StopStreamEncryption": _stop_stream_encryption,
    "RegisterStreamConsumer": _register_stream_consumer,
    "DescribeStreamConsumer": _describe_stream_consumer,
    "ListStreamConsumers": _list_stream_consumers,
    "DeregisterStreamConsumer": _deregister_stream_consumer,
    "EnableEnhancedMonitoring": _enable_enhanced_monitoring,
    "DisableEnhancedMonitoring": _disable_enhanced_monitoring,
    "SplitShard": _split_shard,
    "MergeShards": _merge_shards,
    "PutResourcePolicy": _put_resource_policy,
    "GetResourcePolicy": _get_resource_policy,
    "DeleteResourcePolicy": _delete_resource_policy,
}
