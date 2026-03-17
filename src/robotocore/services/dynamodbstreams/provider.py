"""Native DynamoDB Streams provider.

Uses JSON protocol (X-Amz-Target: DynamoDBStreams_20120810.{Action}).
Delegates to Moto's DynamoDB backend for stream data since DynamoDB
tables (with their StreamShard objects) are managed by Moto.
"""

import base64
import copy
import json
import logging
import threading

from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)

# Shard iterator storage: maps iterator ID -> iterator state
_shard_iterators: dict[str, dict] = {}
_iterator_lock = threading.Lock()


def _get_dynamodb_backend(account_id: str, region: str):
    """Get the Moto DynamoDB backend for the given account/region."""
    from moto.backends import get_backend
    from moto.core import DEFAULT_ACCOUNT_ID

    acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID
    return get_backend("dynamodb")[acct][region]


def _get_table_from_stream_arn(arn: str, account_id: str, region: str):
    """Extract table name from stream ARN and return the Moto table object."""
    # Stream ARN format: arn:aws:dynamodb:region:account:table/name/stream/label
    table_name = arn.split(":", 6)[5].split("/")[1]
    backend = _get_dynamodb_backend(account_id, region)
    return backend.get_table(table_name)


async def handle_dynamodbstreams_request(
    request: Request, region: str, account_id: str
) -> Response:
    """Handle a DynamoDB Streams API request."""
    body = await request.body()
    target = request.headers.get("x-amz-target", "")

    # Extract operation from X-Amz-Target: "DynamoDBStreams_20120810.DescribeStream"
    operation = target.split(".")[-1] if "." in target else target

    params = json.loads(body) if body else {}

    handler = _ACTION_MAP.get(operation)
    if handler is None:
        from robotocore.providers.moto_bridge import forward_to_moto

        return await forward_to_moto(request, "dynamodbstreams", account_id=account_id)

    try:
        result = handler(params, region, account_id)
        return _json(200, result)
    except StreamsError as e:
        return _error(e.code, e.message, e.status)
    except Exception as e:
        logger.exception(f"DynamoDB Streams error in {operation}")
        return _error("InternalServerError", str(e), 500)


class StreamsError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


# --- Operations ---


def _list_streams(params: dict, region: str, account_id: str) -> dict:
    """List all DynamoDB streams, optionally filtered by table name."""
    table_name = params.get("TableName")
    backend = _get_dynamodb_backend(account_id, region)

    streams = []
    for table in backend.tables.values():
        if table_name is not None and table.name != table_name:
            continue
        if table.latest_stream_label:
            desc = table.describe(base_key="Table")
            streams.append(
                {
                    "StreamArn": desc["Table"]["LatestStreamArn"],
                    "TableName": desc["Table"]["TableName"],
                    "StreamLabel": desc["Table"]["LatestStreamLabel"],
                }
            )

    return {"Streams": streams}


def _describe_stream(params: dict, region: str, account_id: str) -> dict:
    """Describe a specific DynamoDB stream."""
    arn = params.get("StreamArn", "")
    if not arn:
        raise StreamsError("ValidationException", "StreamArn is required")

    try:
        table = _get_table_from_stream_arn(arn, account_id, region)
    except Exception:  # noqa: BLE001
        raise StreamsError(
            "ResourceNotFoundException",
            f"Requested resource not found: Stream: {arn} not found",
        )

    if not table.latest_stream_label:
        raise StreamsError(
            "ResourceNotFoundException",
            f"Requested resource not found: Stream: {arn} not found",
        )

    shards = []
    if table.stream_shard:
        shards.append(table.stream_shard.to_json())

    stream = {
        "StreamArn": arn,
        "StreamLabel": table.latest_stream_label,
        "StreamStatus": "ENABLED" if table.latest_stream_label else "DISABLED",
        "StreamViewType": table.stream_specification["StreamViewType"],
        "CreationRequestDateTime": table.stream_shard.created_on.isoformat()
        if table.stream_shard
        else None,
        "TableName": table.name,
        "KeySchema": table.schema,
        "Shards": shards,
    }

    return {"StreamDescription": stream}


def _get_shard_iterator(params: dict, region: str, account_id: str) -> dict:
    """Get a shard iterator for reading stream records."""
    arn = params.get("StreamArn", "")
    shard_id = params.get("ShardId", "")
    iterator_type = params.get("ShardIteratorType", "TRIM_HORIZON")
    sequence_number = params.get("SequenceNumber")

    if not arn:
        raise StreamsError("ValidationException", "StreamArn is required")
    if not shard_id:
        raise StreamsError("ValidationException", "ShardId is required")

    try:
        table = _get_table_from_stream_arn(arn, account_id, region)
    except Exception:  # noqa: BLE001
        raise StreamsError(
            "ResourceNotFoundException",
            f"Requested resource not found: Stream: {arn} not found",
        )

    if not table.stream_shard or table.stream_shard.id != shard_id:
        raise StreamsError(
            "ResourceNotFoundException",
            f"Requested resource not found: Shard: {shard_id} in Stream: {arn}",
        )

    # Calculate starting sequence number based on iterator type
    shard = table.stream_shard
    if iterator_type == "TRIM_HORIZON":
        seq = shard.starting_sequence_number
    elif iterator_type == "LATEST":
        seq = shard.starting_sequence_number + len(shard.items)
    elif iterator_type == "AT_SEQUENCE_NUMBER":
        seq = int(sequence_number) if sequence_number else shard.starting_sequence_number
    elif iterator_type == "AFTER_SEQUENCE_NUMBER":
        seq = (int(sequence_number) + 1) if sequence_number else shard.starting_sequence_number
    else:
        raise StreamsError(
            "ValidationException",
            f"Invalid ShardIteratorType: {iterator_type}",
        )

    # Create iterator state and encode as base64 token
    iterator_state = {
        "stream_arn": arn,
        "shard_id": shard_id,
        "sequence_number": seq,
        "region": region,
        "account_id": account_id,
    }
    iterator_id = base64.b64encode(json.dumps(iterator_state).encode()).decode()

    with _iterator_lock:
        _shard_iterators[iterator_id] = iterator_state

    return {"ShardIterator": iterator_id}


def _get_records(params: dict, region: str, account_id: str) -> dict:
    """Get records from a shard using a shard iterator."""
    iterator_id = params.get("ShardIterator", "")
    limit = params.get("Limit", 1000)

    if not iterator_id:
        raise StreamsError("ValidationException", "ShardIterator is required")

    # Decode the iterator to get the state
    with _iterator_lock:
        state = _shard_iterators.get(iterator_id)

    if state is None:
        # Try decoding from base64
        try:
            state = json.loads(base64.b64decode(iterator_id).decode())
        except Exception:  # noqa: BLE001
            raise StreamsError(
                "ExpiredIteratorException",
                "The shard iterator has expired",
            )

    stream_arn = state["stream_arn"]
    shard_id = state["shard_id"]
    seq = state["sequence_number"]
    iter_region = state.get("region", region)
    iter_account_id = state.get("account_id", account_id)

    try:
        table = _get_table_from_stream_arn(stream_arn, iter_account_id, iter_region)
    except Exception:  # noqa: BLE001
        raise StreamsError(
            "ResourceNotFoundException",
            "Requested resource not found",
        )

    records = []
    if table.stream_shard and table.stream_shard.id == shard_id:
        raw_records = table.stream_shard.get(seq, limit)
        # Deep copy to avoid mutating Moto's internal state, and clean up
        # empty OldImage/NewImage to match AWS behavior (INSERT has no OldImage,
        # REMOVE has no NewImage).
        for r in raw_records:
            rec = copy.deepcopy(r)
            ddb = rec.get("dynamodb", {})
            if "OldImage" in ddb and not ddb["OldImage"]:
                del ddb["OldImage"]
            if "NewImage" in ddb and not ddb["NewImage"]:
                del ddb["NewImage"]
            records.append(rec)

    # Calculate next sequence number
    if records:
        last_seq = max(int(r["dynamodb"]["SequenceNumber"]) for r in records)
        next_seq = last_seq + 1
    else:
        next_seq = seq

    # Create next iterator
    next_state = {
        "stream_arn": stream_arn,
        "shard_id": shard_id,
        "sequence_number": next_seq,
        "region": iter_region,
        "account_id": iter_account_id,
    }
    next_iterator_id = base64.b64encode(json.dumps(next_state).encode()).decode()

    with _iterator_lock:
        _shard_iterators[next_iterator_id] = next_state
        # Clean up old iterator
        _shard_iterators.pop(iterator_id, None)

    return {
        "Records": records,
        "NextShardIterator": next_iterator_id,
    }


# --- Response helpers ---


def _json(status_code: int, data: dict) -> Response:
    return Response(
        content=json.dumps(data, default=str),
        status_code=status_code,
        media_type="application/x-amz-json-1.0",
    )


def _error(code: str, message: str, status: int) -> Response:
    body = json.dumps({"__type": code, "Message": message})
    return Response(content=body, status_code=status, media_type="application/x-amz-json-1.0")


_ACTION_MAP = {
    "DescribeStream": _describe_stream,
    "GetRecords": _get_records,
    "GetShardIterator": _get_shard_iterator,
    "ListStreams": _list_streams,
}
