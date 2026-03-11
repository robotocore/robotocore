"""Native EventBridge Pipes provider.

Implements the Source -> Enrichment -> Target pipeline.
Supports SQS, Kinesis, and DynamoDB Streams sources; Lambda enrichment;
and SQS, SNS, Lambda, EventBridge, Step Functions, Kinesis targets.
"""

import json
import logging
import re
import threading
import time
from urllib.parse import unquote

from moto.backends import get_backend
from moto.core import DEFAULT_ACCOUNT_ID as MOTO_DEFAULT_ACCOUNT_ID
from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.lambda_.invoke import invoke_lambda_async, invoke_lambda_sync

logger = logging.getLogger(__name__)

DEFAULT_ACCOUNT_ID = "123456789012"

# In-memory stores: (account_id, region) -> name -> pipe
_pipes: dict[tuple[str, str], dict[str, dict]] = {}
_lock = threading.Lock()

# Active polling threads: pipe_key -> Thread
_polling_threads: dict[str, threading.Event] = {}
_polling_threads_lock = threading.Lock()


class PipesError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


def _get_pipes(region: str, account_id: str = DEFAULT_ACCOUNT_ID) -> dict[str, dict]:
    key = (account_id, region)
    with _lock:
        if key not in _pipes:
            _pipes[key] = {}
        return _pipes[key]


def _pipe_key(account_id: str, region: str, name: str) -> str:
    return f"{account_id}:{region}:{name}"


# REST-JSON path patterns (match the botocore service model)
_PIPE_PATH = re.compile(r"^/v1/pipes/([^/?]+)$")
_PIPES_LIST = re.compile(r"^/v1/pipes/?$")
_PIPE_START = re.compile(r"^/v1/pipes/([^/?]+)/start$")
_PIPE_STOP = re.compile(r"^/v1/pipes/([^/?]+)/stop$")
_TAGS_PATH = re.compile(r"^/tags/(.+)$")


async def handle_pipes_request(request: Request, region: str, account_id: str) -> Response:
    """Handle an EventBridge Pipes API request."""
    path = request.url.path
    method = request.method.upper()
    body = await request.body()
    params = json.loads(body) if body else {}

    try:
        # Start/Stop must be checked before the generic pipe path
        m = _PIPE_START.match(path)
        if m and method == "POST":
            name = unquote(m.group(1))
            return _json_response(_start_pipe(name, region, account_id))

        m = _PIPE_STOP.match(path)
        if m and method == "POST":
            name = unquote(m.group(1))
            return _json_response(_stop_pipe(name, region, account_id))

        # CRUD on individual pipes
        m = _PIPE_PATH.match(path)
        if m:
            name = unquote(m.group(1))
            if method == "POST":
                return _json_response(_create_pipe(name, params, region, account_id))
            elif method == "GET":
                return _json_response(_describe_pipe(name, region, account_id))
            elif method == "PUT":
                return _json_response(_update_pipe(name, params, region, account_id))
            elif method == "DELETE":
                return _json_response(_delete_pipe(name, region, account_id))

        # List pipes
        if _PIPES_LIST.match(path) and method == "GET":
            return _json_response(_list_pipes(request.query_params, region, account_id))

        # Tags
        m = _TAGS_PATH.match(path)
        if m:
            resource_arn = unquote(m.group(1))
            if method == "GET":
                return _json_response({"tags": _list_tags(resource_arn, region, account_id)})
            elif method == "POST":
                new_tags = params.get("tags", {})
                _tag_resource(resource_arn, new_tags, region, account_id)
                return _json_response({})
            elif method == "DELETE":
                tag_keys = request.query_params.getlist("tagKeys")
                _untag_resource(resource_arn, tag_keys, region, account_id)
                return _json_response({})

        return _error("InvalidAction", f"Unknown path: {method} {path}", 400)

    except PipesError as e:
        return _error(e.code, e.message, e.status)
    except Exception as e:
        logger.exception("Pipes provider error")
        return _error("InternalError", str(e), 500)


def _create_pipe(name: str, params: dict, region: str, account_id: str) -> dict:
    pipes = _get_pipes(region, account_id)
    with _lock:
        if name in pipes:
            raise PipesError("ConflictException", f"Pipe {name} already exists.", 409)

    arn = f"arn:aws:pipes:{region}:{account_id}:pipe/{name}"
    desired_state = params.get("DesiredState", "RUNNING")
    now = time.time()

    pipe = {
        "Name": name,
        "Arn": arn,
        "Source": params.get("Source", ""),
        "SourceParameters": params.get("SourceParameters", {}),
        "Enrichment": params.get("Enrichment", ""),
        "EnrichmentParameters": params.get("EnrichmentParameters", {}),
        "Target": params.get("Target", ""),
        "TargetParameters": params.get("TargetParameters", {}),
        "RoleArn": params.get("RoleArn", ""),
        "Description": params.get("Description", ""),
        "DesiredState": desired_state,
        "CurrentState": "CREATING",
        "CreationTime": now,
        "LastModifiedTime": now,
        "_tags": params.get("Tags", {}),
    }

    with _lock:
        pipes[name] = pipe

    # Transition to RUNNING or STOPPED
    if desired_state == "RUNNING":
        pipe["CurrentState"] = "RUNNING"
        _start_polling(pipe, region, account_id)
    else:
        pipe["CurrentState"] = "STOPPED"

    return {
        "Name": name,
        "Arn": arn,
        "DesiredState": desired_state,
        "CurrentState": pipe["CurrentState"],
        "CreationTime": now,
        "LastModifiedTime": now,
    }


def _describe_pipe(name: str, region: str, account_id: str) -> dict:
    pipes = _get_pipes(region, account_id)
    with _lock:
        pipe = pipes.get(name)
    if not pipe:
        raise PipesError("NotFoundException", f"Pipe {name} does not exist.", 404)
    result = dict(pipe)
    result.pop("_tags", None)
    return result


def _update_pipe(name: str, params: dict, region: str, account_id: str) -> dict:
    pipes = _get_pipes(region, account_id)
    with _lock:
        pipe = pipes.get(name)
        if not pipe:
            raise PipesError("NotFoundException", f"Pipe {name} does not exist.", 404)

        if "SourceParameters" in params:
            pipe["SourceParameters"] = params["SourceParameters"]
        if "Enrichment" in params:
            pipe["Enrichment"] = params["Enrichment"]
        if "EnrichmentParameters" in params:
            pipe["EnrichmentParameters"] = params["EnrichmentParameters"]
        if "Target" in params:
            pipe["Target"] = params["Target"]
        if "TargetParameters" in params:
            pipe["TargetParameters"] = params["TargetParameters"]
        if "RoleArn" in params:
            pipe["RoleArn"] = params["RoleArn"]
        if "Description" in params:
            pipe["Description"] = params["Description"]
        if "DesiredState" in params:
            pipe["DesiredState"] = params["DesiredState"]

        pipe["LastModifiedTime"] = time.time()

    # Handle state transitions
    desired = pipe.get("DesiredState", "RUNNING")
    current = pipe.get("CurrentState", "STOPPED")
    if desired == "RUNNING" and current != "RUNNING":
        pipe["CurrentState"] = "RUNNING"
        _start_polling(pipe, region, account_id)
    elif desired == "STOPPED" and current == "RUNNING":
        _stop_polling(account_id, region, name)
        pipe["CurrentState"] = "STOPPED"

    return {
        "Name": name,
        "Arn": pipe["Arn"],
        "DesiredState": pipe["DesiredState"],
        "CurrentState": pipe["CurrentState"],
        "CreationTime": pipe["CreationTime"],
        "LastModifiedTime": pipe["LastModifiedTime"],
    }


def _delete_pipe(name: str, region: str, account_id: str) -> dict:
    pipes = _get_pipes(region, account_id)
    with _lock:
        if name not in pipes:
            raise PipesError("NotFoundException", f"Pipe {name} does not exist.", 404)
        pipe = pipes.pop(name)

    _stop_polling(account_id, region, name)
    pipe["CurrentState"] = "DELETING"

    return {
        "Name": name,
        "Arn": pipe["Arn"],
        "DesiredState": "DELETED",
        "CurrentState": "DELETING",
        "CreationTime": pipe["CreationTime"],
        "LastModifiedTime": time.time(),
    }


def _list_pipes(query_params, region: str, account_id: str) -> dict:
    pipes = _get_pipes(region, account_id)
    name_prefix = query_params.get("NamePrefix")
    current_state = query_params.get("CurrentState")
    source_prefix = query_params.get("SourcePrefix")
    target_prefix = query_params.get("TargetPrefix")

    with _lock:
        items = list(pipes.values())

    if name_prefix:
        items = [p for p in items if p["Name"].startswith(name_prefix)]
    if current_state:
        items = [p for p in items if p.get("CurrentState") == current_state]
    if source_prefix:
        items = [p for p in items if p.get("Source", "").startswith(source_prefix)]
    if target_prefix:
        items = [p for p in items if p.get("Target", "").startswith(target_prefix)]

    return {
        "Pipes": [
            {
                "Name": p["Name"],
                "Arn": p["Arn"],
                "Source": p.get("Source", ""),
                "Target": p.get("Target", ""),
                "Enrichment": p.get("Enrichment", ""),
                "DesiredState": p.get("DesiredState", "RUNNING"),
                "CurrentState": p.get("CurrentState", "STOPPED"),
                "CreationTime": p.get("CreationTime"),
                "LastModifiedTime": p.get("LastModifiedTime"),
            }
            for p in items
        ]
    }


def _start_pipe(name: str, region: str, account_id: str) -> dict:
    pipes = _get_pipes(region, account_id)
    with _lock:
        pipe = pipes.get(name)
        if not pipe:
            raise PipesError("NotFoundException", f"Pipe {name} does not exist.", 404)
        pipe["DesiredState"] = "RUNNING"
        pipe["CurrentState"] = "RUNNING"
        pipe["LastModifiedTime"] = time.time()

    _start_polling(pipe, region, account_id)

    return {
        "Name": name,
        "Arn": pipe["Arn"],
        "DesiredState": "RUNNING",
        "CurrentState": "RUNNING",
        "CreationTime": pipe["CreationTime"],
        "LastModifiedTime": pipe["LastModifiedTime"],
    }


def _stop_pipe(name: str, region: str, account_id: str) -> dict:
    pipes = _get_pipes(region, account_id)
    with _lock:
        pipe = pipes.get(name)
        if not pipe:
            raise PipesError("NotFoundException", f"Pipe {name} does not exist.", 404)
        pipe["DesiredState"] = "STOPPED"
        pipe["CurrentState"] = "STOPPED"
        pipe["LastModifiedTime"] = time.time()

    _stop_polling(account_id, region, name)

    return {
        "Name": name,
        "Arn": pipe["Arn"],
        "DesiredState": "STOPPED",
        "CurrentState": "STOPPED",
        "CreationTime": pipe["CreationTime"],
        "LastModifiedTime": pipe["LastModifiedTime"],
    }


# ---------------------------------------------------------------------------
# Tag operations
# ---------------------------------------------------------------------------


def _find_pipe_by_arn(resource_arn: str, region: str, account_id: str) -> dict | None:
    pipes = _get_pipes(region, account_id)
    with _lock:
        for p in pipes.values():
            if p.get("Arn") == resource_arn:
                return p
    return None


def _list_tags(resource_arn: str, region: str, account_id: str) -> dict:
    pipe = _find_pipe_by_arn(resource_arn, region, account_id)
    if pipe is None:
        return {}
    return dict(pipe.get("_tags", {}))


def _tag_resource(resource_arn: str, new_tags: dict, region: str, account_id: str) -> None:
    pipe = _find_pipe_by_arn(resource_arn, region, account_id)
    if pipe is None:
        return
    with _lock:
        existing = pipe.setdefault("_tags", {})
        existing.update(new_tags)


def _untag_resource(resource_arn: str, tag_keys: list[str], region: str, account_id: str) -> None:
    pipe = _find_pipe_by_arn(resource_arn, region, account_id)
    if pipe is None:
        return
    with _lock:
        tags = pipe.get("_tags", {})
        for key in tag_keys:
            tags.pop(key, None)


# ---------------------------------------------------------------------------
# Pipeline execution: source polling, enrichment, target delivery
# ---------------------------------------------------------------------------


def _start_polling(pipe: dict, region: str, account_id: str) -> None:
    """Start a background polling thread for a pipe."""
    key = _pipe_key(account_id, region, pipe["Name"])

    with _polling_threads_lock:
        if key in _polling_threads:
            return  # Already running
        stop_event = threading.Event()
        _polling_threads[key] = stop_event

    thread = threading.Thread(
        target=_poll_loop,
        args=(pipe["Name"], region, account_id, stop_event),
        daemon=True,
        name=f"pipes-poll-{pipe['Name']}",
    )
    thread.start()


def _stop_polling(account_id: str, region: str, name: str) -> None:
    """Stop the polling thread for a pipe."""
    key = _pipe_key(account_id, region, name)
    with _polling_threads_lock:
        stop_event = _polling_threads.pop(key, None)
    if stop_event:
        stop_event.set()


def _poll_loop(name: str, region: str, account_id: str, stop_event: threading.Event) -> None:
    """Main polling loop for a pipe."""
    while not stop_event.is_set():
        try:
            pipes = _get_pipes(region, account_id)
            with _lock:
                pipe = pipes.get(name)
            if not pipe or pipe.get("CurrentState") != "RUNNING":
                break

            records = _poll_source(pipe, region, account_id)
            if records:
                enriched = _run_enrichment(pipe, records, region, account_id)
                _deliver_to_target(pipe, enriched, region, account_id)

        except Exception:
            logger.exception("Pipe %s polling error", name)

        # Poll interval: use MaximumBatchingWindowInSeconds from source params, default 1s
        interval = _get_poll_interval(pipe) if pipe else 1.0
        stop_event.wait(interval)


def _get_poll_interval(pipe: dict) -> float:
    """Extract poll interval from source parameters."""
    src_params = pipe.get("SourceParameters", {})
    # Check various source parameter types
    for key in ("SqsQueueParameters", "KinesisStreamParameters", "DynamoDBStreamParameters"):
        params = src_params.get(key, {})
        window = params.get("MaximumBatchingWindowInSeconds")
        if window is not None:
            return max(float(window), 0.1)
    return 1.0


def _poll_source(pipe: dict, region: str, account_id: str) -> list[dict]:
    """Poll records from the pipe's source."""
    source = pipe.get("Source", "")
    src_params = pipe.get("SourceParameters", {})

    if ":sqs:" in source:
        return _poll_sqs_source(source, src_params, region, account_id)
    elif ":kinesis:" in source:
        return _poll_kinesis_source(source, src_params, region, account_id)
    elif ":dynamodb:" in source and "/stream/" in source:
        return _poll_dynamodb_stream_source(source, src_params, region, account_id)

    logger.warning("Unsupported pipe source: %s", source)
    return []


def _poll_sqs_source(source_arn: str, src_params: dict, region: str, account_id: str) -> list[dict]:
    """Poll messages from an SQS queue."""
    try:
        acct = account_id if account_id != "123456789012" else MOTO_DEFAULT_ACCOUNT_ID

        # Extract queue name from ARN
        queue_name = source_arn.split(":")[-1]

        backend = get_backend("sqs")[acct][region]
        sqs_params = src_params.get("SqsQueueParameters", {})
        batch_size = sqs_params.get("BatchSize", 10)

        queue = backend.get_queue(queue_name)
        if not queue:
            return []

        messages = backend.receive_message(
            queue_name=queue_name,
            count=batch_size,
            wait_seconds_timeout=0,
            visibility_timeout=queue.visibility_timeout,
        )

        records = []
        for msg in messages:
            records.append(
                {
                    "messageId": msg.id,
                    "receiptHandle": msg.receipt_handle,
                    "body": msg.body,
                    "attributes": msg.system_attributes or {},
                    "messageAttributes": msg.message_attributes or {},
                    "md5OfBody": msg.body_md5,
                    "eventSource": "aws:sqs",
                    "eventSourceARN": source_arn,
                    "awsRegion": region,
                }
            )

            # Delete message after successful receive (auto-acknowledge)
            try:
                backend.delete_message(queue_name, msg.receipt_handle)
            except Exception:
                pass

        return records

    except Exception:
        logger.exception("SQS source poll error for %s", source_arn)
        return []


def _poll_kinesis_source(
    source_arn: str, src_params: dict, region: str, account_id: str
) -> list[dict]:
    """Poll records from a Kinesis stream."""
    try:
        acct = account_id if account_id != "123456789012" else MOTO_DEFAULT_ACCOUNT_ID

        stream_name = source_arn.split("/")[-1]
        backend = get_backend("kinesis")[acct][region]
        kinesis_params = src_params.get("KinesisStreamParameters", {})
        batch_size = kinesis_params.get("BatchSize", 100)
        starting_position = kinesis_params.get("StartingPosition", "LATEST")

        stream = backend.describe_stream(stream_name)
        if not stream:
            return []

        records = []
        for shard_id in stream.shards:
            iterator = backend.get_shard_iterator(stream_name, shard_id, starting_position)
            result = backend.get_records(iterator, batch_size)
            for record in result[0]:  # result is (records, millis_behind, next_iterator)
                records.append(
                    {
                        "kinesisSchemaVersion": "1.0",
                        "partitionKey": record.partition_key,
                        "sequenceNumber": record.sequence_number,
                        "data": record.data,
                        "approximateArrivalTimestamp": record.created_at,
                        "eventSource": "aws:kinesis",
                        "eventSourceARN": source_arn,
                        "awsRegion": region,
                    }
                )

        return records[:batch_size]

    except Exception:
        logger.exception("Kinesis source poll error for %s", source_arn)
        return []


def _poll_dynamodb_stream_source(
    source_arn: str, src_params: dict, region: str, account_id: str
) -> list[dict]:
    """Poll records from a DynamoDB stream."""
    try:
        acct = account_id if account_id != "123456789012" else MOTO_DEFAULT_ACCOUNT_ID

        ddb_params = src_params.get("DynamoDBStreamParameters", {})
        batch_size = ddb_params.get("BatchSize", 100)

        backend = get_backend("dynamodbstreams")[acct][region]

        # Get shard iterator and records
        stream_arn = source_arn
        description = backend.describe_stream(stream_arn)
        if not description:
            return []

        records = []
        shards = description.get("StreamDescription", {}).get("Shards", [])
        for shard in shards:
            shard_id = shard["ShardId"]
            try:
                iterator = backend.get_shard_iterator(stream_arn, shard_id, "LATEST")
                result = backend.get_records(iterator, batch_size)
                for record in result.get("Records", []):
                    record["eventSource"] = "aws:dynamodb"
                    record["eventSourceARN"] = source_arn
                    record["awsRegion"] = region
                    records.append(record)
            except Exception:
                continue

        return records[:batch_size]

    except Exception:
        logger.exception("DynamoDB stream source poll error for %s", source_arn)
        return []


def _run_enrichment(pipe: dict, records: list[dict], region: str, account_id: str) -> list[dict]:
    """Run optional enrichment on records."""
    enrichment = pipe.get("Enrichment", "")
    if not enrichment:
        return records

    if ":lambda:" in enrichment or ":function:" in enrichment:
        return _lambda_enrichment(enrichment, records, region, account_id)
    elif ":execute-api:" in enrichment:
        return _api_gateway_enrichment(enrichment, records, pipe, region, account_id)

    logger.warning("Unsupported enrichment type: %s", enrichment)
    return records


def _lambda_enrichment(
    function_arn: str, records: list[dict], region: str, account_id: str
) -> list[dict]:
    """Invoke a Lambda function for enrichment."""
    try:
        result, error_type, _logs = invoke_lambda_sync(
            function_arn=function_arn,
            payload=records,
            region=region,
            account_id=account_id,
        )
        if error_type:
            logger.warning("Lambda enrichment error: %s", error_type)
            return records

        if isinstance(result, list):
            return result
        elif isinstance(result, dict):
            return [result]
        else:
            return records

    except Exception:
        logger.exception("Lambda enrichment error for %s", function_arn)
        return records


def _api_gateway_enrichment(
    api_arn: str,
    records: list[dict],
    pipe: dict,
    region: str,
    account_id: str,
) -> list[dict]:
    """POST records to an API Gateway endpoint for enrichment."""
    try:
        import urllib.request

        enrichment_params = pipe.get("EnrichmentParameters", {})
        http_params = enrichment_params.get("HttpParameters", {})
        path = http_params.get("PathParameterValues", ["/"])[0] if http_params else "/"

        # Build local URL to the API Gateway endpoint
        url = f"http://localhost:4566{path}"
        data = json.dumps(records).encode()
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/json")

        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read())
            if isinstance(body, list):
                return body
            return [body]

    except Exception:
        logger.exception("API Gateway enrichment error for %s", api_arn)
        return records


def _deliver_to_target(pipe: dict, records: list[dict], region: str, account_id: str) -> None:
    """Deliver records to the pipe's target."""
    target = pipe.get("Target", "")
    target_params = pipe.get("TargetParameters", {})

    try:
        if ":sqs:" in target:
            _deliver_to_sqs(target, records, target_params, region, account_id)
        elif ":sns:" in target:
            _deliver_to_sns(target, records, target_params, region, account_id)
        elif ":lambda:" in target or ":function:" in target:
            _deliver_to_lambda(target, records, target_params, region, account_id)
        elif ":events:" in target or ":event-bus/" in target:
            _deliver_to_eventbridge(target, records, target_params, region, account_id)
        elif ":states:" in target or ":stateMachine:" in target:
            _deliver_to_stepfunctions(target, records, target_params, region, account_id)
        elif ":kinesis:" in target:
            _deliver_to_kinesis(target, records, target_params, region, account_id)
        else:
            logger.warning("Unsupported pipe target: %s", target)
    except Exception:
        logger.exception("Pipe target delivery error for %s", target)


def _deliver_to_sqs(
    target_arn: str,
    records: list[dict],
    target_params: dict,
    region: str,
    account_id: str,
) -> None:
    """Send messages to an SQS queue."""
    acct = account_id if account_id != "123456789012" else MOTO_DEFAULT_ACCOUNT_ID
    queue_name = target_arn.split(":")[-1]
    backend = get_backend("sqs")[acct][region]

    sqs_params = target_params.get("SqsQueueParameters", {})
    message_group_id = sqs_params.get("MessageGroupId")
    message_dedup_id = sqs_params.get("MessageDeduplicationId")

    for record in records:
        body = json.dumps(record) if isinstance(record, dict) else str(record)
        backend.send_message(
            queue_name=queue_name,
            message_body=body,
            message_attributes={},
            delay_seconds=0,
            group_id=message_group_id or "",
            deduplication_id=message_dedup_id or "",
        )


def _deliver_to_sns(
    target_arn: str,
    records: list[dict],
    target_params: dict,
    region: str,
    account_id: str,
) -> None:
    """Publish messages to an SNS topic."""
    acct = account_id if account_id != "123456789012" else MOTO_DEFAULT_ACCOUNT_ID
    backend = get_backend("sns")[acct][region]

    for record in records:
        message = json.dumps(record) if isinstance(record, dict) else str(record)
        backend.publish(message=message, arn=target_arn)


def _deliver_to_lambda(
    target_arn: str,
    records: list[dict],
    target_params: dict,
    region: str,
    account_id: str,
) -> None:
    """Invoke a Lambda function with the records."""
    invoke_lambda_async(
        function_arn=target_arn,
        payload={"Records": records},
        region=region,
        account_id=account_id,
    )


def _deliver_to_eventbridge(
    target_arn: str,
    records: list[dict],
    target_params: dict,
    region: str,
    account_id: str,
) -> None:
    """Put events on an EventBridge event bus."""
    acct = account_id if account_id != "123456789012" else MOTO_DEFAULT_ACCOUNT_ID

    eb_params = target_params.get("EventBridgeEventBusParameters", {})
    detail_type = eb_params.get("DetailType", "PipeForwarded")
    source_name = eb_params.get("Source", "aws.pipes")

    backend = get_backend("events")[acct][region]

    events = []
    for record in records:
        events.append(
            {
                "Source": source_name,
                "DetailType": detail_type,
                "Detail": json.dumps(record) if isinstance(record, dict) else str(record),
                "EventBusName": target_arn.split("/")[-1] if "/" in target_arn else "default",
            }
        )

    if events:
        backend.put_events(events)


def _deliver_to_stepfunctions(
    target_arn: str,
    records: list[dict],
    target_params: dict,
    region: str,
    account_id: str,
) -> None:
    """Start Step Functions executions."""
    acct = account_id if account_id != "123456789012" else MOTO_DEFAULT_ACCOUNT_ID
    backend = get_backend("stepfunctions")[acct][region]

    for record in records:
        input_data = json.dumps(record) if isinstance(record, dict) else str(record)
        backend.start_execution(target_arn, name=None, execution_input=input_data)


def _deliver_to_kinesis(
    target_arn: str,
    records: list[dict],
    target_params: dict,
    region: str,
    account_id: str,
) -> None:
    """Put records to a Kinesis stream."""
    acct = account_id if account_id != "123456789012" else MOTO_DEFAULT_ACCOUNT_ID
    stream_name = target_arn.split("/")[-1]
    backend = get_backend("kinesis")[acct][region]

    kinesis_params = target_params.get("KinesisStreamParameters", {})
    partition_key = kinesis_params.get("PartitionKey", "pipes-default")

    for record in records:
        data = json.dumps(record) if isinstance(record, dict) else str(record)
        backend.put_record(stream_name, data, partition_key, None, None)


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------


def _json_response(data: dict, status: int = 200) -> Response:
    return Response(
        content=json.dumps(data, default=str),
        status_code=status,
        media_type="application/json",
    )


def _error(code: str, message: str, status: int) -> Response:
    body = json.dumps({"__type": code, "Message": message})
    return Response(content=body, status_code=status, media_type="application/json")


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def reset_pipes_state() -> None:
    """Reset all pipes state. Used by tests."""
    with _lock:
        _pipes.clear()
    with _polling_threads_lock:
        for stop_event in _polling_threads.values():
            stop_event.set()
        _polling_threads.clear()
