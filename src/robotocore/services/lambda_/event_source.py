"""Lambda event source mapping engine — polls SQS/Kinesis/DynamoDB and invokes Lambda.

This is a key Enterprise-grade feature: when messages arrive in SQS (or records
appear in Kinesis/DynamoDB Streams), the engine automatically invokes the mapped
Lambda function with a batch of records.
"""

import logging
import threading
import time

from robotocore.services.lambda_.executor import execute_python_handler

logger = logging.getLogger(__name__)

# Global mapping engine (singleton)
_engine: "EventSourceEngine | None" = None
_engine_lock = threading.Lock()


def get_engine() -> "EventSourceEngine":
    global _engine
    with _engine_lock:
        if _engine is None:
            _engine = EventSourceEngine()
        return _engine


class EventSourceEngine:
    """Polls event sources and invokes Lambda functions."""

    def __init__(self):
        self._running = False
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def start(self):
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(target=self._poll_loop, daemon=True, name="lambda-esm")
            self._thread.start()

    def stop(self):
        with self._lock:
            self._running = False

    def _poll_loop(self):
        """Main polling loop — checks all event source mappings periodically."""
        while self._running:
            try:
                self._poll_all_mappings()
            except Exception:
                logger.exception("Error in event source mapping poll loop")
            time.sleep(1)  # Poll interval

    def _poll_all_mappings(self):
        """Find all enabled event source mappings and poll their sources."""
        from robotocore.services.lambda_.provider import get_event_source_mappings

        mappings = get_event_source_mappings()
        for config in mappings:
            try:
                state = config.get("State", "Enabled")
                if state != "Enabled":
                    continue

                event_source_arn = config.get("EventSourceArn", "")
                function_arn = config.get("FunctionArn", "")
                batch_size = config.get("BatchSize", 10)
                region = config.get("_region", "us-east-1")
                account_id = config.get("_account_id", "123456789012")

                if ":sqs:" in event_source_arn:
                    self._poll_sqs(event_source_arn, function_arn, batch_size, account_id, region)
                elif ":kinesis:" in event_source_arn:
                    self._poll_kinesis(
                        event_source_arn, function_arn, batch_size, account_id, region
                    )
                elif ":dynamodb:" in event_source_arn and "/stream/" in event_source_arn:
                    self._poll_dynamodb_stream(
                        event_source_arn, function_arn, batch_size, account_id, region
                    )
            except Exception:
                logger.exception("Error polling event source mapping")

    def _poll_sqs(
        self, queue_arn: str, function_arn: str, batch_size: int, account_id: str, region: str
    ):
        """Poll an SQS queue and invoke Lambda with received messages."""
        from robotocore.services.sqs.provider import _get_store

        store = _get_store(region)
        queue_name = queue_arn.rsplit(":", 1)[-1]
        queue = store.get_queue(queue_name)
        if not queue:
            return

        # Receive messages (non-blocking)
        messages = queue.receive(
            max_messages=min(batch_size, 10),
            visibility_timeout=None,  # Use queue default
            wait_time_seconds=0,  # Don't block
        )

        if not messages:
            return

        # Build SQS event payload (matches AWS format)
        records = []
        receipt_handles = []
        for msg, receipt_handle in messages:
            receipt_handles.append((receipt_handle, msg))
            record = {
                "messageId": msg.message_id,
                "receiptHandle": receipt_handle,
                "body": msg.body,
                "attributes": {
                    "ApproximateReceiveCount": str(msg.receive_count),
                    "SentTimestamp": str(int(msg.created * 1000)),
                    "ApproximateFirstReceiveTimestamp": str(
                        int((msg.first_received or time.time()) * 1000)
                    ),
                },
                "messageAttributes": _convert_message_attributes(msg.message_attributes),
                "md5OfBody": msg.md5_of_body,
                "eventSource": "aws:sqs",
                "eventSourceARN": queue_arn,
                "awsRegion": region,
            }
            records.append(record)

        event = {"Records": records}

        # Resolve the Lambda function
        function_name = _extract_function_name(function_arn)
        success = self._invoke_lambda(function_name, event, account_id, region)

        if success:
            # Delete successfully processed messages
            for receipt_handle, msg in receipt_handles:
                queue.delete_message(receipt_handle)
        # If invocation failed, messages will become visible again after visibility timeout

    def _poll_kinesis(
        self, stream_arn: str, function_arn: str, batch_size: int, account_id: str, region: str
    ):
        """Poll a Kinesis stream and invoke Lambda with new records."""
        from robotocore.services.kinesis.models import _get_store

        store = _get_store(region)
        stream_name = (
            stream_arn.rsplit("/", 1)[-1] if "/" in stream_arn else stream_arn.rsplit(":", 1)[-1]
        )

        stream = store.get_stream(stream_name)
        if not stream:
            return

        # Track position per shard using a simple in-memory tracker
        if not hasattr(self, "_kinesis_positions"):
            self._kinesis_positions = {}

        for shard in stream.shards:
            position_key = f"{stream_arn}:{shard.shard_id}"
            last_seq = self._kinesis_positions.get(position_key, "")

            # Get records after our last position (string comparison on zero-padded seqs)
            records_to_send = []
            for record in shard.records:
                if record.sequence_number > last_seq:
                    records_to_send.append(record)

            if not records_to_send:
                continue

            records_to_send = records_to_send[:batch_size]

            import base64

            event_records = []
            for rec in records_to_send:
                event_records.append(
                    {
                        "kinesis": {
                            "kinesisSchemaVersion": "1.0",
                            "partitionKey": rec.partition_key,
                            "sequenceNumber": str(rec.sequence_number),
                            "data": base64.b64encode(rec.data).decode(),
                            "approximateArrivalTimestamp": rec.timestamp,
                        },
                        "eventSource": "aws:kinesis",
                        "eventVersion": "1.0",
                        "eventID": f"{shard.shard_id}:{rec.sequence_number}",
                        "eventName": "aws:kinesis:record",
                        "invokeIdentityArn": function_arn,
                        "awsRegion": region,
                        "eventSourceARN": stream_arn,
                    }
                )

            event = {"Records": event_records}
            function_name = _extract_function_name(function_arn)
            success = self._invoke_lambda(function_name, event, account_id, region)

            if success:
                self._kinesis_positions[position_key] = records_to_send[-1].sequence_number

    def _poll_dynamodb_stream(
        self, stream_arn: str, function_arn: str, batch_size: int, account_id: str, region: str
    ):
        """Poll a DynamoDB Stream and invoke Lambda with new records.

        Reads from the hook-based record store populated by DynamoDB mutation hooks.
        """
        from robotocore.services.dynamodbstreams.hooks import get_store as get_ddb_streams_store

        store = get_ddb_streams_store(region)

        if not hasattr(self, "_dynamo_stream_positions"):
            self._dynamo_stream_positions = {}

        position_key = stream_arn
        last_idx = self._dynamo_stream_positions.get(position_key, 0)

        with store._lock:
            records = store._hook_records.get(stream_arn, [])
            new_records = records[last_idx : last_idx + batch_size]

        if not new_records:
            return

        event_records = []
        for rec in new_records:
            event_records.append(
                {
                    "eventID": rec.event_id,
                    "eventName": rec.event_name,
                    "eventVersion": rec.event_version,
                    "eventSource": rec.event_source,
                    "awsRegion": region,
                    "dynamodb": rec.dynamodb,
                    "eventSourceARN": stream_arn,
                }
            )

        event = {"Records": event_records}
        function_name = _extract_function_name(function_arn)
        success = self._invoke_lambda(function_name, event, account_id, region)

        if success:
            self._dynamo_stream_positions[position_key] = last_idx + len(new_records)

    def _invoke_lambda(self, function_name: str, event: dict, account_id: str, region: str) -> bool:
        """Invoke a Lambda function with the given event. Returns True on success."""
        import base64

        from moto.backends import get_backend
        from moto.core import DEFAULT_ACCOUNT_ID

        try:
            acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID
            backend = get_backend("lambda")[acct][region]
            fn = backend.get_function(function_name)
        except Exception:
            logger.error(f"Could not find Lambda function: {function_name}")
            return False

        # Check if it's a Python runtime with code
        runtime = getattr(fn, "run_time", "") or ""
        is_python = runtime.startswith("python")
        code_zip = None

        if is_python:
            # Prefer code_bytes (already decoded) over code["ZipFile"]
            code_zip = getattr(fn, "code_bytes", None)
            if not code_zip and hasattr(fn, "code") and fn.code:
                code_zip = fn.code.get("ZipFile")
                if isinstance(code_zip, str):
                    code_zip = base64.b64decode(code_zip)

        if is_python and code_zip:
            handler = getattr(fn, "handler", "lambda_function.handler")
            timeout = int(getattr(fn, "timeout", 3) or 3)
            memory_size = int(getattr(fn, "memory_size", 128) or 128)
            env_vars = getattr(fn, "environment_vars", {}) or {}

            result, error_type, logs = execute_python_handler(
                code_zip=code_zip,
                handler=handler,
                event=event,
                function_name=function_name,
                timeout=timeout,
                memory_size=memory_size,
                env_vars=env_vars,
                region=region,
                account_id=account_id,
            )

            if error_type:
                logger.warning(f"Lambda {function_name} error: {error_type} - {logs}")
                return False
            return True
        else:
            # Non-Python runtime — treat as success (no-op)
            logger.info(f"Skipping invocation for non-Python runtime: {runtime}")
            return True


def _extract_function_name(arn: str) -> str:
    """Extract function name from ARN or return as-is if already a name."""
    if arn.startswith("arn:"):
        # arn:aws:lambda:region:account:function:name
        parts = arn.split(":")
        if len(parts) >= 7:
            return parts[6]
    return arn


def _convert_message_attributes(attrs: dict) -> dict:
    """Convert SQS message attributes to Lambda event format."""
    result = {}
    for key, value in attrs.items():
        if isinstance(value, dict):
            result[key] = value
        else:
            result[key] = {"stringValue": str(value), "dataType": "String"}
    return result
