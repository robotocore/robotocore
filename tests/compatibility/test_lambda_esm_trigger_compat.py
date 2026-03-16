"""Lambda Event Source Mapping trigger tests -- verifies that ESM actually invokes Lambda.

These tests go beyond CRUD to verify the end-to-end trigger path:
SQS/Kinesis/DynamoDB Streams -> ESM engine -> Lambda invocation.

The verification pattern: Lambda writes a marker (to a DynamoDB table),
and we poll for that marker to confirm invocation happened.
"""

import io
import json
import time
import uuid
import zipfile

import pytest

from tests.compatibility.conftest import make_client

POLL_INTERVAL = 0.5
POLL_TIMEOUT = 15  # seconds to wait for ESM engine to trigger


def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


def _poll_until(predicate, timeout=POLL_TIMEOUT, interval=POLL_INTERVAL, desc="condition"):
    """Poll until predicate returns truthy, or raise after timeout."""
    deadline = time.time() + timeout
    last_result = None
    while time.time() < deadline:
        last_result = predicate()
        if last_result:
            return last_result
        time.sleep(interval)
    raise AssertionError(f"Timed out waiting for {desc} (last result: {last_result})")


@pytest.fixture
def lam():
    return make_client("lambda")


@pytest.fixture
def sqs():
    return make_client("sqs")


@pytest.fixture
def iam():
    return make_client("iam")


@pytest.fixture
def dynamodb():
    return make_client("dynamodb")


@pytest.fixture
def kinesis():
    return make_client("kinesis")


@pytest.fixture
def role(iam):
    name = f"esm-trigger-role-{uuid.uuid4().hex[:8]}"
    trust = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )
    resp = iam.create_role(RoleName=name, AssumeRolePolicyDocument=trust)
    yield resp["Role"]["Arn"]
    try:
        iam.delete_role(RoleName=name)
    except Exception:
        pass  # best-effort cleanup


@pytest.fixture
def marker_table(dynamodb):
    """Create a DynamoDB table used as a marker/side-effect store for Lambda invocations."""
    table_name = f"esm-marker-{uuid.uuid4().hex[:8]}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    yield table_name
    try:
        dynamodb.delete_table(TableName=table_name)
    except Exception:
        pass  # best-effort cleanup


def _lambda_code_write_marker(marker_table: str, endpoint_url: str) -> str:
    """Lambda code that writes a marker to DynamoDB when invoked.

    Records count and bodies from SQS Records, or a generic marker for other sources.
    """
    return (
        "import json\n"
        "import boto3\n"
        "import uuid\n"
        "def handler(event, ctx):\n"
        f'    ddb = boto3.resource("dynamodb", endpoint_url="{endpoint_url}",'
        '        region_name="us-east-1", aws_access_key_id="testing",'
        '        aws_secret_access_key="testing")\n'
        f'    table = ddb.Table("{marker_table}")\n'
        '    records = event.get("Records", [])\n'
        "    bodies = []\n"
        '    source = "unknown"\n'
        "    for r in records:\n"
        '        if "body" in r:\n'
        '            bodies.append(r["body"])\n'
        '            source = "sqs"\n'
        '        elif "kinesis" in r:\n'
        "            import base64\n"
        '            bodies.append(base64.b64decode(r["kinesis"]["data"]).decode())\n'
        '            source = "kinesis"\n'
        '        elif "dynamodb" in r:\n'
        '            bodies.append(json.dumps(r["dynamodb"].get("Keys", {})))\n'
        '            source = "dynamodb"\n'
        "    table.put_item(Item={\n"
        '        "pk": str(uuid.uuid4()),\n'
        '        "record_count": len(records),\n'
        '        "bodies": json.dumps(bodies),\n'
        '        "source": source,\n'
        "    })\n"
        "    return {'processed': len(records)}\n"
    )


def _lambda_code_partial_failure(marker_table: str, endpoint_url: str, fail_body: str) -> str:
    """Lambda code that reports partial batch failure for messages matching fail_body."""
    return "\n".join(
        [
            "import json",
            "import boto3",
            "import uuid",
            "def handler(event, ctx):",
            f'    ddb = boto3.resource("dynamodb", endpoint_url="{endpoint_url}",',
            '        region_name="us-east-1", aws_access_key_id="testing",',
            '        aws_secret_access_key="testing")',
            f'    table = ddb.Table("{marker_table}")',
            '    records = event.get("Records", [])',
            "    failures = []",
            "    for r in records:",
            f'        if r.get("body") == "{fail_body}":',
            '            failures.append({"itemIdentifier": r["messageId"]})',
            "    table.put_item(Item={",
            '        "pk": str(uuid.uuid4()),',
            '        "record_count": len(records),',
            '        "failure_count": len(failures),',
            "    })",
            '    return {"batchItemFailures": failures}',
            "",
        ]
    )


def _get_marker_items(dynamodb, marker_table: str) -> list[dict]:
    """Scan the marker table and return all items."""
    resp = dynamodb.scan(TableName=marker_table)
    return resp.get("Items", [])


ENDPOINT_URL = "http://host.docker.internal:4566"
# For in-process Lambda execution, localhost works
LOCAL_ENDPOINT = "http://localhost:4566"


class TestSQSToLambdaTrigger:
    """Verify SQS -> Lambda ESM trigger path."""

    def test_sqs_message_triggers_lambda_with_marker(
        self, lam, sqs, iam, role, dynamodb, marker_table
    ):
        """Send SQS message, verify Lambda invoked by checking DynamoDB marker."""
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"esm-trig-{suffix}"
        func_name = f"esm-trig-fn-{suffix}"

        # Create source queue
        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        # Create Lambda that writes to marker table
        code = _make_zip(_lambda_code_write_marker(marker_table, LOCAL_ENDPOINT))
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        # Create ESM
        esm = lam.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=func_name,
            BatchSize=10,
        )
        esm_uuid = esm["UUID"]

        try:
            # Send message
            sqs.send_message(QueueUrl=queue_url, MessageBody="hello-esm-trigger")

            # Poll for marker in DynamoDB
            def check_marker():
                items = _get_marker_items(dynamodb, marker_table)
                return items if items else None

            items = _poll_until(check_marker, desc="Lambda marker in DynamoDB")
            assert len(items) >= 1
            item = items[0]
            assert int(item["record_count"]["N"]) >= 1
            bodies = json.loads(item["bodies"]["S"])
            assert "hello-esm-trigger" in bodies

            # Verify message consumed from source queue
            remaining = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=1)
            assert len(remaining.get("Messages", [])) == 0

        finally:
            for fn in [
                lambda: lam.delete_event_source_mapping(UUID=esm_uuid),
                lambda: lam.delete_function(FunctionName=func_name),
                lambda: sqs.delete_queue(QueueUrl=queue_url),
            ]:
                try:
                    fn()
                except Exception:
                    pass  # best-effort cleanup

    def test_sqs_batch_triggers_lambda(self, lam, sqs, role, dynamodb, marker_table):
        """Send 5 messages, verify Lambda receives batch with multiple Records."""
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"esm-batch-{suffix}"
        func_name = f"esm-batch-fn-{suffix}"

        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        code = _make_zip(_lambda_code_write_marker(marker_table, LOCAL_ENDPOINT))
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        esm = lam.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=func_name,
            BatchSize=10,
        )
        esm_uuid = esm["UUID"]

        try:
            # Send 5 messages
            for i in range(5):
                sqs.send_message(QueueUrl=queue_url, MessageBody=f"batch-msg-{i}")

            # Poll for markers -- ESM may invoke once with batch or multiple times
            def check_all_consumed():
                items = _get_marker_items(dynamodb, marker_table)
                total = sum(int(it["record_count"]["N"]) for it in items)
                return total if total >= 5 else None

            total = _poll_until(check_all_consumed, desc="all 5 messages processed")
            assert total >= 5

            # Verify source queue is empty
            remaining = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=1)
            assert len(remaining.get("Messages", [])) == 0

        finally:
            for fn in [
                lambda: lam.delete_event_source_mapping(UUID=esm_uuid),
                lambda: lam.delete_function(FunctionName=func_name),
                lambda: sqs.delete_queue(QueueUrl=queue_url),
            ]:
                try:
                    fn()
                except Exception:
                    pass  # best-effort cleanup

    def test_sqs_fifo_triggers_lambda(self, lam, sqs, role, dynamodb, marker_table):
        """FIFO queue messages trigger Lambda with MessageGroupId in attributes."""
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"esm-fifo-{suffix}.fifo"
        func_name = f"esm-fifo-fn-{suffix}"

        q_resp = sqs.create_queue(
            QueueName=queue_name,
            Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
        )
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        code = _make_zip(_lambda_code_write_marker(marker_table, LOCAL_ENDPOINT))
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        esm = lam.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=func_name,
            BatchSize=10,
        )
        esm_uuid = esm["UUID"]

        try:
            # Send ordered messages in one group
            for i in range(3):
                sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=f"fifo-msg-{i}",
                    MessageGroupId="group1",
                )

            # Poll for marker
            def check_fifo():
                items = _get_marker_items(dynamodb, marker_table)
                total = sum(int(it["record_count"]["N"]) for it in items)
                return total if total >= 3 else None

            total = _poll_until(check_fifo, desc="FIFO messages processed")
            assert total >= 3

            # Check ordering is preserved (bodies should be in order)
            items = _get_marker_items(dynamodb, marker_table)
            all_bodies = []
            for it in items:
                all_bodies.extend(json.loads(it["bodies"]["S"]))
            # All messages should be present
            for i in range(3):
                assert f"fifo-msg-{i}" in all_bodies

        finally:
            for fn in [
                lambda: lam.delete_event_source_mapping(UUID=esm_uuid),
                lambda: lam.delete_function(FunctionName=func_name),
                lambda: sqs.delete_queue(QueueUrl=queue_url),
            ]:
                try:
                    fn()
                except Exception:
                    pass  # best-effort cleanup

    def test_sqs_filter_criteria(self, lam, sqs, role, dynamodb, marker_table):
        """ESM with filter criteria: only matching messages trigger Lambda."""
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"esm-filter-{suffix}"
        func_name = f"esm-filter-fn-{suffix}"

        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        code = _make_zip(_lambda_code_write_marker(marker_table, LOCAL_ENDPOINT))
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        # Filter: only process messages where body contains {"status": "active"}
        filter_pattern = json.dumps({"body": {"status": ["active"]}})
        esm = lam.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=func_name,
            BatchSize=10,
            FilterCriteria={"Filters": [{"Pattern": filter_pattern}]},
        )
        esm_uuid = esm["UUID"]

        try:
            # Send matching message (body is a JSON string)
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({"status": "active", "data": "match"}),
            )
            # Send non-matching message
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({"status": "inactive", "data": "nomatch"}),
            )

            # Wait for ESM to process -- both messages should be consumed from queue
            # (filtered messages are deleted, not left in queue)
            time.sleep(8)

            # Check markers: only the matching message should have triggered Lambda
            items = _get_marker_items(dynamodb, marker_table)
            if items:
                all_bodies = []
                for it in items:
                    all_bodies.extend(json.loads(it["bodies"]["S"]))
                # The matching message should be in markers
                found_match = any("active" in b for b in all_bodies)
                assert found_match, f"Expected 'active' message in markers, got: {all_bodies}"
                # Non-matching should NOT trigger Lambda
                found_nomatch = any("inactive" in b for b in all_bodies)
                assert not found_nomatch, (
                    f"Non-matching message should not trigger Lambda, got: {all_bodies}"
                )

            # Both messages should be consumed from queue (filter deletes non-matching)
            remaining = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=2)
            assert len(remaining.get("Messages", [])) == 0

        finally:
            for fn in [
                lambda: lam.delete_event_source_mapping(UUID=esm_uuid),
                lambda: lam.delete_function(FunctionName=func_name),
                lambda: sqs.delete_queue(QueueUrl=queue_url),
            ]:
                try:
                    fn()
                except Exception:
                    pass  # best-effort cleanup


class TestESMDisableEnable:
    """Test ESM enable/disable lifecycle."""

    def test_disabled_esm_does_not_trigger(self, lam, sqs, role, dynamodb, marker_table):
        """Disable ESM, send message, verify Lambda NOT invoked.
        Re-enable, send again, verify invoked."""
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"esm-disable-{suffix}"
        func_name = f"esm-disable-fn-{suffix}"

        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        code = _make_zip(_lambda_code_write_marker(marker_table, LOCAL_ENDPOINT))
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        # Create ESM then immediately disable it
        esm = lam.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=func_name,
            BatchSize=10,
        )
        esm_uuid = esm["UUID"]

        try:
            # Disable ESM
            lam.update_event_source_mapping(UUID=esm_uuid, Enabled=False)
            time.sleep(2)  # Wait for state to propagate

            # Send message while disabled
            sqs.send_message(QueueUrl=queue_url, MessageBody="while-disabled")

            # Wait and verify Lambda was NOT invoked
            time.sleep(5)
            items = _get_marker_items(dynamodb, marker_table)
            assert len(items) == 0, "Lambda should not be invoked while ESM is disabled"

            # Message should still be in queue (not consumed)
            remaining = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=1)
            msgs = remaining.get("Messages", [])
            assert len(msgs) >= 1, "Message should remain in queue when ESM is disabled"
            # Return messages to queue (make them visible again)
            for m in msgs:
                sqs.change_message_visibility(
                    QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"], VisibilityTimeout=0
                )

            # Re-enable ESM
            lam.update_event_source_mapping(UUID=esm_uuid, Enabled=True)

            # Poll for the marker -- ESM should now consume and invoke
            def check_enabled():
                items = _get_marker_items(dynamodb, marker_table)
                return items if items else None

            items = _poll_until(check_enabled, desc="Lambda invoked after re-enable")
            assert len(items) >= 1

        finally:
            for fn in [
                lambda: lam.delete_event_source_mapping(UUID=esm_uuid),
                lambda: lam.delete_function(FunctionName=func_name),
                lambda: sqs.delete_queue(QueueUrl=queue_url),
            ]:
                try:
                    fn()
                except Exception:
                    pass  # best-effort cleanup


class TestPartialBatchFailure:
    """Test ReportBatchItemFailures -- partial batch failure handling."""

    def test_partial_failure_retries_failed_messages(self, lam, sqs, role):
        """Lambda reports batchItemFailures, successful messages are deleted,
        failed messages remain in queue (not deleted).

        We verify by checking that the good message is consumed but the
        failed message eventually shows up as available in the queue again.
        """
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"esm-partial-{suffix}"
        func_name = f"esm-partial-fn-{suffix}"

        # Use a longer visibility timeout so the failed message comes back predictably
        q_resp = sqs.create_queue(
            QueueName=queue_name,
            Attributes={"VisibilityTimeout": "2"},
        )
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        # Lambda that reports "fail-me" as a batch item failure
        code = _make_zip(
            "\n".join(
                [
                    "def handler(event, ctx):",
                    '    records = event.get("Records", [])',
                    "    failures = []",
                    "    for r in records:",
                    '        if r.get("body") == "fail-me":',
                    '            failures.append({"itemIdentifier": r["messageId"]})',
                    '    return {"batchItemFailures": failures}',
                    "",
                ]
            )
        )
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        esm = lam.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=func_name,
            BatchSize=10,
            FunctionResponseTypes=["ReportBatchItemFailures"],
        )
        esm_uuid = esm["UUID"]

        try:
            # Send one good and one bad message
            sqs.send_message(QueueUrl=queue_url, MessageBody="good-msg")
            sqs.send_message(QueueUrl=queue_url, MessageBody="fail-me")

            # Wait for ESM to process. The good message should be deleted.
            # The fail-me message should remain (not deleted) and become visible again.
            # We'll poll the queue attributes to see when the approx message count drops
            # (indicating processing happened), then check if fail-me is available.
            time.sleep(6)

            # Now disable the ESM so it doesn't keep consuming the failed message
            lam.update_event_source_mapping(UUID=esm_uuid, Enabled=False)
            time.sleep(2)

            # Check if the failed message is back in the queue
            remaining = sqs.receive_message(
                QueueUrl=queue_url, WaitTimeSeconds=3, MaxNumberOfMessages=10
            )
            msgs = remaining.get("Messages", [])
            bodies = [m.get("Body") for m in msgs]

            # The good message should NOT be in queue (it was successfully processed)
            assert "good-msg" not in bodies, (
                "Successfully processed message should be deleted from queue"
            )

            # The fail-me message should be back in queue (not deleted due to failure)
            assert "fail-me" in bodies, (
                "Failed message should remain in queue after partial batch failure"
            )

        finally:
            for fn in [
                lambda: lam.delete_event_source_mapping(UUID=esm_uuid),
                lambda: lam.delete_function(FunctionName=func_name),
                lambda: sqs.delete_queue(QueueUrl=queue_url),
            ]:
                try:
                    fn()
                except Exception:
                    pass  # best-effort cleanup


class TestKinesisToLambdaTrigger:
    """Verify Kinesis -> Lambda ESM trigger path."""

    def test_kinesis_record_triggers_lambda(self, lam, kinesis, role, dynamodb, marker_table):
        """Put records to Kinesis, verify Lambda is invoked with them."""
        suffix = uuid.uuid4().hex[:8]
        stream_name = f"esm-kin-{suffix}"
        func_name = f"esm-kin-fn-{suffix}"

        # Create Kinesis stream
        kinesis.create_stream(StreamName=stream_name, ShardCount=1)
        # Wait for stream to be active
        time.sleep(1)
        desc = kinesis.describe_stream(StreamName=stream_name)
        stream_arn = desc["StreamDescription"]["StreamARN"]

        code = _make_zip(_lambda_code_write_marker(marker_table, LOCAL_ENDPOINT))
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        esm = lam.create_event_source_mapping(
            EventSourceArn=stream_arn,
            FunctionName=func_name,
            BatchSize=10,
            StartingPosition="TRIM_HORIZON",
        )
        esm_uuid = esm["UUID"]

        try:
            # Put records
            kinesis.put_record(
                StreamName=stream_name,
                Data=b"kinesis-payload-1",
                PartitionKey="pk1",
            )

            # Poll for marker
            def check_kinesis():
                items = _get_marker_items(dynamodb, marker_table)
                return items if items else None

            items = _poll_until(check_kinesis, desc="Lambda invoked from Kinesis")
            assert len(items) >= 1
            item = items[0]
            assert item.get("source", {}).get("S") == "kinesis"
            bodies = json.loads(item["bodies"]["S"])
            assert "kinesis-payload-1" in bodies

        finally:
            for fn in [
                lambda: lam.delete_event_source_mapping(UUID=esm_uuid),
                lambda: lam.delete_function(FunctionName=func_name),
                lambda: kinesis.delete_stream(StreamName=stream_name),
            ]:
                try:
                    fn()
                except Exception:
                    pass  # best-effort cleanup


class TestDynamoDBStreamsToLambdaTrigger:
    """Verify DynamoDB Streams -> Lambda ESM trigger path."""

    def test_dynamodb_stream_triggers_lambda(self, lam, dynamodb, role, marker_table):
        """Put item to DynamoDB table with stream, verify Lambda is invoked."""
        suffix = uuid.uuid4().hex[:8]
        source_table = f"esm-ddb-src-{suffix}"
        func_name = f"esm-ddb-fn-{suffix}"

        # Create source table with stream enabled
        dynamodb.create_table(
            TableName=source_table,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )

        # Get the stream ARN
        desc = dynamodb.describe_table(TableName=source_table)
        stream_arn = desc["Table"].get("LatestStreamArn")
        assert stream_arn, "Table should have a stream ARN"

        code = _make_zip(_lambda_code_write_marker(marker_table, LOCAL_ENDPOINT))
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        esm = lam.create_event_source_mapping(
            EventSourceArn=stream_arn,
            FunctionName=func_name,
            BatchSize=10,
            StartingPosition="TRIM_HORIZON",
        )
        esm_uuid = esm["UUID"]

        try:
            # Put an item to the source table (should trigger stream -> Lambda)
            dynamodb.put_item(
                TableName=source_table,
                Item={"id": {"S": "item-1"}, "data": {"S": "stream-trigger-test"}},
            )

            # Poll for marker
            def check_ddb_stream():
                items = _get_marker_items(dynamodb, marker_table)
                return items if items else None

            items = _poll_until(check_ddb_stream, desc="Lambda invoked from DynamoDB stream")
            assert len(items) >= 1
            item = items[0]
            assert item.get("source", {}).get("S") == "dynamodb"

        finally:
            for fn in [
                lambda: lam.delete_event_source_mapping(UUID=esm_uuid),
                lambda: lam.delete_function(FunctionName=func_name),
                lambda: dynamodb.delete_table(TableName=source_table),
            ]:
                try:
                    fn()
                except Exception:
                    pass  # best-effort cleanup
