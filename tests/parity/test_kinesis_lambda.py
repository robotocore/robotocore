"""Kinesis -> Lambda parity test.

Derived from the Kinesis/Firehose scenario pattern:
Kinesis stream -> Lambda ESM -> DynamoDB.

Tests Kinesis stream operations and ESM configuration.
The original test puts records into Kinesis and verifies delivery.

Note: End-to-end Kinesis -> Lambda delivery works in-process now — the
twin's ESM engine invokes the mapped function directly. This test covers:
1. Kinesis streams can be created
2. Records can be put and read from streams
3. ESM can be configured (Kinesis -> Lambda)
4. Records put after the mapping attach are delivered to the Lambda for
   real (the delivery test below follows the data_pipeline/event_chain
   app-suite pattern with a marker DynamoDB table)
"""

import io
import json
import os
import time
import uuid
import zipfile


def _make_lambda_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


SIMPLE_HANDLER = """
import json
def handler(event, context):
    return {"statusCode": 200, "records": len(event.get("Records", []))}
"""


class TestKinesisLambda:
    """Kinesis stream ops and ESM config, mirroring kinesis_firehose."""

    def test_kinesis_put_get_records(self, aws_client):
        """Put records to Kinesis and read them back."""
        kinesis = aws_client.kinesis
        stream_name = _unique("parity-kinesis")

        try:
            # Create stream
            kinesis.create_stream(StreamName=stream_name, ShardCount=1)

            for _ in range(30):
                desc = kinesis.describe_stream(StreamName=stream_name)
                if desc["StreamDescription"]["StreamStatus"] == "ACTIVE":
                    break
                time.sleep(1)

            # Put records (mirrors original 10-message pattern)
            test_message = f"Test-message-{uuid.uuid4().hex[:8]}"
            for i in range(5):
                kinesis.put_record(
                    StreamName=stream_name,
                    Data=json.dumps({"Id": f"msg_{i}", "Data": test_message}),
                    PartitionKey="1",
                )

            # Read records back
            desc = kinesis.describe_stream(StreamName=stream_name)
            shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]

            iterator = kinesis.get_shard_iterator(
                StreamName=stream_name,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )
            shard_iter = iterator["ShardIterator"]

            all_records = []
            for _ in range(10):
                resp = kinesis.get_records(ShardIterator=shard_iter, Limit=100)
                all_records.extend(resp.get("Records", []))
                if len(all_records) >= 5:
                    break
                shard_iter = resp.get("NextShardIterator")
                if not shard_iter:
                    break
                time.sleep(0.5)

            assert len(all_records) >= 5

            # Verify record content
            payloads = []
            for record in all_records:
                data = json.loads(record["Data"])
                payloads.append(data)

            data_values = [p["Data"] for p in payloads]
            assert all(d == test_message for d in data_values[:5])

            ids = sorted([p["Id"] for p in payloads[:5]])
            assert ids == [f"msg_{i}" for i in range(5)]

        finally:
            try:
                kinesis.delete_stream(
                    StreamName=stream_name,
                    EnforceConsumerDeletion=True,
                )
            except Exception:
                pass  # best-effort cleanup

    def test_kinesis_lambda_esm_config(self, aws_client, lambda_role_arn):
        """Verify Kinesis -> Lambda ESM can be configured."""
        kinesis = aws_client.kinesis
        lam = aws_client.lambda_

        stream_name = _unique("parity-kinesis-esm")
        fn_name = _unique("kinesis-processor")
        esm_uuid = None

        try:
            # Create stream
            kinesis.create_stream(StreamName=stream_name, ShardCount=1)

            for _ in range(30):
                desc = kinesis.describe_stream(StreamName=stream_name)
                if desc["StreamDescription"]["StreamStatus"] == "ACTIVE":
                    break
                time.sleep(1)

            stream_arn = desc["StreamDescription"]["StreamARN"]

            # Create Lambda
            lam.create_function(
                FunctionName=fn_name,
                Runtime="python3.12",
                Role=lambda_role_arn,
                Handler="lambda_function.handler",
                Code={"ZipFile": _make_lambda_zip(SIMPLE_HANDLER)},
                Timeout=30,
            )
            for _ in range(30):
                fn = lam.get_function(FunctionName=fn_name)
                if fn["Configuration"]["State"] == "Active":
                    break
                time.sleep(1)

            # Create ESM
            esm = lam.create_event_source_mapping(
                EventSourceArn=stream_arn,
                FunctionName=fn_name,
                StartingPosition="TRIM_HORIZON",
                BatchSize=10,
                Enabled=True,
            )
            esm_uuid = esm["UUID"]
            assert esm["EventSourceArn"] == stream_arn
            assert esm["BatchSize"] == 10

            # Verify ESM is visible via get
            esm_state = lam.get_event_source_mapping(UUID=esm_uuid)
            assert esm_state["FunctionArn"].endswith(fn_name)
            assert esm_state["EventSourceArn"] == stream_arn
            assert esm_state["BatchSize"] == 10

        finally:
            if esm_uuid:
                try:
                    lam.delete_event_source_mapping(UUID=esm_uuid)
                except Exception:
                    pass  # best-effort cleanup
            try:
                lam.delete_function(FunctionName=fn_name)
            except Exception:
                pass  # best-effort cleanup
            try:
                kinesis.delete_stream(
                    StreamName=stream_name,
                    EnforceConsumerDeletion=True,
                )
            except Exception:
                pass  # best-effort cleanup

    DELIVERY_HANDLER = (
        "import json\n"
        "import os\n"
        "import boto3\n"
        "\n"
        "def handler(event, context):\n"
        '    endpoint = os.environ[\'ENDPOINT_URL\']\n'
        '    ddb = boto3.client("dynamodb", endpoint_url=endpoint)\n'
        '    records = event.get("Records", [])\n'
        "    for record in records:\n"
        "        ddb.put_item(\n"
        '            TableName=os.environ["MARKER_TABLE"],\n'
        '            Item={"pk": {"S": record["eventID"]},\n'
        '                  "eventSourceARN": {"S": record["eventSourceARN"]}})\n'
        '    if not os.environ.get("VERIFY_QUEUE_URL"):\n'
        '        return {"delivered": len(records)}\n'
        '    sqs = boto3.client("sqs", endpoint_url=endpoint)\n'
        "    sqs.send_message(\n"
        '        QueueUrl=os.environ["VERIFY_QUEUE_URL"],\n'
        '        MessageBody=json.dumps({\n'
        '            "count": len(records),\n'
        '            "records": records,\n'
        '        }),\n'
        "    )\n"
        '    return {"delivered": len(records)}\n'
    )

    def test_kinesis_esm_delivers_records_for_real(self, aws_client, lambda_role_arn):
        """put_record -> the mapped Lambda actually receives the event.

        The in-process ESM engine delivers the kinesis record batch to the
        function; the handler writes one marker item per record into a
        DynamoDB table and reports the batch to a verify SQS queue (the
        event_chain/data_pipeline app-suite pattern, wait_for_messages
        helper).
        """
        kinesis = aws_client.kinesis
        lam = aws_client.lambda_
        dynamodb = aws_client.dynamodb
        sqs = aws_client.sqs

        # ENDPOINT_URL-aware endpoint (the portability rule: never a bare
        # localhost constant).
        endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")

        stream_name = _unique("parity-kinesis-delivery")
        fn_name = _unique("kinesis-delivery-fn")
        table_name = _unique("parity-kinesis-marker")
        queue_name = _unique("parity-kinesis-verify")
        esm_uuid = None
        queue_url = None
        cleanup_stream = False

        try:
            kinesis.create_stream(StreamName=stream_name, ShardCount=1)
            for _ in range(30):
                desc = kinesis.describe_stream(StreamName=stream_name)
                if desc["StreamDescription"]["StreamStatus"] == "ACTIVE":
                    break
                time.sleep(1)
            stream_arn = desc["StreamDescription"]["StreamARN"]
            cleanup_stream = True

            dynamodb.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            for _ in range(15):
                if dynamodb.describe_table(TableName=table_name)["Table"][
                    "TableStatus"
                ] == "ACTIVE":
                    break
                time.sleep(1)

            vq = sqs.create_queue(QueueName=queue_name)
            queue_url = vq["QueueUrl"]

            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w") as zf:
                zf.writestr("lambda_function.py", self.DELIVERY_HANDLER)
            lam.create_function(
                FunctionName=fn_name,
                Runtime="python3.12",
                Role=lambda_role_arn,
                Handler="lambda_function.handler",
                Code={"ZipFile": buf.getvalue()},
                Environment={
                    "Variables": {
                        "ENDPOINT_URL": endpoint,
                        "MARKER_TABLE": table_name,
                        "VERIFY_QUEUE_URL": queue_url,
                    }
                },
                Timeout=30,
            )
            for _ in range(30):
                if lam.get_function(FunctionName=fn_name)["Configuration"][
                    "State"
                ] == "Active":
                    break
                time.sleep(1)

            esm = lam.create_event_source_mapping(
                EventSourceArn=stream_arn,
                FunctionName=fn_name,
                StartingPosition="LATEST",
                BatchSize=10,
                Enabled=True,
            )
            esm_uuid = esm["UUID"]

            # Records land on the stream after the attach: the poller
            # must invoke the Lambda with them.
            record_datas = [json.dumps({"delivery": i}) for i in range(4)]
            for data in record_datas:
                kinesis.put_record(
                    StreamName=stream_name, Data=data, PartitionKey="p"
                )

            from tests.apps.conftest import wait_for_messages

            messages = wait_for_messages(sqs, queue_url, timeout=10, expected=1)
            assert len(messages) >= 1, (
                "the kinesis ESM did not deliver to the Lambda within 10s"
            )
            report = json.loads(messages[0]["Body"])
            assert report["count"] >= 1
            for record in report["records"]:
                assert record["eventSourceARN"] == stream_arn
                assert record["kinesis"]["data"] is not None

            # Every delivered record left its marker row (eventID is the
            # row key; one per delivered record, >= 1 record).
            deadline = time.time() + 10
            seen = set()
            while time.time() < deadline and len(seen) < report["count"]:
                scan = dynamodb.scan(TableName=table_name)
                seen.update(item["pk"]["S"] for item in scan.get("Items", []))
                time.sleep(1)
            assert len(seen) >= report["count"], (
                f"marker rows for every delivered record: {len(seen)} of "
                f"{report['count']}"
            )
        finally:
            if esm_uuid:
                try:
                    lam.delete_event_source_mapping(UUID=esm_uuid)
                except Exception:
                    pass  # best-effort cleanup
            try:
                lam.delete_function(FunctionName=fn_name)
            except Exception:
                pass  # best-effort cleanup
            try:
                dynamodb.delete_table(TableName=table_name)
            except Exception:
                pass  # best-effort cleanup
            if queue_url:
                try:
                    sqs.delete_queue(QueueUrl=queue_url)
                except Exception:
                    pass  # best-effort cleanup
            if cleanup_stream:
                try:
                    kinesis.delete_stream(
                        StreamName=stream_name,
                        EnforceConsumerDeletion=True,
                    )
                except Exception:
                    pass  # best-effort cleanup
