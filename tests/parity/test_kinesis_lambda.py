"""Kinesis -> Lambda parity test.

Derived from the Kinesis/Firehose scenario pattern:
Kinesis stream -> Lambda ESM -> DynamoDB.

Tests Kinesis stream operations and ESM configuration.
The original test puts records into Kinesis and verifies delivery.

Note: End-to-end Kinesis -> Lambda execution requires Lambda to call back
to the server, which doesn't work with in-process execution. This test
verifies:
1. Kinesis streams can be created
2. Records can be put and read from streams
3. ESM can be configured (Kinesis -> Lambda)
"""

import io
import json
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
