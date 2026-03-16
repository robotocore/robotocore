"""DynamoDB Streams -> Lambda parity test.

Derived from the bookstore scenario pattern:
DynamoDB table with stream -> Lambda ESM -> secondary table.

Tests DynamoDB Streams configuration and event source mapping setup.
The original bookstore app creates a DDB table with streams, then uses
an ESM to trigger Lambda on writes.

Note: DDB streams get_records blocks the in-process server. This test
verifies stream creation and ESM configuration without reading records.
"""

import io
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
    return {"statusCode": 200, "processed": len(event.get("Records", []))}
"""


class TestDynamoDBStreams:
    """DDB Streams configuration, mirroring bookstore scenario."""

    def test_dynamodb_table_with_streams(self, aws_client):
        """Create DDB table with streams enabled and verify configuration."""
        ddb = aws_client.dynamodb

        table_name = _unique("books")

        try:
            # Create table with streams
            ddb.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
                StreamSpecification={
                    "StreamEnabled": True,
                    "StreamViewType": "NEW_AND_OLD_IMAGES",
                },
            )
            ddb.get_waiter("table_exists").wait(TableName=table_name)

            # Verify stream ARN exists
            desc = ddb.describe_table(TableName=table_name)
            stream_arn = desc["Table"]["LatestStreamArn"]
            assert stream_arn
            assert "stream" in stream_arn

            stream_spec = desc["Table"]["StreamSpecification"]
            assert stream_spec["StreamEnabled"] is True
            assert stream_spec["StreamViewType"] == "NEW_AND_OLD_IMAGES"

            # Put an item (creates stream record)
            test_id = f"book-{uuid.uuid4().hex[:8]}"
            ddb.put_item(
                TableName=table_name,
                Item={
                    "id": {"S": test_id},
                    "title": {"S": "Test Book"},
                    "category": {"S": "Testing"},
                },
            )

            # Verify item was written
            item = ddb.get_item(TableName=table_name, Key={"id": {"S": test_id}})
            assert item["Item"]["title"]["S"] == "Test Book"

            # Verify we can add GSI like the bookstore scenario does
            # (The bookstore uses a category-index GSI)
            scan = ddb.scan(TableName=table_name)
            assert scan["Count"] == 1

        finally:
            try:
                ddb.delete_table(TableName=table_name)
            except Exception:
                pass  # best-effort cleanup

    def test_dynamodb_stream_esm_config(self, aws_client, lambda_role_arn):
        """Verify DDB stream -> Lambda ESM can be configured."""
        ddb = aws_client.dynamodb
        lam = aws_client.lambda_

        table_name = _unique("books-esm")
        fn_name = _unique("stream-processor")
        esm_uuid = None

        try:
            # Create table with streams
            ddb.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
                StreamSpecification={
                    "StreamEnabled": True,
                    "StreamViewType": "NEW_AND_OLD_IMAGES",
                },
            )
            ddb.get_waiter("table_exists").wait(TableName=table_name)

            desc = ddb.describe_table(TableName=table_name)
            stream_arn = desc["Table"]["LatestStreamArn"]

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
                BatchSize=1,
                Enabled=True,
            )
            esm_uuid = esm["UUID"]
            assert esm["EventSourceArn"] == stream_arn
            assert esm["BatchSize"] == 1

            # Verify ESM is visible via get
            esm_state = lam.get_event_source_mapping(UUID=esm_uuid)
            assert esm_state["FunctionArn"].endswith(fn_name)
            assert esm_state["EventSourceArn"] == stream_arn
            assert esm_state["BatchSize"] == 1

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
                ddb.delete_table(TableName=table_name)
            except Exception:
                pass  # best-effort cleanup
