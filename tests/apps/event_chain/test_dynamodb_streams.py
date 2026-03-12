"""Tests for DynamoDB Streams → Lambda event source mappings.

Verifies that DynamoDB mutations fire stream events consumed by Lambda.
"""

import time

from tests.apps.conftest import make_lambda_zip


class TestDynamoDBStreams:
    """DynamoDB Streams → Lambda ESM tests."""

    def test_put_item_triggers_stream_lambda(self, chain, unique_name, lambda_role, lambda_client):
        """DDB put_item → stream → Lambda → writes marker to another table."""
        source_info = chain.create_table(f"stream-src-{unique_name}", stream=True)
        source_table = source_info["table_name"]
        stream_arn = source_info["stream_arn"]
        assert stream_arn, "Stream ARN should be set when stream=True"

        marker_info = chain.create_table(f"stream-marker-{unique_name}")
        marker_table = marker_info["table_name"]

        handler_code = f"""\
import json
import boto3

def handler(event, context):
    ddb = boto3.client("dynamodb", endpoint_url="{chain.endpoint_url}")
    for record in event.get("Records", []):
        if record.get("eventName") in ("INSERT", "MODIFY"):
            new_image = record.get("dynamodb", {{}}).get("NewImage", {{}})
            pk_val = new_image.get("pk", {{}}).get("S", "unknown")
            ddb.put_item(
                TableName="{marker_table}",
                Item={{
                    "pk": {{"S": "stream-trigger"}},
                    "sk": {{"S": pk_val}},
                    "event_name": {{"S": record["eventName"]}},
                }},
            )
    return {{"statusCode": 200}}
"""
        fn_name = f"ddb-stream-{unique_name}"
        zip_bytes = make_lambda_zip(handler_code)
        lambda_client.create_function(
            FunctionName=fn_name,
            Runtime="python3.12",
            Role=lambda_role,
            Handler="index.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=30,
        )
        chain._functions.append(fn_name)

        chain.create_dynamodb_stream_esm(stream_arn, fn_name)

        # Write to source table
        chain.dynamodb.put_item(
            TableName=source_table,
            Item={"pk": {"S": "user-123"}, "sk": {"S": "profile"}},
        )

        # Poll marker table
        deadline = time.time() + 15
        item = None
        while time.time() < deadline:
            item = chain.get_ddb_item(marker_table, "stream-trigger", "user-123")
            if item:
                break
            time.sleep(1)

        assert item is not None, "Stream Lambda did not fire within 15s"
        assert item["event_name"]["S"] == "INSERT"

    def test_update_item_triggers_modify_event(
        self, chain, unique_name, lambda_role, lambda_client
    ):
        """DDB update_item → stream fires MODIFY event → Lambda captures it."""
        source_info = chain.create_table(f"stream-upd-{unique_name}", stream=True)
        source_table = source_info["table_name"]
        stream_arn = source_info["stream_arn"]

        marker_info = chain.create_table(f"stream-upd-marker-{unique_name}")
        marker_table = marker_info["table_name"]

        handler_code = f"""\
import json
import boto3

def handler(event, context):
    ddb = boto3.client("dynamodb", endpoint_url="{chain.endpoint_url}")
    for record in event.get("Records", []):
        event_name = record.get("eventName", "UNKNOWN")
        # Write a marker for each event type
        keys = record.get("dynamodb", {{}}).get("Keys", {{}})
        pk_val = keys.get("pk", {{}}).get("S", "unknown")
        ddb.put_item(
            TableName="{marker_table}",
            Item={{
                "pk": {{"S": f"stream-{{event_name}}"}},
                "sk": {{"S": pk_val}},
                "event_name": {{"S": event_name}},
            }},
        )
    return {{"statusCode": 200}}
"""
        fn_name = f"ddb-upd-{unique_name}"
        zip_bytes = make_lambda_zip(handler_code)
        lambda_client.create_function(
            FunctionName=fn_name,
            Runtime="python3.12",
            Role=lambda_role,
            Handler="index.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=30,
        )
        chain._functions.append(fn_name)

        chain.create_dynamodb_stream_esm(stream_arn, fn_name)

        # Insert then update
        chain.dynamodb.put_item(
            TableName=source_table,
            Item={
                "pk": {"S": "order-456"},
                "sk": {"S": "status"},
                "status": {"S": "pending"},
            },
        )
        # Small delay to ensure INSERT is processed before MODIFY
        time.sleep(2)

        chain.dynamodb.update_item(
            TableName=source_table,
            Key={"pk": {"S": "order-456"}, "sk": {"S": "status"}},
            UpdateExpression="SET #s = :new",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":new": {"S": "shipped"}},
        )

        # Poll for MODIFY event marker
        deadline = time.time() + 15
        item = None
        while time.time() < deadline:
            item = chain.get_ddb_item(marker_table, "stream-MODIFY", "order-456")
            if item:
                break
            time.sleep(1)

        assert item is not None, "Stream Lambda did not capture MODIFY event within 15s"
        assert item["event_name"]["S"] == "MODIFY"
