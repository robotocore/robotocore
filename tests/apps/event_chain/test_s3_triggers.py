"""Tests for S3 notification triggers.

Verifies that S3 PutObject fires notifications to Lambda and SQS targets.
"""

import json
import time

from tests.apps.conftest import make_lambda_zip, wait_for_messages


class TestS3ToLambda:
    """S3 PutObject → Lambda → DynamoDB verification."""

    def test_s3_put_triggers_lambda_writes_ddb(
        self, chain, unique_name, lambda_role, lambda_client
    ):
        """Upload to S3 → Lambda fires → writes item to DynamoDB."""
        bucket = chain.create_bucket(f"trigger-{unique_name}")
        table_info = chain.create_table(f"results-{unique_name}")
        table_name = table_info["table_name"]

        handler_code = f"""\
import json
import boto3

def handler(event, context):
    ddb = boto3.client("dynamodb", endpoint_url="{chain.endpoint_url}")
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        ddb.put_item(
            TableName="{table_name}",
            Item={{
                "pk": {{"S": bucket}},
                "sk": {{"S": key}},
                "source": {{"S": "s3-trigger"}},
            }},
        )
    return {{"statusCode": 200}}
"""
        fn_name = f"s3-handler-{unique_name}"
        zip_bytes = make_lambda_zip(handler_code)
        resp = lambda_client.create_function(
            FunctionName=fn_name,
            Runtime="python3.12",
            Role=lambda_role,
            Handler="index.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=30,
            Environment={"Variables": {"ENDPOINT_URL": chain.endpoint_url}},
        )
        fn_arn = resp["FunctionArn"]
        chain._functions.append(fn_name)

        chain.configure_s3_to_lambda(bucket, fn_arn)

        # Upload a file
        chain.s3.put_object(Bucket=bucket, Key="test/file.txt", Body=b"hello")

        # Poll DynamoDB for the result
        deadline = time.time() + 10
        item = None
        while time.time() < deadline:
            item = chain.get_ddb_item(table_name, bucket, "test/file.txt")
            if item:
                break
            time.sleep(0.5)

        assert item is not None, "Lambda did not write to DynamoDB within 10s"
        assert item["source"]["S"] == "s3-trigger"

    def test_s3_prefix_filter_only_matching(self, chain, unique_name, lambda_role, lambda_client):
        """S3 notification with prefix filter — only matching keys trigger Lambda."""
        bucket = chain.create_bucket(f"filter-{unique_name}")
        table_info = chain.create_table(f"filter-results-{unique_name}")
        table_name = table_info["table_name"]

        handler_code = f"""\
import boto3

def handler(event, context):
    ddb = boto3.client("dynamodb", endpoint_url="{chain.endpoint_url}")
    for record in event.get("Records", []):
        key = record["s3"]["object"]["key"]
        ddb.put_item(
            TableName="{table_name}",
            Item={{"pk": {{"S": "triggered"}}, "sk": {{"S": key}}}},
        )
    return {{"statusCode": 200}}
"""
        fn_name = f"s3-filter-{unique_name}"
        zip_bytes = make_lambda_zip(handler_code)
        resp = lambda_client.create_function(
            FunctionName=fn_name,
            Runtime="python3.12",
            Role=lambda_role,
            Handler="index.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=30,
        )
        fn_arn = resp["FunctionArn"]
        chain._functions.append(fn_name)

        # Only trigger on uploads/ prefix
        chain.configure_s3_to_lambda(bucket, fn_arn, prefix="uploads/")

        # Upload non-matching key — should NOT trigger
        chain.s3.put_object(Bucket=bucket, Key="other/file.txt", Body=b"no")
        # Upload matching key — should trigger
        chain.s3.put_object(Bucket=bucket, Key="uploads/doc.pdf", Body=b"yes")

        deadline = time.time() + 10
        item = None
        while time.time() < deadline:
            item = chain.get_ddb_item(table_name, "triggered", "uploads/doc.pdf")
            if item:
                break
            time.sleep(0.5)

        assert item is not None, "Matching prefix did not trigger Lambda"

        # Verify non-matching key was NOT written
        non_match = chain.get_ddb_item(table_name, "triggered", "other/file.txt")
        assert non_match is None, "Non-matching prefix should not trigger Lambda"


class TestS3ToSqs:
    """S3 notification → SQS (no Lambda)."""

    def test_s3_put_sends_to_sqs(self, chain, unique_name, sqs):
        """S3 PutObject → SQS notification message."""
        bucket = chain.create_bucket(f"sqs-notif-{unique_name}")
        queue_url, queue_arn = chain.create_queue(f"s3-notif-{unique_name}")

        chain.configure_s3_to_sqs(bucket, queue_arn)

        chain.s3.put_object(Bucket=bucket, Key="data/report.csv", Body=b"col1,col2")

        messages = wait_for_messages(sqs, queue_url, timeout=10, expected=1)
        assert len(messages) >= 1, "No SQS message received from S3 notification"

        body = json.loads(messages[0]["Body"])
        # S3 notification sends Records array
        records = body.get("Records", [body])
        record = records[0] if records else body
        assert record.get("eventSource") == "aws:s3" or "s3" in str(body).lower()
