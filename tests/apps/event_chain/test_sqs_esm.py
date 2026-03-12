"""Tests for SQS → Lambda event source mappings.

Verifies that messages sent to SQS trigger Lambda via ESM polling.
"""

import json
import time

from tests.apps.conftest import make_lambda_zip


class TestSqsEsm:
    """SQS → Lambda event source mapping tests."""

    def test_sqs_message_triggers_lambda(self, chain, unique_name, lambda_role, lambda_client, sqs):
        """Send SQS message → ESM polls → Lambda writes to DynamoDB."""
        queue_url, queue_arn = chain.create_queue(f"esm-src-{unique_name}")
        table_info = chain.create_table(f"esm-results-{unique_name}")
        table_name = table_info["table_name"]

        handler_code = f"""\
import json
import boto3

def handler(event, context):
    ddb = boto3.client("dynamodb", endpoint_url="{chain.endpoint_url}")
    for record in event.get("Records", []):
        body = json.loads(record["body"])
        ddb.put_item(
            TableName="{table_name}",
            Item={{
                "pk": {{"S": "sqs-esm"}},
                "sk": {{"S": body.get("id", "unknown")}},
                "processed": {{"S": "true"}},
            }},
        )
    return {{"statusCode": 200}}
"""
        fn_name = f"sqs-esm-{unique_name}"
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

        chain.create_sqs_esm(queue_arn, fn_name, batch_size=1)

        # Send a message
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({"id": "MSG-001", "data": "test"}),
        )

        # Wait for Lambda to process
        deadline = time.time() + 15
        item = None
        while time.time() < deadline:
            item = chain.get_ddb_item(table_name, "sqs-esm", "MSG-001")
            if item:
                break
            time.sleep(1)

        assert item is not None, "Lambda did not process SQS message within 15s"
        assert item["processed"]["S"] == "true"

    def test_sqs_esm_batch_processing(self, chain, unique_name, lambda_role, lambda_client, sqs):
        """Send 5 messages → ESM batches → Lambda processes all."""
        queue_url, queue_arn = chain.create_queue(f"esm-batch-{unique_name}")
        table_info = chain.create_table(f"esm-batch-results-{unique_name}")
        table_name = table_info["table_name"]

        handler_code = f"""\
import json
import boto3

def handler(event, context):
    ddb = boto3.client("dynamodb", endpoint_url="{chain.endpoint_url}")
    for record in event.get("Records", []):
        body = json.loads(record["body"])
        ddb.put_item(
            TableName="{table_name}",
            Item={{
                "pk": {{"S": "batch"}},
                "sk": {{"S": body["id"]}},
                "seq": {{"N": str(body["seq"])}},
            }},
        )
    return {{"statusCode": 200}}
"""
        fn_name = f"sqs-batch-{unique_name}"
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

        chain.create_sqs_esm(queue_arn, fn_name, batch_size=5)

        # Send 5 messages
        for i in range(5):
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({"id": f"BATCH-{i}", "seq": i}),
            )

        # Wait for all to be processed
        deadline = time.time() + 15
        while time.time() < deadline:
            items = chain.scan_table(table_name)
            if len(items) >= 5:
                break
            time.sleep(1)

        items = chain.scan_table(table_name)
        assert len(items) >= 5, f"Expected 5 items, got {len(items)}"

    def test_sqs_message_deleted_after_success(
        self, chain, unique_name, lambda_role, lambda_client, sqs
    ):
        """After successful Lambda invocation, message is removed from queue."""
        queue_url, queue_arn = chain.create_queue(f"esm-del-{unique_name}")

        handler_code = """\
def handler(event, context):
    return {"statusCode": 200}
"""
        fn_name = f"sqs-del-{unique_name}"
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

        chain.create_sqs_esm(queue_arn, fn_name, batch_size=1)

        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({"test": "delete-check"}),
        )

        # Wait for ESM to process
        time.sleep(5)

        # Queue should be empty
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )
        count = int(attrs["Attributes"]["ApproximateNumberOfMessages"])
        assert count == 0, f"Queue should be empty after ESM processing, got {count}"
