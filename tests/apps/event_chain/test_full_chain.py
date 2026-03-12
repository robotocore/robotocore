"""Tests for multi-hop event chains — the showpiece.

These tests verify that events flow through multiple services via real triggers,
proving robotocore is a genuine event-driven platform.
"""

import json
import time

from tests.apps.conftest import make_lambda_zip, wait_for_messages


class TestFullChain:
    """Multi-hop event chain tests."""

    def test_s3_to_lambda_to_ddb_to_stream_to_lambda_to_sns_to_sqs(
        self, chain, unique_name, lambda_role, lambda_client, sqs
    ):
        """The showpiece: S3 upload → Lambda A → DDB → Stream → Lambda B → SNS → SQS.

        Flow:
        1. S3 PutObject triggers Lambda A
        2. Lambda A writes to DynamoDB (with streams enabled)
        3. DynamoDB Stream triggers Lambda B
        4. Lambda B publishes to SNS
        5. SNS delivers to SQS
        6. Test reads SQS and verifies the full chain
        """
        # Create resources
        bucket = chain.create_bucket(f"chain-src-{unique_name}")
        table_info = chain.create_table(f"chain-ddb-{unique_name}", stream=True)
        table_name = table_info["table_name"]
        stream_arn = table_info["stream_arn"]
        topic_arn = chain.create_topic(f"chain-topic-{unique_name}")
        queue_url, queue_arn = chain.create_queue(f"chain-sink-{unique_name}")
        chain.subscribe_sqs_to_sns(topic_arn, queue_arn)

        # Lambda A: S3 event → write to DynamoDB
        handler_a_code = f"""\
import json
import boto3

def handler(event, context):
    ddb = boto3.client("dynamodb", endpoint_url="{chain.endpoint_url}")
    for record in event.get("Records", []):
        key = record["s3"]["object"]["key"]
        ddb.put_item(
            TableName="{table_name}",
            Item={{
                "pk": {{"S": "upload"}},
                "sk": {{"S": key}},
                "stage": {{"S": "lambda-a-processed"}},
            }},
        )
    return {{"statusCode": 200}}
"""
        fn_a_name = f"chain-a-{unique_name}"
        zip_a = make_lambda_zip(handler_a_code)
        resp_a = lambda_client.create_function(
            FunctionName=fn_a_name,
            Runtime="python3.12",
            Role=lambda_role,
            Handler="index.handler",
            Code={"ZipFile": zip_a},
            Timeout=30,
        )
        fn_a_arn = resp_a["FunctionArn"]
        chain._functions.append(fn_a_name)

        # Lambda B: DDB Stream event → publish to SNS
        handler_b_code = f"""\
import json
import boto3

def handler(event, context):
    sns = boto3.client("sns", endpoint_url="{chain.endpoint_url}")
    for record in event.get("Records", []):
        if record.get("eventName") == "INSERT":
            new_image = record.get("dynamodb", {{}}).get("NewImage", {{}})
            sk_val = new_image.get("sk", {{}}).get("S", "unknown")
            sns.publish(
                TopicArn="{topic_arn}",
                Message=json.dumps({{
                    "chain_id": "{unique_name}",
                    "file_key": sk_val,
                    "stage": "lambda-b-published",
                }}),
                Subject="ChainComplete",
            )
    return {{"statusCode": 200}}
"""
        fn_b_name = f"chain-b-{unique_name}"
        zip_b = make_lambda_zip(handler_b_code)
        lambda_client.create_function(
            FunctionName=fn_b_name,
            Runtime="python3.12",
            Role=lambda_role,
            Handler="index.handler",
            Code={"ZipFile": zip_b},
            Timeout=30,
        )
        chain._functions.append(fn_b_name)

        # Wire triggers
        chain.configure_s3_to_lambda(bucket, fn_a_arn)
        chain.create_dynamodb_stream_esm(stream_arn, fn_b_name)

        # Trigger the chain: upload to S3
        chain.s3.put_object(Bucket=bucket, Key="reports/q1-summary.csv", Body=b"revenue,100M")

        # Wait for the message to flow all the way through
        messages = wait_for_messages(sqs, queue_url, timeout=30, expected=1)
        assert len(messages) >= 1, "Full chain did not complete within 30s"

        body = json.loads(messages[0]["Body"])
        # SNS wraps the message
        if "Message" in body:
            inner = json.loads(body["Message"])
        else:
            inner = body

        assert inner["chain_id"] == unique_name
        assert inner["file_key"] == "reports/q1-summary.csv"
        assert inner["stage"] == "lambda-b-published"

    def test_eventbridge_to_sqs_esm_to_lambda_to_ddb(
        self, chain, unique_name, lambda_role, lambda_client, sqs
    ):
        """EventBridge → SQS → ESM → Lambda → DynamoDB.

        Flow:
        1. EventBridge PutEvents matches rule → SQS
        2. SQS ESM triggers Lambda
        3. Lambda writes to DynamoDB
        4. Test verifies DynamoDB entry
        """
        queue_url, queue_arn = chain.create_queue(f"eb-esm-{unique_name}")
        table_info = chain.create_table(f"eb-esm-ddb-{unique_name}")
        table_name = table_info["table_name"]

        # Lambda: SQS event → parse EB event from body → write DDB
        handler_code = f"""\
import json
import boto3

def handler(event, context):
    ddb = boto3.client("dynamodb", endpoint_url="{chain.endpoint_url}")
    for record in event.get("Records", []):
        eb_event = json.loads(record["body"])
        detail = eb_event.get("detail", {{}})
        if isinstance(detail, str):
            detail = json.loads(detail)
        ddb.put_item(
            TableName="{table_name}",
            Item={{
                "pk": {{"S": "eb-chain"}},
                "sk": {{"S": detail.get("task_id", "unknown")}},
                "source": {{"S": eb_event.get("source", "")}},
            }},
        )
    return {{"statusCode": 200}}
"""
        fn_name = f"eb-esm-handler-{unique_name}"
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

        # Wire: EB → SQS, SQS → Lambda ESM
        chain.create_eb_rule_to_sqs(
            rule_name=f"eb-esm-rule-{unique_name}",
            queue_arn=queue_arn,
            event_pattern={"source": ["myapp.tasks"]},
        )
        chain.create_sqs_esm(queue_arn, fn_name, batch_size=1)

        # Fire event
        chain.events.put_events(
            Entries=[
                {
                    "Source": "myapp.tasks",
                    "DetailType": "TaskCompleted",
                    "Detail": json.dumps({"task_id": "TASK-999", "status": "done"}),
                }
            ]
        )

        # Wait for DynamoDB entry
        deadline = time.time() + 15
        item = None
        while time.time() < deadline:
            item = chain.get_ddb_item(table_name, "eb-chain", "TASK-999")
            if item:
                break
            time.sleep(1)

        assert item is not None, "EB→SQS→Lambda→DDB chain did not complete within 15s"
        assert item["source"]["S"] == "myapp.tasks"
