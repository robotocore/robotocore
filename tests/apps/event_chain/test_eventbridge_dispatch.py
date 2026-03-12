"""Tests for EventBridge rule dispatch.

Verifies that PutEvents → matching rules → targets (SQS, Lambda).
"""

import json
import time

from tests.apps.conftest import make_lambda_zip, wait_for_messages


class TestEventBridgeToSqs:
    """EventBridge rule → SQS target."""

    def test_put_events_dispatches_to_sqs(self, chain, unique_name, sqs):
        """PutEvents with matching pattern → SQS receives the event."""
        queue_url, queue_arn = chain.create_queue(f"eb-sqs-{unique_name}")

        chain.create_eb_rule_to_sqs(
            rule_name=f"rule-sqs-{unique_name}",
            queue_arn=queue_arn,
            event_pattern={"source": ["myapp.orders"]},
        )

        chain.events.put_events(
            Entries=[
                {
                    "Source": "myapp.orders",
                    "DetailType": "OrderCreated",
                    "Detail": json.dumps({"order_id": "ORD-001", "amount": 99.99}),
                }
            ]
        )

        messages = wait_for_messages(sqs, queue_url, timeout=10, expected=1)
        assert len(messages) >= 1, "SQS did not receive EventBridge event"

        body = json.loads(messages[0]["Body"])
        assert body.get("source") == "myapp.orders"
        assert body.get("detail-type") == "OrderCreated"
        detail = body.get("detail", {})
        if isinstance(detail, str):
            detail = json.loads(detail)
        assert detail["order_id"] == "ORD-001"

    def test_pattern_filter_only_matching(self, chain, unique_name, sqs):
        """Only events matching the pattern are dispatched."""
        queue_url, queue_arn = chain.create_queue(f"eb-filter-{unique_name}")

        chain.create_eb_rule_to_sqs(
            rule_name=f"rule-filter-{unique_name}",
            queue_arn=queue_arn,
            event_pattern={
                "source": ["myapp.orders"],
                "detail-type": ["OrderCreated"],
            },
        )

        # Non-matching event (different source)
        chain.events.put_events(
            Entries=[
                {
                    "Source": "myapp.inventory",
                    "DetailType": "StockUpdated",
                    "Detail": json.dumps({"sku": "ABC"}),
                }
            ]
        )

        # Matching event
        chain.events.put_events(
            Entries=[
                {
                    "Source": "myapp.orders",
                    "DetailType": "OrderCreated",
                    "Detail": json.dumps({"order_id": "ORD-002"}),
                }
            ]
        )

        messages = wait_for_messages(sqs, queue_url, timeout=10, expected=1)
        assert len(messages) == 1, f"Expected 1 message, got {len(messages)}"

        body = json.loads(messages[0]["Body"])
        detail = body.get("detail", {})
        if isinstance(detail, str):
            detail = json.loads(detail)
        assert detail["order_id"] == "ORD-002"


class TestEventBridgeToLambda:
    """EventBridge rule → Lambda target → DynamoDB verification."""

    def test_put_events_invokes_lambda(self, chain, unique_name, lambda_role, lambda_client):
        """PutEvents → Lambda fires → writes to DynamoDB."""
        table_info = chain.create_table(f"eb-lambda-{unique_name}")
        table_name = table_info["table_name"]

        handler_code = f"""\
import json
import boto3

def handler(event, context):
    ddb = boto3.client("dynamodb", endpoint_url="{chain.endpoint_url}")
    detail = event.get("detail", {{}})
    if isinstance(detail, str):
        detail = json.loads(detail)
    ddb.put_item(
        TableName="{table_name}",
        Item={{
            "pk": {{"S": "eb-event"}},
            "sk": {{"S": detail.get("event_id", "unknown")}},
            "source": {{"S": event.get("source", "")}},
        }},
    )
    return {{"statusCode": 200}}
"""
        fn_name = f"eb-handler-{unique_name}"
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

        chain.create_eb_rule_to_lambda(
            rule_name=f"rule-lambda-{unique_name}",
            function_arn=fn_arn,
            event_pattern={"source": ["myapp.test"]},
        )

        chain.events.put_events(
            Entries=[
                {
                    "Source": "myapp.test",
                    "DetailType": "TestEvent",
                    "Detail": json.dumps({"event_id": "EVT-001"}),
                }
            ]
        )

        deadline = time.time() + 10
        item = None
        while time.time() < deadline:
            item = chain.get_ddb_item(table_name, "eb-event", "EVT-001")
            if item:
                break
            time.sleep(0.5)

        assert item is not None, "Lambda did not write to DynamoDB from EB event"
        assert item["source"]["S"] == "myapp.test"
