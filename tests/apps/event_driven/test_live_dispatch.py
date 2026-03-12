"""Test EventBridge → Lambda live dispatch.

Verifies that PutEvents with a matching rule invokes a real Lambda target,
proving the EventBridge integration is more than just rule management.
"""

import json
import time
import uuid

from tests.apps.conftest import make_lambda_zip


class TestLiveDispatch:
    """EventBridge rule → Lambda target → DynamoDB verification."""

    def test_eventbridge_rule_invokes_lambda(self, events, lambda_client, dynamodb, iam):
        """Create rule with Lambda target, PutEvents, verify DDB write."""
        suffix = uuid.uuid4().hex[:8]
        table_name = f"ed-dispatch-{suffix}"
        fn_name = f"ed-handler-{suffix}"
        rule_name = f"ed-rule-{suffix}"

        # Create marker table
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        # Create IAM role
        role_resp = iam.create_role(
            RoleName=f"ed-role-{suffix}",
            AssumeRolePolicyDocument=json.dumps(
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
            ),
        )
        role_arn = role_resp["Role"]["Arn"]

        endpoint_url = "http://localhost:4566"
        handler_code = f"""\
import json
import boto3

def handler(event, context):
    ddb = boto3.client("dynamodb", endpoint_url="{endpoint_url}")
    detail = event.get("detail", {{}})
    if isinstance(detail, str):
        detail = json.loads(detail)
    ddb.put_item(
        TableName="{table_name}",
        Item={{
            "pk": {{"S": "eb-dispatch"}},
            "sk": {{"S": detail.get("order_id", "unknown")}},
            "source": {{"S": event.get("source", "")}},
        }},
    )
    return {{"statusCode": 200}}
"""
        zip_bytes = make_lambda_zip(handler_code)
        resp = lambda_client.create_function(
            FunctionName=fn_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="index.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=30,
        )
        fn_arn = resp["FunctionArn"]

        # Create EB rule → Lambda
        events.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({"source": ["shop.orders"]}),
            State="ENABLED",
        )
        events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "lambda-target", "Arn": fn_arn}],
        )

        # Fire event
        events.put_events(
            Entries=[
                {
                    "Source": "shop.orders",
                    "DetailType": "OrderPlaced",
                    "Detail": json.dumps({"order_id": "ORD-LIVE-001"}),
                }
            ]
        )

        # Verify DDB write
        deadline = time.time() + 10
        item = None
        while time.time() < deadline:
            resp = dynamodb.get_item(
                TableName=table_name,
                Key={"pk": {"S": "eb-dispatch"}, "sk": {"S": "ORD-LIVE-001"}},
            )
            item = resp.get("Item")
            if item:
                break
            time.sleep(0.5)

        assert item is not None, "Lambda not invoked by EventBridge rule"
        assert item["source"]["S"] == "shop.orders"

        # Cleanup
        events.remove_targets(Rule=rule_name, Ids=["lambda-target"])
        events.delete_rule(Name=rule_name)
        lambda_client.delete_function(FunctionName=fn_name)
        dynamodb.delete_table(TableName=table_name)
        iam.delete_role(RoleName=f"ed-role-{suffix}")
