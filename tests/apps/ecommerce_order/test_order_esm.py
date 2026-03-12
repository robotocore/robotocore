"""Test SQS → Lambda ESM trigger for ecommerce order processing.

Verifies that sending an order message to SQS triggers Lambda
processing via an event source mapping.
"""

import json
import time
import uuid

from tests.apps.conftest import make_lambda_zip


class TestOrderEsm:
    """SQS → Lambda ESM for order processing."""

    def test_order_message_triggers_lambda_processing(self, sqs, lambda_client, dynamodb, iam):
        """Send order to queue → Lambda processes → writes to DDB."""
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"ec-orders-{suffix}"
        table_name = f"ec-processed-{suffix}"
        fn_name = f"ec-processor-{suffix}"

        # Create SQS queue
        queue_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = queue_resp["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        # Create DynamoDB table for processed orders
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
            RoleName=f"ec-role-{suffix}",
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
    for record in event.get("Records", []):
        order = json.loads(record["body"])
        ddb.put_item(
            TableName="{table_name}",
            Item={{
                "pk": {{"S": "order"}},
                "sk": {{"S": order["order_id"]}},
                "status": {{"S": "processed"}},
                "total": {{"N": str(order["total"])}},
            }},
        )
    return {{"statusCode": 200}}
"""
        zip_bytes = make_lambda_zip(handler_code)
        lambda_client.create_function(
            FunctionName=fn_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="index.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=30,
        )

        # Create ESM
        esm_resp = lambda_client.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=fn_name,
            BatchSize=1,
            Enabled=True,
        )
        esm_uuid = esm_resp["UUID"]

        # Send order message
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(
                {
                    "order_id": "ORD-EC-001",
                    "customer": "jane@example.com",
                    "total": 149.99,
                    "items": [{"sku": "WIDGET-A", "qty": 2}],
                }
            ),
        )

        # Wait for processing
        deadline = time.time() + 15
        item = None
        while time.time() < deadline:
            resp = dynamodb.get_item(
                TableName=table_name,
                Key={"pk": {"S": "order"}, "sk": {"S": "ORD-EC-001"}},
            )
            item = resp.get("Item")
            if item:
                break
            time.sleep(1)

        assert item is not None, "Lambda did not process order from SQS within 15s"
        assert item["status"]["S"] == "processed"
        assert float(item["total"]["N"]) == 149.99

        # Cleanup
        lambda_client.delete_event_source_mapping(UUID=esm_uuid)
        lambda_client.delete_function(FunctionName=fn_name)
        sqs.delete_queue(QueueUrl=queue_url)
        dynamodb.delete_table(TableName=table_name)
        iam.delete_role(RoleName=f"ec-role-{suffix}")
