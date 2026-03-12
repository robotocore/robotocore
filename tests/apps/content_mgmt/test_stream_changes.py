"""Test DynamoDB Streams → Lambda trigger for content management.

Verifies that updating content in DynamoDB fires a stream event
that triggers a Lambda to log the change to a tracker table.
"""

import json
import time
import uuid

from tests.apps.conftest import make_lambda_zip


class TestStreamChanges:
    """DynamoDB Streams → Lambda change tracking."""

    def test_content_update_triggers_change_tracker(self, dynamodb, lambda_client, iam):
        """Update content → Lambda logs change to tracker table."""
        suffix = uuid.uuid4().hex[:8]
        content_table = f"cm-content-{suffix}"
        tracker_table = f"cm-tracker-{suffix}"
        fn_name = f"cm-tracker-fn-{suffix}"

        # Create content table with streams
        resp = dynamodb.create_table(
            TableName=content_table,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        stream_arn = resp["TableDescription"]["LatestStreamArn"]

        # Create tracker table
        dynamodb.create_table(
            TableName=tracker_table,
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
            RoleName=f"cm-role-{suffix}",
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
        event_name = record.get("eventName", "")
        new_image = record.get("dynamodb", {{}}).get("NewImage", {{}})
        pk_val = new_image.get("pk", {{}}).get("S", "unknown")
        ddb.put_item(
            TableName="{tracker_table}",
            Item={{
                "pk": {{"S": "change-log"}},
                "sk": {{"S": f"{{event_name}}-{{pk_val}}"}},
                "action": {{"S": event_name}},
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
            EventSourceArn=stream_arn,
            FunctionName=fn_name,
            StartingPosition="LATEST",
            BatchSize=1,
            Enabled=True,
        )
        esm_uuid = esm_resp["UUID"]

        # Write content
        dynamodb.put_item(
            TableName=content_table,
            Item={
                "pk": {"S": "article-001"},
                "sk": {"S": "v1"},
                "title": {"S": "Getting Started"},
            },
        )

        # Wait for change tracker
        deadline = time.time() + 15
        item = None
        while time.time() < deadline:
            resp = dynamodb.get_item(
                TableName=tracker_table,
                Key={
                    "pk": {"S": "change-log"},
                    "sk": {"S": "INSERT-article-001"},
                },
            )
            item = resp.get("Item")
            if item:
                break
            time.sleep(1)

        assert item is not None, "Stream Lambda did not track content change"
        assert item["action"]["S"] == "INSERT"

        # Cleanup
        lambda_client.delete_event_source_mapping(UUID=esm_uuid)
        lambda_client.delete_function(FunctionName=fn_name)
        dynamodb.delete_table(TableName=content_table)
        dynamodb.delete_table(TableName=tracker_table)
        iam.delete_role(RoleName=f"cm-role-{suffix}")
