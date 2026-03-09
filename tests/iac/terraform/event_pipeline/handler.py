"""Lambda handler that processes SQS events and writes to DynamoDB."""

import json
import os

import boto3


def handler(event, context):
    table_name = os.environ.get("TABLE_NAME", "events")
    dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))
    table = dynamodb.Table(table_name)

    for record in event.get("Records", []):
        body = json.loads(record["body"])
        table.put_item(Item={"message_id": record["messageId"], "payload": body})

    return {"statusCode": 200, "processed": len(event.get("Records", []))}
