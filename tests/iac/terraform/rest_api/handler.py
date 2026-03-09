"""Simple Lambda handler that returns a hello response."""

import json


def handler(event, context):
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"message": "hello from robotocore"}),
    }
