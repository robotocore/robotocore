"""
Lambda handler source code as string constants.

Each constant is the Python source for a Lambda function that gets zipped
and deployed. Handlers follow the standard AWS Lambda signature:
    def handler(event, context) -> dict
"""

CRUD_HANDLER = """
import json
import os
import traceback

TABLE_NAME = os.environ.get("TABLE_NAME", "users")
ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "")

def _get_dynamodb():
    import boto3
    kwargs = {"region_name": "us-east-1"}
    if ENDPOINT_URL:
        kwargs["endpoint_url"] = ENDPOINT_URL
    return boto3.client("dynamodb", **kwargs)

def handler(event, context):
    try:
        method = event.get("httpMethod", "GET")
        path = event.get("path", "/")
        body = event.get("body")
        if isinstance(body, str):
            body = json.loads(body)
        path_params = event.get("pathParameters") or {}

        ddb = _get_dynamodb()

        if method == "POST" and path == "/users":
            user_id = body["user_id"]
            item = {
                "user_id": {"S": user_id},
                "email": {"S": body.get("email", "")},
                "name": {"S": body.get("name", "")},
                "status": {"S": "active"},
            }
            ddb.put_item(TableName=TABLE_NAME, Item=item)
            return {
                "statusCode": 201,
                "body": json.dumps({"user_id": user_id, "status": "created"}),
            }

        elif method == "GET" and "user_id" in path_params:
            user_id = path_params["user_id"]
            resp = ddb.get_item(
                TableName=TABLE_NAME, Key={"user_id": {"S": user_id}}
            )
            if "Item" not in resp:
                return {"statusCode": 404, "body": json.dumps({"error": "Not found"})}
            item = resp["Item"]
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "user_id": item["user_id"]["S"],
                    "email": item.get("email", {}).get("S", ""),
                    "name": item.get("name", {}).get("S", ""),
                    "status": item.get("status", {}).get("S", ""),
                }),
            }

        elif method == "PUT" and "user_id" in path_params:
            user_id = path_params["user_id"]
            updates = []
            names = {}
            values = {}
            for key, val in (body or {}).items():
                safe = f"#f_{key}"
                placeholder = f":v_{key}"
                updates.append(f"{safe} = {placeholder}")
                names[safe] = key
                values[placeholder] = {"S": str(val)}
            if updates:
                ddb.update_item(
                    TableName=TABLE_NAME,
                    Key={"user_id": {"S": user_id}},
                    UpdateExpression="SET " + ", ".join(updates),
                    ExpressionAttributeNames=names,
                    ExpressionAttributeValues=values,
                )
            return {
                "statusCode": 200,
                "body": json.dumps({"user_id": user_id, "status": "updated"}),
            }

        elif method == "DELETE" and "user_id" in path_params:
            user_id = path_params["user_id"]
            ddb.delete_item(
                TableName=TABLE_NAME, Key={"user_id": {"S": user_id}}
            )
            return {
                "statusCode": 200,
                "body": json.dumps({"user_id": user_id, "status": "deleted"}),
            }

        elif method == "GET" and path == "/users":
            resp = ddb.scan(TableName=TABLE_NAME)
            items = []
            for item in resp.get("Items", []):
                items.append({
                    "user_id": item["user_id"]["S"],
                    "email": item.get("email", {}).get("S", ""),
                    "name": item.get("name", {}).get("S", ""),
                })
            return {
                "statusCode": 200,
                "body": json.dumps({"users": items, "count": len(items)}),
            }

        return {"statusCode": 400, "body": json.dumps({"error": "Bad request"})}

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e), "trace": traceback.format_exc()}),
        }
"""

AUTHORIZER_HANDLER = """
import json

VALID_TOKENS = {"allow-token-123", "admin-token-456"}

def handler(event, context):
    token = event.get("authorizationToken", "")
    method_arn = event.get("methodArn", "*")

    if token in VALID_TOKENS:
        return {
            "principalId": "user",
            "policyDocument": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": "execute-api:Invoke",
                        "Effect": "Allow",
                        "Resource": method_arn,
                    }
                ],
            },
        }
    else:
        return {
            "principalId": "user",
            "policyDocument": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": "execute-api:Invoke",
                        "Effect": "Deny",
                        "Resource": method_arn,
                    }
                ],
            },
        }
"""

WORKFLOW_HANDLER = """
import json

def handler(event, context):
    step = event.get("step", "unknown")
    user_id = event.get("user_id", "")

    if step == "validate":
        is_valid = bool(user_id and "@" in event.get("email", ""))
        return {
            "step": "validate",
            "user_id": user_id,
            "is_valid": is_valid,
            "validation_result": "passed" if is_valid else "failed",
        }
    elif step == "activate":
        return {
            "step": "activate",
            "user_id": user_id,
            "status": "activated",
            "message": f"User {user_id} has been activated",
        }
    elif step == "notify":
        return {
            "step": "notify",
            "user_id": user_id,
            "notification_sent": True,
            "channel": "email",
        }
    else:
        return {
            "step": step,
            "user_id": user_id,
            "status": "processed",
        }
"""

HELLO_HANDLER = """
import json

def handler(event, context):
    name = event.get("name", "World")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "message": f"Hello, {name}!",
            "function_name": context.function_name if context else "unknown",
        }),
    }
"""
