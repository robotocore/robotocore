"""
Serverless API Application Tests

Tests individual components of a serverless stack: DynamoDB for storage,
Lambda for compute, IAM for permissions, API Gateway for HTTP endpoints,
and Step Functions for orchestration.
"""

import json
import uuid

import pytest

from .conftest import make_lambda_zip


@pytest.fixture
def users_table(dynamodb, unique_name):
    table_name = f"users-{unique_name}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "user_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "user_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


@pytest.fixture
def lambda_role(iam, unique_name):
    role_name = f"lambda-exec-{unique_name}"
    assume_role_policy = json.dumps(
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
    )
    resp = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=assume_role_policy,
        Description="Lambda execution role for app tests",
    )
    role_arn = resp["Role"]["Arn"]
    yield role_arn, role_name
    iam.delete_role(RoleName=role_name)


class TestServerlessApiApp:
    def test_dynamodb_crud(self, dynamodb, users_table):
        """Full CRUD lifecycle on a DynamoDB users table."""
        user_id = str(uuid.uuid4())

        # Create
        dynamodb.put_item(
            TableName=users_table,
            Item={
                "user_id": {"S": user_id},
                "email": {"S": "alice@example.com"},
                "name": {"S": "Alice Johnson"},
                "plan": {"S": "free"},
            },
        )

        # Read
        resp = dynamodb.get_item(TableName=users_table, Key={"user_id": {"S": user_id}})
        assert resp["Item"]["email"]["S"] == "alice@example.com"
        assert resp["Item"]["name"]["S"] == "Alice Johnson"

        # Update
        dynamodb.update_item(
            TableName=users_table,
            Key={"user_id": {"S": user_id}},
            UpdateExpression="SET #p = :plan",
            ExpressionAttributeNames={"#p": "plan"},
            ExpressionAttributeValues={":plan": {"S": "pro"}},
        )

        resp = dynamodb.get_item(TableName=users_table, Key={"user_id": {"S": user_id}})
        assert resp["Item"]["plan"]["S"] == "pro"

        # Query (scan since we only have hash key)
        scan_resp = dynamodb.scan(
            TableName=users_table,
            FilterExpression="email = :email",
            ExpressionAttributeValues={":email": {"S": "alice@example.com"}},
        )
        assert scan_resp["Count"] == 1

        # Delete
        dynamodb.delete_item(TableName=users_table, Key={"user_id": {"S": user_id}})
        resp = dynamodb.get_item(TableName=users_table, Key={"user_id": {"S": user_id}})
        assert "Item" not in resp

    def test_lambda_invoke(self, lambda_client, lambda_role, unique_name):
        """Create a Lambda function and invoke it."""
        role_arn, _ = lambda_role
        function_name = f"echo-handler-{unique_name}"

        handler_code = """
def handler(event, context):
    return {
        "statusCode": 200,
        "body": {
            "message": "Hello from Lambda!",
            "input": event
        }
    }
"""
        lambda_client.create_function(
            FunctionName=function_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="index.handler",
            Code={"ZipFile": make_lambda_zip(handler_code)},
            Timeout=30,
            MemorySize=128,
        )

        response = lambda_client.invoke(
            FunctionName=function_name,
            Payload=json.dumps({"user": "alice", "action": "greet"}),
        )

        payload = json.loads(response["Payload"].read())
        assert payload["statusCode"] == 200
        assert payload["body"]["message"] == "Hello from Lambda!"
        assert payload["body"]["input"]["user"] == "alice"

        lambda_client.delete_function(FunctionName=function_name)

    def test_iam_role_for_lambda(self, iam, lambda_role):
        """Verify the Lambda execution role was created with correct policy."""
        role_arn, role_name = lambda_role

        assert ":role/" in role_arn

        resp = iam.get_role(RoleName=role_name)
        role = resp["Role"]
        assert role["RoleName"] == role_name
        assert role["Arn"] == role_arn

        policy_doc = role["AssumeRolePolicyDocument"]
        # May be str or dict depending on implementation
        if isinstance(policy_doc, str):
            policy_doc = json.loads(policy_doc)
        statements = policy_doc["Statement"]
        assert any(s["Principal"].get("Service") == "lambda.amazonaws.com" for s in statements)

    def test_api_gateway_setup(self, apigateway, unique_name):
        """Create REST API with resource and method, verify structure."""
        api_resp = apigateway.create_rest_api(
            name=f"user-api-{unique_name}",
            description="User management API",
        )
        api_id = api_resp["id"]

        # Get root resource
        resources = apigateway.get_resources(restApiId=api_id)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]

        # Create /users resource
        users_resource = apigateway.create_resource(
            restApiId=api_id, parentId=root_id, pathPart="users"
        )
        users_id = users_resource["id"]
        assert users_resource["pathPart"] == "users"

        # Add GET method
        apigateway.put_method(
            restApiId=api_id,
            resourceId=users_id,
            httpMethod="GET",
            authorizationType="NONE",
        )

        # Verify structure
        resources = apigateway.get_resources(restApiId=api_id)
        paths = {r["path"] for r in resources["items"]}
        assert "/" in paths
        assert "/users" in paths

        apigateway.delete_rest_api(restApiId=api_id)

    def test_step_function_workflow(self, stepfunctions, iam, unique_name):
        """Create a Pass→Succeed state machine, start and describe execution."""
        # Create role for Step Functions
        role_name = f"sfn-role-{unique_name}"
        assume_policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "states.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        )
        role_resp = iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=assume_policy)
        role_arn = role_resp["Role"]["Arn"]

        definition = json.dumps(
            {
                "Comment": "Order processing workflow",
                "StartAt": "ValidateOrder",
                "States": {
                    "ValidateOrder": {
                        "Type": "Pass",
                        "Result": {"status": "validated"},
                        "Next": "Complete",
                    },
                    "Complete": {"Type": "Succeed"},
                },
            }
        )

        sm_name = f"order-workflow-{unique_name}"
        sm_resp = stepfunctions.create_state_machine(
            name=sm_name, definition=definition, roleArn=role_arn
        )
        sm_arn = sm_resp["stateMachineArn"]

        exec_resp = stepfunctions.start_execution(
            stateMachineArn=sm_arn,
            input=json.dumps({"order_id": "ORD-SFN-001"}),
        )
        exec_arn = exec_resp["executionArn"]

        desc = stepfunctions.describe_execution(executionArn=exec_arn)
        assert desc["stateMachineArn"] == sm_arn
        assert desc["status"] in ("RUNNING", "SUCCEEDED")

        stepfunctions.delete_state_machine(stateMachineArn=sm_arn)
        iam.delete_role(RoleName=role_name)
