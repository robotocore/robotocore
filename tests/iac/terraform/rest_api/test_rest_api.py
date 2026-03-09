"""IaC test: terraform - rest_api.

Validates API Gateway REST API and Lambda function creation.
Resources are created via boto3 (mirroring the Terraform program).
"""

from __future__ import annotations

import io
import json
import zipfile

import pytest

from tests.iac.helpers.functional_validator import invoke_api_gateway

pytestmark = pytest.mark.iac


def _make_lambda_zip() -> bytes:
    """Create a zip archive containing the Lambda handler."""
    handler_code = """\
def handler(event, context):
    return {
        "statusCode": 200,
        "body": '{"message": "hello from lambda"}'
    }
"""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", handler_code)
    return buf.getvalue()


@pytest.fixture(scope="module")
def rest_api_resources(iam_client, lambda_client, apigateway_client):
    """Create IAM role, Lambda function, and API Gateway REST API via boto3."""
    # IAM role for Lambda
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
    role_name = "tf-lambda-role"
    role = iam_client.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=assume_role_policy,
    )
    role_arn = role["Role"]["Arn"]

    # Lambda function
    fn_name = "tf-api-handler"
    lambda_client.create_function(
        FunctionName=fn_name,
        Runtime="python3.12",
        Role=role_arn,
        Handler="index.handler",
        Code={"ZipFile": _make_lambda_zip()},
    )

    # API Gateway REST API
    api = apigateway_client.create_rest_api(
        name="tf-rest-api",
        description="Terraform IaC test REST API",
    )
    api_id = api["id"]

    # Get root resource
    resources = apigateway_client.get_resources(restApiId=api_id)
    root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]

    # /hello resource
    hello = apigateway_client.create_resource(restApiId=api_id, parentId=root_id, pathPart="hello")
    hello_id = hello["id"]

    # GET method
    apigateway_client.put_method(
        restApiId=api_id,
        resourceId=hello_id,
        httpMethod="GET",
        authorizationType="NONE",
    )

    # Lambda integration
    apigateway_client.put_integration(
        restApiId=api_id,
        resourceId=hello_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=(
            f"arn:aws:apigateway:us-east-1:lambda:path"
            f"/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012"
            f":function:{fn_name}/invocations"
        ),
    )

    yield {
        "rest_api_id": api_id,
        "lambda_function_name": fn_name,
        "role_name": role_name,
    }

    # Cleanup
    apigateway_client.delete_rest_api(restApiId=api_id)
    lambda_client.delete_function(FunctionName=fn_name)
    iam_client.delete_role(RoleName=role_name)


class TestRestApi:
    """Validate REST API resources created by Terraform."""

    def test_api_created(self, rest_api_resources, apigateway_client):
        """Verify the REST API exists and has the correct name."""
        api_id = rest_api_resources["rest_api_id"]
        resp = apigateway_client.get_rest_api(restApiId=api_id)
        assert resp["id"] == api_id
        assert resp["name"] == "tf-rest-api"

    def test_lambda_created(self, rest_api_resources, lambda_client):
        """Verify the Lambda function exists."""
        fn_name = rest_api_resources["lambda_function_name"]
        resp = lambda_client.get_function(FunctionName=fn_name)
        config = resp["Configuration"]
        assert config["FunctionName"] == fn_name
        assert config["Runtime"] == "python3.12"
        assert config["Handler"] == "index.handler"

    def test_invoke_api_endpoint(self, rest_api_resources, apigateway_client):
        """Create a deployment+stage and invoke the API Gateway endpoint."""
        api_id = rest_api_resources["rest_api_id"]
        deployment = apigateway_client.create_deployment(restApiId=api_id)
        apigateway_client.create_stage(
            restApiId=api_id,
            stageName="prod",
            deploymentId=deployment["id"],
        )
        resp = invoke_api_gateway(api_id, "prod", "hello")
        assert resp["status"] == 200
