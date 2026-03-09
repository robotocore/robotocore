"""IaC test: CloudFormation REST API with Lambda + API Gateway."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.resource_validator import (
    assert_api_gateway_exists,
    assert_iam_role_exists,
    assert_lambda_function_exists,
)

pytestmark = pytest.mark.iac

TEMPLATE = (Path(__file__).parent / "template.yaml").read_text()


class TestRestApi:
    """Deploy a REST API stack and validate all resources."""

    def test_deploy_and_validate(self, deploy_stack):
        stack = deploy_stack("rest-api", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        # Extract outputs into a dict for easy lookup
        outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}

        lambda_client = make_client("lambda")
        apigateway_client = make_client("apigateway")
        iam_client = make_client("iam")

        # Validate Lambda function
        function_name = outputs["LambdaFunctionName"]
        config = assert_lambda_function_exists(lambda_client, function_name)
        assert config["Runtime"] == "python3.12"
        assert config["Handler"] == "index.handler"

        # Validate IAM role
        role_name = outputs["RoleName"]
        role = assert_iam_role_exists(iam_client, role_name)
        assert "lambda.amazonaws.com" in str(role["AssumeRolePolicyDocument"])

        # Validate API Gateway REST API
        api_id = outputs["RestApiId"]
        api = assert_api_gateway_exists(apigateway_client, api_id)
        assert "api" in api["name"].lower()

        # Validate /hello resource exists
        resources_resp = apigateway_client.get_resources(restApiId=api_id)
        paths = [r["pathPart"] for r in resources_resp["items"] if r.get("pathPart")]
        assert "hello" in paths, f"Expected 'hello' resource, found: {paths}"

        # Validate GET method on /hello
        hello_resource = next(r for r in resources_resp["items"] if r.get("pathPart") == "hello")
        method_resp = apigateway_client.get_method(
            restApiId=api_id,
            resourceId=hello_resource["id"],
            httpMethod="GET",
        )
        assert method_resp["httpMethod"] == "GET"
        assert method_resp["authorizationType"] == "NONE"
