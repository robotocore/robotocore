"""Terraform IaC test: REST API with Lambda backend via API Gateway."""

from __future__ import annotations

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.resource_validator import (
    assert_api_gateway_exists,
    assert_iam_role_exists,
    assert_lambda_function_exists,
)

pytestmark = [
    pytest.mark.iac,
    pytest.mark.terraform,
]


@pytest.fixture(scope="module")
def deployed(terraform_dir, tf_runner):
    """Apply the REST API Terraform scenario and return outputs."""
    result = tf_runner.apply(terraform_dir)
    if result.returncode != 0:
        pytest.fail(f"terraform apply failed:\n{result.stderr}\n{result.stdout}")
    return tf_runner.output(terraform_dir)


class TestRestApi:
    """Validate that Terraform-provisioned REST API resources exist."""

    def test_lambda_function_exists(self, deployed):
        """Lambda function created by Terraform is visible via the AWS API."""
        function_name = deployed["function_name"]["value"]
        client = make_client("lambda")
        config = assert_lambda_function_exists(client, function_name)
        assert config["Runtime"] == "python3.12"
        assert config["Handler"] == "handler.handler"

    def test_iam_role_exists(self, deployed):
        """IAM execution role for the Lambda is visible via the AWS API."""
        client = make_client("iam")
        assert_iam_role_exists(client, "rc-rest-lambda-role")

    def test_api_gateway_exists(self, deployed):
        """API Gateway REST API is visible via the AWS API."""
        api_id = deployed["api_id"]["value"]
        client = make_client("apigateway")
        api = assert_api_gateway_exists(client, api_id)
        assert api["name"] == "rc-rest-api"

    def test_api_gateway_has_hello_resource(self, deployed):
        """The /hello resource exists on the REST API."""
        api_id = deployed["api_id"]["value"]
        client = make_client("apigateway")
        resources = client.get_resources(restApiId=api_id)["items"]
        paths = [r["path"] for r in resources]
        assert "/hello" in paths, f"/hello not found in {paths}"

    def test_api_gateway_hello_has_get_method(self, deployed):
        """The /hello resource has a GET method configured."""
        api_id = deployed["api_id"]["value"]
        client = make_client("apigateway")
        resources = client.get_resources(restApiId=api_id)["items"]
        hello = next(r for r in resources if r["path"] == "/hello")
        method = client.get_method(
            restApiId=api_id,
            resourceId=hello["id"],
            httpMethod="GET",
        )
        assert method["httpMethod"] == "GET"
        assert method["authorizationType"] == "NONE"

    def test_invoke_url_output(self, deployed):
        """The invoke_url output is populated and ends with /hello."""
        invoke_url = deployed["invoke_url"]["value"]
        assert invoke_url.endswith("/hello"), f"invoke_url does not end with /hello: {invoke_url}"
