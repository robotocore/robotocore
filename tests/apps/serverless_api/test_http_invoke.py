"""Test API Gateway → Lambda HTTP invocation.

Verifies that an HTTP request to a REST API endpoint invokes a Lambda
function via AWS_PROXY integration and returns the Lambda response.
"""

import json
import uuid

import requests

from tests.apps.conftest import ENDPOINT_URL, make_lambda_zip


class TestHttpInvoke:
    """API Gateway → Lambda proxy integration."""

    def test_api_gateway_invokes_lambda(self, apigateway, lambda_client, iam):
        """HTTP request to restapi URL → Lambda response."""
        suffix = uuid.uuid4().hex[:8]
        fn_name = f"apigw-fn-{suffix}"
        api_name = f"apigw-test-{suffix}"

        # Create IAM role
        role_resp = iam.create_role(
            RoleName=f"apigw-role-{suffix}",
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

        # Create Lambda function
        handler_code = """\
import json

def handler(event, context):
    name = event.get("queryStringParameters", {}).get("name", "World")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"message": f"Hello, {name}!"}),
    }
"""
        zip_bytes = make_lambda_zip(handler_code)
        fn_resp = lambda_client.create_function(
            FunctionName=fn_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="index.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=30,
        )
        fn_arn = fn_resp["FunctionArn"]

        # Create REST API
        api_resp = apigateway.create_rest_api(name=api_name, description="Test API")
        api_id = api_resp["id"]

        # Get root resource
        resources = apigateway.get_resources(restApiId=api_id)
        root_id = resources["items"][0]["id"]

        # Create /hello resource
        resource_resp = apigateway.create_resource(
            restApiId=api_id, parentId=root_id, pathPart="hello"
        )
        resource_id = resource_resp["id"]

        # Create GET method
        apigateway.put_method(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="GET",
            authorizationType="NONE",
        )

        # Wire Lambda integration
        region = "us-east-1"
        uri = f"arn:aws:apigateway:{region}:lambda:path/2015-03-31/functions/{fn_arn}/invocations"
        apigateway.put_integration(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="GET",
            type="AWS_PROXY",
            integrationHttpMethod="POST",
            uri=uri,
        )

        # Deploy
        apigateway.create_deployment(restApiId=api_id, stageName="test")

        # Make HTTP request
        url = f"{ENDPOINT_URL}/restapis/{api_id}/test/_user_request_/hello?name=Robotocore"
        resp = requests.get(url, timeout=10)

        assert resp.status_code == 200
        body = resp.json()
        assert body["message"] == "Hello, Robotocore!"

        # Cleanup
        apigateway.delete_rest_api(restApiId=api_id)
        lambda_client.delete_function(FunctionName=fn_name)
        iam.delete_role(RoleName=f"apigw-role-{suffix}")
