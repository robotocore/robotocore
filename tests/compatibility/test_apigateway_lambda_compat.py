"""API Gateway + Lambda integration tests — Enterprise-grade feature."""

import io
import json
import time
import uuid
import zipfile

import pytest
import requests

from tests.compatibility.conftest import ENDPOINT_URL, make_client


def _request_with_retry(method, url, retries=3, delay=0.5, **kwargs):
    """Make an HTTP request with retries for intermittent 404s from execute-api."""
    for attempt in range(retries):
        resp = method(url, **kwargs)
        if resp.status_code != 404 or attempt == retries - 1:
            return resp
        time.sleep(delay)
    return resp


def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


@pytest.fixture
def apigw():
    return make_client("apigateway")


@pytest.fixture
def lam():
    return make_client("lambda")


@pytest.fixture
def iam():
    return make_client("iam")


@pytest.fixture
def role(iam):
    name = f"apigw-role-{uuid.uuid4().hex[:8]}"
    trust = json.dumps(
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
    resp = iam.create_role(RoleName=name, AssumeRolePolicyDocument=trust)
    yield resp["Role"]["Arn"]
    try:
        iam.delete_role(RoleName=name)
    except Exception:
        pass  # best-effort cleanup


class TestAPIGatewayLambdaProxy:
    def test_create_rest_api(self, apigw):
        """Test creating a REST API."""
        resp = apigw.create_rest_api(name="test-api", description="Test API")
        assert "id" in resp
        apigw.delete_rest_api(restApiId=resp["id"])

    def test_create_resource_and_method(self, apigw):
        """Test creating a resource and method."""
        api = apigw.create_rest_api(name="resource-api")
        api_id = api["id"]

        # Get root resource
        resources = apigw.get_resources(restApiId=api_id)
        root_id = resources["items"][0]["id"]

        # Create resource
        resource = apigw.create_resource(
            restApiId=api_id,
            parentId=root_id,
            pathPart="hello",
        )
        assert resource["pathPart"] == "hello"

        # Create method
        apigw.put_method(
            restApiId=api_id,
            resourceId=resource["id"],
            httpMethod="GET",
            authorizationType="NONE",
        )

        apigw.delete_rest_api(restApiId=api_id)

    def test_lambda_proxy_integration(self, apigw, lam, role):
        """Test full API Gateway → Lambda proxy integration.

        This is the core Enterprise feature: an HTTP request to API Gateway
        invokes a Lambda function and returns the response.
        """
        suffix = uuid.uuid4().hex[:8]
        func_name = f"apigw-handler-{suffix}"
        api_id = None

        try:
            code = _make_zip(
                "import json\n"
                "def handler(event, ctx):\n"
                '    qsp = event.get("queryStringParameters")\n'
                '    name = qsp.get("name", "world") if qsp else "world"\n'
                "    return {\n"
                '        "statusCode": 200,\n'
                '        "headers": {"Content-Type": "application/json"},\n'
                '        "body": json.dumps({"message": f"Hello, {name}!"})\n'
                "    }\n"
            )
            lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role=role,
                Handler="lambda_function.handler",
                Code={"ZipFile": code},
            )

            api = apigw.create_rest_api(name=f"proxy-api-{suffix}")
            api_id = api["id"]

            resources = apigw.get_resources(restApiId=api_id)
            root_id = resources["items"][0]["id"]

            resource = apigw.create_resource(
                restApiId=api_id,
                parentId=root_id,
                pathPart="{proxy+}",
            )

            apigw.put_method(
                restApiId=api_id,
                resourceId=resource["id"],
                httpMethod="ANY",
                authorizationType="NONE",
            )

            lambda_uri = (
                "arn:aws:apigateway:us-east-1:lambda:path"
                "/2015-03-31/functions/arn:aws:lambda:us-east-1"
                f":123456789012:function:{func_name}/invocations"
            )
            apigw.put_integration(
                restApiId=api_id,
                resourceId=resource["id"],
                httpMethod="ANY",
                type="AWS_PROXY",
                integrationHttpMethod="POST",
                uri=lambda_uri,
            )

            apigw.create_deployment(restApiId=api_id, stageName="test")

            url = f"{ENDPOINT_URL}/restapis/{api_id}/test/_user_request_/hello?name=Jack"
            resp = _request_with_retry(requests.get, url)
            assert resp.status_code == 200
            body = resp.json()
            assert body["message"] == "Hello, Jack!"
        finally:
            for fn in [
                lambda: lam.delete_function(FunctionName=func_name),
                lambda: apigw.delete_rest_api(restApiId=api_id) if api_id else None,
            ]:
                try:
                    fn()
                except Exception:
                    pass  # best-effort cleanup

    def test_lambda_proxy_post(self, apigw, lam, role):
        """Test POST request through API Gateway → Lambda."""
        suffix = uuid.uuid4().hex[:8]
        func_name = f"apigw-post-{suffix}"
        api_id = None

        try:
            code = _make_zip(
                "import json\n"
                "def handler(event, ctx):\n"
                '    body = json.loads(event.get("body", "{}")) if event.get("body") else {}\n'
                "    return {\n"
                '        "statusCode": 201,\n'
                '        "body": json.dumps({"received": body, "method": event["httpMethod"]})\n'
                "    }\n"
            )
            lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role=role,
                Handler="lambda_function.handler",
                Code={"ZipFile": code},
            )

            api = apigw.create_rest_api(name=f"post-api-{suffix}")
            api_id = api["id"]
            resources = apigw.get_resources(restApiId=api_id)
            root_id = resources["items"][0]["id"]

            resource = apigw.create_resource(
                restApiId=api_id, parentId=root_id, pathPart="{proxy+}"
            )
            apigw.put_method(
                restApiId=api_id,
                resourceId=resource["id"],
                httpMethod="ANY",
                authorizationType="NONE",
            )

            lambda_uri = (
                "arn:aws:apigateway:us-east-1:lambda:path"
                "/2015-03-31/functions/arn:aws:lambda:us-east-1"
                f":123456789012:function:{func_name}/invocations"
            )
            apigw.put_integration(
                restApiId=api_id,
                resourceId=resource["id"],
                httpMethod="ANY",
                type="AWS_PROXY",
                integrationHttpMethod="POST",
                uri=lambda_uri,
            )
            apigw.create_deployment(restApiId=api_id, stageName="test")

            url = f"{ENDPOINT_URL}/restapis/{api_id}/test/_user_request_/items"
            resp = _request_with_retry(requests.post, url, json={"item": "test-item"})
            assert resp.status_code == 201
            body = resp.json()
            assert body["method"] == "POST"
            assert body["received"]["item"] == "test-item"
        finally:
            for fn in [
                lambda: lam.delete_function(FunctionName=func_name),
                lambda: apigw.delete_rest_api(restApiId=api_id) if api_id else None,
            ]:
                try:
                    fn()
                except Exception:
                    pass  # best-effort cleanup

    def test_lambda_proxy_path_params(self, apigw, lam, role):
        """Test that path parameters like /users/{userId} are extracted correctly."""
        suffix = uuid.uuid4().hex[:8]
        func_name = f"apigw-path-{suffix}"
        api_id = None

        try:
            code = _make_zip(
                "import json\n"
                "def handler(event, ctx):\n"
                '    path_params = event.get("pathParameters") or {}\n'
                '    user_id = path_params.get("userId", "unknown")\n'
                "    return {\n"
                '        "statusCode": 200,\n'
                '        "headers": {"Content-Type": "application/json"},\n'
                '        "body": json.dumps({"userId": user_id, "path": event.get("path", "")})\n'
                "    }\n"
            )
            lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role=role,
                Handler="lambda_function.handler",
                Code={"ZipFile": code},
            )

            api = apigw.create_rest_api(name=f"path-api-{suffix}")
            api_id = api["id"]
            resources = apigw.get_resources(restApiId=api_id)
            root_id = resources["items"][0]["id"]

            users_resource = apigw.create_resource(
                restApiId=api_id, parentId=root_id, pathPart="users"
            )
            user_id_resource = apigw.create_resource(
                restApiId=api_id, parentId=users_resource["id"], pathPart="{userId}"
            )

            apigw.put_method(
                restApiId=api_id,
                resourceId=user_id_resource["id"],
                httpMethod="GET",
                authorizationType="NONE",
            )

            lambda_uri = (
                "arn:aws:apigateway:us-east-1:lambda:path"
                "/2015-03-31/functions/arn:aws:lambda:us-east-1"
                f":123456789012:function:{func_name}/invocations"
            )
            apigw.put_integration(
                restApiId=api_id,
                resourceId=user_id_resource["id"],
                httpMethod="GET",
                type="AWS_PROXY",
                integrationHttpMethod="POST",
                uri=lambda_uri,
            )

            apigw.create_deployment(restApiId=api_id, stageName="test")

            url = f"{ENDPOINT_URL}/restapis/{api_id}/test/_user_request_/users/abc123"
            resp = _request_with_retry(requests.get, url)
            assert resp.status_code == 200
            body = resp.json()
            assert body["userId"] == "abc123"
        finally:
            for fn in [
                lambda: lam.delete_function(FunctionName=func_name),
                lambda: apigw.delete_rest_api(restApiId=api_id) if api_id else None,
            ]:
                try:
                    fn()
                except Exception:
                    pass  # best-effort cleanup

    def test_lambda_proxy_multiple_query_params(self, apigw, lam, role):
        """Test that multiple query string parameters are passed correctly."""
        suffix = uuid.uuid4().hex[:8]
        func_name = f"apigw-query-{suffix}"
        api_id = None

        try:
            code = _make_zip(
                "import json\n"
                "def handler(event, ctx):\n"
                '    params = event.get("queryStringParameters") or {}\n'
                '    multi = event.get("multiValueQueryStringParameters") or {}\n'
                "    return {\n"
                '        "statusCode": 200,\n'
                '        "headers": {"Content-Type": "application/json"},\n'
                '        "body": json.dumps({"params": params, "multi": multi})\n'
                "    }\n"
            )
            lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role=role,
                Handler="lambda_function.handler",
                Code={"ZipFile": code},
            )

            api = apigw.create_rest_api(name=f"query-api-{suffix}")
            api_id = api["id"]
            resources = apigw.get_resources(restApiId=api_id)
            root_id = resources["items"][0]["id"]

            resource = apigw.create_resource(
                restApiId=api_id, parentId=root_id, pathPart="{proxy+}"
            )
            apigw.put_method(
                restApiId=api_id,
                resourceId=resource["id"],
                httpMethod="ANY",
                authorizationType="NONE",
            )

            lambda_uri = (
                "arn:aws:apigateway:us-east-1:lambda:path"
                "/2015-03-31/functions/arn:aws:lambda:us-east-1"
                f":123456789012:function:{func_name}/invocations"
            )
            apigw.put_integration(
                restApiId=api_id,
                resourceId=resource["id"],
                httpMethod="ANY",
                type="AWS_PROXY",
                integrationHttpMethod="POST",
                uri=lambda_uri,
            )

            apigw.create_deployment(restApiId=api_id, stageName="test")

            url = (
                f"{ENDPOINT_URL}/restapis/{api_id}/test"
                "/_user_request_/search"
                "?category=books&sort=price&sort=date"
            )
            resp = _request_with_retry(requests.get, url)
            assert resp.status_code == 200
            body = resp.json()
            assert body["params"]["category"] == "books"
            assert "sort" in body["params"]
        finally:
            for fn in [
                lambda: lam.delete_function(FunctionName=func_name),
                lambda: apigw.delete_rest_api(restApiId=api_id) if api_id else None,
            ]:
                try:
                    fn()
                except Exception:
                    pass  # best-effort cleanup
