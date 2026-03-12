"""Lambda Function URL compatibility tests — create, invoke via HTTP, verify response."""

import io
import json
import zipfile

import pytest
import requests

from tests.compatibility.conftest import ENDPOINT_URL, make_client


def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


@pytest.fixture
def lam():
    return make_client("lambda")


@pytest.fixture
def iam():
    return make_client("iam")


@pytest.fixture
def role(iam):
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
    role_name = "lambda-furl-test-role"
    iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=trust)
    yield f"arn:aws:iam::123456789012:role/{role_name}"
    iam.delete_role(RoleName=role_name)


def _create_echo_function(lam, role, func_name: str) -> None:
    """Create a Lambda that echoes event details back as a structured response."""
    code = _make_zip(
        """import json

def handler(event, ctx):
    body = {
        "method": event.get("requestContext", {}).get("http", {}).get("method", ""),
        "path": event.get("rawPath", ""),
        "queryStringParameters": event.get("queryStringParameters"),
        "headers": event.get("headers", {}),
        "body": event.get("body"),
        "isBase64Encoded": event.get("isBase64Encoded", False),
        "rawQueryString": event.get("rawQueryString", ""),
        "version": event.get("version", ""),
    }
    return {
        "statusCode": 200,
        "headers": {"content-type": "application/json", "x-custom-echo": "true"},
        "body": json.dumps(body),
    }
"""
    )
    lam.create_function(
        FunctionName=func_name,
        Runtime="python3.12",
        Role=role,
        Handler="lambda_function.handler",
        Code={"ZipFile": code},
    )


def _get_url_id(lam, func_name: str) -> str:
    """Create a function URL config and return the url-id."""
    resp = lam.create_function_url_config(
        FunctionName=func_name,
        AuthType="NONE",
    )
    # FunctionUrl looks like https://{url-id}.lambda-url.{region}.on.aws/
    return resp["FunctionUrl"].split("//")[1].split(".")[0]


class TestFunctionUrlInvocation:
    """Test invoking Lambda functions via function URLs."""

    def test_get_request_returns_response(self, lam, role):
        """Create function URL, invoke via HTTP GET, verify response."""
        func_name = "furl-get-test"
        _create_echo_function(lam, role, func_name)
        try:
            url_id = _get_url_id(lam, func_name)
            resp = requests.get(f"{ENDPOINT_URL}/lambda-url/{url_id}/")
            assert resp.status_code == 200
            body = resp.json()
            assert body["method"] == "GET"
            assert body["path"] == "/"
            assert body["version"] == "2.0"
            # Cleanup URL config
            lam.delete_function_url_config(FunctionName=func_name)
        finally:
            lam.delete_function(FunctionName=func_name)

    def test_post_with_json_body(self, lam, role):
        """POST with JSON body, verify event.body contains it."""
        func_name = "furl-post-test"
        _create_echo_function(lam, role, func_name)
        try:
            url_id = _get_url_id(lam, func_name)
            payload = {"key": "value", "number": 42}
            resp = requests.post(
                f"{ENDPOINT_URL}/lambda-url/{url_id}/",
                json=payload,
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["method"] == "POST"
            # The body should be the JSON-encoded payload string
            received_body = json.loads(body["body"])
            assert received_body["key"] == "value"
            assert received_body["number"] == 42
            lam.delete_function_url_config(FunctionName=func_name)
        finally:
            lam.delete_function(FunctionName=func_name)

    def test_get_with_query_params(self, lam, role):
        """GET with query params, verify event.queryStringParameters."""
        func_name = "furl-query-test"
        _create_echo_function(lam, role, func_name)
        try:
            url_id = _get_url_id(lam, func_name)
            resp = requests.get(
                f"{ENDPOINT_URL}/lambda-url/{url_id}/",
                params={"foo": "bar", "baz": "123"},
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["queryStringParameters"]["foo"] == "bar"
            assert body["queryStringParameters"]["baz"] == "123"
            assert "foo=" in body["rawQueryString"]
            lam.delete_function_url_config(FunctionName=func_name)
        finally:
            lam.delete_function(FunctionName=func_name)

    def test_custom_headers_forwarded(self, lam, role):
        """Custom headers are forwarded in the event."""
        func_name = "furl-headers-test"
        _create_echo_function(lam, role, func_name)
        try:
            url_id = _get_url_id(lam, func_name)
            resp = requests.get(
                f"{ENDPOINT_URL}/lambda-url/{url_id}/",
                headers={"X-Custom-Header": "test-value", "X-Another": "abc"},
            )
            assert resp.status_code == 200
            body = resp.json()
            # Headers are lowercased in the event
            assert body["headers"]["x-custom-header"] == "test-value"
            assert body["headers"]["x-another"] == "abc"
            lam.delete_function_url_config(FunctionName=func_name)
        finally:
            lam.delete_function(FunctionName=func_name)

    def test_response_headers_from_lambda(self, lam, role):
        """Lambda's response headers are returned in the HTTP response."""
        func_name = "furl-resp-headers-test"
        _create_echo_function(lam, role, func_name)
        try:
            url_id = _get_url_id(lam, func_name)
            resp = requests.get(f"{ENDPOINT_URL}/lambda-url/{url_id}/")
            assert resp.status_code == 200
            assert resp.headers.get("x-custom-echo") == "true"
            lam.delete_function_url_config(FunctionName=func_name)
        finally:
            lam.delete_function(FunctionName=func_name)

    def test_path_forwarded(self, lam, role):
        """Sub-paths after the url-id are forwarded."""
        func_name = "furl-path-test"
        _create_echo_function(lam, role, func_name)
        try:
            url_id = _get_url_id(lam, func_name)
            resp = requests.get(f"{ENDPOINT_URL}/lambda-url/{url_id}/api/items/123")
            assert resp.status_code == 200
            body = resp.json()
            assert body["path"] == "/api/items/123"
            lam.delete_function_url_config(FunctionName=func_name)
        finally:
            lam.delete_function(FunctionName=func_name)

    def test_nonexistent_url_returns_404(self):
        """Request to a non-existent function URL returns 404."""
        resp = requests.get(f"{ENDPOINT_URL}/lambda-url/nonexistent123/")
        assert resp.status_code == 404


class TestFunctionUrlCORS:
    """Test CORS header handling on function URLs."""

    def test_cors_headers_returned(self, lam, role):
        """CORS config on the function URL is reflected in response headers."""
        func_name = "furl-cors-test"
        _create_echo_function(lam, role, func_name)
        try:
            # Create function URL with CORS config
            resp = lam.create_function_url_config(
                FunctionName=func_name,
                AuthType="NONE",
                Cors={
                    "AllowOrigins": ["https://example.com"],
                    "AllowMethods": ["GET", "POST"],
                    "AllowHeaders": ["X-Custom-Header"],
                    "MaxAge": 3600,
                },
            )
            url_id = resp["FunctionUrl"].split("//")[1].split(".")[0]

            resp = requests.get(
                f"{ENDPOINT_URL}/lambda-url/{url_id}/",
                headers={"Origin": "https://example.com"},
            )
            assert resp.status_code == 200
            assert resp.headers.get("access-control-allow-origin") == "https://example.com"
            assert "GET" in resp.headers.get("access-control-allow-methods", "")
            assert "POST" in resp.headers.get("access-control-allow-methods", "")
            assert resp.headers.get("access-control-max-age") == "3600"
            lam.delete_function_url_config(FunctionName=func_name)
        finally:
            lam.delete_function(FunctionName=func_name)

    def test_cors_wildcard_origin(self, lam, role):
        """Wildcard origin returns * in Access-Control-Allow-Origin."""
        func_name = "furl-cors-wildcard"
        _create_echo_function(lam, role, func_name)
        try:
            resp = lam.create_function_url_config(
                FunctionName=func_name,
                AuthType="NONE",
                Cors={"AllowOrigins": ["*"]},
            )
            url_id = resp["FunctionUrl"].split("//")[1].split(".")[0]

            resp = requests.get(
                f"{ENDPOINT_URL}/lambda-url/{url_id}/",
                headers={"Origin": "https://anything.com"},
            )
            assert resp.status_code == 200
            assert resp.headers.get("access-control-allow-origin") == "*"
            lam.delete_function_url_config(FunctionName=func_name)
        finally:
            lam.delete_function(FunctionName=func_name)
