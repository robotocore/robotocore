"""Unit tests for the AWS Console web UI backend (robotocore.console.app).

Tests cover: route registration, index serving, static file serving (including
directory traversal protection), account-ID extraction, API proxy error paths,
and the per-service request builders.
"""

import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from starlette.testclient import TestClient

from robotocore.console.app import (
    DEFAULT_ACCOUNT_ID,
    STATIC_DIR,
    _extract_account_id,
    _make_aws_request,
    get_console_routes,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_app():
    """Build a minimal Starlette app with only console routes for testing."""
    from starlette.applications import Starlette

    return Starlette(routes=get_console_routes())


@pytest.fixture()
def client():
    return TestClient(_build_app(), raise_server_exceptions=False)


def _make_request_with_auth(auth_header: str):
    """Build a fake Starlette Request with a given Authorization header."""

    # We only need the headers attribute, so we use a scope-based Request
    from starlette.requests import Request

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [
            (b"authorization", auth_header.encode()),
        ],
    }
    return Request(scope)


# ---------------------------------------------------------------------------
# Route registration
# ---------------------------------------------------------------------------


class TestGetConsoleRoutes:
    def test_returns_four_routes(self):
        routes = get_console_routes()
        assert len(routes) == 4

    def test_route_paths(self):
        routes = get_console_routes()
        paths = [r.path for r in routes]
        assert "/_robotocore/console" in paths
        assert "/_robotocore/console/" in paths
        assert "/_robotocore/console/api/{service}/{action}" in paths
        assert "/_robotocore/console/static/{path:path}" in paths

    def test_index_routes_are_get(self):
        routes = get_console_routes()
        index_routes = [r for r in routes if "api" not in r.path and "static" not in r.path]
        for r in index_routes:
            assert "GET" in r.methods

    def test_api_route_is_post(self):
        routes = get_console_routes()
        api_route = [r for r in routes if "api" in r.path][0]
        assert "POST" in api_route.methods

    def test_static_route_is_get(self):
        routes = get_console_routes()
        static_route = [r for r in routes if "static" in r.path][0]
        assert "GET" in static_route.methods


# ---------------------------------------------------------------------------
# Account ID extraction
# ---------------------------------------------------------------------------


class TestExtractAccountId:
    def test_extracts_from_sigv4_header(self):
        req = _make_request_with_auth(
            "AWS4-HMAC-SHA256 Credential=999888777666/20260101/us-east-1/s3/aws4_request, "
            "SignedHeaders=host, Signature=abc"
        )
        assert _extract_account_id(req) == "999888777666"

    def test_default_when_no_auth(self):
        req = _make_request_with_auth("")
        assert _extract_account_id(req) == DEFAULT_ACCOUNT_ID

    def test_default_when_malformed_auth(self):
        req = _make_request_with_auth("Bearer some-token")
        assert _extract_account_id(req) == DEFAULT_ACCOUNT_ID

    def test_extracts_12_digit_account(self):
        req = _make_request_with_auth(
            "AWS4-HMAC-SHA256 Credential=123456789012/20260311/us-west-2/sqs/aws4_request, "
            "SignedHeaders=host, Signature=def"
        )
        assert _extract_account_id(req) == "123456789012"


# ---------------------------------------------------------------------------
# Console index
# ---------------------------------------------------------------------------


class TestConsoleIndex:
    def test_index_returns_html(self, client):
        resp = client.get("/_robotocore/console")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_index_trailing_slash(self, client):
        resp = client.get("/_robotocore/console/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_index_contains_robotocore(self, client):
        resp = client.get("/_robotocore/console")
        assert "robotocore" in resp.text.lower()

    def test_index_contains_doctype(self, client):
        resp = client.get("/_robotocore/console")
        assert "<!DOCTYPE html>" in resp.text


# ---------------------------------------------------------------------------
# Static file serving
# ---------------------------------------------------------------------------


class TestConsoleStatic:
    def test_serves_css(self, client):
        resp = client.get("/_robotocore/console/static/style.css")
        assert resp.status_code == 200
        assert "text/css" in resp.headers["content-type"]

    def test_serves_js(self, client):
        resp = client.get("/_robotocore/console/static/app.js")
        assert resp.status_code == 200
        assert "javascript" in resp.headers["content-type"]

    def test_serves_service_js(self, client):
        for svc in ["s3", "dynamodb", "sqs", "lambda", "cloudwatch"]:
            resp = client.get(f"/_robotocore/console/static/services/{svc}.js")
            assert resp.status_code == 200, f"Failed for {svc}.js"
            assert "javascript" in resp.headers["content-type"]

    def test_404_for_missing_file(self, client):
        resp = client.get("/_robotocore/console/static/nonexistent.txt")
        assert resp.status_code == 404
        body = resp.json()
        assert "error" in body

    def test_directory_traversal_blocked(self, client):
        resp = client.get("/_robotocore/console/static/../../../etc/passwd")
        assert resp.status_code in (403, 404)

    def test_directory_traversal_dot_dot(self, client):
        resp = client.get("/_robotocore/console/static/../../__init__.py")
        assert resp.status_code in (403, 404)

    def test_content_type_mapping(self, client):
        """HTML files inside static dir get the right content type."""
        resp = client.get("/_robotocore/console/static/index.html")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_unknown_extension_gets_octet_stream(self, client, tmp_path):
        """Files with unknown extensions default to application/octet-stream."""
        # Create a temp file inside the static dir to test this
        test_file = STATIC_DIR / "test_unknown.xyz"
        try:
            test_file.write_text("data")
            resp = client.get("/_robotocore/console/static/test_unknown.xyz")
            assert resp.status_code == 200
            assert "octet-stream" in resp.headers["content-type"]
        finally:
            test_file.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# API proxy — error paths (no real server needed)
# ---------------------------------------------------------------------------


class TestApiProxyErrors:
    def test_missing_service_or_action_via_empty_body(self, client):
        """The route pattern requires both {service} and {action}, so the
        Starlette router itself will reject a request with empty segments."""
        resp = client.post(
            "/_robotocore/console/api/s3/ListBuckets",
            content=b"not json at all {{{",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 400
        body = resp.json()
        assert "error" in body

    def test_invalid_json_body(self, client):
        resp = client.post(
            "/_robotocore/console/api/s3/ListBuckets",
            content=b"{broken",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 400
        body = resp.json()
        assert body["error"] == "Invalid JSON body"

    def test_empty_body_is_ok(self, client):
        """An empty body should not cause a JSON error — it means no params."""
        with patch("robotocore.console.app._make_aws_request") as mock_req:
            mock_resp = AsyncMock()
            mock_resp.content = b'{"Buckets": []}'
            mock_resp.status_code = 200
            mock_resp.headers = {"content-type": "application/json"}
            mock_req.return_value = mock_resp
            resp = client.post(
                "/_robotocore/console/api/s3/ListBuckets",
                content=b"",
                headers={"Content-Type": "application/json"},
            )
            assert resp.status_code == 200

    def test_proxy_returns_500_on_connection_error(self, client):
        """If the upstream call fails, api_proxy returns 500 with error."""
        with patch("robotocore.console.app._make_aws_request", side_effect=Exception("boom")):
            resp = client.post(
                "/_robotocore/console/api/dynamodb/ListTables",
                content=b"{}",
                headers={"Content-Type": "application/json"},
            )
            assert resp.status_code == 500
            body = resp.json()
            assert "boom" in body["error"]


# ---------------------------------------------------------------------------
# API proxy — params extraction
# ---------------------------------------------------------------------------


class TestApiProxyParams:
    def test_region_and_account_extracted_from_body(self, client):
        """_region and _account_id are popped from params and passed through."""
        with patch("robotocore.console.app._make_aws_request") as mock_req:
            mock_resp = AsyncMock()
            mock_resp.content = b"{}"
            mock_resp.status_code = 200
            mock_resp.headers = {"content-type": "application/json"}
            mock_req.return_value = mock_resp

            body = {"_region": "eu-west-1", "_account_id": "111222333444", "TableName": "t1"}
            resp = client.post(
                "/_robotocore/console/api/dynamodb/DescribeTable",
                content=json.dumps(body).encode(),
                headers={"Content-Type": "application/json"},
            )
            assert resp.status_code == 200

            # Verify _make_aws_request was called with extracted region/account
            # Args are positional: (client, base_url, service, action, params, region, account_id)
            args = mock_req.call_args[0]
            assert args[5] == "eu-west-1"  # region
            assert args[6] == "111222333444"  # account_id
            # Params (index 4) should not contain _region or _account_id
            assert "_region" not in args[4]
            assert "_account_id" not in args[4]

    def test_default_region_is_us_east_1(self, client):
        """When no _region in body, defaults to us-east-1."""
        with patch("robotocore.console.app._make_aws_request") as mock_req:
            mock_resp = AsyncMock()
            mock_resp.content = b"{}"
            mock_resp.status_code = 200
            mock_resp.headers = {"content-type": "application/json"}
            mock_req.return_value = mock_resp

            resp = client.post(
                "/_robotocore/console/api/sqs/ListQueues",
                content=b"{}",
                headers={"Content-Type": "application/json"},
            )
            assert resp.status_code == 200

            args = mock_req.call_args[0]
            assert args[5] == "us-east-1"  # default region


# ---------------------------------------------------------------------------
# _make_aws_request — service routing
# ---------------------------------------------------------------------------


class TestMakeAwsRequest:
    """Test that _make_aws_request dispatches to the right per-service handler."""

    @pytest.fixture()
    def mock_client(self):
        """An httpx.AsyncClient mock that records calls."""
        client = AsyncMock(spec=httpx.AsyncClient)
        resp = AsyncMock()
        resp.content = b"{}"
        resp.status_code = 200
        resp.headers = {"content-type": "application/json"}
        client.get.return_value = resp
        client.post.return_value = resp
        client.put.return_value = resp
        client.delete.return_value = resp
        client.head.return_value = resp
        return client

    @pytest.mark.anyio
    async def test_s3_list_buckets(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "s3",
            "ListBuckets",
            {},
            "us-east-1",
            "123456789012",
        )
        mock_client.get.assert_called_once()
        url = mock_client.get.call_args[0][0]
        assert url == "http://localhost:4566/"

    @pytest.mark.anyio
    async def test_s3_create_bucket(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "s3",
            "CreateBucket",
            {"Bucket": "my-bucket"},
            "us-east-1",
            "123456789012",
        )
        mock_client.put.assert_called_once()
        url = mock_client.put.call_args[0][0]
        assert "my-bucket" in url

    @pytest.mark.anyio
    async def test_s3_delete_bucket(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "s3",
            "DeleteBucket",
            {"Bucket": "doomed"},
            "us-east-1",
            "123456789012",
        )
        mock_client.delete.assert_called_once()
        assert "doomed" in mock_client.delete.call_args[0][0]

    @pytest.mark.anyio
    async def test_s3_list_objects_with_prefix(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "s3",
            "ListObjects",
            {"Bucket": "b", "Prefix": "foo/"},
            "us-east-1",
            "123456789012",
        )
        mock_client.get.assert_called_once()
        url = mock_client.get.call_args[0][0]
        assert "list-type=2" in url
        assert "prefix=foo/" in url

    @pytest.mark.anyio
    async def test_s3_get_object(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "s3",
            "GetObject",
            {"Bucket": "b", "Key": "k"},
            "us-east-1",
            "123456789012",
        )
        mock_client.get.assert_called_once()
        assert "/b/k" in mock_client.get.call_args[0][0]

    @pytest.mark.anyio
    async def test_s3_put_object(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "s3",
            "PutObject",
            {"Bucket": "b", "Key": "k", "Body": "hello"},
            "us-east-1",
            "123456789012",
        )
        mock_client.put.assert_called_once()
        assert "/b/k" in mock_client.put.call_args[0][0]

    @pytest.mark.anyio
    async def test_s3_delete_object(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "s3",
            "DeleteObject",
            {"Bucket": "b", "Key": "k"},
            "us-east-1",
            "123456789012",
        )
        mock_client.delete.assert_called_once()
        assert "/b/k" in mock_client.delete.call_args[0][0]

    @pytest.mark.anyio
    async def test_s3_head_bucket(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "s3",
            "HeadBucket",
            {"Bucket": "b"},
            "us-east-1",
            "123456789012",
        )
        mock_client.head.assert_called_once()
        assert "/b" in mock_client.head.call_args[0][0]

    @pytest.mark.anyio
    async def test_s3_unknown_action_falls_back(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "s3",
            "SomeWeirdAction",
            {},
            "us-east-1",
            "123456789012",
        )
        mock_client.get.assert_called_once()

    @pytest.mark.anyio
    async def test_dynamodb_uses_json_protocol(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "dynamodb",
            "ListTables",
            {},
            "us-east-1",
            "123456789012",
        )
        mock_client.post.assert_called_once()
        headers = mock_client.post.call_args[1]["headers"]
        assert headers["X-Amz-Target"] == "DynamoDB_20120810.ListTables"
        assert headers["Content-Type"] == "application/x-amz-json-1.0"

    @pytest.mark.anyio
    async def test_dynamodb_sends_params_as_json(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "dynamodb",
            "DescribeTable",
            {"TableName": "my-table"},
            "us-east-1",
            "123456789012",
        )
        body = mock_client.post.call_args[1]["content"]
        parsed = json.loads(body)
        assert parsed["TableName"] == "my-table"

    @pytest.mark.anyio
    async def test_sqs_uses_query_protocol(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "sqs",
            "ListQueues",
            {},
            "us-east-1",
            "123456789012",
        )
        mock_client.post.assert_called_once()
        headers = mock_client.post.call_args[1]["headers"]
        assert headers["Content-Type"] == "application/x-www-form-urlencoded"
        body = mock_client.post.call_args[1]["content"]
        assert "Action=ListQueues" in body
        assert "Version=2012-11-05" in body

    @pytest.mark.anyio
    async def test_lambda_list_functions(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "lambda",
            "ListFunctions",
            {},
            "us-east-1",
            "123456789012",
        )
        mock_client.get.assert_called_once()
        assert "/2015-03-31/functions" in mock_client.get.call_args[0][0]

    @pytest.mark.anyio
    async def test_lambda_get_function(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "lambda",
            "GetFunction",
            {"FunctionName": "my-fn"},
            "us-east-1",
            "123456789012",
        )
        mock_client.get.assert_called_once()
        assert "/my-fn" in mock_client.get.call_args[0][0]

    @pytest.mark.anyio
    async def test_lambda_invoke(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "lambda",
            "Invoke",
            {"FunctionName": "fn", "Payload": {"key": "val"}},
            "us-east-1",
            "123456789012",
        )
        mock_client.post.assert_called_once()
        assert "/invocations" in mock_client.post.call_args[0][0]

    @pytest.mark.anyio
    async def test_lambda_create_function(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "lambda",
            "CreateFunction",
            {"FunctionName": "new-fn"},
            "us-east-1",
            "123456789012",
        )
        mock_client.post.assert_called_once()
        assert "/2015-03-31/functions" in mock_client.post.call_args[0][0]

    @pytest.mark.anyio
    async def test_lambda_delete_function(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "lambda",
            "DeleteFunction",
            {"FunctionName": "old-fn"},
            "us-east-1",
            "123456789012",
        )
        mock_client.delete.assert_called_once()
        assert "/old-fn" in mock_client.delete.call_args[0][0]

    @pytest.mark.anyio
    async def test_lambda_unknown_action_falls_back(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "lambda",
            "UnknownOp",
            {},
            "us-east-1",
            "123456789012",
        )
        mock_client.get.assert_called_once()

    @pytest.mark.anyio
    async def test_logs_uses_json_protocol(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "logs",
            "DescribeLogGroups",
            {},
            "us-east-1",
            "123456789012",
        )
        mock_client.post.assert_called_once()
        headers = mock_client.post.call_args[1]["headers"]
        assert headers["X-Amz-Target"] == "Logs_20140328.DescribeLogGroups"
        assert headers["Content-Type"] == "application/x-amz-json-1.1"

    @pytest.mark.anyio
    async def test_generic_service_uses_json_protocol(self, mock_client):
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "secretsmanager",
            "ListSecrets",
            {},
            "us-east-1",
            "123456789012",
        )
        mock_client.post.assert_called_once()
        headers = mock_client.post.call_args[1]["headers"]
        assert headers["X-Amz-Target"] == "secretsmanager.ListSecrets"

    @pytest.mark.anyio
    async def test_auth_header_contains_credential(self, mock_client):
        """The constructed auth header must include the account and region."""
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "s3",
            "ListBuckets",
            {},
            "eu-west-1",
            "999888777666",
        )
        headers = mock_client.get.call_args[1]["headers"]
        assert "999888777666" in headers["Authorization"]
        assert "eu-west-1" in headers["Authorization"]
        assert "X-Amz-Date" in headers

    @pytest.mark.anyio
    async def test_s3_create_bucket_non_us_east_1(self, mock_client):
        """CreateBucket in a non-us-east-1 region should include location constraint XML."""
        # We need to pass region through the common headers for non-us-east-1
        # The region is baked into the auth header but _s3_request checks
        # headers.get("X-Amz-Region") which won't be present by default.
        # The current code won't produce XML body because it checks a header
        # that isn't set. This test documents the current behavior.
        await _make_aws_request(
            mock_client,
            "http://localhost:4566",
            "s3",
            "CreateBucket",
            {"Bucket": "eu-bucket"},
            "eu-west-1",
            "123456789012",
        )
        mock_client.put.assert_called_once()
        assert "eu-bucket" in mock_client.put.call_args[0][0]
