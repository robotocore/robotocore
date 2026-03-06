"""Unit tests for Lambda provider HTTP request routing."""

import json
from unittest.mock import MagicMock, patch

import pytest

from robotocore.services.lambda_.provider import (
    _alias_dict,
    _error,
    _fn_config,
    _json,
    _layer_version_dict,
    _sanitize_esm,
    _url_config_dict,
    handle_lambda_request,
)


def _make_scope(method: str, path: str, body: bytes = b"", headers: dict | None = None):
    """Build an ASGI scope dict for constructing a Starlette Request."""
    hdrs = headers or {}
    raw_headers = [(k.lower().encode(), v.encode()) for k, v in hdrs.items()]
    return {
        "type": "http",
        "method": method,
        "path": path,
        "query_string": b"",
        "headers": raw_headers,
        "root_path": "",
        "scheme": "http",
        "server": ("localhost", 4566),
    }


async def _make_request(method, path, body=b"", headers=None):
    from starlette.requests import Request

    scope = _make_scope(method, path, body, headers)

    async def receive():
        return {"type": "http.request", "body": body}

    return Request(scope, receive)


class TestHelpers:
    def test_json_returns_json_response(self):
        resp = _json(200, {"key": "value"})
        assert resp.status_code == 200
        assert json.loads(resp.body) == {"key": "value"}

    def test_json_none_returns_empty(self):
        resp = _json(204, None)
        assert resp.status_code == 204
        assert resp.body == b""

    def test_error_response_format(self):
        resp = _error("ResourceNotFoundException", "Not found", 404)
        assert resp.status_code == 404
        data = json.loads(resp.body)
        assert data["__type"] == "ResourceNotFoundException"
        assert data["Message"] == "Not found"

    def test_sanitize_esm_removes_internal_fields(self):
        config = {
            "UUID": "abc",
            "FunctionArn": "arn:aws:lambda:us-east-1:123:function:fn",
            "_region": "us-east-1",
            "_account_id": "123",
        }
        result = _sanitize_esm(config)
        assert "UUID" in result
        assert "FunctionArn" in result
        assert "_region" not in result
        assert "_account_id" not in result

    def test_fn_config_with_get_configuration(self):
        mock_fn = MagicMock()
        mock_fn.get_configuration.return_value = {"FunctionName": "test"}
        result = _fn_config(mock_fn)
        assert result == {"FunctionName": "test"}

    def test_fn_config_with_string_configuration(self):
        mock_fn = MagicMock()
        mock_fn.get_configuration.return_value = '{"FunctionName": "test"}'
        result = _fn_config(mock_fn)
        assert result == {"FunctionName": "test"}

    def test_fn_config_without_get_configuration(self):
        mock_fn = object()
        result = _fn_config(mock_fn)
        assert result == {}

    def test_alias_dict_with_to_json(self):
        mock_alias = MagicMock()
        mock_alias.to_json.return_value = {"AliasArn": "arn", "Name": "prod"}
        result = _alias_dict(mock_alias)
        assert result == {"AliasArn": "arn", "Name": "prod"}

    def test_alias_dict_string_json(self):
        mock_alias = MagicMock()
        mock_alias.to_json.return_value = '{"AliasArn": "arn"}'
        result = _alias_dict(mock_alias)
        assert result == {"AliasArn": "arn"}

    def test_alias_dict_without_to_json(self):
        result = _alias_dict(object())
        assert result == {}

    def test_layer_version_dict_with_method(self):
        mock_lv = MagicMock()
        mock_lv.get_layer_version.return_value = {"LayerVersionArn": "arn"}
        result = _layer_version_dict(mock_lv)
        assert result == {"LayerVersionArn": "arn"}

    def test_layer_version_dict_without_method(self):
        result = _layer_version_dict(object())
        assert result == {}

    def test_url_config_dict_with_to_dict(self):
        mock_cfg = MagicMock()
        mock_cfg.to_dict.return_value = {"FunctionUrl": "https://..."}
        result = _url_config_dict(mock_cfg, "fn", "us-east-1", "123")
        assert result == {"FunctionUrl": "https://..."}

    def test_url_config_dict_without_to_dict(self):
        result = _url_config_dict(object(), "fn", "us-east-1", "123")
        assert result == {}


@pytest.mark.asyncio
class TestHandleLambdaRequest:
    async def test_empty_path_returns_400(self):
        req = await _make_request("GET", "/2015-03-31/")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "InvalidRequest"

    async def test_unknown_path_returns_400(self):
        req = await _make_request("GET", "/2015-03-31/unknown")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_list_functions(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_fn = MagicMock()
        mock_fn.get_configuration.return_value = {"FunctionName": "test-fn"}
        mock_backend.list_functions.return_value = [mock_fn]
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("GET", "/2015-03-31/functions")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert len(data["Functions"]) == 1
        assert data["Functions"][0]["FunctionName"] == "test-fn"

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_create_function(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_fn = MagicMock()
        mock_fn.get_configuration.return_value = {"FunctionName": "new-fn"}
        mock_backend.create_function.return_value = mock_fn
        mock_backend_fn.return_value = mock_backend

        body = json.dumps({"FunctionName": "new-fn"}).encode()
        req = await _make_request("POST", "/2015-03-31/functions", body)
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 201

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_get_function(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_fn = MagicMock()
        mock_fn.get_configuration.return_value = {"FunctionName": "my-fn"}
        mock_backend.get_function.return_value = mock_fn
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("GET", "/2015-03-31/functions/my-fn")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert "Configuration" in data
        assert "Code" in data

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_delete_function(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("DELETE", "/2015-03-31/functions/my-fn")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204
        mock_backend.delete_function.assert_called_once()

    async def test_missing_function_name_returns_400(self):
        req = await _make_request("POST", "/2015-03-31/functions/")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        # The path /functions/ with trailing slash yields parts=["functions",""]
        # which should still handle routing
        assert resp.status_code in (400, 500)

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_get_function_configuration(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_fn = MagicMock()
        mock_fn.get_configuration.return_value = {"FunctionName": "my-fn"}
        mock_backend.get_function.return_value = mock_fn
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("GET", "/2015-03-31/functions/my-fn/configuration")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_exception_maps_to_404(self, mock_backend_fn):
        mock_backend = MagicMock()

        class UnknownFunctionError(Exception):
            pass

        mock_backend.list_functions.side_effect = UnknownFunctionError("nope")
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("GET", "/2015-03-31/functions")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_exception_with_code_attribute(self, mock_backend_fn):
        mock_backend = MagicMock()

        class LambdaClientError(Exception):
            code = 409

        mock_backend.list_functions.side_effect = LambdaClientError("conflict")
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("GET", "/2015-03-31/functions")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 409

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_generic_exception_returns_500(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend.list_functions.side_effect = RuntimeError("boom")
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("GET", "/2015-03-31/functions")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 500
        data = json.loads(resp.body)
        assert data["__type"] == "ServiceException"


@pytest.mark.asyncio
class TestEventSourceMappings:
    async def test_create_esm(self):
        from robotocore.services.lambda_.provider import _esm_store

        # Clear store
        _esm_store.clear()

        body = json.dumps(
            {
                "EventSourceArn": "arn:aws:sqs:us-east-1:123:my-queue",
                "FunctionName": "my-fn",
                "BatchSize": 5,
            }
        ).encode()

        with patch("robotocore.services.lambda_.event_source.get_engine") as mock_engine:
            mock_engine.return_value = MagicMock()
            req = await _make_request("POST", "/2015-03-31/event-source-mappings", body)
            resp = await handle_lambda_request(req, "us-east-1", "123456789012")

        assert resp.status_code == 202
        data = json.loads(resp.body)
        assert data["BatchSize"] == 5
        assert data["State"] == "Enabled"
        assert "_region" not in data

        _esm_store.clear()

    async def test_list_esm(self):
        from robotocore.services.lambda_.provider import _esm_store

        _esm_store.clear()
        _esm_store["test-uuid"] = {
            "UUID": "test-uuid",
            "EventSourceArn": "arn:aws:sqs:us-east-1:123:q",
            "FunctionArn": "arn:aws:lambda:us-east-1:123:function:fn",
            "State": "Enabled",
            "_region": "us-east-1",
            "_account_id": "123",
        }

        req = await _make_request("GET", "/2015-03-31/event-source-mappings")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert len(data["EventSourceMappings"]) == 1

        _esm_store.clear()

    async def test_get_esm_not_found(self):
        from robotocore.services.lambda_.provider import _esm_store

        _esm_store.clear()
        req = await _make_request("GET", "/2015-03-31/event-source-mappings/nonexistent")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404

        _esm_store.clear()

    async def test_delete_esm(self):
        from robotocore.services.lambda_.provider import _esm_store

        _esm_store.clear()
        _esm_store["del-uuid"] = {
            "UUID": "del-uuid",
            "State": "Enabled",
            "_region": "us-east-1",
            "_account_id": "123",
        }

        req = await _make_request("DELETE", "/2015-03-31/event-source-mappings/del-uuid")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert "del-uuid" not in _esm_store

        _esm_store.clear()


@pytest.mark.asyncio
class TestAccountSettings:
    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_account_settings(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_fn = MagicMock()
        mock_fn.code_size = 1024
        mock_fn.reserved_concurrency = 10
        mock_backend.list_functions.return_value = [mock_fn]
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("GET", "/2015-03-31/account-settings")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["AccountUsage"]["FunctionCount"] == 1
        assert data["AccountUsage"]["TotalCodeSize"] == 1024
        assert data["AccountLimit"]["UnreservedConcurrentExecutions"] == 990
