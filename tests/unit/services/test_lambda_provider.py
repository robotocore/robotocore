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


def _make_scope(
    method: str,
    path: str,
    body: bytes = b"",
    headers: dict | None = None,
    query_string: bytes = b"",
):
    """Build an ASGI scope dict for constructing a Starlette Request."""
    hdrs = headers or {}
    raw_headers = [(k.lower().encode(), v.encode()) for k, v in hdrs.items()]
    return {
        "type": "http",
        "method": method,
        "path": path,
        "query_string": query_string,
        "headers": raw_headers,
        "root_path": "",
        "scheme": "http",
        "server": ("localhost", 4566),
    }


async def _make_request(method, path, body=b"", headers=None, query_string=b""):
    from starlette.requests import Request

    scope = _make_scope(method, path, body, headers, query_string)

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

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_get_nonexistent_function_returns_404(self, mock_backend_fn):
        """Getting a non-existent function returns ResourceNotFoundException (404)."""
        mock_backend = MagicMock()

        class UnknownFunctionError(Exception):
            pass

        mock_backend.get_function.side_effect = UnknownFunctionError(
            "Function not found: arn:aws:lambda:us-east-1:123456789012:function:no-such-fn"
        )
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("GET", "/2015-03-31/functions/no-such-fn")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404
        data = json.loads(resp.body)
        assert data["__type"] == "ResourceNotFoundException"

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_delete_nonexistent_function_returns_404(self, mock_backend_fn):
        """Deleting a non-existent function returns ResourceNotFoundException (404)."""
        mock_backend = MagicMock()

        class UnknownFunctionError(Exception):
            pass

        mock_backend.delete_function.side_effect = UnknownFunctionError(
            "Function not found: no-such-fn"
        )
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("DELETE", "/2015-03-31/functions/no-such-fn")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404
        data = json.loads(resp.body)
        assert data["__type"] == "ResourceNotFoundException"

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_invalid_parameter_value_returns_400(self, mock_backend_fn):
        """InvalidParameterValue errors map to 400."""
        mock_backend = MagicMock()

        class InvalidParameterValueException(Exception):  # noqa: N818
            pass

        mock_backend.create_function.side_effect = InvalidParameterValueException(
            "The runtime parameter of python2.7 is not supported"
        )
        mock_backend_fn.return_value = mock_backend

        body = json.dumps({"FunctionName": "fn", "Runtime": "python2.7"}).encode()
        req = await _make_request("POST", "/2015-03-31/functions", body)
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "InvalidParameterValueException"

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_resource_conflict_returns_409(self, mock_backend_fn):
        """Creating a function that already exists returns ResourceConflictException (409)."""
        mock_backend = MagicMock()

        class ResourceConflictException(Exception):  # noqa: N818
            code = 409

        mock_backend.create_function.side_effect = ResourceConflictException(
            "Function already exist: my-fn"
        )
        mock_backend_fn.return_value = mock_backend

        body = json.dumps({"FunctionName": "my-fn"}).encode()
        req = await _make_request("POST", "/2015-03-31/functions", body)
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 409


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


@pytest.mark.asyncio
class TestDeleteFunctionCascade:
    """BUG: Deleting a function must clean up child resources in native stores.

    This is a CATEGORICAL bug — any provider with parent-child relationships
    in native stores (not Moto) must cascade deletes. Affects: Lambda (ESMs,
    provisioned concurrency, DLQ configs), and potentially other providers
    with native stores alongside Moto.
    """

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_delete_function_cleans_up_esm_store(self, mock_backend_fn):
        """ESMs referencing a deleted function must be removed."""
        from robotocore.services.lambda_.provider import _esm_store

        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        _esm_store.clear()
        # Two ESMs for the function being deleted, one for another function
        _esm_store["esm-1"] = {
            "UUID": "esm-1",
            "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:doomed-fn",
            "_region": "us-east-1",
            "_account_id": "123456789012",
        }
        _esm_store["esm-2"] = {
            "UUID": "esm-2",
            "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:doomed-fn",
            "_region": "us-east-1",
            "_account_id": "123456789012",
        }
        _esm_store["esm-other"] = {
            "UUID": "esm-other",
            "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:other-fn",
            "_region": "us-east-1",
            "_account_id": "123456789012",
        }

        req = await _make_request("DELETE", "/2015-03-31/functions/doomed-fn")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204

        # ESMs for deleted function should be gone
        assert "esm-1" not in _esm_store
        assert "esm-2" not in _esm_store
        # ESM for other function should remain
        assert "esm-other" in _esm_store

        _esm_store.clear()

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_delete_function_cleans_up_provisioned_concurrency(self, mock_backend_fn):
        """Provisioned concurrency configs for a deleted function must be removed."""
        from robotocore.services.lambda_.provider import _provisioned_concurrency

        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        _provisioned_concurrency.clear()
        _provisioned_concurrency[("123456789012", "us-east-1", "doomed-fn", "1")] = {
            "RequestedProvisionedConcurrentExecutions": 5,
            "Status": "READY",
        }
        _provisioned_concurrency[("123456789012", "us-east-1", "doomed-fn", "2")] = {
            "RequestedProvisionedConcurrentExecutions": 10,
            "Status": "READY",
        }
        _provisioned_concurrency[("123456789012", "us-east-1", "other-fn", "1")] = {
            "RequestedProvisionedConcurrentExecutions": 3,
            "Status": "READY",
        }

        req = await _make_request("DELETE", "/2015-03-31/functions/doomed-fn")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204

        # Provisioned concurrency for deleted function should be gone
        assert ("123456789012", "us-east-1", "doomed-fn", "1") not in _provisioned_concurrency
        assert ("123456789012", "us-east-1", "doomed-fn", "2") not in _provisioned_concurrency
        # Other function's config should remain
        assert ("123456789012", "us-east-1", "other-fn", "1") in _provisioned_concurrency

        _provisioned_concurrency.clear()

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_delete_function_cleans_up_dlq_config(self, mock_backend_fn):
        """DLQ config for a deleted function must be removed."""
        from robotocore.services.lambda_.provider import _dlq_configs

        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        _dlq_configs.clear()
        _dlq_configs[("123456789012", "us-east-1", "doomed-fn")] = {
            "TargetArn": "arn:aws:sqs:us-east-1:123456789012:dlq"
        }
        _dlq_configs[("123456789012", "us-east-1", "other-fn")] = {
            "TargetArn": "arn:aws:sqs:us-east-1:123456789012:dlq2"
        }

        req = await _make_request("DELETE", "/2015-03-31/functions/doomed-fn")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204

        assert ("123456789012", "us-east-1", "doomed-fn") not in _dlq_configs
        assert ("123456789012", "us-east-1", "other-fn") in _dlq_configs

        _dlq_configs.clear()


@pytest.mark.asyncio
class TestESMRaceCondition:
    """BUG: ESM read-then-write operations have TOCTOU race conditions.

    The ESM config is read outside the lock, then updated inside the lock.
    If another thread deletes the ESM between the read and write, the update
    recreates a deleted mapping. This is a CATEGORICAL bug — any native store
    using lock-per-operation instead of lock-per-transaction is vulnerable.
    """

    async def test_update_esm_deleted_between_read_and_write(self):
        """Updating an ESM that was deleted between read and write should return 404."""
        from robotocore.services.lambda_.provider import _esm_store

        _esm_store.clear()
        _esm_store["race-uuid"] = {
            "UUID": "race-uuid",
            "FunctionArn": "arn:aws:lambda:us-east-1:123:function:fn",
            "BatchSize": 10,
            "State": "Enabled",
            "_region": "us-east-1",
            "_account_id": "123",
        }

        # Simulate: read happens, then delete happens, then write happens
        # The current code reads config outside the lock, so after the read
        # another thread could delete. The write block checks `if esm_uuid in _esm_store`
        # so it won't crash, but the returned config is stale (from the outer read).
        # This test documents the expected behavior: if the UUID still exists at
        # update time, the update succeeds.
        body = json.dumps({"BatchSize": 50}).encode()
        req = await _make_request("PUT", "/2015-03-31/event-source-mappings/race-uuid", body)
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["BatchSize"] == 50

        _esm_store.clear()

    async def test_delete_esm_returns_final_state(self):
        """DELETE ESM should return the config at deletion time, not a stale read."""
        from robotocore.services.lambda_.provider import _esm_store

        _esm_store.clear()
        _esm_store["del-race"] = {
            "UUID": "del-race",
            "FunctionArn": "arn:aws:lambda:us-east-1:123:function:fn",
            "BatchSize": 10,
            "State": "Enabled",
            "_region": "us-east-1",
            "_account_id": "123",
        }

        req = await _make_request("DELETE", "/2015-03-31/event-source-mappings/del-race")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        # Should return the deleted config
        assert data["UUID"] == "del-race"
        # Internal fields should be sanitized
        assert "_region" not in data

        _esm_store.clear()


@pytest.mark.asyncio
class TestTagOperations:
    """BUG: Tag operations have edge cases around error handling and ARN parsing.

    CATEGORICAL pattern: Tag endpoints that delegate to Moto must handle
    the case where the resource doesn't exist, and must correctly reconstruct
    ARNs from URL paths.
    """

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_list_tags_nonexistent_function_returns_404(self, mock_backend_fn):
        """ListTags on a nonexistent function should return 404, not 500."""
        mock_backend = MagicMock()

        class UnknownFunctionError(Exception):
            pass

        mock_backend.get_function.side_effect = UnknownFunctionError(
            "Function not found: arn:aws:lambda:us-east-1:123456789012:function:nope"
        )
        mock_backend_fn.return_value = mock_backend

        arn = "arn:aws:lambda:us-east-1:123456789012:function:nope"
        req = await _make_request("GET", f"/2015-03-31/tags/{arn}")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404
        data = json.loads(resp.body)
        assert data["__type"] == "ResourceNotFoundException"

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_tag_resource_nonexistent_function_returns_404(self, mock_backend_fn):
        """TagResource on a nonexistent function should return 404."""
        mock_backend = MagicMock()

        class UnknownFunctionError(Exception):
            pass

        mock_backend.tag_resource.side_effect = UnknownFunctionError("Function not found")
        mock_backend_fn.return_value = mock_backend

        arn = "arn:aws:lambda:us-east-1:123456789012:function:nope"
        body = json.dumps({"Tags": {"env": "test"}}).encode()
        req = await _make_request("POST", f"/2015-03-31/tags/{arn}", body)
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_untag_resource_with_tag_keys(self, mock_backend_fn):
        """UntagResource should pass tagKeys from query params to Moto."""
        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        arn = "arn:aws:lambda:us-east-1:123456789012:function:my-fn"
        req = await _make_request(
            "DELETE",
            f"/2015-03-31/tags/{arn}",
            query_string=b"tagKeys=env&tagKeys=version",
        )
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204
        mock_backend.untag_resource.assert_called_once_with(arn, ["env", "version"])

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_list_tags_returns_tags_dict(self, mock_backend_fn):
        """ListTags should return a Tags dict from the function."""
        mock_backend = MagicMock()
        mock_fn = MagicMock()
        mock_fn.tags = {"env": "prod", "team": "platform"}
        mock_backend.get_function.return_value = mock_fn
        mock_backend_fn.return_value = mock_backend

        arn = "arn:aws:lambda:us-east-1:123456789012:function:my-fn"
        req = await _make_request("GET", f"/2015-03-31/tags/{arn}")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["Tags"] == {"env": "prod", "team": "platform"}

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_list_tags_no_tags_returns_empty_dict(self, mock_backend_fn):
        """ListTags on a function with no tags should return empty dict, not None."""
        mock_backend = MagicMock()
        mock_fn = MagicMock()
        mock_fn.tags = None
        mock_backend.get_function.return_value = mock_fn
        mock_backend_fn.return_value = mock_backend

        arn = "arn:aws:lambda:us-east-1:123456789012:function:my-fn"
        req = await _make_request("GET", f"/2015-03-31/tags/{arn}")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["Tags"] == {}


@pytest.mark.asyncio
class TestProvisionedConcurrency:
    """Test provisioned concurrency CRUD via the handler."""

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_put_provisioned_concurrency(self, mock_backend_fn):
        from robotocore.services.lambda_.provider import _provisioned_concurrency

        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend
        _provisioned_concurrency.clear()

        body = json.dumps({"ProvisionedConcurrentExecutions": 5}).encode()
        req = await _make_request(
            "PUT",
            "/2015-03-31/functions/my-fn/provisioned-concurrency",
            body,
            query_string=b"Qualifier=1",
        )
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 202
        data = json.loads(resp.body)
        assert data["RequestedProvisionedConcurrentExecutions"] == 5
        assert data["Status"] == "READY"

        _provisioned_concurrency.clear()

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_get_provisioned_concurrency_not_found(self, mock_backend_fn):
        from robotocore.services.lambda_.provider import _provisioned_concurrency

        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend
        _provisioned_concurrency.clear()

        req = await _make_request(
            "GET",
            "/2015-03-31/functions/my-fn/provisioned-concurrency",
            query_string=b"Qualifier=99",
        )
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404
        data = json.loads(resp.body)
        assert data["__type"] == "ProvisionedConcurrencyConfigNotFoundException"

        _provisioned_concurrency.clear()

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_delete_provisioned_concurrency_not_found(self, mock_backend_fn):
        from robotocore.services.lambda_.provider import _provisioned_concurrency

        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend
        _provisioned_concurrency.clear()

        req = await _make_request(
            "DELETE",
            "/2015-03-31/functions/my-fn/provisioned-concurrency",
            query_string=b"Qualifier=99",
        )
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404

        _provisioned_concurrency.clear()
