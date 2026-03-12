"""Unit tests for Lambda Milestone 2: Feature Completeness.

Tests cover:
- Function versions and aliases (PublishVersion, CreateAlias, UpdateAlias, etc.)
- Function URLs (CRUD via native store)
- Invoke destinations (on-success and on-failure dispatch)
- Layer CRUD (PublishLayerVersion, GetLayerVersion, etc.)
- Event source mapping enhancements (FilterCriteria, bisect, retry)
- Dead letter queue for async invocation failures
- Provisioned concurrency CRUD
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from robotocore.services.lambda_.provider import (
    _dlq_configs,
    _esm_store,
    _provisioned_concurrency,
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


def _make_scope_with_qs(
    method: str,
    path: str,
    query_string: str = "",
    body: bytes = b"",
    headers: dict | None = None,
):
    """Build an ASGI scope dict with query string."""
    hdrs = headers or {}
    raw_headers = [(k.lower().encode(), v.encode()) for k, v in hdrs.items()]
    return {
        "type": "http",
        "method": method,
        "path": path,
        "query_string": query_string.encode(),
        "headers": raw_headers,
        "root_path": "",
        "scheme": "http",
        "server": ("localhost", 4566),
    }


async def _make_request(method, path, body=b"", headers=None, query_string=""):
    from starlette.requests import Request

    if query_string:
        scope = _make_scope_with_qs(method, path, query_string, body, headers)
    else:
        scope = _make_scope(method, path, body, headers)

    async def receive():
        return {"type": "http.request", "body": body}

    return Request(scope, receive)


# ============================================================
# Versions
# ============================================================


@pytest.mark.asyncio
class TestPublishVersion:
    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_publish_version(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_ver = MagicMock()
        mock_ver.get_configuration.return_value = {
            "FunctionName": "my-fn",
            "Version": "1",
        }
        mock_backend.publish_version.return_value = mock_ver
        mock_backend_fn.return_value = mock_backend

        body = json.dumps({"Description": "v1"}).encode()
        req = await _make_request("POST", "/2015-03-31/functions/my-fn/versions", body)
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 201
        data = json.loads(resp.body)
        assert data["Version"] == "1"
        mock_backend.publish_version.assert_called_once_with("my-fn", "v1")

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_publish_version_no_description(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_ver = MagicMock()
        mock_ver.get_configuration.return_value = {"FunctionName": "fn", "Version": "2"}
        mock_backend.publish_version.return_value = mock_ver
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("POST", "/2015-03-31/functions/fn/versions", b"{}")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 201
        mock_backend.publish_version.assert_called_once_with("fn", "")

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_list_versions_by_function(self, mock_backend_fn):
        mock_backend = MagicMock()
        v1 = MagicMock()
        v1.get_configuration.return_value = {"Version": "$LATEST"}
        v2 = MagicMock()
        v2.get_configuration.return_value = {"Version": "1"}
        mock_backend.list_versions_by_function.return_value = [v1, v2]
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("GET", "/2015-03-31/functions/fn/versions")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert len(data["Versions"]) == 2


# ============================================================
# Aliases
# ============================================================


@pytest.mark.asyncio
class TestAliases:
    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_create_alias(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_alias = MagicMock()
        mock_alias.to_json.return_value = {
            "AliasArn": "arn:aws:lambda:us-east-1:123:function:fn:prod",
            "Name": "prod",
            "FunctionVersion": "1",
            "Description": "Production",
        }
        mock_backend.create_alias.return_value = mock_alias
        mock_backend_fn.return_value = mock_backend

        body = json.dumps(
            {
                "Name": "prod",
                "FunctionVersion": "1",
                "Description": "Production",
            }
        ).encode()
        req = await _make_request("POST", "/2015-03-31/functions/fn/aliases", body)
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 201
        data = json.loads(resp.body)
        assert data["Name"] == "prod"
        assert data["FunctionVersion"] == "1"
        mock_backend.create_alias.assert_called_once_with(
            "prod",
            "fn",
            "1",
            "Production",
            None,
        )

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_create_alias_with_routing_config(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_alias = MagicMock()
        mock_alias.to_json.return_value = {"Name": "canary", "FunctionVersion": "1"}
        mock_backend.create_alias.return_value = mock_alias
        mock_backend_fn.return_value = mock_backend

        routing = {"AdditionalVersionWeights": {"2": 0.1}}
        body = json.dumps(
            {
                "Name": "canary",
                "FunctionVersion": "1",
                "RoutingConfig": routing,
            }
        ).encode()
        req = await _make_request("POST", "/2015-03-31/functions/fn/aliases", body)
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 201
        mock_backend.create_alias.assert_called_once_with(
            "canary",
            "fn",
            "1",
            "",
            routing,
        )

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_list_aliases(self, mock_backend_fn):
        mock_backend = MagicMock()
        a1 = MagicMock()
        a1.to_json.return_value = {"Name": "prod"}
        a2 = MagicMock()
        a2.to_json.return_value = {"Name": "staging"}
        mock_backend.list_aliases.return_value = [a1, a2]
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("GET", "/2015-03-31/functions/fn/aliases")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert len(data["Aliases"]) == 2

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_get_alias(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_alias = MagicMock()
        mock_alias.to_json.return_value = {"Name": "prod", "FunctionVersion": "1"}
        mock_backend.get_alias.return_value = mock_alias
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("GET", "/2015-03-31/functions/fn/aliases/prod")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["Name"] == "prod"
        mock_backend.get_alias.assert_called_once_with("prod", "fn")

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_update_alias(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_alias = MagicMock()
        mock_alias.to_json.return_value = {"Name": "prod", "FunctionVersion": "2"}
        mock_backend.update_alias.return_value = mock_alias
        mock_backend_fn.return_value = mock_backend

        body = json.dumps(
            {
                "FunctionVersion": "2",
                "Description": "updated",
            }
        ).encode()
        req = await _make_request("PUT", "/2015-03-31/functions/fn/aliases/prod", body)
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        mock_backend.update_alias.assert_called_once_with(
            "prod",
            "fn",
            "2",
            "updated",
            None,
        )

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_delete_alias(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("DELETE", "/2015-03-31/functions/fn/aliases/prod")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204
        mock_backend.delete_alias.assert_called_once_with("prod", "fn")


# ============================================================
# Function URLs (native store)
# ============================================================


@pytest.mark.asyncio
class TestFunctionUrls:
    def setup_method(self):
        from robotocore.services.lambda_.urls import clear_store

        clear_store()

    def teardown_method(self):
        from robotocore.services.lambda_.urls import clear_store

        clear_store()

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_create_function_url(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        body = json.dumps(
            {
                "AuthType": "NONE",
                "Cors": {"AllowOrigins": ["*"]},
            }
        ).encode()
        req = await _make_request("POST", "/2015-03-31/functions/my-fn/url", body)
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 201
        data = json.loads(resp.body)
        assert data["AuthType"] == "NONE"
        assert "lambda-url" in data["FunctionUrl"]
        assert data["Cors"] == {"AllowOrigins": ["*"]}

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_create_function_url_duplicate(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        body = json.dumps({"AuthType": "NONE"}).encode()
        req1 = await _make_request("POST", "/2015-03-31/functions/my-fn/url", body)
        await handle_lambda_request(req1, "us-east-1", "123456789012")

        req2 = await _make_request("POST", "/2015-03-31/functions/my-fn/url", body)
        resp = await handle_lambda_request(req2, "us-east-1", "123456789012")
        assert resp.status_code == 409

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_get_function_url(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        # Create first
        body = json.dumps({"AuthType": "IAM"}).encode()
        req = await _make_request("POST", "/2015-03-31/functions/fn/url", body)
        await handle_lambda_request(req, "us-east-1", "123456789012")

        # Get
        req = await _make_request("GET", "/2015-03-31/functions/fn/url")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["AuthType"] == "IAM"

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_get_function_url_not_found(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("GET", "/2015-03-31/functions/nonexistent/url")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_update_function_url(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        # Create
        body = json.dumps({"AuthType": "NONE"}).encode()
        req = await _make_request("POST", "/2015-03-31/functions/fn/url", body)
        await handle_lambda_request(req, "us-east-1", "123456789012")

        # Update
        body = json.dumps({"AuthType": "IAM"}).encode()
        req = await _make_request("PUT", "/2015-03-31/functions/fn/url", body)
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["AuthType"] == "IAM"

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_delete_function_url(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        # Create
        body = json.dumps({"AuthType": "NONE"}).encode()
        req = await _make_request("POST", "/2015-03-31/functions/fn/url", body)
        await handle_lambda_request(req, "us-east-1", "123456789012")

        # Delete
        req = await _make_request("DELETE", "/2015-03-31/functions/fn/url")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204

        # Verify gone
        req = await _make_request("GET", "/2015-03-31/functions/fn/url")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_function_url_invoke_mode(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        body = json.dumps(
            {
                "AuthType": "NONE",
                "InvokeMode": "RESPONSE_STREAM",
            }
        ).encode()
        req = await _make_request("POST", "/2015-03-31/functions/fn/url", body)
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 201
        data = json.loads(resp.body)
        assert data["InvokeMode"] == "RESPONSE_STREAM"


# ============================================================
# Invoke Destinations
# ============================================================


class TestDestinations:
    def test_build_destination_record_success(self):
        from robotocore.services.lambda_.destinations import _build_destination_record

        record = _build_destination_record(
            function_arn="arn:aws:lambda:us-east-1:123:function:fn",
            payload={"key": "value"},
            is_success=True,
            result={"statusCode": 200},
            error=None,
        )
        assert record["version"] == "1.0"
        assert record["requestContext"]["condition"] == "Success"
        assert record["responsePayload"] == {"statusCode": 200}
        assert record["requestPayload"] == {"key": "value"}

    def test_build_destination_record_failure(self):
        from robotocore.services.lambda_.destinations import _build_destination_record

        record = _build_destination_record(
            function_arn="arn:aws:lambda:us-east-1:123:function:fn",
            payload={"key": "value"},
            is_success=False,
            result={"errorMessage": "boom"},
            error="Handled",
        )
        assert record["requestContext"]["condition"] == "RetriesExhausted"
        assert record["responseContext"]["functionError"] == "Handled"

    @patch("robotocore.services.lambda_.destinations._send_to_sqs")
    def test_dispatch_to_sqs(self, mock_sqs):
        from robotocore.services.lambda_.destinations import dispatch_destination

        dispatch_destination(
            destination_arn="arn:aws:sqs:us-east-1:123:my-queue",
            function_arn="arn:aws:lambda:us-east-1:123:function:fn",
            payload={},
            is_success=True,
            result={"ok": True},
            error=None,
            region="us-east-1",
            account_id="123",
        )
        mock_sqs.assert_called_once()

    @patch("robotocore.services.lambda_.destinations._send_to_sns")
    def test_dispatch_to_sns(self, mock_sns):
        from robotocore.services.lambda_.destinations import dispatch_destination

        dispatch_destination(
            destination_arn="arn:aws:sns:us-east-1:123:my-topic",
            function_arn="arn:aws:lambda:us-east-1:123:function:fn",
            payload={},
            is_success=False,
            result=None,
            error="Handled",
            region="us-east-1",
            account_id="123",
        )
        mock_sns.assert_called_once()

    @patch("robotocore.services.lambda_.destinations._send_to_lambda")
    def test_dispatch_to_lambda(self, mock_lambda):
        from robotocore.services.lambda_.destinations import dispatch_destination

        dispatch_destination(
            destination_arn="arn:aws:lambda:us-east-1:123:function:dest-fn",
            function_arn="arn:aws:lambda:us-east-1:123:function:fn",
            payload={},
            is_success=True,
            result="ok",
            error=None,
            region="us-east-1",
            account_id="123",
        )
        mock_lambda.assert_called_once()

    @patch("robotocore.services.lambda_.destinations._send_to_eventbridge")
    def test_dispatch_to_eventbridge(self, mock_eb):
        from robotocore.services.lambda_.destinations import dispatch_destination

        dispatch_destination(
            destination_arn="arn:aws:events:us-east-1:123:event-bus/default",
            function_arn="arn:aws:lambda:us-east-1:123:function:fn",
            payload={},
            is_success=True,
            result={},
            error=None,
            region="us-east-1",
            account_id="123",
        )
        mock_eb.assert_called_once()


# ============================================================
# Event Invoke Config (Moto-backed)
# ============================================================


@pytest.mark.asyncio
class TestEventInvokeConfig:
    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_put_event_invoke_config(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend.put_function_event_invoke_config.return_value = {
            "FunctionArn": "arn:aws:lambda:us-east-1:123:function:fn",
            "MaximumRetryAttempts": 1,
            "DestinationConfig": {
                "OnSuccess": {"Destination": "arn:aws:sqs:us-east-1:123:q"},
            },
        }
        mock_backend_fn.return_value = mock_backend

        body = json.dumps(
            {
                "MaximumRetryAttempts": 1,
                "DestinationConfig": {
                    "OnSuccess": {"Destination": "arn:aws:sqs:us-east-1:123:q"},
                },
            }
        ).encode()
        req = await _make_request("PUT", "/2015-03-31/functions/fn/event-invoke-config", body)
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["MaximumRetryAttempts"] == 1

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_get_event_invoke_config(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend.get_function_event_invoke_config.return_value = {
            "FunctionArn": "arn:aws:lambda:us-east-1:123:function:fn",
            "MaximumRetryAttempts": 2,
        }
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("GET", "/2015-03-31/functions/fn/event-invoke-config")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_delete_event_invoke_config(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("DELETE", "/2015-03-31/functions/fn/event-invoke-config")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204


# ============================================================
# Layers (Moto-backed)
# ============================================================


@pytest.mark.asyncio
class TestLayers:
    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_publish_layer_version(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_lv = MagicMock()
        mock_lv.get_layer_version.return_value = {
            "LayerVersionArn": "arn:aws:lambda:us-east-1:123:layer:my-layer:1",
            "Version": 1,
        }
        mock_backend.publish_layer_version.return_value = mock_lv
        mock_backend_fn.return_value = mock_backend

        body = json.dumps(
            {
                "Content": {"ZipFile": "UEsFBg..."},
                "CompatibleRuntimes": ["python3.12"],
            }
        ).encode()
        req = await _make_request("POST", "/2015-03-31/layers/my-layer/versions", body)
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 201
        data = json.loads(resp.body)
        assert data["Version"] == 1

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_list_layers(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend.list_layers.return_value = [{"LayerName": "layer1"}, {"LayerName": "layer2"}]
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("GET", "/2015-03-31/layers")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert len(data["Layers"]) == 2

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_list_layer_versions(self, mock_backend_fn):
        mock_backend = MagicMock()
        v1 = MagicMock()
        v1.get_layer_version.return_value = {"Version": 1}
        v2 = MagicMock()
        v2.get_layer_version.return_value = {"Version": 2}
        mock_backend.list_layer_versions.return_value = [v1, v2]
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("GET", "/2015-03-31/layers/my-layer/versions")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert len(data["LayerVersions"]) == 2

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_get_layer_version(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_lv = MagicMock()
        mock_lv.get_layer_version.return_value = {"Version": 1, "LayerVersionArn": "arn:..."}
        mock_backend.get_layer_version.return_value = mock_lv
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("GET", "/2015-03-31/layers/my-layer/versions/1")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_delete_layer_version(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        req = await _make_request("DELETE", "/2015-03-31/layers/my-layer/versions/1")
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204
        mock_backend.delete_layer_version.assert_called_once_with("my-layer", 1)


# ============================================================
# Event Source Mapping Enhancements
# ============================================================


@pytest.mark.asyncio
class TestESMEnhancements:
    def setup_method(self):
        _esm_store.clear()

    def teardown_method(self):
        _esm_store.clear()

    async def test_create_esm_with_filter_criteria(self):
        filter_criteria = {"Filters": [{"Pattern": '{"body": {"key": ["value1"]}}'}]}
        body = json.dumps(
            {
                "EventSourceArn": "arn:aws:sqs:us-east-1:123:my-queue",
                "FunctionName": "my-fn",
                "FilterCriteria": filter_criteria,
            }
        ).encode()

        with patch("robotocore.services.lambda_.event_source.get_engine") as me:
            me.return_value = MagicMock()
            req = await _make_request("POST", "/2015-03-31/event-source-mappings", body)
            resp = await handle_lambda_request(req, "us-east-1", "123456789012")

        assert resp.status_code == 202
        data = json.loads(resp.body)
        assert data["FilterCriteria"] == filter_criteria

    async def test_create_esm_with_bisect(self):
        body = json.dumps(
            {
                "EventSourceArn": "arn:aws:dynamodb:us-east-1:123:table/t/stream/s",
                "FunctionName": "fn",
                "BisectBatchOnFunctionError": True,
                "MaximumRetryAttempts": 2,
            }
        ).encode()

        with patch("robotocore.services.lambda_.event_source.get_engine") as me:
            me.return_value = MagicMock()
            req = await _make_request("POST", "/2015-03-31/event-source-mappings", body)
            resp = await handle_lambda_request(req, "us-east-1", "123456789012")

        assert resp.status_code == 202
        data = json.loads(resp.body)
        assert data["BisectBatchOnFunctionError"] is True
        assert data["MaximumRetryAttempts"] == 2

    async def test_update_esm_filter_criteria(self):
        _esm_store["uuid-1"] = {
            "UUID": "uuid-1",
            "EventSourceArn": "arn:aws:sqs:us-east-1:123:q",
            "FunctionArn": "arn:aws:lambda:us-east-1:123:function:fn",
            "State": "Enabled",
            "FilterCriteria": None,
            "BisectBatchOnFunctionError": False,
            "MaximumRetryAttempts": -1,
            "_region": "us-east-1",
            "_account_id": "123",
        }

        new_filter = {"Filters": [{"Pattern": '{"body": {"x": [1]}}'}]}
        body = json.dumps(
            {
                "FilterCriteria": new_filter,
                "BisectBatchOnFunctionError": True,
            }
        ).encode()

        req = await _make_request("PUT", "/2015-03-31/event-source-mappings/uuid-1", body)
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["FilterCriteria"] == new_filter
        assert data["BisectBatchOnFunctionError"] is True

    async def test_create_esm_with_function_response_types(self):
        body = json.dumps(
            {
                "EventSourceArn": "arn:aws:sqs:us-east-1:123:q",
                "FunctionName": "fn",
                "FunctionResponseTypes": ["ReportBatchItemFailures"],
            }
        ).encode()

        with patch("robotocore.services.lambda_.event_source.get_engine") as me:
            me.return_value = MagicMock()
            req = await _make_request("POST", "/2015-03-31/event-source-mappings", body)
            resp = await handle_lambda_request(req, "us-east-1", "123456789012")

        assert resp.status_code == 202
        data = json.loads(resp.body)
        assert "ReportBatchItemFailures" in data["FunctionResponseTypes"]


# ============================================================
# Filter Criteria Matching
# ============================================================


class TestFilterCriteriaMatching:
    def test_no_filter_matches_all(self):
        from robotocore.services.lambda_.event_source import matches_filter_criteria

        assert matches_filter_criteria({"body": "hello"}, None) is True
        assert matches_filter_criteria({"body": "hello"}, {}) is True
        assert matches_filter_criteria({"body": "hello"}, {"Filters": []}) is True

    def test_exact_match(self):
        from robotocore.services.lambda_.event_source import matches_filter_criteria

        criteria = {"Filters": [{"Pattern": '{"eventSource": ["aws:sqs"]}'}]}
        assert matches_filter_criteria({"eventSource": "aws:sqs"}, criteria) is True
        assert matches_filter_criteria({"eventSource": "aws:kinesis"}, criteria) is False

    def test_nested_match(self):
        from robotocore.services.lambda_.event_source import matches_filter_criteria

        criteria = {
            "Filters": [{"Pattern": '{"dynamodb": {"NewImage": {"status": {"S": ["ACTIVE"]}}}}'}]
        }
        record = {
            "dynamodb": {"NewImage": {"status": {"S": "ACTIVE"}}},
        }
        assert matches_filter_criteria(record, criteria) is True

        record2 = {
            "dynamodb": {"NewImage": {"status": {"S": "INACTIVE"}}},
        }
        assert matches_filter_criteria(record2, criteria) is False

    def test_prefix_match(self):
        from robotocore.services.lambda_.event_source import matches_filter_criteria

        criteria = {"Filters": [{"Pattern": '{"body": [{"prefix": "hello"}]}'}]}
        assert matches_filter_criteria({"body": "hello world"}, criteria) is True
        assert matches_filter_criteria({"body": "goodbye"}, criteria) is False

    def test_numeric_match(self):
        from robotocore.services.lambda_.event_source import matches_filter_criteria

        criteria = {"Filters": [{"Pattern": '{"age": [{"numeric": [">=", 18, "<", 65]}]}'}]}
        assert matches_filter_criteria({"age": 25}, criteria) is True
        assert matches_filter_criteria({"age": 10}, criteria) is False
        assert matches_filter_criteria({"age": 65}, criteria) is False

    def test_exists_match(self):
        from robotocore.services.lambda_.event_source import matches_filter_criteria

        criteria = {"Filters": [{"Pattern": '{"name": [{"exists": true}]}'}]}
        assert matches_filter_criteria({"name": "Alice"}, criteria) is True
        assert matches_filter_criteria({"age": 25}, criteria) is False

    def test_exists_false_match(self):
        from robotocore.services.lambda_.event_source import matches_filter_criteria

        criteria = {"Filters": [{"Pattern": '{"deleted": [{"exists": false}]}'}]}
        assert matches_filter_criteria({"name": "Alice"}, criteria) is True
        assert matches_filter_criteria({"deleted": True}, criteria) is False

    def test_anything_but_match(self):
        from robotocore.services.lambda_.event_source import matches_filter_criteria

        criteria = {"Filters": [{"Pattern": '{"status": [{"anything-but": ["DELETED"]}]}'}]}
        assert matches_filter_criteria({"status": "ACTIVE"}, criteria) is True
        assert matches_filter_criteria({"status": "DELETED"}, criteria) is False

    def test_multiple_filters_or_logic(self):
        from robotocore.services.lambda_.event_source import matches_filter_criteria

        criteria = {
            "Filters": [
                {"Pattern": '{"type": ["A"]}'},
                {"Pattern": '{"type": ["B"]}'},
            ]
        }
        assert matches_filter_criteria({"type": "A"}, criteria) is True
        assert matches_filter_criteria({"type": "B"}, criteria) is True
        assert matches_filter_criteria({"type": "C"}, criteria) is False

    def test_empty_pattern_matches_all(self):
        from robotocore.services.lambda_.event_source import matches_filter_criteria

        criteria = {"Filters": [{"Pattern": "{}"}]}
        assert matches_filter_criteria({"any": "thing"}, criteria) is True


# ============================================================
# Dead Letter Queue
# ============================================================


class TestDLQ:
    def setup_method(self):
        _dlq_configs.clear()

    def teardown_method(self):
        _dlq_configs.clear()

    def test_store_and_get_dlq_config(self):
        from robotocore.services.lambda_.provider import (
            _get_dlq_config,
            _store_dlq_config,
        )

        _store_dlq_config(
            "123",
            "us-east-1",
            "my-fn",
            {"TargetArn": "arn:aws:sqs:us-east-1:123:dlq"},
        )
        config = _get_dlq_config("123", "us-east-1", "my-fn")
        assert config is not None
        assert config["TargetArn"] == "arn:aws:sqs:us-east-1:123:dlq"

    def test_store_empty_dlq_removes(self):
        from robotocore.services.lambda_.provider import (
            _get_dlq_config,
            _store_dlq_config,
        )

        _store_dlq_config("123", "us-east-1", "fn", {"TargetArn": "arn:..."})
        _store_dlq_config("123", "us-east-1", "fn", {})
        assert _get_dlq_config("123", "us-east-1", "fn") is None

    def test_get_dlq_config_not_found(self):
        from robotocore.services.lambda_.provider import _get_dlq_config

        assert _get_dlq_config("123", "us-east-1", "nonexistent") is None

    @patch("robotocore.services.sqs.provider._get_store")
    def test_dispatch_to_dlq_sqs(self, mock_get_store):
        from robotocore.services.lambda_.provider import (
            _store_dlq_config,
            dispatch_to_dlq,
        )

        mock_queue = MagicMock()
        mock_store = MagicMock()
        mock_store.get_queue.return_value = mock_queue
        mock_get_store.return_value = mock_store

        _store_dlq_config(
            "123",
            "us-east-1",
            "fn",
            {"TargetArn": "arn:aws:sqs:us-east-1:123:dlq-queue"},
        )

        dispatch_to_dlq(
            func_name="fn",
            payload={"key": "value"},
            error="Handled",
            region="us-east-1",
            account_id="123",
        )

        mock_queue.put.assert_called_once()
        msg = mock_queue.put.call_args[0][0]
        data = json.loads(msg.body)
        assert data["errorMessage"] == "Handled"
        assert data["requestPayload"] == {"key": "value"}

    def test_dispatch_to_dlq_no_config(self):
        from robotocore.services.lambda_.provider import dispatch_to_dlq

        # Should not raise
        dispatch_to_dlq(
            func_name="fn",
            payload={},
            error="err",
            region="us-east-1",
            account_id="123",
        )


# ============================================================
# Provisioned Concurrency
# ============================================================


@pytest.mark.asyncio
class TestProvisionedConcurrency:
    def setup_method(self):
        _provisioned_concurrency.clear()

    def teardown_method(self):
        _provisioned_concurrency.clear()

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_put_provisioned_concurrency(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        body = json.dumps({"ProvisionedConcurrentExecutions": 10}).encode()
        req = await _make_request(
            "PUT",
            "/2015-03-31/functions/fn/provisioned-concurrency",
            body,
            query_string="Qualifier=1",
        )
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 202
        data = json.loads(resp.body)
        assert data["RequestedProvisionedConcurrentExecutions"] == 10
        assert data["Status"] == "READY"

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_get_provisioned_concurrency(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        # Put first
        body = json.dumps({"ProvisionedConcurrentExecutions": 5}).encode()
        req = await _make_request(
            "PUT",
            "/2015-03-31/functions/fn/provisioned-concurrency",
            body,
            query_string="Qualifier=1",
        )
        await handle_lambda_request(req, "us-east-1", "123456789012")

        # Get
        req = await _make_request(
            "GET",
            "/2015-03-31/functions/fn/provisioned-concurrency",
            query_string="Qualifier=1",
        )
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["RequestedProvisionedConcurrentExecutions"] == 5

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_get_provisioned_concurrency_not_found(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        req = await _make_request(
            "GET",
            "/2015-03-31/functions/fn/provisioned-concurrency",
            query_string="Qualifier=99",
        )
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_delete_provisioned_concurrency(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        # Put first
        body = json.dumps({"ProvisionedConcurrentExecutions": 5}).encode()
        req = await _make_request(
            "PUT",
            "/2015-03-31/functions/fn/provisioned-concurrency",
            body,
            query_string="Qualifier=1",
        )
        await handle_lambda_request(req, "us-east-1", "123456789012")

        # Delete
        req = await _make_request(
            "DELETE",
            "/2015-03-31/functions/fn/provisioned-concurrency",
            query_string="Qualifier=1",
        )
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204

        # Verify gone
        req = await _make_request(
            "GET",
            "/2015-03-31/functions/fn/provisioned-concurrency",
            query_string="Qualifier=1",
        )
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_delete_provisioned_concurrency_not_found(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend_fn.return_value = mock_backend

        req = await _make_request(
            "DELETE",
            "/2015-03-31/functions/fn/provisioned-concurrency",
            query_string="Qualifier=99",
        )
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404

    def test_list_provisioned_concurrency_configs(self):
        from robotocore.services.lambda_.provider import (
            list_provisioned_concurrency_configs,
        )

        _provisioned_concurrency[("123", "us-east-1", "fn", "1")] = {
            "RequestedProvisionedConcurrentExecutions": 5,
            "Status": "READY",
        }
        _provisioned_concurrency[("123", "us-east-1", "fn", "2")] = {
            "RequestedProvisionedConcurrentExecutions": 10,
            "Status": "READY",
        }
        _provisioned_concurrency[("123", "us-east-1", "other-fn", "1")] = {
            "RequestedProvisionedConcurrentExecutions": 3,
            "Status": "READY",
        }

        result = list_provisioned_concurrency_configs("fn", "us-east-1", "123")
        assert len(result) == 2


# ============================================================
# Async Invocation with Destinations
# ============================================================


@pytest.mark.asyncio
class TestAsyncInvokeDestinations:
    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_async_invoke_dispatches_on_success(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_fn = MagicMock()
        mock_fn.run_time = ""
        mock_fn.code = None
        mock_fn.code_bytes = None
        mock_fn.handler = "lambda_function.handler"
        mock_fn.timeout = 3
        mock_fn.memory_size = 128
        mock_fn.environment_vars = {}
        mock_fn.function_arn = "arn:aws:lambda:us-east-1:123:function:fn"
        mock_backend.get_function.return_value = mock_fn
        mock_backend.get_function_event_invoke_config.return_value = {
            "DestinationConfig": {
                "OnSuccess": {"Destination": "arn:aws:sqs:us-east-1:123:dest-q"},
            },
        }
        mock_backend_fn.return_value = mock_backend

        with patch(
            "robotocore.services.lambda_.destinations.dispatch_destination"
        ) as mock_dispatch:
            body = json.dumps({"key": "value"}).encode()
            req = await _make_request(
                "POST",
                "/2015-03-31/functions/fn/invocations",
                body,
                headers={"x-amz-invocation-type": "Event"},
            )
            resp = await handle_lambda_request(req, "us-east-1", "123456789012")

            assert resp.status_code == 202
            mock_dispatch.assert_called_once()
            call_kwargs = mock_dispatch.call_args[1]
            assert call_kwargs["destination_arn"] == "arn:aws:sqs:us-east-1:123:dest-q"
            assert call_kwargs["is_success"] is True


# ============================================================
# Function URL store directly
# ============================================================


class TestFunctionUrlStore:
    def setup_method(self):
        from robotocore.services.lambda_.urls import clear_store

        clear_store()

    def teardown_method(self):
        from robotocore.services.lambda_.urls import clear_store

        clear_store()

    def test_create_and_get(self):
        from robotocore.services.lambda_.urls import (
            create_function_url_config,
            get_function_url_config,
        )

        config = create_function_url_config("fn", "us-east-1", "123", {"AuthType": "NONE"})
        assert config["AuthType"] == "NONE"
        assert "lambda-url" in config["FunctionUrl"]

        retrieved = get_function_url_config("fn", "us-east-1", "123")
        assert retrieved["FunctionUrl"] == config["FunctionUrl"]

    def test_create_duplicate_raises(self):
        from robotocore.services.lambda_.urls import (
            FunctionUrlConfigExistsError,
            create_function_url_config,
        )

        create_function_url_config("fn", "us-east-1", "123", {"AuthType": "NONE"})
        with pytest.raises(FunctionUrlConfigExistsError):
            create_function_url_config("fn", "us-east-1", "123", {"AuthType": "NONE"})

    def test_get_not_found_raises(self):
        from robotocore.services.lambda_.urls import (
            FunctionUrlConfigNotFoundError,
            get_function_url_config,
        )

        with pytest.raises(FunctionUrlConfigNotFoundError):
            get_function_url_config("nope", "us-east-1", "123")

    def test_update(self):
        from robotocore.services.lambda_.urls import (
            create_function_url_config,
            update_function_url_config,
        )

        create_function_url_config("fn", "us-east-1", "123", {"AuthType": "NONE"})
        updated = update_function_url_config("fn", "us-east-1", "123", {"AuthType": "IAM"})
        assert updated["AuthType"] == "IAM"

    def test_update_not_found_raises(self):
        from robotocore.services.lambda_.urls import (
            FunctionUrlConfigNotFoundError,
            update_function_url_config,
        )

        with pytest.raises(FunctionUrlConfigNotFoundError):
            update_function_url_config("nope", "us-east-1", "123", {})

    def test_delete(self):
        from robotocore.services.lambda_.urls import (
            FunctionUrlConfigNotFoundError,
            create_function_url_config,
            delete_function_url_config,
            get_function_url_config,
        )

        create_function_url_config("fn", "us-east-1", "123", {"AuthType": "NONE"})
        delete_function_url_config("fn", "us-east-1", "123")
        with pytest.raises(FunctionUrlConfigNotFoundError):
            get_function_url_config("fn", "us-east-1", "123")

    def test_delete_not_found_raises(self):
        from robotocore.services.lambda_.urls import (
            FunctionUrlConfigNotFoundError,
            delete_function_url_config,
        )

        with pytest.raises(FunctionUrlConfigNotFoundError):
            delete_function_url_config("nope", "us-east-1", "123")

    def test_get_all_url_configs(self):
        from robotocore.services.lambda_.urls import (
            create_function_url_config,
            get_all_url_configs,
        )

        create_function_url_config("fn1", "us-east-1", "123", {"AuthType": "NONE"})
        create_function_url_config("fn2", "us-east-1", "123", {"AuthType": "IAM"})
        configs = get_all_url_configs()
        assert len(configs) == 2

    def test_find_function_by_url(self):
        from robotocore.services.lambda_.urls import (
            create_function_url_config,
            find_function_by_url,
        )

        config = create_function_url_config("fn", "us-east-1", "123", {"AuthType": "NONE"})
        url = config["FunctionUrl"]
        host = url.replace("https://", "").replace("/", "")
        found = find_function_by_url(host)
        assert found is not None
        assert found["FunctionArn"] == config["FunctionArn"]

    def test_find_function_by_url_not_found(self):
        from robotocore.services.lambda_.urls import find_function_by_url

        assert find_function_by_url("nonexistent.lambda-url.us-east-1.on.aws") is None
