"""Unit tests for the DynamoDB provider (stream mutation hooks)."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.dynamodb.provider import (
    _MUTATION_OPS,
    _extract_keys_from_item,
    _fire_stream_hooks,
    handle_dynamodb_request,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(target: str, body: dict) -> Request:
    """Build a fake Starlette Request with the given X-Amz-Target and JSON body."""
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "query_string": b"",
        "headers": [(b"x-amz-target", target.encode())],
    }
    body_bytes = json.dumps(body).encode()

    async def receive():
        return {"type": "http.request", "body": body_bytes}

    return Request(scope, receive)


def _mock_moto_response(status_code: int = 200, body: bytes = b"{}") -> Response:
    return Response(content=body, status_code=status_code)


# ---------------------------------------------------------------------------
# _MUTATION_OPS constant
# ---------------------------------------------------------------------------


class TestMutationOps:
    def test_contains_put_item(self):
        assert "DynamoDB_20120810.PutItem" in _MUTATION_OPS

    def test_contains_delete_item(self):
        assert "DynamoDB_20120810.DeleteItem" in _MUTATION_OPS

    def test_contains_update_item(self):
        assert "DynamoDB_20120810.UpdateItem" in _MUTATION_OPS

    def test_contains_batch_write(self):
        assert "DynamoDB_20120810.BatchWriteItem" in _MUTATION_OPS

    def test_contains_transact_write(self):
        assert "DynamoDB_20120810.TransactWriteItems" in _MUTATION_OPS

    def test_does_not_contain_get_item(self):
        assert "DynamoDB_20120810.GetItem" not in _MUTATION_OPS


# ---------------------------------------------------------------------------
# handle_dynamodb_request
# ---------------------------------------------------------------------------


class TestHandleDynamoDBRequest:
    @pytest.mark.asyncio
    async def test_forwards_to_moto(self):
        """All requests are forwarded to Moto."""
        req = _make_request("DynamoDB_20120810.GetItem", {"TableName": "t"})
        mock_resp = _mock_moto_response(200)
        with patch(
            "robotocore.services.dynamodb.provider.forward_to_moto",
            new_callable=AsyncMock,
            return_value=mock_resp,
        ) as mock_fwd:
            resp = await handle_dynamodb_request(req, "us-east-1", "123456789012")
        mock_fwd.assert_awaited_once()
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_fires_hooks_on_mutation_success(self):
        """Stream hooks fire on successful mutation operations."""
        body = {"TableName": "mytable", "Item": {"id": {"S": "1"}}}
        req = _make_request("DynamoDB_20120810.PutItem", body)
        mock_resp = _mock_moto_response(200)
        with (
            patch(
                "robotocore.services.dynamodb.provider.forward_to_moto",
                new_callable=AsyncMock,
                return_value=mock_resp,
            ),
            patch("robotocore.services.dynamodb.provider._fire_stream_hooks") as mock_hooks,
        ):
            resp = await handle_dynamodb_request(req, "us-east-1", "123456789012")
        mock_hooks.assert_called_once()
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_no_hooks_on_non_mutation(self):
        """Stream hooks do NOT fire for read operations."""
        req = _make_request("DynamoDB_20120810.GetItem", {"TableName": "t"})
        mock_resp = _mock_moto_response(200)
        with (
            patch(
                "robotocore.services.dynamodb.provider.forward_to_moto",
                new_callable=AsyncMock,
                return_value=mock_resp,
            ),
            patch("robotocore.services.dynamodb.provider._fire_stream_hooks") as mock_hooks,
        ):
            await handle_dynamodb_request(req, "us-east-1", "123456789012")
        mock_hooks.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_hooks_on_mutation_error(self):
        """Stream hooks do NOT fire when Moto returns an error status."""
        body = {"TableName": "t", "Item": {"id": {"S": "1"}}}
        req = _make_request("DynamoDB_20120810.PutItem", body)
        mock_resp = _mock_moto_response(400)
        with (
            patch(
                "robotocore.services.dynamodb.provider.forward_to_moto",
                new_callable=AsyncMock,
                return_value=mock_resp,
            ),
            patch("robotocore.services.dynamodb.provider._fire_stream_hooks") as mock_hooks,
        ):
            await handle_dynamodb_request(req, "us-east-1", "123456789012")
        mock_hooks.assert_not_called()

    @pytest.mark.asyncio
    async def test_hook_exception_does_not_propagate(self):
        """If _fire_stream_hooks raises, the response is still returned."""
        body = {"TableName": "t", "Item": {"id": {"S": "1"}}}
        req = _make_request("DynamoDB_20120810.PutItem", body)
        mock_resp = _mock_moto_response(200)
        with (
            patch(
                "robotocore.services.dynamodb.provider.forward_to_moto",
                new_callable=AsyncMock,
                return_value=mock_resp,
            ),
            patch(
                "robotocore.services.dynamodb.provider._fire_stream_hooks",
                side_effect=RuntimeError("boom"),
            ),
        ):
            resp = await handle_dynamodb_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# _fire_stream_hooks
# ---------------------------------------------------------------------------


class TestFireStreamHooks:
    def test_put_item_calls_notify(self):
        body = json.dumps({"TableName": "t1", "Item": {"id": {"S": "x"}}}).encode()
        with patch("robotocore.services.dynamodbstreams.hooks.notify_table_change") as mock_notify:
            _fire_stream_hooks("DynamoDB_20120810.PutItem", body, "us-east-1", "123456789012")
        mock_notify.assert_called_once()
        call_kw = mock_notify.call_args[1]
        assert call_kw["table_name"] == "t1"
        assert call_kw["event_name"] == "INSERT"

    def test_delete_item_calls_notify(self):
        body = json.dumps({"TableName": "t1", "Key": {"id": {"S": "x"}}}).encode()
        with patch("robotocore.services.dynamodbstreams.hooks.notify_table_change") as mock_notify:
            _fire_stream_hooks("DynamoDB_20120810.DeleteItem", body, "us-east-1", "123456789012")
        mock_notify.assert_called_once()
        assert mock_notify.call_args[1]["event_name"] == "REMOVE"

    def test_update_item_calls_notify(self):
        body = json.dumps({"TableName": "t1", "Key": {"id": {"S": "x"}}}).encode()
        with patch("robotocore.services.dynamodbstreams.hooks.notify_table_change") as mock_notify:
            _fire_stream_hooks("DynamoDB_20120810.UpdateItem", body, "us-east-1", "123456789012")
        mock_notify.assert_called_once()
        assert mock_notify.call_args[1]["event_name"] == "MODIFY"

    def test_batch_write_item_fires_multiple(self):
        body = json.dumps(
            {
                "RequestItems": {
                    "t1": [
                        {"PutRequest": {"Item": {"id": {"S": "1"}}}},
                        {"DeleteRequest": {"Key": {"id": {"S": "2"}}}},
                    ]
                }
            }
        ).encode()
        with patch("robotocore.services.dynamodbstreams.hooks.notify_table_change") as mock_notify:
            _fire_stream_hooks(
                "DynamoDB_20120810.BatchWriteItem",
                body,
                "us-east-1",
                "123456789012",
            )
        assert mock_notify.call_count == 2
        events = [c[1]["event_name"] for c in mock_notify.call_args_list]
        assert "INSERT" in events
        assert "REMOVE" in events

    def test_transact_write_items_fires_for_each_op(self):
        body = json.dumps(
            {
                "TransactItems": [
                    {"Put": {"TableName": "t1", "Item": {"id": {"S": "a"}}}},
                    {"Delete": {"TableName": "t2", "Key": {"id": {"S": "b"}}}},
                    {"Update": {"TableName": "t3", "Key": {"id": {"S": "c"}}}},
                ]
            }
        ).encode()
        with patch("robotocore.services.dynamodbstreams.hooks.notify_table_change") as mock_notify:
            _fire_stream_hooks(
                "DynamoDB_20120810.TransactWriteItems",
                body,
                "us-east-1",
                "123456789012",
            )
        assert mock_notify.call_count == 3


# ---------------------------------------------------------------------------
# _extract_keys_from_item
# ---------------------------------------------------------------------------


class TestExtractKeysFromItem:
    def test_extracts_hash_and_range_keys(self):
        mock_table = MagicMock()
        mock_table.hash_key_attr = "pk"
        mock_table.range_key_attr = "sk"
        mock_backend = MagicMock()
        mock_backend.get_table.return_value = mock_table

        mock_get = MagicMock()
        mock_get.return_value.__getitem__ = MagicMock(
            return_value=MagicMock(__getitem__=MagicMock(return_value=mock_backend))
        )

        item = {"pk": {"S": "val1"}, "sk": {"S": "val2"}, "data": {"S": "extra"}}
        with patch("moto.backends.get_backend", mock_get):
            keys = _extract_keys_from_item("t", item, "us-east-1", "123456789012")
        assert keys == {"pk": {"S": "val1"}, "sk": {"S": "val2"}}

    def test_hash_key_only(self):
        mock_table = MagicMock()
        mock_table.hash_key_attr = "pk"
        mock_table.range_key_attr = None
        mock_backend = MagicMock()
        mock_backend.get_table.return_value = mock_table

        mock_get = MagicMock()
        mock_get.return_value.__getitem__ = MagicMock(
            return_value=MagicMock(__getitem__=MagicMock(return_value=mock_backend))
        )

        item = {"pk": {"S": "val1"}, "data": {"S": "extra"}}
        with patch("moto.backends.get_backend", mock_get):
            keys = _extract_keys_from_item("t", item, "us-east-1", "123456789012")
        assert keys == {"pk": {"S": "val1"}}

    def test_fallback_returns_full_item_on_error(self):
        """When Moto backend is unavailable, return full item as keys."""
        with patch("moto.backends.get_backend", side_effect=Exception("no backend")):
            item = {"id": {"S": "1"}, "name": {"S": "foo"}}
            keys = _extract_keys_from_item("t", item, "us-east-1", "123456789012")
        assert keys == item


# ---------------------------------------------------------------------------
# Error-path tests for intercepted operations
# ---------------------------------------------------------------------------


class TestDynamoDBErrorPaths:
    @pytest.mark.asyncio
    async def test_describe_nonexistent_global_table(self):
        """DescribeGlobalTable on a non-existent table returns GlobalTableNotFoundException."""
        from robotocore.services.dynamodb.provider import _global_tables

        _global_tables.clear()
        req = _make_request(
            "DynamoDB_20120810.DescribeGlobalTable",
            {"GlobalTableName": "does-not-exist"},
        )
        resp = await handle_dynamodb_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert body["__type"] == "GlobalTableNotFoundException"
        assert "does-not-exist" in body["message"]
        _global_tables.clear()

    @pytest.mark.asyncio
    async def test_create_duplicate_global_table(self):
        """CreateGlobalTable with an existing name returns GlobalTableAlreadyExistsException."""
        from robotocore.services.dynamodb.provider import _global_tables

        _global_tables.clear()
        # Create the table first
        req1 = _make_request(
            "DynamoDB_20120810.CreateGlobalTable",
            {"GlobalTableName": "my-global", "ReplicationGroup": []},
        )
        resp1 = await handle_dynamodb_request(req1, "us-east-1", "123456789012")
        assert resp1.status_code == 200

        # Try to create again -- should fail
        req2 = _make_request(
            "DynamoDB_20120810.CreateGlobalTable",
            {"GlobalTableName": "my-global", "ReplicationGroup": []},
        )
        resp2 = await handle_dynamodb_request(req2, "us-east-1", "123456789012")
        assert resp2.status_code == 400
        body = json.loads(resp2.body)
        assert body["__type"] == "GlobalTableAlreadyExistsException"
        _global_tables.clear()

    @pytest.mark.asyncio
    async def test_moto_error_passthrough(self):
        """When Moto returns an error (e.g., 400 ResourceNotFoundException), it is passed through."""
        error_body = json.dumps(
            {
                "__type": "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
                "message": "Requested resource not found: Table: nonexistent not found",
            }
        ).encode()
        req = _make_request(
            "DynamoDB_20120810.DescribeTable",
            {"TableName": "nonexistent"},
        )
        mock_resp = _mock_moto_response(400, error_body)
        with patch(
            "robotocore.services.dynamodb.provider.forward_to_moto",
            new_callable=AsyncMock,
            return_value=mock_resp,
        ):
            resp = await handle_dynamodb_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert "ResourceNotFoundException" in body["__type"]

    @pytest.mark.asyncio
    async def test_moto_validation_error_passthrough(self):
        """Moto validation errors (e.g., missing required fields) pass through unchanged."""
        error_body = json.dumps(
            {
                "__type": "com.amazonaws.dynamodb.v20120810#ValidationException",
                "message": "1 validation error detected: Value null at 'tableName' failed",
            }
        ).encode()
        req = _make_request("DynamoDB_20120810.DescribeTable", {})
        mock_resp = _mock_moto_response(400, error_body)
        with patch(
            "robotocore.services.dynamodb.provider.forward_to_moto",
            new_callable=AsyncMock,
            return_value=mock_resp,
        ):
            resp = await handle_dynamodb_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert "ValidationException" in body["__type"]
