"""Unit tests for SQS provider request handling."""

import json
from unittest.mock import MagicMock, patch
from urllib.parse import urlencode

import pytest
from starlette.requests import Request

from robotocore.services.sqs.models import SqsStore
from robotocore.services.sqs.provider import (
    SqsError,
    _create_queue,
    _delete_queue,
    _error,
    _get_queue_attributes,
    _get_queue_url,
    _json_response,
    _list_queues,
    _md5,
    _parse_message_attributes,
    _resolve_queue,
    _send_message,
    _xml_response,
    handle_sqs_request,
)


def _make_scope(
    method: str = "POST",
    path: str = "/",
    query_string: bytes = b"",
    headers: dict | None = None,
):
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


def _make_request(body=b"", headers=None, path="/", query_string=b""):
    scope = _make_scope(headers=headers, path=path, query_string=query_string)

    async def receive():
        return {"type": "http.request", "body": body}

    return Request(scope, receive)


def _store_with_queue(name="test-queue"):
    store = SqsStore()
    store.create_queue(name, "us-east-1", "123456789012")
    return store


class TestResponseHelpers:
    def test_json_response(self):
        resp = _json_response({"QueueUrl": "http://localhost/q"})
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["QueueUrl"] == "http://localhost/q"
        assert resp.media_type == "application/x-amz-json-1.0"

    def test_xml_response(self):
        resp = _xml_response("CreateQueueResponse", {"QueueUrl": "http://localhost/q"})
        assert resp.status_code == 200
        assert b"CreateQueueResult" in resp.body
        assert b"http://localhost/q" in resp.body

    def test_error_json(self):
        resp = _error("NonExistentQueue", "no queue", 400, use_json=True)
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "NonExistentQueue"

    def test_error_xml(self):
        resp = _error("NonExistentQueue", "no queue", 400, use_json=False)
        assert resp.status_code == 400
        assert b"<Code>NonExistentQueue</Code>" in resp.body

    def test_md5_helper(self):
        import hashlib

        expected = hashlib.md5(b"hello").hexdigest()
        assert _md5("hello") == expected


class TestResolveQueue:
    def test_resolve_by_url(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        queue = _resolve_queue(
            store,
            {"QueueUrl": "http://localhost:4566/123456789012/test-queue"},
            mock_req,
        )
        assert queue.name == "test-queue"

    def test_resolve_by_path(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        mock_req.url.path = "/123456789012/test-queue"
        queue = _resolve_queue(store, {}, mock_req)
        assert queue.name == "test-queue"

    def test_resolve_not_found(self):
        store = SqsStore()
        mock_req = MagicMock()
        mock_req.url.path = "/"
        with pytest.raises(SqsError) as exc_info:
            _resolve_queue(store, {}, mock_req)
        assert "NonExistentQueue" in exc_info.value.code


class TestParseMessageAttributes:
    def test_query_protocol_attributes(self):
        from robotocore.services.sqs.models import SqsMessage

        msg = SqsMessage(message_id="m1", body="hello", md5_of_body="abc")
        params = {
            "MessageAttribute.1.Name": "attr1",
            "MessageAttribute.1.Value.DataType": "String",
            "MessageAttribute.1.Value.StringValue": "value1",
        }
        _parse_message_attributes(params, msg)
        assert "attr1" in msg.message_attributes
        assert msg.message_attributes["attr1"]["StringValue"] == "value1"

    def test_json_protocol_attributes(self):
        from robotocore.services.sqs.models import SqsMessage

        msg = SqsMessage(message_id="m1", body="hello", md5_of_body="abc")
        params = {"MessageAttributes": {"key1": {"DataType": "String", "StringValue": "v1"}}}
        _parse_message_attributes(params, msg)
        assert msg.message_attributes["key1"]["StringValue"] == "v1"


class TestActions:
    def test_create_queue(self):
        store = SqsStore()
        mock_req = MagicMock()
        result = _create_queue(store, {"QueueName": "q1"}, "us-east-1", "123", mock_req)
        assert "QueueUrl" in result
        assert "q1" in result["QueueUrl"]

    def test_delete_queue(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        _delete_queue(
            store,
            {"QueueUrl": "http://localhost:4566/123456789012/test-queue"},
            "us-east-1",
            "123",
            mock_req,
        )
        assert store.get_queue("test-queue") is None

    def test_list_queues(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        result = _list_queues(store, {}, "us-east-1", "123", mock_req)
        assert len(result["QueueUrls"]) == 1

    def test_list_queues_with_prefix(self):
        store = SqsStore()
        store.create_queue("test-a", "us-east-1", "123")
        store.create_queue("other-b", "us-east-1", "123")
        mock_req = MagicMock()
        result = _list_queues(store, {"QueueNamePrefix": "test"}, "us-east-1", "123", mock_req)
        assert len(result["QueueUrls"]) == 1

    def test_get_queue_url(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        result = _get_queue_url(store, {"QueueName": "test-queue"}, "us-east-1", "123", mock_req)
        assert "test-queue" in result["QueueUrl"]

    def test_get_queue_url_not_found(self):
        store = SqsStore()
        mock_req = MagicMock()
        with pytest.raises(SqsError):
            _get_queue_url(store, {"QueueName": "nope"}, "us-east-1", "123", mock_req)

    def test_send_message(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        mock_req.url.path = "/123456789012/test-queue"
        result = _send_message(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "MessageBody": "hello world",
            },
            "us-east-1",
            "123",
            mock_req,
        )
        assert "MessageId" in result
        assert "MD5OfMessageBody" in result

    def test_get_queue_attributes(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        result = _get_queue_attributes(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
            },
            "us-east-1",
            "123",
            mock_req,
        )
        attrs = result["Attributes"]
        assert "QueueArn" in attrs
        assert "ApproximateNumberOfMessages" in attrs


@pytest.mark.asyncio
class TestHandleSqsRequest:
    async def test_json_protocol_create_queue(self):
        body = json.dumps({"QueueName": "myq"}).encode()
        headers = {
            "content-type": "application/x-amz-json-1.0",
            "x-amz-target": "AmazonSQS.CreateQueue",
        }
        req = _make_request(body=body, headers=headers)

        with (
            patch("robotocore.services.sqs.provider._get_store") as mock_get,
            patch("robotocore.services.sqs.provider._ensure_worker"),
        ):
            store = SqsStore()
            mock_get.return_value = store
            resp = await handle_sqs_request(req, "us-east-1", "123456789012")

        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert "QueueUrl" in data

    async def test_query_protocol_create_queue(self):
        form = urlencode({"Action": "CreateQueue", "QueueName": "legacy-q"}).encode()
        headers = {"content-type": "application/x-www-form-urlencoded"}
        req = _make_request(body=form, headers=headers)

        with (
            patch("robotocore.services.sqs.provider._get_store") as mock_get,
            patch("robotocore.services.sqs.provider._ensure_worker"),
        ):
            store = SqsStore()
            mock_get.return_value = store
            resp = await handle_sqs_request(req, "us-east-1", "123456789012")

        assert resp.status_code == 200
        assert b"CreateQueueResult" in resp.body

    async def test_unknown_action(self):
        body = json.dumps({}).encode()
        headers = {
            "content-type": "application/x-amz-json-1.0",
            "x-amz-target": "AmazonSQS.BogusAction",
        }
        req = _make_request(body=body, headers=headers)

        with (
            patch("robotocore.services.sqs.provider._get_store") as mock_get,
            patch("robotocore.services.sqs.provider._ensure_worker"),
        ):
            mock_get.return_value = SqsStore()
            resp = await handle_sqs_request(req, "us-east-1", "123456789012")

        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "InvalidAction"

    async def test_sqs_error_handling(self):
        body = json.dumps({"QueueName": "nonexistent"}).encode()
        headers = {
            "content-type": "application/x-amz-json-1.0",
            "x-amz-target": "AmazonSQS.GetQueueUrl",
        }
        req = _make_request(body=body, headers=headers)

        with (
            patch("robotocore.services.sqs.provider._get_store") as mock_get,
            patch("robotocore.services.sqs.provider._ensure_worker"),
        ):
            mock_get.return_value = SqsStore()
            resp = await handle_sqs_request(req, "us-east-1", "123456789012")

        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert "NonExistentQueue" in data["__type"]


class TestSqsError:
    def test_error_attributes(self):
        err = SqsError("MyCode", "my message", 404)
        assert err.code == "MyCode"
        assert err.message == "my message"
        assert err.status == 404

    def test_default_status(self):
        err = SqsError("MyCode", "msg")
        assert err.status == 400
