"""Unit tests for SQS provider request handling."""

import hashlib
import json
import struct
from unittest.mock import MagicMock, patch
from urllib.parse import urlencode

import pytest
from starlette.requests import Request

from robotocore.services.sqs.models import SqsStore
from robotocore.services.sqs.provider import (
    SqsError,
    _add_permission,
    _change_message_visibility,
    _create_queue,
    _delete_queue,
    _error,
    _get_queue_attributes,
    _get_queue_url,
    _json_response,
    _list_dead_letter_source_queues,
    _list_queue_tags,
    _list_queues,
    _md5,
    _md5_message_attributes,
    _parse_message_attributes,
    _purge_queue,
    _receive_message,
    _resolve_queue,
    _send_message,
    _send_message_batch,
    _tag_queue,
    _untag_queue,
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


def _expected_message_attributes_md5(message_attributes: dict) -> str | None:
    if not message_attributes:
        return None

    encoded = bytearray()
    for name in sorted(message_attributes):
        details = message_attributes[name]
        encoded.extend(_encode_string_piece(name))
        encoded.extend(_encode_string_piece(details["DataType"]))
        if details["DataType"].startswith("Binary"):
            encoded.extend(b"\x02")
            encoded.extend(_encode_binary_piece(details["BinaryValue"]))
        else:
            encoded.extend(b"\x01")
            encoded.extend(_encode_string_piece(details["StringValue"]))

    return hashlib.md5(encoded).hexdigest()


def _encode_string_piece(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return struct.pack(">I", len(encoded)) + encoded


def _encode_binary_piece(value: bytes) -> bytes:
    return struct.pack(">I", len(value)) + value


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
        expected = hashlib.md5(b"hello").hexdigest()
        assert _md5("hello") == expected

    @pytest.mark.parametrize(
        ("message_attributes", "expected"),
        [
            (
                {"alpha": {"DataType": "String", "StringValue": "hello"}},
                "91ee26032a192ab3cbd3d29cea276597",
            ),
            (
                {
                    "priority": {"DataType": "Number.int", "StringValue": "42"},
                    "status": {"DataType": "String", "StringValue": "ok"},
                },
                "5d2ab12c0f0e22cdf690d6325e192aa4",
            ),
            (
                {
                    "blob": {"DataType": "Binary.png", "BinaryValue": b"\x00\xff"},
                    "name": {"DataType": "String", "StringValue": "robotocore"},
                },
                "d98ac606c97ae982b07df88b73d77022",
            ),
        ],
    )
    def test_message_attributes_md5_matches_aws_encoding_rules(self, message_attributes, expected):
        assert _expected_message_attributes_md5(message_attributes) == expected
        assert _md5_message_attributes(message_attributes) == expected


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

    def test_send_message_with_message_attributes_returns_attribute_md5(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        mock_req.url.path = "/123456789012/test-queue"
        message_attributes = {
            "color": {"DataType": "String", "StringValue": "blue"},
            "count": {"DataType": "Number.int", "StringValue": "42"},
        }
        result = _send_message(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "MessageBody": "hello world",
                "MessageAttributes": message_attributes,
            },
            "us-east-1",
            "123",
            mock_req,
        )
        assert result["MD5OfMessageAttributes"] == _expected_message_attributes_md5(
            message_attributes
        )

    def test_receive_message_with_message_attributes_returns_attribute_md5(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        mock_req.url.path = "/123456789012/test-queue"
        message_attributes = {
            "blob": {"DataType": "Binary", "BinaryValue": b"\x01\x02"},
            "label": {"DataType": "String.custom", "StringValue": "tag"},
        }
        _send_message(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "MessageBody": "hello world",
                "MessageAttributes": message_attributes,
            },
            "us-east-1",
            "123",
            mock_req,
        )

        result = _receive_message(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "MaxNumberOfMessages": "1",
            },
            "us-east-1",
            "123456789012",
            mock_req,
        )

        message = result["Messages"][0]
        assert message["MD5OfMessageAttributes"] == _expected_message_attributes_md5(
            message_attributes
        )
        assert message["MessageAttributes"] == message_attributes

    def test_receive_message_without_message_attributes_omits_attribute_md5(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        mock_req.url.path = "/123456789012/test-queue"
        _send_message(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "MessageBody": "hello world",
            },
            "us-east-1",
            "123",
            mock_req,
        )

        result = _receive_message(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "MaxNumberOfMessages": "1",
            },
            "us-east-1",
            "123456789012",
            mock_req,
        )

        assert "MD5OfMessageAttributes" not in result["Messages"][0]

    def test_send_message_batch_with_message_attributes_returns_attribute_md5(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        mock_req.url.path = "/123456789012/test-queue"
        message_attributes = {
            "priority": {"DataType": "Number.int", "StringValue": "7"},
            "status": {"DataType": "String", "StringValue": "queued"},
        }
        result = _send_message_batch(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "Entries": [
                    {
                        "Id": "msg1",
                        "MessageBody": "batch message",
                        "MessageAttributes": message_attributes,
                    }
                ],
            },
            "us-east-1",
            "123",
            mock_req,
        )

        expected_md5 = _expected_message_attributes_md5(message_attributes)
        assert result["Successful"][0]["MD5OfMessageAttributes"] == expected_md5

    def test_send_message_batch_without_message_attributes_omits_attribute_md5(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        mock_req.url.path = "/123456789012/test-queue"
        result = _send_message_batch(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "Entries": [{"Id": "msg1", "MessageBody": "batch message"}],
            },
            "us-east-1",
            "123",
            mock_req,
        )

        assert "MD5OfMessageAttributes" not in result["Successful"][0]

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

    async def test_json_protocol_send_message_returns_attribute_md5(self):
        store = _store_with_queue()
        body = json.dumps(
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "MessageBody": "hello",
                "MessageAttributes": {
                    "color": {"DataType": "String", "StringValue": "blue"},
                },
            }
        ).encode()
        headers = {
            "content-type": "application/x-amz-json-1.0",
            "x-amz-target": "AmazonSQS.SendMessage",
        }
        req = _make_request(body=body, headers=headers)

        with (
            patch("robotocore.services.sqs.provider._get_store", return_value=store),
            patch("robotocore.services.sqs.provider._ensure_worker"),
        ):
            resp = await handle_sqs_request(req, "us-east-1", "123456789012")

        data = json.loads(resp.body)
        assert data["MD5OfMessageAttributes"] == _expected_message_attributes_md5(
            {"color": {"DataType": "String", "StringValue": "blue"}}
        )

    async def test_json_protocol_receive_message_returns_attribute_md5_when_present(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        mock_req.url.path = "/123456789012/test-queue"
        message_attributes = {
            "color": {"DataType": "String", "StringValue": "blue"},
        }
        _send_message(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "MessageBody": "hello",
                "MessageAttributes": message_attributes,
            },
            "us-east-1",
            "123456789012",
            mock_req,
        )
        body = json.dumps(
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "MaxNumberOfMessages": 1,
            }
        ).encode()
        headers = {
            "content-type": "application/x-amz-json-1.0",
            "x-amz-target": "AmazonSQS.ReceiveMessage",
        }
        req = _make_request(body=body, headers=headers)

        with (
            patch("robotocore.services.sqs.provider._get_store", return_value=store),
            patch("robotocore.services.sqs.provider._ensure_worker"),
        ):
            resp = await handle_sqs_request(req, "us-east-1", "123456789012")

        data = json.loads(resp.body)
        assert data["Messages"][0]["MD5OfMessageAttributes"] == _expected_message_attributes_md5(
            message_attributes
        )

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


# ---------------------------------------------------------------------------
# Bug-exposing tests below. Each test targets a specific correctness bug
# in the SQS provider and is expected to FAIL until the bug is fixed.
# ---------------------------------------------------------------------------


class TestListDeadLetterSourceQueuesResponseKey:
    """ListDeadLetterSourceQueues uses 'queueUrls' (camelCase) per the botocore
    service model, unlike ListQueues which uses 'QueueUrls' (PascalCase)."""

    def test_response_key_is_camel_case(self):
        store = SqsStore()
        dlq = store.create_queue("my-dlq", "us-east-1", "123456789012")
        source = store.create_queue("my-source", "us-east-1", "123456789012")
        source.attributes["RedrivePolicy"] = json.dumps(
            {"deadLetterTargetArn": dlq.arn, "maxReceiveCount": 3}
        )
        mock_req = MagicMock()
        result = _list_dead_letter_source_queues(
            store,
            {"QueueUrl": "http://localhost:4566/123456789012/my-dlq"},
            "us-east-1",
            "123456789012",
            mock_req,
        )
        # AWS uses camelCase "queueUrls" for this operation (confirmed by botocore model)
        assert "queueUrls" in result, f"Expected 'queueUrls' but got keys: {list(result.keys())}"
        assert len(result["queueUrls"]) == 1


class TestBugSendMessageBatchFifoDedup:
    """Bug: _send_message_batch ignores the return value of queue.put() for
    FIFO queues. When a duplicate message is sent, FifoQueue.put() returns
    the original message, but the batch code always uses the new msg_id and
    md5 in the response."""

    def test_batch_dedup_returns_original_message_id(self):
        store = SqsStore()
        queue = store.create_queue(
            "test.fifo",
            "us-east-1",
            "123456789012",
            {"ContentBasedDeduplication": "true"},
        )
        mock_req = MagicMock()
        mock_req.url.path = "/123456789012/test.fifo"

        # Send first message to get the original message ID
        first_result = _send_message(
            store,
            {
                "QueueUrl": queue.url,
                "MessageBody": "hello",
                "MessageGroupId": "g1",
            },
            "us-east-1",
            "123456789012",
            mock_req,
        )
        original_id = first_result["MessageId"]

        # Send duplicate via batch -- should return original ID
        batch_result = _send_message_batch(
            store,
            {
                "QueueUrl": queue.url,
                "Entries": [
                    {
                        "Id": "entry1",
                        "MessageBody": "hello",
                        "MessageGroupId": "g1",
                    }
                ],
            },
            "us-east-1",
            "123456789012",
            mock_req,
        )
        batch_msg_id = batch_result["Successful"][0]["MessageId"]
        assert batch_msg_id == original_id, (
            f"Batch dedup should return original '{original_id}' but got '{batch_msg_id}'"
        )

    def test_batch_dedup_returns_original_md5(self):
        """Same dedup ID but different body -- should return original MD5."""
        store = SqsStore()
        queue = store.create_queue("test2.fifo", "us-east-1", "123456789012")
        mock_req = MagicMock()
        mock_req.url.path = "/123456789012/test2.fifo"

        first_result = _send_message(
            store,
            {
                "QueueUrl": queue.url,
                "MessageBody": "original-body",
                "MessageGroupId": "g1",
                "MessageDeduplicationId": "dedup-1",
            },
            "us-east-1",
            "123456789012",
            mock_req,
        )
        original_md5 = first_result["MD5OfMessageBody"]

        batch_result = _send_message_batch(
            store,
            {
                "QueueUrl": queue.url,
                "Entries": [
                    {
                        "Id": "entry1",
                        "MessageBody": "different-body",
                        "MessageGroupId": "g1",
                        "MessageDeduplicationId": "dedup-1",
                    }
                ],
            },
            "us-east-1",
            "123456789012",
            mock_req,
        )
        batch_md5 = batch_result["Successful"][0]["MD5OfMessageBody"]
        assert batch_md5 == original_md5, (
            f"Expected original MD5 '{original_md5}' but got '{batch_md5}'"
        )


class TestBugAddPermissionEmptyActions:
    """Bug: _add_permission crashes with IndexError when Actions is an empty
    list, because line 460 does `action_list[0]` unconditionally."""

    def test_empty_actions_raises_sqs_error_not_index_error(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        # Should raise SqsError (validation), not crash with IndexError
        with pytest.raises(SqsError):
            _add_permission(
                store,
                {
                    "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                    "Label": "my-label",
                    "AWSAccountIds": ["111111111111"],
                    "Actions": [],
                },
                "us-east-1",
                "123456789012",
                mock_req,
            )


class TestBugDeleteQueueNonExistent:
    """Bug: _delete_queue silently returns {} for a non-existent queue URL.
    AWS returns AWS.SimpleQueueService.NonExistentQueue error."""

    def test_delete_nonexistent_queue_raises_error(self):
        store = SqsStore()
        mock_req = MagicMock()
        with pytest.raises(SqsError):
            _delete_queue(
                store,
                {"QueueUrl": "http://localhost:4566/123456789012/does-not-exist"},
                "us-east-1",
                "123456789012",
                mock_req,
            )


class TestBugCreateQueueTagsCasing:
    """Bug: _create_queue uses params.get('tags') (lowercase) but botocore JSON
    protocol sends 'Tags' (PascalCase). Tags passed via JSON protocol are silently dropped."""

    def test_create_queue_with_pascal_case_tags(self):
        store = SqsStore()
        mock_req = MagicMock()
        _create_queue(
            store,
            {"QueueName": "tagged-q", "Tags": {"env": "prod", "team": "core"}},
            "us-east-1",
            "123456789012",
            mock_req,
        )
        queue = store.get_queue("tagged-q")
        assert queue.tags == {"env": "prod", "team": "core"}, (
            f"Tags should be set via PascalCase 'Tags' key but got: {queue.tags}"
        )

    def test_create_queue_tags_lowercase_still_works(self):
        """Verify lowercase 'tags' still works for backward compat."""
        store = SqsStore()
        mock_req = MagicMock()
        _create_queue(
            store,
            {"QueueName": "tagged-q2", "tags": {"env": "dev"}},
            "us-east-1",
            "123456789012",
            mock_req,
        )
        queue = store.get_queue("tagged-q2")
        assert queue.tags == {"env": "dev"}


class TestBugTagQueueRoundTrip:
    """Categorical: Tag operations (Tag/Untag/List) should round-trip correctly."""

    def test_tag_then_list(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        _tag_queue(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "Tags": {"k1": "v1", "k2": "v2"},
            },
            "us-east-1",
            "123456789012",
            mock_req,
        )
        result = _list_queue_tags(
            store,
            {"QueueUrl": "http://localhost:4566/123456789012/test-queue"},
            "us-east-1",
            "123456789012",
            mock_req,
        )
        assert result["Tags"] == {"k1": "v1", "k2": "v2"}

    def test_tag_then_untag_then_list(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        _tag_queue(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "Tags": {"k1": "v1", "k2": "v2"},
            },
            "us-east-1",
            "123456789012",
            mock_req,
        )
        _untag_queue(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "TagKeys": ["k1"],
            },
            "us-east-1",
            "123456789012",
            mock_req,
        )
        result = _list_queue_tags(
            store,
            {"QueueUrl": "http://localhost:4566/123456789012/test-queue"},
            "us-east-1",
            "123456789012",
            mock_req,
        )
        assert result["Tags"] == {"k2": "v2"}

    def test_tag_overwrites_existing(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        _tag_queue(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "Tags": {"k1": "v1"},
            },
            "us-east-1",
            "123456789012",
            mock_req,
        )
        _tag_queue(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "Tags": {"k1": "v2"},
            },
            "us-east-1",
            "123456789012",
            mock_req,
        )
        result = _list_queue_tags(
            store,
            {"QueueUrl": "http://localhost:4566/123456789012/test-queue"},
            "us-east-1",
            "123456789012",
            mock_req,
        )
        assert result["Tags"]["k1"] == "v2"

    def test_untag_nonexistent_key_is_noop(self):
        """AWS silently ignores untagging keys that don't exist."""
        store = _store_with_queue()
        mock_req = MagicMock()
        _untag_queue(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "TagKeys": ["nonexistent"],
            },
            "us-east-1",
            "123456789012",
            mock_req,
        )
        result = _list_queue_tags(
            store,
            {"QueueUrl": "http://localhost:4566/123456789012/test-queue"},
            "us-east-1",
            "123456789012",
            mock_req,
        )
        assert result["Tags"] == {}

    def test_list_tags_on_nonexistent_queue_raises(self):
        store = SqsStore()
        mock_req = MagicMock()
        mock_req.url.path = "/"
        with pytest.raises(SqsError) as exc_info:
            _list_queue_tags(
                store,
                {"QueueUrl": "http://localhost:4566/123456789012/nope"},
                "us-east-1",
                "123456789012",
                mock_req,
            )
        assert "NonExistentQueue" in exc_info.value.code


class TestBugFifoValidation:
    """Categorical: FIFO queues require MessageGroupId. Sending without it
    should raise MissingParameter, not silently succeed."""

    def test_send_to_fifo_without_group_id_raises(self):
        store = SqsStore()
        store.create_queue("my.fifo", "us-east-1", "123456789012")
        mock_req = MagicMock()
        mock_req.url.path = "/123456789012/my.fifo"
        with pytest.raises(SqsError) as exc_info:
            _send_message(
                store,
                {
                    "QueueUrl": "http://localhost:4566/123456789012/my.fifo",
                    "MessageBody": "hello",
                },
                "us-east-1",
                "123456789012",
                mock_req,
            )
        code = exc_info.value.code
        assert "MissingParameter" in code or "InvalidParameter" in code

    def test_send_to_fifo_without_dedup_id_and_no_content_dedup_raises(self):
        """FIFO queues without ContentBasedDeduplication require explicit dedup ID."""
        store = SqsStore()
        store.create_queue("strict.fifo", "us-east-1", "123456789012")
        mock_req = MagicMock()
        mock_req.url.path = "/123456789012/strict.fifo"
        with pytest.raises(SqsError) as exc_info:
            _send_message(
                store,
                {
                    "QueueUrl": "http://localhost:4566/123456789012/strict.fifo",
                    "MessageBody": "hello",
                    "MessageGroupId": "g1",
                },
                "us-east-1",
                "123456789012",
                mock_req,
            )
        code = exc_info.value.code
        assert "InvalidParameter" in code or "MissingParameter" in code

    def test_send_to_fifo_with_content_based_dedup_ok(self):
        """With ContentBasedDeduplication, no explicit dedup ID needed."""
        store = SqsStore()
        store.create_queue(
            "cbd.fifo",
            "us-east-1",
            "123456789012",
            {"ContentBasedDeduplication": "true"},
        )
        mock_req = MagicMock()
        mock_req.url.path = "/123456789012/cbd.fifo"
        result = _send_message(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/cbd.fifo",
                "MessageBody": "hello",
                "MessageGroupId": "g1",
            },
            "us-east-1",
            "123456789012",
            mock_req,
        )
        assert "MessageId" in result


class TestBugChangeVisibilityInvalidReceipt:
    """Categorical: ChangeMessageVisibility with an invalid receipt handle
    should raise ReceiptHandleIsInvalid, not silently return {}."""

    def test_change_visibility_invalid_receipt_raises(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        with pytest.raises(SqsError) as exc_info:
            _change_message_visibility(
                store,
                {
                    "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                    "ReceiptHandle": "bogus-receipt-handle",
                    "VisibilityTimeout": "60",
                },
                "us-east-1",
                "123456789012",
                mock_req,
            )
        assert "ReceiptHandleIsInvalid" in exc_info.value.code


class TestBugDeleteQueueCleansUpMessages:
    """Categorical: After deleting a queue, its messages should not be accessible."""

    def test_delete_queue_then_send_raises(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        mock_req.url.path = "/123456789012/test-queue"
        # Delete the queue
        _delete_queue(
            store,
            {"QueueUrl": "http://localhost:4566/123456789012/test-queue"},
            "us-east-1",
            "123456789012",
            mock_req,
        )
        # Sending to deleted queue should fail
        with pytest.raises(SqsError) as exc_info:
            _send_message(
                store,
                {
                    "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                    "MessageBody": "hello",
                },
                "us-east-1",
                "123456789012",
                mock_req,
            )
        assert "NonExistentQueue" in exc_info.value.code


class TestBugPurgeQueueNonExistent:
    """Categorical: PurgeQueue on nonexistent queue should raise error."""

    def test_purge_nonexistent_queue_raises(self):
        store = SqsStore()
        mock_req = MagicMock()
        mock_req.url.path = "/"
        with pytest.raises(SqsError) as exc_info:
            _purge_queue(
                store,
                {"QueueUrl": "http://localhost:4566/123456789012/nope"},
                "us-east-1",
                "123456789012",
                mock_req,
            )
        assert "NonExistentQueue" in exc_info.value.code
