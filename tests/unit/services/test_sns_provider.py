"""Unit tests for SNS provider request handling."""

import json
from unittest.mock import MagicMock, patch
from urllib.parse import urlencode

import pytest
from starlette.requests import Request

from robotocore.services.sns.models import SnsStore
from robotocore.services.sns.provider import (
    SnsError,
    _create_topic,
    _delete_topic,
    _error,
    _get_topic_attributes,
    _json_response,
    _list_topics,
    _publish,
    _sub_to_dict,
    _subscribe,
    _xml_response,
    handle_sns_request,
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


def _make_request(body=b"", headers=None, query_string=b""):
    scope = _make_scope(headers=headers, query_string=query_string)

    async def receive():
        return {"type": "http.request", "body": body}

    return Request(scope, receive)


class TestResponseHelpers:
    def test_json_response(self):
        resp = _json_response({"MessageId": "abc"})
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["MessageId"] == "abc"
        assert resp.media_type == "application/x-amz-json-1.0"

    def test_xml_response(self):
        resp = _xml_response("CreateTopicResponse", {"TopicArn": "arn:test"})
        assert resp.status_code == 200
        assert b"CreateTopicResult" in resp.body
        assert b"arn:test" in resp.body
        assert resp.media_type == "text/xml"

    def test_xml_response_with_list(self):
        resp = _xml_response(
            "ListTopicsResponse",
            {"Topics": [{"TopicArn": "arn:1"}, {"TopicArn": "arn:2"}]},
        )
        assert b"<member>" in resp.body

    def test_xml_response_with_map_fields(self):
        resp = _xml_response(
            "GetTopicAttributesResponse",
            {"Attributes": {"TopicArn": "arn:test", "Owner": "123"}},
        )
        assert b"<entry>" in resp.body
        assert b"<key>TopicArn</key>" in resp.body

    def test_error_json(self):
        resp = _error("NotFound", "Topic not found", 404, use_json=True)
        assert resp.status_code == 404
        data = json.loads(resp.body)
        assert data["__type"] == "NotFound"

    def test_error_xml(self):
        resp = _error("NotFound", "Topic not found", 404, use_json=False)
        assert resp.status_code == 404
        assert b"<Code>NotFound</Code>" in resp.body
        assert b"ErrorResponse" in resp.body


class TestSubToDict:
    def test_basic_conversion(self):
        from robotocore.services.sns.models import SnsSubscription

        sub = SnsSubscription(
            subscription_arn="arn:sub",
            topic_arn="arn:topic",
            protocol="sqs",
            endpoint="arn:sqs:q",
            owner="123",
        )
        d = _sub_to_dict(sub)
        assert d["SubscriptionArn"] == "arn:sub"
        assert d["Protocol"] == "sqs"
        assert d["Owner"] == "123"


class TestActions:
    def _store_with_topic(self):
        store = SnsStore()
        store.create_topic("my-topic", "us-east-1", "123456789012")
        return store

    def test_create_topic(self):
        store = SnsStore()
        mock_req = MagicMock()
        result = _create_topic(store, {"Name": "t1"}, "us-east-1", "123", mock_req)
        assert "TopicArn" in result
        assert "t1" in result["TopicArn"]

    def test_delete_topic(self):
        store = self._store_with_topic()
        arn = "arn:aws:sns:us-east-1:123456789012:my-topic"
        mock_req = MagicMock()
        _delete_topic(store, {"TopicArn": arn}, "us-east-1", "123", mock_req)
        assert store.get_topic(arn) is None

    def test_list_topics(self):
        store = self._store_with_topic()
        mock_req = MagicMock()
        result = _list_topics(store, {}, "us-east-1", "123", mock_req)
        assert len(result["Topics"]) == 1

    def test_get_topic_attributes(self):
        store = self._store_with_topic()
        arn = "arn:aws:sns:us-east-1:123456789012:my-topic"
        mock_req = MagicMock()
        result = _get_topic_attributes(store, {"TopicArn": arn}, "us-east-1", "123", mock_req)
        assert result["Attributes"]["TopicArn"] == arn

    def test_get_topic_attributes_not_found(self):
        store = SnsStore()
        mock_req = MagicMock()
        with pytest.raises(SnsError) as exc_info:
            _get_topic_attributes(
                store,
                {"TopicArn": "arn:aws:sns:us-east-1:123:nope"},
                "us-east-1",
                "123",
                mock_req,
            )
        assert exc_info.value.code == "NotFound"

    def test_subscribe(self):
        store = self._store_with_topic()
        arn = "arn:aws:sns:us-east-1:123456789012:my-topic"
        mock_req = MagicMock()
        result = _subscribe(
            store,
            {
                "TopicArn": arn,
                "Protocol": "sqs",
                "Endpoint": "arn:aws:sqs:us-east-1:123:q",
            },
            "us-east-1",
            "123",
            mock_req,
        )
        assert "SubscriptionArn" in result

    def test_subscribe_nonexistent_topic(self):
        store = SnsStore()
        mock_req = MagicMock()
        with pytest.raises(SnsError):
            _subscribe(
                store,
                {
                    "TopicArn": "arn:aws:sns:us-east-1:123:nope",
                    "Protocol": "sqs",
                    "Endpoint": "q",
                },
                "us-east-1",
                "123",
                mock_req,
            )

    def test_publish(self):
        store = self._store_with_topic()
        arn = "arn:aws:sns:us-east-1:123456789012:my-topic"
        mock_req = MagicMock()
        result = _publish(
            store,
            {"TopicArn": arn, "Message": "hello"},
            "us-east-1",
            "123",
            mock_req,
        )
        assert "MessageId" in result

    def test_publish_to_nonexistent_topic(self):
        store = SnsStore()
        mock_req = MagicMock()
        with pytest.raises(SnsError):
            _publish(
                store,
                {"TopicArn": "arn:nope", "Message": "x"},
                "us-east-1",
                "123",
                mock_req,
            )


@pytest.mark.asyncio
class TestHandleSnsRequest:
    async def test_json_protocol_create_topic(self):
        body = json.dumps({"Name": "test-topic"}).encode()
        headers = {
            "content-type": "application/x-amz-json-1.0",
            "x-amz-target": "SNS.CreateTopic",
        }
        req = _make_request(body=body, headers=headers)

        with patch("robotocore.services.sns.provider._get_store") as mock_get:
            store = SnsStore()
            mock_get.return_value = store
            resp = await handle_sns_request(req, "us-east-1", "123456789012")

        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert "TopicArn" in data

    async def test_query_protocol_create_topic(self):
        form = urlencode({"Action": "CreateTopic", "Name": "qt"}).encode()
        headers = {
            "content-type": "application/x-www-form-urlencoded",
        }
        req = _make_request(body=form, headers=headers)

        with patch("robotocore.services.sns.provider._get_store") as mock_get:
            store = SnsStore()
            mock_get.return_value = store
            resp = await handle_sns_request(req, "us-east-1", "123456789012")

        assert resp.status_code == 200
        assert b"CreateTopicResult" in resp.body

    async def test_unknown_action(self):
        body = json.dumps({}).encode()
        headers = {
            "content-type": "application/x-amz-json-1.0",
            "x-amz-target": "SNS.BogusAction",
        }
        req = _make_request(body=body, headers=headers)

        with patch("robotocore.services.sns.provider._get_store") as mock_get:
            mock_get.return_value = SnsStore()
            resp = await handle_sns_request(req, "us-east-1", "123456789012")

        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "InvalidAction"

    async def test_internal_error_handling(self):
        body = json.dumps({"TopicArn": "arn:nope", "Message": "x"}).encode()
        headers = {
            "content-type": "application/x-amz-json-1.0",
            "x-amz-target": "SNS.Publish",
        }
        req = _make_request(body=body, headers=headers)

        with patch("robotocore.services.sns.provider._get_store") as mock_get:
            mock_get.return_value = SnsStore()
            resp = await handle_sns_request(req, "us-east-1", "123456789012")

        assert resp.status_code == 404

    async def test_sns_error_json_format(self):
        body = json.dumps({"TopicArn": "arn:nope"}).encode()
        headers = {
            "content-type": "application/x-amz-json-1.0",
            "x-amz-target": "SNS.GetTopicAttributes",
        }
        req = _make_request(body=body, headers=headers)

        with patch("robotocore.services.sns.provider._get_store") as mock_get:
            mock_get.return_value = SnsStore()
            resp = await handle_sns_request(req, "us-east-1", "123456789012")

        assert resp.status_code == 404
        data = json.loads(resp.body)
        assert data["__type"] == "NotFound"
