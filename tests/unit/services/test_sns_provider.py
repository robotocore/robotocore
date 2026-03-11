"""Unit tests for SNS provider request handling."""

import json
from unittest.mock import MagicMock, patch
from urllib.parse import urlencode

import pytest
from starlette.requests import Request

from robotocore.services.sns.models import SnsStore
from robotocore.services.sns.provider import (
    SnsError,
    _create_platform_endpoint,
    _create_topic,
    _delete_endpoint,
    _delete_topic,
    _error,
    _get_endpoint_attributes,
    _get_topic_attributes,
    _json_response,
    _list_endpoints_by_platform_application,
    _list_tags_for_resource,
    _list_topics,
    _publish,
    _publish_batch,
    _sub_to_dict,
    _subscribe,
    _tag_resource,
    _unsubscribe,
    _untag_resource,
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


class TestSNSPublishBatch:
    """Test PublishBatch query param parsing."""

    def test_publish_batch_parses_entries(self):
        store = SnsStore()
        store.create_topic("batch-topic", "us-east-1", "123456789012")
        params = {
            "TopicArn": "arn:aws:sns:us-east-1:123456789012:batch-topic",
            "PublishBatchRequestEntries.member.1.Id": "msg1",
            "PublishBatchRequestEntries.member.1.Message": "hello",
            "PublishBatchRequestEntries.member.2.Id": "msg2",
            "PublishBatchRequestEntries.member.2.Message": "world",
            "PublishBatchRequestEntries.member.3.Id": "msg3",
            "PublishBatchRequestEntries.member.3.Message": "batch",
        }
        req = MagicMock()
        result = _publish_batch(store, params, "us-east-1", "123456789012", req)
        assert len(result["Successful"]) == 3
        assert len(result["Failed"]) == 0
        ids = [s["Id"] for s in result["Successful"]]
        assert "msg1" in ids
        assert "msg2" in ids
        assert "msg3" in ids

    def test_publish_batch_empty(self):
        store = SnsStore()
        store.create_topic("empty-topic", "us-east-1", "123456789012")
        params = {"TopicArn": "arn:aws:sns:us-east-1:123456789012:empty-topic"}
        req = MagicMock()
        with pytest.raises(SnsError, match="EmptyBatchRequest"):
            _publish_batch(store, params, "us-east-1", "123456789012", req)

    def test_publish_batch_nonexistent_topic(self):
        store = SnsStore()
        params = {
            "TopicArn": "arn:aws:sns:us-east-1:123456789012:nope",
            "PublishBatchRequestEntries.member.1.Id": "msg1",
            "PublishBatchRequestEntries.member.1.Message": "hello",
        }
        req = MagicMock()
        with pytest.raises(SnsError):
            _publish_batch(store, params, "us-east-1", "123456789012", req)


class TestTagOperations:
    """Categorical bug class: tag operations must error on nonexistent resources."""

    def _store_with_topic(self):
        store = SnsStore()
        store.create_topic("tagged-topic", "us-east-1", "123456789012")
        return store

    def test_tag_resource_adds_tags(self):
        store = self._store_with_topic()
        arn = "arn:aws:sns:us-east-1:123456789012:tagged-topic"
        req = MagicMock()
        _tag_resource(
            store,
            {"ResourceArn": arn, "Tags.member.1.Key": "env", "Tags.member.1.Value": "prod"},
            "us-east-1",
            "123",
            req,
        )
        topic = store.get_topic(arn)
        assert topic.tags["env"] == "prod"

    def test_untag_resource_removes_tags(self):
        store = self._store_with_topic()
        arn = "arn:aws:sns:us-east-1:123456789012:tagged-topic"
        req = MagicMock()
        # First add a tag
        _tag_resource(
            store,
            {"ResourceArn": arn, "Tags.member.1.Key": "env", "Tags.member.1.Value": "prod"},
            "us-east-1",
            "123",
            req,
        )
        # Then remove it
        _untag_resource(
            store,
            {"ResourceArn": arn, "TagKeys.member.1": "env"},
            "us-east-1",
            "123",
            req,
        )
        topic = store.get_topic(arn)
        assert "env" not in topic.tags

    def test_list_tags_for_nonexistent_resource_raises_error(self):
        """BUG: ListTagsForResource silently returns empty tags for nonexistent resources.
        AWS returns NotFoundException. This is a categorical bug across providers."""
        store = SnsStore()
        req = MagicMock()
        with pytest.raises(SnsError) as exc_info:
            _list_tags_for_resource(
                store,
                {"ResourceArn": "arn:aws:sns:us-east-1:123:nonexistent"},
                "us-east-1",
                "123",
                req,
            )
        assert exc_info.value.status == 404

    def test_tag_resource_nonexistent_raises_error(self):
        store = SnsStore()
        req = MagicMock()
        with pytest.raises(SnsError):
            _tag_resource(
                store,
                {
                    "ResourceArn": "arn:aws:sns:us-east-1:123:nope",
                    "Tags.member.1.Key": "k",
                    "Tags.member.1.Value": "v",
                },
                "us-east-1",
                "123",
                req,
            )

    def test_untag_resource_nonexistent_raises_error(self):
        store = SnsStore()
        req = MagicMock()
        with pytest.raises(SnsError):
            _untag_resource(
                store,
                {"ResourceArn": "arn:aws:sns:us-east-1:123:nope", "TagKeys.member.1": "k"},
                "us-east-1",
                "123",
                req,
            )

    def test_list_tags_returns_tags(self):
        store = self._store_with_topic()
        arn = "arn:aws:sns:us-east-1:123456789012:tagged-topic"
        req = MagicMock()
        _tag_resource(
            store,
            {"ResourceArn": arn, "Tags.member.1.Key": "team", "Tags.member.1.Value": "backend"},
            "us-east-1",
            "123",
            req,
        )
        result = _list_tags_for_resource(store, {"ResourceArn": arn}, "us-east-1", "123", req)
        assert len(result["Tags"]) == 1
        assert result["Tags"][0]["Key"] == "team"
        assert result["Tags"][0]["Value"] == "backend"


class TestParentChildCascade:
    """Categorical bug class: deleting a parent must clean up children."""

    def test_delete_topic_cleans_up_subscriptions(self):
        """Verify subscriptions are removed from the global subscriptions dict."""
        store = SnsStore()
        store.create_topic("cascade-topic", "us-east-1", "123456789012")
        arn = "arn:aws:sns:us-east-1:123456789012:cascade-topic"
        sub = store.subscribe(arn, "sqs", "arn:aws:sqs:us-east-1:123:q1")
        sub_arn = sub.subscription_arn
        # Subscription exists in global dict
        assert store.get_subscription(sub_arn) is not None
        # Delete topic
        store.delete_topic(arn)
        # Subscription should be gone from global dict
        assert store.get_subscription(sub_arn) is None

    def test_delete_topic_with_multiple_subscriptions(self):
        store = SnsStore()
        store.create_topic("multi-sub", "us-east-1", "123456789012")
        arn = "arn:aws:sns:us-east-1:123456789012:multi-sub"
        sub1 = store.subscribe(arn, "sqs", "arn:aws:sqs:us-east-1:123:q1")
        sub2 = store.subscribe(arn, "sqs", "arn:aws:sqs:us-east-1:123:q2")
        store.delete_topic(arn)
        assert store.get_subscription(sub1.subscription_arn) is None
        assert store.get_subscription(sub2.subscription_arn) is None
        assert store.list_subscriptions() == []

    def test_delete_platform_app_cleans_up_endpoints(self):
        """BUG: Deleting a platform application does NOT clean up its endpoints.
        Orphaned endpoints remain in the store. This is a categorical parent-child bug."""
        store = SnsStore()
        app = store.create_platform_application("myapp", "GCM", "us-east-1", "123456789012")
        ep = store.create_platform_endpoint(app.arn, "device-token-123")
        assert ep is not None
        ep_arn = ep.arn
        # Endpoint exists
        assert store.get_platform_endpoint(ep_arn) is not None
        # Delete the parent app
        store.delete_platform_application(app.arn)
        # Endpoints should be cleaned up
        assert store.get_platform_endpoint(ep_arn) is None
        assert store.list_endpoints_by_platform_application(app.arn) == []

    def test_unsubscribe_removes_from_topic_and_global(self):
        """Verify unsubscribe cleans up both the topic's list and the global dict."""
        store = SnsStore()
        store.create_topic("unsub-topic", "us-east-1", "123456789012")
        arn = "arn:aws:sns:us-east-1:123456789012:unsub-topic"
        sub = store.subscribe(arn, "sqs", "arn:aws:sqs:us-east-1:123:q1")
        req = MagicMock()
        _unsubscribe(store, {"SubscriptionArn": sub.subscription_arn}, "us-east-1", "123", req)
        assert store.get_subscription(sub.subscription_arn) is None
        topic = store.get_topic(arn)
        assert len(topic.subscriptions) == 0


class TestPlatformEndpointOperations:
    """Test platform endpoint CRUD operations added by overnight script."""

    def _store_with_app(self):
        store = SnsStore()
        app = store.create_platform_application("test-app", "GCM", "us-east-1", "123456789012")
        return store, app

    def test_create_platform_endpoint(self):
        store, app = self._store_with_app()
        req = MagicMock()
        result = _create_platform_endpoint(
            store,
            {"PlatformApplicationArn": app.arn, "Token": "device-token"},
            "us-east-1",
            "123",
            req,
        )
        assert "EndpointArn" in result
        assert ":endpoint/" in result["EndpointArn"]

    def test_create_endpoint_nonexistent_app_raises(self):
        store = SnsStore()
        req = MagicMock()
        with pytest.raises(SnsError) as exc_info:
            _create_platform_endpoint(
                store,
                {"PlatformApplicationArn": "arn:aws:sns:us-east-1:123:app/GCM/nope", "Token": "t"},
                "us-east-1",
                "123",
                req,
            )
        assert exc_info.value.status == 404

    def test_get_endpoint_attributes(self):
        store, app = self._store_with_app()
        ep = store.create_platform_endpoint(app.arn, "token-123", "userdata")
        req = MagicMock()
        result = _get_endpoint_attributes(store, {"EndpointArn": ep.arn}, "us-east-1", "123", req)
        assert "Attributes" in result
        assert result["Attributes"]["Token"] == "token-123"
        assert result["Attributes"]["Enabled"] == "true"

    def test_get_endpoint_attributes_nonexistent_raises(self):
        store = SnsStore()
        req = MagicMock()
        with pytest.raises(SnsError) as exc_info:
            _get_endpoint_attributes(
                store,
                {"EndpointArn": "arn:aws:sns:us-east-1:123:endpoint/GCM/app/fake"},
                "us-east-1",
                "123",
                req,
            )
        assert exc_info.value.status == 404

    def test_delete_endpoint(self):
        store, app = self._store_with_app()
        ep = store.create_platform_endpoint(app.arn, "token-123")
        req = MagicMock()
        _delete_endpoint(store, {"EndpointArn": ep.arn}, "us-east-1", "123", req)
        assert store.get_platform_endpoint(ep.arn) is None

    def test_list_endpoints_by_app(self):
        store, app = self._store_with_app()
        store.create_platform_endpoint(app.arn, "token-1")
        store.create_platform_endpoint(app.arn, "token-2")
        req = MagicMock()
        result = _list_endpoints_by_platform_application(
            store, {"PlatformApplicationArn": app.arn}, "us-east-1", "123", req
        )
        assert len(result["Endpoints"]) == 2

    def test_list_endpoints_nonexistent_app_raises(self):
        store = SnsStore()
        req = MagicMock()
        with pytest.raises(SnsError) as exc_info:
            _list_endpoints_by_platform_application(
                store,
                {"PlatformApplicationArn": "arn:aws:sns:us-east-1:123:app/GCM/nope"},
                "us-east-1",
                "123",
                req,
            )
        assert exc_info.value.status == 404

    def test_platform_endpoint_arn_format(self):
        """Verify endpoint ARN is constructed correctly from app ARN."""
        store, app = self._store_with_app()
        ep = store.create_platform_endpoint(app.arn, "token-abc")
        # ARN should replace :app/ with :endpoint/ and append endpoint ID
        assert ":endpoint/GCM/test-app/" in ep.arn
        assert ep.arn.startswith("arn:aws:sns:us-east-1:123456789012:endpoint/")
