"""Unit tests for S3 provider request handling and notification config."""

from unittest.mock import patch

import pytest
from starlette.datastructures import QueryParams
from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.s3.notifications import NotificationConfig
from robotocore.services.s3.provider import (
    _cors_store,
    _is_presigned_url,
    _lifecycle_store,
    _logging_store,
    _notification_config_to_xml,
    _object_legal_hold_store,
    _object_lock_store,
    _parse_notification_config_xml,
    _store_lock,
    _strip_presigned_params,
    delete_bucket_cors,
    delete_bucket_lifecycle,
    get_bucket_cors,
    get_bucket_lifecycle,
    get_object_legal_hold,
    get_object_lock_config,
    handle_s3_request,
    set_bucket_cors,
    set_bucket_lifecycle,
    set_object_legal_hold,
    set_object_lock_config,
)


def _make_scope(
    method: str,
    path: str,
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


def _make_request(method, path, body=b"", query_string=b"", headers=None):
    scope = _make_scope(method, path, query_string, headers)

    async def receive():
        return {"type": "http.request", "body": body}

    req = Request(scope, receive)
    return req


class TestIsPresignedUrl:
    def test_sigv4_presigned(self):
        params = QueryParams("X-Amz-Signature=abc123")
        assert _is_presigned_url(params) is True

    def test_sigv2_presigned(self):
        params = QueryParams("Signature=abc123")
        assert _is_presigned_url(params) is True

    def test_not_presigned(self):
        params = QueryParams("prefix=foo")
        assert _is_presigned_url(params) is False

    def test_empty_params(self):
        params = QueryParams("")
        assert _is_presigned_url(params) is False


class TestStripPresignedParams:
    def test_strips_sigv4_params(self):
        qs = (
            b"X-Amz-Algorithm=AWS4-HMAC-SHA256"
            b"&X-Amz-Credential=AKID/20260101/us-east-1/s3/aws4_request"
            b"&X-Amz-Date=20260101T000000Z"
            b"&X-Amz-Expires=3600"
            b"&X-Amz-SignedHeaders=host"
            b"&X-Amz-Signature=abc123"
            b"&mykey=myval"
        )
        req = _make_request("GET", "/mybucket/mykey", query_string=qs)
        new_req = _strip_presigned_params(req)
        # The cleaned request should only have mykey=myval
        assert "mykey" in str(new_req.scope["query_string"])
        assert b"X-Amz-Signature" not in new_req.scope["query_string"]

    def test_injects_authorization_header(self):
        qs = (
            b"X-Amz-Algorithm=AWS4-HMAC-SHA256"
            b"&X-Amz-Credential=AKID/20260101/us-east-1/s3/aws4_request"
            b"&X-Amz-SignedHeaders=host"
            b"&X-Amz-Signature=abc123"
        )
        req = _make_request("GET", "/mybucket/mykey", query_string=qs)
        new_req = _strip_presigned_params(req)
        auth = new_req.headers.get("authorization", "")
        assert "AWS4-HMAC-SHA256" in auth
        assert "AKID" in auth

    def test_injects_security_token_header(self):
        qs = (
            b"X-Amz-Algorithm=AWS4-HMAC-SHA256"
            b"&X-Amz-Credential=AKID/20260101/us-east-1/s3/aws4_request"
            b"&X-Amz-Signature=abc123"
            b"&X-Amz-Security-Token=my-token"
        )
        req = _make_request("GET", "/mybucket/mykey", query_string=qs)
        new_req = _strip_presigned_params(req)
        assert new_req.headers.get("x-amz-security-token") == "my-token"

    def test_sigv2_fallback_auth(self):
        qs = b"AWSAccessKeyId=AKID&Signature=sig&Expires=9999"
        req = _make_request("GET", "/mybucket/mykey", query_string=qs)
        new_req = _strip_presigned_params(req)
        auth = new_req.headers.get("authorization", "")
        assert "AWS4-HMAC-SHA256" in auth

    def test_put_adds_content_type(self):
        req = _make_request("PUT", "/mybucket/mykey", body=b"data")
        new_req = _strip_presigned_params(req, body=b"data")
        ct_found = any(k == b"content-type" for k, v in new_req.scope["headers"])
        assert ct_found

    def test_preserves_body(self):
        req = _make_request("PUT", "/mybucket/mykey", body=b"some-data")
        new_req = _strip_presigned_params(req, body=b"some-data")
        assert new_req._body == b"some-data"


class TestParseNotificationConfigXml:
    def test_parse_queue_config(self):
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <NotificationConfiguration
            xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <QueueConfiguration>
            <Queue>arn:aws:sqs:us-east-1:123:my-queue</Queue>
            <Event>s3:ObjectCreated:*</Event>
          </QueueConfiguration>
        </NotificationConfiguration>"""
        config = _parse_notification_config_xml(xml)
        assert len(config.queue_configs) == 1
        assert config.queue_configs[0]["QueueArn"] == ("arn:aws:sqs:us-east-1:123:my-queue")
        assert "s3:ObjectCreated:*" in config.queue_configs[0]["Events"]

    def test_parse_topic_config(self):
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <NotificationConfiguration
            xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <TopicConfiguration>
            <Topic>arn:aws:sns:us-east-1:123:my-topic</Topic>
            <Event>s3:ObjectRemoved:*</Event>
          </TopicConfiguration>
        </NotificationConfiguration>"""
        config = _parse_notification_config_xml(xml)
        assert len(config.topic_configs) == 1
        assert config.topic_configs[0]["TopicArn"] == ("arn:aws:sns:us-east-1:123:my-topic")

    def test_parse_with_filter_rules(self):
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <NotificationConfiguration
            xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <QueueConfiguration>
            <Queue>arn:aws:sqs:us-east-1:123:q</Queue>
            <Event>s3:ObjectCreated:*</Event>
            <Filter>
              <S3Key>
                <FilterRule>
                  <Name>prefix</Name>
                  <Value>images/</Value>
                </FilterRule>
                <FilterRule>
                  <Name>suffix</Name>
                  <Value>.jpg</Value>
                </FilterRule>
              </S3Key>
            </Filter>
          </QueueConfiguration>
        </NotificationConfiguration>"""
        config = _parse_notification_config_xml(xml)
        assert len(config.queue_configs) == 1
        rules = config.queue_configs[0]["Filter"]["Key"]["FilterRules"]
        assert len(rules) == 2
        assert rules[0]["Name"] == "prefix"
        assert rules[1]["Value"] == ".jpg"

    def test_parse_invalid_xml(self):
        config = _parse_notification_config_xml("not xml at all")
        assert len(config.queue_configs) == 0
        assert len(config.topic_configs) == 0

    def test_parse_empty_config(self):
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <NotificationConfiguration
            xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        </NotificationConfiguration>"""
        config = _parse_notification_config_xml(xml)
        assert len(config.queue_configs) == 0
        assert len(config.topic_configs) == 0


class TestNotificationConfigToXml:
    def test_empty_config(self):
        config = NotificationConfig()
        xml = _notification_config_to_xml(config)
        assert "<NotificationConfiguration" in xml
        assert "</NotificationConfiguration>" in xml

    def test_queue_config_round_trip(self):
        config = NotificationConfig(
            queue_configs=[
                {
                    "QueueArn": "arn:aws:sqs:us-east-1:123:q",
                    "Events": ["s3:ObjectCreated:*"],
                }
            ]
        )
        xml = _notification_config_to_xml(config)
        assert "<QueueConfiguration>" in xml
        assert "arn:aws:sqs:us-east-1:123:q" in xml
        assert "s3:ObjectCreated:*" in xml

    def test_topic_config_serialization(self):
        config = NotificationConfig(
            topic_configs=[
                {
                    "TopicArn": "arn:aws:sns:us-east-1:123:t",
                    "Events": ["s3:ObjectRemoved:*"],
                }
            ]
        )
        xml = _notification_config_to_xml(config)
        assert "<TopicConfiguration>" in xml
        assert "arn:aws:sns:us-east-1:123:t" in xml

    def test_filter_rules_in_xml(self):
        config = NotificationConfig(
            queue_configs=[
                {
                    "QueueArn": "arn:aws:sqs:us-east-1:123:q",
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": "logs/"}]}},
                }
            ]
        )
        xml = _notification_config_to_xml(config)
        assert "<FilterRule>" in xml
        assert "<Name>prefix</Name>" in xml
        assert "<Value>logs/</Value>" in xml

    def test_eventbridge_enabled_produces_tag(self):
        """eventbridge_enabled=True must emit <EventBridgeConfiguration/>."""
        config = NotificationConfig(eventbridge_enabled=True)
        xml = _notification_config_to_xml(config)
        assert "<EventBridgeConfiguration/>" in xml

    def test_eventbridge_disabled_omits_tag(self):
        """eventbridge_enabled=False must NOT emit <EventBridgeConfiguration/>."""
        config = NotificationConfig(eventbridge_enabled=False)
        xml = _notification_config_to_xml(config)
        assert "EventBridgeConfiguration" not in xml


class TestParseNotificationConfigXmlEventBridge:
    def test_parse_eventbridge_configuration(self):
        """<EventBridgeConfiguration/> in XML sets eventbridge_enabled=True."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <NotificationConfiguration
            xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <EventBridgeConfiguration/>
        </NotificationConfiguration>"""
        config = _parse_notification_config_xml(xml)
        assert config.eventbridge_enabled is True

    def test_parse_without_eventbridge_configuration(self):
        """Missing <EventBridgeConfiguration/> leaves eventbridge_enabled=False."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <NotificationConfiguration
            xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <QueueConfiguration>
            <Queue>arn:aws:sqs:us-east-1:123:q</Queue>
            <Event>s3:ObjectCreated:*</Event>
          </QueueConfiguration>
        </NotificationConfiguration>"""
        config = _parse_notification_config_xml(xml)
        assert config.eventbridge_enabled is False

    def test_parse_eventbridge_with_other_configs(self):
        """EventBridge and SQS configs can coexist in one XML document."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <NotificationConfiguration
            xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <QueueConfiguration>
            <Queue>arn:aws:sqs:us-east-1:123:q</Queue>
            <Event>s3:ObjectCreated:*</Event>
          </QueueConfiguration>
          <EventBridgeConfiguration/>
        </NotificationConfiguration>"""
        config = _parse_notification_config_xml(xml)
        assert config.eventbridge_enabled is True
        assert len(config.queue_configs) == 1


@pytest.mark.asyncio
class TestHandleS3Request:
    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_forwards_to_moto(self, mock_forward):
        mock_forward.return_value = Response(content=b"<ok/>", status_code=200)
        req = _make_request("GET", "/mybucket")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        mock_forward.assert_called_once()

    @patch("robotocore.services.s3.provider.forward_to_moto")
    @patch("robotocore.services.s3.provider.fire_event")
    async def test_put_object_fires_event(self, mock_fire, mock_forward):
        mock_forward.return_value = Response(
            content=b"",
            status_code=200,
            headers={"content-length": "100", "etag": '"abc123"'},
        )
        req = _make_request("PUT", "/mybucket/mykey")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        mock_fire.assert_called_once_with(
            "s3:ObjectCreated:Put",
            "mybucket",
            "mykey",
            "us-east-1",
            "123456789012",
            100,
            "abc123",
        )

    @patch("robotocore.services.s3.provider.forward_to_moto")
    @patch("robotocore.services.s3.provider.fire_event")
    async def test_delete_object_fires_event(self, mock_fire, mock_forward):
        mock_forward.return_value = Response(content=b"", status_code=204)
        req = _make_request("DELETE", "/mybucket/mykey")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204
        mock_fire.assert_called_once_with(
            "s3:ObjectRemoved:Delete",
            "mybucket",
            "mykey",
            "us-east-1",
            "123456789012",
        )

    @patch("robotocore.services.s3.provider.forward_to_moto")
    @patch("robotocore.services.s3.provider.fire_event")
    async def test_no_event_on_error_response(self, mock_fire, mock_forward):
        mock_forward.return_value = Response(content=b"<Error/>", status_code=404)
        req = _make_request("PUT", "/mybucket/mykey")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404
        mock_fire.assert_not_called()

    @patch("robotocore.services.s3.provider.get_notification_config")
    async def test_get_notification_config_endpoint(self, mock_get_config):
        mock_get_config.return_value = NotificationConfig()
        req = _make_request("GET", "/mybucket", query_string=b"notification")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert b"NotificationConfiguration" in resp.body

    @patch("robotocore.services.s3.provider.set_notification_config")
    async def test_put_notification_config_endpoint(self, mock_set_config):
        xml_body = (
            b'<?xml version="1.0" encoding="UTF-8"?>'
            b"<NotificationConfiguration>"
            b"</NotificationConfiguration>"
        )
        req = _make_request(
            "PUT",
            "/mybucket",
            body=xml_body,
            query_string=b"notification",
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        mock_set_config.assert_called_once()

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_moto_nosuchbucket_error_passthrough(self, mock_forward):
        """When Moto returns a NoSuchBucket error (404), it passes through."""
        error_xml = (
            b"<Error><Code>NoSuchBucket</Code>"
            b"<Message>The specified bucket does not exist</Message>"
            b"<BucketName>nonexistent-bucket</BucketName></Error>"
        )
        mock_forward.return_value = Response(content=error_xml, status_code=404)
        req = _make_request("GET", "/nonexistent-bucket")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404
        assert b"NoSuchBucket" in resp.body

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_moto_nosuchkey_error_passthrough(self, mock_forward):
        """When Moto returns a NoSuchKey error (404), it passes through."""
        error_xml = (
            b"<Error><Code>NoSuchKey</Code>"
            b"<Message>The specified key does not exist.</Message>"
            b"<Key>nonexistent-key</Key></Error>"
        )
        mock_forward.return_value = Response(content=error_xml, status_code=404)
        req = _make_request("GET", "/mybucket/nonexistent-key")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404
        assert b"NoSuchKey" in resp.body

    @patch("robotocore.services.s3.provider.forward_to_moto")
    @patch("robotocore.services.s3.provider.fire_event")
    async def test_no_event_on_put_error(self, mock_fire, mock_forward):
        """Events should NOT fire when a PUT returns an error status."""
        mock_forward.return_value = Response(content=b"<Error/>", status_code=403)
        req = _make_request("PUT", "/mybucket/mykey")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 403
        mock_fire.assert_not_called()

    async def test_cors_preflight_no_cors_configured(self):
        """OPTIONS on a bucket without CORS returns 403."""
        from robotocore.services.s3.provider import _cors_store

        _cors_store.pop("nocors-bucket", None)
        req = _make_request(
            "OPTIONS",
            "/nocors-bucket/key",
            headers={"Origin": "http://example.com"},
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 403

    async def test_get_object_lock_not_configured(self):
        """GET ?object-lock on bucket without config returns 404."""
        from robotocore.services.s3.provider import _object_lock_store

        _object_lock_store.pop("no-lock-bucket", None)
        req = _make_request("GET", "/no-lock-bucket", query_string=b"object-lock")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404
        assert b"ObjectLockConfigurationNotFoundError" in resp.body


class TestStoreHelpers:
    """Test that the in-memory store CRUD helpers work correctly."""

    def setup_method(self):
        """Clean up all stores before each test."""
        with _store_lock:
            _cors_store.clear()
            _lifecycle_store.clear()
            _object_lock_store.clear()
            _object_legal_hold_store.clear()
            _logging_store.clear()

    def test_cors_store_roundtrip(self):
        rules = [{"AllowedOrigins": ["*"], "AllowedMethods": ["GET"]}]
        set_bucket_cors("test-bucket", rules)
        assert get_bucket_cors("test-bucket") == rules
        delete_bucket_cors("test-bucket")
        assert get_bucket_cors("test-bucket") is None

    def test_lifecycle_store_roundtrip(self):
        rules = [{"ID": "rule1", "Status": "Enabled"}]
        set_bucket_lifecycle("test-bucket", rules)
        assert get_bucket_lifecycle("test-bucket") == rules
        delete_bucket_lifecycle("test-bucket")
        assert get_bucket_lifecycle("test-bucket") is None

    def test_object_lock_store_roundtrip(self):
        config = {"ObjectLockEnabled": "Enabled"}
        set_object_lock_config("test-bucket", config)
        assert get_object_lock_config("test-bucket") == config

    def test_legal_hold_store_roundtrip(self):
        set_object_legal_hold("test-bucket", "my-key", "ON")
        assert get_object_legal_hold("test-bucket", "my-key") == "ON"

    def test_legal_hold_nonexistent_returns_none(self):
        assert get_object_legal_hold("no-bucket", "no-key") is None

    def test_delete_cors_on_nonexistent_is_noop(self):
        """Deleting CORS for a bucket that has no CORS config should not error."""
        delete_bucket_cors("no-such-bucket")  # should not raise

    def test_delete_lifecycle_on_nonexistent_is_noop(self):
        """Deleting lifecycle for a bucket that has no lifecycle config should not error."""
        delete_bucket_lifecycle("no-such-bucket")  # should not raise


@pytest.mark.asyncio
class TestDeleteBucketStoreCleanup:
    """CATEGORICAL BUG: When a bucket is deleted via Moto, all module-level
    stores (_cors_store, _lifecycle_store, _object_lock_store,
    _object_legal_hold_store, _logging_store, notifications) must be cleaned up.

    Without cleanup, deleted buckets leave orphaned data in memory, and
    recreating a bucket with the same name would inherit stale configs.
    """

    def setup_method(self):
        """Populate all stores for a bucket that will be deleted."""
        with _store_lock:
            _cors_store["cleanup-bucket"] = [{"AllowedOrigins": ["*"]}]
            _lifecycle_store["cleanup-bucket"] = [{"ID": "r1"}]
            _object_lock_store["cleanup-bucket"] = {"ObjectLockEnabled": "Enabled"}
            _object_legal_hold_store["cleanup-bucket/key1"] = "ON"
            _object_legal_hold_store["cleanup-bucket/key2"] = "ON"
            _logging_store["cleanup-bucket"] = {"TargetBucket": "log-bucket"}

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_delete_bucket_cleans_cors_store(self, mock_forward):
        """After successful DELETE /<bucket>, CORS config should be removed."""
        mock_forward.return_value = Response(content=b"", status_code=204)
        req = _make_request("DELETE", "/cleanup-bucket")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204
        assert get_bucket_cors("cleanup-bucket") is None

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_delete_bucket_cleans_lifecycle_store(self, mock_forward):
        """After successful DELETE /<bucket>, lifecycle config should be removed."""
        mock_forward.return_value = Response(content=b"", status_code=204)
        req = _make_request("DELETE", "/cleanup-bucket")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204
        assert get_bucket_lifecycle("cleanup-bucket") is None

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_delete_bucket_cleans_object_lock_store(self, mock_forward):
        """After successful DELETE /<bucket>, object lock config should be removed."""
        mock_forward.return_value = Response(content=b"", status_code=204)
        req = _make_request("DELETE", "/cleanup-bucket")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204
        assert get_object_lock_config("cleanup-bucket") is None

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_delete_bucket_cleans_legal_hold_store(self, mock_forward):
        """After successful DELETE /<bucket>, legal hold entries for that bucket
        should be removed (they use 'bucket/key' compound keys)."""
        mock_forward.return_value = Response(content=b"", status_code=204)
        req = _make_request("DELETE", "/cleanup-bucket")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204
        assert get_object_legal_hold("cleanup-bucket", "key1") is None
        assert get_object_legal_hold("cleanup-bucket", "key2") is None

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_delete_bucket_cleans_logging_store(self, mock_forward):
        """After successful DELETE /<bucket>, logging config should be removed."""
        mock_forward.return_value = Response(content=b"", status_code=204)
        req = _make_request("DELETE", "/cleanup-bucket")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204
        with _store_lock:
            assert "cleanup-bucket" not in _logging_store

    @patch("robotocore.services.s3.provider.forward_to_moto")
    @patch("robotocore.services.s3.notifications.set_notification_config")
    async def test_delete_bucket_cleans_notification_store(self, mock_set_notif, mock_forward):
        """After successful DELETE /<bucket>, notification config should be removed."""
        from robotocore.services.s3.notifications import _bucket_notifications

        _bucket_notifications["cleanup-bucket"] = NotificationConfig(
            queue_configs=[{"QueueArn": "arn:aws:sqs:us-east-1:123:q", "Events": ["s3:*"]}]
        )
        mock_forward.return_value = Response(content=b"", status_code=204)
        req = _make_request("DELETE", "/cleanup-bucket")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204
        # The cleanup should have cleared the notification config
        from robotocore.services.s3.notifications import get_notification_config

        config = get_notification_config("cleanup-bucket")
        assert len(config.queue_configs) == 0

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_failed_delete_does_not_clean_stores(self, mock_forward):
        """If Moto returns an error (e.g., BucketNotEmpty), stores should NOT be cleaned."""
        mock_forward.return_value = Response(
            content=b"<Error><Code>BucketNotEmpty</Code></Error>", status_code=409
        )
        req = _make_request("DELETE", "/cleanup-bucket")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 409
        # All stores should still have their data
        assert get_bucket_cors("cleanup-bucket") is not None
        assert get_bucket_lifecycle("cleanup-bucket") is not None
        assert get_object_lock_config("cleanup-bucket") is not None
        assert get_object_legal_hold("cleanup-bucket", "key1") == "ON"
