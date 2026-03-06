"""Unit tests for S3 provider request handling and notification config."""

from unittest.mock import patch

import pytest
from starlette.datastructures import QueryParams
from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.s3.notifications import NotificationConfig
from robotocore.services.s3.provider import (
    _is_presigned_url,
    _notification_config_to_xml,
    _parse_notification_config_xml,
    _strip_presigned_params,
    handle_s3_request,
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
