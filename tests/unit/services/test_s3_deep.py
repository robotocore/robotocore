"""Unit tests for S3 deep fidelity features: presigned URLs, multipart,
CORS, versioning, lifecycle, object lock, expanded notifications."""

import json
import time
from unittest.mock import patch

import pytest
from starlette.datastructures import QueryParams
from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.s3.notifications import (
    NotificationConfig,
    _bucket_notifications,
    _deliver_to_lambda,
    _event_matches,
    fire_event,
    set_notification_config,
)
from robotocore.services.s3.presigned import (
    PresignedUrlInfo,
    is_presigned_request,
    parse_presigned_url,
    validate_presigned_url,
)
from robotocore.services.s3.provider import (
    _cors_store,
    _cors_to_xml,
    _legal_hold_to_xml,
    _lifecycle_store,
    _lifecycle_to_xml,
    _notification_config_to_xml,
    _object_legal_hold_store,
    _object_lock_store,
    _object_lock_to_xml,
    _parse_cors_xml,
    _parse_legal_hold_xml,
    _parse_lifecycle_xml,
    _parse_notification_config_xml,
    _parse_object_lock_xml,
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

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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

    return Request(scope, receive)


def _clear_stores():
    _cors_store.clear()
    _lifecycle_store.clear()
    _object_lock_store.clear()
    _object_legal_hold_store.clear()
    _bucket_notifications.clear()


# ===================================================================
# 1. Presigned URL parsing and validation
# ===================================================================


class TestPresignedUrlParsingSigV4:
    def test_parse_sigv4_url(self):
        url = (
            "http://localhost:4566/mybucket/mykey"
            "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
            "&X-Amz-Credential=AKID/20260301/us-east-1/s3/aws4_request"
            "&X-Amz-Date=20260301T120000Z"
            "&X-Amz-Expires=3600"
            "&X-Amz-SignedHeaders=host"
            "&X-Amz-Signature=abcdef1234567890"
        )
        info = parse_presigned_url(url)
        assert info is not None
        assert info.version == "v4"
        assert info.bucket == "mybucket"
        assert info.key == "mykey"
        assert info.expires == 3600
        assert info.signature == "abcdef1234567890"
        assert "AKID" in info.credential
        assert info.signed_headers == "host"
        assert info.date == "20260301T120000Z"

    def test_parse_sigv4_with_security_token(self):
        url = (
            "http://localhost:4566/bucket/key"
            "?X-Amz-Signature=sig"
            "&X-Amz-Credential=AKID/20260101/us-east-1/s3/aws4_request"
            "&X-Amz-Date=20260101T000000Z"
            "&X-Amz-Expires=3600"
            "&X-Amz-SignedHeaders=host"
            "&X-Amz-Security-Token=my-session-token"
        )
        info = parse_presigned_url(url)
        assert info is not None
        assert info.security_token == "my-session-token"

    def test_sigv4_not_expired(self):
        # Set date far in the future
        url = (
            "http://localhost:4566/b/k"
            "?X-Amz-Signature=sig"
            "&X-Amz-Credential=AKID/20300101/us-east-1/s3/aws4_request"
            "&X-Amz-Date=20300101T000000Z"
            "&X-Amz-Expires=86400"
            "&X-Amz-SignedHeaders=host"
        )
        info = parse_presigned_url(url)
        assert info is not None
        assert not info.is_expired
        assert validate_presigned_url(info) is True

    def test_sigv4_expired(self):
        url = (
            "http://localhost:4566/b/k"
            "?X-Amz-Signature=sig"
            "&X-Amz-Credential=AKID/20200101/us-east-1/s3/aws4_request"
            "&X-Amz-Date=20200101T000000Z"
            "&X-Amz-Expires=1"
            "&X-Amz-SignedHeaders=host"
        )
        info = parse_presigned_url(url)
        assert info is not None
        assert info.is_expired
        assert validate_presigned_url(info) is False


class TestPresignedUrlParsingSigV2:
    def test_parse_sigv2_url(self):
        future_ts = str(int(time.time()) + 3600)
        url = (
            f"http://localhost:4566/mybucket/mykey"
            f"?AWSAccessKeyId=AKID"
            f"&Signature=abcdef"
            f"&Expires={future_ts}"
        )
        info = parse_presigned_url(url)
        assert info is not None
        assert info.version == "v2"
        assert info.bucket == "mybucket"
        assert info.key == "mykey"
        assert info.credential == "AKID"
        assert info.signature == "abcdef"
        assert not info.is_expired

    def test_sigv2_expired(self):
        url = "http://localhost:4566/b/k?AWSAccessKeyId=AKID&Signature=sig&Expires=1000000"
        info = parse_presigned_url(url)
        assert info is not None
        assert info.is_expired

    def test_sigv2_not_expired(self):
        future_ts = str(int(time.time()) + 9999)
        url = f"http://localhost:4566/b/k?AWSAccessKeyId=AKID&Signature=sig&Expires={future_ts}"
        info = parse_presigned_url(url)
        assert info is not None
        assert not info.is_expired


class TestPresignedUrlNotPresigned:
    def test_no_signature_params(self):
        url = "http://localhost:4566/bucket/key?prefix=foo"
        info = parse_presigned_url(url)
        assert info is None

    def test_is_presigned_request_v4(self):
        params = QueryParams("X-Amz-Signature=abc")
        assert is_presigned_request(params) is True

    def test_is_presigned_request_v2(self):
        params = QueryParams("Signature=abc")
        assert is_presigned_request(params) is True

    def test_is_not_presigned_request(self):
        params = QueryParams("prefix=foo")
        assert is_presigned_request(params) is False


class TestPresignedUrlInfoDataclass:
    def test_dataclass_fields(self):
        info = PresignedUrlInfo(
            version="v4",
            bucket="b",
            key="k",
            expires=3600,
            signature="sig",
            credential="cred",
            signed_headers="host",
            date="20260101T000000Z",
            security_token=None,
            is_expired=False,
        )
        assert info.version == "v4"
        assert info.security_token is None


# ===================================================================
# 2. Multipart upload lifecycle
# ===================================================================


@pytest.mark.asyncio
class TestMultipartUploadRouting:
    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_create_multipart_upload(self, mock_forward):
        """POST /<bucket>/<key>?uploads should route to Moto."""
        mock_forward.return_value = Response(
            content=b"<InitiateMultipartUploadResult/>",
            status_code=200,
        )
        req = _make_request("POST", "/mybucket/mykey", query_string=b"uploads")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        mock_forward.assert_called_once()

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_upload_part(self, mock_forward):
        """PUT /<bucket>/<key>?partNumber=1&uploadId=xxx routed to Moto."""
        mock_forward.return_value = Response(
            content=b"", status_code=200, headers={"ETag": '"abc"'}
        )
        req = _make_request(
            "PUT",
            "/mybucket/mykey",
            body=b"data",
            query_string=b"partNumber=1&uploadId=abc123",
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        mock_forward.assert_called_once()

    @patch("robotocore.services.s3.provider.fire_event")
    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_complete_multipart_fires_event(self, mock_forward, mock_fire):
        """POST /<bucket>/<key>?uploadId=xxx fires CompleteMultipartUpload."""
        mock_forward.return_value = Response(
            content=b"<CompleteMultipartUploadResult/>",
            status_code=200,
        )
        req = _make_request(
            "POST",
            "/mybucket/mykey",
            query_string=b"uploadId=abc123",
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        mock_fire.assert_called_once_with(
            "s3:ObjectCreated:CompleteMultipartUpload",
            "mybucket",
            "mykey",
            "us-east-1",
            "123456789012",
        )

    @patch("robotocore.services.s3.provider.fire_event")
    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_create_multipart_no_event(self, mock_forward, mock_fire):
        """CreateMultipartUpload should not fire any event."""
        mock_forward.return_value = Response(
            content=b"<InitiateMultipartUploadResult/>",
            status_code=200,
        )
        req = _make_request("POST", "/mybucket/mykey", query_string=b"uploads")
        await handle_s3_request(req, "us-east-1", "123456789012")
        mock_fire.assert_not_called()

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_abort_multipart(self, mock_forward):
        """DELETE /<bucket>/<key>?uploadId=xxx routes to Moto."""
        mock_forward.return_value = Response(content=b"", status_code=204)
        req = _make_request(
            "DELETE",
            "/mybucket/mykey",
            query_string=b"uploadId=abc123",
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204
        mock_forward.assert_called_once()

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_list_multipart_uploads(self, mock_forward):
        """GET /<bucket>?uploads routes to Moto."""
        mock_forward.return_value = Response(
            content=b"<ListMultipartUploadsResult/>",
            status_code=200,
        )
        req = _make_request("GET", "/mybucket", query_string=b"uploads")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        mock_forward.assert_called_once()

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_list_parts(self, mock_forward):
        """GET /<bucket>/<key>?uploadId=xxx routes to Moto."""
        mock_forward.return_value = Response(content=b"<ListPartsResult/>", status_code=200)
        req = _make_request(
            "GET",
            "/mybucket/mykey",
            query_string=b"uploadId=abc123",
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        mock_forward.assert_called_once()


# ===================================================================
# 3. CORS configuration and preflight
# ===================================================================


class TestCorsStore:
    def setup_method(self):
        _clear_stores()

    def test_set_and_get_cors(self):
        rules = [
            {
                "AllowedOrigins": ["*"],
                "AllowedMethods": ["GET"],
                "AllowedHeaders": [],
                "ExposeHeaders": [],
            }
        ]
        set_bucket_cors("mybucket", rules)
        assert get_bucket_cors("mybucket") == rules

    def test_get_missing_cors(self):
        assert get_bucket_cors("no-bucket") is None

    def test_delete_cors(self):
        set_bucket_cors("b", [{"AllowedOrigins": ["*"]}])
        delete_bucket_cors("b")
        assert get_bucket_cors("b") is None

    def test_delete_nonexistent_cors(self):
        delete_bucket_cors("nonexistent")  # should not raise


class TestParseCorsXml:
    def test_parse_single_rule(self):
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <CORSConfiguration>
          <CORSRule>
            <AllowedOrigin>http://example.com</AllowedOrigin>
            <AllowedMethod>GET</AllowedMethod>
            <AllowedMethod>PUT</AllowedMethod>
            <AllowedHeader>Authorization</AllowedHeader>
            <ExposeHeader>x-amz-request-id</ExposeHeader>
            <MaxAgeSeconds>3000</MaxAgeSeconds>
          </CORSRule>
        </CORSConfiguration>"""
        rules = _parse_cors_xml(xml)
        assert len(rules) == 1
        assert "http://example.com" in rules[0]["AllowedOrigins"]
        assert "GET" in rules[0]["AllowedMethods"]
        assert "PUT" in rules[0]["AllowedMethods"]
        assert "Authorization" in rules[0]["AllowedHeaders"]
        assert "x-amz-request-id" in rules[0]["ExposeHeaders"]
        assert rules[0]["MaxAgeSeconds"] == 3000

    def test_parse_multiple_rules(self):
        xml = """<CORSConfiguration>
          <CORSRule>
            <AllowedOrigin>*</AllowedOrigin>
            <AllowedMethod>GET</AllowedMethod>
          </CORSRule>
          <CORSRule>
            <AllowedOrigin>http://app.com</AllowedOrigin>
            <AllowedMethod>POST</AllowedMethod>
          </CORSRule>
        </CORSConfiguration>"""
        rules = _parse_cors_xml(xml)
        assert len(rules) == 2

    def test_parse_invalid_xml(self):
        assert _parse_cors_xml("not xml") == []

    def test_parse_empty_config(self):
        xml = "<CORSConfiguration></CORSConfiguration>"
        assert _parse_cors_xml(xml) == []


class TestCorsToXml:
    def test_round_trip(self):
        rules = [
            {
                "AllowedOrigins": ["*"],
                "AllowedMethods": ["GET", "PUT"],
                "AllowedHeaders": ["*"],
                "ExposeHeaders": ["ETag"],
                "MaxAgeSeconds": 600,
            }
        ]
        xml = _cors_to_xml(rules)
        assert "<CORSRule>" in xml
        assert "<AllowedOrigin>*</AllowedOrigin>" in xml
        assert "<AllowedMethod>GET</AllowedMethod>" in xml
        assert "<MaxAgeSeconds>600</MaxAgeSeconds>" in xml
        assert "<ExposeHeader>ETag</ExposeHeader>" in xml


@pytest.mark.asyncio
class TestCorsPreflightHandler:
    def setup_method(self):
        _clear_stores()

    async def test_options_no_cors_returns_403(self):
        req = _make_request(
            "OPTIONS",
            "/mybucket",
            headers={"Origin": "http://example.com"},
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 403

    async def test_options_with_cors_returns_headers(self):
        set_bucket_cors(
            "mybucket",
            [
                {
                    "AllowedOrigins": ["http://example.com"],
                    "AllowedMethods": ["GET", "PUT"],
                    "AllowedHeaders": ["Authorization"],
                    "ExposeHeaders": ["x-amz-request-id"],
                    "MaxAgeSeconds": 3000,
                }
            ],
        )
        req = _make_request(
            "OPTIONS",
            "/mybucket",
            headers={
                "Origin": "http://example.com",
                "Access-Control-Request-Method": "GET",
            },
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert resp.headers["access-control-allow-origin"] == ("http://example.com")
        assert "GET" in resp.headers["access-control-allow-methods"]
        assert resp.headers["access-control-max-age"] == "3000"

    async def test_options_wildcard_origin(self):
        set_bucket_cors(
            "mybucket",
            [
                {
                    "AllowedOrigins": ["*"],
                    "AllowedMethods": ["GET"],
                    "AllowedHeaders": [],
                    "ExposeHeaders": [],
                }
            ],
        )
        req = _make_request(
            "OPTIONS",
            "/mybucket",
            headers={
                "Origin": "http://anything.com",
                "Access-Control-Request-Method": "GET",
            },
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

    async def test_options_wrong_method_returns_403(self):
        set_bucket_cors(
            "mybucket",
            [
                {
                    "AllowedOrigins": ["*"],
                    "AllowedMethods": ["GET"],
                    "AllowedHeaders": [],
                    "ExposeHeaders": [],
                }
            ],
        )
        req = _make_request(
            "OPTIONS",
            "/mybucket",
            headers={
                "Origin": "http://example.com",
                "Access-Control-Request-Method": "DELETE",
            },
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 403

    async def test_options_wrong_origin_returns_403(self):
        set_bucket_cors(
            "mybucket",
            [
                {
                    "AllowedOrigins": ["http://allowed.com"],
                    "AllowedMethods": ["GET"],
                    "AllowedHeaders": [],
                    "ExposeHeaders": [],
                }
            ],
        )
        req = _make_request(
            "OPTIONS",
            "/mybucket",
            headers={
                "Origin": "http://forbidden.com",
                "Access-Control-Request-Method": "GET",
            },
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 403


@pytest.mark.asyncio
class TestCorsConfigEndpoint:
    def setup_method(self):
        _clear_stores()

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_put_cors(self, mock_forward):
        xml = (
            b"<CORSConfiguration>"
            b"<CORSRule>"
            b"<AllowedOrigin>*</AllowedOrigin>"
            b"<AllowedMethod>GET</AllowedMethod>"
            b"</CORSRule>"
            b"</CORSConfiguration>"
        )
        req = _make_request("PUT", "/mybucket", body=xml, query_string=b"cors")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert get_bucket_cors("mybucket") is not None

    async def test_get_cors_no_config(self):
        req = _make_request("GET", "/mybucket", query_string=b"cors")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404

    async def test_get_cors_with_config(self):
        set_bucket_cors(
            "mybucket",
            [
                {
                    "AllowedOrigins": ["*"],
                    "AllowedMethods": ["GET"],
                    "AllowedHeaders": [],
                    "ExposeHeaders": [],
                }
            ],
        )
        req = _make_request("GET", "/mybucket", query_string=b"cors")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert b"CORSConfiguration" in resp.body

    async def test_delete_cors(self):
        set_bucket_cors("mybucket", [{"AllowedOrigins": ["*"]}])
        req = _make_request("DELETE", "/mybucket", query_string=b"cors")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204
        assert get_bucket_cors("mybucket") is None


# ===================================================================
# 4. Bucket versioning (forwarded to Moto)
# ===================================================================


@pytest.mark.asyncio
class TestVersioningRouting:
    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_put_versioning_forwarded(self, mock_forward):
        mock_forward.return_value = Response(content=b"", status_code=200)
        req = _make_request(
            "PUT",
            "/mybucket",
            body=b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
            query_string=b"versioning",
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        mock_forward.assert_called_once()

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_get_versioning_forwarded(self, mock_forward):
        mock_forward.return_value = Response(content=b"<VersioningConfiguration/>", status_code=200)
        req = _make_request("GET", "/mybucket", query_string=b"versioning")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        mock_forward.assert_called_once()

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_get_with_version_id(self, mock_forward):
        mock_forward.return_value = Response(content=b"data", status_code=200)
        req = _make_request(
            "GET",
            "/mybucket/mykey",
            query_string=b"versionId=v1",
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        mock_forward.assert_called_once()

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_delete_with_version_id(self, mock_forward):
        mock_forward.return_value = Response(content=b"", status_code=204)
        req = _make_request(
            "DELETE",
            "/mybucket/mykey",
            query_string=b"versionId=v1",
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204
        mock_forward.assert_called_once()

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_list_object_versions(self, mock_forward):
        mock_forward.return_value = Response(content=b"<ListVersionsResult/>", status_code=200)
        req = _make_request("GET", "/mybucket", query_string=b"versions")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        mock_forward.assert_called_once()


# ===================================================================
# 5. Expanded event notifications
# ===================================================================


class TestExpandedNotifications:
    def setup_method(self):
        _clear_stores()

    def test_event_matches_all_event_types(self):
        """s3:* should match any event."""
        for event in [
            "s3:ObjectCreated:Put",
            "s3:ObjectRemoved:Delete",
            "s3:ObjectRestore:Post",
            "s3:Replication:OperationFailedReplication",
            "s3:ReducedRedundancyLostObject",
            "s3:ObjectTagging:Put",
        ]:
            assert _event_matches(event, ["s3:*"], "key", None) is True

    def test_object_created_wildcard(self):
        assert (
            _event_matches(
                "s3:ObjectCreated:CompleteMultipartUpload",
                ["s3:ObjectCreated:*"],
                "key",
                None,
            )
            is True
        )

    def test_object_removed_wildcard(self):
        assert (
            _event_matches(
                "s3:ObjectRemoved:DeleteMarkerCreated",
                ["s3:ObjectRemoved:*"],
                "key",
                None,
            )
            is True
        )

    def test_restore_wildcard(self):
        assert (
            _event_matches(
                "s3:ObjectRestore:Completed",
                ["s3:ObjectRestore:*"],
                "key",
                None,
            )
            is True
        )

    def test_replication_wildcard(self):
        assert (
            _event_matches(
                "s3:Replication:OperationNotTracked",
                ["s3:Replication:*"],
                "key",
                None,
            )
            is True
        )

    def test_tagging_wildcard(self):
        assert (
            _event_matches(
                "s3:ObjectTagging:Delete",
                ["s3:ObjectTagging:*"],
                "key",
                None,
            )
            is True
        )

    @patch("robotocore.services.s3.notifications._deliver_to_lambda")
    @patch("robotocore.services.s3.notifications._deliver_to_sqs")
    @patch("robotocore.services.s3.notifications._deliver_to_sns")
    def test_fire_event_with_lambda_target(self, mock_sns, mock_sqs, mock_lambda):
        cfg = NotificationConfig(
            lambda_configs=[
                {
                    "LambdaFunctionArn": "arn:aws:lambda:us-east-1:123:function:my-fn",
                    "Events": ["s3:ObjectCreated:*"],
                }
            ]
        )
        set_notification_config("bucket", cfg)
        fire_event("s3:ObjectCreated:Put", "bucket", "key", "us-east-1", "123")
        mock_lambda.assert_called_once()
        args = mock_lambda.call_args[0]
        assert args[0] == "arn:aws:lambda:us-east-1:123:function:my-fn"

    @patch("robotocore.services.s3.notifications._deliver_to_lambda")
    @patch("robotocore.services.s3.notifications._deliver_to_sqs")
    def test_fire_event_sqs_and_lambda(self, mock_sqs, mock_lambda):
        cfg = NotificationConfig(
            queue_configs=[
                {
                    "QueueArn": "arn:aws:sqs:us-east-1:123:q",
                    "Events": ["s3:ObjectCreated:*"],
                }
            ],
            lambda_configs=[
                {
                    "LambdaFunctionArn": "arn:aws:lambda:us-east-1:123:function:fn",
                    "Events": ["s3:ObjectCreated:*"],
                }
            ],
        )
        set_notification_config("bucket", cfg)
        fire_event("s3:ObjectCreated:Put", "bucket", "key", "us-east-1", "123")
        mock_sqs.assert_called_once()
        mock_lambda.assert_called_once()

    @patch("robotocore.services.s3.notifications._deliver_to_lambda")
    def test_lambda_filter_prevents_delivery(self, mock_lambda):
        cfg = NotificationConfig(
            lambda_configs=[
                {
                    "LambdaFunctionArn": "arn:aws:lambda:us-east-1:123:function:fn",
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {"Key": {"FilterRules": [{"Name": "suffix", "Value": ".jpg"}]}},
                }
            ]
        )
        set_notification_config("bucket", cfg)
        fire_event("s3:ObjectCreated:Put", "bucket", "file.txt", "us-east-1", "123")
        mock_lambda.assert_not_called()

    @patch("robotocore.services.s3.notifications._deliver_to_lambda")
    def test_lambda_only_config_fires(self, mock_lambda):
        """Config with only lambda targets should still fire events."""
        cfg = NotificationConfig(
            lambda_configs=[
                {
                    "LambdaFunctionArn": "arn:aws:lambda:us-east-1:123:function:fn",
                    "Events": ["s3:*"],
                }
            ]
        )
        set_notification_config("bucket", cfg)
        fire_event("s3:ObjectRemoved:Delete", "bucket", "key", "us-east-1", "123")
        mock_lambda.assert_called_once()


class TestDeliverToLambda:
    @patch("robotocore.services.lambda_.invoke.invoke_lambda_async")
    def test_deliver_invokes_lambda(self, mock_invoke):
        msg = json.dumps({"Records": [{"event": "test"}]})
        _deliver_to_lambda(
            "arn:aws:lambda:us-east-1:123:function:fn",
            msg,
            "us-east-1",
            "123",
        )
        mock_invoke.assert_called_once()
        call_args = mock_invoke.call_args[0]
        assert call_args[0] == "arn:aws:lambda:us-east-1:123:function:fn"
        assert call_args[1] == {"Records": [{"event": "test"}]}

    @patch(
        "robotocore.services.lambda_.invoke.invoke_lambda_async",
        side_effect=Exception("boom"),
    )
    def test_deliver_handles_exception(self, mock_invoke):
        """Should not raise even if Lambda invocation fails."""
        _deliver_to_lambda(
            "arn:aws:lambda:us-east-1:123:function:fn",
            '{"Records":[]}',
            "us-east-1",
            "123",
        )


class TestNotificationConfigWithLambda:
    def test_parse_lambda_config_xml(self):
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <NotificationConfiguration
            xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <LambdaFunctionConfiguration>
            <LambdaFunctionArn>arn:aws:lambda:us-east-1:123:function:my-fn</LambdaFunctionArn>
            <Event>s3:ObjectCreated:*</Event>
          </LambdaFunctionConfiguration>
        </NotificationConfiguration>"""
        config = _parse_notification_config_xml(xml)
        assert len(config.lambda_configs) == 1
        assert config.lambda_configs[0]["LambdaFunctionArn"] == (
            "arn:aws:lambda:us-east-1:123:function:my-fn"
        )
        assert "s3:ObjectCreated:*" in config.lambda_configs[0]["Events"]

    def test_parse_lambda_with_filter(self):
        xml = """<NotificationConfiguration>
          <LambdaFunctionConfiguration>
            <LambdaFunctionArn>arn:aws:lambda:us-east-1:123:function:fn</LambdaFunctionArn>
            <Event>s3:ObjectCreated:*</Event>
            <Filter>
              <S3Key>
                <FilterRule>
                  <Name>prefix</Name>
                  <Value>uploads/</Value>
                </FilterRule>
              </S3Key>
            </Filter>
          </LambdaFunctionConfiguration>
        </NotificationConfiguration>"""
        config = _parse_notification_config_xml(xml)
        assert len(config.lambda_configs) == 1
        rules = config.lambda_configs[0]["Filter"]["Key"]["FilterRules"]
        assert rules[0]["Name"] == "prefix"
        assert rules[0]["Value"] == "uploads/"

    def test_serialize_lambda_config(self):
        config = NotificationConfig(
            lambda_configs=[
                {
                    "LambdaFunctionArn": "arn:aws:lambda:us-east-1:123:function:fn",
                    "Events": ["s3:ObjectCreated:*"],
                }
            ]
        )
        xml = _notification_config_to_xml(config)
        assert "<LambdaFunctionConfiguration>" in xml
        assert "arn:aws:lambda:us-east-1:123:function:fn" in xml
        assert "s3:ObjectCreated:*" in xml

    def test_serialize_all_three_targets(self):
        config = NotificationConfig(
            queue_configs=[
                {
                    "QueueArn": "arn:aws:sqs:us-east-1:123:q",
                    "Events": ["s3:ObjectCreated:*"],
                }
            ],
            topic_configs=[
                {
                    "TopicArn": "arn:aws:sns:us-east-1:123:t",
                    "Events": ["s3:ObjectRemoved:*"],
                }
            ],
            lambda_configs=[
                {
                    "LambdaFunctionArn": "arn:aws:lambda:us-east-1:123:function:fn",
                    "Events": ["s3:*"],
                }
            ],
        )
        xml = _notification_config_to_xml(config)
        assert "<QueueConfiguration>" in xml
        assert "<TopicConfiguration>" in xml
        assert "<LambdaFunctionConfiguration>" in xml

    def test_parse_mixed_config_xml(self):
        xml = """<NotificationConfiguration
            xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <QueueConfiguration>
            <Queue>arn:aws:sqs:us-east-1:123:q</Queue>
            <Event>s3:ObjectCreated:*</Event>
          </QueueConfiguration>
          <TopicConfiguration>
            <Topic>arn:aws:sns:us-east-1:123:t</Topic>
            <Event>s3:ObjectRemoved:*</Event>
          </TopicConfiguration>
          <LambdaFunctionConfiguration>
            <LambdaFunctionArn>arn:aws:lambda:us-east-1:123:function:fn</LambdaFunctionArn>
            <Event>s3:*</Event>
          </LambdaFunctionConfiguration>
        </NotificationConfiguration>"""
        config = _parse_notification_config_xml(xml)
        assert len(config.queue_configs) == 1
        assert len(config.topic_configs) == 1
        assert len(config.lambda_configs) == 1


# ===================================================================
# 6. Lifecycle rules CRUD
# ===================================================================


class TestLifecycleStore:
    def setup_method(self):
        _clear_stores()

    def test_set_and_get(self):
        rules = [{"ID": "rule1", "Status": "Enabled", "Filter": {"Prefix": "logs/"}}]
        set_bucket_lifecycle("mybucket", rules)
        assert get_bucket_lifecycle("mybucket") == rules

    def test_get_missing(self):
        assert get_bucket_lifecycle("no-bucket") is None

    def test_delete(self):
        set_bucket_lifecycle("b", [{"ID": "r"}])
        delete_bucket_lifecycle("b")
        assert get_bucket_lifecycle("b") is None

    def test_delete_nonexistent(self):
        delete_bucket_lifecycle("nonexistent")  # should not raise


class TestParseLifecycleXml:
    def test_parse_basic_rule(self):
        xml = """<LifecycleConfiguration>
          <Rule>
            <ID>expire-logs</ID>
            <Status>Enabled</Status>
            <Filter><Prefix>logs/</Prefix></Filter>
            <Expiration><Days>30</Days></Expiration>
          </Rule>
        </LifecycleConfiguration>"""
        rules = _parse_lifecycle_xml(xml)
        assert len(rules) == 1
        assert rules[0]["ID"] == "expire-logs"
        assert rules[0]["Status"] == "Enabled"
        assert rules[0]["Filter"]["Prefix"] == "logs/"
        assert rules[0]["Expiration"]["Days"] == "30"

    def test_parse_transition_rule(self):
        xml = """<LifecycleConfiguration>
          <Rule>
            <ID>transition</ID>
            <Status>Enabled</Status>
            <Filter><Prefix></Prefix></Filter>
            <Transition>
              <Days>90</Days>
              <StorageClass>GLACIER</StorageClass>
            </Transition>
          </Rule>
        </LifecycleConfiguration>"""
        rules = _parse_lifecycle_xml(xml)
        assert len(rules) == 1
        assert len(rules[0]["Transitions"]) == 1
        assert rules[0]["Transitions"][0]["Days"] == "90"
        assert rules[0]["Transitions"][0]["StorageClass"] == "GLACIER"

    def test_parse_noncurrent_version_expiration(self):
        xml = """<LifecycleConfiguration>
          <Rule>
            <ID>noncurrent</ID>
            <Status>Enabled</Status>
            <NoncurrentVersionExpiration>
              <NoncurrentDays>30</NoncurrentDays>
            </NoncurrentVersionExpiration>
          </Rule>
        </LifecycleConfiguration>"""
        rules = _parse_lifecycle_xml(xml)
        assert rules[0]["NoncurrentVersionExpiration"]["NoncurrentDays"] == "30"

    def test_parse_abort_incomplete_multipart(self):
        xml = """<LifecycleConfiguration>
          <Rule>
            <ID>cleanup</ID>
            <Status>Enabled</Status>
            <AbortIncompleteMultipartUpload>
              <DaysAfterInitiation>7</DaysAfterInitiation>
            </AbortIncompleteMultipartUpload>
          </Rule>
        </LifecycleConfiguration>"""
        rules = _parse_lifecycle_xml(xml)
        assert rules[0]["AbortIncompleteMultipartUpload"]["DaysAfterInitiation"] == "7"

    def test_parse_invalid_xml(self):
        assert _parse_lifecycle_xml("not xml") == []

    def test_parse_multiple_rules(self):
        xml = """<LifecycleConfiguration>
          <Rule><ID>r1</ID><Status>Enabled</Status></Rule>
          <Rule><ID>r2</ID><Status>Disabled</Status></Rule>
        </LifecycleConfiguration>"""
        rules = _parse_lifecycle_xml(xml)
        assert len(rules) == 2


class TestLifecycleToXml:
    def test_basic_serialization(self):
        rules = [
            {
                "ID": "expire-logs",
                "Status": "Enabled",
                "Filter": {"Prefix": "logs/"},
                "Expiration": {"Days": "30"},
            }
        ]
        xml = _lifecycle_to_xml(rules)
        assert "<LifecycleConfiguration" in xml
        assert "<ID>expire-logs</ID>" in xml
        assert "<Prefix>logs/</Prefix>" in xml
        assert "<Days>30</Days>" in xml

    def test_transition_serialization(self):
        rules = [
            {
                "ID": "t",
                "Status": "Enabled",
                "Transitions": [{"Days": "90", "StorageClass": "GLACIER"}],
            }
        ]
        xml = _lifecycle_to_xml(rules)
        assert "<Transition>" in xml
        assert "<StorageClass>GLACIER</StorageClass>" in xml


@pytest.mark.asyncio
class TestLifecycleEndpoint:
    def setup_method(self):
        _clear_stores()

    async def test_get_lifecycle_no_config(self):
        req = _make_request("GET", "/mybucket", query_string=b"lifecycle")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404

    async def test_put_lifecycle(self):
        xml = (
            b"<LifecycleConfiguration>"
            b"<Rule><ID>r</ID><Status>Enabled</Status>"
            b"<Expiration><Days>30</Days></Expiration>"
            b"</Rule></LifecycleConfiguration>"
        )
        req = _make_request("PUT", "/mybucket", body=xml, query_string=b"lifecycle")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert get_bucket_lifecycle("mybucket") is not None

    async def test_get_lifecycle_with_config(self):
        set_bucket_lifecycle(
            "mybucket",
            [{"ID": "r", "Status": "Enabled"}],
        )
        req = _make_request("GET", "/mybucket", query_string=b"lifecycle")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert b"LifecycleConfiguration" in resp.body

    async def test_delete_lifecycle(self):
        set_bucket_lifecycle("mybucket", [{"ID": "r"}])
        req = _make_request("DELETE", "/mybucket", query_string=b"lifecycle")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204
        assert get_bucket_lifecycle("mybucket") is None


# ===================================================================
# 7. Object lock and legal hold
# ===================================================================


class TestObjectLockStore:
    def setup_method(self):
        _clear_stores()

    def test_set_and_get(self):
        config = {
            "ObjectLockEnabled": "Enabled",
            "Rule": {
                "DefaultRetention": {
                    "Mode": "GOVERNANCE",
                    "Days": "30",
                }
            },
        }
        set_object_lock_config("mybucket", config)
        assert get_object_lock_config("mybucket") == config

    def test_get_missing(self):
        assert get_object_lock_config("no-bucket") is None


class TestObjectLockXml:
    def test_parse_object_lock_xml(self):
        xml = """<ObjectLockConfiguration>
          <ObjectLockEnabled>Enabled</ObjectLockEnabled>
          <Rule>
            <DefaultRetention>
              <Mode>GOVERNANCE</Mode>
              <Days>30</Days>
            </DefaultRetention>
          </Rule>
        </ObjectLockConfiguration>"""
        config = _parse_object_lock_xml(xml)
        assert config["ObjectLockEnabled"] == "Enabled"
        assert config["Rule"]["DefaultRetention"]["Mode"] == "GOVERNANCE"
        assert config["Rule"]["DefaultRetention"]["Days"] == "30"

    def test_parse_invalid_xml(self):
        assert _parse_object_lock_xml("bad xml") == {}

    def test_serialization_round_trip(self):
        config = {
            "ObjectLockEnabled": "Enabled",
            "Rule": {
                "DefaultRetention": {
                    "Mode": "COMPLIANCE",
                    "Years": "1",
                }
            },
        }
        xml = _object_lock_to_xml(config)
        assert "<ObjectLockEnabled>Enabled</ObjectLockEnabled>" in xml
        assert "<Mode>COMPLIANCE</Mode>" in xml
        assert "<Years>1</Years>" in xml


class TestLegalHoldStore:
    def setup_method(self):
        _clear_stores()

    def test_set_and_get(self):
        set_object_legal_hold("mybucket", "mykey", "ON")
        assert get_object_legal_hold("mybucket", "mykey") == "ON"

    def test_get_missing(self):
        assert get_object_legal_hold("mybucket", "nokey") is None

    def test_update(self):
        set_object_legal_hold("mybucket", "mykey", "ON")
        set_object_legal_hold("mybucket", "mykey", "OFF")
        assert get_object_legal_hold("mybucket", "mykey") == "OFF"


class TestLegalHoldXml:
    def test_parse_legal_hold(self):
        xml = "<LegalHold><Status>ON</Status></LegalHold>"
        assert _parse_legal_hold_xml(xml) == "ON"

    def test_parse_legal_hold_off(self):
        xml = "<LegalHold><Status>OFF</Status></LegalHold>"
        assert _parse_legal_hold_xml(xml) == "OFF"

    def test_parse_invalid(self):
        assert _parse_legal_hold_xml("bad") == "OFF"

    def test_serialize(self):
        xml = _legal_hold_to_xml("ON")
        assert "<Status>ON</Status>" in xml


@pytest.mark.asyncio
class TestObjectLockEndpoint:
    def setup_method(self):
        _clear_stores()

    async def test_put_object_lock(self):
        xml = (
            b"<ObjectLockConfiguration>"
            b"<ObjectLockEnabled>Enabled</ObjectLockEnabled>"
            b"</ObjectLockConfiguration>"
        )
        req = _make_request("PUT", "/mybucket", body=xml, query_string=b"object-lock")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert get_object_lock_config("mybucket") is not None

    async def test_get_object_lock_no_config(self):
        req = _make_request("GET", "/mybucket", query_string=b"object-lock")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404

    async def test_get_object_lock_with_config(self):
        set_object_lock_config("mybucket", {"ObjectLockEnabled": "Enabled"})
        req = _make_request("GET", "/mybucket", query_string=b"object-lock")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert b"ObjectLockConfiguration" in resp.body


@pytest.mark.asyncio
class TestLegalHoldEndpoint:
    def setup_method(self):
        _clear_stores()

    async def test_put_legal_hold(self):
        xml = b"<LegalHold><Status>ON</Status></LegalHold>"
        req = _make_request(
            "PUT",
            "/mybucket/mykey",
            body=xml,
            query_string=b"legal-hold",
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert get_object_legal_hold("mybucket", "mykey") == "ON"

    async def test_get_legal_hold_not_set(self):
        req = _make_request(
            "GET",
            "/mybucket/mykey",
            query_string=b"legal-hold",
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404

    async def test_get_legal_hold(self):
        set_object_legal_hold("mybucket", "mykey", "ON")
        req = _make_request(
            "GET",
            "/mybucket/mykey",
            query_string=b"legal-hold",
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert b"<Status>ON</Status>" in resp.body

    async def test_legal_hold_no_key_returns_400(self):
        req = _make_request(
            "PUT",
            "/mybucket",
            body=b"<LegalHold><Status>ON</Status></LegalHold>",
            query_string=b"legal-hold",
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400


# ===================================================================
# 8. Additional presigned URL provider-level tests
# ===================================================================


@pytest.mark.asyncio
class TestPresignedUrlProvider:
    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_presigned_get_strips_params(self, mock_forward):
        mock_forward.return_value = Response(content=b"object-data", status_code=200)
        qs = (
            b"X-Amz-Algorithm=AWS4-HMAC-SHA256"
            b"&X-Amz-Credential=AKID/20990101/us-east-1/s3/aws4_request"
            b"&X-Amz-Date=20990101T000000Z"
            b"&X-Amz-Expires=3600"
            b"&X-Amz-SignedHeaders=host"
            b"&X-Amz-Signature=abc123"
        )
        req = _make_request("GET", "/mybucket/mykey", query_string=qs)
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        # Moto should have been called with stripped params
        mock_forward.assert_called_once()

    async def test_sigv4_expired_returns_403(self):
        """Expired SigV4 presigned URL should return 403 AccessDenied."""
        qs = (
            b"X-Amz-Algorithm=AWS4-HMAC-SHA256"
            b"&X-Amz-Credential=AKID/20260101/us-east-1/s3/aws4_request"
            b"&X-Amz-Date=20200101T000000Z"
            b"&X-Amz-Expires=1"
            b"&X-Amz-SignedHeaders=host"
            b"&X-Amz-Signature=abc123"
        )
        req = _make_request("GET", "/mybucket/mykey", query_string=qs)
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 403
        assert b"AccessDenied" in resp.body
        assert b"Request has expired" in resp.body

    async def test_sigv2_expired_returns_403(self):
        """Expired SigV2 presigned URL should return 403 AccessDenied."""
        qs = b"AWSAccessKeyId=AKID&Signature=sig&Expires=1000000000"
        req = _make_request("GET", "/mybucket/mykey", query_string=qs)
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 403
        assert b"AccessDenied" in resp.body

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_sigv4_not_expired_passes_through(self, mock_forward):
        """Non-expired SigV4 presigned URL should be forwarded to Moto."""
        mock_forward.return_value = Response(content=b"ok", status_code=200)
        qs = (
            b"X-Amz-Algorithm=AWS4-HMAC-SHA256"
            b"&X-Amz-Credential=AKID/20260101/us-east-1/s3/aws4_request"
            b"&X-Amz-Date=20990101T000000Z"
            b"&X-Amz-Expires=3600"
            b"&X-Amz-SignedHeaders=host"
            b"&X-Amz-Signature=abc123"
        )
        req = _make_request("GET", "/mybucket/mykey", query_string=qs)
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        mock_forward.assert_called_once()

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_presigned_get_applies_response_header_overrides(self, mock_forward):
        mock_forward.return_value = Response(content=b"object-data", status_code=200)
        qs = (
            b"X-Amz-Algorithm=AWS4-HMAC-SHA256"
            b"&X-Amz-Credential=AKID/20990101/us-east-1/s3/aws4_request"
            b"&X-Amz-Date=20990101T000000Z"
            b"&X-Amz-Expires=3600"
            b"&X-Amz-SignedHeaders=host"
            b"&X-Amz-Signature=abc123"
            b"&response-content-disposition=attachment%3B+filename%3D%22unit.txt%22"
            b"&response-content-type=text%2Fplain"
        )
        req = _make_request("GET", "/mybucket/mykey", query_string=qs)
        resp = await handle_s3_request(req, "us-east-1", "123456789012")

        assert resp.status_code == 200
        assert resp.headers["content-disposition"] == 'attachment; filename="unit.txt"'
        assert resp.headers["content-type"] == "text/plain"

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_presigned_put_preserves_body(self, mock_forward):
        mock_forward.return_value = Response(
            content=b"", status_code=200, headers={"ETag": '"abc"'}
        )
        qs = b"X-Amz-Signature=abc123&X-Amz-Credential=AKID/20260101/us-east-1/s3/aws4_request"
        req = _make_request(
            "PUT",
            "/mybucket/mykey",
            body=b"file-data",
            query_string=qs,
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_sigv2_presigned(self, mock_forward):
        mock_forward.return_value = Response(content=b"data", status_code=200)
        qs = b"AWSAccessKeyId=AKID&Signature=sig&Expires=9999999999"
        req = _make_request("GET", "/mybucket/mykey", query_string=qs)
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        mock_forward.assert_called_once()


# ===================================================================
# Additional edge-case tests
# ===================================================================


class TestStripPresignedEdgeCases:
    def test_no_query_params_at_all(self):
        req = _make_request("GET", "/mybucket/mykey")
        new_req = _strip_presigned_params(req)
        assert new_req.scope["query_string"] == b""

    def test_existing_auth_header_preserved(self):
        req = _make_request(
            "GET",
            "/mybucket/mykey",
            headers={"Authorization": "AWS4-HMAC-SHA256 existing"},
            query_string=b"X-Amz-Signature=abc",
        )
        new_req = _strip_presigned_params(req)
        # Should not inject a second auth header
        auth_count = sum(1 for k, v in new_req.scope["headers"] if k == b"authorization")
        assert auth_count == 1


@pytest.mark.asyncio
class TestHandleS3RequestEdgeCases:
    @patch("robotocore.services.s3.provider.forward_to_moto")
    async def test_post_without_upload_fires_post_event(self, mock_forward):
        mock_forward.return_value = Response(content=b"", status_code=200)
        req = _make_request("POST", "/mybucket/mykey")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

    @patch("robotocore.services.s3.provider.forward_to_moto")
    @patch("robotocore.services.s3.provider.fire_event")
    async def test_put_without_key_no_event(self, mock_fire, mock_forward):
        """PUT to /<bucket> (e.g., create bucket) should not fire event."""
        mock_forward.return_value = Response(content=b"", status_code=200)
        req = _make_request("PUT", "/mybucket")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        mock_fire.assert_not_called()

    async def test_options_no_bucket_match(self):
        req = _make_request("OPTIONS", "/")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
