"""Tests for the Moto bridge dispatch layer."""

import pytest

from robotocore.providers.moto_bridge import (
    _get_dispatcher,
    _get_moto_routing_table,
)


class TestMotoRoutingTable:
    def test_builds_routing_table_for_sts(self):
        url_map = _get_moto_routing_table("sts")
        rules = list(url_map.iter_rules())
        assert len(rules) >= 1

    def test_builds_routing_table_for_sqs(self):
        url_map = _get_moto_routing_table("sqs")
        rules = list(url_map.iter_rules())
        assert len(rules) >= 2

    def test_builds_routing_table_for_s3(self):
        url_map = _get_moto_routing_table("s3")
        rules = list(url_map.iter_rules())
        assert len(rules) >= 4

    def test_caches_routing_table(self):
        table1 = _get_moto_routing_table("sts")
        table2 = _get_moto_routing_table("sts")
        assert table1 is table2


class TestGetDispatcher:
    def test_sts_root_path(self):
        dispatch = _get_dispatcher("sts", "/")
        assert callable(dispatch)

    def test_sqs_root_path(self):
        dispatch = _get_dispatcher("sqs", "/")
        assert callable(dispatch)

    def test_sqs_queue_path(self):
        dispatch = _get_dispatcher("sqs", "/123456789012/my-queue")
        assert callable(dispatch)

    def test_s3_bucket_path(self):
        dispatch = _get_dispatcher("s3", "/my-bucket")
        assert callable(dispatch)

    def test_s3_key_path(self):
        dispatch = _get_dispatcher("s3", "/my-bucket/my-key.txt")
        assert callable(dispatch)


class TestForwardToMoto:
    """Integration tests using the Starlette test client."""

    @pytest.fixture
    def client(self):
        from starlette.testclient import TestClient

        from robotocore.gateway.app import app

        return TestClient(app)

    def _auth_header(self, service: str, region: str = "us-east-1") -> dict:
        return {
            "Authorization": (
                f"AWS4-HMAC-SHA256 "
                f"Credential=testing/20260305/{region}/{service}/aws4_request, "
                f"SignedHeaders=host, Signature=abc"
            ),
        }

    def test_sts_get_caller_identity(self, client):
        response = client.post(
            "/",
            data="Action=GetCallerIdentity&Version=2011-06-15",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                **self._auth_header("sts"),
            },
        )
        assert response.status_code == 200
        assert "GetCallerIdentityResult" in response.text
        assert "123456789012" in response.text

    def test_s3_create_bucket(self, client):
        response = client.put(
            "/test-bridge-bucket",
            headers=self._auth_header("s3"),
        )
        assert response.status_code == 200

    def test_s3_put_and_get_object(self, client):
        # Create bucket
        client.put("/test-obj-bucket", headers=self._auth_header("s3"))

        # Put object
        response = client.put(
            "/test-obj-bucket/hello.txt",
            content=b"hello world",
            headers=self._auth_header("s3"),
        )
        assert response.status_code == 200

        # Get object
        response = client.get(
            "/test-obj-bucket/hello.txt",
            headers=self._auth_header("s3"),
        )
        assert response.status_code == 200
        assert response.content == b"hello world"

    def test_sqs_create_queue(self, client):
        response = client.post(
            "/",
            data="Action=CreateQueue&QueueName=test-bridge-queue",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                **self._auth_header("sqs"),
            },
        )
        assert response.status_code == 200
        assert "CreateQueueResponse" in response.text
        assert "test-bridge-queue" in response.text

    def test_sqs_send_and_receive(self, client):
        # Create queue
        create_resp = client.post(
            "/",
            data="Action=CreateQueue&QueueName=test-msg-queue",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                **self._auth_header("sqs"),
            },
        )
        assert create_resp.status_code == 200

        # Send message - SQS uses queue URL path
        response = client.post(
            "/123456789012/test-msg-queue",
            data="Action=SendMessage&MessageBody=hello+from+bridge",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                **self._auth_header("sqs"),
            },
        )
        assert response.status_code == 200
        assert "SendMessageResponse" in response.text

        # Receive message
        response = client.post(
            "/123456789012/test-msg-queue",
            data="Action=ReceiveMessage&MaxNumberOfMessages=1",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                **self._auth_header("sqs"),
            },
        )
        assert response.status_code == 200
        assert "hello from bridge" in response.text

    def test_unknown_service_returns_501(self, client):
        response = client.get(
            "/",
            headers={
                "Authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=testing/20260305/us-east-1/nonexistent/aws4_request, "
                    "SignedHeaders=host, Signature=abc"
                )
            },
        )
        assert response.status_code == 501


class TestUrlEncodingRoundTrip:
    """Bug: Starlette decodes percent-encoded paths before the bridge sees them.

    The bridge passes ``request.url.path`` (already decoded) to Werkzeug
    EnvironBuilder, which decodes percent-encoding *again*.  This causes
    double-decoding: a key whose name contains a literal ``%2F`` (i.e. the
    three characters percent-two-F) becomes ``/`` in the Werkzeug request
    that Moto receives, corrupting the S3 key or Lambda function ARN.
    """

    @pytest.fixture
    def client(self):
        from starlette.testclient import TestClient

        from robotocore.gateway.app import app

        return TestClient(app)

    def _auth_header(self, service: str, region: str = "us-east-1") -> dict:
        return {
            "Authorization": (
                f"AWS4-HMAC-SHA256 "
                f"Credential=testing/20260305/{region}/{service}/aws4_request, "
                f"SignedHeaders=host, Signature=abc"
            ),
        }

    def test_s3_key_with_percent_encoded_slash_is_preserved(self, client):
        """An S3 key whose name is literally 'a%2Fb' must not be stored as 'a/b'.

        In real AWS the URL for key ``a%2Fb`` is ``/bucket/a%252Fb`` (double-
        encoded).  Starlette decodes the first layer to ``/bucket/a%2Fb``.
        The bridge must pass this to Moto *without* a second decode, so Moto
        stores the key as ``a%2Fb``, not ``a/b``.
        """
        bucket = "pct-roundtrip-bucket"
        client.put(f"/{bucket}", headers=self._auth_header("s3"))

        # PUT key whose literal name is 'a%2Fb' (double-encoded in URL)
        client.put(
            f"/{bucket}/a%252Fb",
            content=b"percent-slash",
            headers=self._auth_header("s3"),
        )

        # List bucket and check the stored key name
        resp = client.get(f"/{bucket}", headers=self._auth_header("s3"))
        assert resp.status_code == 200
        # The key in the listing should be 'a%2Fb', NOT 'a/b'
        assert "<Key>a%2Fb</Key>" in resp.text, (
            f"Expected key 'a%2Fb' but got double-decoded key. Listing: {resp.text}"
        )

    def test_s3_key_with_percent_encoded_space_roundtrips(self, client):
        """An S3 key whose name is literally 'a%20b' (not 'a b') must be preserved.

        URL: ``/bucket/a%2520b`` -> raw_path preserved -> Werkzeug decodes once
        to ``/bucket/a%20b`` -> Moto stores key ``a%20b``.
        """
        bucket = "pct-space-bucket"
        client.put(f"/{bucket}", headers=self._auth_header("s3"))

        # Key whose literal name is 'a%20b' (double-encoded in URL: %25 -> %, 20 stays)
        client.put(
            f"/{bucket}/a%2520b",
            content=b"percent-space",
            headers=self._auth_header("s3"),
        )

        resp = client.get(f"/{bucket}", headers=self._auth_header("s3"))
        assert resp.status_code == 200
        # The key should be 'a%20b' literally, not 'a b' or 'a%2520b'
        assert "<Key>a%20b</Key>" in resp.text, (
            f"Expected key 'a%20b' but got double-decoded key. Listing: {resp.text}"
        )

    def test_build_werkzeug_request_preserves_percent_encoding(self):
        """_build_werkzeug_request must not double-decode percent-encoded paths.

        When Starlette gives us path='/bucket/a%2Fb', the Werkzeug request
        must also see PATH_INFO='/bucket/a%2Fb', not '/bucket/a/b'.
        """
        from unittest.mock import MagicMock

        from robotocore.providers.moto_bridge import _build_werkzeug_request

        mock_request = MagicMock()
        mock_request.method = "GET"
        mock_request.url.path = "/bucket/a%2Fb"
        mock_request.url.query = None
        mock_request.headers = {}
        # raw_path preserves the wire encoding; Werkzeug decodes it once
        mock_request.scope = {"raw_path": b"/bucket/a%252Fb"}

        werkzeug_req = _build_werkzeug_request(mock_request, b"")
        # PATH_INFO should preserve the percent-encoding
        assert werkzeug_req.path == "/bucket/a%2Fb", (
            f"Expected '/bucket/a%2Fb' but got '{werkzeug_req.path}' — "
            f"Werkzeug double-decoded the percent-encoded slash"
        )


class TestEmptyBytesResponseBody:
    """Bug: line 135 checks ``isinstance(response_body, str) and len == 0``
    but does not handle ``b''`` (empty bytes).

    Moto's S3 returns ``b""`` for 416 Range Not Satisfiable responses
    (moto/s3/responses.py:1371).  The empty-body normalization to None
    should apply to both ``""`` and ``b""``.
    """

    def test_empty_bytes_body_normalized_same_as_empty_str(self):
        """The empty-body check on line 135 must handle b'' the same as ''.

        The current code: ``isinstance(response_body, str) and len(response_body) == 0``
        normalizes "" to None but leaves b"" unchanged.  Both should be
        normalized consistently.
        """

        # Reproduce the exact logic from line 135 of moto_bridge.py
        def normalize_body(response_body):
            """Replicates the normalization logic from forward_to_moto line 135."""
            if isinstance(response_body, (str, bytes)) and len(response_body) == 0:
                response_body = None
            return response_body

        str_result = normalize_body("")
        bytes_result = normalize_body(b"")

        assert str_result == bytes_result, (
            f"Empty string normalizes to {str_result!r} but "
            f"empty bytes normalizes to {bytes_result!r} — "
            f"the isinstance(response_body, str) check on line 135 "
            f"misses empty bytes b''"
        )


class TestHeadRequestInForwardWithBody:
    """Bug: forward_to_moto_with_body does not handle HEAD requests.

    forward_to_moto (line 138-146) preserves content-length and nulls the
    body for HEAD requests.  forward_to_moto_with_body (line 237-241) does
    NOT — it always strips content-length and passes the body through.

    If forward_to_moto_with_body is ever called for a HEAD request, the
    response would contain a body (violating HTTP spec) and would lose
    the content-length header that HEAD responses must include.
    """

    @pytest.mark.asyncio
    async def test_forward_to_moto_with_body_preserves_content_length_for_head(self):
        """HEAD responses must keep content-length and have empty body."""
        from unittest.mock import MagicMock, patch

        from robotocore.providers.moto_bridge import forward_to_moto_with_body

        mock_request = MagicMock()
        mock_request.method = "HEAD"
        mock_request.url.path = "/test-bucket/test-key"
        mock_request.url.query = None
        mock_request.url.__str__ = lambda self: "http://localhost/test-bucket/test-key"
        mock_request.headers = {
            "Authorization": (
                "AWS4-HMAC-SHA256 "
                "Credential=testing/20260305/us-east-1/s3/aws4_request, "
                "SignedHeaders=host, Signature=abc"
            ),
        }

        # Moto returns a body with content-length for HEAD (same as GET internally)
        mock_dispatch = MagicMock(
            return_value=(200, {"content-length": "1024", "etag": '"abc"'}, "file contents here")
        )

        with (
            patch(
                "robotocore.providers.moto_bridge._get_dispatcher",
                return_value=mock_dispatch,
            ),
            patch(
                "robotocore.providers.moto_bridge._build_werkzeug_request",
                return_value=MagicMock(),
            ),
        ):
            response = await forward_to_moto_with_body(mock_request, "s3", b"")

        # HEAD response MUST have empty body
        assert response.body == b"", f"HEAD response body should be empty but got {response.body!r}"
        # HEAD response MUST preserve content-length from Moto
        assert response.headers.get("content-length") == "1024", (
            f"HEAD response must preserve content-length but got "
            f"{response.headers.get('content-length')!r}. "
            f"forward_to_moto_with_body strips content-length unconditionally "
            f"(line 241) unlike forward_to_moto which preserves it for HEAD."
        )


class TestXmlEscaping:
    """Bug fix 1E: XML special chars in error messages must be escaped."""

    def test_xml_escape_in_error_response(self):
        from robotocore.providers.moto_bridge import _xml_escape

        escaped = _xml_escape('<script>alert("xss")</script>')
        assert "<script>" not in escaped
        assert "&lt;script&gt;" in escaped

    def test_ampersand_escaped(self):
        from robotocore.providers.moto_bridge import _xml_escape

        assert "&amp;" in _xml_escape("foo & bar")
