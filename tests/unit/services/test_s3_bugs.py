"""Failing tests exposing correctness bugs in the S3 native provider.

Each test documents a specific bug with a docstring explaining the issue.
These tests are expected to FAIL — they demonstrate real bugs, not regressions.
"""

import time
from unittest.mock import patch

import pytest
from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.s3.notifications import (
    _bucket_notifications,
    _build_event_record,
)
from robotocore.services.s3.provider import (
    _cors_store,
    _lifecycle_store,
    _logging_store,
    _object_legal_hold_store,
    _object_lock_store,
    _origin_matches,
    handle_s3_request,
    set_bucket_cors,
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
    _logging_store.clear()
    _bucket_notifications.clear()


# ===========================================================================
# Bug 1: _build_event_record truncates eventName to just the last segment
# ===========================================================================


class TestEventNameTruncation:
    def test_event_name_includes_category_and_action(self):
        """Bug: _build_event_record uses event_name.split(':')[-1] which returns
        just 'Put' for 's3:ObjectCreated:Put'. AWS returns 'ObjectCreated:Put'
        (without the 's3:' prefix). The current code strips too much."""
        record = _build_event_record(
            "s3:ObjectCreated:Put",
            "my-bucket",
            "my-key",
            "us-east-1",
            "123456789012",
            1024,
            "abc123",
        )
        # AWS returns "ObjectCreated:Put", not just "Put"
        assert record["eventName"] == "ObjectCreated:Put"

    def test_event_name_for_delete(self):
        """Same bug: 's3:ObjectRemoved:Delete' should become 'ObjectRemoved:Delete',
        not just 'Delete'."""
        record = _build_event_record(
            "s3:ObjectRemoved:Delete",
            "my-bucket",
            "my-key",
            "us-east-1",
            "123456789012",
            0,
            "",
        )
        assert record["eventName"] == "ObjectRemoved:Delete"

    def test_event_name_for_complete_multipart(self):
        """Same bug for CompleteMultipartUpload."""
        record = _build_event_record(
            "s3:ObjectCreated:CompleteMultipartUpload",
            "my-bucket",
            "my-key",
            "us-east-1",
            "123456789012",
            0,
            "",
        )
        assert record["eventName"] == "ObjectCreated:CompleteMultipartUpload"


# ===========================================================================
# Bug 2: _origin_matches doesn't support wildcard patterns in origins
# ===========================================================================


class TestCorsWildcardOriginMatching:
    def test_wildcard_subdomain_pattern(self):
        """Bug: AWS S3 CORS supports wildcard origins like 'http://*.example.com'
        which should match 'http://foo.example.com'. The current implementation
        only checks exact match or '*' (match-all), so wildcard patterns in
        origin strings are never matched."""
        assert _origin_matches("http://foo.example.com", ["http://*.example.com"]) is True

    def test_wildcard_subdomain_no_match(self):
        """Wildcard pattern should NOT match a different domain."""
        assert _origin_matches("http://foo.other.com", ["http://*.example.com"]) is False

    def test_wildcard_in_protocol(self):
        """AWS allows wildcard origins like '*.example.com'."""
        assert _origin_matches("http://example.com", ["*.example.com"]) is True


# ===========================================================================
# Bug 3: CORS preflight doesn't validate Access-Control-Request-Headers
# ===========================================================================


@pytest.mark.asyncio
class TestCorsPreflightHeaderValidation:
    async def test_rejects_disallowed_request_headers(self):
        """Bug: AWS S3 validates that all headers in Access-Control-Request-Headers
        are present in the CORS rule's AllowedHeaders. The current implementation
        ignores Access-Control-Request-Headers entirely, allowing requests with
        any headers to pass preflight, which diverges from real AWS behavior."""
        _clear_stores()
        set_bucket_cors(
            "cors-bucket",
            [
                {
                    "AllowedOrigins": ["http://example.com"],
                    "AllowedMethods": ["GET"],
                    "AllowedHeaders": ["Content-Type"],  # only Content-Type is allowed
                    "ExposeHeaders": [],
                }
            ],
        )
        req = _make_request(
            "OPTIONS",
            "/cors-bucket/key",
            headers={
                "Origin": "http://example.com",
                "Access-Control-Request-Method": "GET",
                # X-Custom-Header is NOT in AllowedHeaders
                "Access-Control-Request-Headers": "X-Custom-Header",
            },
        )
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        # AWS would return 403 because X-Custom-Header is not allowed
        assert resp.status_code == 403


# ===========================================================================
# Bug 4: SigV4 expiration check uses local time instead of UTC
# ===========================================================================


class TestSigV4ExpirationTimezone:
    def test_expiration_uses_utc_not_local_time(self):
        """Bug: _check_sigv4_expiration uses time.mktime() which interprets the
        parsed struct_time as LOCAL time. But X-Amz-Date is always in UTC
        (denoted by the trailing 'Z'). This means in timezones east of UTC,
        URLs appear to expire later than they should, and in timezones west
        of UTC, they expire earlier. The correct function is calendar.timegm()
        which interprets struct_time as UTC.

        This test constructs a URL signed exactly 2 seconds ago with a 1-second
        expiry. It should be expired regardless of timezone. We patch time.time()
        to a known UTC epoch to make the test deterministic."""
        import calendar

        from robotocore.services.s3.presigned import _check_sigv4_expiration

        sign_epoch = calendar.timegm(time.strptime("20260101T000000Z", "%Y%m%dT%H%M%SZ"))

        with patch("robotocore.services.s3.presigned.time") as mock_time:
            mock_time.strptime = time.strptime
            # Set current time to 2 seconds after signing
            mock_time.time.return_value = sign_epoch + 2
            # mktime interprets as local time -- this is the bug.
            # We need to verify the function uses timegm (UTC), not mktime (local).
            # If we're in UTC, mktime and timegm give the same result, so we
            # need to make mktime return a different value than timegm would.
            # Simulate being in UTC+5 timezone: mktime would return sign_epoch - 18000
            mock_time.mktime = lambda t: calendar.timegm(t) - 18000  # UTC+5

            expired = _check_sigv4_expiration("20260101T000000Z", 1)
            # The URL was signed at epoch sign_epoch, with 1s expiry.
            # Current time is sign_epoch + 2, so it IS expired.
            # But with the mktime bug (UTC+5), sign_time becomes sign_epoch - 18000,
            # so expiry would be at sign_epoch - 17999, and current time > that,
            # so it appears expired. Let's test the other direction.

        # Better test: simulate UTC-8. mktime returns sign_epoch + 28800.
        # Then expiry = sign_epoch + 28801, and current time = sign_epoch + 2,
        # so it appears NOT expired when it actually IS.
        with patch("robotocore.services.s3.presigned.time") as mock_time:
            mock_time.strptime = time.strptime
            mock_time.time.return_value = sign_epoch + 2  # 2s after signing
            # Simulate UTC-8: mktime adds 8 hours to the epoch
            mock_time.mktime = lambda t: calendar.timegm(t) + 28800

            expired = _check_sigv4_expiration("20260101T000000Z", 1)
            # Should be expired (signed 2s ago, 1s expiry) but mktime bug
            # makes it think sign_time is 8 hours in the future
            assert expired is True, (
                "URL signed 2 seconds ago with 1-second expiry should be expired, "
                "but mktime() interprets UTC timestamp as local time"
            )


# ===========================================================================
# Bug 5: Bucket deletion doesn't clean up provider-side stores
# ===========================================================================


@pytest.mark.asyncio
class TestBucketDeletionStoreCleanup:
    async def test_cors_store_leaks_after_bucket_deletion(self):
        """Bug: When a bucket is deleted via Moto (DELETE /bucket), the S3 provider's
        in-memory _cors_store is never cleaned up. This means if you create a new
        bucket with the same name, it inherits the old CORS configuration.
        Real AWS does not exhibit this behavior — deleted bucket configs are gone."""
        _clear_stores()
        set_bucket_cors("deleteme", [{"AllowedOrigins": ["*"], "AllowedMethods": ["GET"]}])

        # Simulate Moto successfully deleting the bucket
        with patch("robotocore.services.s3.provider.forward_to_moto") as mock_forward:
            mock_forward.return_value = Response(content=b"", status_code=204)
            req = _make_request("DELETE", "/deleteme")
            await handle_s3_request(req, "us-east-1", "123456789012")

        # After bucket deletion, CORS config should be gone
        from robotocore.services.s3.provider import get_bucket_cors

        assert get_bucket_cors("deleteme") is None, (
            "CORS config leaked after bucket deletion — _cors_store not cleaned up"
        )


# ===========================================================================
# Bug 6: Legal hold GET returns wrong error code for unset holds
# ===========================================================================


@pytest.mark.asyncio
class TestLegalHoldErrorCode:
    async def test_unset_legal_hold_returns_correct_error(self):
        """Bug: GET ?legal-hold on an object without a legal hold returns
        <Code>NoSuchKey</Code>, which implies the object doesn't exist.
        AWS actually returns <Code>InvalidRequest</Code> with a message like
        'Bucket is missing Object Lock Configuration' or returns the default
        status OFF. NoSuchKey is misleading because the object may well exist —
        it just doesn't have a legal hold set."""
        _clear_stores()
        req = _make_request("GET", "/mybucket/mykey", query_string=b"legal-hold")
        resp = await handle_s3_request(req, "us-east-1", "123456789012")
        # Should NOT claim the key doesn't exist
        assert b"NoSuchKey" not in resp.body, (
            "Legal hold GET returns NoSuchKey error for objects without legal hold set. "
            "AWS returns the default hold status OFF or InvalidRequest, not NoSuchKey."
        )


# ===========================================================================
# Bug 7: CORS headers missing from normal (non-OPTIONS) responses
# ===========================================================================


@pytest.mark.asyncio
class TestCorsHeadersOnNormalResponses:
    async def test_get_response_includes_cors_headers_when_origin_matches(self):
        """Bug: Real AWS S3 adds Access-Control-Allow-Origin (and related CORS
        headers) to ALL responses when the request includes an Origin header
        that matches a CORS rule — not just OPTIONS preflight. The provider
        only handles CORS for OPTIONS requests, so normal GET/PUT/DELETE
        responses never include CORS headers, breaking browser-based apps."""
        _clear_stores()
        set_bucket_cors(
            "cors-bucket",
            [
                {
                    "AllowedOrigins": ["http://example.com"],
                    "AllowedMethods": ["GET"],
                    "AllowedHeaders": [],
                    "ExposeHeaders": ["x-amz-request-id"],
                }
            ],
        )

        with patch("robotocore.services.s3.provider.forward_to_moto") as mock_forward:
            mock_forward.return_value = Response(
                content=b"<data/>", status_code=200, media_type="application/xml"
            )
            req = _make_request(
                "GET",
                "/cors-bucket/mykey",
                headers={"Origin": "http://example.com"},
            )
            resp = await handle_s3_request(req, "us-east-1", "123456789012")

        assert resp.status_code == 200
        # Real AWS includes these headers on normal responses when Origin matches
        resp_headers = dict(resp.headers) if hasattr(resp, "headers") else {}
        assert "access-control-allow-origin" in resp_headers, (
            "CORS Access-Control-Allow-Origin header missing from normal GET response. "
            "AWS adds CORS headers to all responses when Origin matches a CORS rule."
        )
