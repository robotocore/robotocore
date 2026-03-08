"""Tests for correctness bugs found and fixed in the S3 native provider.

Each test documents a specific bug that has been fixed. Do NOT remove these tests.
"""

from __future__ import annotations

import calendar
import time
from unittest.mock import patch

from robotocore.services.s3.notifications import _build_event_record
from robotocore.services.s3.presigned import _check_sigv4_expiration

# ===========================================================================
# Bug 1: _build_event_record truncates eventName to just the last segment
# ===========================================================================


class TestEventNameTruncation:
    def test_event_name_includes_category_and_action(self):
        """'s3:ObjectCreated:Put' should become 'ObjectCreated:Put', not 'Put'."""
        record = _build_event_record(
            "s3:ObjectCreated:Put",
            "my-bucket",
            "my-key",
            "us-east-1",
            "123456789012",
            1024,
            "abc123",
        )
        assert record["eventName"] == "ObjectCreated:Put"

    def test_event_name_for_delete(self):
        """'s3:ObjectRemoved:Delete' should become 'ObjectRemoved:Delete'."""
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
# Bug 2: SigV4 expiration check uses local time instead of UTC
# ===========================================================================


class TestSigV4ExpirationTimezone:
    def test_expiration_uses_utc_not_local_time(self):
        """_check_sigv4_expiration must use calendar.timegm (UTC), not
        time.mktime (local time), to interpret X-Amz-Date timestamps."""
        sign_epoch = calendar.timegm(time.strptime("20260101T000000Z", "%Y%m%dT%H%M%SZ"))

        # Verify expired URL is detected as expired
        with patch("robotocore.services.s3.presigned.time") as mock_time:
            mock_time.strptime = time.strptime
            mock_time.time.return_value = sign_epoch + 2  # 2s after signing
            expired = _check_sigv4_expiration("20260101T000000Z", 1)
            assert expired is True, (
                "URL signed 2 seconds ago with 1-second expiry should be expired"
            )

    def test_not_expired_url(self):
        """A URL that hasn't expired yet should return False."""
        sign_epoch = calendar.timegm(time.strptime("20260101T000000Z", "%Y%m%dT%H%M%SZ"))

        with patch("robotocore.services.s3.presigned.time") as mock_time:
            mock_time.strptime = time.strptime
            mock_time.time.return_value = sign_epoch + 0.5  # 0.5s after signing
            expired = _check_sigv4_expiration("20260101T000000Z", 3600)
            assert expired is False
