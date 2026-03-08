"""Tests for audit log bug fixes.

Validates fixes for real bugs found during code audit:
- AuditLog(max_size=0) was silently ignored due to `0 or 1000` falsy check
"""

from robotocore.audit.log import AuditLog


class TestAuditLogMaxSizeZero:
    """Fixed: AuditLog(max_size=0) now correctly creates a zero-capacity deque."""

    def test_explicit_zero_max_size_should_store_nothing(self):
        log = AuditLog(max_size=0)
        log.record(service="s3", operation="PutObject")

        entries = log.recent()
        assert len(entries) == 0

    def test_explicit_zero_max_size_deque_maxlen(self):
        log = AuditLog(max_size=0)
        assert log._entries.maxlen == 0
