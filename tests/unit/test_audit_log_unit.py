"""Unit tests for the audit log ring buffer."""

import time

from robotocore.audit.log import AuditLog


class TestAuditLog:
    def test_record_and_recent(self):
        log = AuditLog(max_size=100)
        log.record(service="s3", operation="PutObject", status_code=200)
        entries = log.recent()
        assert len(entries) == 1
        assert entries[0]["service"] == "s3"
        assert entries[0]["operation"] == "PutObject"
        assert entries[0]["status_code"] == 200

    def test_recent_newest_first(self):
        log = AuditLog(max_size=100)
        log.record(service="s3", operation="First")
        log.record(service="s3", operation="Second")
        log.record(service="s3", operation="Third")
        entries = log.recent()
        assert entries[0]["operation"] == "Third"
        assert entries[1]["operation"] == "Second"
        assert entries[2]["operation"] == "First"

    def test_recent_limit(self):
        log = AuditLog(max_size=100)
        for i in range(10):
            log.record(service="s3", operation=f"Op{i}")
        entries = log.recent(limit=3)
        assert len(entries) == 3
        assert entries[0]["operation"] == "Op9"

    def test_ring_buffer_eviction(self):
        log = AuditLog(max_size=3)
        for i in range(5):
            log.record(service="s3", operation=f"Op{i}")
        entries = log.recent()
        assert len(entries) == 3
        # Oldest (Op0, Op1) should be evicted
        assert entries[0]["operation"] == "Op4"
        assert entries[1]["operation"] == "Op3"
        assert entries[2]["operation"] == "Op2"

    def test_clear(self):
        log = AuditLog(max_size=100)
        log.record(service="s3", operation="PutObject")
        log.record(service="sqs", operation="SendMessage")
        count = log.clear()
        assert count == 2
        assert log.recent() == []

    def test_clear_empty(self):
        log = AuditLog(max_size=100)
        assert log.clear() == 0

    def test_record_with_error(self):
        log = AuditLog(max_size=100)
        log.record(service="s3", operation="PutObject", error="AccessDenied")
        entry = log.recent()[0]
        assert entry["error"] == "AccessDenied"

    def test_record_without_error_no_key(self):
        log = AuditLog(max_size=100)
        log.record(service="s3", operation="PutObject")
        entry = log.recent()[0]
        assert "error" not in entry

    def test_record_all_fields(self):
        log = AuditLog(max_size=100)
        log.record(
            service="dynamodb",
            operation="PutItem",
            method="POST",
            path="/",
            status_code=200,
            duration_ms=12.345,
            account_id="123456789012",
            region="us-west-2",
        )
        entry = log.recent()[0]
        assert entry["service"] == "dynamodb"
        assert entry["method"] == "POST"
        assert entry["path"] == "/"
        assert entry["duration_ms"] == 12.35  # rounded to 2 decimal places
        assert entry["account_id"] == "123456789012"
        assert entry["region"] == "us-west-2"
        assert "timestamp" in entry

    def test_timestamp_is_recent(self):
        log = AuditLog(max_size=100)
        before = time.time()
        log.record(service="s3", operation="Test")
        after = time.time()
        ts = log.recent()[0]["timestamp"]
        assert before <= ts <= after

    def test_defaults(self):
        log = AuditLog(max_size=100)
        log.record(service="s3")
        entry = log.recent()[0]
        assert entry["operation"] is None
        assert entry["method"] == "POST"
        assert entry["path"] == "/"
        assert entry["status_code"] == 200
        assert entry["duration_ms"] == 0.0
        assert entry["account_id"] == ""
        assert entry["region"] == ""
