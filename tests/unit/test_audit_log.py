"""Unit tests for audit log module."""

from robotocore.audit.log import AuditLog


class TestAuditLog:
    def test_record_and_recent(self):
        log = AuditLog(max_size=100)
        log.record(service="s3", operation="PutObject", status_code=200)
        log.record(service="dynamodb", operation="GetItem", status_code=200)

        entries = log.recent()
        assert len(entries) == 2
        # Newest first
        assert entries[0]["service"] == "dynamodb"
        assert entries[1]["service"] == "s3"

    def test_recent_limit(self):
        log = AuditLog(max_size=100)
        for i in range(10):
            log.record(service=f"svc-{i}", status_code=200)

        entries = log.recent(limit=3)
        assert len(entries) == 3

    def test_ring_buffer_overflow(self):
        log = AuditLog(max_size=5)
        for i in range(10):
            log.record(service=f"svc-{i}", status_code=200)

        entries = log.recent()
        assert len(entries) == 5
        # Should have the last 5 entries
        assert entries[0]["service"] == "svc-9"
        assert entries[4]["service"] == "svc-5"

    def test_clear(self):
        log = AuditLog(max_size=100)
        log.record(service="s3", status_code=200)
        log.record(service="sqs", status_code=200)
        count = log.clear()
        assert count == 2
        assert len(log.recent()) == 0

    def test_record_with_error(self):
        log = AuditLog(max_size=100)
        log.record(service="s3", operation="PutObject", status_code=500, error="InternalError")
        entries = log.recent()
        assert entries[0]["error"] == "InternalError"
        assert entries[0]["status_code"] == 500

    def test_record_fields(self):
        log = AuditLog(max_size=100)
        log.record(
            service="lambda",
            operation="Invoke",
            method="POST",
            path="/2015-03-31/functions/my-fn/invocations",
            status_code=200,
            duration_ms=42.5,
            account_id="123456789012",
            region="us-east-1",
        )
        entry = log.recent()[0]
        assert entry["service"] == "lambda"
        assert entry["operation"] == "Invoke"
        assert entry["method"] == "POST"
        assert entry["duration_ms"] == 42.5
        assert entry["account_id"] == "123456789012"
        assert "timestamp" in entry
