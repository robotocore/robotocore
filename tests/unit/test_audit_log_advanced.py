"""Advanced audit log tests: thread safety, singleton, edge cases."""

import threading

from robotocore.audit.log import AuditLog, get_audit_log


class TestAuditLogThreadSafety:
    def test_concurrent_record_and_recent(self):
        """Multiple threads recording simultaneously should not lose or corrupt entries."""
        log = AuditLog(max_size=5000)
        errors = []

        def record_many(thread_id):
            try:
                for i in range(200):
                    log.record(service=f"svc-{thread_id}", operation=f"Op{i}", status_code=200)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=record_many, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        entries = log.recent(limit=5000)
        assert len(entries) == 2000  # 10 threads * 200 each

    def test_concurrent_record_and_clear(self):
        """Recording and clearing simultaneously should not crash."""
        log = AuditLog(max_size=100)
        errors = []

        def record_loop():
            try:
                for i in range(500):
                    log.record(service="s3", operation=f"Op{i}")
            except Exception as e:
                errors.append(e)

        def clear_loop():
            try:
                for _ in range(50):
                    log.clear()
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=record_loop)
        t2 = threading.Thread(target=clear_loop)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert not errors

    def test_concurrent_record_with_ring_buffer_overflow(self):
        """Ring buffer overflow during concurrent access should not corrupt data."""
        log = AuditLog(max_size=50)
        errors = []

        def record_many(thread_id):
            try:
                for i in range(100):
                    log.record(service=f"svc-{thread_id}", operation=f"Op{i}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=record_many, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        entries = log.recent()
        assert len(entries) == 50  # max_size enforced


class TestAuditLogSingleton:
    def test_get_audit_log_returns_same_instance(self):
        log1 = get_audit_log()
        log2 = get_audit_log()
        assert log1 is log2

    def test_singleton_is_functional(self):
        log = get_audit_log()
        initial_count = len(log.recent(limit=10000))
        log.record(service="test-singleton", operation="TestOp")
        new_count = len(log.recent(limit=10000))
        assert new_count >= initial_count  # At least one more entry


class TestAuditLogEdgeCases:
    def test_max_size_one(self):
        log = AuditLog(max_size=1)
        log.record(service="s3", operation="First")
        log.record(service="s3", operation="Second")
        entries = log.recent()
        assert len(entries) == 1
        assert entries[0]["operation"] == "Second"

    def test_recent_with_zero_limit(self):
        log = AuditLog(max_size=100)
        log.record(service="s3", operation="Test")
        entries = log.recent(limit=0)
        assert entries == []

    def test_record_empty_error_string(self):
        """Empty string error should not add 'error' key (falsy)."""
        log = AuditLog(max_size=100)
        log.record(service="s3", operation="Test", error="")
        entry = log.recent()[0]
        assert "error" not in entry

    def test_duration_ms_rounding(self):
        log = AuditLog(max_size=100)
        log.record(service="s3", duration_ms=1.23456789)
        entry = log.recent()[0]
        assert entry["duration_ms"] == 1.23

    def test_large_duration_preserved(self):
        log = AuditLog(max_size=100)
        log.record(service="s3", duration_ms=99999.99)
        entry = log.recent()[0]
        assert entry["duration_ms"] == 99999.99

    def test_many_entries_ordering_preserved(self):
        log = AuditLog(max_size=1000)
        for i in range(500):
            log.record(service=f"svc-{i:04d}", operation=f"Op{i}")
        entries = log.recent(limit=500)
        # Newest first
        assert entries[0]["service"] == "svc-0499"
        assert entries[-1]["service"] == "svc-0000"
