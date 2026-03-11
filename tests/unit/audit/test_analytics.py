"""Unit tests for the usage analytics engine."""

import threading
import time

import pytest

from robotocore.audit.analytics import UsageAnalytics

RR = dict  # shorthand for record_request kwargs


def _rr(analytics, **kwargs):
    """Shorthand for record_request with defaults."""
    defaults = {"service": "s3", "operation": "Get", "status_code": 200, "duration_ms": 1.0}
    defaults.update(kwargs)
    analytics.record_request(**defaults)


@pytest.fixture
def analytics():
    """Create a fresh analytics instance for each test."""
    return UsageAnalytics(window_minutes=60)


class TestRecordRequest:
    def test_increments_service_counter(self, analytics):
        _rr(analytics, operation="PutObject", duration_ms=5.0)
        _rr(analytics, operation="GetObject", duration_ms=3.0)
        stats = analytics.get_service_stats("s3")
        assert stats["total_requests"] == 2

    def test_increments_operation_counter(self, analytics):
        _rr(analytics, operation="PutObject", duration_ms=5.0)
        _rr(analytics, operation="PutObject", duration_ms=3.0)
        _rr(analytics, operation="GetObject", duration_ms=1.0)
        stats = analytics.get_service_stats("s3")
        assert stats["operations"]["PutObject"]["total_requests"] == 2
        assert stats["operations"]["GetObject"]["total_requests"] == 1

    def test_tracks_latency(self, analytics):
        _rr(analytics, service="sqs", operation="SendMessage", duration_ms=10.0)
        _rr(analytics, service="sqs", operation="SendMessage", duration_ms=20.0)
        stats = analytics.get_service_stats("sqs")
        assert stats["avg_latency_ms"] == 15.0
        op_stats = stats["operations"]["SendMessage"]
        assert op_stats["avg_latency_ms"] == 15.0

    def test_tracks_errors_vs_successes(self, analytics):
        _rr(analytics, operation="GetObject", status_code=200)
        _rr(analytics, operation="GetObject", status_code=404)
        _rr(analytics, operation="GetObject", status_code=500)
        stats = analytics.get_service_stats("s3")
        assert stats["success_count"] == 1
        assert stats["error_count"] == 2

    def test_per_minute_time_series_bucketing(self, analytics):
        # Use a timestamp aligned to the start of a minute to avoid boundary issues
        now = (int(time.time()) // 60) * 60 + 10  # 10 seconds into a minute
        _rr(analytics, operation="GetObject", timestamp=now)
        _rr(analytics, operation="PutObject", timestamp=now + 1)
        timeline = analytics.get_timeline()
        assert any(entry["count"] >= 2 for entry in timeline)

    def test_rolling_window_drops_old_entries(self):
        analytics = UsageAnalytics(window_minutes=1)
        old_time = time.time() - 120  # 2 minutes ago
        _rr(analytics, operation="Get", timestamp=old_time)
        _rr(analytics, operation="Put")
        timeline = analytics.get_timeline()
        total = sum(e["count"] for e in timeline)
        assert total == 1

    def test_error_breakdown_by_status_code(self, analytics):
        _rr(analytics, status_code=400)
        _rr(analytics, status_code=400)
        _rr(analytics, status_code=500)
        errors = analytics.get_error_summary()
        assert errors["by_status_code"]["400"] == 2
        assert errors["by_status_code"]["500"] == 1

    def test_error_breakdown_by_error_type(self, analytics):
        _rr(analytics, status_code=404, error_type="NoSuchKey")
        _rr(analytics, status_code=403, error_type="AccessDenied")
        _rr(analytics, status_code=404, error_type="NoSuchKey")
        errors = analytics.get_error_summary()
        assert errors["by_error_type"]["NoSuchKey"] == 2
        assert errors["by_error_type"]["AccessDenied"] == 1

    def test_client_tracking_by_access_key(self, analytics):
        key1 = "AKIAIOSFODNN7EXAMPLE"
        key2 = "AKIAI44QH8DHBEXAMPLE"
        _rr(analytics, access_key_id=key1)
        _rr(analytics, operation="Put", access_key_id=key1)
        _rr(analytics, service="sqs", access_key_id=key2)
        summary = analytics.get_usage_summary()
        assert summary["unique_clients"] == 2
        assert summary["top_clients"][0]["access_key_id"] == key1
        assert summary["top_clients"][0]["request_count"] == 2

    def test_thread_safe_concurrent_recording(self, analytics):
        errors = []

        def record_batch(svc: str):
            try:
                for _ in range(100):
                    _rr(analytics, service=svc, operation="Op")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=record_batch, args=(f"svc{i}",)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        summary = analytics.get_usage_summary()
        assert summary["total_requests"] == 1000

    def test_disabled_via_env_var(self, monkeypatch):
        monkeypatch.setenv("USAGE_ANALYTICS", "0")
        a = UsageAnalytics()
        _rr(a)
        summary = a.get_usage_summary()
        assert summary["total_requests"] == 0

    def test_usage_summary_computation(self, analytics):
        for i in range(5):
            _rr(analytics, duration_ms=float(i))
        _rr(analytics, service="sqs", operation="Send", status_code=500, duration_ms=10.0)
        summary = analytics.get_usage_summary()
        assert summary["total_requests"] == 6
        assert summary["total_errors"] == 1
        assert summary["services_used"] == 2
        assert "avg_latency_ms" in summary

    def test_per_service_stats_computation(self, analytics):
        _rr(analytics, operation="Get", duration_ms=5.0)
        _rr(analytics, operation="Put", duration_ms=15.0)
        _rr(analytics, service="sqs", operation="Send", duration_ms=10.0)
        all_services = analytics.get_all_service_stats()
        assert "s3" in all_services
        assert "sqs" in all_services
        assert all_services["s3"]["total_requests"] == 2
        assert all_services["s3"]["avg_latency_ms"] == 10.0

    def test_timeline_generation(self, analytics):
        now = time.time()
        _rr(analytics, timestamp=now)
        _rr(analytics, timestamp=now - 60)
        timeline = analytics.get_timeline()
        assert isinstance(timeline, list)
        assert len(timeline) >= 2
        for entry in timeline:
            assert "minute" in entry
            assert "count" in entry
