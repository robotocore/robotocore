"""
Tests for CloudWatch metrics, alarms, and CloudWatch Logs audit trail.
"""

import json
import uuid

import pytest

from .models import AlertConfig, PipelineMetrics

pytestmark = pytest.mark.apps


class TestCloudWatchMetrics:
    """CloudWatch custom metrics for pipeline monitoring."""

    def test_publish_and_query_metrics(self, pipeline):
        """Publish pipeline metrics and query the sum back."""
        metrics = PipelineMetrics(records_in=150, records_out=145, errors=5, bytes_ingested=50000)
        pipeline.publish_metrics(metrics)

        total = pipeline.get_metric_sum("RecordsProcessed")
        assert total == 145.0

    def test_publish_multiple_metric_points(self, pipeline):
        """Publish metrics twice and verify sums accumulate."""
        m1 = PipelineMetrics(records_in=100, records_out=100, errors=0, bytes_ingested=30000)
        m2 = PipelineMetrics(records_in=200, records_out=195, errors=5, bytes_ingested=60000)
        pipeline.publish_metrics(m1)
        pipeline.publish_metrics(m2)

        total_processed = pipeline.get_metric_sum("RecordsProcessed")
        assert total_processed == 295.0

        total_errors = pipeline.get_metric_sum("ErrorCount")
        assert total_errors == 5.0

    def test_publish_latency_metric(self, pipeline):
        """Publish metrics with avg_latency_ms and verify it appears."""
        metrics = PipelineMetrics(
            records_in=50, records_out=50, errors=0, bytes_ingested=15000, avg_latency_ms=42.5
        )
        pipeline.publish_metrics(metrics)

        total = pipeline.get_metric_sum("AvgLatencyMs")
        assert total == 42.5


class TestCloudWatchAlarms:
    """CloudWatch alarms for pipeline health."""

    def test_create_and_describe_alarm(self, pipeline):
        """Create an alarm and verify its configuration."""
        alarm_name = f"test-alarm-{uuid.uuid4().hex[:8]}"
        alert = AlertConfig(
            metric_name="ErrorCount",
            threshold=100,
            comparison="GreaterThanThreshold",
            period_seconds=300,
            alarm_name=alarm_name,
        )
        pipeline.create_alarm(alert)

        alarm = pipeline.describe_alarm(alarm_name)
        assert alarm is not None
        assert alarm["Threshold"] == 100.0
        assert alarm["ComparisonOperator"] == "GreaterThanThreshold"

        pipeline.delete_alarm(alarm_name)

    def test_set_alarm_state(self, pipeline):
        """Force alarm to ALARM state and verify."""
        alarm_name = f"test-alarm-state-{uuid.uuid4().hex[:8]}"
        alert = AlertConfig(
            metric_name="ErrorCount",
            threshold=50,
            comparison="GreaterThanThreshold",
            alarm_name=alarm_name,
        )
        pipeline.create_alarm(alert)
        pipeline.set_alarm_state(alarm_name, "ALARM", "Testing: error count exceeded")

        alarm = pipeline.describe_alarm(alarm_name)
        assert alarm["StateValue"] == "ALARM"

        pipeline.delete_alarm(alarm_name)

    def test_describe_nonexistent_alarm(self, pipeline):
        """Describing a nonexistent alarm returns None."""
        result = pipeline.describe_alarm("no-such-alarm-ever")
        assert result is None


class TestAuditLog:
    """CloudWatch Logs audit trail."""

    def test_log_single_event(self, pipeline):
        """Log a single event and read it back."""
        pipeline.log_event("Pipeline started")
        events = pipeline.get_log_events()
        assert len(events) >= 1
        assert any("Pipeline started" in e["message"] for e in events)

    def test_log_multiple_events(self, pipeline):
        """Log multiple events and read them all back."""
        messages = [f"Batch {i} processed" for i in range(5)]
        pipeline.log_events(messages)

        events = pipeline.get_log_events()
        assert len(events) >= 5
        event_messages = [e["message"] for e in events]
        assert "Batch 0 processed" in event_messages
        assert "Batch 4 processed" in event_messages

    def test_filter_logs_by_pattern(self, pipeline):
        """Filter audit logs by error pattern."""
        pipeline.log_events(
            [
                "INFO: Batch 1 processed successfully",
                "ERROR: Sensor s-42 timeout",
                "INFO: Batch 2 processed successfully",
                "ERROR: DynamoDB write throttled",
                "WARN: High latency detected",
            ]
        )

        errors = pipeline.filter_logs("ERROR")
        assert len(errors) == 2
        error_messages = [e["message"] for e in errors]
        assert any("timeout" in m for m in error_messages)
        assert any("throttled" in m for m in error_messages)

    def test_log_structured_event(self, pipeline):
        """Log a JSON-structured event and parse it back."""
        event_data = {
            "event": "batch_processed",
            "batch_key": "raw/2026/03/08/batch-001.jsonl",
            "record_count": 42,
        }
        pipeline.log_event(json.dumps(event_data))

        events = pipeline.get_log_events()
        assert len(events) >= 1
        parsed = json.loads(events[-1]["message"])
        assert parsed["event"] == "batch_processed"
        assert parsed["record_count"] == 42

    def test_pipeline_statistics_after_processing(self, pipeline):
        """Pipeline statistics reflect actual processing work."""
        # Simulate some processing
        for i in range(3):
            raw = {
                "sensor_id": f"s-{i}",
                "timestamp": f"2026-03-08T10:0{i}:00Z",
                "sensor_type": "temperature",
                "value": 70.0 + i,
            }
            result = pipeline.process_reading(raw)
            if result:
                pipeline.index_record(result)

        stats = pipeline.get_statistics()
        assert stats.records_out == 3

    def test_reset_statistics(self, pipeline):
        """Reset statistics clears all counters."""
        pipeline._metrics.records_in = 100
        pipeline._metrics.errors = 5
        pipeline.reset_statistics()
        stats = pipeline.get_statistics()
        assert stats.records_in == 0
        assert stats.errors == 0
