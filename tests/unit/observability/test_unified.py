"""Tests for the unified observability hub."""

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from robotocore.observability.unified import (
    EventType,
    ObservabilityHub,
    TimelineEntry,
    get_observability_hub,
    reset_hub,
)


@pytest.fixture
def hub():
    """Create a fresh ObservabilityHub for each test."""
    return ObservabilityHub(max_events=1000)


@pytest.fixture(autouse=True)
def _reset_singleton():
    """Reset the singleton hub between tests."""
    reset_hub()
    yield
    reset_hub()


# ---------------------------------------------------------------------------
# Basic event recording
# ---------------------------------------------------------------------------


class TestEventRecording:
    def test_record_event_stores_in_timeline(self, hub):
        entry = TimelineEntry(
            timestamp=time.time(),
            event_type=EventType.AUDIT,
            request_id="req-1",
            service="s3",
            operation="PutObject",
            summary="test",
        )
        hub.record_event(entry)
        timeline = hub.get_timeline()
        assert len(timeline) == 1
        assert timeline[0]["request_id"] == "req-1"

    def test_record_multiple_events(self, hub):
        for i in range(5):
            hub.record_event(
                TimelineEntry(
                    timestamp=time.time(),
                    event_type=EventType.AUDIT,
                    request_id=f"req-{i}",
                    service="sqs",
                )
            )
        assert len(hub.get_timeline(limit=100)) == 5

    def test_timeline_newest_first(self, hub):
        hub.record_event(
            TimelineEntry(timestamp=1.0, event_type=EventType.AUDIT, request_id="old", service="s3")
        )
        hub.record_event(
            TimelineEntry(timestamp=2.0, event_type=EventType.AUDIT, request_id="new", service="s3")
        )
        timeline = hub.get_timeline()
        assert timeline[0]["request_id"] == "new"
        assert timeline[1]["request_id"] == "old"

    def test_max_events_cap(self):
        hub = ObservabilityHub(max_events=5)
        for i in range(10):
            hub.record_event(
                TimelineEntry(
                    timestamp=float(i),
                    event_type=EventType.AUDIT,
                    request_id=f"req-{i}",
                    service="s3",
                )
            )
        timeline = hub.get_timeline(limit=100)
        assert len(timeline) == 5
        # Should have the 5 newest
        assert timeline[0]["request_id"] == "req-9"


# ---------------------------------------------------------------------------
# Chaos event recording
# ---------------------------------------------------------------------------


class TestChaosEvents:
    def test_record_chaos_event(self, hub):
        hub.record_chaos_event(
            request_id="req-c1",
            service="dynamodb",
            operation="PutItem",
            rule={"rule_id": "r1", "error_code": "ThrottlingException"},
            action_taken="error_injected",
        )
        timeline = hub.get_timeline()
        assert len(timeline) == 1
        assert timeline[0]["event_type"] == EventType.CHAOS
        assert timeline[0]["service"] == "dynamodb"
        assert "r1" in timeline[0]["summary"]

    def test_chaos_event_updates_trace(self, hub):
        hub.record_chaos_event(
            request_id="req-c2",
            service="s3",
            operation="GetObject",
            rule={"rule_id": "r2"},
            action_taken="latency_added",
        )
        trace = hub.get_request_trace("req-c2")
        assert trace is not None
        assert trace.chaos_action_taken == "latency_added"
        assert trace.chaos_rule_matched == {"rule_id": "r2"}

    def test_chaos_event_details(self, hub):
        hub.record_chaos_event(
            request_id="req-c3",
            service="sqs",
            operation="SendMessage",
            rule={"rule_id": "r3", "latency_ms": 500},
            action_taken="error_injected+latency_added",
        )
        timeline = hub.get_timeline()
        assert timeline[0]["details"]["action_taken"] == "error_injected+latency_added"
        assert timeline[0]["details"]["rule"]["rule_id"] == "r3"


# ---------------------------------------------------------------------------
# IAM event recording
# ---------------------------------------------------------------------------


class TestIamEvents:
    def test_record_iam_allow(self, hub):
        hub.record_iam_event(
            request_id="req-i1",
            service="s3",
            operation="PutObject",
            decision="Allow",
            principal="user-1",
        )
        timeline = hub.get_timeline()
        assert len(timeline) == 1
        assert timeline[0]["event_type"] == EventType.IAM
        assert "Allow" in timeline[0]["summary"]

    def test_record_iam_deny(self, hub):
        hub.record_iam_event(
            request_id="req-i2",
            service="dynamodb",
            operation="DeleteTable",
            decision="Deny",
            principal="user-2",
            matched_policy="arn:aws:iam::123:policy/deny-all",
        )
        trace = hub.get_request_trace("req-i2")
        assert trace is not None
        assert trace.iam_decision == "Deny"
        assert trace.iam_matched_policy == "arn:aws:iam::123:policy/deny-all"
        assert trace.iam_principal == "user-2"

    def test_iam_event_with_action_resource(self, hub):
        hub.record_iam_event(
            request_id="req-i3",
            service="s3",
            operation="GetObject",
            decision="Allow",
            principal="admin",
            action="s3:GetObject",
            resource="arn:aws:s3:::my-bucket/key",
        )
        timeline = hub.get_timeline()
        assert timeline[0]["details"]["action"] == "s3:GetObject"
        assert timeline[0]["details"]["resource"] == "arn:aws:s3:::my-bucket/key"


# ---------------------------------------------------------------------------
# Audit event recording
# ---------------------------------------------------------------------------


class TestAuditEvents:
    def test_record_audit_event(self, hub):
        hub.record_audit_event(
            request_id="req-a1",
            service="lambda",
            operation="Invoke",
            status_code=200,
            duration_ms=42.5,
        )
        trace = hub.get_request_trace("req-a1")
        assert trace is not None
        assert trace.response_status == 200
        assert trace.duration_ms == 42.5

    def test_audit_event_with_error(self, hub):
        hub.record_audit_event(
            request_id="req-a2",
            service="sqs",
            operation="SendMessage",
            status_code=500,
            duration_ms=10.0,
            error="InternalError",
        )
        timeline = hub.get_timeline()
        assert timeline[0]["event_type"] == EventType.ERROR
        assert timeline[0]["details"]["error"] == "InternalError"

    def test_audit_without_error_is_audit_type(self, hub):
        hub.record_audit_event(
            request_id="req-a3",
            service="s3",
            operation="ListBuckets",
            status_code=200,
            duration_ms=5.0,
        )
        timeline = hub.get_timeline()
        assert timeline[0]["event_type"] == EventType.AUDIT


# ---------------------------------------------------------------------------
# Request trace correlation
# ---------------------------------------------------------------------------


class TestRequestTraceCorrelation:
    def test_full_trace_correlation(self, hub):
        """A request that goes through chaos + IAM + audit should produce one trace."""
        req_id = "req-full"
        hub.record_chaos_event(
            request_id=req_id,
            service="s3",
            operation="PutObject",
            rule={"rule_id": "r10"},
            action_taken="latency_added",
        )
        hub.record_iam_event(
            request_id=req_id,
            service="s3",
            operation="PutObject",
            decision="Allow",
            principal="admin",
        )
        hub.record_audit_event(
            request_id=req_id,
            service="s3",
            operation="PutObject",
            status_code=200,
            duration_ms=150.0,
        )

        trace = hub.get_request_trace(req_id)
        assert trace is not None
        assert trace.chaos_action_taken == "latency_added"
        assert trace.iam_decision == "Allow"
        assert trace.response_status == 200
        assert trace.duration_ms == 150.0

    def test_trace_not_found_returns_none(self, hub):
        assert hub.get_request_trace("nonexistent") is None

    def test_multiple_traces_independent(self, hub):
        hub.record_audit_event(
            request_id="r1", service="s3", operation="Get", status_code=200, duration_ms=10
        )
        hub.record_audit_event(
            request_id="r2", service="sqs", operation="Send", status_code=500, duration_ms=20
        )
        t1 = hub.get_request_trace("r1")
        t2 = hub.get_request_trace("r2")
        assert t1.service == "s3"
        assert t2.service == "sqs"
        assert t1.response_status == 200
        assert t2.response_status == 500


# ---------------------------------------------------------------------------
# Timeline filtering
# ---------------------------------------------------------------------------


class TestTimelineFiltering:
    def test_filter_by_service(self, hub):
        hub.record_audit_event(
            request_id="r1", service="s3", operation="Get", status_code=200, duration_ms=10
        )
        hub.record_audit_event(
            request_id="r2", service="sqs", operation="Send", status_code=200, duration_ms=10
        )
        timeline = hub.get_timeline(service="s3")
        assert len(timeline) == 1
        assert timeline[0]["service"] == "s3"

    def test_filter_by_event_type(self, hub):
        hub.record_chaos_event(
            request_id="r1",
            service="s3",
            operation="Get",
            rule={"rule_id": "x"},
            action_taken="error_injected",
        )
        hub.record_iam_event(request_id="r2", service="s3", operation="Put", decision="Allow")
        timeline = hub.get_timeline(event_types=["chaos"])
        assert len(timeline) == 1
        assert timeline[0]["event_type"] == EventType.CHAOS

    def test_filter_by_multiple_event_types(self, hub):
        hub.record_chaos_event(
            request_id="r1",
            service="s3",
            operation="Get",
            rule={"rule_id": "x"},
            action_taken="error_injected",
        )
        hub.record_iam_event(request_id="r2", service="s3", operation="Put", decision="Deny")
        hub.record_audit_event(
            request_id="r3", service="s3", operation="List", status_code=200, duration_ms=5
        )
        timeline = hub.get_timeline(event_types=["chaos", "iam"])
        assert len(timeline) == 2

    def test_filter_by_request_id(self, hub):
        hub.record_audit_event(
            request_id="target", service="s3", operation="Get", status_code=200, duration_ms=10
        )
        hub.record_audit_event(
            request_id="other", service="sqs", operation="Send", status_code=200, duration_ms=10
        )
        timeline = hub.get_timeline(request_id="target")
        assert len(timeline) == 1
        assert timeline[0]["request_id"] == "target"

    def test_filter_by_min_duration(self, hub):
        hub.record_audit_event(
            request_id="fast", service="s3", operation="Get", status_code=200, duration_ms=10
        )
        hub.record_audit_event(
            request_id="slow", service="s3", operation="Put", status_code=200, duration_ms=2000
        )
        timeline = hub.get_timeline(min_duration=1000)
        assert len(timeline) == 1
        assert timeline[0]["request_id"] == "slow"

    def test_limit(self, hub):
        for i in range(20):
            hub.record_audit_event(
                request_id=f"r{i}", service="s3", operation="Get", status_code=200, duration_ms=1
            )
        timeline = hub.get_timeline(limit=5)
        assert len(timeline) == 5

    def test_combined_filters(self, hub):
        hub.record_audit_event(
            request_id="r1", service="s3", operation="Get", status_code=200, duration_ms=5000
        )
        hub.record_audit_event(
            request_id="r2", service="sqs", operation="Send", status_code=200, duration_ms=5000
        )
        hub.record_audit_event(
            request_id="r3", service="s3", operation="Put", status_code=200, duration_ms=10
        )
        timeline = hub.get_timeline(service="s3", min_duration=1000)
        assert len(timeline) == 1
        assert timeline[0]["request_id"] == "r1"


# ---------------------------------------------------------------------------
# Diagnostics summary
# ---------------------------------------------------------------------------


class TestDiagnosticsSummary:
    def test_empty_diagnostics(self, hub):
        diag = hub.get_diagnostics_summary()
        assert diag["chaos"]["total_faults_injected"] == 0
        assert diag["iam"]["total_denials"] == 0
        assert diag["latency"] == {}
        assert diag["error_rates"] == {}

    def test_chaos_stats(self, hub):
        hub.record_chaos_event(
            request_id="r1",
            service="s3",
            operation="Get",
            rule={"rule_id": "rule1"},
            action_taken="error_injected",
        )
        diag = hub.get_diagnostics_summary()
        assert diag["chaos"]["total_faults_injected"] == 1
        assert len(diag["chaos"]["recent_faults"]) == 1
        assert diag["chaos"]["recent_faults"][0]["rule_id"] == "rule1"

    def test_iam_denial_stats(self, hub):
        hub.record_iam_event(
            request_id="r1",
            service="dynamodb",
            operation="DeleteTable",
            decision="Deny",
            principal="baduser",
            matched_policy="arn:aws:iam::123:policy/x",
        )
        diag = hub.get_diagnostics_summary()
        assert diag["iam"]["total_denials"] == 1
        assert diag["iam"]["recent_denials"][0]["principal"] == "baduser"

    def test_latency_percentiles(self, hub):
        for i in range(100):
            hub.record_audit_event(
                request_id=f"r{i}",
                service="s3",
                operation="Get",
                status_code=200,
                duration_ms=float(i + 1),
            )
        diag = hub.get_diagnostics_summary()
        assert "p50" in diag["latency"]
        assert "p95" in diag["latency"]
        assert "p99" in diag["latency"]
        assert "mean" in diag["latency"]
        assert diag["latency"]["p50"] > 0
        assert diag["latency"]["p95"] >= diag["latency"]["p50"]

    def test_error_rates_by_service(self, hub):
        for i in range(10):
            hub.record_audit_event(
                request_id=f"ok-{i}",
                service="s3",
                operation="Get",
                status_code=200,
                duration_ms=10,
            )
        for i in range(5):
            hub.record_audit_event(
                request_id=f"err-{i}",
                service="s3",
                operation="Get",
                status_code=500,
                duration_ms=10,
            )
        diag = hub.get_diagnostics_summary()
        s3_rate = diag["error_rates"]["s3"]
        assert s3_rate["total_requests"] == 15
        assert s3_rate["error_count"] == 5
        assert abs(s3_rate["error_rate"] - 5 / 15) < 0.01


# ---------------------------------------------------------------------------
# Request ID generation and threading
# ---------------------------------------------------------------------------


class TestRequestIdGeneration:
    def test_generate_unique_ids(self):
        ids = {ObservabilityHub.generate_request_id() for _ in range(100)}
        assert len(ids) == 100

    def test_request_id_length(self):
        rid = ObservabilityHub.generate_request_id()
        assert len(rid) == 16


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------


class TestThreadSafety:
    def test_concurrent_recording(self, hub):
        """Record events from multiple threads and verify no data loss."""
        errors = []

        def record_events(thread_id):
            try:
                for i in range(50):
                    hub.record_audit_event(
                        request_id=f"t{thread_id}-r{i}",
                        service="s3",
                        operation="Get",
                        status_code=200,
                        duration_ms=1.0,
                    )
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=record_events, args=(t,)) for t in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        timeline = hub.get_timeline(limit=10000)
        assert len(timeline) == 200  # 4 threads * 50 events

    def test_concurrent_read_write(self, hub):
        """Read timeline while writing should not crash."""
        errors = []
        stop = threading.Event()

        def writer():
            i = 0
            while not stop.is_set():
                hub.record_audit_event(
                    request_id=f"w-{i}",
                    service="s3",
                    operation="Get",
                    status_code=200,
                    duration_ms=1.0,
                )
                i += 1

        def reader():
            try:
                while not stop.is_set():
                    hub.get_timeline(limit=50)
                    hub.get_request_trace("w-0")
            except Exception as e:
                errors.append(e)

        w = threading.Thread(target=writer)
        r = threading.Thread(target=reader)
        w.start()
        r.start()
        time.sleep(0.1)
        stop.set()
        w.join()
        r.join()
        assert not errors


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_timeline(self, hub):
        timeline = hub.get_timeline()
        assert timeline == []

    def test_empty_diagnostics_summary(self, hub):
        diag = hub.get_diagnostics_summary()
        assert isinstance(diag, dict)
        assert diag["chaos"]["total_faults_injected"] == 0

    def test_trace_eviction_on_capacity(self):
        hub = ObservabilityHub(max_events=5)
        for i in range(10):
            hub.record_audit_event(
                request_id=f"r{i}",
                service="s3",
                operation="Get",
                status_code=200,
                duration_ms=1.0,
            )
        # Oldest traces should be evicted
        assert hub.get_request_trace("r0") is None
        assert hub.get_request_trace("r9") is not None

    def test_clear_hub(self, hub):
        hub.record_audit_event(
            request_id="r1", service="s3", operation="Get", status_code=200, duration_ms=1
        )
        hub.clear()
        assert hub.get_timeline() == []
        assert hub.get_request_trace("r1") is None

    def test_none_operation(self, hub):
        hub.record_audit_event(
            request_id="r1", service="s3", operation=None, status_code=200, duration_ms=1
        )
        trace = hub.get_request_trace("r1")
        assert trace.operation is None


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------


class TestSingleton:
    def test_get_hub_returns_same_instance(self):
        h1 = get_observability_hub()
        h2 = get_observability_hub()
        assert h1 is h2

    def test_reset_hub_creates_new_instance(self):
        h1 = get_observability_hub()
        reset_hub()
        h2 = get_observability_hub()
        assert h1 is not h2


# ---------------------------------------------------------------------------
# Chaos -> Audit integration
# ---------------------------------------------------------------------------


class TestChaosAuditIntegration:
    def test_chaos_handler_records_to_hub_and_audit(self):
        """When chaos injects a fault, it should appear in both hub and audit."""
        from robotocore.chaos.fault_rules import FaultRule, FaultRuleStore
        from robotocore.chaos.middleware import chaos_handler

        rule = FaultRule(
            rule_id="test-rule",
            service="s3",
            error_code="ThrottlingException",
            probability=1.0,
        )
        store = FaultRuleStore()
        store.add(rule)

        mock_request = MagicMock()
        mock_request.method = "POST"
        mock_request.url.path = "/"
        mock_request.headers = {}

        ctx = MagicMock()
        ctx.service_name = "s3"
        ctx.operation = "PutObject"
        ctx.region = "us-east-1"
        ctx.request = mock_request
        ctx.request_id = "chaos-test-req"
        ctx.response = None
        ctx.account_id = "123456789012"

        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            chaos_handler(ctx)

        # Verify response was set (fault injected)
        assert ctx.response is not None

        # Verify hub has the chaos event
        hub = get_observability_hub()
        timeline = hub.get_timeline(event_types=["chaos"])
        assert len(timeline) >= 1
        assert timeline[0]["request_id"] == "chaos-test-req"

    def test_chaos_latency_only_records_event(self):
        """Latency-only chaos (no error) should still record an event."""
        from robotocore.chaos.fault_rules import FaultRule, FaultRuleStore
        from robotocore.chaos.middleware import chaos_handler

        rule = FaultRule(
            rule_id="latency-rule",
            service="sqs",
            latency_ms=100,
            error_code=None,
            probability=1.0,
        )
        store = FaultRuleStore()
        store.add(rule)

        mock_request = MagicMock()
        mock_request.method = "POST"
        mock_request.url.path = "/"
        mock_request.headers = {}

        ctx = MagicMock()
        ctx.service_name = "sqs"
        ctx.operation = "SendMessage"
        ctx.region = "us-east-1"
        ctx.request = mock_request
        ctx.request_id = "latency-req"
        ctx.response = None
        ctx.account_id = "123456789012"

        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            chaos_handler(ctx)

        hub = get_observability_hub()
        trace = hub.get_request_trace("latency-req")
        assert trace is not None
        assert trace.chaos_action_taken == "latency_added"


# ---------------------------------------------------------------------------
# IAM -> Audit integration
# ---------------------------------------------------------------------------


class TestIamAuditIntegration:
    def test_iam_deny_records_to_audit(self):
        """When IAM denies, it should appear in the audit log."""
        from robotocore.audit.log import AuditLog
        from robotocore.gateway.iam_middleware import _record_to_stream

        mock_audit = AuditLog(max_size=100)
        with (
            patch("robotocore.audit.log.get_audit_log", return_value=mock_audit),
            patch("robotocore.services.iam.policy_stream.is_stream_enabled", return_value=False),
        ):
            _record_to_stream(
                principal="bad-user",
                action="s3:DeleteBucket",
                resource="arn:aws:s3:::my-bucket",
                decision="Deny",
                service="s3",
                operation="DeleteBucket",
                request_id="iam-deny-req",
            )

        entries = mock_audit.recent()
        assert len(entries) == 1
        assert "iam:Deny" in entries[0]["error"]
        assert "bad-user" in entries[0]["error"]

    def test_iam_allow_does_not_record_audit_error(self):
        """IAM Allow should not create an audit error entry."""
        from robotocore.audit.log import AuditLog
        from robotocore.gateway.iam_middleware import _record_to_stream

        mock_audit = AuditLog(max_size=100)
        with (
            patch("robotocore.audit.log.get_audit_log", return_value=mock_audit),
            patch("robotocore.services.iam.policy_stream.is_stream_enabled", return_value=False),
        ):
            _record_to_stream(
                principal="admin",
                action="s3:PutObject",
                resource="arn:aws:s3:::bucket",
                decision="Allow",
                service="s3",
                operation="PutObject",
                request_id="iam-allow-req",
            )

        entries = mock_audit.recent()
        assert len(entries) == 0  # No error entry for Allow


# ---------------------------------------------------------------------------
# Request ID threading
# ---------------------------------------------------------------------------


class TestRequestIdThreading:
    def test_request_id_propagated_from_context(self):
        """Request ID set on context should be used by chaos handler."""
        from robotocore.chaos.fault_rules import FaultRule, FaultRuleStore
        from robotocore.chaos.middleware import chaos_handler

        rule = FaultRule(
            rule_id="prop-rule",
            service="lambda",
            error_code="TooManyRequestsException",
            probability=1.0,
        )
        store = FaultRuleStore()
        store.add(rule)

        mock_request = MagicMock()
        mock_request.method = "POST"
        mock_request.url.path = "/"
        mock_request.headers = {}

        ctx = MagicMock()
        ctx.service_name = "lambda"
        ctx.operation = "Invoke"
        ctx.region = "us-east-1"
        ctx.request = mock_request
        ctx.request_id = "my-custom-req-id"
        ctx.response = None
        ctx.account_id = "123456789012"

        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            chaos_handler(ctx)

        hub = get_observability_hub()
        trace = hub.get_request_trace("my-custom-req-id")
        assert trace is not None
        assert trace.service == "lambda"


# ---------------------------------------------------------------------------
# RequestContext request_id field
# ---------------------------------------------------------------------------


class TestRequestContextRequestId:
    def test_request_context_has_request_id_field(self):
        from robotocore.gateway.handler_chain import RequestContext

        mock_request = MagicMock()
        ctx = RequestContext(request=mock_request, service_name="s3")
        assert ctx.request_id == ""

    def test_request_context_accepts_request_id(self):
        from robotocore.gateway.handler_chain import RequestContext

        mock_request = MagicMock()
        ctx = RequestContext(request=mock_request, service_name="s3", request_id="abc123")
        assert ctx.request_id == "abc123"


# ---------------------------------------------------------------------------
# Diagnostics bundle integration
# ---------------------------------------------------------------------------


class TestDiagnosticsBundleIntegration:
    def test_observability_section_in_diagnostics(self):
        """The diagnostics bundle should include an 'observability' section."""
        from robotocore.diagnostics_bundle import ALL_SECTIONS

        assert "observability" in ALL_SECTIONS

    def test_collect_observability_returns_dict(self):
        from robotocore.diagnostics_bundle import _collect_observability

        result = _collect_observability()
        assert isinstance(result, dict)
        assert "chaos" in result
        assert "iam" in result
        assert "latency" in result
        assert "error_rates" in result
