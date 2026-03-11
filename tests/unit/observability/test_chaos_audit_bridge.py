"""Tests for chaos-audit bridge and unified timeline."""

from starlette.testclient import TestClient

from robotocore.audit.log import AuditLog, get_audit_log
from robotocore.observability.chaos_audit_bridge import record_chaos_event
from robotocore.observability.request_context import (
    RequestContext,
    get_current_context,
    set_current_context,
)


class TestRequestContext:
    """Test per-request observability context."""

    def test_create_context_with_defaults(self):
        ctx = RequestContext()
        assert ctx.request_id  # non-empty UUID
        assert ctx.service == ""
        assert ctx.operation == ""
        assert ctx.chaos_applied == []

    def test_create_context_with_service(self):
        ctx = RequestContext(service="sqs", operation="SendMessage")
        assert ctx.service == "sqs"
        assert ctx.operation == "SendMessage"

    def test_elapsed_ms(self):
        ctx = RequestContext()
        # elapsed_ms should be non-negative and small (just created)
        assert ctx.elapsed_ms >= 0
        assert ctx.elapsed_ms < 1000  # less than 1 second

    def test_set_and_get_context(self):
        ctx = RequestContext(service="dynamodb")
        set_current_context(ctx)
        retrieved = get_current_context()
        assert retrieved is ctx
        assert retrieved.service == "dynamodb"


class TestRecordChaosEvent:
    """Test chaos event recording in request context and audit log."""

    def test_records_in_request_context(self):
        ctx = RequestContext(service="s3", operation="GetObject")
        set_current_context(ctx)

        record_chaos_event("rule-1", "error", {"status_code": 503})

        assert len(ctx.chaos_applied) == 1
        assert ctx.chaos_applied[0]["rule"] == "rule-1"
        assert ctx.chaos_applied[0]["type"] == "error"
        assert ctx.chaos_applied[0]["status_code"] == 503

    def test_records_in_audit_log(self, monkeypatch):
        audit = AuditLog(max_size=100)
        monkeypatch.setattr("robotocore.audit.log.get_audit_log", lambda: audit)

        ctx = RequestContext(service="sqs", operation="SendMessage")
        set_current_context(ctx)

        # Call the function which imports get_audit_log internally
        record_chaos_event("throttle-rule", "error", {"status_code": 429})

        entries = audit.recent(limit=10)
        assert len(entries) >= 1
        chaos_entries = [e for e in entries if "chaos_injection" in (e.get("error") or "")]
        assert len(chaos_entries) == 1
        assert chaos_entries[0]["service"] == "sqs"
        assert chaos_entries[0]["operation"] == "chaos:error"

    def test_multiple_chaos_events_accumulate(self):
        ctx = RequestContext(service="lambda")
        set_current_context(ctx)

        record_chaos_event("rule-a", "latency", {"latency_ms": 500})
        record_chaos_event("rule-b", "error", {"status_code": 500})

        assert len(ctx.chaos_applied) == 2
        assert ctx.chaos_applied[0]["type"] == "latency"
        assert ctx.chaos_applied[1]["type"] == "error"


class TestTimeline:
    """Test the unified timeline endpoint."""

    def test_timeline_returns_entries(self):
        from starlette.applications import Starlette
        from starlette.routing import Route

        from robotocore.observability.timeline import handle_timeline

        app = Starlette(routes=[Route("/timeline", handle_timeline, methods=["GET"])])
        client = TestClient(app)

        # Record some audit entries first
        audit = get_audit_log()
        audit.record(service="s3", operation="PutObject", status_code=200)
        audit.record(
            service="sqs",
            operation="chaos:error",
            status_code=429,
            error="chaos_injection:throttle-rule",
        )

        response = client.get("/timeline")
        assert response.status_code == 200
        data = response.json()
        assert "entries" in data
        assert "count" in data
        assert "server_time" in data
        assert data["count"] >= 2

    def test_timeline_categories(self):
        from starlette.applications import Starlette
        from starlette.routing import Route

        from robotocore.observability.timeline import handle_timeline

        app = Starlette(routes=[Route("/timeline", handle_timeline, methods=["GET"])])
        client = TestClient(app)

        audit = get_audit_log()
        audit.clear()
        audit.record(service="s3", operation="PutObject", status_code=200)
        audit.record(
            service="sqs",
            operation="chaos:error",
            status_code=429,
            error="chaos_injection:throttle-rule",
        )

        response = client.get("/timeline")
        data = response.json()
        categories = {e["_category"] for e in data["entries"]}
        assert "api_call" in categories
        assert "chaos" in categories

    def test_timeline_service_filter(self):
        from starlette.applications import Starlette
        from starlette.routing import Route

        from robotocore.observability.timeline import handle_timeline

        app = Starlette(routes=[Route("/timeline", handle_timeline, methods=["GET"])])
        client = TestClient(app)

        audit = get_audit_log()
        audit.clear()
        audit.record(service="s3", operation="PutObject", status_code=200)
        audit.record(service="sqs", operation="SendMessage", status_code=200)

        response = client.get("/timeline?service=s3")
        data = response.json()
        assert all(e["service"] == "s3" for e in data["entries"])

    def test_timeline_limit(self):
        from starlette.applications import Starlette
        from starlette.routing import Route

        from robotocore.observability.timeline import handle_timeline

        app = Starlette(routes=[Route("/timeline", handle_timeline, methods=["GET"])])
        client = TestClient(app)

        audit = get_audit_log()
        audit.clear()
        for i in range(10):
            audit.record(service="s3", operation=f"Op{i}", status_code=200)

        response = client.get("/timeline?limit=3")
        data = response.json()
        assert data["count"] == 3
