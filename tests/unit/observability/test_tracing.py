"""Tests for request tracing."""

from robotocore.observability.tracing import generate_request_id


class TestGenerateRequestId:
    def test_returns_string(self):
        rid = generate_request_id()
        assert isinstance(rid, str)

    def test_unique_ids(self):
        ids = {generate_request_id() for _ in range(100)}
        assert len(ids) == 100

    def test_uuid_format(self):
        rid = generate_request_id()
        parts = rid.split("-")
        assert len(parts) == 5


class TestTracingMiddlewareNoDoubleCount:
    """Bug fix 1A: Verify tracing middleware does NOT increment request_counter."""

    def test_tracing_middleware_does_not_increment_counter(self):
        """The counter should only be incremented in app.py, not in tracing middleware."""
        import inspect

        from robotocore.observability import tracing

        source = inspect.getsource(tracing.TracingMiddleware)
        assert "request_counter" not in source, (
            "TracingMiddleware should not reference request_counter -- "
            "counting happens in app.py to avoid double-counting"
        )
