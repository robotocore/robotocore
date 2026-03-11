"""Advanced tests for the tracing module: request ID generation,
TracingMiddleware behavior."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from robotocore.observability.tracing import TracingMiddleware, generate_request_id


class TestGenerateRequestId:
    def test_returns_string(self):
        rid = generate_request_id()
        assert isinstance(rid, str)

    def test_unique_ids(self):
        ids = {generate_request_id() for _ in range(100)}
        assert len(ids) == 100

    def test_uuid_format(self):
        """Request IDs should be valid UUIDs (contain hyphens, 36 chars)."""
        rid = generate_request_id()
        assert len(rid) == 36
        assert rid.count("-") == 4


class TestTracingMiddleware:
    def test_adds_request_id_header(self):
        middleware = TracingMiddleware(app=MagicMock())

        async def _run():
            request = MagicMock()
            request.method = "POST"
            request.url.path = "/"
            request.headers = {}
            request.body = AsyncMock(return_value=b"")
            request.state = MagicMock()

            response = MagicMock()
            response.status_code = 200
            response.headers = {}

            async def call_next(_req):
                return response

            with patch("robotocore.observability.tracing.log_request"):
                with patch("robotocore.observability.tracing.log_response"):
                    result = await middleware.dispatch(request, call_next)

            assert "X-Amz-Request-Id" in result.headers
            assert "X-Robotocore-Request-Id" in result.headers
            # Both headers should have the same value
            assert result.headers["X-Amz-Request-Id"] == result.headers["X-Robotocore-Request-Id"]

        asyncio.get_event_loop().run_until_complete(_run())

    def test_sets_request_state(self):
        middleware = TracingMiddleware(app=MagicMock())

        async def _run():
            request = MagicMock()
            request.method = "GET"
            request.url.path = "/test"
            request.headers = {}
            request.body = AsyncMock(return_value=b"")

            class State:
                pass

            request.state = State()

            response = MagicMock()
            response.status_code = 200
            response.headers = {}

            async def call_next(_req):
                return response

            with patch("robotocore.observability.tracing.log_request"):
                with patch("robotocore.observability.tracing.log_response"):
                    await middleware.dispatch(request, call_next)

            assert hasattr(request.state, "request_id")
            assert hasattr(request.state, "start_time")
            assert isinstance(request.state.request_id, str)

        asyncio.get_event_loop().run_until_complete(_run())

    def test_calls_log_request_and_log_response(self):
        middleware = TracingMiddleware(app=MagicMock())

        async def _run():
            request = MagicMock()
            request.method = "POST"
            request.url.path = "/"
            request.headers = {"content-type": "application/json"}
            request.body = AsyncMock(return_value=b'{"key": "value"}')
            request.state = MagicMock()

            response = MagicMock()
            response.status_code = 200
            response.headers = {"content-length": "42"}

            async def call_next(_req):
                return response

            with patch("robotocore.observability.tracing.log_request") as mock_log_req:
                with patch("robotocore.observability.tracing.log_response") as mock_log_resp:
                    await middleware.dispatch(request, call_next)

            mock_log_req.assert_called_once()
            mock_log_resp.assert_called_once()
            # log_response should include status_code and body_size
            call_kwargs = mock_log_resp.call_args
            assert call_kwargs.kwargs["status_code"] == 200
            assert call_kwargs.kwargs["body_size"] == 42

        asyncio.get_event_loop().run_until_complete(_run())
