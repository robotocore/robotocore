"""Tests for the handler chain and request context."""

from unittest.mock import MagicMock

from starlette.responses import Response

from robotocore.gateway.handler_chain import HandlerChain, RequestContext


def _make_context(**kwargs) -> RequestContext:
    request = MagicMock()
    request.headers = kwargs.pop("headers", {})
    request.url.path = kwargs.pop("path", "/")
    request.query_params = kwargs.pop("query_params", {})
    request.method = kwargs.pop("method", "POST")
    return RequestContext(
        request=request,
        service_name=kwargs.pop("service_name", "sts"),
        **kwargs,
    )


class TestRequestContext:
    def test_defaults(self):
        ctx = _make_context()
        assert ctx.account_id == "123456789012"
        assert ctx.region == "us-east-1"
        assert ctx.protocol is None
        assert ctx.operation is None
        assert ctx.response is None

    def test_custom_values(self):
        ctx = _make_context(
            service_name="s3",
            region="eu-west-1",
            account_id="999999999999",
            protocol="rest-xml",
        )
        assert ctx.service_name == "s3"
        assert ctx.region == "eu-west-1"
        assert ctx.account_id == "999999999999"
        assert ctx.protocol == "rest-xml"


class TestHandlerChain:
    def test_runs_request_handlers_in_order(self):
        chain = HandlerChain()
        order = []
        chain.request_handlers.append(lambda ctx: order.append(1))
        chain.request_handlers.append(lambda ctx: order.append(2))
        chain.request_handlers.append(lambda ctx: order.append(3))

        chain.handle(_make_context())
        assert order == [1, 2, 3]

    def test_response_handlers_not_run_in_handle(self):
        """Response handlers should NOT run inside handle() — they are called
        explicitly by app.py after the provider returns a response."""
        chain = HandlerChain()
        called = []
        chain.response_handlers.append(lambda ctx: called.append("response"))

        chain.handle(_make_context())
        assert called == [], "response handlers must not run inside handle()"

    def test_stops_on_response_set(self):
        chain = HandlerChain()
        order = []

        def set_response(ctx):
            order.append(1)
            ctx.response = Response(status_code=200)

        chain.request_handlers.append(set_response)
        chain.request_handlers.append(lambda ctx: order.append(2))

        chain.handle(_make_context())
        assert order == [1]  # Second handler not called

    def test_exception_handler_called(self):
        chain = HandlerChain()
        errors = []

        def fail(ctx):
            raise ValueError("boom")

        def catch(ctx, exc):
            errors.append(str(exc))
            ctx.response = Response(status_code=500)

        chain.request_handlers.append(fail)
        chain.exception_handlers.append(catch)

        ctx = _make_context()
        chain.handle(ctx)
        assert errors == ["boom"]
        assert ctx.response.status_code == 500

    def test_exception_reraises_if_no_response(self):
        chain = HandlerChain()
        chain.request_handlers.append(lambda ctx: (_ for _ in ()).throw(ValueError("unhandled")))

        import pytest

        with pytest.raises(ValueError, match="unhandled"):
            chain.handle(_make_context())

    def test_response_handlers_not_run_after_exception(self):
        """After an exception is handled, response handlers should NOT run
        inside handle() — they're run by app.py."""
        chain = HandlerChain()
        called = []

        def catch(ctx, exc):
            ctx.response = Response(status_code=500)

        chain.request_handlers.append(lambda ctx: (_ for _ in ()).throw(RuntimeError()))
        chain.exception_handlers.append(catch)
        chain.response_handlers.append(lambda ctx: called.append("response"))

        chain.handle(_make_context())
        assert called == [], "response handlers must not run inside handle()"

    def test_response_handlers_stored_on_chain(self):
        """Response handlers are stored and available for external callers."""
        chain = HandlerChain()

        def bad_response_handler(ctx):
            raise RuntimeError("response handler crash")

        chain.response_handlers.append(bad_response_handler)
        # handle() should not invoke response handlers at all
        chain.handle(_make_context())
        assert len(chain.response_handlers) == 1
