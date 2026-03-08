"""Unit tests for the handler chain."""

from unittest.mock import MagicMock

from starlette.responses import Response

from robotocore.gateway.handler_chain import HandlerChain, RequestContext


def _make_context(**kwargs):
    request = MagicMock()
    return RequestContext(
        request=request,
        service_name=kwargs.get("service", "s3"),
        operation=kwargs.get("operation"),
        region=kwargs.get("region", "us-east-1"),
    )


class TestRequestContext:
    def test_defaults(self):
        request = MagicMock()
        ctx = RequestContext(request=request, service_name="s3")
        assert ctx.service_name == "s3"
        assert ctx.operation is None
        assert ctx.account_id == "123456789012"
        assert ctx.region == "us-east-1"
        assert ctx.protocol is None
        assert ctx.response is None
        assert ctx.parsed_request == {}


class TestHandlerChain:
    def test_empty_chain(self):
        chain = HandlerChain()
        ctx = _make_context()
        chain.handle(ctx)
        assert ctx.response is None

    def test_request_handlers_called_in_order(self):
        order = []
        chain = HandlerChain()
        chain.request_handlers = [
            lambda ctx: order.append("first"),
            lambda ctx: order.append("second"),
            lambda ctx: order.append("third"),
        ]
        chain.handle(_make_context())
        assert order == ["first", "second", "third"]

    def test_response_set_stops_chain(self):
        order = []

        def set_response(ctx):
            order.append("handler1")
            ctx.response = Response(content="early")

        def should_not_run(ctx):
            order.append("handler2")

        chain = HandlerChain()
        chain.request_handlers = [set_response, should_not_run]
        ctx = _make_context()
        chain.handle(ctx)
        assert order == ["handler1"]
        assert ctx.response.body == b"early"

    def test_response_handlers_always_run(self):
        called = []

        def set_resp(ctx):
            ctx.response = Response(content="ok")

        def resp_handler(ctx):
            called.append(True)

        chain = HandlerChain()
        chain.request_handlers = [set_resp]
        chain.response_handlers = [resp_handler]
        chain.handle(_make_context())
        assert called == [True]

    def test_response_handlers_run_after_request_handlers(self):
        order = []
        chain = HandlerChain()
        chain.request_handlers = [lambda ctx: order.append("request")]
        chain.response_handlers = [lambda ctx: order.append("response")]
        chain.handle(_make_context())
        assert order == ["request", "response"]

    def test_exception_handler_called(self):
        handled = []

        def bad_handler(ctx):
            raise ValueError("test error")

        def exc_handler(ctx, exc):
            handled.append(str(exc))
            ctx.response = Response(content="error handled")

        chain = HandlerChain()
        chain.request_handlers = [bad_handler]
        chain.exception_handlers = [exc_handler]
        ctx = _make_context()
        chain.handle(ctx)
        assert handled == ["test error"]
        assert ctx.response.body == b"error handled"

    def test_unhandled_exception_propagates(self):
        def bad_handler(ctx):
            raise RuntimeError("unhandled")

        chain = HandlerChain()
        chain.request_handlers = [bad_handler]
        try:
            chain.handle(_make_context())
            assert False, "Should have raised"
        except RuntimeError as e:
            assert str(e) == "unhandled"

    def test_response_handler_error_doesnt_crash(self):
        def bad_resp_handler(ctx):
            raise RuntimeError("resp error")

        chain = HandlerChain()
        chain.response_handlers = [bad_resp_handler]
        # Should not raise
        chain.handle(_make_context())

    def test_multiple_exception_handlers(self):
        handled = []

        def bad_handler(ctx):
            raise ValueError("boom")

        def exc1(ctx, exc):
            handled.append("exc1")

        def exc2(ctx, exc):
            handled.append("exc2")
            ctx.response = Response(content="handled")

        chain = HandlerChain()
        chain.request_handlers = [bad_handler]
        chain.exception_handlers = [exc1, exc2]
        ctx = _make_context()
        chain.handle(ctx)
        assert handled == ["exc1", "exc2"]
