"""Advanced handler chain tests: exception handler failures,
nested exceptions, context state mutations, chained stopping."""

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


class TestExceptionHandlerEdgeCases:
    def test_exception_handler_itself_throws(self):
        """If an exception handler throws, it should be caught and logged."""

        def bad_handler(ctx):
            raise ValueError("original error")

        def crashing_exc_handler(ctx, exc):
            raise RuntimeError("handler crashed too")

        def good_exc_handler(ctx, exc):
            ctx.response = Response(content="recovered")

        chain = HandlerChain()
        chain.request_handlers = [bad_handler]
        chain.exception_handlers = [crashing_exc_handler, good_exc_handler]
        ctx = _make_context()
        chain.handle(ctx)
        # Good handler should still have run despite crashing handler
        assert ctx.response is not None
        assert ctx.response.body == b"recovered"

    def test_all_exception_handlers_crash_reraises_original(self):
        """If all exception handlers fail and no response is set, original exc propagates."""

        def bad_handler(ctx):
            raise ValueError("original")

        def crash1(ctx, exc):
            raise RuntimeError("crash1")

        def crash2(ctx, exc):
            raise RuntimeError("crash2")

        chain = HandlerChain()
        chain.request_handlers = [bad_handler]
        chain.exception_handlers = [crash1, crash2]
        ctx = _make_context()
        try:
            chain.handle(ctx)
            assert False, "Should have raised"
        except ValueError as e:
            assert str(e) == "original"

    def test_exception_handler_sets_response_stops_reraise(self):
        """If an exception handler sets response, the original exception is NOT re-raised."""

        def bad_handler(ctx):
            raise ValueError("error")

        def exc_handler(ctx, exc):
            ctx.response = Response(content=f"caught: {exc}")

        chain = HandlerChain()
        chain.request_handlers = [bad_handler]
        chain.exception_handlers = [exc_handler]
        ctx = _make_context()
        chain.handle(ctx)
        assert ctx.response is not None
        assert b"caught: error" in ctx.response.body


class TestHandlerChainContextMutation:
    def test_handlers_can_modify_context_fields(self):
        """Request handlers can modify context fields for downstream handlers."""

        def set_operation(ctx):
            ctx.operation = "PutObject"

        def check_operation(ctx):
            assert ctx.operation == "PutObject"
            ctx.response = Response(content="ok")

        chain = HandlerChain()
        chain.request_handlers = [set_operation, check_operation]
        ctx = _make_context()
        chain.handle(ctx)
        assert ctx.operation == "PutObject"

    def test_parsed_request_dict_mutated(self):
        """Handlers can populate parsed_request for downstream use."""

        def parser(ctx):
            ctx.parsed_request = {"Action": "CreateBucket", "Bucket": "test"}

        def checker(ctx):
            assert ctx.parsed_request["Action"] == "CreateBucket"
            ctx.response = Response(content="ok")

        chain = HandlerChain()
        chain.request_handlers = [parser, checker]
        ctx = _make_context()
        chain.handle(ctx)
        assert ctx.parsed_request["Bucket"] == "test"


class TestHandlerChainStoppingBehavior:
    def test_first_handler_sets_response_stops_all(self):
        order = []

        def h1(ctx):
            order.append(1)
            ctx.response = Response(content="h1")

        def h2(ctx):
            order.append(2)

        def h3(ctx):
            order.append(3)

        chain = HandlerChain()
        chain.request_handlers = [h1, h2, h3]
        ctx = _make_context()
        chain.handle(ctx)
        assert order == [1]

    def test_middle_handler_sets_response(self):
        order = []

        def h1(ctx):
            order.append(1)

        def h2(ctx):
            order.append(2)
            ctx.response = Response(content="h2")

        def h3(ctx):
            order.append(3)

        chain = HandlerChain()
        chain.request_handlers = [h1, h2, h3]
        ctx = _make_context()
        chain.handle(ctx)
        assert order == [1, 2]

    def test_exception_in_second_handler_stops_chain(self):
        order = []

        def h1(ctx):
            order.append(1)

        def h2(ctx):
            order.append(2)
            raise RuntimeError("boom")

        def h3(ctx):
            order.append(3)

        def exc_handler(ctx, exc):
            ctx.response = Response(content="error")

        chain = HandlerChain()
        chain.request_handlers = [h1, h2, h3]
        chain.exception_handlers = [exc_handler]
        ctx = _make_context()
        chain.handle(ctx)
        assert order == [1, 2]  # h3 never ran


class TestRequestContextDefaults:
    def test_custom_account_id(self):
        ctx = RequestContext(
            request=MagicMock(),
            service_name="iam",
            account_id="999999999999",
        )
        assert ctx.account_id == "999999999999"

    def test_custom_region(self):
        ctx = RequestContext(
            request=MagicMock(),
            service_name="s3",
            region="eu-west-1",
        )
        assert ctx.region == "eu-west-1"

    def test_custom_protocol(self):
        ctx = RequestContext(
            request=MagicMock(),
            service_name="s3",
            protocol="rest-xml",
        )
        assert ctx.protocol == "rest-xml"
