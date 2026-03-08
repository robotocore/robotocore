"""Unit tests for the chaos middleware handler."""

from unittest.mock import MagicMock, patch

from robotocore.chaos.fault_rules import FaultRule, FaultRuleStore
from robotocore.chaos.middleware import chaos_handler
from robotocore.gateway.handler_chain import RequestContext


def _make_context(service="s3", operation="PutObject", region="us-east-1"):
    request = MagicMock()
    ctx = RequestContext(
        request=request,
        service_name=service,
        operation=operation,
        region=region,
    )
    return ctx


class TestChaosHandler:
    def test_no_rules_no_effect(self):
        store = FaultRuleStore()
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            ctx = _make_context()
            chaos_handler(ctx)
            assert ctx.response is None

    def test_matching_error_rule_sets_response(self):
        store = FaultRuleStore()
        store.add(FaultRule(service="s3", error_code="InternalError", status_code=500))
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            ctx = _make_context()
            chaos_handler(ctx)
            assert ctx.response is not None
            assert ctx.response.status_code == 500
            assert ctx.response.headers.get("x-robotocore-chaos") is not None

    def test_non_matching_rule_no_effect(self):
        store = FaultRuleStore()
        store.add(FaultRule(service="dynamodb", error_code="InternalError"))
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            ctx = _make_context(service="s3")
            chaos_handler(ctx)
            assert ctx.response is None

    @patch("robotocore.chaos.middleware.time.sleep")
    def test_latency_injection(self, mock_sleep):
        store = FaultRuleStore()
        store.add(FaultRule(service="s3", latency_ms=200))
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            ctx = _make_context()
            chaos_handler(ctx)
            mock_sleep.assert_called_once_with(0.2)

    @patch("robotocore.chaos.middleware.time.sleep")
    def test_latency_plus_error(self, mock_sleep):
        store = FaultRuleStore()
        store.add(FaultRule(service="s3", latency_ms=100, error_code="ThrottlingException"))
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            ctx = _make_context()
            chaos_handler(ctx)
            mock_sleep.assert_called_once_with(0.1)
            assert ctx.response is not None
            assert ctx.response.status_code == 429

    def test_latency_only_no_response(self):
        store = FaultRuleStore()
        store.add(FaultRule(service="s3", latency_ms=1))
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            with patch("robotocore.chaos.middleware.time.sleep"):
                ctx = _make_context()
                chaos_handler(ctx)
                # No error_code means no response set
                assert ctx.response is None

    def test_error_body_contains_type_and_message(self):
        store = FaultRuleStore()
        rule = FaultRule(
            service="s3",
            error_code="ServiceUnavailable",
            error_message="test fault",
        )
        store.add(rule)
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            ctx = _make_context()
            chaos_handler(ctx)
            import json

            body = json.loads(ctx.response.body.decode())
            assert body["__type"] == "ServiceUnavailable"
            assert body["message"] == "test fault"
            assert body["Message"] == "test fault"

    # --- Bug 3: Error response is always JSON (even for XML services) ---

    def test_error_response_media_type_is_json(self):
        """Chaos errors always return JSON. This documents current behavior.

        NOTE: XML-protocol services (S3, CloudFormation) would expect XML error
        responses. Clients may fail to parse JSON errors for these services.
        """
        store = FaultRuleStore()
        store.add(FaultRule(service="s3", error_code="InternalError"))
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            ctx = _make_context(service="s3")
            chaos_handler(ctx)
            assert ctx.response.media_type == "application/json"

    def test_error_response_has_chaos_header(self):
        store = FaultRuleStore()
        rule = FaultRule(rule_id="test-rule", service="s3", error_code="InternalError")
        store.add(rule)
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            ctx = _make_context()
            chaos_handler(ctx)
            assert ctx.response.headers["x-robotocore-chaos"] == "test-rule"

    def test_error_body_is_valid_json(self):
        """The response body must be parseable JSON."""
        import json

        store = FaultRuleStore()
        store.add(FaultRule(service="s3", error_code="Boom", error_message="kaboom"))
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            ctx = _make_context()
            chaos_handler(ctx)
            body = json.loads(ctx.response.body.decode())
            assert isinstance(body, dict)
            assert "__type" in body

    def test_error_uses_rule_status_code(self):
        store = FaultRuleStore()
        store.add(FaultRule(service="s3", error_code="SlowDown", status_code=503))
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            ctx = _make_context()
            chaos_handler(ctx)
            assert ctx.response.status_code == 503

    # --- Bug 5: time.sleep blocks event loop ---

    @patch("robotocore.chaos.middleware.time.sleep")
    def test_latency_calls_sleep_with_correct_duration(self, mock_sleep):
        store = FaultRuleStore()
        store.add(FaultRule(service="s3", latency_ms=1500))
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            ctx = _make_context()
            chaos_handler(ctx)
            mock_sleep.assert_called_once_with(1.5)

    @patch("robotocore.chaos.middleware.time.sleep")
    def test_zero_latency_does_not_sleep(self, mock_sleep):
        store = FaultRuleStore()
        store.add(FaultRule(service="s3", latency_ms=0, error_code="InternalError"))
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            ctx = _make_context()
            chaos_handler(ctx)
            mock_sleep.assert_not_called()

    def test_no_error_code_means_latency_only(self):
        """A rule with latency but no error_code should not set a response."""
        store = FaultRuleStore()
        store.add(FaultRule(service="s3", latency_ms=100))
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            with patch("robotocore.chaos.middleware.time.sleep"):
                ctx = _make_context()
                chaos_handler(ctx)
                assert ctx.response is None

    def test_error_body_has_exactly_three_keys(self):
        """Error body should have __type, message, and Message — nothing extra."""
        import json

        store = FaultRuleStore()
        store.add(FaultRule(service="s3", error_code="InternalError", error_message="boom"))
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            ctx = _make_context()
            chaos_handler(ctx)
            body = json.loads(ctx.response.body.decode())
            assert set(body.keys()) == {"__type", "message", "Message"}

    def test_error_for_query_protocol_service_still_json(self):
        """SQS uses query protocol but chaos returns JSON, not XML.

        This documents a known limitation: SDK XML parsers may fail on this.
        """
        store = FaultRuleStore()
        store.add(FaultRule(service="sqs", error_code="ThrottlingException"))
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            ctx = _make_context(service="sqs", operation="SendMessage")
            chaos_handler(ctx)
            assert ctx.response.media_type == "application/json"
            # Should arguably be text/xml for query protocol services
            assert b"<Error>" not in ctx.response.body

    def test_error_for_rest_xml_service_still_json(self):
        """S3 uses rest-xml but chaos returns JSON."""
        store = FaultRuleStore()
        store.add(FaultRule(service="s3", error_code="SlowDown"))
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            ctx = _make_context(service="s3", operation="PutObject")
            chaos_handler(ctx)
            assert ctx.response.media_type == "application/json"

    def test_error_for_rest_json_service_is_json(self):
        """DynamoDB uses json protocol — JSON error is correct."""
        store = FaultRuleStore()
        store.add(FaultRule(service="dynamodb", error_code="ThrottlingException"))
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            ctx = _make_context(service="dynamodb", operation="PutItem")
            chaos_handler(ctx)
            assert ctx.response.media_type == "application/json"

    def test_chaos_handler_sets_response_which_stops_chain(self):
        """When chaos sets context.response, the handler chain should stop."""
        from robotocore.gateway.handler_chain import HandlerChain

        store = FaultRuleStore()
        store.add(FaultRule(service="s3", error_code="InternalError"))

        subsequent_called = False

        def subsequent_handler(ctx):
            nonlocal subsequent_called
            subsequent_called = True

        chain = HandlerChain()
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            chain.request_handlers.append(chaos_handler)
            chain.request_handlers.append(subsequent_handler)
            ctx = _make_context()
            chain.handle(ctx)
            assert ctx.response is not None
            assert subsequent_called is False

    def test_chaos_handler_position_in_real_chain(self):
        """chaos_handler should be in the handler chain, after populate_context."""
        from robotocore.gateway.app import _handler_chain

        handler_names = [h.__name__ for h in _handler_chain.request_handlers]
        assert "chaos_handler" in handler_names
        chaos_idx = handler_names.index("chaos_handler")
        # Should come after populate_context_handler
        assert "populate_context_handler" in handler_names
        populate_idx = handler_names.index("populate_context_handler")
        assert chaos_idx > populate_idx

    def test_chaos_handler_is_sync_function(self):
        """chaos_handler is sync (not async), called directly in sync handler chain.

        This means time.sleep() blocks the calling thread. If the handler chain
        runs in the event loop thread, this blocks all concurrent requests.
        """
        import inspect

        assert not inspect.iscoroutinefunction(chaos_handler)
        # And the chain's handle method is also sync
        from robotocore.gateway.handler_chain import HandlerChain

        assert not inspect.iscoroutinefunction(HandlerChain.handle)

    @patch("robotocore.chaos.middleware.time.sleep")
    def test_negative_latency_treated_as_zero(self, mock_sleep):
        """FaultRule clamps latency_ms to its input; negative triggers no sleep."""
        store = FaultRuleStore()
        # latency_ms is stored as-is (no clamping in FaultRule), so -100 stored
        store.add(FaultRule(service="s3", latency_ms=-100, error_code="InternalError"))
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            ctx = _make_context()
            chaos_handler(ctx)
            # -100 > 0 is False, so sleep should not be called
            mock_sleep.assert_not_called()

    # --- Singleton isolation tests (Bug 4) ---

    def test_uses_global_store_by_default(self):
        """chaos_handler uses the global singleton store."""
        from robotocore.chaos.fault_rules import get_fault_store

        store = get_fault_store()
        original_rules = store.list_rules()
        try:
            store.add(FaultRule(rule_id="global-test", service="s3", error_code="Err"))
            ctx = _make_context()
            chaos_handler(ctx)
            assert ctx.response is not None
        finally:
            store.remove("global-test")
            # Verify cleanup
            assert store.list_rules() == original_rules
