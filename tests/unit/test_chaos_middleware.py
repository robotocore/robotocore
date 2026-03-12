"""Unit tests for the chaos middleware handler."""

import json
from unittest.mock import MagicMock, patch

import pytest

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
    @pytest.fixture(autouse=True)
    def fault_store(self):
        store = FaultRuleStore()
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            yield store

    def test_no_rules_no_effect(self, fault_store):
        ctx = _make_context()
        chaos_handler(ctx)
        assert ctx.response is None

    def test_matching_error_rule_sets_response(self, fault_store):
        fault_store.add(FaultRule(service="s3", error_code="InternalError", status_code=500))
        ctx = _make_context()
        chaos_handler(ctx)
        assert ctx.response is not None
        assert ctx.response.status_code == 500
        assert ctx.response.media_type == "application/json"
        assert "x-robotocore-chaos" in ctx.response.headers

    def test_non_matching_rule_no_effect(self, fault_store):
        fault_store.add(FaultRule(service="dynamodb", error_code="InternalError"))
        ctx = _make_context(service="s3")
        chaos_handler(ctx)
        assert ctx.response is None

    @patch("robotocore.chaos.middleware.time.sleep")
    def test_latency_injection(self, mock_run, fault_store):
        fault_store.add(FaultRule(service="s3", latency_ms=200))
        ctx = _make_context()
        chaos_handler(ctx)
        mock_run.assert_called_once()
        # Verify the coroutine was created with asyncio.sleep
        assert mock_run.call_count == 1

    @patch("robotocore.chaos.middleware.time.sleep")
    def test_latency_plus_error(self, mock_run, fault_store):
        fault_store.add(FaultRule(service="s3", latency_ms=100, error_code="ThrottlingException"))
        ctx = _make_context()
        chaos_handler(ctx)
        mock_run.assert_called_once()
        assert ctx.response is not None
        assert ctx.response.status_code == 429
        assert ctx.response.media_type == "application/json"
        assert "x-robotocore-chaos" in ctx.response.headers

    @patch("robotocore.chaos.middleware.time.sleep")
    def test_latency_only_no_response(self, mock_run, fault_store):
        fault_store.add(FaultRule(service="s3", latency_ms=1))
        ctx = _make_context()
        chaos_handler(ctx)
        # No error_code means no response set
        assert ctx.response is None

    def test_error_body_contains_type_and_message(self, fault_store):
        rule = FaultRule(
            service="s3",
            error_code="ServiceUnavailable",
            error_message="test fault",
        )
        fault_store.add(rule)
        ctx = _make_context()
        chaos_handler(ctx)
        body = json.loads(ctx.response.body.decode())
        assert "__type" in body
        assert "message" in body
        assert "Message" in body
        assert body["__type"] == "ServiceUnavailable"
        assert body["message"] == "test fault"
        assert body["Message"] == "test fault"

    # --- Bug 3: Error response is always JSON (even for XML services) ---

    def test_error_response_media_type_is_json(self, fault_store):
        """Chaos errors always return JSON. This documents current behavior.

        NOTE: XML-protocol services (S3, CloudFormation) would expect XML error
        responses. Clients may fail to parse JSON errors for these services.
        """
        fault_store.add(FaultRule(service="s3", error_code="InternalError"))
        ctx = _make_context(service="s3")
        chaos_handler(ctx)
        assert ctx.response.media_type == "application/json"

    def test_error_response_has_chaos_header(self, fault_store):
        rule = FaultRule(rule_id="test-rule", service="s3", error_code="InternalError")
        fault_store.add(rule)
        ctx = _make_context()
        chaos_handler(ctx)
        assert ctx.response.headers["x-robotocore-chaos"] == "test-rule"

    def test_error_uses_rule_status_code(self, fault_store):
        fault_store.add(FaultRule(service="s3", error_code="SlowDown", status_code=503))
        ctx = _make_context()
        chaos_handler(ctx)
        assert ctx.response.status_code == 503
        assert ctx.response.media_type == "application/json"
        assert "x-robotocore-chaos" in ctx.response.headers

    # --- Bug 5: time.sleep for blocking latency (runs in thread) ---

    @patch("robotocore.chaos.middleware.time.sleep")
    def test_latency_calls_sleep_with_correct_duration(self, mock_sleep, fault_store):
        fault_store.add(FaultRule(service="s3", latency_ms=1500))
        ctx = _make_context()
        chaos_handler(ctx)
        mock_sleep.assert_called_once_with(1.5)

    @patch("robotocore.chaos.middleware.time.sleep")
    def test_zero_latency_does_not_sleep(self, mock_run, fault_store):
        fault_store.add(FaultRule(service="s3", latency_ms=0, error_code="InternalError"))
        ctx = _make_context()
        chaos_handler(ctx)
        mock_run.assert_not_called()

    @patch("robotocore.chaos.middleware.time.sleep")
    def test_no_error_code_means_latency_only(self, mock_run, fault_store):
        """A rule with latency but no error_code should not set a response."""
        fault_store.add(FaultRule(service="s3", latency_ms=100))
        ctx = _make_context()
        chaos_handler(ctx)
        assert ctx.response is None

    def test_error_for_query_protocol_service_still_json(self, fault_store):
        """SQS uses query protocol but chaos returns JSON, not XML.

        This documents a known limitation: SDK XML parsers may fail on this.
        """
        fault_store.add(FaultRule(service="sqs", error_code="ThrottlingException"))
        ctx = _make_context(service="sqs", operation="SendMessage")
        chaos_handler(ctx)
        assert ctx.response.media_type == "application/json"
        # Should arguably be text/xml for query protocol services
        assert b"<Error>" not in ctx.response.body

    def test_error_for_rest_xml_service_still_json(self, fault_store):
        """S3 uses rest-xml but chaos returns JSON."""
        fault_store.add(FaultRule(service="s3", error_code="SlowDown"))
        ctx = _make_context(service="s3", operation="PutObject")
        chaos_handler(ctx)
        assert ctx.response.media_type == "application/json"

    def test_error_for_rest_json_service_is_json(self, fault_store):
        """DynamoDB uses json protocol — JSON error is correct."""
        fault_store.add(FaultRule(service="dynamodb", error_code="ThrottlingException"))
        ctx = _make_context(service="dynamodb", operation="PutItem")
        chaos_handler(ctx)
        assert ctx.response.media_type == "application/json"

    def test_chaos_handler_sets_response_which_stops_chain(self, fault_store):
        """When chaos sets context.response, the handler chain should stop."""
        from robotocore.gateway.handler_chain import HandlerChain

        fault_store.add(FaultRule(service="s3", error_code="InternalError"))

        subsequent_called = False

        def subsequent_handler(ctx):
            nonlocal subsequent_called
            subsequent_called = True

        chain = HandlerChain()
        chain.request_handlers.append(chaos_handler)
        chain.request_handlers.append(subsequent_handler)
        ctx = _make_context()
        chain.handle(ctx)
        assert ctx.response is not None
        assert subsequent_called is False

    def test_chaos_handler_position_in_real_chain(self, fault_store):
        """chaos_handler should be in the handler chain, after populate_context."""
        from robotocore.gateway.app import _handler_chain

        handler_names = [h.__name__ for h in _handler_chain.request_handlers]
        assert "chaos_handler" in handler_names
        chaos_idx = handler_names.index("chaos_handler")
        # Should come after populate_context_handler
        assert "populate_context_handler" in handler_names
        populate_idx = handler_names.index("populate_context_handler")
        assert chaos_idx > populate_idx

    @patch("robotocore.chaos.middleware.time.sleep")
    def test_negative_latency_treated_as_zero(self, mock_run, fault_store):
        """FaultRule clamps latency_ms to its input; negative triggers no sleep."""
        # latency_ms is stored as-is (no clamping in FaultRule), so -100 stored
        fault_store.add(FaultRule(service="s3", latency_ms=-100, error_code="InternalError"))
        ctx = _make_context()
        chaos_handler(ctx)
        # -100 > 0 is False, so sleep should not be called
        mock_run.assert_not_called()


class TestChaosGlobalStore:
    """Tests that use the real global store (no autouse fixture)."""

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
