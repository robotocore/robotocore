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
