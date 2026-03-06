"""Unit tests for EventBridge cross-service target dispatch (Lambda, SQS, SNS)."""

import io
import json
import uuid
import zipfile
from unittest.mock import patch

import pytest

from robotocore.services.events.models import EventRule, EventsStore, EventTarget
from robotocore.services.events.provider import (
    _dispatch_to_targets,
    _invoke_lambda_target,
    _invoke_target,
    clear_invocation_log,
    get_invocation_log,
)


def _make_lambda_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


@pytest.fixture(autouse=True)
def _clear_log():
    """Clear the invocation log before each test."""
    clear_invocation_log()
    yield
    clear_invocation_log()


class TestInvokeLambdaTarget:
    """Test EventBridge -> Lambda target invocation via invoke_lambda_async."""

    def test_invokes_python_lambda_with_event_payload(self):
        """EventBridge should dispatch to invoke_lambda_async with the event as payload."""
        event = {
            "version": "0",
            "id": str(uuid.uuid4()),
            "source": "test.source",
            "detail-type": "TestEvent",
            "detail": {"message": "hello"},
            "account": "123456789012",
            "region": "us-east-1",
        }
        payload = json.dumps(event)
        arn = "arn:aws:lambda:us-east-1:123456789012:function:my-func"

        with patch("robotocore.services.lambda_.invoke.invoke_lambda_async") as mock_invoke:
            _invoke_lambda_target(arn, payload, "us-east-1", "123456789012")

            mock_invoke.assert_called_once()
            call_args = mock_invoke.call_args
            assert call_args[0][0] == arn  # function ARN
            invoked_event = call_args[0][1]  # event payload
            assert invoked_event["source"] == "test.source"
            assert invoked_event["detail"]["message"] == "hello"

    def test_parses_json_string_payload(self):
        """Should parse JSON string payload before dispatching."""
        arn = "arn:aws:lambda:us-east-1:123456789012:function:test-fn"
        payload = json.dumps({"test": True, "source": "eb"})

        with patch("robotocore.services.lambda_.invoke.invoke_lambda_async") as mock_invoke:
            _invoke_lambda_target(arn, payload, "us-east-1", "123456789012")

            invoked_event = mock_invoke.call_args[0][1]
            assert invoked_event["test"] is True
            assert invoked_event["source"] == "eb"

    def test_passes_callback_for_invocation_log(self):
        """Should pass a callback that logs to the invocation log."""
        arn = "arn:aws:lambda:us-east-1:123456789012:function:my-func"

        with patch("robotocore.services.lambda_.invoke.invoke_lambda_async") as mock_invoke:
            _invoke_lambda_target(arn, '{"test": true}', "us-east-1", "123456789012")

            # Verify a callback was passed
            call_kwargs = mock_invoke.call_args[1]
            assert "callback" in call_kwargs
            assert callable(call_kwargs["callback"])

            # Simulate callback execution
            call_kwargs["callback"]({"result": "ok"}, None, "logs")
            log = get_invocation_log()
            assert len(log) == 1
            assert log[0]["target_type"] == "lambda"
            assert log[0]["target_arn"] == arn

    def test_handles_missing_function_gracefully(self):
        """Should not raise when Lambda function doesn't exist — error handled by invoke module."""
        arn = "arn:aws:lambda:us-east-1:123456789012:function:nonexistent"

        with patch("robotocore.services.lambda_.invoke.invoke_lambda_async") as mock_invoke:
            # Should not raise
            _invoke_lambda_target(arn, '{"test": true}', "us-east-1", "123456789012")
            mock_invoke.assert_called_once()


class TestInvokeTarget:
    """Test the _invoke_target routing."""

    def test_routes_lambda_arn(self):
        target = EventTarget(target_id="t1", arn="arn:aws:lambda:us-east-1:123456789012:function:f")
        event = {"source": "test", "detail": {}}

        with patch("robotocore.services.events.provider._invoke_lambda_target") as mock:
            _invoke_target(target, event, "us-east-1", "123456789012")
            mock.assert_called_once()

    def test_routes_sqs_arn(self):
        target = EventTarget(target_id="t1", arn="arn:aws:sqs:us-east-1:123456789012:my-queue")
        event = {"source": "test", "detail": {}}

        with patch("robotocore.services.events.provider._invoke_sqs_target") as mock:
            _invoke_target(target, event, "us-east-1", "123456789012")
            mock.assert_called_once()

    def test_routes_sns_arn(self):
        target = EventTarget(target_id="t1", arn="arn:aws:sns:us-east-1:123456789012:my-topic")
        event = {"source": "test", "detail": {}}

        with patch("robotocore.services.events.provider._invoke_sns_target") as mock:
            _invoke_target(target, event, "us-east-1", "123456789012")
            mock.assert_called_once()

    def test_uses_target_input_when_set(self):
        """When target.input is set, use it instead of the event."""
        custom_input = json.dumps({"custom": "payload"})
        target = EventTarget(
            target_id="t1",
            arn="arn:aws:lambda:us-east-1:123456789012:function:f",
            input=custom_input,
        )
        event = {"source": "test", "detail": {"original": True}}

        with patch("robotocore.services.events.provider._invoke_lambda_target") as mock:
            _invoke_target(target, event, "us-east-1", "123456789012")
            # Should pass the custom input, not the event
            assert mock.call_args[0][1] == custom_input


class TestDispatchToTargets:
    """Test that _dispatch_to_targets invokes all targets of a rule."""

    def test_dispatches_to_all_targets(self):
        rule = EventRule(
            name="test-rule",
            event_bus_name="default",
            region="us-east-1",
            account_id="123456789012",
            event_pattern={"source": ["test"]},
        )
        rule.targets["t1"] = EventTarget(
            target_id="t1", arn="arn:aws:lambda:us-east-1:123456789012:function:f1"
        )
        rule.targets["t2"] = EventTarget(
            target_id="t2", arn="arn:aws:sqs:us-east-1:123456789012:q1"
        )

        event = {"source": "test", "detail": {}}

        with patch("robotocore.services.events.provider._invoke_target") as mock:
            _dispatch_to_targets(rule, event, "us-east-1", "123456789012")
            assert mock.call_count == 2

    def test_continues_on_target_error(self):
        """If one target fails, the other targets should still be invoked."""
        rule = EventRule(
            name="test-rule",
            event_bus_name="default",
            region="us-east-1",
            account_id="123456789012",
        )
        rule.targets["t1"] = EventTarget(
            target_id="t1", arn="arn:aws:lambda:us-east-1:123456789012:function:f1"
        )
        rule.targets["t2"] = EventTarget(
            target_id="t2", arn="arn:aws:sqs:us-east-1:123456789012:q1"
        )

        event = {"source": "test", "detail": {}}

        with patch("robotocore.services.events.provider._invoke_target") as mock:
            mock.side_effect = [Exception("boom"), None]
            _dispatch_to_targets(rule, event, "us-east-1", "123456789012")
            assert mock.call_count == 2


class TestEventMatchingAndDispatch:
    """Integration-style test: put_events -> match rules -> dispatch targets."""

    def test_put_events_matches_and_dispatches(self):
        """Full flow: create store/bus/rule/target, put event, verify dispatch."""
        store = EventsStore()
        store.ensure_default_bus("us-east-1", "123456789012")
        store.put_rule(
            "test-rule",
            "default",
            "us-east-1",
            "123456789012",
            event_pattern={"source": ["myapp.orders"]},
        )
        store.put_targets(
            "test-rule",
            "default",
            [
                {
                    "Id": "lambda-1",
                    "Arn": "arn:aws:lambda:us-east-1:123456789012:function:process-order",
                },
            ],
        )

        # Simulate what _put_events does
        rule = store.get_rule("test-rule", "default")
        event = {
            "version": "0",
            "id": str(uuid.uuid4()),
            "source": "myapp.orders",
            "detail-type": "OrderPlaced",
            "detail": {"order_id": "12345"},
            "account": "123456789012",
            "region": "us-east-1",
        }

        assert rule.matches_event(event) is True

        with patch("robotocore.services.events.provider._invoke_target") as mock:
            _dispatch_to_targets(rule, event, "us-east-1", "123456789012")
            mock.assert_called_once()
            target_arg = mock.call_args[0][0]
            assert target_arg.arn == "arn:aws:lambda:us-east-1:123456789012:function:process-order"

    def test_non_matching_event_not_dispatched(self):
        """Events that don't match should not trigger dispatch."""
        store = EventsStore()
        store.ensure_default_bus("us-east-1", "123456789012")
        store.put_rule(
            "test-rule",
            "default",
            "us-east-1",
            "123456789012",
            event_pattern={"source": ["myapp.orders"]},
        )
        store.put_targets(
            "test-rule",
            "default",
            [
                {
                    "Id": "lambda-1",
                    "Arn": "arn:aws:lambda:us-east-1:123456789012:function:process-order",
                },
            ],
        )

        rule = store.get_rule("test-rule", "default")
        event = {
            "source": "other.source",
            "detail-type": "SomeEvent",
            "detail": {},
        }

        assert rule.matches_event(event) is False
