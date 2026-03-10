"""Advanced tests for EventBridge dead-letter queue (DLQ) handling."""

import json
from unittest.mock import MagicMock, patch

from robotocore.services.events.models import EventRule, EventTarget
from robotocore.services.events.provider import (
    _dispatch_to_targets,
    _send_to_dlq,
    clear_invocation_log,
    get_invocation_log,
)

# _send_to_dlq lazily imports _get_store from sqs.provider, so we must patch
# at the source module for the lazy import to pick up the mock.
SQS_GET_STORE = "robotocore.services.sqs.provider._get_store"


def _make_rule(
    name="test-rule",
    bus_name="default",
    region="us-east-1",
    account_id="123456789012",
    dead_letter_config=None,
):
    return EventRule(
        name=name,
        event_bus_name=bus_name,
        region=region,
        account_id=account_id,
        event_pattern={"source": ["test"]},
        dead_letter_config=dead_letter_config,
    )


def _make_target(
    target_id="t1",
    arn="arn:aws:lambda:us-east-1:123456789012:function:my-fn",
    dead_letter_config=None,
):
    return EventTarget(
        target_id=target_id,
        arn=arn,
        dead_letter_config=dead_letter_config,
    )


def _mock_sqs_store(mock_queue):
    """Return a mock SQS store whose get_queue returns mock_queue."""
    mock_store = MagicMock()
    mock_store.get_queue.return_value = mock_queue
    return mock_store


class TestDLQSendsFailedEvents:
    """DLQ receives failed event messages with correct metadata."""

    def setup_method(self):
        clear_invocation_log()

    def test_send_to_dlq_creates_sqs_message(self):
        mock_queue = MagicMock()
        mock_store = _mock_sqs_store(mock_queue)

        with patch(SQS_GET_STORE, return_value=mock_store):
            rule = _make_rule(name="my-rule")
            target = _make_target(target_id="target-1")
            event = {"source": "test", "detail": {"key": "value"}}
            _send_to_dlq(
                {"Arn": "arn:aws:sqs:us-east-1:123456789012:dlq"},
                event,
                target,
                rule,
                RuntimeError("invoke failed"),
                "us-east-1",
                "123456789012",
            )

        mock_queue.put.assert_called_once()
        msg = mock_queue.put.call_args[0][0]
        body = json.loads(msg.body)
        assert body["rule"] == "my-rule"
        assert body["target"] == "target-1"
        assert body["error"] == "invoke failed"
        assert body["event"]["source"] == "test"

    def test_send_to_dlq_missing_queue(self):
        """DLQ with nonexistent queue does not crash."""
        mock_store = MagicMock()
        mock_store.get_queue.return_value = None

        with patch(SQS_GET_STORE, return_value=mock_store):
            rule = _make_rule()
            target = _make_target()
            _send_to_dlq(
                {"Arn": "arn:aws:sqs:us-east-1:123456789012:gone"},
                {"source": "test"},
                target,
                rule,
                RuntimeError("err"),
                "us-east-1",
                "123456789012",
            )

    def test_send_to_dlq_non_sqs_arn_ignored(self):
        """DLQ config with non-SQS ARN is silently ignored."""
        mock_store = MagicMock()
        with patch(SQS_GET_STORE, return_value=mock_store):
            rule = _make_rule()
            target = _make_target()
            _send_to_dlq(
                {"Arn": "arn:aws:sns:us-east-1:123456789012:not-sqs"},
                {"source": "test"},
                target,
                rule,
                RuntimeError("err"),
                "us-east-1",
                "123456789012",
            )
        mock_store.get_queue.assert_not_called()


class TestDLQOnTargetFailure:
    """DLQ fires when target invocation fails."""

    def setup_method(self):
        clear_invocation_log()

    def test_target_failure_routes_to_dlq(self):
        """When a target invocation raises, the event goes to the DLQ."""
        dlq_config = {"Arn": "arn:aws:sqs:us-east-1:123456789012:my-dlq"}
        rule = _make_rule()
        target = _make_target(
            arn="arn:aws:lambda:us-east-1:123456789012:function:broken-fn",
            dead_letter_config=dlq_config,
        )
        rule.targets[target.target_id] = target
        event = {"source": "test", "detail-type": "Test", "detail": {}}

        mock_queue = MagicMock()
        mock_store = _mock_sqs_store(mock_queue)

        with (
            patch(
                "robotocore.services.events.provider._invoke_target",
                side_effect=RuntimeError("Lambda not found"),
            ),
            patch(SQS_GET_STORE, return_value=mock_store),
        ):
            _dispatch_to_targets(rule, event, "us-east-1", "123456789012")

        mock_queue.put.assert_called_once()
        msg_body = json.loads(mock_queue.put.call_args[0][0].body)
        assert msg_body["error"] == "Lambda not found"

    def test_rule_level_dlq_used_when_target_has_none(self):
        """If target has no DLQ but rule does, rule DLQ is used."""
        dlq_config = {"Arn": "arn:aws:sqs:us-east-1:123456789012:rule-dlq"}
        rule = _make_rule(dead_letter_config=dlq_config)
        target = _make_target(dead_letter_config=None)
        rule.targets[target.target_id] = target
        event = {"source": "test", "detail-type": "Test", "detail": {}}

        mock_queue = MagicMock()
        mock_store = _mock_sqs_store(mock_queue)

        with (
            patch(
                "robotocore.services.events.provider._invoke_target",
                side_effect=RuntimeError("boom"),
            ),
            patch(SQS_GET_STORE, return_value=mock_store),
        ):
            _dispatch_to_targets(rule, event, "us-east-1", "123456789012")

        mock_queue.put.assert_called_once()

    def test_target_dlq_takes_precedence_over_rule_dlq(self):
        """Target-level DLQ is preferred over rule-level DLQ."""
        rule = _make_rule(dead_letter_config={"Arn": "arn:aws:sqs:us-east-1:123456789012:rule-dlq"})
        target = _make_target(
            dead_letter_config={"Arn": "arn:aws:sqs:us-east-1:123456789012:target-dlq"}
        )
        rule.targets[target.target_id] = target
        event = {"source": "test", "detail-type": "Test", "detail": {}}

        mock_queue = MagicMock()
        mock_store = _mock_sqs_store(mock_queue)

        with (
            patch(
                "robotocore.services.events.provider._invoke_target",
                side_effect=RuntimeError("fail"),
            ),
            patch(SQS_GET_STORE, return_value=mock_store),
        ):
            _dispatch_to_targets(rule, event, "us-east-1", "123456789012")

        mock_store.get_queue.assert_called_with("target-dlq")


class TestDLQHighVolume:
    """High volume: many events, some fail, verify DLQ gets only failures."""

    def setup_method(self):
        clear_invocation_log()

    def test_only_failed_events_reach_dlq(self):
        """Of N events dispatched to a target, only failing ones go to DLQ."""
        dlq_config = {"Arn": "arn:aws:sqs:us-east-1:123456789012:fail-dlq"}
        rule = _make_rule()
        target = _make_target(dead_letter_config=dlq_config)
        rule.targets[target.target_id] = target

        mock_queue = MagicMock()
        mock_store = _mock_sqs_store(mock_queue)

        call_count = 0

        def invoke_side_effect(tgt, evt, region, account):
            nonlocal call_count
            call_count += 1
            if call_count % 3 == 0:
                raise RuntimeError(f"fail-{call_count}")

        with (
            patch(
                "robotocore.services.events.provider._invoke_target",
                side_effect=invoke_side_effect,
            ),
            patch(SQS_GET_STORE, return_value=mock_store),
        ):
            for i in range(9):
                _dispatch_to_targets(
                    rule,
                    {"source": "test", "id": str(i)},
                    "us-east-1",
                    "123456789012",
                )

        # 9 events total, every 3rd fails -> 3 failures go to DLQ
        assert mock_queue.put.call_count == 3
        for put_call in mock_queue.put.call_args_list:
            msg_body = json.loads(put_call[0][0].body)
            assert "fail-" in msg_body["error"]


class TestDLQInvocationLog:
    """DLQ invocations are logged."""

    def setup_method(self):
        clear_invocation_log()

    def test_dlq_invocation_logged(self):
        mock_queue = MagicMock()
        mock_store = _mock_sqs_store(mock_queue)

        with patch(SQS_GET_STORE, return_value=mock_store):
            rule = _make_rule(name="log-rule")
            target = _make_target(target_id="log-target")
            _send_to_dlq(
                {"Arn": "arn:aws:sqs:us-east-1:123456789012:log-dlq"},
                {"source": "test"},
                target,
                rule,
                RuntimeError("err"),
                "us-east-1",
                "123456789012",
            )

        log = get_invocation_log()
        dlq_entries = [e for e in log if e["target_type"] == "dlq"]
        assert len(dlq_entries) == 1
        assert "log-dlq" in dlq_entries[0]["target_arn"]
