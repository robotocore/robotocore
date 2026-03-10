"""Tests for EventBridge dead-letter queue (DLQ) behavior.

Covers:
1. Target-level DLQ: failed invocation sends to SQS DLQ
2. Missing Lambda target: event goes to DLQ
3. Missing SQS target: event goes to DLQ
4. DLQ message format: contains original event, error info, rule/target metadata
5. No DLQ configured: target fails, event silently dropped
6. Multiple targets, one fails: only the failed target's event goes to DLQ
7. Rule-level DLQ fallback
8. DLQ queue not found
"""

import hashlib
import json
from unittest.mock import patch

from robotocore.services.events.models import EventRule, EventTarget
from robotocore.services.events.provider import (
    _dispatch_to_targets,
    _send_to_dlq,
    clear_invocation_log,
    get_invocation_log,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ACCOUNT_ID = "123456789012"
REGION = "us-east-1"

SAMPLE_EVENT = {
    "version": "0",
    "id": "test-event-id",
    "source": "my.app",
    "account": ACCOUNT_ID,
    "time": "2026-03-09T00:00:00Z",
    "region": REGION,
    "resources": [],
    "detail-type": "MyEvent",
    "detail": {"key": "value"},
}


class FakeSqsQueue:
    """A simple fake SQS queue that stores messages in a list."""

    def __init__(self):
        self.messages: list = []

    def put(self, msg):
        self.messages.append(msg)


class FakeSqsStore:
    """A fake SQS store that returns queues by name."""

    def __init__(self, queues: dict[str, FakeSqsQueue] | None = None):
        self._queues = queues or {}

    def get_queue(self, name: str):
        return self._queues.get(name)


def _make_rule(
    name: str = "test-rule",
    bus_name: str = "default",
    targets: dict | None = None,
    dead_letter_config: dict | None = None,
) -> EventRule:
    rule = EventRule(
        name=name,
        event_bus_name=bus_name,
        region=REGION,
        account_id=ACCOUNT_ID,
        state="ENABLED",
        event_pattern={"source": ["my.app"]},
        dead_letter_config=dead_letter_config,
    )
    if targets:
        rule.targets = targets
    return rule


def _make_target(
    target_id: str = "target-1",
    arn: str = "arn:aws:lambda:us-east-1:123456789012:function:my-func",
    dead_letter_config: dict | None = None,
) -> EventTarget:
    return EventTarget(
        target_id=target_id,
        arn=arn,
        dead_letter_config=dead_letter_config,
    )


DLQ_ARN = f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:my-dlq"
DLQ_CONFIG = {"Arn": DLQ_ARN}


# ---------------------------------------------------------------------------
# 1. Target-level DLQ: failed invocation sends to SQS DLQ
# ---------------------------------------------------------------------------


class TestTargetLevelDlq:
    def test_failed_target_sends_to_target_dlq(self):
        """When a target has DeadLetterConfig and invocation fails, DLQ receives the event."""
        dlq_queue = FakeSqsQueue()
        sqs_store = FakeSqsStore({"my-dlq": dlq_queue})
        target = _make_target(dead_letter_config=DLQ_CONFIG)
        rule = _make_rule(targets={"target-1": target})

        with (
            patch(
                "robotocore.services.events.provider._invoke_target",
                side_effect=RuntimeError("Lambda invocation failed"),
            ),
            patch(
                "robotocore.services.sqs.provider._get_store",
                return_value=sqs_store,
            ),
        ):
            _dispatch_to_targets(rule, SAMPLE_EVENT, REGION, ACCOUNT_ID)

        assert len(dlq_queue.messages) == 1
        body = json.loads(dlq_queue.messages[0].body)
        assert body["event"] == SAMPLE_EVENT
        assert body["rule"] == "test-rule"
        assert body["target"] == "target-1"
        assert "Lambda invocation failed" in body["error"]


# ---------------------------------------------------------------------------
# 2. Missing Lambda target DLQ
# ---------------------------------------------------------------------------


class TestMissingLambdaDlq:
    def test_missing_lambda_function_sends_to_dlq(self):
        """When Lambda function doesn't exist, event goes to DLQ."""
        dlq_queue = FakeSqsQueue()
        sqs_store = FakeSqsStore({"my-dlq": dlq_queue})
        target = _make_target(
            arn="arn:aws:lambda:us-east-1:123456789012:function:nonexistent",
            dead_letter_config=DLQ_CONFIG,
        )
        rule = _make_rule(targets={"target-1": target})

        with (
            patch(
                "robotocore.services.events.provider._invoke_target",
                side_effect=RuntimeError("Lambda function not found: nonexistent"),
            ),
            patch(
                "robotocore.services.sqs.provider._get_store",
                return_value=sqs_store,
            ),
        ):
            _dispatch_to_targets(rule, SAMPLE_EVENT, REGION, ACCOUNT_ID)

        assert len(dlq_queue.messages) == 1
        body = json.loads(dlq_queue.messages[0].body)
        assert "nonexistent" in body["error"]


# ---------------------------------------------------------------------------
# 3. SQS target failure DLQ
# ---------------------------------------------------------------------------


class TestSqsTargetFailureDlq:
    def test_sqs_target_failure_sends_to_dlq(self):
        """When SQS target invocation fails, event goes to DLQ."""
        dlq_queue = FakeSqsQueue()
        sqs_store = FakeSqsStore({"my-dlq": dlq_queue})
        target = _make_target(
            arn=f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:nonexistent-queue",
            dead_letter_config=DLQ_CONFIG,
        )
        rule = _make_rule(targets={"target-1": target})

        with (
            patch(
                "robotocore.services.events.provider._invoke_target",
                side_effect=RuntimeError("SQS queue not found"),
            ),
            patch(
                "robotocore.services.sqs.provider._get_store",
                return_value=sqs_store,
            ),
        ):
            _dispatch_to_targets(rule, SAMPLE_EVENT, REGION, ACCOUNT_ID)

        assert len(dlq_queue.messages) == 1
        body = json.loads(dlq_queue.messages[0].body)
        assert body["target"] == "target-1"


# ---------------------------------------------------------------------------
# 4. DLQ message format
# ---------------------------------------------------------------------------


class TestDlqMessageFormat:
    def test_dlq_message_contains_all_expected_fields(self):
        """The DLQ message should contain the original event, rule, target, and error."""
        dlq_queue = FakeSqsQueue()
        sqs_store = FakeSqsStore({"my-dlq": dlq_queue})
        target = _make_target(target_id="t-42", dead_letter_config=DLQ_CONFIG)
        rule = _make_rule(name="my-rule", targets={"t-42": target})
        exc = RuntimeError("Something went wrong")

        with patch(
            "robotocore.services.sqs.provider._get_store",
            return_value=sqs_store,
        ):
            _send_to_dlq(DLQ_CONFIG, SAMPLE_EVENT, target, rule, exc, REGION, ACCOUNT_ID)

        assert len(dlq_queue.messages) == 1
        msg = dlq_queue.messages[0]

        # Verify message structure
        body = json.loads(msg.body)
        assert body["event"] == SAMPLE_EVENT
        assert body["rule"] == "my-rule"
        assert body["target"] == "t-42"
        assert body["error"] == "Something went wrong"

        # Verify message has an ID and MD5
        assert msg.message_id is not None
        expected_md5 = hashlib.md5(msg.body.encode()).hexdigest()
        assert msg.md5_of_body == expected_md5

    def test_dlq_message_event_is_complete_json(self):
        """The event embedded in DLQ body should be valid JSON and match the original."""
        dlq_queue = FakeSqsQueue()
        sqs_store = FakeSqsStore({"my-dlq": dlq_queue})
        target = _make_target(dead_letter_config=DLQ_CONFIG)
        rule = _make_rule(targets={"target-1": target})
        exc = ValueError("test error")

        with patch(
            "robotocore.services.sqs.provider._get_store",
            return_value=sqs_store,
        ):
            _send_to_dlq(DLQ_CONFIG, SAMPLE_EVENT, target, rule, exc, REGION, ACCOUNT_ID)

        body = json.loads(dlq_queue.messages[0].body)
        assert body["event"]["id"] == "test-event-id"
        assert body["event"]["source"] == "my.app"
        assert body["event"]["detail"] == {"key": "value"}


# ---------------------------------------------------------------------------
# 5. No DLQ configured: target fails, event silently dropped
# ---------------------------------------------------------------------------


class TestNoDlqConfigured:
    def test_failed_target_without_dlq_is_silently_dropped(self):
        """When no DLQ is configured, failed invocations are silently dropped."""
        clear_invocation_log()
        target = _make_target(dead_letter_config=None)
        rule = _make_rule(targets={"target-1": target}, dead_letter_config=None)

        with patch(
            "robotocore.services.events.provider._invoke_target",
            side_effect=RuntimeError("target failed"),
        ):
            # Should not raise
            _dispatch_to_targets(rule, SAMPLE_EVENT, REGION, ACCOUNT_ID)

        # No DLQ invocation should be logged
        log = get_invocation_log()
        dlq_entries = [e for e in log if e["target_type"] == "dlq"]
        assert len(dlq_entries) == 0

    def test_no_dlq_arn_does_nothing(self):
        """_send_to_dlq with empty Arn returns silently."""
        target = _make_target()
        rule = _make_rule()
        exc = RuntimeError("err")

        # Should not raise and should not try to access SQS
        _send_to_dlq({"Arn": ""}, SAMPLE_EVENT, target, rule, exc, REGION, ACCOUNT_ID)

    def test_non_sqs_dlq_arn_does_nothing(self):
        """_send_to_dlq with a non-SQS ARN returns silently."""
        target = _make_target()
        rule = _make_rule()
        exc = RuntimeError("err")

        # SNS ARN instead of SQS -- should be ignored
        _send_to_dlq(
            {"Arn": "arn:aws:sns:us-east-1:123456789012:topic"},
            SAMPLE_EVENT,
            target,
            rule,
            exc,
            REGION,
            ACCOUNT_ID,
        )


# ---------------------------------------------------------------------------
# 6. Multiple targets, one fails: only the failed target's event goes to DLQ
# ---------------------------------------------------------------------------


class TestMultipleTargetsPartialFailure:
    def test_only_failed_target_sends_to_dlq(self):
        """With multiple targets, only the failing one sends to DLQ."""
        dlq_queue = FakeSqsQueue()
        sqs_store = FakeSqsStore({"my-dlq": dlq_queue})

        target_ok = _make_target(
            target_id="target-ok",
            arn=f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:good-queue",
            dead_letter_config=DLQ_CONFIG,
        )
        target_fail = _make_target(
            target_id="target-fail",
            arn="arn:aws:lambda:us-east-1:123456789012:function:broken",
            dead_letter_config=DLQ_CONFIG,
        )
        rule = _make_rule(targets={"target-ok": target_ok, "target-fail": target_fail})

        def selective_invoke(target, event, region, account_id):
            if target.target_id == "target-fail":
                raise RuntimeError("broken function")
            # target-ok succeeds

        with (
            patch(
                "robotocore.services.events.provider._invoke_target",
                side_effect=selective_invoke,
            ),
            patch(
                "robotocore.services.sqs.provider._get_store",
                return_value=sqs_store,
            ),
        ):
            _dispatch_to_targets(rule, SAMPLE_EVENT, REGION, ACCOUNT_ID)

        # Only one DLQ message for the failed target
        assert len(dlq_queue.messages) == 1
        body = json.loads(dlq_queue.messages[0].body)
        assert body["target"] == "target-fail"
        assert "broken function" in body["error"]

    def test_both_targets_fail_both_go_to_dlq(self):
        """When both targets fail, both events go to DLQ."""
        dlq_queue = FakeSqsQueue()
        sqs_store = FakeSqsStore({"my-dlq": dlq_queue})

        target_a = _make_target(
            target_id="target-a",
            arn="arn:aws:lambda:us-east-1:123456789012:function:a",
            dead_letter_config=DLQ_CONFIG,
        )
        target_b = _make_target(
            target_id="target-b",
            arn="arn:aws:lambda:us-east-1:123456789012:function:b",
            dead_letter_config=DLQ_CONFIG,
        )
        rule = _make_rule(targets={"target-a": target_a, "target-b": target_b})

        with (
            patch(
                "robotocore.services.events.provider._invoke_target",
                side_effect=RuntimeError("all broken"),
            ),
            patch(
                "robotocore.services.sqs.provider._get_store",
                return_value=sqs_store,
            ),
        ):
            _dispatch_to_targets(rule, SAMPLE_EVENT, REGION, ACCOUNT_ID)

        assert len(dlq_queue.messages) == 2
        targets_in_dlq = {json.loads(m.body)["target"] for m in dlq_queue.messages}
        assert targets_in_dlq == {"target-a", "target-b"}


# ---------------------------------------------------------------------------
# Rule-level DLQ fallback
# ---------------------------------------------------------------------------


class TestRuleLevelDlqFallback:
    def test_rule_dlq_used_when_target_has_no_dlq(self):
        """When target has no DLQ but rule does, rule-level DLQ is used."""
        dlq_queue = FakeSqsQueue()
        sqs_store = FakeSqsStore({"my-dlq": dlq_queue})
        target = _make_target(dead_letter_config=None)  # no target DLQ
        rule = _make_rule(
            targets={"target-1": target},
            dead_letter_config=DLQ_CONFIG,  # rule-level DLQ
        )

        with (
            patch(
                "robotocore.services.events.provider._invoke_target",
                side_effect=RuntimeError("invocation failed"),
            ),
            patch(
                "robotocore.services.sqs.provider._get_store",
                return_value=sqs_store,
            ),
        ):
            _dispatch_to_targets(rule, SAMPLE_EVENT, REGION, ACCOUNT_ID)

        assert len(dlq_queue.messages) == 1
        body = json.loads(dlq_queue.messages[0].body)
        assert body["rule"] == "test-rule"

    def test_target_dlq_takes_precedence_over_rule_dlq(self):
        """Target-level DLQ should be preferred over rule-level DLQ."""
        target_dlq_arn = f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:target-dlq"
        rule_dlq_arn = f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:rule-dlq"

        target_dlq_queue = FakeSqsQueue()
        rule_dlq_queue = FakeSqsQueue()

        sqs_store = FakeSqsStore(
            {
                "target-dlq": target_dlq_queue,
                "rule-dlq": rule_dlq_queue,
            }
        )

        target = _make_target(dead_letter_config={"Arn": target_dlq_arn})
        rule = _make_rule(
            targets={"target-1": target},
            dead_letter_config={"Arn": rule_dlq_arn},
        )

        with (
            patch(
                "robotocore.services.events.provider._invoke_target",
                side_effect=RuntimeError("failed"),
            ),
            patch(
                "robotocore.services.sqs.provider._get_store",
                return_value=sqs_store,
            ),
        ):
            _dispatch_to_targets(rule, SAMPLE_EVENT, REGION, ACCOUNT_ID)

        # Target DLQ should receive the message (target DLQ is checked first via `or`)
        assert len(target_dlq_queue.messages) == 1
        assert len(rule_dlq_queue.messages) == 0


# ---------------------------------------------------------------------------
# DLQ queue not found
# ---------------------------------------------------------------------------


class TestDlqQueueNotFound:
    def test_dlq_queue_not_found_does_not_raise(self):
        """If the DLQ queue itself doesn't exist, should log error but not crash."""
        sqs_store = FakeSqsStore({})  # no queues
        target = _make_target(dead_letter_config=DLQ_CONFIG)
        rule = _make_rule(targets={"target-1": target})
        exc = RuntimeError("err")

        with patch(
            "robotocore.services.sqs.provider._get_store",
            return_value=sqs_store,
        ):
            # Should not raise
            _send_to_dlq(DLQ_CONFIG, SAMPLE_EVENT, target, rule, exc, REGION, ACCOUNT_ID)
