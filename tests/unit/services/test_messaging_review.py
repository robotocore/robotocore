"""Failing tests for EventBridge, SNS, and SES edge cases.

Each test documents correct AWS behavior that the current implementation
gets wrong. These tests should all FAIL until the underlying bugs are fixed.
"""

import json

import pytest

from robotocore.services.events.models import (
    EventsStore,
    _match_pattern,
)
from robotocore.services.events.provider import (
    _apply_input_transformer,
    _matches_pattern,
)
from robotocore.services.sns.models import (
    SnsStore,
    SnsSubscription,
    _matches_filter_value,
)
from robotocore.services.sns.provider import (
    _publish,
)
from robotocore.services.sqs.models import SqsStore

# =============================================================================
# EventBridge: TestEventPattern vs PutEvents pattern matching inconsistency
# =============================================================================


class TestEventBridgePatternMatchingInconsistency:
    """The provider has TWO pattern matching functions:
    - _match_pattern (in models.py) - used by PutEvents, supports complex filters
    - _matches_pattern (in provider.py) - used by TestEventPattern, only supports simple matching

    This means TestEventPattern returns wrong results for prefix, suffix, numeric,
    exists, and anything-but filters.
    """

    def test_test_event_pattern_with_prefix_filter(self):
        """TestEventPattern should support prefix matching.
        Correct behavior: {"source": [{"prefix": "aws."}]} matches {"source": "aws.ec2"}
        Bug: _matches_pattern in provider.py doesn't handle dict matchers in lists.
        """
        pattern = {"source": [{"prefix": "aws."}]}
        event = {"source": "aws.ec2"}
        # _matches_pattern (used by TestEventPattern) should return True
        assert _matches_pattern(pattern, event) is True

    def test_test_event_pattern_with_anything_but(self):
        """TestEventPattern should support anything-but matching.
        Correct: {"source": [{"anything-but": ["aws.health"]}]} matches {"source": "aws.ec2"}
        Bug: _matches_pattern doesn't handle anything-but dicts in value lists.
        """
        pattern = {"source": [{"anything-but": ["aws.health"]}]}
        event = {"source": "aws.ec2"}
        assert _matches_pattern(pattern, event) is True

    def test_test_event_pattern_with_numeric_filter(self):
        """TestEventPattern should support numeric matching.
        Correct: {"detail": {"count": [{"numeric": [">=", 5]}]}} matches {"detail": {"count": 10}}
        Bug: _matches_pattern doesn't handle numeric matchers.
        """
        pattern = {"detail": {"count": [{"numeric": [">=", 5]}]}}
        event = {"detail": {"count": 10}}
        assert _matches_pattern(pattern, event) is True

    def test_test_event_pattern_with_exists_true(self):
        """TestEventPattern should support exists:true matching.
        Correct: {"detail": {"user-id": [{"exists": true}]}} matches when key present.
        Bug: _matches_pattern doesn't handle exists matchers.
        """
        pattern = {"detail": {"user-id": [{"exists": True}]}}
        event = {"detail": {"user-id": "abc123"}}
        assert _matches_pattern(pattern, event) is True

    def test_test_event_pattern_with_exists_false(self):
        """TestEventPattern should support exists:false matching.
        Correct: {"detail": {"error-code": [{"exists": false}]}} matches when key is absent.
        Bug: _matches_pattern doesn't handle exists matchers.
        """
        pattern = {"detail": {"error-code": [{"exists": False}]}}
        event = {"detail": {"user-id": "abc123"}}
        assert _matches_pattern(pattern, event) is True

    def test_test_event_pattern_with_suffix_filter(self):
        """TestEventPattern should support suffix matching.
        Correct: {"source": [{"suffix": ".ec2"}]} matches {"source": "aws.ec2"}
        Bug: _matches_pattern doesn't handle suffix matchers.
        """
        pattern = {"source": [{"suffix": ".ec2"}]}
        event = {"source": "aws.ec2"}
        assert _matches_pattern(pattern, event) is True


# =============================================================================
# EventBridge: PutEvents partial failure reporting
# =============================================================================


class TestEventBridgePutEventsPartialFailures:
    """PutEvents should report partial failures when an event targets
    a non-existent bus. Currently FailedEntryCount is always 0."""

    def test_put_events_with_nonexistent_bus_reports_failure(self):
        """When an event specifies a non-existent EventBusName, that entry
        should be reported as failed in the response.
        Correct behavior: FailedEntryCount > 0 and the entry has an ErrorCode.
        Bug: Current code always returns FailedEntryCount: 0.
        """
        store = EventsStore()
        store.ensure_default_bus("us-east-1", "123456789012")
        from robotocore.services.events.provider import _put_events

        result = _put_events(
            store,
            {
                "Entries": [
                    {
                        "Source": "test",
                        "DetailType": "test",
                        "Detail": "{}",
                        "EventBusName": "nonexistent-bus",
                    }
                ]
            },
            "us-east-1",
            "123456789012",
        )
        # AWS would report this as a failed entry
        assert result["FailedEntryCount"] == 1
        assert result["Entries"][0].get("ErrorCode") is not None


# =============================================================================
# SNS: Filter policy with MessageBody scope
# =============================================================================


class TestSnsFilterPolicyMessageBodyScope:
    """When FilterPolicyScope is 'MessageBody', the filter policy should be
    applied to the parsed message body, not to message attributes.
    Currently the scope field is stored but completely ignored."""

    def test_filter_policy_on_message_body(self):
        """A subscription with FilterPolicyScope=MessageBody should filter
        based on message content, not message attributes.
        Bug: matches_filter always uses message_attributes regardless of scope.
        """
        sub = SnsSubscription(
            subscription_arn="arn:aws:sns:us-east-1:123456789012:topic:sub1",
            topic_arn="arn:aws:sns:us-east-1:123456789012:topic",
            protocol="sqs",
            endpoint="arn:aws:sqs:us-east-1:123456789012:queue",
            filter_policy={"status": ["active"]},
            filter_policy_scope="MessageBody",
        )
        # Message body contains {"status": "active"} but no message attributes
        # With MessageBody scope, this should match because the body matches
        # But the current code only checks message_attributes (which is empty)
        message_body = json.dumps({"status": "active"})  # noqa: F841
        message_attributes = {}

        # The subscription should match based on the body, not attributes
        # Current behavior: returns False because it only checks message_attributes
        # Correct behavior: parse message_body as JSON and match against it
        # We'd need a different method signature, but let's verify the sub
        # at least stores the scope correctly and it affects matching
        assert sub.filter_policy_scope == "MessageBody"
        # This test verifies the fundamental bug: matches_filter ignores scope
        # and always checks message_attributes
        assert sub.matches_filter(message_attributes) is True


class TestSnsFilterPolicyAdvanced:
    """Advanced filter policy matching edge cases."""

    def test_filter_policy_suffix_matching(self):
        """SNS filter policies support suffix matching (added in 2022).
        Correct: {"email": [{"suffix": "@example.com"}]} matches
        {"email": {"DataType": "String", "StringValue": "user@example.com"}}
        Bug: _matches_filter_value doesn't handle "suffix" key.
        """
        rule = {"suffix": "@example.com"}
        assert _matches_filter_value(rule, "user@example.com") is True


# =============================================================================
# SNS: FIFO topic deduplication returns wrong message ID
# =============================================================================


class TestSnsFifoDeduplication:
    """FIFO deduplication edge cases."""

    def test_fifo_publish_without_dedup_id_and_no_content_based_dedup(self):
        """On a FIFO topic without ContentBasedDeduplication, publishing
        without a MessageDeduplicationId should raise an error.
        Correct behavior: InvalidParameter error.
        Bug: Currently the code only checks for MessageGroupId but doesn't
        validate that MessageDeduplicationId is provided when
        ContentBasedDeduplication is not enabled.
        """
        store = SnsStore()
        topic = store.create_topic(
            "test-topic.fifo",
            "us-east-1",
            "123456789012",
            {"FifoTopic": "true"},
        )
        # Manually add a confirmed SQS subscription so publish works
        store.subscribe(
            topic.arn,
            "sqs",
            "arn:aws:sqs:us-east-1:123456789012:test-queue",
        )

        # Build minimal params -- has MessageGroupId but no MessageDeduplicationId
        # and topic has ContentBasedDeduplication=false
        params = {
            "TopicArn": topic.arn,
            "Message": "test message",
            "MessageGroupId": "group1",
        }

        # AWS would reject this with InvalidParameter since neither
        # MessageDeduplicationId nor ContentBasedDeduplication is set
        from robotocore.services.sns.provider import SnsError

        with pytest.raises(SnsError) as exc_info:
            _publish(store, params, "us-east-1", "123456789012", None)
        assert "MessageDeduplicationId" in exc_info.value.message


# =============================================================================
# SNS: Message structure with per-protocol messages
# =============================================================================


class TestSnsMessageStructure:
    """When MessageStructure is 'json', the Message field should be parsed
    as JSON with per-protocol messages."""

    def test_publish_with_json_message_structure(self):
        """When MessageStructure=json, the message body should be a JSON object
        with keys for each protocol (e.g., 'default', 'sqs', 'lambda').
        The subscriber should receive only the message for their protocol.
        Bug: _publish doesn't check or handle MessageStructure parameter at all.
        """
        sns_store = SnsStore()
        sqs_store = SqsStore()
        topic = sns_store.create_topic("test-topic", "us-east-1", "123456789012")

        # Create an SQS queue
        sqs_store.create_queue("test-queue", "us-east-1", "123456789012")

        # Subscribe with SQS protocol
        sub = sns_store.subscribe(
            topic.arn,
            "sqs",
            "arn:aws:sqs:us-east-1:123456789012:test-queue",
        )
        sub.raw_message_delivery = True

        # Publish with MessageStructure=json
        message = json.dumps(
            {
                "default": "default message",
                "sqs": "sqs-specific message",
            }
        )

        params = {
            "TopicArn": topic.arn,
            "Message": message,
            "MessageStructure": "json",
        }

        # Monkeypatch the SQS store
        from unittest.mock import patch

        with patch(
            "robotocore.services.sns.provider.get_sqs_store",
            return_value=sqs_store,
        ):
            _publish(sns_store, params, "us-east-1", "123456789012", None)

        # The SQS subscriber should receive "sqs-specific message", not the full JSON
        queue = sqs_store.get_queue("test-queue")
        msg = queue.receive(max_messages=1)
        assert len(msg) == 1
        # receive() returns list of (SqsMessage, receipt_handle) tuples
        # With raw delivery, the body should be the protocol-specific message
        assert msg[0][0].body == "sqs-specific message"


# =============================================================================
# SNS: Raw message delivery should NOT include SNS metadata
# =============================================================================


class TestSnsRawMessageDelivery:
    """When RawMessageDelivery is true, the message body sent to SQS should be
    the raw message, not wrapped in SNS notification JSON."""

    def test_raw_delivery_preserves_exact_message(self):
        """With raw delivery enabled, the SQS message body should be exactly
        the original message, not JSON-wrapped."""
        sns_store = SnsStore()
        sqs_store = SqsStore()

        topic = sns_store.create_topic("test-topic", "us-east-1", "123456789012")
        sqs_store.create_queue("raw-queue", "us-east-1", "123456789012")

        sub = sns_store.subscribe(
            topic.arn,
            "sqs",
            "arn:aws:sqs:us-east-1:123456789012:raw-queue",
        )
        sub.raw_message_delivery = True

        original_message = "Hello, World!"
        params = {
            "TopicArn": topic.arn,
            "Message": original_message,
        }

        from unittest.mock import patch

        with patch(
            "robotocore.services.sns.provider.get_sqs_store",
            return_value=sqs_store,
        ):
            _publish(sns_store, params, "us-east-1", "123456789012", None)

        queue = sqs_store.get_queue("raw-queue")
        msgs = queue.receive(max_messages=1)
        assert len(msgs) == 1
        # receive() returns list of (SqsMessage, receipt_handle) tuples
        # Raw delivery: body should be exactly the original message
        assert msgs[0][0].body == original_message


# =============================================================================
# EventBridge: InputTransformer with missing keys
# =============================================================================


class TestEventBridgeInputTransformer:
    """Edge cases in the InputTransformer implementation."""

    def test_input_transformer_with_missing_path_resolves_to_empty_string(self):
        """When an InputPathsMap key references a path that doesn't exist in the event,
        AWS replaces the placeholder with an empty string (or null for JSON templates).
        Bug: _resolve_jsonpath returns "null" for missing paths, which gets inserted
        literally into non-JSON templates.
        """
        transformer = {
            "InputPathsMap": {"instance": "$.detail.instance-id"},
            "InputTemplate": "Instance <instance> was terminated",
        }
        event = {"detail": {}}  # no instance-id
        result = _apply_input_transformer(transformer, event)
        # AWS would produce: "Instance  was terminated" (empty string, not "null")
        assert result == "Instance  was terminated"

    def test_input_transformer_with_whole_event_reference(self):
        """<aws.events.event> should resolve to the whole event JSON."""
        transformer = {
            "InputPathsMap": {},
            "InputTemplate": "<aws.events.event>",
        }
        event = {"source": "aws.ec2", "detail": {"key": "value"}}
        result = _apply_input_transformer(transformer, event)
        # The <aws.events.event> placeholder should not be replaced (no such key in paths_map)
        # AWS has special built-in variables like <aws.events.event>
        # Bug: These special variables are not handled at all
        parsed = json.loads(result)
        assert parsed["source"] == "aws.ec2"


# =============================================================================
# EventBridge: PutRule should validate event pattern JSON
# =============================================================================


class TestEventBridgePutRuleValidation:
    """PutRule should validate the event pattern."""

    def test_put_rule_rejects_invalid_event_pattern(self):
        """PutRule with invalid event pattern JSON should raise an error.
        Bug: The provider parses the pattern with json.loads but doesn't validate
        the structure (e.g., top-level values must be arrays or objects).
        """
        store = EventsStore()
        store.ensure_default_bus("us-east-1", "123456789012")

        # Event pattern with a string value instead of array at top level
        # AWS rejects this: "Event pattern is not valid"
        from robotocore.services.events.provider import EventsError, _put_rule

        with pytest.raises(EventsError):
            _put_rule(
                store,
                {
                    "Name": "bad-pattern-rule",
                    "EventPattern": json.dumps({"source": "aws.ec2"}),  # should be ["aws.ec2"]
                },
                "us-east-1",
                "123456789012",
            )


# =============================================================================
# EventBridge: Rule with both pattern and schedule
# =============================================================================


class TestEventBridgeRuleWithPatternAndSchedule:
    """A rule cannot have both EventPattern and ScheduleExpression.
    AWS rejects this, but our implementation allows it."""

    def test_put_rule_rejects_both_pattern_and_schedule(self):
        """PutRule with both EventPattern and ScheduleExpression should raise.
        Bug: Current code allows setting both.
        """
        from robotocore.services.events.provider import EventsError, _put_rule

        store = EventsStore()
        store.ensure_default_bus("us-east-1", "123456789012")

        with pytest.raises(EventsError):
            _put_rule(
                store,
                {
                    "Name": "dual-rule",
                    "EventPattern": json.dumps({"source": ["test"]}),
                    "ScheduleExpression": "rate(5 minutes)",
                },
                "us-east-1",
                "123456789012",
            )


# =============================================================================
# SNS: Publish batch with > 10 entries (JSON protocol)
# =============================================================================


class TestSnsPublishBatchValidation:
    """PublishBatch edge cases."""

    def test_publish_batch_validates_empty_entries(self):
        """PublishBatch with no entries should raise an error.
        AWS rejects empty batch requests.
        Bug: Current code processes an empty list without error.
        """
        from robotocore.services.sns.provider import SnsError, _publish_batch

        store = SnsStore()
        topic = store.create_topic("test-topic", "us-east-1", "123456789012")

        with pytest.raises(SnsError):
            _publish_batch(
                store,
                {"TopicArn": topic.arn, "PublishBatchRequestEntries": []},
                "us-east-1",
                "123456789012",
                None,
            )


# =============================================================================
# EventBridge: Delete rule with existing targets should fail
# =============================================================================


class TestEventBridgeDeleteRuleWithTargets:
    """Deleting a rule that still has targets should fail.
    AWS returns: 'Rule can not be deleted since it has targets.'"""

    def test_delete_rule_with_targets_raises_error(self):
        """DeleteRule should fail when the rule has targets attached.
        Bug: Current code deletes the rule regardless of targets.
        """
        from robotocore.services.events.provider import EventsError, _delete_rule, _put_targets

        store = EventsStore()
        store.ensure_default_bus("us-east-1", "123456789012")

        store.put_rule(
            "test-rule",
            "default",
            "us-east-1",
            "123456789012",
            event_pattern={"source": ["test"]},
        )

        _put_targets(
            store,
            {
                "Rule": "test-rule",
                "Targets": [
                    {
                        "Id": "target-1",
                        "Arn": "arn:aws:sqs:us-east-1:123456789012:queue",
                    }
                ],
            },
            "us-east-1",
            "123456789012",
        )

        with pytest.raises(EventsError) as exc_info:
            _delete_rule(
                store,
                {"Name": "test-rule"},
                "us-east-1",
                "123456789012",
            )
        assert "targets" in exc_info.value.message.lower()


# =============================================================================
# EventBridge: PutTargets limit validation
# =============================================================================


class TestEventBridgePutTargetsValidation:
    """PutTargets should enforce the 5-target-per-rule limit."""

    def test_put_targets_rejects_more_than_five(self):
        """A rule can have at most 5 targets. PutTargets should reject
        requests that would exceed this limit.
        Bug: Current code doesn't enforce any target limit.
        """
        from robotocore.services.events.provider import _put_targets

        store = EventsStore()
        store.ensure_default_bus("us-east-1", "123456789012")

        store.put_rule(
            "test-rule",
            "default",
            "us-east-1",
            "123456789012",
            event_pattern={"source": ["test"]},
        )

        targets = [
            {"Id": f"target-{i}", "Arn": f"arn:aws:sqs:us-east-1:123456789012:q{i}"}
            for i in range(6)
        ]

        result = _put_targets(
            store,
            {"Rule": "test-rule", "Targets": targets},
            "us-east-1",
            "123456789012",
        )
        # AWS would report at least 1 failed entry for exceeding the limit
        assert result["FailedEntryCount"] > 0


# =============================================================================
# SNS: Topic attribute EffectiveDeliveryPolicy
# =============================================================================


class TestSnsTopicAttributes:
    """Topic attributes edge cases."""

    def test_get_topic_attributes_includes_effective_delivery_policy(self):
        """GetTopicAttributes should include EffectiveDeliveryPolicy.
        AWS always returns this attribute with default retry policy.
        Bug: Current code doesn't include EffectiveDeliveryPolicy.
        """
        store = SnsStore()
        topic = store.create_topic("test-topic", "us-east-1", "123456789012")

        from robotocore.services.sns.provider import _get_topic_attributes

        result = _get_topic_attributes(
            store,
            {"TopicArn": topic.arn},
            "us-east-1",
            "123456789012",
            None,
        )
        attrs = result["Attributes"]
        assert "EffectiveDeliveryPolicy" in attrs


# =============================================================================
# EventBridge: CreateEventBus with duplicate name should error
# =============================================================================


class TestEventBridgeCreateEventBusDuplicate:
    """Creating an event bus with a name that already exists should fail."""

    def test_create_duplicate_event_bus_raises_error(self):
        """CreateEventBus with an existing name should raise ResourceAlreadyExistsException.
        Bug: Current code silently overwrites the existing bus.
        """
        from robotocore.services.events.provider import EventsError, _create_event_bus

        store = EventsStore()
        store.ensure_default_bus("us-east-1", "123456789012")

        _create_event_bus(
            store,
            {"Name": "custom-bus"},
            "us-east-1",
            "123456789012",
        )

        with pytest.raises(EventsError) as exc_info:
            _create_event_bus(
                store,
                {"Name": "custom-bus"},
                "us-east-1",
                "123456789012",
            )
        assert "AlreadyExists" in exc_info.value.code


# =============================================================================
# SNS: CreateTopic with identical attributes should return existing topic
# but different attributes should raise
# =============================================================================


class TestSnsCreateTopicIdempotency:
    """CreateTopic idempotency with attribute mismatch."""

    def test_create_topic_with_different_attributes_raises(self):
        """Creating a topic with the same name but different attributes should error.
        AWS raises InvalidParameter when attributes differ.
        Bug: Current code always returns the existing topic without checking attributes.
        """
        store = SnsStore()
        store.create_topic(
            "test-topic",
            "us-east-1",
            "123456789012",
            {"DisplayName": "Original"},
        )

        # Creating with different attributes should raise

        # The store just returns the existing topic -- it should validate
        topic2 = store.create_topic(
            "test-topic",
            "us-east-1",
            "123456789012",
            {"DisplayName": "Different"},
        )
        # Bug: topic2 has the original attributes, not the new ones
        # AWS would raise an error for conflicting attributes
        assert topic2.attributes.get("DisplayName") == "Different" or pytest.fail(
            "CreateTopic with different attributes should either raise or update the topic"
        )


# =============================================================================
# EventBridge: $or pattern matching (compound event patterns)
# =============================================================================


class TestEventBridgeOrPatterns:
    """EventBridge supports $or patterns for compound matching.
    E.g., {"$or": [{"source": ["aws.ec2"]}, {"source": ["aws.s3"]}]}
    """

    def test_or_pattern_matches_either_condition(self):
        """$or pattern should match if any sub-pattern matches.
        Bug: _match_pattern doesn't handle $or key at all.
        """
        pattern = {"$or": [{"source": ["aws.ec2"]}, {"source": ["aws.s3"]}]}
        event_ec2 = {"source": "aws.ec2", "detail-type": "test", "detail": {}}
        event_s3 = {"source": "aws.s3", "detail-type": "test", "detail": {}}
        event_rds = {"source": "aws.rds", "detail-type": "test", "detail": {}}

        assert _match_pattern(pattern, event_ec2) is True
        assert _match_pattern(pattern, event_s3) is True
        assert _match_pattern(pattern, event_rds) is False
