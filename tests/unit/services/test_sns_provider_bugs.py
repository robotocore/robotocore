"""Tests for correctness bugs found and fixed in the SNS native provider.

Each test documents a specific bug that has been fixed. Do NOT remove these tests.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from robotocore.services.sns.models import SnsStore, SnsSubscription
from robotocore.services.sns.provider import (
    SnsError,
    _deliver_to_sqs,
    _get_topic_attributes,
    _publish,
    _publish_batch,
    _tag_resource,
    _untag_resource,
)

# ===========================================================================
# Bug 1: _deliver_to_sqs hardcoded us-east-1 in SigningCertURL/UnsubscribeURL
# ===========================================================================


class TestSqsNotificationRegion:
    def test_sqs_notification_uses_topic_region_in_signing_cert_url(self):
        """SigningCertURL in SQS notification must reflect the actual region."""
        from robotocore.services.sqs.models import StandardQueue
        from robotocore.services.sqs.provider import _get_store

        sqs_store = _get_store("eu-west-1")
        queue = StandardQueue(
            name="region-test-q",
            region="eu-west-1",
            account_id="123456789012",
        )
        sqs_store.queues["region-test-q"] = queue

        sub = SnsSubscription(
            subscription_arn="arn:aws:sns:eu-west-1:123456789012:topic:sub-1",
            topic_arn="arn:aws:sns:eu-west-1:123456789012:topic",
            protocol="sqs",
            endpoint="arn:aws:sqs:eu-west-1:123456789012:region-test-q",
            owner="123456789012",
            confirmed=True,
            raw_message_delivery=False,
        )

        _deliver_to_sqs(
            sub,
            "hello",
            None,
            {},
            "msg-1",
            "arn:aws:sns:eu-west-1:123456789012:topic",
            "eu-west-1",
        )

        messages = queue.receive(max_messages=1, wait_time_seconds=0)
        assert len(messages) == 1
        body = json.loads(messages[0][0].body)
        assert "eu-west-1" in body["SigningCertURL"], (
            f"Expected eu-west-1 in SigningCertURL but got: {body['SigningCertURL']}"
        )

    def test_sqs_notification_uses_topic_region_in_unsubscribe_url(self):
        """UnsubscribeURL in SQS notification must reflect the actual region."""
        from robotocore.services.sqs.models import StandardQueue
        from robotocore.services.sqs.provider import _get_store

        sqs_store = _get_store("ap-southeast-1")
        queue = StandardQueue(
            name="unsub-url-q",
            region="ap-southeast-1",
            account_id="123456789012",
        )
        sqs_store.queues["unsub-url-q"] = queue

        sub = SnsSubscription(
            subscription_arn="arn:aws:sns:ap-southeast-1:123456789012:topic:sub-2",
            topic_arn="arn:aws:sns:ap-southeast-1:123456789012:topic",
            protocol="sqs",
            endpoint="arn:aws:sqs:ap-southeast-1:123456789012:unsub-url-q",
            owner="123456789012",
            confirmed=True,
            raw_message_delivery=False,
        )

        _deliver_to_sqs(
            sub,
            "test",
            None,
            {},
            "msg-2",
            "arn:aws:sns:ap-southeast-1:123456789012:topic",
            "ap-southeast-1",
        )

        messages = queue.receive(max_messages=1, wait_time_seconds=0)
        assert len(messages) == 1
        body = json.loads(messages[0][0].body)
        assert body["UnsubscribeURL"].startswith("https://sns.ap-southeast-1.amazonaws.com/")


# ===========================================================================
# Bug 2: FIFO topic Publish does not require MessageGroupId
# ===========================================================================


class TestFifoRequiresMessageGroupId:
    def test_publish_to_fifo_without_group_id_should_error(self):
        """Publishing to a FIFO topic without MessageGroupId must raise an error."""
        store = SnsStore()
        store.create_topic(
            "strict.fifo",
            "us-east-1",
            "123",
            {"FifoTopic": "true", "ContentBasedDeduplication": "true"},
        )
        arn = "arn:aws:sns:us-east-1:123:strict.fifo"
        mock_req = MagicMock()

        with pytest.raises(SnsError) as exc_info:
            _publish(
                store,
                {"TopicArn": arn, "Message": "no group id"},
                "us-east-1",
                "123",
                mock_req,
            )
        err = exc_info.value
        assert "MessageGroupId" in err.message or "InvalidParameter" in err.code


# ===========================================================================
# Bug 3: SQS notification includes Subject key when no subject is provided
# ===========================================================================


class TestSqsNotificationSubjectHandling:
    def test_sqs_notification_omits_subject_when_none(self):
        """When no Subject is provided, real AWS omits the Subject key."""
        from robotocore.services.sqs.models import StandardQueue
        from robotocore.services.sqs.provider import _get_store

        sqs_store = _get_store("us-east-1")
        queue = StandardQueue(
            name="subject-test-q",
            region="us-east-1",
            account_id="123456789012",
        )
        sqs_store.queues["subject-test-q"] = queue

        sub = SnsSubscription(
            subscription_arn="arn:aws:sns:us-east-1:123456789012:topic:sub-3",
            topic_arn="arn:aws:sns:us-east-1:123456789012:topic",
            protocol="sqs",
            endpoint="arn:aws:sqs:us-east-1:123456789012:subject-test-q",
            owner="123456789012",
            confirmed=True,
            raw_message_delivery=False,
        )

        _deliver_to_sqs(
            sub,
            "no subject message",
            None,
            {},
            "msg-subj-1",
            "arn:aws:sns:us-east-1:123456789012:topic",
            "us-east-1",
        )

        messages = queue.receive(max_messages=1, wait_time_seconds=0)
        assert len(messages) == 1
        body = json.loads(messages[0][0].body)
        assert "Subject" not in body


# ===========================================================================
# Bug 4: _tag_resource / _untag_resource silently succeed for nonexistent topics
# ===========================================================================


class TestTagNonExistentResource:
    def test_tag_nonexistent_resource_should_error(self):
        store = SnsStore()
        mock_req = MagicMock()
        with pytest.raises(SnsError) as exc_info:
            _tag_resource(
                store,
                {
                    "ResourceArn": "arn:aws:sns:us-east-1:123:nonexistent",
                    "Tags": [{"Key": "k", "Value": "v"}],
                },
                "us-east-1",
                "123",
                mock_req,
            )
        assert "NotFound" in exc_info.value.code or "Resource" in exc_info.value.code

    def test_untag_nonexistent_resource_should_error(self):
        store = SnsStore()
        mock_req = MagicMock()
        with pytest.raises(SnsError) as exc_info:
            _untag_resource(
                store,
                {"ResourceArn": "arn:aws:sns:us-east-1:123:nonexistent", "TagKeys": ["k"]},
                "us-east-1",
                "123",
                mock_req,
            )
        assert "NotFound" in exc_info.value.code or "Resource" in exc_info.value.code


# ===========================================================================
# Bug 5: _publish_batch does not validate max 10 entries
# ===========================================================================


class TestPublishBatchMaxEntries:
    def test_publish_batch_rejects_more_than_10_entries(self):
        store = SnsStore()
        store.create_topic("batch-limit", "us-east-1", "123")
        arn = "arn:aws:sns:us-east-1:123:batch-limit"
        mock_req = MagicMock()

        params = {"TopicArn": arn}
        for i in range(1, 12):  # 11 entries
            params[f"PublishBatchRequestEntries.member.{i}.Id"] = f"msg{i}"
            params[f"PublishBatchRequestEntries.member.{i}.Message"] = f"message {i}"

        with pytest.raises(SnsError) as exc_info:
            _publish_batch(store, params, "us-east-1", "123", mock_req)
        assert "TooMany" in exc_info.value.code


# ===========================================================================
# Bug 6: _publish_batch does not validate duplicate entry IDs
# ===========================================================================


class TestPublishBatchDuplicateIds:
    def test_publish_batch_rejects_duplicate_ids(self):
        store = SnsStore()
        store.create_topic("dup-id-topic", "us-east-1", "123")
        arn = "arn:aws:sns:us-east-1:123:dup-id-topic"
        mock_req = MagicMock()

        params = {
            "TopicArn": arn,
            "PublishBatchRequestEntries.member.1.Id": "same-id",
            "PublishBatchRequestEntries.member.1.Message": "first",
            "PublishBatchRequestEntries.member.2.Id": "same-id",
            "PublishBatchRequestEntries.member.2.Message": "second",
        }

        with pytest.raises(SnsError) as exc_info:
            _publish_batch(store, params, "us-east-1", "123", mock_req)
        assert "Distinct" in exc_info.value.code


# ===========================================================================
# Bug 7: _get_topic_attributes computed fields overwritten by topic.attributes
# ===========================================================================


class TestGetTopicAttributesOverwrite:
    def test_subscriptions_confirmed_reflects_actual_count(self):
        store = SnsStore()
        store.create_topic("t", "us-east-1", "123")
        arn = "arn:aws:sns:us-east-1:123:t"
        topic = store.get_topic(arn)

        # Simulate a stale attribute
        topic.attributes["SubscriptionsConfirmed"] = "999"

        # Add a real subscription
        store.subscribe(arn, "sqs", "arn:aws:sqs:us-east-1:123:q")

        mock_req = MagicMock()
        result = _get_topic_attributes(store, {"TopicArn": arn}, "us-east-1", "123", mock_req)

        assert result["Attributes"]["SubscriptionsConfirmed"] == "1"
