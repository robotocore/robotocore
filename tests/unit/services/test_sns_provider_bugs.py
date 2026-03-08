"""Failing tests exposing bugs in the SNS native provider.

Each test targets a specific bug and is expected to FAIL until the bug is fixed.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from robotocore.services.sns.models import SnsStore, SnsSubscription
from robotocore.services.sns.provider import (
    SnsError,
    _deliver_to_sqs,
    _get_topic_attributes,
    _publish,
    _publish_batch,
    _set_subscription_attributes,
    _tag_resource,
    _untag_resource,
)

# ---------------------------------------------------------------------------
# Bug 1: _deliver_to_sqs hardcodes us-east-1 in SigningCertURL / UnsubscribeURL
# The notification JSON should use the topic's actual region, not a hardcoded
# "us-east-1". Compare with _deliver_to_lambda and _deliver_to_http which
# correctly use the `region` parameter.
# ---------------------------------------------------------------------------


class TestSqsNotificationRegionBug:
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
        # The URL should contain eu-west-1, not us-east-1
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
        # Check the hostname specifically -- the region in the subscription ARN
        # embedded in the URL is not sufficient; the hostname must be correct.
        assert body["UnsubscribeURL"].startswith("https://sns.ap-southeast-1.amazonaws.com/"), (
            f"Expected UnsubscribeURL hostname to use ap-southeast-1 but got: "
            f"{body['UnsubscribeURL']}"
        )


# ---------------------------------------------------------------------------
# Bug 2: filter_policy_scope="MessageBody" is ignored -- filter is always
# applied against message_attributes, never against the message body.
# ---------------------------------------------------------------------------


class TestFilterPolicyScopeMessageBody:
    def test_filter_policy_scope_message_body_matches_body_content(self):
        """When FilterPolicyScope is MessageBody, filter should match against
        the JSON-parsed message body, not against MessageAttributes."""
        store = SnsStore()
        store.create_topic("t", "us-east-1", "123")
        arn = "arn:aws:sns:us-east-1:123:t"

        # Subscribe with MessageBody scope
        sub = store.subscribe(arn, "sqs", "arn:aws:sqs:us-east-1:123:q")
        mock_req = MagicMock()
        _set_subscription_attributes(
            store,
            {
                "SubscriptionArn": sub.subscription_arn,
                "AttributeName": "FilterPolicy",
                "AttributeValue": json.dumps({"status": ["active"]}),
            },
            "us-east-1",
            "123",
            mock_req,
        )
        _set_subscription_attributes(
            store,
            {
                "SubscriptionArn": sub.subscription_arn,
                "AttributeName": "FilterPolicyScope",
                "AttributeValue": "MessageBody",
            },
            "us-east-1",
            "123",
            mock_req,
        )

        assert sub.filter_policy_scope == "MessageBody"
        assert sub.filter_policy == {"status": ["active"]}

        # The message body contains {"status": "active"} so it should match
        # but MessageAttributes is empty, so if scope is wrongly applied to
        # attributes, the filter will reject the message.
        message_body = json.dumps({"status": "active", "data": "payload"})

        # matches_filter should return True when scope is MessageBody and the
        # body contains matching fields. Currently it only checks message_attributes.
        # We need to call matches_filter with the message body parsed as the
        # "attributes" when scope is MessageBody.
        #
        # The bug is in _publish: it always passes message_attributes to
        # sub.matches_filter(), ignoring filter_policy_scope entirely.
        # With MessageBody scope and empty message_attributes, the filter
        # should look at the message body instead, and match.
        #
        # Test by publishing and checking that delivery occurs:
        with patch("robotocore.services.sns.provider._deliver_to_sqs") as mock_deliver:
            _publish(
                store,
                {
                    "TopicArn": arn,
                    "Message": message_body,
                    # No MessageAttributes -- filter must use body
                },
                "us-east-1",
                "123",
                mock_req,
            )
            # The subscriber has FilterPolicyScope=MessageBody and the message
            # body matches the filter. Delivery SHOULD happen.
            (
                mock_deliver.assert_called_once(),
                (
                    "Expected delivery to SQS subscriber with MessageBody filter scope, "
                    "but _deliver_to_sqs was not called"
                ),
            )


# ---------------------------------------------------------------------------
# Bug 3: _publish_batch does not check FIFO deduplication
# ---------------------------------------------------------------------------


class TestPublishBatchFifoDedup:
    def test_publish_batch_deduplicates_fifo_messages(self):
        """PublishBatch on a FIFO topic should deduplicate messages
        with the same MessageDeduplicationId, just like Publish does."""
        store = SnsStore()
        store.create_topic(
            "batch.fifo",
            "us-east-1",
            "123",
            {"FifoTopic": "true"},
        )
        arn = "arn:aws:sns:us-east-1:123:batch.fifo"
        mock_req = MagicMock()

        # First batch: publish with dedup-id "d1"
        result1 = _publish_batch(
            store,
            {
                "TopicArn": arn,
                "PublishBatchRequestEntries.member.1.Id": "msg1",
                "PublishBatchRequestEntries.member.1.Message": "hello",
                "PublishBatchRequestEntries.member.1.MessageDeduplicationId": "d1",
                "PublishBatchRequestEntries.member.1.MessageGroupId": "g1",
            },
            "us-east-1",
            "123",
            mock_req,
        )
        assert len(result1["Successful"]) == 1

        # Second batch: same dedup-id "d1" should be rejected as duplicate
        # (or at minimum, the batch should report it in Failed, not Successful)
        _publish_batch(
            store,
            {
                "TopicArn": arn,
                "PublishBatchRequestEntries.member.1.Id": "msg2",
                "PublishBatchRequestEntries.member.1.Message": "hello again",
                "PublishBatchRequestEntries.member.1.MessageDeduplicationId": "d1",
                "PublishBatchRequestEntries.member.1.MessageGroupId": "g1",
            },
            "us-east-1",
            "123",
            mock_req,
        )
        # The duplicate message should NOT be in Successful
        # AWS would still return MessageId for dedup'd messages (idempotent)
        # but should not deliver to subscribers. Let's verify no delivery:
        # Add a subscriber to verify delivery doesn't happen for dupes
        sub = store.subscribe(arn, "sqs", "arn:aws:sqs:us-east-1:123:q")
        sub.confirmed = True

        with patch("robotocore.services.sns.provider._deliver_to_sqs") as mock_deliver:
            _publish_batch(
                store,
                {
                    "TopicArn": arn,
                    "PublishBatchRequestEntries.member.1.Id": "msg3",
                    "PublishBatchRequestEntries.member.1.Message": "dedup test",
                    "PublishBatchRequestEntries.member.1.MessageDeduplicationId": "d1",
                    "PublishBatchRequestEntries.member.1.MessageGroupId": "g1",
                },
                "us-east-1",
                "123",
                mock_req,
            )
            # Should NOT deliver because d1 is already dedup'd
            (
                mock_deliver.assert_not_called(),
                ("Expected no delivery for deduplicated FIFO message in PublishBatch"),
            )


# ---------------------------------------------------------------------------
# Bug 4: FIFO topic Publish does not require MessageGroupId
# AWS returns InvalidParameter when MessageGroupId is missing for FIFO topics.
# ---------------------------------------------------------------------------


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
                {
                    "TopicArn": arn,
                    "Message": "no group id",
                    # MessageGroupId deliberately omitted
                },
                "us-east-1",
                "123",
                mock_req,
            )
        err = exc_info.value
        assert "MessageGroupId" in err.message or "InvalidParameter" in err.code


# ---------------------------------------------------------------------------
# Bug 5: SQS notification omits Subject when it is None instead of including
# the key. Actually the bug is the reverse: real AWS OMITS the Subject key
# when no subject is provided, but the code always includes it as "".
# ---------------------------------------------------------------------------


class TestSqsNotificationSubjectHandling:
    def test_sqs_notification_omits_subject_when_none(self):
        """When no Subject is provided, real AWS omits the Subject key
        from the notification JSON. The provider includes it as empty string."""
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
            None,  # No subject
            {},
            "msg-subj-1",
            "arn:aws:sns:us-east-1:123456789012:topic",
            "us-east-1",
        )

        messages = queue.receive(max_messages=1, wait_time_seconds=0)
        assert len(messages) == 1
        body = json.loads(messages[0][0].body)
        # Real AWS does not include Subject key when no subject is provided
        assert "Subject" not in body, (
            f"Subject key should be absent when no subject provided, "
            f"but found Subject={body.get('Subject')!r}"
        )


# ---------------------------------------------------------------------------
# Bug 6: _tag_resource and _untag_resource silently succeed for non-existent
# resources. AWS returns ResourceNotFoundException.
# ---------------------------------------------------------------------------


class TestTagNonExistentResource:
    def test_tag_nonexistent_resource_should_error(self):
        """TagResource on a non-existent topic ARN should raise an error."""
        store = SnsStore()
        mock_req = MagicMock()
        with pytest.raises(SnsError) as exc_info:
            _tag_resource(
                store,
                {
                    "ResourceArn": "arn:aws:sns:us-east-1:123:nonexistent-topic",
                    "Tags": [{"Key": "k", "Value": "v"}],
                },
                "us-east-1",
                "123",
                mock_req,
            )
        assert exc_info.value.code in ("NotFound", "ResourceNotFound", "ResourceNotFoundException")

    def test_untag_nonexistent_resource_should_error(self):
        """UntagResource on a non-existent topic ARN should raise an error."""
        store = SnsStore()
        mock_req = MagicMock()
        with pytest.raises(SnsError) as exc_info:
            _untag_resource(
                store,
                {
                    "ResourceArn": "arn:aws:sns:us-east-1:123:nonexistent-topic",
                    "TagKeys": ["k"],
                },
                "us-east-1",
                "123",
                mock_req,
            )
        assert exc_info.value.code in ("NotFound", "ResourceNotFound", "ResourceNotFoundException")


# ---------------------------------------------------------------------------
# Bug 7: _publish_batch does not validate max 10 entries
# AWS returns TooManyEntriesInBatchRequest if more than 10 entries.
# ---------------------------------------------------------------------------


class TestPublishBatchMaxEntries:
    def test_publish_batch_rejects_more_than_10_entries(self):
        """AWS limits PublishBatch to 10 entries. The provider should reject >10."""
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
        assert "TooMany" in exc_info.value.code or "BatchEntriesTooLong" in exc_info.value.code


# ---------------------------------------------------------------------------
# Bug 8: _publish_batch does not validate duplicate entry IDs
# AWS returns BatchEntryIdsNotDistinct if IDs are not unique.
# ---------------------------------------------------------------------------


class TestPublishBatchDuplicateIds:
    def test_publish_batch_rejects_duplicate_ids(self):
        """AWS rejects batch entries with duplicate Id values."""
        store = SnsStore()
        store.create_topic("dup-id-topic", "us-east-1", "123")
        arn = "arn:aws:sns:us-east-1:123:dup-id-topic"
        mock_req = MagicMock()

        params = {
            "TopicArn": arn,
            "PublishBatchRequestEntries.member.1.Id": "same-id",
            "PublishBatchRequestEntries.member.1.Message": "first",
            "PublishBatchRequestEntries.member.2.Id": "same-id",  # Duplicate!
            "PublishBatchRequestEntries.member.2.Message": "second",
        }

        with pytest.raises(SnsError) as exc_info:
            _publish_batch(store, params, "us-east-1", "123", mock_req)
        err = exc_info.value
        assert "Distinct" in err.code or "BatchEntryIdsNotDistinct" in err.code


# ---------------------------------------------------------------------------
# Bug 9: _get_topic_attributes computed fields can be overwritten by
# topic.attributes via attrs.update(). If someone stores a raw attribute
# with key "SubscriptionsConfirmed", it will overwrite the computed count.
# ---------------------------------------------------------------------------


class TestGetTopicAttributesOverwrite:
    def test_subscriptions_confirmed_reflects_actual_count(self):
        """SubscriptionsConfirmed should reflect the actual number of
        subscriptions, not a stale value from topic.attributes."""
        store = SnsStore()
        store.create_topic("t", "us-east-1", "123")
        arn = "arn:aws:sns:us-east-1:123:t"
        topic = store.get_topic(arn)

        # Simulate a stale attribute (e.g., set via SetTopicAttributes)
        topic.attributes["SubscriptionsConfirmed"] = "999"

        # Add a real subscription
        store.subscribe(arn, "sqs", "arn:aws:sqs:us-east-1:123:q")

        mock_req = MagicMock()
        result = _get_topic_attributes(store, {"TopicArn": arn}, "us-east-1", "123", mock_req)

        # Should be "1" (actual count), not "999" (stale attribute)
        assert result["Attributes"]["SubscriptionsConfirmed"] == "1", (
            f"Expected SubscriptionsConfirmed='1' but got "
            f"'{result['Attributes']['SubscriptionsConfirmed']}'"
        )
