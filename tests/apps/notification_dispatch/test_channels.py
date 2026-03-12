"""
Tests for channel management: creating SNS topics + SQS subscriber queues,
sending to specific channels, and verifying channel isolation.
"""

import json
import time

from .models import Channel


def _poll_messages(sqs, queue_url, expected=1, timeout=10):
    """Poll an SQS queue until expected message count or timeout."""
    messages = []
    deadline = time.time() + timeout
    while len(messages) < expected and time.time() < deadline:
        resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        messages.extend(resp.get("Messages", []))
    return messages


class TestChannelCreation:
    def test_create_email_channel(self, notifier, unique_name):
        """Creating an email channel provisions an SNS topic and SQS queue."""
        info = notifier.create_channel(Channel.EMAIL, unique_name)
        assert "topic_arn" in info
        assert "queue_url" in info
        assert "queue_arn" in info
        assert "subscription_arn" in info
        assert info["channel"] == "email"

    def test_create_sms_channel(self, notifier, unique_name):
        """Creating an SMS channel provisions separate resources."""
        info = notifier.create_channel(Channel.SMS, unique_name)
        assert info["channel"] == "sms"
        assert "topic_arn" in info

    def test_create_webhook_channel(self, notifier, unique_name):
        """Webhook channel gets its own topic and queue."""
        info = notifier.create_channel(Channel.WEBHOOK, unique_name)
        assert info["channel"] == "webhook"
        assert "topic_arn" in info
        assert "queue_url" in info

    def test_list_channels(self, notifier, unique_name):
        """list_channels returns all registered channels."""
        notifier.create_channel(Channel.EMAIL, unique_name + "-e")
        notifier.create_channel(Channel.SMS, unique_name + "-s")
        channels = notifier.list_channels()
        channel_names = {c["channel"] for c in channels}
        assert "email" in channel_names
        assert "sms" in channel_names

    def test_get_channel(self, notifier, email_channel):
        """get_channel returns info for a registered channel."""
        info = notifier.get_channel(Channel.EMAIL)
        assert info is not None
        assert info["channel"] == "email"
        assert info["topic_arn"] == email_channel["topic_arn"]


class TestChannelSendReceive:
    def test_send_to_email_channel(self, notifier, sqs, email_channel):
        """Publishing to email topic delivers to the SQS subscriber."""
        notifier.sns.publish(
            TopicArn=email_channel["topic_arn"],
            Message=json.dumps(
                {
                    "notification_id": "CH-001",
                    "recipient": "alice@example.com",
                }
            ),
        )
        messages = _poll_messages(sqs, email_channel["queue_url"])
        assert len(messages) >= 1
        body = json.loads(messages[0]["Body"])
        inner = json.loads(body["Message"]) if "Message" in body else body
        assert inner["notification_id"] == "CH-001"

    def test_channel_isolation(self, notifier, sqs, email_channel, sms_channel):
        """Message sent to email channel does not appear in SMS queue."""
        notifier.sns.publish(
            TopicArn=email_channel["topic_arn"],
            Message=json.dumps({"notification_id": "ISO-001", "channel": "email"}),
        )
        # Email queue should have the message
        email_msgs = _poll_messages(sqs, email_channel["queue_url"])
        assert len(email_msgs) >= 1

        # SMS queue should be empty
        sms_resp = sqs.receive_message(
            QueueUrl=sms_channel["queue_url"],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )
        sms_msgs = sms_resp.get("Messages", [])
        assert len(sms_msgs) == 0

    def test_batch_messages_on_channel(self, notifier, sqs, email_channel):
        """Multiple messages published to a channel all arrive in the queue."""
        for i in range(5):
            notifier.sns.publish(
                TopicArn=email_channel["topic_arn"],
                Message=json.dumps({"notification_id": f"BATCH-{i:03d}"}),
            )
        messages = _poll_messages(sqs, email_channel["queue_url"], expected=5, timeout=15)
        assert len(messages) == 5
        ids = set()
        for msg in messages:
            body = json.loads(msg["Body"])
            inner = json.loads(body["Message"]) if "Message" in body else body
            ids.add(inner["notification_id"])
        for i in range(5):
            assert f"BATCH-{i:03d}" in ids


class TestChannelDeletion:
    def test_delete_channel(self, notifier, unique_name):
        """Deleting a channel removes it from the registry."""
        notifier.create_channel(Channel.PUSH, unique_name)
        assert notifier.get_channel(Channel.PUSH) is not None
        notifier.delete_channel(Channel.PUSH)
        assert notifier.get_channel(Channel.PUSH) is None
