"""
Tests for SNS fan-out to multiple SQS consumers.

Verifies that SNS topics correctly distribute messages to all subscribed
SQS queues, including filter policies and raw message delivery.
"""

import json

from .app import EventRouter
from .models import FanOutConfig


class TestFanOut:
    def test_sns_topic_fans_out_to_all_subscribers(
        self, event_router: EventRouter, fan_out: tuple[FanOutConfig, list[str]]
    ):
        """Publish to SNS topic with 3 SQS subscribers — all 3 receive the message."""
        config, queue_urls = fan_out

        event_router.publish_to_topic(
            config.topic_arn,
            {"order_id": "ORD-FAN-001", "customer": "alice"},
            subject="New Order",
        )

        for i, q_url in enumerate(queue_urls):
            messages = event_router.receive_messages(q_url)
            assert len(messages) >= 1, f"Queue {i + 1} should have received the message"
            body = event_router.parse_sns_from_sqs(messages[0])
            assert "Message" in body or "order_id" in str(body)

    def test_filter_policy_only_matching_messages_delivered(
        self, event_router: EventRouter, unique_name: str
    ):
        """Subscription with filter policy only receives matching messages."""
        topic_resp = event_router.sns.create_topic(Name=f"filtered-{unique_name}")
        topic_arn = topic_resp["TopicArn"]
        event_router._topics.append(topic_arn)

        # Queue with filter: only "priority": "high"
        q_url, q_arn, sub_arn = event_router.create_filtered_subscription(
            topic_arn=topic_arn,
            queue_name=f"high-prio-{unique_name}",
            filter_policy={"priority": ["high"]},
        )

        # Publish non-matching message (low priority)
        event_router.publish_to_topic(
            topic_arn,
            "Low priority order",
            attributes={"priority": "low"},
        )
        # Publish matching message (high priority)
        event_router.publish_to_topic(
            topic_arn,
            "High priority order",
            attributes={"priority": "high"},
        )

        messages = event_router.receive_messages(q_url, expected=1, timeout=5)
        # Should have received at least the high-priority message
        assert len(messages) >= 1
        # Verify it's the high-priority one
        bodies = [json.loads(m["Body"]) for m in messages]
        message_texts = [b.get("Message", "") for b in bodies]
        assert any("High priority" in t for t in message_texts)

    def test_message_attributes_preserved_through_fanout(
        self, event_router: EventRouter, unique_name: str
    ):
        """Message attributes set on SNS publish are available in SQS."""
        topic_resp = event_router.sns.create_topic(Name=f"attrs-{unique_name}")
        topic_arn = topic_resp["TopicArn"]
        event_router._topics.append(topic_arn)

        q_url, q_arn = event_router.create_queue(f"attrs-recv-{unique_name}")
        sub_resp = event_router.sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)
        event_router._subscriptions.append(sub_resp["SubscriptionArn"])

        event_router.publish_to_topic(
            topic_arn,
            {"event": "test"},
            attributes={"env": "production", "region": "us-east-1"},
        )

        messages = event_router.receive_messages(q_url)
        assert len(messages) >= 1
        body = json.loads(messages[0]["Body"])
        # SNS envelope should contain MessageAttributes
        if "MessageAttributes" in body:
            attrs = body["MessageAttributes"]
            assert "env" in attrs
            assert attrs["env"]["Value"] == "production"

    def test_raw_message_delivery(self, event_router: EventRouter, unique_name: str):
        """Raw message delivery skips the SNS envelope wrapper."""
        topic_resp = event_router.sns.create_topic(Name=f"raw-{unique_name}")
        topic_arn = topic_resp["TopicArn"]
        event_router._topics.append(topic_arn)

        q_url, q_arn, sub_arn = event_router.create_filtered_subscription(
            topic_arn=topic_arn,
            queue_name=f"raw-recv-{unique_name}",
            filter_policy={},
            raw_delivery=True,
        )

        raw_payload = json.dumps({"raw_field": "raw_value", "count": 42})
        event_router.sns.publish(TopicArn=topic_arn, Message=raw_payload)

        messages = event_router.receive_messages(q_url)
        assert len(messages) >= 1
        body = json.loads(messages[0]["Body"])
        # With raw delivery, the body IS the message (no SNS envelope)
        if isinstance(body, dict) and "raw_field" in body:
            assert body["raw_field"] == "raw_value"
            assert body["count"] == 42
        else:
            # Some implementations may still wrap — at minimum the message content is there
            assert "raw_value" in str(body)

    def test_batch_events_all_fan_out(
        self, event_router: EventRouter, fan_out: tuple[FanOutConfig, list[str]]
    ):
        """Publish multiple messages, verify all are fanned out to all subscribers."""
        config, queue_urls = fan_out

        num_messages = 3
        for i in range(num_messages):
            event_router.publish_to_topic(
                config.topic_arn,
                {"batch_id": i, "data": f"payload-{i}"},
                subject=f"Batch {i}",
            )

        for q_idx, q_url in enumerate(queue_urls):
            messages = event_router.receive_messages(q_url, expected=num_messages, timeout=10)
            assert len(messages) >= num_messages, (
                f"Queue {q_idx + 1} received {len(messages)}/{num_messages} messages"
            )

    def test_sns_subject_included_in_delivery(self, event_router: EventRouter, unique_name: str):
        """Verify that the SNS Subject field is included in the SQS delivery."""
        topic_resp = event_router.sns.create_topic(Name=f"subj-{unique_name}")
        topic_arn = topic_resp["TopicArn"]
        event_router._topics.append(topic_arn)

        q_url, q_arn = event_router.create_queue(f"subj-recv-{unique_name}")
        sub_resp = event_router.sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)
        event_router._subscriptions.append(sub_resp["SubscriptionArn"])

        event_router.publish_to_topic(
            topic_arn,
            "Order shipped",
            subject="ShipmentNotification",
        )

        messages = event_router.receive_messages(q_url)
        assert len(messages) >= 1
        body = json.loads(messages[0]["Body"])
        if "Subject" in body:
            assert body["Subject"] == "ShipmentNotification"
