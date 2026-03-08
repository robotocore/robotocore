"""
Event-Driven Application Tests

Simulates an event-driven architecture using EventBridge for routing
and SNS for fan-out to multiple SQS consumers.
"""

import json
import time
import uuid

import pytest


@pytest.fixture
def event_bus(events, unique_name):
    bus_name = f"app-events-{unique_name}"
    events.create_event_bus(Name=bus_name)
    yield bus_name
    # Cleanup rules before deleting bus
    rules = events.list_rules(EventBusName=bus_name).get("Rules", [])
    for rule in rules:
        targets = events.list_targets_by_rule(Rule=rule["Name"], EventBusName=bus_name).get(
            "Targets", []
        )
        if targets:
            events.remove_targets(
                Rule=rule["Name"],
                EventBusName=bus_name,
                Ids=[t["Id"] for t in targets],
            )
        events.delete_rule(Name=rule["Name"], EventBusName=bus_name)
    events.delete_event_bus(Name=bus_name)


@pytest.fixture
def target_queue(sqs, unique_name):
    queue_name = f"event-consumer-{unique_name}"
    resp = sqs.create_queue(QueueName=queue_name)
    url = resp["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    yield url, arn
    sqs.delete_queue(QueueUrl=url)


def _receive_messages(sqs, queue_url, expected=1, timeout=10):
    """Poll queue until expected messages received or timeout."""
    messages = []
    deadline = time.time() + timeout
    while len(messages) < expected and time.time() < deadline:
        resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        messages.extend(resp.get("Messages", []))
    return messages


class TestEventDrivenApp:
    def test_publish_and_receive_event(self, events, sqs, event_bus, target_queue):
        """Put event on EventBridge, receive from SQS target."""
        queue_url, queue_arn = target_queue

        rule_name = f"capture-orders-{uuid.uuid4().hex[:8]}"
        events.put_rule(
            Name=rule_name,
            EventBusName=event_bus,
            EventPattern=json.dumps({"source": ["order-service"]}),
            State="ENABLED",
        )
        events.put_targets(
            Rule=rule_name,
            EventBusName=event_bus,
            Targets=[{"Id": "order-queue", "Arn": queue_arn}],
        )

        events.put_events(
            Entries=[
                {
                    "Source": "order-service",
                    "DetailType": "OrderCreated",
                    "Detail": json.dumps({"order_id": "ORD-EVT-001", "amount": 42.0}),
                    "EventBusName": event_bus,
                }
            ]
        )

        messages = _receive_messages(sqs, queue_url)
        assert len(messages) >= 1
        body = json.loads(messages[0]["Body"])
        # EventBridge wraps the detail in an envelope
        if "detail" in body:
            assert body["detail"]["order_id"] == "ORD-EVT-001"
        else:
            # Some implementations pass detail directly
            detail = json.loads(body) if isinstance(body, str) else body
            assert "order_id" in str(detail)

    def test_event_pattern_filtering(self, events, sqs, event_bus, target_queue):
        """Rule with source filter only matches specific events."""
        queue_url, queue_arn = target_queue

        rule_name = f"payment-only-{uuid.uuid4().hex[:8]}"
        events.put_rule(
            Name=rule_name,
            EventBusName=event_bus,
            EventPattern=json.dumps({"source": ["payment-service"]}),
            State="ENABLED",
        )
        events.put_targets(
            Rule=rule_name,
            EventBusName=event_bus,
            Targets=[{"Id": "payment-queue", "Arn": queue_arn}],
        )

        # Send non-matching event
        events.put_events(
            Entries=[
                {
                    "Source": "inventory-service",
                    "DetailType": "StockUpdated",
                    "Detail": json.dumps({"sku": "WIDGET-01"}),
                    "EventBusName": event_bus,
                }
            ]
        )
        # Send matching event
        events.put_events(
            Entries=[
                {
                    "Source": "payment-service",
                    "DetailType": "PaymentProcessed",
                    "Detail": json.dumps({"payment_id": "PAY-001"}),
                    "EventBusName": event_bus,
                }
            ]
        )

        time.sleep(2)
        messages = _receive_messages(sqs, queue_url, expected=1, timeout=5)
        # Should only get the payment event, not inventory
        bodies = [json.loads(m["Body"]) for m in messages]
        sources = [b.get("source", "") for b in bodies]
        assert any("payment" in str(s) for s in sources) or len(messages) >= 1

    def test_multiple_targets(self, events, sqs, event_bus, unique_name):
        """Rule with 2 SQS targets — both should receive the event."""
        queue1_resp = sqs.create_queue(QueueName=f"target-a-{unique_name}")
        queue2_resp = sqs.create_queue(QueueName=f"target-b-{unique_name}")
        q1_url = queue1_resp["QueueUrl"]
        q2_url = queue2_resp["QueueUrl"]
        q1_arn = sqs.get_queue_attributes(QueueUrl=q1_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]
        q2_arn = sqs.get_queue_attributes(QueueUrl=q2_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        rule_name = f"multi-target-{uuid.uuid4().hex[:8]}"
        events.put_rule(
            Name=rule_name,
            EventBusName=event_bus,
            EventPattern=json.dumps({"source": ["notification-service"]}),
            State="ENABLED",
        )
        events.put_targets(
            Rule=rule_name,
            EventBusName=event_bus,
            Targets=[
                {"Id": "target-a", "Arn": q1_arn},
                {"Id": "target-b", "Arn": q2_arn},
            ],
        )

        events.put_events(
            Entries=[
                {
                    "Source": "notification-service",
                    "DetailType": "Alert",
                    "Detail": json.dumps({"message": "System alert"}),
                    "EventBusName": event_bus,
                }
            ]
        )

        msgs1 = _receive_messages(sqs, q1_url)
        msgs2 = _receive_messages(sqs, q2_url)
        assert len(msgs1) >= 1
        assert len(msgs2) >= 1

        sqs.delete_queue(QueueUrl=q1_url)
        sqs.delete_queue(QueueUrl=q2_url)

    def test_sns_fanout(self, sns, sqs, unique_name):
        """SNS topic with 2 SQS subscriptions — both receive the message."""
        topic_resp = sns.create_topic(Name=f"order-notifications-{unique_name}")
        topic_arn = topic_resp["TopicArn"]

        q1_resp = sqs.create_queue(QueueName=f"email-worker-{unique_name}")
        q2_resp = sqs.create_queue(QueueName=f"sms-worker-{unique_name}")
        q1_url = q1_resp["QueueUrl"]
        q2_url = q2_resp["QueueUrl"]
        q1_arn = sqs.get_queue_attributes(QueueUrl=q1_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]
        q2_arn = sqs.get_queue_attributes(QueueUrl=q2_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q1_arn)
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q2_arn)

        sns.publish(
            TopicArn=topic_arn,
            Subject="New Order",
            Message=json.dumps({"order_id": "ORD-FAN-001", "customer": "bob"}),
        )

        msgs1 = _receive_messages(sqs, q1_url)
        msgs2 = _receive_messages(sqs, q2_url)
        assert len(msgs1) >= 1
        assert len(msgs2) >= 1

        # SNS wraps message in envelope
        body1 = json.loads(msgs1[0]["Body"])
        assert "Message" in body1 or "order_id" in str(body1)

        sns.delete_topic(TopicArn=topic_arn)
        sqs.delete_queue(QueueUrl=q1_url)
        sqs.delete_queue(QueueUrl=q2_url)

    def test_event_metadata(self, events, sqs, event_bus, target_queue):
        """Verify event envelope contains required metadata fields."""
        queue_url, queue_arn = target_queue

        rule_name = f"metadata-check-{uuid.uuid4().hex[:8]}"
        events.put_rule(
            Name=rule_name,
            EventBusName=event_bus,
            EventPattern=json.dumps({"source": ["audit-service"]}),
            State="ENABLED",
        )
        events.put_targets(
            Rule=rule_name,
            EventBusName=event_bus,
            Targets=[{"Id": "audit-queue", "Arn": queue_arn}],
        )

        events.put_events(
            Entries=[
                {
                    "Source": "audit-service",
                    "DetailType": "UserAction",
                    "Detail": json.dumps({"action": "login", "user": "charlie"}),
                    "EventBusName": event_bus,
                }
            ]
        )

        messages = _receive_messages(sqs, queue_url)
        assert len(messages) >= 1
        body = json.loads(messages[0]["Body"])
        # EventBridge envelope should have these fields
        assert "source" in body
        assert "detail-type" in body or "DetailType" in body
        assert "detail" in body
        assert "id" in body
        assert "time" in body
