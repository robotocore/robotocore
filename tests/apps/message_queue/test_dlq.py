"""
Tests for dead-letter queue (DLQ) behavior via the MessageBroker.

Covers: messages exceeding maxReceiveCount move to DLQ, attribute preservation,
configurable redrive policy, and selective DLQ routing.
"""

import json
import time

from .models import Message


class TestDLQRedrive:
    """Dead-letter queue redrive behavior."""

    def test_message_moves_to_dlq_after_max_receives(self, broker, dlq_pair):
        """A message received maxReceiveCount times without ack moves to the DLQ."""
        main_url, dlq_url = dlq_pair

        broker.send(
            main_url,
            Message(body=json.dumps({"order_id": "ORD-FAIL-001", "status": "bad"})),
        )

        # Receive once (maxReceiveCount=1), don't delete — let visibility expire
        messages = broker.receive(main_url, wait_time=5)
        assert len(messages) == 1

        # Wait for visibility timeout (1s) + margin, then receive again to trigger redrive
        time.sleep(3)
        broker.receive(main_url, wait_time=1)

        # Give time for redrive to complete
        time.sleep(2)

        # Message should now be on DLQ
        dlq_messages = broker.receive(dlq_url, wait_time=5)
        assert len(dlq_messages) == 1
        body = json.loads(dlq_messages[0].body)
        assert body["order_id"] == "ORD-FAIL-001"
        broker.acknowledge(dlq_url, dlq_messages[0])

    def test_dlq_message_retains_attributes(self, broker, dlq_pair):
        """Message attributes are preserved when a message moves to the DLQ."""
        main_url, dlq_url = dlq_pair

        broker.send(
            main_url,
            Message(
                body="attr-test",
                attributes={
                    "priority": {"DataType": "String", "StringValue": "critical"},
                    "source": {"DataType": "String", "StringValue": "payment-service"},
                },
            ),
        )

        # Trigger redrive: receive without ack, wait, receive again
        broker.receive(main_url, wait_time=5)
        time.sleep(3)
        broker.receive(main_url, wait_time=1)
        time.sleep(2)

        dlq_messages = broker.receive(dlq_url, wait_time=5)
        assert len(dlq_messages) == 1
        assert dlq_messages[0].attributes["priority"]["StringValue"] == "critical"
        assert dlq_messages[0].attributes["source"]["StringValue"] == "payment-service"
        broker.acknowledge(dlq_url, dlq_messages[0])

    def test_acknowledged_messages_do_not_go_to_dlq(self, broker, dlq_pair):
        """Messages properly acknowledged never end up on the DLQ."""
        main_url, dlq_url = dlq_pair

        broker.send(main_url, Message(body="good-message"))

        # Receive and acknowledge
        messages = broker.receive(main_url, wait_time=5)
        assert len(messages) == 1
        broker.acknowledge(main_url, messages[0])

        # Wait and check DLQ — should be empty
        time.sleep(2)
        dlq_messages = broker.receive(dlq_url, wait_time=2)
        assert len(dlq_messages) == 0

    def test_multiple_messages_only_failed_go_to_dlq(self, broker, dlq_pair):
        """Of multiple messages, only the unacknowledged ones end up on the DLQ."""
        main_url, dlq_url = dlq_pair

        # Send two messages
        broker.send(main_url, Message(body=json.dumps({"id": "good"})))
        broker.send(main_url, Message(body=json.dumps({"id": "bad"})))

        # Receive and selectively acknowledge
        messages = broker.consume(main_url, batch_size=10, max_messages=2, wait_time=5)
        assert len(messages) == 2

        for msg in messages:
            body = json.loads(msg.body)
            if body["id"] == "good":
                broker.acknowledge(main_url, msg)
            # "bad" message is left unacknowledged

        # Wait for visibility timeout + redrive
        time.sleep(3)
        broker.receive(main_url, wait_time=1)
        time.sleep(2)

        # Only "bad" should be on DLQ
        dlq_messages = broker.receive(dlq_url, wait_time=5)
        assert len(dlq_messages) == 1
        assert json.loads(dlq_messages[0].body)["id"] == "bad"
        broker.acknowledge(dlq_url, dlq_messages[0])


class TestRedriveConfig:
    """Changing redrive policy configuration."""

    def test_attach_dlq_changes_redrive_policy(self, broker, sqs, unique_name):
        """attach_dlq wires a DLQ to an existing queue."""
        from .models import QueueConfig

        # Create two standalone queues
        main_config = QueueConfig(name=f"reconf-main-{unique_name}", visibility_timeout=1)
        dlq_config = QueueConfig(name=f"reconf-dlq-{unique_name}", visibility_timeout=5)

        main_url = broker.create_queue(main_config)
        dlq_url = broker.create_queue(dlq_config)

        try:
            # Wire them together
            broker.attach_dlq(main_url, dlq_url, max_receive_count=2)

            # Verify redrive policy was set
            attrs = sqs.get_queue_attributes(QueueUrl=main_url, AttributeNames=["RedrivePolicy"])
            policy = json.loads(attrs["Attributes"]["RedrivePolicy"])
            assert policy["maxReceiveCount"] == "2"
            assert "arn" in policy["deadLetterTargetArn"].lower()
        finally:
            broker.delete_queue(main_url)
            broker.delete_queue(dlq_url)
