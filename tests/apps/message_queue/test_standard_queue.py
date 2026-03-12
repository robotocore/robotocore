"""
Tests for standard SQS queue operations via the MessageBroker.

Covers: send/receive, message attributes, visibility timeout, batch operations,
purge, and long polling.
"""

import json
import time

import pytest

from .models import Message


class TestSendReceive:
    """Basic send and receive operations."""

    def test_send_and_receive_single_message(self, broker, standard_queue):
        """Send a single message and verify it can be received with correct body."""
        body = json.dumps({"order_id": "ORD-001", "customer": "alice", "total": 99.95})
        msg_id = broker.send(standard_queue, Message(body=body))
        assert msg_id is not None

        messages = broker.receive(standard_queue, wait_time=5)
        assert len(messages) == 1
        received = json.loads(messages[0].body)
        assert received["order_id"] == "ORD-001"
        assert received["customer"] == "alice"
        assert received["total"] == 99.95

        broker.acknowledge(standard_queue, messages[0])

    def test_send_and_receive_with_string_attribute(self, broker, standard_queue):
        """Message attributes of type String are preserved."""
        msg = Message(
            body="test-body",
            attributes={
                "priority": {"DataType": "String", "StringValue": "high"},
            },
        )
        broker.send(standard_queue, msg)

        messages = broker.receive(standard_queue, wait_time=5)
        assert len(messages) == 1
        assert messages[0].attributes["priority"]["StringValue"] == "high"
        broker.acknowledge(standard_queue, messages[0])

    def test_send_and_receive_with_number_attribute(self, broker, standard_queue):
        """Message attributes of type Number are preserved."""
        msg = Message(
            body="test-body",
            attributes={
                "retry_count": {"DataType": "Number", "StringValue": "42"},
            },
        )
        broker.send(standard_queue, msg)

        messages = broker.receive(standard_queue, wait_time=5)
        assert len(messages) == 1
        assert messages[0].attributes["retry_count"]["StringValue"] == "42"
        assert messages[0].attributes["retry_count"]["DataType"] == "Number"
        broker.acknowledge(standard_queue, messages[0])

    def test_send_and_receive_with_multiple_attributes(self, broker, standard_queue):
        """Multiple message attributes are all preserved."""
        msg = Message(
            body=json.dumps({"order_id": "ORD-ATTR-001"}),
            attributes={
                "priority": {"DataType": "String", "StringValue": "high"},
                "retry_count": {"DataType": "Number", "StringValue": "0"},
                "source_system": {"DataType": "String", "StringValue": "web-checkout"},
            },
        )
        broker.send(standard_queue, msg)

        messages = broker.receive(standard_queue, wait_time=5)
        assert len(messages) == 1
        attrs = messages[0].attributes
        assert attrs["priority"]["StringValue"] == "high"
        assert attrs["retry_count"]["StringValue"] == "0"
        assert attrs["source_system"]["StringValue"] == "web-checkout"
        broker.acknowledge(standard_queue, messages[0])

    def test_send_json_convenience(self, broker, standard_queue):
        """send_json serializes dict to JSON automatically."""
        broker.send_json(standard_queue, {"event": "order_placed", "value": 150})

        messages = broker.receive(standard_queue, wait_time=5)
        assert len(messages) == 1
        body = json.loads(messages[0].body)
        assert body["event"] == "order_placed"
        assert body["value"] == 150
        broker.acknowledge(standard_queue, messages[0])


class TestVisibilityTimeout:
    """Visibility timeout behavior."""

    def test_message_reappears_after_visibility_timeout(self, broker, standard_queue):
        """A message not acknowledged reappears after the visibility timeout expires."""
        broker.send(standard_queue, Message(body="timeout-test"))

        # Receive but don't acknowledge
        messages = broker.receive(standard_queue, wait_time=5)
        assert len(messages) == 1
        assert messages[0].body == "timeout-test"

        # Immediately try to receive again — should get nothing (message is invisible)
        messages2 = broker.receive(standard_queue, wait_time=1)
        assert len(messages2) == 0

        # Wait for visibility timeout (5s) + margin
        time.sleep(6)

        # Now the message should reappear
        messages3 = broker.receive(standard_queue, wait_time=5)
        assert len(messages3) == 1
        assert messages3[0].body == "timeout-test"
        broker.acknowledge(standard_queue, messages3[0])


class TestBatchOperations:
    """Batch send, receive, and delete."""

    def test_batch_send_10_messages(self, broker, standard_queue):
        """Send 10 messages in a single batch, receive all of them."""
        messages = [
            Message(body=json.dumps({"item_id": f"ITEM-{i:03d}", "qty": i + 1})) for i in range(10)
        ]
        result = broker.send_batch(standard_queue, messages)
        assert result.all_succeeded
        assert len(result.successful) == 10

        # Collect all messages
        received = broker.consume(standard_queue, batch_size=10, max_messages=10, wait_time=5)
        assert len(received) == 10

        item_ids = {json.loads(m.body)["item_id"] for m in received}
        assert item_ids == {f"ITEM-{i:03d}" for i in range(10)}

        broker.acknowledge_batch(standard_queue, received)

    def test_batch_send_exceeds_limit_raises(self, broker, standard_queue):
        """Attempting to send more than 10 messages raises ValueError."""
        messages = [Message(body=f"msg-{i}") for i in range(11)]
        with pytest.raises(ValueError, match="10 messages"):
            broker.send_batch(standard_queue, messages)

    def test_batch_acknowledge(self, broker, standard_queue):
        """Batch acknowledge removes all messages from the queue."""
        for i in range(5):
            broker.send(standard_queue, Message(body=f"batch-ack-{i}"))

        received = broker.consume(standard_queue, batch_size=10, max_messages=5, wait_time=5)
        assert len(received) == 5

        broker.acknowledge_batch(standard_queue, received)

        # Verify queue is empty
        remaining = broker.receive(standard_queue, wait_time=2)
        assert len(remaining) == 0


class TestPurge:
    """Queue purge operations."""

    def test_purge_removes_all_messages(self, broker, standard_queue):
        """Purging a queue removes all messages."""
        for i in range(5):
            broker.send(standard_queue, Message(body=f"purge-test-{i}"))

        # Verify messages are there
        stats = broker.get_stats(standard_queue)
        assert stats.approximate_messages >= 1  # SQS counts are approximate

        broker.purge_queue(standard_queue)

        # Give SQS time to process purge
        time.sleep(1)

        # Verify queue is empty
        messages = broker.receive(standard_queue, wait_time=2)
        assert len(messages) == 0


class TestLongPolling:
    """Long polling behavior."""

    def test_long_poll_receives_message(self, broker, standard_queue):
        """Long polling returns a message when one arrives."""
        # Send a message
        broker.send(standard_queue, Message(body="long-poll-test"))

        # Long poll with generous wait
        messages = broker.receive(standard_queue, wait_time=10)
        assert len(messages) == 1
        assert messages[0].body == "long-poll-test"
        broker.acknowledge(standard_queue, messages[0])

    def test_long_poll_empty_returns_nothing(self, broker, standard_queue):
        """Long polling an empty queue returns no messages after timeout."""
        messages = broker.receive(standard_queue, wait_time=1)
        assert len(messages) == 0
