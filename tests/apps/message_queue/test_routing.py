"""
Tests for message routing, filtering, delay, and forwarding via the MessageBroker.

Covers: attribute-based routing, message filtering, delay queues, per-message delay,
and queue-to-queue forwarding.
"""

import json
import time

from .models import Message, QueueConfig


class TestMessageRouting:
    """Route messages to different queues based on attributes."""

    def test_route_by_attribute_to_different_queues(self, broker, unique_name):
        """Messages are routed to different queues based on a 'type' attribute."""
        orders_config = QueueConfig(name=f"route-orders-{unique_name}")
        returns_config = QueueConfig(name=f"route-returns-{unique_name}")

        orders_url = broker.create_queue(orders_config)
        returns_url = broker.create_queue(returns_config)

        try:
            # Register routes
            def is_order(msg: Message) -> bool:
                attr = msg.attributes.get("type", {})
                return attr.get("StringValue") == "order"

            def is_return(msg: Message) -> bool:
                attr = msg.attributes.get("type", {})
                return attr.get("StringValue") == "return"

            broker.add_route(is_order, orders_url)
            broker.add_route(is_return, returns_url)

            # Route messages
            order_msg = Message(
                body=json.dumps({"item": "widget", "action": "buy"}),
                attributes={"type": {"DataType": "String", "StringValue": "order"}},
            )
            return_msg = Message(
                body=json.dumps({"item": "gadget", "action": "return"}),
                attributes={"type": {"DataType": "String", "StringValue": "return"}},
            )

            target1 = broker.route_message(order_msg)
            target2 = broker.route_message(return_msg)

            assert target1 == orders_url
            assert target2 == returns_url

            # Verify messages landed in correct queues
            order_msgs = broker.receive(orders_url, wait_time=5)
            assert len(order_msgs) == 1
            assert json.loads(order_msgs[0].body)["action"] == "buy"

            return_msgs = broker.receive(returns_url, wait_time=5)
            assert len(return_msgs) == 1
            assert json.loads(return_msgs[0].body)["action"] == "return"

            broker.acknowledge(orders_url, order_msgs[0])
            broker.acknowledge(returns_url, return_msgs[0])
        finally:
            broker.delete_queue(orders_url)
            broker.delete_queue(returns_url)

    def test_unmatched_route_returns_none(self, broker, standard_queue):
        """A message that matches no route returns None."""
        msg = Message(body="no-route")
        result = broker.route_message(msg)
        assert result is None


class TestMessageFiltering:
    """Filter messages by attributes during receive."""

    def test_filter_by_priority_attribute(self, broker, standard_queue):
        """Only receive messages matching the priority filter."""
        # Send mixed priority messages
        broker.send(
            standard_queue,
            Message(
                body="high-priority-task",
                attributes={"priority": {"DataType": "String", "StringValue": "high"}},
            ),
        )
        broker.send(
            standard_queue,
            Message(
                body="low-priority-task",
                attributes={"priority": {"DataType": "String", "StringValue": "low"}},
            ),
        )

        # Filter for high priority only
        matched = broker.receive_filtered(
            standard_queue,
            attribute_filter={"priority": "high"},
            max_polls=5,
            wait_time=2,
        )

        assert len(matched) >= 1
        assert all(m.attributes["priority"]["StringValue"] == "high" for m in matched)

        # Clean up
        for m in matched:
            broker.acknowledge(standard_queue, m)
        broker.drain(standard_queue, wait_time=1)

    def test_filter_with_no_matches_returns_empty(self, broker, standard_queue):
        """When no messages match the filter, an empty list is returned."""
        broker.send(
            standard_queue,
            Message(
                body="only-low",
                attributes={"priority": {"DataType": "String", "StringValue": "low"}},
            ),
        )

        matched = broker.receive_filtered(
            standard_queue,
            attribute_filter={"priority": "critical"},
            max_polls=2,
            wait_time=1,
        )
        assert len(matched) == 0

        # Clean up
        broker.drain(standard_queue, wait_time=2)


class TestDelayQueue:
    """Delay queue behavior."""

    def test_delay_queue_messages_not_immediately_visible(self, broker, delay_queue):
        """Messages sent to a delay queue are not visible for the delay period."""
        broker.send(delay_queue, Message(body="delayed-message"))

        # Try to receive immediately — should get nothing (5s delay)
        messages = broker.receive(delay_queue, wait_time=1)
        assert len(messages) == 0

    def test_delay_queue_messages_visible_after_delay(self, broker, delay_queue):
        """Messages become visible after the delay period."""
        broker.send(delay_queue, Message(body="delayed-visible"))

        # Wait for the delay (5s) + margin
        time.sleep(6)

        messages = broker.receive(delay_queue, wait_time=5)
        assert len(messages) == 1
        assert messages[0].body == "delayed-visible"
        broker.acknowledge(delay_queue, messages[0])

    def test_per_message_delay(self, broker, standard_queue):
        """Per-message delay on a standard queue delays individual messages."""
        broker.send(
            standard_queue,
            Message(body="nodelay", delay_seconds=0),
        )
        broker.send(
            standard_queue,
            Message(body="delayed-3s", delay_seconds=3),
        )

        # Immediately receive — only the non-delayed message should be there
        messages = broker.receive(standard_queue, wait_time=2)
        assert len(messages) >= 1
        bodies = [m.body for m in messages]
        assert "nodelay" in bodies

        for m in messages:
            broker.acknowledge(standard_queue, m)

        # Wait for the delayed message
        time.sleep(4)
        delayed = broker.receive(standard_queue, wait_time=5)
        assert len(delayed) >= 1
        assert any(m.body == "delayed-3s" for m in delayed)
        for m in delayed:
            broker.acknowledge(standard_queue, m)


class TestQueueForwarding:
    """Queue-to-queue forwarding."""

    def test_forward_messages_between_queues(self, broker, unique_name):
        """forward() consumes from source and produces to target."""
        src_config = QueueConfig(name=f"fwd-src-{unique_name}")
        dst_config = QueueConfig(name=f"fwd-dst-{unique_name}")
        src_url = broker.create_queue(src_config)
        dst_url = broker.create_queue(dst_config)

        try:
            # Send messages to source
            for i in range(3):
                broker.send(src_url, Message(body=f"forward-{i}"))

            # Forward all
            count = broker.forward(src_url, dst_url, batch_size=10, wait_time=5)
            assert count == 3

            # Verify messages are in destination
            received = broker.consume(dst_url, batch_size=10, max_messages=3, wait_time=5)
            assert len(received) == 3
            bodies = {m.body for m in received}
            assert bodies == {"forward-0", "forward-1", "forward-2"}

            broker.acknowledge_batch(dst_url, received)

            # Source should be empty
            remaining = broker.receive(src_url, wait_time=1)
            assert len(remaining) == 0
        finally:
            broker.delete_queue(src_url)
            broker.delete_queue(dst_url)

    def test_forward_with_transform(self, broker, unique_name):
        """forward() can transform message bodies during forwarding."""
        src_config = QueueConfig(name=f"xform-src-{unique_name}")
        dst_config = QueueConfig(name=f"xform-dst-{unique_name}")
        src_url = broker.create_queue(src_config)
        dst_url = broker.create_queue(dst_config)

        try:
            broker.send(src_url, Message(body=json.dumps({"value": 10})))

            def double_value(body: str) -> str:
                data = json.loads(body)
                data["value"] *= 2
                return json.dumps(data)

            count = broker.forward(src_url, dst_url, transform=double_value, wait_time=5)
            assert count == 1

            received = broker.receive(dst_url, wait_time=5)
            assert len(received) == 1
            assert json.loads(received[0].body)["value"] == 20
            broker.acknowledge(dst_url, received[0])
        finally:
            broker.delete_queue(src_url)
            broker.delete_queue(dst_url)


class TestQueueMetrics:
    """Queue statistics and metrics."""

    def test_get_stats_shows_message_count(self, broker, standard_queue):
        """get_stats reports approximate message count."""
        for i in range(3):
            broker.send(standard_queue, Message(body=f"stats-{i}"))

        stats = broker.get_stats(standard_queue)
        assert stats.approximate_messages >= 1  # SQS counts are approximate

        # Clean up
        broker.drain(standard_queue, wait_time=2)

    def test_count_messages(self, broker, standard_queue):
        """count_messages returns approximate visible count."""
        broker.send(standard_queue, Message(body="count-test"))

        count = broker.count_messages(standard_queue)
        assert count >= 1

        broker.drain(standard_queue, wait_time=2)

    def test_wait_for_empty(self, broker, standard_queue):
        """wait_for_empty blocks until queue is drained."""
        broker.send(standard_queue, Message(body="drain-me"))

        # Drain in background by consuming
        msgs = broker.receive(standard_queue, wait_time=5)
        for m in msgs:
            broker.acknowledge(standard_queue, m)

        result = broker.wait_for_empty(standard_queue, timeout=10)
        assert result is True
