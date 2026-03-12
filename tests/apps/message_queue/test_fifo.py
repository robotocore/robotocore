"""
Tests for FIFO queue operations via the MessageBroker.

Covers: strict ordering, message group isolation, deduplication (explicit and
content-based), and ordered send helper.
"""

import json

from .models import Message


class TestFifoOrdering:
    """FIFO queues preserve message order."""

    def test_messages_received_in_send_order(self, broker, fifo_queue):
        """Send 5 messages to a FIFO queue, verify they arrive in order."""
        group_id = "order-group-1"
        sent_bodies = []
        for i in range(5):
            body = json.dumps({"sequence": i, "step": f"step-{i}"})
            sent_bodies.append(body)
            broker.send(
                fifo_queue,
                Message(body=body, group_id=group_id, dedup_id=f"dedup-{i}"),
            )

        received = []
        for _ in range(5):
            msgs = broker.receive(fifo_queue, batch_size=1, wait_time=5)
            if msgs:
                received.append(msgs[0])
                broker.acknowledge(fifo_queue, msgs[0])

        sequences = [json.loads(m.body)["sequence"] for m in received]
        assert sequences == [0, 1, 2, 3, 4]

    def test_send_ordered_helper(self, broker, fifo_queue):
        """send_ordered sends a sequence of bodies in order under one group."""
        bodies = [f"ordered-msg-{i}" for i in range(5)]
        msg_ids = broker.send_ordered(fifo_queue, bodies, group_id="test-group")
        assert len(msg_ids) == 5

        received = []
        for _ in range(5):
            msgs = broker.receive(fifo_queue, batch_size=1, wait_time=5)
            if msgs:
                received.append(msgs[0].body)
                broker.acknowledge(fifo_queue, msgs[0])

        assert received == bodies


class TestMessageGroupIsolation:
    """Message groups in FIFO queues are processed independently."""

    def test_interleaved_groups_maintain_per_group_order(self, broker, fifo_queue):
        """Interleave messages across groups, verify per-group ordering is preserved."""
        # Send alternating messages to two groups
        for i in range(4):
            broker.send(
                fifo_queue,
                Message(
                    body=json.dumps({"group": "A", "seq": i}),
                    group_id="group-A",
                    dedup_id=f"a-{i}",
                ),
            )
            broker.send(
                fifo_queue,
                Message(
                    body=json.dumps({"group": "B", "seq": i}),
                    group_id="group-B",
                    dedup_id=f"b-{i}",
                ),
            )

        # Receive all messages
        received = broker.consume(fifo_queue, batch_size=10, max_messages=8, wait_time=5)
        assert len(received) == 8

        # Group by group ID and verify per-group order
        group_a = [json.loads(m.body) for m in received if json.loads(m.body)["group"] == "A"]
        group_b = [json.loads(m.body) for m in received if json.loads(m.body)["group"] == "B"]

        assert [m["seq"] for m in group_a] == [0, 1, 2, 3]
        assert [m["seq"] for m in group_b] == [0, 1, 2, 3]

        broker.acknowledge_batch(fifo_queue, received)

    def test_three_groups_independent(self, broker, fifo_queue):
        """Three message groups each maintain their own ordering."""
        groups = ["alpha", "beta", "gamma"]
        for g in groups:
            for i in range(3):
                broker.send(
                    fifo_queue,
                    Message(
                        body=json.dumps({"group": g, "index": i}),
                        group_id=g,
                        dedup_id=f"{g}-{i}",
                    ),
                )

        received = broker.consume(fifo_queue, batch_size=10, max_messages=9, wait_time=5)
        assert len(received) == 9

        for g in groups:
            group_msgs = [json.loads(m.body) for m in received if json.loads(m.body)["group"] == g]
            assert [m["index"] for m in group_msgs] == [0, 1, 2]

        broker.acknowledge_batch(fifo_queue, received)


class TestDeduplication:
    """FIFO deduplication behavior."""

    def test_explicit_dedup_id_prevents_duplicate(self, broker, fifo_queue):
        """Sending the same deduplication ID twice within window delivers only one message."""
        msg1 = Message(body="dedup-test-body", group_id="dedup-group", dedup_id="same-dedup-id")
        msg2 = Message(
            body="dedup-test-body-different", group_id="dedup-group", dedup_id="same-dedup-id"
        )

        broker.send(fifo_queue, msg1)
        broker.send(fifo_queue, msg2)

        received = broker.consume(fifo_queue, batch_size=10, max_messages=2, wait_time=5)
        # Only one message should be delivered (same dedup ID)
        assert len(received) == 1
        assert received[0].body == "dedup-test-body"
        broker.acknowledge_batch(fifo_queue, received)

    def test_content_based_dedup_identical_bodies(self, broker, fifo_queue):
        """Content-based dedup: identical body within 5-min window delivers once."""
        body = "identical-content-for-dedup"
        msg1 = Message(body=body, group_id="content-dedup-group", dedup_id="cd-1")
        msg2 = Message(body=body, group_id="content-dedup-group", dedup_id="cd-1")

        broker.send(fifo_queue, msg1)
        broker.send(fifo_queue, msg2)

        received = broker.consume(fifo_queue, batch_size=10, max_messages=2, wait_time=5)
        assert len(received) == 1
        broker.acknowledge_batch(fifo_queue, received)

    def test_different_dedup_ids_both_delivered(self, broker, fifo_queue):
        """Different deduplication IDs deliver both messages even with same body."""
        body = "same-body-different-dedup"
        msg1 = Message(body=body, group_id="dedup-group", dedup_id="dedup-A")
        msg2 = Message(body=body, group_id="dedup-group", dedup_id="dedup-B")

        broker.send(fifo_queue, msg1)
        broker.send(fifo_queue, msg2)

        received = broker.consume(fifo_queue, batch_size=10, max_messages=2, wait_time=5)
        assert len(received) == 2
        broker.acknowledge_batch(fifo_queue, received)
