"""Unit tests for SQS in-memory models."""

import time

from robotocore.services.sqs.models import (
    FifoQueue,
    SqsMessage,
    SqsStore,
    StandardQueue,
)


def _msg(body="hello", **kwargs):
    import hashlib
    import uuid

    return SqsMessage(
        message_id=str(uuid.uuid4()),
        body=body,
        md5_of_body=hashlib.md5(body.encode()).hexdigest(),
        **kwargs,
    )


class TestSqsMessage:
    def test_is_visible_when_no_deadline(self):
        msg = _msg()
        assert msg.is_visible is True

    def test_not_visible_during_timeout(self):
        msg = _msg()
        msg.update_visibility_timeout(30)
        assert msg.is_visible is False

    def test_visible_after_timeout_expires(self):
        msg = _msg()
        msg.visibility_deadline = time.time() - 1
        assert msg.is_visible is True

    def test_not_visible_when_deleted(self):
        msg = _msg()
        msg.deleted = True
        assert msg.is_visible is False

    def test_is_delayed(self):
        msg = _msg(delay_seconds=10)
        assert msg.is_delayed is True

    def test_not_delayed_when_zero(self):
        msg = _msg(delay_seconds=0)
        assert msg.is_delayed is False

    def test_priority_is_created_time(self):
        msg = _msg()
        assert msg.priority == msg.created


class TestStandardQueue:
    def test_put_and_receive(self):
        q = StandardQueue("test-q", "us-east-1", "123456789012")
        q.put(_msg("hello"))
        results = q.receive(max_messages=1)
        assert len(results) == 1
        assert results[0][0].body == "hello"

    def test_receive_returns_receipt_handle(self):
        q = StandardQueue("test-q", "us-east-1", "123456789012")
        q.put(_msg("hello"))
        results = q.receive()
        _, receipt = results[0]
        assert len(receipt) > 0

    def test_delete_message(self):
        q = StandardQueue("test-q", "us-east-1", "123456789012")
        q.put(_msg("hello"))
        results = q.receive()
        _, receipt = results[0]
        assert q.delete_message(receipt) is True

    def test_delete_nonexistent_returns_false(self):
        q = StandardQueue("test-q", "us-east-1", "123456789012")
        assert q.delete_message("bad-receipt") is False

    def test_visibility_timeout_hides_message(self):
        q = StandardQueue("test-q", "us-east-1", "123456789012")
        q.put(_msg("hello"))
        q.receive(visibility_timeout=30)
        # Should be empty now
        results = q.receive(max_messages=1, wait_time_seconds=0)
        assert len(results) == 0

    def test_change_visibility_to_zero(self):
        q = StandardQueue("test-q", "us-east-1", "123456789012")
        q.put(_msg("hello"))
        results = q.receive(visibility_timeout=30)
        _, receipt = results[0]
        q.change_visibility(receipt, 0)
        results2 = q.receive()
        assert len(results2) == 1

    def test_requeue_expired_inflight(self):
        q = StandardQueue("test-q", "us-east-1", "123456789012")
        q.put(_msg("hello"))
        results = q.receive(visibility_timeout=0)
        msg = results[0][0]
        msg.visibility_deadline = time.time() - 1  # Expired
        q.requeue_inflight_messages()
        results2 = q.receive()
        assert len(results2) == 1

    def test_delayed_message(self):
        q = StandardQueue("test-q", "us-east-1", "123456789012")
        q.put(_msg("delayed", delay_seconds=100))
        results = q.receive(wait_time_seconds=0)
        assert len(results) == 0

    def test_enqueue_delayed_when_ready(self):
        q = StandardQueue("test-q", "us-east-1", "123456789012")
        msg = _msg("was delayed", delay_seconds=1)
        msg.created = time.time() - 10  # Created 10s ago
        q.put(msg)
        q.enqueue_delayed_messages()
        results = q.receive()
        assert len(results) == 1

    def test_purge(self):
        q = StandardQueue("test-q", "us-east-1", "123456789012")
        for i in range(5):
            q.put(_msg(f"msg {i}"))
        q.purge()
        results = q.receive(max_messages=10, wait_time_seconds=0)
        assert len(results) == 0

    def test_get_attributes(self):
        q = StandardQueue("test-q", "us-east-1", "123456789012")
        q.put(_msg("hello"))
        attrs = q.get_attributes()
        assert attrs["QueueArn"] == "arn:aws:sqs:us-east-1:123456789012:test-q"
        assert int(attrs["ApproximateNumberOfMessages"]) >= 1

    def test_receive_increments_count(self):
        q = StandardQueue("test-q", "us-east-1", "123456789012")
        q.put(_msg("hello"))
        results = q.receive()
        assert results[0][0].receive_count == 1

    def test_url_format(self):
        q = StandardQueue("my-queue", "us-east-1", "123456789012")
        assert "123456789012" in q.url
        assert "my-queue" in q.url

    def test_arn_format(self):
        q = StandardQueue("my-queue", "us-east-1", "123456789012")
        assert q.arn == "arn:aws:sqs:us-east-1:123456789012:my-queue"

    def test_is_fifo(self):
        q = StandardQueue("my-queue", "us-east-1", "123456789012")
        assert q.is_fifo is False
        q2 = StandardQueue("my-queue.fifo", "us-east-1", "123456789012")
        assert q2.is_fifo is True


class TestFifoQueue:
    def test_message_ordering(self):
        q = FifoQueue("test.fifo", "us-east-1", "123456789012")
        for i in range(3):
            q.put(_msg(f"msg {i}", message_group_id="g1"))
        results = []
        for _ in range(3):
            recv = q.receive()
            if recv:
                results.append(recv[0][0].body)
                q.delete_message(recv[0][1])
        assert results == ["msg 0", "msg 1", "msg 2"]

    def test_content_based_dedup(self):
        q = FifoQueue(
            "test.fifo",
            "us-east-1",
            "123456789012",
            attributes={"ContentBasedDeduplication": "true"},
        )
        q.put(_msg("same body", message_group_id="g1"))
        q.put(_msg("same body", message_group_id="g1"))
        results = q.receive(max_messages=10)
        assert len(results) == 1

    def test_explicit_dedup_id(self):
        q = FifoQueue("test.fifo", "us-east-1", "123456789012")
        q.put(_msg("body1", message_group_id="g1", message_deduplication_id="dedup1"))
        q.put(_msg("body2", message_group_id="g1", message_deduplication_id="dedup1"))
        results = q.receive(max_messages=10)
        assert len(results) == 1
        assert results[0][0].body == "body1"

    def test_sequence_number_assigned(self):
        q = FifoQueue("test.fifo", "us-east-1", "123456789012")
        msg = q.put(_msg("hello", message_group_id="g1", message_deduplication_id="d1"))
        assert msg.sequence_number is not None

    def test_different_groups_independent(self):
        q = FifoQueue(
            "test.fifo",
            "us-east-1",
            "123456789012",
            attributes={"ContentBasedDeduplication": "true"},
        )
        q.put(_msg("g1 msg", message_group_id="group1"))
        q.put(_msg("g2 msg", message_group_id="group2"))
        r1 = q.receive()
        assert len(r1) == 1
        # Group1 is now in-flight, group2 should still be available
        r2 = q.receive()
        assert len(r2) == 1
        # Verify we got both groups
        bodies = {r1[0][0].body, r2[0][0].body}
        assert bodies == {"g1 msg", "g2 msg"}


class TestSqsStore:
    def test_create_and_get_queue(self):
        store = SqsStore()
        q = store.create_queue("test-q", "us-east-1", "123456789012")
        assert store.get_queue("test-q") is q

    def test_create_fifo_queue(self):
        store = SqsStore()
        q = store.create_queue("test.fifo", "us-east-1", "123456789012")
        assert isinstance(q, FifoQueue)

    def test_idempotent_create(self):
        store = SqsStore()
        q1 = store.create_queue("test-q", "us-east-1", "123456789012")
        q2 = store.create_queue("test-q", "us-east-1", "123456789012")
        assert q1 is q2

    def test_delete_queue(self):
        store = SqsStore()
        store.create_queue("test-q", "us-east-1", "123456789012")
        assert store.delete_queue("test-q") is True
        assert store.get_queue("test-q") is None

    def test_get_queue_by_url(self):
        store = SqsStore()
        q = store.create_queue("test-q", "us-east-1", "123456789012")
        found = store.get_queue_by_url(q.url)
        assert found is q

    def test_get_queue_by_arn(self):
        store = SqsStore()
        q = store.create_queue("test-q", "us-east-1", "123456789012")
        found = store.get_queue_by_arn(q.arn)
        assert found is q

    def test_list_queues(self):
        store = SqsStore()
        store.create_queue("alpha", "us-east-1", "123456789012")
        store.create_queue("beta", "us-east-1", "123456789012")
        assert len(store.list_queues()) == 2

    def test_list_queues_with_prefix(self):
        store = SqsStore()
        store.create_queue("alpha-1", "us-east-1", "123456789012")
        store.create_queue("alpha-2", "us-east-1", "123456789012")
        store.create_queue("beta-1", "us-east-1", "123456789012")
        assert len(store.list_queues("alpha")) == 2
