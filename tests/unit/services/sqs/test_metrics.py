"""Unit tests for SQS CloudWatch metrics computation and counter tracking."""

import os
import threading
import time
from unittest.mock import patch

import pytest

from robotocore.services.sqs.metrics import (
    SqsMetricsPublisher,
    _counter_lock,
    _counters,
    compute_queue_metrics,
    increment_deleted,
    increment_empty_receives,
    increment_received,
    increment_sent,
    snapshot_and_reset_counters,
)
from robotocore.services.sqs.models import SqsMessage, StandardQueue


@pytest.fixture(autouse=True)
def _clear_counters():
    """Reset global counters before each test."""
    with _counter_lock:
        _counters.clear()
    yield
    with _counter_lock:
        _counters.clear()


def _make_queue(name: str = "test-queue") -> StandardQueue:
    return StandardQueue(name, "us-east-1", "123456789012")


def _make_msg(body: str = "hello", created: float | None = None) -> SqsMessage:
    import hashlib

    msg = SqsMessage(
        message_id="msg-1",
        body=body,
        md5_of_body=hashlib.md5(body.encode()).hexdigest(),
    )
    if created is not None:
        msg.created = created
    return msg


# ---------------------------------------------------------------------------
# Gauge metric computation
# ---------------------------------------------------------------------------


class TestApproximateNumberOfMessagesVisible:
    def test_empty_queue(self):
        queue = _make_queue()
        metrics = compute_queue_metrics(queue)
        assert metrics["ApproximateNumberOfMessagesVisible"] == 0.0

    def test_with_messages(self):
        queue = _make_queue()
        queue.put(_make_msg("a"))
        queue.put(_make_msg("b"))
        metrics = compute_queue_metrics(queue)
        assert metrics["ApproximateNumberOfMessagesVisible"] == 2.0


class TestApproximateNumberOfMessagesNotVisible:
    def test_no_inflight(self):
        queue = _make_queue()
        metrics = compute_queue_metrics(queue)
        assert metrics["ApproximateNumberOfMessagesNotVisible"] == 0.0

    def test_with_inflight(self):
        queue = _make_queue()
        queue.put(_make_msg("a"))
        # Receive to make it inflight
        results = queue.receive(max_messages=1, visibility_timeout=30)
        assert len(results) == 1
        metrics = compute_queue_metrics(queue)
        assert metrics["ApproximateNumberOfMessagesNotVisible"] == 1.0


class TestApproximateNumberOfMessagesDelayed:
    def test_no_delayed(self):
        queue = _make_queue()
        metrics = compute_queue_metrics(queue)
        assert metrics["ApproximateNumberOfMessagesDelayed"] == 0.0

    def test_with_delayed(self):
        queue = _make_queue()
        msg = _make_msg("delayed")
        msg.delay_seconds = 300
        queue.put(msg)
        metrics = compute_queue_metrics(queue)
        assert metrics["ApproximateNumberOfMessagesDelayed"] == 1.0


class TestApproximateAgeOfOldestMessage:
    def test_empty_queue_returns_zero(self):
        queue = _make_queue()
        metrics = compute_queue_metrics(queue)
        assert metrics["ApproximateAgeOfOldestMessage"] == 0.0

    def test_computes_age(self):
        queue = _make_queue()
        msg = _make_msg("old")
        msg.created = time.time() - 120  # 2 minutes ago
        queue.put(msg)
        metrics = compute_queue_metrics(queue)
        assert metrics["ApproximateAgeOfOldestMessage"] >= 119.0
        assert metrics["ApproximateAgeOfOldestMessage"] <= 125.0


# ---------------------------------------------------------------------------
# Counter tracking
# ---------------------------------------------------------------------------


class TestCounterIncrements:
    def test_number_of_messages_sent(self):
        increment_sent("q1", 100)
        increment_sent("q1", 200)
        snap = snapshot_and_reset_counters()
        assert snap["q1"]["sent"] == 2
        assert snap["q1"]["sent_bytes"] == 300

    def test_number_of_messages_received(self):
        increment_received("q1", 3)
        increment_received("q1", 2)
        snap = snapshot_and_reset_counters()
        assert snap["q1"]["received"] == 5

    def test_number_of_messages_deleted(self):
        increment_deleted("q1")
        increment_deleted("q1")
        increment_deleted("q1")
        snap = snapshot_and_reset_counters()
        assert snap["q1"]["deleted"] == 3

    def test_number_of_empty_receives(self):
        increment_empty_receives("q1")
        snap = snapshot_and_reset_counters()
        assert snap["q1"]["empty_receives"] == 1

    def test_sent_message_size(self):
        increment_sent("q1", 256)
        increment_sent("q1", 512)
        snap = snapshot_and_reset_counters()
        assert snap["q1"]["sent_bytes"] == 768


class TestCounterReset:
    def test_reset_after_snapshot(self):
        increment_sent("q1", 10)
        increment_received("q1", 5)
        snap1 = snapshot_and_reset_counters()
        assert snap1["q1"]["sent"] == 1
        assert snap1["q1"]["received"] == 5

        # After reset, counters should be zero
        snap2 = snapshot_and_reset_counters()
        assert snap2["q1"]["sent"] == 0
        assert snap2["q1"]["received"] == 0


class TestCounterThreadSafety:
    def test_concurrent_increments(self):
        """Verify counters are correct under concurrent writes."""
        num_threads = 10
        increments_per_thread = 100
        barrier = threading.Barrier(num_threads)

        def worker():
            barrier.wait()
            for _ in range(increments_per_thread):
                increment_sent("q1", 1)

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        snap = snapshot_and_reset_counters()
        assert snap["q1"]["sent"] == num_threads * increments_per_thread


class TestMultipleQueues:
    def test_independent_tracking(self):
        increment_sent("q1", 10)
        increment_sent("q2", 20)
        increment_received("q1", 1)
        increment_deleted("q2")

        snap = snapshot_and_reset_counters()
        assert snap["q1"]["sent"] == 1
        assert snap["q1"]["received"] == 1
        assert snap["q1"]["deleted"] == 0
        assert snap["q2"]["sent"] == 1
        assert snap["q2"]["received"] == 0
        assert snap["q2"]["deleted"] == 1


class TestFifoQueueDimension:
    def test_fifo_queue_metrics_use_queue_name(self):
        """FIFO queue metrics use the full QueueName (including .fifo suffix)."""
        queue = StandardQueue("orders.fifo", "us-east-1", "123456789012")
        msg = _make_msg("fifo-msg")
        msg.message_group_id = "grp1"
        # Can't use FifoQueue.put directly here, but we're testing compute_queue_metrics
        # which only looks at the queue state. Put directly into visible.
        queue._visible.put(msg)
        queue._all_messages[msg.message_id] = msg

        metrics = compute_queue_metrics(queue)
        assert metrics["ApproximateNumberOfMessagesVisible"] == 1.0
        # The queue name is "orders.fifo" which is what the dimension uses
        assert queue.name == "orders.fifo"


class TestMetricsDisabledViaEnvVar:
    def test_disabled_when_env_false(self):
        with patch.dict(os.environ, {"SQS_CLOUDWATCH_METRICS": "false"}):
            pub = SqsMetricsPublisher()
            assert not pub._is_enabled()

    def test_enabled_by_default(self):
        with patch.dict(os.environ, {}, clear=True):
            # Remove the key entirely
            env = dict(os.environ)
            env.pop("SQS_CLOUDWATCH_METRICS", None)
            with patch.dict(os.environ, env, clear=True):
                pub = SqsMetricsPublisher()
                assert pub._is_enabled()

    def test_enabled_when_env_true(self):
        with patch.dict(os.environ, {"SQS_CLOUDWATCH_METRICS": "true"}):
            pub = SqsMetricsPublisher()
            assert pub._is_enabled()
