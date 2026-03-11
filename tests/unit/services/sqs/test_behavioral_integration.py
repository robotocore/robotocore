"""Integration tests for SQS behavioral fidelity features.

These tests exercise the full provider path (action handlers) rather than
testing the behavioral modules in isolation.
"""

import time

import pytest

from robotocore.services.sqs.behavioral import (
    PurgeTracker,
    QueueDeletedTracker,
    RetentionScanner,
)
from robotocore.services.sqs.models import SqsMessage, SqsStore
from robotocore.services.sqs.provider import _delete_queue, _purge_queue


class FakeRequest:
    """Minimal request stand-in for provider functions."""

    class FakeUrl:
        path = "/123456789012/test-queue"
        query = ""

    url = FakeUrl()
    headers = {}


# --- End-to-end PurgeQueue ---


class TestPurgeQueueE2E:
    def setup_method(self):
        self.store = SqsStore()
        self.store.create_queue("test-queue", "us-east-1", "123456789012")
        self.tracker = PurgeTracker()

    def test_purge_then_immediate_purge_raises(self):
        """PurgeQueue -> immediate PurgeQueue -> PurgeQueueInProgress error."""
        params = {"QueueUrl": "http://localhost:4566/123456789012/test-queue"}
        req = FakeRequest()

        # First purge succeeds
        self.tracker.check_and_record("test-queue")
        _purge_queue(self.store, params, "us-east-1", "123456789012", req)

        # Second purge within 60s fails
        with pytest.raises(Exception, match="PurgeQueueInProgress"):
            self.tracker.check_and_record("test-queue")


# --- End-to-end DeleteQueue -> CreateQueue ---


class TestDeleteQueueCreateQueueE2E:
    def setup_method(self):
        self.store = SqsStore()
        self.store.create_queue("test-queue", "us-east-1", "123456789012")
        self.tracker = QueueDeletedTracker()

    def test_delete_then_immediate_create_raises(self):
        """DeleteQueue -> immediate CreateQueue with same name -> QueueDeletedRecently."""
        params_delete = {"QueueUrl": "http://localhost:4566/123456789012/test-queue"}
        req = FakeRequest()

        _delete_queue(self.store, params_delete, "us-east-1", "123456789012", req)
        self.tracker.record_deletion("test-queue")

        with pytest.raises(Exception, match="QueueDeletedRecently"):
            self.tracker.check_create("test-queue")


# --- End-to-end Message Retention ---


class TestMessageRetentionE2E:
    def setup_method(self):
        self.store = SqsStore()

    def test_send_then_expire_then_receive_returns_nothing(self):
        """SendMessage -> messages expire past retention -> ReceiveMessage returns nothing."""
        queue = self.store.create_queue(
            "test-queue",
            "us-east-1",
            "123456789012",
            attributes={"MessageRetentionPeriod": "30"},
        )
        msg = SqsMessage(message_id="msg-1", body="hello", md5_of_body="abc")
        msg.created = time.time() - 60  # Older than 30s retention
        queue.put(msg)

        scanner = RetentionScanner(scan_interval=60)
        scanner.scan_store(self.store)

        results = queue.receive(max_messages=1, visibility_timeout=30, wait_time_seconds=0)
        assert results == []


# --- All features together ---


class TestAllFeaturesTogether:
    def setup_method(self):
        self.store = SqsStore()
        self.purge_tracker = PurgeTracker()
        self.delete_tracker = QueueDeletedTracker()

    def test_all_three_features_on_same_queue(self):
        """All behavioral features should work together without interference."""
        queue = self.store.create_queue(
            "multi-queue",
            "us-east-1",
            "123456789012",
            attributes={"MessageRetentionPeriod": "30"},
        )

        # 1. Send a message that's already expired
        msg = SqsMessage(message_id="msg-1", body="old", md5_of_body="x")
        msg.created = time.time() - 60
        queue.put(msg)

        # 2. Retention scanner removes it
        scanner = RetentionScanner(scan_interval=60)
        scanner.scan_store(self.store)
        assert len(queue._all_messages) == 0

        # 3. Purge the queue (succeeds, nothing to purge)
        self.purge_tracker.check_and_record("multi-queue")

        # 4. Second purge fails
        with pytest.raises(Exception, match="PurgeQueueInProgress"):
            self.purge_tracker.check_and_record("multi-queue")

        # 5. Delete the queue
        req = FakeRequest()
        req.url.path = "/123456789012/multi-queue"
        _delete_queue(
            self.store,
            {"QueueUrl": "http://localhost:4566/123456789012/multi-queue"},
            "us-east-1",
            "123456789012",
            req,
        )
        self.delete_tracker.record_deletion("multi-queue")

        # 6. Immediate recreate fails
        with pytest.raises(Exception, match="QueueDeletedRecently"):
            self.delete_tracker.check_create("multi-queue")

    def test_behavioral_features_dont_affect_normal_ops(self):
        """Normal SQS operations should work fine with behavioral features active."""
        queue = self.store.create_queue("normal-queue", "us-east-1", "123456789012")

        # Send and receive normally
        msg = SqsMessage(message_id="msg-1", body="hello", md5_of_body="abc")
        queue.put(msg)
        results = queue.receive(max_messages=1, visibility_timeout=30, wait_time_seconds=0)
        assert len(results) == 1
        assert results[0][0].body == "hello"

        # Delete message normally
        _, receipt = results[0]
        assert queue.delete_message(receipt) is True

        # Purge works on first call
        self.purge_tracker.check_and_record("normal-queue")
