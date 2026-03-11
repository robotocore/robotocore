"""Unit tests for SQS behavioral fidelity features."""

import os
import time
from unittest.mock import patch

import pytest

from robotocore.services.sqs.behavioral import (
    PurgeTracker,
    QueueDeletedTracker,
    RetentionScanner,
)
from robotocore.services.sqs.models import SqsMessage, SqsStore

# --- PurgeInProgress ---


class TestPurgeTracker:
    def setup_method(self):
        self.tracker = PurgeTracker()

    def test_first_purge_succeeds(self):
        """First purge on a queue should always succeed."""
        self.tracker.check_and_record("my-queue")
        # No exception = success

    def test_immediate_second_purge_fails(self):
        """Second purge within 60s should raise PurgeQueueInProgress."""
        self.tracker.check_and_record("my-queue")
        with pytest.raises(Exception, match="PurgeQueueInProgress"):
            self.tracker.check_and_record("my-queue")

    def test_second_purge_after_delay_succeeds(self):
        """Second purge after 60s should succeed."""
        self.tracker.check_and_record("my-queue")
        # Simulate time passing
        self.tracker._purge_times["my-queue"] = time.time() - 61
        self.tracker.check_and_record("my-queue")
        # No exception = success

    def test_disabled_via_env_var(self):
        """When SQS_DELAY_PURGE_RETRY=false, second purge succeeds immediately."""
        with patch.dict(os.environ, {"SQS_DELAY_PURGE_RETRY": "false"}):
            tracker = PurgeTracker()
            tracker.check_and_record("my-queue")
            tracker.check_and_record("my-queue")
            # No exception = success

    def test_different_queues_dont_interfere(self):
        """Purging queue A should not block purging queue B."""
        self.tracker.check_and_record("queue-a")
        self.tracker.check_and_record("queue-b")
        # No exception = success


# --- QueueDeletedRecently ---


class TestQueueDeletedTracker:
    def setup_method(self):
        self.tracker = QueueDeletedTracker()

    def test_create_after_delete_within_60s_fails(self):
        """Creating a queue with same name within 60s of deletion should fail."""
        self.tracker.record_deletion("my-queue")
        with pytest.raises(Exception, match="QueueDeletedRecently"):
            self.tracker.check_create("my-queue")

    def test_create_after_delete_after_60s_succeeds(self):
        """Creating a queue after 60s of deletion should succeed."""
        self.tracker.record_deletion("my-queue")
        self.tracker._deletion_times["my-queue"] = time.time() - 61
        self.tracker.check_create("my-queue")
        # No exception = success

    def test_disabled_via_env_var(self):
        """When SQS_DELAY_RECENTLY_DELETED=false, immediate recreate succeeds."""
        with patch.dict(os.environ, {"SQS_DELAY_RECENTLY_DELETED": "false"}):
            tracker = QueueDeletedTracker()
            tracker.record_deletion("my-queue")
            tracker.check_create("my-queue")
            # No exception = success

    def test_different_queue_name_not_affected(self):
        """Deleting queue A should not block creating queue B."""
        self.tracker.record_deletion("queue-a")
        self.tracker.check_create("queue-b")
        # No exception = success


# --- Message Retention Period ---


class TestRetentionScanner:
    def setup_method(self):
        self.store = SqsStore()

    def test_message_older_than_retention_is_removed(self):
        """Messages older than MessageRetentionPeriod should be removed."""
        queue = self.store.create_queue("test-q", "us-east-1", "123456789012")
        queue.attributes["MessageRetentionPeriod"] = "60"  # 60 seconds for test
        msg = SqsMessage(message_id="msg-1", body="hello", md5_of_body="abc")
        msg.created = time.time() - 120  # 2 minutes ago
        queue.put(msg)
        assert len(queue._all_messages) == 1

        scanner = RetentionScanner(scan_interval=60)
        scanner.scan_store(self.store)

        assert len(queue._all_messages) == 0

    def test_message_within_retention_is_kept(self):
        """Messages within MessageRetentionPeriod should not be removed."""
        queue = self.store.create_queue("test-q", "us-east-1", "123456789012")
        queue.attributes["MessageRetentionPeriod"] = "3600"  # 1 hour
        msg = SqsMessage(message_id="msg-1", body="hello", md5_of_body="abc")
        queue.put(msg)
        assert len(queue._all_messages) == 1

        scanner = RetentionScanner(scan_interval=60)
        scanner.scan_store(self.store)

        assert len(queue._all_messages) == 1

    def test_custom_retention_period_per_queue(self):
        """Each queue can have its own MessageRetentionPeriod."""
        q_short = self.store.create_queue(
            "short-q",
            "us-east-1",
            "123456789012",
            attributes={"MessageRetentionPeriod": "30"},
        )
        q_long = self.store.create_queue(
            "long-q",
            "us-east-1",
            "123456789012",
            attributes={"MessageRetentionPeriod": "3600"},
        )

        old_time = time.time() - 60  # 1 minute ago
        msg1 = SqsMessage(message_id="msg-1", body="short", md5_of_body="a")
        msg1.created = old_time
        q_short.put(msg1)

        msg2 = SqsMessage(message_id="msg-2", body="long", md5_of_body="b")
        msg2.created = old_time
        q_long.put(msg2)

        scanner = RetentionScanner(scan_interval=60)
        scanner.scan_store(self.store)

        assert len(q_short._all_messages) == 0  # Expired (60s > 30s retention)
        assert len(q_long._all_messages) == 1  # Kept (60s < 3600s retention)

    def test_disabled_via_env_var(self):
        """When SQS_ENABLE_MESSAGE_RETENTION_PERIOD=false, no messages are removed."""
        with patch.dict(os.environ, {"SQS_ENABLE_MESSAGE_RETENTION_PERIOD": "false"}):
            queue = self.store.create_queue("test-q", "us-east-1", "123456789012")
            queue.attributes["MessageRetentionPeriod"] = "60"
            msg = SqsMessage(message_id="msg-1", body="hello", md5_of_body="abc")
            msg.created = time.time() - 120
            queue.put(msg)

            scanner = RetentionScanner(scan_interval=60)
            scanner.scan_store(self.store)

            assert len(queue._all_messages) == 1  # Not removed

    def test_scan_interval_is_configurable(self):
        """RetentionScanner accepts a custom scan interval."""
        scanner = RetentionScanner(scan_interval=120)
        assert scanner.scan_interval == 120

    def test_scanner_thread_starts_and_stops(self):
        """RetentionScanner thread should start and stop cleanly."""
        scanner = RetentionScanner(scan_interval=1)
        scanner.start({"test": self.store})
        assert scanner._thread is not None
        assert scanner._thread.is_alive()
        scanner.stop()
        scanner._thread.join(timeout=3)
        assert not scanner._thread.is_alive()
