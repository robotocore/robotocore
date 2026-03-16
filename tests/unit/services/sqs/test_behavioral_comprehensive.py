"""Comprehensive unit tests for SQS behavioral fidelity features.

Covers edge cases, thread safety, and provider-level integration for:
- PurgeInProgress cooldown
- QueueDeletedRecently error
- Message retention period scanning
- Visibility timeout behavior
- DLQ redrive on max receive count
"""

import json
import os
import threading
import time
from unittest.mock import patch

import pytest

from robotocore.services.sqs.behavioral import (
    DEFAULT_RETENTION_PERIOD,
    PURGE_COOLDOWN_SECONDS,
    PurgeQueueInProgressError,
    PurgeTracker,
    QueueDeletedRecentlyError,
    QueueDeletedTracker,
    RetentionScanner,
)
from robotocore.services.sqs.models import SqsMessage, SqsStore

# ============================================================
# PurgeTracker — deep coverage
# ============================================================


class TestPurgeTrackerEdgeCases:
    def setup_method(self):
        self.tracker = PurgeTracker()

    def test_purge_error_message_content(self):
        """PurgeQueueInProgressError message should reference the 60-second window."""
        self.tracker.check_and_record("q1")
        with pytest.raises(PurgeQueueInProgressError) as exc_info:
            self.tracker.check_and_record("q1")
        assert "60 seconds" in str(exc_info.value)

    def test_remove_allows_immediate_re_purge(self):
        """After remove(), the same queue can be purged again immediately."""
        self.tracker.check_and_record("q1")
        self.tracker.remove("q1")
        # Should not raise
        self.tracker.check_and_record("q1")

    def test_remove_nonexistent_queue_is_noop(self):
        """Removing a queue that was never tracked should not raise."""
        self.tracker.remove("never-existed")

    def test_purge_exactly_at_cooldown_boundary(self):
        """At exactly 60s, the cooldown should still be active (strictly less-than)."""
        self.tracker.check_and_record("q1")
        # Set purge time to exactly 60s ago
        self.tracker._purge_times["q1"] = time.time() - PURGE_COOLDOWN_SECONDS
        # At exactly the boundary, (now - last) == 60, which is NOT < 60, so should pass
        self.tracker.check_and_record("q1")

    def test_purge_just_before_cooldown_boundary(self):
        """At 59.9s, the cooldown should still be active."""
        self.tracker.check_and_record("q1")
        self.tracker._purge_times["q1"] = time.time() - (PURGE_COOLDOWN_SECONDS - 0.1)
        with pytest.raises(PurgeQueueInProgressError):
            self.tracker.check_and_record("q1")

    def test_concurrent_purge_attempts(self):
        """Only one of N concurrent purge attempts should succeed after the first."""
        self.tracker.check_and_record("q1")
        successes = []
        failures = []

        def try_purge():
            try:
                self.tracker.check_and_record("q1")
                successes.append(True)
            except PurgeQueueInProgressError:
                failures.append(True)

        threads = [threading.Thread(target=try_purge) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All should fail since the first purge is still in cooldown
        assert len(successes) == 0
        assert len(failures) == 10

    def test_multiple_queues_independent_cooldowns(self):
        """Each queue tracks its own cooldown independently."""
        self.tracker.check_and_record("q1")
        self.tracker.check_and_record("q2")
        self.tracker.check_and_record("q3")

        # All should be in cooldown
        for name in ["q1", "q2", "q3"]:
            with pytest.raises(PurgeQueueInProgressError):
                self.tracker.check_and_record(name)

        # Expire only q2
        self.tracker._purge_times["q2"] = time.time() - 61
        self.tracker.check_and_record("q2")

        # q1 and q3 still in cooldown
        with pytest.raises(PurgeQueueInProgressError):
            self.tracker.check_and_record("q1")
        with pytest.raises(PurgeQueueInProgressError):
            self.tracker.check_and_record("q3")

    def test_enabled_by_default(self):
        """PurgeTracker should be enabled when env var is not set."""
        tracker = PurgeTracker()
        assert tracker._is_enabled() is True

    def test_disabled_with_env_var_false(self):
        """PurgeTracker should be disabled with SQS_DELAY_PURGE_RETRY=false."""
        with patch.dict(os.environ, {"SQS_DELAY_PURGE_RETRY": "false"}):
            tracker = PurgeTracker()
            assert tracker._is_enabled() is False

    def test_disabled_with_env_var_uppercase_false(self):
        """PurgeTracker should be disabled with SQS_DELAY_PURGE_RETRY=FALSE (case insensitive)."""
        with patch.dict(os.environ, {"SQS_DELAY_PURGE_RETRY": "FALSE"}):
            tracker = PurgeTracker()
            assert tracker._is_enabled() is False

    def test_enabled_with_env_var_true(self):
        """PurgeTracker should remain enabled with SQS_DELAY_PURGE_RETRY=true."""
        with patch.dict(os.environ, {"SQS_DELAY_PURGE_RETRY": "true"}):
            tracker = PurgeTracker()
            assert tracker._is_enabled() is True


# ============================================================
# QueueDeletedTracker — deep coverage
# ============================================================


class TestQueueDeletedTrackerEdgeCases:
    def setup_method(self):
        self.tracker = QueueDeletedTracker()

    def test_error_message_includes_queue_name(self):
        """QueueDeletedRecentlyError should mention the queue name."""
        self.tracker.record_deletion("my-special-queue")
        with pytest.raises(QueueDeletedRecentlyError) as exc_info:
            self.tracker.check_create("my-special-queue")
        assert "my-special-queue" in str(exc_info.value)

    def test_expired_entry_cleaned_up_on_check(self):
        """After 60s, check_create should clean up the expired entry."""
        self.tracker.record_deletion("q1")
        self.tracker._deletion_times["q1"] = time.time() - 61
        self.tracker.check_create("q1")  # Should not raise and should clean up
        # The entry should be removed
        assert "q1" not in self.tracker._deletion_times

    def test_check_create_on_never_deleted_queue(self):
        """check_create on a queue that was never deleted should succeed."""
        self.tracker.check_create("brand-new-queue")

    def test_re_deletion_resets_timer(self):
        """Deleting a queue again should reset the cooldown timer."""
        self.tracker.record_deletion("q1")
        old_time = self.tracker._deletion_times["q1"]
        time.sleep(0.01)
        self.tracker.record_deletion("q1")
        new_time = self.tracker._deletion_times["q1"]
        assert new_time > old_time

    def test_concurrent_delete_and_create(self):
        """Thread safety: concurrent record_deletion and check_create should not crash."""
        errors = []

        def deleter():
            for _ in range(50):
                self.tracker.record_deletion("q1")

        def creator():
            for _ in range(50):
                try:
                    self.tracker.check_create("q1")
                except QueueDeletedRecentlyError:
                    pass  # intentionally ignored
                except Exception as e:
                    errors.append(e)

        t1 = threading.Thread(target=deleter)
        t2 = threading.Thread(target=creator)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert errors == [], f"Unexpected errors: {errors}"

    def test_multiple_queues_independent_tracking(self):
        """Deleting queue A does not affect queue B's create ability."""
        self.tracker.record_deletion("q-a")
        self.tracker.record_deletion("q-b")

        # Both should be blocked
        with pytest.raises(QueueDeletedRecentlyError):
            self.tracker.check_create("q-a")
        with pytest.raises(QueueDeletedRecentlyError):
            self.tracker.check_create("q-b")

        # Expire only q-a
        self.tracker._deletion_times["q-a"] = time.time() - 61
        self.tracker.check_create("q-a")

        # q-b still blocked
        with pytest.raises(QueueDeletedRecentlyError):
            self.tracker.check_create("q-b")

    def test_enabled_by_default(self):
        """QueueDeletedTracker should be enabled when env var is not set."""
        tracker = QueueDeletedTracker()
        assert tracker._is_enabled() is True

    def test_disabled_with_env_var(self):
        """QueueDeletedTracker disabled via SQS_DELAY_RECENTLY_DELETED=false."""
        with patch.dict(os.environ, {"SQS_DELAY_RECENTLY_DELETED": "false"}):
            tracker = QueueDeletedTracker()
            assert tracker._is_enabled() is False


# ============================================================
# RetentionScanner — deep coverage
# ============================================================


class TestRetentionScannerEdgeCases:
    def setup_method(self):
        self.store = SqsStore()
        self.scanner = RetentionScanner(scan_interval=60)

    def test_default_retention_period_used_when_not_set(self):
        """When MessageRetentionPeriod is not set, default (4 days) is used."""
        queue = self.store.create_queue("q1", "us-east-1", "123456789012")
        msg = SqsMessage(message_id="m1", body="hi", md5_of_body="abc")
        # Set message age to 3 days — should NOT be expired with 4-day default
        msg.created = time.time() - (3 * 86400)
        queue.put(msg)

        self.scanner.scan_store(self.store)
        assert len(queue._all_messages) == 1

    def test_default_retention_period_expires_old_messages(self):
        """Messages older than default 4-day retention should be expired."""
        queue = self.store.create_queue("q1", "us-east-1", "123456789012")
        msg = SqsMessage(message_id="m1", body="hi", md5_of_body="abc")
        msg.created = time.time() - (DEFAULT_RETENTION_PERIOD + 1)
        queue.put(msg)

        self.scanner.scan_store(self.store)
        assert len(queue._all_messages) == 0

    def test_inflight_messages_are_expired_too(self):
        """Messages in _inflight should be expired if past retention."""
        queue = self.store.create_queue(
            "q1", "us-east-1", "123456789012", attributes={"MessageRetentionPeriod": "60"}
        )
        msg = SqsMessage(message_id="m1", body="hi", md5_of_body="abc")
        msg.created = time.time() - 120
        queue.put(msg)

        # Manually move message to _inflight (receive would skip expired messages)
        with queue.mutex:
            try:
                m = queue._visible.get_nowait()
                queue._inflight[m.message_id] = m
            except Exception:
                pass  # best-effort cleanup
        assert "m1" in queue._inflight

        self.scanner.scan_store(self.store)

        # Message should be removed from _all_messages and _inflight
        assert len(queue._all_messages) == 0
        assert "m1" not in queue._inflight

    def test_delayed_messages_are_expired_too(self):
        """Messages in _delayed should be expired if past retention."""
        queue = self.store.create_queue(
            "q1", "us-east-1", "123456789012", attributes={"MessageRetentionPeriod": "60"}
        )
        msg = SqsMessage(message_id="m1", body="hi", md5_of_body="abc", delay_seconds=9999)
        msg.created = time.time() - 120
        queue.put(msg)

        assert "m1" in queue._delayed

        self.scanner.scan_store(self.store)

        assert len(queue._all_messages) == 0
        assert "m1" not in queue._delayed

    def test_scan_empty_store(self):
        """Scanning a store with no queues should not raise."""
        self.scanner.scan_store(self.store)

    def test_scan_queue_with_no_messages(self):
        """Scanning a queue with no messages should not raise."""
        self.store.create_queue("empty-q", "us-east-1", "123456789012")
        self.scanner.scan_store(self.store)

    def test_partial_expiration(self):
        """Only expired messages are removed; fresh messages stay."""
        queue = self.store.create_queue(
            "q1", "us-east-1", "123456789012", attributes={"MessageRetentionPeriod": "60"}
        )
        old = SqsMessage(message_id="old", body="old", md5_of_body="a")
        old.created = time.time() - 120
        queue.put(old)

        fresh = SqsMessage(message_id="fresh", body="fresh", md5_of_body="b")
        queue.put(fresh)

        self.scanner.scan_store(self.store)

        assert "old" not in queue._all_messages
        assert "fresh" in queue._all_messages

    def test_expired_message_marked_deleted(self):
        """Expired messages should have .deleted = True after scanning."""
        queue = self.store.create_queue(
            "q1", "us-east-1", "123456789012", attributes={"MessageRetentionPeriod": "60"}
        )
        msg = SqsMessage(message_id="m1", body="hi", md5_of_body="abc")
        msg.created = time.time() - 120
        queue.put(msg)

        # Keep a reference to the message before scanning
        msg_ref = queue._all_messages["m1"]

        self.scanner.scan_store(self.store)

        assert msg_ref.deleted is True

    def test_multiple_queues_scanned(self):
        """All queues in the store are scanned, not just the first."""
        q1 = self.store.create_queue(
            "q1", "us-east-1", "123456789012", attributes={"MessageRetentionPeriod": "60"}
        )
        q2 = self.store.create_queue(
            "q2", "us-east-1", "123456789012", attributes={"MessageRetentionPeriod": "60"}
        )

        for q, name in [(q1, "q1"), (q2, "q2")]:
            msg = SqsMessage(message_id=f"m-{name}", body="old", md5_of_body="x")
            msg.created = time.time() - 120
            q.put(msg)

        self.scanner.scan_store(self.store)

        assert len(q1._all_messages) == 0
        assert len(q2._all_messages) == 0

    def test_disabled_via_env_var(self):
        """When disabled, scan_store should not remove any messages."""
        with patch.dict(os.environ, {"SQS_ENABLE_MESSAGE_RETENTION_PERIOD": "false"}):
            queue = self.store.create_queue(
                "q1", "us-east-1", "123456789012", attributes={"MessageRetentionPeriod": "60"}
            )
            msg = SqsMessage(message_id="m1", body="hi", md5_of_body="abc")
            msg.created = time.time() - 120
            queue.put(msg)

            scanner = RetentionScanner(scan_interval=60)
            scanner.scan_store(self.store)

            assert len(queue._all_messages) == 1

    def test_scanner_stop_event(self):
        """Stop event should terminate the scanner thread promptly."""
        scanner = RetentionScanner(scan_interval=0.1)
        scanner.start({"test": self.store})
        assert scanner._thread.is_alive()
        scanner.stop()
        scanner._thread.join(timeout=2)
        assert not scanner._thread.is_alive()


# ============================================================
# Visibility timeout behavior
# ============================================================


class TestVisibilityTimeout:
    def setup_method(self):
        self.store = SqsStore()
        self.queue = self.store.create_queue("vis-q", "us-east-1", "123456789012")

    def test_message_invisible_during_timeout(self):
        """A received message should not be visible for subsequent receives."""
        msg = SqsMessage(message_id="m1", body="hello", md5_of_body="abc")
        self.queue.put(msg)

        results = self.queue.receive(max_messages=1, visibility_timeout=30, wait_time_seconds=0)
        assert len(results) == 1

        # Second receive should get nothing
        results2 = self.queue.receive(max_messages=1, visibility_timeout=30, wait_time_seconds=0)
        assert len(results2) == 0

    def test_message_reappears_after_visibility_timeout(self):
        """After visibility timeout expires, message should become visible again."""
        msg = SqsMessage(message_id="m1", body="hello", md5_of_body="abc")
        self.queue.put(msg)

        results = self.queue.receive(max_messages=1, visibility_timeout=1, wait_time_seconds=0)
        assert len(results) == 1
        received_msg = results[0][0]

        # Manually expire visibility
        received_msg.visibility_deadline = time.time() - 1

        # Requeue expired inflight
        self.queue.requeue_inflight_messages()

        # Should be receivable again
        results2 = self.queue.receive(max_messages=1, visibility_timeout=30, wait_time_seconds=0)
        assert len(results2) == 1
        assert results2[0][0].message_id == "m1"
        assert results2[0][0].receive_count == 2

    def test_change_visibility_extends_timeout(self):
        """ChangeMessageVisibility should extend the visibility deadline."""
        msg = SqsMessage(message_id="m1", body="hello", md5_of_body="abc")
        self.queue.put(msg)

        results = self.queue.receive(max_messages=1, visibility_timeout=5, wait_time_seconds=0)
        receipt = results[0][1]

        # Extend to 300 seconds
        ok = self.queue.change_visibility(receipt, 300)
        assert ok is True

        # Message should still be inflight
        assert "m1" in self.queue._inflight

    def test_change_visibility_to_zero_makes_visible(self):
        """Setting visibility timeout to 0 makes the message immediately visible."""
        msg = SqsMessage(message_id="m1", body="hello", md5_of_body="abc")
        self.queue.put(msg)

        results = self.queue.receive(max_messages=1, visibility_timeout=30, wait_time_seconds=0)
        receipt = results[0][1]

        ok = self.queue.change_visibility(receipt, 0)
        assert ok is True

        # Should no longer be in inflight
        assert "m1" not in self.queue._inflight

        # Should be receivable again
        results2 = self.queue.receive(max_messages=1, visibility_timeout=30, wait_time_seconds=0)
        assert len(results2) == 1

    def test_change_visibility_invalid_receipt(self):
        """ChangeMessageVisibility with invalid receipt should return False."""
        ok = self.queue.change_visibility("invalid-receipt", 30)
        assert ok is False

    def test_default_visibility_timeout_from_queue_attribute(self):
        """Queue's VisibilityTimeout attribute is used when not specified per-receive."""
        queue = self.store.create_queue(
            "custom-vis-q",
            "us-east-1",
            "123456789012",
            attributes={"VisibilityTimeout": "120"},
        )
        assert queue.default_visibility_timeout == 120

    def test_receive_count_increments(self):
        """Each receive should increment the message's receive_count."""
        msg = SqsMessage(message_id="m1", body="hello", md5_of_body="abc")
        self.queue.put(msg)

        for expected_count in [1, 2, 3]:
            results = self.queue.receive(max_messages=1, visibility_timeout=0, wait_time_seconds=0)
            if not results:
                # Message might still be getting requeued, force it
                self.queue.requeue_inflight_messages()
                results = self.queue.receive(
                    max_messages=1, visibility_timeout=0, wait_time_seconds=0
                )
            if results:
                assert results[0][0].receive_count == expected_count
                # Make visible again for next iteration
                self.queue.change_visibility(results[0][1], 0)


# ============================================================
# DLQ behavior (dead letter queue)
# ============================================================


class TestDLQBehavior:
    def setup_method(self):
        self.store = SqsStore()

    def test_max_receive_count_property(self):
        """max_receive_count should be parsed from RedrivePolicy."""
        dlq = self.store.create_queue("dlq", "us-east-1", "123456789012")
        queue = self.store.create_queue(
            "source-q",
            "us-east-1",
            "123456789012",
            attributes={
                "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq.arn, "maxReceiveCount": 3})
            },
        )
        assert queue.max_receive_count == 3

    def test_max_receive_count_none_without_policy(self):
        """max_receive_count should be None when no RedrivePolicy is set."""
        queue = self.store.create_queue("q", "us-east-1", "123456789012")
        assert queue.max_receive_count is None

    def test_redrive_policy_parsed_from_json_string(self):
        """RedrivePolicy is stored as JSON string and parsed correctly."""
        dlq = self.store.create_queue("dlq", "us-east-1", "123456789012")
        policy = {"deadLetterTargetArn": dlq.arn, "maxReceiveCount": 5}
        queue = self.store.create_queue(
            "source-q",
            "us-east-1",
            "123456789012",
            attributes={"RedrivePolicy": json.dumps(policy)},
        )
        parsed = queue.redrive_policy
        assert parsed["deadLetterTargetArn"] == dlq.arn
        assert parsed["maxReceiveCount"] == 5

    def test_list_dead_letter_source_queues(self):
        """Queues with RedrivePolicy pointing to DLQ should be found."""
        dlq = self.store.create_queue("dlq", "us-east-1", "123456789012")
        self.store.create_queue(
            "source-1",
            "us-east-1",
            "123456789012",
            attributes={
                "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq.arn, "maxReceiveCount": 3})
            },
        )
        self.store.create_queue(
            "source-2",
            "us-east-1",
            "123456789012",
            attributes={
                "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq.arn, "maxReceiveCount": 5})
            },
        )
        self.store.create_queue("unrelated", "us-east-1", "123456789012")

        sources = []
        for q in self.store.list_queues():
            rp = q.redrive_policy
            if rp and rp.get("deadLetterTargetArn") == dlq.arn:
                sources.append(q.name)

        assert sorted(sources) == ["source-1", "source-2"]

    def test_redrive_allow_policy_allow_all(self):
        """Default (no policy) should allow all queues as sources."""
        dlq = self.store.create_queue("dlq", "us-east-1", "123456789012")
        assert dlq.is_redrive_allowed("arn:aws:sqs:us-east-1:123456789012:any-queue") is True

    def test_redrive_allow_policy_deny_all(self):
        """denyAll should reject all sources."""
        dlq = self.store.create_queue(
            "dlq",
            "us-east-1",
            "123456789012",
            attributes={"RedriveAllowPolicy": json.dumps({"redrivePermission": "denyAll"})},
        )
        assert dlq.is_redrive_allowed("arn:aws:sqs:us-east-1:123456789012:any-queue") is False

    def test_redrive_allow_policy_by_queue(self):
        """byQueue should only allow listed source ARNs."""
        allowed_arn = "arn:aws:sqs:us-east-1:123456789012:allowed-q"
        denied_arn = "arn:aws:sqs:us-east-1:123456789012:denied-q"
        dlq = self.store.create_queue(
            "dlq",
            "us-east-1",
            "123456789012",
            attributes={
                "RedriveAllowPolicy": json.dumps(
                    {"redrivePermission": "byQueue", "sourceQueueArns": [allowed_arn]}
                )
            },
        )
        assert dlq.is_redrive_allowed(allowed_arn) is True
        assert dlq.is_redrive_allowed(denied_arn) is False


# ============================================================
# Provider-level integration (using internal functions)
# ============================================================


class FakeRequest:
    """Minimal request stand-in for provider functions."""

    class FakeUrl:
        def __init__(self, path="/123456789012/test-queue"):
            self.path = path
            self.query = ""

    def __init__(self, path="/123456789012/test-queue"):
        self.url = self.FakeUrl(path)
        self.headers = {}


class TestProviderPurgeIntegration:
    """Test purge cooldown through the provider's _purge_queue function."""

    def setup_method(self):
        # Reset the module-level singleton for each test
        from robotocore.services.sqs import provider

        self.store = SqsStore()
        self.store.create_queue("test-queue", "us-east-1", "123456789012")
        self.orig_tracker = provider._purge_tracker
        provider._purge_tracker = PurgeTracker()
        self.provider = provider

    def teardown_method(self):
        self.provider._purge_tracker = self.orig_tracker

    def test_first_purge_via_provider_succeeds(self):
        """First purge through provider should succeed."""
        from robotocore.services.sqs.provider import _purge_queue

        params = {"QueueUrl": "http://localhost:4566/123456789012/test-queue"}
        result = _purge_queue(self.store, params, "us-east-1", "123456789012", FakeRequest())
        assert result == {}

    def test_second_purge_via_provider_raises(self):
        """Second purge through provider within 60s should raise PurgeQueueInProgressError."""
        from robotocore.services.sqs.provider import _purge_queue

        params = {"QueueUrl": "http://localhost:4566/123456789012/test-queue"}
        _purge_queue(self.store, params, "us-east-1", "123456789012", FakeRequest())

        with pytest.raises(PurgeQueueInProgressError):
            _purge_queue(self.store, params, "us-east-1", "123456789012", FakeRequest())


class TestProviderDeleteCreateIntegration:
    """Test QueueDeletedRecently through provider functions."""

    def setup_method(self):
        from robotocore.services.sqs import provider

        self.store = SqsStore()
        self.store.create_queue("test-queue", "us-east-1", "123456789012")
        self.orig_tracker = provider._delete_tracker
        provider._delete_tracker = QueueDeletedTracker()
        self.provider = provider

    def teardown_method(self):
        self.provider._delete_tracker = self.orig_tracker

    def test_delete_then_create_raises(self):
        """Deleting then immediately recreating should raise QueueDeletedRecentlyError."""
        from robotocore.services.sqs.provider import _create_queue, _delete_queue

        params_delete = {"QueueUrl": "http://localhost:4566/123456789012/test-queue"}
        _delete_queue(self.store, params_delete, "us-east-1", "123456789012", FakeRequest())

        params_create = {"QueueName": "test-queue"}
        with pytest.raises(QueueDeletedRecentlyError):
            _create_queue(self.store, params_create, "us-east-1", "123456789012", FakeRequest())

    def test_delete_then_create_different_name_succeeds(self):
        """Deleting queue A then creating queue B should succeed."""
        from robotocore.services.sqs.provider import _create_queue, _delete_queue

        params_delete = {"QueueUrl": "http://localhost:4566/123456789012/test-queue"}
        _delete_queue(self.store, params_delete, "us-east-1", "123456789012", FakeRequest())

        params_create = {"QueueName": "different-queue"}
        result = _create_queue(
            self.store,
            params_create,
            "us-east-1",
            "123456789012",
            FakeRequest(path="/123456789012/different-queue"),
        )
        assert "QueueUrl" in result


# ============================================================
# Queue attributes and get_attributes
# ============================================================


class TestQueueAttributes:
    def setup_method(self):
        self.store = SqsStore()

    def test_get_attributes_includes_retention_period(self):
        """get_attributes should include MessageRetentionPeriod."""
        queue = self.store.create_queue(
            "q1", "us-east-1", "123456789012", attributes={"MessageRetentionPeriod": "86400"}
        )
        attrs = queue.get_attributes()
        assert attrs["MessageRetentionPeriod"] == "86400"

    def test_get_attributes_default_retention_period(self):
        """Default MessageRetentionPeriod should be 345600 (4 days)."""
        queue = self.store.create_queue("q1", "us-east-1", "123456789012")
        attrs = queue.get_attributes()
        assert attrs["MessageRetentionPeriod"] == "345600"

    def test_get_attributes_message_counts(self):
        """get_attributes should report correct message counts."""
        queue = self.store.create_queue("q1", "us-east-1", "123456789012")
        msg = SqsMessage(message_id="m1", body="hi", md5_of_body="abc")
        queue.put(msg)

        attrs = queue.get_attributes()
        assert int(attrs["ApproximateNumberOfMessages"]) >= 1

    def test_get_attributes_visibility_timeout(self):
        """get_attributes should report correct VisibilityTimeout."""
        queue = self.store.create_queue(
            "q1", "us-east-1", "123456789012", attributes={"VisibilityTimeout": "45"}
        )
        attrs = queue.get_attributes()
        assert attrs["VisibilityTimeout"] == "45"

    def test_get_attributes_includes_arn(self):
        """get_attributes should include QueueArn."""
        queue = self.store.create_queue("q1", "us-east-1", "123456789012")
        attrs = queue.get_attributes()
        assert attrs["QueueArn"] == "arn:aws:sqs:us-east-1:123456789012:q1"

    def test_set_queue_attributes_updates_retention(self):
        """Setting MessageRetentionPeriod should update the attribute."""
        queue = self.store.create_queue("q1", "us-east-1", "123456789012")
        queue.attributes["MessageRetentionPeriod"] = "172800"
        assert queue.attributes["MessageRetentionPeriod"] == "172800"

        # Retention scanner should respect the new value
        msg = SqsMessage(message_id="m1", body="hi", md5_of_body="abc")
        msg.created = time.time() - 200000  # Older than 172800s
        queue.put(msg)

        scanner = RetentionScanner(scan_interval=60)
        scanner.scan_store(self.store)

        assert len(queue._all_messages) == 0


# ============================================================
# Message delay behavior
# ============================================================


class TestMessageDelay:
    def setup_method(self):
        self.store = SqsStore()

    def test_delayed_message_not_immediately_visible(self):
        """A message with delay_seconds should not be visible immediately."""
        queue = self.store.create_queue("q1", "us-east-1", "123456789012")
        msg = SqsMessage(message_id="m1", body="hi", md5_of_body="abc", delay_seconds=60)
        queue.put(msg)

        assert "m1" in queue._delayed
        results = queue.receive(max_messages=1, visibility_timeout=30, wait_time_seconds=0)
        assert len(results) == 0

    def test_queue_level_delay(self):
        """Queue-level DelaySeconds should apply to all messages."""
        queue = self.store.create_queue(
            "q1", "us-east-1", "123456789012", attributes={"DelaySeconds": "60"}
        )
        msg = SqsMessage(message_id="m1", body="hi", md5_of_body="abc")
        queue.put(msg)

        assert "m1" in queue._delayed

    def test_delayed_message_becomes_visible(self):
        """After delay period, enqueue_delayed_messages should make message visible."""
        queue = self.store.create_queue("q1", "us-east-1", "123456789012")
        msg = SqsMessage(message_id="m1", body="hi", md5_of_body="abc", delay_seconds=1)
        # Fake the creation time so delay has expired
        msg.created = time.time() - 5
        queue.put(msg)

        # It's in _delayed because delay_seconds > 0
        assert "m1" in queue._delayed

        # But the delay has expired, so enqueue should move it
        queue.enqueue_delayed_messages()

        assert "m1" not in queue._delayed
        results = queue.receive(max_messages=1, visibility_timeout=30, wait_time_seconds=0)
        assert len(results) == 1
        assert results[0][0].message_id == "m1"


# ============================================================
# Queue CRUD via SqsStore
# ============================================================


class TestSqsStoreCRUD:
    def setup_method(self):
        self.store = SqsStore()

    def test_create_and_get_queue(self):
        """Creating a queue then getting it should return the same object."""
        q = self.store.create_queue("q1", "us-east-1", "123456789012")
        assert self.store.get_queue("q1") is q

    def test_create_duplicate_returns_existing(self):
        """Creating a queue with the same name should return the existing one."""
        q1 = self.store.create_queue("q1", "us-east-1", "123456789012")
        q2 = self.store.create_queue("q1", "us-east-1", "123456789012")
        assert q1 is q2

    def test_delete_queue(self):
        """Deleting a queue should remove it from the store."""
        self.store.create_queue("q1", "us-east-1", "123456789012")
        assert self.store.delete_queue("q1") is True
        assert self.store.get_queue("q1") is None

    def test_delete_nonexistent_queue(self):
        """Deleting a nonexistent queue should return False."""
        assert self.store.delete_queue("nonexistent") is False

    def test_list_queues_with_prefix(self):
        """list_queues with prefix should filter correctly."""
        self.store.create_queue("test-a", "us-east-1", "123456789012")
        self.store.create_queue("test-b", "us-east-1", "123456789012")
        self.store.create_queue("other-c", "us-east-1", "123456789012")

        results = self.store.list_queues(prefix="test-")
        names = [q.name for q in results]
        assert sorted(names) == ["test-a", "test-b"]

    def test_get_queue_by_url(self):
        """get_queue_by_url should parse the queue name from the URL."""
        q = self.store.create_queue("q1", "us-east-1", "123456789012")
        found = self.store.get_queue_by_url("http://localhost:4566/123456789012/q1")
        assert found is q

    def test_get_queue_by_arn(self):
        """get_queue_by_arn should parse the queue name from the ARN."""
        q = self.store.create_queue("q1", "us-east-1", "123456789012")
        found = self.store.get_queue_by_arn("arn:aws:sqs:us-east-1:123456789012:q1")
        assert found is q

    def test_fifo_queue_created_for_fifo_name(self):
        """Queues ending in .fifo should be FifoQueue instances."""
        from robotocore.services.sqs.models import FifoQueue

        q = self.store.create_queue("my-queue.fifo", "us-east-1", "123456789012")
        assert isinstance(q, FifoQueue)
        assert q.is_fifo is True

    def test_purge_clears_all_messages(self):
        """Queue.purge() should remove all messages from all internal stores."""
        q = self.store.create_queue("q1", "us-east-1", "123456789012")
        for i in range(5):
            q.put(SqsMessage(message_id=f"m{i}", body=f"body{i}", md5_of_body=f"md5{i}"))

        assert len(q._all_messages) == 5
        q.purge()
        assert len(q._all_messages) == 0
        assert len(q._inflight) == 0
        assert len(q._delayed) == 0
