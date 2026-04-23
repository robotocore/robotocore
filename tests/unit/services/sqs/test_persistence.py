"""Unit tests for SQS native state persistence."""

import hashlib
import json
import logging
import time

import pytest

import robotocore.services.sqs.provider as sqs_provider_module
from robotocore.services.sqs.behavioral import (
    PurgeQueueInProgressError,
    QueueDeletedRecentlyError,
)
from robotocore.services.sqs.models import FifoQueue, SqsMessage, StandardQueue
from robotocore.services.sqs.provider import (
    _get_store,
    _reset_for_tests,
    export_state,
    load_state,
    register_state_handler,
)
from robotocore.state.manager import StateManager


@pytest.fixture(autouse=True)
def _clear_sqs_state(reset_state_manager_singleton_fixture):
    """Reset global SQS state and state-manager singleton around each test."""
    reset_state_manager_singleton_fixture()
    _reset_for_tests()
    yield
    _reset_for_tests()
    reset_state_manager_singleton_fixture()


def _msg(
    message_id: str,
    body: str,
    *,
    delay_seconds: int = 0,
    created: float | None = None,
) -> SqsMessage:
    message = SqsMessage(
        message_id=message_id,
        body=body,
        md5_of_body=hashlib.md5(body.encode()).hexdigest(),
        delay_seconds=delay_seconds,
    )
    if created is not None:
        message.created = created
    return message


def _fifo_msg(
    message_id: str,
    body: str,
    *,
    group_id: str,
    dedup_id: str,
    created: float | None = None,
) -> SqsMessage:
    message = _msg(message_id, body, created=created)
    message.message_group_id = group_id
    message.message_deduplication_id = dedup_id
    return message


class TestStandardQueueRoundTrip:
    def test_round_trip_preserves_visible_inflight_and_delayed(self):
        store = _get_store("ap-southeast-2", "111111111111")
        queue = store.create_queue(
            "persist-standard",
            "ap-southeast-2",
            "111111111111",
            {"VisibilityTimeout": "45"},
        )
        assert queue is not None
        queue.tags["env"] = "test"

        queue.put(_msg("inflight-1", "inflight-body"))
        receipt = queue.receive(max_messages=1, visibility_timeout=60)[0][1]
        queue.put(_msg("visible-1", "visible-body"))
        queue.put(_msg("delayed-1", "delayed-body", delay_seconds=30))

        load_state(export_state())

        restored = _get_store("ap-southeast-2", "111111111111").get_queue("persist-standard")
        assert restored is not None
        assert restored.tags == {"env": "test"}

        attrs = restored.get_attributes()
        assert attrs["ApproximateNumberOfMessages"] == "1"
        assert attrs["ApproximateNumberOfMessagesNotVisible"] == "1"
        assert attrs["ApproximateNumberOfMessagesDelayed"] == "1"

        assert restored.change_visibility(receipt, 0) is True
        received = restored.receive(max_messages=10, visibility_timeout=0)
        bodies = {message.body for message, _ in received}
        assert bodies == {"inflight-body", "visible-body"}

    def test_receipt_handle_still_usable_after_restore(self):
        store = _get_store("us-east-1", "111111111111")
        queue = store.create_queue("receipt-queue", "us-east-1", "111111111111")
        assert queue is not None

        queue.put(_msg("receipt-msg", "body"))
        receipt = queue.receive(max_messages=1, visibility_timeout=60)[0][1]

        load_state(export_state())

        restored = _get_store("us-east-1", "111111111111").get_queue("receipt-queue")
        assert restored is not None
        assert restored.delete_message(receipt) is True

        attrs = restored.get_attributes()
        assert attrs["ApproximateNumberOfMessages"] == "0"
        assert attrs["ApproximateNumberOfMessagesNotVisible"] == "0"

    def test_inflight_with_expired_visibility_is_redelivered_after_restore(self):
        store = _get_store("us-east-1", "111111111111")
        queue = store.create_queue("expired-inflight", "us-east-1", "111111111111")
        assert queue is not None

        queue.put(_msg("expired-msg", "body"))
        queue.receive(max_messages=1, visibility_timeout=30)
        inflight_message = next(iter(queue._inflight.values()))
        inflight_message.visibility_deadline = time.time() - 1

        load_state(export_state())

        restored = _get_store("us-east-1", "111111111111").get_queue("expired-inflight")
        assert restored is not None
        received = restored.receive(max_messages=1, visibility_timeout=30)
        assert [message.body for message, _ in received] == ["body"]

    def test_round_trip_is_idempotent(self):
        store = _get_store("us-east-1", "111111111111")
        queue = store.create_queue(
            "idempotent-queue",
            "us-east-1",
            "111111111111",
            {"VisibilityTimeout": "45"},
        )
        assert queue is not None
        queue.tags["env"] = "test"
        queue.put(_msg("visible-1", "visible-body"))
        queue.put(_msg("delayed-1", "delayed-body", delay_seconds=15))
        sqs_provider_module._purge_tracker.check_and_record("idempotent-purged")

        snapshot = export_state()
        load_state(snapshot)

        assert export_state() == snapshot

    def test_dlq_redrive_policy_survives_restore(self):
        store = _get_store("us-east-1", "111111111111")
        dlq = store.create_queue("orders-dlq", "us-east-1", "111111111111")
        assert dlq is not None
        queue = store.create_queue(
            "orders",
            "us-east-1",
            "111111111111",
            {
                "RedrivePolicy": json.dumps(
                    {
                        "maxReceiveCount": "1",
                        "deadLetterTargetArn": dlq.arn,
                    }
                )
            },
        )
        assert queue is not None

        queue.put(_msg("dlq-msg", "body"))
        load_state(export_state())

        restored_store = _get_store("us-east-1", "111111111111")
        restored_queue = restored_store.get_queue("orders")
        restored_dlq = restored_store.get_queue("orders-dlq")
        assert restored_queue is not None
        assert restored_dlq is not None

        restored_queue.receive(max_messages=1, visibility_timeout=0)
        restored_queue.receive(max_messages=1, visibility_timeout=0)

        dlq_received = restored_dlq.receive(max_messages=1, visibility_timeout=30)
        assert [message.body for message, _ in dlq_received] == ["body"]


class TestMultiStoreRoundTrip:
    def test_round_trip_preserves_multi_account_and_region_isolation(self):
        east_store = _get_store("us-east-1", "111111111111")
        west_store = _get_store("eu-west-1", "111111111111")
        other_store = _get_store("us-east-1", "222222222222")

        east_store.create_queue("east-queue", "us-east-1", "111111111111")
        west_store.create_queue("west-queue", "eu-west-1", "111111111111")
        other_store.create_queue("other-queue", "us-east-1", "222222222222")

        load_state(export_state())

        restored_east = _get_store("us-east-1", "111111111111")
        restored_west = _get_store("eu-west-1", "111111111111")
        restored_other = _get_store("us-east-1", "222222222222")

        assert restored_east.get_queue("east-queue") is not None
        assert restored_west.get_queue("west-queue") is not None
        assert restored_other.get_queue("other-queue") is not None
        assert restored_east.get_queue("other-queue") is None
        assert restored_other.get_queue("east-queue") is None

    def test_round_trip_preserves_move_tasks_and_recently_deleted(self):
        store = _get_store("us-east-1", "111111111111")
        source = store.create_queue("source-queue", "us-east-1", "111111111111")
        destination = store.create_queue("destination-queue", "us-east-1", "111111111111")
        deleted = store.create_queue("deleted-queue", "us-east-1", "111111111111")
        assert source is not None
        assert destination is not None
        assert deleted is not None

        source.put(_msg("move-1", "move-body"))
        move_task = store.start_message_move_task(source.arn, destination.arn)
        assert store.delete_queue("deleted-queue") is True

        load_state(export_state())

        restored_store = _get_store("us-east-1", "111111111111")
        restored_destination = restored_store.get_queue("destination-queue")
        restored_source = restored_store.get_queue("source-queue")
        assert restored_destination is not None
        assert restored_source is not None

        restored_move_task = restored_store.get_message_move_task(move_task.task_handle)
        assert restored_move_task is not None
        assert restored_move_task.status == "COMPLETED"
        assert restored_move_task.approximate_number_of_messages_moved == 1
        assert restored_destination.get_attributes()["ApproximateNumberOfMessages"] == "1"
        assert restored_source.get_attributes()["ApproximateNumberOfMessages"] == "0"
        assert restored_source.get_all_messages() == []
        assert "deleted-queue" in restored_store._recently_deleted


class TestBehavioralTrackerRoundTrip:
    def test_round_trip_preserves_purge_tracker(self):
        sqs_provider_module._purge_tracker.check_and_record("purged-queue")

        load_state(export_state())

        with pytest.raises(PurgeQueueInProgressError):
            sqs_provider_module._purge_tracker.check_and_record("purged-queue")

    def test_round_trip_preserves_recently_deleted_tracker(self):
        sqs_provider_module._delete_tracker.record_deletion("recently-deleted")

        load_state(export_state())

        with pytest.raises(QueueDeletedRecentlyError):
            sqs_provider_module._delete_tracker.check_create("recently-deleted")

    def test_load_state_warns_on_future_schema_version(self, caplog):
        store = _get_store("us-east-1", "111111111111")
        queue = store.create_queue("versioned-queue", "us-east-1", "111111111111")
        assert queue is not None

        snapshot = export_state()
        snapshot["schema_version"] = 2

        with caplog.at_level(logging.WARNING):
            load_state(snapshot)

        assert "sqs snapshot schema_version=2; expected 1" in caplog.text
        restored = _get_store("us-east-1", "111111111111")
        assert restored.get_queue("versioned-queue") is not None


class TestFifoQueueRoundTrip:
    def test_disk_round_trip_preserves_group_order_dedup_tags_and_attributes(self, tmp_path):
        store = _get_store("us-east-1", "222222222222")
        queue = store.create_queue(
            "orders.fifo",
            "us-east-1",
            "222222222222",
            {"FifoQueue": "true", "VisibilityTimeout": "45"},
        )
        assert isinstance(queue, FifoQueue)
        queue.tags["env"] = "test"

        queue.put(
            _fifo_msg(
                "fifo-1",
                "alpha-first",
                group_id="alpha",
                dedup_id="dedup-1",
                created=1.0,
            )
        )
        queue.put(
            _fifo_msg(
                "fifo-2",
                "alpha-second",
                group_id="alpha",
                dedup_id="dedup-2",
                created=2.0,
            )
        )
        queue.put(
            _fifo_msg(
                "fifo-3",
                "beta-first",
                group_id="beta",
                dedup_id="dedup-3",
                created=3.0,
            )
        )

        manager = StateManager(state_dir=str(tmp_path))
        register_state_handler(manager)
        manager.save(name="sqs-cache", services=["sqs"])

        _reset_for_tests()
        manager.load(name="sqs-cache", services=["sqs"])
        restored = _get_store("us-east-1", "222222222222").get_queue("orders.fifo")
        assert isinstance(restored, FifoQueue)
        assert restored.tags == {"env": "test"}
        assert restored.attributes["VisibilityTimeout"] == "45"

        received = restored.receive(max_messages=10, visibility_timeout=30)
        assert [message.body for message, _ in received] == [
            "alpha-first",
            "alpha-second",
            "beta-first",
        ]

        duplicate = _fifo_msg(
            "fifo-dup",
            "duplicate",
            group_id="alpha",
            dedup_id="dedup-2",
            created=4.0,
        )
        accepted_duplicate = restored.put(duplicate)
        assert accepted_duplicate.message_id == "fifo-2"

        fresh = _fifo_msg(
            "fifo-4",
            "alpha-third",
            group_id="alpha",
            dedup_id="dedup-4",
            created=5.0,
        )
        accepted_fresh = restored.put(fresh)
        assert accepted_fresh.sequence_number == "4"

    def test_restore_heapifies_message_groups(self):
        queue_state = {
            "queue_type": "fifo",
            "created": 1.0,
            "attributes": {"FifoQueue": "true"},
            "tags": {},
            "last_purged_at": None,
            "visible_ids": [],
            "inflight_ids": [],
            "delayed_ids": [],
            "messages": {
                "alpha-2": _fifo_msg(
                    "alpha-2",
                    "alpha-2",
                    group_id="alpha",
                    dedup_id="alpha-2",
                    created=2.0,
                ).snapshot_state(),
                "alpha-3": _fifo_msg(
                    "alpha-3",
                    "alpha-3",
                    group_id="alpha",
                    dedup_id="alpha-3",
                    created=3.0,
                ).snapshot_state(),
                "beta-1": _fifo_msg(
                    "beta-1",
                    "beta-1",
                    group_id="beta",
                    dedup_id="beta-1",
                    created=4.0,
                ).snapshot_state(),
            },
            "receipts": {},
            "dedup_cache": {},
            "message_groups": {
                "alpha": ["alpha-3", "alpha-2"],
                "beta": ["beta-1"],
            },
            "inflight_groups": [],
            "queued_groups": ["alpha", "beta"],
            "group_queue": ["alpha", "beta"],
            "sequence_counter": 3,
        }

        restored = StandardQueue.from_snapshot(
            queue_state,
            region="us-east-1",
            account_id="333333333333",
            name="damaged.fifo",
        )
        assert isinstance(restored, FifoQueue)

        received = restored.receive(max_messages=10, visibility_timeout=30)
        assert [message.body for message, _ in received] == [
            "alpha-2",
            "alpha-3",
            "beta-1",
        ]


class TestDiskRoundTripViaStateManager:
    def test_binary_message_attributes_survive_disk_round_trip(self, tmp_path):
        store = _get_store("us-east-1", "111111111111")
        queue = store.create_queue("binary-attrs", "us-east-1", "111111111111")
        assert queue is not None

        message = _msg("binary-msg", "body")
        message.message_attributes["blob"] = {
            "DataType": "Binary",
            "BinaryValue": b"\x00\x01\x02",
        }
        queue.put(message)

        manager = StateManager(state_dir=str(tmp_path))
        register_state_handler(manager)
        manager.save(name="binary-attrs", services=["sqs"])

        _reset_for_tests()
        manager.load(name="binary-attrs", services=["sqs"])

        restored = _get_store("us-east-1", "111111111111").get_queue("binary-attrs")
        assert restored is not None
        restored_message = restored.get_all_messages()[0]
        restored_attr = restored_message.message_attributes["blob"]
        assert restored_attr["DataType"] == "Binary"
        assert restored_attr["BinaryValue"] == b"\x00\x01\x02"
