"""Unit tests for SQS completeness: FIFO dedup, group ordering, purge, tags,
policies, message system attributes, SSE, redrive allow policy, message move tasks.
"""

import hashlib
import json
import time
import uuid
from unittest.mock import MagicMock

import pytest

from robotocore.services.sqs.models import (
    FifoQueue,
    MessageMoveTask,
    SqsMessage,
    SqsStore,
    StandardQueue,
)
from robotocore.services.sqs.provider import (
    SqsError,
    _cancel_message_move_task,
    _create_queue,
    _get_queue_attributes,
    _list_message_move_tasks,
    _list_queue_tags,
    _purge_queue,
    _receive_message,
    _send_message,
    _set_queue_attributes,
    _start_message_move_task,
    _tag_queue,
    _untag_queue,
)


def _msg(body="hello", **kwargs):
    return SqsMessage(
        message_id=str(uuid.uuid4()),
        body=body,
        md5_of_body=hashlib.md5(body.encode()).hexdigest(),
        **kwargs,
    )


def _store_with_queue(name="test-queue", **attrs):
    store = SqsStore()
    store.create_queue(name, "us-east-1", "123456789012", attrs or None)
    return store


# ==== FIFO Queue Enhancements ====


class TestFifoContentBasedDedup:
    def test_identical_bodies_deduped(self):
        q = FifoQueue(
            "q.fifo", "us-east-1", "123",
            attributes={"ContentBasedDeduplication": "true"},
        )
        q.put(_msg("same", message_group_id="g1"))
        q.put(_msg("same", message_group_id="g1"))
        results = q.receive(max_messages=10)
        assert len(results) == 1

    def test_different_bodies_not_deduped(self):
        q = FifoQueue(
            "q.fifo", "us-east-1", "123",
            attributes={"ContentBasedDeduplication": "true"},
        )
        q.put(_msg("body-a", message_group_id="g1"))
        q.put(_msg("body-b", message_group_id="g1"))
        r1 = q.receive()
        q.delete_message(r1[0][1])
        r2 = q.receive()
        assert len(r2) == 1
        assert r2[0][0].body == "body-b"

    def test_dedup_uses_sha256(self):
        q = FifoQueue(
            "q.fifo", "us-east-1", "123",
            attributes={"ContentBasedDeduplication": "true"},
        )
        msg = _msg("test body", message_group_id="g1")
        result = q.put(msg)
        expected_dedup = hashlib.sha256(b"test body").hexdigest()
        assert result.message_deduplication_id == expected_dedup


class TestFifoExplicitDedupId:
    def test_duplicate_dedup_id_rejected(self):
        q = FifoQueue("q.fifo", "us-east-1", "123")
        q.put(_msg("a", message_group_id="g1", message_deduplication_id="d1"))
        q.put(_msg("b", message_group_id="g1", message_deduplication_id="d1"))
        results = q.receive(max_messages=10)
        assert len(results) == 1
        assert results[0][0].body == "a"

    def test_different_dedup_ids_both_accepted(self):
        q = FifoQueue("q.fifo", "us-east-1", "123")
        q.put(_msg("a", message_group_id="g1", message_deduplication_id="d1"))
        q.put(_msg("b", message_group_id="g1", message_deduplication_id="d2"))
        r1 = q.receive()
        q.delete_message(r1[0][1])
        r2 = q.receive()
        assert r1[0][0].body == "a"
        assert r2[0][0].body == "b"

    def test_dedup_expires_after_interval(self):
        q = FifoQueue("q.fifo", "us-east-1", "123")
        q.put(_msg("a", message_group_id="g1", message_deduplication_id="d1"))
        # Manually expire the cache
        for k in list(q._dedup_cache):
            q._dedup_cache[k] = (q._dedup_cache[k][0], time.time() - 301)
        q.put(_msg("b", message_group_id="g1", message_deduplication_id="d1"))
        # Should have both now
        r1 = q.receive()
        q.delete_message(r1[0][1])
        r2 = q.receive()
        assert len(r2) == 1


class TestFifoGroupOrdering:
    def test_messages_in_same_group_ordered(self):
        q = FifoQueue("q.fifo", "us-east-1", "123")
        for i in range(5):
            q.put(_msg(f"msg-{i}", message_group_id="g1",
                        message_deduplication_id=f"d{i}"))
        results = []
        for _ in range(5):
            r = q.receive()
            if r:
                results.append(r[0][0].body)
                q.delete_message(r[0][1])
        assert results == [f"msg-{i}" for i in range(5)]

    def test_group_blocked_while_inflight(self):
        q = FifoQueue("q.fifo", "us-east-1", "123")
        q.put(_msg("m1", message_group_id="g1", message_deduplication_id="d1"))
        q.put(_msg("m2", message_group_id="g1", message_deduplication_id="d2"))
        r1 = q.receive()
        assert r1[0][0].body == "m1"
        # Group is blocked, second receive returns empty
        r2 = q.receive(wait_time_seconds=0)
        assert len(r2) == 0

    def test_different_groups_concurrent(self):
        q = FifoQueue("q.fifo", "us-east-1", "123",
                       attributes={"ContentBasedDeduplication": "true"})
        q.put(_msg("g1-m1", message_group_id="g1"))
        q.put(_msg("g2-m1", message_group_id="g2"))
        r1 = q.receive()
        r2 = q.receive()
        bodies = {r1[0][0].body, r2[0][0].body}
        assert bodies == {"g1-m1", "g2-m1"}

    def test_sequence_numbers_increment(self):
        q = FifoQueue("q.fifo", "us-east-1", "123")
        m1 = q.put(_msg("a", message_group_id="g1", message_deduplication_id="d1"))
        m2 = q.put(_msg("b", message_group_id="g1", message_deduplication_id="d2"))
        assert int(m2.sequence_number) > int(m1.sequence_number)


# ==== Queue Policies ====


class TestQueuePolicies:
    def test_set_and_get_policy(self):
        store = _store_with_queue()
        q = store.get_queue("test-queue")
        policy = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": "sqs:SendMessage",
                "Resource": q.arn,
            }],
        })
        mock_req = MagicMock()
        _set_queue_attributes(
            store,
            {
                "QueueUrl": q.url,
                "Attributes": {"Policy": policy},
            },
            "us-east-1", "123", mock_req,
        )
        result = _get_queue_attributes(
            store, {"QueueUrl": q.url}, "us-east-1", "123", mock_req,
        )
        assert result["Attributes"]["Policy"] == policy


# ==== Redrive Allow Policy ====


class TestRedriveAllowPolicy:
    def test_allow_all_default(self):
        q = StandardQueue("dlq", "us-east-1", "123")
        assert q.is_redrive_allowed("arn:aws:sqs:us-east-1:123:src") is True

    def test_deny_all(self):
        q = StandardQueue("dlq", "us-east-1", "123", attributes={
            "RedriveAllowPolicy": json.dumps({"redrivePermission": "denyAll"}),
        })
        assert q.is_redrive_allowed("arn:aws:sqs:us-east-1:123:src") is False

    def test_by_queue_allowed(self):
        policy = {
            "redrivePermission": "byQueue",
            "sourceQueueArns": ["arn:aws:sqs:us-east-1:123:allowed"],
        }
        q = StandardQueue("dlq", "us-east-1", "123", attributes={
            "RedriveAllowPolicy": json.dumps(policy),
        })
        assert q.is_redrive_allowed("arn:aws:sqs:us-east-1:123:allowed") is True
        assert q.is_redrive_allowed("arn:aws:sqs:us-east-1:123:denied") is False

    def test_get_attributes_includes_redrive_allow(self):
        policy = json.dumps({"redrivePermission": "denyAll"})
        q = StandardQueue("dlq", "us-east-1", "123", attributes={
            "RedriveAllowPolicy": policy,
        })
        attrs = q.get_attributes()
        assert attrs["RedriveAllowPolicy"] == policy


# ==== Message System Attributes ====


class TestMessageSystemAttributes:
    def test_send_with_system_attributes(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        mock_req.url.path = "/123456789012/test-queue"
        result = _send_message(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "MessageBody": "hello",
                "MessageSystemAttributes": {
                    "AWSTraceHeader": {
                        "DataType": "String",
                        "StringValue": "Root=1-abc-def",
                    },
                },
            },
            "us-east-1", "123456789012", mock_req,
        )
        assert "MessageId" in result

    def test_receive_includes_system_attributes(self):
        store = _store_with_queue()
        q = store.get_queue("test-queue")
        msg = _msg("hello")
        msg.system_attributes = {
            "AWSTraceHeader": {
                "DataType": "String",
                "StringValue": "Root=1-abc-def",
            },
        }
        q.put(msg)

        mock_req = MagicMock()
        mock_req.url.path = "/123456789012/test-queue"
        result = _receive_message(
            store,
            {"QueueUrl": "http://localhost:4566/123456789012/test-queue"},
            "us-east-1", "123456789012", mock_req,
        )
        msgs = result["Messages"]
        assert len(msgs) == 1
        assert msgs[0]["Attributes"]["AWSTraceHeader"] == "Root=1-abc-def"


# ==== Purge Queue ====


class TestPurgeQueue:
    def test_purge_clears_visible(self):
        store = _store_with_queue()
        q = store.get_queue("test-queue")
        for i in range(5):
            q.put(_msg(f"m{i}"))
        mock_req = MagicMock()
        _purge_queue(
            store,
            {"QueueUrl": "http://localhost:4566/123456789012/test-queue"},
            "us-east-1", "123", mock_req,
        )
        results = q.receive(max_messages=10, wait_time_seconds=0)
        assert len(results) == 0

    def test_purge_clears_inflight(self):
        store = _store_with_queue()
        q = store.get_queue("test-queue")
        q.put(_msg("m1"))
        q.receive(visibility_timeout=300)
        q.purge()
        assert len(q._inflight) == 0

    def test_purge_clears_delayed(self):
        store = _store_with_queue()
        q = store.get_queue("test-queue")
        q.put(_msg("m1", delay_seconds=300))
        q.purge()
        assert len(q._delayed) == 0


# ==== SSE Encryption (simulated) ====


class TestSSEAttributes:
    def test_sqs_managed_sse(self):
        store = SqsStore()
        mock_req = MagicMock()
        _create_queue(
            store,
            {
                "QueueName": "sse-queue",
                "Attributes": {"SqsManagedSseEnabled": "true"},
            },
            "us-east-1", "123", mock_req,
        )
        q = store.get_queue("sse-queue")
        attrs = q.get_attributes()
        assert attrs["SqsManagedSseEnabled"] == "true"

    def test_kms_sse(self):
        store = SqsStore()
        mock_req = MagicMock()
        _create_queue(
            store,
            {
                "QueueName": "kms-queue",
                "Attributes": {
                    "KmsMasterKeyId": "alias/my-key",
                    "KmsDataKeyReusePeriodSeconds": "600",
                },
            },
            "us-east-1", "123", mock_req,
        )
        q = store.get_queue("kms-queue")
        attrs = q.get_attributes()
        assert attrs["KmsMasterKeyId"] == "alias/my-key"
        assert attrs["KmsDataKeyReusePeriodSeconds"] == "600"


# ==== Tags ====


class TestQueueTags:
    def test_tag_queue(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        _tag_queue(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "Tags": {"env": "dev", "team": "platform"},
            },
            "us-east-1", "123", mock_req,
        )
        result = _list_queue_tags(
            store,
            {"QueueUrl": "http://localhost:4566/123456789012/test-queue"},
            "us-east-1", "123", mock_req,
        )
        assert result["Tags"]["env"] == "dev"
        assert result["Tags"]["team"] == "platform"

    def test_untag_queue(self):
        store = _store_with_queue()
        q = store.get_queue("test-queue")
        q.tags = {"env": "dev", "team": "platform"}
        mock_req = MagicMock()
        _untag_queue(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "TagKeys": ["env"],
            },
            "us-east-1", "123", mock_req,
        )
        result = _list_queue_tags(
            store,
            {"QueueUrl": "http://localhost:4566/123456789012/test-queue"},
            "us-east-1", "123", mock_req,
        )
        assert "env" not in result["Tags"]
        assert result["Tags"]["team"] == "platform"

    def test_tag_queue_query_protocol(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        _tag_queue(
            store,
            {
                "QueueUrl": "http://localhost:4566/123456789012/test-queue",
                "Tag.1.Key": "region",
                "Tag.1.Value": "east",
            },
            "us-east-1", "123", mock_req,
        )
        q = store.get_queue("test-queue")
        assert q.tags["region"] == "east"

    def test_list_tags_empty(self):
        store = _store_with_queue()
        mock_req = MagicMock()
        result = _list_queue_tags(
            store,
            {"QueueUrl": "http://localhost:4566/123456789012/test-queue"},
            "us-east-1", "123", mock_req,
        )
        assert result["Tags"] == {}


# ==== Message Move Tasks ====


class TestMessageMoveTasks:
    def _setup_dlq_pair(self):
        store = SqsStore()
        store.create_queue("dlq", "us-east-1", "123")
        store.create_queue("source", "us-east-1", "123", {
            "RedrivePolicy": json.dumps({
                "deadLetterTargetArn": "arn:aws:sqs:us-east-1:123:dlq",
                "maxReceiveCount": 3,
            }),
        })
        return store

    def test_start_move_task(self):
        store = self._setup_dlq_pair()
        dlq = store.get_queue("dlq")
        # Put messages in DLQ
        for i in range(3):
            dlq.put(_msg(f"dlq-msg-{i}"))

        mock_req = MagicMock()
        result = _start_message_move_task(
            store,
            {
                "SourceArn": "arn:aws:sqs:us-east-1:123:dlq",
            },
            "us-east-1", "123", mock_req,
        )
        assert "TaskHandle" in result

    def test_move_task_moves_messages(self):
        store = self._setup_dlq_pair()
        dlq = store.get_queue("dlq")
        source = store.get_queue("source")
        for i in range(3):
            dlq.put(_msg(f"dlq-msg-{i}"))

        mock_req = MagicMock()
        _start_message_move_task(
            store,
            {"SourceArn": "arn:aws:sqs:us-east-1:123:dlq"},
            "us-east-1", "123", mock_req,
        )

        # Messages should be in source queue now
        results = source.receive(max_messages=10, wait_time_seconds=0)
        assert len(results) == 3

    def test_move_task_with_explicit_destination(self):
        store = SqsStore()
        store.create_queue("dlq", "us-east-1", "123")
        store.create_queue("dest", "us-east-1", "123")
        dlq = store.get_queue("dlq")
        dlq.put(_msg("msg1"))

        mock_req = MagicMock()
        _start_message_move_task(
            store,
            {
                "SourceArn": "arn:aws:sqs:us-east-1:123:dlq",
                "DestinationArn": "arn:aws:sqs:us-east-1:123:dest",
            },
            "us-east-1", "123", mock_req,
        )
        dest = store.get_queue("dest")
        results = dest.receive(max_messages=10, wait_time_seconds=0)
        assert len(results) == 1

    def test_list_move_tasks(self):
        store = self._setup_dlq_pair()
        dlq = store.get_queue("dlq")
        dlq.put(_msg("msg"))

        mock_req = MagicMock()
        _start_message_move_task(
            store,
            {"SourceArn": "arn:aws:sqs:us-east-1:123:dlq"},
            "us-east-1", "123", mock_req,
        )
        result = _list_message_move_tasks(
            store,
            {"SourceArn": "arn:aws:sqs:us-east-1:123:dlq"},
            "us-east-1", "123", mock_req,
        )
        assert len(result["Results"]) == 1
        assert result["Results"][0]["Status"] == "COMPLETED"

    def test_cancel_move_task(self):
        store = SqsStore()
        store.create_queue("dlq", "us-east-1", "123")
        # Create a task directly in the store
        task = MessageMoveTask(
            task_handle="test-handle",
            source_arn="arn:aws:sqs:us-east-1:123:dlq",
            destination_arn=None,
            max_number_of_messages_per_second=500,
            status="RUNNING",
        )
        store._move_tasks["test-handle"] = task

        mock_req = MagicMock()
        result = _cancel_message_move_task(
            store,
            {"TaskHandle": "test-handle"},
            "us-east-1", "123", mock_req,
        )
        assert "ApproximateNumberOfMessagesMoved" in result
        assert task.status == "CANCELLED"

    def test_cancel_nonexistent_task(self):
        store = SqsStore()
        mock_req = MagicMock()
        with pytest.raises(SqsError) as exc:
            _cancel_message_move_task(
                store,
                {"TaskHandle": "nope"},
                "us-east-1", "123", mock_req,
            )
        assert "ResourceNotFoundException" in exc.value.code

    def test_start_move_nonexistent_source(self):
        store = SqsStore()
        mock_req = MagicMock()
        with pytest.raises(SqsError):
            _start_message_move_task(
                store,
                {"SourceArn": "arn:aws:sqs:us-east-1:123:nope"},
                "us-east-1", "123", mock_req,
            )


# ==== get_all_messages ====


class TestGetAllMessages:
    def test_includes_visible_and_delayed(self):
        q = StandardQueue("q", "us-east-1", "123")
        q.put(_msg("visible"))
        q.put(_msg("delayed", delay_seconds=300))
        msgs = q.get_all_messages()
        assert len(msgs) == 2
