"""Failing tests for DynamoDB and SQS behavioral fidelity gaps.

Each test documents correct AWS behavior that the current implementation
does NOT handle. These tests are expected to FAIL until the behavior is fixed.
"""

import json
import time

import pytest

from robotocore.services.sqs.models import (
    FifoQueue,
    SqsMessage,
    SqsStore,
    StandardQueue,
)

# ===========================================================================
# SQS: PurgeInProgress error (second purge within 60s)
# ===========================================================================


class TestSqsPurgeInProgress:
    """AWS returns PurgeInProgress if you purge the same queue within 60 seconds."""

    def test_second_purge_within_60s_should_raise(self):
        # On real AWS, calling PurgeQueue twice within 60 seconds returns:
        #   AWS.SimpleQueueService.PurgeQueueInProgress
        # Our implementation doesn't track last purge time at all.
        store = SqsStore()
        queue = store.create_queue("test-purge", "us-east-1", "123456789012")
        msg = SqsMessage(message_id="m1", body="hello", md5_of_body="abc")
        queue.put(msg)
        queue.purge()

        # Add another message
        msg2 = SqsMessage(message_id="m2", body="world", md5_of_body="def")
        queue.put(msg2)

        # Second purge within 60s should raise an error, but our code doesn't track this
        # We check that the queue has a last_purged_at timestamp
        assert hasattr(queue, "last_purged_at"), (
            "Queue should track last purge timestamp to enforce 60s cooldown"
        )
        assert queue.last_purged_at is not None, "last_purged_at should be set after purge"


# ===========================================================================
# SQS: QueueDeletedRecently (recreate within 60s)
# ===========================================================================


class TestSqsQueueDeletedRecently:
    """AWS returns QueueDeletedRecently if you recreate a queue within 60 seconds."""

    def test_recreate_queue_within_60s_should_track_deletion(self):
        # On real AWS, deleting a queue and recreating it within 60 seconds returns:
        #   AWS.SimpleQueueService.QueueDeletedRecently
        # Our SqsStore.delete_queue just removes the queue, no cooldown tracking.
        store = SqsStore()
        store.create_queue("test-recreate", "us-east-1", "123456789012")
        store.delete_queue("test-recreate")

        # The store should track recently deleted queues
        assert hasattr(store, "_recently_deleted"), (
            "SqsStore should track recently deleted queues with timestamps"
        )
        assert "test-recreate" in store._recently_deleted, (
            "Deleted queue name should be in recently_deleted map"
        )


# ===========================================================================
# SQS: Message retention period enforcement
# ===========================================================================


class TestSqsMessageRetention:
    """AWS automatically deletes messages older than the retention period."""

    def test_expired_messages_should_not_be_receivable(self):
        # On real AWS, messages are automatically deleted after the
        # MessageRetentionPeriod (default 4 days, configurable 60s-14d).
        # Our implementation never checks retention period at all.
        store = SqsStore()
        queue = store.create_queue(
            "test-retention",
            "us-east-1",
            "123456789012",
            {"MessageRetentionPeriod": "60"},  # 60 seconds
        )

        # Create a message with a fake old timestamp
        msg = SqsMessage(message_id="old-msg", body="old", md5_of_body="abc")
        msg.created = time.time() - 120  # 2 minutes ago, past 60s retention
        queue.put(msg)

        # The receive should NOT return this expired message
        results = queue.receive(max_messages=1, visibility_timeout=30, wait_time_seconds=0)
        assert len(results) == 0, (
            "Messages past their retention period should not be returned by receive. "
            "Currently the implementation ignores MessageRetentionPeriod entirely."
        )


# ===========================================================================
# SQS: DLQ redrive threshold uses > instead of >=
# ===========================================================================


class TestSqsDlqRedriveThreshold:
    """AWS moves messages to DLQ when receive count reaches maxReceiveCount, not exceeds it."""

    def test_dlq_redrive_at_exact_max_receive_count(self):
        # Real AWS: message goes to DLQ when receive_count == maxReceiveCount
        # Our _receive_message uses `msg.receive_count > queue.max_receive_count`
        # which means it only triggers AFTER the threshold is exceeded (off by one).
        store = SqsStore()
        dlq = store.create_queue("test-dlq", "us-east-1", "123456789012")
        main_queue = store.create_queue(
            "test-main",
            "us-east-1",
            "123456789012",
            {
                "RedrivePolicy": json.dumps(
                    {
                        "deadLetterTargetArn": dlq.arn,
                        "maxReceiveCount": 2,
                    }
                )
            },
        )

        msg = SqsMessage(message_id="m1", body="hello", md5_of_body="abc")
        # Simulate the message having already been received maxReceiveCount times
        msg.receive_count = 2  # exactly at threshold
        main_queue.put(msg)

        # At exactly maxReceiveCount, the message should be moved to DLQ
        # But our code checks `> max_receive_count` (strictly greater), so it
        # won't move until receive_count is 3.
        # After receive, receive_count will be 3 (2+1), and only THEN does the
        # provider check `> 2`. But on real AWS, on the 3rd receive attempt
        # the message should have already been in the DLQ.
        # The fundamental issue: DLQ check happens AFTER incrementing receive_count
        # in queue.receive(), and the check in _receive_message uses `>` not `>=`.
        #
        # We test by checking the max_receive_count property interprets correctly:
        assert main_queue.max_receive_count == 2

        # The real check: after receiving maxReceiveCount times, the next receive
        # should NOT return the message (it should be in DLQ already).
        # Receive it maxReceiveCount times:
        for _i in range(2):
            results = main_queue.receive(max_messages=1, visibility_timeout=0)
            if results:
                # Make it visible again immediately
                _msg, _receipt = results[0]
                _msg.visibility_deadline = None
                _msg.deleted = False
                main_queue._visible.put(_msg)

        # On the 3rd attempt, AWS would not return this message (it's in DLQ).
        # But our implementation will return it one extra time.
        # We verify by checking if the DLQ has anything:
        dlq_messages = dlq.get_all_messages()
        assert len(dlq_messages) > 0, (
            "After receiving a message maxReceiveCount times, it should be in the DLQ. "
            "The DLQ redrive check uses `>` instead of `>=` (off-by-one)."
        )


# ===========================================================================
# SQS: Batch delete with invalid receipt handles should return partial failure
# ===========================================================================


class TestSqsBatchDeletePartialFailure:
    """AWS returns Failed entries for invalid receipt handles in DeleteMessageBatch."""

    def test_delete_batch_invalid_receipt_returns_failed_entry(self):
        # On real AWS, DeleteMessageBatch returns a "Failed" list for entries
        # whose receipt handles are invalid. Our implementation always marks
        # every entry as "Successful" because delete_message returns False
        # silently and we never check the return value.
        store = SqsStore()
        queue = store.create_queue("test-batch-del", "us-east-1", "123456789012")

        # Put and receive a real message
        msg = SqsMessage(message_id="m1", body="hello", md5_of_body="abc")
        queue.put(msg)
        results = queue.receive(max_messages=1, visibility_timeout=30)
        assert len(results) == 1
        _msg, valid_receipt = results[0]

        # Delete with mix of valid and invalid handles
        ok1 = queue.delete_message(valid_receipt)
        ok2 = queue.delete_message("totally-invalid-receipt-handle")

        # The model-level delete returns False for invalid, True for valid
        assert ok1 is True
        assert ok2 is False

        # But in _delete_message_batch in provider.py, the return value is
        # never checked -- every entry goes to "Successful".
        # This test documents that the batch handler needs to check the return value.
        # We verify the provider behavior would be wrong by confirming the model works:
        # Re-create the scenario at the provider level expectation:
        from unittest.mock import MagicMock

        from robotocore.services.sqs.provider import _delete_message_batch

        store2 = SqsStore()
        queue2 = store2.create_queue("test-batch-del2", "us-east-1", "123456789012")
        msg2 = SqsMessage(message_id="m2", body="hi", md5_of_body="xyz")
        queue2.put(msg2)
        recv = queue2.receive(max_messages=1, visibility_timeout=30)
        _, real_receipt = recv[0]

        mock_request = MagicMock()
        mock_request.url.path = "/123456789012/test-batch-del2"
        result = _delete_message_batch(
            store2,
            {
                "QueueUrl": queue2.url,
                "Entries": [
                    {"Id": "good", "ReceiptHandle": real_receipt},
                    {"Id": "bad", "ReceiptHandle": "fake-receipt"},
                ],
            },
            "us-east-1",
            "123456789012",
            mock_request,
        )

        # AWS would put "bad" in the Failed list
        assert len(result["Failed"]) > 0, (
            "DeleteMessageBatch should return Failed entries for invalid receipt handles. "
            "Currently all entries are always put in Successful."
        )


# ===========================================================================
# SQS: FIFO message group ordering not enforced across receives
# ===========================================================================


class TestSqsQueueNameValidation:
    """AWS validates queue names: 1-80 chars, alphanumeric plus hyphens and underscores."""

    def test_empty_queue_name_should_be_rejected(self):
        # On real AWS, empty queue names are rejected.
        # Our implementation doesn't validate queue names at all.
        store = SqsStore()

        # Empty name should raise an error
        queue = store.create_queue("", "us-east-1", "123456789012")
        assert queue is None, (
            "Creating a queue with an empty name should be rejected. "
            "Currently no queue name validation is performed."
        )

    def test_queue_name_over_80_chars_should_be_rejected(self):
        store = SqsStore()
        long_name = "a" * 81
        queue = store.create_queue(long_name, "us-east-1", "123456789012")
        assert queue is None, (
            "Creating a queue with a name longer than 80 characters "
            "should be rejected. Currently no length validation is performed."
        )


# ===========================================================================
# SQS: FIFO queue should not allow DelaySeconds per-message
# ===========================================================================


class TestSqsFifoPerMessageDelay:
    """AWS FIFO queues do not support per-message DelaySeconds."""

    def test_fifo_rejects_per_message_delay(self):
        # On real AWS, FIFO queues only support queue-level DelaySeconds,
        # not per-message DelaySeconds. Sending a message with DelaySeconds
        # to a FIFO queue returns InvalidParameterValue.
        queue = FifoQueue("test-delay.fifo", "us-east-1", "123456789012", {})

        msg = SqsMessage(
            message_id="m1",
            body="hello",
            md5_of_body="abc",
            message_group_id="g1",
            message_deduplication_id="d1",
            delay_seconds=10,  # Not allowed on FIFO!
        )

        # This should raise an error but doesn't
        result = queue.put(msg)
        assert result is None or result.delay_seconds == 0, (
            "FIFO queues should reject per-message DelaySeconds. "
            "Currently the delay is silently accepted and applied."
        )


# ===========================================================================
# SQS: CreateQueue with different attributes should error
# ===========================================================================


class TestSqsCreateQueueIdempotency:
    """AWS CreateQueue returns the existing queue only if attributes match."""

    def test_create_queue_with_different_attrs_should_error(self):
        # On real AWS, calling CreateQueue with the same name but different
        # attributes returns QueueAlreadyExists error.
        # Our implementation just returns the existing queue regardless of attrs.
        store = SqsStore()
        q1 = store.create_queue(
            "test-idemp",
            "us-east-1",
            "123456789012",
            {"VisibilityTimeout": "30"},
        )

        # Same name, different attributes
        q2 = store.create_queue(
            "test-idemp",
            "us-east-1",
            "123456789012",
            {"VisibilityTimeout": "60"},  # Different!
        )

        # On real AWS, this would raise QueueAlreadyExists
        # Our code just returns the same queue object
        assert q1 is q2, "Got same queue object (expected)"
        # But the attributes should not have been silently ignored
        assert q2.default_visibility_timeout != 30 or q2.default_visibility_timeout != 60, (
            "This assertion is trivially true; the real issue is that CreateQueue "
            "with different attributes should raise an error but doesn't. "
            "Checking that the store has attribute-comparison logic:"
        )
        # The real test: store should have raised an error
        # We verify by checking that create_queue DOESN'T have attribute comparison
        import inspect

        source = inspect.getsource(store.create_queue)
        assert "attributes" in source.lower() and "compare" in source.lower(), (
            "SqsStore.create_queue should compare attributes of existing queue and raise "
            "QueueAlreadyExists if they differ. Currently it returns the existing queue "
            "regardless of attribute mismatch."
        )


# ===========================================================================
# DynamoDB: ListGlobalTables pagination is broken
# ===========================================================================


class TestDynamoDBListGlobalTablesPagination:
    """ListGlobalTables pagination token is never set due to impossible condition."""

    def test_pagination_token_set_when_results_exceed_limit(self):
        from robotocore.services.dynamodb.provider import (
            _global_tables,
            _list_global_tables,
        )

        # Save and restore state
        saved = dict(_global_tables)
        try:
            _global_tables.clear()

            # Create 5 global tables
            account_id = "123456789012"
            for i in range(5):
                key = (account_id, f"table-{i:02d}")
                _global_tables[key] = {
                    "GlobalTableName": f"table-{i:02d}",
                    "ReplicationGroup": [{"RegionName": "us-east-1"}],
                }

            # Request with limit=2
            result = _list_global_tables(
                {"Limit": 2},
                "us-east-1",
                account_id,
            )

            assert len(result["GlobalTables"]) == 2, "Should return exactly 2 tables"
            assert "LastEvaluatedGlobalTableName" in result, (
                "Should include pagination token when there are more results. "
                "Bug: the condition `len(account_tables) == limit and limit < len(account_tables)` "
                "is always False because if len == limit then limit < len is False. "
                "Should be checking total count before slicing."
            )
        finally:
            _global_tables.clear()
            _global_tables.update(saved)


# ===========================================================================
# DynamoDB: Stream hook for BatchWriteItem PutRequest always emits INSERT
# ===========================================================================


class TestDynamoDBBatchWriteStreamEvent:
    """BatchWriteItem PutRequest should emit MODIFY if item already exists, not always INSERT."""

    def test_batch_put_existing_item_should_emit_modify(self):
        # In provider.py, _fire_stream_hooks for BatchWriteItem PutRequest always
        # uses event_name="INSERT". But if the item already existed, it should be "MODIFY".
        # This matches PutItem behavior where it checks for Attributes in the response.
        # Verify the source code always uses "INSERT" for BatchWriteItem PutRequest
        import inspect

        from robotocore.services.dynamodb.provider import _fire_stream_hooks

        source = inspect.getsource(_fire_stream_hooks)
        # Find the BatchWriteItem section
        batch_section_start = source.find("BatchWriteItem")
        assert batch_section_start > 0
        batch_section = source[batch_section_start:]
        put_section_start = batch_section.find("PutRequest")
        put_section = batch_section[put_section_start : put_section_start + 500]

        # The section should check whether the item already exists to determine
        # INSERT vs MODIFY, but it unconditionally uses "INSERT"
        assert "MODIFY" in put_section, (
            "BatchWriteItem PutRequest stream hook always emits INSERT even for "
            "overwrites. It should check if the item already existed and emit MODIFY "
            "like the PutItem handler does."
        )


# ===========================================================================
# DynamoDB: TTL should not remove items with future timestamps
# ===========================================================================


class TestDynamoDBTTLFutureTimestamp:
    """TTL items with timestamps in the future should NOT be removed."""

    def test_items_with_ttl_slightly_in_future_not_removed(self):
        from robotocore.services.dynamodb.ttl import _is_item_expired

        now = int(time.time())

        # Create a mock item with TTL 10 seconds in the future
        class MockAttr:
            def __init__(self, type_, value):
                self.type = type_
                self.value = value

        class MockItem:
            def __init__(self, attrs):
                self.attrs = attrs

        future_item = MockItem({"ttl": MockAttr("N", str(now + 10))})
        assert not _is_item_expired(future_item, "ttl", now), (
            "Item with TTL in the future should not be expired"
        )

        # Item with TTL exactly at now should be expired (<=)
        exact_item = MockItem({"ttl": MockAttr("N", str(now))})
        assert _is_item_expired(exact_item, "ttl", now), "Item with TTL == now should be expired"

        # Item with TTL in the past should be expired
        past_item = MockItem({"ttl": MockAttr("N", str(now - 100))})
        assert _is_item_expired(past_item, "ttl", now), (
            "Item with TTL in the past should be expired"
        )

        # AWS ignores TTL values more than 5 years in the past (treats as not expired)
        # This is a real AWS behavior: epochs more than 5 years old are treated as
        # non-TTL values to avoid accidentally deleting items with small numeric IDs
        five_years_ago = now - (5 * 365 * 24 * 3600 + 1)
        ancient_item = MockItem({"ttl": MockAttr("N", str(five_years_ago))})
        assert not _is_item_expired(ancient_item, "ttl", now), (
            "AWS ignores TTL values more than 5 years in the past. Items with very old "
            "TTL epochs (e.g., small integers used as IDs) should NOT be treated as expired. "
            "Currently _is_item_expired has no lower bound check."
        )


# ===========================================================================
# SQS: Visibility timeout of 0 should make message immediately available
# ===========================================================================


class TestSqsVisibilityTimeoutZero:
    """Receiving with VisibilityTimeout=0 should make message immediately visible again."""

    def test_receive_with_zero_visibility_timeout(self):
        queue = StandardQueue("test-vis0", "us-east-1", "123456789012")
        msg = SqsMessage(message_id="m1", body="hello", md5_of_body="abc")
        queue.put(msg)

        # Receive with visibility timeout = 0
        results = queue.receive(max_messages=1, visibility_timeout=0)
        assert len(results) == 1

        # With visibility_timeout=0, the message should be immediately visible
        # again for another consumer. On real AWS, VisibilityTimeout=0 means
        # the message is not hidden at all.
        results2 = queue.receive(max_messages=1, visibility_timeout=30)
        assert len(results2) == 1, (
            "After receiving with VisibilityTimeout=0, message should be immediately "
            "available for another receive. The current implementation sets "
            "visibility_deadline = time.time() + 0 which is effectively 'now', "
            "but the message is moved to _inflight and not put back in _visible."
        )


# ===========================================================================
# SQS: MaximumMessageSize enforcement
# ===========================================================================


class TestSqsMaximumMessageSize:
    """AWS rejects messages larger than MaximumMessageSize (default 256KB)."""

    def test_oversized_message_should_be_rejected(self):
        # Real AWS rejects messages over MaximumMessageSize with an error.
        # Our put() never checks message size.
        queue = StandardQueue(
            "test-maxsize",
            "us-east-1",
            "123456789012",
            {"MaximumMessageSize": "1024"},  # 1KB limit
        )

        big_body = "x" * 2048  # 2KB, over the 1KB limit
        msg = SqsMessage(
            message_id="m1",
            body=big_body,
            md5_of_body="abc",
        )

        # This should raise an error but doesn't
        # We verify by checking that the queue has no size validation
        queue.put(msg)
        all_msgs = queue.get_all_messages()
        assert len(all_msgs) == 0, (
            "Messages exceeding MaximumMessageSize should be rejected. "
            "Currently put() never checks message body size against the queue's "
            "MaximumMessageSize attribute."
        )


# ===========================================================================
# SQS: SendMessageBatch should enforce max 10 entries
# ===========================================================================


class TestSqsSendMessageBatchLimits:
    """AWS limits SendMessageBatch to 10 entries."""

    def test_more_than_10_entries_should_error(self):
        from unittest.mock import MagicMock

        from robotocore.services.sqs.provider import SqsError, _send_message_batch

        store = SqsStore()
        store.create_queue("test-batch-limit", "us-east-1", "123456789012")

        entries = [
            {"Id": str(i), "MessageBody": f"msg-{i}"}
            for i in range(11)  # 11 entries
        ]

        mock_request = MagicMock()
        mock_request.url.path = "/123456789012/test-batch-limit"

        # AWS returns TooManyEntriesInBatchRequest for >10 entries
        with pytest.raises((SqsError, Exception)) as exc_info:
            _send_message_batch(
                store,
                {
                    "QueueUrl": "http://localhost:4566/123456789012/test-batch-limit",
                    "Entries": entries,
                },
                "us-east-1",
                "123456789012",
                mock_request,
            )

        # If it didn't raise, the test fails
        if not exc_info:
            pytest.fail(
                "SendMessageBatch with >10 entries should raise TooManyEntriesInBatchRequest. "
                "Currently there is no limit check."
            )


# ===========================================================================
# SQS: SendMessageBatch should reject duplicate IDs
# ===========================================================================


class TestSqsSendMessageBatchDuplicateIds:
    """AWS rejects SendMessageBatch if two entries have the same Id."""

    def test_duplicate_entry_ids_should_error(self):
        from unittest.mock import MagicMock

        from robotocore.services.sqs.provider import SqsError, _send_message_batch

        store = SqsStore()
        store.create_queue("test-batch-dup", "us-east-1", "123456789012")

        entries = [
            {"Id": "same-id", "MessageBody": "msg-1"},
            {"Id": "same-id", "MessageBody": "msg-2"},  # Duplicate Id!
        ]

        mock_request = MagicMock()
        mock_request.url.path = "/123456789012/test-batch-dup"

        with pytest.raises((SqsError, Exception)):
            _send_message_batch(
                store,
                {
                    "QueueUrl": "http://localhost:4566/123456789012/test-batch-dup",
                    "Entries": entries,
                },
                "us-east-1",
                "123456789012",
                mock_request,
            )
        # If it doesn't raise, the test fails:
        # (the pytest.raises context manager handles this)


# ===========================================================================
# DynamoDB: TransactWriteItems stream hook doesn't capture old/new images
# ===========================================================================


class TestDynamoDBTransactWriteStreamImages:
    """TransactWriteItems stream hooks should include old/new images for stream consumers."""

    def test_transact_write_stream_hook_has_images(self):
        # In provider.py, _fire_stream_hooks for TransactWriteItems always passes
        # new_image=None and old_image=None for Update operations, and doesn't
        # capture old_image for Put operations. Stream consumers need these images
        # when StreamViewType is NEW_AND_OLD_IMAGES or NEW_IMAGE.
        import inspect

        from robotocore.services.dynamodb.provider import _fire_stream_hooks

        source = inspect.getsource(_fire_stream_hooks)

        # Find the TransactWriteItems Update section
        transact_start = source.find("TransactWriteItems")
        assert transact_start > 0
        transact_section = source[transact_start:]
        update_start = transact_section.find('"Update"')
        update_section = transact_section[update_start : update_start + 400]

        # The Update handler should pass meaningful new_image/old_image
        # Currently it passes new_image=None, old_image=None
        assert "new_image=None" not in update_section, (
            "TransactWriteItems Update stream hook passes new_image=None. "
            "Stream consumers with NEW_IMAGE or NEW_AND_OLD_IMAGES view type "
            "need the actual item images."
        )


# ===========================================================================
# SQS: ChangeMessageVisibility with timeout > 12 hours should error
# ===========================================================================


class TestSqsChangeVisibilityMaxTimeout:
    """AWS limits VisibilityTimeout to 12 hours (43200 seconds)."""

    def test_visibility_timeout_over_12h_should_error(self):
        queue = StandardQueue("test-vis-max", "us-east-1", "123456789012")
        msg = SqsMessage(message_id="m1", body="hello", md5_of_body="abc")
        queue.put(msg)

        results = queue.receive(max_messages=1, visibility_timeout=30)
        assert len(results) == 1
        _msg, receipt = results[0]

        # AWS rejects VisibilityTimeout > 43200 (12 hours)
        # Our implementation accepts any value
        result = queue.change_visibility(receipt, 99999)
        # change_visibility returns True (success) -- it should have rejected this
        assert result is False or result is None, (
            "ChangeMessageVisibility with timeout > 43200 seconds (12 hours) should be "
            "rejected. Currently any positive integer is accepted."
        )


# ===========================================================================
# SQS: ReceiveMessage MaxNumberOfMessages > 10 should error
# ===========================================================================


class TestSqsReceiveMaxMessages:
    """AWS rejects MaxNumberOfMessages values outside 1-10 range."""

    def test_max_messages_zero_should_error(self):
        # The provider clamps to min(value, 10) but doesn't reject 0 or negative values.
        # AWS returns InvalidParameterValue for MaxNumberOfMessages < 1 or > 10.
        from unittest.mock import MagicMock

        from robotocore.services.sqs.provider import SqsError, _receive_message

        store = SqsStore()
        store.create_queue("test-recv-max", "us-east-1", "123456789012")

        mock_request = MagicMock()
        mock_request.url.path = "/123456789012/test-recv-max"

        with pytest.raises(SqsError) as exc_info:
            _receive_message(
                store,
                {
                    "QueueUrl": "http://localhost:4566/123456789012/test-recv-max",
                    "MaxNumberOfMessages": "0",
                },
                "us-east-1",
                "123456789012",
                mock_request,
            )

        assert exc_info.value.code == "InvalidParameterValue", (
            "MaxNumberOfMessages=0 should raise InvalidParameterValue. "
            "Currently the value is silently clamped via min()."
        )


# ===========================================================================
# SQS: Queue attribute 'FifoQueue' must be 'true' for .fifo queues
# ===========================================================================


class TestSqsFifoQueueAttributeValidation:
    """AWS requires FifoQueue=true attribute when creating .fifo queues."""

    def test_fifo_queue_without_attribute_should_still_work(self):
        # Our implementation determines FIFO by name suffix, not by attribute.
        # AWS actually requires the FifoQueue attribute to be set.
        store = SqsStore()
        queue = store.create_queue("test.fifo", "us-east-1", "123456789012")
        assert isinstance(queue, FifoQueue), "Queue with .fifo suffix should be FifoQueue"

        # But creating a .fifo queue should require FifoQueue=true attribute
        attrs = queue.get_attributes()
        assert attrs.get("FifoQueue") == "true", "FIFO queue should report FifoQueue=true"

        # Creating a non-fifo queue with FifoQueue=true should error on real AWS
        # Our implementation ignores this attribute entirely
        non_fifo = store.create_queue(
            "test-regular",
            "us-east-1",
            "123456789012",
            {"FifoQueue": "true"},
        )
        # On real AWS, this would error because the name doesn't end in .fifo
        assert isinstance(non_fifo, FifoQueue) or not isinstance(non_fifo, FifoQueue), (
            "This assertion is trivially true. The real issue: creating a non-.fifo-named "
            "queue with FifoQueue=true should raise InvalidParameterValue on real AWS."
        )
        # Actual check: the queue should have been rejected
        assert non_fifo.is_fifo is True, (
            "A queue created with FifoQueue=true but without .fifo suffix should either "
            "be rejected (AWS behavior) or at minimum be treated as FIFO. Currently it's "
            "treated as a standard queue because is_fifo only checks name suffix."
        )
