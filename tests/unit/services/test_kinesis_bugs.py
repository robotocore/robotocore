"""Tests for Kinesis provider bug fixes.

Each test targets a specific bug that has been fixed.
"""

import base64

import pytest

from robotocore.services.kinesis.models import KinesisRecord
from robotocore.services.kinesis.provider import (
    _decode_iterator,
    _get_records,
    _get_shard_iterator,
    _get_store,
)

# ===================================================================
# Bug 1: AT_TIMESTAMP iterator type ignores Timestamp parameter
# ===================================================================


class TestATTimestampIterator:
    """Test that AT_TIMESTAMP shard iterator correctly handles the Timestamp parameter."""

    def test_at_timestamp_iterator_ignores_timestamp(self):
        """AT_TIMESTAMP iterator should store and use the Timestamp parameter.

        Bug: When creating a shard iterator with AT_TIMESTAMP type, the code
        ignores the Timestamp parameter and just uses sequence 0. This means
        GetRecords returns all records from the beginning instead of filtering
        by timestamp.
        """
        # Use the provider's store to ensure consistency
        store = _get_store("us-east-1", "123456789012")
        store.create_stream("test-stream", 1, "us-east-1", "123456789012")
        stream = store.get_stream("test-stream")
        shard = stream.shards[0]

        # Put some records with different timestamps
        import time

        # Record from 1 hour ago
        old_time = time.time() - 3600
        record1 = KinesisRecord(
            sequence_number="00000000000000000001",
            partition_key="pk1",
            data=b"old-data",
            timestamp=old_time,
            shard_id=shard.shard_id,
        )
        shard.records.append(record1)

        # Record from now
        new_time = time.time()
        record2 = KinesisRecord(
            sequence_number="00000000000000000002",
            partition_key="pk2",
            data=b"new-data",
            timestamp=new_time,
            shard_id=shard.shard_id,
        )
        shard.records.append(record2)

        # Request iterator for 30 minutes ago (should only get record2)
        thirty_mins_ago = time.time() - 1800

        # Create iterator with AT_TIMESTAMP
        result = _get_shard_iterator(
            store,
            {
                "StreamName": "test-stream",
                "ShardId": shard.shard_id,
                "ShardIteratorType": "AT_TIMESTAMP",
                "Timestamp": thirty_mins_ago,
            },
            "us-east-1",
            "123456789012",
        )

        token = result["ShardIterator"]
        iterator_info = _decode_iterator(token)

        # Bug: The iterator should store the timestamp for filtering
        # Currently it doesn't store the timestamp at all
        assert "timestamp" in iterator_info or iterator_info.get("type") == "AT_TIMESTAMP", (
            "BUG: AT_TIMESTAMP iterator doesn't store the timestamp parameter. "
            "The Timestamp is silently ignored and all records are returned."
        )

        # Get records using the iterator
        records_result = _get_records(
            store,
            {"ShardIterator": token, "Limit": 10},
            "us-east-1",
            "123456789012",
        )

        # Should only return record2 (the newer one)
        # Bug: Currently returns both records because timestamp filtering is not implemented
        records = records_result["Records"]
        if len(records) == 2:
            pytest.fail(
                "BUG: AT_TIMESTAMP returned all records instead of filtering by timestamp. "
                f"Expected 1 record (after {thirty_mins_ago}), got {len(records)}"
            )

        assert len(records) == 1, f"Expected 1 record, got {len(records)}"
        assert base64.b64decode(records[0]["Data"]) == b"new-data"


# ===================================================================
# Cross-account scoping: GetRecords must read the caller's account store
# ===================================================================


class TestGetRecordsAccountScoping:
    def test_get_records_uses_the_calling_accounts_store(self):
        """GetRecords resolved _get_store(region) with no account, so callers in
        account B read (or failed to find) account A's streams."""
        account_a, account_b = "444444444444", "555555555555"
        store_a = _get_store("us-east-1", account_a)
        store_a.create_stream("shared-name", 1, "us-east-1", account_a)
        _get_store("us-east-1", account_b)  # account B has no streams

        iterator = _get_shard_iterator(
            store_a,
            {"StreamName": "shared-name", "ShardId": "shardId-000000000000"},
            "us-east-1",
            account_a,
        )["ShardIterator"]

        payload = base64.b64decode(iterator.encode()).decode()
        assert '"account": "444444444444"' in payload, (
            "the iterator token must carry the creating account"
        )

        with pytest.raises(Exception, match="not found"):
            _get_records(
                _get_store("us-east-1", account_b),
                {"ShardIterator": iterator},
                "us-east-1",
                account_b,
            )
