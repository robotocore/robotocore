"""Comprehensive unit tests for the Kinesis native provider.

Tests all action functions directly, covering stream CRUD, shard operations,
record put/get, consumer management, encryption, enhanced monitoring,
resource policies, and edge cases.
"""

import base64
import json
import time

import pytest

from robotocore.services.kinesis.models import KinesisStore, _get_store, _stores
from robotocore.services.kinesis.provider import (
    KinesisError,
    _add_tags,
    _create_stream,
    _decrease_retention,
    _delete_resource_policy,
    _delete_stream,
    _deregister_stream_consumer,
    _describe_stream,
    _describe_stream_consumer,
    _describe_stream_summary,
    _disable_enhanced_monitoring,
    _enable_enhanced_monitoring,
    _encode_iterator,
    _extract_stream_name_from_arn,
    _get_records,
    _get_resource_policy,
    _get_shard_iterator,
    _increase_retention,
    _list_shards,
    _list_stream_consumers,
    _list_streams,
    _list_tags,
    _merge_shards,
    _put_record,
    _put_records,
    _put_resource_policy,
    _register_stream_consumer,
    _remove_tags,
    _split_shard,
    _start_stream_encryption,
    _stop_stream_encryption,
    _update_shard_count,
)

REGION = "us-east-1"
ACCOUNT = "123456789012"


@pytest.fixture(autouse=True)
def _clear_stores():
    _stores.clear()
    yield
    _stores.clear()


@pytest.fixture()
def store() -> KinesisStore:
    return _get_store(REGION, ACCOUNT)


@pytest.fixture()
def stream_store(store: KinesisStore) -> KinesisStore:
    """A store with a pre-created stream named 'test-stream' with 2 shards."""
    _create_stream(store, {"StreamName": "test-stream", "ShardCount": 2}, REGION, ACCOUNT)
    return store


# ---------------------------------------------------------------------------
# Stream CRUD
# ---------------------------------------------------------------------------


class TestCreateStream:
    def test_create_basic(self, store):
        result = _create_stream(store, {"StreamName": "s1", "ShardCount": 3}, REGION, ACCOUNT)
        assert result == {}
        stream = store.get_stream("s1")
        assert stream is not None
        assert stream.name == "s1"
        assert len(stream.shards) == 3

    def test_create_default_shard_count(self, store):
        _create_stream(store, {"StreamName": "s1"}, REGION, ACCOUNT)
        stream = store.get_stream("s1")
        assert len(stream.shards) == 1

    def test_create_missing_name_raises(self, store):
        with pytest.raises(KinesisError, match="StreamName is required"):
            _create_stream(store, {"ShardCount": 1}, REGION, ACCOUNT)

    def test_create_duplicate_raises(self, store):
        _create_stream(store, {"StreamName": "s1", "ShardCount": 1}, REGION, ACCOUNT)
        with pytest.raises(KinesisError) as exc:
            _create_stream(store, {"StreamName": "s1", "ShardCount": 1}, REGION, ACCOUNT)
        assert exc.value.code == "ResourceInUseException"

    def test_create_stream_arn_format(self, store):
        _create_stream(store, {"StreamName": "my-stream", "ShardCount": 1}, REGION, ACCOUNT)
        stream = store.get_stream("my-stream")
        assert stream.arn == f"arn:aws:kinesis:{REGION}:{ACCOUNT}:stream/my-stream"

    def test_create_stream_shard_hash_key_ranges(self, store):
        _create_stream(store, {"StreamName": "s1", "ShardCount": 2}, REGION, ACCOUNT)
        stream = store.get_stream("s1")
        # First shard starts at 0
        assert stream.shards[0].hash_key_start == 0
        # Last shard ends at MAX_HASH_KEY
        from robotocore.services.kinesis.models import MAX_HASH_KEY

        assert stream.shards[-1].hash_key_end == MAX_HASH_KEY
        # No gaps between shards
        assert stream.shards[0].hash_key_end + 1 == stream.shards[1].hash_key_start


class TestDeleteStream:
    def test_delete_existing(self, stream_store):
        result = _delete_stream(stream_store, {"StreamName": "test-stream"}, REGION, ACCOUNT)
        assert result == {}
        assert stream_store.get_stream("test-stream") is None

    def test_delete_nonexistent_raises(self, store):
        with pytest.raises(KinesisError) as exc:
            _delete_stream(store, {"StreamName": "nope"}, REGION, ACCOUNT)
        assert exc.value.code == "ResourceNotFoundException"

    def test_delete_cleans_up_resource_policies(self, stream_store):
        stream = stream_store.get_stream("test-stream")
        stream_arn = stream.arn
        stream_store.resource_policies[stream_arn] = '{"Version":"2012-10-17"}'
        consumer_arn = f"{stream_arn}/consumer/my-consumer:12345"
        stream_store.resource_policies[consumer_arn] = '{"Version":"2012-10-17"}'

        _delete_stream(stream_store, {"StreamName": "test-stream"}, REGION, ACCOUNT)
        assert stream_arn not in stream_store.resource_policies
        assert consumer_arn not in stream_store.resource_policies


class TestDescribeStream:
    def test_describe_basic(self, stream_store):
        result = _describe_stream(stream_store, {"StreamName": "test-stream"}, REGION, ACCOUNT)
        desc = result["StreamDescription"]
        assert desc["StreamName"] == "test-stream"
        assert desc["StreamStatus"] == "ACTIVE"
        assert len(desc["Shards"]) == 2
        assert desc["RetentionPeriodHours"] == 24
        assert desc["EncryptionType"] == "NONE"
        assert desc["HasMoreShards"] is False
        assert "StreamARN" in desc

    def test_describe_nonexistent_raises(self, store):
        with pytest.raises(KinesisError) as exc:
            _describe_stream(store, {"StreamName": "nope"}, REGION, ACCOUNT)
        assert exc.value.code == "ResourceNotFoundException"

    def test_describe_with_limit(self, store):
        _create_stream(store, {"StreamName": "s1", "ShardCount": 5}, REGION, ACCOUNT)
        result = _describe_stream(store, {"StreamName": "s1", "Limit": 2}, REGION, ACCOUNT)
        desc = result["StreamDescription"]
        assert len(desc["Shards"]) == 2
        assert desc["HasMoreShards"] is True

    def test_describe_with_exclusive_start_shard_id(self, store):
        _create_stream(store, {"StreamName": "s1", "ShardCount": 4}, REGION, ACCOUNT)
        result = _describe_stream(
            store,
            {"StreamName": "s1", "ExclusiveStartShardId": "shardId-000000000001"},
            REGION,
            ACCOUNT,
        )
        desc = result["StreamDescription"]
        # Should skip shardId-000000000000 and shardId-000000000001
        shard_ids = [s["ShardId"] for s in desc["Shards"]]
        assert "shardId-000000000000" not in shard_ids
        assert "shardId-000000000001" not in shard_ids
        assert "shardId-000000000002" in shard_ids

    def test_describe_shard_structure(self, stream_store):
        result = _describe_stream(stream_store, {"StreamName": "test-stream"}, REGION, ACCOUNT)
        shard = result["StreamDescription"]["Shards"][0]
        assert "ShardId" in shard
        assert "HashKeyRange" in shard
        assert "StartingHashKey" in shard["HashKeyRange"]
        assert "EndingHashKey" in shard["HashKeyRange"]
        assert "SequenceNumberRange" in shard
        assert "StartingSequenceNumber" in shard["SequenceNumberRange"]


class TestListStreams:
    def test_list_empty(self, store):
        result = _list_streams(store, {}, REGION, ACCOUNT)
        assert result["StreamNames"] == []
        assert result["HasMoreStreams"] is False

    def test_list_multiple(self, store):
        for name in ("charlie", "alpha", "bravo"):
            _create_stream(store, {"StreamName": name, "ShardCount": 1}, REGION, ACCOUNT)
        result = _list_streams(store, {}, REGION, ACCOUNT)
        assert result["StreamNames"] == ["alpha", "bravo", "charlie"]

    def test_list_with_limit(self, store):
        for name in ("a", "b", "c"):
            _create_stream(store, {"StreamName": name, "ShardCount": 1}, REGION, ACCOUNT)
        result = _list_streams(store, {"Limit": 2}, REGION, ACCOUNT)
        assert len(result["StreamNames"]) == 2
        assert result["HasMoreStreams"] is True

    def test_list_with_exclusive_start(self, store):
        for name in ("a", "b", "c"):
            _create_stream(store, {"StreamName": name, "ShardCount": 1}, REGION, ACCOUNT)
        result = _list_streams(store, {"ExclusiveStartStreamName": "a"}, REGION, ACCOUNT)
        assert "a" not in result["StreamNames"]
        assert "b" in result["StreamNames"]

    def test_list_exclusive_start_nonexistent(self, store):
        for name in ("a", "b"):
            _create_stream(store, {"StreamName": name, "ShardCount": 1}, REGION, ACCOUNT)
        result = _list_streams(store, {"ExclusiveStartStreamName": "nonexistent"}, REGION, ACCOUNT)
        # When start name not found, returns all streams
        assert len(result["StreamNames"]) == 2


class TestDescribeStreamSummary:
    def test_summary_basic(self, stream_store):
        result = _describe_stream_summary(
            stream_store, {"StreamName": "test-stream"}, REGION, ACCOUNT
        )
        summary = result["StreamDescriptionSummary"]
        assert summary["StreamName"] == "test-stream"
        assert summary["OpenShardCount"] == 2
        assert summary["ConsumerCount"] == 0
        assert summary["StreamStatus"] == "ACTIVE"
        assert "StreamARN" in summary

    def test_summary_nonexistent_raises(self, store):
        with pytest.raises(KinesisError) as exc:
            _describe_stream_summary(store, {"StreamName": "nope"}, REGION, ACCOUNT)
        assert exc.value.code == "ResourceNotFoundException"

    def test_summary_consumer_count(self, stream_store):
        stream = stream_store.get_stream("test-stream")
        stream.consumers["c1"] = {"ConsumerName": "c1"}
        stream.consumers["c2"] = {"ConsumerName": "c2"}
        result = _describe_stream_summary(
            stream_store, {"StreamName": "test-stream"}, REGION, ACCOUNT
        )
        assert result["StreamDescriptionSummary"]["ConsumerCount"] == 2


# ---------------------------------------------------------------------------
# Record Operations
# ---------------------------------------------------------------------------


class TestPutRecord:
    def test_put_record_basic(self, stream_store):
        data_b64 = base64.b64encode(b"hello").decode()
        result = _put_record(
            stream_store,
            {"StreamName": "test-stream", "PartitionKey": "pk1", "Data": data_b64},
            REGION,
            ACCOUNT,
        )
        assert "ShardId" in result
        assert "SequenceNumber" in result
        assert result["EncryptionType"] == "NONE"

    def test_put_record_nonexistent_stream(self, store):
        data_b64 = base64.b64encode(b"hello").decode()
        with pytest.raises(KinesisError) as exc:
            _put_record(
                store,
                {"StreamName": "nope", "PartitionKey": "pk1", "Data": data_b64},
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_put_record_with_explicit_hash_key(self, stream_store):
        data_b64 = base64.b64encode(b"data").decode()
        result = _put_record(
            stream_store,
            {
                "StreamName": "test-stream",
                "PartitionKey": "pk1",
                "Data": data_b64,
                "ExplicitHashKey": "0",
            },
            REGION,
            ACCOUNT,
        )
        assert result["ShardId"] == "shardId-000000000000"

    def test_put_record_empty_data(self, stream_store):
        result = _put_record(
            stream_store,
            {"StreamName": "test-stream", "PartitionKey": "pk1", "Data": ""},
            REGION,
            ACCOUNT,
        )
        assert "SequenceNumber" in result


class TestPutRecords:
    def test_put_records_batch(self, stream_store):
        data_b64 = base64.b64encode(b"rec").decode()
        result = _put_records(
            stream_store,
            {
                "StreamName": "test-stream",
                "Records": [
                    {"PartitionKey": "pk1", "Data": data_b64},
                    {"PartitionKey": "pk2", "Data": data_b64},
                    {"PartitionKey": "pk3", "Data": data_b64},
                ],
            },
            REGION,
            ACCOUNT,
        )
        assert result["FailedRecordCount"] == 0
        assert len(result["Records"]) == 3
        assert result["EncryptionType"] == "NONE"
        for rec in result["Records"]:
            assert "ShardId" in rec
            assert "SequenceNumber" in rec

    def test_put_records_nonexistent_stream(self, store):
        with pytest.raises(KinesisError) as exc:
            _put_records(
                store,
                {
                    "StreamName": "nope",
                    "Records": [{"PartitionKey": "pk1", "Data": "ZGF0YQ=="}],
                },
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_put_records_empty_list(self, stream_store):
        result = _put_records(
            stream_store,
            {"StreamName": "test-stream", "Records": []},
            REGION,
            ACCOUNT,
        )
        assert result["FailedRecordCount"] == 0
        assert result["Records"] == []


# ---------------------------------------------------------------------------
# Shard Iterator and GetRecords
# ---------------------------------------------------------------------------


class TestGetShardIterator:
    def test_trim_horizon(self, stream_store):
        result = _get_shard_iterator(
            stream_store,
            {
                "StreamName": "test-stream",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "TRIM_HORIZON",
            },
            REGION,
            ACCOUNT,
        )
        assert "ShardIterator" in result
        # Decode and verify
        token = result["ShardIterator"]
        payload = json.loads(base64.b64decode(token))
        assert payload["seq"] == "00000000000000000000"

    def test_latest(self, stream_store):
        result = _get_shard_iterator(
            stream_store,
            {
                "StreamName": "test-stream",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "LATEST",
            },
            REGION,
            ACCOUNT,
        )
        assert "ShardIterator" in result

    def test_at_sequence_number(self, stream_store):
        result = _get_shard_iterator(
            stream_store,
            {
                "StreamName": "test-stream",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_SEQUENCE_NUMBER",
                "StartingSequenceNumber": "00000000000000000005",
            },
            REGION,
            ACCOUNT,
        )
        token = result["ShardIterator"]
        payload = json.loads(base64.b64decode(token))
        assert payload["seq"] == "00000000000000000005"

    def test_after_sequence_number(self, stream_store):
        result = _get_shard_iterator(
            stream_store,
            {
                "StreamName": "test-stream",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AFTER_SEQUENCE_NUMBER",
                "StartingSequenceNumber": "00000000000000000005",
            },
            REGION,
            ACCOUNT,
        )
        token = result["ShardIterator"]
        payload = json.loads(base64.b64decode(token))
        assert payload["seq"] == "00000000000000000006"

    def test_at_timestamp(self, stream_store):
        result = _get_shard_iterator(
            stream_store,
            {
                "StreamName": "test-stream",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_TIMESTAMP",
                "Timestamp": time.time(),
            },
            REGION,
            ACCOUNT,
        )
        assert "ShardIterator" in result

    def test_invalid_iterator_type(self, stream_store):
        with pytest.raises(KinesisError) as exc:
            _get_shard_iterator(
                stream_store,
                {
                    "StreamName": "test-stream",
                    "ShardId": "shardId-000000000000",
                    "ShardIteratorType": "BOGUS",
                },
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "InvalidArgumentException"

    def test_nonexistent_stream(self, store):
        with pytest.raises(KinesisError) as exc:
            _get_shard_iterator(
                store,
                {
                    "StreamName": "nope",
                    "ShardId": "shardId-000000000000",
                    "ShardIteratorType": "TRIM_HORIZON",
                },
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_nonexistent_shard(self, stream_store):
        with pytest.raises(KinesisError) as exc:
            _get_shard_iterator(
                stream_store,
                {
                    "StreamName": "test-stream",
                    "ShardId": "shardId-999999999999",
                    "ShardIteratorType": "TRIM_HORIZON",
                },
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_after_sequence_number_non_numeric(self, stream_store):
        with pytest.raises(KinesisError) as exc:
            _get_shard_iterator(
                stream_store,
                {
                    "StreamName": "test-stream",
                    "ShardId": "shardId-000000000000",
                    "ShardIteratorType": "AFTER_SEQUENCE_NUMBER",
                    "StartingSequenceNumber": "not-a-number",
                },
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "InvalidArgumentException"


class TestGetRecords:
    def test_get_records_from_empty_shard(self, stream_store):
        iter_result = _get_shard_iterator(
            stream_store,
            {
                "StreamName": "test-stream",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "TRIM_HORIZON",
            },
            REGION,
            ACCOUNT,
        )
        result = _get_records(
            stream_store,
            {"ShardIterator": iter_result["ShardIterator"]},
            REGION,
            ACCOUNT,
        )
        assert result["Records"] == []
        assert "NextShardIterator" in result
        assert result["MillisBehindLatest"] == 0

    def test_get_records_after_put(self, stream_store):
        # Put a record targeting the first shard
        data_b64 = base64.b64encode(b"payload").decode()
        _put_record(
            stream_store,
            {
                "StreamName": "test-stream",
                "PartitionKey": "pk1",
                "Data": data_b64,
                "ExplicitHashKey": "0",
            },
            REGION,
            ACCOUNT,
        )

        iter_result = _get_shard_iterator(
            stream_store,
            {
                "StreamName": "test-stream",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "TRIM_HORIZON",
            },
            REGION,
            ACCOUNT,
        )
        result = _get_records(
            stream_store,
            {"ShardIterator": iter_result["ShardIterator"]},
            REGION,
            ACCOUNT,
        )
        assert len(result["Records"]) == 1
        rec = result["Records"][0]
        assert base64.b64decode(rec["Data"]) == b"payload"
        assert rec["PartitionKey"] == "pk1"
        assert "SequenceNumber" in rec
        assert "ApproximateArrivalTimestamp" in rec

    def test_get_records_with_limit(self, stream_store):
        # Put 5 records to shard 0
        for i in range(5):
            data_b64 = base64.b64encode(f"rec-{i}".encode()).decode()
            _put_record(
                stream_store,
                {
                    "StreamName": "test-stream",
                    "PartitionKey": "pk",
                    "Data": data_b64,
                    "ExplicitHashKey": "0",
                },
                REGION,
                ACCOUNT,
            )

        iter_result = _get_shard_iterator(
            stream_store,
            {
                "StreamName": "test-stream",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "TRIM_HORIZON",
            },
            REGION,
            ACCOUNT,
        )
        result = _get_records(
            stream_store,
            {"ShardIterator": iter_result["ShardIterator"], "Limit": 2},
            REGION,
            ACCOUNT,
        )
        assert len(result["Records"]) == 2

    def test_get_records_limit_capped_at_10000(self, stream_store):
        """Limit should be capped at 10000 even if a higher value is passed."""
        iter_result = _get_shard_iterator(
            stream_store,
            {
                "StreamName": "test-stream",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "TRIM_HORIZON",
            },
            REGION,
            ACCOUNT,
        )
        # This should not raise, just cap at 10000
        result = _get_records(
            stream_store,
            {"ShardIterator": iter_result["ShardIterator"], "Limit": 50000},
            REGION,
            ACCOUNT,
        )
        assert "Records" in result

    def test_get_records_invalid_iterator(self, store):
        with pytest.raises(KinesisError) as exc:
            _get_records(store, {"ShardIterator": "not-valid!!!"}, REGION, ACCOUNT)
        assert exc.value.code == "InvalidArgumentException"

    def test_get_records_stream_deleted(self, stream_store):
        """GetRecords should fail if the stream was deleted after getting the iterator."""
        iter_result = _get_shard_iterator(
            stream_store,
            {
                "StreamName": "test-stream",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "TRIM_HORIZON",
            },
            REGION,
            ACCOUNT,
        )
        _delete_stream(stream_store, {"StreamName": "test-stream"}, REGION, ACCOUNT)
        with pytest.raises(KinesisError) as exc:
            _get_records(
                stream_store,
                {"ShardIterator": iter_result["ShardIterator"]},
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Shard Operations
# ---------------------------------------------------------------------------


class TestListShards:
    def test_list_shards_basic(self, stream_store):
        result = _list_shards(stream_store, {"StreamName": "test-stream"}, REGION, ACCOUNT)
        assert len(result["Shards"]) == 2
        assert "NextToken" not in result

    def test_list_shards_nonexistent(self, store):
        with pytest.raises(KinesisError) as exc:
            _list_shards(store, {"StreamName": "nope"}, REGION, ACCOUNT)
        assert exc.value.code == "ResourceNotFoundException"

    def test_list_shards_with_pagination(self, store):
        _create_stream(store, {"StreamName": "s1", "ShardCount": 5}, REGION, ACCOUNT)
        result = _list_shards(store, {"StreamName": "s1", "MaxResults": 2}, REGION, ACCOUNT)
        assert len(result["Shards"]) == 2
        assert "NextToken" in result

        # Use NextToken to get more (must pass MaxResults again)
        result2 = _list_shards(
            store, {"NextToken": result["NextToken"], "MaxResults": 2}, REGION, ACCOUNT
        )
        assert len(result2["Shards"]) == 2
        assert "NextToken" in result2

        # Third page
        result3 = _list_shards(
            store, {"NextToken": result2["NextToken"], "MaxResults": 2}, REGION, ACCOUNT
        )
        assert len(result3["Shards"]) == 1
        assert "NextToken" not in result3

    def test_list_shards_shard_structure(self, stream_store):
        result = _list_shards(stream_store, {"StreamName": "test-stream"}, REGION, ACCOUNT)
        shard = result["Shards"][0]
        assert "ShardId" in shard
        assert "HashKeyRange" in shard
        assert "StartingHashKey" in shard["HashKeyRange"]
        assert "EndingHashKey" in shard["HashKeyRange"]
        assert "SequenceNumberRange" in shard


class TestSplitShard:
    def test_split_basic(self, store):
        _create_stream(store, {"StreamName": "s1", "ShardCount": 1}, REGION, ACCOUNT)
        stream = store.get_stream("s1")
        mid = (stream.shards[0].hash_key_start + stream.shards[0].hash_key_end) // 2

        result = _split_shard(
            store,
            {
                "StreamName": "s1",
                "ShardToSplit": "shardId-000000000000",
                "NewStartingHashKey": str(mid),
            },
            REGION,
            ACCOUNT,
        )
        assert result == {}
        stream = store.get_stream("s1")
        assert len(stream.shards) == 2
        assert stream.shard_count == 2

    def test_split_nonexistent_stream(self, store):
        with pytest.raises(KinesisError) as exc:
            _split_shard(
                store,
                {"StreamName": "nope", "ShardToSplit": "shardId-000000000000"},
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_split_nonexistent_shard(self, stream_store):
        with pytest.raises(KinesisError) as exc:
            _split_shard(
                stream_store,
                {"StreamName": "test-stream", "ShardToSplit": "shardId-999999999999"},
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_split_default_midpoint(self, store):
        """When NewStartingHashKey is empty, should split at midpoint."""
        _create_stream(store, {"StreamName": "s1", "ShardCount": 1}, REGION, ACCOUNT)
        _split_shard(
            store,
            {"StreamName": "s1", "ShardToSplit": "shardId-000000000000"},
            REGION,
            ACCOUNT,
        )
        stream = store.get_stream("s1")
        assert len(stream.shards) == 2


class TestMergeShards:
    def test_merge_basic(self, store):
        _create_stream(store, {"StreamName": "s1", "ShardCount": 2}, REGION, ACCOUNT)
        result = _merge_shards(
            store,
            {
                "StreamName": "s1",
                "ShardToMerge": "shardId-000000000000",
                "AdjacentShardToMerge": "shardId-000000000001",
            },
            REGION,
            ACCOUNT,
        )
        assert result == {}
        stream = store.get_stream("s1")
        assert len(stream.shards) == 1
        assert stream.shard_count == 1

    def test_merge_nonexistent_stream(self, store):
        with pytest.raises(KinesisError) as exc:
            _merge_shards(
                store,
                {
                    "StreamName": "nope",
                    "ShardToMerge": "shardId-000000000000",
                    "AdjacentShardToMerge": "shardId-000000000001",
                },
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_merge_nonexistent_shard(self, stream_store):
        with pytest.raises(KinesisError) as exc:
            _merge_shards(
                stream_store,
                {
                    "StreamName": "test-stream",
                    "ShardToMerge": "shardId-000000000000",
                    "AdjacentShardToMerge": "shardId-999999999999",
                },
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_merge_covers_full_hash_range(self, store):
        """Merged shard should cover the combined hash range."""
        _create_stream(store, {"StreamName": "s1", "ShardCount": 2}, REGION, ACCOUNT)
        from robotocore.services.kinesis.models import MAX_HASH_KEY

        _merge_shards(
            store,
            {
                "StreamName": "s1",
                "ShardToMerge": "shardId-000000000000",
                "AdjacentShardToMerge": "shardId-000000000001",
            },
            REGION,
            ACCOUNT,
        )
        stream = store.get_stream("s1")
        assert stream.shards[0].hash_key_start == 0
        assert stream.shards[0].hash_key_end == MAX_HASH_KEY


class TestUpdateShardCount:
    def test_update_basic(self, stream_store):
        result = _update_shard_count(
            stream_store,
            {
                "StreamName": "test-stream",
                "TargetShardCount": 4,
                "ScalingType": "UNIFORM_SCALING",
            },
            REGION,
            ACCOUNT,
        )
        assert result["CurrentShardCount"] == 2
        assert result["TargetShardCount"] == 4
        assert result["StreamName"] == "test-stream"
        assert "StreamARN" in result

        stream = stream_store.get_stream("test-stream")
        assert len(stream.shards) == 4

    def test_update_nonexistent_stream(self, store):
        with pytest.raises(KinesisError) as exc:
            _update_shard_count(
                store,
                {
                    "StreamName": "nope",
                    "TargetShardCount": 2,
                    "ScalingType": "UNIFORM_SCALING",
                },
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_update_missing_target_count(self, stream_store):
        with pytest.raises(KinesisError) as exc:
            _update_shard_count(
                stream_store,
                {"StreamName": "test-stream", "ScalingType": "UNIFORM_SCALING"},
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ValidationException"

    def test_update_invalid_scaling_type(self, stream_store):
        with pytest.raises(KinesisError) as exc:
            _update_shard_count(
                stream_store,
                {
                    "StreamName": "test-stream",
                    "TargetShardCount": 4,
                    "ScalingType": "INVALID",
                },
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ValidationException"

    def test_update_zero_target_count(self, stream_store):
        with pytest.raises(KinesisError) as exc:
            _update_shard_count(
                stream_store,
                {
                    "StreamName": "test-stream",
                    "TargetShardCount": 0,
                    "ScalingType": "UNIFORM_SCALING",
                },
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "InvalidArgumentException"


# ---------------------------------------------------------------------------
# Retention Period
# ---------------------------------------------------------------------------


class TestIncreaseRetention:
    def test_increase_basic(self, stream_store):
        result = _increase_retention(
            stream_store,
            {"StreamName": "test-stream", "RetentionPeriodHours": 48},
            REGION,
            ACCOUNT,
        )
        assert result == {}
        stream = stream_store.get_stream("test-stream")
        assert stream.retention_hours == 48

    def test_increase_nonexistent(self, store):
        with pytest.raises(KinesisError) as exc:
            _increase_retention(
                store, {"StreamName": "nope", "RetentionPeriodHours": 48}, REGION, ACCOUNT
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_increase_below_current_raises(self, stream_store):
        _increase_retention(
            stream_store,
            {"StreamName": "test-stream", "RetentionPeriodHours": 72},
            REGION,
            ACCOUNT,
        )
        with pytest.raises(KinesisError) as exc:
            _increase_retention(
                stream_store,
                {"StreamName": "test-stream", "RetentionPeriodHours": 48},
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "InvalidArgumentException"


class TestDecreaseRetention:
    def test_decrease_basic(self, stream_store):
        # First increase
        _increase_retention(
            stream_store,
            {"StreamName": "test-stream", "RetentionPeriodHours": 72},
            REGION,
            ACCOUNT,
        )
        result = _decrease_retention(
            stream_store,
            {"StreamName": "test-stream", "RetentionPeriodHours": 48},
            REGION,
            ACCOUNT,
        )
        assert result == {}
        stream = stream_store.get_stream("test-stream")
        assert stream.retention_hours == 48

    def test_decrease_nonexistent(self, store):
        with pytest.raises(KinesisError) as exc:
            _decrease_retention(
                store, {"StreamName": "nope", "RetentionPeriodHours": 24}, REGION, ACCOUNT
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_decrease_below_minimum_raises(self, stream_store):
        _increase_retention(
            stream_store,
            {"StreamName": "test-stream", "RetentionPeriodHours": 48},
            REGION,
            ACCOUNT,
        )
        with pytest.raises(KinesisError) as exc:
            _decrease_retention(
                stream_store,
                {"StreamName": "test-stream", "RetentionPeriodHours": 12},
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "InvalidArgumentException"

    def test_decrease_above_current_raises(self, stream_store):
        """Cannot decrease to a value >= current retention."""
        with pytest.raises(KinesisError) as exc:
            _decrease_retention(
                stream_store,
                {"StreamName": "test-stream", "RetentionPeriodHours": 24},
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "InvalidArgumentException"


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


class TestTags:
    def test_add_and_list_tags(self, stream_store):
        _add_tags(
            stream_store,
            {"StreamName": "test-stream", "Tags": {"env": "prod", "team": "data"}},
            REGION,
            ACCOUNT,
        )
        result = _list_tags(
            stream_store,
            {"StreamName": "test-stream"},
            REGION,
            ACCOUNT,
        )
        tags_dict = {t["Key"]: t["Value"] for t in result["Tags"]}
        assert tags_dict["env"] == "prod"
        assert tags_dict["team"] == "data"

    def test_remove_tags(self, stream_store):
        _add_tags(
            stream_store,
            {"StreamName": "test-stream", "Tags": {"a": "1", "b": "2", "c": "3"}},
            REGION,
            ACCOUNT,
        )
        _remove_tags(
            stream_store,
            {"StreamName": "test-stream", "TagKeys": ["b"]},
            REGION,
            ACCOUNT,
        )
        result = _list_tags(stream_store, {"StreamName": "test-stream"}, REGION, ACCOUNT)
        keys = [t["Key"] for t in result["Tags"]]
        assert "b" not in keys
        assert "a" in keys
        assert "c" in keys

    def test_remove_nonexistent_tag_succeeds(self, stream_store):
        result = _remove_tags(
            stream_store,
            {"StreamName": "test-stream", "TagKeys": ["nonexistent"]},
            REGION,
            ACCOUNT,
        )
        assert result == {}

    def test_add_tags_nonexistent_stream(self, store):
        with pytest.raises(KinesisError) as exc:
            _add_tags(store, {"StreamName": "nope", "Tags": {"k": "v"}}, REGION, ACCOUNT)
        assert exc.value.code == "ResourceNotFoundException"

    def test_list_tags_nonexistent_stream(self, store):
        with pytest.raises(KinesisError) as exc:
            _list_tags(store, {"StreamName": "nope"}, REGION, ACCOUNT)
        assert exc.value.code == "ResourceNotFoundException"

    def test_list_tags_pagination_limit(self, stream_store):
        tags = {f"key{i:02d}": f"val{i}" for i in range(15)}
        _add_tags(
            stream_store,
            {"StreamName": "test-stream", "Tags": tags},
            REGION,
            ACCOUNT,
        )
        result = _list_tags(
            stream_store,
            {"StreamName": "test-stream", "Limit": 5},
            REGION,
            ACCOUNT,
        )
        assert len(result["Tags"]) == 5
        assert result["HasMoreTags"] is True

    def test_list_tags_exclusive_start(self, stream_store):
        _add_tags(
            stream_store,
            {"StreamName": "test-stream", "Tags": {"aaa": "1", "bbb": "2", "ccc": "3"}},
            REGION,
            ACCOUNT,
        )
        result = _list_tags(
            stream_store,
            {"StreamName": "test-stream", "ExclusiveStartTagKey": "aaa"},
            REGION,
            ACCOUNT,
        )
        keys = [t["Key"] for t in result["Tags"]]
        assert "aaa" not in keys
        assert "bbb" in keys

    def test_list_tags_sorted_by_key(self, stream_store):
        _add_tags(
            stream_store,
            {"StreamName": "test-stream", "Tags": {"zzz": "3", "aaa": "1", "mmm": "2"}},
            REGION,
            ACCOUNT,
        )
        result = _list_tags(stream_store, {"StreamName": "test-stream"}, REGION, ACCOUNT)
        keys = [t["Key"] for t in result["Tags"]]
        assert keys == sorted(keys)

    def test_add_tags_overwrites_existing(self, stream_store):
        _add_tags(
            stream_store,
            {"StreamName": "test-stream", "Tags": {"env": "dev"}},
            REGION,
            ACCOUNT,
        )
        _add_tags(
            stream_store,
            {"StreamName": "test-stream", "Tags": {"env": "prod"}},
            REGION,
            ACCOUNT,
        )
        result = _list_tags(stream_store, {"StreamName": "test-stream"}, REGION, ACCOUNT)
        tags_dict = {t["Key"]: t["Value"] for t in result["Tags"]}
        assert tags_dict["env"] == "prod"


# ---------------------------------------------------------------------------
# Encryption
# ---------------------------------------------------------------------------


class TestEncryption:
    def test_start_encryption(self, stream_store):
        result = _start_stream_encryption(
            stream_store,
            {
                "StreamName": "test-stream",
                "EncryptionType": "KMS",
                "KeyId": "alias/my-key",
            },
            REGION,
            ACCOUNT,
        )
        assert result == {}
        stream = stream_store.get_stream("test-stream")
        assert stream.encryption_type == "KMS"
        assert stream.key_id == "alias/my-key"

    def test_stop_encryption(self, stream_store):
        _start_stream_encryption(
            stream_store,
            {
                "StreamName": "test-stream",
                "EncryptionType": "KMS",
                "KeyId": "alias/my-key",
            },
            REGION,
            ACCOUNT,
        )
        result = _stop_stream_encryption(
            stream_store, {"StreamName": "test-stream"}, REGION, ACCOUNT
        )
        assert result == {}
        stream = stream_store.get_stream("test-stream")
        assert stream.encryption_type == "NONE"
        assert stream.key_id == ""

    def test_start_encryption_nonexistent_stream(self, store):
        with pytest.raises(KinesisError) as exc:
            _start_stream_encryption(
                store,
                {"StreamName": "nope", "EncryptionType": "KMS", "KeyId": "alias/key"},
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_stop_encryption_nonexistent_stream(self, store):
        with pytest.raises(KinesisError) as exc:
            _stop_stream_encryption(store, {"StreamName": "nope"}, REGION, ACCOUNT)
        assert exc.value.code == "ResourceNotFoundException"

    def test_describe_shows_encryption(self, stream_store):
        _start_stream_encryption(
            stream_store,
            {
                "StreamName": "test-stream",
                "EncryptionType": "KMS",
                "KeyId": "arn:aws:kms:us-east-1:123:key/abc",
            },
            REGION,
            ACCOUNT,
        )
        result = _describe_stream(stream_store, {"StreamName": "test-stream"}, REGION, ACCOUNT)
        desc = result["StreamDescription"]
        assert desc["EncryptionType"] == "KMS"
        assert desc["KeyId"] == "arn:aws:kms:us-east-1:123:key/abc"


# ---------------------------------------------------------------------------
# Enhanced Fan-Out Consumers
# ---------------------------------------------------------------------------


class TestConsumers:
    def test_register_consumer(self, stream_store):
        stream = stream_store.get_stream("test-stream")
        result = _register_stream_consumer(
            stream_store,
            {"StreamARN": stream.arn, "ConsumerName": "my-consumer"},
            REGION,
            ACCOUNT,
        )
        consumer = result["Consumer"]
        assert consumer["ConsumerName"] == "my-consumer"
        assert consumer["ConsumerStatus"] == "ACTIVE"
        assert "ConsumerARN" in consumer
        assert "ConsumerCreationTimestamp" in consumer

    def test_register_duplicate_consumer_raises(self, stream_store):
        stream = stream_store.get_stream("test-stream")
        _register_stream_consumer(
            stream_store,
            {"StreamARN": stream.arn, "ConsumerName": "my-consumer"},
            REGION,
            ACCOUNT,
        )
        with pytest.raises(KinesisError) as exc:
            _register_stream_consumer(
                stream_store,
                {"StreamARN": stream.arn, "ConsumerName": "my-consumer"},
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceInUseException"

    def test_register_consumer_nonexistent_stream(self, store):
        with pytest.raises(KinesisError) as exc:
            _register_stream_consumer(
                store,
                {"StreamARN": "arn:aws:kinesis:us-east-1:123:stream/nope", "ConsumerName": "c1"},
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_describe_consumer_by_name(self, stream_store):
        stream = stream_store.get_stream("test-stream")
        _register_stream_consumer(
            stream_store,
            {"StreamARN": stream.arn, "ConsumerName": "my-consumer"},
            REGION,
            ACCOUNT,
        )
        result = _describe_stream_consumer(
            stream_store,
            {"StreamARN": stream.arn, "ConsumerName": "my-consumer"},
            REGION,
            ACCOUNT,
        )
        assert result["ConsumerDescription"]["ConsumerName"] == "my-consumer"

    def test_describe_consumer_by_arn(self, stream_store):
        stream = stream_store.get_stream("test-stream")
        reg = _register_stream_consumer(
            stream_store,
            {"StreamARN": stream.arn, "ConsumerName": "my-consumer"},
            REGION,
            ACCOUNT,
        )
        consumer_arn = reg["Consumer"]["ConsumerARN"]
        result = _describe_stream_consumer(
            stream_store, {"ConsumerARN": consumer_arn}, REGION, ACCOUNT
        )
        assert result["ConsumerDescription"]["ConsumerARN"] == consumer_arn

    def test_describe_consumer_not_found(self, stream_store):
        stream = stream_store.get_stream("test-stream")
        with pytest.raises(KinesisError) as exc:
            _describe_stream_consumer(
                stream_store,
                {"StreamARN": stream.arn, "ConsumerName": "does-not-exist"},
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_describe_consumer_by_arn_not_found(self, store):
        with pytest.raises(KinesisError) as exc:
            _describe_stream_consumer(
                store,
                {"ConsumerARN": "arn:aws:kinesis:us-east-1:123:stream/s1/consumer/nope:12345"},
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_list_consumers(self, stream_store):
        stream = stream_store.get_stream("test-stream")
        _register_stream_consumer(
            stream_store,
            {"StreamARN": stream.arn, "ConsumerName": "c1"},
            REGION,
            ACCOUNT,
        )
        _register_stream_consumer(
            stream_store,
            {"StreamARN": stream.arn, "ConsumerName": "c2"},
            REGION,
            ACCOUNT,
        )
        result = _list_stream_consumers(stream_store, {"StreamARN": stream.arn}, REGION, ACCOUNT)
        names = [c["ConsumerName"] for c in result["Consumers"]]
        assert "c1" in names
        assert "c2" in names

    def test_list_consumers_nonexistent_stream(self, store):
        with pytest.raises(KinesisError) as exc:
            _list_stream_consumers(
                store,
                {"StreamARN": "arn:aws:kinesis:us-east-1:123:stream/nope"},
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_deregister_consumer(self, stream_store):
        stream = stream_store.get_stream("test-stream")
        _register_stream_consumer(
            stream_store,
            {"StreamARN": stream.arn, "ConsumerName": "my-consumer"},
            REGION,
            ACCOUNT,
        )
        result = _deregister_stream_consumer(
            stream_store,
            {"StreamARN": stream.arn, "ConsumerName": "my-consumer"},
            REGION,
            ACCOUNT,
        )
        assert result == {}
        assert "my-consumer" not in stream.consumers

    def test_deregister_nonexistent_consumer(self, stream_store):
        stream = stream_store.get_stream("test-stream")
        with pytest.raises(KinesisError) as exc:
            _deregister_stream_consumer(
                stream_store,
                {"StreamARN": stream.arn, "ConsumerName": "does-not-exist"},
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_deregister_nonexistent_stream(self, store):
        with pytest.raises(KinesisError) as exc:
            _deregister_stream_consumer(
                store,
                {
                    "StreamARN": "arn:aws:kinesis:us-east-1:123:stream/nope",
                    "ConsumerName": "c1",
                },
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Enhanced Monitoring
# ---------------------------------------------------------------------------


class TestEnhancedMonitoring:
    def test_enable_specific_metrics(self, stream_store):
        result = _enable_enhanced_monitoring(
            stream_store,
            {
                "StreamName": "test-stream",
                "ShardLevelMetrics": ["IncomingBytes", "OutgoingBytes"],
            },
            REGION,
            ACCOUNT,
        )
        assert result["StreamName"] == "test-stream"
        assert "StreamARN" in result
        assert result["CurrentShardLevelMetrics"] == []
        assert "IncomingBytes" in result["DesiredShardLevelMetrics"]
        assert "OutgoingBytes" in result["DesiredShardLevelMetrics"]

    def test_enable_all_metrics(self, stream_store):
        result = _enable_enhanced_monitoring(
            stream_store,
            {"StreamName": "test-stream", "ShardLevelMetrics": ["ALL"]},
            REGION,
            ACCOUNT,
        )
        assert "ALL" in result["DesiredShardLevelMetrics"]
        assert "IncomingBytes" in result["DesiredShardLevelMetrics"]

    def test_enable_by_arn(self, stream_store):
        stream = stream_store.get_stream("test-stream")
        result = _enable_enhanced_monitoring(
            stream_store,
            {"StreamARN": stream.arn, "ShardLevelMetrics": ["IncomingBytes"]},
            REGION,
            ACCOUNT,
        )
        assert result["StreamName"] == "test-stream"

    def test_enable_nonexistent_raises(self, store):
        with pytest.raises(KinesisError) as exc:
            _enable_enhanced_monitoring(
                store,
                {"StreamName": "nope", "ShardLevelMetrics": ["IncomingBytes"]},
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_disable_specific_metrics(self, stream_store):
        _enable_enhanced_monitoring(
            stream_store,
            {
                "StreamName": "test-stream",
                "ShardLevelMetrics": ["IncomingBytes", "OutgoingBytes"],
            },
            REGION,
            ACCOUNT,
        )
        result = _disable_enhanced_monitoring(
            stream_store,
            {"StreamName": "test-stream", "ShardLevelMetrics": ["IncomingBytes"]},
            REGION,
            ACCOUNT,
        )
        assert "IncomingBytes" in result["CurrentShardLevelMetrics"]
        assert "IncomingBytes" not in result["DesiredShardLevelMetrics"]
        assert "OutgoingBytes" in result["DesiredShardLevelMetrics"]

    def test_disable_all_metrics(self, stream_store):
        _enable_enhanced_monitoring(
            stream_store,
            {"StreamName": "test-stream", "ShardLevelMetrics": ["ALL"]},
            REGION,
            ACCOUNT,
        )
        result = _disable_enhanced_monitoring(
            stream_store,
            {"StreamName": "test-stream", "ShardLevelMetrics": ["ALL"]},
            REGION,
            ACCOUNT,
        )
        assert result["DesiredShardLevelMetrics"] == []

    def test_disable_by_arn(self, stream_store):
        stream = stream_store.get_stream("test-stream")
        _enable_enhanced_monitoring(
            stream_store,
            {"StreamARN": stream.arn, "ShardLevelMetrics": ["IncomingBytes"]},
            REGION,
            ACCOUNT,
        )
        result = _disable_enhanced_monitoring(
            stream_store,
            {"StreamARN": stream.arn, "ShardLevelMetrics": ["IncomingBytes"]},
            REGION,
            ACCOUNT,
        )
        assert result["DesiredShardLevelMetrics"] == []

    def test_disable_nonexistent_raises(self, store):
        with pytest.raises(KinesisError) as exc:
            _disable_enhanced_monitoring(
                store,
                {"StreamName": "nope", "ShardLevelMetrics": ["IncomingBytes"]},
                REGION,
                ACCOUNT,
            )
        assert exc.value.code == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Resource Policies
# ---------------------------------------------------------------------------


class TestResourcePolicies:
    def test_put_and_get_policy(self, stream_store):
        stream = stream_store.get_stream("test-stream")
        policy_doc = '{"Version":"2012-10-17","Statement":[]}'
        _put_resource_policy(
            stream_store,
            {"ResourceARN": stream.arn, "Policy": policy_doc},
            REGION,
            ACCOUNT,
        )
        result = _get_resource_policy(stream_store, {"ResourceARN": stream.arn}, REGION, ACCOUNT)
        assert result["Policy"] == policy_doc

    def test_get_policy_default(self, stream_store):
        """Getting a policy that was never set returns empty JSON object."""
        stream = stream_store.get_stream("test-stream")
        result = _get_resource_policy(stream_store, {"ResourceARN": stream.arn}, REGION, ACCOUNT)
        assert result["Policy"] == "{}"

    def test_delete_policy(self, stream_store):
        stream = stream_store.get_stream("test-stream")
        _put_resource_policy(
            stream_store,
            {"ResourceARN": stream.arn, "Policy": "{}"},
            REGION,
            ACCOUNT,
        )
        result = _delete_resource_policy(stream_store, {"ResourceARN": stream.arn}, REGION, ACCOUNT)
        assert result == {}

    def test_delete_nonexistent_policy_raises(self, stream_store):
        stream = stream_store.get_stream("test-stream")
        with pytest.raises(KinesisError) as exc:
            _delete_resource_policy(stream_store, {"ResourceARN": stream.arn}, REGION, ACCOUNT)
        assert exc.value.code == "ResourceNotFoundException"

    def test_policy_on_nonexistent_stream(self, store):
        fake_arn = f"arn:aws:kinesis:{REGION}:{ACCOUNT}:stream/nope"
        with pytest.raises(KinesisError) as exc:
            _put_resource_policy(store, {"ResourceARN": fake_arn, "Policy": "{}"}, REGION, ACCOUNT)
        assert exc.value.code == "ResourceNotFoundException"

    def test_policy_on_consumer_arn(self, stream_store):
        stream = stream_store.get_stream("test-stream")
        _register_stream_consumer(
            stream_store,
            {"StreamARN": stream.arn, "ConsumerName": "c1"},
            REGION,
            ACCOUNT,
        )
        consumer_arn = stream.consumers["c1"]["ConsumerARN"]
        _put_resource_policy(
            stream_store,
            {"ResourceARN": consumer_arn, "Policy": '{"Version":"2012-10-17"}'},
            REGION,
            ACCOUNT,
        )
        result = _get_resource_policy(stream_store, {"ResourceARN": consumer_arn}, REGION, ACCOUNT)
        assert result["Policy"] == '{"Version":"2012-10-17"}'


# ---------------------------------------------------------------------------
# ARN Parsing Helper
# ---------------------------------------------------------------------------


class TestExtractStreamNameFromArn:
    def test_stream_arn(self):
        arn = "arn:aws:kinesis:us-east-1:123456789012:stream/mystream"
        assert _extract_stream_name_from_arn(arn) == "mystream"

    def test_consumer_arn(self):
        arn = "arn:aws:kinesis:us-east-1:123456789012:stream/mystream/consumer/myconsumer:12345"
        assert _extract_stream_name_from_arn(arn) == "mystream"

    def test_fallback_last_segment(self):
        arn = "something/unexpected"
        result = _extract_stream_name_from_arn(arn)
        assert result == "unexpected"


# ---------------------------------------------------------------------------
# Iterator Encoding
# ---------------------------------------------------------------------------


class TestIteratorEncoding:
    def test_encode_contains_all_fields(self):
        token = _encode_iterator("s1", "shard-0", "TRIM_HORIZON", "000", "us-east-1")
        payload = json.loads(base64.b64decode(token))
        assert payload["stream"] == "s1"
        assert payload["shard"] == "shard-0"
        assert payload["type"] == "TRIM_HORIZON"
        assert payload["seq"] == "000"
        assert payload["region"] == "us-east-1"
        assert "ts" in payload

    def test_encode_is_valid_base64(self):
        token = _encode_iterator("s1", "shard-0", "LATEST", "000", "us-west-2")
        decoded = base64.b64decode(token)
        assert json.loads(decoded)


# ---------------------------------------------------------------------------
# Store isolation
# ---------------------------------------------------------------------------


class TestStoreIsolation:
    def test_different_regions_are_isolated(self):
        store1 = _get_store("us-east-1", ACCOUNT)
        store2 = _get_store("us-west-2", ACCOUNT)
        _create_stream(store1, {"StreamName": "s1", "ShardCount": 1}, "us-east-1", ACCOUNT)
        assert store1.get_stream("s1") is not None
        assert store2.get_stream("s1") is None

    def test_different_accounts_are_isolated(self):
        store1 = _get_store(REGION, "111111111111")
        store2 = _get_store(REGION, "222222222222")
        _create_stream(store1, {"StreamName": "s1", "ShardCount": 1}, REGION, "111111111111")
        assert store1.get_stream("s1") is not None
        assert store2.get_stream("s1") is None
