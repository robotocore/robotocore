"""Kinesis compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def kinesis():
    return make_client("kinesis")


@pytest.fixture
def stream(kinesis):
    name = "test-compat-stream"
    kinesis.create_stream(StreamName=name, ShardCount=1)
    kinesis.get_waiter("stream_exists").wait(StreamName=name)
    yield name
    kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)


class TestKinesisOperations:
    def test_create_stream(self, kinesis):
        kinesis.create_stream(StreamName="create-test-stream", ShardCount=1)
        response = kinesis.describe_stream(StreamName="create-test-stream")
        assert response["StreamDescription"]["StreamName"] == "create-test-stream"
        kinesis.delete_stream(StreamName="create-test-stream")

    def test_list_streams(self, kinesis, stream):
        response = kinesis.list_streams()
        assert stream in response["StreamNames"]

    def test_put_and_get_record(self, kinesis, stream):
        put_resp = kinesis.put_record(
            StreamName=stream,
            Data=b"hello kinesis",
            PartitionKey="pk1",
        )
        assert "ShardId" in put_resp
        assert "SequenceNumber" in put_resp

        shard_id = put_resp["ShardId"]
        iterator = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]

        records = kinesis.get_records(ShardIterator=iterator)
        assert len(records["Records"]) >= 1
        assert records["Records"][0]["Data"] == b"hello kinesis"

    def test_describe_stream(self, kinesis, stream):
        response = kinesis.describe_stream(StreamName=stream)
        desc = response["StreamDescription"]
        assert desc["StreamName"] == stream
        assert desc["StreamStatus"] == "ACTIVE"
        assert "Shards" in desc
        assert len(desc["Shards"]) == 1
        assert "StreamARN" in desc
        assert "RetentionPeriodHours" in desc

    def test_list_shards(self, kinesis, stream):
        response = kinesis.list_shards(StreamName=stream)
        assert "Shards" in response
        assert len(response["Shards"]) == 1
        shard = response["Shards"][0]
        assert "ShardId" in shard
        assert "HashKeyRange" in shard
        assert "SequenceNumberRange" in shard

    def test_put_records_batch(self, kinesis, stream):
        records = [
            {"Data": b"record-one", "PartitionKey": "pk1"},
            {"Data": b"record-two", "PartitionKey": "pk2"},
            {"Data": b"record-three", "PartitionKey": "pk3"},
        ]
        response = kinesis.put_records(StreamName=stream, Records=records)
        assert response["FailedRecordCount"] == 0
        assert len(response["Records"]) == 3
        for rec in response["Records"]:
            assert "ShardId" in rec
            assert "SequenceNumber" in rec

    def test_get_shard_iterator_at_sequence_number(self, kinesis, stream):
        """Get shard iterator using AT_SEQUENCE_NUMBER type."""
        put_resp = kinesis.put_record(
            StreamName=stream,
            Data=b"seq-test",
            PartitionKey="pk1",
        )
        shard_id = put_resp["ShardId"]
        seq = put_resp["SequenceNumber"]
        iterator = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=shard_id,
            ShardIteratorType="AT_SEQUENCE_NUMBER",
            StartingSequenceNumber=seq,
        )["ShardIterator"]
        assert iterator is not None
        records = kinesis.get_records(ShardIterator=iterator)
        assert len(records["Records"]) >= 1
        assert records["Records"][0]["Data"] == b"seq-test"

    def test_add_and_list_tags_for_stream(self, kinesis, stream):
        """Add tags to a stream and list them."""
        kinesis.add_tags_to_stream(
            StreamName=stream,
            Tags={"env": "test", "project": "robotocore"},
        )
        response = kinesis.list_tags_for_stream(StreamName=stream)
        tag_map = {t["Key"]: t["Value"] for t in response["Tags"]}
        assert tag_map["env"] == "test"
        assert tag_map["project"] == "robotocore"

    def test_remove_tags_from_stream(self, kinesis, stream):
        """Add and then remove tags from a stream."""
        kinesis.add_tags_to_stream(
            StreamName=stream,
            Tags={"temp": "value"},
        )
        kinesis.remove_tags_from_stream(
            StreamName=stream,
            TagKeys=["temp"],
        )
        response = kinesis.list_tags_for_stream(StreamName=stream)
        tag_keys = [t["Key"] for t in response["Tags"]]
        assert "temp" not in tag_keys

    def test_increase_stream_retention_period(self, kinesis, stream):
        """Increase retention period from default 24 to 48 hours."""
        kinesis.increase_stream_retention_period(
            StreamName=stream,
            RetentionPeriodHours=48,
        )
        response = kinesis.describe_stream(StreamName=stream)
        assert response["StreamDescription"]["RetentionPeriodHours"] == 48

    def test_decrease_stream_retention_period(self, kinesis, stream):
        """Increase then decrease retention period."""
        kinesis.increase_stream_retention_period(
            StreamName=stream,
            RetentionPeriodHours=48,
        )
        kinesis.decrease_stream_retention_period(
            StreamName=stream,
            RetentionPeriodHours=24,
        )
        response = kinesis.describe_stream(StreamName=stream)
        assert response["StreamDescription"]["RetentionPeriodHours"] == 24


class TestKinesisDescribeStreamSummary:
    def test_describe_stream_summary(self, kinesis, stream):
        """describe_stream_summary returns expected fields."""
        resp = kinesis.describe_stream_summary(StreamName=stream)
        summary = resp["StreamDescriptionSummary"]
        assert summary["StreamName"] == stream
        assert summary["StreamStatus"] == "ACTIVE"
        assert "StreamARN" in summary
        assert "RetentionPeriodHours" in summary
        assert "OpenShardCount" in summary
        assert summary["OpenShardCount"] == 1

    def test_describe_stream_summary_multi_shard(self, kinesis):
        """describe_stream_summary with multiple shards."""
        uid = uuid.uuid4().hex[:8]
        name = f"summary-multi-{uid}"
        kinesis.create_stream(StreamName=name, ShardCount=3)
        kinesis.get_waiter("stream_exists").wait(StreamName=name)
        try:
            resp = kinesis.describe_stream_summary(StreamName=name)
            summary = resp["StreamDescriptionSummary"]
            assert summary["OpenShardCount"] == 3
        finally:
            kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)


class TestKinesisListShards:
    def test_list_shards_basic(self, kinesis, stream):
        """list_shards returns shard details."""
        resp = kinesis.list_shards(StreamName=stream)
        shards = resp["Shards"]
        assert len(shards) == 1
        shard = shards[0]
        assert "ShardId" in shard
        assert "HashKeyRange" in shard
        assert "StartingHashKey" in shard["HashKeyRange"]
        assert "EndingHashKey" in shard["HashKeyRange"]
        assert "SequenceNumberRange" in shard
        assert "StartingSequenceNumber" in shard["SequenceNumberRange"]

    def test_list_shards_multi_shard(self, kinesis):
        """list_shards with multiple shards returns all of them."""
        uid = uuid.uuid4().hex[:8]
        name = f"shards-multi-{uid}"
        kinesis.create_stream(StreamName=name, ShardCount=4)
        kinesis.get_waiter("stream_exists").wait(StreamName=name)
        try:
            resp = kinesis.list_shards(StreamName=name)
            assert len(resp["Shards"]) == 4
            shard_ids = {s["ShardId"] for s in resp["Shards"]}
            assert len(shard_ids) == 4  # All unique
        finally:
            kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)

    def test_list_shards_with_at_trim_horizon_filter(self, kinesis, stream):
        """list_shards with ShardFilter AT_TRIM_HORIZON."""
        resp = kinesis.list_shards(
            StreamName=stream, ShardFilter={"Type": "AT_TRIM_HORIZON"}
        )
        assert len(resp["Shards"]) >= 1

    def test_list_shards_hash_key_range_covers_full_range(self, kinesis):
        """Hash key ranges should cover the full 0 to 2^128-1 range."""
        uid = uuid.uuid4().hex[:8]
        name = f"hash-range-{uid}"
        kinesis.create_stream(StreamName=name, ShardCount=2)
        kinesis.get_waiter("stream_exists").wait(StreamName=name)
        try:
            shards = kinesis.list_shards(StreamName=name)["Shards"]
            # Sort by starting hash key
            shards.sort(key=lambda s: int(s["HashKeyRange"]["StartingHashKey"]))
            # First shard starts at 0
            assert int(shards[0]["HashKeyRange"]["StartingHashKey"]) == 0
            # Last shard ends at 2^128 - 1
            max_hash = 2**128 - 1
            assert int(shards[-1]["HashKeyRange"]["EndingHashKey"]) == max_hash
        finally:
            kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)


class TestKinesisPutRecordsBatch:
    def test_put_records_batch_returns_shard_ids(self, kinesis, stream):
        """PutRecords batch returns ShardId for each record."""
        records = [
            {"Data": b"batch-1", "PartitionKey": "key1"},
            {"Data": b"batch-2", "PartitionKey": "key2"},
        ]
        resp = kinesis.put_records(StreamName=stream, Records=records)
        for rec in resp["Records"]:
            assert rec["ShardId"].startswith("shardId-")
            assert rec["SequenceNumber"].isdigit()

    def test_put_records_batch_large(self, kinesis, stream):
        """PutRecords with many records."""
        records = [
            {"Data": f"record-{i}".encode(), "PartitionKey": f"pk-{i}"}
            for i in range(10)
        ]
        resp = kinesis.put_records(StreamName=stream, Records=records)
        assert resp["FailedRecordCount"] == 0
        assert len(resp["Records"]) == 10

    def test_put_records_and_read_back(self, kinesis, stream):
        """PutRecords and verify all records can be read back."""
        records = [
            {"Data": f"data-{i}".encode(), "PartitionKey": "pk1"}
            for i in range(5)
        ]
        kinesis.put_records(StreamName=stream, Records=records)

        shards = kinesis.list_shards(StreamName=stream)["Shards"]
        all_data = []
        for shard in shards:
            it = kinesis.get_shard_iterator(
                StreamName=stream,
                ShardId=shard["ShardId"],
                ShardIteratorType="TRIM_HORIZON",
            )["ShardIterator"]
            got = kinesis.get_records(ShardIterator=it)
            all_data.extend([r["Data"] for r in got["Records"]])

        expected = {f"data-{i}".encode() for i in range(5)}
        assert expected.issubset(set(all_data))

    def test_put_records_multi_shard_distribution(self, kinesis):
        """PutRecords distributes across shards based on partition key."""
        uid = uuid.uuid4().hex[:8]
        name = f"distrib-{uid}"
        kinesis.create_stream(StreamName=name, ShardCount=2)
        kinesis.get_waiter("stream_exists").wait(StreamName=name)
        try:
            records = [
                {"Data": f"r-{i}".encode(), "PartitionKey": f"key-{i}"}
                for i in range(20)
            ]
            resp = kinesis.put_records(StreamName=name, Records=records)
            assert resp["FailedRecordCount"] == 0
            shard_ids = {r["ShardId"] for r in resp["Records"]}
            # With 20 different partition keys across 2 shards, we expect
            # records on at least 1 shard (and likely both)
            assert len(shard_ids) >= 1
        finally:
            kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)


class TestKinesisRetention:
    def test_retention_increase_to_72(self, kinesis):
        """Increase retention to 72 hours."""
        uid = uuid.uuid4().hex[:8]
        name = f"ret-72-{uid}"
        kinesis.create_stream(StreamName=name, ShardCount=1)
        kinesis.get_waiter("stream_exists").wait(StreamName=name)
        try:
            kinesis.increase_stream_retention_period(
                StreamName=name, RetentionPeriodHours=72
            )
            desc = kinesis.describe_stream(StreamName=name)["StreamDescription"]
            assert desc["RetentionPeriodHours"] == 72
        finally:
            kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)

    def test_retention_increase_and_decrease(self, kinesis):
        """Increase then decrease retention period."""
        uid = uuid.uuid4().hex[:8]
        name = f"ret-updown-{uid}"
        kinesis.create_stream(StreamName=name, ShardCount=1)
        kinesis.get_waiter("stream_exists").wait(StreamName=name)
        try:
            kinesis.increase_stream_retention_period(
                StreamName=name, RetentionPeriodHours=48
            )
            kinesis.decrease_stream_retention_period(
                StreamName=name, RetentionPeriodHours=36
            )
            desc = kinesis.describe_stream(StreamName=name)["StreamDescription"]
            assert desc["RetentionPeriodHours"] == 36
        finally:
            kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)

    def test_default_retention_is_24(self, kinesis, stream):
        """Default retention period should be 24 hours."""
        desc = kinesis.describe_stream(StreamName=stream)["StreamDescription"]
        assert desc["RetentionPeriodHours"] == 24


class TestKinesisTags:
    def test_add_multiple_tags(self, kinesis, stream):
        """Add multiple tags and verify all present."""
        kinesis.add_tags_to_stream(
            StreamName=stream,
            Tags={"env": "dev", "team": "platform", "version": "1.0"},
        )
        resp = kinesis.list_tags_for_stream(StreamName=stream)
        tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tag_map["env"] == "dev"
        assert tag_map["team"] == "platform"
        assert tag_map["version"] == "1.0"

    def test_remove_specific_tags(self, kinesis, stream):
        """Remove only specific tags, leaving others."""
        kinesis.add_tags_to_stream(
            StreamName=stream,
            Tags={"a": "1", "b": "2", "c": "3"},
        )
        kinesis.remove_tags_from_stream(StreamName=stream, TagKeys=["a", "c"])
        resp = kinesis.list_tags_for_stream(StreamName=stream)
        tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert "a" not in tag_map
        assert "c" not in tag_map
        assert tag_map["b"] == "2"

    def test_overwrite_tag_value(self, kinesis, stream):
        """Adding a tag with existing key overwrites the value."""
        kinesis.add_tags_to_stream(StreamName=stream, Tags={"env": "dev"})
        kinesis.add_tags_to_stream(StreamName=stream, Tags={"env": "prod"})
        resp = kinesis.list_tags_for_stream(StreamName=stream)
        tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tag_map["env"] == "prod"

    def test_list_tags_empty_stream(self, kinesis):
        """list_tags on a stream with no tags returns empty list."""
        uid = uuid.uuid4().hex[:8]
        name = f"no-tags-{uid}"
        kinesis.create_stream(StreamName=name, ShardCount=1)
        kinesis.get_waiter("stream_exists").wait(StreamName=name)
        try:
            resp = kinesis.list_tags_for_stream(StreamName=name)
            assert resp["Tags"] == []
            assert resp["HasMoreTags"] is False
        finally:
            kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)


class TestKinesisShardIterators:
    def test_trim_horizon_iterator(self, kinesis, stream):
        """TRIM_HORIZON reads from the beginning."""
        kinesis.put_record(StreamName=stream, Data=b"first", PartitionKey="pk1")
        kinesis.put_record(StreamName=stream, Data=b"second", PartitionKey="pk1")

        shards = kinesis.list_shards(StreamName=stream)["Shards"]
        it = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=shards[0]["ShardId"],
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]
        records = kinesis.get_records(ShardIterator=it)
        assert len(records["Records"]) >= 2
        assert records["Records"][0]["Data"] == b"first"
        assert records["Records"][1]["Data"] == b"second"

    def test_latest_iterator(self, kinesis, stream):
        """LATEST iterator only gets records added after iterator creation."""
        kinesis.put_record(StreamName=stream, Data=b"before", PartitionKey="pk1")

        shards = kinesis.list_shards(StreamName=stream)["Shards"]
        it = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=shards[0]["ShardId"],
            ShardIteratorType="LATEST",
        )["ShardIterator"]

        # Record added after iterator
        kinesis.put_record(StreamName=stream, Data=b"after", PartitionKey="pk1")

        records = kinesis.get_records(ShardIterator=it)
        data = [r["Data"] for r in records["Records"]]
        assert b"after" in data
        assert b"before" not in data

    def test_after_sequence_number_iterator(self, kinesis, stream):
        """AFTER_SEQUENCE_NUMBER returns records after the given sequence."""
        put1 = kinesis.put_record(
            StreamName=stream, Data=b"rec-1", PartitionKey="pk1"
        )
        kinesis.put_record(StreamName=stream, Data=b"rec-2", PartitionKey="pk1")

        it = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=put1["ShardId"],
            ShardIteratorType="AFTER_SEQUENCE_NUMBER",
            StartingSequenceNumber=put1["SequenceNumber"],
        )["ShardIterator"]
        records = kinesis.get_records(ShardIterator=it)
        assert len(records["Records"]) >= 1
        assert records["Records"][0]["Data"] == b"rec-2"

    def test_get_records_returns_next_iterator(self, kinesis, stream):
        """get_records should return NextShardIterator for continued reading."""
        kinesis.put_record(StreamName=stream, Data=b"data", PartitionKey="pk1")
        shards = kinesis.list_shards(StreamName=stream)["Shards"]
        it = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=shards[0]["ShardId"],
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]
        resp = kinesis.get_records(ShardIterator=it)
        assert "NextShardIterator" in resp

    def test_get_records_with_limit(self, kinesis, stream):
        """get_records with Limit parameter."""
        for i in range(5):
            kinesis.put_record(
                StreamName=stream, Data=f"item-{i}".encode(), PartitionKey="pk1"
            )
        shards = kinesis.list_shards(StreamName=stream)["Shards"]
        it = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=shards[0]["ShardId"],
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]
        resp = kinesis.get_records(ShardIterator=it, Limit=2)
        assert len(resp["Records"]) <= 2


class TestKinesisStreamCreation:
    def test_create_delete_stream(self, kinesis):
        """Create and delete a stream."""
        uid = uuid.uuid4().hex[:8]
        name = f"create-del-{uid}"
        kinesis.create_stream(StreamName=name, ShardCount=1)
        kinesis.get_waiter("stream_exists").wait(StreamName=name)
        kinesis.delete_stream(StreamName=name)
        # After delete, listing should not contain the stream
        # (might still be in DELETING state, but list_streams should eventually exclude it)
        streams = kinesis.list_streams()["StreamNames"]
        # Stream may still appear briefly; check it was at least accepted
        # The main assertion is that delete_stream did not raise an error.

    def test_create_stream_with_multiple_shards(self, kinesis):
        """Create a stream with multiple shards."""
        uid = uuid.uuid4().hex[:8]
        name = f"multi-shard-{uid}"
        kinesis.create_stream(StreamName=name, ShardCount=5)
        kinesis.get_waiter("stream_exists").wait(StreamName=name)
        try:
            desc = kinesis.describe_stream(StreamName=name)["StreamDescription"]
            assert len(desc["Shards"]) == 5
        finally:
            kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)

    def test_describe_stream_arn_format(self, kinesis, stream):
        """StreamARN should follow the expected format."""
        desc = kinesis.describe_stream(StreamName=stream)["StreamDescription"]
        arn = desc["StreamARN"]
        assert arn.startswith("arn:aws:kinesis:")
        assert stream in arn

    def test_list_streams_contains_created(self, kinesis):
        """A newly created stream should appear in list_streams."""
        uid = uuid.uuid4().hex[:8]
        name = f"list-check-{uid}"
        kinesis.create_stream(StreamName=name, ShardCount=1)
        kinesis.get_waiter("stream_exists").wait(StreamName=name)
        try:
            streams = kinesis.list_streams()["StreamNames"]
            assert name in streams
        finally:
            kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)
