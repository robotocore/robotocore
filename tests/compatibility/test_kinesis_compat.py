"""Kinesis compatibility tests."""

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
            StreamName=stream, Data=b"seq-test", PartitionKey="pk1",
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
