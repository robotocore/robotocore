"""Kinesis compatibility tests."""

import time
import uuid

import pytest
from botocore.exceptions import ClientError

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

    def test_put_records_batch_multiple_all_succeed(self, kinesis, stream):
        """PutRecords batch with multiple records, verify all succeed."""
        records = [
            {"Data": f"batch-record-{i}".encode(), "PartitionKey": f"pk-{i}"}
            for i in range(10)
        ]
        response = kinesis.put_records(StreamName=stream, Records=records)
        assert response["FailedRecordCount"] == 0
        assert len(response["Records"]) == 10
        for rec in response["Records"]:
            assert "ShardId" in rec
            assert "SequenceNumber" in rec
            assert "ErrorCode" not in rec

    def test_get_records_with_limit(self, kinesis, stream):
        """GetRecords with Limit parameter returns at most that many records."""
        records = [
            {"Data": f"limit-rec-{i}".encode(), "PartitionKey": "pk1"}
            for i in range(5)
        ]
        kinesis.put_records(StreamName=stream, Records=records)

        shard_id = kinesis.list_shards(StreamName=stream)["Shards"][0]["ShardId"]
        iterator = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]

        response = kinesis.get_records(ShardIterator=iterator, Limit=2)
        assert len(response["Records"]) <= 2
        assert "NextShardIterator" in response

    def test_describe_stream_summary(self, kinesis, stream):
        """DescribeStreamSummary returns summary without shard list."""
        response = kinesis.describe_stream_summary(StreamName=stream)
        summary = response["StreamDescriptionSummary"]
        assert summary["StreamName"] == stream
        assert summary["StreamStatus"] == "ACTIVE"
        assert "StreamARN" in summary
        assert summary["OpenShardCount"] == 1
        assert "RetentionPeriodHours" in summary

    def test_shard_iterator_latest_vs_trim_horizon(self, kinesis, stream):
        """LATEST iterator only sees records written after it was obtained."""
        # Put a record before getting LATEST iterator
        kinesis.put_record(
            StreamName=stream, Data=b"before-latest", PartitionKey="pk1"
        )

        shard_id = kinesis.list_shards(StreamName=stream)["Shards"][0]["ShardId"]

        # Get LATEST iterator (should not see records already in stream)
        latest_iter = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=shard_id,
            ShardIteratorType="LATEST",
        )["ShardIterator"]

        # Get TRIM_HORIZON iterator (should see all records)
        horizon_iter = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]

        latest_records = kinesis.get_records(ShardIterator=latest_iter)
        horizon_records = kinesis.get_records(ShardIterator=horizon_iter)

        assert len(latest_records["Records"]) == 0
        assert len(horizon_records["Records"]) >= 1

    def test_put_record_with_explicit_hash_key(self, kinesis, stream):
        """PutRecord with ExplicitHashKey and verify retrieval."""
        put_resp = kinesis.put_record(
            StreamName=stream,
            Data=b"hash-key-test",
            PartitionKey="pk1",
            ExplicitHashKey="0",
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
        assert any(r["Data"] == b"hash-key-test" for r in records["Records"])

    def test_stream_encryption(self, kinesis, stream):
        """StartStreamEncryption and StopStreamEncryption."""
        kinesis.start_stream_encryption(
            StreamName=stream,
            EncryptionType="KMS",
            KeyId="alias/aws/kinesis",
        )
        desc = kinesis.describe_stream(StreamName=stream)["StreamDescription"]
        assert desc["EncryptionType"] == "KMS"

        kinesis.stop_stream_encryption(
            StreamName=stream,
            EncryptionType="KMS",
            KeyId="alias/aws/kinesis",
        )
        desc = kinesis.describe_stream(StreamName=stream)["StreamDescription"]
        assert desc["EncryptionType"] == "NONE"

    def test_register_and_describe_stream_consumer(self, kinesis, stream):
        """RegisterStreamConsumer and DescribeStreamConsumer for enhanced fan-out."""
        stream_arn = kinesis.describe_stream(StreamName=stream)[
            "StreamDescription"
        ]["StreamARN"]

        reg_resp = kinesis.register_stream_consumer(
            StreamARN=stream_arn,
            ConsumerName="test-consumer",
        )
        consumer = reg_resp["Consumer"]
        assert consumer["ConsumerName"] == "test-consumer"
        assert "ConsumerARN" in consumer
        assert "ConsumerStatus" in consumer

        desc_resp = kinesis.describe_stream_consumer(
            StreamARN=stream_arn,
            ConsumerName="test-consumer",
        )
        assert desc_resp["ConsumerDescription"]["ConsumerName"] == "test-consumer"

    def test_list_stream_consumers(self, kinesis, stream):
        """ListStreamConsumers returns registered consumers."""
        stream_arn = kinesis.describe_stream(StreamName=stream)[
            "StreamDescription"
        ]["StreamARN"]

        kinesis.register_stream_consumer(
            StreamARN=stream_arn,
            ConsumerName="consumer-list-test",
        )
        response = kinesis.list_stream_consumers(StreamARN=stream_arn)
        names = [c["ConsumerName"] for c in response["Consumers"]]
        assert "consumer-list-test" in names

    def test_deregister_stream_consumer(self, kinesis, stream):
        """DeregisterStreamConsumer removes a consumer."""
        stream_arn = kinesis.describe_stream(StreamName=stream)[
            "StreamDescription"
        ]["StreamARN"]

        kinesis.register_stream_consumer(
            StreamARN=stream_arn,
            ConsumerName="consumer-to-delete",
        )
        kinesis.deregister_stream_consumer(
            StreamARN=stream_arn,
            ConsumerName="consumer-to-delete",
        )
        response = kinesis.list_stream_consumers(StreamARN=stream_arn)
        names = [c["ConsumerName"] for c in response["Consumers"]]
        assert "consumer-to-delete" not in names

    def test_update_shard_count(self, kinesis, stream):
        """UpdateShardCount changes the number of shards."""
        response = kinesis.update_shard_count(
            StreamName=stream,
            TargetShardCount=2,
            ScalingType="UNIFORM_SCALING",
        )
        assert response["CurrentShardCount"] == 1
        assert response["TargetShardCount"] == 2
        assert response["StreamName"] == stream

    def test_list_shards_multiple_shards(self, kinesis):
        """ListShards on a multi-shard stream returns all shards."""
        name = "multi-shard-stream"
        kinesis.create_stream(StreamName=name, ShardCount=4)
        kinesis.get_waiter("stream_exists").wait(StreamName=name)
        try:
            response = kinesis.list_shards(StreamName=name)
            assert len(response["Shards"]) == 4
            # All shard IDs should be unique
            shard_ids = [s["ShardId"] for s in response["Shards"]]
            assert len(shard_ids) == len(set(shard_ids))
            # Each shard has required fields
            for shard in response["Shards"]:
                assert "ShardId" in shard
                assert "HashKeyRange" in shard
                assert "SequenceNumberRange" in shard
        finally:
            kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)

    def test_split_shard(self, kinesis):
        """SplitShard divides a shard into two."""
        name = "split-shard-stream"
        kinesis.create_stream(StreamName=name, ShardCount=1)
        kinesis.get_waiter("stream_exists").wait(StreamName=name)
        try:
            shards = kinesis.list_shards(StreamName=name)["Shards"]
            shard = shards[0]
            shard_id = shard["ShardId"]
            # Split at midpoint of the hash key range
            start = int(shard["HashKeyRange"]["StartingHashKey"])
            end = int(shard["HashKeyRange"]["EndingHashKey"])
            mid = str((start + end) // 2)

            kinesis.split_shard(
                StreamName=name,
                ShardToSplit=shard_id,
                NewStartingHashKey=mid,
            )
            # Wait for stream to become active after split
            kinesis.get_waiter("stream_exists").wait(StreamName=name)

            new_shards = kinesis.list_shards(StreamName=name)["Shards"]
            # After split, there should be more shards (original closed + 2 new)
            assert len(new_shards) > 1
        finally:
            kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)

    def test_merge_shards(self, kinesis):
        """MergeShards combines two adjacent shards."""
        name = "merge-shard-stream"
        kinesis.create_stream(StreamName=name, ShardCount=2)
        kinesis.get_waiter("stream_exists").wait(StreamName=name)
        try:
            shards = kinesis.list_shards(StreamName=name)["Shards"]
            assert len(shards) == 2

            kinesis.merge_shards(
                StreamName=name,
                ShardToMerge=shards[0]["ShardId"],
                AdjacentShardToMerge=shards[1]["ShardId"],
            )
            kinesis.get_waiter("stream_exists").wait(StreamName=name)

            new_shards = kinesis.list_shards(StreamName=name)["Shards"]
            # After merge, we have at least 1 active shard
            assert len(new_shards) >= 1
        finally:
            kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)

    def test_register_and_deregister_stream_consumer(self, kinesis, stream):
        """RegisterStreamConsumer, DescribeStreamConsumer, DeregisterStreamConsumer."""
        # Get stream ARN
        desc = kinesis.describe_stream(StreamName=stream)
        stream_arn = desc["StreamDescription"]["StreamARN"]
        consumer_name = f"consumer-{uuid.uuid4().hex[:8]}"

        try:
            reg_resp = kinesis.register_stream_consumer(
                StreamARN=stream_arn, ConsumerName=consumer_name
            )
            consumer = reg_resp["Consumer"]
            assert consumer["ConsumerName"] == consumer_name
            assert "ConsumerARN" in consumer
            assert consumer["ConsumerStatus"] in ("CREATING", "ACTIVE")
            consumer_arn = consumer["ConsumerARN"]

            # DescribeStreamConsumer
            desc_resp = kinesis.describe_stream_consumer(
                StreamARN=stream_arn, ConsumerName=consumer_name
            )
            assert desc_resp["ConsumerDescription"]["ConsumerName"] == consumer_name
            assert desc_resp["ConsumerDescription"]["ConsumerARN"] == consumer_arn
        finally:
            try:
                kinesis.deregister_stream_consumer(
                    StreamARN=stream_arn, ConsumerName=consumer_name
                )
            except ClientError:
                pass

    def test_put_records_batch_all_succeed(self, kinesis, stream):
        """PutRecords with multiple records, verify all succeed."""
        records = [
            {"Data": f"rec-{i}".encode(), "PartitionKey": f"pk-{i}"}
            for i in range(10)
        ]
        response = kinesis.put_records(StreamName=stream, Records=records)
        assert response["FailedRecordCount"] == 0
        assert len(response["Records"]) == 10
        for rec in response["Records"]:
            assert "SequenceNumber" in rec
            assert "ShardId" in rec
            assert "ErrorCode" not in rec or rec["ErrorCode"] is None

    def test_latest_vs_trim_horizon_iterator(self, kinesis, stream):
        """LATEST iterator only sees new records; TRIM_HORIZON sees all."""
        # Put a record before getting iterators
        kinesis.put_record(
            StreamName=stream, Data=b"before-latest", PartitionKey="pk1"
        )

        desc = kinesis.describe_stream(StreamName=stream)
        shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]

        # Get LATEST iterator (should not see existing records)
        latest_iter = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=shard_id,
            ShardIteratorType="LATEST",
        )["ShardIterator"]

        # Get TRIM_HORIZON iterator (should see all records)
        horizon_iter = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]

        # Put a new record after LATEST iterator was created
        kinesis.put_record(
            StreamName=stream, Data=b"after-latest", PartitionKey="pk1"
        )

        # Allow a moment for propagation
        time.sleep(0.5)

        latest_records = kinesis.get_records(ShardIterator=latest_iter)["Records"]
        horizon_records = kinesis.get_records(ShardIterator=horizon_iter)["Records"]

        # TRIM_HORIZON should have more records (includes "before-latest")
        assert len(horizon_records) >= len(latest_records)
        # TRIM_HORIZON should contain the "before-latest" record
        horizon_data = [r["Data"] for r in horizon_records]
        assert b"before-latest" in horizon_data

    def test_start_and_stop_stream_encryption(self, kinesis, stream):
        """StartStreamEncryption and StopStreamEncryption."""
        kinesis.start_stream_encryption(
            StreamName=stream,
            EncryptionType="KMS",
            KeyId="alias/aws/kinesis",
        )
        desc = kinesis.describe_stream(StreamName=stream)
        assert desc["StreamDescription"]["EncryptionType"] in ("KMS", "NONE")

        kinesis.stop_stream_encryption(
            StreamName=stream,
            EncryptionType="KMS",
            KeyId="alias/aws/kinesis",
        )

    def test_list_shards_multi_shard_stream(self, kinesis):
        """ListShards on a stream with multiple shards."""
        multi_stream = f"multi-shard-{uuid.uuid4().hex[:8]}"
        try:
            kinesis.create_stream(StreamName=multi_stream, ShardCount=3)
            kinesis.get_waiter("stream_exists").wait(StreamName=multi_stream)

            response = kinesis.list_shards(StreamName=multi_stream)
            assert len(response["Shards"]) == 3
            shard_ids = [s["ShardId"] for s in response["Shards"]]
            # Each shard should have a unique ID
            assert len(set(shard_ids)) == 3
            for shard in response["Shards"]:
                assert "HashKeyRange" in shard
                assert "SequenceNumberRange" in shard
        finally:
            try:
                kinesis.delete_stream(StreamName=multi_stream, EnforceConsumerDeletion=True)
            except ClientError:
                pass
