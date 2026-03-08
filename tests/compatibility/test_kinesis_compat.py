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
            {"Data": f"batch-record-{i}".encode(), "PartitionKey": f"pk-{i}"} for i in range(10)
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
        records = [{"Data": f"limit-rec-{i}".encode(), "PartitionKey": "pk1"} for i in range(5)]
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
        kinesis.put_record(StreamName=stream, Data=b"before-latest", PartitionKey="pk1")

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
        stream_arn = kinesis.describe_stream(StreamName=stream)["StreamDescription"]["StreamARN"]

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
        stream_arn = kinesis.describe_stream(StreamName=stream)["StreamDescription"]["StreamARN"]

        kinesis.register_stream_consumer(
            StreamARN=stream_arn,
            ConsumerName="consumer-list-test",
        )
        response = kinesis.list_stream_consumers(StreamARN=stream_arn)
        names = [c["ConsumerName"] for c in response["Consumers"]]
        assert "consumer-list-test" in names

    def test_deregister_stream_consumer(self, kinesis, stream):
        """DeregisterStreamConsumer removes a consumer."""
        stream_arn = kinesis.describe_stream(StreamName=stream)["StreamDescription"]["StreamARN"]

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
                kinesis.deregister_stream_consumer(StreamARN=stream_arn, ConsumerName=consumer_name)
            except ClientError:
                pass

    def test_put_records_batch_all_succeed(self, kinesis, stream):
        """PutRecords with multiple records, verify all succeed."""
        records = [{"Data": f"rec-{i}".encode(), "PartitionKey": f"pk-{i}"} for i in range(10)]
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
        kinesis.put_record(StreamName=stream, Data=b"before-latest", PartitionKey="pk1")

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
        kinesis.put_record(StreamName=stream, Data=b"after-latest", PartitionKey="pk1")

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

    def test_increase_stream_retention_period_v2(self, kinesis, stream):
        """IncreaseStreamRetentionPeriod beyond default 24 hours."""
        kinesis.increase_stream_retention_period(StreamName=stream, RetentionPeriodHours=48)
        desc = kinesis.describe_stream(StreamName=stream)
        assert desc["StreamDescription"]["RetentionPeriodHours"] >= 48

    def test_decrease_stream_retention_period_v2(self, kinesis, stream):
        """DecreaseStreamRetentionPeriod back to 24 hours."""
        kinesis.increase_stream_retention_period(StreamName=stream, RetentionPeriodHours=48)
        kinesis.decrease_stream_retention_period(StreamName=stream, RetentionPeriodHours=24)
        desc = kinesis.describe_stream(StreamName=stream)
        assert desc["StreamDescription"]["RetentionPeriodHours"] == 24

    def test_add_tags_to_stream(self, kinesis, stream):
        kinesis.add_tags_to_stream(
            StreamName=stream,
            Tags={"env": "test", "team": "platform"},
        )
        resp = kinesis.list_tags_for_stream(StreamName=stream)
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags["env"] == "test"
        assert tags["team"] == "platform"

    def test_remove_tags_from_stream_v2(self, kinesis, stream):
        kinesis.add_tags_to_stream(StreamName=stream, Tags={"keep": "yes", "drop": "no"})
        kinesis.remove_tags_from_stream(StreamName=stream, TagKeys=["drop"])
        resp = kinesis.list_tags_for_stream(StreamName=stream)
        keys = [t["Key"] for t in resp["Tags"]]
        assert "keep" in keys
        assert "drop" not in keys

    def test_describe_stream_summary_v2(self, kinesis, stream):
        resp = kinesis.describe_stream_summary(StreamName=stream)
        summary = resp["StreamDescriptionSummary"]
        assert summary["StreamName"] == stream
        assert "StreamARN" in summary
        assert "StreamStatus" in summary
        assert "OpenShardCount" in summary

    def test_list_streams_v2(self, kinesis, stream):
        resp = kinesis.list_streams()
        assert stream in resp["StreamNames"]

    def test_put_record_explicit_hash_key(self, kinesis, stream):
        resp = kinesis.put_record(
            StreamName=stream,
            Data=b"hash-key-data",
            PartitionKey="pk1",
            ExplicitHashKey="170141183460469231731687303715884105727",
        )
        assert "ShardId" in resp
        assert "SequenceNumber" in resp

    def test_get_records_limit(self, kinesis, stream):
        """GetRecords with Limit parameter."""
        for i in range(5):
            kinesis.put_record(StreamName=stream, Data=f"rec-{i}".encode(), PartitionKey="pk")
        desc = kinesis.describe_stream(StreamName=stream)
        shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]
        iterator = kinesis.get_shard_iterator(
            StreamName=stream, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON"
        )["ShardIterator"]
        time.sleep(0.5)
        resp = kinesis.get_records(ShardIterator=iterator, Limit=2)
        assert len(resp["Records"]) <= 2

    def test_register_deregister_stream_consumer(self, kinesis, stream):
        """RegisterStreamConsumer, DescribeStreamConsumer, DeregisterStreamConsumer."""
        desc = kinesis.describe_stream(StreamName=stream)
        stream_arn = desc["StreamDescription"]["StreamARN"]
        consumer_name = f"consumer-{uuid.uuid4().hex[:8]}"
        try:
            reg = kinesis.register_stream_consumer(StreamARN=stream_arn, ConsumerName=consumer_name)
            consumer = reg["Consumer"]
            assert consumer["ConsumerName"] == consumer_name
            assert "ConsumerARN" in consumer

            desc_resp = kinesis.describe_stream_consumer(
                StreamARN=stream_arn, ConsumerName=consumer_name
            )
            assert desc_resp["ConsumerDescription"]["ConsumerName"] == consumer_name
        finally:
            try:
                kinesis.deregister_stream_consumer(StreamARN=stream_arn, ConsumerName=consumer_name)
            except ClientError:
                pass

    def test_list_stream_consumers_v2(self, kinesis, stream):
        """ListStreamConsumers."""
        desc = kinesis.describe_stream(StreamName=stream)
        stream_arn = desc["StreamDescription"]["StreamARN"]
        resp = kinesis.list_stream_consumers(StreamARN=stream_arn)
        assert "Consumers" in resp


class TestKinesisExtended:
    """Extended Kinesis compatibility tests covering additional operations and edge cases."""

    def test_create_stream_with_multiple_shards(self, kinesis):
        """Create a stream with 5 shards and verify shard count."""
        name = f"multi-{uuid.uuid4().hex[:8]}"
        try:
            kinesis.create_stream(StreamName=name, ShardCount=5)
            kinesis.get_waiter("stream_exists").wait(StreamName=name)
            desc = kinesis.describe_stream(StreamName=name)["StreamDescription"]
            assert desc["StreamName"] == name
            assert len(desc["Shards"]) == 5
        finally:
            try:
                kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)
            except ClientError:
                pass

    def test_describe_stream_all_fields(self, kinesis, stream):
        """Verify all expected fields in DescribeStream response."""
        desc = kinesis.describe_stream(StreamName=stream)["StreamDescription"]
        assert desc["StreamName"] == stream
        assert "StreamARN" in desc
        assert desc["StreamStatus"] == "ACTIVE"
        assert isinstance(desc["Shards"], list)
        assert "HasMoreShards" in desc
        assert isinstance(desc["HasMoreShards"], bool)
        assert "RetentionPeriodHours" in desc
        assert desc["RetentionPeriodHours"] == 24  # default
        assert "EnhancedMonitoring" in desc
        assert "StreamCreationTimestamp" in desc

    def test_describe_stream_has_more_shards_false(self, kinesis, stream):
        """HasMoreShards is False when all shards fit in response."""
        desc = kinesis.describe_stream(StreamName=stream)["StreamDescription"]
        assert desc["HasMoreShards"] is False

    def test_list_streams_with_limit(self, kinesis):
        """ListStreams with Limit parameter for pagination."""
        names = [f"limit-test-{uuid.uuid4().hex[:8]}" for _ in range(3)]
        try:
            for name in names:
                kinesis.create_stream(StreamName=name, ShardCount=1)
            for name in names:
                kinesis.get_waiter("stream_exists").wait(StreamName=name)

            response = kinesis.list_streams(Limit=1)
            assert "StreamNames" in response
            assert len(response["StreamNames"]) >= 1
            assert "HasMoreStreams" in response
        finally:
            for name in names:
                try:
                    kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)
                except ClientError:
                    pass

    def test_list_streams_has_more_streams(self, kinesis):
        """ListStreams HasMoreStreams field is correct with pagination."""
        names = [f"hasmore-{uuid.uuid4().hex[:8]}" for _ in range(3)]
        try:
            for name in names:
                kinesis.create_stream(StreamName=name, ShardCount=1)
            for name in names:
                kinesis.get_waiter("stream_exists").wait(StreamName=name)

            # Request with limit smaller than total streams
            resp = kinesis.list_streams(Limit=1)
            # With at least 3 streams and limit=1, HasMoreStreams should be True
            assert resp["HasMoreStreams"] is True
        finally:
            for name in names:
                try:
                    kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)
                except ClientError:
                    pass

    def test_list_streams_exclusive_start(self, kinesis):
        """ListStreams with ExclusiveStartStreamName for pagination."""
        names = sorted([f"page-{uuid.uuid4().hex[:8]}" for _ in range(3)])
        try:
            for name in names:
                kinesis.create_stream(StreamName=name, ShardCount=1)
            for name in names:
                kinesis.get_waiter("stream_exists").wait(StreamName=name)

            # Get first page
            resp1 = kinesis.list_streams(Limit=1)
            first_stream = resp1["StreamNames"][0]

            # Get next page starting after the first
            resp2 = kinesis.list_streams(Limit=100, ExclusiveStartStreamName=first_stream)
            assert first_stream not in resp2["StreamNames"]
        finally:
            for name in names:
                try:
                    kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)
                except ClientError:
                    pass

    def test_after_sequence_number_iterator(self, kinesis, stream):
        """AFTER_SEQUENCE_NUMBER iterator skips the record at the given sequence."""
        # Put two records
        resp1 = kinesis.put_record(StreamName=stream, Data=b"first-record", PartitionKey="pk1")
        kinesis.put_record(StreamName=stream, Data=b"second-record", PartitionKey="pk1")

        shard_id = resp1["ShardId"]
        seq = resp1["SequenceNumber"]

        # AFTER_SEQUENCE_NUMBER should skip the first record
        iterator = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=shard_id,
            ShardIteratorType="AFTER_SEQUENCE_NUMBER",
            StartingSequenceNumber=seq,
        )["ShardIterator"]

        records = kinesis.get_records(ShardIterator=iterator)["Records"]
        assert len(records) >= 1
        # First record returned should be "second-record", not "first-record"
        assert records[0]["Data"] == b"second-record"

    def test_get_records_millis_behind_latest(self, kinesis, stream):
        """GetRecords response includes MillisBehindLatest field."""
        kinesis.put_record(StreamName=stream, Data=b"millis-test", PartitionKey="pk1")
        shard_id = kinesis.list_shards(StreamName=stream)["Shards"][0]["ShardId"]
        iterator = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]

        response = kinesis.get_records(ShardIterator=iterator)
        assert "MillisBehindLatest" in response
        assert isinstance(response["MillisBehindLatest"], int)

    def test_get_records_returns_next_shard_iterator(self, kinesis, stream):
        """GetRecords always returns NextShardIterator for open shards."""
        shard_id = kinesis.list_shards(StreamName=stream)["Shards"][0]["ShardId"]
        iterator = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]

        response = kinesis.get_records(ShardIterator=iterator)
        assert "NextShardIterator" in response
        assert response["NextShardIterator"] is not None

    def test_put_records_batch_with_explicit_hash_key(self, kinesis, stream):
        """PutRecords batch where records include ExplicitHashKey."""
        records = [
            {"Data": b"hash-batch-0", "PartitionKey": "pk1", "ExplicitHashKey": "0"},
            {
                "Data": b"hash-batch-1",
                "PartitionKey": "pk2",
                "ExplicitHashKey": "170141183460469231731687303715884105727",
            },
        ]
        response = kinesis.put_records(StreamName=stream, Records=records)
        assert response["FailedRecordCount"] == 0
        assert len(response["Records"]) == 2

    def test_list_shards_with_max_results(self, kinesis):
        """ListShards with MaxResults for pagination."""
        name = f"shardpage-{uuid.uuid4().hex[:8]}"
        try:
            kinesis.create_stream(StreamName=name, ShardCount=4)
            kinesis.get_waiter("stream_exists").wait(StreamName=name)

            response = kinesis.list_shards(StreamName=name, MaxResults=2)
            assert len(response["Shards"]) == 2
            assert "NextToken" in response
        finally:
            try:
                kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)
            except ClientError:
                pass

    def test_list_shards_pagination_next_token(self, kinesis):
        """ListShards pagination using NextToken."""
        name = f"shardnext-{uuid.uuid4().hex[:8]}"
        try:
            kinesis.create_stream(StreamName=name, ShardCount=4)
            kinesis.get_waiter("stream_exists").wait(StreamName=name)

            page1 = kinesis.list_shards(StreamName=name, MaxResults=2)
            assert len(page1["Shards"]) == 2
            assert "NextToken" in page1

            page2 = kinesis.list_shards(NextToken=page1["NextToken"])
            assert len(page2["Shards"]) == 2

            # All shard IDs across pages should be unique
            all_ids = [s["ShardId"] for s in page1["Shards"] + page2["Shards"]]
            assert len(set(all_ids)) == 4
        finally:
            try:
                kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)
            except ClientError:
                pass

    def test_create_stream_duplicate_name_error(self, kinesis, stream):
        """Creating a stream with an existing name raises ResourceInUseException."""
        with pytest.raises(ClientError) as exc_info:
            kinesis.create_stream(StreamName=stream, ShardCount=1)
        assert exc_info.value.response["Error"]["Code"] == "ResourceInUseException"

    def test_delete_nonexistent_stream_error(self, kinesis):
        """Deleting a non-existent stream raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            kinesis.delete_stream(StreamName=f"nonexistent-{uuid.uuid4().hex[:8]}")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_nonexistent_stream_error(self, kinesis):
        """Describing a non-existent stream raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            kinesis.describe_stream(StreamName=f"nonexistent-{uuid.uuid4().hex[:8]}")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_put_record_to_nonexistent_stream_error(self, kinesis):
        """PutRecord to a non-existent stream raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            kinesis.put_record(
                StreamName=f"nonexistent-{uuid.uuid4().hex[:8]}",
                Data=b"test",
                PartitionKey="pk1",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_add_multiple_tags_and_verify(self, kinesis, stream):
        """Add multiple batches of tags and verify they accumulate."""
        kinesis.add_tags_to_stream(
            StreamName=stream,
            Tags={"key1": "val1", "key2": "val2"},
        )
        kinesis.add_tags_to_stream(
            StreamName=stream,
            Tags={"key3": "val3"},
        )
        response = kinesis.list_tags_for_stream(StreamName=stream)
        tag_map = {t["Key"]: t["Value"] for t in response["Tags"]}
        assert tag_map["key1"] == "val1"
        assert tag_map["key2"] == "val2"
        assert tag_map["key3"] == "val3"

    def test_overwrite_tag_value(self, kinesis, stream):
        """Adding a tag with an existing key overwrites the value."""
        kinesis.add_tags_to_stream(StreamName=stream, Tags={"mykey": "original"})
        kinesis.add_tags_to_stream(StreamName=stream, Tags={"mykey": "updated"})
        response = kinesis.list_tags_for_stream(StreamName=stream)
        tag_map = {t["Key"]: t["Value"] for t in response["Tags"]}
        assert tag_map["mykey"] == "updated"

    def test_list_tags_has_more_tags(self, kinesis, stream):
        """ListTagsForStream HasMoreTags field."""
        response = kinesis.list_tags_for_stream(StreamName=stream)
        assert "HasMoreTags" in response
        assert isinstance(response["HasMoreTags"], bool)

    def test_describe_stream_consumer_by_arn(self, kinesis, stream):
        """DescribeStreamConsumer using ConsumerARN."""
        stream_arn = kinesis.describe_stream(StreamName=stream)["StreamDescription"]["StreamARN"]
        consumer_name = f"consumer-{uuid.uuid4().hex[:8]}"
        try:
            reg_resp = kinesis.register_stream_consumer(
                StreamARN=stream_arn, ConsumerName=consumer_name
            )
            consumer_arn = reg_resp["Consumer"]["ConsumerARN"]

            desc_resp = kinesis.describe_stream_consumer(ConsumerARN=consumer_arn)
            assert desc_resp["ConsumerDescription"]["ConsumerName"] == consumer_name
            assert desc_resp["ConsumerDescription"]["ConsumerARN"] == consumer_arn
            assert "ConsumerCreationTimestamp" in desc_resp["ConsumerDescription"]
        finally:
            try:
                kinesis.deregister_stream_consumer(ConsumerARN=consumer_arn)
            except ClientError:
                pass

    def test_register_duplicate_consumer_error(self, kinesis, stream):
        """Registering a consumer with the same name raises ResourceInUseException."""
        stream_arn = kinesis.describe_stream(StreamName=stream)["StreamDescription"]["StreamARN"]
        consumer_name = f"dup-consumer-{uuid.uuid4().hex[:8]}"
        try:
            kinesis.register_stream_consumer(StreamARN=stream_arn, ConsumerName=consumer_name)
            with pytest.raises(ClientError) as exc_info:
                kinesis.register_stream_consumer(StreamARN=stream_arn, ConsumerName=consumer_name)
            assert exc_info.value.response["Error"]["Code"] == "ResourceInUseException"
        finally:
            try:
                kinesis.deregister_stream_consumer(StreamARN=stream_arn, ConsumerName=consumer_name)
            except ClientError:
                pass

    def test_describe_stream_summary_fields(self, kinesis, stream):
        """DescribeStreamSummary contains all expected fields."""
        resp = kinesis.describe_stream_summary(StreamName=stream)
        summary = resp["StreamDescriptionSummary"]
        assert "StreamName" in summary
        assert "StreamARN" in summary
        assert "StreamStatus" in summary
        assert "RetentionPeriodHours" in summary
        assert "StreamCreationTimestamp" in summary
        assert "EnhancedMonitoring" in summary
        assert "OpenShardCount" in summary
        assert summary["OpenShardCount"] >= 1

    def test_put_record_returns_encryption_type(self, kinesis, stream):
        """PutRecord response includes EncryptionType field."""
        resp = kinesis.put_record(StreamName=stream, Data=b"enc-test", PartitionKey="pk1")
        assert "EncryptionType" in resp
        assert resp["EncryptionType"] in ("NONE", "KMS")

    def test_put_records_returns_encryption_type(self, kinesis, stream):
        """PutRecords response includes EncryptionType field."""
        records = [{"Data": b"enc-batch", "PartitionKey": "pk1"}]
        resp = kinesis.put_records(StreamName=stream, Records=records)
        assert "EncryptionType" in resp
        assert resp["EncryptionType"] in ("NONE", "KMS")

    def test_shard_hash_key_range_covers_full_space(self, kinesis):
        """Shard hash key ranges cover the full 0 to 2^128-1 space."""
        name = f"hashrange-{uuid.uuid4().hex[:8]}"
        try:
            kinesis.create_stream(StreamName=name, ShardCount=2)
            kinesis.get_waiter("stream_exists").wait(StreamName=name)

            shards = kinesis.list_shards(StreamName=name)["Shards"]
            # Sort by starting hash key
            shards.sort(key=lambda s: int(s["HashKeyRange"]["StartingHashKey"]))

            # First shard starts at 0
            assert int(shards[0]["HashKeyRange"]["StartingHashKey"]) == 0
            # Last shard ends at 2^128 - 1
            max_hash = 2**128 - 1
            assert int(shards[-1]["HashKeyRange"]["EndingHashKey"]) == max_hash
            # No gaps between shards
            for i in range(len(shards) - 1):
                end = int(shards[i]["HashKeyRange"]["EndingHashKey"])
                start_next = int(shards[i + 1]["HashKeyRange"]["StartingHashKey"])
                assert start_next == end + 1
        finally:
            try:
                kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)
            except ClientError:
                pass

    def test_describe_stream_with_limit(self, kinesis):
        """DescribeStream with Limit returns partial shard list and HasMoreShards=True."""
        name = f"desclimit-{uuid.uuid4().hex[:8]}"
        try:
            kinesis.create_stream(StreamName=name, ShardCount=4)
            kinesis.get_waiter("stream_exists").wait(StreamName=name)

            desc = kinesis.describe_stream(StreamName=name, Limit=2)["StreamDescription"]
            assert len(desc["Shards"]) == 2
            assert desc["HasMoreShards"] is True
        finally:
            try:
                kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)
            except ClientError:
                pass


class TestKinesisGapStubs:
    """Tests for gap operations: describe_limits, list_streams."""

    def test_describe_limits(self, kinesis):
        resp = kinesis.describe_limits()
        assert "ShardLimit" in resp
        assert "OpenShardCount" in resp
        assert isinstance(resp["ShardLimit"], int)
        assert isinstance(resp["OpenShardCount"], int)

    def test_list_streams(self, kinesis):
        resp = kinesis.list_streams()
        assert "StreamNames" in resp
        assert "HasMoreStreams" in resp


class TestKinesisAutoCoverage:
    """Auto-generated coverage tests for kinesis."""

    @pytest.fixture
    def client(self):
        return make_client("kinesis")

    def test_describe_account_settings(self, client):
        """DescribeAccountSettings returns a response."""
        client.describe_account_settings()


class TestKinesisResourcePolicy:
    """Tests for ResourcePolicy operations: Put, Get, Delete."""

    @pytest.fixture
    def client(self):
        return make_client("kinesis")

    @pytest.fixture
    def stream_arn(self, client):
        name = f"rp-stream-{uuid.uuid4().hex[:8]}"
        client.create_stream(StreamName=name, ShardCount=1)
        client.get_waiter("stream_exists").wait(StreamName=name)
        desc = client.describe_stream(StreamName=name)
        arn = desc["StreamDescription"]["StreamARN"]
        yield arn
        client.delete_stream(StreamName=name, EnforceConsumerDeletion=True)

    def test_put_resource_policy(self, client, stream_arn):
        """PutResourcePolicy sets a policy on a stream."""
        import json

        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "123456789012"},
                        "Action": "kinesis:DescribeStream",
                        "Resource": stream_arn,
                    }
                ],
            }
        )
        resp = client.put_resource_policy(ResourceARN=stream_arn, Policy=policy)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_resource_policy(self, client, stream_arn):
        """GetResourcePolicy retrieves a previously set policy."""
        import json

        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "123456789012"},
                        "Action": "kinesis:DescribeStream",
                        "Resource": stream_arn,
                    }
                ],
            }
        )
        client.put_resource_policy(ResourceARN=stream_arn, Policy=policy)
        resp = client.get_resource_policy(ResourceARN=stream_arn)
        assert "Policy" in resp

    def test_delete_resource_policy(self, client, stream_arn):
        """DeleteResourcePolicy removes a policy from a stream."""
        import json

        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "123456789012"},
                        "Action": "kinesis:DescribeStream",
                        "Resource": stream_arn,
                    }
                ],
            }
        )
        client.put_resource_policy(ResourceARN=stream_arn, Policy=policy)
        resp = client.delete_resource_policy(ResourceARN=stream_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestKinesisConsumerEdgeCases:
    """Edge case tests for Kinesis stream consumers."""

    @pytest.fixture
    def kinesis(self):
        return make_client("kinesis")

    @pytest.fixture
    def stream_with_arn(self, kinesis):
        name = f"cons-edge-{uuid.uuid4().hex[:8]}"
        kinesis.create_stream(StreamName=name, ShardCount=1)
        kinesis.get_waiter("stream_exists").wait(StreamName=name)
        arn = kinesis.describe_stream(StreamName=name)["StreamDescription"]["StreamARN"]
        yield name, arn
        kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)

    def test_describe_stream_consumer_by_arn_only(self, kinesis, stream_with_arn):
        """DescribeStreamConsumer using ConsumerARN without StreamARN."""
        _, stream_arn = stream_with_arn
        reg = kinesis.register_stream_consumer(
            StreamARN=stream_arn, ConsumerName="arn-only-consumer"
        )
        consumer_arn = reg["Consumer"]["ConsumerARN"]
        time.sleep(1)

        desc = kinesis.describe_stream_consumer(ConsumerARN=consumer_arn)
        assert desc["ConsumerDescription"]["ConsumerName"] == "arn-only-consumer"
        assert desc["ConsumerDescription"]["ConsumerARN"] == consumer_arn
        assert "ConsumerStatus" in desc["ConsumerDescription"]

    def test_list_stream_consumers_multiple(self, kinesis, stream_with_arn):
        """ListStreamConsumers returns all registered consumers."""
        _, stream_arn = stream_with_arn
        for i in range(3):
            kinesis.register_stream_consumer(StreamARN=stream_arn, ConsumerName=f"multi-cons-{i}")
        time.sleep(1)

        resp = kinesis.list_stream_consumers(StreamARN=stream_arn)
        names = [c["ConsumerName"] for c in resp["Consumers"]]
        for i in range(3):
            assert f"multi-cons-{i}" in names

    def test_register_consumer_returns_creation_timestamp(self, kinesis, stream_with_arn):
        """RegisterStreamConsumer response includes ConsumerCreationTimestamp."""
        _, stream_arn = stream_with_arn
        reg = kinesis.register_stream_consumer(StreamARN=stream_arn, ConsumerName="ts-consumer")
        consumer = reg["Consumer"]
        assert "ConsumerCreationTimestamp" in consumer
        assert consumer["ConsumerName"] == "ts-consumer"
        assert consumer["ConsumerStatus"] in ("CREATING", "ACTIVE")

    def test_deregister_consumer_by_name(self, kinesis, stream_with_arn):
        """DeregisterStreamConsumer using StreamARN + ConsumerName."""
        _, stream_arn = stream_with_arn
        kinesis.register_stream_consumer(StreamARN=stream_arn, ConsumerName="dereg-by-name")
        time.sleep(1)

        kinesis.deregister_stream_consumer(StreamARN=stream_arn, ConsumerName="dereg-by-name")
        time.sleep(1)

        resp = kinesis.list_stream_consumers(StreamARN=stream_arn)
        names = [c["ConsumerName"] for c in resp["Consumers"]]
        assert "dereg-by-name" not in names


class TestKinesisRecordDetails:
    """Detailed tests for Kinesis record operations."""

    @pytest.fixture
    def kinesis(self):
        return make_client("kinesis")

    @pytest.fixture
    def stream(self, kinesis):
        name = f"rec-detail-{uuid.uuid4().hex[:8]}"
        kinesis.create_stream(StreamName=name, ShardCount=1)
        kinesis.get_waiter("stream_exists").wait(StreamName=name)
        yield name
        kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)

    def test_put_record_returns_shard_id_and_sequence(self, kinesis, stream):
        """PutRecord response has ShardId, SequenceNumber, and EncryptionType."""
        resp = kinesis.put_record(StreamName=stream, Data=b"detail-test", PartitionKey="pk1")
        assert "ShardId" in resp
        assert "SequenceNumber" in resp
        assert resp["ShardId"].startswith("shardId-")
        assert int(resp["SequenceNumber"]) >= 0

    def test_get_records_returns_millis_behind_latest(self, kinesis, stream):
        """GetRecords response includes MillisBehindLatest."""
        kinesis.put_record(StreamName=stream, Data=b"millis-test", PartitionKey="pk1")
        shard_id = kinesis.list_shards(StreamName=stream)["Shards"][0]["ShardId"]
        iterator = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]

        resp = kinesis.get_records(ShardIterator=iterator)
        assert "MillisBehindLatest" in resp
        assert isinstance(resp["MillisBehindLatest"], int)
        assert resp["MillisBehindLatest"] >= 0

    def test_get_records_record_fields(self, kinesis, stream):
        """Records have SequenceNumber, ArrivalTimestamp, Data, PartitionKey."""
        kinesis.put_record(StreamName=stream, Data=b"field-test", PartitionKey="pk-fields")
        shard_id = kinesis.list_shards(StreamName=stream)["Shards"][0]["ShardId"]
        iterator = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]

        resp = kinesis.get_records(ShardIterator=iterator)
        assert len(resp["Records"]) >= 1
        record = resp["Records"][0]
        assert "SequenceNumber" in record
        assert "ApproximateArrivalTimestamp" in record
        assert record["Data"] == b"field-test"
        assert record["PartitionKey"] == "pk-fields"

    def test_put_records_partial_failure_shape(self, kinesis, stream):
        """PutRecords response shape includes FailedRecordCount and per-record results."""
        records = [{"Data": f"batch-{i}".encode(), "PartitionKey": f"pk-{i}"} for i in range(5)]
        resp = kinesis.put_records(StreamName=stream, Records=records)
        assert "FailedRecordCount" in resp
        assert isinstance(resp["FailedRecordCount"], int)
        assert "Records" in resp
        assert len(resp["Records"]) == 5
        for rec in resp["Records"]:
            assert "ShardId" in rec
            assert "SequenceNumber" in rec

    def test_get_shard_iterator_after_sequence_number(self, kinesis, stream):
        """AFTER_SEQUENCE_NUMBER iterator starts after the given sequence."""
        put1 = kinesis.put_record(StreamName=stream, Data=b"first", PartitionKey="pk1")
        kinesis.put_record(StreamName=stream, Data=b"second", PartitionKey="pk1")

        iterator = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=put1["ShardId"],
            ShardIteratorType="AFTER_SEQUENCE_NUMBER",
            StartingSequenceNumber=put1["SequenceNumber"],
        )["ShardIterator"]

        resp = kinesis.get_records(ShardIterator=iterator)
        # Should get the second record (after the first)
        assert len(resp["Records"]) >= 1
        assert resp["Records"][0]["Data"] == b"second"


class TestKinesisStreamManagement:
    """Tests for stream management operations."""

    @pytest.fixture
    def kinesis(self):
        return make_client("kinesis")

    def test_describe_stream_has_retention_period(self, kinesis):
        """DescribeStream includes RetentionPeriodHours (default 24)."""
        name = f"ret-{uuid.uuid4().hex[:8]}"
        kinesis.create_stream(StreamName=name, ShardCount=1)
        kinesis.get_waiter("stream_exists").wait(StreamName=name)
        try:
            desc = kinesis.describe_stream(StreamName=name)["StreamDescription"]
            assert desc["RetentionPeriodHours"] == 24
        finally:
            kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)

    def test_describe_stream_summary_enhanced_monitoring(self, kinesis):
        """DescribeStreamSummary includes EnhancedMonitoring field."""
        name = f"enh-{uuid.uuid4().hex[:8]}"
        kinesis.create_stream(StreamName=name, ShardCount=1)
        kinesis.get_waiter("stream_exists").wait(StreamName=name)
        try:
            resp = kinesis.describe_stream_summary(StreamName=name)
            summary = resp["StreamDescriptionSummary"]
            assert "EnhancedMonitoring" in summary
            assert isinstance(summary["EnhancedMonitoring"], list)
        finally:
            kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)

    def test_list_shards_with_shard_filter(self, kinesis):
        """ListShards with ShardFilter to get only open shards."""
        name = f"filt-{uuid.uuid4().hex[:8]}"
        kinesis.create_stream(StreamName=name, ShardCount=2)
        kinesis.get_waiter("stream_exists").wait(StreamName=name)
        try:
            resp = kinesis.list_shards(
                StreamName=name,
                ShardFilter={"Type": "AT_TRIM_HORIZON"},
            )
            assert "Shards" in resp
            assert len(resp["Shards"]) == 2
        finally:
            kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)

    def test_update_shard_count_response_fields(self, kinesis):
        """UpdateShardCount returns stream name and shard counts."""
        name = f"upd-{uuid.uuid4().hex[:8]}"
        kinesis.create_stream(StreamName=name, ShardCount=1)
        kinesis.get_waiter("stream_exists").wait(StreamName=name)
        try:
            resp = kinesis.update_shard_count(
                StreamName=name,
                TargetShardCount=2,
                ScalingType="UNIFORM_SCALING",
            )
            assert resp["StreamName"] == name
            assert resp["CurrentShardCount"] == 1
            assert resp["TargetShardCount"] == 2
            assert "StreamARN" in resp
        finally:
            kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)

    def test_create_stream_on_demand_mode(self, kinesis):
        """CreateStream with StreamModeDetails ON_DEMAND."""
        name = f"ondemand-{uuid.uuid4().hex[:8]}"
        try:
            kinesis.create_stream(
                StreamName=name,
                StreamModeDetails={"StreamMode": "ON_DEMAND"},
            )
            kinesis.get_waiter("stream_exists").wait(StreamName=name)
            desc = kinesis.describe_stream(StreamName=name)["StreamDescription"]
            assert desc["StreamName"] == name
            assert desc["StreamStatus"] == "ACTIVE"
        finally:
            try:
                kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)
            except Exception:
                pass
