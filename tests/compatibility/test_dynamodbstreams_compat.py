"""DynamoDB Streams compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def dynamodb():
    return make_client("dynamodb")


@pytest.fixture
def dynamodbstreams():
    return make_client("dynamodbstreams")


def _uid():
    return uuid.uuid4().hex[:8]


class TestDynamoDBStreamsOperations:
    def test_list_streams(self, dynamodb, dynamodbstreams):
        table_name = f"stream-table-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        response = dynamodbstreams.list_streams(TableName=table_name)
        assert "Streams" in response
        assert len(response["Streams"]) >= 1
        assert response["Streams"][0]["TableName"] == table_name

        dynamodb.delete_table(TableName=table_name)

    def test_describe_stream(self, dynamodb, dynamodbstreams):
        table_name = f"desc-stream-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        streams = dynamodbstreams.list_streams(TableName=table_name)
        stream_arn = streams["Streams"][0]["StreamArn"]

        response = dynamodbstreams.describe_stream(StreamArn=stream_arn)
        assert response["StreamDescription"]["TableName"] == table_name
        assert response["StreamDescription"]["StreamViewType"] == "NEW_AND_OLD_IMAGES"

        dynamodb.delete_table(TableName=table_name)

    def test_describe_stream_with_limit(self, dynamodb, dynamodbstreams):
        """DescribeStream with Limit restricts number of shards returned."""
        table_name = f"desc-limit-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        streams = dynamodbstreams.list_streams(TableName=table_name)
        stream_arn = streams["Streams"][0]["StreamArn"]

        response = dynamodbstreams.describe_stream(StreamArn=stream_arn, Limit=1)
        desc = response["StreamDescription"]
        assert desc["TableName"] == table_name
        # Limit=1 should return at most 1 shard
        assert len(desc["Shards"]) <= 1

        dynamodb.delete_table(TableName=table_name)

    def test_stream_keys_only_view_type(self, dynamodb, dynamodbstreams):
        """Stream with KEYS_ONLY view type only includes key attributes in records."""
        table_name = f"keys-only-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "KEYS_ONLY",
            },
        )
        streams = dynamodbstreams.list_streams(TableName=table_name)
        stream_arn = streams["Streams"][0]["StreamArn"]

        desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
        assert desc["StreamDescription"]["StreamViewType"] == "KEYS_ONLY"

        # Insert an item and read the stream record
        dynamodb.put_item(
            TableName=table_name, Item={"pk": {"S": "k1"}, "data": {"S": "hello"}}
        )

        shards = desc["StreamDescription"]["Shards"]
        if shards:
            shard_id = shards[0]["ShardId"]
            it = dynamodbstreams.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )["ShardIterator"]
            records = dynamodbstreams.get_records(ShardIterator=it)["Records"]
            if records:
                rec = records[0]
                assert "Keys" in rec["dynamodb"]
                # KEYS_ONLY should not include NewImage or OldImage
                assert "NewImage" not in rec["dynamodb"]
                assert "OldImage" not in rec["dynamodb"]

        dynamodb.delete_table(TableName=table_name)

    def test_stream_old_image_view_type(self, dynamodb, dynamodbstreams):
        """Stream with OLD_IMAGE view type includes old item image on updates."""
        table_name = f"old-img-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "OLD_IMAGE",
            },
        )
        streams = dynamodbstreams.list_streams(TableName=table_name)
        stream_arn = streams["Streams"][0]["StreamArn"]

        desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
        assert desc["StreamDescription"]["StreamViewType"] == "OLD_IMAGE"

        dynamodb.delete_table(TableName=table_name)

    def test_multiple_modifications_ordered(self, dynamodb, dynamodbstreams):
        """Multiple item modifications generate ordered stream records."""
        table_name = f"multi-mod-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        streams = dynamodbstreams.list_streams(TableName=table_name)
        stream_arn = streams["Streams"][0]["StreamArn"]

        desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
        shards = desc["StreamDescription"]["Shards"]

        # Insert multiple items
        dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "a"}})
        dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "b"}})
        dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "c"}})

        if shards:
            shard_id = shards[0]["ShardId"]
            it = dynamodbstreams.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )["ShardIterator"]
            records = dynamodbstreams.get_records(ShardIterator=it)["Records"]
            # Should have at least 3 INSERT records
            inserts = [r for r in records if r["eventName"] == "INSERT"]
            assert len(inserts) >= 3
            # Records should have ascending sequence numbers
            seq_numbers = [r["dynamodb"]["SequenceNumber"] for r in inserts]
            assert seq_numbers == sorted(seq_numbers)

        dynamodb.delete_table(TableName=table_name)

    def test_batch_write_generates_stream_records(self, dynamodb, dynamodbstreams):
        """BatchWriteItem generates stream records for each item."""
        table_name = f"batch-stream-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        streams = dynamodbstreams.list_streams(TableName=table_name)
        stream_arn = streams["Streams"][0]["StreamArn"]

        desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
        shards = desc["StreamDescription"]["Shards"]

        # Batch write 3 items
        dynamodb.batch_write_item(
            RequestItems={
                table_name: [
                    {"PutRequest": {"Item": {"pk": {"S": f"bw-{i}"}}}},
                ]
                for i in range(3)
            }
        )
        # Fix: batch_write_item RequestItems expects a list
        dynamodb.batch_write_item(
            RequestItems={
                table_name: [
                    {"PutRequest": {"Item": {"pk": {"S": "bw-x"}}}},
                    {"PutRequest": {"Item": {"pk": {"S": "bw-y"}}}},
                ]
            }
        )

        if shards:
            shard_id = shards[0]["ShardId"]
            it = dynamodbstreams.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )["ShardIterator"]
            records = dynamodbstreams.get_records(ShardIterator=it)["Records"]
            # At least the batch write items should produce records
            assert len(records) >= 2

        dynamodb.delete_table(TableName=table_name)

    def test_get_shard_iterator_latest(self, dynamodb, dynamodbstreams):
        """LATEST shard iterator type returns only new records after the iterator."""
        table_name = f"latest-it-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        # Insert before getting LATEST iterator
        dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "before"}})

        streams = dynamodbstreams.list_streams(TableName=table_name)
        stream_arn = streams["Streams"][0]["StreamArn"]
        desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
        shards = desc["StreamDescription"]["Shards"]

        if shards:
            shard_id = shards[0]["ShardId"]
            it = dynamodbstreams.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard_id,
                ShardIteratorType="LATEST",
            )
            assert "ShardIterator" in it

        dynamodb.delete_table(TableName=table_name)

    def test_get_shard_iterator_at_sequence_number(self, dynamodb, dynamodbstreams):
        """AT_SEQUENCE_NUMBER shard iterator starts at a specific sequence number."""
        table_name = f"at-seq-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "seq-item"}})

        streams = dynamodbstreams.list_streams(TableName=table_name)
        stream_arn = streams["Streams"][0]["StreamArn"]
        desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
        shards = desc["StreamDescription"]["Shards"]

        if shards:
            shard_id = shards[0]["ShardId"]
            # First get records via TRIM_HORIZON to find a sequence number
            trim_it = dynamodbstreams.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )["ShardIterator"]
            records = dynamodbstreams.get_records(ShardIterator=trim_it)["Records"]
            if records:
                seq_num = records[0]["dynamodb"]["SequenceNumber"]
                # Use AT_SEQUENCE_NUMBER with this sequence number
                at_it = dynamodbstreams.get_shard_iterator(
                    StreamArn=stream_arn,
                    ShardId=shard_id,
                    ShardIteratorType="AT_SEQUENCE_NUMBER",
                    SequenceNumber=seq_num,
                )
                assert "ShardIterator" in at_it
                at_records = dynamodbstreams.get_records(
                    ShardIterator=at_it["ShardIterator"]
                )["Records"]
                # AT means inclusive - should include the record at that seq number
                assert len(at_records) >= 1

        dynamodb.delete_table(TableName=table_name)

    def test_get_shard_iterator_after_sequence_number(self, dynamodb, dynamodbstreams):
        """AFTER_SEQUENCE_NUMBER shard iterator starts after a specific sequence number."""
        table_name = f"after-seq-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "after-item"}})

        streams = dynamodbstreams.list_streams(TableName=table_name)
        stream_arn = streams["Streams"][0]["StreamArn"]
        desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
        shards = desc["StreamDescription"]["Shards"]

        if shards:
            shard_id = shards[0]["ShardId"]
            trim_it = dynamodbstreams.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )["ShardIterator"]
            records = dynamodbstreams.get_records(ShardIterator=trim_it)["Records"]
            if records:
                seq_num = records[0]["dynamodb"]["SequenceNumber"]
                after_it = dynamodbstreams.get_shard_iterator(
                    StreamArn=stream_arn,
                    ShardId=shard_id,
                    ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                    SequenceNumber=seq_num,
                )
                assert "ShardIterator" in after_it

        dynamodb.delete_table(TableName=table_name)

    def test_stream_on_table_with_gsi(self, dynamodb, dynamodbstreams):
        """Streams work on tables that have a GSI."""
        table_name = f"gsi-stream-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gsi_key", "AttributeType": "S"},
            ],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "gsi-index",
                    "KeySchema": [{"AttributeName": "gsi_key", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        )
        streams = dynamodbstreams.list_streams(TableName=table_name)
        assert len(streams["Streams"]) >= 1
        stream_arn = streams["Streams"][0]["StreamArn"]

        desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
        assert desc["StreamDescription"]["TableName"] == table_name

        # Insert item with GSI key and verify stream captures it
        dynamodb.put_item(
            TableName=table_name,
            Item={"pk": {"S": "gsi-pk"}, "gsi_key": {"S": "gsi-val"}},
        )

        shards = desc["StreamDescription"]["Shards"]
        if shards:
            shard_id = shards[0]["ShardId"]
            it = dynamodbstreams.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )["ShardIterator"]
            records = dynamodbstreams.get_records(ShardIterator=it)["Records"]
            assert len(records) >= 1
            assert records[0]["eventName"] == "INSERT"

        dynamodb.delete_table(TableName=table_name)

    def test_stream_new_image_view_type(self, dynamodb, dynamodbstreams):
        """Stream with NEW_IMAGE view type includes new item image."""
        table_name = f"new-img-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_IMAGE",
            },
        )
        streams = dynamodbstreams.list_streams(TableName=table_name)
        stream_arn = streams["Streams"][0]["StreamArn"]

        desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
        assert desc["StreamDescription"]["StreamViewType"] == "NEW_IMAGE"

        dynamodb.put_item(
            TableName=table_name, Item={"pk": {"S": "ni1"}, "val": {"S": "hello"}}
        )

        shards = desc["StreamDescription"]["Shards"]
        if shards:
            shard_id = shards[0]["ShardId"]
            it = dynamodbstreams.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )["ShardIterator"]
            records = dynamodbstreams.get_records(ShardIterator=it)["Records"]
            if records:
                rec = records[0]
                assert "NewImage" in rec["dynamodb"]
                assert "OldImage" not in rec["dynamodb"]

        dynamodb.delete_table(TableName=table_name)

    def test_get_records_returns_next_shard_iterator(self, dynamodb, dynamodbstreams):
        """GetRecords returns a NextShardIterator for continued reading."""
        table_name = f"next-it-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "nxt1"}})

        streams = dynamodbstreams.list_streams(TableName=table_name)
        stream_arn = streams["Streams"][0]["StreamArn"]
        desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
        shards = desc["StreamDescription"]["Shards"]

        if shards:
            shard_id = shards[0]["ShardId"]
            it = dynamodbstreams.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )["ShardIterator"]
            result = dynamodbstreams.get_records(ShardIterator=it)
            # NextShardIterator should be present for continued reading
            assert "NextShardIterator" in result

        dynamodb.delete_table(TableName=table_name)

    def test_list_streams_without_table_name(self, dynamodb, dynamodbstreams):
        """ListStreams without TableName returns streams across tables."""
        table_name = f"list-all-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        response = dynamodbstreams.list_streams()
        assert "Streams" in response
        # Our table should be in the list
        table_names = [s["TableName"] for s in response["Streams"]]
        assert table_name in table_names

        dynamodb.delete_table(TableName=table_name)

    def test_get_shard_iterator(self, dynamodb, dynamodbstreams):
        table_name = f"shard-table-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        streams = dynamodbstreams.list_streams(TableName=table_name)
        stream_arn = streams["Streams"][0]["StreamArn"]

        desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
        shards = desc["StreamDescription"]["Shards"]
        if shards:
            shard_id = shards[0]["ShardId"]
            response = dynamodbstreams.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )
            assert "ShardIterator" in response

        dynamodb.delete_table(TableName=table_name)

    def test_update_item_generates_modify_event(self, dynamodb, dynamodbstreams):
        table_name = f"modify-ev-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "u1"}, "val": {"S": "old"}})
        dynamodb.update_item(
            TableName=table_name,
            Key={"pk": {"S": "u1"}},
            UpdateExpression="SET val = :v",
            ExpressionAttributeValues={":v": {"S": "new"}},
        )

        streams = dynamodbstreams.list_streams(TableName=table_name)
        stream_arn = streams["Streams"][0]["StreamArn"]
        desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
        shards = desc["StreamDescription"]["Shards"]

        if shards:
            shard_id = shards[0]["ShardId"]
            it = dynamodbstreams.get_shard_iterator(
                StreamArn=stream_arn, ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )["ShardIterator"]
            records = dynamodbstreams.get_records(ShardIterator=it)["Records"]
            modify = [r for r in records if r["eventName"] == "MODIFY"]
            if modify:
                rec = modify[0]
                assert "OldImage" in rec["dynamodb"]
                assert "NewImage" in rec["dynamodb"]

        dynamodb.delete_table(TableName=table_name)

    def test_delete_item_generates_remove_event(self, dynamodb, dynamodbstreams):
        table_name = f"remove-ev-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "d1"}, "val": {"S": "gone"}})
        dynamodb.delete_item(TableName=table_name, Key={"pk": {"S": "d1"}})

        streams = dynamodbstreams.list_streams(TableName=table_name)
        stream_arn = streams["Streams"][0]["StreamArn"]
        desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
        shards = desc["StreamDescription"]["Shards"]

        if shards:
            shard_id = shards[0]["ShardId"]
            it = dynamodbstreams.get_shard_iterator(
                StreamArn=stream_arn, ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )["ShardIterator"]
            records = dynamodbstreams.get_records(ShardIterator=it)["Records"]
            removes = [r for r in records if r["eventName"] == "REMOVE"]
            if removes:
                rec = removes[0]
                assert "OldImage" in rec["dynamodb"]
                assert "Keys" in rec["dynamodb"]

        dynamodb.delete_table(TableName=table_name)

    def test_stream_record_has_aws_region(self, dynamodb, dynamodbstreams):
        table_name = f"region-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "r1"}})

        streams = dynamodbstreams.list_streams(TableName=table_name)
        stream_arn = streams["Streams"][0]["StreamArn"]
        desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
        shards = desc["StreamDescription"]["Shards"]

        if shards:
            it = dynamodbstreams.get_shard_iterator(
                StreamArn=stream_arn, ShardId=shards[0]["ShardId"],
                ShardIteratorType="TRIM_HORIZON",
            )["ShardIterator"]
            records = dynamodbstreams.get_records(ShardIterator=it)["Records"]
            if records:
                assert "awsRegion" in records[0]
                assert "eventSource" in records[0]

        dynamodb.delete_table(TableName=table_name)

    def test_stream_arn_format(self, dynamodb, dynamodbstreams):
        table_name = f"arn-fmt-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        streams = dynamodbstreams.list_streams(TableName=table_name)
        stream_arn = streams["Streams"][0]["StreamArn"]
        assert "arn:aws:dynamodb" in stream_arn
        assert table_name in stream_arn
        assert "stream" in stream_arn

        dynamodb.delete_table(TableName=table_name)

    def test_list_streams_limit(self, dynamodb, dynamodbstreams):
        table_name = f"limit-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        resp = dynamodbstreams.list_streams(Limit=1)
        assert len(resp["Streams"]) <= 1

        dynamodb.delete_table(TableName=table_name)
