"""DynamoDB Streams compatibility tests."""

import time
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


def _create_stream_table(dynamodb, view_type="NEW_AND_OLD_IMAGES", suffix=None):
    """Helper to create a DynamoDB table with streams enabled."""
    table_name = f"stream-{suffix or _uid()}"
    dynamodb.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={
            "StreamEnabled": True,
            "StreamViewType": view_type,
        },
    )
    return table_name


def _get_stream_arn(dynamodbstreams, table_name):
    """Get the stream ARN for a table."""
    streams = dynamodbstreams.list_streams(TableName=table_name)
    return streams["Streams"][0]["StreamArn"]


def _get_shard_iterator(dynamodbstreams, stream_arn, iterator_type="TRIM_HORIZON", **kwargs):
    """Get a shard iterator for the first shard of a stream."""
    desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
    shards = desc["StreamDescription"]["Shards"]
    assert len(shards) >= 1, "Expected at least one shard"
    shard_id = shards[0]["ShardId"]
    return dynamodbstreams.get_shard_iterator(
        StreamArn=stream_arn,
        ShardId=shard_id,
        ShardIteratorType=iterator_type,
        **kwargs,
    )["ShardIterator"]


def _poll_records(dynamodbstreams, shard_iterator, max_attempts=10):
    """Poll for stream records, retrying until records appear or max attempts reached."""
    records = []
    current_iterator = shard_iterator
    for _ in range(max_attempts):
        response = dynamodbstreams.get_records(ShardIterator=current_iterator)
        records.extend(response.get("Records", []))
        if records:
            break
        current_iterator = response.get("NextShardIterator", current_iterator)
        time.sleep(0.3)
    return records


class TestDynamoDBStreamsOperations:
    def test_list_streams(self, dynamodb, dynamodbstreams):
        table_name = _create_stream_table(dynamodb)
        try:
            response = dynamodbstreams.list_streams(TableName=table_name)
            assert "Streams" in response
            assert len(response["Streams"]) >= 1
            assert response["Streams"][0]["TableName"] == table_name
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_list_streams_without_table_filter(self, dynamodb, dynamodbstreams):
        """ListStreams without TableName returns streams across all tables."""
        table_name = _create_stream_table(dynamodb)
        try:
            response = dynamodbstreams.list_streams()
            assert "Streams" in response
            # Should contain at least our table's stream
            table_names = [s["TableName"] for s in response["Streams"]]
            assert table_name in table_names
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_describe_stream(self, dynamodb, dynamodbstreams):
        table_name = _create_stream_table(dynamodb)
        try:
            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            response = dynamodbstreams.describe_stream(StreamArn=stream_arn)
            desc = response["StreamDescription"]
            assert desc["TableName"] == table_name
            assert desc["StreamViewType"] == "NEW_AND_OLD_IMAGES"
            assert "StreamArn" in desc
            assert "Shards" in desc
            assert desc["StreamStatus"] in ("ENABLED", "ENABLING")
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_describe_stream_has_key_schema(self, dynamodb, dynamodbstreams):
        """DescribeStream should include the table's key schema."""
        table_name = _create_stream_table(dynamodb)
        try:
            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            response = dynamodbstreams.describe_stream(StreamArn=stream_arn)
            desc = response["StreamDescription"]
            assert "KeySchema" in desc
            key_schema = desc["KeySchema"]
            assert len(key_schema) >= 1
            assert key_schema[0]["AttributeName"] == "pk"
            assert key_schema[0]["KeyType"] == "HASH"
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_get_shard_iterator_trim_horizon(self, dynamodb, dynamodbstreams):
        """TRIM_HORIZON returns an iterator starting at the oldest record."""
        table_name = _create_stream_table(dynamodb)
        try:
            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
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
                assert isinstance(response["ShardIterator"], str)
                assert len(response["ShardIterator"]) > 0
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_get_shard_iterator_latest(self, dynamodb, dynamodbstreams):
        """LATEST returns an iterator pointing after the most recent record."""
        table_name = _create_stream_table(dynamodb)
        try:
            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
            shards = desc["StreamDescription"]["Shards"]
            if shards:
                shard_id = shards[0]["ShardId"]
                response = dynamodbstreams.get_shard_iterator(
                    StreamArn=stream_arn,
                    ShardId=shard_id,
                    ShardIteratorType="LATEST",
                )
                assert "ShardIterator" in response
                assert isinstance(response["ShardIterator"], str)
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_get_shard_iterator_at_sequence_number(self, dynamodb, dynamodbstreams):
        """AT_SEQUENCE_NUMBER requires a SequenceNumber and returns records from that point."""
        table_name = _create_stream_table(dynamodb)
        try:
            # Insert an item to generate a stream record
            dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "seq-test"}})
            stream_arn = _get_stream_arn(dynamodbstreams, table_name)

            # First get records via TRIM_HORIZON to find a sequence number
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)
            assert len(records) >= 1, "Expected at least one record"

            seq_number = records[0]["dynamodb"]["SequenceNumber"]

            # Now use AT_SEQUENCE_NUMBER
            desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
            shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]
            response = dynamodbstreams.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard_id,
                ShardIteratorType="AT_SEQUENCE_NUMBER",
                SequenceNumber=seq_number,
            )
            assert "ShardIterator" in response

            # Reading from this iterator should include the record at that sequence number
            at_records = _poll_records(dynamodbstreams, response["ShardIterator"])
            assert len(at_records) >= 1
            assert at_records[0]["dynamodb"]["SequenceNumber"] == seq_number
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_get_shard_iterator_after_sequence_number(self, dynamodb, dynamodbstreams):
        """AFTER_SEQUENCE_NUMBER returns records after the given sequence number."""
        table_name = _create_stream_table(dynamodb)
        try:
            # Insert two items to generate stream records
            dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "after-1"}})
            dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "after-2"}})
            stream_arn = _get_stream_arn(dynamodbstreams, table_name)

            # Get all records via TRIM_HORIZON
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)
            assert len(records) >= 2, "Expected at least two records"

            first_seq = records[0]["dynamodb"]["SequenceNumber"]

            # Use AFTER_SEQUENCE_NUMBER with the first record's sequence number
            desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
            shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]
            response = dynamodbstreams.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard_id,
                ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                SequenceNumber=first_seq,
            )
            assert "ShardIterator" in response

            # Records returned should NOT include the first record
            after_records = _poll_records(dynamodbstreams, response["ShardIterator"])
            assert len(after_records) >= 1
            after_seq_numbers = [r["dynamodb"]["SequenceNumber"] for r in after_records]
            assert first_seq not in after_seq_numbers
        finally:
            dynamodb.delete_table(TableName=table_name)


class TestStreamRecordTypes:
    """Test that stream records correctly capture INSERT, MODIFY, and REMOVE events."""

    @pytest.mark.xfail(reason="Not yet implemented")
    def test_insert_record(self, dynamodb, dynamodbstreams):
        """PutItem on a new key produces an INSERT stream record."""
        table_name = _create_stream_table(dynamodb)
        try:
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "insert-item"}, "data": {"S": "hello"}},
            )
            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)

            assert len(records) >= 1
            insert_record = records[0]
            assert insert_record["eventName"] == "INSERT"
            assert "dynamodb" in insert_record
            assert "NewImage" in insert_record["dynamodb"]
            assert insert_record["dynamodb"]["NewImage"]["pk"] == {"S": "insert-item"}
            # INSERT should not have OldImage
            assert "OldImage" not in insert_record["dynamodb"] or insert_record["dynamodb"].get(
                "OldImage"
            ) is None
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_modify_record(self, dynamodb, dynamodbstreams):
        """Updating an existing item produces a MODIFY stream record."""
        table_name = _create_stream_table(dynamodb)
        try:
            # Insert first
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "modify-item"}, "data": {"S": "original"}},
            )
            # Modify the item
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "modify-item"}, "data": {"S": "updated"}},
            )

            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)

            assert len(records) >= 2
            modify_record = records[1]
            assert modify_record["eventName"] == "MODIFY"
            assert "NewImage" in modify_record["dynamodb"]
            assert modify_record["dynamodb"]["NewImage"]["data"] == {"S": "updated"}
            assert "OldImage" in modify_record["dynamodb"]
            assert modify_record["dynamodb"]["OldImage"]["data"] == {"S": "original"}
        finally:
            dynamodb.delete_table(TableName=table_name)

    @pytest.mark.xfail(reason="Not yet implemented")
    def test_remove_record(self, dynamodb, dynamodbstreams):
        """Deleting an item produces a REMOVE stream record."""
        table_name = _create_stream_table(dynamodb)
        try:
            # Insert then delete
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "remove-item"}, "data": {"S": "bye"}},
            )
            dynamodb.delete_item(
                TableName=table_name,
                Key={"pk": {"S": "remove-item"}},
            )

            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)

            assert len(records) >= 2
            remove_record = records[1]
            assert remove_record["eventName"] == "REMOVE"
            assert "OldImage" in remove_record["dynamodb"]
            assert remove_record["dynamodb"]["OldImage"]["pk"] == {"S": "remove-item"}
            # REMOVE should not have NewImage
            assert "NewImage" not in remove_record["dynamodb"] or remove_record["dynamodb"].get(
                "NewImage"
            ) is None
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_all_event_types_in_order(self, dynamodb, dynamodbstreams):
        """INSERT, MODIFY, REMOVE appear in sequence when performing those operations."""
        table_name = _create_stream_table(dynamodb)
        try:
            # INSERT
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "lifecycle"}, "val": {"N": "1"}},
            )
            # MODIFY
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "lifecycle"}, "val": {"N": "2"}},
            )
            # REMOVE
            dynamodb.delete_item(
                TableName=table_name,
                Key={"pk": {"S": "lifecycle"}},
            )

            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)

            assert len(records) >= 3
            event_names = [r["eventName"] for r in records[:3]]
            assert event_names == ["INSERT", "MODIFY", "REMOVE"]
        finally:
            dynamodb.delete_table(TableName=table_name)


class TestGetRecords:
    """Test GetRecords behavior including limits and pagination."""

    def test_get_records_basic(self, dynamodb, dynamodbstreams):
        """GetRecords returns records and a NextShardIterator."""
        table_name = _create_stream_table(dynamodb)
        try:
            dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "basic-rec"}})
            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")

            response = dynamodbstreams.get_records(ShardIterator=iterator)
            assert "Records" in response
            assert "NextShardIterator" in response
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_get_records_with_limit(self, dynamodb, dynamodbstreams):
        """GetRecords with Limit returns at most that many records."""
        table_name = _create_stream_table(dynamodb)
        try:
            # Insert several items
            for i in range(5):
                dynamodb.put_item(
                    TableName=table_name, Item={"pk": {"S": f"limit-{i}"}}
                )

            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")

            # Wait for records to be available
            records = _poll_records(dynamodbstreams, iterator)
            assert len(records) >= 5, "Expected at least 5 records"

            # Now re-read with a limit
            iterator2 = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            response = dynamodbstreams.get_records(ShardIterator=iterator2, Limit=2)
            assert len(response["Records"]) <= 2
            assert "NextShardIterator" in response
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_get_records_pagination(self, dynamodb, dynamodbstreams):
        """Using NextShardIterator allows reading all records across pages."""
        table_name = _create_stream_table(dynamodb)
        try:
            for i in range(4):
                dynamodb.put_item(
                    TableName=table_name, Item={"pk": {"S": f"page-{i}"}}
                )

            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")

            all_records = []
            current_iterator = iterator
            for _ in range(20):  # safety limit
                response = dynamodbstreams.get_records(ShardIterator=current_iterator, Limit=2)
                all_records.extend(response.get("Records", []))
                if len(all_records) >= 4:
                    break
                next_iter = response.get("NextShardIterator")
                if not next_iter:
                    break
                current_iterator = next_iter
                time.sleep(0.3)

            assert len(all_records) >= 4
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_get_records_empty_stream(self, dynamodb, dynamodbstreams):
        """GetRecords on a stream with no mutations returns empty Records list."""
        table_name = _create_stream_table(dynamodb)
        try:
            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
            shards = desc["StreamDescription"]["Shards"]
            if shards:
                shard_id = shards[0]["ShardId"]
                iterator = dynamodbstreams.get_shard_iterator(
                    StreamArn=stream_arn,
                    ShardId=shard_id,
                    ShardIteratorType="TRIM_HORIZON",
                )["ShardIterator"]
                response = dynamodbstreams.get_records(ShardIterator=iterator)
                assert response["Records"] == []
        finally:
            dynamodb.delete_table(TableName=table_name)


class TestMultipleItemStreaming:
    """Test that multiple item mutations produce corresponding stream records."""

    def test_multiple_inserts(self, dynamodb, dynamodbstreams):
        """Multiple PutItem calls produce multiple INSERT records."""
        table_name = _create_stream_table(dynamodb)
        try:
            items = [f"multi-{i}" for i in range(5)]
            for item_pk in items:
                dynamodb.put_item(TableName=table_name, Item={"pk": {"S": item_pk}})

            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)

            assert len(records) >= 5
            insert_records = [r for r in records if r["eventName"] == "INSERT"]
            assert len(insert_records) >= 5

            recorded_keys = {r["dynamodb"]["Keys"]["pk"]["S"] for r in insert_records}
            for item_pk in items:
                assert item_pk in recorded_keys
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_batch_write_produces_records(self, dynamodb, dynamodbstreams):
        """BatchWriteItem produces individual stream records for each item."""
        table_name = _create_stream_table(dynamodb)
        try:
            dynamodb.batch_write_item(
                RequestItems={
                    table_name: [
                        {"PutRequest": {"Item": {"pk": {"S": f"batch-{i}"}}}},
                    ]
                    for i in range(3)
                }
            )
            # batch_write_item RequestItems needs a list, fix the comprehension
            dynamodb.batch_write_item(
                RequestItems={
                    table_name: [
                        {"PutRequest": {"Item": {"pk": {"S": f"batch2-{i}"}}}
                        } for i in range(3)
                    ]
                }
            )

            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)

            # Should have at least 3 records from the second batch_write
            batch_keys = {r["dynamodb"]["Keys"]["pk"]["S"] for r in records}
            for i in range(3):
                assert f"batch2-{i}" in batch_keys
        finally:
            dynamodb.delete_table(TableName=table_name)

    @pytest.mark.xfail(reason="Not yet implemented")
    def test_update_item_produces_modify_record(self, dynamodb, dynamodbstreams):
        """UpdateItem on existing item produces a MODIFY record."""
        table_name = _create_stream_table(dynamodb)
        try:
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "upd-item"}, "counter": {"N": "0"}},
            )
            dynamodb.update_item(
                TableName=table_name,
                Key={"pk": {"S": "upd-item"}},
                UpdateExpression="SET counter = counter + :inc",
                ExpressionAttributeValues={":inc": {"N": "1"}},
            )

            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)

            assert len(records) >= 2
            modify_records = [r for r in records if r["eventName"] == "MODIFY"]
            assert len(modify_records) >= 1
            mod = modify_records[0]
            assert mod["dynamodb"]["NewImage"]["counter"] == {"N": "1"}
            assert mod["dynamodb"]["OldImage"]["counter"] == {"N": "0"}
        finally:
            dynamodb.delete_table(TableName=table_name)


class TestStreamViewTypes:
    """Test different StreamViewType configurations."""

    def test_new_and_old_images(self, dynamodb, dynamodbstreams):
        """NEW_AND_OLD_IMAGES includes both NewImage and OldImage on MODIFY."""
        table_name = _create_stream_table(dynamodb, view_type="NEW_AND_OLD_IMAGES")
        try:
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "view-noi"}, "attr": {"S": "before"}},
            )
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "view-noi"}, "attr": {"S": "after"}},
            )

            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)

            modify_records = [r for r in records if r["eventName"] == "MODIFY"]
            assert len(modify_records) >= 1
            mod = modify_records[0]
            assert "NewImage" in mod["dynamodb"]
            assert "OldImage" in mod["dynamodb"]
            assert mod["dynamodb"]["NewImage"]["attr"] == {"S": "after"}
            assert mod["dynamodb"]["OldImage"]["attr"] == {"S": "before"}
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_keys_only(self, dynamodb, dynamodbstreams):
        """KEYS_ONLY only includes the key attributes in stream records."""
        table_name = _create_stream_table(dynamodb, view_type="KEYS_ONLY")
        try:
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "keys-only"}, "extra": {"S": "hidden"}},
            )

            stream_arn = _get_stream_arn(dynamodbstreams, table_name)

            # Verify stream view type
            desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
            assert desc["StreamDescription"]["StreamViewType"] == "KEYS_ONLY"

            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)

            assert len(records) >= 1
            rec = records[0]
            assert rec["eventName"] == "INSERT"
            ddb = rec["dynamodb"]
            assert "Keys" in ddb
            assert ddb["Keys"]["pk"] == {"S": "keys-only"}
            # KEYS_ONLY should NOT include NewImage or OldImage
            assert "NewImage" not in ddb
            assert "OldImage" not in ddb
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_new_image_only(self, dynamodb, dynamodbstreams):
        """NEW_IMAGE includes only NewImage (no OldImage) on MODIFY."""
        table_name = _create_stream_table(dynamodb, view_type="NEW_IMAGE")
        try:
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "ni-item"}, "val": {"S": "v1"}},
            )
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "ni-item"}, "val": {"S": "v2"}},
            )

            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)

            modify_records = [r for r in records if r["eventName"] == "MODIFY"]
            assert len(modify_records) >= 1
            mod = modify_records[0]
            assert "NewImage" in mod["dynamodb"]
            assert mod["dynamodb"]["NewImage"]["val"] == {"S": "v2"}
            assert "OldImage" not in mod["dynamodb"]
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_old_image_only(self, dynamodb, dynamodbstreams):
        """OLD_IMAGE includes only OldImage (no NewImage) on MODIFY."""
        table_name = _create_stream_table(dynamodb, view_type="OLD_IMAGE")
        try:
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "oi-item"}, "val": {"S": "original"}},
            )
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "oi-item"}, "val": {"S": "changed"}},
            )

            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)

            modify_records = [r for r in records if r["eventName"] == "MODIFY"]
            assert len(modify_records) >= 1
            mod = modify_records[0]
            assert "OldImage" in mod["dynamodb"]
            assert mod["dynamodb"]["OldImage"]["val"] == {"S": "original"}
            assert "NewImage" not in mod["dynamodb"]
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_keys_only_on_modify(self, dynamodb, dynamodbstreams):
        """KEYS_ONLY on MODIFY still only returns keys, no images."""
        table_name = _create_stream_table(dynamodb, view_type="KEYS_ONLY")
        try:
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "ko-mod"}, "data": {"S": "a"}},
            )
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "ko-mod"}, "data": {"S": "b"}},
            )

            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)

            modify_records = [r for r in records if r["eventName"] == "MODIFY"]
            assert len(modify_records) >= 1
            mod = modify_records[0]
            assert "Keys" in mod["dynamodb"]
            assert "NewImage" not in mod["dynamodb"]
            assert "OldImage" not in mod["dynamodb"]
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_keys_only_on_remove(self, dynamodb, dynamodbstreams):
        """KEYS_ONLY on REMOVE returns keys but no OldImage."""
        table_name = _create_stream_table(dynamodb, view_type="KEYS_ONLY")
        try:
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "ko-del"}, "data": {"S": "gone"}},
            )
            dynamodb.delete_item(
                TableName=table_name,
                Key={"pk": {"S": "ko-del"}},
            )

            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)

            remove_records = [r for r in records if r["eventName"] == "REMOVE"]
            assert len(remove_records) >= 1
            rem = remove_records[0]
            assert "Keys" in rem["dynamodb"]
            assert rem["dynamodb"]["Keys"]["pk"] == {"S": "ko-del"}
            assert "NewImage" not in rem["dynamodb"]
            assert "OldImage" not in rem["dynamodb"]
        finally:
            dynamodb.delete_table(TableName=table_name)


class TestStreamRecordMetadata:
    """Test metadata fields on stream records."""

    def test_record_has_event_source(self, dynamodb, dynamodbstreams):
        """Stream records should have eventSource set to aws:dynamodb."""
        table_name = _create_stream_table(dynamodb)
        try:
            dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "meta-src"}})
            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)

            assert len(records) >= 1
            rec = records[0]
            assert rec.get("eventSource") == "aws:dynamodb"
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_record_has_event_version(self, dynamodb, dynamodbstreams):
        """Stream records should have an eventVersion field."""
        table_name = _create_stream_table(dynamodb)
        try:
            dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "meta-ver"}})
            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)

            assert len(records) >= 1
            rec = records[0]
            assert "eventVersion" in rec
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_record_has_aws_region(self, dynamodb, dynamodbstreams):
        """Stream records should include an awsRegion field."""
        table_name = _create_stream_table(dynamodb)
        try:
            dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "meta-reg"}})
            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)

            assert len(records) >= 1
            rec = records[0]
            assert rec.get("awsRegion") == "us-east-1"
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_record_has_sequence_number(self, dynamodb, dynamodbstreams):
        """Each stream record should have a unique SequenceNumber."""
        table_name = _create_stream_table(dynamodb)
        try:
            dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "seq-1"}})
            dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "seq-2"}})

            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)

            assert len(records) >= 2
            seq_numbers = [r["dynamodb"]["SequenceNumber"] for r in records]
            # All sequence numbers should be unique
            assert len(set(seq_numbers)) == len(seq_numbers)
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_record_has_size_bytes(self, dynamodb, dynamodbstreams):
        """Stream records should include SizeBytes in the dynamodb section."""
        table_name = _create_stream_table(dynamodb)
        try:
            dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "size-test"}})
            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)

            assert len(records) >= 1
            assert "SizeBytes" in records[0]["dynamodb"]
            assert isinstance(records[0]["dynamodb"]["SizeBytes"], int)
            assert records[0]["dynamodb"]["SizeBytes"] > 0
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_record_has_stream_view_type(self, dynamodb, dynamodbstreams):
        """Stream records should include StreamViewType in the dynamodb section."""
        table_name = _create_stream_table(dynamodb, view_type="NEW_AND_OLD_IMAGES")
        try:
            dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "svt-test"}})
            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "TRIM_HORIZON")
            records = _poll_records(dynamodbstreams, iterator)

            assert len(records) >= 1
            assert records[0]["dynamodb"].get("StreamViewType") == "NEW_AND_OLD_IMAGES"
        finally:
            dynamodb.delete_table(TableName=table_name)


class TestLatestIterator:
    """Test LATEST iterator behavior specifically."""

    def test_latest_does_not_return_old_records(self, dynamodb, dynamodbstreams):
        """LATEST iterator should not return records written before the iterator was obtained."""
        table_name = _create_stream_table(dynamodb)
        try:
            # Write before getting LATEST iterator
            dynamodb.put_item(TableName=table_name, Item={"pk": {"S": "old-item"}})
            time.sleep(0.5)

            stream_arn = _get_stream_arn(dynamodbstreams, table_name)
            iterator = _get_shard_iterator(dynamodbstreams, stream_arn, "LATEST")

            # Read immediately -- should get no records (or only new ones)
            response = dynamodbstreams.get_records(ShardIterator=iterator)
            old_keys = {
                r["dynamodb"]["Keys"]["pk"]["S"]
                for r in response.get("Records", [])
            }
            # The old-item should NOT appear when using LATEST
            assert "old-item" not in old_keys
        finally:
            dynamodb.delete_table(TableName=table_name)
