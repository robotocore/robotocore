"""Tests for robotocore.services.dynamodbstreams.models."""

from robotocore.services.dynamodbstreams.models import (
    DynamoDBStream,
    DynamoDBStreamsStore,
    StreamRecord,
    StreamShard,
)


class TestStreamRecord:
    def test_fields(self):
        r = StreamRecord(
            event_id="001",
            event_name="INSERT",
            dynamodb={"Keys": {"pk": {"S": "1"}}},
            event_source_arn="arn:aws:dynamodb:us-east-1:123:table/t/stream/label",
            aws_region="us-east-1",
        )
        assert r.event_id == "001"
        assert r.event_name == "INSERT"
        assert r.event_source == "aws:dynamodb"
        assert r.event_version == "1.1"
        assert r.dynamodb["Keys"]["pk"]["S"] == "1"


class TestStreamShard:
    def test_defaults(self):
        s = StreamShard(shard_id="shard-0")
        assert s.records == []
        assert s.parent_shard_id is None
        assert s.sequence_counter == 0


class TestDynamoDBStream:
    def test_defaults(self):
        s = DynamoDBStream(
            stream_arn="arn:stream",
            table_name="t",
            stream_label="2024-01-01T00:00:00.000",
        )
        assert s.status == "ENABLED"
        assert s.shards == []
        assert s.view_type == "NEW_AND_OLD_IMAGES"
        assert s.key_schema == []
        assert s.creation_time > 0


class TestDynamoDBStreamsStore:
    def make_store(self) -> DynamoDBStreamsStore:
        return DynamoDBStreamsStore()

    def test_record_change_insert(self):
        store = self.make_store()
        stream_arn = "arn:aws:dynamodb:us-east-1:123:table/t/stream/lbl"
        store.record_change(
            table_name="t",
            event_name="INSERT",
            keys={"pk": {"S": "1"}},
            new_image={"pk": {"S": "1"}, "data": {"S": "hello"}},
            old_image=None,
            region="us-east-1",
            account_id="123",
            stream_arn=stream_arn,
        )
        records = store._hook_records[stream_arn]
        assert len(records) == 1
        rec = records[0]
        assert rec.event_name == "INSERT"
        assert rec.event_source_arn == stream_arn
        assert rec.aws_region == "us-east-1"
        assert rec.dynamodb["Keys"] == {"pk": {"S": "1"}}
        assert rec.dynamodb["NewImage"]["data"]["S"] == "hello"
        assert "OldImage" not in rec.dynamodb  # INSERT -> no OldImage
        assert rec.dynamodb["SequenceNumber"] == "000000000000000000001"
        assert rec.dynamodb["StreamViewType"] == "NEW_AND_OLD_IMAGES"

    def test_record_change_modify(self):
        store = self.make_store()
        stream_arn = "arn:stream"
        store.record_change(
            table_name="t",
            event_name="MODIFY",
            keys={"pk": {"S": "1"}},
            new_image={"pk": {"S": "1"}, "val": {"N": "2"}},
            old_image={"pk": {"S": "1"}, "val": {"N": "1"}},
            region="us-east-1",
            account_id="123",
            stream_arn=stream_arn,
        )
        rec = store._hook_records[stream_arn][0]
        assert rec.event_name == "MODIFY"
        assert rec.dynamodb["NewImage"]["val"]["N"] == "2"
        assert rec.dynamodb["OldImage"]["val"]["N"] == "1"

    def test_record_change_remove(self):
        store = self.make_store()
        stream_arn = "arn:stream"
        store.record_change(
            table_name="t",
            event_name="REMOVE",
            keys={"pk": {"S": "1"}},
            new_image=None,
            old_image={"pk": {"S": "1"}, "val": {"S": "x"}},
            region="us-east-1",
            account_id="123",
            stream_arn=stream_arn,
        )
        rec = store._hook_records[stream_arn][0]
        assert rec.event_name == "REMOVE"
        assert "NewImage" not in rec.dynamodb
        assert rec.dynamodb["OldImage"]["val"]["S"] == "x"

    def test_record_change_new_image_view_type(self):
        store = self.make_store()
        stream_arn = "arn:stream"
        store.record_change(
            table_name="t",
            event_name="MODIFY",
            keys={"pk": {"S": "1"}},
            new_image={"pk": {"S": "1"}},
            old_image={"pk": {"S": "1"}},
            region="us-east-1",
            account_id="123",
            stream_arn=stream_arn,
            view_type="NEW_IMAGE",
        )
        rec = store._hook_records[stream_arn][0]
        assert "NewImage" in rec.dynamodb
        assert "OldImage" not in rec.dynamodb  # NEW_IMAGE excludes OldImage

    def test_record_change_old_image_view_type(self):
        store = self.make_store()
        stream_arn = "arn:stream"
        store.record_change(
            table_name="t",
            event_name="MODIFY",
            keys={"pk": {"S": "1"}},
            new_image={"pk": {"S": "1"}},
            old_image={"pk": {"S": "1"}},
            region="us-east-1",
            account_id="123",
            stream_arn=stream_arn,
            view_type="OLD_IMAGE",
        )
        rec = store._hook_records[stream_arn][0]
        assert "NewImage" not in rec.dynamodb
        assert "OldImage" in rec.dynamodb

    def test_record_change_keys_only_view_type(self):
        store = self.make_store()
        stream_arn = "arn:stream"
        store.record_change(
            table_name="t",
            event_name="MODIFY",
            keys={"pk": {"S": "1"}},
            new_image={"pk": {"S": "1"}},
            old_image={"pk": {"S": "1"}},
            region="us-east-1",
            account_id="123",
            stream_arn=stream_arn,
            view_type="KEYS_ONLY",
        )
        rec = store._hook_records[stream_arn][0]
        assert "NewImage" not in rec.dynamodb
        assert "OldImage" not in rec.dynamodb
        assert rec.dynamodb["Keys"] == {"pk": {"S": "1"}}

    def test_sequence_numbers_increment(self):
        store = self.make_store()
        stream_arn = "arn:stream"
        for i in range(5):
            store.record_change(
                table_name="t",
                event_name="INSERT",
                keys={"pk": {"S": str(i)}},
                new_image={"pk": {"S": str(i)}},
                old_image=None,
                region="us-east-1",
                account_id="123",
                stream_arn=stream_arn,
            )
        records = store._hook_records[stream_arn]
        seq_nums = [int(r.dynamodb["SequenceNumber"]) for r in records]
        assert seq_nums == [1, 2, 3, 4, 5]

    def test_multiple_streams_isolated(self):
        store = self.make_store()
        store.record_change(
            table_name="t1",
            event_name="INSERT",
            keys={"pk": {"S": "1"}},
            new_image=None,
            old_image=None,
            region="us-east-1",
            account_id="123",
            stream_arn="arn:stream1",
        )
        store.record_change(
            table_name="t2",
            event_name="INSERT",
            keys={"pk": {"S": "2"}},
            new_image=None,
            old_image=None,
            region="us-east-1",
            account_id="123",
            stream_arn="arn:stream2",
        )
        assert len(store._hook_records["arn:stream1"]) == 1
        assert len(store._hook_records["arn:stream2"]) == 1

    def test_empty_store(self):
        store = self.make_store()
        assert store._hook_records == {}
