"""Tests for robotocore.services.kinesis.models."""

import hashlib

import pytest

from robotocore.services.kinesis.models import (
    MAX_HASH_KEY,
    KinesisStore,
    KinesisStream,
    Shard,
    _get_store,
)


class TestShard:
    def make_shard(self, shard_id: str = "shardId-000000000000") -> Shard:
        return Shard(shard_id=shard_id)

    def test_put_record(self):
        shard = self.make_shard()
        rec = shard.put_record("pk1", b"hello")
        assert rec.partition_key == "pk1"
        assert rec.data == b"hello"
        assert rec.sequence_number == "00000000000000000001"
        assert rec.shard_id == "shardId-000000000000"

    def test_put_record_increments_sequence(self):
        shard = self.make_shard()
        r1 = shard.put_record("pk", b"a")
        r2 = shard.put_record("pk", b"b")
        assert int(r2.sequence_number) == int(r1.sequence_number) + 1

    def test_get_records_from_start(self):
        shard = self.make_shard()
        shard.put_record("pk", b"a")
        shard.put_record("pk", b"b")
        shard.put_record("pk", b"c")
        records, next_seq = shard.get_records("00000000000000000001")
        assert len(records) == 3
        assert records[0].data == b"a"
        assert next_seq == "00000000000000000004"

    def test_get_records_from_middle(self):
        shard = self.make_shard()
        shard.put_record("pk", b"a")
        shard.put_record("pk", b"b")
        shard.put_record("pk", b"c")
        records, next_seq = shard.get_records("00000000000000000002")
        assert len(records) == 2
        assert records[0].data == b"b"

    def test_get_records_empty_shard(self):
        shard = self.make_shard()
        records, next_seq = shard.get_records("00000000000000000001")
        assert records == []
        assert next_seq is None

    def test_get_records_past_end(self):
        shard = self.make_shard()
        shard.put_record("pk", b"a")
        records, next_seq = shard.get_records("00000000000000000099")
        assert records == []
        assert next_seq is None

    def test_get_records_with_limit(self):
        shard = self.make_shard()
        for i in range(10):
            shard.put_record("pk", f"data{i}".encode())
        records, next_seq = shard.get_records("00000000000000000001", limit=3)
        assert len(records) == 3
        assert records[-1].data == b"data2"

    def test_get_records_after(self):
        shard = self.make_shard()
        shard.put_record("pk", b"a")
        shard.put_record("pk", b"b")
        shard.put_record("pk", b"c")
        # after sequence 1 -> start at 2
        records, next_seq = shard.get_records_after("00000000000000000001")
        assert len(records) == 2
        assert records[0].data == b"b"

    def test_get_latest_sequence(self):
        shard = self.make_shard()
        assert shard.get_latest_sequence() == "00000000000000000001"
        shard.put_record("pk", b"a")
        assert shard.get_latest_sequence() == "00000000000000000002"


class TestKinesisStream:
    def test_hash_partition_key(self):
        stream = KinesisStream(name="s", arn="arn:s")
        h = stream._hash_partition_key("test")
        expected = int(hashlib.md5(b"test").hexdigest(), 16)
        assert h == expected

    def test_select_shard_single(self):
        stream = KinesisStream(
            name="s",
            arn="arn:s",
            shards=[Shard(shard_id="shard-0", hash_key_start=0, hash_key_end=MAX_HASH_KEY)],
        )
        shard = stream._select_shard("any-key")
        assert shard.shard_id == "shard-0"

    def test_select_shard_multiple(self):
        mid = MAX_HASH_KEY // 2
        stream = KinesisStream(
            name="s",
            arn="arn:s",
            shards=[
                Shard(shard_id="shard-0", hash_key_start=0, hash_key_end=mid),
                Shard(shard_id="shard-1", hash_key_start=mid + 1, hash_key_end=MAX_HASH_KEY),
            ],
        )
        # All keys should land in one of the two shards
        shard = stream._select_shard("test-key")
        assert shard.shard_id in ("shard-0", "shard-1")

    def test_put_record(self):
        stream = KinesisStream(
            name="s",
            arn="arn:s",
            shards=[Shard(shard_id="shard-0")],
        )
        rec = stream.put_record("pk", b"data")
        assert rec.data == b"data"
        assert rec.partition_key == "pk"

    def test_put_record_with_explicit_hash_key(self):
        mid = MAX_HASH_KEY // 2
        stream = KinesisStream(
            name="s",
            arn="arn:s",
            shards=[
                Shard(shard_id="shard-0", hash_key_start=0, hash_key_end=mid),
                Shard(shard_id="shard-1", hash_key_start=mid + 1, hash_key_end=MAX_HASH_KEY),
            ],
        )
        # Explicit hash key in the second shard range
        rec = stream.put_record("pk", b"data", explicit_hash_key=str(mid + 1))
        assert rec.shard_id == "shard-1"

    def test_put_record_explicit_hash_key_first_shard(self):
        mid = MAX_HASH_KEY // 2
        stream = KinesisStream(
            name="s",
            arn="arn:s",
            shards=[
                Shard(shard_id="shard-0", hash_key_start=0, hash_key_end=mid),
                Shard(shard_id="shard-1", hash_key_start=mid + 1, hash_key_end=MAX_HASH_KEY),
            ],
        )
        rec = stream.put_record("pk", b"data", explicit_hash_key="0")
        assert rec.shard_id == "shard-0"


class TestKinesisStore:
    def make_store(self) -> KinesisStore:
        return KinesisStore()

    def test_create_stream(self):
        store = self.make_store()
        stream = store.create_stream("my-stream", 2, "us-east-1", "123")
        assert stream.name == "my-stream"
        assert stream.arn == "arn:aws:kinesis:us-east-1:123:stream/my-stream"
        assert len(stream.shards) == 2
        assert stream.shard_count == 2

    def test_create_stream_shard_hash_ranges(self):
        store = self.make_store()
        stream = store.create_stream("s", 3, "us-east-1", "123")
        # Shards should cover entire hash key space without gaps
        all_starts = sorted(s.hash_key_start for s in stream.shards)
        all_ends = sorted(s.hash_key_end for s in stream.shards)
        assert all_starts[0] == 0
        assert all_ends[-1] == MAX_HASH_KEY
        # Adjacent ranges: end of shard i + 1 == start of shard i+1
        for i in range(len(stream.shards) - 1):
            sorted_shards = sorted(stream.shards, key=lambda s: s.hash_key_start)
            assert sorted_shards[i].hash_key_end + 1 == sorted_shards[i + 1].hash_key_start

    def test_create_stream_duplicate_raises(self):
        store = self.make_store()
        store.create_stream("s", 1, "us-east-1", "123")
        with pytest.raises(ValueError, match="already exists"):
            store.create_stream("s", 1, "us-east-1", "123")

    def test_get_stream(self):
        store = self.make_store()
        store.create_stream("s", 1, "us-east-1", "123")
        assert store.get_stream("s") is not None
        assert store.get_stream("nope") is None

    def test_delete_stream(self):
        store = self.make_store()
        store.create_stream("s", 1, "us-east-1", "123")
        assert store.delete_stream("s") is True
        assert store.get_stream("s") is None

    def test_delete_stream_nonexistent(self):
        store = self.make_store()
        assert store.delete_stream("nope") is False

    def test_list_streams(self):
        store = self.make_store()
        store.create_stream("b", 1, "us-east-1", "123")
        store.create_stream("a", 1, "us-east-1", "123")
        assert store.list_streams() == ["a", "b"]

    def test_list_streams_empty(self):
        store = self.make_store()
        assert store.list_streams() == []

    def test_shard_ids_are_formatted(self):
        store = self.make_store()
        stream = store.create_stream("s", 3, "us-east-1", "123")
        ids = [s.shard_id for s in stream.shards]
        assert ids == ["shardId-000000000000", "shardId-000000000001", "shardId-000000000002"]


class TestGetStore:
    def test_returns_same_store_for_same_region(self):
        # Clear module-level state for isolation
        import robotocore.services.kinesis.models as mod

        mod._stores.clear()
        s1 = _get_store("us-east-1")
        s2 = _get_store("us-east-1")
        assert s1 is s2

    def test_returns_different_store_for_different_region(self):
        import robotocore.services.kinesis.models as mod

        mod._stores.clear()
        s1 = _get_store("us-east-1")
        s2 = _get_store("eu-west-1")
        assert s1 is not s2
