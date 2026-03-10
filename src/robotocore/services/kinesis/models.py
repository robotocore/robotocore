"""In-memory models for Kinesis Streams."""

import hashlib
import threading
import time
from dataclasses import dataclass, field

# Maximum hash key value for shard range partitioning (2^128 - 1)
MAX_HASH_KEY = (2**128) - 1


@dataclass
class KinesisRecord:
    partition_key: str
    data: bytes
    sequence_number: str
    timestamp: float
    shard_id: str


@dataclass
class Shard:
    shard_id: str
    records: list[KinesisRecord] = field(default_factory=list)
    sequence_counter: int = 0
    hash_key_start: int = 0
    hash_key_end: int = MAX_HASH_KEY
    starting_sequence_number: str = "00000000000000000000"
    lock: threading.Lock = field(default_factory=threading.Lock)

    def put_record(self, partition_key: str, data: bytes) -> KinesisRecord:
        with self.lock:
            self.sequence_counter += 1
            seq = f"{self.sequence_counter:020d}"
            record = KinesisRecord(
                partition_key=partition_key,
                data=data,
                sequence_number=seq,
                timestamp=time.time(),
                shard_id=self.shard_id,
            )
            self.records.append(record)
            return record

    def get_records(
        self, start_sequence: str, limit: int = 10000
    ) -> tuple[list[KinesisRecord], str | None]:
        """Return records starting at or after start_sequence.

        Returns (records, next_sequence) where next_sequence is the sequence
        number to use for the next call, or None if no more records.
        """
        with self.lock:
            start_idx = 0
            for i, rec in enumerate(self.records):
                if rec.sequence_number >= start_sequence:
                    start_idx = i
                    break
            else:
                # No records at or after start_sequence
                return [], None

            batch = self.records[start_idx : start_idx + limit]
            if not batch:
                return [], None

            # Next position is after the last returned record
            last_seq = int(batch[-1].sequence_number)
            next_seq = f"{last_seq + 1:020d}"
            return batch, next_seq

    def get_records_after(
        self, after_sequence: str, limit: int = 10000
    ) -> tuple[list[KinesisRecord], str | None]:
        """Return records strictly after after_sequence."""
        next_seq = f"{int(after_sequence) + 1:020d}"
        return self.get_records(next_seq, limit)

    def get_latest_sequence(self) -> str:
        """Return the sequence number that would be assigned to the next record."""
        with self.lock:
            return f"{self.sequence_counter + 1:020d}"


@dataclass
class KinesisStream:
    name: str
    arn: str
    status: str = "ACTIVE"
    shards: list[Shard] = field(default_factory=list)
    shard_count: int = 1
    retention_hours: int = 24
    tags: dict[str, str] = field(default_factory=dict)
    created: float = field(default_factory=time.time)
    encryption_type: str = "NONE"
    key_id: str = ""
    consumers: dict[str, dict] = field(default_factory=dict)
    shard_level_metrics: list[str] = field(default_factory=list)

    def _hash_partition_key(self, partition_key: str) -> int:
        """Hash a partition key to determine shard placement (MD5-based, like AWS)."""
        md5 = hashlib.md5(partition_key.encode()).hexdigest()
        return int(md5, 16)

    def _select_shard(self, partition_key: str) -> Shard:
        """Select shard based on partition key hash."""
        hash_val = self._hash_partition_key(partition_key)
        for shard in self.shards:
            if shard.hash_key_start <= hash_val <= shard.hash_key_end:
                return shard
        # Fallback to first shard
        return self.shards[0]

    def put_record(
        self, partition_key: str, data: bytes, explicit_hash_key: str | None = None
    ) -> KinesisRecord:
        if explicit_hash_key is not None:
            hash_val = int(explicit_hash_key)
            for shard in self.shards:
                if shard.hash_key_start <= hash_val <= shard.hash_key_end:
                    return shard.put_record(partition_key, data)
        return self._select_shard(partition_key).put_record(partition_key, data)


class KinesisStore:
    def __init__(self):
        self.streams: dict[str, KinesisStream] = {}
        self.resource_policies: dict[str, str] = {}
        self.lock = threading.Lock()

    def create_stream(
        self, name: str, shard_count: int, region: str, account_id: str
    ) -> KinesisStream:
        with self.lock:
            if name in self.streams:
                raise ValueError(f"Stream {name} already exists")
            arn = f"arn:aws:kinesis:{region}:{account_id}:stream/{name}"

            shards = []
            for i in range(shard_count):
                # Divide hash key space evenly among shards
                range_size = (MAX_HASH_KEY + 1) // shard_count
                start = i * range_size
                end = (i + 1) * range_size - 1 if i < shard_count - 1 else MAX_HASH_KEY

                shard = Shard(
                    shard_id=f"shardId-{i:012d}",
                    hash_key_start=start,
                    hash_key_end=end,
                    starting_sequence_number="00000000000000000000",
                )
                shards.append(shard)

            stream = KinesisStream(
                name=name,
                arn=arn,
                shards=shards,
                shard_count=shard_count,
            )
            self.streams[name] = stream
            return stream

    def get_stream(self, name: str) -> KinesisStream | None:
        with self.lock:
            return self.streams.get(name)

    def delete_stream(self, name: str) -> bool:
        with self.lock:
            if name in self.streams:
                del self.streams[name]
                return True
            return False

    def list_streams(self) -> list[str]:
        with self.lock:
            return sorted(self.streams.keys())


DEFAULT_ACCOUNT_ID = "123456789012"

_stores: dict[tuple[str, str], KinesisStore] = {}
_store_lock = threading.Lock()


def _get_store(region: str = "us-east-1", account_id: str = DEFAULT_ACCOUNT_ID) -> KinesisStore:
    key = (account_id, region)
    with _store_lock:
        if key not in _stores:
            _stores[key] = KinesisStore()
        return _stores[key]
