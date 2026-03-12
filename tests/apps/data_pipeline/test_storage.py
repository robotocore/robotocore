"""
Tests for S3 raw data storage and DynamoDB indexed storage.
"""

from datetime import UTC, datetime

import pytest

from .models import ProcessedRecord

pytestmark = pytest.mark.apps


class TestS3Storage:
    """S3 data lake: raw batch storage, partitioned by date."""

    def test_store_and_read_raw_batch(self, pipeline):
        """Store a JSONL batch to S3 and read it back."""
        records = [
            {"sensor_id": f"s-{i}", "value": 14.7 + i * 0.1, "sensor_type": "pressure"}
            for i in range(5)
        ]
        result = pipeline.store_raw_batch(records)
        assert result.records_written == 5
        assert result.bytes_written > 0
        assert result.s3_key.startswith("raw/")
        assert result.s3_key.endswith(".jsonl")

        read_back = pipeline.read_raw_batch(result.s3_key)
        assert len(read_back) == 5
        assert read_back[0]["sensor_id"] == "s-0"

    def test_date_partitioned_keys(self, pipeline):
        """Verify S3 keys use year/month/day/hour partitioning."""
        dt = datetime(2026, 3, 8, 14, tzinfo=UTC)
        records = [{"sensor_id": "s-1", "value": 1.0}]
        result = pipeline.store_raw_batch(records, partition_time=dt)

        assert "year=2026" in result.s3_key
        assert "month=03" in result.s3_key
        assert "day=08" in result.s3_key
        assert "hour=14" in result.s3_key

    def test_list_partitions(self, pipeline):
        """Store batches in different partitions and list them."""
        for hour in [10, 11, 12]:
            dt = datetime(2026, 3, 8, hour, tzinfo=UTC)
            pipeline.store_raw_batch([{"sensor_id": "s-1", "hour": hour}], partition_time=dt)

        keys = pipeline.list_partitions("raw/year=2026/month=03/day=08/")
        assert len(keys) == 3

    def test_s3_metadata(self, pipeline):
        """Verify S3 object metadata includes content hash and record count."""
        records = [{"sensor_id": "s-1", "value": 42.0}]
        result = pipeline.store_raw_batch(records)

        # Read the object metadata directly
        head = pipeline.s3.head_object(Bucket=pipeline.bucket_name, Key=result.s3_key)
        metadata = head.get("Metadata", {})
        assert "content-sha256" in metadata
        assert metadata["record-count"] == "1"

    def test_multiple_batches_same_partition(self, pipeline):
        """Multiple batches in the same hour get unique keys."""
        dt = datetime(2026, 3, 8, 10, tzinfo=UTC)
        keys = set()
        for i in range(3):
            result = pipeline.store_raw_batch(
                [{"sensor_id": f"s-{i}", "value": float(i)}], partition_time=dt
            )
            keys.add(result.s3_key)
        # Each batch gets a unique key
        assert len(keys) == 3


class TestDynamoDBStorage:
    """DynamoDB indexed record storage with GSI queries."""

    def test_index_and_get_record(self, pipeline):
        """Index a processed record and retrieve it by primary key."""
        rec = ProcessedRecord(
            record_id="rec-001",
            sensor_id="sensor-001",
            timestamp="2026-03-08T10:00:00Z",
            value=72.5,
            processed_at="2026-03-08T10:00:01Z",
            partition_key="temperature#sensor-001",
        )
        pipeline.index_record(rec)

        item = pipeline.get_record("sensor-001", "2026-03-08T10:00:00Z")
        assert item is not None
        assert item["record_id"]["S"] == "rec-001"
        assert item["value"]["N"] == "72.5"

    def test_query_by_sensor_id(self, pipeline):
        """Query all readings for a specific sensor."""
        for i in range(5):
            rec = ProcessedRecord(
                record_id=f"rec-{i}",
                sensor_id="sensor-A",
                timestamp=f"2026-03-08T10:{i:02d}:00Z",
                value=70.0 + i,
                processed_at="2026-03-08T10:00:00Z",
                partition_key="temperature#sensor-A",
            )
            pipeline.index_record(rec)

        items = pipeline.query_by_sensor("sensor-A")
        assert len(items) == 5

    def test_query_by_time_range(self, pipeline):
        """Query readings within a time range."""
        for i in range(10):
            rec = ProcessedRecord(
                record_id=f"rec-{i}",
                sensor_id="sensor-TR",
                timestamp=f"2026-03-08T{10 + i}:00:00Z",
                value=70.0 + i,
                processed_at="2026-03-08T10:00:00Z",
                partition_key="temperature#sensor-TR",
            )
            pipeline.index_record(rec)

        # Query hours 12-15 (inclusive)
        items = pipeline.query_by_time_range(
            "sensor-TR", "2026-03-08T12:00:00Z", "2026-03-08T15:00:00Z"
        )
        assert len(items) == 4

    def test_query_by_sensor_type_gsi(self, pipeline):
        """Query readings by sensor type using the GSI."""
        types = [("s1", "temperature"), ("s2", "temperature"), ("s3", "pressure")]
        for i, (sid, stype) in enumerate(types):
            rec = ProcessedRecord(
                record_id=f"rec-{i}",
                sensor_id=sid,
                timestamp=f"2026-03-08T10:0{i}:00Z",
                value=70.0 + i,
                processed_at="2026-03-08T10:00:00Z",
                partition_key=f"{stype}#{sid}",
            )
            pipeline.index_record(rec)

        items = pipeline.query_by_sensor_type("temperature")
        assert len(items) == 2
        ids = {item["sensor_id"]["S"] for item in items}
        assert ids == {"s1", "s2"}

    def test_batch_index_records(self, pipeline):
        """Batch-write multiple records to DynamoDB."""
        records = [
            ProcessedRecord(
                record_id=f"batch-{i}",
                sensor_id=f"sensor-{i % 3}",
                timestamp=f"2026-03-08T10:{i:02d}:00Z",
                value=60.0 + i,
                processed_at="2026-03-08T10:00:00Z",
                partition_key=f"humidity#sensor-{i % 3}",
            )
            for i in range(15)
        ]
        written = pipeline.batch_index_records(records)
        assert written == 15

    def test_update_record_status(self, pipeline):
        """Update the status field of an indexed record."""
        rec = ProcessedRecord(
            record_id="rec-upd",
            sensor_id="sensor-X",
            timestamp="2026-03-08T12:00:00Z",
            value=50.0,
            processed_at="2026-03-08T12:00:01Z",
            partition_key="temperature#sensor-X",
        )
        pipeline.index_record(rec)
        pipeline.update_record_status("sensor-X", "2026-03-08T12:00:00Z", "processed")

        item = pipeline.get_record("sensor-X", "2026-03-08T12:00:00Z")
        assert item["status"]["S"] == "processed"

    def test_s3_key_structure(self, pipeline):
        """Verify S3 key structure matches the Hive-style partitioning scheme."""
        dt = datetime(2026, 7, 15, 9, tzinfo=UTC)
        result = pipeline.store_raw_batch([{"x": 1}], partition_time=dt)
        parts = result.s3_key.split("/")
        assert parts[0] == "raw"
        assert parts[1] == "year=2026"
        assert parts[2] == "month=07"
        assert parts[3] == "day=15"
        assert parts[4] == "hour=09"
        assert parts[5].endswith(".jsonl")
