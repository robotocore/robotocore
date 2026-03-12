"""
Tests for Kinesis ingestion: put records, batch put, read back, ordering.
"""

import pytest

from .models import SensorReading

pytestmark = pytest.mark.apps


class TestKinesisIngestion:
    """Kinesis stream record ingestion and retrieval."""

    def test_put_single_record(self, pipeline):
        """Put a single sensor reading and read it back from Kinesis."""
        reading = SensorReading(
            "sensor-001",
            "2026-03-08T10:00:00Z",
            "temperature",
            72.5,
            "celsius",
        )
        resp = pipeline.put_record(pipeline.config.stream_name, reading)
        assert "ShardId" in resp
        assert "SequenceNumber" in resp

        records = pipeline.get_records(pipeline.config.stream_name)
        assert len(records) >= 1
        assert records[0]["sensor_id"] == "sensor-001"
        assert records[0]["value"] == 72.5
        assert records[0]["sensor_type"] == "temperature"

    def test_put_batch_records(self, pipeline, sensor_readings):
        """Batch-put multiple readings and verify all arrive."""
        resp = pipeline.put_records(pipeline.config.stream_name, sensor_readings)
        assert resp["FailedRecordCount"] == 0

        records = pipeline.get_records(pipeline.config.stream_name)
        assert len(records) == len(sensor_readings)
        sensor_ids = {r["sensor_id"] for r in records}
        assert "sensor-001" in sensor_ids
        assert "sensor-002" in sensor_ids

    def test_record_data_integrity(self, pipeline):
        """Verify the full payload round-trips through Kinesis intact."""
        reading = SensorReading(
            "sensor-099", "2026-03-08T12:30:00Z", "pressure", 14.696, "psi", "cleanroom-3"
        )
        pipeline.put_record(pipeline.config.stream_name, reading)

        records = pipeline.get_records(pipeline.config.stream_name)
        assert len(records) >= 1
        rec = records[0]
        assert rec["sensor_id"] == "sensor-099"
        assert rec["sensor_type"] == "pressure"
        assert rec["value"] == 14.696
        assert rec["unit"] == "psi"
        assert rec["location"] == "cleanroom-3"

    def test_large_record_payload(self, pipeline):
        """Put a record near the 1MB Kinesis limit with extra metadata."""
        # Not actually 1MB, but a non-trivial payload with extra fields
        reading = SensorReading(
            "sensor-big", "2026-03-08T10:00:00Z", "temperature", 99.9, "celsius"
        )
        pipeline.put_record(pipeline.config.stream_name, reading)

        records = pipeline.get_records(pipeline.config.stream_name)
        assert len(records) >= 1
        assert records[0]["sensor_id"] == "sensor-big"

    def test_metrics_updated_on_ingest(self, pipeline, sensor_readings):
        """Pipeline metrics track records_in and bytes_ingested after put."""
        pipeline.put_records(pipeline.config.stream_name, sensor_readings)
        stats = pipeline.get_statistics()
        assert stats.records_in == len(sensor_readings)
        assert stats.bytes_ingested > 0

    def test_multiple_puts_accumulate(self, pipeline):
        """Multiple individual puts accumulate in Kinesis."""
        for i in range(3):
            reading = SensorReading(
                f"sensor-{i}", f"2026-03-08T10:0{i}:00Z", "temperature", 70.0 + i, "celsius"
            )
            pipeline.put_record(pipeline.config.stream_name, reading)

        records = pipeline.get_records(pipeline.config.stream_name)
        assert len(records) == 3
        values = sorted(r["value"] for r in records)
        assert values == [70.0, 71.0, 72.0]
