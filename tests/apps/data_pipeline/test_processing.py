"""
Tests for data transformation, schema validation, deduplication, and dead-letter handling.
"""

import pytest

pytestmark = pytest.mark.apps


class TestDataTransformation:
    """Raw record processing: validation, enrichment, dedup."""

    def test_transform_valid_record(self, pipeline):
        """A valid raw record is transformed into a ProcessedRecord."""
        raw = {
            "sensor_id": "sensor-001",
            "timestamp": "2026-03-08T10:00:00Z",
            "sensor_type": "temperature",
            "value": 72.5,
        }
        result = pipeline.process_reading(raw)
        assert result is not None
        assert result.sensor_id == "sensor-001"
        assert result.value == 72.5
        assert result.partition_key == "temperature#sensor-001"
        assert result.processed_at  # non-empty
        assert len(result.record_id) == 16  # sha256 hex prefix

    def test_reject_missing_fields(self, pipeline):
        """Records missing required fields are rejected."""
        incomplete = {"sensor_id": "sensor-001", "timestamp": "2026-03-08T10:00:00Z"}
        result = pipeline.process_reading(incomplete)
        assert result is None

    def test_reject_invalid_value_type(self, pipeline):
        """Records with non-numeric value are rejected."""
        bad_value = {
            "sensor_id": "sensor-001",
            "timestamp": "2026-03-08T10:00:00Z",
            "sensor_type": "temperature",
            "value": "not-a-number",
        }
        result = pipeline.process_reading(bad_value)
        assert result is None

    def test_deduplication(self, pipeline):
        """Duplicate records (same sensor_id + timestamp) are skipped."""
        raw = {
            "sensor_id": "sensor-dup",
            "timestamp": "2026-03-08T10:00:00Z",
            "sensor_type": "temperature",
            "value": 72.5,
        }
        first = pipeline.process_reading(raw)
        assert first is not None

        duplicate = pipeline.process_reading(raw)
        assert duplicate is None

    def test_dedup_different_sensors_same_time(self, pipeline):
        """Different sensors at the same timestamp are NOT deduplicated."""
        raw_a = {
            "sensor_id": "sensor-A",
            "timestamp": "2026-03-08T10:00:00Z",
            "sensor_type": "temperature",
            "value": 72.5,
        }
        raw_b = {
            "sensor_id": "sensor-B",
            "timestamp": "2026-03-08T10:00:00Z",
            "sensor_type": "pressure",
            "value": 14.7,
        }
        assert pipeline.process_reading(raw_a) is not None
        assert pipeline.process_reading(raw_b) is not None

    def test_reset_dedup_cache(self, pipeline):
        """After resetting dedup cache, the same record can be processed again."""
        raw = {
            "sensor_id": "sensor-reset",
            "timestamp": "2026-03-08T10:00:00Z",
            "sensor_type": "temperature",
            "value": 72.5,
        }
        assert pipeline.process_reading(raw) is not None
        assert pipeline.process_reading(raw) is None  # duplicate

        pipeline.reset_dedup_cache()
        assert pipeline.process_reading(raw) is not None  # processed again

    def test_enrichment_partition_key(self, pipeline):
        """Processed records have partition_key = sensor_type#sensor_id."""
        raw = {
            "sensor_id": "sensor-enrich",
            "timestamp": "2026-03-08T10:00:00Z",
            "sensor_type": "humidity",
            "value": 45.0,
        }
        result = pipeline.process_reading(raw)
        assert result.partition_key == "humidity#sensor-enrich"

    def test_enrichment_processed_at(self, pipeline):
        """Processed records have a non-empty processed_at timestamp."""
        raw = {
            "sensor_id": "sensor-time",
            "timestamp": "2026-03-08T10:00:00Z",
            "sensor_type": "temperature",
            "value": 72.5,
        }
        result = pipeline.process_reading(raw)
        assert "2026" in result.processed_at or "202" in result.processed_at


class TestBatchProcessing:
    """Batch processing: valid + invalid records separated."""

    def test_process_batch_separates_good_and_bad(self, pipeline):
        """process_batch returns (processed, dead_letters)."""
        ts = "2026-03-08T10:00:0"
        raw_records = [
            {
                "sensor_id": "s1",
                "timestamp": f"{ts}0Z",
                "sensor_type": "temperature",
                "value": 72.5,
            },
            {"sensor_id": "s2", "timestamp": f"{ts}1Z"},
            {"sensor_id": "s3", "timestamp": f"{ts}2Z", "sensor_type": "pressure", "value": 14.7},
            {"sensor_id": "s4", "timestamp": f"{ts}3Z", "sensor_type": "humidity", "value": "bad"},
        ]
        processed, dead_letters = pipeline.process_batch(raw_records)
        assert len(processed) == 2
        assert len(dead_letters) == 2

    def test_dead_letter_storage(self, pipeline):
        """Malformed records are stored in the dead-letter S3 prefix."""
        bad_record = {"sensor_id": "bad-sensor", "garbage": True}
        key = pipeline.store_dead_letter(bad_record, "missing required fields")
        assert key.startswith("dead-letter/")

        dl_keys = pipeline.list_dead_letters()
        assert len(dl_keys) >= 1

        # Read it back
        obj = pipeline.s3.get_object(Bucket=pipeline.bucket_name, Key=key)
        import json

        payload = json.loads(obj["Body"].read())
        assert payload["record"]["sensor_id"] == "bad-sensor"
        assert payload["error"] == "missing required fields"

    def test_batch_buffer_auto_flush(self, pipeline):
        """Adding records beyond batch_size triggers auto-flush to S3."""
        # batch_size is 5 in test config
        for i in range(4):
            result = pipeline.add_to_batch({"sensor_id": f"s-{i}", "value": float(i)})
            assert result is None  # not flushed yet

        assert pipeline.batch_buffer_size() == 4

        # 5th record triggers flush
        result = pipeline.add_to_batch({"sensor_id": "s-4", "value": 4.0})
        assert result is not None
        assert result.records_written == 5
        assert pipeline.batch_buffer_size() == 0

    def test_manual_flush(self, pipeline):
        """Manually flush a partial batch."""
        pipeline.add_to_batch({"sensor_id": "s-0", "value": 0.0})
        pipeline.add_to_batch({"sensor_id": "s-1", "value": 1.0})
        assert pipeline.batch_buffer_size() == 2

        result = pipeline.flush_batch()
        assert result.records_written == 2
        assert pipeline.batch_buffer_size() == 0

    def test_flush_empty_buffer(self, pipeline):
        """Flushing an empty buffer returns a zero-count result."""
        result = pipeline.flush_batch()
        assert result.records_written == 0
