"""
Data models for the IoT sensor data pipeline.

Pure Python dataclasses — no AWS imports. These model the domain objects
that flow through the pipeline: sensor readings, configuration, processed
records, metrics, and batch results.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SensorReading:
    """A single reading from an IoT sensor on the factory floor."""

    sensor_id: str
    timestamp: str
    reading_type: str
    value: float
    unit: str
    location: str = "factory-floor-1"


@dataclass
class PipelineConfig:
    """Configuration for a single pipeline stream."""

    stream_name: str
    batch_size: int = 100
    flush_interval: int = 30
    s3_prefix: str = "raw"
    table_name: str = "sensor-readings"


@dataclass
class ProcessedRecord:
    """A sensor reading after processing: validated, enriched, assigned partition."""

    record_id: str
    sensor_id: str
    timestamp: str
    value: float
    processed_at: str
    partition_key: str


@dataclass
class PipelineMetrics:
    """Aggregate metrics for a pipeline processing window."""

    records_in: int = 0
    records_out: int = 0
    errors: int = 0
    bytes_ingested: int = 0
    avg_latency_ms: float = 0.0


@dataclass
class AlertConfig:
    """CloudWatch alarm configuration for pipeline health."""

    metric_name: str
    threshold: float
    comparison: str  # "GreaterThanThreshold", "LessThanThreshold", etc.
    period_seconds: int = 300
    alarm_name: str = ""


@dataclass
class BatchResult:
    """Result of flushing a batch of records to S3."""

    records_written: int = 0
    bytes_written: int = 0
    s3_key: str = ""
    errors: list[str] = field(default_factory=list)
