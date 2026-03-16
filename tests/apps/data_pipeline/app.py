"""
IoT Sensor Data Pipeline Application

A factory-floor monitoring system that ingests sensor readings through Kinesis,
archives raw data to S3 (partitioned by date), indexes processed records in
DynamoDB with GSIs for efficient querying, stores configuration in SSM Parameter
Store and credentials in Secrets Manager, and publishes operational metrics and
alarms to CloudWatch.

This module contains the DataPipeline class which orchestrates all AWS service
interactions. It is a pure boto3 application — no robotocore or moto imports.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from .models import (
    AlertConfig,
    BatchResult,
    PipelineConfig,
    PipelineMetrics,
    ProcessedRecord,
    SensorReading,
)


class DataPipeline:
    """Orchestrates an IoT sensor data pipeline across 7 AWS services.

    Services used:
        - Kinesis: real-time ingestion of sensor readings
        - S3: raw data lake with date-partitioned storage
        - DynamoDB: indexed storage with GSI for sensor-type queries
        - SSM Parameter Store: pipeline configuration
        - Secrets Manager: database credentials and API keys
        - CloudWatch Metrics/Alarms: operational monitoring
        - CloudWatch Logs: audit trail
    """

    def __init__(
        self,
        kinesis,
        s3,
        dynamodb,
        ssm,
        secretsmanager,
        cloudwatch,
        logs,
        *,
        config: PipelineConfig | None = None,
        bucket_name: str = "",
        metrics_namespace: str = "IoTPipeline",
        log_group: str = "",
        log_stream: str = "main",
    ):
        self.kinesis = kinesis
        self.s3 = s3
        self.dynamodb = dynamodb
        self.ssm = ssm
        self.secretsmanager = secretsmanager
        self.cloudwatch = cloudwatch
        self.logs = logs

        self.config = config or PipelineConfig(stream_name="default")
        self.bucket_name = bucket_name
        self.metrics_namespace = metrics_namespace
        self.log_group = log_group
        self.log_stream = log_stream

        # Internal state
        self._batch_buffer: list[dict] = []
        self._seen_record_ids: set[str] = set()
        self._metrics = PipelineMetrics()
        self._streams: dict[str, str] = {}  # stream_name -> stream ARN or status

    # -----------------------------------------------------------------------
    # Kinesis stream management
    # -----------------------------------------------------------------------

    def create_stream(self, stream_name: str, shard_count: int = 1) -> str:
        """Create a Kinesis stream and wait for it to become ACTIVE."""
        self.kinesis.create_stream(StreamName=stream_name, ShardCount=shard_count)
        for _ in range(30):
            desc = self.kinesis.describe_stream(StreamName=stream_name)
            status = desc["StreamDescription"]["StreamStatus"]
            if status == "ACTIVE":
                arn = desc["StreamDescription"]["StreamARN"]
                self._streams[stream_name] = arn
                return arn
            time.sleep(0.5)
        raise TimeoutError(f"Stream {stream_name} did not become ACTIVE")

    def delete_stream(self, stream_name: str) -> None:
        """Delete a Kinesis stream."""
        self.kinesis.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)
        self._streams.pop(stream_name, None)

    def put_record(self, stream_name: str, reading: SensorReading) -> dict:
        """Put a single sensor reading to a Kinesis stream."""
        data = self._reading_to_dict(reading)
        resp = self.kinesis.put_record(
            StreamName=stream_name,
            Data=json.dumps(data).encode(),
            PartitionKey=reading.sensor_id,
        )
        self._metrics.records_in += 1
        self._metrics.bytes_ingested += len(json.dumps(data))
        return resp

    def put_records(self, stream_name: str, readings: list[SensorReading]) -> dict:
        """Batch-put multiple sensor readings to a Kinesis stream."""
        records = []
        for reading in readings:
            data = self._reading_to_dict(reading)
            records.append(
                {
                    "Data": json.dumps(data).encode(),
                    "PartitionKey": reading.sensor_id,
                }
            )
        resp = self.kinesis.put_records(StreamName=stream_name, Records=records)
        successful = len(readings) - resp.get("FailedRecordCount", 0)
        self._metrics.records_in += successful
        for reading in readings:
            self._metrics.bytes_ingested += len(json.dumps(self._reading_to_dict(reading)))
        return resp

    def get_records(self, stream_name: str, limit: int = 100) -> list[dict]:
        """Read all available records from a Kinesis stream (all shards)."""
        desc = self.kinesis.describe_stream(StreamName=stream_name)
        shards = desc["StreamDescription"]["Shards"]
        all_records: list[dict] = []

        for shard in shards:
            shard_id = shard["ShardId"]
            iterator = self.kinesis.get_shard_iterator(
                StreamName=stream_name,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )["ShardIterator"]

            for _ in range(5):
                resp = self.kinesis.get_records(ShardIterator=iterator, Limit=limit)
                for record in resp["Records"]:
                    decoded = json.loads(record["Data"])
                    decoded["_sequence_number"] = record["SequenceNumber"]
                    decoded["_shard_id"] = shard_id
                    all_records.append(decoded)
                iterator = resp["NextShardIterator"]
                if not resp["Records"]:
                    break

        return all_records

    def list_streams(self) -> list[str]:
        """List all known Kinesis streams managed by this pipeline."""
        return list(self._streams.keys())

    # -----------------------------------------------------------------------
    # S3 data lake
    # -----------------------------------------------------------------------

    def store_raw_batch(
        self, records: list[dict], *, partition_time: datetime | None = None
    ) -> BatchResult:
        """Store a batch of records as a JSONL file in S3, partitioned by date.

        Key format: {s3_prefix}/year=YYYY/month=MM/day=DD/hour=HH/{batch_id}.jsonl
        """
        if partition_time is None:
            partition_time = datetime.now(UTC)

        batch_id = uuid.uuid4().hex[:12]
        key = (
            f"{self.config.s3_prefix}"
            f"/year={partition_time.year:04d}"
            f"/month={partition_time.month:02d}"
            f"/day={partition_time.day:02d}"
            f"/hour={partition_time.hour:02d}"
            f"/{batch_id}.jsonl"
        )

        body = "\n".join(json.dumps(r) for r in records).encode()
        content_hash = hashlib.sha256(body).hexdigest()

        self.s3.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=body,
            Metadata={"content-sha256": content_hash, "record-count": str(len(records))},
        )

        return BatchResult(
            records_written=len(records),
            bytes_written=len(body),
            s3_key=key,
        )

    def read_raw_batch(self, s3_key: str) -> list[dict]:
        """Read a JSONL batch file from S3 and parse into records."""
        obj = self.s3.get_object(Bucket=self.bucket_name, Key=s3_key)
        body = obj["Body"].read().decode()
        return [json.loads(line) for line in body.strip().split("\n") if line.strip()]

    def list_partitions(self, prefix: str) -> list[str]:
        """List all S3 keys under a given prefix (partition browsing)."""
        resp = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        contents = resp.get("Contents", [])
        return [obj["Key"] for obj in contents]

    def store_dead_letter(self, record: dict, error: str) -> str:
        """Store a malformed record in the dead-letter prefix for investigation."""
        dl_id = uuid.uuid4().hex[:12]
        key = f"dead-letter/{dl_id}.json"
        payload = {"record": record, "error": error, "timestamp": datetime.now(UTC).isoformat()}
        self.s3.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=json.dumps(payload).encode(),
        )
        self._metrics.errors += 1
        return key

    def list_dead_letters(self) -> list[str]:
        """List all dead-letter records."""
        return self.list_partitions("dead-letter/")

    # -----------------------------------------------------------------------
    # DynamoDB indexing
    # -----------------------------------------------------------------------

    def index_record(self, record: ProcessedRecord) -> None:
        """Store a processed record in DynamoDB."""
        self.dynamodb.put_item(
            TableName=self.config.table_name,
            Item={
                "sensor_id": {"S": record.sensor_id},
                "timestamp": {"S": record.timestamp},
                "record_id": {"S": record.record_id},
                "value": {"N": str(record.value)},
                "processed_at": {"S": record.processed_at},
                "partition_key": {"S": record.partition_key},
                "sensor_type": {
                    "S": record.partition_key.split("#")[0]
                    if "#" in record.partition_key
                    else "unknown"
                },
            },
        )
        self._metrics.records_out += 1

    def batch_index_records(self, records: list[ProcessedRecord]) -> int:
        """Batch-write processed records to DynamoDB. Returns count written."""
        items = []
        for rec in records:
            items.append(
                {
                    "PutRequest": {
                        "Item": {
                            "sensor_id": {"S": rec.sensor_id},
                            "timestamp": {"S": rec.timestamp},
                            "record_id": {"S": rec.record_id},
                            "value": {"N": str(rec.value)},
                            "processed_at": {"S": rec.processed_at},
                            "partition_key": {"S": rec.partition_key},
                            "sensor_type": {
                                "S": rec.partition_key.split("#")[0]
                                if "#" in rec.partition_key
                                else "unknown"
                            },
                        }
                    }
                }
            )

        # DynamoDB batch_write_item supports max 25 items per call
        written = 0
        for i in range(0, len(items), 25):
            chunk = items[i : i + 25]
            self.dynamodb.batch_write_item(RequestItems={self.config.table_name: chunk})
            written += len(chunk)

        self._metrics.records_out += written
        return written

    def query_by_sensor(self, sensor_id: str, limit: int = 100) -> list[dict]:
        """Query readings by sensor ID (primary key)."""
        resp = self.dynamodb.query(
            TableName=self.config.table_name,
            KeyConditionExpression="sensor_id = :sid",
            ExpressionAttributeValues={":sid": {"S": sensor_id}},
            Limit=limit,
        )
        return resp["Items"]

    def query_by_time_range(self, sensor_id: str, start_time: str, end_time: str) -> list[dict]:
        """Query readings for a sensor within a time range."""
        resp = self.dynamodb.query(
            TableName=self.config.table_name,
            KeyConditionExpression="sensor_id = :sid AND #ts BETWEEN :start AND :end",
            ExpressionAttributeNames={"#ts": "timestamp"},
            ExpressionAttributeValues={
                ":sid": {"S": sensor_id},
                ":start": {"S": start_time},
                ":end": {"S": end_time},
            },
        )
        return resp["Items"]

    def query_by_sensor_type(self, sensor_type: str, limit: int = 100) -> list[dict]:
        """Query readings by sensor type using the GSI."""
        resp = self.dynamodb.query(
            TableName=self.config.table_name,
            IndexName="by-sensor-type",
            KeyConditionExpression="sensor_type = :st",
            ExpressionAttributeValues={":st": {"S": sensor_type}},
            Limit=limit,
        )
        return resp["Items"]

    def get_record(self, sensor_id: str, timestamp: str) -> dict | None:
        """Get a single record by primary key."""
        resp = self.dynamodb.get_item(
            TableName=self.config.table_name,
            Key={
                "sensor_id": {"S": sensor_id},
                "timestamp": {"S": timestamp},
            },
        )
        return resp.get("Item")

    def update_record_status(self, sensor_id: str, timestamp: str, status: str) -> None:
        """Update the processing status of a record."""
        self.dynamodb.update_item(
            TableName=self.config.table_name,
            Key={
                "sensor_id": {"S": sensor_id},
                "timestamp": {"S": timestamp},
            },
            UpdateExpression="SET #s = :st",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":st": {"S": status}},
        )

    # -----------------------------------------------------------------------
    # SSM Parameter Store (configuration)
    # -----------------------------------------------------------------------

    def store_config(self, pipeline_id: str, config: PipelineConfig) -> None:
        """Store pipeline configuration as hierarchical SSM parameters."""
        prefix = f"/pipeline/{pipeline_id}"
        params = {
            f"{prefix}/stream_name": config.stream_name,
            f"{prefix}/batch_size": str(config.batch_size),
            f"{prefix}/flush_interval": str(config.flush_interval),
            f"{prefix}/s3_prefix": config.s3_prefix,
            f"{prefix}/table_name": config.table_name,
        }
        for name, value in params.items():
            self.ssm.put_parameter(Name=name, Value=value, Type="String", Overwrite=True)

    def load_config(self, pipeline_id: str) -> PipelineConfig:
        """Load pipeline configuration from SSM parameters."""
        prefix = f"/pipeline/{pipeline_id}"
        resp = self.ssm.get_parameters_by_path(Path=prefix, Recursive=True)
        params = {p["Name"].split("/")[-1]: p["Value"] for p in resp["Parameters"]}
        return PipelineConfig(
            stream_name=params.get("stream_name", "default"),
            batch_size=int(params.get("batch_size", "100")),
            flush_interval=int(params.get("flush_interval", "30")),
            s3_prefix=params.get("s3_prefix", "raw"),
            table_name=params.get("table_name", "sensor-readings"),
        )

    def update_config_param(self, pipeline_id: str, key: str, value: str) -> int:
        """Update a single config parameter. Returns the new version number."""
        name = f"/pipeline/{pipeline_id}/{key}"
        resp = self.ssm.put_parameter(Name=name, Value=value, Type="String", Overwrite=True)
        return resp["Version"]

    def get_config_history(self, pipeline_id: str, key: str) -> list[dict]:
        """Get version history for a config parameter."""
        name = f"/pipeline/{pipeline_id}/{key}"
        resp = self.ssm.get_parameter_history(Name=name)
        return resp["Parameters"]

    def delete_config(self, pipeline_id: str) -> None:
        """Delete all SSM parameters for a pipeline."""
        prefix = f"/pipeline/{pipeline_id}"
        resp = self.ssm.get_parameters_by_path(Path=prefix, Recursive=True)
        for param in resp["Parameters"]:
            try:
                self.ssm.delete_parameter(Name=param["Name"])
            except Exception:
                pass  # best-effort cleanup

    # -----------------------------------------------------------------------
    # Secrets Manager (credentials)
    # -----------------------------------------------------------------------

    def store_credentials(self, secret_name: str, credentials: dict) -> str:
        """Store database credentials or API keys in Secrets Manager."""
        resp = self.secretsmanager.create_secret(
            Name=secret_name, SecretString=json.dumps(credentials)
        )
        return resp["ARN"]

    def get_credentials(self, secret_name: str) -> dict:
        """Retrieve credentials from Secrets Manager."""
        resp = self.secretsmanager.get_secret_value(SecretId=secret_name)
        return json.loads(resp["SecretString"])

    def update_credentials(self, secret_name: str, credentials: dict) -> None:
        """Update stored credentials."""
        self.secretsmanager.update_secret(
            SecretId=secret_name, SecretString=json.dumps(credentials)
        )

    def delete_credentials(self, secret_name: str) -> None:
        """Delete credentials from Secrets Manager."""
        self.secretsmanager.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)

    # -----------------------------------------------------------------------
    # CloudWatch metrics and alarms
    # -----------------------------------------------------------------------

    def publish_metrics(self, metrics: PipelineMetrics | None = None) -> None:
        """Publish pipeline metrics to CloudWatch."""
        m = metrics or self._metrics
        metric_data = [
            {"MetricName": "RecordsProcessed", "Value": m.records_out, "Unit": "Count"},
            {"MetricName": "RecordsIngested", "Value": m.records_in, "Unit": "Count"},
            {"MetricName": "ErrorCount", "Value": m.errors, "Unit": "Count"},
            {"MetricName": "BytesIngested", "Value": m.bytes_ingested, "Unit": "Bytes"},
        ]
        if m.avg_latency_ms > 0:
            metric_data.append(
                {"MetricName": "AvgLatencyMs", "Value": m.avg_latency_ms, "Unit": "Milliseconds"}
            )
        self.cloudwatch.put_metric_data(Namespace=self.metrics_namespace, MetricData=metric_data)

    def get_metric_sum(
        self,
        metric_name: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> float:
        """Get the sum of a metric over a time period."""
        if start is None:
            start = datetime(2020, 1, 1, tzinfo=UTC)
        if end is None:
            end = datetime(2030, 1, 1, tzinfo=UTC)
        resp = self.cloudwatch.get_metric_statistics(
            Namespace=self.metrics_namespace,
            MetricName=metric_name,
            StartTime=start,
            EndTime=end,
            Period=86400,
            Statistics=["Sum"],
        )
        return sum(dp["Sum"] for dp in resp.get("Datapoints", []))

    def create_alarm(self, alert: AlertConfig) -> None:
        """Create a CloudWatch alarm for pipeline health monitoring."""
        alarm_name = alert.alarm_name or f"{self.metrics_namespace}-{alert.metric_name}"
        self.cloudwatch.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace=self.metrics_namespace,
            MetricName=alert.metric_name,
            Statistic="Sum",
            Period=alert.period_seconds,
            EvaluationPeriods=1,
            Threshold=alert.threshold,
            ComparisonOperator=alert.comparison,
        )

    def set_alarm_state(self, alarm_name: str, state: str, reason: str) -> None:
        """Force an alarm state (for testing)."""
        self.cloudwatch.set_alarm_state(AlarmName=alarm_name, StateValue=state, StateReason=reason)

    def describe_alarm(self, alarm_name: str) -> dict | None:
        """Get alarm details."""
        resp = self.cloudwatch.describe_alarms(AlarmNames=[alarm_name])
        alarms = resp.get("MetricAlarms", [])
        return alarms[0] if alarms else None

    def delete_alarm(self, alarm_name: str) -> None:
        """Delete a CloudWatch alarm."""
        self.cloudwatch.delete_alarms(AlarmNames=[alarm_name])

    # -----------------------------------------------------------------------
    # CloudWatch Logs (audit trail)
    # -----------------------------------------------------------------------

    def log_event(self, message: str) -> None:
        """Write a single audit log event."""
        if not self.log_group:
            return
        self.logs.put_log_events(
            logGroupName=self.log_group,
            logStreamName=self.log_stream,
            logEvents=[{"timestamp": int(time.time() * 1000), "message": message}],
        )

    def log_events(self, messages: list[str]) -> None:
        """Write multiple audit log events."""
        if not self.log_group:
            return
        now_ms = int(time.time() * 1000)
        events = [{"timestamp": now_ms + i, "message": msg} for i, msg in enumerate(messages)]
        self.logs.put_log_events(
            logGroupName=self.log_group,
            logStreamName=self.log_stream,
            logEvents=events,
        )

    def get_log_events(self) -> list[dict]:
        """Read all audit log events."""
        if not self.log_group:
            return []
        resp = self.logs.get_log_events(
            logGroupName=self.log_group,
            logStreamName=self.log_stream,
            startFromHead=True,
        )
        return resp.get("events", [])

    def filter_logs(self, pattern: str) -> list[dict]:
        """Filter audit log events by pattern."""
        if not self.log_group:
            return []
        resp = self.logs.filter_log_events(logGroupName=self.log_group, filterPattern=pattern)
        return resp.get("events", [])

    # -----------------------------------------------------------------------
    # Data transformation and processing
    # -----------------------------------------------------------------------

    def process_reading(self, raw: dict) -> ProcessedRecord | None:
        """Transform a raw Kinesis record dict into a ProcessedRecord.

        Validates schema, deduplicates, enriches with metadata.
        Returns None if the record is invalid or a duplicate.
        """
        # Schema validation
        required_fields = {"sensor_id", "timestamp", "value", "sensor_type"}
        if not required_fields.issubset(raw.keys()):
            return None

        # Type validation
        try:
            value = float(raw["value"])
        except (ValueError, TypeError):
            return None

        # Deduplication
        record_id = self._compute_record_id(raw)
        if record_id in self._seen_record_ids:
            return None
        self._seen_record_ids.add(record_id)

        # Enrich
        processed_at = datetime.now(UTC).isoformat()
        partition_key = f"{raw['sensor_type']}#{raw['sensor_id']}"

        return ProcessedRecord(
            record_id=record_id,
            sensor_id=raw["sensor_id"],
            timestamp=raw["timestamp"],
            value=value,
            processed_at=processed_at,
            partition_key=partition_key,
        )

    def process_batch(self, raw_records: list[dict]) -> tuple[list[ProcessedRecord], list[dict]]:
        """Process a batch of raw records.

        Returns (processed_records, dead_letters) where dead_letters are
        records that failed validation.
        """
        processed = []
        dead_letters = []
        for raw in raw_records:
            result = self.process_reading(raw)
            if result is not None:
                processed.append(result)
            else:
                dead_letters.append(raw)
        return processed, dead_letters

    # -----------------------------------------------------------------------
    # Batch buffer management
    # -----------------------------------------------------------------------

    def add_to_batch(self, record: dict) -> BatchResult | None:
        """Add a record to the internal batch buffer.

        If the buffer reaches batch_size, automatically flushes to S3.
        Returns a BatchResult if a flush occurred, None otherwise.
        """
        self._batch_buffer.append(record)
        if len(self._batch_buffer) >= self.config.batch_size:
            return self.flush_batch()
        return None

    def flush_batch(self, partition_time: datetime | None = None) -> BatchResult:
        """Flush the current batch buffer to S3."""
        if not self._batch_buffer:
            return BatchResult()
        records = list(self._batch_buffer)
        self._batch_buffer.clear()
        return self.store_raw_batch(records, partition_time=partition_time)

    def batch_buffer_size(self) -> int:
        """Return the current number of records in the batch buffer."""
        return len(self._batch_buffer)

    # -----------------------------------------------------------------------
    # Backfill: re-process records from S3
    # -----------------------------------------------------------------------

    def backfill_from_s3(self, s3_key: str) -> tuple[int, int]:
        """Re-process records from an S3 batch file through the pipeline.

        Returns (processed_count, error_count).
        """
        raw_records = self.read_raw_batch(s3_key)
        processed, dead_letters = self.process_batch(raw_records)

        if processed:
            self.batch_index_records(processed)

        for dl in dead_letters:
            self.store_dead_letter(dl, "backfill: schema validation failed")

        return len(processed), len(dead_letters)

    # -----------------------------------------------------------------------
    # Pipeline statistics
    # -----------------------------------------------------------------------

    def get_statistics(self) -> PipelineMetrics:
        """Return current pipeline metrics."""
        return PipelineMetrics(
            records_in=self._metrics.records_in,
            records_out=self._metrics.records_out,
            errors=self._metrics.errors,
            bytes_ingested=self._metrics.bytes_ingested,
            avg_latency_ms=self._metrics.avg_latency_ms,
        )

    def reset_statistics(self) -> None:
        """Reset internal metrics counters."""
        self._metrics = PipelineMetrics()

    def reset_dedup_cache(self) -> None:
        """Clear the deduplication cache (e.g., for backfill operations)."""
        self._seen_record_ids.clear()

    # -----------------------------------------------------------------------
    # End-to-end pipeline run
    # -----------------------------------------------------------------------

    def run_ingestion_cycle(
        self, stream_name: str, *, partition_time: datetime | None = None
    ) -> dict[str, Any]:
        """Execute a full ingestion cycle: read Kinesis → process → S3 + DynamoDB.

        Returns a summary dict with counts and keys.
        """
        # Read from Kinesis
        raw_records = self.get_records(stream_name)
        if not raw_records:
            return {"raw_count": 0, "processed_count": 0, "error_count": 0}

        # Process
        processed, dead_letters = self.process_batch(raw_records)

        # Archive raw to S3
        batch_result = self.store_raw_batch(raw_records, partition_time=partition_time)

        # Index processed in DynamoDB
        if processed:
            self.batch_index_records(processed)

        # Store dead letters
        dl_keys = []
        for dl in dead_letters:
            key = self.store_dead_letter(dl, "ingestion: schema validation failed")
            dl_keys.append(key)

        # Log
        self.log_event(
            json.dumps(
                {
                    "event": "ingestion_cycle",
                    "raw_count": len(raw_records),
                    "processed_count": len(processed),
                    "error_count": len(dead_letters),
                    "s3_key": batch_result.s3_key,
                }
            )
        )

        return {
            "raw_count": len(raw_records),
            "processed_count": len(processed),
            "error_count": len(dead_letters),
            "batch_result": batch_result,
            "dead_letter_keys": dl_keys,
        }

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    @staticmethod
    def _reading_to_dict(reading: SensorReading) -> dict:
        """Convert a SensorReading dataclass to a plain dict for serialization."""
        return {
            "sensor_id": reading.sensor_id,
            "timestamp": reading.timestamp,
            "sensor_type": reading.reading_type,
            "value": reading.value,
            "unit": reading.unit,
            "location": reading.location,
        }

    @staticmethod
    def _compute_record_id(raw: dict) -> str:
        """Compute a deterministic record ID from sensor_id + timestamp."""
        key = f"{raw.get('sensor_id', '')}:{raw.get('timestamp', '')}"
        return hashlib.sha256(key.encode()).hexdigest()[:16]
