# IoT Sensor Data Pipeline

A factory-floor monitoring system that ingests sensor data from industrial IoT devices,
processes it through a real-time pipeline, and stores it for analysis and alerting.

## Scenario

A manufacturing plant has hundreds of sensors measuring temperature, pressure, and humidity
across multiple zones. Readings flow in real-time through a data pipeline that:

1. **Ingests** readings via Kinesis streams (one per sensor type)
2. **Validates** each reading's schema and data types
3. **Deduplicates** readings to prevent double-counting
4. **Archives** raw data to S3 in a date-partitioned data lake
5. **Indexes** processed records in DynamoDB for fast lookups
6. **Monitors** pipeline health via CloudWatch metrics and alarms
7. **Audits** all pipeline activity via CloudWatch Logs

## Architecture

```
Sensors ──► Kinesis Stream ──► Processing ──┬──► S3 (data lake)
                                            ├──► DynamoDB (index)
                                            ├──► CloudWatch (metrics)
                                            └──► Dead Letter (S3)

SSM Parameter Store ──► Pipeline Config
Secrets Manager ──────► DB Credentials
CloudWatch Logs ──────► Audit Trail
```

## AWS Services

| Service | Purpose |
|---------|---------|
| **Kinesis** | Real-time ingestion of sensor readings |
| **S3** | Raw data lake with Hive-style partitioning (`year=YYYY/month=MM/day=DD/hour=HH`) |
| **DynamoDB** | Indexed storage with GSI for sensor-type queries |
| **SSM Parameter Store** | Hierarchical pipeline configuration (`/pipeline/{id}/batch_size`, etc.) |
| **Secrets Manager** | Database credentials and external API keys |
| **CloudWatch Metrics** | RecordsProcessed, ErrorCount, BytesIngested, AvgLatencyMs |
| **CloudWatch Alarms** | Threshold alerts on error rate, latency |
| **CloudWatch Logs** | Structured audit trail of pipeline events |

## Data Flow

1. `SensorReading` objects are created from IoT device telemetry
2. Readings are serialized to JSON and put to a Kinesis stream
3. The pipeline reads from the stream and processes each record:
   - **Schema validation**: required fields present, value is numeric
   - **Deduplication**: SHA-256 hash of `sensor_id:timestamp`
   - **Enrichment**: adds `processed_at`, `partition_key`, `record_id`
4. Valid records become `ProcessedRecord` objects and are:
   - Archived as JSONL batches in S3 (partitioned by date)
   - Indexed in DynamoDB (PK=sensor_id, SK=timestamp, GSI on sensor_type)
5. Invalid records go to the dead-letter prefix in S3
6. Metrics are published to CloudWatch for monitoring dashboards

## Running Tests

```bash
# Against robotocore (default)
AWS_ENDPOINT_URL=http://localhost:4566 pytest tests/apps/data_pipeline/ -v

# Against real AWS
AWS_ENDPOINT_URL= pytest tests/apps/data_pipeline/ -v
```

## Test Modules

- **test_ingestion.py** — Kinesis put/get, batch records, data integrity
- **test_storage.py** — S3 partitioned storage, DynamoDB indexing, GSI queries
- **test_processing.py** — Schema validation, dedup, dead-letter, batch buffer
- **test_monitoring.py** — CloudWatch metrics, alarms, audit logs
- **test_configuration.py** — SSM parameters, Secrets Manager credentials
