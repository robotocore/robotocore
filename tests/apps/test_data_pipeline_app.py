"""
IoT Sensor Data Pipeline Application Tests

Simulates a factory-floor IoT pipeline: sensors send temperature/pressure/humidity
readings through Kinesis → S3 (raw archive) → DynamoDB (indexed) with configuration
in SSM/SecretsManager and monitoring via CloudWatch/Logs.

Exercises 7 AWS services with real data flowing end-to-end.
"""

import hashlib
import json
import time
import uuid
from datetime import UTC, datetime

import pytest

pytestmark = pytest.mark.apps

# Counter for generating unique timestamps within a test run
_ts_counter = 0

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sensor_stream(kinesis, unique_name):
    stream_name = f"sensor-readings-{unique_name}"
    kinesis.create_stream(StreamName=stream_name, ShardCount=1)
    # Wait for stream to become ACTIVE
    for _ in range(30):
        desc = kinesis.describe_stream(StreamName=stream_name)
        if desc["StreamDescription"]["StreamStatus"] == "ACTIVE":
            break
        time.sleep(0.5)
    yield stream_name
    kinesis.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)


@pytest.fixture
def raw_data_bucket(s3, unique_name):
    bucket = f"raw-sensor-data-{unique_name}"
    s3.create_bucket(Bucket=bucket)
    yield bucket
    # Cleanup all objects then bucket
    try:
        objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
        for obj in objects:
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
    except Exception:
        pass
    s3.delete_bucket(Bucket=bucket)


@pytest.fixture
def readings_table(dynamodb, unique_name):
    table_name = f"sensor-readings-{unique_name}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "sensor_id", "KeyType": "HASH"},
            {"AttributeName": "timestamp", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "sensor_id", "AttributeType": "S"},
            {"AttributeName": "timestamp", "AttributeType": "S"},
            {"AttributeName": "sensor_type", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-sensor-type",
                "KeySchema": [
                    {"AttributeName": "sensor_type", "KeyType": "HASH"},
                    {"AttributeName": "timestamp", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


@pytest.fixture
def pipeline_config(ssm, unique_name):
    pipeline_id = unique_name
    prefix = f"/pipeline/{pipeline_id}"
    params = {
        f"{prefix}/batch_size": "100",
        f"{prefix}/flush_interval_seconds": "30",
        f"{prefix}/alert_threshold_celsius": "85.0",
    }
    for name, value in params.items():
        ssm.put_parameter(Name=name, Value=value, Type="String")
    yield {"prefix": prefix, "params": params, "pipeline_id": pipeline_id}
    for name in params:
        try:
            ssm.delete_parameter(Name=name)
        except Exception:
            pass


@pytest.fixture
def pipeline_secret(secretsmanager, unique_name):
    secret_name = f"pipeline/db-creds-{unique_name}"
    creds = {
        "host": "timescaledb.internal",
        "port": 5432,
        "username": "pipeline_writer",
        "password": "s3cur3-p@ssw0rd!",
        "database": "sensor_data",
    }
    secretsmanager.create_secret(Name=secret_name, SecretString=json.dumps(creds))
    yield {"name": secret_name, "creds": creds}
    secretsmanager.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)


@pytest.fixture
def metrics_namespace(unique_name):
    return f"IoTPipeline/{unique_name}"


@pytest.fixture
def audit_log(logs, unique_name):
    group_name = f"/pipeline/audit-{unique_name}"
    stream_name = "main"
    logs.create_log_group(logGroupName=group_name)
    logs.create_log_stream(logGroupName=group_name, logStreamName=stream_name)
    yield {"group": group_name, "stream": stream_name}
    logs.delete_log_group(logGroupName=group_name)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_reading(sensor_id: str, sensor_type: str, value: float, *, seq: int | None = None) -> dict:
    """Create a sensor reading with a guaranteed-unique timestamp.

    Uses a module-level counter to ensure no two readings share a timestamp,
    avoiding DynamoDB PK+SK collisions in tight loops.
    """
    global _ts_counter  # noqa: PLW0603
    if seq is not None:
        ts = f"2026-03-08T10:{seq:02d}:{0:02d}Z"
    else:
        _ts_counter += 1
        ts = f"2026-03-08T10:00:{_ts_counter:05d}Z"
    return {
        "sensor_id": sensor_id,
        "sensor_type": sensor_type,
        "timestamp": ts,
        "value": value,
        "unit": {"temperature": "celsius", "pressure": "psi", "humidity": "percent"}.get(
            sensor_type, "unknown"
        ),
    }


def get_all_kinesis_records(kinesis, stream_name: str) -> list[dict]:
    """Read all available records from a single-shard Kinesis stream."""
    desc = kinesis.describe_stream(StreamName=stream_name)
    shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]
    iterator = kinesis.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON",
    )["ShardIterator"]
    records = []
    # May need multiple reads
    for _ in range(5):
        resp = kinesis.get_records(ShardIterator=iterator, Limit=100)
        records.extend(resp["Records"])
        iterator = resp["NextShardIterator"]
        if not resp["Records"]:
            break
    return records


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPipelineIngestion:
    """Kinesis ingest + S3 raw storage."""

    def test_ingest_single_reading(self, kinesis, sensor_stream):
        """Put a single sensor reading to Kinesis and read it back."""
        reading = make_reading("sensor-001", "temperature", 72.5)
        kinesis.put_record(
            StreamName=sensor_stream,
            Data=json.dumps(reading).encode(),
            PartitionKey=reading["sensor_id"],
        )

        records = get_all_kinesis_records(kinesis, sensor_stream)
        assert len(records) >= 1
        decoded = json.loads(records[0]["Data"])
        assert decoded["sensor_id"] == "sensor-001"
        assert decoded["value"] == 72.5
        assert decoded["sensor_type"] == "temperature"

    def test_ingest_batch_readings(self, kinesis, sensor_stream):
        """PutRecords with 10 readings from 3 sensors, verify all arrive."""
        sensors = ["sensor-A", "sensor-B", "sensor-C"]
        readings = []
        for i in range(10):
            sid = sensors[i % 3]
            readings.append(make_reading(sid, "temperature", 60.0 + i))

        records_input = [
            {
                "Data": json.dumps(r).encode(),
                "PartitionKey": r["sensor_id"],
            }
            for r in readings
        ]
        resp = kinesis.put_records(StreamName=sensor_stream, Records=records_input)
        assert resp["FailedRecordCount"] == 0

        fetched = get_all_kinesis_records(kinesis, sensor_stream)
        assert len(fetched) == 10
        values = sorted(json.loads(r["Data"])["value"] for r in fetched)
        assert values == sorted(60.0 + i for i in range(10))

    def test_store_raw_batch_to_s3(self, s3, raw_data_bucket):
        """Upload a JSONL batch file to S3 and verify content integrity."""
        readings = [make_reading(f"s-{i}", "pressure", 14.7 + i * 0.1) for i in range(5)]
        body = "\n".join(json.dumps(r) for r in readings).encode()
        content_hash = hashlib.sha256(body).hexdigest()

        key = "raw/2026/03/08/batch-001.jsonl"
        s3.put_object(Bucket=raw_data_bucket, Key=key, Body=body)

        obj = s3.get_object(Bucket=raw_data_bucket, Key=key)
        downloaded = obj["Body"].read()
        assert hashlib.sha256(downloaded).hexdigest() == content_hash
        lines = downloaded.decode().strip().split("\n")
        assert len(lines) == 5

    def test_s3_partitioned_storage(self, s3, raw_data_bucket):
        """Upload to date-partitioned keys and verify prefix listing."""
        partitions = {
            "raw/2026/03/06/batch-001.jsonl": b"day1-batch1",
            "raw/2026/03/06/batch-002.jsonl": b"day1-batch2",
            "raw/2026/03/07/batch-001.jsonl": b"day2-batch1",
            "raw/2026/03/08/batch-001.jsonl": b"day3-batch1",
        }
        for key, body in partitions.items():
            s3.put_object(Bucket=raw_data_bucket, Key=key, Body=body)

        # List just March 6th
        resp = s3.list_objects_v2(Bucket=raw_data_bucket, Prefix="raw/2026/03/06/")
        assert resp["KeyCount"] == 2
        keys = {o["Key"] for o in resp["Contents"]}
        assert "raw/2026/03/06/batch-001.jsonl" in keys
        assert "raw/2026/03/06/batch-002.jsonl" in keys

        # List all of March
        resp = s3.list_objects_v2(Bucket=raw_data_bucket, Prefix="raw/2026/03/")
        assert resp["KeyCount"] == 4


class TestPipelineIndexing:
    """DynamoDB indexing with GSI queries."""

    def test_index_reading(self, dynamodb, readings_table):
        """PutItem a reading, GetItem by PK+SK, verify all attributes."""
        ts = "2026-03-08T10:00:00Z"
        dynamodb.put_item(
            TableName=readings_table,
            Item={
                "sensor_id": {"S": "sensor-001"},
                "timestamp": {"S": ts},
                "sensor_type": {"S": "temperature"},
                "value": {"N": "72.5"},
                "unit": {"S": "celsius"},
            },
        )

        resp = dynamodb.get_item(
            TableName=readings_table,
            Key={"sensor_id": {"S": "sensor-001"}, "timestamp": {"S": ts}},
        )
        item = resp["Item"]
        assert item["sensor_type"]["S"] == "temperature"
        assert item["value"]["N"] == "72.5"
        assert item["unit"]["S"] == "celsius"

    def test_query_by_sensor(self, dynamodb, readings_table):
        """Insert readings for two sensors, query by PK, verify count."""
        for i in range(5):
            dynamodb.put_item(
                TableName=readings_table,
                Item={
                    "sensor_id": {"S": "sensor-A"},
                    "timestamp": {"S": f"2026-03-08T10:{i:02d}:00Z"},
                    "sensor_type": {"S": "temperature"},
                    "value": {"N": str(70 + i)},
                },
            )
        for i in range(3):
            dynamodb.put_item(
                TableName=readings_table,
                Item={
                    "sensor_id": {"S": "sensor-B"},
                    "timestamp": {"S": f"2026-03-08T10:{i:02d}:00Z"},
                    "sensor_type": {"S": "pressure"},
                    "value": {"N": str(14.7 + i)},
                },
            )

        resp = dynamodb.query(
            TableName=readings_table,
            KeyConditionExpression="sensor_id = :sid",
            ExpressionAttributeValues={":sid": {"S": "sensor-A"}},
        )
        assert resp["Count"] == 5

    def test_query_by_sensor_type_gsi(self, dynamodb, readings_table):
        """Insert temp + pressure readings, query GSI by sensor_type."""
        items = [
            ("s1", "temperature", "72.0"),
            ("s2", "temperature", "73.0"),
            ("s3", "pressure", "14.7"),
            ("s4", "temperature", "71.5"),
            ("s5", "pressure", "15.0"),
        ]
        for i, (sid, stype, val) in enumerate(items):
            dynamodb.put_item(
                TableName=readings_table,
                Item={
                    "sensor_id": {"S": sid},
                    "timestamp": {"S": f"2026-03-08T10:00:{i:02d}Z"},
                    "sensor_type": {"S": stype},
                    "value": {"N": val},
                },
            )

        resp = dynamodb.query(
            TableName=readings_table,
            IndexName="by-sensor-type",
            KeyConditionExpression="sensor_type = :st",
            ExpressionAttributeValues={":st": {"S": "temperature"}},
        )
        assert resp["Count"] == 3
        sensor_ids = {item["sensor_id"]["S"] for item in resp["Items"]}
        assert sensor_ids == {"s1", "s2", "s4"}

    def test_batch_write_readings(self, dynamodb, readings_table):
        """BatchWriteItem 15 readings, Scan to verify count."""
        items = []
        for i in range(15):
            items.append(
                {
                    "PutRequest": {
                        "Item": {
                            "sensor_id": {"S": f"batch-sensor-{i % 5}"},
                            "timestamp": {"S": f"2026-03-08T{10 + i // 5}:{i % 5 * 10:02d}:00Z"},
                            "sensor_type": {"S": "humidity"},
                            "value": {"N": str(40 + i)},
                        }
                    }
                }
            )

        # BatchWriteItem max 25 items, we have 15
        dynamodb.batch_write_item(RequestItems={readings_table: items})

        resp = dynamodb.scan(TableName=readings_table, Select="COUNT")
        assert resp["Count"] == 15

    def test_update_latest_reading(self, dynamodb, readings_table):
        """PutItem then UpdateItem with expression, verify update."""
        ts = "2026-03-08T12:00:00Z"
        dynamodb.put_item(
            TableName=readings_table,
            Item={
                "sensor_id": {"S": "sensor-X"},
                "timestamp": {"S": ts},
                "value": {"N": "50.0"},
                "sensor_type": {"S": "temperature"},
                "status": {"S": "raw"},
            },
        )

        dynamodb.update_item(
            TableName=readings_table,
            Key={"sensor_id": {"S": "sensor-X"}, "timestamp": {"S": ts}},
            UpdateExpression="SET #s = :st, processed_at = :pa",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":st": {"S": "processed"},
                ":pa": {"S": "2026-03-08T12:01:00Z"},
            },
        )

        resp = dynamodb.get_item(
            TableName=readings_table,
            Key={"sensor_id": {"S": "sensor-X"}, "timestamp": {"S": ts}},
        )
        assert resp["Item"]["status"]["S"] == "processed"
        assert resp["Item"]["processed_at"]["S"] == "2026-03-08T12:01:00Z"


class TestPipelineConfiguration:
    """SSM Parameter Store + Secrets Manager configuration."""

    def test_config_hierarchy(self, ssm, pipeline_config):
        """GetParametersByPath retrieves all params under the pipeline prefix."""
        resp = ssm.get_parameters_by_path(Path=pipeline_config["prefix"], Recursive=True)
        params = {p["Name"]: p["Value"] for p in resp["Parameters"]}
        assert len(params) == 3
        prefix = pipeline_config["prefix"]
        assert params[f"{prefix}/batch_size"] == "100"
        assert params[f"{prefix}/flush_interval_seconds"] == "30"
        assert params[f"{prefix}/alert_threshold_celsius"] == "85.0"

    def test_secure_credentials(self, secretsmanager, pipeline_secret):
        """Create JSON secret, retrieve, update password, verify new value."""
        secret_name = pipeline_secret["name"]

        # Read initial
        resp = secretsmanager.get_secret_value(SecretId=secret_name)
        creds = json.loads(resp["SecretString"])
        assert creds["username"] == "pipeline_writer"
        assert creds["password"] == "s3cur3-p@ssw0rd!"

        # Update password
        creds["password"] = "n3w-s3cur3-p@ss!"
        secretsmanager.update_secret(SecretId=secret_name, SecretString=json.dumps(creds))

        # Verify update
        resp = secretsmanager.get_secret_value(SecretId=secret_name)
        updated = json.loads(resp["SecretString"])
        assert updated["password"] == "n3w-s3cur3-p@ss!"
        assert updated["host"] == "timescaledb.internal"

    def test_config_versioning(self, ssm, unique_name):
        """Put a param, overwrite 3 times, verify history has 4 versions."""
        param_name = f"/pipeline/{unique_name}/versioned-setting"
        for i in range(4):
            ssm.put_parameter(
                Name=param_name,
                Value=f"value-v{i}",
                Type="String",
                Overwrite=True,
            )

        resp = ssm.get_parameter_history(Name=param_name)
        assert len(resp["Parameters"]) == 4
        versions = [p["Version"] for p in resp["Parameters"]]
        assert versions == [1, 2, 3, 4]

        # Cleanup
        ssm.delete_parameter(Name=param_name)

    def test_tagged_resources(self, ssm, secretsmanager, unique_name):
        """Create param + secret with tags, verify tags via API."""
        param_name = f"/pipeline/{unique_name}/tagged-param"
        ssm.put_parameter(Name=param_name, Value="tagged", Type="String")
        ssm.add_tags_to_resource(
            ResourceType="Parameter",
            ResourceId=param_name,
            Tags=[
                {"Key": "Environment", "Value": "production"},
                {"Key": "Team", "Value": "data-engineering"},
            ],
        )

        tags_resp = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=param_name)
        tag_map = {t["Key"]: t["Value"] for t in tags_resp["TagList"]}
        assert tag_map["Environment"] == "production"
        assert tag_map["Team"] == "data-engineering"

        # Secret with tags
        secret_name = f"pipeline/tagged-secret-{unique_name}"
        secretsmanager.create_secret(
            Name=secret_name,
            SecretString="tagged-value",
            Tags=[
                {"Key": "Environment", "Value": "staging"},
                {"Key": "Service", "Value": "iot-pipeline"},
            ],
        )
        desc = secretsmanager.describe_secret(SecretId=secret_name)
        secret_tags = {t["Key"]: t["Value"] for t in desc["Tags"]}
        assert secret_tags["Environment"] == "staging"
        assert secret_tags["Service"] == "iot-pipeline"

        # Cleanup
        ssm.delete_parameter(Name=param_name)
        secretsmanager.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)


class TestPipelineMonitoring:
    """CloudWatch metrics + CloudWatch Logs audit trail."""

    def test_publish_metrics(self, cloudwatch, metrics_namespace):
        """PutMetricData then GetMetricStatistics, verify Sum."""
        cloudwatch.put_metric_data(
            Namespace=metrics_namespace,
            MetricData=[
                {"MetricName": "ReadingsProcessed", "Value": 150, "Unit": "Count"},
                {"MetricName": "ReadingsProcessed", "Value": 200, "Unit": "Count"},
                {"MetricName": "ErrorCount", "Value": 3, "Unit": "Count"},
            ],
        )

        resp = cloudwatch.get_metric_statistics(
            Namespace=metrics_namespace,
            MetricName="ReadingsProcessed",
            StartTime=datetime(2020, 1, 1, tzinfo=UTC),
            EndTime=datetime(2030, 1, 1, tzinfo=UTC),
            Period=86400,
            Statistics=["Sum"],
        )
        assert len(resp["Datapoints"]) >= 1
        total = sum(dp["Sum"] for dp in resp["Datapoints"])
        assert total == 350.0

    def test_metric_alarm(self, cloudwatch, metrics_namespace):
        """PutMetricAlarm, SetAlarmState, verify state change."""
        alarm_name = f"high-error-rate-{uuid.uuid4().hex[:8]}"
        cloudwatch.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace=metrics_namespace,
            MetricName="ErrorCount",
            Statistic="Sum",
            Period=300,
            EvaluationPeriods=1,
            Threshold=100,
            ComparisonOperator="GreaterThanThreshold",
        )

        # Force alarm state
        cloudwatch.set_alarm_state(
            AlarmName=alarm_name,
            StateValue="ALARM",
            StateReason="Testing: error count exceeded threshold",
        )

        resp = cloudwatch.describe_alarms(AlarmNames=[alarm_name])
        alarms = resp["MetricAlarms"]
        assert len(alarms) == 1
        assert alarms[0]["StateValue"] == "ALARM"
        assert alarms[0]["Threshold"] == 100.0

        # Cleanup
        cloudwatch.delete_alarms(AlarmNames=[alarm_name])

    def test_audit_log_entries(self, logs, audit_log):
        """PutLogEvents, GetLogEvents, verify all events returned."""
        events = [
            {"timestamp": int(time.time() * 1000) + i, "message": f"Reading batch {i} processed"}
            for i in range(5)
        ]

        logs.put_log_events(
            logGroupName=audit_log["group"],
            logStreamName=audit_log["stream"],
            logEvents=events,
        )

        resp = logs.get_log_events(
            logGroupName=audit_log["group"],
            logStreamName=audit_log["stream"],
            startFromHead=True,
        )
        messages = [e["message"] for e in resp["events"]]
        assert len(messages) == 5
        assert "Reading batch 0 processed" in messages
        assert "Reading batch 4 processed" in messages

    def test_filter_audit_logs(self, logs, audit_log):
        """Put mixed-severity events, FilterLogEvents with pattern."""
        now_ms = int(time.time() * 1000)
        events = [
            {"timestamp": now_ms + 0, "message": "INFO: Batch 1 processed successfully"},
            {"timestamp": now_ms + 1, "message": "ERROR: Sensor s-42 timeout"},
            {"timestamp": now_ms + 2, "message": "INFO: Batch 2 processed successfully"},
            {"timestamp": now_ms + 3, "message": "ERROR: DynamoDB write throttled"},
            {"timestamp": now_ms + 4, "message": "WARN: High latency detected"},
        ]

        logs.put_log_events(
            logGroupName=audit_log["group"],
            logStreamName=audit_log["stream"],
            logEvents=events,
        )

        resp = logs.filter_log_events(
            logGroupName=audit_log["group"],
            filterPattern="ERROR",
        )
        error_messages = [e["message"] for e in resp["events"]]
        assert len(error_messages) == 2
        assert any("timeout" in m for m in error_messages)
        assert any("throttled" in m for m in error_messages)


class TestPipelineEndToEnd:
    """Full data flow through all 7 services."""

    def test_full_ingestion_cycle(
        self,
        kinesis,
        s3,
        dynamodb,
        ssm,
        secretsmanager,
        cloudwatch,
        logs,
        sensor_stream,
        raw_data_bucket,
        readings_table,
        pipeline_config,
        pipeline_secret,
        metrics_namespace,
        audit_log,
    ):
        """
        End-to-end: read config → read creds → ingest to Kinesis → read back →
        archive to S3 → index in DynamoDB → query → publish metric → audit log.
        """
        # Step 1: Read pipeline configuration from SSM
        config_resp = ssm.get_parameters_by_path(Path=pipeline_config["prefix"], Recursive=True)
        config = {p["Name"].split("/")[-1]: p["Value"] for p in config_resp["Parameters"]}
        assert "batch_size" in config
        assert config["batch_size"] == "100"

        # Step 2: Read database credentials from Secrets Manager
        creds_resp = secretsmanager.get_secret_value(SecretId=pipeline_secret["name"])
        db_creds = json.loads(creds_resp["SecretString"])
        assert db_creds["host"] == "timescaledb.internal"

        # Step 3: Ingest 5 sensor readings to Kinesis
        readings = [
            make_reading("factory-sensor-1", "temperature", 71.2 + i, seq=i) for i in range(5)
        ]
        records_input = [
            {"Data": json.dumps(r).encode(), "PartitionKey": r["sensor_id"]} for r in readings
        ]
        put_resp = kinesis.put_records(StreamName=sensor_stream, Records=records_input)
        assert put_resp["FailedRecordCount"] == 0

        # Step 4: Read back from Kinesis
        fetched = get_all_kinesis_records(kinesis, sensor_stream)
        assert len(fetched) == 5
        decoded_readings = [json.loads(r["Data"]) for r in fetched]

        # Step 5: Archive raw batch to S3
        batch_body = "\n".join(json.dumps(r) for r in decoded_readings).encode()
        batch_key = "raw/2026/03/08/e2e-batch.jsonl"
        s3.put_object(Bucket=raw_data_bucket, Key=batch_key, Body=batch_body)

        obj = s3.get_object(Bucket=raw_data_bucket, Key=batch_key)
        assert len(obj["Body"].read().decode().strip().split("\n")) == 5

        # Step 6: Index readings in DynamoDB
        for reading in decoded_readings:
            dynamodb.put_item(
                TableName=readings_table,
                Item={
                    "sensor_id": {"S": reading["sensor_id"]},
                    "timestamp": {"S": reading["timestamp"]},
                    "sensor_type": {"S": reading["sensor_type"]},
                    "value": {"N": str(reading["value"])},
                    "unit": {"S": reading["unit"]},
                },
            )

        # Step 7: Query DynamoDB to verify
        query_resp = dynamodb.query(
            TableName=readings_table,
            KeyConditionExpression="sensor_id = :sid",
            ExpressionAttributeValues={":sid": {"S": "factory-sensor-1"}},
        )
        assert query_resp["Count"] == 5

        # Step 8: Publish throughput metric to CloudWatch
        cloudwatch.put_metric_data(
            Namespace=metrics_namespace,
            MetricData=[
                {
                    "MetricName": "ReadingsProcessed",
                    "Value": len(decoded_readings),
                    "Unit": "Count",
                }
            ],
        )

        metric_resp = cloudwatch.get_metric_statistics(
            Namespace=metrics_namespace,
            MetricName="ReadingsProcessed",
            StartTime=datetime(2020, 1, 1, tzinfo=UTC),
            EndTime=datetime(2030, 1, 1, tzinfo=UTC),
            Period=86400,
            Statistics=["Sum"],
        )
        assert len(metric_resp["Datapoints"]) >= 1

        # Step 9: Write audit log
        logs.put_log_events(
            logGroupName=audit_log["group"],
            logStreamName=audit_log["stream"],
            logEvents=[
                {
                    "timestamp": int(time.time() * 1000),
                    "message": json.dumps(
                        {
                            "event": "batch_processed",
                            "batch_key": batch_key,
                            "record_count": 5,
                            "source_stream": sensor_stream,
                        }
                    ),
                }
            ],
        )

        log_resp = logs.get_log_events(
            logGroupName=audit_log["group"],
            logStreamName=audit_log["stream"],
            startFromHead=True,
        )
        assert len(log_resp["events"]) >= 1
        audit_entry = json.loads(log_resp["events"][0]["message"])
        assert audit_entry["event"] == "batch_processed"
        assert audit_entry["record_count"] == 5
