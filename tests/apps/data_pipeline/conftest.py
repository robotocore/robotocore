"""
Fixtures for the IoT sensor data pipeline tests.

Creates all AWS resources (Kinesis stream, S3 bucket, DynamoDB table, SSM params,
Secrets Manager secret, CloudWatch log group) and tears them down after tests.
"""

import time
import uuid

import pytest

from .app import DataPipeline
from .models import PipelineConfig, SensorReading

pytestmark = pytest.mark.apps


@pytest.fixture
def unique_suffix():
    """Short unique suffix for resource names."""
    return uuid.uuid4().hex[:8]


@pytest.fixture
def pipeline_config_obj(unique_suffix):
    """A PipelineConfig with unique resource names."""
    return PipelineConfig(
        stream_name=f"sensor-stream-{unique_suffix}",
        batch_size=5,
        flush_interval=10,
        s3_prefix="raw",
        table_name=f"sensor-readings-{unique_suffix}",
    )


@pytest.fixture
def sensor_readings():
    """Sample sensor readings for testing."""
    return [
        SensorReading(
            "sensor-001",
            "2026-03-08T10:00:00Z",
            "temperature",
            72.5,
            "celsius",
            "zone-A",
        ),
        SensorReading(
            "sensor-002",
            "2026-03-08T10:00:01Z",
            "pressure",
            14.7,
            "psi",
            "zone-A",
        ),
        SensorReading(
            "sensor-003",
            "2026-03-08T10:00:02Z",
            "humidity",
            45.2,
            "percent",
            "zone-B",
        ),
        SensorReading(
            "sensor-001",
            "2026-03-08T10:01:00Z",
            "temperature",
            73.1,
            "celsius",
            "zone-A",
        ),
        SensorReading(
            "sensor-004",
            "2026-03-08T10:01:01Z",
            "temperature",
            68.9,
            "celsius",
            "zone-C",
        ),
    ]


@pytest.fixture
def pipeline(
    kinesis,
    s3,
    dynamodb,
    ssm,
    secretsmanager,
    cloudwatch,
    logs,
    unique_suffix,
    pipeline_config_obj,
):
    """Fully provisioned DataPipeline with all AWS resources created."""
    stream_name = pipeline_config_obj.stream_name
    bucket_name = f"data-lake-{unique_suffix}"
    table_name = pipeline_config_obj.table_name
    log_group = f"/pipeline/audit-{unique_suffix}"
    log_stream = "main"
    metrics_ns = f"IoTPipeline/{unique_suffix}"

    # Create Kinesis stream
    kinesis.create_stream(StreamName=stream_name, ShardCount=1)
    for _ in range(30):
        desc = kinesis.describe_stream(StreamName=stream_name)
        if desc["StreamDescription"]["StreamStatus"] == "ACTIVE":
            break
        time.sleep(0.5)

    # Create S3 bucket
    s3.create_bucket(Bucket=bucket_name)

    # Create DynamoDB table with GSI
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

    # Create CloudWatch Logs group + stream
    logs.create_log_group(logGroupName=log_group)
    logs.create_log_stream(logGroupName=log_group, logStreamName=log_stream)

    # Store SSM config
    prefix = f"/pipeline/{unique_suffix}"
    ssm.put_parameter(Name=f"{prefix}/batch_size", Value="5", Type="String")
    ssm.put_parameter(Name=f"{prefix}/flush_interval", Value="10", Type="String")
    ssm.put_parameter(Name=f"{prefix}/s3_prefix", Value="raw", Type="String")

    pipe = DataPipeline(
        kinesis=kinesis,
        s3=s3,
        dynamodb=dynamodb,
        ssm=ssm,
        secretsmanager=secretsmanager,
        cloudwatch=cloudwatch,
        logs=logs,
        config=pipeline_config_obj,
        bucket_name=bucket_name,
        metrics_namespace=metrics_ns,
        log_group=log_group,
        log_stream=log_stream,
    )

    yield pipe

    # Cleanup
    try:
        kinesis.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)
    except Exception:
        pass
    try:
        objs = s3.list_objects_v2(Bucket=bucket_name).get("Contents", [])
        for obj in objs:
            s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
        s3.delete_bucket(Bucket=bucket_name)
    except Exception:
        pass
    try:
        dynamodb.delete_table(TableName=table_name)
    except Exception:
        pass
    try:
        logs.delete_log_group(logGroupName=log_group)
    except Exception:
        pass
    # Clean SSM params
    try:
        resp = ssm.get_parameters_by_path(Path=prefix, Recursive=True)
        for p in resp["Parameters"]:
            ssm.delete_parameter(Name=p["Name"])
    except Exception:
        pass
