"""IaC test: terraform - data_lake.

Validates S3 bucket, Kinesis stream, and DynamoDB table creation.
Resources are created via boto3 (mirroring the Terraform program).
"""

from __future__ import annotations

import pytest

from tests.iac.helpers.functional_validator import (
    put_and_get_dynamodb_item,
    put_and_get_s3_object,
    put_and_read_kinesis_record,
)
from tests.iac.helpers.resource_validator import (
    assert_dynamodb_table_exists,
    assert_kinesis_stream_exists,
    assert_s3_bucket_exists,
)

pytestmark = pytest.mark.iac


@pytest.fixture(scope="module")
def data_lake_resources(s3_client, kinesis_client, dynamodb_client):
    """Create S3 bucket, Kinesis stream, and DynamoDB table via boto3."""
    bucket_name = "tf-data-lake-landing"
    s3_client.create_bucket(Bucket=bucket_name)

    stream_name = "tf-ingest-stream"
    kinesis_client.create_stream(StreamName=stream_name, ShardCount=1)

    table_name = "tf-catalog"
    dynamodb_client.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "dataset_id", "AttributeType": "S"},
            {"AttributeName": "timestamp", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "dataset_id", "KeyType": "HASH"},
            {"AttributeName": "timestamp", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    yield {
        "bucket_name": bucket_name,
        "stream_name": stream_name,
        "table_name": table_name,
    }

    # Cleanup
    dynamodb_client.delete_table(TableName=table_name)
    kinesis_client.delete_stream(StreamName=stream_name)
    # Delete all objects from bucket before deleting it
    try:
        objs = s3_client.list_objects_v2(Bucket=bucket_name)
        for obj in objs.get("Contents", []):
            s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
    except Exception:
        pass  # best-effort cleanup
    s3_client.delete_bucket(Bucket=bucket_name)


class TestDataLake:
    """Validate Terraform-provisioned data lake resources."""

    def test_resources_exist(self, data_lake_resources, s3_client, kinesis_client, dynamodb_client):
        """Verify all three resources exist."""
        assert_s3_bucket_exists(s3_client, data_lake_resources["bucket_name"])
        assert_kinesis_stream_exists(kinesis_client, data_lake_resources["stream_name"])
        assert_dynamodb_table_exists(dynamodb_client, data_lake_resources["table_name"])

    def test_kinesis_stream_active(self, data_lake_resources, kinesis_client):
        """Kinesis ingest stream is ACTIVE with 1 shard."""
        stream_name = data_lake_resources["stream_name"]
        desc = assert_kinesis_stream_exists(kinesis_client, stream_name, expected_status="ACTIVE")
        assert len(desc["Shards"]) == 1, "Expected 1 shard"

    def test_dynamodb_schema(self, data_lake_resources, dynamodb_client):
        """DynamoDB table has correct hash and range key schema."""
        table_name = data_lake_resources["table_name"]
        resp = dynamodb_client.describe_table(TableName=table_name)
        key_schema = resp["Table"]["KeySchema"]

        hash_keys = [k for k in key_schema if k["KeyType"] == "HASH"]
        range_keys = [k for k in key_schema if k["KeyType"] == "RANGE"]

        assert len(hash_keys) == 1, "Expected exactly one HASH key"
        assert hash_keys[0]["AttributeName"] == "dataset_id"
        assert len(range_keys) == 1, "Expected exactly one RANGE key"
        assert range_keys[0]["AttributeName"] == "timestamp"

    def test_s3_data_roundtrip(self, data_lake_resources, s3_client):
        """Upload and download data from the landing zone bucket."""
        bucket = data_lake_resources["bucket_name"]
        put_and_get_s3_object(s3_client, bucket, "data/test.csv", "id,name\n1,test")

    def test_kinesis_data_roundtrip(self, data_lake_resources, kinesis_client):
        """Put and read a record from the Kinesis ingest stream."""
        stream = data_lake_resources["stream_name"]
        put_and_read_kinesis_record(kinesis_client, stream, "test-data", "pk1")

    def test_dynamodb_data_roundtrip(self, data_lake_resources, dynamodb_client):
        """Put and get an item from the DynamoDB catalog table."""
        table = data_lake_resources["table_name"]
        put_and_get_dynamodb_item(
            dynamodb_client,
            table,
            item={
                "dataset_id": {"S": "ds-001"},
                "timestamp": {"S": "2026-01-01T00:00:00Z"},
                "size": {"N": "1024"},
            },
            key={
                "dataset_id": {"S": "ds-001"},
                "timestamp": {"S": "2026-01-01T00:00:00Z"},
            },
        )
