"""IaC test: pulumi - data_lake."""

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
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

SCENARIO_DIR = Path(__file__).parent


class TestDataLake:
    """Validate Pulumi-provisioned data lake resources."""

    def test_deploy_creates_resources(self, pulumi_runner):
        """Deploy the stack and verify all three resources exist."""
        result = pulumi_runner.up(SCENARIO_DIR)
        assert result.returncode == 0, f"pulumi up failed:\n{result.stderr}"

        outputs = pulumi_runner.stack_output(SCENARIO_DIR)

        s3 = make_client("s3")
        kinesis = make_client("kinesis")
        dynamodb = make_client("dynamodb")

        assert_s3_bucket_exists(s3, outputs["bucket_name"])
        assert_kinesis_stream_exists(kinesis, outputs["stream_name"])
        assert_dynamodb_table_exists(dynamodb, outputs["table_name"])

        # Cleanup
        pulumi_runner.destroy(SCENARIO_DIR)

    def test_kinesis_stream_active(self, pulumi_runner):
        """Kinesis ingest stream is ACTIVE with 1 shard."""
        result = pulumi_runner.up(SCENARIO_DIR)
        assert result.returncode == 0, f"pulumi up failed:\n{result.stderr}"

        outputs = pulumi_runner.stack_output(SCENARIO_DIR)
        stream_name = outputs["stream_name"]

        kinesis = make_client("kinesis")
        desc = assert_kinesis_stream_exists(kinesis, stream_name, expected_status="ACTIVE")
        assert len(desc["Shards"]) == 1, "Expected 1 shard"

        # Cleanup
        pulumi_runner.destroy(SCENARIO_DIR)

    def test_dynamodb_schema(self, pulumi_runner):
        """DynamoDB table has correct hash and range key schema."""
        result = pulumi_runner.up(SCENARIO_DIR)
        assert result.returncode == 0, f"pulumi up failed:\n{result.stderr}"

        outputs = pulumi_runner.stack_output(SCENARIO_DIR)
        table_name = outputs["table_name"]

        dynamodb = make_client("dynamodb")
        resp = dynamodb.describe_table(TableName=table_name)
        key_schema = resp["Table"]["KeySchema"]

        hash_keys = [k for k in key_schema if k["KeyType"] == "HASH"]
        range_keys = [k for k in key_schema if k["KeyType"] == "RANGE"]

        assert len(hash_keys) == 1, "Expected exactly one HASH key"
        assert hash_keys[0]["AttributeName"] == "dataset_id"
        assert len(range_keys) == 1, "Expected exactly one RANGE key"
        assert range_keys[0]["AttributeName"] == "timestamp"

        # Cleanup
        pulumi_runner.destroy(SCENARIO_DIR)

    def test_s3_data_roundtrip(self, pulumi_runner):
        """Upload and download data from the landing zone bucket."""
        result = pulumi_runner.up(SCENARIO_DIR)
        assert result.returncode == 0, f"pulumi up failed:\n{result.stderr}"

        outputs = pulumi_runner.stack_output(SCENARIO_DIR)
        bucket = outputs["bucket_name"]

        s3 = make_client("s3")
        put_and_get_s3_object(s3, bucket, "data/test.csv", "id,name\n1,test")

        # Cleanup
        pulumi_runner.destroy(SCENARIO_DIR)

    def test_kinesis_data_roundtrip(self, pulumi_runner):
        """Put and read a record from the Kinesis ingest stream."""
        result = pulumi_runner.up(SCENARIO_DIR)
        assert result.returncode == 0, f"pulumi up failed:\n{result.stderr}"

        outputs = pulumi_runner.stack_output(SCENARIO_DIR)
        stream = outputs["stream_name"]

        kinesis = make_client("kinesis")
        put_and_read_kinesis_record(kinesis, stream, "test-data", "pk1")

        # Cleanup
        pulumi_runner.destroy(SCENARIO_DIR)

    def test_dynamodb_data_roundtrip(self, pulumi_runner):
        """Put and get an item from the DynamoDB catalog table."""
        result = pulumi_runner.up(SCENARIO_DIR)
        assert result.returncode == 0, f"pulumi up failed:\n{result.stderr}"

        outputs = pulumi_runner.stack_output(SCENARIO_DIR)
        table = outputs["table_name"]

        ddb = make_client("dynamodb")
        put_and_get_dynamodb_item(
            ddb,
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

        # Cleanup
        pulumi_runner.destroy(SCENARIO_DIR)
