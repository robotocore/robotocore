"""IaC test: cdk - data_lake.

Deploys an S3 landing zone, Kinesis ingest stream, and DynamoDB catalog
table via CDK and validates the resources with boto3.
"""

from __future__ import annotations

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


def _get_stack_outputs(stack_name: str = "DataLake") -> dict[str, str]:
    """Retrieve CloudFormation stack outputs as a dict."""
    cfn = make_client("cloudformation")
    resp = cfn.describe_stacks(StackName=stack_name)
    return {o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])}


class TestDataLake:
    """CDK data-lake scenario tests."""

    def test_deploy_creates_resources(self, cdk_runner, ensure_server):
        """Deploying the stack should create all three resources."""
        result = cdk_runner.deploy(SCENARIO_DIR, stack_name="DataLake")
        assert result.returncode == 0, f"cdk deploy failed:\n{result.stderr}"

        try:
            outputs = _get_stack_outputs()

            # Verify S3 landing zone bucket
            bucket_name = outputs.get("LandingBucketName")
            assert bucket_name, "Stack should have a LandingBucketName output"
            s3 = make_client("s3")
            assert_s3_bucket_exists(s3, bucket_name)

            # Verify Kinesis stream
            stream_name = outputs.get("IngestStreamName")
            assert stream_name, "Stack should have an IngestStreamName output"
            kin = make_client("kinesis")
            assert_kinesis_stream_exists(kin, stream_name)

            # Verify DynamoDB table
            table_name = outputs.get("CatalogTableName")
            assert table_name, "Stack should have a CatalogTableName output"
            ddb = make_client("dynamodb")
            assert_dynamodb_table_exists(ddb, table_name)
        finally:
            cdk_runner.destroy(SCENARIO_DIR, stack_name="DataLake")

    def test_kinesis_stream_active(self, cdk_runner, ensure_server):
        """The Kinesis stream should be in ACTIVE status after deploy."""
        result = cdk_runner.deploy(SCENARIO_DIR, stack_name="DataLake")
        assert result.returncode == 0, f"cdk deploy failed:\n{result.stderr}"

        try:
            outputs = _get_stack_outputs()
            stream_name = outputs.get("IngestStreamName")
            assert stream_name, "Stack should have an IngestStreamName output"

            kin = make_client("kinesis")
            desc = assert_kinesis_stream_exists(kin, stream_name, expected_status="ACTIVE")
            assert len(desc["Shards"]) == 1, "Stream should have exactly 1 shard"
        finally:
            cdk_runner.destroy(SCENARIO_DIR, stack_name="DataLake")

    def test_dynamodb_schema(self, cdk_runner, ensure_server):
        """The DynamoDB table should have the correct key schema."""
        result = cdk_runner.deploy(SCENARIO_DIR, stack_name="DataLake")
        assert result.returncode == 0, f"cdk deploy failed:\n{result.stderr}"

        try:
            outputs = _get_stack_outputs()
            table_name = outputs.get("CatalogTableName")
            assert table_name, "Stack should have a CatalogTableName output"

            ddb = make_client("dynamodb")
            table = assert_dynamodb_table_exists(ddb, table_name)

            key_schema = {ks["AttributeName"]: ks["KeyType"] for ks in table["KeySchema"]}
            assert key_schema.get("dataset_id") == "HASH", "Partition key should be 'dataset_id'"
            assert key_schema.get("timestamp") == "RANGE", "Sort key should be 'timestamp'"
        finally:
            cdk_runner.destroy(SCENARIO_DIR, stack_name="DataLake")

    def test_s3_data_roundtrip(self, cdk_runner, ensure_server):
        """Upload and download data from the landing zone bucket."""
        result = cdk_runner.deploy(SCENARIO_DIR, stack_name="DataLake")
        assert result.returncode == 0, f"cdk deploy failed:\n{result.stderr}"

        try:
            outputs = _get_stack_outputs()
            bucket_name = outputs.get("LandingBucketName")
            assert bucket_name, "Stack should have a LandingBucketName output"

            s3 = make_client("s3")
            put_and_get_s3_object(s3, bucket_name, "data/test.csv", "id,name\n1,test")
        finally:
            cdk_runner.destroy(SCENARIO_DIR, stack_name="DataLake")

    def test_kinesis_data_roundtrip(self, cdk_runner, ensure_server):
        """Put and read a record from the Kinesis ingest stream."""
        result = cdk_runner.deploy(SCENARIO_DIR, stack_name="DataLake")
        assert result.returncode == 0, f"cdk deploy failed:\n{result.stderr}"

        try:
            outputs = _get_stack_outputs()
            stream_name = outputs.get("IngestStreamName")
            assert stream_name, "Stack should have an IngestStreamName output"

            kinesis = make_client("kinesis")
            put_and_read_kinesis_record(kinesis, stream_name, "test-data", "pk1")
        finally:
            cdk_runner.destroy(SCENARIO_DIR, stack_name="DataLake")

    def test_dynamodb_data_roundtrip(self, cdk_runner, ensure_server):
        """Put and get an item from the DynamoDB catalog table."""
        result = cdk_runner.deploy(SCENARIO_DIR, stack_name="DataLake")
        assert result.returncode == 0, f"cdk deploy failed:\n{result.stderr}"

        try:
            outputs = _get_stack_outputs()
            table_name = outputs.get("CatalogTableName")
            assert table_name, "Stack should have a CatalogTableName output"

            ddb = make_client("dynamodb")
            put_and_get_dynamodb_item(
                ddb,
                table_name,
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
        finally:
            cdk_runner.destroy(SCENARIO_DIR, stack_name="DataLake")
