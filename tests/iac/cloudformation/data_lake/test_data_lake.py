"""IaC test: CloudFormation data lake with S3, Kinesis, and DynamoDB."""

from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from tests.iac.helpers.resource_validator import (
    assert_dynamodb_table_exists,
    assert_kinesis_stream_exists,
    assert_s3_bucket_exists,
)

pytestmark = pytest.mark.iac

TEMPLATE = (Path(__file__).parent / "template.yaml").read_text()


class TestDataLake:
    """Deploy a data lake stack and validate all three resources."""

    def test_deploy_and_validate(self, deploy_stack, s3, kinesis, dynamodb, test_run_id):
        """Deploy stack, validate S3 bucket, Kinesis stream, and DDB table."""
        stack = deploy_stack("data-lake", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        bucket_name = f"{test_run_id}-data-lake-landing"
        stream_name = f"{test_run_id}-data-lake-ingest"
        table_name = f"{test_run_id}-data-lake-catalog"

        # S3 bucket exists
        assert_s3_bucket_exists(s3, bucket_name)

        # Kinesis stream is ACTIVE
        stream_desc = assert_kinesis_stream_exists(kinesis, stream_name, "ACTIVE")
        assert stream_desc["Shards"] is not None
        assert len(stream_desc["Shards"]) == 1

        # DynamoDB table exists with correct schema
        table = assert_dynamodb_table_exists(dynamodb, table_name, "ACTIVE")
        key_schema = {ks["AttributeName"]: ks["KeyType"] for ks in table["KeySchema"]}
        assert key_schema == {"dataset_id": "HASH", "timestamp": "RANGE"}

    def test_cleanup_removes_resources(self, cfn_runner, s3, kinesis, dynamodb, test_run_id):
        """Deploy and delete stack, then verify all resources are gone."""
        stack_name = f"{test_run_id}-data-lake-cleanup"
        bucket_name = f"{stack_name}-landing"
        stream_name = f"{stack_name}-ingest"
        table_name = f"{stack_name}-catalog"

        cfn_runner.deploy_stack(stack_name, TEMPLATE)
        cfn_runner.delete_stack(stack_name)

        # S3 bucket gone
        with pytest.raises(ClientError) as exc_info:
            s3.head_bucket(Bucket=bucket_name)
        assert exc_info.value.response["Error"]["Code"] in ("404", "NoSuchBucket")

        # Kinesis stream gone
        with pytest.raises(ClientError) as exc_info:
            kinesis.describe_stream(StreamName=stream_name)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

        # DynamoDB table gone
        with pytest.raises(ClientError) as exc_info:
            dynamodb.describe_table(TableName=table_name)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
