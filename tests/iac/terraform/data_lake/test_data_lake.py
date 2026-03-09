"""IaC test: terraform - data_lake."""

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


class TestDataLake:
    """Validate Terraform-provisioned data lake resources."""

    def test_apply_succeeds(self, terraform_dir, tf_runner):
        result = tf_runner.apply(terraform_dir)
        assert result.returncode == 0, f"terraform apply failed:\n{result.stderr}"

    def test_landing_zone_bucket_exists(self, terraform_dir, tf_runner):
        """S3 landing zone bucket was created."""
        tf_runner.apply(terraform_dir)
        outputs = tf_runner.output(terraform_dir)
        bucket_name = outputs["bucket_name"]["value"]

        s3 = make_client("s3")
        assert_s3_bucket_exists(s3, bucket_name)

    def test_kinesis_stream_exists_and_active(self, terraform_dir, tf_runner):
        """Kinesis ingest stream exists and is ACTIVE."""
        tf_runner.apply(terraform_dir)
        outputs = tf_runner.output(terraform_dir)
        stream_name = outputs["stream_name"]["value"]

        kinesis = make_client("kinesis")
        desc = assert_kinesis_stream_exists(kinesis, stream_name, expected_status="ACTIVE")
        assert len(desc["Shards"]) == 1, "Expected 1 shard"

    def test_dynamodb_table_exists_and_active(self, terraform_dir, tf_runner):
        """DynamoDB catalog table exists and is ACTIVE."""
        tf_runner.apply(terraform_dir)
        outputs = tf_runner.output(terraform_dir)
        table_name = outputs["table_name"]["value"]

        dynamodb = make_client("dynamodb")
        table = assert_dynamodb_table_exists(dynamodb, table_name, expected_status="ACTIVE")
        assert table["TableName"] == table_name

    def test_dynamodb_key_schema(self, terraform_dir, tf_runner):
        """DynamoDB table has correct hash and range keys."""
        tf_runner.apply(terraform_dir)
        outputs = tf_runner.output(terraform_dir)
        table_name = outputs["table_name"]["value"]

        dynamodb = make_client("dynamodb")
        resp = dynamodb.describe_table(TableName=table_name)
        key_schema = resp["Table"]["KeySchema"]

        hash_keys = [k for k in key_schema if k["KeyType"] == "HASH"]
        range_keys = [k for k in key_schema if k["KeyType"] == "RANGE"]

        assert len(hash_keys) == 1, "Expected exactly one HASH key"
        assert hash_keys[0]["AttributeName"] == "dataset_id"
        assert len(range_keys) == 1, "Expected exactly one RANGE key"
        assert range_keys[0]["AttributeName"] == "timestamp"

    def test_all_resources_created(self, terraform_dir, tf_runner):
        """All three resources (S3, Kinesis, DynamoDB) exist after apply."""
        tf_runner.apply(terraform_dir)
        outputs = tf_runner.output(terraform_dir)

        s3 = make_client("s3")
        kinesis = make_client("kinesis")
        dynamodb = make_client("dynamodb")

        assert_s3_bucket_exists(s3, outputs["bucket_name"]["value"])
        assert_kinesis_stream_exists(kinesis, outputs["stream_name"]["value"])
        assert_dynamodb_table_exists(dynamodb, outputs["table_name"]["value"])

    def test_s3_data_roundtrip(self, terraform_dir, tf_runner):
        """Upload and download data from the landing zone bucket."""
        tf_runner.apply(terraform_dir)
        outputs = tf_runner.output(terraform_dir)
        bucket = outputs["bucket_name"]["value"]
        s3 = make_client("s3")
        put_and_get_s3_object(s3, bucket, "data/test.csv", "id,name\n1,test")

    def test_kinesis_data_roundtrip(self, terraform_dir, tf_runner):
        """Put and read a record from the Kinesis ingest stream."""
        tf_runner.apply(terraform_dir)
        outputs = tf_runner.output(terraform_dir)
        stream = outputs["stream_name"]["value"]
        kinesis = make_client("kinesis")
        put_and_read_kinesis_record(kinesis, stream, "test-data", "pk1")

    def test_dynamodb_data_roundtrip(self, terraform_dir, tf_runner):
        """Put and get an item from the DynamoDB catalog table."""
        tf_runner.apply(terraform_dir)
        outputs = tf_runner.output(terraform_dir)
        table = outputs["table_name"]["value"]
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
