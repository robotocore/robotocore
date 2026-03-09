"""Functional test: deploy data lake and exercise S3, Kinesis, and DynamoDB."""

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.functional_validator import (
    put_and_get_dynamodb_item,
    put_and_get_s3_object,
    put_and_read_kinesis_record,
)

pytestmark = pytest.mark.iac

TEMPLATE = (Path(__file__).parent / "template.yaml").read_text()


def _get_outputs(stack: dict) -> dict[str, str]:
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


class TestDataLakeFunctional:
    """Deploy data lake stack and verify data roundtrips across all three stores."""

    def test_s3_landing_zone_upload(self, deploy_stack):
        """Upload a data file to the S3 landing zone bucket."""
        stack = deploy_stack("dlake-func-s3", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        bucket_name = outputs["BucketName"]
        s3 = make_client("s3")

        csv_data = "id,name,value\n1,sensor-a,42.5\n2,sensor-b,17.3"
        resp = put_and_get_s3_object(s3, bucket_name, "raw/2026/03/sensors.csv", csv_data)
        assert resp["ContentLength"] == len(csv_data.encode("utf-8"))

    def test_kinesis_ingestion_stream(self, deploy_stack):
        """Put a record into the Kinesis ingestion stream and read it back."""
        stack = deploy_stack("dlake-func-kin", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        stream_name = outputs["StreamName"]
        kinesis = make_client("kinesis")

        data = '{"sensor": "temp-01", "reading": 22.5}'
        record = put_and_read_kinesis_record(kinesis, stream_name, data, "temp-01")
        assert record["Data"] == data.encode("utf-8")
        assert record["PartitionKey"] == "temp-01"

    def test_dynamodb_catalog_entry(self, deploy_stack):
        """Put and get an item in the DynamoDB catalog table."""
        stack = deploy_stack("dlake-func-ddb", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        table_name = outputs["TableName"]
        dynamodb = make_client("dynamodb")

        item = {
            "dataset_id": {"S": "ds-sensors-001"},
            "timestamp": {"S": "2026-03-08T10:00:00Z"},
            "source": {"S": "s3://landing/raw/sensors.csv"},
            "record_count": {"N": "2"},
        }
        key = {
            "dataset_id": {"S": "ds-sensors-001"},
            "timestamp": {"S": "2026-03-08T10:00:00Z"},
        }

        returned = put_and_get_dynamodb_item(dynamodb, table_name, item, key)
        assert returned["dataset_id"] == {"S": "ds-sensors-001"}
        assert returned["timestamp"] == {"S": "2026-03-08T10:00:00Z"}
        assert returned["source"] == {"S": "s3://landing/raw/sensors.csv"}
        assert returned["record_count"] == {"N": "2"}
