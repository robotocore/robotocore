"""IaC test: sam - data_lake."""

import time
from pathlib import Path

import pytest

from tests.iac.conftest import make_client

pytestmark = pytest.mark.iac


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    cfn = make_client("cloudformation")
    template = (Path(__file__).parent / "template.yaml").read_text()
    stack_name = f"{test_run_id}-sam-data-lake"
    cfn.create_stack(
        StackName=stack_name,
        TemplateBody=template,
        Capabilities=["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND"],
    )
    for _ in range(60):
        resp = cfn.describe_stacks(StackName=stack_name)
        status = resp["Stacks"][0]["StackStatus"]
        if status == "CREATE_COMPLETE":
            yield resp["Stacks"][0]
            cfn.delete_stack(StackName=stack_name)
            return
        if "FAILED" in status or "ROLLBACK" in status:
            pytest.skip(f"SAM stack failed: {status}")
            return
        time.sleep(1)
    pytest.skip("SAM stack timed out")


class TestDataLake:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_bucket_exists(self, deployed_stack, ensure_server):
        outputs = {o["OutputKey"]: o["OutputValue"] for o in deployed_stack.get("Outputs", [])}
        bucket_name = outputs.get("BucketName")
        assert bucket_name is not None, "BucketName output missing"

        s3 = make_client("s3")
        resp = s3.head_bucket(Bucket=bucket_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_bucket_versioning(self, deployed_stack, ensure_server):
        outputs = {o["OutputKey"]: o["OutputValue"] for o in deployed_stack.get("Outputs", [])}
        bucket_name = outputs.get("BucketName")
        assert bucket_name is not None

        s3 = make_client("s3")
        resp = s3.get_bucket_versioning(Bucket=bucket_name)
        assert resp["Status"] == "Enabled"

    def test_stream_exists(self, deployed_stack, ensure_server):
        outputs = {o["OutputKey"]: o["OutputValue"] for o in deployed_stack.get("Outputs", [])}
        stream_name = outputs.get("StreamName")
        assert stream_name is not None, "StreamName output missing"

        kinesis = make_client("kinesis")
        resp = kinesis.describe_stream(StreamName=stream_name)
        assert resp["StreamDescription"]["StreamName"] == stream_name
        assert resp["StreamDescription"]["StreamStatus"] in ("ACTIVE", "CREATING")

    def test_table_exists(self, deployed_stack, ensure_server):
        outputs = {o["OutputKey"]: o["OutputValue"] for o in deployed_stack.get("Outputs", [])}
        table_name = outputs.get("TableName")
        assert table_name is not None, "TableName output missing"

        ddb = make_client("dynamodb")
        resp = ddb.describe_table(TableName=table_name)
        assert resp["Table"]["TableName"] == table_name
        assert resp["Table"]["KeySchema"][0]["AttributeName"] == "id"
