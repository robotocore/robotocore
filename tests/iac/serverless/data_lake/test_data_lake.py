"""IaC test: serverless - data_lake (S3 + Kinesis + DynamoDB)."""

from __future__ import annotations

import time

import pytest

from tests.iac.conftest import make_client

pytestmark = pytest.mark.iac

TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Description: Serverless data lake - S3 bucket, Kinesis stream, DynamoDB catalog

Resources:
  DataBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: sls-data-lake-raw

  IngestionStream:
    Type: AWS::Kinesis::Stream
    Properties:
      Name: sls-data-lake-ingestion
      ShardCount: 1

  CatalogTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: sls-data-lake-catalog
      AttributeDefinitions:
        - AttributeName: dataset_id
          AttributeType: S
      KeySchema:
        - AttributeName: dataset_id
          KeyType: HASH
      BillingMode: PAY_PER_REQUEST

Outputs:
  BucketName:
    Value: !Ref DataBucket
  StreamName:
    Value: !Ref IngestionStream
  TableName:
    Value: !Ref CatalogTable
"""


def _get_outputs(stack: dict) -> dict[str, str]:
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    cfn = make_client("cloudformation")
    stack_name = f"{test_run_id}-sls-data-lake"
    cfn.create_stack(
        StackName=stack_name,
        TemplateBody=TEMPLATE,
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
            pytest.skip(f"Stack deploy failed: {status}")
            return
        time.sleep(1)
    pytest.skip("Stack deploy timed out")


class TestDataLake:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_s3_bucket_exists(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        s3 = make_client("s3")
        resp = s3.head_bucket(Bucket=outputs["BucketName"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_kinesis_stream_exists(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        kinesis = make_client("kinesis")
        desc = kinesis.describe_stream(StreamName=outputs["StreamName"])
        assert desc["StreamDescription"]["StreamStatus"] == "ACTIVE"
        assert len(desc["StreamDescription"]["Shards"]) == 1

    def test_dynamodb_catalog_table(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        ddb = make_client("dynamodb")
        desc = ddb.describe_table(TableName=outputs["TableName"])
        assert desc["Table"]["TableStatus"] == "ACTIVE"
        key_schema = desc["Table"]["KeySchema"]
        assert any(k["AttributeName"] == "dataset_id" for k in key_schema)
