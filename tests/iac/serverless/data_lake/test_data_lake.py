"""IaC test: serverless - data_lake (S3 + Kinesis + DynamoDB)."""

from __future__ import annotations

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.stack_deployer import delete_stack, deploy_and_yield, get_stack_outputs

pytestmark = pytest.mark.iac

TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Description: Serverless data lake - S3 bucket, Kinesis stream, DynamoDB catalog

Resources:
  DataBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${AWS::StackName}-raw"

  IngestionStream:
    Type: AWS::Kinesis::Stream
    Properties:
      Name: !Sub "${AWS::StackName}-ingestion"
      ShardCount: 1

  CatalogTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: !Sub "${AWS::StackName}-catalog"
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


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    stack_name = f"{test_run_id}-sls-data-lake"
    stack = deploy_and_yield(stack_name, TEMPLATE)
    yield stack
    delete_stack(stack_name)


class TestDataLake:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_s3_bucket_exists(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        s3 = make_client("s3")
        resp = s3.head_bucket(Bucket=outputs["BucketName"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_kinesis_stream_exists(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        kinesis = make_client("kinesis")
        desc = kinesis.describe_stream(StreamName=outputs["StreamName"])
        assert desc["StreamDescription"]["StreamStatus"] == "ACTIVE"
        assert len(desc["StreamDescription"]["Shards"]) == 1

    def test_dynamodb_catalog_table(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        ddb = make_client("dynamodb")
        desc = ddb.describe_table(TableName=outputs["TableName"])
        assert desc["Table"]["TableStatus"] == "ACTIVE"
        key_schema = desc["Table"]["KeySchema"]
        assert any(k["AttributeName"] == "dataset_id" for k in key_schema)
