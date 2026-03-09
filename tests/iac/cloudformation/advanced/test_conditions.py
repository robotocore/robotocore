"""Tests for CloudFormation Conditions (conditional resource creation)."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from tests.iac.conftest import make_client
from tests.iac.helpers.tool_runner import CloudFormationRunner

pytestmark = pytest.mark.iac

TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Parameters:
  Environment:
    Type: String
    Default: dev
Conditions:
  IsProd: !Equals [!Ref Environment, prod]
Resources:
  AlwaysBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${AWS::StackName}-always"
  ProdOnlyBucket:
    Type: AWS::S3::Bucket
    Condition: IsProd
    Properties:
      BucketName: !Sub "${AWS::StackName}-prod-only"
Outputs:
  AlwaysBucketName:
    Value: !Ref AlwaysBucket
"""


@pytest.fixture(scope="module")
def cfn(ensure_server):
    client = make_client("cloudformation")
    return CloudFormationRunner(client)


def _bucket_exists(s3_client, bucket_name: str) -> bool:
    """Check whether a bucket exists."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError:
        return False


class TestConditions:
    """Verify conditional resource creation based on parameter values."""

    def test_dev_skips_prod_bucket(self, cfn, test_run_id):
        """With Environment=dev, AlwaysBucket exists but ProdOnlyBucket does not."""
        stack_name = f"{test_run_id}-cond-dev"
        try:
            stack = cfn.deploy_stack(stack_name, TEMPLATE, params={"Environment": "dev"})
            assert stack["StackStatus"] == "CREATE_COMPLETE"

            s3 = make_client("s3")
            assert _bucket_exists(s3, f"{stack_name}-always")
            assert not _bucket_exists(s3, f"{stack_name}-prod-only")
        finally:
            try:
                cfn.delete_stack(stack_name)
            except Exception:
                pass

    def test_prod_creates_both_buckets(self, cfn, test_run_id):
        """With Environment=prod, both AlwaysBucket and ProdOnlyBucket exist."""
        stack_name = f"{test_run_id}-cond-prod"
        try:
            stack = cfn.deploy_stack(stack_name, TEMPLATE, params={"Environment": "prod"})
            assert stack["StackStatus"] == "CREATE_COMPLETE"

            s3 = make_client("s3")
            assert _bucket_exists(s3, f"{stack_name}-always")
            assert _bucket_exists(s3, f"{stack_name}-prod-only")
        finally:
            try:
                cfn.delete_stack(stack_name)
            except Exception:
                pass
