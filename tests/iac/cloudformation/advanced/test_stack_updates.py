"""Tests for CloudFormation stack update operations."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from tests.iac.conftest import make_client
from tests.iac.helpers.tool_runner import CloudFormationRunner

pytestmark = pytest.mark.iac

TEMPLATE_V1 = """\
AWSTemplateFormatVersion: "2010-09-09"
Resources:
  Bucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${AWS::StackName}-bucket"
"""

TEMPLATE_V2_TAG = """\
AWSTemplateFormatVersion: "2010-09-09"
Resources:
  Bucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${AWS::StackName}-bucket"
      Tags:
        - Key: Environment
          Value: staging
"""

TEMPLATE_V3_TWO_BUCKETS = """\
AWSTemplateFormatVersion: "2010-09-09"
Resources:
  Bucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${AWS::StackName}-bucket"
      Tags:
        - Key: Environment
          Value: staging
  SecondBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${AWS::StackName}-second"
"""


@pytest.fixture(scope="module")
def cfn(ensure_server):
    client = make_client("cloudformation")
    return CloudFormationRunner(client)


class TestStackUpdates:
    """Verify stack update lifecycle: update tags, add resources, no-op update."""

    def test_update_adds_tag(self, cfn, test_run_id):
        """Deploy v1, update to v2 with a tag, verify UPDATE_COMPLETE."""
        stack_name = f"{test_run_id}-upd-tag"
        try:
            cfn.deploy_stack(stack_name, TEMPLATE_V1)
            result = cfn.update_stack(stack_name, TEMPLATE_V2_TAG)
            assert result["StackStatus"] == "UPDATE_COMPLETE"

            s3 = make_client("s3")
            tagging = s3.get_bucket_tagging(Bucket=f"{stack_name}-bucket")
            tag_set = {t["Key"]: t["Value"] for t in tagging["TagSet"]}
            assert tag_set.get("Environment") == "staging"
        finally:
            try:
                cfn.delete_stack(stack_name)
            except Exception:
                pass

    def test_update_adds_second_bucket(self, cfn, test_run_id):
        """Update to add a second S3 bucket, verify both exist."""
        stack_name = f"{test_run_id}-upd-add"
        try:
            cfn.deploy_stack(stack_name, TEMPLATE_V1)
            cfn.update_stack(stack_name, TEMPLATE_V3_TWO_BUCKETS)

            s3 = make_client("s3")
            buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
            assert f"{stack_name}-bucket" in buckets
            assert f"{stack_name}-second" in buckets
        finally:
            try:
                cfn.delete_stack(stack_name)
            except Exception:
                pass

    def test_update_no_changes_raises(self, cfn, test_run_id):
        """Updating with the same template should raise 'No updates' error."""
        stack_name = f"{test_run_id}-upd-noop"
        try:
            cfn.deploy_stack(stack_name, TEMPLATE_V1)
            client = make_client("cloudformation")
            with pytest.raises(ClientError) as exc_info:
                client.update_stack(
                    StackName=stack_name,
                    TemplateBody=TEMPLATE_V1,
                    Capabilities=["CAPABILITY_IAM"],
                )
            assert "No updates" in str(exc_info.value)
        finally:
            try:
                cfn.delete_stack(stack_name)
            except Exception:
                pass
