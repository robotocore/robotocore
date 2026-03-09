"""Tests for CloudFormation stack rollback behavior on failure."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from tests.iac.conftest import make_client
from tests.iac.helpers.tool_runner import CloudFormationRunner

pytestmark = pytest.mark.iac

INVALID_RESOURCE_TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Resources:
  BadResource:
    Type: AWS::Fake::DoesNotExist
    Properties:
      Name: this-will-fail
"""

MIXED_TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Resources:
  GoodBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${AWS::StackName}-good"
  BadResource:
    Type: AWS::Fake::DoesNotExist
    Properties:
      Name: this-will-fail
"""


@pytest.fixture(scope="module")
def cfn(ensure_server):
    client = make_client("cloudformation")
    return CloudFormationRunner(client)


class TestRollback:
    """Verify stack rollback on creation failure."""

    def test_invalid_resource_type_causes_rollback(self, cfn, test_run_id):
        """A template with an invalid resource type should result in ROLLBACK_COMPLETE."""
        stack_name = f"{test_run_id}-rb-invalid"
        client = make_client("cloudformation")
        try:
            client.create_stack(
                StackName=stack_name,
                TemplateBody=INVALID_RESOURCE_TEMPLATE,
                Capabilities=["CAPABILITY_IAM"],
            )

            # Wait for the stack to settle into a terminal state
            import time

            deadline = time.monotonic() + 60
            status = None
            while time.monotonic() < deadline:
                try:
                    resp = client.describe_stacks(StackName=stack_name)
                    status = resp["Stacks"][0]["StackStatus"]
                    if status in ("ROLLBACK_COMPLETE", "CREATE_FAILED", "DELETE_COMPLETE"):
                        break
                except ClientError:
                    # Stack may have been deleted
                    status = "DELETE_COMPLETE"
                    break
                time.sleep(2)

            assert status in ("ROLLBACK_COMPLETE", "CREATE_FAILED"), (
                f"Expected rollback state, got {status}"
            )
        finally:
            try:
                cfn.delete_stack(stack_name)
            except Exception:
                pass

    def test_mixed_valid_invalid_resources(self, cfn, test_run_id):
        """A template mixing valid and invalid resources should fail the stack."""
        stack_name = f"{test_run_id}-rb-mixed"
        client = make_client("cloudformation")
        try:
            client.create_stack(
                StackName=stack_name,
                TemplateBody=MIXED_TEMPLATE,
                Capabilities=["CAPABILITY_IAM"],
            )

            import time

            deadline = time.monotonic() + 60
            status = None
            while time.monotonic() < deadline:
                try:
                    resp = client.describe_stacks(StackName=stack_name)
                    status = resp["Stacks"][0]["StackStatus"]
                    if status in ("ROLLBACK_COMPLETE", "CREATE_FAILED", "DELETE_COMPLETE"):
                        break
                except ClientError:
                    status = "DELETE_COMPLETE"
                    break
                time.sleep(2)

            assert status in ("ROLLBACK_COMPLETE", "CREATE_FAILED"), (
                f"Expected failure state, got {status}"
            )
        finally:
            try:
                cfn.delete_stack(stack_name)
            except Exception:
                pass
