"""Tests for CloudFormation cross-stack references via Exports and Fn::ImportValue."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from tests.iac.conftest import make_client
from tests.iac.helpers.tool_runner import CloudFormationRunner

pytestmark = pytest.mark.iac

STACK_A_TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Resources:
  Bucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${{AWS::StackName}}-shared"
Outputs:
  BucketArn:
    Value: !GetAtt Bucket.Arn
    Export:
      Name: !Sub "${{AWS::StackName}}-BucketArn"
  BucketName:
    Value: !Ref Bucket
    Export:
      Name: !Sub "${{AWS::StackName}}-BucketName"
"""

STACK_B_TEMPLATE_FMT = """\
AWSTemplateFormatVersion: "2010-09-09"
Parameters:
  ProducerStack:
    Type: String
Resources:
  Placeholder:
    Type: AWS::CloudFormation::WaitConditionHandle
Outputs:
  ImportedArn:
    Value: !ImportValue
      Fn::Sub: "${{ProducerStack}}-BucketArn"
"""


@pytest.fixture(scope="module")
def cfn(ensure_server):
    client = make_client("cloudformation")
    return CloudFormationRunner(client)


class TestCrossStackRefs:
    """Verify cross-stack references with Exports and Fn::ImportValue."""

    def test_import_value_resolves(self, cfn, test_run_id):
        """Stack B successfully imports Stack A's exported bucket ARN."""
        stack_a = f"{test_run_id}-xref-a"
        stack_b = f"{test_run_id}-xref-b"
        try:
            cfn.deploy_stack(stack_a, STACK_A_TEMPLATE)

            outputs_a = cfn.get_stack_outputs(stack_a)
            assert "BucketArn" in outputs_a
            assert "BucketName" in outputs_a

            cfn.deploy_stack(
                stack_b,
                STACK_B_TEMPLATE_FMT,
                params={"ProducerStack": stack_a},
            )

            outputs_b = cfn.get_stack_outputs(stack_b)
            assert "ImportedArn" in outputs_b
            assert outputs_b["ImportedArn"] == outputs_a["BucketArn"]
        finally:
            for name in [stack_b, stack_a]:
                try:
                    cfn.delete_stack(name)
                except Exception:
                    pass

    def test_cannot_delete_exporting_stack(self, cfn, test_run_id):
        """Deleting Stack A while Stack B imports from it should fail."""
        stack_a = f"{test_run_id}-xref-nodelete-a"
        stack_b = f"{test_run_id}-xref-nodelete-b"
        try:
            cfn.deploy_stack(stack_a, STACK_A_TEMPLATE)
            cfn.deploy_stack(
                stack_b,
                STACK_B_TEMPLATE_FMT,
                params={"ProducerStack": stack_a},
            )

            with pytest.raises((ClientError, RuntimeError)):
                # Should fail because Stack B depends on Stack A's exports
                cfn.delete_stack(stack_a)
        finally:
            for name in [stack_b, stack_a]:
                try:
                    cfn.delete_stack(name)
                except Exception:
                    pass

    def test_delete_consumer_then_producer(self, cfn, test_run_id):
        """Deleting B first, then A should succeed."""
        stack_a = f"{test_run_id}-xref-delord-a"
        stack_b = f"{test_run_id}-xref-delord-b"
        try:
            cfn.deploy_stack(stack_a, STACK_A_TEMPLATE)
            cfn.deploy_stack(
                stack_b,
                STACK_B_TEMPLATE_FMT,
                params={"ProducerStack": stack_a},
            )

            cfn.delete_stack(stack_b)
            cfn.delete_stack(stack_a)

            # Verify both are gone
            client = make_client("cloudformation")
            with pytest.raises(ClientError) as exc_info:
                client.describe_stacks(StackName=stack_a)
            assert "does not exist" in str(exc_info.value)
        except Exception:
            # Cleanup on failure
            for name in [stack_b, stack_a]:
                try:
                    cfn.delete_stack(name)
                except Exception:
                    pass
            raise
