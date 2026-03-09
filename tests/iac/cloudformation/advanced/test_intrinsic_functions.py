"""Tests for CloudFormation intrinsic functions (Fn::Sub, Fn::Join, Fn::Select, Fn::GetAtt)."""

from __future__ import annotations

import pytest

from tests.iac.conftest import ACCOUNT_ID, REGION, make_client
from tests.iac.helpers.tool_runner import CloudFormationRunner

pytestmark = pytest.mark.iac

TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Resources:
  Bucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${AWS::StackName}-bucket"
Outputs:
  SubResult:
    Value: !Sub "arn:aws:s3:::${AWS::StackName}-bucket"
  JoinResult:
    Value: !Join ["-", [!Ref "AWS::StackName", "joined"]]
  SelectResult:
    Value: !Select [1, ["a", "b", "c"]]
  GetAttResult:
    Value: !GetAtt Bucket.Arn
  RefResult:
    Value: !Ref Bucket
  AccountId:
    Value: !Ref "AWS::AccountId"
  RegionOutput:
    Value: !Ref "AWS::Region"
"""


@pytest.fixture(scope="module")
def cfn(ensure_server):
    client = make_client("cloudformation")
    return CloudFormationRunner(client)


class TestIntrinsicFunctions:
    """Verify that intrinsic functions resolve correctly in stack outputs."""

    def test_fn_sub_resolves_stack_name(self, cfn, test_run_id):
        """Fn::Sub should interpolate the stack name into the ARN."""
        stack_name = f"{test_run_id}-intrin"
        try:
            stack = cfn.deploy_stack(stack_name, TEMPLATE)
            assert stack["StackStatus"] == "CREATE_COMPLETE"

            outputs = cfn.get_stack_outputs(stack_name)
            assert outputs["SubResult"] == f"arn:aws:s3:::{stack_name}-bucket"
        finally:
            try:
                cfn.delete_stack(stack_name)
            except Exception:
                pass

    def test_fn_join_concatenates(self, cfn, test_run_id):
        """Fn::Join should concatenate elements with the delimiter."""
        stack_name = f"{test_run_id}-intrin-join"
        try:
            stack = cfn.deploy_stack(stack_name, TEMPLATE)
            assert stack["StackStatus"] == "CREATE_COMPLETE"

            outputs = cfn.get_stack_outputs(stack_name)
            assert outputs["JoinResult"] == f"{stack_name}-joined"
        finally:
            try:
                cfn.delete_stack(stack_name)
            except Exception:
                pass

    def test_fn_select_picks_element(self, cfn, test_run_id):
        """Fn::Select with index 1 should return 'b'."""
        stack_name = f"{test_run_id}-intrin-sel"
        try:
            stack = cfn.deploy_stack(stack_name, TEMPLATE)
            assert stack["StackStatus"] == "CREATE_COMPLETE"

            outputs = cfn.get_stack_outputs(stack_name)
            assert outputs["SelectResult"] == "b"
        finally:
            try:
                cfn.delete_stack(stack_name)
            except Exception:
                pass

    def test_fn_getatt_returns_bucket_arn(self, cfn, test_run_id):
        """Fn::GetAtt Bucket.Arn should return a valid S3 ARN."""
        stack_name = f"{test_run_id}-intrin-gatt"
        try:
            stack = cfn.deploy_stack(stack_name, TEMPLATE)
            assert stack["StackStatus"] == "CREATE_COMPLETE"

            outputs = cfn.get_stack_outputs(stack_name)
            arn = outputs["GetAttResult"]
            assert arn.startswith("arn:aws:s3:::")
            assert f"{stack_name}-bucket" in arn
        finally:
            try:
                cfn.delete_stack(stack_name)
            except Exception:
                pass

    def test_ref_returns_bucket_name(self, cfn, test_run_id):
        """Ref on a bucket resource should return the bucket name."""
        stack_name = f"{test_run_id}-intrin-ref"
        try:
            stack = cfn.deploy_stack(stack_name, TEMPLATE)
            assert stack["StackStatus"] == "CREATE_COMPLETE"

            outputs = cfn.get_stack_outputs(stack_name)
            assert outputs["RefResult"] == f"{stack_name}-bucket"
        finally:
            try:
                cfn.delete_stack(stack_name)
            except Exception:
                pass

    def test_pseudo_parameters(self, cfn, test_run_id):
        """AWS::AccountId and AWS::Region pseudo-parameters should resolve."""
        stack_name = f"{test_run_id}-intrin-pseudo"
        try:
            stack = cfn.deploy_stack(stack_name, TEMPLATE)
            assert stack["StackStatus"] == "CREATE_COMPLETE"

            outputs = cfn.get_stack_outputs(stack_name)
            assert outputs["AccountId"] == ACCOUNT_ID
            assert outputs["RegionOutput"] == REGION
        finally:
            try:
                cfn.delete_stack(stack_name)
            except Exception:
                pass
