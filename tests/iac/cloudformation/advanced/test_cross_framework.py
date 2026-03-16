"""Cross-framework consistency: verify identical templates produce identical resources."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from tests.iac.conftest import make_client
from tests.iac.helpers.resource_validator import (
    assert_dynamodb_table_exists,
    assert_s3_bucket_exists,
)
from tests.iac.helpers.tool_runner import CloudFormationRunner

pytestmark = pytest.mark.iac

# Template A: uses short-form intrinsics (!Sub, !Ref)
TEMPLATE_SHORT_FORM = """\
AWSTemplateFormatVersion: "2010-09-09"
Resources:
  Bucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${AWS::StackName}-data"
  Table:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: !Sub "${AWS::StackName}-items"
      AttributeDefinitions:
        - AttributeName: pk
          AttributeType: S
      KeySchema:
        - AttributeName: pk
          KeyType: HASH
      BillingMode: PAY_PER_REQUEST
Outputs:
  BucketName:
    Value: !Ref Bucket
  TableName:
    Value: !Ref Table
"""

# Template B: uses long-form intrinsics (Fn::Sub, Ref)
TEMPLATE_LONG_FORM = """\
AWSTemplateFormatVersion: "2010-09-09"
Resources:
  Bucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName:
        Fn::Sub: "${AWS::StackName}-data"
  Table:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName:
        Fn::Sub: "${AWS::StackName}-items"
      AttributeDefinitions:
        - AttributeName: pk
          AttributeType: S
      KeySchema:
        - AttributeName: pk
          KeyType: HASH
      BillingMode: PAY_PER_REQUEST
Outputs:
  BucketName:
    Value:
      Ref: Bucket
  TableName:
    Value:
      Ref: Table
"""


@pytest.fixture(scope="module")
def cfn(ensure_server):
    return CloudFormationRunner(make_client("cloudformation"))


class TestCrossFrameworkConsistency:
    """Verify that different template syntax produces identical resources."""

    def test_short_and_long_form_produce_same_resources(self, cfn, test_run_id):
        """Short-form (!Sub) and long-form (Fn::Sub) should be equivalent."""
        stack_a = f"{test_run_id}-xframe-short"
        stack_b = f"{test_run_id}-xframe-long"
        try:
            result_a = cfn.deploy_stack(stack_a, TEMPLATE_SHORT_FORM)
            result_b = cfn.deploy_stack(stack_b, TEMPLATE_LONG_FORM)

            outputs_a = {o["OutputKey"]: o["OutputValue"] for o in result_a.get("Outputs", [])}
            outputs_b = {o["OutputKey"]: o["OutputValue"] for o in result_b.get("Outputs", [])}

            s3 = make_client("s3")
            ddb = make_client("dynamodb")

            # Both buckets exist
            assert_s3_bucket_exists(s3, outputs_a["BucketName"])
            assert_s3_bucket_exists(s3, outputs_b["BucketName"])

            # Both tables exist with same schema
            table_a = assert_dynamodb_table_exists(ddb, outputs_a["TableName"])
            table_b = assert_dynamodb_table_exists(ddb, outputs_b["TableName"])
            assert table_a["KeySchema"] == table_b["KeySchema"]
            assert (
                table_a["BillingModeSummary"]["BillingMode"]
                == table_b["BillingModeSummary"]["BillingMode"]
            )
        finally:
            for name in [stack_b, stack_a]:
                try:
                    cfn.delete_stack(name)
                except Exception:
                    pass  # best-effort cleanup

    def test_resources_are_functional_after_deploy(self, cfn, test_run_id):
        """Resources from CFN deployment should be fully functional."""
        from tests.iac.helpers.functional_validator import (
            put_and_get_dynamodb_item,
            put_and_get_s3_object,
        )

        stack_name = f"{test_run_id}-xframe-func"
        try:
            result = cfn.deploy_stack(stack_name, TEMPLATE_SHORT_FORM)
            outputs = {o["OutputKey"]: o["OutputValue"] for o in result.get("Outputs", [])}

            s3 = make_client("s3")
            ddb = make_client("dynamodb")

            # S3 roundtrip
            put_and_get_s3_object(s3, outputs["BucketName"], "test.txt", "hello world")

            # DynamoDB roundtrip
            put_and_get_dynamodb_item(
                ddb,
                outputs["TableName"],
                item={"pk": {"S": "item-1"}, "data": {"S": "value-1"}},
                key={"pk": {"S": "item-1"}},
            )
        finally:
            try:
                cfn.delete_stack(stack_name)
            except Exception:
                pass  # best-effort cleanup

    def test_stack_deletion_cleans_up_resources(self, cfn, test_run_id):
        """After stack deletion, resources should not exist."""
        stack_name = f"{test_run_id}-xframe-del"
        result = cfn.deploy_stack(stack_name, TEMPLATE_SHORT_FORM)
        outputs = {o["OutputKey"]: o["OutputValue"] for o in result.get("Outputs", [])}
        bucket_name = outputs["BucketName"]
        table_name = outputs["TableName"]

        cfn.delete_stack(stack_name)

        s3 = make_client("s3")
        ddb = make_client("dynamodb")

        with pytest.raises(ClientError):
            s3.head_bucket(Bucket=bucket_name)
        with pytest.raises(ClientError):
            ddb.describe_table(TableName=table_name)
