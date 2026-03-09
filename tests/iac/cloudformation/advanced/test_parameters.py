"""Tests for CloudFormation template parameters."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from tests.iac.conftest import make_client
from tests.iac.helpers.tool_runner import CloudFormationRunner

pytestmark = pytest.mark.iac

TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Parameters:
  EnvName:
    Type: String
    Default: dev
  BucketSuffix:
    Type: String
    AllowedValues:
      - data
      - logs
      - assets
    Default: data
Resources:
  Bucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${AWS::StackName}-${EnvName}-${BucketSuffix}"
Outputs:
  BucketName:
    Value: !Ref Bucket
"""


@pytest.fixture(scope="module")
def cfn(ensure_server):
    client = make_client("cloudformation")
    return CloudFormationRunner(client)


class TestParameters:
    """Verify parameter defaults, overrides, and validation."""

    def test_deploy_with_defaults(self, cfn, test_run_id):
        """Deploy with default params -> bucket name contains 'dev' and 'data'."""
        stack_name = f"{test_run_id}-param-def"
        try:
            stack = cfn.deploy_stack(stack_name, TEMPLATE)
            assert stack["StackStatus"] == "CREATE_COMPLETE"

            outputs = cfn.get_stack_outputs(stack_name)
            bucket_name = outputs["BucketName"]
            assert "dev" in bucket_name
            assert "data" in bucket_name
        finally:
            try:
                cfn.delete_stack(stack_name)
            except Exception:
                pass

    def test_deploy_with_custom_params(self, cfn, test_run_id):
        """Deploy with custom params -> bucket name reflects them."""
        stack_name = f"{test_run_id}-param-cust"
        try:
            stack = cfn.deploy_stack(
                stack_name,
                TEMPLATE,
                params={"EnvName": "prod", "BucketSuffix": "logs"},
            )
            assert stack["StackStatus"] == "CREATE_COMPLETE"

            outputs = cfn.get_stack_outputs(stack_name)
            bucket_name = outputs["BucketName"]
            assert "prod" in bucket_name
            assert "logs" in bucket_name
        finally:
            try:
                cfn.delete_stack(stack_name)
            except Exception:
                pass

    def test_deploy_with_invalid_allowed_value(self, cfn, test_run_id):
        """Deploy with a value not in AllowedValues -> should fail."""
        stack_name = f"{test_run_id}-param-bad"
        client = make_client("cloudformation")
        try:
            with pytest.raises(ClientError) as exc_info:
                client.create_stack(
                    StackName=stack_name,
                    TemplateBody=TEMPLATE,
                    Parameters=[
                        {"ParameterKey": "EnvName", "ParameterValue": "staging"},
                        {"ParameterKey": "BucketSuffix", "ParameterValue": "invalid-value"},
                    ],
                    Capabilities=["CAPABILITY_IAM"],
                )
            err_msg = str(exc_info.value)
            assert (
                "invalid-value" in err_msg
                or "AllowedValues" in err_msg
                or "ValidationError" in err_msg
            )
        finally:
            try:
                cfn.delete_stack(stack_name)
            except Exception:
                pass
