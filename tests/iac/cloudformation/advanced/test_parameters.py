"""CFN advanced engine test: parameters."""

from __future__ import annotations

import uuid

import pytest
import yaml

from tests.iac.conftest import make_client
from tests.iac.helpers.tool_runner import CloudFormationRunner

pytestmark = pytest.mark.iac

DEFAULT_BUCKET = f"param-default-{uuid.uuid4().hex[:8]}"

TEMPLATE = yaml.dump(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Parameters": {
            "BucketName": {
                "Type": "String",
                "Default": DEFAULT_BUCKET,
            },
            "Environment": {
                "Type": "String",
                "AllowedValues": ["dev", "staging", "prod"],
                "Default": "dev",
            },
        },
        "Resources": {
            "TestBucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": {"Ref": "BucketName"}},
            }
        },
        "Outputs": {
            "BucketNameOut": {
                "Value": {"Ref": "BucketName"},
            },
            "EnvOut": {
                "Value": {"Ref": "Environment"},
            },
        },
    }
)


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="module")
def cfn(ensure_server):
    return make_client("cloudformation")


@pytest.fixture(scope="module")
def runner(cfn):
    return CloudFormationRunner(cfn)


class TestParameters:
    def test_deploy_with_defaults(self, runner, cfn):
        """Deploy with default params, verify outputs match defaults."""
        stack_name = _unique("param-def")
        try:
            runner.deploy_stack(stack_name, TEMPLATE)
            outputs = runner.get_stack_outputs(stack_name)
            assert outputs["BucketNameOut"] == DEFAULT_BUCKET
            assert outputs["EnvOut"] == "dev"
        finally:
            try:
                runner.delete_stack(stack_name)
            except Exception:
                pass  # best-effort cleanup

    def test_deploy_with_custom_params(self, runner, cfn):
        """Deploy with custom BucketName, verify output matches."""
        stack_name = _unique("param-cust")
        custom_bucket = _unique("custom-bkt")
        try:
            runner.deploy_stack(
                stack_name,
                TEMPLATE,
                params={"BucketName": custom_bucket, "Environment": "prod"},
            )
            outputs = runner.get_stack_outputs(stack_name)
            assert outputs["BucketNameOut"] == custom_bucket
            assert outputs["EnvOut"] == "prod"
        finally:
            try:
                runner.delete_stack(stack_name)
            except Exception:
                pass  # best-effort cleanup

    def test_invalid_allowed_value_rejected(self, runner):
        """Deploy with invalid Environment value should fail validation."""
        stack_name = _unique("param-bad")
        try:
            with pytest.raises(Exception, match="AllowedValues|ValidationError"):
                runner.deploy_stack(
                    stack_name,
                    TEMPLATE,
                    params={"Environment": "invalid-env"},
                )
        finally:
            try:
                runner.delete_stack(stack_name)
            except Exception:
                pass  # best-effort cleanup
