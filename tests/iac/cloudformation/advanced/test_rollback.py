"""CFN advanced engine test: rollback on failure."""

from __future__ import annotations

import uuid

import pytest
import yaml

from tests.iac.conftest import make_client
from tests.iac.helpers.tool_runner import CloudFormationRunner

pytestmark = pytest.mark.iac


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="module")
def cfn(ensure_server):
    return make_client("cloudformation")


@pytest.fixture(scope="module")
def s3(ensure_server):
    return make_client("s3")


@pytest.fixture(scope="module")
def runner(cfn):
    return CloudFormationRunner(cfn)


class TestRollback:
    def test_invalid_resource_causes_rollback(self, runner, cfn, s3):
        """Stack with valid S3 bucket + invalid resource should rollback and clean up."""
        stack_name = _unique("rb-inv")
        bucket_name = _unique("rb-bucket")

        template = yaml.dump(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "GoodBucket": {
                        "Type": "AWS::S3::Bucket",
                        "Properties": {"BucketName": bucket_name},
                    },
                    "BadResource": {
                        "Type": "AWS::FakeService::DoesNotExist",
                        "Properties": {},
                        "DependsOn": ["GoodBucket"],
                    },
                },
            }
        )

        # deploy_stack will raise because the stack hits ROLLBACK_COMPLETE
        with pytest.raises(RuntimeError, match="ROLLBACK"):
            runner.deploy_stack(stack_name, template)

        # Verify the stack reached ROLLBACK_COMPLETE
        stack = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
        assert stack["StackStatus"] == "ROLLBACK_COMPLETE"

        # The S3 bucket should have been cleaned up during rollback
        buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
        assert bucket_name not in buckets, "Bucket should be cleaned up on rollback"

        # Cleanup
        try:
            runner.delete_stack(stack_name)
        except Exception:
            pass  # best-effort cleanup

    def test_entirely_invalid_template_fails(self, runner, cfn):
        """Stack with only invalid resource types should fail."""
        stack_name = _unique("rb-allinv")

        template = yaml.dump(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "BadOne": {
                        "Type": "AWS::Nonexistent::Service",
                        "Properties": {},
                    }
                },
            }
        )

        with pytest.raises(RuntimeError, match="ROLLBACK"):
            runner.deploy_stack(stack_name, template)

        stack = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
        assert stack["StackStatus"] == "ROLLBACK_COMPLETE"

        try:
            runner.delete_stack(stack_name)
        except Exception:
            pass  # best-effort cleanup
