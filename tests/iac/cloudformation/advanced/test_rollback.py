"""CFN advanced engine test: rollback on failure."""

from __future__ import annotations

import os
import uuid

import pytest
import yaml

from tests.iac.conftest import make_client
from tests.iac.helpers.tool_runner import CloudFormationRunner

pytestmark = pytest.mark.iac

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="module")
def cfn(ensure_server):
    return make_client("cloudformation")


@pytest.fixture(scope="module")
def s3(ensure_server):
    return make_client("s3")


@pytest.fixture(scope="module")
def sqs(ensure_server):
    return make_client("sqs")


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

    def test_parent_rollback_deletes_nested_stack(self, runner, cfn, s3, sqs):
        """Parent rollback after a nested deploy unwinds the child stack.

        The failing resource DependsOn the nested stack so the child (and its
        SQS queue) exist when the parent starts rolling back: rollback must
        delete the child stack, its resources, and its registered exports —
        not leave the child CREATE_IN_PROGRESS with a synthetic export in
        ListExports while claiming Nested DELETE_COMPLETE in the parent's
        events.
        """
        template_bucket = _unique("rbnest-tmpl")
        stack_name = _unique("rbnest-parent")
        queue_name = _unique("rbnest-queue")
        bucket_name = _unique("rbnest-bucket")
        export_name = _unique("rbnest-export")

        child_template = yaml.dump(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Parameters": {"QueueName": {"Type": "String"}},
                "Resources": {
                    "ChildQueue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": {"Ref": "QueueName"}},
                    }
                },
                "Outputs": {
                    "ChildQueueUrl": {
                        "Value": {"Fn::GetAtt": ["ChildQueue", "QueueUrl"]},
                        "Export": {"Name": export_name},
                    }
                },
            }
        )
        s3.create_bucket(Bucket=template_bucket)
        s3.put_object(Bucket=template_bucket, Key="child.yaml", Body=child_template.encode())

        template_url = f"{ENDPOINT_URL}/{template_bucket}/child.yaml"
        parent_template = yaml.dump(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "NestedStack": {
                        "Type": "AWS::CloudFormation::Stack",
                        "Properties": {
                            "TemplateURL": template_url,
                            "Parameters": {"QueueName": queue_name},
                        },
                    },
                    "GoodBucket": {
                        "Type": "AWS::S3::Bucket",
                        "Properties": {"BucketName": bucket_name},
                    },
                    "BadResource": {
                        "Type": "AWS::FakeService::DoesNotExist",
                        "Properties": {},
                        "DependsOn": ["NestedStack"],
                    },
                },
            }
        )

        try:
            with pytest.raises(RuntimeError, match="ROLLBACK"):
                runner.deploy_stack(stack_name, parent_template)

            stack = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
            assert stack["StackStatus"] == "ROLLBACK_COMPLETE"

            # The child's SQS queue must not survive the parent's rollback.
            queues = sqs.list_queues(QueueNamePrefix=queue_name).get("QueueUrls", [])
            assert not queues, "Child queue should be deleted with the nested stack"

            # The parent's own resources were unwound as usual.
            buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
            assert bucket_name not in buckets, "Bucket should be cleaned up on rollback"

            # The child stack row must be delete-complete, not stuck
            # CREATE_IN_PROGRESS.
            children = [
                s
                for s in cfn.list_stacks()["StackSummaries"]
                if s["StackName"].startswith(f"{stack_name}-")
            ]
            assert children, "Nested stack row should exist"
            for child in children:
                assert child["StackStatus"] == "DELETE_COMPLETE", (
                    f"Child stack {child['StackName']} stuck in {child['StackStatus']}"
                )

            # The child's export must leave ListExports with the child stack.
            exports = [e["Name"] for e in cfn.list_exports().get("Exports", [])]
            assert export_name not in exports, "Child export should be removed on rollback"

            # The parent's Nested DELETE_COMPLETE event must now be truthful.
            events = cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
            nested_done = [
                e
                for e in events
                if e["LogicalResourceId"] == "NestedStack"
                and e["ResourceStatus"] == "DELETE_COMPLETE"
            ]
            assert nested_done, "Parent should record Nested DELETE_COMPLETE"
        finally:
            try:
                runner.delete_stack(stack_name)
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_object(Bucket=template_bucket, Key="child.yaml")
                s3.delete_bucket(Bucket=template_bucket)
            except Exception:
                pass  # best-effort cleanup
