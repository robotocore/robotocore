"""CFN advanced engine test: stack updates."""

from __future__ import annotations

import uuid

import pytest
import yaml

from tests.iac.conftest import make_client
from tests.iac.helpers.tool_runner import CloudFormationRunner

pytestmark = pytest.mark.iac

BUCKET_TEMPLATE = yaml.dump(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Stack update test - S3 bucket",
        "Resources": {
            "TestBucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "PLACEHOLDER"},
            }
        },
    }
)

BUCKET_AND_QUEUE_TEMPLATE = yaml.dump(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Stack update test - S3 bucket + SQS queue",
        "Resources": {
            "TestBucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "PLACEHOLDER"},
            },
            "TestQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": "PLACEHOLDER"},
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
def s3(ensure_server):
    return make_client("s3")


@pytest.fixture(scope="module")
def sqs(ensure_server):
    return make_client("sqs")


@pytest.fixture(scope="module")
def runner(cfn):
    return CloudFormationRunner(cfn)


class TestStackUpdates:
    def test_add_resource_via_update(self, runner, cfn, sqs):
        """Deploy with S3 bucket, update to add SQS queue, verify queue exists."""
        stack_name = _unique("upd-add")
        bucket_name = _unique("upd-bucket")
        queue_name = _unique("upd-queue")

        tmpl_v1 = BUCKET_TEMPLATE.replace("PLACEHOLDER", bucket_name)
        tmpl_v2 = BUCKET_AND_QUEUE_TEMPLATE.replace("PLACEHOLDER", bucket_name, 1).replace(
            "PLACEHOLDER", queue_name, 1
        )

        try:
            runner.deploy_stack(stack_name, tmpl_v1)
            stack = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
            assert stack["StackStatus"] == "CREATE_COMPLETE"

            # Update: add queue
            runner.update_stack(stack_name, tmpl_v2)
            stack = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
            assert stack["StackStatus"] == "UPDATE_COMPLETE"

            # Verify queue was created
            resp = sqs.list_queues(QueueNamePrefix=queue_name)
            urls = resp.get("QueueUrls", [])
            assert any(queue_name in u for u in urls), f"Queue {queue_name} not found"
        finally:
            try:
                runner.delete_stack(stack_name)
            except Exception:
                pass  # best-effort cleanup

    def test_remove_resource_via_update(self, runner, cfn, sqs):
        """Deploy with bucket + queue, update to remove queue, verify queue deleted."""
        stack_name = _unique("upd-rm")
        bucket_name = _unique("uprm-bucket")
        queue_name = _unique("uprm-queue")

        tmpl_v1 = BUCKET_AND_QUEUE_TEMPLATE.replace("PLACEHOLDER", bucket_name, 1).replace(
            "PLACEHOLDER", queue_name, 1
        )
        tmpl_v2 = BUCKET_TEMPLATE.replace("PLACEHOLDER", bucket_name)

        try:
            runner.deploy_stack(stack_name, tmpl_v1)

            # Update: remove queue
            runner.update_stack(stack_name, tmpl_v2)

            # Verify queue is gone
            resp = sqs.list_queues(QueueNamePrefix=queue_name)
            urls = resp.get("QueueUrls", [])
            assert not any(queue_name in u for u in urls), "Queue should be deleted"
        finally:
            try:
                runner.delete_stack(stack_name)
            except Exception:
                pass  # best-effort cleanup

    def test_update_with_invalid_resource_fails(self, runner, cfn):
        """Update with an unsupported resource type should fail."""
        stack_name = _unique("upd-inv")
        bucket_name = _unique("upinv-bucket")

        tmpl_v1 = BUCKET_TEMPLATE.replace("PLACEHOLDER", bucket_name)
        tmpl_bad = yaml.dump(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "TestBucket": {
                        "Type": "AWS::S3::Bucket",
                        "Properties": {"BucketName": bucket_name},
                    },
                    "BadResource": {
                        "Type": "AWS::FakeService::FakeResource",
                        "Properties": {},
                    },
                },
            }
        )

        try:
            runner.deploy_stack(stack_name, tmpl_v1)

            # Update with bad resource should fail
            with pytest.raises(RuntimeError, match="ROLLBACK|FAILED"):
                runner.update_stack(stack_name, tmpl_bad)
        finally:
            try:
                runner.delete_stack(stack_name)
            except Exception:
                pass  # best-effort cleanup
