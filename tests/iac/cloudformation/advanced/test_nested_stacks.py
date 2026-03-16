"""CFN advanced engine test: nested stacks."""

from __future__ import annotations

import uuid

import pytest
import yaml

from tests.iac.conftest import make_client
from tests.iac.helpers.tool_runner import CloudFormationRunner

pytestmark = pytest.mark.iac

CHILD_TEMPLATE = yaml.dump(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Parameters": {
            "QueueName": {
                "Type": "String",
            }
        },
        "Resources": {
            "ChildQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": {"Ref": "QueueName"}},
            }
        },
        "Outputs": {
            "ChildQueueUrl": {
                "Value": {"Fn::GetAtt": ["ChildQueue", "QueueUrl"]},
            }
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


class TestNestedStacks:
    def test_nested_stack_creates_child_resources(self, runner, cfn, s3, sqs):
        """Parent stack with AWS::CloudFormation::Stack creates child resources."""
        template_bucket = _unique("nest-tmpl")
        stack_name = _unique("nest-parent")
        queue_name = _unique("nest-queue")

        # Upload child template to S3
        s3.create_bucket(Bucket=template_bucket)
        s3.put_object(
            Bucket=template_bucket,
            Key="child.yaml",
            Body=CHILD_TEMPLATE.encode(),
        )

        # URL format: http://host:port/bucket/key — parsed by _fetch_template_from_s3
        template_url = f"http://localhost:4566/{template_bucket}/child.yaml"

        parent_template = yaml.dump(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "ChildStack": {
                        "Type": "AWS::CloudFormation::Stack",
                        "Properties": {
                            "TemplateURL": template_url,
                            "Parameters": {"QueueName": queue_name},
                        },
                    }
                },
            }
        )

        try:
            runner.deploy_stack(stack_name, parent_template)
            stack = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
            assert stack["StackStatus"] == "CREATE_COMPLETE"

            # Verify the child queue was created
            resp = sqs.list_queues(QueueNamePrefix=queue_name)
            urls = resp.get("QueueUrls", [])
            assert any(queue_name in u for u in urls), f"Child queue {queue_name} should exist"
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

    def test_deleting_parent_cleans_up_child(self, runner, cfn, s3, sqs):
        """Deleting parent stack should clean up nested stack resources."""
        template_bucket = _unique("nestdel-tmpl")
        stack_name = _unique("nestdel-parent")
        queue_name = _unique("nestdel-queue")

        s3.create_bucket(Bucket=template_bucket)
        s3.put_object(
            Bucket=template_bucket,
            Key="child.yaml",
            Body=CHILD_TEMPLATE.encode(),
        )

        template_url = f"http://localhost:4566/{template_bucket}/child.yaml"

        parent_template = yaml.dump(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "ChildStack": {
                        "Type": "AWS::CloudFormation::Stack",
                        "Properties": {
                            "TemplateURL": template_url,
                            "Parameters": {"QueueName": queue_name},
                        },
                    }
                },
            }
        )

        try:
            runner.deploy_stack(stack_name, parent_template)

            # Verify queue exists before delete
            resp = sqs.list_queues(QueueNamePrefix=queue_name)
            urls = resp.get("QueueUrls", [])
            assert any(queue_name in u for u in urls)

            # Delete parent
            runner.delete_stack(stack_name)

            # Verify child queue is cleaned up
            resp = sqs.list_queues(QueueNamePrefix=queue_name)
            urls = resp.get("QueueUrls", [])
            assert not any(queue_name in u for u in urls), "Child queue should be deleted"
        finally:
            try:
                s3.delete_object(Bucket=template_bucket, Key="child.yaml")
                s3.delete_bucket(Bucket=template_bucket)
            except Exception:
                pass  # best-effort cleanup
