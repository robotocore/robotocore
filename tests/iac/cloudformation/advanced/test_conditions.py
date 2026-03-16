"""CFN advanced engine test: conditions."""

from __future__ import annotations

import uuid

import pytest
import yaml

from tests.iac.conftest import make_client
from tests.iac.helpers.tool_runner import CloudFormationRunner

pytestmark = pytest.mark.iac

TEMPLATE = yaml.dump(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Parameters": {
            "Environment": {
                "Type": "String",
                "AllowedValues": ["dev", "prod"],
                "Default": "dev",
            }
        },
        "Conditions": {
            "IsProd": {"Fn::Equals": [{"Ref": "Environment"}, "prod"]},
        },
        "Resources": {
            "AlwaysBucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "PLACEHOLDER_BUCKET"},
            },
            "ProdOnlyQueue": {
                "Type": "AWS::SQS::Queue",
                "Condition": "IsProd",
                "Properties": {"QueueName": "PLACEHOLDER_QUEUE"},
            },
        },
        "Outputs": {
            "BucketName": {"Value": "PLACEHOLDER_BUCKET"},
            "QueueCreated": {
                "Condition": "IsProd",
                "Value": "yes",
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
def sqs(ensure_server):
    return make_client("sqs")


@pytest.fixture(scope="module")
def runner(cfn):
    return CloudFormationRunner(cfn)


class TestConditions:
    def test_condition_true_creates_resource(self, runner, cfn, sqs):
        """When Environment=prod, the conditional SQS queue IS created."""
        stack_name = _unique("cond-prod")
        bucket_name = _unique("cond-prod-bkt")
        queue_name = _unique("cond-prod-q")

        tmpl = TEMPLATE.replace("PLACEHOLDER_BUCKET", bucket_name).replace(
            "PLACEHOLDER_QUEUE", queue_name
        )

        try:
            runner.deploy_stack(stack_name, tmpl, params={"Environment": "prod"})
            outputs = runner.get_stack_outputs(stack_name)
            assert outputs.get("QueueCreated") == "yes"

            # Verify queue exists
            resp = sqs.list_queues(QueueNamePrefix=queue_name)
            urls = resp.get("QueueUrls", [])
            assert any(queue_name in u for u in urls), "Prod queue should exist"
        finally:
            try:
                runner.delete_stack(stack_name)
            except Exception:
                pass  # best-effort cleanup

    def test_condition_false_skips_resource(self, runner, cfn, sqs):
        """When Environment=dev, the conditional SQS queue is NOT created."""
        stack_name = _unique("cond-dev")
        bucket_name = _unique("cond-dev-bkt")
        queue_name = _unique("cond-dev-q")

        tmpl = TEMPLATE.replace("PLACEHOLDER_BUCKET", bucket_name).replace(
            "PLACEHOLDER_QUEUE", queue_name
        )

        try:
            runner.deploy_stack(stack_name, tmpl, params={"Environment": "dev"})
            outputs = runner.get_stack_outputs(stack_name)
            # QueueCreated output should not exist (condition is false)
            assert "QueueCreated" not in outputs

            # Verify queue does NOT exist
            resp = sqs.list_queues(QueueNamePrefix=queue_name)
            urls = resp.get("QueueUrls", [])
            assert not any(queue_name in u for u in urls), "Dev queue should not exist"
        finally:
            try:
                runner.delete_stack(stack_name)
            except Exception:
                pass  # best-effort cleanup
