"""CFN advanced engine test: cross-stack references via Exports/ImportValue."""

from __future__ import annotations

import uuid

import pytest
import yaml

from tests.iac.conftest import make_client
from tests.iac.helpers.tool_runner import CloudFormationRunner

pytestmark = pytest.mark.iac


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _stack_a_template(bucket_name: str, export_prefix: str) -> str:
    return yaml.dump(
        {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Description": "Stack A: exports bucket name",
            "Resources": {
                "SharedBucket": {
                    "Type": "AWS::S3::Bucket",
                    "Properties": {"BucketName": bucket_name},
                }
            },
            "Outputs": {
                "BucketNameOut": {
                    "Value": bucket_name,
                    "Export": {"Name": f"{export_prefix}-BucketName"},
                },
                "BucketArnOut": {
                    "Value": f"arn:aws:s3:::{bucket_name}",
                    "Export": {"Name": f"{export_prefix}-BucketArn"},
                },
            },
        }
    )


def _stack_b_template(export_prefix: str, queue_name: str) -> str:
    return yaml.dump(
        {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Description": "Stack B: imports from Stack A",
            "Resources": {
                "ConsumerQueue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": queue_name},
                }
            },
            "Outputs": {
                "ImportedBucketName": {
                    "Value": {"Fn::ImportValue": f"{export_prefix}-BucketName"},
                },
                "ImportedBucketArn": {
                    "Value": {"Fn::ImportValue": f"{export_prefix}-BucketArn"},
                },
            },
        }
    )


@pytest.fixture(scope="module")
def cfn(ensure_server):
    return make_client("cloudformation")


@pytest.fixture(scope="module")
def runner(cfn):
    return CloudFormationRunner(cfn)


class TestCrossStackRefs:
    def test_import_value_resolves(self, runner, cfn):
        """Stack B can import values exported by Stack A."""
        prefix = _unique("xref")
        stack_a = _unique("xref-a")
        stack_b = _unique("xref-b")
        bucket_name = _unique("xref-bucket")
        queue_name = _unique("xref-queue")

        try:
            # Deploy Stack A (exporter)
            runner.deploy_stack(stack_a, _stack_a_template(bucket_name, prefix))
            outputs_a = runner.get_stack_outputs(stack_a)
            assert outputs_a["BucketNameOut"] == bucket_name

            # Deploy Stack B (importer)
            runner.deploy_stack(stack_b, _stack_b_template(prefix, queue_name))
            outputs_b = runner.get_stack_outputs(stack_b)
            assert outputs_b["ImportedBucketName"] == bucket_name
            assert outputs_b["ImportedBucketArn"] == f"arn:aws:s3:::{bucket_name}"
        finally:
            # Delete in correct order: consumer first, then exporter
            for name in (stack_b, stack_a):
                try:
                    runner.delete_stack(name)
                except Exception:
                    pass  # best-effort cleanup

    def test_list_exports_includes_stack_exports(self, runner, cfn):
        """ListExports should include exports from Stack A."""
        prefix = _unique("lexp")
        stack_name = _unique("lexp-stk")
        bucket_name = _unique("lexp-bucket")

        try:
            runner.deploy_stack(stack_name, _stack_a_template(bucket_name, prefix))
            resp = cfn.list_exports()
            export_names = [e["Name"] for e in resp.get("Exports", [])]
            assert f"{prefix}-BucketName" in export_names
            assert f"{prefix}-BucketArn" in export_names
        finally:
            try:
                runner.delete_stack(stack_name)
            except Exception:
                pass  # best-effort cleanup
