"""CFN advanced engine test: intrinsic functions."""

from __future__ import annotations

import uuid

import pytest
import yaml

from tests.iac.conftest import make_client
from tests.iac.helpers.tool_runner import CloudFormationRunner

pytestmark = pytest.mark.iac


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _make_template(bucket_name: str) -> str:
    return yaml.dump(
        {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Parameters": {
                "BucketName": {
                    "Type": "String",
                    "Default": bucket_name,
                },
                "Env": {
                    "Type": "String",
                    "Default": "test",
                },
            },
            "Resources": {
                "TestBucket": {
                    "Type": "AWS::S3::Bucket",
                    "Properties": {"BucketName": {"Ref": "BucketName"}},
                }
            },
            "Outputs": {
                "SubResult": {
                    "Description": "Fn::Sub test",
                    "Value": {"Fn::Sub": "arn:aws:s3:::${BucketName}"},
                },
                "JoinResult": {
                    "Description": "Fn::Join test",
                    "Value": {"Fn::Join": ["-", ["my", "joined", "value"]]},
                },
                "SelectResult": {
                    "Description": "Fn::Select test",
                    "Value": {"Fn::Select": ["1", ["alpha", "bravo", "charlie"]]},
                },
                "RefResult": {
                    "Description": "Ref test",
                    "Value": {"Ref": "Env"},
                },
                "GetAttArn": {
                    "Description": "Fn::GetAtt test",
                    "Value": {"Fn::GetAtt": ["TestBucket", "Arn"]},
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


class TestIntrinsicFunctions:
    def test_fn_sub(self, runner):
        """Fn::Sub resolves parameter references."""
        stack_name = _unique("intr-sub")
        bucket_name = _unique("intr-bucket")
        try:
            runner.deploy_stack(stack_name, _make_template(bucket_name))
            outputs = runner.get_stack_outputs(stack_name)
            assert outputs["SubResult"] == f"arn:aws:s3:::{bucket_name}"
        finally:
            try:
                runner.delete_stack(stack_name)
            except Exception:
                pass  # best-effort cleanup

    def test_fn_join(self, runner):
        """Fn::Join concatenates values with delimiter."""
        stack_name = _unique("intr-join")
        bucket_name = _unique("intr-jbkt")
        try:
            runner.deploy_stack(stack_name, _make_template(bucket_name))
            outputs = runner.get_stack_outputs(stack_name)
            assert outputs["JoinResult"] == "my-joined-value"
        finally:
            try:
                runner.delete_stack(stack_name)
            except Exception:
                pass  # best-effort cleanup

    def test_fn_select(self, runner):
        """Fn::Select picks an element by index."""
        stack_name = _unique("intr-sel")
        bucket_name = _unique("intr-sbkt")
        try:
            runner.deploy_stack(stack_name, _make_template(bucket_name))
            outputs = runner.get_stack_outputs(stack_name)
            assert outputs["SelectResult"] == "bravo"
        finally:
            try:
                runner.delete_stack(stack_name)
            except Exception:
                pass  # best-effort cleanup

    def test_ref(self, runner):
        """Ref resolves a parameter value."""
        stack_name = _unique("intr-ref")
        bucket_name = _unique("intr-rbkt")
        try:
            runner.deploy_stack(stack_name, _make_template(bucket_name))
            outputs = runner.get_stack_outputs(stack_name)
            assert outputs["RefResult"] == "test"
        finally:
            try:
                runner.delete_stack(stack_name)
            except Exception:
                pass  # best-effort cleanup

    def test_fn_getatt(self, runner):
        """Fn::GetAtt retrieves a resource attribute."""
        stack_name = _unique("intr-ga")
        bucket_name = _unique("intr-gabkt")
        try:
            runner.deploy_stack(stack_name, _make_template(bucket_name))
            outputs = runner.get_stack_outputs(stack_name)
            # The ARN should contain the bucket name
            assert bucket_name in outputs["GetAttArn"]
            assert outputs["GetAttArn"].startswith("arn:aws:s3")
        finally:
            try:
                runner.delete_stack(stack_name)
            except Exception:
                pass  # best-effort cleanup
