"""IaC test: cdk - static_website.

Deploys an S3 bucket with static website hosting via CDK and validates
the resources with boto3.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.resource_validator import assert_s3_bucket_exists

pytestmark = pytest.mark.iac

SCENARIO_DIR = Path(__file__).parent


class TestStaticWebsite:
    """CDK static-website scenario tests."""

    def test_synth_produces_template(self, cdk_runner):
        """``cdk synth`` should produce a CloudFormation template."""
        out_dir = cdk_runner.synth(SCENARIO_DIR)
        template_path = Path(out_dir) / "StaticWebsite.template.json"
        assert template_path.exists(), f"Expected template at {template_path}"

        template = json.loads(template_path.read_text())
        resources = template.get("Resources", {})
        # Should contain at least one S3 Bucket resource
        bucket_resources = [r for r in resources.values() if r.get("Type") == "AWS::S3::Bucket"]
        assert len(bucket_resources) >= 1, "Template should contain an S3 Bucket resource"

    def test_deploy_creates_bucket(self, cdk_runner, ensure_server):
        """Deploying the stack should create the S3 bucket."""
        result = cdk_runner.deploy(SCENARIO_DIR, stack_name="StaticWebsite")
        assert result.returncode == 0, f"cdk deploy failed:\n{result.stderr}"

        try:
            # Discover the bucket name from stack outputs
            cfn = make_client("cloudformation")
            resp = cfn.describe_stacks(StackName="StaticWebsite")
            outputs = {
                o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])
            }
            bucket_name = outputs.get("BucketName")
            assert bucket_name, "Stack should have a BucketName output"

            s3 = make_client("s3")
            assert_s3_bucket_exists(s3, bucket_name)
        finally:
            cdk_runner.destroy(SCENARIO_DIR, stack_name="StaticWebsite")

    def test_website_configuration(self, cdk_runner, ensure_server):
        """The deployed bucket should have website hosting configured."""
        result = cdk_runner.deploy(SCENARIO_DIR, stack_name="StaticWebsite")
        assert result.returncode == 0, f"cdk deploy failed:\n{result.stderr}"

        try:
            cfn = make_client("cloudformation")
            resp = cfn.describe_stacks(StackName="StaticWebsite")
            outputs = {
                o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])
            }
            bucket_name = outputs.get("BucketName")
            assert bucket_name, "Stack should have a BucketName output"

            s3 = make_client("s3")
            website_cfg = s3.get_bucket_website(Bucket=bucket_name)
            assert website_cfg["IndexDocument"]["Suffix"] == "index.html"
            assert website_cfg["ErrorDocument"]["Key"] == "error.html"
        finally:
            cdk_runner.destroy(SCENARIO_DIR, stack_name="StaticWebsite")
