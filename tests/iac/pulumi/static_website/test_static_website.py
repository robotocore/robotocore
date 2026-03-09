"""IaC test: pulumi - static_website."""

import json
from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.resource_validator import assert_s3_bucket_exists

pytestmark = pytest.mark.iac

SCENARIO_DIR = Path(__file__).parent


class TestStaticWebsite:
    """Validate Pulumi-provisioned S3 static website resources."""

    def test_deploy_creates_bucket(self, pulumi_runner):
        """Deploy the stack and verify the S3 bucket exists."""
        result = pulumi_runner.up(SCENARIO_DIR)
        assert result.returncode == 0, f"pulumi up failed:\n{result.stderr}"

        outputs = pulumi_runner.stack_output(SCENARIO_DIR)
        bucket_name = outputs["bucket_name"]

        s3 = make_client("s3")
        assert_s3_bucket_exists(s3, bucket_name)

        # Cleanup
        pulumi_runner.destroy(SCENARIO_DIR)

    def test_website_configuration(self, pulumi_runner):
        """Verify index and error documents are configured correctly."""
        result = pulumi_runner.up(SCENARIO_DIR)
        assert result.returncode == 0, f"pulumi up failed:\n{result.stderr}"

        outputs = pulumi_runner.stack_output(SCENARIO_DIR)
        bucket_name = outputs["bucket_name"]

        s3 = make_client("s3")
        resp = s3.get_bucket_website(Bucket=bucket_name)
        assert resp["IndexDocument"]["Suffix"] == "index.html"
        assert resp["ErrorDocument"]["Key"] == "error.html"

        # Cleanup
        pulumi_runner.destroy(SCENARIO_DIR)

    def test_bucket_policy(self, pulumi_runner):
        """Bucket policy allows public read access."""
        result = pulumi_runner.up(SCENARIO_DIR)
        assert result.returncode == 0, f"pulumi up failed:\n{result.stderr}"

        outputs = pulumi_runner.stack_output(SCENARIO_DIR)
        bucket_name = outputs["bucket_name"]

        s3 = make_client("s3")
        resp = s3.get_bucket_policy(Bucket=bucket_name)
        policy = json.loads(resp["Policy"])

        statements = policy.get("Statement", [])
        assert len(statements) >= 1, "Expected at least one policy statement"

        public_stmt = statements[0]
        assert public_stmt["Effect"] == "Allow"
        assert public_stmt["Principal"] == "*"
        assert "s3:GetObject" in public_stmt["Action"]

        # Cleanup
        pulumi_runner.destroy(SCENARIO_DIR)
