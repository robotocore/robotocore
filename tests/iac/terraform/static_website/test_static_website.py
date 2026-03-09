"""IaC test: terraform - static_website."""

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.resource_validator import assert_s3_bucket_exists

pytestmark = pytest.mark.iac


class TestStaticWebsite:
    """Validate Terraform-provisioned S3 static website resources."""

    def test_apply_succeeds(self, terraform_dir, tf_runner):
        result = tf_runner.apply(terraform_dir)
        assert result.returncode == 0, f"terraform apply failed:\n{result.stderr}"

    def test_bucket_exists(self, terraform_dir, tf_runner):
        """S3 bucket was created and is accessible via boto3."""
        tf_runner.apply(terraform_dir)
        outputs = tf_runner.output(terraform_dir)
        bucket_name = outputs["bucket_name"]["value"]

        s3 = make_client("s3")
        assert_s3_bucket_exists(s3, bucket_name)

    def test_website_configuration(self, terraform_dir, tf_runner):
        """Website configuration has correct index and error documents."""
        tf_runner.apply(terraform_dir)
        outputs = tf_runner.output(terraform_dir)
        bucket_name = outputs["bucket_name"]["value"]

        s3 = make_client("s3")
        resp = s3.get_bucket_website(Bucket=bucket_name)
        assert resp["IndexDocument"]["Suffix"] == "index.html"
        assert resp["ErrorDocument"]["Key"] == "error.html"

    def test_website_endpoint_output(self, terraform_dir, tf_runner):
        """Terraform outputs include a non-empty website_endpoint."""
        tf_runner.apply(terraform_dir)
        outputs = tf_runner.output(terraform_dir)
        endpoint = outputs["website_endpoint"]["value"]
        assert endpoint, "website_endpoint output should not be empty"

    def test_bucket_policy(self, terraform_dir, tf_runner):
        """Bucket policy allows public read access."""
        import json

        tf_runner.apply(terraform_dir)
        outputs = tf_runner.output(terraform_dir)
        bucket_name = outputs["bucket_name"]["value"]

        s3 = make_client("s3")
        resp = s3.get_bucket_policy(Bucket=bucket_name)
        policy = json.loads(resp["Policy"])

        statements = policy.get("Statement", [])
        assert len(statements) >= 1, "Expected at least one policy statement"

        public_stmt = statements[0]
        assert public_stmt["Effect"] == "Allow"
        assert public_stmt["Principal"] == "*"
        assert "s3:GetObject" in public_stmt["Action"]
