"""IaC test: CloudFormation static website with S3 bucket and bucket policy."""

from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from tests.iac.helpers.resource_validator import assert_s3_bucket_exists

pytestmark = pytest.mark.iac

TEMPLATE = (Path(__file__).parent / "template.yaml").read_text()


class TestStaticWebsite:
    """Deploy an S3-hosted static website via CloudFormation."""

    def test_deploy_and_validate(self, deploy_stack, s3, test_run_id):
        """Deploy stack, validate resources, then teardown validates deletion."""
        stack = deploy_stack("static-site", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        bucket_name = f"{test_run_id}-static-site-website"

        # Bucket exists
        assert_s3_bucket_exists(s3, bucket_name)

        # Website configuration is set
        website_cfg = s3.get_bucket_website(Bucket=bucket_name)
        assert website_cfg["IndexDocument"]["Suffix"] == "index.html"
        assert website_cfg["ErrorDocument"]["Key"] == "error.html"

        # Bucket policy exists
        policy_resp = s3.get_bucket_policy(Bucket=bucket_name)
        assert "Statement" in policy_resp["Policy"] or "Statement" in str(policy_resp["Policy"])

    def test_cleanup_removes_bucket(self, cfn_runner, s3, test_run_id):
        """Deploy and explicitly delete, then verify the bucket is gone."""
        stack_name = f"{test_run_id}-static-site-cleanup"
        bucket_name = f"{stack_name}-website"

        cfn_runner.deploy_stack(stack_name, TEMPLATE)
        # Confirm bucket exists before delete
        assert_s3_bucket_exists(s3, bucket_name)

        cfn_runner.delete_stack(stack_name)

        # Bucket should no longer exist
        with pytest.raises(ClientError) as exc_info:
            s3.head_bucket(Bucket=bucket_name)
        err_code = exc_info.value.response["Error"]["Code"]
        assert err_code in ("404", "NoSuchBucket"), f"Unexpected error code: {err_code}"
