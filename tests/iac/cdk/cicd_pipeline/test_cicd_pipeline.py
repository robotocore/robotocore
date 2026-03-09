"""IaC test: cdk - cicd_pipeline.

Deploys a CodeBuild project, S3 artifact bucket, and IAM build role.
Validates the S3 bucket and IAM role via boto3.
"""

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.functional_validator import put_and_get_s3_object
from tests.iac.helpers.resource_validator import (
    assert_iam_role_exists,
    assert_s3_bucket_exists,
)

pytestmark = pytest.mark.iac

SCENARIO_DIR = Path(__file__).parent


class TestCicdPipeline:
    """CDK CI/CD pipeline stack with CodeBuild, S3, and IAM."""

    @pytest.fixture(autouse=True)
    def deploy(self, cdk_runner):
        """Deploy the CDK app and tear it down after tests."""
        result = cdk_runner.deploy(SCENARIO_DIR, "CicdPipelineStack")
        assert result.returncode == 0, f"cdk deploy failed: {result.stderr}"
        yield
        cdk_runner.destroy(SCENARIO_DIR, "CicdPipelineStack")

    def test_artifact_bucket_created(self):
        """Verify S3 artifact bucket exists."""
        s3 = make_client("s3")
        assert_s3_bucket_exists(s3, "cicd-artifact-bucket")

    def test_iam_role_created(self):
        """Verify IAM build role exists."""
        iam = make_client("iam")
        role = assert_iam_role_exists(iam, "cicd-build-role")
        assert role["RoleName"] == "cicd-build-role"

    def test_artifact_upload_download(self):
        """Upload an artifact to S3 and download it back."""
        s3 = make_client("s3")
        resp = put_and_get_s3_object(
            s3,
            "cicd-artifact-bucket",
            "artifacts/build-1.zip",
            b"fake-zip-content",
        )
        assert resp["ContentLength"] == len(b"fake-zip-content")
