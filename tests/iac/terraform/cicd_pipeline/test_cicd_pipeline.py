"""IaC test: terraform - cicd_pipeline.

Validates S3 artifact bucket and IAM role creation.
Resources are created via boto3 (mirroring the Terraform program).
"""

from __future__ import annotations

import json

import pytest

from tests.iac.helpers.functional_validator import put_and_get_s3_object
from tests.iac.helpers.resource_validator import (
    assert_iam_role_exists,
    assert_s3_bucket_exists,
)

pytestmark = pytest.mark.iac


@pytest.fixture(scope="module")
def cicd_resources(s3_client, iam_client):
    """Create S3 bucket and IAM role via boto3."""
    bucket_name = "tf-cicd-artifacts-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    assume_role_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "codepipeline.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )

    role_name = "tf-cicd-pipeline-role"
    iam_client.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=assume_role_policy,
    )

    s3_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:PutObject"],
                    "Resource": f"arn:aws:s3:::{bucket_name}/*",
                }
            ],
        }
    )

    iam_client.put_role_policy(
        RoleName=role_name,
        PolicyName="tf-cicd-s3-policy",
        PolicyDocument=s3_policy,
    )

    yield {
        "bucket_name": bucket_name,
        "role_name": role_name,
    }

    # Cleanup
    iam_client.delete_role_policy(RoleName=role_name, PolicyName="tf-cicd-s3-policy")
    iam_client.delete_role(RoleName=role_name)
    # Delete all objects from bucket before deleting it
    try:
        objs = s3_client.list_objects_v2(Bucket=bucket_name)
        for obj in objs.get("Contents", []):
            s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
    except Exception:
        pass  # best-effort cleanup
    s3_client.delete_bucket(Bucket=bucket_name)


class TestCicdPipeline:
    """Terraform CI/CD pipeline: S3 artifact bucket + IAM role."""

    def test_artifact_bucket_created(self, cicd_resources, s3_client):
        bucket_name = cicd_resources["bucket_name"]
        assert_s3_bucket_exists(s3_client, bucket_name)

    def test_iam_role_created(self, cicd_resources, iam_client):
        role_name = cicd_resources["role_name"]
        role = assert_iam_role_exists(iam_client, role_name)

        # Validate trust policy allows codepipeline.amazonaws.com
        trust = role["AssumeRolePolicyDocument"]
        if isinstance(trust, str):
            trust = json.loads(trust)
        statements = trust.get("Statement", [])
        assert len(statements) >= 1
        principals = []
        for stmt in statements:
            principal = stmt.get("Principal", {})
            svc = principal.get("Service", "")
            if isinstance(svc, list):
                principals.extend(svc)
            else:
                principals.append(svc)
        assert "codepipeline.amazonaws.com" in principals

        # Verify inline S3 policy exists
        resp = iam_client.list_role_policies(RoleName=role_name)
        policy_names = resp["PolicyNames"]
        assert len(policy_names) >= 1

        policy_resp = iam_client.get_role_policy(RoleName=role_name, PolicyName=policy_names[0])
        policy_doc = policy_resp["PolicyDocument"]
        if isinstance(policy_doc, str):
            policy_doc = json.loads(policy_doc)

        statements = policy_doc.get("Statement", [])
        actions = []
        for stmt in statements:
            act = stmt.get("Action", [])
            if isinstance(act, str):
                actions.append(act)
            else:
                actions.extend(act)
        assert "s3:GetObject" in actions
        assert "s3:PutObject" in actions

    def test_artifact_upload_download(self, cicd_resources, s3_client):
        """Upload an artifact to S3 and download it back."""
        bucket_name = cicd_resources["bucket_name"]
        resp = put_and_get_s3_object(
            s3_client,
            bucket_name,
            "artifacts/build-1.zip",
            b"fake-zip-content",
        )
        assert resp["ContentLength"] == len(b"fake-zip-content")
