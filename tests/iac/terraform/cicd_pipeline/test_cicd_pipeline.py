"""IaC test: Terraform CI/CD pipeline scenario."""

from __future__ import annotations

import json

import pytest

from tests.iac.conftest import make_client


@pytest.fixture(scope="module")
def cicd_outputs(terraform_dir, tf_runner):
    """Apply the CI/CD pipeline scenario and return Terraform outputs."""
    result = tf_runner.apply(terraform_dir)
    if result.returncode != 0:
        pytest.fail(f"terraform apply failed:\n{result.stderr}")
    return tf_runner.output(terraform_dir)


@pytest.fixture(scope="module")
def s3_client():
    return make_client("s3")


@pytest.fixture(scope="module")
def iam_client():
    return make_client("iam")


@pytest.fixture(scope="module")
def sns_client():
    return make_client("sns")


class TestCicdPipeline:
    """Validate CI/CD pipeline resources created by Terraform."""

    def test_s3_bucket_exists(self, cicd_outputs, s3_client):
        bucket_name = cicd_outputs["bucket_name"]["value"]
        resp = s3_client.head_bucket(Bucket=bucket_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_iam_role_exists_with_trust_policy(self, cicd_outputs, iam_client):
        role_arn = cicd_outputs["role_arn"]["value"]
        # Extract role name from ARN (arn:aws:iam::123456789012:role/name)
        role_name = role_arn.rsplit("/", 1)[-1]

        resp = iam_client.get_role(RoleName=role_name)
        role = resp["Role"]
        assert role["Arn"] == role_arn

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

    def test_iam_role_has_s3_policy(self, cicd_outputs, iam_client):
        role_arn = cicd_outputs["role_arn"]["value"]
        role_name = role_arn.rsplit("/", 1)[-1]

        resp = iam_client.list_role_policies(RoleName=role_name)
        policy_names = resp["PolicyNames"]
        assert len(policy_names) >= 1

        # Get the inline policy and verify S3 actions
        policy_resp = iam_client.get_role_policy(RoleName=role_name, PolicyName=policy_names[0])
        policy_doc = policy_resp["PolicyDocument"]
        if isinstance(policy_doc, str):
            policy_doc = json.loads(policy_doc)

        statements = policy_doc.get("Statement", [])
        assert len(statements) >= 1
        actions = []
        for stmt in statements:
            act = stmt.get("Action", [])
            if isinstance(act, str):
                actions.append(act)
            else:
                actions.extend(act)
        assert "s3:GetObject" in actions
        assert "s3:PutObject" in actions

    def test_sns_topic_exists(self, cicd_outputs, sns_client):
        topic_arn = cicd_outputs["topic_arn"]["value"]
        resp = sns_client.get_topic_attributes(TopicArn=topic_arn)
        assert resp["Attributes"]["TopicArn"] == topic_arn
