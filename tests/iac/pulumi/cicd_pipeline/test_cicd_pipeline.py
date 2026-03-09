"""IaC test: pulumi - cicd_pipeline.

Validates S3 artifact bucket and IAM role creation via Pulumi.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.resource_validator import (
    assert_iam_role_exists,
    assert_s3_bucket_exists,
)

pytestmark = pytest.mark.iac

SCENARIO_DIR = Path(__file__).parent


@pytest.fixture(scope="module")
def stack_outputs(pulumi_runner):
    """Deploy the CI/CD pipeline stack and return Pulumi outputs."""
    result = pulumi_runner.up(SCENARIO_DIR)
    if result.returncode != 0:
        pytest.fail(f"pulumi up failed:\n{result.stderr}")
    yield pulumi_runner.stack_output(SCENARIO_DIR)
    pulumi_runner.destroy(SCENARIO_DIR)


@pytest.fixture(scope="module")
def s3_client():
    return make_client("s3")


@pytest.fixture(scope="module")
def iam_client():
    return make_client("iam")


class TestCicdPipeline:
    """Pulumi CI/CD pipeline: S3 artifact bucket + IAM role."""

    def test_artifact_bucket_created(self, stack_outputs, s3_client):
        bucket_name = stack_outputs["bucket_name"]
        assert_s3_bucket_exists(s3_client, bucket_name)

    def test_iam_role_created(self, stack_outputs, iam_client):
        role_name = stack_outputs["role_name"]
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
