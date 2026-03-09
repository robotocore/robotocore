"""IaC test: CloudFormation CI/CD pipeline stack."""

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.resource_validator import (
    assert_iam_role_exists,
    assert_s3_bucket_exists,
    assert_sns_topic_exists,
)

pytestmark = pytest.mark.iac

TEMPLATE = (Path(__file__).parent / "template.yaml").read_text()


def _get_output(stack: dict, key: str) -> str:
    """Extract an output value from a CloudFormation stack description."""
    for out in stack.get("Outputs", []):
        if out["OutputKey"] == key:
            return out["OutputValue"]
    raise KeyError(f"Output {key!r} not found in stack outputs")


class TestCicdPipeline:
    def test_deploy_and_validate(self, deploy_stack):
        """Deploy CI/CD stack, validate all resources, then delete."""
        s3 = make_client("s3")
        iam = make_client("iam")
        sns = make_client("sns")

        # Deploy
        stack = deploy_stack("cicd-pipeline", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        # Extract outputs
        bucket_name = _get_output(stack, "ArtifactsBucketName")
        role_name = _get_output(stack, "PipelineRoleName")
        role_arn = _get_output(stack, "PipelineRoleArn")
        topic_arn = _get_output(stack, "BuildNotificationsTopicArn")

        # Validate S3 bucket exists
        assert_s3_bucket_exists(s3, bucket_name)

        # Validate IAM role exists
        role = assert_iam_role_exists(iam, role_name)
        assert role["Arn"] == role_arn

        # Validate the role has an inline policy with S3 permissions
        policies_resp = iam.list_role_policies(RoleName=role_name)
        policy_names = policies_resp["PolicyNames"]
        assert len(policy_names) >= 1
        # Verify the policy document grants S3 access
        policy_resp = iam.get_role_policy(
            RoleName=role_name,
            PolicyName=policy_names[0],
        )
        statements = policy_resp["PolicyDocument"]["Statement"]
        s3_actions = []
        for stmt in statements:
            actions = stmt.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]
            s3_actions.extend(a for a in actions if a.startswith("s3:"))
        assert "s3:GetObject" in s3_actions
        assert "s3:PutObject" in s3_actions

        # Validate SNS topic exists
        assert_sns_topic_exists(sns, topic_arn)
