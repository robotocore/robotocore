"""IaC test: terraform - auth_stack.

Validates Cognito user pool and client creation via Terraform.
"""

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.resource_validator import assert_cognito_user_pool_exists

pytestmark = pytest.mark.iac


class TestAuthStack:
    """Terraform auth stack: Cognito user pool + client."""

    def test_apply_succeeds(self, terraform_dir, tf_runner):
        result = tf_runner.apply(terraform_dir)
        assert result.returncode == 0, f"terraform apply failed:\n{result.stderr}"

    def test_user_pool_exists(self, terraform_dir, tf_runner):
        tf_runner.apply(terraform_dir)
        outputs = tf_runner.output(terraform_dir)
        pool_id = outputs["user_pool_id"]["value"]

        cognito = make_client("cognito-idp")
        pool = assert_cognito_user_pool_exists(cognito, pool_id)
        assert pool["Name"].endswith("-user-pool")

    def test_password_policy(self, terraform_dir, tf_runner):
        tf_runner.apply(terraform_dir)
        outputs = tf_runner.output(terraform_dir)
        pool_id = outputs["user_pool_id"]["value"]

        cognito = make_client("cognito-idp")
        pool = assert_cognito_user_pool_exists(cognito, pool_id)
        policy = pool["Policies"]["PasswordPolicy"]
        assert policy["MinimumLength"] == 8
        assert policy["RequireLowercase"] is True
        assert policy["RequireNumbers"] is True

    def test_client_exists(self, terraform_dir, tf_runner):
        tf_runner.apply(terraform_dir)
        outputs = tf_runner.output(terraform_dir)
        pool_id = outputs["user_pool_id"]["value"]
        client_id = outputs["client_id"]["value"]

        cognito = make_client("cognito-idp")
        resp = cognito.describe_user_pool_client(UserPoolId=pool_id, ClientId=client_id)
        client = resp["UserPoolClient"]
        assert client["ClientId"] == client_id
        assert "ALLOW_USER_PASSWORD_AUTH" in client["ExplicitAuthFlows"]
        assert "ALLOW_REFRESH_TOKEN_AUTH" in client["ExplicitAuthFlows"]
