"""IaC test: cloudformation - auth_stack.

Deploys a Cognito UserPool with password policy and auto-verified email,
plus a UserPoolClient. Validates resources and stack outputs.
"""

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.resource_validator import assert_cognito_user_pool_exists

pytestmark = pytest.mark.iac

TEMPLATE = (Path(__file__).parent / "template.yaml").read_text()


class TestAuthStack:
    """CloudFormation auth stack with Cognito UserPool and Client."""

    def test_deploy_and_validate(self, deploy_stack):
        stack = deploy_stack("auth", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        # Extract outputs into a dict
        outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
        assert "UserPoolId" in outputs
        assert "UserPoolClientId" in outputs

        pool_id = outputs["UserPoolId"]
        client_id = outputs["UserPoolClientId"]

        # Validate user pool exists and has correct settings
        cognito = make_client("cognito-idp")
        pool = assert_cognito_user_pool_exists(cognito, pool_id)
        assert pool["Name"].endswith("-userpool")

        password_policy = pool.get("Policies", {}).get("PasswordPolicy", {})
        assert password_policy.get("MinimumLength") == 12
        assert password_policy.get("RequireLowercase") is True
        assert password_policy.get("RequireUppercase") is True
        assert password_policy.get("RequireNumbers") is True
        assert password_policy.get("RequireSymbols") is True

        auto_verified = pool.get("AutoVerifiedAttributes", [])
        assert "email" in auto_verified

        # Validate user pool client exists
        resp = cognito.describe_user_pool_client(UserPoolId=pool_id, ClientId=client_id)
        client_desc = resp["UserPoolClient"]
        assert client_desc["ClientId"] == client_id
        assert client_desc["ClientName"].endswith("-client")
        assert "ALLOW_USER_PASSWORD_AUTH" in client_desc.get("ExplicitAuthFlows", [])
        assert "ALLOW_REFRESH_TOKEN_AUTH" in client_desc.get("ExplicitAuthFlows", [])
