"""IaC test: pulumi - auth_stack.

Validates Cognito user pool and app client creation via Pulumi.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.resource_validator import assert_cognito_user_pool_exists

pytestmark = pytest.mark.iac

SCENARIO_DIR = Path(__file__).parent


@pytest.fixture(scope="module")
def stack_outputs(pulumi_runner):
    """Deploy the auth stack and return Pulumi outputs."""
    result = pulumi_runner.up(SCENARIO_DIR)
    if result.returncode != 0:
        pytest.fail(f"pulumi up failed:\n{result.stderr}")
    yield pulumi_runner.stack_output(SCENARIO_DIR)
    pulumi_runner.destroy(SCENARIO_DIR)


@pytest.fixture(scope="module")
def cognito_client():
    return make_client("cognito-idp")


class TestAuthStack:
    """Pulumi auth stack: Cognito user pool + app client."""

    def test_user_pool_created(self, stack_outputs, cognito_client):
        pool_id = stack_outputs["user_pool_id"]
        pool = assert_cognito_user_pool_exists(cognito_client, pool_id)
        assert pool["Name"] == "auth-user-pool"

        policy = pool["Policies"]["PasswordPolicy"]
        assert policy["MinimumLength"] == 8
        assert policy["RequireLowercase"] is True
        assert policy["RequireNumbers"] is True

    def test_app_client_created(self, stack_outputs, cognito_client):
        pool_id = stack_outputs["user_pool_id"]
        client_id = stack_outputs["app_client_id"]

        resp = cognito_client.describe_user_pool_client(UserPoolId=pool_id, ClientId=client_id)
        client = resp["UserPoolClient"]
        assert client["ClientId"] == client_id
        assert "ALLOW_USER_PASSWORD_AUTH" in client["ExplicitAuthFlows"]
        assert "ALLOW_REFRESH_TOKEN_AUTH" in client["ExplicitAuthFlows"]
