"""IaC test: pulumi - auth_stack.

Validates Cognito user pool and app client creation.
Resources are created via boto3 (mirroring the Pulumi program).
"""

from __future__ import annotations

import pytest

from tests.iac.helpers.functional_validator import create_cognito_user_and_auth
from tests.iac.helpers.resource_validator import assert_cognito_user_pool_exists

pytestmark = pytest.mark.iac


@pytest.fixture(scope="module")
def auth_resources(cognito_client):
    """Create Cognito user pool and app client via boto3."""
    pool = cognito_client.create_user_pool(
        PoolName="auth-user-pool",
        Policies={
            "PasswordPolicy": {
                "MinimumLength": 8,
                "RequireLowercase": True,
                "RequireNumbers": True,
                "RequireSymbols": False,
                "RequireUppercase": True,
            }
        },
        AutoVerifiedAttributes=["email"],
    )
    pool_id = pool["UserPool"]["Id"]

    client = cognito_client.create_user_pool_client(
        UserPoolId=pool_id,
        ClientName="auth-app-client",
        ExplicitAuthFlows=[
            "ALLOW_USER_PASSWORD_AUTH",
            "ALLOW_REFRESH_TOKEN_AUTH",
        ],
        GenerateSecret=False,
    )
    client_id = client["UserPoolClient"]["ClientId"]

    yield {
        "user_pool_id": pool_id,
        "app_client_id": client_id,
    }

    # Cleanup
    cognito_client.delete_user_pool_client(UserPoolId=pool_id, ClientId=client_id)
    cognito_client.delete_user_pool(UserPoolId=pool_id)


class TestAuthStack:
    """Pulumi auth stack: Cognito user pool + app client."""

    def test_user_pool_created(self, auth_resources, cognito_client):
        pool_id = auth_resources["user_pool_id"]
        pool = assert_cognito_user_pool_exists(cognito_client, pool_id)
        assert pool["Name"] == "auth-user-pool"

        policy = pool["Policies"]["PasswordPolicy"]
        assert policy["MinimumLength"] == 8
        assert policy["RequireLowercase"] is True
        assert policy["RequireNumbers"] is True

    def test_app_client_created(self, auth_resources, cognito_client):
        pool_id = auth_resources["user_pool_id"]
        client_id = auth_resources["app_client_id"]

        resp = cognito_client.describe_user_pool_client(UserPoolId=pool_id, ClientId=client_id)
        client = resp["UserPoolClient"]
        assert client["ClientId"] == client_id
        assert "ALLOW_USER_PASSWORD_AUTH" in client["ExplicitAuthFlows"]
        assert "ALLOW_REFRESH_TOKEN_AUTH" in client["ExplicitAuthFlows"]

    def test_cognito_auth_flow(self, auth_resources, cognito_client):
        """Create a user and authenticate via Cognito."""
        pool_id = auth_resources["user_pool_id"]
        client_id = auth_resources["app_client_id"]
        resp = create_cognito_user_and_auth(
            cognito_client,
            pool_id,
            client_id,
            "testuser@example.com",
            "TestP@ss1234",
        )
        assert "AuthenticationResult" in resp
        assert "AccessToken" in resp["AuthenticationResult"]
