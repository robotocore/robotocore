"""IaC test: cdk - auth_stack.

Deploys a Cognito User Pool with password policy and auto-verified email,
plus a UserPoolClient and IAM role. Validates resources via boto3.
"""

from pathlib import Path

import pytest

from tests.iac.conftest import make_client

pytestmark = pytest.mark.iac

SCENARIO_DIR = Path(__file__).parent


class TestAuthStack:
    """CDK auth stack with Cognito UserPool, AppClient, and IAM role."""

    @pytest.fixture(autouse=True)
    def deploy(self, cdk_runner):
        """Deploy the CDK app and tear it down after tests."""
        result = cdk_runner.deploy(SCENARIO_DIR, "AuthStack")
        assert result.returncode == 0, f"cdk deploy failed: {result.stderr}"
        yield
        cdk_runner.destroy(SCENARIO_DIR, "AuthStack")

    def test_user_pool_created(self):
        """Verify Cognito user pool exists with correct settings."""
        cognito = make_client("cognito-idp")
        resp = cognito.list_user_pools(MaxResults=60)
        pools = [p for p in resp["UserPools"] if p["Name"] == "auth-userpool"]
        assert len(pools) >= 1, "User pool 'auth-userpool' not found"

        pool_id = pools[0]["Id"]
        detail = cognito.describe_user_pool(UserPoolId=pool_id)["UserPool"]
        assert detail["Name"] == "auth-userpool"

        password_policy = detail.get("Policies", {}).get("PasswordPolicy", {})
        assert password_policy.get("MinimumLength") == 12

    def test_app_client_created(self):
        """Verify app client exists on the user pool."""
        cognito = make_client("cognito-idp")
        resp = cognito.list_user_pools(MaxResults=60)
        pools = [p for p in resp["UserPools"] if p["Name"] == "auth-userpool"]
        assert len(pools) >= 1

        pool_id = pools[0]["Id"]
        clients_resp = cognito.list_user_pool_clients(UserPoolId=pool_id, MaxResults=10)
        clients = clients_resp["UserPoolClients"]
        matching = [c for c in clients if c["ClientName"] == "auth-client"]
        assert len(matching) >= 1, "App client 'auth-client' not found"
