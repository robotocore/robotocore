"""Functional test: deploy auth stack and exercise Cognito user creation and auth."""

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.functional_validator import create_cognito_user_and_auth

pytestmark = pytest.mark.iac

TEMPLATE = (Path(__file__).parent / "template.yaml").read_text()


def _get_outputs(stack: dict) -> dict[str, str]:
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


class TestAuthStackFunctional:
    """Deploy auth stack and verify Cognito user creation and authentication."""

    def test_create_user_and_authenticate(self, deploy_stack):
        """Create a Cognito user and authenticate to get JWT tokens."""
        stack = deploy_stack("auth-func", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        pool_id = outputs["UserPoolId"]
        client_id = outputs["UserPoolClientId"]
        cognito = make_client("cognito-idp")

        auth_resp = create_cognito_user_and_auth(
            cognito,
            pool_id,
            client_id,
            username="testuser1",
            password="TestP@ss1234",
        )

        result = auth_resp["AuthenticationResult"]
        assert "AccessToken" in result
        assert "IdToken" in result
        assert "RefreshToken" in result
        assert "TokenType" in result

    def test_second_user_auth(self, deploy_stack):
        """Create and authenticate a second distinct user."""
        stack = deploy_stack("auth-func-2", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        pool_id = outputs["UserPoolId"]
        client_id = outputs["UserPoolClientId"]
        cognito = make_client("cognito-idp")

        auth_resp = create_cognito_user_and_auth(
            cognito,
            pool_id,
            client_id,
            username="anotheruser",
            password="Str0ng!Pass99",
        )

        result = auth_resp["AuthenticationResult"]
        assert len(result["AccessToken"]) > 0
        assert len(result["IdToken"]) > 0

    def test_user_exists_after_creation(self, deploy_stack):
        """Verify the user appears in the user pool after creation."""
        stack = deploy_stack("auth-func-list", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        pool_id = outputs["UserPoolId"]
        client_id = outputs["UserPoolClientId"]
        cognito = make_client("cognito-idp")

        create_cognito_user_and_auth(cognito, pool_id, client_id, "listuser", "MyP@ssw0rd12")

        users_resp = cognito.list_users(UserPoolId=pool_id)
        usernames = [u["Username"] for u in users_resp["Users"]]
        assert "listuser" in usernames
