"""Failing tests exposing bugs in the Cognito Identity Provider.

Each test targets a specific correctness bug discovered during audit.
All tests are expected to FAIL against the current implementation.
"""

import base64
import hashlib
import hmac
import json

import pytest

from robotocore.services.cognito.provider import (
    _generate_jwt,
    _secret_hash,
    _stores,
    handle_cognito_request,
)

# ---------------------------------------------------------------------------
# Helpers (copied from existing test file)
# ---------------------------------------------------------------------------


def _make_request(action: str, body: dict):
    from starlette.requests import Request

    target = f"AWSCognitoIdentityProviderService.{action}"
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "query_string": b"",
        "headers": [(b"x-amz-target", target.encode())],
    }
    body_bytes = json.dumps(body).encode()

    async def receive():
        return {"type": "http.request", "body": body_bytes}

    return Request(scope, receive)


@pytest.fixture(autouse=True)
def _clear_stores():
    _stores.clear()
    yield
    _stores.clear()


async def _create_pool(region="us-east-1", account="123456789012"):
    req = _make_request("CreateUserPool", {"PoolName": "TestPool"})
    resp = await handle_cognito_request(req, region, account)
    data = json.loads(resp.body)
    return data["UserPool"]["Id"]


async def _create_pool_and_client(region="us-east-1", account="123456789012"):
    pool_id = await _create_pool(region, account)
    req = _make_request(
        "CreateUserPoolClient",
        {"UserPoolId": pool_id, "ClientName": "TestClient"},
    )
    resp = await handle_cognito_request(req, region, account)
    data = json.loads(resp.body)
    return pool_id, data["UserPoolClient"]["ClientId"]


async def _signup_and_confirm(pool_id, client_id, username="user1", password="Pass1!"):
    """Sign up a user and confirm them, return the username."""
    await handle_cognito_request(
        _make_request(
            "SignUp",
            {"ClientId": client_id, "Username": username, "Password": password},
        ),
        "us-east-1",
        "123456789012",
    )
    await handle_cognito_request(
        _make_request("AdminConfirmSignUp", {"UserPoolId": pool_id, "Username": username}),
        "us-east-1",
        "123456789012",
    )
    return username


async def _authenticate(pool_id, client_id, username, password):
    """Authenticate and return the auth result dict."""
    resp = await handle_cognito_request(
        _make_request(
            "InitiateAuth",
            {
                "AuthFlow": "USER_PASSWORD_AUTH",
                "ClientId": client_id,
                "AuthParameters": {"USERNAME": username, "PASSWORD": password},
            },
        ),
        "us-east-1",
        "123456789012",
    )
    return json.loads(resp.body), resp.status_code


def _decode_jwt_payload(token: str) -> dict:
    payload_part = token.split(".")[1]
    padding = 4 - len(payload_part) % 4
    if padding != 4:
        payload_part += "=" * padding
    return json.loads(base64.urlsafe_b64decode(payload_part))


# ---------------------------------------------------------------------------
# Bug 1: _secret_hash uses SHA-256 instead of HMAC-SHA256
# AWS Cognito uses HMAC_SHA256(client_secret, username + client_id) but
# the implementation does SHA256(username + client_id + client_secret).
# ---------------------------------------------------------------------------


class TestSecretHashBug:
    def test_secret_hash_uses_hmac_sha256(self):
        """_secret_hash should use HMAC-SHA256 as AWS requires, not plain SHA-256."""
        username = "testuser"
        client_id = "abc123"
        client_secret = "supersecret"

        # Correct AWS implementation: HMAC-SHA256
        msg = (username + client_id).encode("utf-8")
        expected = base64.b64encode(
            hmac.new(client_secret.encode("utf-8"), msg, hashlib.sha256).digest()
        ).decode("utf-8")

        actual = _secret_hash(username, client_id, client_secret)
        assert actual == expected, (
            f"_secret_hash should use HMAC-SHA256 but got wrong result. "
            f"Expected {expected}, got {actual}"
        )


# ---------------------------------------------------------------------------
# Bug 2: _admin_update_user_attributes sets wrong key for last modified date
# It sets user["UserLastModifiedDate"] but the user dict uses "LastModifiedDate".
# So the actual LastModifiedDate is never updated.
# ---------------------------------------------------------------------------


class TestAdminUpdateUserAttributesBug:
    @pytest.mark.asyncio
    async def test_update_user_attributes_updates_last_modified_date(self):
        """AdminUpdateUserAttributes should update LastModifiedDate, not UserLastModifiedDate."""
        pool_id = await _create_pool()
        await handle_cognito_request(
            _make_request(
                "AdminCreateUser",
                {
                    "UserPoolId": pool_id,
                    "Username": "user1",
                    "UserAttributes": [{"Name": "email", "Value": "old@test.com"}],
                },
            ),
            "us-east-1",
            "123456789012",
        )

        # Get the original LastModifiedDate
        resp1 = await handle_cognito_request(
            _make_request("AdminGetUser", {"UserPoolId": pool_id, "Username": "user1"}),
            "us-east-1",
            "123456789012",
        )
        original_date = json.loads(resp1.body)["UserLastModifiedDate"]

        # Wait a tiny bit so timestamps differ
        import time

        time.sleep(0.01)

        # Update attributes
        await handle_cognito_request(
            _make_request(
                "AdminUpdateUserAttributes",
                {
                    "UserPoolId": pool_id,
                    "Username": "user1",
                    "UserAttributes": [{"Name": "email", "Value": "new@test.com"}],
                },
            ),
            "us-east-1",
            "123456789012",
        )

        # Get user again - LastModifiedDate should have changed
        resp2 = await handle_cognito_request(
            _make_request("AdminGetUser", {"UserPoolId": pool_id, "Username": "user1"}),
            "us-east-1",
            "123456789012",
        )
        data = json.loads(resp2.body)
        new_date = data["UserLastModifiedDate"]

        assert new_date > original_date, (
            f"LastModifiedDate should be updated after AdminUpdateUserAttributes. "
            f"Original: {original_date}, After update: {new_date}"
        )


# ---------------------------------------------------------------------------
# Bug 3: No password policy validation on SignUp
# AWS enforces password policy (min length, uppercase, lowercase, numbers,
# special chars) but this implementation accepts any password.
# ---------------------------------------------------------------------------


class TestPasswordPolicyBug:
    @pytest.mark.asyncio
    async def test_signup_rejects_weak_password_when_policy_set(self):
        """SignUp should reject passwords that don't meet the pool's password policy."""
        # Create pool with strict password policy
        req = _make_request(
            "CreateUserPool",
            {
                "PoolName": "StrictPool",
                "Policies": {
                    "PasswordPolicy": {
                        "MinimumLength": 8,
                        "RequireUppercase": True,
                        "RequireLowercase": True,
                        "RequireNumbers": True,
                        "RequireSymbols": True,
                    }
                },
            },
        )
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        pool_id = json.loads(resp.body)["UserPool"]["Id"]

        # Create a client for this pool
        client_resp = await handle_cognito_request(
            _make_request(
                "CreateUserPoolClient",
                {"UserPoolId": pool_id, "ClientName": "TestClient"},
            ),
            "us-east-1",
            "123456789012",
        )
        client_id = json.loads(client_resp.body)["UserPoolClient"]["ClientId"]

        # Try to sign up with a weak password (too short, no uppercase, no special chars)
        signup_resp = await handle_cognito_request(
            _make_request(
                "SignUp",
                {"ClientId": client_id, "Username": "user1", "Password": "weak"},
            ),
            "us-east-1",
            "123456789012",
        )

        assert signup_resp.status_code == 400, (
            "SignUp should reject a weak password ('weak') when the pool has a strict "
            "password policy, but it was accepted"
        )
        data = json.loads(signup_resp.body)
        assert data["__type"] == "InvalidPasswordException", (
            f"Expected InvalidPasswordException but got {data.get('__type')}"
        )


# ---------------------------------------------------------------------------
# Bug 4: Disabled users can still authenticate
# _authenticate_user checks UNCONFIRMED and FORCE_CHANGE_PASSWORD but
# never checks user["Enabled"]. A disabled user should not be able to log in.
# ---------------------------------------------------------------------------


class TestDisabledUserAuthBug:
    @pytest.mark.asyncio
    async def test_disabled_user_cannot_authenticate(self):
        """A disabled user should not be able to authenticate."""
        pool_id, client_id = await _create_pool_and_client()
        await _signup_and_confirm(pool_id, client_id, "user1", "Pass1!")

        # Disable the user
        await handle_cognito_request(
            _make_request("AdminDisableUser", {"UserPoolId": pool_id, "Username": "user1"}),
            "us-east-1",
            "123456789012",
        )

        # Try to authenticate - should fail
        auth_data, status_code = await _authenticate(pool_id, client_id, "user1", "Pass1!")

        assert status_code == 400, (
            "Disabled user should not be able to authenticate, but auth succeeded"
        )
        assert "NotAuthorizedException" in json.dumps(auth_data), (
            "Expected NotAuthorizedException for disabled user"
        )


# ---------------------------------------------------------------------------
# Bug 5: JWT access tokens incorrectly include 'aud' claim
# AWS Cognito access tokens do NOT have an 'aud' claim -- they have 'client_id'.
# Only ID tokens have 'aud'. The current implementation puts 'aud' on both.
# ---------------------------------------------------------------------------


class TestJwtAccessTokenAudBug:
    def test_access_token_should_have_client_id_not_aud(self):
        """Access tokens should have 'client_id' claim, not 'aud'."""
        token = _generate_jwt("sub-123", "https://issuer", "my-client-id", "access")
        payload = _decode_jwt_payload(token)

        # Access tokens should have client_id, not aud
        assert "client_id" in payload, (
            "Access token should contain 'client_id' claim but it's missing"
        )
        assert payload["client_id"] == "my-client-id"

    def test_id_token_should_have_aud(self):
        """ID tokens should have 'aud' claim (this already works)."""
        token = _generate_jwt("sub-123", "https://issuer", "my-client-id", "id")
        payload = _decode_jwt_payload(token)
        assert "aud" in payload
        assert payload["aud"] == "my-client-id"


# ---------------------------------------------------------------------------
# Bug 6: REFRESH_TOKEN_AUTH flow not supported in InitiateAuth
# AWS Cognito supports REFRESH_TOKEN and REFRESH_TOKEN_AUTH flows to get
# new access/id tokens using a refresh token. The current implementation
# only supports USER_PASSWORD_AUTH.
# ---------------------------------------------------------------------------


class TestRefreshTokenFlowBug:
    @pytest.mark.asyncio
    async def test_refresh_token_auth_flow(self):
        """InitiateAuth with REFRESH_TOKEN_AUTH should return new tokens."""
        pool_id, client_id = await _create_pool_and_client()
        await _signup_and_confirm(pool_id, client_id, "user1", "Pass1!")

        # Get initial tokens
        auth_data, status = await _authenticate(pool_id, client_id, "user1", "Pass1!")
        assert status == 200
        refresh_token = auth_data["AuthenticationResult"]["RefreshToken"]

        # Use refresh token to get new tokens
        refresh_resp = await handle_cognito_request(
            _make_request(
                "InitiateAuth",
                {
                    "AuthFlow": "REFRESH_TOKEN_AUTH",
                    "ClientId": client_id,
                    "AuthParameters": {"REFRESH_TOKEN": refresh_token},
                },
            ),
            "us-east-1",
            "123456789012",
        )

        assert refresh_resp.status_code == 200, (
            f"REFRESH_TOKEN_AUTH should succeed but got status {refresh_resp.status_code}. "
            f"Response: {refresh_resp.body.decode()}"
        )
        data = json.loads(refresh_resp.body)
        assert "AuthenticationResult" in data
        assert "AccessToken" in data["AuthenticationResult"]
        assert "IdToken" in data["AuthenticationResult"]


# ---------------------------------------------------------------------------
# Bug 7: SignUp does not validate password at all (no password param validation)
# Even without a pool password policy, AWS has a default policy:
# minimum 8 chars, at least one uppercase, lowercase, number, special char.
# The implementation accepts empty passwords.
# ---------------------------------------------------------------------------


class TestSignupEmptyPasswordBug:
    @pytest.mark.asyncio
    async def test_signup_rejects_empty_password(self):
        """SignUp should reject an empty password."""
        pool_id, client_id = await _create_pool_and_client()

        resp = await handle_cognito_request(
            _make_request(
                "SignUp",
                {"ClientId": client_id, "Username": "user1", "Password": ""},
            ),
            "us-east-1",
            "123456789012",
        )

        assert resp.status_code == 400, "SignUp should reject an empty password but it was accepted"
