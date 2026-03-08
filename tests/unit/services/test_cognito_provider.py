"""Unit tests for the Cognito Identity Provider."""

import base64
import json

import pytest
from starlette.requests import Request

from robotocore.services.cognito.provider import (
    CognitoError,
    _error,
    _generate_jwt,
    _get_store,
    _json_response,
    _stores,
    handle_cognito_request,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(action: str, body: dict):
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
    """Helper to create a pool and return pool_id."""
    req = _make_request("CreateUserPool", {"PoolName": "TestPool"})
    resp = await handle_cognito_request(req, region, account)
    data = json.loads(resp.body)
    return data["UserPool"]["Id"]


async def _create_pool_and_client(region="us-east-1", account="123456789012"):
    """Helper to create pool + client, return (pool_id, client_id)."""
    pool_id = await _create_pool(region, account)
    req = _make_request(
        "CreateUserPoolClient",
        {"UserPoolId": pool_id, "ClientName": "TestClient"},
    )
    resp = await handle_cognito_request(req, region, account)
    data = json.loads(resp.body)
    return pool_id, data["UserPoolClient"]["ClientId"]


# ---------------------------------------------------------------------------
# Error and response helpers
# ---------------------------------------------------------------------------


class TestCognitoError:
    def test_default_status(self):
        e = CognitoError("Code", "msg")
        assert e.status == 400

    def test_custom_status(self):
        e = CognitoError("Code", "msg", 404)
        assert e.status == 404


class TestResponseHelpers:
    def test_json_response(self):
        resp = _json_response({"key": "val"})
        assert resp.status_code == 200
        assert json.loads(resp.body) == {"key": "val"}

    def test_error_response(self):
        resp = _error("TestCode", "test msg", 404)
        assert resp.status_code == 404
        data = json.loads(resp.body)
        assert data["__type"] == "TestCode"


# ---------------------------------------------------------------------------
# JWT generation
# ---------------------------------------------------------------------------


class TestJwtGeneration:
    def test_jwt_structure(self):
        token = _generate_jwt("sub-123", "https://issuer", "client-id", "access")
        parts = token.split(".")
        assert len(parts) == 3

    def test_jwt_header(self):
        token = _generate_jwt("sub-123", "https://issuer", "client-id", "access")
        header_part = token.split(".")[0]
        padding = 4 - len(header_part) % 4
        if padding != 4:
            header_part += "=" * padding
        header = json.loads(base64.urlsafe_b64decode(header_part))
        assert header["alg"] == "RS256"
        assert header["typ"] == "JWT"

    def test_jwt_payload(self):
        token = _generate_jwt("sub-123", "https://issuer", "client-id", "access")
        payload_part = token.split(".")[1]
        padding = 4 - len(payload_part) % 4
        if padding != 4:
            payload_part += "=" * padding
        payload = json.loads(base64.urlsafe_b64decode(payload_part))
        assert payload["sub"] == "sub-123"
        assert payload["iss"] == "https://issuer"
        assert payload["aud"] == "client-id"
        assert payload["token_use"] == "access"
        assert "exp" in payload
        assert "iat" in payload

    def test_jwt_extra_claims(self):
        token = _generate_jwt(
            "sub-123",
            "https://issuer",
            "client-id",
            "id",
            {"cognito:username": "john"},
        )
        payload_part = token.split(".")[1]
        padding = 4 - len(payload_part) % 4
        if padding != 4:
            payload_part += "=" * padding
        payload = json.loads(base64.urlsafe_b64decode(payload_part))
        assert payload["cognito:username"] == "john"


# ---------------------------------------------------------------------------
# User Pool CRUD
# ---------------------------------------------------------------------------


class TestUserPoolCrud:
    @pytest.mark.asyncio
    async def test_create_user_pool(self):
        req = _make_request("CreateUserPool", {"PoolName": "MyPool"})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["UserPool"]["Name"] == "MyPool"
        assert "Id" in data["UserPool"]

    @pytest.mark.asyncio
    async def test_create_pool_missing_name(self):
        req = _make_request("CreateUserPool", {})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_describe_user_pool(self):
        pool_id = await _create_pool()
        req = _make_request("DescribeUserPool", {"UserPoolId": pool_id})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["UserPool"]["Id"] == pool_id

    @pytest.mark.asyncio
    async def test_describe_nonexistent_pool(self):
        req = _make_request("DescribeUserPool", {"UserPoolId": "nope"})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_user_pool(self):
        pool_id = await _create_pool()
        req = _make_request("DeleteUserPool", {"UserPoolId": pool_id})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

        req2 = _make_request("DescribeUserPool", {"UserPoolId": pool_id})
        resp2 = await handle_cognito_request(req2, "us-east-1", "123456789012")
        assert resp2.status_code == 404

    @pytest.mark.asyncio
    async def test_list_user_pools(self):
        await _create_pool()
        await _create_pool()
        req = _make_request("ListUserPools", {"MaxResults": 10})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        assert len(data["UserPools"]) == 2


# ---------------------------------------------------------------------------
# User Pool Client CRUD
# ---------------------------------------------------------------------------


class TestUserPoolClientCrud:
    @pytest.mark.asyncio
    async def test_create_client(self):
        pool_id = await _create_pool()
        req = _make_request(
            "CreateUserPoolClient",
            {"UserPoolId": pool_id, "ClientName": "MyClient"},
        )
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["UserPoolClient"]["ClientName"] == "MyClient"

    @pytest.mark.asyncio
    async def test_create_client_with_secret(self):
        pool_id = await _create_pool()
        req = _make_request(
            "CreateUserPoolClient",
            {"UserPoolId": pool_id, "ClientName": "SecretClient", "GenerateSecret": True},
        )
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        assert "ClientSecret" in data["UserPoolClient"]

    @pytest.mark.asyncio
    async def test_describe_client(self):
        pool_id, client_id = await _create_pool_and_client()
        req = _make_request(
            "DescribeUserPoolClient",
            {"UserPoolId": pool_id, "ClientId": client_id},
        )
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_delete_client(self):
        pool_id, client_id = await _create_pool_and_client()
        req = _make_request(
            "DeleteUserPoolClient",
            {"UserPoolId": pool_id, "ClientId": client_id},
        )
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_list_clients(self):
        pool_id, _ = await _create_pool_and_client()
        req = _make_request("ListUserPoolClients", {"UserPoolId": pool_id})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        assert len(data["UserPoolClients"]) == 1


# ---------------------------------------------------------------------------
# Auth flows
# ---------------------------------------------------------------------------


class TestAuthFlows:
    @pytest.mark.asyncio
    async def test_sign_up(self):
        pool_id, client_id = await _create_pool_and_client()
        req = _make_request(
            "SignUp",
            {
                "ClientId": client_id,
                "Username": "testuser",
                "Password": "P@ssw0rd!",
            },
        )
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["UserConfirmed"] is False
        assert "UserSub" in data

    @pytest.mark.asyncio
    async def test_sign_up_duplicate(self):
        pool_id, client_id = await _create_pool_and_client()
        req = _make_request(
            "SignUp", {"ClientId": client_id, "Username": "testuser", "Password": "P@ss1!"}
        )
        await handle_cognito_request(req, "us-east-1", "123456789012")
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_admin_confirm_sign_up(self):
        pool_id, client_id = await _create_pool_and_client()
        req = _make_request(
            "SignUp", {"ClientId": client_id, "Username": "testuser", "Password": "P@ss1!"}
        )
        await handle_cognito_request(req, "us-east-1", "123456789012")

        req2 = _make_request("AdminConfirmSignUp", {"UserPoolId": pool_id, "Username": "testuser"})
        resp = await handle_cognito_request(req2, "us-east-1", "123456789012")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_initiate_auth_user_password(self):
        pool_id, client_id = await _create_pool_and_client()
        # Sign up and confirm
        await handle_cognito_request(
            _make_request(
                "SignUp", {"ClientId": client_id, "Username": "user1", "Password": "Pass1!"}
            ),
            "us-east-1",
            "123456789012",
        )
        await handle_cognito_request(
            _make_request("AdminConfirmSignUp", {"UserPoolId": pool_id, "Username": "user1"}),
            "us-east-1",
            "123456789012",
        )

        req = _make_request(
            "InitiateAuth",
            {
                "AuthFlow": "USER_PASSWORD_AUTH",
                "ClientId": client_id,
                "AuthParameters": {"USERNAME": "user1", "PASSWORD": "Pass1!"},
            },
        )
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert "AuthenticationResult" in data
        assert "AccessToken" in data["AuthenticationResult"]
        assert "IdToken" in data["AuthenticationResult"]

    @pytest.mark.asyncio
    async def test_initiate_auth_wrong_password(self):
        pool_id, client_id = await _create_pool_and_client()
        await handle_cognito_request(
            _make_request(
                "SignUp", {"ClientId": client_id, "Username": "user1", "Password": "Pass1!"}
            ),
            "us-east-1",
            "123456789012",
        )
        await handle_cognito_request(
            _make_request("AdminConfirmSignUp", {"UserPoolId": pool_id, "Username": "user1"}),
            "us-east-1",
            "123456789012",
        )

        req = _make_request(
            "InitiateAuth",
            {
                "AuthFlow": "USER_PASSWORD_AUTH",
                "ClientId": client_id,
                "AuthParameters": {"USERNAME": "user1", "PASSWORD": "Wrong!"},
            },
        )
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_initiate_auth_unconfirmed(self):
        pool_id, client_id = await _create_pool_and_client()
        await handle_cognito_request(
            _make_request(
                "SignUp", {"ClientId": client_id, "Username": "user1", "Password": "Pass1!"}
            ),
            "us-east-1",
            "123456789012",
        )

        req = _make_request(
            "InitiateAuth",
            {
                "AuthFlow": "USER_PASSWORD_AUTH",
                "ClientId": client_id,
                "AuthParameters": {"USERNAME": "user1", "PASSWORD": "Pass1!"},
            },
        )
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_admin_initiate_auth(self):
        pool_id, client_id = await _create_pool_and_client()
        await handle_cognito_request(
            _make_request(
                "SignUp", {"ClientId": client_id, "Username": "user1", "Password": "Pass1!"}
            ),
            "us-east-1",
            "123456789012",
        )
        await handle_cognito_request(
            _make_request("AdminConfirmSignUp", {"UserPoolId": pool_id, "Username": "user1"}),
            "us-east-1",
            "123456789012",
        )

        req = _make_request(
            "AdminInitiateAuth",
            {
                "UserPoolId": pool_id,
                "ClientId": client_id,
                "AuthFlow": "ADMIN_USER_PASSWORD_AUTH",
                "AuthParameters": {"USERNAME": "user1", "PASSWORD": "Pass1!"},
            },
        )
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert "AuthenticationResult" in data

    @pytest.mark.asyncio
    async def test_respond_to_new_password_challenge(self):
        pool_id, client_id = await _create_pool_and_client()
        # AdminCreateUser creates FORCE_CHANGE_PASSWORD
        await handle_cognito_request(
            _make_request("AdminCreateUser", {"UserPoolId": pool_id, "Username": "admin_user"}),
            "us-east-1",
            "123456789012",
        )

        # Auth should return NEW_PASSWORD_REQUIRED challenge
        auth_req = _make_request(
            "AdminInitiateAuth",
            {
                "UserPoolId": pool_id,
                "ClientId": client_id,
                "AuthFlow": "ADMIN_USER_PASSWORD_AUTH",
                "AuthParameters": {"USERNAME": "admin_user", "PASSWORD": "TempPass1!"},
            },
        )
        auth_resp = await handle_cognito_request(auth_req, "us-east-1", "123456789012")
        auth_data = json.loads(auth_resp.body)
        assert auth_data["ChallengeName"] == "NEW_PASSWORD_REQUIRED"

        # Respond to challenge
        challenge_req = _make_request(
            "RespondToAuthChallenge",
            {
                "ClientId": client_id,
                "ChallengeName": "NEW_PASSWORD_REQUIRED",
                "ChallengeResponses": {
                    "USERNAME": "admin_user",
                    "NEW_PASSWORD": "NewP@ss1!",
                },
            },
        )
        challenge_resp = await handle_cognito_request(challenge_req, "us-east-1", "123456789012")
        assert challenge_resp.status_code == 200
        data = json.loads(challenge_resp.body)
        assert "AuthenticationResult" in data


# ---------------------------------------------------------------------------
# User management
# ---------------------------------------------------------------------------


class TestUserManagement:
    @pytest.mark.asyncio
    async def test_get_user_by_token(self):
        pool_id, client_id = await _create_pool_and_client()
        await handle_cognito_request(
            _make_request(
                "SignUp", {"ClientId": client_id, "Username": "user1", "Password": "Pass1!"}
            ),
            "us-east-1",
            "123456789012",
        )
        await handle_cognito_request(
            _make_request("AdminConfirmSignUp", {"UserPoolId": pool_id, "Username": "user1"}),
            "us-east-1",
            "123456789012",
        )
        auth_resp = await handle_cognito_request(
            _make_request(
                "InitiateAuth",
                {
                    "AuthFlow": "USER_PASSWORD_AUTH",
                    "ClientId": client_id,
                    "AuthParameters": {"USERNAME": "user1", "PASSWORD": "Pass1!"},
                },
            ),
            "us-east-1",
            "123456789012",
        )
        token = json.loads(auth_resp.body)["AuthenticationResult"]["AccessToken"]

        req = _make_request("GetUser", {"AccessToken": token})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["Username"] == "user1"

    @pytest.mark.asyncio
    async def test_admin_get_user(self):
        pool_id, client_id = await _create_pool_and_client()
        await handle_cognito_request(
            _make_request("AdminCreateUser", {"UserPoolId": pool_id, "Username": "admin_user"}),
            "us-east-1",
            "123456789012",
        )

        req = _make_request("AdminGetUser", {"UserPoolId": pool_id, "Username": "admin_user"})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["Username"] == "admin_user"

    @pytest.mark.asyncio
    async def test_admin_create_user(self):
        pool_id = await _create_pool()
        req = _make_request(
            "AdminCreateUser",
            {
                "UserPoolId": pool_id,
                "Username": "newuser",
                "TemporaryPassword": "Temp1!",
            },
        )
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["User"]["UserStatus"] == "FORCE_CHANGE_PASSWORD"

    @pytest.mark.asyncio
    async def test_admin_delete_user(self):
        pool_id = await _create_pool()
        await handle_cognito_request(
            _make_request("AdminCreateUser", {"UserPoolId": pool_id, "Username": "tobedeleted"}),
            "us-east-1",
            "123456789012",
        )

        req = _make_request("AdminDeleteUser", {"UserPoolId": pool_id, "Username": "tobedeleted"})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

        # Verify deleted
        req2 = _make_request("AdminGetUser", {"UserPoolId": pool_id, "Username": "tobedeleted"})
        resp2 = await handle_cognito_request(req2, "us-east-1", "123456789012")
        assert resp2.status_code == 404


# ---------------------------------------------------------------------------
# Password operations
# ---------------------------------------------------------------------------


class TestPasswordOps:
    @pytest.mark.asyncio
    async def test_forgot_password(self):
        pool_id, client_id = await _create_pool_and_client()
        await handle_cognito_request(
            _make_request(
                "SignUp", {"ClientId": client_id, "Username": "user1", "Password": "Pass1!"}
            ),
            "us-east-1",
            "123456789012",
        )

        req = _make_request("ForgotPassword", {"ClientId": client_id, "Username": "user1"})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert "CodeDeliveryDetails" in json.loads(resp.body)

    @pytest.mark.asyncio
    async def test_confirm_forgot_password(self):
        pool_id, client_id = await _create_pool_and_client()
        await handle_cognito_request(
            _make_request(
                "SignUp", {"ClientId": client_id, "Username": "user1", "Password": "Pass1!"}
            ),
            "us-east-1",
            "123456789012",
        )

        req = _make_request(
            "ConfirmForgotPassword",
            {
                "ClientId": client_id,
                "Username": "user1",
                "Password": "NewPass1!",
                "ConfirmationCode": "123456",
            },
        )
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_admin_set_user_password(self):
        pool_id = await _create_pool()
        await handle_cognito_request(
            _make_request("AdminCreateUser", {"UserPoolId": pool_id, "Username": "user1"}),
            "us-east-1",
            "123456789012",
        )

        req = _make_request(
            "AdminSetUserPassword",
            {
                "UserPoolId": pool_id,
                "Username": "user1",
                "Password": "NewPerm1!",
                "Permanent": True,
            },
        )
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

        # Verify status changed to CONFIRMED
        req2 = _make_request("AdminGetUser", {"UserPoolId": pool_id, "Username": "user1"})
        resp2 = await handle_cognito_request(req2, "us-east-1", "123456789012")
        data = json.loads(resp2.body)
        assert data["UserStatus"] == "CONFIRMED"

    @pytest.mark.asyncio
    async def test_change_password(self):
        pool_id, client_id = await _create_pool_and_client()
        await handle_cognito_request(
            _make_request(
                "SignUp", {"ClientId": client_id, "Username": "user1", "Password": "Pass1!"}
            ),
            "us-east-1",
            "123456789012",
        )
        await handle_cognito_request(
            _make_request("AdminConfirmSignUp", {"UserPoolId": pool_id, "Username": "user1"}),
            "us-east-1",
            "123456789012",
        )
        auth_resp = await handle_cognito_request(
            _make_request(
                "InitiateAuth",
                {
                    "AuthFlow": "USER_PASSWORD_AUTH",
                    "ClientId": client_id,
                    "AuthParameters": {"USERNAME": "user1", "PASSWORD": "Pass1!"},
                },
            ),
            "us-east-1",
            "123456789012",
        )
        token = json.loads(auth_resp.body)["AuthenticationResult"]["AccessToken"]

        req = _make_request(
            "ChangePassword",
            {
                "AccessToken": token,
                "PreviousPassword": "Pass1!",
                "ProposedPassword": "NewPass1!",
            },
        )
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# List users with filter
# ---------------------------------------------------------------------------


class TestListUsers:
    @pytest.mark.asyncio
    async def test_list_users(self):
        pool_id = await _create_pool()
        for name in ("alice", "bob"):
            await handle_cognito_request(
                _make_request("AdminCreateUser", {"UserPoolId": pool_id, "Username": name}),
                "us-east-1",
                "123456789012",
            )

        req = _make_request("ListUsers", {"UserPoolId": pool_id})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        assert len(data["Users"]) == 2

    @pytest.mark.asyncio
    async def test_list_users_with_filter(self):
        pool_id = await _create_pool()
        for name in ("alice", "bob"):
            await handle_cognito_request(
                _make_request("AdminCreateUser", {"UserPoolId": pool_id, "Username": name}),
                "us-east-1",
                "123456789012",
            )

        req = _make_request("ListUsers", {"UserPoolId": pool_id, "Filter": 'username = "alice"'})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        assert len(data["Users"]) == 1
        assert data["Users"][0]["Username"] == "alice"


# ---------------------------------------------------------------------------
# Groups
# ---------------------------------------------------------------------------


class TestGroups:
    @pytest.mark.asyncio
    async def test_create_group(self):
        pool_id = await _create_pool()
        req = _make_request("CreateGroup", {"UserPoolId": pool_id, "GroupName": "admins"})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["Group"]["GroupName"] == "admins"

    @pytest.mark.asyncio
    async def test_create_duplicate_group(self):
        pool_id = await _create_pool()
        req = _make_request("CreateGroup", {"UserPoolId": pool_id, "GroupName": "admins"})
        await handle_cognito_request(req, "us-east-1", "123456789012")
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_delete_group(self):
        pool_id = await _create_pool()
        await handle_cognito_request(
            _make_request("CreateGroup", {"UserPoolId": pool_id, "GroupName": "admins"}),
            "us-east-1",
            "123456789012",
        )
        req = _make_request("DeleteGroup", {"UserPoolId": pool_id, "GroupName": "admins"})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_list_groups(self):
        pool_id = await _create_pool()
        for g in ("admins", "users"):
            await handle_cognito_request(
                _make_request("CreateGroup", {"UserPoolId": pool_id, "GroupName": g}),
                "us-east-1",
                "123456789012",
            )
        req = _make_request("ListGroups", {"UserPoolId": pool_id})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        assert len(data["Groups"]) == 2

    @pytest.mark.asyncio
    async def test_add_user_to_group(self):
        pool_id = await _create_pool()
        await handle_cognito_request(
            _make_request("AdminCreateUser", {"UserPoolId": pool_id, "Username": "user1"}),
            "us-east-1",
            "123456789012",
        )
        await handle_cognito_request(
            _make_request("CreateGroup", {"UserPoolId": pool_id, "GroupName": "admins"}),
            "us-east-1",
            "123456789012",
        )

        req = _make_request(
            "AdminAddUserToGroup",
            {"UserPoolId": pool_id, "Username": "user1", "GroupName": "admins"},
        )
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_list_groups_for_user(self):
        pool_id = await _create_pool()
        await handle_cognito_request(
            _make_request("AdminCreateUser", {"UserPoolId": pool_id, "Username": "user1"}),
            "us-east-1",
            "123456789012",
        )
        await handle_cognito_request(
            _make_request("CreateGroup", {"UserPoolId": pool_id, "GroupName": "admins"}),
            "us-east-1",
            "123456789012",
        )
        await handle_cognito_request(
            _make_request(
                "AdminAddUserToGroup",
                {"UserPoolId": pool_id, "Username": "user1", "GroupName": "admins"},
            ),
            "us-east-1",
            "123456789012",
        )

        req = _make_request("AdminListGroupsForUser", {"UserPoolId": pool_id, "Username": "user1"})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        assert len(data["Groups"]) == 1
        assert data["Groups"][0]["GroupName"] == "admins"

    @pytest.mark.asyncio
    async def test_remove_user_from_group(self):
        pool_id = await _create_pool()
        await handle_cognito_request(
            _make_request("AdminCreateUser", {"UserPoolId": pool_id, "Username": "user1"}),
            "us-east-1",
            "123456789012",
        )
        await handle_cognito_request(
            _make_request("CreateGroup", {"UserPoolId": pool_id, "GroupName": "admins"}),
            "us-east-1",
            "123456789012",
        )
        await handle_cognito_request(
            _make_request(
                "AdminAddUserToGroup",
                {"UserPoolId": pool_id, "Username": "user1", "GroupName": "admins"},
            ),
            "us-east-1",
            "123456789012",
        )
        await handle_cognito_request(
            _make_request(
                "AdminRemoveUserFromGroup",
                {"UserPoolId": pool_id, "Username": "user1", "GroupName": "admins"},
            ),
            "us-east-1",
            "123456789012",
        )

        req = _make_request("AdminListGroupsForUser", {"UserPoolId": pool_id, "Username": "user1"})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        assert len(data["Groups"]) == 0


# ---------------------------------------------------------------------------
# Unknown action / missing target
# ---------------------------------------------------------------------------


class TestEdgeCases:
    @pytest.mark.asyncio
    async def test_missing_target(self):
        scope = {
            "type": "http",
            "method": "POST",
            "path": "/",
            "query_string": b"",
            "headers": [],
        }

        async def receive():
            return {"type": "http.request", "body": b"{}"}

        req = Request(scope, receive)
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# CATEGORICAL BUG: Tag operations stubbed/missing
# ---------------------------------------------------------------------------


class TestTagOperations:
    """Tags should be stored and retrieved, not silently dropped."""

    @pytest.mark.asyncio
    async def test_tag_resource_and_list_tags(self):
        """TagResource should persist tags, ListTagsForResource should return them."""
        pool_id = await _create_pool()
        # Get the pool ARN
        req = _make_request("DescribeUserPool", {"UserPoolId": pool_id})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        arn = json.loads(resp.body)["UserPool"]["Arn"]

        # Tag the resource
        req = _make_request(
            "TagResource",
            {"ResourceArn": arn, "Tags": {"env": "test", "team": "platform"}},
        )
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

        # List tags
        req = _make_request("ListTagsForResource", {"ResourceArn": arn})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["Tags"]["env"] == "test"
        assert data["Tags"]["team"] == "platform"

    @pytest.mark.asyncio
    async def test_untag_resource(self):
        """UntagResource should remove specific tags."""
        pool_id = await _create_pool()
        req = _make_request("DescribeUserPool", {"UserPoolId": pool_id})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        arn = json.loads(resp.body)["UserPool"]["Arn"]

        # Tag
        req = _make_request(
            "TagResource",
            {"ResourceArn": arn, "Tags": {"env": "test", "team": "platform"}},
        )
        await handle_cognito_request(req, "us-east-1", "123456789012")

        # Untag one key
        req = _make_request("UntagResource", {"ResourceArn": arn, "TagKeys": ["env"]})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

        # Verify only "team" remains
        req = _make_request("ListTagsForResource", {"ResourceArn": arn})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        assert "env" not in data["Tags"]
        assert data["Tags"]["team"] == "platform"

    @pytest.mark.asyncio
    async def test_list_tags_nonexistent_resource(self):
        """ListTagsForResource on a bad ARN should return 404."""
        req = _make_request(
            "ListTagsForResource",
            {"ResourceArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/nope"},
        )
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# CATEGORICAL BUG: Parent-child cascade — delete group leaves stale memberships
# ---------------------------------------------------------------------------


class TestParentChildCascade:
    @pytest.mark.asyncio
    async def test_delete_group_cleans_up_user_memberships(self):
        """Deleting a group should remove it from all users' membership lists."""
        pool_id = await _create_pool()
        await handle_cognito_request(
            _make_request("AdminCreateUser", {"UserPoolId": pool_id, "Username": "user1"}),
            "us-east-1",
            "123456789012",
        )
        await handle_cognito_request(
            _make_request("CreateGroup", {"UserPoolId": pool_id, "GroupName": "admins"}),
            "us-east-1",
            "123456789012",
        )
        await handle_cognito_request(
            _make_request(
                "AdminAddUserToGroup",
                {"UserPoolId": pool_id, "Username": "user1", "GroupName": "admins"},
            ),
            "us-east-1",
            "123456789012",
        )

        # Delete the group
        await handle_cognito_request(
            _make_request("DeleteGroup", {"UserPoolId": pool_id, "GroupName": "admins"}),
            "us-east-1",
            "123456789012",
        )

        # user_groups should no longer reference the deleted group
        store = _get_store("us-east-1")
        user_groups = store.user_groups.get(pool_id, {}).get("user1", [])
        assert "admins" not in user_groups, (
            "Deleted group still present in user_groups — stale membership data"
        )

    @pytest.mark.asyncio
    async def test_delete_user_cleans_up_group_memberships(self):
        """Deleting a user should remove them from user_groups store."""
        pool_id = await _create_pool()
        await handle_cognito_request(
            _make_request("AdminCreateUser", {"UserPoolId": pool_id, "Username": "user1"}),
            "us-east-1",
            "123456789012",
        )
        await handle_cognito_request(
            _make_request("CreateGroup", {"UserPoolId": pool_id, "GroupName": "admins"}),
            "us-east-1",
            "123456789012",
        )
        await handle_cognito_request(
            _make_request(
                "AdminAddUserToGroup",
                {"UserPoolId": pool_id, "Username": "user1", "GroupName": "admins"},
            ),
            "us-east-1",
            "123456789012",
        )

        # Delete the user
        await handle_cognito_request(
            _make_request("AdminDeleteUser", {"UserPoolId": pool_id, "Username": "user1"}),
            "us-east-1",
            "123456789012",
        )

        # ListUsersInGroup should not include deleted user
        req = _make_request("ListUsersInGroup", {"UserPoolId": pool_id, "GroupName": "admins"})
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        usernames = [u["Username"] for u in data["Users"]]
        assert "user1" not in usernames


# ---------------------------------------------------------------------------
# CATEGORICAL BUG: Lock inconsistency (_lock vs store.lock)
# ---------------------------------------------------------------------------


class TestLockConsistency:
    """_get_user and _change_password use module-level _lock instead of store.lock.
    This test verifies the operations work correctly across regions (which would
    fail with wrong lock if stores were truly isolated)."""

    @pytest.mark.asyncio
    async def test_get_user_uses_store_lock(self):
        """GetUser should use store.lock, not the module-level _lock."""
        from robotocore.services.cognito import provider

        pool_id, client_id = await _create_pool_and_client()
        await handle_cognito_request(
            _make_request(
                "SignUp", {"ClientId": client_id, "Username": "user1", "Password": "Pass1!"}
            ),
            "us-east-1",
            "123456789012",
        )
        await handle_cognito_request(
            _make_request("AdminConfirmSignUp", {"UserPoolId": pool_id, "Username": "user1"}),
            "us-east-1",
            "123456789012",
        )
        auth_resp = await handle_cognito_request(
            _make_request(
                "InitiateAuth",
                {
                    "AuthFlow": "USER_PASSWORD_AUTH",
                    "ClientId": client_id,
                    "AuthParameters": {"USERNAME": "user1", "PASSWORD": "Pass1!"},
                },
            ),
            "us-east-1",
            "123456789012",
        )
        json.loads(auth_resp.body)["AuthenticationResult"]["AccessToken"]

        # Verify _get_user function uses store.lock (not _lock)
        # We check by inspecting the source — the bug is that _get_user uses `with _lock:`
        import inspect

        source = inspect.getsource(provider._get_user)
        assert "with store.lock" in source or "store.lock" in source, (
            "_get_user uses module-level _lock instead of store.lock — lock inconsistency"
        )

    @pytest.mark.asyncio
    async def test_change_password_uses_store_lock(self):
        """ChangePassword should use store.lock, not the module-level _lock."""
        import inspect

        from robotocore.services.cognito import provider

        source = inspect.getsource(provider._change_password)
        assert "with store.lock" in source or "store.lock" in source, (
            "_change_password uses module-level _lock instead of store.lock — lock inconsistency"
        )


# ---------------------------------------------------------------------------
# CATEGORICAL BUG: AdminUpdateUserAttributes writes wrong field name
# ---------------------------------------------------------------------------


class TestAttributeUpdateTimestamp:
    @pytest.mark.asyncio
    async def test_admin_update_user_attributes_updates_last_modified(self):
        """AdminUpdateUserAttributes should update LastModifiedDate (not UserLastModifiedDate)."""
        pool_id = await _create_pool()
        await handle_cognito_request(
            _make_request("AdminCreateUser", {"UserPoolId": pool_id, "Username": "user1"}),
            "us-east-1",
            "123456789012",
        )

        # Get initial LastModifiedDate
        store = _get_store("us-east-1")
        initial_ts = store.users[pool_id]["user1"]["LastModifiedDate"]

        import time as _time

        _time.sleep(0.01)

        # Update attributes
        req = _make_request(
            "AdminUpdateUserAttributes",
            {
                "UserPoolId": pool_id,
                "Username": "user1",
                "UserAttributes": [{"Name": "email", "Value": "new@test.com"}],
            },
        )
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

        # LastModifiedDate (the canonical field) should be updated
        updated_ts = store.users[pool_id]["user1"]["LastModifiedDate"]
        assert updated_ts > initial_ts, (
            "AdminUpdateUserAttributes writes to UserLastModifiedDate instead of LastModifiedDate"
        )

    @pytest.mark.asyncio
    async def test_admin_update_attributes_on_nonexistent_pool_returns_pool_not_found(self):
        """AdminUpdateUserAttributes on a nonexistent pool should return ResourceNotFoundException,
        not UserNotFoundException."""
        req = _make_request(
            "AdminUpdateUserAttributes",
            {
                "UserPoolId": "nonexistent",
                "Username": "user1",
                "UserAttributes": [{"Name": "email", "Value": "x@test.com"}],
            },
        )
        resp = await handle_cognito_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404
        data = json.loads(resp.body)
        assert data["__type"] == "ResourceNotFoundException"
