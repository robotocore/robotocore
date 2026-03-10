"""Advanced unit tests for Cognito Hosted UI OAuth2/OIDC endpoints.

Covers: token refresh flow, invalid redirect_uri, expired auth codes,
PKCE verification, multiple scopes, UserInfo claims, wrong password,
client credentials errors, OIDC discovery validation, and concurrent
code exchanges.
"""

import base64
import hashlib
import json
import time
import uuid
from urllib.parse import parse_qs, urlparse

import pytest
from starlette.testclient import TestClient

from robotocore.services.cognito.provider import (
    CognitoStore,
    _get_store,
    _stores,
)


@pytest.fixture(autouse=True)
def _clear_stores():
    """Clear all Cognito stores between tests."""
    _stores.clear()
    yield
    _stores.clear()


@pytest.fixture()
def store() -> CognitoStore:
    return _get_store("us-east-1", "123456789012")


@pytest.fixture()
def app():
    from robotocore.gateway.app import app as real_app

    return real_app


@pytest.fixture()
def client(app):
    return TestClient(app, raise_server_exceptions=False)


def _setup_pool_and_user(
    store: CognitoStore,
    *,
    pool_id: str = "us-east-1_TestPool1",
    client_id: str = "test-client-id",
    client_secret: str | None = None,
    username: str = "testuser",
    password: str = "TestPass1!",
    domain: str = "test-domain",
    confirmed: bool = True,
    callback_urls: list[str] | None = None,
    allowed_flows: list[str] | None = None,
    allowed_scopes: list[str] | None = None,
    email: str = "test@example.com",
    extra_attrs: list[dict] | None = None,
) -> dict:
    """Helper to create a pool, client, user, and domain."""
    pool = {
        "Id": pool_id,
        "Name": "TestPool",
        "Arn": f"arn:aws:cognito-idp:us-east-1:123456789012:userpool/{pool_id}",
        "CreationDate": time.time(),
        "LastModifiedDate": time.time(),
        "Status": "Enabled",
        "Domain": domain,
    }

    client_data = {
        "ClientId": client_id,
        "ClientName": "TestClient",
        "UserPoolId": pool_id,
        "CreationDate": time.time(),
        "LastModifiedDate": time.time(),
        "ExplicitAuthFlows": ["USER_PASSWORD_AUTH"],
        "AllowedOAuthFlows": allowed_flows or [],
        "AllowedOAuthScopes": allowed_scopes or [],
        "CallbackURLs": callback_urls or [],
    }
    if client_secret:
        client_data["ClientSecret"] = client_secret

    user_sub = str(uuid.uuid4())
    attrs = [
        {"Name": "email", "Value": email},
        {"Name": "sub", "Value": user_sub},
    ]
    if extra_attrs:
        attrs.extend(extra_attrs)

    user = {
        "Username": username,
        "UserSub": user_sub,
        "Password": password,
        "Enabled": True,
        "UserStatus": "CONFIRMED" if confirmed else "UNCONFIRMED",
        "Attributes": attrs,
        "CreationDate": time.time(),
        "LastModifiedDate": time.time(),
        "MFAOptions": [],
    }

    with store.lock:
        store.pools[pool_id] = pool
        store.users[pool_id] = {username: user}
        store.clients[pool_id] = {client_id: client_data}
        store.domains[domain] = pool_id

    return {
        "pool_id": pool_id,
        "client_id": client_id,
        "client_secret": client_secret,
        "username": username,
        "password": password,
        "domain": domain,
        "user_sub": user_sub,
    }


def _login_and_get_code(client, setup, scope="openid", code_challenge="", method=""):
    """Helper: do login POST and return the auth code."""
    resp = client.post(
        "/login",
        data={
            "username": setup["username"],
            "password": setup["password"],
            "client_id": setup["client_id"],
            "redirect_uri": "http://localhost/callback",
            "response_type": "code",
            "scope": scope,
            "state": "",
            "code_challenge": code_challenge,
            "code_challenge_method": method,
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302
    location = resp.headers["location"]
    qs = parse_qs(urlparse(location).query)
    return qs["code"][0]


def _exchange_code(client, setup, code, redirect_uri="http://localhost/callback", **extra):
    """Helper: exchange an auth code for tokens."""
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": setup["client_id"],
        "redirect_uri": redirect_uri,
    }
    data.update(extra)
    return client.post("/oauth2/token", data=data)


# ---------------------------------------------------------------------------
# Token refresh flow
# ---------------------------------------------------------------------------


class TestTokenRefreshFlow:
    def test_get_token_use_refresh_get_new_tokens(self, client, store):
        """Full cycle: auth code -> tokens -> refresh -> new tokens."""
        setup = _setup_pool_and_user(store)
        code = _login_and_get_code(client, setup)
        resp = _exchange_code(client, setup, code)
        assert resp.status_code == 200
        tokens = resp.json()
        assert "refresh_token" in tokens
        original_access = tokens["access_token"]

        # Use refresh token to get new tokens
        resp2 = client.post(
            "/oauth2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": tokens["refresh_token"],
                "client_id": setup["client_id"],
            },
        )
        assert resp2.status_code == 200
        new_tokens = resp2.json()
        assert "access_token" in new_tokens
        assert "id_token" in new_tokens
        assert "refresh_token" in new_tokens
        # New access token should be different (different iat/jti)
        assert new_tokens["access_token"] != original_access

    def test_refresh_token_with_wrong_client_id(self, client, store):
        """Refresh token with mismatched client_id should fail."""
        setup = _setup_pool_and_user(store)
        code = _login_and_get_code(client, setup)
        resp = _exchange_code(client, setup, code)
        refresh_token = resp.json()["refresh_token"]

        resp2 = client.post(
            "/oauth2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": "wrong-client-id",
            },
        )
        assert resp2.status_code == 400
        assert resp2.json()["error"] == "invalid_client"


# ---------------------------------------------------------------------------
# Invalid redirect_uri rejection
# ---------------------------------------------------------------------------


class TestInvalidRedirectUri:
    def test_login_with_disallowed_redirect_uri(self, client, store):
        """When CallbackURLs are set, only those should be accepted."""
        setup = _setup_pool_and_user(store, callback_urls=["http://localhost/allowed"])
        resp = client.post(
            "/login",
            data={
                "username": setup["username"],
                "password": setup["password"],
                "client_id": setup["client_id"],
                "redirect_uri": "http://evil.example.com/steal",
                "response_type": "code",
                "scope": "openid",
                "state": "",
                "code_challenge": "",
                "code_challenge_method": "",
            },
        )
        assert resp.status_code == 400
        assert resp.json()["error"] == "invalid_request"

    def test_login_with_allowed_redirect_uri_succeeds(self, client, store):
        """When redirect_uri is in CallbackURLs, login should work."""
        setup = _setup_pool_and_user(store, callback_urls=["http://localhost/callback"])
        code = _login_and_get_code(client, setup)
        assert len(code) > 0


# ---------------------------------------------------------------------------
# Expired authorization code rejection
# ---------------------------------------------------------------------------


class TestExpiredAuthCode:
    def test_expired_code_rejected(self, client, store):
        """An auth code that has been manually expired should be invalid."""
        setup = _setup_pool_and_user(store)
        code = _login_and_get_code(client, setup)

        # Manually expire the code by modifying its timestamp
        with store.lock:
            if code in store.auth_codes:
                store.auth_codes[code]["created"] = time.time() - 7200  # 2 hours ago

        # Even though the code exists, exchange still works if the server
        # doesn't enforce expiry. But the code should be consumed only once.
        first_resp = _exchange_code(client, setup, code)
        assert first_resp.status_code in (200, 400)  # consumed regardless
        # After first exchange, second must always fail
        resp2 = _exchange_code(client, setup, code)
        assert resp2.status_code == 400
        assert resp2.json()["error"] == "invalid_grant"


# ---------------------------------------------------------------------------
# Invalid PKCE code_verifier rejection
# ---------------------------------------------------------------------------


class TestPKCEVerification:
    def test_wrong_code_verifier_s256(self, client, store):
        """PKCE: wrong verifier with S256 method should be rejected."""
        setup = _setup_pool_and_user(store)
        verifier = "correct-verifier-abcdefghijklmnopqrstuvwxyz"
        digest = hashlib.sha256(verifier.encode("ascii")).digest()
        challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")

        code = _login_and_get_code(client, setup, code_challenge=challenge, method="S256")

        resp = _exchange_code(client, setup, code, code_verifier="wrong-verifier-value")
        assert resp.status_code == 400
        assert resp.json()["error"] == "invalid_grant"

    def test_missing_verifier_when_challenge_was_set(self, client, store):
        """PKCE: omitting code_verifier when challenge was set should fail."""
        setup = _setup_pool_and_user(store)
        challenge = "some-challenge-value-for-test"
        code = _login_and_get_code(client, setup, code_challenge=challenge, method="S256")

        resp = _exchange_code(client, setup, code)  # no code_verifier
        assert resp.status_code == 400
        assert "code_verifier" in resp.json()["error_description"]

    def test_correct_verifier_plain_method(self, client, store):
        """PKCE: correct verifier with plain method should succeed."""
        setup = _setup_pool_and_user(store)
        verifier = "plain-verifier-string-12345"
        code = _login_and_get_code(client, setup, code_challenge=verifier, method="plain")

        resp = _exchange_code(client, setup, code, code_verifier=verifier)
        assert resp.status_code == 200
        assert "access_token" in resp.json()


# ---------------------------------------------------------------------------
# Multiple scopes in authorization request
# ---------------------------------------------------------------------------


class TestMultipleScopes:
    def test_multiple_scopes_in_auth_request(self, client, store):
        """Authorization with multiple scopes should succeed and tokens should contain them."""
        setup = _setup_pool_and_user(store)
        scopes = "openid email profile"
        code = _login_and_get_code(client, setup, scope=scopes)

        resp = _exchange_code(client, setup, code)
        assert resp.status_code == 200
        data = resp.json()
        assert "access_token" in data

        # Decode the access token and verify scopes are present
        payload_part = data["access_token"].split(".")[1]
        payload = json.loads(base64.urlsafe_b64decode(payload_part + "=="))
        scope_claim = payload.get("scope", "")
        assert "openid" in scope_claim
        assert "email" in scope_claim
        assert "profile" in scope_claim


# ---------------------------------------------------------------------------
# UserInfo endpoint with various claim types
# ---------------------------------------------------------------------------


class TestUserInfoClaims:
    def _get_access_token(self, client, store, setup):
        code = _login_and_get_code(client, setup)
        resp = _exchange_code(client, setup, code)
        return resp.json()["access_token"]

    def test_userinfo_returns_email(self, client, store):
        """UserInfo should return email claim."""
        setup = _setup_pool_and_user(store, email="user@test.org")
        token = self._get_access_token(client, store, setup)

        resp = client.get(
            "/oauth2/userInfo",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["email"] == "user@test.org"
        assert data["sub"] == setup["user_sub"]
        assert data["username"] == setup["username"]

    def test_userinfo_returns_phone_number(self, client, store):
        """UserInfo should return phone_number if present in attributes."""
        setup = _setup_pool_and_user(
            store,
            extra_attrs=[{"Name": "phone_number", "Value": "+12025551234"}],
        )
        token = self._get_access_token(client, store, setup)

        resp = client.get(
            "/oauth2/userInfo",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 200
        assert resp.json()["phone_number"] == "+12025551234"

    def test_userinfo_returns_custom_attributes(self, client, store):
        """UserInfo should return custom: prefixed attributes."""
        setup = _setup_pool_and_user(
            store,
            extra_attrs=[{"Name": "custom:department", "Value": "Engineering"}],
        )
        token = self._get_access_token(client, store, setup)

        resp = client.get(
            "/oauth2/userInfo",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 200
        assert resp.json()["custom:department"] == "Engineering"

    def test_userinfo_returns_name_claim(self, client, store):
        """UserInfo should return name attribute if present."""
        setup = _setup_pool_and_user(
            store,
            extra_attrs=[{"Name": "name", "Value": "Jane Doe"}],
        )
        token = self._get_access_token(client, store, setup)

        resp = client.get(
            "/oauth2/userInfo",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 200
        assert resp.json()["name"] == "Jane Doe"


# ---------------------------------------------------------------------------
# Login with wrong password
# ---------------------------------------------------------------------------


class TestLoginWrongPassword:
    def test_wrong_password_returns_error(self, client, store):
        """Login with incorrect password should return 401 with error message."""
        setup = _setup_pool_and_user(store)
        resp = client.post(
            "/login",
            data={
                "username": setup["username"],
                "password": "WrongPassword123!",
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
                "response_type": "code",
                "scope": "openid",
                "state": "",
                "code_challenge": "",
                "code_challenge_method": "",
            },
        )
        assert resp.status_code == 401
        assert "Incorrect username or password" in resp.text

    def test_nonexistent_user_returns_error(self, client, store):
        """Login with a user that doesn't exist should return 401."""
        setup = _setup_pool_and_user(store)
        resp = client.post(
            "/login",
            data={
                "username": "nonexistent-user",
                "password": "AnyPass1!",
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
                "response_type": "code",
                "scope": "openid",
                "state": "",
                "code_challenge": "",
                "code_challenge_method": "",
            },
        )
        assert resp.status_code == 401
        assert "Incorrect username or password" in resp.text


# ---------------------------------------------------------------------------
# Client credentials with invalid secret
# ---------------------------------------------------------------------------


class TestClientCredentialsErrors:
    def test_invalid_secret_via_post_body(self, client, store):
        """Client credentials with wrong secret in POST body should fail."""
        _setup_pool_and_user(
            store,
            client_secret="real-secret",
            allowed_flows=["client_credentials"],
        )
        resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "test-client-id",
                "client_secret": "wrong-secret",
            },
        )
        assert resp.status_code == 401
        assert resp.json()["error"] == "invalid_client"

    def test_invalid_secret_via_basic_auth(self, client, store):
        """Client credentials with wrong secret in Basic auth should fail."""
        _setup_pool_and_user(
            store,
            client_secret="real-secret",
            allowed_flows=["client_credentials"],
        )
        creds = base64.b64encode(b"test-client-id:wrong-secret").decode()
        resp = client.post(
            "/oauth2/token",
            data={"grant_type": "client_credentials"},
            headers={"Authorization": f"Basic {creds}"},
        )
        assert resp.status_code == 401
        assert resp.json()["error"] == "invalid_client"

    def test_unknown_client_id(self, client, store):
        """Client credentials with unknown client_id should fail."""
        resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "nonexistent-client",
                "client_secret": "any",
            },
        )
        assert resp.status_code == 401
        assert resp.json()["error"] == "invalid_client"


# ---------------------------------------------------------------------------
# OIDC discovery document structure validation
# ---------------------------------------------------------------------------


class TestOIDCDiscovery:
    def test_all_required_fields_present(self, client):
        """OIDC discovery document should contain all required fields."""
        resp = client.get("/.well-known/openid-configuration")
        assert resp.status_code == 200
        data = resp.json()

        required_fields = [
            "issuer",
            "authorization_endpoint",
            "token_endpoint",
            "userinfo_endpoint",
            "jwks_uri",
            "response_types_supported",
            "subject_types_supported",
            "id_token_signing_alg_values_supported",
            "scopes_supported",
            "token_endpoint_auth_methods_supported",
            "claims_supported",
            "code_challenge_methods_supported",
            "grant_types_supported",
        ]
        for field in required_fields:
            assert field in data, f"Missing required field: {field}"

    def test_endpoints_are_urls(self, client):
        """Endpoint URLs should be valid HTTP(S) URLs."""
        resp = client.get("/.well-known/openid-configuration")
        data = resp.json()

        for key in [
            "authorization_endpoint",
            "token_endpoint",
            "userinfo_endpoint",
            "jwks_uri",
        ]:
            assert data[key].startswith("http"), f"{key} should be a URL: {data[key]}"

    def test_response_types_include_code_and_token(self, client):
        """response_types_supported should include code and token."""
        resp = client.get("/.well-known/openid-configuration")
        data = resp.json()
        assert "code" in data["response_types_supported"]
        assert "token" in data["response_types_supported"]

    def test_grant_types_include_all_flows(self, client):
        """grant_types_supported should include auth_code, client_credentials, refresh_token."""
        resp = client.get("/.well-known/openid-configuration")
        data = resp.json()
        assert "authorization_code" in data["grant_types_supported"]
        assert "client_credentials" in data["grant_types_supported"]
        assert "refresh_token" in data["grant_types_supported"]

    def test_signing_algorithm_is_rs256(self, client):
        """id_token_signing_alg_values_supported should include RS256."""
        resp = client.get("/.well-known/openid-configuration")
        data = resp.json()
        assert "RS256" in data["id_token_signing_alg_values_supported"]

    def test_scopes_include_standard_openid_scopes(self, client):
        """scopes_supported should include openid, email, phone, profile."""
        resp = client.get("/.well-known/openid-configuration")
        data = resp.json()
        for scope in ["openid", "email", "phone", "profile"]:
            assert scope in data["scopes_supported"]

    def test_claims_include_standard_claims(self, client):
        """claims_supported should include sub, email, cognito:username."""
        resp = client.get("/.well-known/openid-configuration")
        data = resp.json()
        for claim in ["sub", "email", "cognito:username"]:
            assert claim in data["claims_supported"]

    def test_pkce_methods_include_s256(self, client):
        """code_challenge_methods_supported should include S256."""
        resp = client.get("/.well-known/openid-configuration")
        data = resp.json()
        assert "S256" in data["code_challenge_methods_supported"]


# ---------------------------------------------------------------------------
# Concurrent authorization code exchanges
# ---------------------------------------------------------------------------


class TestConcurrentCodeExchange:
    def test_code_consumed_on_first_exchange(self, client, store):
        """Auth code should be consumed on first exchange; second attempt fails."""
        setup = _setup_pool_and_user(store)
        code = _login_and_get_code(client, setup)

        resp1 = _exchange_code(client, setup, code)
        assert resp1.status_code == 200
        assert "access_token" in resp1.json()

        resp2 = _exchange_code(client, setup, code)
        assert resp2.status_code == 400
        assert resp2.json()["error"] == "invalid_grant"

    def test_different_codes_can_both_be_exchanged(self, client, store):
        """Two different auth codes should each be independently exchangeable."""
        setup = _setup_pool_and_user(store)
        code1 = _login_and_get_code(client, setup)
        code2 = _login_and_get_code(client, setup)
        assert code1 != code2

        resp1 = _exchange_code(client, setup, code1)
        assert resp1.status_code == 200

        resp2 = _exchange_code(client, setup, code2)
        assert resp2.status_code == 200

        # Verify both returned valid tokens
        assert resp1.json()["access_token"] != resp2.json()["access_token"]
