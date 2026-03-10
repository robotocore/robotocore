"""Unit tests for Cognito Hosted UI OAuth2/OIDC endpoints."""

import base64
import hashlib
import json
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
    """Get a fresh store for us-east-1."""
    return _get_store("us-east-1", "123456789012")


@pytest.fixture()
def app():
    """Create a test ASGI app with hosted UI routes."""
    from robotocore.gateway.app import app as real_app

    return real_app


@pytest.fixture()
def client(app):
    """Starlette test client."""
    return TestClient(app, raise_server_exceptions=False)


def _setup_pool_and_user(
    store: CognitoStore,
    *,
    pool_id: str = "us-east-1_TestPool1",
    pool_name: str = "TestPool",
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
) -> dict:
    """Helper to create a pool, client, user, and domain."""
    import time
    import uuid

    pool = {
        "Id": pool_id,
        "Name": pool_name,
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
    user = {
        "Username": username,
        "UserSub": user_sub,
        "Password": password,
        "Enabled": True,
        "UserStatus": "CONFIRMED" if confirmed else "UNCONFIRMED",
        "Attributes": [
            {"Name": "email", "Value": email},
            {"Name": "sub", "Value": user_sub},
        ],
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


# ---------------------------------------------------------------------------
# OIDC discovery
# ---------------------------------------------------------------------------


class TestOpenIDConfiguration:
    def test_discovery_returns_standard_fields(self, client):
        resp = client.get("/.well-known/openid-configuration")
        assert resp.status_code == 200
        data = resp.json()
        assert "issuer" in data
        assert "authorization_endpoint" in data
        assert "token_endpoint" in data
        assert "userinfo_endpoint" in data
        assert "jwks_uri" in data
        assert "response_types_supported" in data
        assert "code" in data["response_types_supported"]
        assert "token" in data["response_types_supported"]

    def test_discovery_grant_types(self, client):
        resp = client.get("/.well-known/openid-configuration")
        data = resp.json()
        assert "authorization_code" in data["grant_types_supported"]
        assert "client_credentials" in data["grant_types_supported"]
        assert "refresh_token" in data["grant_types_supported"]

    def test_discovery_pkce_methods(self, client):
        resp = client.get("/.well-known/openid-configuration")
        data = resp.json()
        assert "S256" in data["code_challenge_methods_supported"]

    def test_discovery_scopes(self, client):
        resp = client.get("/.well-known/openid-configuration")
        data = resp.json()
        assert "openid" in data["scopes_supported"]
        assert "email" in data["scopes_supported"]


class TestJWKS:
    def test_jwks_returns_keys(self, client):
        resp = client.get("/.well-known/jwks.json")
        assert resp.status_code == 200
        data = resp.json()
        assert "keys" in data
        assert len(data["keys"]) >= 1
        key = data["keys"][0]
        assert key["kty"] == "RSA"
        assert key["alg"] == "RS256"
        assert key["use"] == "sig"
        assert "kid" in key
        assert "n" in key
        assert "e" in key


# ---------------------------------------------------------------------------
# Authorization endpoint
# ---------------------------------------------------------------------------


class TestOAuth2Authorize:
    def test_authorize_renders_login_form(self, client, store):
        _setup_pool_and_user(store)
        resp = client.get(
            "/oauth2/authorize",
            params={
                "client_id": "test-client-id",
                "redirect_uri": "http://localhost/callback",
                "response_type": "code",
                "scope": "openid",
            },
        )
        assert resp.status_code == 200
        assert "Sign In" in resp.text
        assert "test-client-id" in resp.text

    def test_authorize_missing_client_id(self, client):
        resp = client.get("/oauth2/authorize", params={"response_type": "code"})
        assert resp.status_code == 400
        assert resp.json()["error"] == "invalid_request"

    def test_authorize_unsupported_response_type(self, client):
        resp = client.get(
            "/oauth2/authorize",
            params={"client_id": "x", "response_type": "id_token"},
        )
        assert resp.status_code == 400
        assert resp.json()["error"] == "unsupported_response_type"

    def test_authorize_with_state(self, client, store):
        _setup_pool_and_user(store)
        resp = client.get(
            "/oauth2/authorize",
            params={
                "client_id": "test-client-id",
                "redirect_uri": "http://localhost/callback",
                "response_type": "code",
                "state": "mystate123",
            },
        )
        assert resp.status_code == 200
        assert "mystate123" in resp.text


# ---------------------------------------------------------------------------
# Login POST (authorization code flow)
# ---------------------------------------------------------------------------


class TestLoginPost:
    def test_login_success_redirects_with_code(self, client, store):
        setup = _setup_pool_and_user(store)
        resp = client.post(
            "/login",
            data={
                "username": setup["username"],
                "password": setup["password"],
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
                "response_type": "code",
                "scope": "openid",
                "state": "mystate",
                "code_challenge": "",
                "code_challenge_method": "",
            },
            follow_redirects=False,
        )
        assert resp.status_code == 302
        location = resp.headers["location"]
        parsed = urlparse(location)
        qs = parse_qs(parsed.query)
        assert "code" in qs
        assert qs["state"] == ["mystate"]

    def test_login_wrong_password(self, client, store):
        setup = _setup_pool_and_user(store)
        resp = client.post(
            "/login",
            data={
                "username": setup["username"],
                "password": "WrongPass!",
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

    def test_login_unconfirmed_user(self, client, store):
        setup = _setup_pool_and_user(store, confirmed=False)
        resp = client.post(
            "/login",
            data={
                "username": setup["username"],
                "password": setup["password"],
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
                "response_type": "code",
                "scope": "openid",
                "state": "",
                "code_challenge": "",
                "code_challenge_method": "",
            },
        )
        assert resp.status_code == 403
        assert "not confirmed" in resp.text

    def test_login_invalid_client(self, client, store):
        _setup_pool_and_user(store)
        resp = client.post(
            "/login",
            data={
                "username": "testuser",
                "password": "TestPass1!",
                "client_id": "nonexistent-client",
                "redirect_uri": "http://localhost/callback",
                "response_type": "code",
                "scope": "openid",
                "state": "",
                "code_challenge": "",
                "code_challenge_method": "",
            },
        )
        assert resp.status_code == 400
        assert resp.json()["error"] == "invalid_client"

    def test_login_implicit_flow_returns_tokens_in_fragment(self, client, store):
        setup = _setup_pool_and_user(store)
        resp = client.post(
            "/login",
            data={
                "username": setup["username"],
                "password": setup["password"],
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
                "response_type": "token",
                "scope": "openid",
                "state": "implicit-state",
                "code_challenge": "",
                "code_challenge_method": "",
            },
            follow_redirects=False,
        )
        assert resp.status_code == 302
        location = resp.headers["location"]
        assert "#" in location
        fragment = location.split("#", 1)[1]
        assert "access_token=" in fragment
        assert "implicit-state" in fragment

    def test_login_invalid_redirect_uri(self, client, store):
        setup = _setup_pool_and_user(store, callback_urls=["http://localhost/allowed"])
        resp = client.post(
            "/login",
            data={
                "username": setup["username"],
                "password": setup["password"],
                "client_id": setup["client_id"],
                "redirect_uri": "http://evil.com/steal",
                "response_type": "code",
                "scope": "openid",
                "state": "",
                "code_challenge": "",
                "code_challenge_method": "",
            },
        )
        assert resp.status_code == 400
        assert resp.json()["error"] == "invalid_request"


# ---------------------------------------------------------------------------
# Token endpoint
# ---------------------------------------------------------------------------


class TestOAuth2Token:
    def _get_auth_code(self, client, store, setup):
        """Helper to get an authorization code through login."""
        resp = client.post(
            "/login",
            data={
                "username": setup["username"],
                "password": setup["password"],
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
                "response_type": "code",
                "scope": "openid",
                "state": "",
                "code_challenge": "",
                "code_challenge_method": "",
            },
            follow_redirects=False,
        )
        location = resp.headers["location"]
        parsed = urlparse(location)
        qs = parse_qs(parsed.query)
        return qs["code"][0]

    def test_auth_code_exchange(self, client, store):
        setup = _setup_pool_and_user(store)
        code = self._get_auth_code(client, store, setup)

        resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "access_token" in data
        assert "id_token" in data
        assert "refresh_token" in data
        assert data["token_type"] == "Bearer"
        assert data["expires_in"] == 3600

    def test_auth_code_replay_fails(self, client, store):
        setup = _setup_pool_and_user(store)
        code = self._get_auth_code(client, store, setup)

        # First exchange succeeds
        resp1 = client.post(
            "/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
            },
        )
        assert resp1.status_code == 200

        # Second exchange fails (code is consumed)
        resp2 = client.post(
            "/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
            },
        )
        assert resp2.status_code == 400
        assert resp2.json()["error"] == "invalid_grant"

    def test_auth_code_wrong_client_id(self, client, store):
        setup = _setup_pool_and_user(store)
        code = self._get_auth_code(client, store, setup)

        resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": "wrong-client",
                "redirect_uri": "http://localhost/callback",
            },
        )
        assert resp.status_code == 400
        assert resp.json()["error"] == "invalid_client"

    def test_invalid_code(self, client, store):
        resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "code": "invalid-code",
                "client_id": "some-client",
            },
        )
        assert resp.status_code == 400
        assert resp.json()["error"] == "invalid_grant"

    def test_unsupported_grant_type(self, client, store):
        resp = client.post(
            "/oauth2/token",
            data={"grant_type": "password", "username": "x", "password": "y"},
        )
        assert resp.status_code == 400
        assert resp.json()["error"] == "unsupported_grant_type"

    def test_missing_code(self, client, store):
        resp = client.post(
            "/oauth2/token",
            data={"grant_type": "authorization_code", "client_id": "x"},
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# PKCE
# ---------------------------------------------------------------------------


class TestPKCE:
    def _get_auth_code_with_pkce(self, client, store, setup, code_challenge, method="S256"):
        resp = client.post(
            "/login",
            data={
                "username": setup["username"],
                "password": setup["password"],
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
                "response_type": "code",
                "scope": "openid",
                "state": "",
                "code_challenge": code_challenge,
                "code_challenge_method": method,
            },
            follow_redirects=False,
        )
        location = resp.headers["location"]
        parsed = urlparse(location)
        qs = parse_qs(parsed.query)
        return qs["code"][0]

    def test_pkce_s256_success(self, client, store):
        setup = _setup_pool_and_user(store)

        # Generate PKCE pair
        code_verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
        digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
        code_challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")

        code = self._get_auth_code_with_pkce(client, store, setup, code_challenge)

        resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
                "code_verifier": code_verifier,
            },
        )
        assert resp.status_code == 200
        assert "access_token" in resp.json()

    def test_pkce_wrong_verifier(self, client, store):
        setup = _setup_pool_and_user(store)

        code_verifier = "correct-verifier-value-here-1234567890"
        digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
        code_challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")

        code = self._get_auth_code_with_pkce(client, store, setup, code_challenge)

        resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
                "code_verifier": "wrong-verifier",
            },
        )
        assert resp.status_code == 400
        assert resp.json()["error"] == "invalid_grant"

    def test_pkce_missing_verifier(self, client, store):
        setup = _setup_pool_and_user(store)

        code_challenge = "some-challenge-value"
        code = self._get_auth_code_with_pkce(client, store, setup, code_challenge)

        resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
            },
        )
        assert resp.status_code == 400
        assert "code_verifier" in resp.json()["error_description"]

    def test_pkce_plain_method(self, client, store):
        setup = _setup_pool_and_user(store)
        code_verifier = "my-plain-verifier-string"

        code = self._get_auth_code_with_pkce(client, store, setup, code_verifier, method="plain")

        resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
                "code_verifier": code_verifier,
            },
        )
        assert resp.status_code == 200
        assert "access_token" in resp.json()


# ---------------------------------------------------------------------------
# Client credentials
# ---------------------------------------------------------------------------


class TestClientCredentials:
    def test_client_credentials_basic_auth(self, client, store):
        setup = _setup_pool_and_user(
            store,
            client_secret="my-secret",
            allowed_flows=["client_credentials"],
            allowed_scopes=["api/read"],
        )

        creds = base64.b64encode(f"{setup['client_id']}:{setup['client_secret']}".encode()).decode()

        resp = client.post(
            "/oauth2/token",
            data={"grant_type": "client_credentials", "scope": "api/read"},
            headers={"Authorization": f"Basic {creds}"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "access_token" in data
        assert data["token_type"] == "Bearer"
        # Client credentials should NOT return id_token or refresh_token
        assert "id_token" not in data

    def test_client_credentials_post_body(self, client, store):
        setup = _setup_pool_and_user(
            store,
            client_secret="my-secret",
            allowed_flows=["client_credentials"],
        )

        resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "client_credentials",
                "client_id": setup["client_id"],
                "client_secret": setup["client_secret"],
            },
        )
        assert resp.status_code == 200
        assert "access_token" in resp.json()

    def test_client_credentials_wrong_secret(self, client, store):
        _setup_pool_and_user(
            store,
            client_secret="my-secret",
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

    def test_client_credentials_flow_not_allowed(self, client, store):
        _setup_pool_and_user(
            store,
            client_secret="my-secret",
            allowed_flows=["code"],  # Not client_credentials
        )

        resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "test-client-id",
                "client_secret": "my-secret",
            },
        )
        assert resp.status_code == 400
        assert resp.json()["error"] == "unauthorized_client"

    def test_client_credentials_unknown_client(self, client, store):
        resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "nonexistent",
                "client_secret": "whatever",
            },
        )
        assert resp.status_code == 401
        assert resp.json()["error"] == "invalid_client"


# ---------------------------------------------------------------------------
# Refresh token
# ---------------------------------------------------------------------------


class TestRefreshToken:
    def test_refresh_token_flow(self, client, store):
        setup = _setup_pool_and_user(store)

        # First, get tokens via auth code
        login_resp = client.post(
            "/login",
            data={
                "username": setup["username"],
                "password": setup["password"],
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
                "response_type": "code",
                "scope": "openid",
                "state": "",
                "code_challenge": "",
                "code_challenge_method": "",
            },
            follow_redirects=False,
        )
        location = login_resp.headers["location"]
        parsed = urlparse(location)
        qs = parse_qs(parsed.query)
        code = qs["code"][0]

        token_resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
            },
        )
        refresh_token = token_resp.json()["refresh_token"]

        # Use refresh token
        resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": setup["client_id"],
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "access_token" in data
        assert "id_token" in data
        assert "refresh_token" in data

    def test_invalid_refresh_token(self, client, store):
        resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": "invalid-token",
                "client_id": "some-client",
            },
        )
        assert resp.status_code == 400
        assert resp.json()["error"] == "invalid_grant"


# ---------------------------------------------------------------------------
# UserInfo
# ---------------------------------------------------------------------------


class TestUserInfo:
    def _get_access_token(self, client, store, setup):
        """Get an access token through the full flow."""
        login_resp = client.post(
            "/login",
            data={
                "username": setup["username"],
                "password": setup["password"],
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
                "response_type": "code",
                "scope": "openid",
                "state": "",
                "code_challenge": "",
                "code_challenge_method": "",
            },
            follow_redirects=False,
        )
        location = login_resp.headers["location"]
        parsed = urlparse(location)
        qs = parse_qs(parsed.query)
        code = qs["code"][0]

        token_resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
            },
        )
        return token_resp.json()["access_token"]

    def test_userinfo_returns_claims(self, client, store):
        setup = _setup_pool_and_user(store)
        access_token = self._get_access_token(client, store, setup)

        resp = client.get(
            "/oauth2/userInfo",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["sub"] == setup["user_sub"]
        assert data["username"] == setup["username"]
        assert data["email"] == "test@example.com"

    def test_userinfo_missing_token(self, client, store):
        resp = client.get("/oauth2/userInfo")
        assert resp.status_code == 401
        assert resp.json()["error"] == "invalid_token"

    def test_userinfo_invalid_token(self, client, store):
        resp = client.get(
            "/oauth2/userInfo",
            headers={"Authorization": "Bearer invalid-jwt"},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Logout
# ---------------------------------------------------------------------------


class TestLogout:
    def test_logout_redirects(self, client, store):
        resp = client.get(
            "/logout",
            params={"logout_uri": "http://localhost/signed-out"},
            follow_redirects=False,
        )
        assert resp.status_code == 302
        assert resp.headers["location"] == "http://localhost/signed-out"

    def test_logout_with_client_id(self, client, store):
        resp = client.get(
            "/logout",
            params={
                "logout_uri": "http://localhost/signed-out",
                "client_id": "my-client",
            },
            follow_redirects=False,
        )
        assert resp.status_code == 302
        location = resp.headers["location"]
        assert "client_id=my-client" in location

    def test_logout_no_redirect(self, client, store):
        resp = client.get("/logout")
        assert resp.status_code == 200
        assert "signed out" in resp.text.lower()


# ---------------------------------------------------------------------------
# Password reset endpoints
# ---------------------------------------------------------------------------


class TestForgotPassword:
    def test_forgot_password_success(self, client, store):
        setup = _setup_pool_and_user(store)
        resp = client.post(
            "/forgotpassword",
            data={
                "client_id": setup["client_id"],
                "username": setup["username"],
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "CodeDeliveryDetails" in data

    def test_forgot_password_missing_params(self, client, store):
        resp = client.post(
            "/forgotpassword",
            data={"client_id": "x"},
        )
        assert resp.status_code == 400

    def test_confirm_forgot_password(self, client, store):
        setup = _setup_pool_and_user(store)
        resp = client.post(
            "/confirmforgotpassword",
            data={
                "client_id": setup["client_id"],
                "username": setup["username"],
                "password": "NewPass1!",
                "confirmation_code": "123456",
            },
        )
        assert resp.status_code == 200

        # Verify password was changed
        with store.lock:
            user = store.users["us-east-1_TestPool1"]["testuser"]
            assert user["Password"] == "NewPass1!"


# ---------------------------------------------------------------------------
# Token JWT structure
# ---------------------------------------------------------------------------


class TestTokenStructure:
    def test_access_token_is_valid_jwt(self, client, store):
        setup = _setup_pool_and_user(store)
        login_resp = client.post(
            "/login",
            data={
                "username": setup["username"],
                "password": setup["password"],
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
                "response_type": "code",
                "scope": "openid",
                "state": "",
                "code_challenge": "",
                "code_challenge_method": "",
            },
            follow_redirects=False,
        )
        code = parse_qs(urlparse(login_resp.headers["location"]).query)["code"][0]

        token_resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
            },
        )
        data = token_resp.json()

        # Decode access token
        parts = data["access_token"].split(".")
        assert len(parts) == 3  # header.payload.signature

        header = json.loads(base64.urlsafe_b64decode(parts[0] + "=="))
        assert header["alg"] == "RS256"
        assert header["typ"] == "JWT"

        payload = json.loads(base64.urlsafe_b64decode(parts[1] + "=="))
        assert payload["token_use"] == "access"
        assert payload["sub"] == setup["user_sub"]
        assert "iss" in payload
        assert "exp" in payload

    def test_id_token_contains_username(self, client, store):
        setup = _setup_pool_and_user(store)
        login_resp = client.post(
            "/login",
            data={
                "username": setup["username"],
                "password": setup["password"],
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
                "response_type": "code",
                "scope": "openid",
                "state": "",
                "code_challenge": "",
                "code_challenge_method": "",
            },
            follow_redirects=False,
        )
        code = parse_qs(urlparse(login_resp.headers["location"]).query)["code"][0]

        token_resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": setup["client_id"],
                "redirect_uri": "http://localhost/callback",
            },
        )
        data = token_resp.json()

        payload_part = data["id_token"].split(".")[1]
        payload = json.loads(base64.urlsafe_b64decode(payload_part + "=="))
        assert payload["token_use"] == "id"
        assert payload["cognito:username"] == setup["username"]
