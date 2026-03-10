"""End-to-end tests for Cognito Hosted UI.

Full flow: create pool -> create domain -> create app client -> authorize -> get code
-> exchange for token -> get userInfo
"""

import base64
import hashlib
import json
from urllib.parse import parse_qs, urlparse

import pytest
from starlette.testclient import TestClient

from robotocore.services.cognito.provider import (
    _stores,
)


@pytest.fixture(autouse=True)
def _clear_stores():
    """Clear all Cognito stores between tests."""
    _stores.clear()
    yield
    _stores.clear()


@pytest.fixture()
def client():
    """Starlette test client."""
    from robotocore.gateway.app import app

    return TestClient(app, raise_server_exceptions=False)


def _cognito_api(client, action: str, body: dict) -> dict:
    """Helper to call the Cognito IDP API via X-Amz-Target."""
    resp = client.post(
        "/",
        content=json.dumps(body),
        headers={
            "Content-Type": "application/x-amz-json-1.1",
            "X-Amz-Target": f"AWSCognitoIdentityProviderService.{action}",
            "Authorization": (
                "AWS4-HMAC-SHA256 "
                "Credential=123456789012/20260101/us-east-1/cognito-idp/aws4_request, "
                "SignedHeaders=host;x-amz-target, "
                "Signature=fake"
            ),
        },
    )
    return {"status": resp.status_code, "body": resp.json() if resp.content else {}}


class TestFullAuthCodeFlow:
    """Test the complete authorization code flow from pool creation to userInfo."""

    def test_full_flow(self, client):
        # 1. Create user pool
        result = _cognito_api(client, "CreateUserPool", {"PoolName": "E2ETestPool"})
        assert result["status"] == 200
        pool_id = result["body"]["UserPool"]["Id"]

        # 2. Create app client
        result = _cognito_api(
            client,
            "CreateUserPoolClient",
            {
                "UserPoolId": pool_id,
                "ClientName": "E2EClient",
                "ExplicitAuthFlows": ["USER_PASSWORD_AUTH"],
                "CallbackURLs": ["http://localhost:3000/callback"],
                "AllowedOAuthFlows": ["code"],
                "AllowedOAuthScopes": ["openid", "email"],
            },
        )
        assert result["status"] == 200
        client_id = result["body"]["UserPoolClient"]["ClientId"]

        # 3. Create domain
        result = _cognito_api(
            client,
            "CreateUserPoolDomain",
            {"UserPoolId": pool_id, "Domain": "e2e-test-domain"},
        )
        assert result["status"] == 200

        # 4. Sign up a user
        result = _cognito_api(
            client,
            "SignUp",
            {
                "ClientId": client_id,
                "Username": "e2euser",
                "Password": "E2EPass1!",
                "UserAttributes": [{"Name": "email", "Value": "e2e@example.com"}],
            },
        )
        assert result["status"] == 200

        # 5. Confirm sign up
        result = _cognito_api(
            client,
            "AdminConfirmSignUp",
            {"UserPoolId": pool_id, "Username": "e2euser"},
        )
        assert result["status"] == 200

        # 6. Visit authorize endpoint
        resp = client.get(
            "/oauth2/authorize",
            params={
                "client_id": client_id,
                "redirect_uri": "http://localhost:3000/callback",
                "response_type": "code",
                "scope": "openid email",
            },
        )
        assert resp.status_code == 200
        assert "Sign In" in resp.text

        # 7. Submit login form
        resp = client.post(
            "/login",
            data={
                "username": "e2euser",
                "password": "E2EPass1!",
                "client_id": client_id,
                "redirect_uri": "http://localhost:3000/callback",
                "response_type": "code",
                "scope": "openid email",
                "state": "e2e-state",
                "code_challenge": "",
                "code_challenge_method": "",
            },
            follow_redirects=False,
        )
        assert resp.status_code == 302
        location = resp.headers["location"]
        parsed = urlparse(location)
        assert parsed.netloc == "localhost:3000"
        assert parsed.path == "/callback"
        qs = parse_qs(parsed.query)
        assert "code" in qs
        assert qs["state"] == ["e2e-state"]
        auth_code = qs["code"][0]

        # 8. Exchange code for tokens
        resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "code": auth_code,
                "client_id": client_id,
                "redirect_uri": "http://localhost:3000/callback",
            },
        )
        assert resp.status_code == 200
        tokens = resp.json()
        assert "access_token" in tokens
        assert "id_token" in tokens
        assert "refresh_token" in tokens
        assert tokens["token_type"] == "Bearer"
        assert tokens["expires_in"] == 3600

        # 9. Get userInfo
        resp = client.get(
            "/oauth2/userInfo",
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        )
        assert resp.status_code == 200
        user_info = resp.json()
        assert user_info["username"] == "e2euser"
        assert user_info["email"] == "e2e@example.com"

        # 10. Verify OIDC discovery
        resp = client.get("/.well-known/openid-configuration")
        assert resp.status_code == 200
        oidc = resp.json()
        assert "issuer" in oidc
        assert "/oauth2/token" in oidc["token_endpoint"]

        # 11. Verify JWKS
        resp = client.get("/.well-known/jwks.json")
        assert resp.status_code == 200
        jwks = resp.json()
        assert len(jwks["keys"]) >= 1


class TestFullPKCEFlow:
    """Test the complete PKCE flow end to end."""

    def test_pkce_flow(self, client):
        # Setup pool, client, user, domain
        result = _cognito_api(client, "CreateUserPool", {"PoolName": "PKCEPool"})
        pool_id = result["body"]["UserPool"]["Id"]

        result = _cognito_api(
            client,
            "CreateUserPoolClient",
            {
                "UserPoolId": pool_id,
                "ClientName": "PKCEClient",
                "CallbackURLs": ["http://localhost:3000/callback"],
            },
        )
        client_id = result["body"]["UserPoolClient"]["ClientId"]

        _cognito_api(
            client,
            "CreateUserPoolDomain",
            {"UserPoolId": pool_id, "Domain": "pkce-domain"},
        )

        _cognito_api(
            client,
            "SignUp",
            {
                "ClientId": client_id,
                "Username": "pkceuser",
                "Password": "PKCEPass1!",
                "UserAttributes": [{"Name": "email", "Value": "pkce@test.com"}],
            },
        )
        _cognito_api(
            client,
            "AdminConfirmSignUp",
            {"UserPoolId": pool_id, "Username": "pkceuser"},
        )

        # Generate PKCE challenge
        code_verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
        digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
        code_challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")

        # Login with PKCE challenge
        resp = client.post(
            "/login",
            data={
                "username": "pkceuser",
                "password": "PKCEPass1!",
                "client_id": client_id,
                "redirect_uri": "http://localhost:3000/callback",
                "response_type": "code",
                "scope": "openid",
                "state": "",
                "code_challenge": code_challenge,
                "code_challenge_method": "S256",
            },
            follow_redirects=False,
        )
        assert resp.status_code == 302
        auth_code = parse_qs(urlparse(resp.headers["location"]).query)["code"][0]

        # Exchange with verifier
        resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "code": auth_code,
                "client_id": client_id,
                "redirect_uri": "http://localhost:3000/callback",
                "code_verifier": code_verifier,
            },
        )
        assert resp.status_code == 200
        tokens = resp.json()
        assert "access_token" in tokens
        assert "id_token" in tokens

        # Wrong verifier should fail (need a new code)
        # First get a new code
        resp = client.post(
            "/login",
            data={
                "username": "pkceuser",
                "password": "PKCEPass1!",
                "client_id": client_id,
                "redirect_uri": "http://localhost:3000/callback",
                "response_type": "code",
                "scope": "openid",
                "state": "",
                "code_challenge": code_challenge,
                "code_challenge_method": "S256",
            },
            follow_redirects=False,
        )
        auth_code2 = parse_qs(urlparse(resp.headers["location"]).query)["code"][0]

        resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "code": auth_code2,
                "client_id": client_id,
                "redirect_uri": "http://localhost:3000/callback",
                "code_verifier": "wrong-verifier",
            },
        )
        assert resp.status_code == 400
        assert resp.json()["error"] == "invalid_grant"


class TestClientCredentialsE2E:
    """Test client credentials flow end to end."""

    def test_client_credentials_flow(self, client):
        # Create pool
        result = _cognito_api(client, "CreateUserPool", {"PoolName": "CCPool"})
        pool_id = result["body"]["UserPool"]["Id"]

        # Create client with secret and client_credentials flow
        result = _cognito_api(
            client,
            "CreateUserPoolClient",
            {
                "UserPoolId": pool_id,
                "ClientName": "CCClient",
                "GenerateSecret": True,
                "AllowedOAuthFlows": ["client_credentials"],
                "AllowedOAuthScopes": ["api/read", "api/write"],
            },
        )
        client_data = result["body"]["UserPoolClient"]
        cc_client_id = client_data["ClientId"]
        cc_secret = client_data["ClientSecret"]

        # Create domain
        _cognito_api(
            client,
            "CreateUserPoolDomain",
            {"UserPoolId": pool_id, "Domain": "cc-domain"},
        )

        # Get token via client credentials with Basic auth
        creds = base64.b64encode(f"{cc_client_id}:{cc_secret}".encode()).decode()
        resp = client.post(
            "/oauth2/token",
            data={"grant_type": "client_credentials", "scope": "api/read"},
            headers={"Authorization": f"Basic {creds}"},
        )
        assert resp.status_code == 200
        tokens = resp.json()
        assert "access_token" in tokens
        assert tokens["token_type"] == "Bearer"
        assert "id_token" not in tokens  # client_credentials doesn't return id_token


class TestRefreshTokenE2E:
    """Test refresh token flow end to end."""

    def test_refresh_token_returns_new_tokens(self, client):
        # Setup
        result = _cognito_api(client, "CreateUserPool", {"PoolName": "RTPool"})
        pool_id = result["body"]["UserPool"]["Id"]

        result = _cognito_api(
            client,
            "CreateUserPoolClient",
            {
                "UserPoolId": pool_id,
                "ClientName": "RTClient",
                "CallbackURLs": ["http://localhost:3000/callback"],
            },
        )
        rt_client_id = result["body"]["UserPoolClient"]["ClientId"]

        _cognito_api(
            client,
            "SignUp",
            {
                "ClientId": rt_client_id,
                "Username": "rtuser",
                "Password": "RTPass1!",
                "UserAttributes": [{"Name": "email", "Value": "rt@test.com"}],
            },
        )
        _cognito_api(
            client,
            "AdminConfirmSignUp",
            {"UserPoolId": pool_id, "Username": "rtuser"},
        )

        # Get initial tokens
        resp = client.post(
            "/login",
            data={
                "username": "rtuser",
                "password": "RTPass1!",
                "client_id": rt_client_id,
                "redirect_uri": "http://localhost:3000/callback",
                "response_type": "code",
                "scope": "openid",
                "state": "",
                "code_challenge": "",
                "code_challenge_method": "",
            },
            follow_redirects=False,
        )
        code = parse_qs(urlparse(resp.headers["location"]).query)["code"][0]

        resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": rt_client_id,
                "redirect_uri": "http://localhost:3000/callback",
            },
        )
        initial_tokens = resp.json()
        refresh_token = initial_tokens["refresh_token"]

        # Use refresh token
        resp = client.post(
            "/oauth2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": rt_client_id,
            },
        )
        assert resp.status_code == 200
        new_tokens = resp.json()
        assert "access_token" in new_tokens
        assert "id_token" in new_tokens
        # New tokens should be different
        assert new_tokens["access_token"] != initial_tokens["access_token"]


class TestDomainOperations:
    """Test domain CRUD via Cognito API."""

    def test_create_and_describe_domain(self, client):
        result = _cognito_api(client, "CreateUserPool", {"PoolName": "DomainPool"})
        pool_id = result["body"]["UserPool"]["Id"]

        result = _cognito_api(
            client,
            "CreateUserPoolDomain",
            {"UserPoolId": pool_id, "Domain": "my-test-domain"},
        )
        assert result["status"] == 200

        result = _cognito_api(
            client,
            "DescribeUserPoolDomain",
            {"Domain": "my-test-domain"},
        )
        assert result["status"] == 200
        desc = result["body"]["DomainDescription"]
        assert desc["UserPoolId"] == pool_id
        assert desc["Domain"] == "my-test-domain"
        assert desc["Status"] == "ACTIVE"

    def test_describe_nonexistent_domain(self, client):
        result = _cognito_api(
            client,
            "DescribeUserPoolDomain",
            {"Domain": "nonexistent"},
        )
        assert result["status"] == 200
        # Returns empty description (not an error)
        assert result["body"]["DomainDescription"] == {}

    def test_delete_domain(self, client):
        result = _cognito_api(client, "CreateUserPool", {"PoolName": "DelDomPool"})
        pool_id = result["body"]["UserPool"]["Id"]

        _cognito_api(
            client,
            "CreateUserPoolDomain",
            {"UserPoolId": pool_id, "Domain": "del-domain"},
        )

        result = _cognito_api(
            client,
            "DeleteUserPoolDomain",
            {"UserPoolId": pool_id, "Domain": "del-domain"},
        )
        assert result["status"] == 200

        # Verify domain is gone
        result = _cognito_api(
            client,
            "DescribeUserPoolDomain",
            {"Domain": "del-domain"},
        )
        assert result["body"]["DomainDescription"] == {}

    def test_duplicate_domain(self, client):
        result = _cognito_api(client, "CreateUserPool", {"PoolName": "DupPool1"})
        pool_id1 = result["body"]["UserPool"]["Id"]

        result = _cognito_api(client, "CreateUserPool", {"PoolName": "DupPool2"})
        pool_id2 = result["body"]["UserPool"]["Id"]

        _cognito_api(
            client,
            "CreateUserPoolDomain",
            {"UserPoolId": pool_id1, "Domain": "shared-domain"},
        )

        result = _cognito_api(
            client,
            "CreateUserPoolDomain",
            {"UserPoolId": pool_id2, "Domain": "shared-domain"},
        )
        assert result["status"] == 400  # Should fail - domain already taken


class TestLogoutFlow:
    """Test the logout endpoint."""

    def test_logout_with_redirect(self, client):
        resp = client.get(
            "/logout",
            params={
                "logout_uri": "http://localhost:3000/",
                "client_id": "test",
            },
            follow_redirects=False,
        )
        assert resp.status_code == 302
        assert "localhost:3000" in resp.headers["location"]

    def test_logout_without_redirect(self, client):
        resp = client.get("/logout")
        assert resp.status_code == 200
        assert "signed out" in resp.text.lower()
