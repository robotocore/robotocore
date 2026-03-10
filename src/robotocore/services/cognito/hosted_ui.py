"""Cognito Hosted UI -- OAuth2/OIDC endpoints.

Implements the standard Cognito hosted UI endpoints:
- GET  /oauth2/authorize  -- Authorization endpoint (renders login form or redirects)
- POST /oauth2/token      -- Token endpoint (authorization_code, client_credentials, refresh_token)
- GET  /oauth2/userInfo   -- UserInfo endpoint (returns claims from access token)
- GET  /.well-known/openid-configuration -- OIDC discovery document
- GET  /.well-known/jwks.json            -- JSON Web Key Set
- GET  /login             -- Login page (form submission handler)
- POST /login             -- Process login form
- GET  /logout            -- Logout endpoint
- POST /forgotpassword    -- Forgot password flow
- POST /confirmforgotpassword -- Confirm forgot password
"""

import base64
import hashlib
import json
import time
import uuid
from urllib.parse import urlencode

from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from robotocore.services.cognito.provider import (
    CognitoStore,
    _generate_jwt,
    _get_store,
    _new_id,
)

# ---------------------------------------------------------------------------
# HTML templates
# ---------------------------------------------------------------------------

_LOGIN_PAGE = """<!DOCTYPE html>
<html>
<head><title>Sign In</title>
<style>
body {{ font-family: sans-serif; display: flex; justify-content: center; padding-top: 60px;
       background: #f5f5f5; }}
.card {{ background: white; padding: 40px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,.1);
         width: 360px; }}
h2 {{ margin-top: 0; text-align: center; }}
input {{ width: 100%; padding: 10px; margin: 8px 0; box-sizing: border-box;
         border: 1px solid #ccc; border-radius: 4px; }}
button {{ width: 100%; padding: 10px; background: #0073bb; color: white; border: none;
          border-radius: 4px; cursor: pointer; font-size: 16px; }}
button:hover {{ background: #005fa3; }}
.error {{ color: #d32f2f; text-align: center; margin-bottom: 12px; }}
</style>
</head>
<body>
<div class="card">
<h2>Sign In</h2>
{error}
<form method="POST" action="/login">
<input type="hidden" name="client_id" value="{client_id}" />
<input type="hidden" name="redirect_uri" value="{redirect_uri}" />
<input type="hidden" name="response_type" value="{response_type}" />
<input type="hidden" name="scope" value="{scope}" />
<input type="hidden" name="state" value="{state}" />
<input type="hidden" name="code_challenge" value="{code_challenge}" />
<input type="hidden" name="code_challenge_method" value="{code_challenge_method}" />
<input name="username" placeholder="Username" required />
<input name="password" type="password" placeholder="Password" required />
<button type="submit">Sign In</button>
</form>
</div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Helper: resolve store + pool from domain
# ---------------------------------------------------------------------------


def _resolve_domain(
    domain: str, region: str = "us-east-1", account_id: str = "123456789012"
) -> tuple[CognitoStore, str] | None:
    """Find the store and pool_id for a given Cognito domain prefix.

    Scans all region/account stores for the domain.
    """
    from robotocore.services.cognito.provider import _stores

    # Check all stores for this domain
    for (_acct, _reg), st in _stores.items():
        with st.lock:
            pool_id = st.domains.get(domain)
            if pool_id:
                return st, pool_id

    # If not found in any store, try the default
    store = _get_store(region, account_id)
    with store.lock:
        pool_id = store.domains.get(domain)
        if pool_id:
            return store, pool_id
    return None


def _extract_domain_from_host(host: str) -> str:
    """Extract the Cognito domain prefix from a Host header.

    e.g. 'my-domain.auth.us-east-1.amazoncognito.com' -> 'my-domain'
    Also supports plain domain prefixes when running locally.
    """
    if ".auth." in host and ".amazoncognito.com" in host:
        return host.split(".auth.")[0]
    return host.split(":")[0]  # strip port


def _validate_redirect_uri(client: dict, redirect_uri: str) -> bool:
    """Validate that redirect_uri is in the client's allowed callback URLs."""
    callbacks = client.get("CallbackURLs", [])
    if not callbacks:
        return True  # No restriction if no callbacks configured
    return redirect_uri in callbacks


def _validate_pkce(code_verifier: str, code_challenge: str, method: str) -> bool:
    """Validate PKCE code_verifier against the stored code_challenge."""
    if method == "S256":
        digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
        computed = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
        return computed == code_challenge
    elif method == "plain":
        return code_verifier == code_challenge
    return False


def _parse_basic_auth(auth_header: str) -> tuple[str, str] | None:
    """Parse HTTP Basic auth header, returning (client_id, client_secret) or None."""
    if not auth_header.startswith("Basic "):
        return None
    try:
        decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
        client_id, client_secret = decoded.split(":", 1)
        return client_id, client_secret
    except Exception:
        return None


def _find_client_in_store(store: CognitoStore, pool_id: str, client_id: str) -> dict | None:
    """Look up a client in the store."""
    with store.lock:
        return store.clients.get(pool_id, {}).get(client_id)


def _generate_tokens(
    store: CognitoStore,
    pool_id: str,
    client_id: str,
    username: str,
    region: str,
    scopes: list[str] | None = None,
) -> dict:
    """Generate access, id, and refresh tokens for a user."""
    with store.lock:
        user = store.users.get(pool_id, {}).get(username, {})
    user_sub = user.get("UserSub", str(uuid.uuid4()))
    issuer = f"https://cognito-idp.{region}.amazonaws.com/{pool_id}"

    extra_access = {}
    if scopes:
        extra_access["scope"] = " ".join(scopes)

    access_token = _generate_jwt(user_sub, issuer, client_id, "access", extra_access)
    id_token = _generate_jwt(
        user_sub,
        issuer,
        client_id,
        "id",
        {"cognito:username": username, "email": _get_user_email(user)},
    )
    refresh_token = _new_id()

    # Store refresh token for later exchange
    with store.lock:
        store.refresh_tokens[refresh_token] = {
            "pool_id": pool_id,
            "client_id": client_id,
            "username": username,
            "sub": user_sub,
            "created": time.time(),
        }

    return {
        "access_token": access_token,
        "id_token": id_token,
        "refresh_token": refresh_token,
        "token_type": "Bearer",
        "expires_in": 3600,
    }


def _get_user_email(user: dict) -> str:
    """Extract email from user attributes."""
    for attr in user.get("Attributes", []):
        if attr.get("Name") == "email":
            return attr.get("Value", "")
    return ""


# ---------------------------------------------------------------------------
# OAuth2 Endpoints
# ---------------------------------------------------------------------------


async def oauth2_authorize(request: Request) -> Response:
    """GET /oauth2/authorize -- render login form or redirect with code."""
    params = dict(request.query_params)
    client_id = params.get("client_id", "")
    redirect_uri = params.get("redirect_uri", "")
    response_type = params.get("response_type", "code")
    scope = params.get("scope", "openid")
    state = params.get("state", "")
    code_challenge = params.get("code_challenge", "")
    code_challenge_method = params.get("code_challenge_method", "")

    if not client_id:
        return JSONResponse(
            {"error": "invalid_request", "error_description": "client_id required"}, status_code=400
        )

    if response_type not in ("code", "token"):
        return JSONResponse(
            {
                "error": "unsupported_response_type",
                "error_description": f"Unsupported response_type: {response_type}",
            },
            status_code=400,
        )

    # Render login form
    html = _LOGIN_PAGE.format(
        client_id=client_id,
        redirect_uri=redirect_uri,
        response_type=response_type,
        scope=scope,
        state=state,
        code_challenge=code_challenge,
        code_challenge_method=code_challenge_method,
        error="",
    )
    return HTMLResponse(html)


async def login_get(request: Request) -> Response:
    """GET /login -- same as authorize, renders login form."""
    return await oauth2_authorize(request)


async def login_post(request: Request) -> Response:
    """POST /login -- process login form submission."""
    form = await request.form()
    username = form.get("username", "")
    password = form.get("password", "")
    client_id = form.get("client_id", "")
    redirect_uri = form.get("redirect_uri", "")
    response_type = form.get("response_type", "code")
    scope = form.get("scope", "openid")
    state = form.get("state", "")
    code_challenge = form.get("code_challenge", "")
    code_challenge_method = form.get("code_challenge_method", "")

    # Find the pool that owns this client
    from robotocore.services.cognito.provider import _stores

    store = None
    pool_id = None
    client = None
    region = "us-east-1"

    for (_acct, _reg), st in _stores.items():
        with st.lock:
            for pid, clients in st.clients.items():
                if client_id in clients:
                    store = st
                    pool_id = pid
                    client = clients[client_id]
                    region = _reg
                    break
        if store:
            break

    if not store or not pool_id:
        return JSONResponse(
            {"error": "invalid_client", "error_description": "Client not found"},
            status_code=400,
        )

    # Validate redirect_uri
    if redirect_uri and not _validate_redirect_uri(client, redirect_uri):
        return JSONResponse(
            {"error": "invalid_request", "error_description": "Invalid redirect_uri"},
            status_code=400,
        )

    # Authenticate user
    with store.lock:
        user = store.users.get(pool_id, {}).get(username)
    if not user or user.get("Password") != password:
        html = _LOGIN_PAGE.format(
            client_id=client_id,
            redirect_uri=redirect_uri,
            response_type=response_type,
            scope=scope,
            state=state,
            code_challenge=code_challenge,
            code_challenge_method=code_challenge_method,
            error='<p class="error">Incorrect username or password.</p>',
        )
        return HTMLResponse(html, status_code=401)

    if user.get("UserStatus") == "UNCONFIRMED":
        html = _LOGIN_PAGE.format(
            client_id=client_id,
            redirect_uri=redirect_uri,
            response_type=response_type,
            scope=scope,
            state=state,
            code_challenge=code_challenge,
            code_challenge_method=code_challenge_method,
            error='<p class="error">User is not confirmed.</p>',
        )
        return HTMLResponse(html, status_code=403)

    scopes = scope.split() if scope else ["openid"]

    if response_type == "token":
        # Implicit flow: return tokens in fragment
        tokens = _generate_tokens(store, pool_id, client_id, username, region, scopes)
        fragment = urlencode(tokens)
        if state:
            fragment += f"&state={state}"
        return RedirectResponse(f"{redirect_uri}#{fragment}", status_code=302)

    # Authorization code flow
    auth_code = str(uuid.uuid4())
    with store.lock:
        store.auth_codes[auth_code] = {
            "pool_id": pool_id,
            "client_id": client_id,
            "username": username,
            "redirect_uri": redirect_uri,
            "scope": scope,
            "code_challenge": code_challenge,
            "code_challenge_method": code_challenge_method,
            "created": time.time(),
            "region": region,
        }

    params = {"code": auth_code}
    if state:
        params["state"] = state
    return RedirectResponse(f"{redirect_uri}?{urlencode(params)}", status_code=302)


async def oauth2_token(request: Request) -> Response:
    """POST /oauth2/token -- exchange code/credentials for tokens."""
    content_type = request.headers.get("content-type", "")

    # Parse body (form-urlencoded)
    if "application/x-www-form-urlencoded" in content_type:
        form = await request.form()
        params = dict(form)
    else:
        body = await request.body()
        if body:
            try:
                params = dict(
                    item.split("=", 1) for item in body.decode().split("&") if "=" in item
                )
            except Exception:
                params = {}
        else:
            params = {}

    grant_type = params.get("grant_type", "")

    # Parse Basic auth for client credentials
    auth_header = request.headers.get("authorization", "")
    basic_creds = _parse_basic_auth(auth_header) if auth_header else None

    if grant_type == "authorization_code":
        return await _handle_auth_code_grant(params, basic_creds)
    elif grant_type == "client_credentials":
        return await _handle_client_credentials_grant(params, basic_creds)
    elif grant_type == "refresh_token":
        return await _handle_refresh_token_grant(params, basic_creds)
    else:
        return JSONResponse(
            {
                "error": "unsupported_grant_type",
                "error_description": f"Unsupported grant_type: {grant_type}",
            },
            status_code=400,
        )


async def _handle_auth_code_grant(params: dict, basic_creds: tuple[str, str] | None) -> Response:
    """Exchange authorization code for tokens."""
    code = params.get("code", "")
    redirect_uri = params.get("redirect_uri", "")
    client_id = params.get("client_id", "")
    code_verifier = params.get("code_verifier", "")

    if basic_creds:
        client_id = basic_creds[0]

    if not code:
        return JSONResponse(
            {"error": "invalid_request", "error_description": "code is required"},
            status_code=400,
        )

    # Find the auth code across all stores
    from robotocore.services.cognito.provider import _stores

    auth_data = None
    store = None
    for (_acct, _reg), st in _stores.items():
        with st.lock:
            if code in st.auth_codes:
                auth_data = st.auth_codes.pop(code)
                store = st
                break

    if not auth_data or not store:
        return JSONResponse(
            {"error": "invalid_grant", "error_description": "Invalid authorization code"},
            status_code=400,
        )

    # Validate client_id matches
    if client_id and auth_data["client_id"] != client_id:
        return JSONResponse(
            {"error": "invalid_client", "error_description": "client_id mismatch"},
            status_code=400,
        )

    # Validate redirect_uri matches
    if redirect_uri and auth_data.get("redirect_uri") and redirect_uri != auth_data["redirect_uri"]:
        return JSONResponse(
            {"error": "invalid_request", "error_description": "redirect_uri mismatch"},
            status_code=400,
        )

    # Validate PKCE if code_challenge was provided
    if auth_data.get("code_challenge"):
        if not code_verifier:
            return JSONResponse(
                {
                    "error": "invalid_request",
                    "error_description": "code_verifier is required for PKCE",
                },
                status_code=400,
            )
        method = auth_data.get("code_challenge_method", "S256")
        if not _validate_pkce(code_verifier, auth_data["code_challenge"], method):
            return JSONResponse(
                {"error": "invalid_grant", "error_description": "Invalid code_verifier"},
                status_code=400,
            )

    # Validate client secret if client has one
    if basic_creds:
        client = _find_client_in_store(store, auth_data["pool_id"], basic_creds[0])
        if client and client.get("ClientSecret"):
            if basic_creds[1] != client["ClientSecret"]:
                return JSONResponse(
                    {"error": "invalid_client", "error_description": "Invalid client secret"},
                    status_code=401,
                )

    scopes = auth_data.get("scope", "openid").split()
    region = auth_data.get("region", "us-east-1")
    tokens = _generate_tokens(
        store,
        auth_data["pool_id"],
        auth_data["client_id"],
        auth_data["username"],
        region,
        scopes,
    )

    return JSONResponse(tokens)


async def _handle_client_credentials_grant(
    params: dict, basic_creds: tuple[str, str] | None
) -> Response:
    """Client credentials grant -- return access token with requested scopes."""
    client_id = params.get("client_id", "")
    client_secret = params.get("client_secret", "")
    scope = params.get("scope", "")

    if basic_creds:
        client_id, client_secret = basic_creds

    if not client_id:
        return JSONResponse(
            {"error": "invalid_client", "error_description": "client_id is required"},
            status_code=400,
        )

    # Find client across all stores
    from robotocore.services.cognito.provider import _stores

    store = None
    pool_id = None
    client = None
    region = "us-east-1"

    for (_acct, _reg), st in _stores.items():
        with st.lock:
            for pid, clients in st.clients.items():
                c = clients.get(client_id)
                if c:
                    store = st
                    pool_id = pid
                    client = c
                    region = _reg
                    break
        if store:
            break

    if not store or not client:
        return JSONResponse(
            {"error": "invalid_client", "error_description": "Client not found"},
            status_code=401,
        )

    # Validate client_credentials is an allowed flow
    allowed_flows = client.get("AllowedOAuthFlows", [])
    if allowed_flows and "client_credentials" not in allowed_flows:
        return JSONResponse(
            {
                "error": "unauthorized_client",
                "error_description": "client_credentials flow not allowed",
            },
            status_code=400,
        )

    # Validate client secret
    if client.get("ClientSecret"):
        if client["ClientSecret"] != client_secret:
            return JSONResponse(
                {"error": "invalid_client", "error_description": "Invalid client secret"},
                status_code=401,
            )

    scopes = scope.split() if scope else client.get("AllowedOAuthScopes", [])
    issuer = f"https://cognito-idp.{region}.amazonaws.com/{pool_id}"

    extra = {}
    if scopes:
        extra["scope"] = " ".join(scopes)

    access_token = _generate_jwt(client_id, issuer, client_id, "access", extra)

    return JSONResponse(
        {
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": 3600,
        }
    )


async def _handle_refresh_token_grant(
    params: dict, basic_creds: tuple[str, str] | None
) -> Response:
    """Refresh token grant -- return new tokens."""
    refresh_token = params.get("refresh_token", "")
    client_id = params.get("client_id", "")

    if basic_creds:
        client_id = basic_creds[0]

    if not refresh_token:
        return JSONResponse(
            {"error": "invalid_request", "error_description": "refresh_token is required"},
            status_code=400,
        )

    # Find refresh token across all stores
    from robotocore.services.cognito.provider import _stores

    token_data = None
    store = None

    for (_acct, _reg), st in _stores.items():
        with st.lock:
            if refresh_token in st.refresh_tokens:
                token_data = st.refresh_tokens[refresh_token]
                store = st
                break

    if not token_data or not store:
        return JSONResponse(
            {"error": "invalid_grant", "error_description": "Invalid refresh token"},
            status_code=400,
        )

    # Validate client_id matches
    if client_id and token_data["client_id"] != client_id:
        return JSONResponse(
            {"error": "invalid_client", "error_description": "client_id mismatch"},
            status_code=400,
        )

    pool_id = token_data["pool_id"]
    region = pool_id.split("_")[0] if "_" in pool_id else "us-east-1"
    tokens = _generate_tokens(
        store,
        pool_id,
        token_data["client_id"],
        token_data["username"],
        region,
    )

    return JSONResponse(tokens)


async def oauth2_userinfo(request: Request) -> Response:
    """GET /oauth2/userInfo -- return user claims from access token."""
    auth_header = request.headers.get("authorization", "")
    access_token = ""

    if auth_header.startswith("Bearer "):
        access_token = auth_header[7:]
    else:
        access_token = request.query_params.get("access_token", "")

    if not access_token:
        return JSONResponse(
            {"error": "invalid_token", "error_description": "Access token required"},
            status_code=401,
        )

    # Decode the JWT
    try:
        payload_part = access_token.split(".")[1]
        padding = 4 - len(payload_part) % 4
        if padding != 4:
            payload_part += "=" * padding
        payload = json.loads(base64.urlsafe_b64decode(payload_part))
    except Exception:
        return JSONResponse(
            {"error": "invalid_token", "error_description": "Invalid access token"},
            status_code=401,
        )

    sub = payload.get("sub", "")

    # Find the user across all stores
    from robotocore.services.cognito.provider import _stores

    for (_acct, _reg), st in _stores.items():
        with st.lock:
            for pool_id, users in st.users.items():
                for username, user in users.items():
                    if user.get("UserSub") == sub:
                        claims = {
                            "sub": sub,
                            "username": username,
                            "email_verified": "true",
                        }
                        for attr in user.get("Attributes", []):
                            name = attr.get("Name", "")
                            val = attr.get("Value", "")
                            if name == "email":
                                claims["email"] = val
                            elif name == "phone_number":
                                claims["phone_number"] = val
                            elif name == "name":
                                claims["name"] = val
                            elif name.startswith("custom:"):
                                claims[name] = val
                        return JSONResponse(claims)

    return JSONResponse(
        {"error": "invalid_token", "error_description": "User not found"},
        status_code=401,
    )


async def openid_configuration(request: Request) -> Response:
    """GET /.well-known/openid-configuration -- OIDC discovery document."""
    # Determine issuer from Host header or default
    host = request.headers.get("host", "localhost:4566")
    scheme = "https" if request.url.scheme == "https" else "http"
    base_url = f"{scheme}://{host}"

    # Try to find the pool from the domain in the Host header
    domain = _extract_domain_from_host(host)
    resolved = _resolve_domain(domain)
    if resolved:
        _store, pool_id = resolved
        region = pool_id.split("_")[0] if "_" in pool_id else "us-east-1"
        issuer = f"https://cognito-idp.{region}.amazonaws.com/{pool_id}"
    else:
        issuer = base_url

    return JSONResponse(
        {
            "issuer": issuer,
            "authorization_endpoint": f"{base_url}/oauth2/authorize",
            "token_endpoint": f"{base_url}/oauth2/token",
            "userinfo_endpoint": f"{base_url}/oauth2/userInfo",
            "jwks_uri": f"{base_url}/.well-known/jwks.json",
            "response_types_supported": ["code", "token"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["RS256"],
            "scopes_supported": ["openid", "email", "phone", "profile"],
            "token_endpoint_auth_methods_supported": [
                "client_secret_basic",
                "client_secret_post",
            ],
            "claims_supported": [
                "sub",
                "iss",
                "aud",
                "exp",
                "iat",
                "auth_time",
                "email",
                "email_verified",
                "name",
                "phone_number",
                "cognito:username",
            ],
            "code_challenge_methods_supported": ["S256", "plain"],
            "grant_types_supported": [
                "authorization_code",
                "client_credentials",
                "refresh_token",
            ],
        }
    )


_MOCK_RSA_N = (
    "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86z"
    "wu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsG"
    "Y4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAt"
    "aSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFT"
    "WhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-"
    "kEgU8awapJzKnqDKgw"
)


async def jwks_json(request: Request) -> Response:
    """GET /.well-known/jwks.json -- JSON Web Key Set (mock keys for testing)."""
    return JSONResponse(
        {
            "keys": [
                {
                    "kty": "RSA",
                    "alg": "RS256",
                    "use": "sig",
                    "kid": "robotocore-test-key-1",
                    "n": _MOCK_RSA_N,
                    "e": "AQAB",
                }
            ]
        }
    )


async def logout(request: Request) -> Response:
    """GET /logout -- clear session and redirect."""
    params = dict(request.query_params)
    logout_uri = params.get("logout_uri", params.get("redirect_uri", ""))
    client_id = params.get("client_id", "")

    if logout_uri:
        redirect_params = {}
        if client_id:
            redirect_params["client_id"] = client_id
        if redirect_params:
            return RedirectResponse(f"{logout_uri}?{urlencode(redirect_params)}", status_code=302)
        return RedirectResponse(logout_uri, status_code=302)

    return HTMLResponse("<html><body><h2>You have been signed out.</h2></body></html>")


async def forgot_password_endpoint(request: Request) -> Response:
    """POST /forgotpassword -- initiate password reset."""
    form = await request.form()
    client_id = form.get("client_id", "")
    username = form.get("username", "")

    if not client_id or not username:
        return JSONResponse(
            {
                "error": "invalid_request",
                "error_description": "client_id and username are required",
            },
            status_code=400,
        )

    # Find user
    from robotocore.services.cognito.provider import _stores

    for (_acct, _reg), st in _stores.items():
        with st.lock:
            for pid, clients in st.clients.items():
                if client_id in clients:
                    user = st.users.get(pid, {}).get(username)
                    if user:
                        return JSONResponse(
                            {
                                "CodeDeliveryDetails": {
                                    "Destination": "***",
                                    "DeliveryMedium": "EMAIL",
                                    "AttributeName": "email",
                                }
                            }
                        )

    return JSONResponse(
        {"error": "invalid_request", "error_description": "User not found"},
        status_code=400,
    )


async def confirm_forgot_password_endpoint(request: Request) -> Response:
    """POST /confirmforgotpassword -- confirm password reset with code."""
    form = await request.form()
    client_id = form.get("client_id", "")
    username = form.get("username", "")
    password = form.get("password", "")
    # confirmation_code accepted but not validated in emulator mode

    if not client_id or not username or not password:
        return JSONResponse(
            {
                "error": "invalid_request",
                "error_description": "client_id, username, and password are required",
            },
            status_code=400,
        )

    from robotocore.services.cognito.provider import _stores

    for (_acct, _reg), st in _stores.items():
        with st.lock:
            for pid, clients in st.clients.items():
                if client_id in clients:
                    user = st.users.get(pid, {}).get(username)
                    if user:
                        user["Password"] = password
                        user["LastModifiedDate"] = time.time()
                        return JSONResponse({"success": True})

    return JSONResponse(
        {"error": "invalid_request", "error_description": "User not found"},
        status_code=400,
    )
