"""Native Cognito Identity Provider with auth flows and JWT generation.

Uses JSON protocol via X-Amz-Target: AWSCognitoIdentityProviderService.{Action}.
"""

import base64
import hashlib
import hmac
import json
import threading
import time
import uuid
from collections.abc import Callable

from starlette.requests import Request
from starlette.responses import Response

# ---------------------------------------------------------------------------
# In-memory stores (region-scoped)
# ---------------------------------------------------------------------------

DEFAULT_ACCOUNT_ID = "123456789012"

_stores: dict[tuple[str, str], "CognitoStore"] = {}
_lock = threading.RLock()


class CognitoStore:
    """Per-region in-memory store for Cognito resources."""

    def __init__(self) -> None:
        self.pools: dict[str, dict] = {}  # pool_id -> pool
        self.users: dict[str, dict[str, dict]] = {}  # pool_id -> username -> user
        self.clients: dict[str, dict[str, dict]] = {}  # pool_id -> client_id -> client
        self.groups: dict[str, dict[str, dict]] = {}  # pool_id -> group_name -> group
        self.user_groups: dict[str, dict[str, list[str]]] = {}  # pool_id -> user -> [groups]
        self.domains: dict[str, str] = {}  # domain_prefix -> pool_id
        self.auth_codes: dict[str, dict] = {}  # code -> {pool_id, client_id, username, ...}
        self.refresh_tokens: dict[str, dict] = {}  # refresh_token -> {pool_id, username, ...}
        self.lock = threading.RLock()


def _get_store(region: str = "us-east-1", account_id: str = DEFAULT_ACCOUNT_ID) -> CognitoStore:
    key = (account_id, region)
    with _lock:
        if key not in _stores:
            _stores[key] = CognitoStore()
        return _stores[key]


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class CognitoError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------


def _generate_jwt(
    sub: str,
    issuer: str,
    audience: str,
    token_use: str = "access",
    extra_claims: dict | None = None,
) -> str:
    """Generate a fake JWT token (unsigned, for local testing)."""
    header = {"alg": "RS256", "typ": "JWT", "kid": str(uuid.uuid4())}
    now = int(time.time())
    payload = {
        "sub": sub,
        "iss": issuer,
        "aud": audience,
        "token_use": token_use,
        "exp": now + 3600,
        "iat": now,
        "auth_time": now,
    }
    if extra_claims:
        payload.update(extra_claims)

    def _b64(d: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()

    # Fake signature (not cryptographically valid, but structurally correct)
    sig = base64.urlsafe_b64encode(b"fake-signature").rstrip(b"=").decode()
    return f"{_b64(header)}.{_b64(payload)}.{sig}"


def _new_id() -> str:
    return str(uuid.uuid4())


def _secret_hash(username: str, client_id: str, client_secret: str) -> str:
    msg = (username + client_id).encode("utf-8")
    return base64.b64encode(
        hmac.new(client_secret.encode("utf-8"), msg, hashlib.sha256).digest()
    ).decode("utf-8")


# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------


async def handle_cognito_request(request: Request, region: str, account_id: str) -> Response:
    """Handle a Cognito Identity Provider API request."""
    body = await request.body()
    target = request.headers.get("x-amz-target", "")

    if not target:
        return _error("InvalidAction", "Missing X-Amz-Target header", 400)

    action = target.split(".")[-1]
    params = json.loads(body) if body else {}

    store = _get_store(region, account_id)
    handler = _ACTION_MAP.get(action)
    if handler is None:
        from robotocore.providers.moto_bridge import forward_to_moto

        return await forward_to_moto(request, "cognito-idp", account_id=account_id)

    try:
        result = handler(store, params, region, account_id)
        return _json_response(result)
    except CognitoError as e:
        return _error(e.code, e.message, e.status)
    except Exception as e:
        return _error("InternalError", str(e), 500)


# ---------------------------------------------------------------------------
# Moto backend sync — keep Moto aware of pools we create/delete natively
# so that operations we don't handle (e.g. CreateIdentityProvider) work.
# ---------------------------------------------------------------------------


def _sync_pool_to_moto(
    pool_id: str, pool_name: str, params: dict, region: str, account_id: str
) -> None:
    """Create a matching pool in Moto's backend."""
    try:
        from moto.backends import get_backend

        backend = get_backend("cognito-idp")[account_id][region]
        # Use Moto's create_user_pool; it returns a CognitoIdpUserPool.
        # We override the auto-generated ID with ours so lookups match.
        moto_pool = backend.create_user_pool(pool_name, params)
        old_id = moto_pool.id
        moto_pool.id = pool_id
        # Remove the auto-generated key and re-key with our ID
        backend.user_pools.pop(old_id, None)
        backend.user_pools[pool_id] = moto_pool
    except Exception:
        pass  # Best-effort: if Moto isn't available, native-only ops still work


def _sync_user_to_moto(
    pool_id: str, username: str, password: str, region: str, account_id: str
) -> None:
    """Create a matching user in Moto's backend."""
    try:
        from moto.backends import get_backend

        backend = get_backend("cognito-idp")[account_id][region]
        backend.admin_create_user(pool_id, username, password, {})
    except Exception:
        pass


def _sync_client_to_moto(
    pool_id: str, client_id: str, client_name: str, region: str, account_id: str
) -> None:
    """Create a matching client in Moto's backend and override its ID."""
    try:
        from moto.backends import get_backend

        backend = get_backend("cognito-idp")[account_id][region]
        moto_client = backend.create_user_pool_client(pool_id, False, {"ClientName": client_name})
        old_id = moto_client.id
        moto_client.id = client_id
        # Re-key in the pool's clients dict
        pool = backend.user_pools.get(pool_id)
        if pool:
            pool.clients.pop(old_id, None)
            pool.clients[client_id] = moto_client
    except Exception:
        pass


def _delete_pool_from_moto(pool_id: str, region: str, account_id: str) -> None:
    """Remove a pool from Moto's backend."""
    try:
        from moto.backends import get_backend

        backend = get_backend("cognito-idp")[account_id][region]
        backend.user_pools.pop(pool_id, None)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# User Pool CRUD
# ---------------------------------------------------------------------------


def _create_user_pool(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    pool_id = f"{region}_{_new_id()[:8]}"
    pool_name = params.get("PoolName", "")
    if not pool_name:
        raise CognitoError("InvalidParameterException", "PoolName is required.")

    pool = {
        "Id": pool_id,
        "Name": pool_name,
        "Arn": f"arn:aws:cognito-idp:{region}:{account_id}:userpool/{pool_id}",
        "CreationDate": time.time(),
        "LastModifiedDate": time.time(),
        "Status": "Enabled",
        "Policies": params.get("Policies", {}),
        "LambdaConfig": params.get("LambdaConfig", {}),
        "AutoVerifiedAttributes": params.get("AutoVerifiedAttributes", []),
        "Schema": params.get("Schema", []),
        "MfaConfiguration": params.get("MfaConfiguration", "OFF"),
    }
    if "EmailConfiguration" in params:
        pool["EmailConfiguration"] = params["EmailConfiguration"]
    if "SmsConfiguration" in params:
        pool["SmsConfiguration"] = params["SmsConfiguration"]
    if "AdminCreateUserConfig" in params:
        pool["AdminCreateUserConfig"] = params["AdminCreateUserConfig"]
    if "UsernameAttributes" in params:
        pool["UsernameAttributes"] = params["UsernameAttributes"]

    with store.lock:
        store.pools[pool_id] = pool
        store.users[pool_id] = {}
        store.clients[pool_id] = {}
        store.groups[pool_id] = {}
        store.user_groups[pool_id] = {}

    # Mirror to Moto backend so operations that fall through to Moto
    # (e.g. CreateIdentityProvider, CreateResourceServer) can find the pool.
    _sync_pool_to_moto(pool_id, pool_name, params, region, account_id)

    return {"UserPool": pool}


def _describe_user_pool(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    pool_id = params.get("UserPoolId", "")
    with store.lock:
        pool = store.pools.get(pool_id)
    if not pool:
        raise CognitoError("ResourceNotFoundException", f"User pool {pool_id} does not exist.", 404)
    # AWS returns Schema as SchemaAttributes in describe
    result = dict(pool)
    if "Schema" in result:
        result["SchemaAttributes"] = result.pop("Schema")
    return {"UserPool": result}


def _delete_user_pool(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    pool_id = params.get("UserPoolId", "")
    with store.lock:
        if pool_id not in store.pools:
            raise CognitoError(
                "ResourceNotFoundException",
                f"User pool {pool_id} does not exist.",
                404,
            )
        del store.pools[pool_id]
        store.users.pop(pool_id, None)
        store.clients.pop(pool_id, None)
        store.groups.pop(pool_id, None)
        store.user_groups.pop(pool_id, None)
    _delete_pool_from_moto(pool_id, region, account_id)
    return {}


def _list_user_pools(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    max_results = int(params.get("MaxResults", 60))
    with store.lock:
        pools = list(store.pools.values())
    return {
        "UserPools": [
            {
                "Id": p["Id"],
                "Name": p["Name"],
                "Status": p["Status"],
                "CreationDate": p["CreationDate"],
                "LastModifiedDate": p["LastModifiedDate"],
            }
            for p in pools[:max_results]
        ]
    }


# ---------------------------------------------------------------------------
# User Pool Client CRUD
# ---------------------------------------------------------------------------


def _create_user_pool_client(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    pool_id = params.get("UserPoolId", "")
    _require_pool(store, pool_id)

    client_id = _new_id().replace("-", "")[:26]
    client_name = params.get("ClientName", "")
    generate_secret = params.get("GenerateSecret", False)

    client = {
        "ClientId": client_id,
        "ClientName": client_name,
        "UserPoolId": pool_id,
        "CreationDate": time.time(),
        "LastModifiedDate": time.time(),
        "ExplicitAuthFlows": params.get("ExplicitAuthFlows", []),
        "AllowedOAuthFlows": params.get("AllowedOAuthFlows", []),
        "AllowedOAuthScopes": params.get("AllowedOAuthScopes", []),
        "CallbackURLs": params.get("CallbackURLs", []),
    }
    if generate_secret:
        client["ClientSecret"] = _new_id()

    with store.lock:
        store.clients[pool_id][client_id] = client

    _sync_client_to_moto(pool_id, client_id, client_name, region, account_id)

    return {"UserPoolClient": client}


def _describe_user_pool_client(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    pool_id = params.get("UserPoolId", "")
    client_id = params.get("ClientId", "")
    _require_pool(store, pool_id)

    with store.lock:
        client = store.clients.get(pool_id, {}).get(client_id)
    if not client:
        raise CognitoError("ResourceNotFoundException", f"Client {client_id} does not exist.", 404)
    return {"UserPoolClient": client}


def _delete_user_pool_client(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    pool_id = params.get("UserPoolId", "")
    client_id = params.get("ClientId", "")
    _require_pool(store, pool_id)

    with store.lock:
        clients = store.clients.get(pool_id, {})
        if client_id not in clients:
            raise CognitoError(
                "ResourceNotFoundException",
                f"Client {client_id} does not exist.",
                404,
            )
        del clients[client_id]
    return {}


def _update_user_pool_client(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    pool_id = params.get("UserPoolId", "")
    client_id = params.get("ClientId", "")
    _require_pool(store, pool_id)

    with store.lock:
        client = store.clients.get(pool_id, {}).get(client_id)
        if not client:
            raise CognitoError(
                "ResourceNotFoundException", f"Client {client_id} does not exist.", 404
            )
        # Update mutable fields
        updatable = [
            "ClientName",
            "ExplicitAuthFlows",
            "AllowedOAuthFlows",
            "AllowedOAuthScopes",
            "CallbackURLs",
            "LogoutURLs",
            "DefaultRedirectURI",
            "ReadAttributes",
            "WriteAttributes",
            "SupportedIdentityProviders",
            "AllowedOAuthFlowsUserPoolClient",
            "TokenValidityUnits",
            "AccessTokenValidity",
            "IdTokenValidity",
            "RefreshTokenValidity",
        ]
        for key in updatable:
            if key in params:
                client[key] = params[key]
        client["LastModifiedDate"] = time.time()

    return {"UserPoolClient": client}


def _list_user_pool_clients(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    pool_id = params.get("UserPoolId", "")
    _require_pool(store, pool_id)

    with store.lock:
        clients = list(store.clients.get(pool_id, {}).values())
    return {
        "UserPoolClients": [
            {
                "ClientId": c["ClientId"],
                "ClientName": c["ClientName"],
                "UserPoolId": c["UserPoolId"],
            }
            for c in clients
        ]
    }


# ---------------------------------------------------------------------------
# User operations
# ---------------------------------------------------------------------------


def _sign_up(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    client_id = params.get("ClientId", "")
    username = params.get("Username", "")
    password = params.get("Password", "")

    # Find pool by client_id
    pool_id = _find_pool_by_client(store, client_id)

    with store.lock:
        users = store.users.get(pool_id, {})
        if username in users:
            raise CognitoError(
                "UsernameExistsException",
                f"User {username} already exists.",
            )

    user_sub = _new_id()
    user_attrs = params.get("UserAttributes", [])

    user = {
        "Username": username,
        "UserSub": user_sub,
        "Password": password,
        "Enabled": True,
        "UserStatus": "UNCONFIRMED",
        "Attributes": user_attrs,
        "CreationDate": time.time(),
        "LastModifiedDate": time.time(),
        "MFAOptions": [],
    }

    # Lambda trigger: PreSignUp
    pool = store.pools.get(pool_id, {})
    _invoke_trigger(
        pool,
        "PreSignUp",
        {
            "userPoolId": pool_id,
            "userName": username,
            "request": {"userAttributes": _attrs_to_dict(user_attrs)},
        },
    )

    with store.lock:
        store.users[pool_id][username] = user

    return {
        "UserConfirmed": False,
        "UserSub": user_sub,
    }


def _admin_confirm_sign_up(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    pool_id = params.get("UserPoolId", "")
    username = params.get("Username", "")
    _require_pool(store, pool_id)

    with store.lock:
        user = store.users.get(pool_id, {}).get(username)
        if not user:
            raise CognitoError("UserNotFoundException", f"User {username} does not exist.", 404)
        user["UserStatus"] = "CONFIRMED"
        user["LastModifiedDate"] = time.time()

    # Lambda trigger: PostConfirmation
    pool = store.pools.get(pool_id, {})
    _invoke_trigger(
        pool,
        "PostConfirmation",
        {
            "userPoolId": pool_id,
            "userName": username,
        },
    )

    return {}


def _initiate_auth(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    auth_flow = params.get("AuthFlow", "")
    client_id = params.get("ClientId", "")
    auth_params = params.get("AuthParameters", {})

    pool_id = _find_pool_by_client(store, client_id)

    if auth_flow == "USER_PASSWORD_AUTH":
        username = auth_params.get("USERNAME", "")
        password = auth_params.get("PASSWORD", "")
        return _authenticate_user(store, pool_id, client_id, username, password)
    else:
        raise CognitoError(
            "InvalidParameterException",
            f"Unsupported auth flow: {auth_flow}",
        )


def _admin_initiate_auth(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    pool_id = params.get("UserPoolId", "")
    client_id = params.get("ClientId", "")
    auth_flow = params.get("AuthFlow", "")
    auth_params = params.get("AuthParameters", {})

    _require_pool(store, pool_id)

    if auth_flow in ("ADMIN_USER_PASSWORD_AUTH", "USER_PASSWORD_AUTH"):
        username = auth_params.get("USERNAME", "")
        password = auth_params.get("PASSWORD", "")
        return _authenticate_user(store, pool_id, client_id, username, password)
    else:
        raise CognitoError(
            "InvalidParameterException",
            f"Unsupported auth flow: {auth_flow}",
        )


def _respond_to_auth_challenge(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    challenge_name = params.get("ChallengeName", "")
    client_id = params.get("ClientId", "")
    responses = params.get("ChallengeResponses", {})

    pool_id = _find_pool_by_client(store, client_id)

    if challenge_name == "NEW_PASSWORD_REQUIRED":
        username = responses.get("USERNAME", "")
        new_password = responses.get("NEW_PASSWORD", "")

        with store.lock:
            user = store.users.get(pool_id, {}).get(username)
            if not user:
                raise CognitoError(
                    "UserNotFoundException",
                    f"User {username} does not exist.",
                    404,
                )
            user["Password"] = new_password
            user["UserStatus"] = "CONFIRMED"
            user["LastModifiedDate"] = time.time()

        issuer = f"https://cognito-idp.{region}.amazonaws.com/{pool_id}"
        return {
            "AuthenticationResult": {
                "AccessToken": _generate_jwt(user["UserSub"], issuer, client_id, "access"),
                "IdToken": _generate_jwt(
                    user["UserSub"],
                    issuer,
                    client_id,
                    "id",
                    {"cognito:username": username},
                ),
                "RefreshToken": _new_id(),
                "TokenType": "Bearer",
                "ExpiresIn": 3600,
            },
            "ChallengeParameters": {},
        }
    else:
        raise CognitoError(
            "InvalidParameterException",
            f"Unsupported challenge: {challenge_name}",
        )


def _get_user(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    access_token = params.get("AccessToken", "")
    if not access_token:
        raise CognitoError("NotAuthorizedException", "Missing access token.")

    # Decode the JWT to find the user
    try:
        payload_part = access_token.split(".")[1]
        # Add padding
        padding = 4 - len(payload_part) % 4
        if padding != 4:
            payload_part += "=" * padding
        payload = json.loads(base64.urlsafe_b64decode(payload_part))
    except Exception:
        raise CognitoError("NotAuthorizedException", "Invalid access token.")

    sub = payload.get("sub", "")

    # Find user by sub across all pools
    with store.lock:
        for pool_id, users in store.users.items():
            for username, user in users.items():
                if user["UserSub"] == sub:
                    return {
                        "Username": username,
                        "UserAttributes": user.get("Attributes", []),
                        "UserCreateDate": user["CreationDate"],
                        "UserLastModifiedDate": user["LastModifiedDate"],
                        "Enabled": user["Enabled"],
                        "UserStatus": user["UserStatus"],
                    }

    raise CognitoError("UserNotFoundException", "User not found.", 404)


def _admin_get_user(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    pool_id = params.get("UserPoolId", "")
    username = params.get("Username", "")
    _require_pool(store, pool_id)

    with store.lock:
        user = store.users.get(pool_id, {}).get(username)
    if not user:
        raise CognitoError("UserNotFoundException", f"User {username} does not exist.", 404)
    return {
        "Username": user["Username"],
        "UserAttributes": user.get("Attributes", []),
        "UserCreateDate": user["CreationDate"],
        "UserLastModifiedDate": user["LastModifiedDate"],
        "Enabled": user["Enabled"],
        "UserStatus": user["UserStatus"],
    }


def _admin_create_user(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    pool_id = params.get("UserPoolId", "")
    username = params.get("Username", "")
    _require_pool(store, pool_id)

    temp_password = params.get("TemporaryPassword", "TempPass1!")
    user_attrs = params.get("UserAttributes", [])

    with store.lock:
        users = store.users.get(pool_id, {})
        if username in users:
            raise CognitoError("UsernameExistsException", f"User {username} already exists.")

    user_sub = _new_id()
    user = {
        "Username": username,
        "UserSub": user_sub,
        "Password": temp_password,
        "Enabled": True,
        "UserStatus": "FORCE_CHANGE_PASSWORD",
        "Attributes": user_attrs,
        "CreationDate": time.time(),
        "LastModifiedDate": time.time(),
        "MFAOptions": [],
    }

    with store.lock:
        store.users[pool_id][username] = user

    _sync_user_to_moto(pool_id, username, temp_password, region, account_id)

    return {
        "User": {
            "Username": username,
            "Attributes": user_attrs,
            "UserCreateDate": user["CreationDate"],
            "UserLastModifiedDate": user["LastModifiedDate"],
            "Enabled": True,
            "UserStatus": "FORCE_CHANGE_PASSWORD",
        }
    }


def _admin_disable_user(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    pool_id = params.get("UserPoolId", "")
    username = params.get("Username", "")
    _require_pool(store, pool_id)

    with store.lock:
        user = store.users.get(pool_id, {}).get(username)
        if not user:
            raise CognitoError("UserNotFoundException", f"User {username} does not exist.", 404)
        user["Enabled"] = False
        user["LastModifiedDate"] = time.time()
    return {}


def _admin_enable_user(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    pool_id = params.get("UserPoolId", "")
    username = params.get("Username", "")
    _require_pool(store, pool_id)

    with store.lock:
        user = store.users.get(pool_id, {}).get(username)
        if not user:
            raise CognitoError("UserNotFoundException", f"User {username} does not exist.", 404)
        user["Enabled"] = True
        user["LastModifiedDate"] = time.time()
    return {}


def _admin_delete_user(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    pool_id = params.get("UserPoolId", "")
    username = params.get("Username", "")
    _require_pool(store, pool_id)

    with store.lock:
        users = store.users.get(pool_id, {})
        if username not in users:
            raise CognitoError("UserNotFoundException", f"User {username} does not exist.", 404)
        del users[username]
        # Remove from groups
        store.user_groups.get(pool_id, {}).pop(username, None)
    return {}


def _forgot_password(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    client_id = params.get("ClientId", "")
    username = params.get("Username", "")
    pool_id = _find_pool_by_client(store, client_id)

    with store.lock:
        user = store.users.get(pool_id, {}).get(username)
    if not user:
        raise CognitoError("UserNotFoundException", f"User {username} does not exist.", 404)

    return {
        "CodeDeliveryDetails": {
            "Destination": "***",
            "DeliveryMedium": "EMAIL",
            "AttributeName": "email",
        }
    }


def _confirm_forgot_password(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    client_id = params.get("ClientId", "")
    username = params.get("Username", "")
    password = params.get("Password", "")
    pool_id = _find_pool_by_client(store, client_id)

    with store.lock:
        user = store.users.get(pool_id, {}).get(username)
        if not user:
            raise CognitoError("UserNotFoundException", f"User {username} does not exist.", 404)
        user["Password"] = password
        user["LastModifiedDate"] = time.time()
    return {}


def _change_password(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    access_token = params.get("AccessToken", "")
    previous_password = params.get("PreviousPassword", "")
    proposed_password = params.get("ProposedPassword", "")

    if not access_token:
        raise CognitoError("NotAuthorizedException", "Missing access token.")

    # Decode token to find user
    try:
        payload_part = access_token.split(".")[1]
        padding = 4 - len(payload_part) % 4
        if padding != 4:
            payload_part += "=" * padding
        payload = json.loads(base64.urlsafe_b64decode(payload_part))
    except Exception:
        raise CognitoError("NotAuthorizedException", "Invalid access token.")

    sub = payload.get("sub", "")

    with store.lock:
        for pool_id, users in store.users.items():
            for username, user in users.items():
                if user["UserSub"] == sub:
                    if user["Password"] != previous_password:
                        raise CognitoError("NotAuthorizedException", "Incorrect password.")
                    user["Password"] = proposed_password
                    user["LastModifiedDate"] = time.time()
                    return {}

    raise CognitoError("UserNotFoundException", "User not found.", 404)


def _admin_set_user_password(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    pool_id = params.get("UserPoolId", "")
    username = params.get("Username", "")
    password = params.get("Password", "")
    permanent = params.get("Permanent", False)
    _require_pool(store, pool_id)

    with store.lock:
        user = store.users.get(pool_id, {}).get(username)
        if not user:
            raise CognitoError("UserNotFoundException", f"User {username} does not exist.", 404)
        user["Password"] = password
        if permanent:
            user["UserStatus"] = "CONFIRMED"
        user["LastModifiedDate"] = time.time()
    return {}


def _list_users(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    pool_id = params.get("UserPoolId", "")
    _require_pool(store, pool_id)
    filter_str = params.get("Filter", "")

    with store.lock:
        users = list(store.users.get(pool_id, {}).values())

    if filter_str:
        users = _apply_filter(users, filter_str)

    return {
        "Users": [
            {
                "Username": u["Username"],
                "Attributes": u.get("Attributes", []),
                "UserCreateDate": u["CreationDate"],
                "UserLastModifiedDate": u["LastModifiedDate"],
                "Enabled": u["Enabled"],
                "UserStatus": u["UserStatus"],
            }
            for u in users
        ]
    }


# ---------------------------------------------------------------------------
# Group operations
# ---------------------------------------------------------------------------


def _create_group(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    pool_id = params.get("UserPoolId", "")
    group_name = params.get("GroupName", "")
    _require_pool(store, pool_id)

    with store.lock:
        groups = store.groups.get(pool_id, {})
        if group_name in groups:
            raise CognitoError("GroupExistsException", f"Group {group_name} already exists.")
        group = {
            "GroupName": group_name,
            "UserPoolId": pool_id,
            "Description": params.get("Description", ""),
            "RoleArn": params.get("RoleArn", ""),
            "Precedence": params.get("Precedence", 0),
            "CreationDate": time.time(),
            "LastModifiedDate": time.time(),
        }
        groups[group_name] = group
    return {"Group": group}


def _get_group(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    pool_id = params.get("UserPoolId", "")
    group_name = params.get("GroupName", "")
    _require_pool(store, pool_id)

    with store.lock:
        group = store.groups.get(pool_id, {}).get(group_name)
    if not group:
        raise CognitoError("ResourceNotFoundException", f"Group {group_name} does not exist.", 404)
    return {"Group": group}


def _delete_group(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    pool_id = params.get("UserPoolId", "")
    group_name = params.get("GroupName", "")
    _require_pool(store, pool_id)

    with store.lock:
        groups = store.groups.get(pool_id, {})
        if group_name not in groups:
            raise CognitoError(
                "ResourceNotFoundException",
                f"Group {group_name} does not exist.",
                404,
            )
        del groups[group_name]
        # Cascade: remove group from all user memberships
        for username, user_group_list in store.user_groups.get(pool_id, {}).items():
            if group_name in user_group_list:
                user_group_list.remove(group_name)
    return {}


def _list_groups(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    pool_id = params.get("UserPoolId", "")
    _require_pool(store, pool_id)

    with store.lock:
        groups = list(store.groups.get(pool_id, {}).values())
    return {"Groups": groups}


def _admin_add_user_to_group(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    pool_id = params.get("UserPoolId", "")
    username = params.get("Username", "")
    group_name = params.get("GroupName", "")
    _require_pool(store, pool_id)
    _require_user(store, pool_id, username)
    _require_group(store, pool_id, group_name)

    with store.lock:
        user_groups = store.user_groups.setdefault(pool_id, {})
        groups_list = user_groups.setdefault(username, [])
        if group_name not in groups_list:
            groups_list.append(group_name)
    return {}


def _admin_remove_user_from_group(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    pool_id = params.get("UserPoolId", "")
    username = params.get("Username", "")
    group_name = params.get("GroupName", "")
    _require_pool(store, pool_id)
    _require_user(store, pool_id, username)

    with store.lock:
        user_groups = store.user_groups.get(pool_id, {})
        groups_list = user_groups.get(username, [])
        if group_name in groups_list:
            groups_list.remove(group_name)
    return {}


def _admin_list_groups_for_user(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    pool_id = params.get("UserPoolId", "")
    username = params.get("Username", "")
    _require_pool(store, pool_id)
    _require_user(store, pool_id, username)

    with store.lock:
        user_groups = store.user_groups.get(pool_id, {})
        group_names = user_groups.get(username, [])
        all_groups = store.groups.get(pool_id, {})
        result = [all_groups[g] for g in group_names if g in all_groups]
    return {"Groups": result}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _require_pool(store: CognitoStore, pool_id: str) -> None:
    with store.lock:
        if pool_id not in store.pools:
            raise CognitoError(
                "ResourceNotFoundException",
                f"User pool {pool_id} does not exist.",
                404,
            )


def _require_user(store: CognitoStore, pool_id: str, username: str) -> None:
    with store.lock:
        if username not in store.users.get(pool_id, {}):
            raise CognitoError("UserNotFoundException", f"User {username} does not exist.", 404)


def _require_group(store: CognitoStore, pool_id: str, group_name: str) -> None:
    with store.lock:
        if group_name not in store.groups.get(pool_id, {}):
            raise CognitoError(
                "ResourceNotFoundException",
                f"Group {group_name} does not exist.",
                404,
            )


def _find_pool_by_client(store: CognitoStore, client_id: str) -> str:
    with store.lock:
        for pool_id, clients in store.clients.items():
            if client_id in clients:
                return pool_id
    raise CognitoError("ResourceNotFoundException", f"Client {client_id} does not exist.", 404)


def _authenticate_user(
    store: CognitoStore,
    pool_id: str,
    client_id: str,
    username: str,
    password: str,
) -> dict:
    pool = store.pools.get(pool_id, {})

    # PreAuthentication trigger
    _invoke_trigger(
        pool,
        "PreAuthentication",
        {
            "userPoolId": pool_id,
            "userName": username,
        },
    )

    with store.lock:
        user = store.users.get(pool_id, {}).get(username)
    if not user:
        raise CognitoError("NotAuthorizedException", "Incorrect username or password.")
    if user["Password"] != password:
        raise CognitoError("NotAuthorizedException", "Incorrect username or password.")
    if user["UserStatus"] == "UNCONFIRMED":
        raise CognitoError("UserNotConfirmedException", "User is not confirmed.")
    if user["UserStatus"] == "FORCE_CHANGE_PASSWORD":
        return {
            "ChallengeName": "NEW_PASSWORD_REQUIRED",
            "ChallengeParameters": {
                "USER_ID_FOR_SRP": username,
                "requiredAttributes": "[]",
            },
            "Session": _new_id(),
        }

    region = pool_id.split("_")[0] if "_" in pool_id else "us-east-1"
    issuer = f"https://cognito-idp.{region}.amazonaws.com/{pool_id}"

    # PostAuthentication trigger
    _invoke_trigger(
        pool,
        "PostAuthentication",
        {
            "userPoolId": pool_id,
            "userName": username,
        },
    )

    return {
        "AuthenticationResult": {
            "AccessToken": _generate_jwt(user["UserSub"], issuer, client_id, "access"),
            "IdToken": _generate_jwt(
                user["UserSub"],
                issuer,
                client_id,
                "id",
                {"cognito:username": username},
            ),
            "RefreshToken": _new_id(),
            "TokenType": "Bearer",
            "ExpiresIn": 3600,
        },
        "ChallengeParameters": {},
    }


def _invoke_trigger(pool: dict, trigger_name: str, event: dict) -> None:
    """Invoke a Lambda trigger if configured. Currently a no-op hook."""
    lambda_config = pool.get("LambdaConfig", {})
    if trigger_name in lambda_config:
        # Hook point for future Lambda trigger invocation
        pass


def _attrs_to_dict(attrs: list[dict]) -> dict:
    return {a["Name"]: a["Value"] for a in attrs if "Name" in a and "Value" in a}


def _apply_filter(users: list[dict], filter_str: str) -> list[dict]:
    """Apply a simple filter like 'username = "john"' or 'email ^= "test"'."""
    parts = filter_str.split()
    if len(parts) < 3:
        return users

    attr_name = parts[0]
    operator = parts[1]
    value = " ".join(parts[2:]).strip('"').strip("'")

    result = []
    for user in users:
        if attr_name == "username":
            user_val = user["Username"]
        else:
            user_val = ""
            for attr in user.get("Attributes", []):
                if attr.get("Name") == attr_name:
                    user_val = attr.get("Value", "")
                    break

        if operator == "=" and user_val == value:
            result.append(user)
        elif operator == "^=" and user_val.startswith(value):
            result.append(user)

    return result


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------


def _json_response(data: dict) -> Response:
    return Response(
        content=json.dumps(data, default=str),
        status_code=200,
        media_type="application/x-amz-json-1.1",
    )


def _update_user_pool(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    pool_id = params.get("UserPoolId", "")
    with store.lock:
        pool = store.pools.get(pool_id)
        if not pool:
            raise CognitoError("ResourceNotFoundException", f"User pool {pool_id} not found")
        # Update mutable fields
        for key in (
            "Policies",
            "LambdaConfig",
            "AutoVerifiedAttributes",
            "MfaConfiguration",
            "EmailConfiguration",
            "SmsConfiguration",
            "AdminCreateUserConfig",
        ):
            if key in params:
                pool[key] = params[key]
        pool["LastModifiedDate"] = time.time()
    return {}


def _admin_update_user_attributes(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    pool_id = params.get("UserPoolId", "")
    username = params.get("Username", "")
    user_attributes = params.get("UserAttributes", [])
    _require_pool(store, pool_id)
    with store.lock:
        users = store.users.get(pool_id, {})
        user = users.get(username)
        if not user:
            raise CognitoError("UserNotFoundException", f"User {username} not found")
        # Merge attributes
        existing = {a["Name"]: a["Value"] for a in user.get("Attributes", [])}
        for attr in user_attributes:
            existing[attr["Name"]] = attr["Value"]
        user["Attributes"] = [{"Name": k, "Value": v} for k, v in existing.items()]
        user["LastModifiedDate"] = time.time()
    return {}


def _update_group(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    pool_id = params.get("UserPoolId", "")
    group_name = params.get("GroupName", "")
    with store.lock:
        groups = store.groups.get(pool_id, {})
        group = groups.get(group_name)
        if not group:
            raise CognitoError("ResourceNotFoundException", f"Group {group_name} not found")
        if "Description" in params:
            group["Description"] = params["Description"]
        if "RoleArn" in params:
            group["RoleArn"] = params["RoleArn"]
        if "Precedence" in params:
            group["Precedence"] = params["Precedence"]
        group["LastModifiedDate"] = time.time()
    return {"Group": group}


def _add_custom_attributes(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    pool_id = params.get("UserPoolId", "")
    custom_attrs = params.get("CustomAttributes", [])
    with store.lock:
        pool = store.pools.get(pool_id)
        if not pool:
            raise CognitoError("ResourceNotFoundException", f"User pool {pool_id} not found")
        schema = pool.get("Schema", [])
        for attr in custom_attrs:
            name = attr.get("Name", "")
            if not name.startswith("custom:"):
                name = f"custom:{name}"
            schema.append(
                {
                    "Name": name,
                    "AttributeDataType": attr.get("AttributeDataType", "String"),
                    "Mutable": attr.get("Mutable", True),
                    "Required": False,
                }
            )
        pool["Schema"] = schema
    return {}


def _get_user_pool_mfa_config(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    pool_id = params.get("UserPoolId", "")
    with store.lock:
        pool = store.pools.get(pool_id)
        if not pool:
            raise CognitoError("ResourceNotFoundException", f"User pool {pool_id} not found")
        result: dict = {"MfaConfiguration": pool.get("MfaConfiguration", "OFF")}
        if "SmsMfaConfiguration" in pool:
            result["SmsMfaConfiguration"] = pool["SmsMfaConfiguration"]
        if "SoftwareTokenMfaConfiguration" in pool:
            result["SoftwareTokenMfaConfiguration"] = pool["SoftwareTokenMfaConfiguration"]
    return result


def _set_user_pool_mfa_config(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    pool_id = params.get("UserPoolId", "")
    with store.lock:
        pool = store.pools.get(pool_id)
        if not pool:
            raise CognitoError("ResourceNotFoundException", f"User pool {pool_id} not found")
        mfa_config = params.get("MfaConfiguration", "OFF")
        pool["MfaConfiguration"] = mfa_config
        result: dict = {"MfaConfiguration": mfa_config}
        if "SmsMfaConfiguration" in params:
            pool["SmsMfaConfiguration"] = params["SmsMfaConfiguration"]
            result["SmsMfaConfiguration"] = params["SmsMfaConfiguration"]
        if "SoftwareTokenMfaConfiguration" in params:
            pool["SoftwareTokenMfaConfiguration"] = params["SoftwareTokenMfaConfiguration"]
            result["SoftwareTokenMfaConfiguration"] = params["SoftwareTokenMfaConfiguration"]
    return result


def _list_users_in_group(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    pool_id = params.get("UserPoolId", "")
    group_name = params.get("GroupName", "")
    with store.lock:
        groups = store.groups.get(pool_id, {})
        if group_name not in groups:
            raise CognitoError("ResourceNotFoundException", f"Group {group_name} not found")
        user_groups = store.user_groups.get(pool_id, {})
        users_store = store.users.get(pool_id, {})
        result = []
        for username, user_group_set in user_groups.items():
            if group_name in user_group_set:
                user = users_store.get(username)
                if user:
                    result.append(user)
    return {"Users": result}


# ---------------------------------------------------------------------------
# Tag operations
# ---------------------------------------------------------------------------


def _tag_resource(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    resource_arn = params.get("ResourceArn", "")
    tags = params.get("Tags", {})
    pool_id = _pool_id_from_arn(resource_arn)
    _require_pool(store, pool_id)

    with store.lock:
        pool = store.pools[pool_id]
        existing_tags = pool.setdefault("Tags", {})
        existing_tags.update(tags)
    return {}


def _untag_resource(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    resource_arn = params.get("ResourceArn", "")
    tag_keys = params.get("TagKeys", [])
    pool_id = _pool_id_from_arn(resource_arn)
    _require_pool(store, pool_id)

    with store.lock:
        pool = store.pools[pool_id]
        existing_tags = pool.get("Tags", {})
        for key in tag_keys:
            existing_tags.pop(key, None)
    return {}


def _list_tags_for_resource(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    resource_arn = params.get("ResourceArn", "")
    pool_id = _pool_id_from_arn(resource_arn)
    _require_pool(store, pool_id)

    with store.lock:
        pool = store.pools[pool_id]
        tags = pool.get("Tags", {})
    return {"Tags": dict(tags)}


# ---------------------------------------------------------------------------
# Additional admin/auth operations (avoid Moto fallthrough sync issues)
# ---------------------------------------------------------------------------


def _admin_delete_user_attributes(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    pool_id = params.get("UserPoolId", "")
    username = params.get("Username", "")
    attr_names = params.get("UserAttributeNames", [])
    _require_pool(store, pool_id)

    with store.lock:
        user = store.users.get(pool_id, {}).get(username)
        if not user:
            raise CognitoError("UserNotFoundException", f"User {username} does not exist.", 404)
        user["Attributes"] = [a for a in user.get("Attributes", []) if a["Name"] not in attr_names]
        user["LastModifiedDate"] = time.time()
    return {}


def _admin_reset_user_password(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    pool_id = params.get("UserPoolId", "")
    username = params.get("Username", "")
    _require_pool(store, pool_id)

    with store.lock:
        user = store.users.get(pool_id, {}).get(username)
        if not user:
            raise CognitoError("UserNotFoundException", f"User {username} does not exist.", 404)
        user["UserStatus"] = "RESET_REQUIRED"
        user["LastModifiedDate"] = time.time()
    return {}


def _admin_set_user_mfa_preference(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    pool_id = params.get("UserPoolId", "")
    username = params.get("Username", "")
    _require_pool(store, pool_id)

    with store.lock:
        user = store.users.get(pool_id, {}).get(username)
        if not user:
            raise CognitoError("UserNotFoundException", f"User {username} does not exist.", 404)
        if "SMSMfaSettings" in params:
            user["SMSMfaSettings"] = params["SMSMfaSettings"]
        if "SoftwareTokenMfaSettings" in params:
            user["SoftwareTokenMfaSettings"] = params["SoftwareTokenMfaSettings"]
        user["LastModifiedDate"] = time.time()
    return {}


def _admin_user_global_sign_out(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    pool_id = params.get("UserPoolId", "")
    username = params.get("Username", "")
    _require_pool(store, pool_id)

    with store.lock:
        user = store.users.get(pool_id, {}).get(username)
        if not user:
            raise CognitoError("UserNotFoundException", f"User {username} does not exist.", 404)
    return {}


def _confirm_sign_up(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    client_id = params.get("ClientId", "")
    username = params.get("Username", "")

    pool_id = _find_pool_by_client(store, client_id)

    with store.lock:
        user = store.users.get(pool_id, {}).get(username)
        if not user:
            raise CognitoError("UserNotFoundException", f"User {username} does not exist.", 404)
        user["UserStatus"] = "CONFIRMED"
        user["LastModifiedDate"] = time.time()
    return {}


def _global_sign_out(store: CognitoStore, params: dict, region: str, account_id: str) -> dict:
    access_token = params.get("AccessToken", "")
    if not access_token:
        raise CognitoError("NotAuthorizedException", "Missing access token.")
    # Validate token belongs to a known user
    _user_from_token(store, access_token)
    return {}


def _update_user_attributes(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    access_token = params.get("AccessToken", "")
    user_attributes = params.get("UserAttributes", [])
    if not access_token:
        raise CognitoError("NotAuthorizedException", "Missing access token.")

    user, pool_id = _user_from_token(store, access_token)
    attr_names_to_update = {a["Name"] for a in user_attributes}

    with store.lock:
        existing = [a for a in user.get("Attributes", []) if a["Name"] not in attr_names_to_update]
        existing.extend(user_attributes)
        user["Attributes"] = existing
        user["LastModifiedDate"] = time.time()
    return {}


def _user_from_token(store: CognitoStore, access_token: str) -> tuple:
    """Find a user by their access token (JWT). Returns (user_dict, pool_id)."""
    try:
        payload_part = access_token.split(".")[1]
        padding = 4 - len(payload_part) % 4
        if padding != 4:
            payload_part += "=" * padding
        payload = json.loads(base64.urlsafe_b64decode(payload_part))
    except Exception:
        raise CognitoError("NotAuthorizedException", "Invalid access token.")

    sub = payload.get("sub", "")
    with store.lock:
        for pool_id, users in store.users.items():
            for _uname, user in users.items():
                if user["UserSub"] == sub:
                    return user, pool_id
    raise CognitoError("NotAuthorizedException", "Invalid access token.")


def _create_user_pool_domain(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    pool_id = params.get("UserPoolId", "")
    domain = params.get("Domain", "")
    _require_pool(store, pool_id)

    if not domain:
        raise CognitoError("InvalidParameterException", "Domain is required.")

    with store.lock:
        if domain in store.domains:
            raise CognitoError(
                "InvalidParameterException",
                f"Domain {domain} is already associated with a user pool.",
            )
        store.domains[domain] = pool_id
        pool = store.pools[pool_id]
        pool["Domain"] = domain

    return {}


def _describe_user_pool_domain(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    domain = params.get("Domain", "")

    with store.lock:
        pool_id = store.domains.get(domain)
        if not pool_id:
            return {"DomainDescription": {}}

    return {
        "DomainDescription": {
            "UserPoolId": pool_id,
            "AWSAccountId": account_id,
            "Domain": domain,
            "Status": "ACTIVE",
            "CloudFrontDistribution": f"d{_new_id()[:13]}.cloudfront.net",
            "S3Bucket": f"aws-cognito-idp-{region}",
            "Version": "20241001",
        }
    }


def _delete_user_pool_domain(
    store: CognitoStore, params: dict, region: str, account_id: str
) -> dict:
    pool_id = params.get("UserPoolId", "")
    domain = params.get("Domain", "")
    _require_pool(store, pool_id)

    with store.lock:
        if domain in store.domains:
            del store.domains[domain]
            pool = store.pools.get(pool_id, {})
            pool.pop("Domain", None)
    return {}


def _pool_id_from_arn(arn: str) -> str:
    """Extract pool_id from an ARN like arn:aws:cognito-idp:REGION:ACCT:userpool/POOL_ID."""
    if "/userpool/" in arn:
        return arn.split("/userpool/")[-1]
    if "/" in arn:
        return arn.rsplit("/", 1)[-1]
    return arn


def _error(code: str, message: str, status: int) -> Response:
    body = json.dumps({"__type": code, "message": message})
    return Response(content=body, status_code=status, media_type="application/x-amz-json-1.1")


# ---------------------------------------------------------------------------
# Action map
# ---------------------------------------------------------------------------

_ACTION_MAP: dict[str, Callable] = {
    "CreateUserPool": _create_user_pool,
    "DescribeUserPool": _describe_user_pool,
    "DeleteUserPool": _delete_user_pool,
    "ListUserPools": _list_user_pools,
    "CreateUserPoolClient": _create_user_pool_client,
    "DescribeUserPoolClient": _describe_user_pool_client,
    "UpdateUserPoolClient": _update_user_pool_client,
    "DeleteUserPoolClient": _delete_user_pool_client,
    "ListUserPoolClients": _list_user_pool_clients,
    "SignUp": _sign_up,
    "AdminConfirmSignUp": _admin_confirm_sign_up,
    "InitiateAuth": _initiate_auth,
    "AdminInitiateAuth": _admin_initiate_auth,
    "RespondToAuthChallenge": _respond_to_auth_challenge,
    "GetUser": _get_user,
    "AdminGetUser": _admin_get_user,
    "AdminCreateUser": _admin_create_user,
    "AdminDisableUser": _admin_disable_user,
    "AdminEnableUser": _admin_enable_user,
    "AdminDeleteUser": _admin_delete_user,
    "ForgotPassword": _forgot_password,
    "ConfirmForgotPassword": _confirm_forgot_password,
    "ChangePassword": _change_password,
    "AdminSetUserPassword": _admin_set_user_password,
    "ListUsers": _list_users,
    "CreateGroup": _create_group,
    "GetGroup": _get_group,
    "DeleteGroup": _delete_group,
    "ListGroups": _list_groups,
    "AdminAddUserToGroup": _admin_add_user_to_group,
    "AdminRemoveUserFromGroup": _admin_remove_user_from_group,
    "AdminListGroupsForUser": _admin_list_groups_for_user,
    "UpdateUserPool": _update_user_pool,
    "AdminUpdateUserAttributes": _admin_update_user_attributes,
    "UpdateGroup": _update_group,
    "AddCustomAttributes": _add_custom_attributes,
    "GetUserPoolMfaConfig": _get_user_pool_mfa_config,
    "SetUserPoolMfaConfig": _set_user_pool_mfa_config,
    "ListUsersInGroup": _list_users_in_group,
    "TagResource": _tag_resource,
    "UntagResource": _untag_resource,
    "ListTagsForResource": _list_tags_for_resource,
    "AdminDeleteUserAttributes": _admin_delete_user_attributes,
    "AdminResetUserPassword": _admin_reset_user_password,
    "AdminSetUserMFAPreference": _admin_set_user_mfa_preference,
    "AdminUserGlobalSignOut": _admin_user_global_sign_out,
    "ConfirmSignUp": _confirm_sign_up,
    "GlobalSignOut": _global_sign_out,
    "UpdateUserAttributes": _update_user_attributes,
    "CreateUserPoolDomain": _create_user_pool_domain,
    "DescribeUserPoolDomain": _describe_user_pool_domain,
    "DeleteUserPoolDomain": _delete_user_pool_domain,
}
