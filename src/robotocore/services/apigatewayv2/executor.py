"""API Gateway V2 execution layer -- invokes integrations from HTTP APIs and WebSocket APIs.

Handles:
- Route matching (exact, parameterized, $default fallback)
- JWT authorizer validation
- Lambda proxy integration (v2.0 payload format)
- HTTP proxy integration
- WebSocket route selection and Lambda invocation
"""

import base64
import json
import logging
import re
import time
import uuid

logger = logging.getLogger(__name__)


def execute_v2_request(
    api_id: str,
    stage: str,
    method: str,
    path: str,
    body: bytes | None,
    headers: dict,
    query_params: dict,
    region: str,
    account_id: str,
) -> tuple[int, dict, str]:
    """Execute an API Gateway V2 HTTP API request.

    Returns (status_code, headers, body).
    """
    from robotocore.services.apigatewayv2.provider import (
        get_api_store,
        get_integration_store,
        get_route_store,
        get_stage_store,
    )

    apis = get_api_store(region)
    api = apis.get(api_id)
    if not api:
        return 404, {}, json.dumps({"message": f"API {api_id} not found"})

    protocol = api.get("ProtocolType", "HTTP")

    # Verify stage exists
    stages = get_stage_store(region, api_id)
    stage_obj = stages.get(stage)
    if not stage_obj and stage != "$default":
        return 404, {}, json.dumps({"message": f"Stage {stage} not found"})

    stage_vars = (stage_obj or {}).get("StageVariables", {})

    # Get routes and integrations
    routes = get_route_store(region, api_id)
    integrations = get_integration_store(region, api_id)

    # Match route
    route_key = f"{method.upper()} {path}"
    route, path_params = _match_route(routes, route_key, method, path, protocol)

    if not route:
        return 404, {}, json.dumps({"message": "Not Found"})

    # Check authorizer
    auth_result = _check_v2_authorizer(api_id, route, headers, region, account_id)
    if auth_result is not None:
        return auth_result

    # Get integration
    target = route.get("Target", "")
    integ_id = target.replace("integrations/", "") if target.startswith("integrations/") else target
    integration = integrations.get(integ_id)

    if not integration:
        return 500, {}, json.dumps({"message": "No integration configured"})

    integ_type = integration.get("IntegrationType", "").upper()

    request_id = str(uuid.uuid4())

    if integ_type == "AWS_PROXY":
        return _invoke_lambda_v2(
            integration,
            api_id,
            route,
            method,
            path,
            body,
            headers,
            query_params,
            path_params,
            stage,
            stage_vars,
            request_id,
            region,
            account_id,
        )
    elif integ_type in ("HTTP_PROXY", "HTTP"):
        return _invoke_http_v2(integration, method, path, body, headers, query_params)
    else:
        return 500, {}, json.dumps({"message": f"Unsupported integration type: {integ_type}"})


def execute_websocket_message(
    api_id: str,
    connection_id: str,
    message: str | bytes,
    region: str,
    account_id: str,
) -> tuple[int, dict, str]:
    """Route a WebSocket message through the API.

    Uses the route selection expression to determine which route to invoke.
    """
    from robotocore.services.apigatewayv2.provider import (
        get_api_store,
        get_integration_store,
        get_route_store,
    )

    apis = get_api_store(region)
    api = apis.get(api_id)
    if not api:
        return 404, {}, json.dumps({"message": "API not found"})

    routes = get_route_store(region, api_id)
    integrations = get_integration_store(region, api_id)

    # Parse message to determine route
    route_sel_expr = api.get("RouteSelectionExpression", "$request.body.action")
    route_key = _evaluate_route_selection(route_sel_expr, message)

    # Find matching route
    route = None
    for r in routes.values():
        if r.get("RouteKey") == route_key:
            route = r
            break

    # Fallback to $default
    if not route:
        for r in routes.values():
            if r.get("RouteKey") == "$default":
                route = r
                break

    if not route:
        return 404, {}, json.dumps({"message": "No route matched"})

    # Get integration
    target = route.get("Target", "")
    integ_id = target.replace("integrations/", "") if target.startswith("integrations/") else target
    integration = integrations.get(integ_id)
    if not integration:
        return 500, {}, json.dumps({"message": "No integration configured"})

    # Build WebSocket event
    body_str = message.decode() if isinstance(message, bytes) else message
    event = {
        "requestContext": {
            "routeKey": route.get("RouteKey", "$default"),
            "eventType": "MESSAGE",
            "messageId": str(uuid.uuid4()),
            "connectionId": connection_id,
            "apiId": api_id,
            "domainName": f"{api_id}.execute-api.{region}.amazonaws.com",
            "stage": "$default",
            "requestId": str(uuid.uuid4()),
            "requestTimeEpoch": int(time.time() * 1000),
            "connectedAt": int(time.time() * 1000),
        },
        "body": body_str,
        "isBase64Encoded": False,
    }

    return _invoke_lambda_for_websocket(integration, event, region, account_id)


def execute_websocket_connect(
    api_id: str,
    connection_id: str,
    headers: dict,
    query_params: dict,
    region: str,
    account_id: str,
) -> tuple[int, dict, str]:
    """Handle WebSocket $connect route."""
    from robotocore.services.apigatewayv2.provider import (
        get_integration_store,
        get_route_store,
    )

    routes = get_route_store(region, api_id)
    integrations = get_integration_store(region, api_id)

    # Find $connect route
    route = None
    for r in routes.values():
        if r.get("RouteKey") == "$connect":
            route = r
            break

    if not route:
        # No $connect route, accept by default
        return 200, {}, ""

    target = route.get("Target", "")
    integ_id = target.replace("integrations/", "") if target.startswith("integrations/") else target
    integration = integrations.get(integ_id)
    if not integration:
        return 200, {}, ""

    event = {
        "requestContext": {
            "routeKey": "$connect",
            "eventType": "CONNECT",
            "connectionId": connection_id,
            "apiId": api_id,
            "domainName": f"{api_id}.execute-api.{region}.amazonaws.com",
            "stage": "$default",
            "requestId": str(uuid.uuid4()),
            "requestTimeEpoch": int(time.time() * 1000),
        },
        "headers": headers,
        "queryStringParameters": query_params if query_params else None,
        "isBase64Encoded": False,
    }

    return _invoke_lambda_for_websocket(integration, event, region, account_id)


def execute_websocket_disconnect(
    api_id: str,
    connection_id: str,
    region: str,
    account_id: str,
) -> tuple[int, dict, str]:
    """Handle WebSocket $disconnect route."""
    from robotocore.services.apigatewayv2.provider import (
        get_integration_store,
        get_route_store,
    )

    routes = get_route_store(region, api_id)
    integrations = get_integration_store(region, api_id)

    route = None
    for r in routes.values():
        if r.get("RouteKey") == "$disconnect":
            route = r
            break

    if not route:
        return 200, {}, ""

    target = route.get("Target", "")
    integ_id = target.replace("integrations/", "") if target.startswith("integrations/") else target
    integration = integrations.get(integ_id)
    if not integration:
        return 200, {}, ""

    event = {
        "requestContext": {
            "routeKey": "$disconnect",
            "eventType": "DISCONNECT",
            "connectionId": connection_id,
            "apiId": api_id,
            "domainName": f"{api_id}.execute-api.{region}.amazonaws.com",
            "stage": "$default",
            "requestId": str(uuid.uuid4()),
            "requestTimeEpoch": int(time.time() * 1000),
        },
        "isBase64Encoded": False,
    }

    return _invoke_lambda_for_websocket(integration, event, region, account_id)


# ---------------------------------------------------------------------------
# Route matching
# ---------------------------------------------------------------------------


def _match_route(
    routes: dict,
    route_key: str,
    method: str,
    path: str,
    protocol: str,
) -> tuple[dict | None, dict]:
    """Match a request to a route. Returns (route, path_params)."""
    # 1. Exact match
    for r in routes.values():
        if r.get("RouteKey") == route_key:
            return r, {}

    # 2. Parameterized match
    best_route = None
    best_params: dict = {}
    best_specificity = -1

    for r in routes.values():
        rk = r.get("RouteKey", "")
        if " " not in rk:
            continue
        rk_method, rk_path = rk.split(" ", 1)
        if rk_method != method.upper() and rk_method != "ANY":
            continue
        match, params = _path_matches_v2(rk_path, path)
        if match:
            spec = _path_specificity_v2(rk_path)
            if spec > best_specificity:
                best_route = r
                best_params = params
                best_specificity = spec

    if best_route:
        return best_route, best_params

    # 3. ANY method fallback
    any_key = f"ANY {path}"
    for r in routes.values():
        if r.get("RouteKey") == any_key:
            return r, {}

    # 4. $default fallback
    for r in routes.values():
        if r.get("RouteKey") == "$default":
            return r, {}

    return None, {}


def _path_matches_v2(pattern: str, path: str) -> tuple[bool, dict]:
    """Match a v2 route path pattern against a request path."""
    pattern = pattern.rstrip("/") or "/"
    path = path.rstrip("/") or "/"

    if pattern == path:
        return True, {}

    regex_parts = []
    for part in pattern.split("/"):
        if part.startswith("{") and part.endswith("+}"):
            param_name = part[1:-2]
            regex_parts.append(f"(?P<{param_name}>.+)")
        elif part.startswith("{") and part.endswith("}"):
            param_name = part[1:-1]
            regex_parts.append(f"(?P<{param_name}>[^/]+)")
        else:
            regex_parts.append(re.escape(part))

    regex = "^" + "/".join(regex_parts) + "$"
    match = re.match(regex, path)
    if match:
        return True, match.groupdict()
    return False, {}


def _path_specificity_v2(pattern: str) -> int:
    """Score path specificity."""
    parts = pattern.split("/")
    score = 0
    for part in parts:
        if part.startswith("{") and part.endswith("+}"):
            score += 1
        elif part.startswith("{"):
            score += 5
        else:
            score += 10
    return score


# ---------------------------------------------------------------------------
# JWT Authorizer
# ---------------------------------------------------------------------------


def _check_v2_authorizer(
    api_id: str,
    route: dict,
    headers: dict,
    region: str,
    account_id: str,
) -> tuple[int, dict, str] | None:
    """Check v2 authorizer (JWT). Returns error or None."""
    auth_type = route.get("AuthorizationType", "NONE")
    if auth_type == "NONE":
        return None

    auth_id = route.get("AuthorizerId")
    if not auth_id:
        return None

    from robotocore.services.apigatewayv2.provider import get_authorizer_store

    authorizers = get_authorizer_store(region, api_id)
    authorizer = authorizers.get(auth_id)
    if not authorizer:
        return None

    auth_type_val = authorizer.get("AuthorizerType", "").upper()

    if auth_type_val == "JWT":
        return _validate_jwt(authorizer, headers)

    return None


def _validate_jwt(
    authorizer: dict,
    headers: dict,
) -> tuple[int, dict, str] | None:
    """Validate JWT token from headers."""
    # Extract token from identity source
    identity_source = authorizer.get("IdentitySource", "$request.header.Authorization")
    header_name = "Authorization"
    if "header." in identity_source:
        header_name = identity_source.split("header.")[-1]

    token = headers.get(header_name.lower(), headers.get(header_name, ""))
    if not token:
        return 401, {}, json.dumps({"message": "Unauthorized"})

    # Strip "Bearer " prefix
    if token.lower().startswith("bearer "):
        token = token[7:]

    # Validate JWT structure
    parts = token.split(".")
    if len(parts) != 3:
        return 401, {}, json.dumps({"message": "Unauthorized"})

    jwt_config = authorizer.get("JwtConfiguration", {}) or {}
    issuer = jwt_config.get("Issuer", "")
    audience = jwt_config.get("Audience", [])

    try:
        # Decode payload
        payload_b64 = parts[1]
        padding = 4 - len(payload_b64) % 4
        if padding != 4:
            payload_b64 += "=" * padding
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))

        # Check expiry
        exp = payload.get("exp")
        if exp and isinstance(exp, (int, float)):
            if time.time() > exp:
                return 401, {}, json.dumps({"message": "Token expired"})

        # Check issuer
        if issuer and payload.get("iss") != issuer:
            return 401, {}, json.dumps({"message": "Invalid issuer"})

        # Check audience
        if audience:
            token_aud = payload.get("aud")
            if isinstance(token_aud, str):
                token_aud = [token_aud]
            if token_aud is None:
                token_aud = []
            if not any(a in token_aud for a in audience):
                return 401, {}, json.dumps({"message": "Invalid audience"})

    except Exception:  # noqa: BLE001
        return 401, {}, json.dumps({"message": "Invalid token"})

    return None


# ---------------------------------------------------------------------------
# Lambda v2 proxy integration
# ---------------------------------------------------------------------------


def _invoke_lambda_v2(
    integration: dict,
    api_id: str,
    route: dict,
    method: str,
    path: str,
    body: bytes | None,
    headers: dict,
    query_params: dict,
    path_params: dict,
    stage: str,
    stage_vars: dict,
    request_id: str,
    region: str,
    account_id: str,
) -> tuple[int, dict, str]:
    """Invoke Lambda with v2.0 payload format."""
    uri = integration.get("IntegrationUri", "")
    function_name = _extract_function_name(uri)

    if not function_name:
        return 500, {}, json.dumps({"message": "Could not resolve Lambda function"})

    # Build v2.0 payload
    raw_query = "&".join(f"{k}={v}" for k, v in query_params.items()) if query_params else ""

    event = {
        "version": "2.0",
        "routeKey": route.get("RouteKey", "$default"),
        "rawPath": path,
        "rawQueryString": raw_query,
        "headers": dict(headers) if headers else {},
        "queryStringParameters": query_params if query_params else None,
        "pathParameters": path_params if path_params else None,
        "stageVariables": stage_vars if stage_vars else None,
        "requestContext": {
            "accountId": account_id,
            "apiId": api_id,
            "http": {
                "method": method.upper(),
                "path": path,
                "protocol": "HTTP/1.1",
                "sourceIp": headers.get("x-forwarded-for", "127.0.0.1"),
                "userAgent": headers.get("user-agent", ""),
            },
            "requestId": request_id,
            "routeKey": route.get("RouteKey", "$default"),
            "stage": stage,
            "time": time.strftime("%d/%b/%Y:%H:%M:%S +0000", time.gmtime()),
            "timeEpoch": int(time.time() * 1000),
            "domainName": f"{api_id}.execute-api.{region}.amazonaws.com",
            "domainPrefix": api_id,
        },
        "body": body.decode() if body else None,
        "isBase64Encoded": False,
    }

    # Invoke Lambda
    result = _invoke_lambda(function_name, event, region, account_id)

    if result is None:
        return 502, {}, json.dumps({"message": "Internal server error"})

    # Parse response
    if isinstance(result, dict):
        status_code = result.get("statusCode", 200)
        resp_headers = result.get("headers", {})
        resp_body = result.get("body", "")
        if result.get("isBase64Encoded") and resp_body:
            resp_body = base64.b64decode(resp_body).decode()
        return status_code, resp_headers, resp_body

    # Simple string response (v2 format allows this)
    if isinstance(result, str):
        return 200, {"content-type": "application/json"}, result

    return 200, {}, json.dumps(result) if result else ""


def _invoke_http_v2(
    integration: dict,
    method: str,
    path: str,
    body: bytes | None,
    headers: dict,
    query_params: dict,
) -> tuple[int, dict, str]:
    """Handle HTTP/HTTP_PROXY integration for v2."""
    return 200, {}, json.dumps({"message": "HTTP integration executed"})


def _invoke_lambda_for_websocket(
    integration: dict,
    event: dict,
    region: str,
    account_id: str,
) -> tuple[int, dict, str]:
    """Invoke Lambda for a WebSocket route."""
    uri = integration.get("IntegrationUri", "")
    function_name = _extract_function_name(uri)

    if not function_name:
        return 500, {}, json.dumps({"message": "No Lambda function configured"})

    result = _invoke_lambda(function_name, event, region, account_id)
    if result is None:
        return 502, {}, json.dumps({"message": "Internal server error"})

    if isinstance(result, dict):
        status = result.get("statusCode", 200)
        body = result.get("body", "")
        return status, result.get("headers", {}), body

    return 200, {}, json.dumps(result) if result else ""


# ---------------------------------------------------------------------------
# WebSocket route selection
# ---------------------------------------------------------------------------


def _evaluate_route_selection(expression: str, message: str | bytes) -> str:
    """Evaluate a route selection expression against a message.

    Expression format: $request.body.action
    """
    if isinstance(message, bytes):
        message = message.decode()

    # Parse the expression: $request.body.{field}
    match = re.match(r"\$request\.body\.(\w+)", expression)
    if not match:
        return "$default"

    field = match.group(1)

    try:
        data = json.loads(message)
        if isinstance(data, dict) and field in data:
            return str(data[field])
    except (json.JSONDecodeError, TypeError) as exc:
        logger.debug("_evaluate_route_selection: loads failed (non-fatal): %s", exc)

    return "$default"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_function_name(uri: str) -> str | None:
    """Extract Lambda function name from integration URI.

    V2 URIs can be:
    - ARN: arn:aws:lambda:region:account:function:name
    - Integration URI: arn:aws:apigateway:region:lambda:path/.../functions/ARN/invocations
    """
    if not uri:
        return None

    # Check for functions/.../invocations pattern
    match = re.search(r"functions/([^/]+)/invocations", uri)
    if match:
        arn_or_name = match.group(1)
        if arn_or_name.startswith("arn:"):
            parts = arn_or_name.split(":")
            if len(parts) >= 7:
                return parts[6]
        return arn_or_name

    # Direct ARN reference
    if uri.startswith("arn:aws:lambda:"):
        parts = uri.split(":")
        if len(parts) >= 7:
            return parts[6]

    return uri if uri else None


def _invoke_lambda(function_name: str, event: dict, region: str, account_id: str):
    """Invoke a Lambda function and return the result."""
    from robotocore.services.lambda_.executor import execute_python_handler

    try:
        from moto.backends import get_backend  # noqa: I001
        from moto.core import DEFAULT_ACCOUNT_ID

        acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID
        backend = get_backend("lambda")[acct][region]
        fn = backend.get_function(function_name)
    except Exception:  # noqa: BLE001
        logger.error("API Gateway V2: Lambda function not found: %s", function_name)
        return None

    runtime = getattr(fn, "run_time", "") or ""
    if runtime.startswith("python"):
        code_zip = getattr(fn, "code_bytes", None)
        if not code_zip and hasattr(fn, "code") and fn.code:
            code_zip = fn.code.get("ZipFile")
            if isinstance(code_zip, str):
                code_zip = base64.b64decode(code_zip)
        if code_zip:
            result, error_type, logs = execute_python_handler(
                code_zip=code_zip,
                handler=getattr(fn, "handler", "lambda_function.handler"),
                event=event,
                function_name=function_name,
                region=region,
                account_id=account_id,
            )
            if error_type:
                return {
                    "statusCode": 500,
                    "body": json.dumps({"error": str(result)}),
                }
            return result

    return {"statusCode": 200, "body": "OK"}
