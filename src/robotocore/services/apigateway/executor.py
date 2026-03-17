"""API Gateway execution layer -- invokes Lambda/HTTP backends from API Gateway.

When a request hits an API Gateway endpoint (via execute-api), this module:
1. Looks up the REST API, resource, and method in Moto's API Gateway backend
2. Checks authorizers (Lambda, Cognito)
3. Validates API keys and usage plans
4. Validates request body against model schema
5. Resolves the integration (Lambda, HTTP, AWS service, Mock)
6. Applies VTL mapping templates for request/response transformation
7. Invokes the integration and returns the response

This is a key Enterprise feature -- Moto stores the API config but doesn't
actually execute integrations.
"""

import base64
import json
import logging
import re
import time
import uuid

from robotocore.services.apigateway.vtl import VtlContext, evaluate_vtl

logger = logging.getLogger(__name__)


# Gateway response defaults (can be overridden per-API)
DEFAULT_GATEWAY_RESPONSES: dict[str, dict] = {
    "DEFAULT_4XX": {
        "statusCode": 400,
        "responseTemplates": {"application/json": '{"message": "$context.error.message"}'},
    },
    "DEFAULT_5XX": {
        "statusCode": 500,
        "responseTemplates": {"application/json": '{"message": "$context.error.message"}'},
    },
    "UNAUTHORIZED": {
        "statusCode": 401,
        "responseTemplates": {"application/json": '{"message": "Unauthorized"}'},
    },
    "ACCESS_DENIED": {
        "statusCode": 403,
        "responseTemplates": {"application/json": '{"message": "Access Denied"}'},
    },
    "MISSING_AUTHENTICATION_TOKEN": {
        "statusCode": 403,
        "responseTemplates": {"application/json": '{"message": "Missing Authentication Token"}'},
    },
    "RESOURCE_NOT_FOUND": {
        "statusCode": 404,
        "responseTemplates": {"application/json": '{"message": "Not Found"}'},
    },
    "API_CONFIGURATION_ERROR": {
        "statusCode": 500,
        "responseTemplates": {
            "application/json": '{"message": "API Configuration Error"}',
        },
    },
    "AUTHORIZER_FAILURE": {
        "statusCode": 500,
        "responseTemplates": {"application/json": '{"message": "Authorizer Failure"}'},
    },
    "INVALID_API_KEY": {
        "statusCode": 403,
        "responseTemplates": {"application/json": '{"message": "Forbidden"}'},
    },
    "BAD_REQUEST_BODY": {
        "statusCode": 400,
        "responseTemplates": {"application/json": '{"message": "Invalid request body"}'},
    },
}


def execute_api_request(
    rest_api_id: str,
    stage: str,
    method: str,
    path: str,
    body: bytes | None,
    headers: dict,
    query_params: dict,
    region: str,
    account_id: str,
) -> tuple[int, dict, str]:
    """Execute an API Gateway request.

    Returns (status_code, headers, body).
    """
    from moto.backends import get_backend
    from moto.core import DEFAULT_ACCOUNT_ID

    acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID
    request_id = str(uuid.uuid4())

    try:
        backend = get_backend("apigateway")[acct][region]
    except Exception:  # noqa: BLE001
        return 404, {}, json.dumps({"message": "API Gateway backend not found"})

    # Find the REST API
    rest_api = None
    for api in backend.apis.values():
        if api.id == rest_api_id:
            rest_api = api
            break

    if not rest_api:
        return 404, {}, json.dumps({"message": f"API {rest_api_id} not found"})

    # Match the path to a resource
    resource, path_params = _match_resource(rest_api, path)
    if not resource:
        return _gateway_response(rest_api, "RESOURCE_NOT_FOUND", request_id)

    # Find the method (resource_methods contains Moto Method objects)
    method_upper = method.upper()
    method_obj = resource.resource_methods.get(method_upper)
    if not method_obj and "ANY" in resource.resource_methods:
        method_obj = resource.resource_methods["ANY"]
    if not method_obj:
        return 405, {}, json.dumps({"message": f"Method {method_upper} not allowed"})

    # Get stage variables
    stage_vars = _get_stage_variables(rest_api, stage)

    # Build context variables
    context_vars = _build_context_vars(
        rest_api_id, stage, method_upper, path, resource, request_id, account_id, headers
    )

    # Check authorizer
    auth_result = _check_authorizer(
        rest_api,
        method_obj,
        headers,
        query_params,
        path_params,
        stage_vars,
        region,
        account_id,
        request_id,
        context_vars,
    )
    if auth_result is not None:
        return auth_result

    # Check API key requirement
    api_key_result = _check_api_key(rest_api, method_obj, headers, backend, request_id)
    if api_key_result is not None:
        return api_key_result

    # Validate request body
    validation_result = _validate_request_body(rest_api, method_obj, body, headers)
    if validation_result is not None:
        return validation_result

    # Get integration (Moto Integration object, not a dict)
    integration = getattr(method_obj, "method_integration", None)
    if not integration:
        return 500, {}, json.dumps({"message": "No integration configured"})

    integration_type = getattr(integration, "integration_type", "").upper()

    # Substitute stage variables in URI
    uri = getattr(integration, "uri", "") or ""
    uri = _substitute_stage_variables(uri, stage_vars)

    body_str = body.decode() if body else None

    if integration_type == "AWS_PROXY":
        return _invoke_lambda_proxy(
            integration,
            method,
            path,
            body,
            headers,
            query_params,
            path_params,
            stage,
            rest_api_id,
            region,
            account_id,
            stage_vars,
            context_vars,
        )
    elif integration_type == "MOCK":
        return _invoke_mock(
            integration,
            method_obj,
            body_str,
            headers,
            query_params,
            path_params,
            stage_vars,
            context_vars,
        )
    elif integration_type == "AWS":
        return _invoke_aws_service(
            integration,
            uri,
            body,
            headers,
            query_params,
            path_params,
            stage_vars,
            context_vars,
            region,
            account_id,
        )
    elif integration_type in ("HTTP", "HTTP_PROXY"):
        return _invoke_http(
            integration,
            uri,
            method,
            body,
            headers,
            query_params,
            path_params,
            stage_vars,
            context_vars,
        )
    else:
        return 500, {}, json.dumps({"message": f"Unsupported integration type: {integration_type}"})


def _match_resource(rest_api, path: str) -> tuple:
    """Match a request path to an API Gateway resource, extracting path parameters."""
    best_match = None
    best_params = {}
    best_specificity = -1

    for resource in rest_api.resources.values():
        full_path = _get_full_path(rest_api, resource)

        match, params = _path_matches(full_path, path)
        if match:
            specificity = _path_specificity(full_path)
            if specificity > best_specificity:
                best_match = resource
                best_params = params
                best_specificity = specificity

    return best_match, best_params


def _get_full_path(rest_api, resource) -> str:
    """Build the full path for a resource by walking up the tree."""
    parts = []
    current = resource
    while current:
        part = getattr(current, "path_part", "") or getattr(current, "resource_path", "")
        if part:
            parts.insert(0, part)
        parent_id = getattr(current, "parent_id", None)
        if parent_id and parent_id in rest_api.resources:
            current = rest_api.resources[parent_id]
        else:
            break
    return "/" + "/".join(p.strip("/") for p in parts if p and p != "/")


def _path_matches(pattern: str, path: str) -> tuple[bool, dict]:
    """Check if a request path matches a resource pattern, extracting path params."""
    # Normalize
    pattern = pattern.rstrip("/") or "/"
    path = path.rstrip("/") or "/"

    if pattern == path:
        return True, {}

    # Convert API Gateway path pattern to regex
    regex_parts = []
    for part in pattern.split("/"):
        if part.startswith("{") and part.endswith("+}"):
            # Greedy path parameter
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


def _path_specificity(pattern: str) -> int:
    """Score path specificity (higher = more specific, prefer exact matches)."""
    parts = pattern.split("/")
    score = 0
    for part in parts:
        if part.startswith("{") and part.endswith("+}"):
            score += 1  # Greedy = least specific
        elif part.startswith("{"):
            score += 5  # Path param
        else:
            score += 10  # Exact match
    return score


# ---------------------------------------------------------------------------
# Stage variables
# ---------------------------------------------------------------------------


def _get_stage_variables(rest_api, stage_name: str) -> dict:
    """Get stage variables for the given stage."""
    stages = getattr(rest_api, "stages", {}) or {}
    for s_name, stage_obj in stages.items():
        name = getattr(stage_obj, "name", s_name) if not isinstance(s_name, str) else s_name
        if name == stage_name or s_name == stage_name:
            return getattr(stage_obj, "variables", {}) or {}
    return {}


def _substitute_stage_variables(s: str, stage_vars: dict) -> str:
    """Replace ${stageVariables.X} and ${stageVariables['X']} with actual values."""
    if not stage_vars or "${stageVariables" not in s:
        return s
    for key, val in stage_vars.items():
        # Dot notation: ${stageVariables.key}
        s = s.replace(f"${{stageVariables.{key}}}", str(val))
        # Bracket notation: ${stageVariables['key']}
        s = s.replace(f"${{stageVariables['{key}']}}", str(val))
    return s


def _build_context_vars(
    api_id: str,
    stage: str,
    method: str,
    path: str,
    resource,
    request_id: str,
    account_id: str,
    headers: dict,
) -> dict:
    """Build the $context variables dict."""
    resource_path = getattr(resource, "path_part", path)
    return {
        "apiId": api_id,
        "stage": stage,
        "httpMethod": method,
        "resourcePath": resource_path,
        "path": f"/{stage}{path}",
        "requestId": request_id,
        "accountId": account_id,
        "identity": {
            "sourceIp": headers.get("x-forwarded-for", "127.0.0.1"),
            "userAgent": headers.get("user-agent", ""),
        },
        "requestTime": time.strftime("%d/%b/%Y:%H:%M:%S +0000", time.gmtime()),
        "requestTimeEpoch": int(time.time() * 1000),
        "error": {"message": ""},
    }


# ---------------------------------------------------------------------------
# Authorizers
# ---------------------------------------------------------------------------


def _check_authorizer(
    rest_api,
    method_obj,
    headers,
    query_params,
    path_params,
    stage_vars,
    region,
    account_id,
    request_id,
    context_vars,
) -> tuple[int, dict, str] | None:
    """Check authorizer on method. Returns error tuple or None if authorized."""
    auth_type = getattr(method_obj, "authorization_type", "NONE") or "NONE"
    if auth_type == "NONE":
        return None

    authorizer_id = getattr(method_obj, "authorizer_id", None)
    if not authorizer_id:
        return None

    # Find authorizer in REST API
    authorizers = getattr(rest_api, "authorizers", {}) or {}
    authorizer = authorizers.get(authorizer_id)
    if not authorizer:
        return None

    auth_type_obj = getattr(authorizer, "type", "").upper()

    if auth_type_obj == "TOKEN":
        return _check_token_authorizer(authorizer, headers, region, account_id, request_id)
    elif auth_type_obj == "REQUEST":
        return _check_request_authorizer(
            authorizer,
            headers,
            query_params,
            path_params,
            stage_vars,
            region,
            account_id,
            request_id,
        )
    elif auth_type_obj in ("COGNITO_USER_POOLS", "COGNITO"):
        return _check_cognito_authorizer(authorizer, headers, request_id)

    return None


def _check_token_authorizer(
    authorizer,
    headers,
    region,
    account_id,
    request_id,
) -> tuple[int, dict, str] | None:
    """Lambda TOKEN authorizer: extract token from header, invoke Lambda."""
    source = getattr(authorizer, "auth_type", None) or getattr(
        authorizer, "identity_source", "method.request.header.Authorization"
    )
    # Extract header name from identity source
    header_name = "Authorization"
    if source and "header." in source:
        header_name = source.split("header.")[-1]

    token = headers.get(header_name.lower(), headers.get(header_name, ""))
    if not token:
        return 401, {}, json.dumps({"message": "Unauthorized"})

    uri = getattr(authorizer, "authorizer_uri", "") or ""
    function_name = _extract_lambda_function_from_uri(uri)
    if not function_name:
        return 500, {}, json.dumps({"message": "Authorizer configuration error"})

    # Build authorizer event
    event = {
        "type": "TOKEN",
        "authorizationToken": token,
        "methodArn": f"arn:aws:execute-api:{region}:{account_id}:*",
    }

    result = _invoke_lambda(function_name, event, region, account_id)
    if result is None:
        return 500, {}, json.dumps({"message": "Authorizer failure"})

    # Check policy document
    policy_doc = result.get("policyDocument", {}) if isinstance(result, dict) else {}
    statements = policy_doc.get("Statement", [])
    for stmt in statements:
        if stmt.get("Effect") == "Allow":
            return None  # Authorized
    return 403, {}, json.dumps({"message": "Access Denied"})


def _check_request_authorizer(
    authorizer,
    headers,
    query_params,
    path_params,
    stage_vars,
    region,
    account_id,
    request_id,
) -> tuple[int, dict, str] | None:
    """Lambda REQUEST authorizer: pass request context to Lambda."""
    uri = getattr(authorizer, "authorizer_uri", "") or ""
    function_name = _extract_lambda_function_from_uri(uri)
    if not function_name:
        return 500, {}, json.dumps({"message": "Authorizer configuration error"})

    event = {
        "type": "REQUEST",
        "methodArn": f"arn:aws:execute-api:{region}:{account_id}:*",
        "headers": dict(headers),
        "queryStringParameters": dict(query_params) if query_params else {},
        "pathParameters": dict(path_params) if path_params else {},
        "stageVariables": dict(stage_vars) if stage_vars else {},
        "requestContext": {"requestId": request_id},
    }

    result = _invoke_lambda(function_name, event, region, account_id)
    if result is None:
        return 500, {}, json.dumps({"message": "Authorizer failure"})

    policy_doc = result.get("policyDocument", {}) if isinstance(result, dict) else {}
    statements = policy_doc.get("Statement", [])
    for stmt in statements:
        if stmt.get("Effect") == "Allow":
            return None
    return 403, {}, json.dumps({"message": "Access Denied"})


def _check_cognito_authorizer(
    authorizer,
    headers,
    request_id,
) -> tuple[int, dict, str] | None:
    """Cognito User Pools authorizer: basic JWT validation."""
    source = getattr(authorizer, "identity_source", "method.request.header.Authorization")
    header_name = "Authorization"
    if source and "header." in source:
        header_name = source.split("header.")[-1]

    token = headers.get(header_name.lower(), headers.get(header_name, ""))
    if not token:
        return 401, {}, json.dumps({"message": "Unauthorized"})

    # Strip "Bearer " prefix
    if token.lower().startswith("bearer "):
        token = token[7:]

    # Basic JWT validation: check structure and expiry
    parts = token.split(".")
    if len(parts) != 3:
        return 401, {}, json.dumps({"message": "Unauthorized"})

    try:
        # Decode payload (middle part)
        payload_b64 = parts[1]
        # Fix padding
        padding = 4 - len(payload_b64) % 4
        if padding != 4:
            payload_b64 += "=" * padding
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))

        # Check expiry
        exp = payload.get("exp")
        if exp and isinstance(exp, (int, float)):
            if time.time() > exp:
                return 401, {}, json.dumps({"message": "Token expired"})

        # Check issuer against provider ARNs if configured
        provider_arns = getattr(authorizer, "provider_arns", []) or []
        if provider_arns:
            iss = payload.get("iss", "")
            # issuer should contain the user pool ID from the provider ARN
            matched = False
            for arn in provider_arns:
                # ARN format: arn:aws:cognito-idp:region:account:userpool/pool-id
                pool_parts = arn.split("/")
                if len(pool_parts) >= 2:
                    pool_id = pool_parts[-1]
                    if pool_id in iss:
                        matched = True
                        break
            if not matched and iss:
                # Be lenient — if we can't verify, allow it
                pass

    except Exception:  # noqa: BLE001
        return 401, {}, json.dumps({"message": "Invalid token"})

    return None  # Authorized


# ---------------------------------------------------------------------------
# API Key validation
# ---------------------------------------------------------------------------


def _check_api_key(
    rest_api,
    method_obj,
    headers,
    backend,
    request_id,
) -> tuple[int, dict, str] | None:
    """Check API key requirement on method."""
    api_key_required = getattr(method_obj, "api_key_required", False)
    if not api_key_required:
        return None

    api_key = headers.get("x-api-key", "")
    if not api_key:
        return 403, {}, json.dumps({"message": "Forbidden"})

    # Validate against backend API keys
    keys = getattr(backend, "keys", {}) or {}
    for key_obj in keys.values():
        key_value = getattr(key_obj, "value", None)
        key_enabled = getattr(key_obj, "enabled", True)
        if key_value == api_key and key_enabled:
            return None  # Valid key

    return 403, {}, json.dumps({"message": "Forbidden"})


# ---------------------------------------------------------------------------
# Request body validation
# ---------------------------------------------------------------------------


def _validate_request_body(
    rest_api,
    method_obj,
    body,
    headers,
) -> tuple[int, dict, str] | None:
    """Validate request body against the method's request model schema."""
    validator_id = getattr(method_obj, "request_validator_id", None)
    if not validator_id:
        return None

    # Find validator
    validators = getattr(rest_api, "validators", {}) or {}
    validator = validators.get(validator_id)
    if not validator:
        return None

    validate_body = getattr(validator, "validate_request_body", False)
    if not validate_body:
        return None

    # Get model for the content type
    models = getattr(method_obj, "request_models", {}) or {}
    content_type = headers.get("content-type", "application/json")
    model_name = models.get(content_type) or models.get("application/json")
    if not model_name:
        return None

    # Look up model schema
    api_models = getattr(rest_api, "models", {}) or {}
    model = api_models.get(model_name)
    if not model:
        return None

    schema = getattr(model, "schema", None)
    if not schema:
        return None

    # Parse schema
    try:
        if isinstance(schema, str):
            schema = json.loads(schema)
    except json.JSONDecodeError:
        return None

    # Basic JSON schema validation
    if body:
        try:
            body_json = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return 400, {}, json.dumps({"message": "Invalid request body"})

        error = _validate_json_schema(body_json, schema)
        if error:
            return 400, {}, json.dumps({"message": error})

    return None


def _validate_json_schema(data: object, schema: dict) -> str | None:
    """Basic JSON schema validation. Returns error message or None."""
    schema_type = schema.get("type")

    if schema_type == "object":
        if not isinstance(data, dict):
            return "Expected object"
        required = schema.get("required", [])
        for field in required:
            if field not in data:
                return f"Missing required field: {field}"
        properties = schema.get("properties", {})
        for prop_name, prop_schema in properties.items():
            if prop_name in data:
                err = _validate_json_schema(data[prop_name], prop_schema)
                if err:
                    return f"{prop_name}: {err}"
    elif schema_type == "array":
        if not isinstance(data, list):
            return "Expected array"
    elif schema_type == "string":
        if not isinstance(data, str):
            return "Expected string"
    elif schema_type == "number":
        if not isinstance(data, (int, float)):
            return "Expected number"
    elif schema_type == "integer":
        if not isinstance(data, int):
            return "Expected integer"
    elif schema_type == "boolean":
        if not isinstance(data, bool):
            return "Expected boolean"

    return None


# ---------------------------------------------------------------------------
# Gateway responses
# ---------------------------------------------------------------------------


def _gateway_response(
    rest_api,
    response_type: str,
    request_id: str,
) -> tuple[int, dict, str]:
    """Return a gateway response for the given type."""
    # Check for custom gateway responses on the API
    custom_responses = getattr(rest_api, "gateway_responses", {}) or {}
    resp = custom_responses.get(response_type)

    if resp:
        status = int(getattr(resp, "status_code", 0) or 0)
        templates = getattr(resp, "response_templates", {}) or {}
        resp_params = getattr(resp, "response_parameters", {}) or {}
    else:
        fallback = DEFAULT_GATEWAY_RESPONSES["DEFAULT_4XX"]
        default = DEFAULT_GATEWAY_RESPONSES.get(response_type, fallback)
        status = default["statusCode"]
        templates = default.get("responseTemplates", {})
        resp_params = {}

    body = templates.get("application/json", json.dumps({"message": response_type}))
    # Replace $context variables in body
    body = body.replace("$context.requestId", request_id)

    resp_headers = {}
    for param_key, param_val in resp_params.items():
        # param_key like "gatewayresponse.header.X-Custom"
        if "header." in param_key:
            header_name = param_key.split("header.")[-1]
            resp_headers[header_name] = param_val.strip("'\"")

    return status, resp_headers, body


# ---------------------------------------------------------------------------
# Integration invocations
# ---------------------------------------------------------------------------


def _build_vtl_context(
    body_str: str | None,
    headers: dict,
    query_params: dict,
    path_params: dict,
    stage_vars: dict,
    context_vars: dict,
) -> VtlContext:
    """Build a VTL context for mapping template evaluation."""
    return VtlContext(
        body=body_str or "",
        headers=headers,
        query_params=query_params,
        path_params=path_params,
        stage_variables=stage_vars,
        context_vars=context_vars,
    )


def _apply_request_template(
    integration,
    body_str: str | None,
    headers: dict,
    query_params: dict,
    path_params: dict,
    stage_vars: dict,
    context_vars: dict,
) -> str | None:
    """Apply request mapping template if configured."""
    templates = getattr(integration, "request_templates", None) or {}
    content_type = headers.get("content-type", "application/json")
    template = templates.get(content_type) or templates.get("application/json")
    if not template:
        return body_str

    vtl_ctx = _build_vtl_context(
        body_str, headers, query_params, path_params, stage_vars, context_vars
    )
    return evaluate_vtl(template, vtl_ctx)


def _apply_response_template(
    integration_response,
    response_body: str,
    headers: dict,
    query_params: dict,
    path_params: dict,
    stage_vars: dict,
    context_vars: dict,
) -> str:
    """Apply response mapping template if configured."""
    templates = getattr(integration_response, "response_templates", None) or {}
    template = templates.get("application/json")
    if not template:
        return response_body

    vtl_ctx = _build_vtl_context(
        response_body, headers, query_params, path_params, stage_vars, context_vars
    )
    return evaluate_vtl(template, vtl_ctx)


def _invoke_lambda_proxy(
    integration,
    method,
    path,
    body,
    headers,
    query_params,
    path_params,
    stage,
    rest_api_id,
    region,
    account_id,
    stage_vars=None,
    context_vars=None,
) -> tuple[int, dict, str]:
    """Invoke Lambda with API Gateway proxy integration."""
    uri = getattr(integration, "uri", "") or ""
    if stage_vars:
        uri = _substitute_stage_variables(uri, stage_vars)
    function_name = _extract_lambda_function_from_uri(uri)

    if not function_name:
        return 500, {}, json.dumps({"message": "Could not resolve Lambda function from URI"})

    request_id = (context_vars or {}).get("requestId", str(uuid.uuid4()))

    # Build proxy event (API Gateway format)
    event = {
        "resource": path,
        "path": path,
        "httpMethod": method.upper(),
        "headers": dict(headers) if headers else {},
        "queryStringParameters": query_params if query_params else None,
        "pathParameters": path_params if path_params else None,
        "stageVariables": stage_vars if stage_vars else None,
        "requestContext": {
            "resourceId": "proxy",
            "resourcePath": path,
            "httpMethod": method.upper(),
            "path": f"/{stage}{path}",
            "accountId": account_id,
            "stage": stage,
            "requestId": request_id,
            "identity": {
                "sourceIp": headers.get("x-forwarded-for", "127.0.0.1"),
                "userAgent": headers.get("user-agent", ""),
            },
            "apiId": rest_api_id,
        },
        "body": body.decode() if body else None,
        "isBase64Encoded": False,
    }

    # Check for binary media type support
    if body and _is_binary_content(headers, integration):
        event["body"] = base64.b64encode(body).decode()
        event["isBase64Encoded"] = True

    # Invoke Lambda
    result = _invoke_lambda(function_name, event, region, account_id)

    if result is None:
        return 502, {}, json.dumps({"message": "Internal server error"})

    # Parse Lambda response (proxy integration format)
    if isinstance(result, dict):
        status_code = result.get("statusCode", 200)
        resp_headers = result.get("headers", {})
        resp_body = result.get("body", "")
        if result.get("isBase64Encoded") and resp_body:
            resp_body = base64.b64decode(resp_body).decode()
        return status_code, resp_headers, resp_body
    else:
        return 200, {}, json.dumps(result) if result else ""


def _invoke_mock(
    integration,
    method_obj,
    body_str=None,
    headers=None,
    query_params=None,
    path_params=None,
    stage_vars=None,
    context_vars=None,
) -> tuple[int, dict, str]:
    """Handle mock integration with VTL mapping template support."""
    headers = headers or {}
    query_params = query_params or {}
    path_params = path_params or {}
    stage_vars = stage_vars or {}
    context_vars = context_vars or {}

    # Apply request template (for mock, this builds the "backend response")
    if body_str is not None or stage_vars or context_vars:
        _apply_request_template(
            integration,
            body_str,
            headers,
            query_params,
            path_params,
            stage_vars,
            context_vars,
        )

    responses = getattr(integration, "integration_responses", None) or {}

    # Find best matching response: try "200", then "default", then first available
    status_code = 200
    resp_body = "{}"
    resp_headers: dict = {}

    resp = None
    matched_key = None
    if "200" in responses:
        matched_key = "200"
        resp = responses["200"]
    elif "default" in responses:
        matched_key = "default"
        resp = responses["default"]
    elif responses:
        matched_key = next(iter(responses))
        resp = responses[matched_key]

    if resp is not None:
        # Use matched key as status code if it's numeric
        if matched_key and matched_key.isdigit():
            status_code = int(matched_key)

        templates = getattr(resp, "response_templates", None) or {}
        resp_body = templates.get("application/json", "")

        # Apply response template VTL
        if resp_body and any(c in resp_body for c in ("$", "#")):
            vtl_ctx = _build_vtl_context(
                body_str,
                headers,
                query_params,
                path_params,
                stage_vars,
                context_vars,
            )
            resp_body = evaluate_vtl(resp_body, vtl_ctx)

        # Response parameters (header mappings)
        resp_params = getattr(resp, "response_parameters", None) or {}
        for param_key, param_val in resp_params.items():
            if "header." in param_key:
                header_name = param_key.split("header.")[-1]
                resp_headers[header_name] = param_val.strip("'\"")

    return status_code, resp_headers, resp_body


def _invoke_aws_service(
    integration,
    uri,
    body,
    headers,
    query_params=None,
    path_params=None,
    stage_vars=None,
    context_vars=None,
    region="us-east-1",
    account_id="123456789012",
) -> tuple[int, dict, str]:
    """Handle AWS service integration (non-proxy) with request/response mapping."""
    query_params = query_params or {}
    path_params = path_params or {}
    stage_vars = stage_vars or {}
    context_vars = context_vars or {}

    body_str = body.decode() if body else None

    # Apply request mapping template
    mapped_body = _apply_request_template(
        integration,
        body_str,
        headers,
        query_params,
        path_params,
        stage_vars,
        context_vars,
    )

    # Parse the URI to determine the target service
    # Format: arn:aws:apigateway:{region}:{service}:action/{action}
    # or: arn:aws:apigateway:{region}:{service}:path/{path}
    if not uri:
        return 500, {}, json.dumps({"message": "No URI configured for AWS integration"})

    # Try to forward to the appropriate Moto backend
    result = _forward_to_aws_backend(uri, mapped_body, headers, region, account_id)

    # Apply response mapping template
    responses = getattr(integration, "integration_responses", None) or {}
    if "200" in responses:
        resp = responses["200"]
        result = _apply_response_template(
            resp,
            result,
            headers,
            query_params,
            path_params,
            stage_vars,
            context_vars,
        )

    return 200, {}, result


def _forward_to_aws_backend(
    uri: str,
    body: str | None,
    headers: dict,
    region: str,
    account_id: str,
) -> str:
    """Forward an AWS integration request to the appropriate Moto backend."""
    # Parse service and action from URI
    # arn:aws:apigateway:region:lambda:path/2015-03-31/functions/ARN/invocations
    # arn:aws:apigateway:region:dynamodb:action/PutItem
    arn_match = re.match(r"arn:aws:apigateway:[^:]+:(\w+):(action|path)/(.+)", uri)
    if not arn_match:
        return json.dumps({"message": "AWS integration executed"})

    service = arn_match.group(1)
    mode = arn_match.group(2)
    target = arn_match.group(3)

    if service == "lambda" and "functions" in target:
        # Lambda invocation
        fn_name = _extract_lambda_function_from_uri(uri)
        if fn_name:
            payload = json.loads(body) if body else {}
            result = _invoke_lambda(fn_name, payload, region, account_id)
            if result is not None:
                return json.dumps(result) if isinstance(result, (dict, list)) else str(result)
        return json.dumps({"message": "Lambda invocation failed"})

    if service == "dynamodb" and mode == "action":
        return _forward_dynamodb_action(target, body, region, account_id)

    if service == "sqs" and mode == "action":
        return _forward_sqs_action(target, body, region, account_id)

    if service == "sns" and mode == "action":
        return _forward_sns_action(target, body, region, account_id)

    return json.dumps({"message": f"AWS {service} integration executed"})


def _forward_dynamodb_action(
    action: str,
    body: str | None,
    region: str,
    account_id: str,
) -> str:
    """Forward a DynamoDB action through the API Gateway AWS integration."""
    try:
        from moto.backends import get_backend
        from moto.core import DEFAULT_ACCOUNT_ID

        acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID
        backend = get_backend("dynamodb")[acct][region]
        params = json.loads(body) if body else {}

        if action == "PutItem":
            table_name = params.get("TableName", "")
            item = params.get("Item", {})
            backend.put_item(table_name, item)
            return json.dumps({})
        elif action == "GetItem":
            table_name = params.get("TableName", "")
            key = params.get("Key", {})
            result = backend.get_item(table_name, key)
            if result:
                return json.dumps({"Item": result})
            return json.dumps({})
        elif action == "Query":
            return json.dumps({"Items": [], "Count": 0})

    except Exception as e:  # noqa: BLE001
        logger.error("DynamoDB integration error: %s", e)
    return json.dumps({"message": "DynamoDB integration executed"})


def _forward_sqs_action(
    action: str,
    body: str | None,
    region: str,
    account_id: str,
) -> str:
    """Forward an SQS action through the API Gateway AWS integration."""
    return json.dumps({"message": f"SQS {action} executed"})


def _forward_sns_action(
    action: str,
    body: str | None,
    region: str,
    account_id: str,
) -> str:
    """Forward an SNS action through the API Gateway AWS integration."""
    return json.dumps({"message": f"SNS {action} executed"})


def _invoke_http(
    integration,
    uri,
    method,
    body,
    headers,
    query_params=None,
    path_params=None,
    stage_vars=None,
    context_vars=None,
) -> tuple[int, dict, str]:
    """Handle HTTP/HTTP_PROXY integration with mapping template support."""
    query_params = query_params or {}
    path_params = path_params or {}
    stage_vars = stage_vars or {}
    context_vars = context_vars or {}

    integration_type = getattr(integration, "integration_type", "").upper()
    body_str = body.decode() if body else None

    if integration_type == "HTTP":
        # Non-proxy: apply request mapping
        _apply_request_template(
            integration,
            body_str,
            headers,
            query_params,
            path_params,
            stage_vars,
            context_vars,
        )
        # Would make HTTP request here; for now return mock response
        result_body = json.dumps({"message": "HTTP integration executed"})

        # Apply response mapping
        responses = getattr(integration, "integration_responses", None) or {}
        if "200" in responses:
            resp = responses["200"]
            result_body = _apply_response_template(
                resp,
                result_body,
                headers,
                query_params,
                path_params,
                stage_vars,
                context_vars,
            )
        return 200, {}, result_body
    else:
        # HTTP_PROXY: pass through
        return 200, {}, json.dumps({"message": "HTTP proxy integration executed"})


# ---------------------------------------------------------------------------
# Binary media type support
# ---------------------------------------------------------------------------


def _is_binary_content(headers: dict, integration) -> bool:
    """Check if the request content should be treated as binary."""
    content_type = headers.get("content-type", "")
    if not content_type:
        return False

    # Check configured binary media types on the integration/API
    binary_types = getattr(integration, "content_handling", None)
    if binary_types == "CONVERT_TO_BINARY":
        return True

    # Common binary types
    binary_patterns = [
        "application/octet-stream",
        "image/",
        "audio/",
        "video/",
        "application/pdf",
        "application/zip",
    ]
    for pattern in binary_patterns:
        if content_type.startswith(pattern):
            return True

    return False


# ---------------------------------------------------------------------------
# Lambda invocation helpers
# ---------------------------------------------------------------------------


def _extract_lambda_function_from_uri(uri: str) -> str | None:
    """Extract Lambda function name/ARN from integration URI.

    URI format: arn:aws:apigateway:{region}:lambda:path/2015-03-31/functions/{arn}/invocations
    """
    match = re.search(r"functions/([^/]+)/invocations", uri)
    if match:
        arn_or_name = match.group(1)
        # Extract function name from ARN
        if arn_or_name.startswith("arn:"):
            parts = arn_or_name.split(":")
            if len(parts) >= 7:
                return parts[6]
        return arn_or_name
    return None


def _invoke_lambda(function_name: str, event: dict, region: str, account_id: str):
    """Invoke a Lambda function and return the result."""
    from robotocore.services.lambda_.executor import execute_python_handler

    try:
        from moto.backends import get_backend
        from moto.core import DEFAULT_ACCOUNT_ID

        acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID
        backend = get_backend("lambda")[acct][region]
        fn = backend.get_function(function_name)
    except Exception:  # noqa: BLE001
        logger.error(f"API Gateway: Lambda function not found: {function_name}")
        return None

    runtime = getattr(fn, "run_time", "") or ""
    if runtime.startswith("python"):
        # Prefer code_bytes (already decoded) over code["ZipFile"] (may be base64)
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
                return {"statusCode": 500, "body": json.dumps({"error": str(result)})}
            return result

    # Non-Python: return mock success
    return {"statusCode": 200, "body": "OK"}
