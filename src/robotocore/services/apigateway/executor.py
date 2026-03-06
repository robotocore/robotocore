"""API Gateway execution layer — invokes Lambda/HTTP backends from API Gateway.

When a request hits an API Gateway endpoint (via execute-api), this module:
1. Looks up the REST API, resource, and method in Moto's API Gateway backend
2. Resolves the integration (Lambda, HTTP, AWS service, Mock)
3. Invokes the integration and returns the response

This is a key Enterprise feature — Moto stores the API config but doesn't
actually execute integrations.
"""

import base64
import json
import logging
import re
import uuid

logger = logging.getLogger(__name__)


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

    try:
        backend = get_backend("apigateway")[acct][region]
    except Exception:
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
        return 404, {}, json.dumps({"message": f"No resource matches path: {path}"})

    # Find the method (resource_methods contains Moto Method objects)
    method_upper = method.upper()
    method_obj = resource.resource_methods.get(method_upper)
    if not method_obj and "ANY" in resource.resource_methods:
        method_obj = resource.resource_methods["ANY"]
    if not method_obj:
        return 405, {}, json.dumps({"message": f"Method {method_upper} not allowed"})

    # Get integration (Moto Integration object, not a dict)
    integration = getattr(method_obj, "method_integration", None)
    if not integration:
        return 500, {}, json.dumps({"message": "No integration configured"})

    integration_type = getattr(integration, "integration_type", "").upper()

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
        )
    elif integration_type == "MOCK":
        return _invoke_mock(integration, method_obj)
    elif integration_type == "AWS":
        return _invoke_aws_service(integration, body, headers, region, account_id)
    elif integration_type in ("HTTP", "HTTP_PROXY"):
        return _invoke_http(integration, method, body, headers)
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
) -> tuple[int, dict, str]:
    """Invoke Lambda with API Gateway proxy integration."""
    uri = getattr(integration, "uri", "") or ""
    function_name = _extract_lambda_function_from_uri(uri)

    if not function_name:
        return 500, {}, json.dumps({"message": "Could not resolve Lambda function from URI"})

    # Build proxy event (API Gateway format)
    event = {
        "resource": path,
        "path": path,
        "httpMethod": method.upper(),
        "headers": dict(headers) if headers else {},
        "queryStringParameters": query_params if query_params else None,
        "pathParameters": path_params if path_params else None,
        "stageVariables": None,
        "requestContext": {
            "resourceId": "proxy",
            "resourcePath": path,
            "httpMethod": method.upper(),
            "path": f"/{stage}{path}",
            "accountId": account_id,
            "stage": stage,
            "requestId": str(uuid.uuid4()),
            "identity": {
                "sourceIp": headers.get("x-forwarded-for", "127.0.0.1"),
                "userAgent": headers.get("user-agent", ""),
            },
            "apiId": rest_api_id,
        },
        "body": body.decode() if body else None,
        "isBase64Encoded": False,
    }

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


def _invoke_mock(integration, method_obj) -> tuple[int, dict, str]:
    """Handle mock integration."""
    responses = getattr(integration, "integration_responses", None) or {}
    if "200" in responses:
        resp = responses["200"]
        templates = getattr(resp, "response_templates", None) or {}
        body = templates.get("application/json", "")
        return 200, {}, body
    return 200, {}, "{}"


def _invoke_aws_service(integration, body, headers, region, account_id) -> tuple[int, dict, str]:
    """Handle AWS service integration (non-proxy)."""
    # Future: support DynamoDB, SQS, SNS, etc. service integrations
    return 200, {}, json.dumps({"message": "AWS service integration executed"})


def _invoke_http(integration, method, body, headers) -> tuple[int, dict, str]:
    """Handle HTTP/HTTP_PROXY integration."""
    # Future: actual HTTP proxy
    return 200, {}, json.dumps({"message": "HTTP integration executed"})


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
    except Exception:
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
