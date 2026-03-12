"""Lambda Function URL routing — invokes Lambda functions via their Function URLs.

When a function URL config is created, the function becomes invocable at a URL like:
  https://{url-id}.lambda-url.{region}.on.aws/

In the emulator, we route requests matching the path pattern:
  /lambda-url/{url-id}/{path}
or Host header matching:
  {url-id}.lambda-url.{region}.on.aws

The request is converted to the API Gateway v2 payload format and the Lambda
function is invoked. The function's return value becomes the HTTP response.
"""

import base64
import json
import logging
import time

from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.lambda_.invoke import invoke_lambda_sync
from robotocore.services.lambda_.urls import _url_configs, _url_lock

logger = logging.getLogger(__name__)


def find_config_by_url_id(url_id: str) -> dict | None:
    """Find a function URL config by its URL ID prefix."""
    with _url_lock:
        for config in _url_configs.values():
            func_url = config.get("FunctionUrl", "")
            # FunctionUrl looks like https://{url_id}.lambda-url.{region}.on.aws/
            if f"https://{url_id}." in func_url:
                return dict(config)
    return None


def _parse_function_url_host(host: str) -> str | None:
    """Extract the url-id from a lambda-url Host header.

    Pattern: {url-id}.lambda-url.{region}.on.aws
    Returns the url-id or None if the host doesn't match.
    """
    if ".lambda-url." not in host:
        return None
    # Strip port if present
    hostname = host.split(":")[0]
    parts = hostname.split(".")
    # Expect: url-id.lambda-url.region.on.aws (5 parts)
    if len(parts) >= 5 and parts[1] == "lambda-url" and parts[3] == "on" and parts[4] == "aws":
        return parts[0]
    return None


def is_function_url_request(scope: dict) -> bool:
    """Check if an ASGI scope represents a Lambda function URL request."""
    # Check path pattern: /lambda-url/{url-id}/...
    path = scope.get("path", "")
    if path.startswith("/lambda-url/"):
        return True

    # Check Host header pattern
    for key, val in scope.get("headers", []):
        if key == b"host":
            host = val.decode("latin-1")
            if ".lambda-url." in host:
                return True
            break

    return False


def _extract_url_id_from_path(path: str) -> tuple[str, str]:
    """Extract url-id and remaining path from /lambda-url/{url-id}/{path}.

    Returns (url_id, remaining_path).
    """
    # /lambda-url/{url-id}/...
    parts = path.split("/", 3)  # ['', 'lambda-url', 'url-id', 'rest...']
    url_id = parts[2] if len(parts) > 2 else ""
    remaining = "/" + parts[3] if len(parts) > 3 else "/"
    return url_id, remaining


def _build_function_url_event(
    request: Request,
    body: bytes,
    path: str,
    url_id: str,
    func_arn: str,
) -> dict:
    """Build the API Gateway v2 payload format event for a function URL invocation."""
    headers = dict(request.headers)
    query_params = dict(request.query_params)
    method = request.method

    # Determine if body is base64 encoded
    is_base64 = False
    body_str = ""
    if body:
        try:
            body_str = body.decode("utf-8")
        except UnicodeDecodeError:
            body_str = base64.b64encode(body).decode("ascii")
            is_base64 = True

    # Build queryStringParameters (single values, last wins — matches AWS behavior)
    query_string_params = None
    if query_params:
        query_string_params = dict(query_params)

    # Build the raw query string
    raw_query = request.url.query or ""

    # Request context
    now = time.time()
    time_str = time.strftime("%d/%b/%Y:%H:%M:%S +0000", time.gmtime(now))
    epoch_ms = int(now * 1000)

    request_context = {
        "accountId": "123456789012",
        "apiId": url_id,
        "domainName": f"{url_id}.lambda-url.us-east-1.on.aws",
        "domainPrefix": url_id,
        "http": {
            "method": method,
            "path": path,
            "protocol": "HTTP/1.1",
            "sourceIp": request.client.host if request.client else "127.0.0.1",
            "userAgent": headers.get("user-agent", ""),
        },
        "requestId": f"req-{url_id}-{epoch_ms}",
        "routeKey": "$default",
        "stage": "$default",
        "time": time_str,
        "timeEpoch": epoch_ms,
    }

    event = {
        "version": "2.0",
        "routeKey": "$default",
        "rawPath": path,
        "rawQueryString": raw_query,
        "headers": headers,
        "requestContext": request_context,
        "isBase64Encoded": is_base64,
    }

    if body_str:
        event["body"] = body_str

    if query_string_params:
        event["queryStringParameters"] = query_string_params

    return event


async def handle_function_url_request(
    request: Request,
    url_id: str,
    path: str,
) -> Response:
    """Handle a Lambda function URL invocation.

    Builds the v2 event format, invokes the function, and returns the response.
    """
    config = find_config_by_url_id(url_id)
    if not config:
        return Response(
            content=json.dumps({"message": "Function URL not found"}),
            status_code=404,
            media_type="application/json",
        )

    func_arn = config.get("FunctionArn", "")
    cors_config = config.get("Cors", {})

    body = await request.body()
    event = _build_function_url_event(request, body, path, url_id, func_arn)

    # Parse region and account from the ARN
    arn_parts = func_arn.split(":")
    region = arn_parts[3] if len(arn_parts) > 3 else "us-east-1"
    account_id = arn_parts[4] if len(arn_parts) > 4 else "123456789012"

    result, error_type, logs = invoke_lambda_sync(
        function_arn=func_arn,
        payload=event,
        region=region,
        account_id=account_id,
    )

    # Build response from Lambda result
    resp_headers: dict[str, str] = {}

    # Apply CORS headers if configured
    if cors_config:
        if cors_config.get("AllowOrigins"):
            origins = cors_config["AllowOrigins"]
            request_origin = request.headers.get("origin", "")
            if "*" in origins:
                resp_headers["access-control-allow-origin"] = "*"
            elif request_origin in origins:
                resp_headers["access-control-allow-origin"] = request_origin
        if cors_config.get("AllowMethods"):
            resp_headers["access-control-allow-methods"] = ", ".join(cors_config["AllowMethods"])
        if cors_config.get("AllowHeaders"):
            resp_headers["access-control-allow-headers"] = ", ".join(cors_config["AllowHeaders"])
        if cors_config.get("ExposeHeaders"):
            resp_headers["access-control-expose-headers"] = ", ".join(cors_config["ExposeHeaders"])
        if cors_config.get("MaxAge") is not None:
            resp_headers["access-control-max-age"] = str(cors_config["MaxAge"])
        if cors_config.get("AllowCredentials"):
            resp_headers["access-control-allow-credentials"] = "true"

    if error_type:
        error_body = json.dumps(
            {
                "errorMessage": str(result) if result else error_type,
                "errorType": error_type,
            }
        )
        resp_headers["x-amzn-errortype"] = error_type
        return Response(
            content=error_body,
            status_code=502,
            headers=resp_headers,
            media_type="application/json",
        )

    # Lambda can return a structured response (like API Gateway v2 format)
    # or a simple value
    if isinstance(result, dict):
        # Check if it's a structured response with statusCode
        if "statusCode" in result:
            status_code = int(result["statusCode"])
            resp_body = result.get("body", "")
            if result.get("headers"):
                resp_headers.update(result["headers"])
            if result.get("isBase64Encoded") and resp_body:
                resp_body = base64.b64decode(resp_body)
                return Response(
                    content=resp_body,
                    status_code=status_code,
                    headers=resp_headers,
                )
            return Response(
                content=resp_body if isinstance(resp_body, str) else json.dumps(resp_body),
                status_code=status_code,
                headers=resp_headers,
                media_type="application/json",
            )
        # Simple dict response
        return Response(
            content=json.dumps(result),
            status_code=200,
            headers=resp_headers,
            media_type="application/json",
        )
    elif isinstance(result, str):
        return Response(
            content=result,
            status_code=200,
            headers=resp_headers,
            media_type="application/json",
        )
    else:
        return Response(
            content=json.dumps(result) if result is not None else "null",
            status_code=200,
            headers=resp_headers,
            media_type="application/json",
        )
