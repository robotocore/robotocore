"""Console web UI routes and API proxy endpoints."""

import json
import re
from pathlib import Path

import httpx
from starlette.requests import Request
from starlette.responses import FileResponse, HTMLResponse, JSONResponse, Response

STATIC_DIR = Path(__file__).parent / "static"

# Regex to extract account ID from SigV4 Credential
_CREDENTIAL_RE = re.compile(r"Credential=(\d+)/")
DEFAULT_ACCOUNT_ID = "123456789012"


def _extract_account_id(request: Request) -> str:
    """Extract account ID from Authorization header."""
    auth = request.headers.get("authorization", "")
    match = _CREDENTIAL_RE.search(auth)
    if match:
        return match.group(1)
    return DEFAULT_ACCOUNT_ID


async def console_index(request: Request) -> HTMLResponse:
    """Serve the main console SPA shell."""
    index_path = STATIC_DIR / "index.html"
    content = index_path.read_text()
    return HTMLResponse(content)


async def console_static(request: Request) -> Response:
    """Serve static files (CSS, JS) from the console static directory."""
    file_path = request.path_params.get("path", "")
    full_path = STATIC_DIR / file_path

    # Security: prevent directory traversal
    try:
        full_path = full_path.resolve()
        if not str(full_path).startswith(str(STATIC_DIR.resolve())):
            return JSONResponse({"error": "Forbidden"}, status_code=403)
    except (ValueError, OSError):
        return JSONResponse({"error": "Invalid path"}, status_code=400)

    if not full_path.exists() or not full_path.is_file():
        return JSONResponse({"error": "Not found"}, status_code=404)

    # Determine content type
    suffix = full_path.suffix.lower()
    content_types = {
        ".html": "text/html",
        ".css": "text/css",
        ".js": "application/javascript",
        ".json": "application/json",
        ".png": "image/png",
        ".svg": "image/svg+xml",
        ".ico": "image/x-icon",
    }
    media_type = content_types.get(suffix, "application/octet-stream")

    return FileResponse(str(full_path), media_type=media_type)


# ---------------------------------------------------------------------------
# API proxy endpoints -- these call the real AWS endpoints on the local server
# ---------------------------------------------------------------------------


async def api_proxy(request: Request) -> Response:
    """Generic API proxy that forwards requests to the actual AWS endpoints.

    Routes like /_robotocore/console/api/s3/ListBuckets get converted into
    actual AWS API calls against the local emulator.
    """
    import httpx

    service = request.path_params.get("service", "")
    action = request.path_params.get("action", "")

    if not service or not action:
        return JSONResponse({"error": "Missing service or action"}, status_code=400)

    # Read the request body
    body = await request.body()
    params = {}
    if body:
        try:
            params = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    region = params.pop("_region", "us-east-1")
    account_id = params.pop("_account_id", DEFAULT_ACCOUNT_ID)

    # Build the AWS-style request based on service protocol
    base_url = "http://localhost:4566"

    try:
        async with httpx.AsyncClient() as client:
            response = await _make_aws_request(
                client, base_url, service, action, params, region, account_id
            )
            # Return the response
            return Response(
                content=response.content,
                status_code=response.status_code,
                headers={"Content-Type": response.headers.get("content-type", "application/json")},
            )
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"error": str(e)}, status_code=500)


async def _make_aws_request(
    client: httpx.AsyncClient,
    base_url: str,
    service: str,
    action: str,
    params: dict,
    region: str,
    account_id: str,
) -> httpx.Response:
    """Build and send an AWS-style HTTP request to the local emulator."""
    from datetime import UTC, datetime

    # Common SigV4-like auth header
    now = datetime.now(UTC)
    date_stamp = now.strftime("%Y%m%d")
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")

    credential = f"{account_id}/{date_stamp}/{region}/{service}/aws4_request"
    auth_header = (
        f"AWS4-HMAC-SHA256 Credential={credential}, "
        f"SignedHeaders=host;x-amz-date, Signature={'a' * 64}"
    )

    common_headers = {
        "Authorization": auth_header,
        "X-Amz-Date": amz_date,
        "Host": "localhost:4566",
    }

    # Service-specific request building
    if service == "s3":
        return await _s3_request(client, base_url, action, params, common_headers)
    elif service == "dynamodb":
        return await _dynamodb_request(client, base_url, action, params, common_headers)
    elif service == "sqs":
        return await _sqs_request(client, base_url, action, params, common_headers)
    elif service == "lambda":
        return await _lambda_request(client, base_url, action, params, common_headers)
    elif service == "logs":
        return await _logs_request(client, base_url, action, params, common_headers)
    else:
        return await _generic_json_request(
            client, base_url, service, action, params, common_headers
        )


async def _s3_request(client, base_url, action, params, headers):
    """Build S3 REST-XML style requests."""
    bucket = params.get("Bucket", "")
    key = params.get("Key", "")

    if action == "ListBuckets":
        return await client.get(f"{base_url}/", headers=headers)
    elif action == "CreateBucket":
        body = ""
        region = headers.get("X-Amz-Region", "us-east-1")
        if region and region != "us-east-1":
            body = (
                f'<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                f"<LocationConstraint>{region}</LocationConstraint>"
                f"</CreateBucketConfiguration>"
            )
        return await client.put(f"{base_url}/{bucket}", content=body, headers=headers)
    elif action == "DeleteBucket":
        return await client.delete(f"{base_url}/{bucket}", headers=headers)
    elif action == "ListObjects":
        prefix = params.get("Prefix", "")
        url = f"{base_url}/{bucket}?list-type=2"
        if prefix:
            url += f"&prefix={prefix}"
        return await client.get(url, headers=headers)
    elif action == "GetObject":
        return await client.get(f"{base_url}/{bucket}/{key}", headers=headers)
    elif action == "PutObject":
        body = params.get("Body", "")
        if isinstance(body, str):
            body = body.encode("utf-8")
        content_type = params.get("ContentType", "application/octet-stream")
        put_headers = {**headers, "Content-Type": content_type}
        return await client.put(f"{base_url}/{bucket}/{key}", content=body, headers=put_headers)
    elif action == "DeleteObject":
        return await client.delete(f"{base_url}/{bucket}/{key}", headers=headers)
    elif action == "HeadBucket":
        return await client.head(f"{base_url}/{bucket}", headers=headers)
    else:
        return await client.get(f"{base_url}/", headers=headers)


async def _dynamodb_request(client, base_url, action, params, headers):
    """Build DynamoDB JSON protocol requests."""
    ddb_headers = {
        **headers,
        "Content-Type": "application/x-amz-json-1.0",
        "X-Amz-Target": f"DynamoDB_20120810.{action}",
    }
    body = json.dumps(params) if params else "{}"
    return await client.post(base_url, content=body, headers=ddb_headers)


async def _sqs_request(client, base_url, action, params, headers):
    """Build SQS query protocol requests."""
    query_params = {"Action": action, "Version": "2012-11-05"}
    query_params.update(params)

    sqs_headers = {
        **headers,
        "Content-Type": "application/x-www-form-urlencoded",
    }
    # Build form body
    form_body = "&".join(f"{k}={v}" for k, v in query_params.items())
    return await client.post(base_url, content=form_body, headers=sqs_headers)


async def _lambda_request(client, base_url, action, params, headers):
    """Build Lambda REST-JSON requests."""
    lambda_headers = {
        **headers,
        "Content-Type": "application/json",
    }

    if action == "ListFunctions":
        return await client.get(f"{base_url}/2015-03-31/functions", headers=lambda_headers)
    elif action == "GetFunction":
        name = params.get("FunctionName", "")
        return await client.get(f"{base_url}/2015-03-31/functions/{name}", headers=lambda_headers)
    elif action == "Invoke":
        name = params.get("FunctionName", "")
        payload = params.get("Payload", "{}")
        if isinstance(payload, dict):
            payload = json.dumps(payload)
        return await client.post(
            f"{base_url}/2015-03-31/functions/{name}/invocations",
            content=payload,
            headers=lambda_headers,
        )
    elif action == "CreateFunction":
        return await client.post(
            f"{base_url}/2015-03-31/functions",
            content=json.dumps(params),
            headers=lambda_headers,
        )
    elif action == "DeleteFunction":
        name = params.get("FunctionName", "")
        return await client.delete(
            f"{base_url}/2015-03-31/functions/{name}", headers=lambda_headers
        )
    else:
        return await client.get(f"{base_url}/2015-03-31/functions", headers=lambda_headers)


async def _logs_request(client, base_url, action, params, headers):
    """Build CloudWatch Logs JSON requests."""
    logs_headers = {
        **headers,
        "Content-Type": "application/x-amz-json-1.1",
        "X-Amz-Target": f"Logs_20140328.{action}",
    }
    body = json.dumps(params) if params else "{}"
    return await client.post(base_url, content=body, headers=logs_headers)


async def _generic_json_request(client, base_url, service, action, params, headers):
    """Generic JSON protocol request for services we don't have specific handlers for."""
    json_headers = {
        **headers,
        "Content-Type": "application/x-amz-json-1.1",
        "X-Amz-Target": f"{service}.{action}",
    }
    body = json.dumps(params) if params else "{}"
    return await client.post(base_url, content=body, headers=json_headers)


def get_console_routes():
    """Return Starlette Route objects for the console."""
    from starlette.routing import Route

    return [
        Route("/_robotocore/console", console_index, methods=["GET"]),
        Route("/_robotocore/console/", console_index, methods=["GET"]),
        Route("/_robotocore/console/api/{service}/{action}", api_proxy, methods=["POST"]),
        Route("/_robotocore/console/static/{path:path}", console_static, methods=["GET"]),
    ]
