"""Moto bridge sidecar - wraps Moto backends behind a minimal HTTP server.

This is the Python component of the Rust-Moto bridge. The Rust robotocore
server handles native services directly and proxies all other AWS service
requests to this sidecar, which dispatches them to Moto backends.

Run with:
    python scripts/moto_sidecar.py --port 4567

The Rust server talks to this over HTTP on localhost.
"""

import argparse
import json
import logging
import os

os.environ.setdefault("MOTO_ALLOW_NONEXISTENT_REGION", "true")

import moto.backends as moto_backends
import uvicorn
from moto.core.base_backend import BackendDict
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route
from urllib.parse import parse_qsl
from werkzeug.datastructures import ImmutableMultiDict
from werkzeug.routing import Map, Rule
from werkzeug.routing.converters import BaseConverter
from werkzeug.test import EnvironBuilder
from werkzeug.wrappers import Request as WerkzeugRequest

DEFAULT_ACCOUNT_ID = "123456789012"

logger = logging.getLogger("moto_sidecar")


class WerkzeugRawBodyRequest(WerkzeugRequest):
    """Werkzeug request that preserves a raw body for Moto.

    Moto reads `request.body` (boto-style) and also `request.form`
    (query-protocol params live in the urlencoded body). Parse the form from
    the raw body for urlencoded content types so parameters survive, while
    still exposing the undecoded payload via `.body` / `.data`.
    """

    def __init__(self, environ: dict, body: bytes):
        super().__init__(environ)
        self.body = body
        self._form_cache: ImmutableMultiDict | None = None

    @property
    def data(self) -> bytes:
        return self.body

    def get_data(self, *args, **kwargs) -> bytes:
        return self.body

    @property
    def form(self):
        if self._form_cache is None:
            mimetype = (self.mimetype or "").split(";")[0].strip()
            if mimetype == "application/x-www-form-urlencoded":
                try:
                    pairs = parse_qsl(self.body.decode("utf-8"), keep_blank_values=True)
                except UnicodeDecodeError:
                    pairs = []
            else:
                pairs = []
            self._form_cache = ImmutableMultiDict(pairs)
        return self._form_cache


class _RegexConverter(BaseConverter):
    part_isolating = False

    def __init__(self, map, *args, **kwargs):
        super().__init__(map, *args, **kwargs)
        self.regex = args[0] if args else ".*"


_routing_cache: dict[str, Map] = {}


def _get_routing_table(service: str, region: str = "us-east-1") -> Map:
    """Build and cache a Werkzeug URL Map from a Moto backend's flask_paths."""
    cache_key = f"{service}:{region}"
    if cache_key in _routing_cache:
        return _routing_cache[cache_key]

    backend_dict = moto_backends.get_backend(service)
    if isinstance(backend_dict, BackendDict):
        # BackendDict auto-creates backends on access
        try:
            backend = backend_dict[DEFAULT_ACCOUNT_ID][region]
        except (KeyError, TypeError):
            backend = backend_dict[DEFAULT_ACCOUNT_ID]["us-east-1"]
    else:
        try:
            backend = backend_dict[region]
        except (KeyError, TypeError):
            backend = backend_dict["global"]

    url_map = Map()
    url_map.converters["regex"] = _RegexConverter

    for url_path, handler in backend.flask_paths.items():
        if url_path in ("", "/"):
            url_map.add(Rule("/", endpoint=handler, strict_slashes=False))
            continue
        if url_path in ("/.*", "/.+"):
            url_map.add(Rule("/<path:__catch_all>", endpoint=handler, strict_slashes=False))
            url_map.add(Rule("/", endpoint=handler, strict_slashes=False))
            continue
        url_map.add(Rule(url_path, endpoint=handler, strict_slashes=False))

    _routing_cache[service] = url_map
    return url_map


def _extract_service(request: Request) -> str:
    """Extract the AWS service name from the request."""
    # 1. X-Amz-Target header
    target = request.headers.get("x-amz-target", "")
    if target:
        prefix = target.split(".")[0]
        # Map known prefixes
        prefix_map = {
            "DynamoDB": "dynamodb",
            "DynamoDBStreams": "dynamodbstreams",
            "SecretsManager": "secretsmanager",
            "secretsmanager": "secretsmanager",
            "AmazonSSM": "ssm",
            "TrentService": "kms",
            "Kinesis_20131202": "kinesis",
            "Firehose_20150804": "firehose",
            "Logs_20140328": "logs",
            "AWSEvents": "events",
            "CloudWatchEvents": "events",
            "monitoring": "cloudwatch",
            "AmazonEC2ContainerRegistry": "ecr",
            "AmazonEC2ContainerServiceV20141113": "ecs",
            "AWSStepFunctions": "stepfunctions",
            "CertificateManager": "acm",
            "RekognitionService": "rekognition",
            "StarlingDoveService": "config",
            "OvertureService": "support",
            "AWSSupport": "support",
        }
        if prefix in prefix_map:
            return prefix_map[prefix]
        # Fallback: use the prefix as-is (lowercase)
        return prefix.lower()

    # 2. Authorization header (credential scope)
    # Format: AWS4-HMAC-SHA256 Credential=KEY/DATE/REGION/SERVICE/aws4_request
    auth = request.headers.get("authorization", "")
    if "Credential=" in auth:
        try:
            cred_part = auth.split("Credential=")[1].split(",")[0]
            # Format: KEY/DATE/REGION/SERVICE/aws4_request
            parts = cred_part.strip().split("/")
            if len(parts) >= 4:
                return parts[3]
        except (IndexError, AttributeError):
            pass

    # 3. Host header
    host = request.headers.get("host", "")
    if ".amazonaws.com" in host or ".amazonaws.com.cn" in host:
        service = host.split(".")[0]
        if service and service != "s3":
            return service

    # 4. Path-based detection
    path = request.url.path
    if path.startswith("/2015-03-31/"):
        return "lambda"
    if path.startswith("/2018-01-01/"):
        return "lambda"
    if path.startswith("/2016-11-15/"):
        return "apigatewayv2"

    # 5. Use the path component as service name (e.g., /rds, /ec2)
    # This is how the Rust proxy sends requests
    parts = path.strip("/").split("/")
    if parts and parts[0]:
        return parts[0]

    # 5. Body-based (Action= parameter for query protocol)
    try:
        body = (
            request.url.query.decode()
            if hasattr(request.url.query, "decode")
            else str(request.url.query)
        )
        if "Action=" in body:
            pass  # need auth header to map action to service
    except Exception:
        pass

    return ""


async def moto_handler(request: Request) -> Response:
    """Main handler - dispatch to Moto backend."""
    body = await request.body()

    # Get the service from the X-Robotocore-Service header,
    # the path component, or extract it from the request
    service = request.headers.get("x-robotocore-service", "")
    if not service:
        parts = request.url.path.strip("/").split("/")
        if parts and parts[0]:
            service = parts[0]
    if not service:
        service = _extract_service(request)

    # Map service names to Moto backend names
    service_map = {
        "amazonsqs": "sqs",
        "s3": "s3",
        "ec2": "ec2",
        "rds": "rds",
        "cloudformation": "cloudformation",
        "lambda": "lambda",
        "dynamodb": "dynamodb",
        "sts": "sts",
        "iam": "iam",
        "sns": "sns",
        "kms": "kms",
        "ssm": "ssm",
        "logs": "logs",
        "events": "events",
        "kinesis": "kinesis",
        "firehose": "firehose",
        "secretsmanager": "secretsmanager",
        "stepfunctions": "stepfunctions",
        "cloudwatch": "cloudwatch",
    }
    service = service_map.get(service, service)

    if not service:
        return Response(
            content=json.dumps({
                "__type": "UnknownService",
                "message": "Could not determine service",
            }),
            status_code=400,
            media_type="application/json",
        )

    # Extract region from auth header
    region = "us-east-1"
    auth = request.headers.get("authorization", "")
    if "Credential=" in auth:
        try:
            cred_part = auth.split("Credential=")[1].split(",")[0]
            parts = cred_part.strip().split("/")
            if len(parts) >= 3:
                region = parts[2]
        except (IndexError, AttributeError):
            pass

    # Build the full URL
    full_url = str(request.url)

    # Find the Moto backend and dispatcher
    try:
        routing_map = _get_routing_table(service, region)
    except Exception as e:
        return Response(
            content=json.dumps({
                "__type": "ServiceNotAvailable",
                "message": f"Moto backend not found for {service}: {e}",
            }),
            status_code=501,
            media_type="application/json",
        )

    # Build the Werkzeug environ
    method = request.method
    path = request.url.path
    query = str(request.url.query)

    environ = EnvironBuilder(
        path=f"{path}?{query}" if query else path,
        method=method,
        data=body,
        headers=dict(request.headers),
    ).get_environ()

    werkzeug_request = WerkzeugRawBodyRequest(environ, body)

    # Route to the correct Moto handler
    try:
        binding = routing_map.bind_to_environ(environ)
        endpoint, _args = binding.match()
    except Exception:
        return Response(
            content=json.dumps({
                "__type": "NotFound",
                "message": f"No Moto handler for {service} {method} {path}",
            }),
            status_code=404,
            media_type="application/json",
        )

    # Call the Moto dispatcher with (request, full_url, headers)
    try:
        result = endpoint(werkzeug_request, full_url, werkzeug_request.headers)
        if not result:
            return Response(
                content=json.dumps({
                    "__type": "NotImplemented",
                    "message": f"Operation not implemented for {service}",
                }),
                status_code=501,
                media_type="application/json",
            )
        status_code, resp_headers, resp_body = result
        if isinstance(resp_body, (str, bytes)) and len(resp_body) == 0:
            resp_body = None

        headers_dict = {}
        if resp_headers:
            for k, v in resp_headers.items():
                if k.lower() not in ("content-length",):
                    headers_dict[k] = v

        content_type = headers_dict.get(
            "Content-Type", "application/xml"
        )
        if isinstance(resp_body, bytes):
            resp_body = resp_body.decode("utf-8", errors="replace")

        return Response(
            content=resp_body or "",
            status_code=status_code,
            media_type=content_type,
            headers=headers_dict,
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        return Response(
            content=json.dumps({
                "__type": "InternalError",
                "message": str(e),
            }),
            status_code=500,
            media_type="application/json",
        )



async def health(request: Request) -> Response:
    """Health check endpoint."""
    services = sorted(moto_backends.backends.keys()) if hasattr(moto_backends, "backends") else []
    return Response(
        content=json.dumps(
            {
                "status": "ok",
                "service": "moto-sidecar",
                "moto_version": __import__("moto").__version__,
                "services": len(services),
            }
        ),
        media_type="application/json",
    )


async def echo_debug(request: Request) -> Response:
    """Debug: dump exactly what the sidecar receives."""
    body = await request.body()
    info = {
        "path": request.url.path,
        "query": str(request.url.query),
        "method": request.method,
        "service_header": request.headers.get("x-robotocore-service"),
        "content_type": request.headers.get("content-type"),
        "authorization": request.headers.get("authorization"),
        "x_amz_target": request.headers.get("x-amz-target"),
        "body_len": len(body),
        "body_preview": body[:300].decode("utf-8", "replace"),
    }
    return Response(content=json.dumps(info), media_type="application/json")


def create_app() -> Starlette:
    """Create the Starlette ASGI application."""
    all_methods = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]
    return Starlette(
        routes=[
            Route("/_sidecar/health", health, methods=["GET"]),
            Route("/_sidecar/echo", echo_debug, methods=["GET", "POST"]),
            Route("/{service:path}", moto_handler, methods=all_methods),
        ],
    )


def main():
    parser = argparse.ArgumentParser(description="Moto bridge sidecar")
    parser.add_argument("--port", type=int, default=4567)
    parser.add_argument("--host", type=str, default="127.0.0.1")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    logger.info(f"Starting Moto sidecar on {args.host}:{args.port}")

    app = create_app()
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
