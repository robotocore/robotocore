"""Shared fixtures for integration tests.

Integration tests run against the ASGI app directly via httpx.AsyncClient,
requiring no running server process. For boto3 clients, a lightweight uvicorn
server is started on an ephemeral port in a background thread.
"""

import io
import socket
import threading
import time
import zipfile
from typing import Any

import boto3
import httpx
import pytest
import uvicorn
from botocore.config import Config

from robotocore.gateway.app import app

BASE_URL = "http://localhost:4566"


@pytest.fixture
def asgi_app():
    """Return the Starlette ASGI application."""
    return app


@pytest.fixture
async def client(asgi_app):
    """Async httpx client using ASGI transport -- no server needed."""
    transport = httpx.ASGITransport(app=asgi_app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url=BASE_URL,
    ) as c:
        yield c


def _find_free_port() -> int:
    """Find a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def _server_url():
    """Start a uvicorn server on an ephemeral port in a background thread.

    Shared across all tests in a module for speed.
    """
    port = _find_free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    # Wait for server to be ready
    url = f"http://127.0.0.1:{port}"
    for _ in range(50):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                break
        except OSError:
            time.sleep(0.1)

    yield url

    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture
def make_boto_client(_server_url):
    """Factory that creates boto3 clients pointed at the running test server."""

    def _factory(service_name: str, region_name: str = "us-east-1", **kwargs):
        config_kwargs: dict[str, Any] = {}
        if service_name == "s3":
            config_kwargs["s3"] = {"addressing_style": "path"}

        return boto3.client(
            service_name,
            endpoint_url=_server_url,
            region_name=region_name,
            aws_access_key_id=kwargs.pop("aws_access_key_id", "testing"),
            aws_secret_access_key=kwargs.pop("aws_secret_access_key", "testing"),
            config=Config(**config_kwargs),
            **kwargs,
        )

    return _factory


def make_lambda_zip(code: str) -> bytes:
    """Create a zip file containing a single Python Lambda handler."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


def make_lambda_role_arn() -> str:
    """Return a well-known IAM role ARN for Lambda tests."""
    return "arn:aws:iam::123456789012:role/integration-test-lambda-role"


def auth_header(service: str, region: str = "us-east-1") -> dict[str, str]:
    """Build a minimal AWS SigV4 Authorization header for test requests."""
    return {
        "Authorization": (
            "AWS4-HMAC-SHA256 "
            f"Credential=testing/20260306/{region}/{service}/aws4_request, "
            "SignedHeaders=host, Signature=abc"
        ),
    }


def json_headers(service: str, region: str = "us-east-1") -> dict[str, str]:
    """Headers for JSON-protocol AWS services."""
    return {
        **auth_header(service, region),
        "Content-Type": "application/x-amz-json-1.0",
    }
