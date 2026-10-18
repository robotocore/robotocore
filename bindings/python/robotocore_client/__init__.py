"""robotocore client bindings (Python).

Thin wrapper that aims an arbitrary boto3/botocore client at a running
robotocore server. Language-agnostic behavior lives server-side; this
package only wires credentials/endpoints. Sits alongside the TypeScript,
Go, and Rust bindings under `bindings/`.

Usage:
    from robotocore_client import session
    client = session().client("s3")  # talks to http://localhost:4566
"""

from __future__ import annotations

import os

DEFAULT_ENDPOINT = "http://127.0.0.1:4566"
DEFAULT_ACCOUNT = "123456789012"


def endpoint_url(endpoint: str | None = None) -> str:
    endpoint = endpoint or os.environ.get("ROBOTOCORE_ENDPOINT") or DEFAULT_ENDPOINT
    return endpoint.rstrip("/")


def credentials(account: str | None = None) -> tuple[str, str]:
    """(access_key, secret) pair; a 12-digit access key IS the account id."""
    key = (account or os.environ.get("ROBOTOCORE_ACCOUNT") or DEFAULT_ACCOUNT).zfill(12)
    return key, "test"


def session(account: str | None = None, endpoint: str | None = None, region: str = "us-east-1"):
    """A boto3 Session configured for a robotocore server."""
    import boto3  # import lazily so the runtime server needn't exist at import

    key, secret = credentials(account)
    return boto3.Session(
        endpoint_url=endpoint_url(endpoint),
        region_name=region,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
    )


def client(service: str, **kwargs):
    return session(**kwargs).client(service, **kwargs)
