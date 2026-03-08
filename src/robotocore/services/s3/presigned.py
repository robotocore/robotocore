"""Presigned URL generation and validation for S3.

Supports both SigV2 and SigV4 presigned URLs:
- SigV4: X-Amz-Algorithm, X-Amz-Credential, X-Amz-Date, X-Amz-Expires,
         X-Amz-SignedHeaders, X-Amz-Signature
- SigV2: AWSAccessKeyId, Signature, Expires
"""

import calendar
import time
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

from starlette.datastructures import QueryParams


@dataclass
class PresignedUrlInfo:
    """Parsed presigned URL information."""

    version: str  # "v2" or "v4"
    bucket: str
    key: str
    expires: int  # expiration timestamp (v2) or seconds-from-sign (v4)
    signature: str
    credential: str  # access key (v2) or full credential scope (v4)
    signed_headers: str  # v4 only
    date: str  # v4 only (X-Amz-Date)
    security_token: str | None
    is_expired: bool


def parse_presigned_url(url: str) -> PresignedUrlInfo | None:
    """Parse a presigned URL and extract signing information.

    Returns None if the URL is not a presigned URL.
    """
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)

    bucket = ""
    key = ""
    path_parts = parsed.path.lstrip("/").split("/", 1)
    if path_parts:
        bucket = path_parts[0]
        key = path_parts[1] if len(path_parts) > 1 else ""

    if "X-Amz-Signature" in params:
        return _parse_sigv4(params, bucket, key)
    elif "Signature" in params:
        return _parse_sigv2(params, bucket, key)
    return None


def is_presigned_request(query_params: QueryParams) -> bool:
    """Check if query parameters indicate a presigned URL request."""
    return "X-Amz-Signature" in query_params or "Signature" in query_params


def validate_presigned_url(info: PresignedUrlInfo) -> bool:
    """Validate a presigned URL (check expiration).

    In a real implementation this would also verify the signature, but
    for local emulation we only check expiration to match LocalStack
    behavior.
    """
    if info.is_expired:
        return False
    return True


def _parse_sigv4(params: dict[str, list[str]], bucket: str, key: str) -> PresignedUrlInfo:
    """Parse SigV4 presigned URL parameters."""
    signature = _first(params, "X-Amz-Signature")
    credential = _first(params, "X-Amz-Credential")
    date_str = _first(params, "X-Amz-Date")
    expires_str = _first(params, "X-Amz-Expires")
    signed_headers = _first(params, "X-Amz-SignedHeaders")
    security_token = _first(params, "X-Amz-Security-Token") or None

    try:
        expires_seconds = int(expires_str) if expires_str else 3600
    except ValueError:
        expires_seconds = 3600

    is_expired = _check_sigv4_expiration(date_str, expires_seconds)

    return PresignedUrlInfo(
        version="v4",
        bucket=bucket,
        key=key,
        expires=expires_seconds,
        signature=signature,
        credential=credential,
        signed_headers=signed_headers,
        date=date_str,
        security_token=security_token,
        is_expired=is_expired,
    )


def _parse_sigv2(params: dict[str, list[str]], bucket: str, key: str) -> PresignedUrlInfo:
    """Parse SigV2 presigned URL parameters."""
    signature = _first(params, "Signature")
    access_key = _first(params, "AWSAccessKeyId")
    expires_str = _first(params, "Expires")
    security_token = _first(params, "x-amz-security-token") or None

    try:
        expires_ts = int(expires_str) if expires_str else 0
    except ValueError:
        expires_ts = 0

    is_expired = expires_ts > 0 and time.time() > expires_ts

    return PresignedUrlInfo(
        version="v2",
        bucket=bucket,
        key=key,
        expires=expires_ts,
        signature=signature,
        credential=access_key,
        signed_headers="",
        date="",
        security_token=security_token,
        is_expired=is_expired,
    )


def _check_sigv4_expiration(date_str: str, expires_seconds: int) -> bool:
    """Check if a SigV4 presigned URL has expired."""
    if not date_str:
        return False
    try:
        # Parse ISO 8601 basic format: 20260101T000000Z
        sign_time = calendar.timegm(time.strptime(date_str, "%Y%m%dT%H%M%SZ"))
        return time.time() > (sign_time + expires_seconds)
    except (ValueError, OverflowError):
        return False


def _first(params: dict[str, list[str]], key: str) -> str:
    """Get the first value for a query parameter key, or empty string."""
    values = params.get(key, [])
    return values[0] if values else ""
