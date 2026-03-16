"""S3 virtual-hosted-style routing.

Parses Host headers to detect S3 virtual-hosted-style requests:
- ``mybucket.s3.localhost.robotocore.cloud`` -> bucket=mybucket
- ``mybucket.s3.us-east-1.amazonaws.com`` -> bucket=mybucket, region=us-east-1
- ``mybucket.s3.amazonaws.com`` -> bucket=mybucket

Also accepts ``mybucket.s3.localhost.localstack.cloud`` as a backwards-compatible alias.

Rewrites the ASGI scope so that downstream handlers see a path-style request.
"""

import os
import re
import threading

# Default hostname bases for S3 virtual-hosted-style requests
DEFAULT_S3_HOSTNAME = "s3.localhost.robotocore.cloud"

# Backwards-compatible alias for localstack.cloud hostnames
S3_LOCALSTACK_HOSTNAME = "s3.localhost.localstack.cloud"

# Patterns for virtual-hosted-style S3 requests
# mybucket.s3.localhost.robotocore.cloud
# mybucket.s3.us-east-1.amazonaws.com
# mybucket.s3.amazonaws.com
_VHOST_RE = re.compile(
    r"^(?P<bucket>[a-zA-Z0-9][a-zA-Z0-9.\-]{1,61}[a-zA-Z0-9])"
    r"\.s3(?:\.(?P<region>[a-z]{2}-[a-z]+-\d+))?"
    r"\.(?P<rest>.+?)(?::\d+)?$"
)

# Cached (pattern, hostname_base) tuple for configurable hostname: <bucket>.s3.<hostname_base>
_VHOST_CUSTOM_CACHE: tuple[re.Pattern, str] | None = None
_VHOST_CACHE_LOCK = threading.Lock()

# Pre-compiled pattern for the localstack.cloud backwards-compatible alias
_VHOST_LOCALSTACK_RE = re.compile(
    r"^(?P<bucket>[a-zA-Z0-9][a-zA-Z0-9.\-]{1,61}[a-zA-Z0-9])"
    rf"\.{re.escape(S3_LOCALSTACK_HOSTNAME)}(?::\d+)?$"
)


def _get_s3_hostname() -> str:
    """Return the configured S3 hostname base."""
    return os.environ.get("S3_HOSTNAME", DEFAULT_S3_HOSTNAME)


def _get_custom_pattern() -> tuple[re.Pattern, str]:
    """Build and cache the regex for the custom hostname."""
    global _VHOST_CUSTOM_CACHE
    base = _get_s3_hostname()
    if _VHOST_CUSTOM_CACHE is None or _VHOST_CUSTOM_CACHE[1] != base:
        with _VHOST_CACHE_LOCK:
            if _VHOST_CUSTOM_CACHE is None or _VHOST_CUSTOM_CACHE[1] != base:
                escaped = re.escape(base)
                pattern = re.compile(
                    r"^(?P<bucket>[a-zA-Z0-9][a-zA-Z0-9.\-]{1,61}[a-zA-Z0-9])"
                    rf"\.{escaped}(?::\d+)?$"
                )
                _VHOST_CUSTOM_CACHE = (pattern, base)
    return _VHOST_CUSTOM_CACHE


def parse_s3_vhost(host: str) -> dict | None:
    """Parse an S3 virtual-hosted-style Host header.

    Returns a dict with keys ``bucket`` and optionally ``region``,
    or ``None`` if the host does not match any S3 pattern.
    """
    if not host:
        return None

    # Strip port if present for matching (but keep original for comparison)
    host_no_port = host.rsplit(":", 1)[0] if ":" in host else host

    # Check custom hostname pattern first (most specific)
    custom_re, base = _get_custom_pattern()
    m = custom_re.match(host)
    if m:
        return {"bucket": m.group("bucket")}

    # Check localstack.cloud backwards-compatible alias
    m = _VHOST_LOCALSTACK_RE.match(host)
    if m:
        return {"bucket": m.group("bucket")}

    # Check standard AWS patterns
    m = _VHOST_RE.match(host)
    if m:
        result: dict = {"bucket": m.group("bucket")}
        if m.group("region"):
            result["region"] = m.group("region")
        else:
            # Try to extract region from the rest part (e.g., dualstack.us-east-1.amazonaws.com)
            rest = m.group("rest")
            region_match = re.search(r"(?:^|\.)((?:us|eu|ap|sa|ca|me|af|il)-[a-z]+-\d+)", rest)
            if region_match:
                result["region"] = region_match.group(1)
        return result

    # Check bare s3 pattern: <bucket>.s3.<anything>
    # This catches cases like mybucket.s3.dualstack.us-east-1.amazonaws.com
    if ".s3." in host_no_port:
        parts = host_no_port.split(".s3.", 1)
        if parts[0] and not parts[0].startswith("."):
            bucket = parts[0]
            # Try to extract region from the remainder
            remainder = parts[1]
            region_match = re.search(r"(?:^|\.)(us|eu|ap|sa|ca|me|af|il)(-[a-z]+-\d+)", remainder)
            result = {"bucket": bucket}
            if region_match:
                result["region"] = region_match.group(1) + region_match.group(2)
            return result

    # S3 Express directory buckets: boto3 uses {bucket}.localhost:{port} when the
    # bucket name ends with --x-s3 (e.g. mybucket--use1-az1--x-s3.localhost:4566).
    # Similarly, S3 Object Lambda uses {route-token}.localhost:{port} for
    # WriteGetObjectResponse. Detect both by checking if the bucket portion of the
    # host is the entire left-most label before .localhost (no .s3. separator).
    # Exclude AWS account IDs (12-digit numeric strings) — S3 Control sends
    # requests to {AccountId}.localhost:{port} which is NOT a bucket vhost.
    if host_no_port.endswith(".localhost") or ".localhost:" in host:
        label = host_no_port.split(".localhost")[0]
        if label and "." not in label and not (label.isdigit() and len(label) == 12):
            return {"bucket": label}

    return None


def is_s3_vhost_request(scope: dict) -> bool:
    """Check if an ASGI scope represents an S3 virtual-hosted-style request."""
    if scope.get("type") != "http":
        return False
    host = b""
    for key, val in scope.get("headers", []):
        if key == b"host":
            host = val
            break
    if not host:
        return False
    return parse_s3_vhost(host.decode("latin-1")) is not None


def rewrite_vhost_to_path(scope: dict) -> dict | None:
    """Rewrite a virtual-hosted-style S3 request scope to path-style.

    Returns a new scope dict with the path rewritten to include the bucket,
    or ``None`` if the Host header does not match.

    The query string is preserved. The Host header is rewritten to strip the
    bucket-name prefix so that Moto sees a clean path-style request (not a
    virtual-hosted one) and does not double-count the bucket.
    """
    host = b""
    for key, val in scope.get("headers", []):
        if key == b"host":
            host = val
            break
    if not host:
        return None

    parsed = parse_s3_vhost(host.decode("latin-1"))
    if parsed is None:
        return None

    bucket = parsed["bucket"]
    original_path = scope.get("path", "/")

    # Rewrite path: /key -> /bucket/key, / -> /bucket
    if original_path == "/":
        new_path = f"/{bucket}"
    else:
        new_path = f"/{bucket}{original_path}"

    # Strip the bucket prefix from the Host header so the downstream Moto bridge
    # receives a path-style request.  For example:
    #   mybucket--x-s3.localhost:4566 -> localhost:4566
    #   mybucket.s3.localhost.robotocore.cloud -> s3.localhost.robotocore.cloud
    host_str = host.decode("latin-1")
    new_host = host_str[len(bucket) + 1 :]  # strip "bucket."
    new_headers = [
        (b"host", new_host.encode("latin-1")) if k == b"host" else (k, v)
        for k, v in scope.get("headers", [])
    ]

    new_scope = dict(scope)
    new_scope["path"] = new_path
    new_scope["headers"] = new_headers
    # raw_path should match
    qs = scope.get("query_string", b"")
    if qs:
        new_scope["raw_path"] = new_path.encode("utf-8") + b"?" + qs
    else:
        new_scope["raw_path"] = new_path.encode("utf-8")

    return new_scope


def get_s3_routing_config() -> dict:
    """Return the current S3 routing configuration as a JSON-serializable dict."""
    return {
        "s3_hostname": _get_s3_hostname(),
        "virtual_hosted_style": True,
        "website_hostname": f"s3-website.{_get_s3_hostname()}",
        "supported_patterns": [
            "<bucket>.s3.<hostname>",
            "<bucket>.s3.<region>.amazonaws.com",
            "<bucket>.s3.amazonaws.com",
        ],
    }
