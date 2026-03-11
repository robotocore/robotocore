"""S3 static website hosting.

Handles requests to S3 website endpoints:
- ``mybucket.s3-website.localhost.robotocore.cloud``
- ``mybucket.s3-website-us-east-1.amazonaws.com``

Also accepts ``mybucket.s3-website.localhost.localstack.cloud`` as a backwards-compatible alias.

Serves index documents, error documents, and handles redirect rules
from the bucket's website configuration stored in Moto.
"""

import mimetypes
import os
import re
import xml.etree.ElementTree as ET

from starlette.requests import Request
from starlette.responses import Response

from robotocore.gateway.s3_routing import DEFAULT_S3_HOSTNAME

# Website host patterns
# mybucket.s3-website.localhost.robotocore.cloud
# mybucket.s3-website-us-east-1.amazonaws.com
# mybucket.s3-website.us-east-1.amazonaws.com
_WEBSITE_HOST_RE = re.compile(
    r"^(?P<bucket>[a-zA-Z0-9][a-zA-Z0-9.\-]{1,61}[a-zA-Z0-9])"
    r"\.s3-website[.\-](?P<rest>.+?)(?::\d+)?$"
)

# Ensure common types are registered
mimetypes.init()
# Additional content types not always in the default database
_EXTRA_TYPES = {
    ".json": "application/json",
    ".js": "application/javascript",
    ".mjs": "application/javascript",
    ".css": "text/css",
    ".html": "text/html",
    ".htm": "text/html",
    ".xml": "application/xml",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".ico": "image/x-icon",
    ".webp": "image/webp",
    ".woff": "font/woff",
    ".woff2": "font/woff2",
    ".ttf": "font/ttf",
    ".eot": "application/vnd.ms-fontobject",
    ".otf": "font/otf",
    ".txt": "text/plain",
    ".csv": "text/csv",
    ".pdf": "application/pdf",
    ".zip": "application/zip",
    ".gz": "application/gzip",
    ".tar": "application/x-tar",
    ".mp4": "video/mp4",
    ".webm": "video/webm",
    ".mp3": "audio/mpeg",
    ".wav": "audio/wav",
    ".wasm": "application/wasm",
}


def _get_website_hostname() -> str:
    """Return the configured website hostname base."""
    base = os.environ.get("S3_HOSTNAME", DEFAULT_S3_HOSTNAME)
    return f"s3-website.{base}"


def parse_website_host(host: str) -> dict | None:
    """Parse a website-style Host header.

    Returns a dict with ``bucket`` and optionally ``region``,
    or ``None`` if the host is not a website endpoint.
    """
    if not host:
        return None

    m = _WEBSITE_HOST_RE.match(host)
    if not m:
        return None

    result: dict = {"bucket": m.group("bucket")}
    rest = m.group("rest")

    # Extract region from the rest part
    region_match = re.match(r"(us|eu|ap|sa|ca|me|af|il)(-[a-z]+-\d+)", rest)
    if region_match:
        result["region"] = region_match.group(1) + region_match.group(2)

    return result


def is_website_request(scope: dict) -> bool:
    """Check if an ASGI scope represents an S3 website request."""
    if scope.get("type") != "http":
        return False
    for key, val in scope.get("headers", []):
        if key == b"host":
            return parse_website_host(val.decode("latin-1")) is not None
    return False


def guess_content_type(key: str) -> str:
    """Guess the Content-Type for an S3 object key based on its extension."""
    if not key or key.endswith("/"):
        return "text/html"

    # Get extension
    dot_idx = key.rfind(".")
    if dot_idx == -1:
        return "application/octet-stream"

    ext = key[dot_idx:].lower()

    # Check our extras first
    if ext in _EXTRA_TYPES:
        return _EXTRA_TYPES[ext]

    # Fall back to mimetypes
    guessed, _ = mimetypes.guess_type(f"file{ext}")
    return guessed or "application/octet-stream"


def _get_s3_backend(account_id: str, region: str):
    """Get the Moto S3 backend for the given account/region.

    S3 in Moto is global — the region key is typically 'global' or 'aws',
    not an actual AWS region name. We try the common Moto region keys.
    """
    from moto.backends import get_backend

    account_backends = get_backend("s3")[account_id]
    # Try common Moto S3 region keys
    for key in ("global", "aws", region):
        try:
            return account_backends[key]
        except KeyError:
            continue
    # Last resort: return the first available region
    for key in account_backends:
        return account_backends[key]
    raise KeyError(f"No S3 backend found for account {account_id}")


def _get_bucket(backend, bucket_name: str):
    """Get a bucket from the Moto S3 backend, or None."""
    try:
        return backend.get_bucket(bucket_name)
    except Exception:
        return None


def _get_object_body(backend, bucket_name: str, key: str) -> tuple[bytes, str] | None:
    """Get an object's body and content type from S3.

    Returns (body_bytes, content_type) or None if the object doesn't exist.
    """
    try:
        obj = backend.get_object(bucket_name, key)
        if obj is None:
            return None
        body = obj.value
        # Try to get stored content type
        ct = getattr(obj, "content_type", None)
        if not ct:
            ct = guess_content_type(key)
        return (body, ct)
    except Exception:
        return None


_S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"


def _get_website_config(bucket) -> dict | None:
    """Get the website configuration from a Moto FakeBucket.

    Moto stores website_configuration as raw XML bytes. We parse it into
    a dict with ``IndexDocument``, ``ErrorDocument``, and ``RoutingRules``.
    Returns None if website hosting is not configured.
    """
    wc = getattr(bucket, "website_configuration", None)
    if not wc:
        return None

    # If already a dict (future-proofing), return it
    if isinstance(wc, dict):
        return wc

    # Moto stores as bytes or str XML
    xml_str = wc.decode("utf-8") if isinstance(wc, bytes) else str(wc)
    if not xml_str.strip():
        return None

    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return None

    config: dict = {"IndexDocument": {}, "ErrorDocument": {}, "RoutingRules": []}

    # Parse IndexDocument
    for tag_name in (f"{{{_S3_NS}}}IndexDocument", "IndexDocument"):
        idx_el = root.find(tag_name)
        if idx_el is not None:
            suffix_el = idx_el.find(f"{{{_S3_NS}}}Suffix")
            if suffix_el is None:
                suffix_el = idx_el.find("Suffix")
            if suffix_el is not None and suffix_el.text:
                config["IndexDocument"] = {"Suffix": suffix_el.text}
            break

    # Parse ErrorDocument
    for tag_name in (f"{{{_S3_NS}}}ErrorDocument", "ErrorDocument"):
        err_el = root.find(tag_name)
        if err_el is not None:
            key_el = err_el.find(f"{{{_S3_NS}}}Key")
            if key_el is None:
                key_el = err_el.find("Key")
            if key_el is not None and key_el.text:
                config["ErrorDocument"] = {"Key": key_el.text}
            break

    # Parse RoutingRules
    for tag_name in (f"{{{_S3_NS}}}RoutingRules", "RoutingRules"):
        rules_el = root.find(tag_name)
        if rules_el is not None:
            for rule_el in rules_el.findall(f"{{{_S3_NS}}}RoutingRule") + rules_el.findall(
                "RoutingRule"
            ):
                rule = _parse_routing_rule(rule_el)
                config["RoutingRules"].append(rule)
            break

    return config


def _parse_routing_rule(rule_el: ET.Element) -> dict:
    """Parse a single RoutingRule XML element into a dict."""
    rule: dict = {"Condition": {}, "Redirect": {}}

    for tag_name in (f"{{{_S3_NS}}}Condition", "Condition"):
        cond_el = rule_el.find(tag_name)
        if cond_el is not None:
            for child in cond_el:
                ctag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if child.text:
                    rule["Condition"][ctag] = child.text
            break

    for tag_name in (f"{{{_S3_NS}}}Redirect", "Redirect"):
        redir_el = rule_el.find(tag_name)
        if redir_el is not None:
            for child in redir_el:
                ctag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if child.text:
                    rule["Redirect"][ctag] = child.text
            break

    return rule


def _s3_xml_error(code: str, message: str, status_code: int = 404) -> Response:
    """Return a standard S3 XML error response."""
    body = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<Error>"
        f"<Code>{code}</Code>"
        f"<Message>{message}</Message>"
        "</Error>"
    )
    return Response(content=body, status_code=status_code, media_type="application/xml")


def _check_redirect_rules(
    rules: list[dict], key: str, http_error_code: int | None = None
) -> Response | None:
    """Check redirect rules and return a redirect Response if any match.

    Rules can match on:
    - KeyPrefixEquals: the requested key starts with this prefix
    - HttpErrorCodeReturnedEquals: the HTTP error code matches
    """
    for rule in rules:
        condition = rule.get("Condition", {})
        redirect = rule.get("Redirect", {})

        # Check condition
        prefix_match = True
        error_match = True

        key_prefix = condition.get("KeyPrefixEquals")
        if key_prefix is not None:
            prefix_match = key.startswith(key_prefix)

        error_code = condition.get("HttpErrorCodeReturnedEquals")
        if error_code is not None:
            error_match = http_error_code == int(error_code)

        if not (prefix_match and error_match):
            continue

        # Build redirect URL
        protocol = redirect.get("Protocol", "")
        hostname = redirect.get("HostName", "")
        replace_prefix = redirect.get("ReplaceKeyPrefixWith")
        replace_key = redirect.get("ReplaceKeyWith")
        status = int(redirect.get("HttpRedirectCode", 301))

        if replace_key is not None:
            new_key = replace_key
        elif replace_prefix is not None and key_prefix is not None:
            new_key = replace_prefix + key[len(key_prefix) :]
        else:
            new_key = key

        if hostname:
            if protocol:
                location = f"{protocol}://{hostname}/{new_key}"
            else:
                location = f"http://{hostname}/{new_key}"
        else:
            location = f"/{new_key}"

        return Response(
            status_code=status,
            headers={"Location": location},
        )

    return None


async def handle_website_request(
    request: Request,
    bucket_name: str,
    region: str = "us-east-1",
    account_id: str = "123456789012",
) -> Response:
    """Handle a request to an S3 website endpoint.

    Serves index documents, error documents, and applies redirect rules.
    """
    backend = _get_s3_backend(account_id, region)
    bucket = _get_bucket(backend, bucket_name)

    if bucket is None:
        return _s3_xml_error("NoSuchBucket", f"The specified bucket does not exist: {bucket_name}")

    website_config = _get_website_config(bucket)
    if website_config is None:
        return _s3_xml_error(
            "NoSuchWebsiteConfiguration",
            "The specified bucket does not have a website configuration",
            status_code=404,
        )

    path = request.url.path
    # Normalize: strip leading slash
    key = path.lstrip("/")

    # Get config elements
    index_doc = website_config.get("IndexDocument", {})
    error_doc = website_config.get("ErrorDocument", {})
    routing_rules = website_config.get("RoutingRules", [])

    # Get index suffix (usually "index.html")
    index_suffix = ""
    if isinstance(index_doc, dict):
        index_suffix = index_doc.get("Suffix", "index.html")
    elif isinstance(index_doc, str):
        index_suffix = index_doc

    # Check redirect rules BEFORE serving content (prefix-based redirects)
    redirect = _check_redirect_rules(routing_rules, key)
    if redirect is not None:
        return redirect

    # Determine the object key to serve
    if not key or key.endswith("/"):
        # Directory-like path: serve index document
        target_key = f"{key}{index_suffix}"
    else:
        target_key = key

    # Try to fetch the object
    obj = _get_object_body(backend, bucket_name, target_key)
    if obj is not None:
        body, content_type = obj
        return Response(
            content=body,
            status_code=200,
            media_type=content_type,
        )

    # Object not found — check error-condition redirect rules
    redirect = _check_redirect_rules(routing_rules, key, http_error_code=404)
    if redirect is not None:
        return redirect

    # Serve error document if configured
    error_key = ""
    if isinstance(error_doc, dict):
        error_key = error_doc.get("Key", "")
    elif isinstance(error_doc, str):
        error_key = error_doc

    if error_key:
        error_obj = _get_object_body(backend, bucket_name, error_key)
        if error_obj is not None:
            body, content_type = error_obj
            return Response(
                content=body,
                status_code=404,
                media_type=content_type,
            )

    # Default 404
    return _s3_xml_error("NoSuchKey", f"The specified key does not exist: {target_key}")
