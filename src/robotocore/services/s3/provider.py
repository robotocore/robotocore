"""Enhanced S3 provider — wraps Moto's S3 with event notifications, CORS,
versioning, lifecycle, object lock, multipart support, and presigned URLs."""

import logging
import re
import threading
import xml.etree.ElementTree as ET
from urllib.parse import urlencode

from starlette.datastructures import QueryParams
from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto
from robotocore.services.s3.notifications import (
    NotificationConfig,
    fire_event,
    get_notification_config,
    set_notification_config,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Monkey-patch Moto's FakeBucket.get_permission to handle policy as str or bytes.
# Moto assumes self.policy is always bytes, but put_bucket_policy passes self.body
# which is a str. This causes "'str' object has no attribute 'decode'" on PutObject
# when a bucket policy is set.
# ---------------------------------------------------------------------------
try:
    from moto.s3.models import FakeBucket

    _orig_get_permission = FakeBucket.get_permission

    def _patched_get_permission(self, action, resource):  # type: ignore[no-untyped-def]
        if self.policy is not None and isinstance(self.policy, str):
            self.policy = self.policy.encode()
        return _orig_get_permission(self, action, resource)

    FakeBucket.get_permission = _patched_get_permission  # type: ignore[assignment]
except Exception as exc:
    logger.debug("<module>: _orig_get_permission failed (non-fatal): %s", exc)

# Patterns to detect bucket and key from S3 paths
# Path style: /<bucket>/<key>
_PATH_RE = re.compile(r"^/([^/]+)(?:/(.+))?$")

# SigV4 presigned URL query parameters
_SIGV4_PARAMS = {
    "X-Amz-Algorithm",
    "X-Amz-Credential",
    "X-Amz-Date",
    "X-Amz-Expires",
    "X-Amz-SignedHeaders",
    "X-Amz-Signature",
    "X-Amz-Security-Token",
}

# SigV2 presigned URL query parameters
_SIGV2_PARAMS = {
    "AWSAccessKeyId",
    "Signature",
    "Expires",
}

# All signature-related parameters to strip
_ALL_SIG_PARAMS = _SIGV4_PARAMS | _SIGV2_PARAMS

# ---------------------------------------------------------------------------
# In-memory stores for CORS, lifecycle, and object lock
# ---------------------------------------------------------------------------
_cors_store: dict[str, list[dict]] = {}
_lifecycle_store: dict[str, list[dict]] = {}
_object_lock_store: dict[str, dict] = {}
_object_legal_hold_store: dict[str, dict[str, str]] = {}
_logging_store: dict[str, dict] = {}
# Directory bucket metadata (bucket_name -> metadata dict)
_directory_bucket_store: dict[str, dict] = {}
_store_lock = threading.Lock()

S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"


# ---------------------------------------------------------------------------
# Presigned URL helpers
# ---------------------------------------------------------------------------


def _is_presigned_url(query_params: QueryParams) -> bool:
    """Check if the request is a presigned URL request."""
    return "X-Amz-Signature" in query_params or "Signature" in query_params


def _strip_presigned_params(request: Request, body: bytes | None = None) -> Request:
    """Return a modified request with presigned URL params stripped.

    Converts X-Amz-Security-Token query param into a header (Moto expects it
    there) and removes all signature-related query params so Moto sees a clean
    request.
    """
    scope = dict(request.scope)

    # Collect non-signature query params
    clean_params = []
    security_token = None
    for key, value in request.query_params.multi_items():
        if key in _ALL_SIG_PARAMS:
            if key == "X-Amz-Security-Token":
                security_token = value
            continue
        clean_params.append((key, value))

    # Rebuild query string
    new_qs = urlencode(clean_params).encode("utf-8") if clean_params else b""
    scope["query_string"] = new_qs

    # If there was a security token, inject it as a header
    if security_token:
        headers = list(scope.get("headers", []))
        headers.append((b"x-amz-security-token", security_token.encode("utf-8")))
        scope["headers"] = headers

    # Inject a fake Authorization header so Moto can extract region/credentials
    if not request.headers.get("authorization"):
        credential = request.query_params.get("X-Amz-Credential", "")
        if credential:
            signed_headers = request.query_params.get("X-Amz-SignedHeaders", "host")
            auth_value = (
                f"AWS4-HMAC-SHA256 Credential={credential}, "
                f"SignedHeaders={signed_headers}, "
                f"Signature=presigned-placeholder"
            )
        else:
            access_key = request.query_params.get("AWSAccessKeyId", "testing")
            auth_value = (
                f"AWS4-HMAC-SHA256 "
                f"Credential={access_key}/20260101/us-east-1/s3/aws4_request, "
                f"SignedHeaders=host, "
                f"Signature=presigned-placeholder"
            )

        headers = list(scope.get("headers", []))
        headers.append((b"authorization", auth_value.encode("utf-8")))
        scope["headers"] = headers

    # Ensure Content-Type header exists for PUT/POST
    method = scope.get("method", "GET").upper()
    if method in ("PUT", "POST"):
        has_ct = any(k == b"content-type" for k, v in scope.get("headers", []))
        if not has_ct:
            headers = list(scope.get("headers", []))
            headers.append((b"content-type", b"application/octet-stream"))
            scope["headers"] = headers

    if body is not None:
        new_req = Request(scope, request.receive)
        new_req._body = body
        return new_req

    return Request(scope, request.receive)


# ---------------------------------------------------------------------------
# CORS helpers
# ---------------------------------------------------------------------------


def set_bucket_cors(bucket: str, rules: list[dict]) -> None:
    with _store_lock:
        _cors_store[bucket] = rules


def get_bucket_cors(bucket: str) -> list[dict] | None:
    with _store_lock:
        return _cors_store.get(bucket)


def delete_bucket_cors(bucket: str) -> None:
    with _store_lock:
        _cors_store.pop(bucket, None)


def _handle_cors_preflight(bucket: str, request: Request) -> Response | None:
    """Handle OPTIONS request by checking CORS rules."""
    rules = get_bucket_cors(bucket)
    if not rules:
        return Response(status_code=403, content="CORSNotConfigured")

    origin = request.headers.get("origin", "")
    req_method = request.headers.get("access-control-request-method", "")

    for rule in rules:
        allowed_origins = rule.get("AllowedOrigins", [])
        allowed_methods = rule.get("AllowedMethods", [])
        allowed_headers = rule.get("AllowedHeaders", [])
        expose_headers = rule.get("ExposeHeaders", [])
        max_age = rule.get("MaxAgeSeconds")

        if not _origin_matches(origin, allowed_origins):
            continue
        if req_method and req_method not in allowed_methods:
            continue

        headers = {
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Methods": ", ".join(allowed_methods),
        }
        if allowed_headers:
            headers["Access-Control-Allow-Headers"] = ", ".join(allowed_headers)
        if expose_headers:
            headers["Access-Control-Expose-Headers"] = ", ".join(expose_headers)
        if max_age is not None:
            headers["Access-Control-Max-Age"] = str(max_age)
        return Response(status_code=200, headers=headers)

    return Response(status_code=403, content="CORSNotAllowed")


def _origin_matches(origin: str, allowed_origins: list[str]) -> bool:
    for allowed in allowed_origins:
        if allowed == "*" or allowed == origin:
            return True
    return False


def _parse_cors_xml(xml_str: str) -> list[dict]:
    """Parse CORSConfiguration XML into a list of rule dicts."""
    rules = []
    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return rules

    for cr in root.findall(f"{{{S3_NS}}}CORSRule") + root.findall("CORSRule"):
        rule: dict = {
            "AllowedOrigins": [],
            "AllowedMethods": [],
            "AllowedHeaders": [],
            "ExposeHeaders": [],
        }
        for child in cr:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            text = child.text or ""
            if tag == "AllowedOrigin":
                rule["AllowedOrigins"].append(text)
            elif tag == "AllowedMethod":
                rule["AllowedMethods"].append(text)
            elif tag == "AllowedHeader":
                rule["AllowedHeaders"].append(text)
            elif tag == "ExposeHeader":
                rule["ExposeHeaders"].append(text)
            elif tag == "MaxAgeSeconds":
                try:
                    rule["MaxAgeSeconds"] = int(text)
                except ValueError as exc:
                    logger.debug("_parse_cors_xml: int failed (non-fatal): %s", exc)
        rules.append(rule)
    return rules


def _cors_to_xml(rules: list[dict]) -> str:
    parts = ['<?xml version="1.0" encoding="UTF-8"?>']
    parts.append(f'<CORSConfiguration xmlns="{S3_NS}">')
    for rule in rules:
        parts.append("<CORSRule>")
        for origin in rule.get("AllowedOrigins", []):
            parts.append(f"<AllowedOrigin>{origin}</AllowedOrigin>")
        for method in rule.get("AllowedMethods", []):
            parts.append(f"<AllowedMethod>{method}</AllowedMethod>")
        for header in rule.get("AllowedHeaders", []):
            parts.append(f"<AllowedHeader>{header}</AllowedHeader>")
        for header in rule.get("ExposeHeaders", []):
            parts.append(f"<ExposeHeader>{header}</ExposeHeader>")
        if "MaxAgeSeconds" in rule:
            parts.append(f"<MaxAgeSeconds>{rule['MaxAgeSeconds']}</MaxAgeSeconds>")
        parts.append("</CORSRule>")
    parts.append("</CORSConfiguration>")
    return "".join(parts)


# ---------------------------------------------------------------------------
# Lifecycle helpers
# ---------------------------------------------------------------------------


def set_bucket_lifecycle(bucket: str, rules: list[dict]) -> None:
    with _store_lock:
        _lifecycle_store[bucket] = rules


def get_bucket_lifecycle(bucket: str) -> list[dict] | None:
    with _store_lock:
        return _lifecycle_store.get(bucket)


def delete_bucket_lifecycle(bucket: str) -> None:
    with _store_lock:
        _lifecycle_store.pop(bucket, None)


def _parse_lifecycle_xml(xml_str: str) -> list[dict]:
    """Parse LifecycleConfiguration XML into a list of rule dicts."""
    rules = []
    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return rules

    for rule_el in root.findall(f"{{{S3_NS}}}Rule") + root.findall("Rule"):
        rule: dict = {}
        for child in rule_el:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if tag == "ID":
                rule["ID"] = child.text or ""
            elif tag == "Status":
                rule["Status"] = child.text or ""
            elif tag == "Filter":
                flt: dict = {}
                for fc in child:
                    ftag = fc.tag.split("}")[-1] if "}" in fc.tag else fc.tag
                    if ftag == "Prefix":
                        flt["Prefix"] = fc.text or ""
                    elif ftag == "Tag":
                        tag_dict: dict = {}
                        for tc in fc:
                            ttag = tc.tag.split("}")[-1] if "}" in tc.tag else tc.tag
                            tag_dict[ttag] = tc.text or ""
                        flt.setdefault("Tags", []).append(tag_dict)
                rule["Filter"] = flt
            elif tag == "Prefix":
                rule.setdefault("Filter", {})["Prefix"] = child.text or ""
            elif tag == "Expiration":
                exp: dict = {}
                for ec in child:
                    etag = ec.tag.split("}")[-1] if "}" in ec.tag else ec.tag
                    exp[etag] = ec.text or ""
                rule["Expiration"] = exp
            elif tag == "Transition":
                trans: dict = {}
                for tc_el in child:
                    ttag = tc_el.tag.split("}")[-1] if "}" in tc_el.tag else tc_el.tag
                    trans[ttag] = tc_el.text or ""
                rule.setdefault("Transitions", []).append(trans)
            elif tag == "NoncurrentVersionExpiration":
                nve: dict = {}
                for nc in child:
                    ntag = nc.tag.split("}")[-1] if "}" in nc.tag else nc.tag
                    nve[ntag] = nc.text or ""
                rule["NoncurrentVersionExpiration"] = nve
            elif tag == "AbortIncompleteMultipartUpload":
                aimu: dict = {}
                for ac in child:
                    atag = ac.tag.split("}")[-1] if "}" in ac.tag else ac.tag
                    aimu[atag] = ac.text or ""
                rule["AbortIncompleteMultipartUpload"] = aimu
        rules.append(rule)
    return rules


def _lifecycle_to_xml(rules: list[dict]) -> str:
    parts = ['<?xml version="1.0" encoding="UTF-8"?>']
    parts.append(f'<LifecycleConfiguration xmlns="{S3_NS}">')
    for rule in rules:
        parts.append("<Rule>")
        if "ID" in rule:
            parts.append(f"<ID>{rule['ID']}</ID>")
        if "Status" in rule:
            parts.append(f"<Status>{rule['Status']}</Status>")
        if "Filter" in rule:
            flt = rule["Filter"]
            parts.append("<Filter>")
            if "Prefix" in flt:
                parts.append(f"<Prefix>{flt['Prefix']}</Prefix>")
            for tag in flt.get("Tags", []):
                parts.append("<Tag>")
                for k, v in tag.items():
                    parts.append(f"<{k}>{v}</{k}>")
                parts.append("</Tag>")
            parts.append("</Filter>")
        if "Expiration" in rule:
            parts.append("<Expiration>")
            for k, v in rule["Expiration"].items():
                parts.append(f"<{k}>{v}</{k}>")
            parts.append("</Expiration>")
        for trans in rule.get("Transitions", []):
            parts.append("<Transition>")
            for k, v in trans.items():
                parts.append(f"<{k}>{v}</{k}>")
            parts.append("</Transition>")
        if "NoncurrentVersionExpiration" in rule:
            parts.append("<NoncurrentVersionExpiration>")
            for k, v in rule["NoncurrentVersionExpiration"].items():
                parts.append(f"<{k}>{v}</{k}>")
            parts.append("</NoncurrentVersionExpiration>")
        if "AbortIncompleteMultipartUpload" in rule:
            parts.append("<AbortIncompleteMultipartUpload>")
            for k, v in rule["AbortIncompleteMultipartUpload"].items():
                parts.append(f"<{k}>{v}</{k}>")
            parts.append("</AbortIncompleteMultipartUpload>")
        parts.append("</Rule>")
    parts.append("</LifecycleConfiguration>")
    return "".join(parts)


# ---------------------------------------------------------------------------
# Object lock / legal hold helpers
# ---------------------------------------------------------------------------


def set_object_lock_config(bucket: str, config: dict) -> None:
    with _store_lock:
        _object_lock_store[bucket] = config


def get_object_lock_config(bucket: str) -> dict | None:
    with _store_lock:
        return _object_lock_store.get(bucket)


def set_object_legal_hold(bucket: str, key: str, status: str) -> None:
    with _store_lock:
        _object_legal_hold_store[f"{bucket}/{key}"] = status


def get_object_legal_hold(bucket: str, key: str) -> str | None:
    with _store_lock:
        return _object_legal_hold_store.get(f"{bucket}/{key}")


def _parse_object_lock_xml(xml_str: str) -> dict:
    config: dict = {}
    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return config

    for child in root:
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        if tag == "ObjectLockEnabled":
            config["ObjectLockEnabled"] = child.text or ""
        elif tag == "Rule":
            rule: dict = {}
            for rc in child:
                rtag = rc.tag.split("}")[-1] if "}" in rc.tag else rc.tag
                if rtag == "DefaultRetention":
                    retention: dict = {}
                    for dc in rc:
                        dtag = dc.tag.split("}")[-1] if "}" in dc.tag else dc.tag
                        retention[dtag] = dc.text or ""
                    rule["DefaultRetention"] = retention
            config["Rule"] = rule
    return config


def _object_lock_to_xml(config: dict) -> str:
    parts = ['<?xml version="1.0" encoding="UTF-8"?>']
    parts.append(f'<ObjectLockConfiguration xmlns="{S3_NS}">')
    if "ObjectLockEnabled" in config:
        parts.append(f"<ObjectLockEnabled>{config['ObjectLockEnabled']}</ObjectLockEnabled>")
    if "Rule" in config:
        parts.append("<Rule>")
        if "DefaultRetention" in config["Rule"]:
            parts.append("<DefaultRetention>")
            for k, v in config["Rule"]["DefaultRetention"].items():
                parts.append(f"<{k}>{v}</{k}>")
            parts.append("</DefaultRetention>")
        parts.append("</Rule>")
    parts.append("</ObjectLockConfiguration>")
    return "".join(parts)


def _parse_legal_hold_xml(xml_str: str) -> str:
    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return "OFF"
    for child in root:
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        if tag == "Status":
            return child.text or "OFF"
    return "OFF"


def _legal_hold_to_xml(status: str) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<LegalHold xmlns="{S3_NS}">'
        f"<Status>{status}</Status>"
        f"</LegalHold>"
    )


# ---------------------------------------------------------------------------
# Query-param routing helpers
# ---------------------------------------------------------------------------


def _get_query_param(request: Request) -> str | None:
    """Return the first S3 sub-resource query parameter, or None."""
    qs = str(request.url.query)
    if not qs:
        return None
    for part in qs.split("&"):
        key = part.split("=")[0]
        if key in _S3_SUBRESOURCES:
            return key
    return None


_S3_SUBRESOURCES = {
    "notification",
    "cors",
    "lifecycle",
    "logging",
    "versioning",
    "object-lock",
    "legal-hold",
    "uploads",
    "uploadId",
    "session",
    "renameObject",
}


# ---------------------------------------------------------------------------
# Bucket deletion cleanup
# ---------------------------------------------------------------------------


def _cleanup_bucket_stores(bucket: str) -> None:
    """Remove all module-level store entries for a deleted bucket.

    Called after Moto successfully deletes a bucket (204). Without this,
    recreating a bucket with the same name would inherit stale CORS,
    lifecycle, object lock, legal hold, logging, and notification configs.
    """
    with _store_lock:
        _cors_store.pop(bucket, None)
        _lifecycle_store.pop(bucket, None)
        _object_lock_store.pop(bucket, None)
        _logging_store.pop(bucket, None)
        _directory_bucket_store.pop(bucket, None)
        # Legal hold uses compound keys "bucket/key" — remove all for this bucket
        prefix = f"{bucket}/"
        keys_to_remove = [k for k in _object_legal_hold_store if k.startswith(prefix)]
        for k in keys_to_remove:
            del _object_legal_hold_store[k]

    # Clean up notification config (separate module, separate lock)
    from robotocore.services.s3.notifications import _bucket_notifications
    from robotocore.services.s3.notifications import _lock as _notif_lock

    with _notif_lock:
        _bucket_notifications.pop(bucket, None)


# ---------------------------------------------------------------------------
# Main request handler
# ---------------------------------------------------------------------------


async def handle_s3_request(request: Request, region: str, account_id: str) -> Response:
    """Handle S3 request: delegate to Moto, then fire notifications."""
    path = request.url.path
    method = request.method.upper()

    # Handle OPTIONS (CORS preflight)
    if method == "OPTIONS":
        match = _PATH_RE.match(path)
        if match:
            return _handle_cors_preflight(match.group(1), request)
        return Response(status_code=400)

    # WriteGetObjectResponse — S3 Object Lambda. A Lambda function calls this to
    # deliver the transformed object back to the caller. boto3 sends the request to
    # {RequestRoute}.localhost:{port}/WriteGetObjectResponse; after S3 vhost rewriting
    # the path becomes /{route-token}/WriteGetObjectResponse. Accept and return 200.
    if path.endswith("/WriteGetObjectResponse") and method == "POST":
        return Response(status_code=200)

    # Handle presigned URL requests by stripping signature params
    if _is_presigned_url(request.query_params):
        body = await request.body()
        request = _strip_presigned_params(request, body)

    # Check query-param-based sub-resource routing
    query = str(request.url.query)
    sub = _get_query_param(request)

    # Notification config
    if sub == "notification" or (
        query == "notification"
        or query.startswith("notification=")
        or "notification" in query.split("&")
    ):
        return await _handle_notification_config(request, method, path)

    # CORS config
    if sub == "cors":
        return await _handle_cors_config(request, method, path)

    # Lifecycle config
    if sub == "lifecycle":
        return await _handle_lifecycle_config(request, method, path)

    # Object lock config
    if sub == "object-lock":
        return await _handle_object_lock_config(request, method, path)

    # Legal hold
    if sub == "legal-hold":
        return await _handle_legal_hold(request, method, path)

    # Logging config — intercept to skip Moto's strict permission checks
    if sub == "logging":
        return await _handle_logging_config(request, method, path)

    # Multipart: ?uploads and ?uploadId= are forwarded to Moto directly
    # Versioning: ?versioning and ?versionId= are forwarded to Moto directly
    # These are native Moto operations and need no interception.

    # CreateSession (S3 Express) — return session credentials for directory buckets.
    # boto3 automatically calls CreateSession before every S3 Express operation,
    # including CreateBucket. We handle this natively so the credentials are
    # available immediately without requiring the bucket to already be a directory
    # bucket in Moto, which would create a chicken-and-egg problem.
    if sub == "session":
        return _handle_create_session(path)

    # RenameObject (S3 Express — directory buckets only) — forward to Moto
    if sub == "renameObject":
        return await forward_to_moto(request, "s3", account_id=account_id)

    # Forward to Moto for actual S3 operation
    response = await forward_to_moto(request, "s3", account_id=account_id)

    # Post-response cleanup and event firing
    if response.status_code in (200, 202, 204):
        match = _PATH_RE.match(path)
        if match:
            bucket = match.group(1)
            key = match.group(2) or ""

            # Clean up all module-level stores when a bucket is deleted
            if method == "DELETE" and not key and not query:
                _cleanup_bucket_stores(bucket)

            # Track directory bucket creation for local metadata lookup
            if method == "PUT" and not key and not query:
                try:
                    body = await request.body()
                    body_str = body.decode() if body else ""
                    if "<Type>Directory</Type>" in body_str:
                        with _store_lock:
                            _directory_bucket_store[bucket] = {
                                "type": "Directory",
                                "location_type": "AvailabilityZone",
                            }
                except Exception:
                    # Best-effort tracking — body may already be consumed
                    logging.debug("Failed to detect directory bucket type from request body")

            if method == "PUT" and key:
                content_length = 0
                for h, v in response.raw_headers:
                    hname = h.decode() if isinstance(h, bytes) else h
                    if hname.lower() == "content-length":
                        try:
                            content_length = int(v)
                        except (ValueError, TypeError) as exc:
                            logger.debug("handle_s3_request: int failed (non-fatal): %s", exc)
                etag = ""
                for h, v in response.raw_headers:
                    hname = h.decode() if isinstance(h, bytes) else h
                    if hname.lower() == "etag":
                        etag = (v.decode() if isinstance(v, bytes) else v).strip('"')

                # Detect multipart complete (POST with uploadId)
                if "uploadId" in query:
                    pass  # handled below in POST
                elif request.headers.get("x-amz-copy-source"):
                    fire_event(
                        "s3:ObjectCreated:Copy",
                        bucket,
                        key,
                        region,
                        account_id,
                        content_length,
                        etag,
                    )
                else:
                    fire_event(
                        "s3:ObjectCreated:Put",
                        bucket,
                        key,
                        region,
                        account_id,
                        content_length,
                        etag,
                    )
                    from robotocore.services.s3.replication import maybe_replicate

                    maybe_replicate(bucket, key, region, account_id)
            elif method == "POST" and key:
                if "uploadId" in query:
                    fire_event(
                        "s3:ObjectCreated:CompleteMultipartUpload",
                        bucket,
                        key,
                        region,
                        account_id,
                    )
                elif "uploads" in query:
                    pass  # CreateMultipartUpload — no notification
                elif "restore" in query:
                    fire_event(
                        "s3:ObjectRestore:Post",
                        bucket,
                        key,
                        region,
                        account_id,
                    )
                else:
                    fire_event(
                        "s3:ObjectCreated:Post",
                        bucket,
                        key,
                        region,
                        account_id,
                    )
            elif method == "DELETE" and key:
                # Check if Moto created a delete marker
                is_delete_marker = False
                for h, v in response.raw_headers:
                    hname = h.decode() if isinstance(h, bytes) else h
                    if hname.lower() == "x-amz-delete-marker":
                        hval = v.decode() if isinstance(v, bytes) else v
                        if hval.lower() == "true":
                            is_delete_marker = True
                        break
                if is_delete_marker:
                    fire_event(
                        "s3:ObjectRemoved:DeleteMarkerCreated",
                        bucket,
                        key,
                        region,
                        account_id,
                    )
                else:
                    fire_event(
                        "s3:ObjectRemoved:Delete",
                        bucket,
                        key,
                        region,
                        account_id,
                    )

    return response


# ---------------------------------------------------------------------------
# S3 Express session handler
# ---------------------------------------------------------------------------

_S3_EXPRESS_SESSION_EXPIRY_HOURS = 12

_CREATE_SESSION_RESPONSE_TEMPLATE = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    "<CreateSessionResult>"
    "<Credentials>"
    "<SessionToken>{token}</SessionToken>"
    "<SecretAccessKey>{secret}</SecretAccessKey>"
    "<AccessKeyId>{key_id}</AccessKeyId>"
    "<Expiration>{expiry}</Expiration>"
    "</Credentials>"
    "</CreateSessionResult>"
)


def _handle_create_session(path: str) -> Response:
    """Return temporary S3 Express session credentials.

    Handled natively rather than delegated to Moto because boto3's automatic
    session management calls CreateSession before *every* S3 Express operation —
    including CreateBucket itself — creating a chicken-and-egg situation where
    the bucket does not yet exist when the first session request arrives.
    """
    import datetime
    import hashlib

    seed = path + datetime.datetime.now(datetime.UTC).isoformat()
    h = hashlib.sha256(seed.encode()).hexdigest()
    expiry = (
        datetime.datetime.now(datetime.UTC)
        + datetime.timedelta(hours=_S3_EXPRESS_SESSION_EXPIRY_HOURS)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    body = _CREATE_SESSION_RESPONSE_TEMPLATE.format(
        token=f"FwoGZXIvYXdzE{h}",
        secret=h,
        key_id=f"ASIA{h[:16].upper()}",
        expiry=expiry,
    )
    return Response(
        status_code=200,
        content=body,
        media_type="application/xml",
    )


# ---------------------------------------------------------------------------
# Sub-resource handlers
# ---------------------------------------------------------------------------


async def _handle_notification_config(request: Request, method: str, path: str) -> Response:
    match = _PATH_RE.match(path)
    if not match:
        return Response(status_code=400, content="Bad request")
    bucket = match.group(1)

    if method == "GET":
        config = get_notification_config(bucket)
        xml = _notification_config_to_xml(config)
        return Response(content=xml, status_code=200, media_type="application/xml")
    elif method == "PUT":
        body = await request.body()
        config = _parse_notification_config_xml(body.decode())
        set_notification_config(bucket, config)
        return Response(status_code=200)

    return Response(status_code=405)


async def _handle_cors_config(request: Request, method: str, path: str) -> Response:
    match = _PATH_RE.match(path)
    if not match:
        return Response(status_code=400, content="Bad request")
    bucket = match.group(1)

    if method == "GET":
        rules = get_bucket_cors(bucket)
        if rules is None:
            return Response(
                status_code=404,
                content=(
                    "<Error><Code>NoSuchCORSConfiguration</Code>"
                    "<Message>The CORS configuration does not exist"
                    "</Message></Error>"
                ),
                media_type="application/xml",
            )
        xml = _cors_to_xml(rules)
        return Response(content=xml, status_code=200, media_type="application/xml")
    elif method == "PUT":
        body = await request.body()
        rules = _parse_cors_xml(body.decode())
        set_bucket_cors(bucket, rules)
        return Response(status_code=200)
    elif method == "DELETE":
        delete_bucket_cors(bucket)
        return Response(status_code=204)

    return Response(status_code=405)


async def _handle_lifecycle_config(request: Request, method: str, path: str) -> Response:
    match = _PATH_RE.match(path)
    if not match:
        return Response(status_code=400, content="Bad request")
    bucket = match.group(1)

    if method == "GET":
        rules = get_bucket_lifecycle(bucket)
        if rules is None:
            return Response(
                status_code=404,
                content=(
                    "<Error><Code>NoSuchLifecycleConfiguration</Code>"
                    "<Message>The lifecycle configuration does not exist"
                    "</Message></Error>"
                ),
                media_type="application/xml",
            )
        xml = _lifecycle_to_xml(rules)
        return Response(content=xml, status_code=200, media_type="application/xml")
    elif method == "PUT":
        body = await request.body()
        rules = _parse_lifecycle_xml(body.decode())
        set_bucket_lifecycle(bucket, rules)
        return Response(status_code=200)
    elif method == "DELETE":
        delete_bucket_lifecycle(bucket)
        return Response(status_code=204)

    return Response(status_code=405)


async def _handle_object_lock_config(request: Request, method: str, path: str) -> Response:
    match = _PATH_RE.match(path)
    if not match:
        return Response(status_code=400, content="Bad request")
    bucket = match.group(1)

    if method == "GET":
        config = get_object_lock_config(bucket)
        if config is None:
            return Response(
                status_code=404,
                content=(
                    "<Error><Code>ObjectLockConfigurationNotFoundError"
                    "</Code><Message>Object Lock configuration does "
                    "not exist for this bucket</Message></Error>"
                ),
                media_type="application/xml",
            )
        xml = _object_lock_to_xml(config)
        return Response(content=xml, status_code=200, media_type="application/xml")
    elif method == "PUT":
        body = await request.body()
        config = _parse_object_lock_xml(body.decode())
        set_object_lock_config(bucket, config)
        return Response(status_code=200)

    return Response(status_code=405)


async def _handle_legal_hold(request: Request, method: str, path: str) -> Response:
    match = _PATH_RE.match(path)
    if not match:
        return Response(status_code=400, content="Bad request")
    bucket = match.group(1)
    key = match.group(2) or ""

    if not key:
        return Response(status_code=400, content="Key required")

    if method == "GET":
        status = get_object_legal_hold(bucket, key)
        if status is None:
            return Response(
                status_code=404,
                content=(
                    "<Error><Code>NoSuchKey</Code><Message>Legal hold not set</Message></Error>"
                ),
                media_type="application/xml",
            )
        xml = _legal_hold_to_xml(status)
        return Response(content=xml, status_code=200, media_type="application/xml")
    elif method == "PUT":
        body = await request.body()
        status = _parse_legal_hold_xml(body.decode())
        set_object_legal_hold(bucket, key, status)
        return Response(status_code=200)

    return Response(status_code=405)


async def _handle_logging_config(request: Request, method: str, path: str) -> Response:
    """Handle ?logging sub-resource — skip Moto's strict permission checks."""
    match = _PATH_RE.match(path)
    if not match:
        return Response(status_code=400, content="Bad request")
    bucket = match.group(1)

    if method == "GET":
        with _store_lock:
            config = _logging_store.get(bucket)
        if config:
            xml = (
                f'<?xml version="1.0" encoding="UTF-8"?>'
                f'<BucketLoggingStatus xmlns="{S3_NS}">'
                f"<LoggingEnabled>"
                f"<TargetBucket>{config['TargetBucket']}</TargetBucket>"
                f"<TargetPrefix>{config.get('TargetPrefix', '')}</TargetPrefix>"
                f"</LoggingEnabled>"
                f"</BucketLoggingStatus>"
            )
        else:
            xml = f'<?xml version="1.0" encoding="UTF-8"?><BucketLoggingStatus xmlns="{S3_NS}"/>'
        return Response(content=xml, status_code=200, media_type="application/xml")
    elif method == "PUT":
        body = await request.body()
        body_str = body.decode() if body else ""
        if body_str and "<LoggingEnabled>" in body_str:
            root = ET.fromstring(body_str)
            ns = {"s3": S3_NS}
            le = root.find("s3:LoggingEnabled", ns) or root.find("LoggingEnabled")
            if le is not None:
                tb = le.findtext(f"{{{S3_NS}}}TargetBucket") or le.findtext("TargetBucket")
                tp = le.findtext(f"{{{S3_NS}}}TargetPrefix") or le.findtext("TargetPrefix") or ""
                with _store_lock:
                    _logging_store[bucket] = {
                        "TargetBucket": tb or "",
                        "TargetPrefix": tp,
                    }
        else:
            # Empty body or no LoggingEnabled means disable logging
            with _store_lock:
                _logging_store.pop(bucket, None)
        return Response(status_code=200)

    return Response(status_code=405)


# ---------------------------------------------------------------------------
# Notification config XML parsing/serialization
# ---------------------------------------------------------------------------


def _parse_notification_config_xml(xml_str: str) -> NotificationConfig:
    """Parse S3 notification configuration XML."""
    config = NotificationConfig()

    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return config

    ns = S3_NS

    for qc in root.findall(f"{{{ns}}}QueueConfiguration") + root.findall("QueueConfiguration"):
        queue_arn = ""
        events: list[str] = []
        filter_rules: list[dict] = []

        for child in qc:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if tag == "Queue":
                queue_arn = child.text or ""
            elif tag == "Event":
                events.append(child.text or "")
            elif tag == "Filter":
                filter_rules = _parse_filter_rules(child)

        entry: dict = {"QueueArn": queue_arn, "Events": events}
        if filter_rules:
            entry["Filter"] = {"Key": {"FilterRules": filter_rules}}
        config.queue_configs.append(entry)

    for tc in root.findall(f"{{{ns}}}TopicConfiguration") + root.findall("TopicConfiguration"):
        topic_arn = ""
        events = []
        filter_rules = []

        for child in tc:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if tag == "Topic":
                topic_arn = child.text or ""
            elif tag == "Event":
                events.append(child.text or "")
            elif tag == "Filter":
                filter_rules = _parse_filter_rules(child)

        entry = {"TopicArn": topic_arn, "Events": events}
        if filter_rules:
            entry["Filter"] = {"Key": {"FilterRules": filter_rules}}
        config.topic_configs.append(entry)

    # Parse LambdaFunctionConfiguration
    for lc in (
        root.findall(f"{{{ns}}}CloudFunctionConfiguration")
        + root.findall("CloudFunctionConfiguration")
        + root.findall(f"{{{ns}}}LambdaFunctionConfiguration")
        + root.findall("LambdaFunctionConfiguration")
    ):
        lambda_arn = ""
        events = []
        filter_rules = []

        for child in lc:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if tag in ("CloudFunction", "LambdaFunctionArn"):
                lambda_arn = child.text or ""
            elif tag == "Event":
                events.append(child.text or "")
            elif tag == "Filter":
                filter_rules = _parse_filter_rules(child)

        entry = {"LambdaFunctionArn": lambda_arn, "Events": events}
        if filter_rules:
            entry["Filter"] = {"Key": {"FilterRules": filter_rules}}
        config.lambda_configs.append(entry)

    # Parse EventBridgeConfiguration
    eb_elements = root.findall(f"{{{ns}}}EventBridgeConfiguration") + root.findall(
        "EventBridgeConfiguration"
    )
    if eb_elements:
        config.eventbridge_enabled = True

    return config


def _parse_filter_rules(filter_el: ET.Element) -> list[dict]:
    """Extract FilterRule elements from a Filter element."""
    filter_rules: list[dict] = []
    for rule in filter_el.iter():
        rtag = rule.tag.split("}")[-1] if "}" in rule.tag else rule.tag
        if rtag == "FilterRule":
            name = ""
            value = ""
            for rc in rule:
                rctag = rc.tag.split("}")[-1] if "}" in rc.tag else rc.tag
                if rctag == "Name":
                    name = rc.text or ""
                elif rctag == "Value":
                    value = rc.text or ""
            if name:
                filter_rules.append({"Name": name, "Value": value})
    return filter_rules


def _notification_config_to_xml(config: NotificationConfig) -> str:
    parts = ['<?xml version="1.0" encoding="UTF-8"?>']
    parts.append(f'<NotificationConfiguration xmlns="{S3_NS}">')

    for qc in config.queue_configs:
        parts.append("<QueueConfiguration>")
        parts.append(f"<Queue>{qc['QueueArn']}</Queue>")
        for evt in qc.get("Events", []):
            parts.append(f"<Event>{evt}</Event>")
        _append_filter_xml(parts, qc)
        parts.append("</QueueConfiguration>")

    for tc in config.topic_configs:
        parts.append("<TopicConfiguration>")
        parts.append(f"<Topic>{tc['TopicArn']}</Topic>")
        for evt in tc.get("Events", []):
            parts.append(f"<Event>{evt}</Event>")
        _append_filter_xml(parts, tc)
        parts.append("</TopicConfiguration>")

    for lc in config.lambda_configs:
        parts.append("<LambdaFunctionConfiguration>")
        parts.append(f"<LambdaFunctionArn>{lc['LambdaFunctionArn']}</LambdaFunctionArn>")
        for evt in lc.get("Events", []):
            parts.append(f"<Event>{evt}</Event>")
        _append_filter_xml(parts, lc)
        parts.append("</LambdaFunctionConfiguration>")

    if config.eventbridge_enabled:
        parts.append("<EventBridgeConfiguration/>")

    parts.append("</NotificationConfiguration>")
    return "".join(parts)


def _append_filter_xml(parts: list[str], entry: dict) -> None:
    if "Filter" in entry:
        parts.append("<Filter><S3Key>")
        for rule in entry["Filter"].get("Key", {}).get("FilterRules", []):
            parts.append(
                f"<FilterRule><Name>{rule['Name']}</Name>"
                f"<Value>{rule['Value']}</Value></FilterRule>"
            )
        parts.append("</S3Key></Filter>")
