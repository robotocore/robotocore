"""Enhanced S3 provider — wraps Moto's S3 and adds event notifications."""

import re
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


def _is_presigned_url(query_params: QueryParams) -> bool:
    """Check if the request is a presigned URL request."""
    return "X-Amz-Signature" in query_params or "Signature" in query_params


def _strip_presigned_params(request: Request, body: bytes | None = None) -> Request:
    """Return a modified request with presigned URL params stripped.

    Converts X-Amz-Security-Token query param into a header (Moto expects it there)
    and removes all signature-related query params so Moto sees a clean request.
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
    new_query_string = urlencode(clean_params).encode("utf-8") if clean_params else b""
    scope["query_string"] = new_query_string

    # If there was a security token, inject it as a header
    if security_token:
        headers = list(scope.get("headers", []))
        headers.append((b"x-amz-security-token", security_token.encode("utf-8")))
        scope["headers"] = headers

    # Inject a fake Authorization header so Moto can extract region/credentials
    if not request.headers.get("authorization"):
        credential = request.query_params.get("X-Amz-Credential", "")
        if credential:
            # SigV4 presigned URL
            signed_headers = request.query_params.get("X-Amz-SignedHeaders", "host")
            auth_value = (
                f"AWS4-HMAC-SHA256 Credential={credential}, "
                f"SignedHeaders={signed_headers}, "
                f"Signature=presigned-placeholder"
            )
        else:
            # SigV2 presigned URL — inject a minimal SigV4 auth header
            # so Moto can route to the right backend
            access_key = request.query_params.get("AWSAccessKeyId", "testing")
            auth_value = (
                f"AWS4-HMAC-SHA256 Credential={access_key}/20260101/us-east-1/s3/aws4_request, "
                f"SignedHeaders=host, "
                f"Signature=presigned-placeholder"
            )

        headers = list(scope.get("headers", []))
        headers.append((b"authorization", auth_value.encode("utf-8")))
        scope["headers"] = headers

    # Ensure Content-Type header exists for PUT/POST (some ASGI servers skip body without it)
    method = scope.get("method", "GET").upper()
    if method in ("PUT", "POST"):
        has_ct = any(k == b"content-type" for k, v in scope.get("headers", []))
        if not has_ct:
            headers = list(scope.get("headers", []))
            headers.append((b"content-type", b"application/octet-stream"))
            scope["headers"] = headers

    # If we have cached body bytes, set _body directly on the new Request
    if body is not None:
        new_req = Request(scope, request.receive)
        new_req._body = body
        return new_req

    return Request(scope, request.receive)


async def handle_s3_request(request: Request, region: str, account_id: str) -> Response:
    """Handle S3 request: delegate to Moto, then fire notifications on mutations."""
    path = request.url.path
    method = request.method.upper()

    # Handle presigned URL requests by stripping signature params
    if _is_presigned_url(request.query_params):
        # Cache the body before creating a new Request, since receive() can only be called once
        body = await request.body()
        request = _strip_presigned_params(request, body)

    # Handle notification configuration API
    query = str(request.url.query)
    if (
        query == "notification"
        or query.startswith("notification=")
        or "notification" in query.split("&")
    ):
        return await _handle_notification_config(request, method, path)

    # Forward to Moto for actual S3 operation
    response = await forward_to_moto(request, "s3")

    # Fire notifications on successful mutations
    if response.status_code in (200, 204):
        match = _PATH_RE.match(path)
        if match:
            bucket = match.group(1)
            key = match.group(2) or ""

            if method == "PUT" and key:
                content_length = 0
                for h, v in response.raw_headers:
                    if h.lower() == b"content-length":
                        try:
                            content_length = int(v)
                        except (ValueError, TypeError):
                            pass
                etag = ""
                for h, v in response.raw_headers:
                    if h.lower() == b"etag":
                        etag = v.decode().strip('"')
                fire_event(
                    "s3:ObjectCreated:Put",
                    bucket,
                    key,
                    region,
                    account_id,
                    content_length,
                    etag,
                )
            elif method == "POST" and key:
                fire_event(
                    "s3:ObjectCreated:Post",
                    bucket,
                    key,
                    region,
                    account_id,
                )
            elif method == "DELETE" and key:
                fire_event(
                    "s3:ObjectRemoved:Delete",
                    bucket,
                    key,
                    region,
                    account_id,
                )

    return response


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


def _parse_notification_config_xml(xml_str: str) -> NotificationConfig:
    """Parse S3 notification configuration XML."""
    import xml.etree.ElementTree as ET

    config = NotificationConfig()

    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return config

    ns = "http://s3.amazonaws.com/doc/2006-03-01/"

    for qc in root.findall(f"{{{ns}}}QueueConfiguration") + root.findall("QueueConfiguration"):
        queue_arn = ""
        events = []
        filter_rules = []

        for child in qc:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if tag == "Queue":
                queue_arn = child.text or ""
            elif tag == "Event":
                events.append(child.text or "")
            elif tag == "Filter":
                for rule in child.iter():
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

        entry = {"QueueArn": queue_arn, "Events": events}
        if filter_rules:
            entry["Filter"] = {"Key": {"FilterRules": filter_rules}}
        config.queue_configs.append(entry)

    for tc in root.findall(f"{{{ns}}}TopicConfiguration") + root.findall("TopicConfiguration"):
        topic_arn = ""
        events = []

        for child in tc:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if tag == "Topic":
                topic_arn = child.text or ""
            elif tag == "Event":
                events.append(child.text or "")

        config.topic_configs.append({"TopicArn": topic_arn, "Events": events})

    return config


def _notification_config_to_xml(config: NotificationConfig) -> str:
    parts = ['<?xml version="1.0" encoding="UTF-8"?>']
    parts.append('<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">')

    for qc in config.queue_configs:
        parts.append("<QueueConfiguration>")
        parts.append(f"<Queue>{qc['QueueArn']}</Queue>")
        for evt in qc.get("Events", []):
            parts.append(f"<Event>{evt}</Event>")
        if "Filter" in qc:
            parts.append("<Filter><S3Key>")
            for rule in qc["Filter"].get("Key", {}).get("FilterRules", []):
                parts.append(
                    f"<FilterRule><Name>{rule['Name']}</Name><Value>{rule['Value']}</Value></FilterRule>"
                )
            parts.append("</S3Key></Filter>")
        parts.append("</QueueConfiguration>")

    for tc in config.topic_configs:
        parts.append("<TopicConfiguration>")
        parts.append(f"<Topic>{tc['TopicArn']}</Topic>")
        for evt in tc.get("Events", []):
            parts.append(f"<Event>{evt}</Event>")
        parts.append("</TopicConfiguration>")

    parts.append("</NotificationConfiguration>")
    return "".join(parts)
