"""Enhanced S3 provider — wraps Moto's S3 and adds event notifications."""

import json
import re

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


async def handle_s3_request(request: Request, region: str, account_id: str) -> Response:
    """Handle S3 request: delegate to Moto, then fire notifications on mutations."""
    path = request.url.path
    method = request.method.upper()

    # Handle notification configuration API
    query = str(request.url.query)
    if query == "notification" or query.startswith("notification=") or "notification" in query.split("&"):
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
                    "s3:ObjectCreated:Put", bucket, key,
                    region, account_id, content_length, etag,
                )
            elif method == "POST" and key:
                fire_event(
                    "s3:ObjectCreated:Post", bucket, key,
                    region, account_id,
                )
            elif method == "DELETE" and key:
                fire_event(
                    "s3:ObjectRemoved:Delete", bucket, key,
                    region, account_id,
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
                parts.append(f"<FilterRule><Name>{rule['Name']}</Name><Value>{rule['Value']}</Value></FilterRule>")
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
