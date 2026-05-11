"""Native SES v2 provider.

Handles operations that Moto's sesv2 doesn't support (email templates).
Also captures SendEmail calls into the shared EmailStore so they appear at
/_robotocore/ses/messages alongside SMTP and SES v1 API emails.
Delegates everything else to Moto via forward_to_moto.
Uses REST-JSON protocol with path-based routing.
"""

import json
import logging
import re
import time

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto
from robotocore.services.ses.email_store import get_email_store

logger = logging.getLogger(__name__)

# In-memory template store: {region: {template_name: template_data}}
_templates: dict[str, dict[str, dict]] = {}

# Path patterns for operations we handle natively
_TEMPLATE_PATH = re.compile(r"^/v2/email/templates/?$")
_TEMPLATE_ITEM_PATH = re.compile(r"^/v2/email/templates/([^/]+)$")
_TEMPLATE_RENDER_PATH = re.compile(r"^/v2/email/templates/([^/]+)/render$")
_MESSAGE_INSIGHTS_PATH = re.compile(r"^/v2/email/insights/([^/]+)/?$")
_OUTBOUND_EMAILS_PATH = re.compile(r"^/v2/email/outbound-emails/?$")


async def handle_sesv2_request(request: Request, region: str, account_id: str) -> Response:
    """Handle SES v2 API requests (REST-JSON protocol)."""
    path = request.url.path
    method = request.method.upper()

    # Email templates — not in Moto
    m = _TEMPLATE_ITEM_PATH.match(path)
    if m:
        template_name = m.group(1)
        if method == "GET":
            return _get_email_template(template_name, region)
        elif method == "PUT":
            try:
                body = json.loads(await request.body())
            except json.JSONDecodeError as e:
                from starlette.responses import JSONResponse

                return JSONResponse({"error": f"Invalid JSON: {e}"}, status_code=400)
            return _update_email_template(template_name, body, region)
        elif method == "DELETE":
            return _delete_email_template(template_name, region)

    if _TEMPLATE_PATH.match(path):
        if method == "POST":
            try:
                body = json.loads(await request.body())
            except json.JSONDecodeError as e:
                from starlette.responses import JSONResponse

                return JSONResponse({"error": f"Invalid JSON: {e}"}, status_code=400)
            return _create_email_template(body, region)
        elif method == "GET":
            return _list_email_templates(region)

    m = _MESSAGE_INSIGHTS_PATH.match(path)
    if m:
        message_id = m.group(1)
        return _get_message_insights(message_id)

    # Intercept SendEmail to capture it in the email store
    if _OUTBOUND_EMAILS_PATH.match(path) and method == "POST":
        body_bytes = await request.body()
        response = await forward_to_moto(request, "sesv2", account_id=account_id)
        if response.status_code == 200:
            try:
                _capture_sesv2_send_email(json.loads(body_bytes))
            except Exception:  # noqa: BLE001
                logger.debug("SES v2 email capture failed (non-fatal)")
        return response

    # Everything else → Moto
    return await forward_to_moto(request, "sesv2", account_id=account_id)


def _store(region: str) -> dict[str, dict]:
    return _templates.setdefault(region, {})


def _create_email_template(body: dict, region: str) -> Response:
    name = body.get("TemplateName", "")
    content = body.get("TemplateContent", {})
    store = _store(region)
    if name in store:
        return _error("AlreadyExistsException", f"Template {name} already exists", 409)
    store[name] = {
        "TemplateName": name,
        "TemplateContent": content,
        "CreatedTimestamp": time.time(),
    }
    return Response(content=json.dumps({}), status_code=200, media_type="application/json")


def _get_email_template(name: str, region: str) -> Response:
    store = _store(region)
    tmpl = store.get(name)
    if not tmpl:
        return _error("NotFoundException", f"Template {name} does not exist", 404)
    return Response(
        content=json.dumps(
            {
                "TemplateName": tmpl["TemplateName"],
                "TemplateContent": tmpl["TemplateContent"],
            }
        ),
        status_code=200,
        media_type="application/json",
    )


def _list_email_templates(region: str) -> Response:
    store = _store(region)
    metadata = [
        {
            "TemplateName": t["TemplateName"],
            "CreatedTimestamp": t.get("CreatedTimestamp", 0),
        }
        for t in store.values()
    ]
    return Response(
        content=json.dumps({"TemplatesMetadata": metadata}),
        status_code=200,
        media_type="application/json",
    )


def _update_email_template(name: str, body: dict, region: str) -> Response:
    store = _store(region)
    if name not in store:
        return _error("NotFoundException", f"Template {name} does not exist", 404)
    content = body.get("TemplateContent", {})
    store[name]["TemplateContent"] = content
    return Response(content=json.dumps({}), status_code=200, media_type="application/json")


def _delete_email_template(name: str, region: str) -> Response:
    store = _store(region)
    store.pop(name, None)
    return Response(content=json.dumps({}), status_code=200, media_type="application/json")


def _get_message_insights(message_id: str) -> Response:
    result = {
        "MessageId": message_id,
        "FromEmailAddress": "",
        "Subject": "",
        "EmailTags": [],
        "Insights": [],
    }
    return Response(content=json.dumps(result), status_code=200, media_type="application/json")


def _capture_sesv2_send_email(body: dict) -> None:
    """Capture a SES v2 SendEmail call into the shared EmailStore."""
    sender = body.get("FromEmailAddress", "")
    dest = body.get("Destination", {})
    recipients = (
        dest.get("ToAddresses", []) + dest.get("CcAddresses", []) + dest.get("BccAddresses", [])
    )

    content = body.get("Content", {})
    simple = content.get("Simple", {})
    subject = simple.get("Subject", {}).get("Data", "")
    text_body = simple.get("Body", {}).get("Text", {}).get("Data", "")
    html_body = simple.get("Body", {}).get("Html", {}).get("Data", "")
    email_body = text_body or html_body

    # Template-based sends
    if not subject:
        template = content.get("Template", {})
        template_name = template.get("TemplateName", "")
        if template_name:
            subject = f"[template: {template_name}]"

    get_email_store().add_message(
        sender=sender,
        recipients=recipients,
        subject=subject,
        body=email_body,
        raw="",
        source="api",
    )


def _error(code: str, message: str, status: int) -> Response:
    return Response(
        content=json.dumps({"__type": code, "message": message}),
        status_code=status,
        media_type="application/json",
    )
