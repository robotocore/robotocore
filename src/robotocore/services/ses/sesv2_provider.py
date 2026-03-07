"""Native SES v2 provider.

Handles operations that Moto's sesv2 doesn't support (email templates).
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

logger = logging.getLogger(__name__)

# In-memory template store: {region: {template_name: template_data}}
_templates: dict[str, dict[str, dict]] = {}

# Path patterns for operations we handle natively
_TEMPLATE_PATH = re.compile(r"^/v2/email/templates/?$")
_TEMPLATE_ITEM_PATH = re.compile(r"^/v2/email/templates/([^/]+)$")
_TEMPLATE_RENDER_PATH = re.compile(r"^/v2/email/templates/([^/]+)/render$")


async def handle_sesv2_request(
    request: Request, region: str, account_id: str
) -> Response:
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
            body = json.loads(await request.body())
            return _update_email_template(template_name, body, region)
        elif method == "DELETE":
            return _delete_email_template(template_name, region)

    if _TEMPLATE_PATH.match(path):
        if method == "POST":
            body = json.loads(await request.body())
            return _create_email_template(body, region)
        elif method == "GET":
            return _list_email_templates(region)

    # Everything else → Moto
    return await forward_to_moto(request, "sesv2")


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
        content=json.dumps({
            "TemplateName": tmpl["TemplateName"],
            "TemplateContent": tmpl["TemplateContent"],
        }),
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


def _error(code: str, message: str, status: int) -> Response:
    return Response(
        content=json.dumps({"__type": code, "message": message}),
        status_code=status,
        media_type="application/json",
    )
