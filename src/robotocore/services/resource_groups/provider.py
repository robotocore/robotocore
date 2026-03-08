"""Native Resource Groups provider.

Intercepts tag operations where Moto's URL routing breaks on encoded ARNs:
- GetTags / Tag / Untag: ARN in URL path contains encoded slashes
"""

import json
import re
import urllib.parse

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto

_TAGS_RE = re.compile(r"^/resources/(.+)/tags$")


async def handle_resource_groups_request(
    request: Request, region: str, account_id: str
) -> Response:
    """Handle Resource Groups requests, intercepting tag operations."""
    path = request.url.path
    m = _TAGS_RE.match(path)
    if m:
        arn = urllib.parse.unquote(m.group(1))
        body = await request.body()

        try:
            if request.method == "GET":
                return _get_tags(arn, region, account_id)
            elif request.method == "PUT":
                return _tag(arn, body, region, account_id)
            elif request.method == "PATCH":
                return _untag(arn, body, region, account_id)
        except Exception as e:
            return Response(
                content=json.dumps({"__type": "InternalError", "message": str(e)}),
                status_code=500,
                media_type="application/json",
            )

    return await forward_to_moto(request, "resource-groups")


def _get_tags(arn: str, region: str, account_id: str) -> Response:
    from moto.backends import get_backend

    # Use request region, not ARN region — Moto hardcodes us-west-1 in ARNs
    backend = get_backend("resource-groups")[account_id][region]
    tags = {}
    if arn in backend.groups.by_arn:
        group = backend.groups.by_arn[arn]
        tags = dict(group.tags) if hasattr(group, "tags") and group.tags else {}

    return Response(
        content=json.dumps({"Arn": arn, "Tags": tags}),
        status_code=200,
        media_type="application/json",
    )


def _tag(arn: str, body: bytes, region: str, account_id: str) -> Response:
    from moto.backends import get_backend

    params = json.loads(body) if body else {}
    new_tags = params.get("Tags", {})

    backend = get_backend("resource-groups")[account_id][region]
    if arn in backend.groups.by_arn:
        group = backend.groups.by_arn[arn]
        if hasattr(group, "tags") and group.tags is not None:
            group.tags.update(new_tags)
        else:
            group.tags = dict(new_tags)

    return Response(
        content=json.dumps({"Arn": arn, "Tags": new_tags}),
        status_code=200,
        media_type="application/json",
    )


def _untag(arn: str, body: bytes, region: str, account_id: str) -> Response:
    from moto.backends import get_backend

    params = json.loads(body) if body else {}
    keys = params.get("Keys", [])

    backend = get_backend("resource-groups")[account_id][region]
    if arn in backend.groups.by_arn:
        group = backend.groups.by_arn[arn]
        if hasattr(group, "tags") and group.tags:
            for key in keys:
                group.tags.pop(key, None)

    return Response(
        content=json.dumps({"Arn": arn, "Keys": keys}),
        status_code=200,
        media_type="application/json",
    )
