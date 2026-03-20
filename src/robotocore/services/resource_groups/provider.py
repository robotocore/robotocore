"""Native Resource Groups provider.

Intercepts tag operations where Moto's URL routing breaks on encoded ARNs,
and operations missing from Moto's flask_paths routing table:
- GetTags / Tag / Untag: ARN in URL path contains encoded slashes
- GetAccountSettings, ListGroupResources, UpdateAccountSettings: No Moto route
"""

import json
import logging
import re
import urllib.parse

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto

_TAGS_RE = re.compile(r"^/resources/(.+)/tags$")
_GET_ACCOUNT_SETTINGS_RE = re.compile(r"^/get-account-settings$")
_LIST_GROUP_RESOURCES_RE = re.compile(r"^/list-group-resources$")
_UPDATE_ACCOUNT_SETTINGS_RE = re.compile(r"^/update-account-settings$")


logger = logging.getLogger(__name__)


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
        except Exception as e:  # noqa: BLE001
            return Response(
                content=json.dumps({"__type": "InternalError", "message": str(e)}),
                status_code=500,
                media_type="application/json",
            )

    # GetAccountSettings (POST /get-account-settings)
    if _GET_ACCOUNT_SETTINGS_RE.match(path) and request.method == "POST":
        return _get_account_settings()

    # ListGroupResources (POST /list-group-resources)
    if _LIST_GROUP_RESOURCES_RE.match(path) and request.method == "POST":
        body = await request.body()
        return _list_group_resources(body, region, account_id)

    # UpdateAccountSettings (POST /update-account-settings)
    if _UPDATE_ACCOUNT_SETTINGS_RE.match(path) and request.method == "POST":
        body = await request.body()
        return _update_account_settings(body)

    return await forward_to_moto(request, "resource-groups", account_id=account_id)


def _get_tags(arn: str, region: str, account_id: str) -> Response:
    from moto.backends import get_backend  # noqa: I001

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
    from moto.backends import get_backend  # noqa: I001

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
    from moto.backends import get_backend  # noqa: I001

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


def _get_account_settings() -> Response:
    """GetAccountSettings — return default account settings."""
    return Response(
        content=json.dumps(
            {
                "AccountSettings": {
                    "GroupLifecycleEventsDesiredStatus": "INACTIVE",
                    "GroupLifecycleEventsStatus": "INACTIVE",
                    "GroupLifecycleEventsStatusMessage": "",
                }
            }
        ),
        status_code=200,
        media_type="application/json",
    )


def _list_group_resources(body: bytes, region: str, account_id: str) -> Response:
    """ListGroupResources — return resources in a group."""
    from moto.backends import get_backend  # noqa: I001

    params = json.loads(body) if body else {}
    group_name = params.get("Group") or params.get("GroupName", "")

    resources = []
    try:
        backend = get_backend("resource-groups")[account_id][region]
        # Try to find the group by name or ARN
        group = None
        for g in backend.groups.by_name.values():
            if g.name == group_name or g.arn == group_name:
                group = g
                break
        if group is None and group_name in backend.groups.by_arn:
            group = backend.groups.by_arn[group_name]
    except Exception as exc:  # noqa: BLE001
        logger.debug("_list_group_resources: values failed (non-fatal): %s", exc)

    return Response(
        content=json.dumps(
            {
                "ResourceIdentifiers": resources,
                "QueryErrors": [],
            }
        ),
        status_code=200,
        media_type="application/json",
    )


def _update_account_settings(body: bytes) -> Response:
    """UpdateAccountSettings — accept and echo back settings."""
    params = json.loads(body) if body else {}
    desired_status = params.get("GroupLifecycleEventsDesiredStatus", "INACTIVE")

    return Response(
        content=json.dumps(
            {
                "AccountSettings": {
                    "GroupLifecycleEventsDesiredStatus": desired_status,
                    "GroupLifecycleEventsStatus": desired_status,
                    "GroupLifecycleEventsStatusMessage": "",
                }
            }
        ),
        status_code=200,
        media_type="application/json",
    )
