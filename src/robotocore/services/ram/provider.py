"""Native RAM provider.

Moto's RAM backend is single-account: ``GetResourceShares``/``ListResources`` with
``resourceOwner=OTHER-ACCOUNTS`` raise NotImplemented, only a fixed list of resource types is
shareable (no ``ipam-pool``, no ACM PCA ``certificate-authority``), and OU principals must be
direct children of the root. This provider answers the OTHER-ACCOUNTS views from every account's
shares (see ``sharing.py``) and lets well-formed ARNs and nested OUs be shared; everything else
is forwarded to Moto unchanged.
"""

import json
import logging
import re
from typing import Any

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto_with_body
from robotocore.services.ram.sharing import shares_visible_to

logger = logging.getLogger(__name__)

_RESOURCE_ARN_RE = re.compile(r"^arn:aws[a-z-]*:[a-z0-9-]+:[a-z0-9-]*:\d{12}:([a-z-]+)[/:].+$")
_MOTO_RESOURCE_ARN_RE = re.compile(r"^arn:aws:[a-z0-9-]+:[a-z0-9-]*:[0-9]{12}:([a-z-]+)[/:].*$")
_OU_ARN_RE = re.compile(r"^arn:aws[a-z-]*:organizations::\d{12}:ou/(o-\w+)/(ou-[\w-]+)$")


def _json(data: dict[str, Any], status: int = 200) -> Response:
    return Response(content=json.dumps(data), status_code=status, media_type="application/json")


def _moto_accepts_resource(arn: str) -> bool:
    from moto.ram.models import ResourceShare

    m = _MOTO_RESOURCE_ARN_RE.match(arn)
    return bool(m) and m.group(1) in ResourceShare.SHAREABLE_RESOURCES


def _ou_exists(ou_arn: str, owner: str, region: str) -> bool:
    from moto.organizations.models import organizations_backends
    from moto.utilities.utils import get_partition

    partition = get_partition(region)
    master = organizations_backends.master_accounts.get(owner)
    backend = organizations_backends[master[0] if master else owner][partition]
    return any(ou.arn == ou_arn for ou in backend.ou)


def _split_extras(payload: dict, account_id: str, region: str) -> tuple[list[str], list[str]]:
    """Remove (resources, principals) that AWS accepts but Moto would reject; return them."""
    extra_resources = [
        a
        for a in payload.get("resourceArns", [])
        if _RESOURCE_ARN_RE.match(a) and not _moto_accepts_resource(a)
    ]
    extra_principals = [
        p
        for p in payload.get("principals", [])
        # Moto validates OUs against the owner's own backend, which only works for the
        # management account and only for OUs directly under the root; validate them here.
        if _OU_ARN_RE.match(p) and _ou_exists(p, account_id, region)
    ]
    if extra_resources:
        payload["resourceArns"] = [a for a in payload["resourceArns"] if a not in extra_resources]
    if extra_principals:
        payload["principals"] = [p for p in payload["principals"] if p not in extra_principals]
    return extra_resources, extra_principals


def _find_share(account_id: str, region: str, arn: str):
    from moto.ram.models import ram_backends

    backend = ram_backends[account_id][region]
    return next((s for s in backend.resource_shares if s.arn == arn), None)


def _association(share, entity: str, kind: str) -> dict[str, Any]:
    from moto.core.utils import unix_time, utcnow

    return {
        "resourceShareArn": share.arn,
        "resourceShareName": share.name,
        "associatedEntity": entity,
        "associationType": kind,
        "status": "ASSOCIATED",
        "creationTime": unix_time(share.creation_time),
        "lastUpdatedTime": unix_time(utcnow()),
        "external": False,
    }


def _resource_type(arn: str) -> str:
    m = _RESOURCE_ARN_RE.match(arn)
    if not m:
        return ""
    service = arn.split(":")[2]
    return f"{service}:{m.group(1)}" if service != "ec2" else f"ec2:{m.group(1)}"


async def _create_or_associate(
    request: Request, operation: str, body: bytes, region: str, account_id: str
) -> Response:
    try:
        payload = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return await forward_to_moto_with_body(request, "ram", body, account_id=account_id)
    extra_resources, extra_principals = _split_extras(payload, account_id, region)
    if not extra_resources and not extra_principals:
        return await forward_to_moto_with_body(request, "ram", body, account_id=account_id)
    response = await forward_to_moto_with_body(
        request, "ram", json.dumps(payload).encode(), account_id=account_id
    )
    if response.status_code != 200:
        return response
    data = json.loads(response.body)
    arn = (
        data["resourceShare"]["resourceShareArn"]
        if operation == "createresourceshare"
        else payload.get("resourceShareArn", "")
    )
    share = _find_share(account_id, region, arn)
    if share is None:
        return response
    share.resource_arns.extend(a for a in extra_resources if a not in share.resource_arns)
    share.principals.extend(p for p in extra_principals if p not in share.principals)
    if operation == "associateresourceshare":
        data.setdefault("resourceShareAssociations", [])
        data["resourceShareAssociations"] += [
            _association(share, a, "RESOURCE") for a in extra_resources
        ]
        data["resourceShareAssociations"] += [
            _association(share, p, "PRINCIPAL") for p in extra_principals
        ]
    return _json(data)


def _get_resource_shares_other_accounts(payload: dict, account_id: str, region: str) -> Response:
    names = payload.get("name")
    arns = set(payload.get("resourceShareArns") or [])
    status = payload.get("resourceShareStatus")
    tag_filters = payload.get("tagFilters") or []
    out = []
    for share in shares_visible_to(account_id, region):
        if names and share.name != names:
            continue
        if arns and share.arn not in arns:
            continue
        if status and share.status != status:
            continue
        tags = {t["key"]: t["value"] for t in share.tags}
        if any(tags.get(f.get("tagKey")) not in (f.get("tagValues") or []) for f in tag_filters):
            continue
        described = share.describe()
        out.append(described)
    return _json({"resourceShares": out})


def _list_resources_other_accounts(payload: dict, account_id: str, region: str) -> Response:
    from moto.core.utils import unix_time

    share_arns = set(payload.get("resourceShareArns") or [])
    arn_filter = set(payload.get("resourceArns") or [])
    rtype = payload.get("resourceType")
    out = []
    for share in shares_visible_to(account_id, region):
        if share_arns and share.arn not in share_arns:
            continue
        for arn in share.resource_arns:
            if arn_filter and arn not in arn_filter:
                continue
            t = _resource_type(arn)
            if rtype and t != rtype and t.split(":")[-1] != rtype:
                continue
            out.append(
                {
                    "arn": arn,
                    "type": t,
                    "resourceShareArn": share.arn,
                    "status": "AVAILABLE",
                    "creationTime": unix_time(share.creation_time),
                    "lastUpdatedTime": unix_time(share.last_updated_time),
                    "resourceRegionScope": "REGIONAL",
                }
            )
    return _json({"resources": out})


async def handle_ram_request(request: Request, region: str, account_id: str) -> Response:
    body = await request.body()
    operation = request.url.path.rstrip("/").rsplit("/", 1)[-1].lower()

    if operation in ("getresourceshares", "listresources"):
        try:
            payload = json.loads(body or b"{}")
        except json.JSONDecodeError:
            payload = {}
        if payload.get("resourceOwner") == "OTHER-ACCOUNTS":
            if operation == "getresourceshares":
                return _get_resource_shares_other_accounts(payload, account_id, region)
            return _list_resources_other_accounts(payload, account_id, region)

    if operation in ("createresourceshare", "associateresourceshare"):
        return await _create_or_associate(request, operation, body, region, account_id)

    return await forward_to_moto_with_body(request, "ram", body, account_id=account_id)
