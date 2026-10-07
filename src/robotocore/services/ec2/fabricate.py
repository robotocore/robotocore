"""Fabricate resources that exist outside the workloads under test.

Some real estates reference resources by a literal ID (a peer VPC created years ago that
is managed outside Terraform, for example). Applying such a config against a fresh twin
fails at the first call that validates the referenced object, and no AWS API answers
``CreateVpc`` with a caller-chosen id — the reference can only ever match if the twin
already holds an object with that identity. This admin plane creates exactly that:
a real backend object with a caller-determined id, in a named account and region.

Only VPCs are supported today; other referenced-by-literal-identity resources can be
added the same way. The object is built by the real backend, so describe-, peering- and
attach-style callers see the same shape as for a VPC created through the AWS API.
"""

import logging
import re
import uuid
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)

_VPC_ID_RE = re.compile(r"^vpc-[0-9a-f]{8,17}$")


def _cidr_for(seed: str) -> str:
    return f"10.{(uuid.uuid5(uuid.NAMESPACE_DNS, seed).int % 240) + 1}.0.0/16"


class FabricateError(ValueError):
    """Payload validation failure; reported as HTTP 400 with the failing field."""


def _validate_cidr(cidr: str, where: str) -> None:
    """Same bounds the AWS CreateVpc API enforces, before Moto gets a chance to 500."""
    import ipaddress

    try:
        network = ipaddress.ip_network(cidr, strict=False)
    except ValueError:
        raise FabricateError(f"{where} {cidr!r} is not a CIDR block")
    if (
        not isinstance(network, (ipaddress.IPv4Network,))
        or network.prefixlen < 16
        or network.prefixlen > 28
    ):
        raise FabricateError(f"{where} {cidr!r} is not a valid VPC cidr (prefix 16..28)")


def _validate_region(region: str, where: str) -> None:
    import botocore.session

    session = botocore.session.get_session()
    partitions = ("aws", "aws-us-gov", "aws-cn", "aws-iso", "aws-iso-b")
    for partition in partitions:
        if region in session.get_available_regions("ec2", partition_name=partition):
            return
    raise FabricateError(f"{where} {region!r} is not a known AWS region")


def fabricate_resources(items: list[dict]) -> tuple[list[dict], list[dict]]:
    """Create each requested object; returns ``(fabricated, skipped)``.

    An item whose id already exists is skipped as is — fabrication is idempotent.
    Raises :class:`FabricateError` describing the first invalid item.
    """
    from moto.core.exceptions import ServiceException as MotoServiceError
    from moto.ec2.models import ec2_backends

    fabricated: list[dict] = []
    skipped: list[dict] = []

    for i, item in enumerate(items):
        where = f"resources[{i}]"
        if not isinstance(item, dict):
            raise FabricateError(f"{where} must be an object")
        account = item.get("account")
        region = item.get("region") or "us-east-1"
        kind = item.get("kind") or "vpc"
        ident = item.get("id")
        cidr = item.get("cidr_block")
        tags = item.get("tags") or {}
        if kind != "vpc":
            raise FabricateError(f"{where}.kind {kind!r} is not supported")
        if not account or not (len(str(account)) == 12 and str(account).isdigit()):
            raise FabricateError(f"{where}.account must be a 12-digit account id")
        if not isinstance(ident, str) or not _VPC_ID_RE.match(ident):
            raise FabricateError(f"{where}.id {ident!r} is not a vpc id (vpc-hex)")
        if cidr is not None and not isinstance(cidr, str):
            raise FabricateError(f"{where}.cidr_block must be a string")
        if not isinstance(tags, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in tags.items()
        ):
            raise FabricateError(f"{where}.tags must be an object of string keys and values")
        _validate_region(region, f"{where}.region")
        if cidr is not None:
            _validate_cidr(cidr, f"{where}.cidr_block")
        backend = ec2_backends[account][region]
        if ident in backend.vpcs:
            skipped.append({"kind": "vpc", "vpc_id": ident, "account": account, "region": region})
            continue
        cidr = cidr or _cidr_for(f"{account}/{region}/{ident}")
        try:
            vpc = backend.create_vpc(cidr_block=cidr)
        except MotoServiceError as e:
            raise FabricateError(f"{where}.cidr_block: {e}") from e
        for key, value in tags.items():
            vpc.add_tag(key, value)
        _rebind_vpc_id(backend, vpc, ident)
        logger.info("fabricated vpc %s in %s/%s (cidr %s)", ident, account, region, cidr)
        fabricated.append(
            {"vpc_id": ident, "account": account, "region": region, "cidr_block": cidr}
        )
    return fabricated, skipped


def _rebind_vpc_id(backend: Any, vpc: Any, ident: str) -> None:
    """Give a backend-created VPC a caller-chosen id, everywhere the id is stored.

    The VPC is created through the ordinary call (route table, main network ACL and default
    security group with AWS's real behaviour), then every object that references the generated id
    is re-bound. Unlike patching the id generator for the create call, re-binding afterwards
    touches only the fabricated object's own state: a concurrent CreateVpc cannot land on the
    caller-chosen id, whichever thread serves it.
    """
    old_id = vpc.id
    if old_id == ident:
        return
    backend.vpcs.pop(old_id, None)
    vpc.id = ident
    backend.vpcs[ident] = vpc
    for table in list(getattr(backend, "route_tables", {}).values()):
        if table.vpc_id == old_id:
            table.vpc_id = ident
    for acl in list(getattr(backend, "network_acls", {}).values()):
        if acl.vpc_id == old_id:
            acl.vpc_id = ident
    # Security groups are stored nested by vpc id (`groups[vpc_id][group_id]`), so move the
    # nested entries under the new id; the objects' own `vpc_id` follows.
    groups = getattr(backend, "groups", None)
    if isinstance(groups, dict) and old_id in groups:
        moved = groups.pop(old_id)
        for group in moved.values():
            group.vpc_id = ident
        groups[ident] = moved
    # The tag index is keyed by resource id; the stored tag objects move with it unchanged.
    tags = getattr(backend, "tags", None)
    if isinstance(tags, dict) and old_id in tags:
        moved_tags = tags.pop(old_id)
        tags[ident] = moved_tags


async def handle_fabricate(request: Request) -> JSONResponse:
    """POST /_robotocore/ec2/fabricate with {resources: [{account, region?, id, ...}]}."""
    try:
        body = await request.json()
    except ValueError:
        return JSONResponse({"error": "body must be JSON"}, status_code=400)
    items = body.get("resources") if isinstance(body, dict) else None
    if not isinstance(items, list) or not items:
        return JSONResponse({"error": "resources must be a non-empty array"}, status_code=400)
    try:
        made, skipped = fabricate_resources(items)
    except FabricateError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    return JSONResponse({"fabricated": made, "skipped": skipped})
