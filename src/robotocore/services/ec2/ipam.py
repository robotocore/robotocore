"""IPAM behaviours Moto lacks: pool filters, RAM-shared pools, and VPCs allocated from a pool.

- ``DescribeIpamPools`` applies ``Filter.N`` (Moto ignores filters) and includes pools that other
  accounts share with the caller through AWS RAM, as AWS does for organization-wide IPAM. Filter
  values honour the EC2 ``*`` / ``?`` wildcards, so ``description=*public-ingress*`` finds a pool
  the way the ``aws_vpc_ipam_pool`` Terraform data source expects.
- ``CreateVpc`` with ``Ipv4IpamPoolId`` (+ ``Ipv4NetmaskLength`` or an explicit ``CidrBlock``)
  allocates a non-overlapping CIDR from the pool's provisioned space and records the allocation,
  instead of failing with "Value (None) for parameter cidrBlock is invalid".
"""

import fnmatch
import ipaddress
import logging
import uuid
from typing import Any
from xml.sax.saxutils import escape as xml_escape

from starlette.responses import Response

logger = logging.getLogger(__name__)

_NS = "http://ec2.amazonaws.com/doc/2016-11-15/"


def _filters(params: dict) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    i = 1
    while f"Filter.{i}.Name" in params:
        name = params[f"Filter.{i}.Name"][0]
        vals = []
        j = 1
        while f"Filter.{i}.Value.{j}" in params:
            vals.append(params[f"Filter.{i}.Value.{j}"][0])
            j += 1
        out[name] = vals
        i += 1
    return out


def _value_matches(value: str, patterns: list[str]) -> bool:
    """EC2 filter semantics: a value matches if it equals any pattern, where ``*`` matches any
    run of characters and ``?`` exactly one. Case-sensitive, as on AWS."""
    return any(fnmatch.fnmatchcase(value, pattern) for pattern in patterns)


def _ids(params: dict, prefix: str) -> list[str]:
    out, i = [], 1
    while f"{prefix}.{i}" in params:
        out.append(params[f"{prefix}.{i}"][0])
        i += 1
    return out


def _all_pools():
    """(backend, pool) for every IPAM pool in every account and region."""
    from moto.ec2.models import ec2_backends

    for _acct, by_region in list(ec2_backends.items()):
        for _region, backend in list(by_region.items()):
            for pool in list(getattr(backend, "ipam_pools", {}).values()):
                yield backend, pool


def _visible_pools(account_id: str, region: str):
    from robotocore.services.ram.sharing import resource_shared_with

    for backend, pool in _all_pools():
        if backend.account_id == account_id:
            if backend.region_name == region:
                yield backend, pool
        elif resource_shared_with(pool.arn, account_id):
            yield backend, pool


def find_pool(pool_id: str, account_id: str, region: str):
    for backend, pool in _visible_pools(account_id, region):
        if pool.id == pool_id:
            return backend, pool
    # Shared pools are usable from any region in their locale.
    for backend, pool in _all_pools():
        if pool.id == pool_id and (backend.account_id == account_id or _shared(pool, account_id)):
            return backend, pool
    return None


def _shared(pool, account_id: str) -> bool:
    from robotocore.services.ram.sharing import resource_shared_with

    return resource_shared_with(pool.arn, account_id)


def _matches(pool, backend, filters: dict[str, list[str]]) -> bool:
    # TaggedEC2Resource.get_tags() returns describe_tags dicts ({"key": ..., "value": ...}).
    tags = {t["key"]: t["value"] for t in pool.get_tags()}
    fields = {
        "ipam-pool-id": pool.id,
        "ipam-pool-arn": pool.arn,
        "ipam-scope-id": pool.ipam_scope_id,
        "description": pool.description or "",
        "locale": pool.locale or "",
        "address-family": pool.address_family,
        "state": pool.state,
        "owner-id": backend.account_id,
        "source-ipam-pool-id": pool.source_ipam_pool_id or "",
        "pool-depth": str(pool.pool_depth),
        "auto-import": str(bool(pool.auto_import)).lower(),
    }
    for name, values in filters.items():
        if name.startswith("tag:"):
            if not _value_matches(tags.get(name[4:], ""), values):
                return False
        elif name == "tag-key":
            if not any(_value_matches(key, values) for key in tags):
                return False
        elif name in fields:
            if not _value_matches(fields[name], values):
                return False
    return True


def _scope_info(backend, scope_id: str) -> tuple[str, str, str, str]:
    """(scope arn, scope type, ipam arn, ipam region) for a pool's scope, best effort."""
    scope = getattr(backend, "ipam_scopes", {}).get(scope_id)
    if scope is None:
        return "", "private", "", backend.region_name
    ipam = getattr(backend, "ipams", {}).get(scope.ipam_id)
    ipam_arn = getattr(ipam, "arn", "") if ipam else ""
    return getattr(scope, "arn", ""), scope.scope_type, ipam_arn, backend.region_name


def _item(backend, pool) -> str:
    scope_arn, scope_type, ipam_arn, ipam_region = _scope_info(backend, pool.ipam_scope_id)
    parts = [
        "<item>",
        f"<ownerId>{backend.account_id}</ownerId>",
        f"<ipamPoolId>{pool.id}</ipamPoolId>",
        f"<ipamPoolArn>{xml_escape(pool.arn)}</ipamPoolArn>",
        f"<ipamScopeArn>{xml_escape(scope_arn)}</ipamScopeArn>",
        f"<ipamScopeType>{scope_type}</ipamScopeType>",
        f"<ipamArn>{xml_escape(ipam_arn)}</ipamArn>",
        f"<ipamRegion>{ipam_region}</ipamRegion>",
        f"<locale>{xml_escape(pool.locale or '')}</locale>",
        f"<poolDepth>{pool.pool_depth}</poolDepth>",
        f"<state>{pool.state}</state>",
        f"<description>{xml_escape(pool.description or '')}</description>",
        f"<autoImport>{str(bool(pool.auto_import)).lower()}</autoImport>",
        f"<publiclyAdvertisable>{str(bool(pool.publicly_advertisable)).lower()}</publiclyAdvertisable>",
        f"<addressFamily>{pool.address_family}</addressFamily>",
        f"<allocationMinNetmaskLength>{pool.allocation_min_netmask_length}</allocationMinNetmaskLength>",
        f"<allocationMaxNetmaskLength>{pool.allocation_max_netmask_length}</allocationMaxNetmaskLength>",
    ]
    if pool.allocation_default_netmask_length:
        parts.append(
            f"<allocationDefaultNetmaskLength>{pool.allocation_default_netmask_length}</allocationDefaultNetmaskLength>"
        )
    if pool.source_ipam_pool_id:
        parts.append(f"<sourceIpamPoolId>{pool.source_ipam_pool_id}</sourceIpamPoolId>")
    parts.append("<tagSet>")
    for t in pool.get_tags():
        parts.append(
            f"<item><key>{xml_escape(t.key)}</key><value>{xml_escape(t.value)}</value></item>"
        )
    parts.append("</tagSet></item>")
    return "".join(parts)


def describe_ipam_pools(params: dict, region: str, account_id: str) -> Response:
    filters = _filters(params)
    wanted = set(_ids(params, "IpamPoolId"))
    items = [
        _item(b, p)
        for b, p in _visible_pools(account_id, region)
        if (not wanted or p.id in wanted) and _matches(p, b, filters)
    ]
    xml = (
        f'<?xml version="1.0" encoding="UTF-8"?><DescribeIpamPoolsResponse xmlns="{_NS}">'
        f"<requestId>{uuid.uuid4()}</requestId><ipamPoolSet>{''.join(items)}</ipamPoolSet>"
        "</DescribeIpamPoolsResponse>"
    )
    return Response(content=xml, status_code=200, media_type="text/xml")


def _pool_space(pool, backend) -> list[ipaddress.IPv4Network | ipaddress.IPv6Network]:
    """Provisioned CIDRs of a pool, else its source pool's space (when provisioned by size)."""
    nets = []
    for c in pool.cidrs:
        if c.cidr:
            try:
                nets.append(ipaddress.ip_network(c.cidr, strict=False))
            except ValueError as exc:
                logger.debug("ipam: bad pool cidr %s: %s", c.cidr, exc)
    if not nets and pool.source_ipam_pool_id:
        parent = backend.ipam_pools.get(pool.source_ipam_pool_id)
        if parent is not None:
            nets = _pool_space(parent, backend)
    return nets


def allocate(
    pool,
    backend,
    netmask_length: int | None,
    cidr: str | None,
    resource_id: str,
    owner: str,
    region: str,
) -> str | None:
    """Reserve a CIDR in ``pool`` (explicit or the first free block of ``/netmask_length``)."""
    from moto.ec2.models.ipam import IpamPoolAllocation

    taken = []
    for a in pool.allocations.values():
        try:
            taken.append(ipaddress.ip_network(a.cidr, strict=False))
        except (ValueError, TypeError) as exc:
            logger.debug("ipam: skipping allocation %s: %s", getattr(a, "cidr", None), exc)
    chosen = None
    if cidr:
        chosen = ipaddress.ip_network(cidr, strict=False)
    else:
        length = netmask_length or pool.allocation_default_netmask_length or 16
        for space in _pool_space(pool, backend):
            if length < space.prefixlen:
                continue
            for candidate in space.subnets(new_prefix=length):
                if not any(candidate.overlaps(t) for t in taken):
                    chosen = candidate
                    break
            if chosen:
                break
    if chosen is None:
        return None
    alloc = IpamPoolAllocation(
        cidr=str(chosen), ipam_pool_allocation_id=f"ipam-pool-alloc-{uuid.uuid4().hex[:17]}"
    )
    alloc.resource_type = "vpc"
    alloc.resource_id = resource_id
    alloc.resource_region = region
    alloc.resource_owner = owner
    pool.allocations[alloc.ipam_pool_allocation_id] = alloc
    return str(chosen)


def prepare_create_vpc(
    params: dict, region: str, account_id: str
) -> tuple[dict, Any] | Response | None:
    """CidrBlock for a CreateVpc naming Ipv4IpamPoolId: (params, pool), an error, or None."""
    pool_id = (params.get("Ipv4IpamPoolId") or [""])[0]
    if not pool_id:
        return None
    found = find_pool(pool_id, account_id, region)
    if found is None:
        from robotocore.services.ec2.provider import _ec2_error

        return _ec2_error("InvalidIpamPoolId.NotFound", f"The pool ID '{pool_id}' does not exist")
    backend, pool = found
    netmask = (params.get("Ipv4NetmaskLength") or [None])[0]
    cidr = allocate(
        pool,
        backend,
        int(netmask) if netmask else None,
        (params.get("CidrBlock") or [None])[0],
        resource_id="",
        owner=account_id,
        region=region,
    )
    if cidr is None:
        from robotocore.services.ec2.provider import _ec2_error

        return _ec2_error(
            "InsufficientCidrBlocks",
            f"The specified IPAM pool {pool_id} has no free CIDR of that size",
        )
    new = {k: v for k, v in params.items() if k not in ("Ipv4IpamPoolId", "Ipv4NetmaskLength")}
    new["CidrBlock"] = [cidr]
    return new, pool


def describe_ipam_scopes(params: dict, region: str, account_id: str) -> Response:
    """DescribeIpamScopes honouring IpamScopeId.N and Filter.N (Moto ignores both)."""
    from moto.ec2.models import ec2_backends

    backend = ec2_backends[account_id][region]
    wanted = set(_ids(params, "IpamScopeId"))
    filters = _filters(params)
    items = []
    for scope in list(getattr(backend, "ipam_scopes", {}).values()):
        if wanted and scope.id not in wanted:
            continue
        fields = {
            "ipam-scope-id": scope.id,
            "ipam-id": scope.ipam_id,
            "ipam-scope-type": scope.scope_type,
            "is-default": str(bool(scope.is_default)).lower(),
            "description": scope.description or "",
        }
        if any(n in fields and not _value_matches(fields[n], v) for n, v in filters.items()):
            continue
        ipam = backend.ipams.get(scope.ipam_id)
        items.append(
            "<item>"
            f"<ownerId>{backend.account_id}</ownerId>"
            f"<ipamScopeId>{scope.id}</ipamScopeId>"
            f"<ipamScopeArn>{xml_escape(scope.arn)}</ipamScopeArn>"
            f"<ipamArn>{xml_escape(getattr(ipam, 'arn', ''))}</ipamArn>"
            f"<ipamRegion>{backend.region_name}</ipamRegion>"
            f"<ipamScopeType>{scope.scope_type}</ipamScopeType>"
            f"<isDefault>{str(bool(scope.is_default)).lower()}</isDefault>"
            f"<description>{xml_escape(scope.description or '')}</description>"
            f"<poolCount>{scope.pool_count}</poolCount>"
            f"<state>{scope.state}</state><tagSet/></item>"
        )
    xml = (
        f'<?xml version="1.0" encoding="UTF-8"?><DescribeIpamScopesResponse xmlns="{_NS}">'
        f"<requestId>{uuid.uuid4()}</requestId><ipamScopeSet>{''.join(items)}</ipamScopeSet>"
        "</DescribeIpamScopesResponse>"
    )
    return Response(content=xml, status_code=200, media_type="text/xml")
