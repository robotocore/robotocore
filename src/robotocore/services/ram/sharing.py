"""Cross-account visibility of AWS RAM resource shares.

A share's principals may name an account id, an IAM principal ARN in an account, the owner's
organization ARN, or an organizational-unit ARN. An account can use a shared resource when any
principal covers it: its own id or IAM ARN, the organization it belongs to, or any OU on its path
to the organization root (OUs nest, so membership is transitive).
"""

import re
from collections.abc import Iterator
from typing import Any

_ORG_ARN_RE = re.compile(r"^arn:aws[a-z-]*:organizations::\d{12}:organization/(o-[a-z0-9]+)$")
_OU_ARN_RE = re.compile(r"^arn:aws[a-z-]*:organizations::\d{12}:ou/(o-[a-z0-9]+)/(ou-[a-z0-9-]+)$")
_IAM_ARN_ACCOUNT_RE = re.compile(r"^arn:aws[a-z-]*:iam::(\d{12}):")


def _membership(account_id: str) -> tuple[str | None, set[str]]:
    """(organization id, set of OU ids on the account's path to root) for an account."""
    from moto.organizations.models import organizations_backends

    master = organizations_backends.master_accounts.get(account_id)
    candidates = []
    if master:
        candidates.append(organizations_backends[master[0]][master[1]])
    for _acct, by_partition in list(organizations_backends.items()):
        for _p, backend in list(by_partition.items()):
            if backend.org is not None and backend.account_id == account_id:
                candidates.append(backend)
    for backend in candidates:
        org = backend.org
        if org is None:
            continue
        account = next((a for a in backend.accounts if a.id == account_id), None)
        if account is None:
            return org.id, set()
        ous: set[str] = set()
        parent = account.parent_id
        by_id = {ou.id: ou for ou in backend.ou}
        while parent in by_id:
            ous.add(parent)
            parent = by_id[parent].parent_id
        return org.id, ous
    return None, set()


def principal_covers(principal: str, account_id: str) -> bool:
    if principal == account_id:
        return True
    m = _IAM_ARN_ACCOUNT_RE.match(principal)
    if m:
        return m.group(1) == account_id
    m = _ORG_ARN_RE.match(principal)
    if m:
        org_id, _ = _membership(account_id)
        return org_id == m.group(1)
    m = _OU_ARN_RE.match(principal)
    if m:
        org_id, ous = _membership(account_id)
        return org_id == m.group(1) and m.group(2) in ous
    return False


def shares_visible_to(account_id: str, region: str | None = None) -> Iterator[Any]:
    """ACTIVE resource shares owned by OTHER accounts whose principals cover ``account_id``."""
    from moto.ram.models import ram_backends

    for owner, by_region in list(ram_backends.items()):
        if owner == account_id:
            continue
        for share_region, backend in list(by_region.items()):
            if region and share_region != region:
                continue
            for share in backend.resource_shares:
                if share.status != "ACTIVE":
                    continue
                if any(principal_covers(p, account_id) for p in share.principals):
                    yield share


def resource_shared_with(resource_arn: str, account_id: str) -> bool:
    return any(resource_arn in s.resource_arns for s in shares_visible_to(account_id))
