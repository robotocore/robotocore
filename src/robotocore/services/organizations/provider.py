"""Native Organizations provider.

Moto keeps a handshake only in the backend of the account that created it (the management
account), so the invited member's ``AcceptHandshake`` raised ``HandshakeNotFoundException``, and an
accepted invitation never added the member to the organization. In AWS the invited account sees
and accepts the handshake, then becomes a member that can ``DescribeOrganization``.

This provider makes handshakes visible to every party and joins the target account on accept.
Everything else is forwarded to Moto.
"""

import json
import logging

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto_with_body

logger = logging.getLogger(__name__)

_HANDSHAKE_OPS = {"AcceptHandshake", "DeclineHandshake", "DescribeHandshake", "CancelHandshake"}


def _partition(region: str) -> str:
    from moto.utilities.utils import get_partition

    return get_partition(region)


def _find_handshake(handshake_id: str):
    """(backend, handshake) for a handshake id in any account's backend, or None."""
    from moto.organizations.models import organizations_backends

    for _account, by_partition in list(organizations_backends.items()):
        for _partition_name, backend in list(by_partition.items()):
            for handshake in backend.handshakes:
                if handshake.id == handshake_id:
                    return backend, handshake
    return None


def _is_party(handshake, account_id: str) -> bool:
    return any(p.get("Id") == account_id for p in handshake.parties)


def _join_organization(owner_backend, account_id: str, partition: str) -> None:
    """Add an invited, accepting account to the inviting organization (as AWS does on accept)."""
    from moto.organizations import utils
    from moto.organizations.models import FakeAccount, organizations_backends

    org = owner_backend.org
    if org is None or any(a.id == account_id for a in owner_backend.accounts):
        return
    account = FakeAccount(
        org, AccountName=f"account-{account_id}", Email=f"{account_id}@example.com"
    )
    account.id = account_id
    account.joined_method = "INVITED"
    owner_backend.accounts.append(account)
    for policy_id in (utils.DEFAULT_SCP_POLICY_ID, utils.DEFAULT_RCP_POLICY_ID):
        try:
            owner_backend.attach_policy(PolicyId=policy_id, TargetId=account_id)
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "organizations join: attach %s to %s failed: %s", policy_id, account_id, exc
            )
    organizations_backends.master_accounts[account_id] = (owner_backend.account_id, partition)


async def handle_organizations_request(request: Request, region: str, account_id: str) -> Response:
    body = await request.body()
    operation = request.headers.get("x-amz-target", "").rsplit(".", 1)[-1]

    if operation in _HANDSHAKE_OPS:
        try:
            handshake_id = json.loads(body or b"{}").get("HandshakeId", "")
        except json.JSONDecodeError:
            handshake_id = ""
        found = _find_handshake(handshake_id) if handshake_id else None
        if found is not None:
            owner_backend, handshake = found
            from moto.organizations.models import organizations_backends

            caller_backend = organizations_backends[account_id][_partition(region)]
            if caller_backend is not owner_backend and _is_party(handshake, account_id):
                # Let Moto operate on the shared handshake object from the caller's backend.
                if handshake not in caller_backend.handshakes:
                    caller_backend.handshakes.append(handshake)
            response = await forward_to_moto_with_body(
                request, "organizations", body, account_id=account_id
            )
            if (
                operation == "AcceptHandshake"
                and response.status_code == 200
                and handshake.action == "INVITE"
                and handshake.state == "ACCEPTED"
            ):
                target = handshake.parties[1]["Id"]
                if target == account_id:
                    _join_organization(owner_backend, account_id, _partition(region))
            return response

    if operation == "ListHandshakesForAccount":
        from moto.organizations.models import organizations_backends

        caller_backend = organizations_backends[account_id][_partition(region)]
        for _account, by_partition in list(organizations_backends.items()):
            for _partition_name, backend in list(by_partition.items()):
                if backend is caller_backend:
                    continue
                for handshake in backend.handshakes:
                    if (
                        _is_party(handshake, account_id)
                        and handshake not in caller_backend.handshakes
                    ):
                        caller_backend.handshakes.append(handshake)

    return await forward_to_moto_with_body(request, "organizations", body, account_id=account_id)
