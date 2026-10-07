"""Unit tests for the NetworkManager admin read (core-network listing)."""

import asyncio
import json

import pytest

from robotocore.services.networkmanager.admin import handle_networkmanager_read


@pytest.fixture(autouse=True)
def core_network():
    from moto.networkmanager.models import networkmanager_backends

    account = "555555555555"
    backend = networkmanager_backends[account]["aws"]
    gn = backend.create_global_network(description="twin-gn", tags=[])
    cn = backend.create_core_network(
        global_network_id=gn.global_network_id,
        description=None,
        policy_document=None,
        client_token=None,
        tags=[],
    )
    yield backend, cn
    backend.core_networks.pop(cn.core_network_id, None)


def _run(account):
    """Call the handler with a stub carrying query_params, as the gateway route does."""
    import asyncio
    from types import SimpleNamespace

    req = SimpleNamespace(query_params={"account": account})
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(handle_networkmanager_read(req))
    finally:
        loop.close()


class TestCoreNetworkRead:
    def test_lists_core_network(self, core_network):
        backend, cn = core_network
        resp = _run("555555555555")
        assert resp.status_code == 200
        body = json.loads(asyncio.run(_body(resp)))
        rows = body["core_networks"]
        assert any(row["core_network_id"] == cn.core_network_id for row in rows)
        assert rows and all(row["state"] in ("PENDING", "AVAILABLE") for row in rows)

    def test_rejects_missing_account(self, core_network):
        resp = _run(None)
        assert resp.status_code == 400


async def _body(resp):
    """Render a JSONResponse body (Content-Length set by the response itself)."""
    return resp.body
