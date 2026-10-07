"""NetworkManager admin-plane reads for dual-type lookups.

The `awscc` Terraform provider reads AWS resources through the CloudControl API, which the twin
does not serve; an estate tool bridging that provider's data sources therefore asks the twin's
admin plane what NetworkManager holds instead. Read-only: the same core-network records Moto
serves for `ListCoreNetworks`, exposed without SigV4 handling so a stand-in provider can call them.
"""

import logging
from xml.sax.saxutils import escape as xml_escape

from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)


def _account_is_valid(account: str | None) -> bool:
    return bool(account) and str(account).isdigit() and len(str(account)) == 12


async def handle_networkmanager_read(request: Request) -> JSONResponse:
    """GET /_robotocore/networkmanager/core-networks?account=<12 digits>.

    Returns every core network the account holds across partitions — the shape the awscc
    `data.awscc_networkmanager_core_networks` bridge needs (ids/arns/global ids/state).
    """
    account = request.query_params.get("account")
    if not _account_is_valid(account):
        return JSONResponse({"error": "account must be a 12-digit account id"}, status_code=400)

    from moto.networkmanager.models import networkmanager_backends

    networks = []
    for _, by_region in list(networkmanager_backends.items()):
        if _ != account:
            continue
        for _region, backend in list(by_region.items()):
            for cn in list(backend.core_networks.values()):
                networks.append(
                    {
                        "core_network_id": cn.core_network_id,
                        "core_network_arn": cn.core_network_arn,
                        "global_network_id": cn.global_network_id,
                        "state": cn.state,
                    }
                )
    for network in networks:
        logger.debug("networkmanager read: %s", xml_escape(network["core_network_id"]))
    return JSONResponse({"core_networks": networks})
