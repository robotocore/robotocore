"""Native STS provider.

Intercepts operations Moto doesn't support:
- GetAccessKeyInfo

Delegates everything else to Moto via forward_to_moto.
Uses query protocol (Action parameter).
"""

from urllib.parse import parse_qs

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto

DEFAULT_ACCOUNT_ID = "123456789012"


async def handle_sts_request(
    request: Request, region: str, account_id: str
) -> Response:
    """Handle STS API requests."""
    body = await request.body()
    parsed = parse_qs(body.decode(), keep_blank_values=True)
    params = {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}
    action = params.get("Action", "")

    if action == "GetAccessKeyInfo":
        return _get_access_key_info(params, account_id)

    return await forward_to_moto(request, "sts")


def _get_access_key_info(params: dict, account_id: str) -> Response:
    xml = (
        '<GetAccessKeyInfoResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
        "<GetAccessKeyInfoResult>"
        f"<Account>{account_id}</Account>"
        "</GetAccessKeyInfoResult>"
        "<ResponseMetadata>"
        "<RequestId>12345678-1234-1234-1234-123456789012</RequestId>"
        "</ResponseMetadata>"
        "</GetAccessKeyInfoResponse>"
    )
    return Response(content=xml, status_code=200, media_type="text/xml")
