"""Native ACM provider.

Intercepts operations that Moto doesn't implement:
- UpdateCertificateOptions: Not implemented in Moto
"""

import json

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto


async def handle_acm_request(request: Request, region: str, account_id: str) -> Response:
    """Handle ACM requests, intercepting unimplemented operations."""
    target = request.headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    if action == "UpdateCertificateOptions":
        body = await request.body()
        params = json.loads(body) if body else {}
        cert_arn = params.get("CertificateArn", "")
        options = params.get("Options", {})

        from moto.backends import get_backend  # noqa: I001

        backend = get_backend("acm")[account_id][region]
        if cert_arn in backend._certificates:
            cert = backend._certificates[cert_arn]
            cert.cert_options.update(options)

        return Response(
            content="{}",
            status_code=200,
            media_type="application/x-amz-json-1.1",
        )

    return await forward_to_moto(request, "acm", account_id=account_id)
