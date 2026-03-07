"""Native API Gateway provider.

Intercepts operations Moto doesn't support:
- DELETE /restapis/{id}/models/{name} (DeleteModel)

Delegates everything else to Moto via forward_to_moto.
"""

import json
import re

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto

_DELETE_MODEL_RE = re.compile(r"^/restapis/([^/]+)/models/([^/]+)$")

DEFAULT_ACCOUNT_ID = "123456789012"


async def handle_apigateway_request(
    request: Request, region: str, account_id: str
) -> Response:
    """Handle API Gateway requests."""
    path = request.url.path
    method = request.method

    if method == "DELETE":
        match = _DELETE_MODEL_RE.match(path)
        if match:
            rest_api_id = match.group(1)
            model_name = match.group(2)
            return _delete_model(rest_api_id, model_name, account_id, region)

    return await forward_to_moto(request, "apigateway")


def _delete_model(
    rest_api_id: str, model_name: str, account_id: str, region: str
) -> Response:
    from moto.backends import get_backend

    backend = get_backend("apigateway")[account_id][region]
    try:
        rest_api = backend.get_rest_api(rest_api_id)
    except Exception:
        return Response(
            content=json.dumps({"message": "Invalid API identifier specified"}),
            status_code=404,
            media_type="application/json",
        )

    if model_name in rest_api.models:
        del rest_api.models[model_name]
        return Response(content="{}", status_code=202, media_type="application/json")

    return Response(
        content=json.dumps({"message": f"Invalid Model Name specified: {model_name}"}),
        status_code=404,
        media_type="application/json",
    )
