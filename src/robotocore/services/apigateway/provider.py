"""Native API Gateway provider.

Intercepts operations Moto doesn't support:
- DELETE /restapis/{id}/models/{name} (DeleteModel)
- PUT /tags/{arn} (TagResource)
- DELETE /tags/{arn} (UntagResource)
- GET /tags/{arn} (GetTags)
- DELETE /restapis/{id}/stages/{name}/cache/authorizers (FlushStageAuthorizersCache)
- DELETE /restapis/{id}/stages/{name}/cache/data (FlushStageCache)

Delegates everything else to Moto via forward_to_moto.
"""

import json
import re
from urllib.parse import unquote

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto

_DELETE_MODEL_RE = re.compile(r"^/restapis/([^/]+)/models/([^/]+)$")
_TAGS_RE = re.compile(r"^/tags/(.+)$")
_FLUSH_AUTH_CACHE_RE = re.compile(r"^/restapis/([^/]+)/stages/([^/]+)/cache/authorizers$")
_FLUSH_CACHE_RE = re.compile(r"^/restapis/([^/]+)/stages/([^/]+)/cache/data$")

DEFAULT_ACCOUNT_ID = "123456789012"


async def handle_apigateway_request(request: Request, region: str, account_id: str) -> Response:
    """Handle API Gateway requests."""
    path = request.url.path
    method = request.method

    if method == "DELETE":
        match = _DELETE_MODEL_RE.match(path)
        if match:
            return _delete_model(match.group(1), match.group(2), account_id, region)
        match = _FLUSH_AUTH_CACHE_RE.match(path)
        if match:
            return Response(content="{}", status_code=202, media_type="application/json")
        match = _FLUSH_CACHE_RE.match(path)
        if match:
            return Response(content="{}", status_code=202, media_type="application/json")

    # Tag operations: PUT/DELETE/GET /tags/{arn}
    match = _TAGS_RE.match(path)
    if match:
        resource_arn = unquote(match.group(1))
        return await _handle_tags(request, method, resource_arn, account_id, region)

    return await forward_to_moto(request, "apigateway", account_id=account_id)


async def _handle_tags(
    request: Request,
    method: str,
    resource_arn: str,
    account_id: str,
    region: str,
) -> Response:
    """Handle TagResource / UntagResource / GetTags."""
    from moto.backends import get_backend  # noqa: I001

    backend = get_backend("apigateway")[account_id][region]

    # Extract rest api ID from ARN like arn:aws:apigateway:us-east-1::/restapis/{id}
    api_match = re.search(r"/restapis/([^/]+)", resource_arn)
    if not api_match:
        return Response(
            content=json.dumps({"message": "Invalid resource ARN"}),
            status_code=404,
            media_type="application/json",
        )
    api_id = api_match.group(1)

    try:
        rest_api = backend.get_rest_api(api_id)
    except Exception:  # noqa: BLE001
        return Response(
            content=json.dumps({"message": "Invalid API identifier specified"}),
            status_code=404,
            media_type="application/json",
        )

    if not hasattr(rest_api, "tags") or rest_api.tags is None:
        rest_api.tags = {}

    if method == "PUT":
        body = await request.body()
        data = json.loads(body) if body else {}
        tags = data.get("tags", {})
        rest_api.tags.update(tags)
        return Response(content="{}", status_code=204, media_type="application/json")

    elif method == "DELETE":
        # Tag keys come as query params: tagKeys=key1&tagKeys=key2
        tag_keys = request.query_params.getlist("tagKeys")
        for key in tag_keys:
            rest_api.tags.pop(key, None)
        return Response(content="{}", status_code=204, media_type="application/json")

    elif method == "GET":
        return Response(
            content=json.dumps({"tags": rest_api.tags}),
            status_code=200,
            media_type="application/json",
        )

    return Response(status_code=405)


def _delete_model(rest_api_id: str, model_name: str, account_id: str, region: str) -> Response:
    from moto.backends import get_backend  # noqa: I001

    backend = get_backend("apigateway")[account_id][region]
    try:
        rest_api = backend.get_rest_api(rest_api_id)
    except Exception:  # noqa: BLE001
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
