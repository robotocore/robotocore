"""Native ECR provider.

Intercepts operations that Moto doesn't implement or has bugs:
- BatchCheckLayerAvailability: Not implemented in Moto
- DescribeRepositories: maxResults pagination not enforced
"""

import json

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto


async def handle_ecr_request(request: Request, region: str, account_id: str) -> Response:
    """Handle ECR requests, intercepting unimplemented operations."""
    target = request.headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    body = await request.body()

    if action == "BatchCheckLayerAvailability":
        params = json.loads(body) if body else {}
        digests = params.get("layerDigests", [])
        repo_name = params.get("repositoryName", "")
        registry_id = params.get("registryId", account_id)
        layers = []
        for digest in digests:
            layers.append(
                {
                    "layerDigest": digest,
                    "layerAvailability": "UNAVAILABLE",
                    "repositoryName": repo_name,
                    "registryId": registry_id,
                }
            )
        return Response(
            content=json.dumps({"layers": layers, "failures": []}),
            status_code=200,
            media_type="application/x-amz-json-1.1",
        )

    if action == "DescribeRepositories":
        params = json.loads(body) if body else {}
        max_results = params.get("maxResults")
        if max_results:
            # Forward to Moto, then truncate results
            response = await forward_to_moto(request, "ecr", account_id=account_id)
            resp_body = json.loads(response.body)
            repos = resp_body.get("repositories", [])
            if len(repos) > max_results:
                resp_body["repositories"] = repos[:max_results]
                resp_body["nextToken"] = "pagination-token"
                return Response(
                    content=json.dumps(resp_body),
                    status_code=response.status_code,
                    media_type="application/x-amz-json-1.1",
                )
            return response

    return await forward_to_moto(request, "ecr", account_id=account_id)
