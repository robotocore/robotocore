"""Native EKS provider with mock Kubernetes API server.

REST-JSON protocol. Operations determined by HTTP method + URL path pattern.
Intercepts CreateCluster/DescribeCluster/DeleteCluster to manage mock K8s
servers; everything else forwards to Moto.
"""

import base64
import json
import logging
import re
import threading

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto
from robotocore.services.eks.k8s_mock import K8sMockServer
from robotocore.services.eks.kubeconfig import _FAKE_CA_CERT

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# K8s server registry (cluster_name -> K8sMockServer)
# ---------------------------------------------------------------------------

_k8s_servers: dict[str, K8sMockServer] = {}
_lock = threading.Lock()

# ---------------------------------------------------------------------------
# URL pattern matching for rest-json routing
# ---------------------------------------------------------------------------

_CLUSTER_RE = re.compile(r"^/clusters/(?P<name>[^/]+)$")
_NODEGROUPS_COLLECTION_RE = re.compile(r"^/clusters/(?P<name>[^/]+)/node-groups$")
_NODEGROUP_RE = re.compile(r"^/clusters/(?P<name>[^/]+)/node-groups/(?P<nodegroupName>[^/]+)$")


# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------


async def handle_eks_request(request: Request, region: str, account_id: str) -> Response:
    """Handle an EKS API request (rest-json protocol)."""
    path = request.url.path
    method = request.method.upper()

    try:
        # POST /clusters -> CreateCluster
        if path == "/clusters" and method == "POST":
            return await _create_cluster(request, region, account_id)

        # GET /clusters -> ListClusters
        if path == "/clusters" and method == "GET":
            return await forward_to_moto(request, "eks")

        # Match /clusters/{name}
        m = _CLUSTER_RE.match(path)
        if m:
            if method == "GET":
                return await _describe_cluster(request, region, account_id, m.group("name"))
            if method == "DELETE":
                return await _delete_cluster(request, region, account_id, m.group("name"))

        # Match /clusters/{name}/node-groups
        m = _NODEGROUPS_COLLECTION_RE.match(path)
        if m:
            return await forward_to_moto(request, "eks")

        # Match /clusters/{name}/node-groups/{nodegroupName}
        m = _NODEGROUP_RE.match(path)
        if m:
            return await forward_to_moto(request, "eks")

        # Everything else -> Moto
        return await forward_to_moto(request, "eks")

    except Exception as e:
        logger.exception("EKS provider error: %s", e)
        return _error_response("ServerException", str(e), 500)


# ---------------------------------------------------------------------------
# Intercepted operations
# ---------------------------------------------------------------------------


async def _create_cluster(request: Request, region: str, account_id: str) -> Response:
    """Intercept CreateCluster: forward to Moto, then start a mock K8s server."""
    # Forward to Moto first to get the cluster metadata
    moto_response = await forward_to_moto(request, "eks")

    if moto_response.status_code >= 400:
        return moto_response

    # Parse the Moto response to get cluster details
    body = json.loads(moto_response.body.decode("utf-8"))
    cluster = body.get("cluster", {})
    cluster_name = cluster.get("name", "")

    if not cluster_name:
        return moto_response

    # Start a mock K8s server for this cluster
    server = K8sMockServer()
    port = server.start(cluster_name, port=0)

    with _lock:
        _k8s_servers[cluster_name] = server

    # Patch the response with the real mock K8s endpoint
    endpoint = f"http://localhost:{port}"
    cluster["endpoint"] = endpoint
    cluster["certificateAuthority"] = {
        "data": base64.b64encode(_FAKE_CA_CERT).decode("ascii"),
    }

    patched_body = json.dumps(body)
    return Response(
        content=patched_body,
        status_code=moto_response.status_code,
        media_type="application/json",
    )


async def _describe_cluster(
    request: Request, region: str, account_id: str, cluster_name: str
) -> Response:
    """Intercept DescribeCluster: forward to Moto, patch endpoint + cert."""
    moto_response = await forward_to_moto(request, "eks")

    if moto_response.status_code >= 400:
        return moto_response

    body = json.loads(moto_response.body.decode("utf-8"))
    cluster = body.get("cluster", {})

    with _lock:
        server = _k8s_servers.get(cluster_name)

    if server and server.port:
        cluster["endpoint"] = f"http://localhost:{server.port}"
        cluster["certificateAuthority"] = {
            "data": base64.b64encode(_FAKE_CA_CERT).decode("ascii"),
        }

    patched_body = json.dumps(body)
    return Response(
        content=patched_body,
        status_code=moto_response.status_code,
        media_type="application/json",
    )


async def _delete_cluster(
    request: Request, region: str, account_id: str, cluster_name: str
) -> Response:
    """Intercept DeleteCluster: stop the mock K8s server, then forward to Moto."""
    # Stop the K8s server first
    with _lock:
        server = _k8s_servers.pop(cluster_name, None)

    if server:
        server.stop()

    # Forward to Moto
    return await forward_to_moto(request, "eks")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _error_response(code: str, message: str, status: int) -> Response:
    body = json.dumps({"__type": code, "message": message})
    return Response(content=body, status_code=status, media_type="application/json")
