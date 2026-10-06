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
import uuid

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto
from robotocore.services.eks.k8s_mock import K8sMockServer
from robotocore.services.eks.kubeconfig import _FAKE_CA_CERT

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# K8s server registry  (region:account:cluster_name -> K8sMockServer)
# ---------------------------------------------------------------------------

_k8s_servers: dict[str, K8sMockServer] = {}
_lock = threading.Lock()


def _server_key(region: str, account_id: str, cluster_name: str) -> str:
    return f"{region}:{account_id}:{cluster_name}"


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
            return await forward_to_moto(request, "eks", account_id=account_id)

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
            return await forward_to_moto(request, "eks", account_id=account_id)

        # Match /clusters/{name}/node-groups/{nodegroupName}
        m = _NODEGROUP_RE.match(path)
        if m:
            return await forward_to_moto(request, "eks", account_id=account_id)

        # Pod identity associations: keep the fields Moto drops
        if _POD_IDENTITY_RE.match(path):
            return await _pod_identity(request, method, account_id)

        # Everything else -> Moto
        return await forward_to_moto(request, "eks", account_id=account_id)

    except Exception as e:
        logger.exception("EKS provider error: %s", e)
        return _error_response("ServerException", str(e), 500)


# ---------------------------------------------------------------------------
# Intercepted operations
# ---------------------------------------------------------------------------


_POD_IDENTITY_RE = re.compile(r"^/clusters/[^/]+/pod-identity-associations(/[^/]+)?$")
# association id -> (disableSessionTags, externalId)
_pod_identity_extra: dict[str, tuple[bool, str]] = {}


async def _pod_identity(request: Request, method: str, account_id: str) -> Response:
    """Return disableSessionTags/externalId on pod identity associations, as AWS does.

    Moto omits both, so Terraform re-plans every aws_eks_pod_identity_association right after a
    clean apply. Values come from Create/Update (externalId is AWS-generated, stable per
    association; disableSessionTags defaults to false).
    """
    body = await request.body()
    try:
        sent = json.loads(body) if body else {}
    except json.JSONDecodeError:
        sent = {}
    response = await forward_to_moto(request, "eks", account_id=account_id)
    if response.status_code >= 300 or not response.body:
        return response
    try:
        data = json.loads(response.body)
    except json.JSONDecodeError:
        return response
    assoc = data.get("association")
    if isinstance(assoc, dict) and assoc.get("associationId"):
        aid = assoc["associationId"]
        disable, ext = _pod_identity_extra.get(aid, (False, str(uuid.uuid4())))
        if method in ("POST",) and "disableSessionTags" in sent:
            disable = bool(sent["disableSessionTags"])
        if method == "DELETE":
            _pod_identity_extra.pop(aid, None)
        else:
            _pod_identity_extra[aid] = (disable, ext)
        assoc.setdefault("disableSessionTags", disable)
        assoc["disableSessionTags"] = disable
        assoc.setdefault("externalId", ext)
    headers = {k: v for k, v in response.headers.items() if k.lower() != "content-length"}
    return Response(content=json.dumps(data), status_code=response.status_code, headers=headers)


async def _create_cluster(request: Request, region: str, account_id: str) -> Response:
    """Intercept CreateCluster: forward to Moto, then start a mock K8s server."""
    # Forward to Moto first to get the cluster metadata
    moto_response = await forward_to_moto(request, "eks", account_id=account_id)

    if moto_response.status_code >= 400:
        return moto_response

    # Parse the Moto response to get cluster details
    body = json.loads(moto_response.body.decode("utf-8"))
    cluster = body.get("cluster", {})
    cluster_name = cluster.get("name", "")

    if not cluster_name:
        return moto_response

    # Start a mock K8s server for this cluster
    key = _server_key(region, account_id, cluster_name)
    try:
        server = K8sMockServer()
        port = server.start(cluster_name, port=0)
    except RuntimeError:
        logger.exception("Failed to start mock K8s server for cluster %s", cluster_name)
        # Return the Moto response as-is; cluster exists but no mock server
        return moto_response

    with _lock:
        # Stop any previous server for this key (shouldn't happen, but be safe)
        old = _k8s_servers.pop(key, None)
        if old:
            old.stop()
        _k8s_servers[key] = server

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
    moto_response = await forward_to_moto(request, "eks", account_id=account_id)

    if moto_response.status_code >= 400:
        return moto_response

    body = json.loads(moto_response.body.decode("utf-8"))
    cluster = body.get("cluster", {})

    key = _server_key(region, account_id, cluster_name)
    with _lock:
        server = _k8s_servers.get(key)

    if server and server.port and server.is_running:
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
    key = _server_key(region, account_id, cluster_name)
    with _lock:
        server = _k8s_servers.pop(key, None)

    if server:
        server.stop()

    # Forward to Moto
    return await forward_to_moto(request, "eks", account_id=account_id)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _error_response(code: str, message: str, status: int) -> Response:
    body = json.dumps({"__type": code, "message": message})
    return Response(content=body, status_code=status, media_type="application/json")
