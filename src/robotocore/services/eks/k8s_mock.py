"""Lightweight mock Kubernetes API server using Starlette.

Provides a minimal K8s API that supports pods, services, deployments, and namespaces.
Each EKS cluster gets its own K8sMockServer instance running on a random port.
"""

import logging
import threading
import time
import uuid

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _new_uid() -> str:
    return str(uuid.uuid4())


class K8sMockServer:
    """A mock Kubernetes API server backed by in-memory stores."""

    def __init__(self) -> None:
        self.cluster_name: str = ""
        self.port: int = 0
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None

        # In-memory stores keyed by (namespace, name)
        self._namespaces: dict[str, dict] = {}
        self._pods: dict[tuple[str, str], dict] = {}
        self._services: dict[tuple[str, str], dict] = {}
        self._deployments: dict[tuple[str, str], dict] = {}

        # Always have a default namespace
        self._namespaces["default"] = self._make_namespace("default")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self, cluster_name: str, port: int = 0) -> int:
        """Start the mock K8s API server in a background thread.

        Returns the actual port the server is listening on.
        """
        self.cluster_name = cluster_name
        app = self._build_app()

        config = uvicorn.Config(
            app=app,
            host="0.0.0.0",
            port=port,
            log_level="error",
        )
        self._server = uvicorn.Server(config)

        self._thread = threading.Thread(
            target=self._server.run,
            name=f"k8s-mock-{cluster_name}",
            daemon=True,
        )
        self._thread.start()

        # Wait for the server to bind and determine the actual port
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if self._server.started:
                break
            time.sleep(0.05)

        # Extract the actual port from the server sockets
        if self._server.servers:
            for server in self._server.servers:
                for sock in server.sockets:
                    self.port = sock.getsockname()[1]
                    break
                if self.port:
                    break

        logger.info("K8s mock server for %s started on port %d", cluster_name, self.port)
        return self.port

    def stop(self) -> None:
        """Signal shutdown and wait for the server thread to finish."""
        if self._server:
            self._server.should_exit = True
        if self._thread:
            self._thread.join(timeout=5.0)
            self._thread = None
        self._server = None
        logger.info("K8s mock server for %s stopped", self.cluster_name)

    # ------------------------------------------------------------------
    # Starlette app
    # ------------------------------------------------------------------

    def _build_app(self) -> Starlette:
        routes = [
            # Discovery
            Route("/api", self._handle_api, methods=["GET"]),
            Route("/api/v1", self._handle_api_v1, methods=["GET"]),
            # Namespaces
            Route("/api/v1/namespaces", self._handle_namespaces, methods=["GET", "POST"]),
            # Pods
            Route(
                "/api/v1/namespaces/{ns}/pods",
                self._handle_pods_collection,
                methods=["GET", "POST"],
            ),
            Route(
                "/api/v1/namespaces/{ns}/pods/{name}",
                self._handle_pod,
                methods=["GET", "DELETE"],
            ),
            # Services
            Route(
                "/api/v1/namespaces/{ns}/services",
                self._handle_services_collection,
                methods=["GET", "POST"],
            ),
            Route(
                "/api/v1/namespaces/{ns}/services/{name}",
                self._handle_service,
                methods=["GET", "DELETE"],
            ),
            # Deployments
            Route(
                "/apis/apps/v1/namespaces/{ns}/deployments",
                self._handle_deployments_collection,
                methods=["GET", "POST"],
            ),
            Route(
                "/apis/apps/v1/namespaces/{ns}/deployments/{name}",
                self._handle_deployment,
                methods=["GET", "DELETE"],
            ),
        ]
        return Starlette(routes=routes)

    # ------------------------------------------------------------------
    # Discovery endpoints
    # ------------------------------------------------------------------

    async def _handle_api(self, request: Request) -> JSONResponse:
        return JSONResponse(
            {
                "kind": "APIVersions",
                "versions": ["v1"],
                "serverAddressByClientCIDRs": [
                    {"clientCIDR": "0.0.0.0/0", "serverAddress": f"localhost:{self.port}"}
                ],
            }
        )

    async def _handle_api_v1(self, request: Request) -> JSONResponse:
        return JSONResponse(
            {
                "kind": "APIResourceList",
                "groupVersion": "v1",
                "resources": [
                    {
                        "name": "namespaces",
                        "singularName": "namespace",
                        "namespaced": False,
                        "kind": "Namespace",
                        "verbs": ["create", "delete", "get", "list"],
                    },
                    {
                        "name": "pods",
                        "singularName": "pod",
                        "namespaced": True,
                        "kind": "Pod",
                        "verbs": ["create", "delete", "get", "list"],
                    },
                    {
                        "name": "services",
                        "singularName": "service",
                        "namespaced": True,
                        "kind": "Service",
                        "verbs": ["create", "delete", "get", "list"],
                    },
                ],
            }
        )

    # ------------------------------------------------------------------
    # Namespaces
    # ------------------------------------------------------------------

    async def _handle_namespaces(self, request: Request) -> JSONResponse:
        if request.method == "GET":
            items = list(self._namespaces.values())
            return JSONResponse(
                {
                    "apiVersion": "v1",
                    "kind": "NamespaceList",
                    "metadata": {},
                    "items": items,
                }
            )

        # POST - create namespace
        body = await request.json()
        name = body.get("metadata", {}).get("name", "")
        if not name:
            return JSONResponse(
                {"kind": "Status", "status": "Failure", "message": "name is required"},
                status_code=400,
            )
        if name in self._namespaces:
            return JSONResponse(
                {
                    "kind": "Status",
                    "status": "Failure",
                    "message": f'namespaces "{name}" already exists',
                    "reason": "AlreadyExists",
                },
                status_code=409,
            )
        ns = self._make_namespace(name)
        self._namespaces[name] = ns
        return JSONResponse(ns, status_code=201)

    # ------------------------------------------------------------------
    # Pods
    # ------------------------------------------------------------------

    async def _handle_pods_collection(self, request: Request) -> JSONResponse:
        ns = request.path_params["ns"]
        if request.method == "GET":
            items = [v for (n, _), v in self._pods.items() if n == ns]
            return JSONResponse(
                {"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": items}
            )

        # POST - create pod
        body = await request.json()
        name = body.get("metadata", {}).get("name", "")
        if not name:
            return JSONResponse(
                {"kind": "Status", "status": "Failure", "message": "name is required"},
                status_code=400,
            )
        key = (ns, name)
        if key in self._pods:
            return JSONResponse(
                {
                    "kind": "Status",
                    "status": "Failure",
                    "message": f'pods "{name}" already exists',
                    "reason": "AlreadyExists",
                },
                status_code=409,
            )
        pod = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": ns,
                "uid": _new_uid(),
                "creationTimestamp": _now_iso(),
            },
            "spec": body.get("spec", {}),
            "status": {"phase": "Running"},
        }
        self._pods[key] = pod
        return JSONResponse(pod, status_code=201)

    async def _handle_pod(self, request: Request) -> JSONResponse:
        ns = request.path_params["ns"]
        name = request.path_params["name"]
        key = (ns, name)

        if request.method == "GET":
            pod = self._pods.get(key)
            if not pod:
                return JSONResponse(
                    {
                        "kind": "Status",
                        "status": "Failure",
                        "message": f'pods "{name}" not found',
                        "reason": "NotFound",
                    },
                    status_code=404,
                )
            return JSONResponse(pod)

        # DELETE
        pod = self._pods.pop(key, None)
        if not pod:
            return JSONResponse(
                {
                    "kind": "Status",
                    "status": "Failure",
                    "message": f'pods "{name}" not found',
                    "reason": "NotFound",
                },
                status_code=404,
            )
        return JSONResponse(
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": pod["metadata"],
                "status": {"phase": "Succeeded"},
            }
        )

    # ------------------------------------------------------------------
    # Services
    # ------------------------------------------------------------------

    async def _handle_services_collection(self, request: Request) -> JSONResponse:
        ns = request.path_params["ns"]
        if request.method == "GET":
            items = [v for (n, _), v in self._services.items() if n == ns]
            return JSONResponse(
                {"apiVersion": "v1", "kind": "ServiceList", "metadata": {}, "items": items}
            )

        # POST - create service
        body = await request.json()
        name = body.get("metadata", {}).get("name", "")
        if not name:
            return JSONResponse(
                {"kind": "Status", "status": "Failure", "message": "name is required"},
                status_code=400,
            )
        key = (ns, name)
        if key in self._services:
            return JSONResponse(
                {
                    "kind": "Status",
                    "status": "Failure",
                    "message": f'services "{name}" already exists',
                    "reason": "AlreadyExists",
                },
                status_code=409,
            )
        svc = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": name,
                "namespace": ns,
                "uid": _new_uid(),
                "creationTimestamp": _now_iso(),
            },
            "spec": body.get("spec", {}),
        }
        self._services[key] = svc
        return JSONResponse(svc, status_code=201)

    async def _handle_service(self, request: Request) -> JSONResponse:
        ns = request.path_params["ns"]
        name = request.path_params["name"]
        key = (ns, name)

        if request.method == "GET":
            svc = self._services.get(key)
            if not svc:
                return JSONResponse(
                    {
                        "kind": "Status",
                        "status": "Failure",
                        "message": f'services "{name}" not found',
                        "reason": "NotFound",
                    },
                    status_code=404,
                )
            return JSONResponse(svc)

        # DELETE
        svc = self._services.pop(key, None)
        if not svc:
            return JSONResponse(
                {
                    "kind": "Status",
                    "status": "Failure",
                    "message": f'services "{name}" not found',
                    "reason": "NotFound",
                },
                status_code=404,
            )
        return JSONResponse(svc)

    # ------------------------------------------------------------------
    # Deployments
    # ------------------------------------------------------------------

    async def _handle_deployments_collection(self, request: Request) -> JSONResponse:
        ns = request.path_params["ns"]
        if request.method == "GET":
            items = [v for (n, _), v in self._deployments.items() if n == ns]
            return JSONResponse(
                {
                    "apiVersion": "apps/v1",
                    "kind": "DeploymentList",
                    "metadata": {},
                    "items": items,
                }
            )

        # POST - create deployment
        body = await request.json()
        name = body.get("metadata", {}).get("name", "")
        if not name:
            return JSONResponse(
                {"kind": "Status", "status": "Failure", "message": "name is required"},
                status_code=400,
            )
        key = (ns, name)
        if key in self._deployments:
            return JSONResponse(
                {
                    "kind": "Status",
                    "status": "Failure",
                    "message": f'deployments "{name}" already exists',
                    "reason": "AlreadyExists",
                },
                status_code=409,
            )
        dep = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": name,
                "namespace": ns,
                "uid": _new_uid(),
                "creationTimestamp": _now_iso(),
            },
            "spec": body.get("spec", {}),
            "status": {
                "replicas": body.get("spec", {}).get("replicas", 1),
                "readyReplicas": body.get("spec", {}).get("replicas", 1),
                "availableReplicas": body.get("spec", {}).get("replicas", 1),
            },
        }
        self._deployments[key] = dep
        return JSONResponse(dep, status_code=201)

    async def _handle_deployment(self, request: Request) -> JSONResponse:
        ns = request.path_params["ns"]
        name = request.path_params["name"]
        key = (ns, name)

        if request.method == "GET":
            dep = self._deployments.get(key)
            if not dep:
                return JSONResponse(
                    {
                        "kind": "Status",
                        "status": "Failure",
                        "message": f'deployments "{name}" not found',
                        "reason": "NotFound",
                    },
                    status_code=404,
                )
            return JSONResponse(dep)

        # DELETE
        dep = self._deployments.pop(key, None)
        if not dep:
            return JSONResponse(
                {
                    "kind": "Status",
                    "status": "Failure",
                    "message": f'deployments "{name}" not found',
                    "reason": "NotFound",
                },
                status_code=404,
            )
        return JSONResponse(dep)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_namespace(name: str) -> dict:
        return {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": name,
                "uid": _new_uid(),
                "creationTimestamp": _now_iso(),
            },
            "status": {"phase": "Active"},
        }

    def get_app(self) -> Starlette:
        """Return the Starlette app for testing with TestClient."""
        return self._build_app()
