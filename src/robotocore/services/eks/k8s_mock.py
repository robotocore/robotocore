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

# Monotonically increasing resource version counter (global across all servers)
_resource_version_counter = 0
_rv_lock = threading.Lock()


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _new_uid() -> str:
    return str(uuid.uuid4())


def _next_resource_version() -> str:
    global _resource_version_counter
    with _rv_lock:
        _resource_version_counter += 1
        return str(_resource_version_counter)


def _k8s_error(status_code: int, message: str, reason: str) -> JSONResponse:
    """Return a Kubernetes-style Status error response."""
    return JSONResponse(
        {
            "apiVersion": "v1",
            "kind": "Status",
            "metadata": {},
            "status": "Failure",
            "message": message,
            "reason": reason,
            "code": status_code,
        },
        status_code=status_code,
    )


class K8sMockServer:
    """A mock Kubernetes API server backed by in-memory stores.

    Thread-safe: all store access is protected by ``_lock``.
    """

    def __init__(self) -> None:
        self.cluster_name: str = ""
        self.port: int = 0
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None
        self._app: Starlette | None = None

        # Lock protecting all in-memory stores
        self._lock = threading.RLock()

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
        Raises RuntimeError if the server fails to start within 10 seconds.
        """
        self.cluster_name = cluster_name
        self._app = self._build_app()

        config = uvicorn.Config(
            app=self._app,
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
        else:
            # Timed out — clean up
            self._server.should_exit = True
            self._thread.join(timeout=3.0)
            self._thread = None
            self._server = None
            raise RuntimeError(f"K8s mock server for {cluster_name} failed to start within 10s")

        # Extract the actual port from the server sockets
        if self._server.servers:
            for server in self._server.servers:
                for sock in server.sockets:
                    self.port = sock.getsockname()[1]
                    break
                if self.port:
                    break

        if not self.port:
            self._server.should_exit = True
            self._thread.join(timeout=3.0)
            self._thread = None
            self._server = None
            raise RuntimeError(
                f"K8s mock server for {cluster_name} started but could not determine port"
            )

        logger.info("K8s mock server for %s started on port %d", cluster_name, self.port)
        return self.port

    def stop(self) -> None:
        """Signal shutdown and wait for the server thread to finish."""
        if self._server:
            self._server.should_exit = True
        if self._thread:
            self._thread.join(timeout=5.0)
            if self._thread.is_alive():
                logger.warning(
                    "K8s mock server thread for %s did not stop within timeout",
                    self.cluster_name,
                )
            self._thread = None
        self._server = None
        self.port = 0
        logger.info("K8s mock server for %s stopped", self.cluster_name)

    @property
    def is_running(self) -> bool:
        """Return True if the server thread is alive and the server is started."""
        return (
            self._thread is not None
            and self._thread.is_alive()
            and self._server is not None
            and self._server.started
        )

    # ------------------------------------------------------------------
    # Starlette app
    # ------------------------------------------------------------------

    def _build_app(self) -> Starlette:
        routes = [
            # Discovery
            Route("/api", self._handle_api, methods=["GET"]),
            Route("/api/v1", self._handle_api_v1, methods=["GET"]),
            Route("/apis", self._handle_apis, methods=["GET"]),
            Route("/apis/apps/v1", self._handle_apis_apps_v1, methods=["GET"]),
            # Namespaces
            Route("/api/v1/namespaces", self._handle_namespaces, methods=["GET", "POST"]),
            Route(
                "/api/v1/namespaces/{ns}",
                self._handle_namespace,
                methods=["GET", "DELETE"],
            ),
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
            Route(
                "/api/v1/namespaces/{ns}/pods/{name}/status",
                self._handle_pod_status,
                methods=["GET"],
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
                        "name": "pods/status",
                        "singularName": "",
                        "namespaced": True,
                        "kind": "Pod",
                        "verbs": ["get"],
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

    async def _handle_apis(self, request: Request) -> JSONResponse:
        return JSONResponse(
            {
                "kind": "APIGroupList",
                "apiVersion": "v1",
                "groups": [
                    {
                        "name": "apps",
                        "versions": [
                            {"groupVersion": "apps/v1", "version": "v1"},
                        ],
                        "preferredVersion": {"groupVersion": "apps/v1", "version": "v1"},
                    },
                ],
            }
        )

    async def _handle_apis_apps_v1(self, request: Request) -> JSONResponse:
        return JSONResponse(
            {
                "kind": "APIResourceList",
                "groupVersion": "apps/v1",
                "resources": [
                    {
                        "name": "deployments",
                        "singularName": "deployment",
                        "namespaced": True,
                        "kind": "Deployment",
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
            with self._lock:
                items = list(self._namespaces.values())
            return JSONResponse(
                {
                    "apiVersion": "v1",
                    "kind": "NamespaceList",
                    "metadata": {"resourceVersion": _next_resource_version()},
                    "items": items,
                }
            )

        # POST - create namespace
        body = await request.json()
        name = body.get("metadata", {}).get("name", "")
        if not name:
            return _k8s_error(400, "name is required", "Invalid")
        with self._lock:
            if name in self._namespaces:
                return _k8s_error(
                    409,
                    f'namespaces "{name}" already exists',
                    "AlreadyExists",
                )
            ns = self._make_namespace(name, labels=body.get("metadata", {}).get("labels"))
            self._namespaces[name] = ns
        return JSONResponse(ns, status_code=201)

    async def _handle_namespace(self, request: Request) -> JSONResponse:
        ns_name = request.path_params["ns"]

        if request.method == "GET":
            with self._lock:
                ns = self._namespaces.get(ns_name)
            if not ns:
                return _k8s_error(404, f'namespaces "{ns_name}" not found', "NotFound")
            return JSONResponse(ns)

        # DELETE
        with self._lock:
            ns = self._namespaces.pop(ns_name, None)
            if not ns:
                return _k8s_error(404, f'namespaces "{ns_name}" not found', "NotFound")
            # Cascade-delete resources in this namespace
            self._pods = {k: v for k, v in self._pods.items() if k[0] != ns_name}
            self._services = {k: v for k, v in self._services.items() if k[0] != ns_name}
            self._deployments = {k: v for k, v in self._deployments.items() if k[0] != ns_name}
        ns["status"] = {"phase": "Terminating"}
        return JSONResponse(ns)

    # ------------------------------------------------------------------
    # Pods
    # ------------------------------------------------------------------

    async def _handle_pods_collection(self, request: Request) -> JSONResponse:
        ns = request.path_params["ns"]
        if request.method == "GET":
            with self._lock:
                items = [v for (n, _), v in self._pods.items() if n == ns]
            return JSONResponse(
                {
                    "apiVersion": "v1",
                    "kind": "PodList",
                    "metadata": {"resourceVersion": _next_resource_version()},
                    "items": items,
                }
            )

        # POST - create pod
        body = await request.json()
        name = body.get("metadata", {}).get("name", "")
        if not name:
            return _k8s_error(400, "name is required", "Invalid")
        key = (ns, name)
        with self._lock:
            if key in self._pods:
                return _k8s_error(409, f'pods "{name}" already exists', "AlreadyExists")
            labels = body.get("metadata", {}).get("labels")
            pod = {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": name,
                    "namespace": ns,
                    "uid": _new_uid(),
                    "creationTimestamp": _now_iso(),
                    "resourceVersion": _next_resource_version(),
                    **({"labels": labels} if labels else {}),
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
            with self._lock:
                pod = self._pods.get(key)
            if not pod:
                return _k8s_error(404, f'pods "{name}" not found', "NotFound")
            return JSONResponse(pod)

        # DELETE
        with self._lock:
            pod = self._pods.pop(key, None)
        if not pod:
            return _k8s_error(404, f'pods "{name}" not found', "NotFound")
        return JSONResponse(
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": pod["metadata"],
                "status": {"phase": "Succeeded"},
            }
        )

    async def _handle_pod_status(self, request: Request) -> JSONResponse:
        """GET /api/v1/namespaces/{ns}/pods/{name}/status"""
        ns = request.path_params["ns"]
        name = request.path_params["name"]
        key = (ns, name)
        with self._lock:
            pod = self._pods.get(key)
        if not pod:
            return _k8s_error(404, f'pods "{name}" not found', "NotFound")
        return JSONResponse(pod)

    # ------------------------------------------------------------------
    # Services
    # ------------------------------------------------------------------

    async def _handle_services_collection(self, request: Request) -> JSONResponse:
        ns = request.path_params["ns"]
        if request.method == "GET":
            with self._lock:
                items = [v for (n, _), v in self._services.items() if n == ns]
            return JSONResponse(
                {
                    "apiVersion": "v1",
                    "kind": "ServiceList",
                    "metadata": {"resourceVersion": _next_resource_version()},
                    "items": items,
                }
            )

        # POST - create service
        body = await request.json()
        name = body.get("metadata", {}).get("name", "")
        if not name:
            return _k8s_error(400, "name is required", "Invalid")
        key = (ns, name)
        with self._lock:
            if key in self._services:
                return _k8s_error(409, f'services "{name}" already exists', "AlreadyExists")
            labels = body.get("metadata", {}).get("labels")
            spec = body.get("spec", {})
            svc = {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {
                    "name": name,
                    "namespace": ns,
                    "uid": _new_uid(),
                    "creationTimestamp": _now_iso(),
                    "resourceVersion": _next_resource_version(),
                    **({"labels": labels} if labels else {}),
                },
                "spec": {
                    "type": spec.get("type", "ClusterIP"),
                    **spec,
                },
            }
            self._services[key] = svc
        return JSONResponse(svc, status_code=201)

    async def _handle_service(self, request: Request) -> JSONResponse:
        ns = request.path_params["ns"]
        name = request.path_params["name"]
        key = (ns, name)

        if request.method == "GET":
            with self._lock:
                svc = self._services.get(key)
            if not svc:
                return _k8s_error(404, f'services "{name}" not found', "NotFound")
            return JSONResponse(svc)

        # DELETE
        with self._lock:
            svc = self._services.pop(key, None)
        if not svc:
            return _k8s_error(404, f'services "{name}" not found', "NotFound")
        return JSONResponse(svc)

    # ------------------------------------------------------------------
    # Deployments
    # ------------------------------------------------------------------

    async def _handle_deployments_collection(self, request: Request) -> JSONResponse:
        ns = request.path_params["ns"]
        if request.method == "GET":
            with self._lock:
                items = [v for (n, _), v in self._deployments.items() if n == ns]
            return JSONResponse(
                {
                    "apiVersion": "apps/v1",
                    "kind": "DeploymentList",
                    "metadata": {"resourceVersion": _next_resource_version()},
                    "items": items,
                }
            )

        # POST - create deployment
        body = await request.json()
        name = body.get("metadata", {}).get("name", "")
        if not name:
            return _k8s_error(400, "name is required", "Invalid")
        key = (ns, name)
        with self._lock:
            if key in self._deployments:
                return _k8s_error(409, f'deployments "{name}" already exists', "AlreadyExists")
            labels = body.get("metadata", {}).get("labels")
            replicas = body.get("spec", {}).get("replicas", 1)
            dep = {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": name,
                    "namespace": ns,
                    "uid": _new_uid(),
                    "creationTimestamp": _now_iso(),
                    "resourceVersion": _next_resource_version(),
                    **({"labels": labels} if labels else {}),
                },
                "spec": body.get("spec", {}),
                "status": {
                    "replicas": replicas,
                    "readyReplicas": replicas,
                    "availableReplicas": replicas,
                },
            }
            self._deployments[key] = dep
        return JSONResponse(dep, status_code=201)

    async def _handle_deployment(self, request: Request) -> JSONResponse:
        ns = request.path_params["ns"]
        name = request.path_params["name"]
        key = (ns, name)

        if request.method == "GET":
            with self._lock:
                dep = self._deployments.get(key)
            if not dep:
                return _k8s_error(404, f'deployments "{name}" not found', "NotFound")
            return JSONResponse(dep)

        # DELETE
        with self._lock:
            dep = self._deployments.pop(key, None)
        if not dep:
            return _k8s_error(404, f'deployments "{name}" not found', "NotFound")
        return JSONResponse(dep)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_namespace(name: str, labels: dict | None = None) -> dict:
        return {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": name,
                "uid": _new_uid(),
                "creationTimestamp": _now_iso(),
                "resourceVersion": _next_resource_version(),
                **({"labels": labels} if labels else {}),
            },
            "status": {"phase": "Active"},
        }

    def get_app(self) -> Starlette:
        """Return the Starlette app for testing with TestClient.

        Reuses the already-built app if available, avoiding route duplication.
        """
        if self._app is None:
            self._app = self._build_app()
        return self._app
