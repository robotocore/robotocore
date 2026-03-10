"""Tests for the mock Kubernetes API server."""

import pytest
from starlette.testclient import TestClient

from robotocore.services.eks.k8s_mock import K8sMockServer


@pytest.fixture
def k8s_client():
    """Create a TestClient for the mock K8s app."""
    server = K8sMockServer()
    app = server.get_app()
    return TestClient(app)


@pytest.fixture
def k8s_server():
    """Return a K8sMockServer instance (not started) for state inspection."""
    return K8sMockServer()


# ------------------------------------------------------------------
# Discovery endpoints
# ------------------------------------------------------------------


class TestAPIDiscovery:
    def test_api_versions(self, k8s_client: TestClient):
        resp = k8s_client.get("/api")
        assert resp.status_code == 200
        data = resp.json()
        assert data["kind"] == "APIVersions"
        assert "v1" in data["versions"]

    def test_api_v1_resources(self, k8s_client: TestClient):
        resp = k8s_client.get("/api/v1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["kind"] == "APIResourceList"
        assert data["groupVersion"] == "v1"
        resource_names = [r["name"] for r in data["resources"]]
        assert "pods" in resource_names
        assert "services" in resource_names
        assert "namespaces" in resource_names


# ------------------------------------------------------------------
# Namespaces
# ------------------------------------------------------------------


class TestNamespaces:
    def test_default_namespace_exists(self, k8s_client: TestClient):
        resp = k8s_client.get("/api/v1/namespaces")
        assert resp.status_code == 200
        data = resp.json()
        assert data["kind"] == "NamespaceList"
        names = [ns["metadata"]["name"] for ns in data["items"]]
        assert "default" in names

    def test_create_namespace(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/api/v1/namespaces",
            json={"metadata": {"name": "test-ns"}},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["kind"] == "Namespace"
        assert data["metadata"]["name"] == "test-ns"
        assert data["status"]["phase"] == "Active"

    def test_create_duplicate_namespace(self, k8s_client: TestClient):
        k8s_client.post("/api/v1/namespaces", json={"metadata": {"name": "dup-ns"}})
        resp = k8s_client.post("/api/v1/namespaces", json={"metadata": {"name": "dup-ns"}})
        assert resp.status_code == 409
        assert resp.json()["reason"] == "AlreadyExists"

    def test_create_namespace_no_name(self, k8s_client: TestClient):
        resp = k8s_client.post("/api/v1/namespaces", json={"metadata": {}})
        assert resp.status_code == 400


# ------------------------------------------------------------------
# Pods
# ------------------------------------------------------------------


class TestPods:
    def test_create_pod(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/api/v1/namespaces/default/pods",
            json={
                "metadata": {"name": "my-pod"},
                "spec": {"containers": [{"name": "nginx", "image": "nginx:latest"}]},
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["kind"] == "Pod"
        assert data["metadata"]["name"] == "my-pod"
        assert data["metadata"]["namespace"] == "default"
        assert data["status"]["phase"] == "Running"
        assert "uid" in data["metadata"]
        assert "creationTimestamp" in data["metadata"]

    def test_get_pod(self, k8s_client: TestClient):
        k8s_client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "get-pod"}, "spec": {}},
        )
        resp = k8s_client.get("/api/v1/namespaces/default/pods/get-pod")
        assert resp.status_code == 200
        assert resp.json()["metadata"]["name"] == "get-pod"

    def test_get_pod_not_found(self, k8s_client: TestClient):
        resp = k8s_client.get("/api/v1/namespaces/default/pods/no-such-pod")
        assert resp.status_code == 404
        assert resp.json()["reason"] == "NotFound"

    def test_list_pods(self, k8s_client: TestClient):
        k8s_client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "pod-a"}, "spec": {}},
        )
        k8s_client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "pod-b"}, "spec": {}},
        )
        resp = k8s_client.get("/api/v1/namespaces/default/pods")
        assert resp.status_code == 200
        data = resp.json()
        assert data["kind"] == "PodList"
        names = [p["metadata"]["name"] for p in data["items"]]
        assert "pod-a" in names
        assert "pod-b" in names

    def test_list_pods_namespace_isolation(self, k8s_client: TestClient):
        # Create namespace
        k8s_client.post("/api/v1/namespaces", json={"metadata": {"name": "ns-a"}})
        k8s_client.post(
            "/api/v1/namespaces/ns-a/pods",
            json={"metadata": {"name": "pod-in-a"}, "spec": {}},
        )
        k8s_client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "pod-in-default"}, "spec": {}},
        )

        resp = k8s_client.get("/api/v1/namespaces/ns-a/pods")
        names = [p["metadata"]["name"] for p in resp.json()["items"]]
        assert "pod-in-a" in names
        assert "pod-in-default" not in names

    def test_delete_pod(self, k8s_client: TestClient):
        k8s_client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "del-pod"}, "spec": {}},
        )
        resp = k8s_client.delete("/api/v1/namespaces/default/pods/del-pod")
        assert resp.status_code == 200

        # Verify gone
        resp = k8s_client.get("/api/v1/namespaces/default/pods/del-pod")
        assert resp.status_code == 404

    def test_delete_pod_not_found(self, k8s_client: TestClient):
        resp = k8s_client.delete("/api/v1/namespaces/default/pods/no-pod")
        assert resp.status_code == 404

    def test_create_duplicate_pod(self, k8s_client: TestClient):
        k8s_client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "dup-pod"}, "spec": {}},
        )
        resp = k8s_client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "dup-pod"}, "spec": {}},
        )
        assert resp.status_code == 409


# ------------------------------------------------------------------
# Services
# ------------------------------------------------------------------


class TestServices:
    def test_create_service(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/api/v1/namespaces/default/services",
            json={
                "metadata": {"name": "my-svc"},
                "spec": {"ports": [{"port": 80, "targetPort": 8080}]},
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["kind"] == "Service"
        assert data["metadata"]["name"] == "my-svc"

    def test_list_services(self, k8s_client: TestClient):
        k8s_client.post(
            "/api/v1/namespaces/default/services",
            json={"metadata": {"name": "svc-a"}, "spec": {}},
        )
        resp = k8s_client.get("/api/v1/namespaces/default/services")
        assert resp.status_code == 200
        data = resp.json()
        assert data["kind"] == "ServiceList"
        assert len(data["items"]) >= 1

    def test_delete_service(self, k8s_client: TestClient):
        k8s_client.post(
            "/api/v1/namespaces/default/services",
            json={"metadata": {"name": "del-svc"}, "spec": {}},
        )
        resp = k8s_client.delete("/api/v1/namespaces/default/services/del-svc")
        assert resp.status_code == 200

        resp = k8s_client.get("/api/v1/namespaces/default/services/del-svc")
        assert resp.status_code == 404

    def test_delete_service_not_found(self, k8s_client: TestClient):
        resp = k8s_client.delete("/api/v1/namespaces/default/services/no-svc")
        assert resp.status_code == 404

    def test_get_service(self, k8s_client: TestClient):
        k8s_client.post(
            "/api/v1/namespaces/default/services",
            json={"metadata": {"name": "get-svc"}, "spec": {}},
        )
        resp = k8s_client.get("/api/v1/namespaces/default/services/get-svc")
        assert resp.status_code == 200
        assert resp.json()["metadata"]["name"] == "get-svc"


# ------------------------------------------------------------------
# Deployments
# ------------------------------------------------------------------


class TestDeployments:
    def test_create_deployment(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={
                "metadata": {"name": "my-deploy"},
                "spec": {
                    "replicas": 3,
                    "selector": {"matchLabels": {"app": "test"}},
                    "template": {
                        "metadata": {"labels": {"app": "test"}},
                        "spec": {"containers": [{"name": "app", "image": "app:v1"}]},
                    },
                },
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["kind"] == "Deployment"
        assert data["metadata"]["name"] == "my-deploy"
        assert data["status"]["replicas"] == 3
        assert data["status"]["readyReplicas"] == 3

    def test_list_deployments(self, k8s_client: TestClient):
        k8s_client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={"metadata": {"name": "dep-a"}, "spec": {}},
        )
        resp = k8s_client.get("/apis/apps/v1/namespaces/default/deployments")
        assert resp.status_code == 200
        data = resp.json()
        assert data["kind"] == "DeploymentList"
        assert len(data["items"]) >= 1

    def test_delete_deployment(self, k8s_client: TestClient):
        k8s_client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={"metadata": {"name": "del-dep"}, "spec": {}},
        )
        resp = k8s_client.delete("/apis/apps/v1/namespaces/default/deployments/del-dep")
        assert resp.status_code == 200

        resp = k8s_client.get("/apis/apps/v1/namespaces/default/deployments/del-dep")
        assert resp.status_code == 404

    def test_delete_deployment_not_found(self, k8s_client: TestClient):
        resp = k8s_client.delete("/apis/apps/v1/namespaces/default/deployments/no-dep")
        assert resp.status_code == 404

    def test_get_deployment(self, k8s_client: TestClient):
        k8s_client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={"metadata": {"name": "get-dep"}, "spec": {}},
        )
        resp = k8s_client.get("/apis/apps/v1/namespaces/default/deployments/get-dep")
        assert resp.status_code == 200
        assert resp.json()["metadata"]["name"] == "get-dep"


# ------------------------------------------------------------------
# Server lifecycle
# ------------------------------------------------------------------


class TestServerLifecycle:
    def test_server_has_default_namespace(self):
        server = K8sMockServer()
        assert "default" in server._namespaces

    def test_separate_servers_have_isolated_state(self):
        s1 = K8sMockServer()
        s2 = K8sMockServer()
        s1._pods[("default", "pod-1")] = {"metadata": {"name": "pod-1"}}
        assert ("default", "pod-1") not in s2._pods
