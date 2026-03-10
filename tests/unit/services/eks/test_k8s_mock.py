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

    def test_api_v1_lists_pod_status_subresource(self, k8s_client: TestClient):
        resp = k8s_client.get("/api/v1")
        data = resp.json()
        resource_names = [r["name"] for r in data["resources"]]
        assert "pods/status" in resource_names

    def test_apis_group_list(self, k8s_client: TestClient):
        resp = k8s_client.get("/apis")
        assert resp.status_code == 200
        data = resp.json()
        assert data["kind"] == "APIGroupList"
        group_names = [g["name"] for g in data["groups"]]
        assert "apps" in group_names

    def test_apis_apps_v1_resources(self, k8s_client: TestClient):
        resp = k8s_client.get("/apis/apps/v1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["kind"] == "APIResourceList"
        assert data["groupVersion"] == "apps/v1"
        resource_names = [r["name"] for r in data["resources"]]
        assert "deployments" in resource_names


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

    def test_namespace_list_has_resource_version(self, k8s_client: TestClient):
        resp = k8s_client.get("/api/v1/namespaces")
        data = resp.json()
        assert "resourceVersion" in data["metadata"]

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

    def test_create_namespace_has_metadata_fields(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/api/v1/namespaces",
            json={"metadata": {"name": "meta-ns"}},
        )
        data = resp.json()
        assert "uid" in data["metadata"]
        assert "creationTimestamp" in data["metadata"]
        assert "resourceVersion" in data["metadata"]

    def test_create_namespace_with_labels(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/api/v1/namespaces",
            json={"metadata": {"name": "labeled-ns", "labels": {"env": "test"}}},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["metadata"]["labels"] == {"env": "test"}

    def test_create_duplicate_namespace(self, k8s_client: TestClient):
        k8s_client.post("/api/v1/namespaces", json={"metadata": {"name": "dup-ns"}})
        resp = k8s_client.post("/api/v1/namespaces", json={"metadata": {"name": "dup-ns"}})
        assert resp.status_code == 409
        assert resp.json()["reason"] == "AlreadyExists"

    def test_create_namespace_no_name(self, k8s_client: TestClient):
        resp = k8s_client.post("/api/v1/namespaces", json={"metadata": {}})
        assert resp.status_code == 400

    def test_get_namespace(self, k8s_client: TestClient):
        k8s_client.post("/api/v1/namespaces", json={"metadata": {"name": "get-ns"}})
        resp = k8s_client.get("/api/v1/namespaces/get-ns")
        assert resp.status_code == 200
        assert resp.json()["metadata"]["name"] == "get-ns"

    def test_get_namespace_not_found(self, k8s_client: TestClient):
        resp = k8s_client.get("/api/v1/namespaces/nonexistent")
        assert resp.status_code == 404
        data = resp.json()
        assert data["reason"] == "NotFound"
        assert data["kind"] == "Status"

    def test_delete_namespace(self, k8s_client: TestClient):
        k8s_client.post("/api/v1/namespaces", json={"metadata": {"name": "del-ns"}})
        resp = k8s_client.delete("/api/v1/namespaces/del-ns")
        assert resp.status_code == 200
        assert resp.json()["status"]["phase"] == "Terminating"

        # Verify gone
        resp = k8s_client.get("/api/v1/namespaces/del-ns")
        assert resp.status_code == 404

    def test_delete_namespace_cascades_pods(self, k8s_client: TestClient):
        k8s_client.post("/api/v1/namespaces", json={"metadata": {"name": "cascade-ns"}})
        k8s_client.post(
            "/api/v1/namespaces/cascade-ns/pods",
            json={"metadata": {"name": "pod-in-cascade"}, "spec": {}},
        )
        k8s_client.delete("/api/v1/namespaces/cascade-ns")

        # Pod should be gone
        resp = k8s_client.get("/api/v1/namespaces/cascade-ns/pods/pod-in-cascade")
        assert resp.status_code == 404

    def test_delete_namespace_cascades_services(self, k8s_client: TestClient):
        k8s_client.post("/api/v1/namespaces", json={"metadata": {"name": "svc-ns"}})
        k8s_client.post(
            "/api/v1/namespaces/svc-ns/services",
            json={"metadata": {"name": "svc-in-ns"}, "spec": {}},
        )
        k8s_client.delete("/api/v1/namespaces/svc-ns")

        resp = k8s_client.get("/api/v1/namespaces/svc-ns/services/svc-in-ns")
        assert resp.status_code == 404

    def test_delete_namespace_cascades_deployments(self, k8s_client: TestClient):
        k8s_client.post("/api/v1/namespaces", json={"metadata": {"name": "dep-ns"}})
        k8s_client.post(
            "/apis/apps/v1/namespaces/dep-ns/deployments",
            json={"metadata": {"name": "dep-in-ns"}, "spec": {}},
        )
        k8s_client.delete("/api/v1/namespaces/dep-ns")

        resp = k8s_client.get("/apis/apps/v1/namespaces/dep-ns/deployments/dep-in-ns")
        assert resp.status_code == 404

    def test_delete_namespace_not_found(self, k8s_client: TestClient):
        resp = k8s_client.delete("/api/v1/namespaces/no-such-ns")
        assert resp.status_code == 404

    def test_error_response_has_k8s_format(self, k8s_client: TestClient):
        """Error responses must match Kubernetes Status format."""
        resp = k8s_client.get("/api/v1/namespaces/nonexistent")
        data = resp.json()
        assert data["apiVersion"] == "v1"
        assert data["kind"] == "Status"
        assert data["status"] == "Failure"
        assert data["code"] == 404
        assert "metadata" in data


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

    def test_create_pod_has_resource_version(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "rv-pod"}, "spec": {}},
        )
        data = resp.json()
        assert "resourceVersion" in data["metadata"]

    def test_create_pod_with_labels(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/api/v1/namespaces/default/pods",
            json={
                "metadata": {"name": "labeled-pod", "labels": {"app": "web", "tier": "frontend"}},
                "spec": {},
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["metadata"]["labels"] == {"app": "web", "tier": "frontend"}

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

    def test_get_pod_not_found_has_full_status(self, k8s_client: TestClient):
        resp = k8s_client.get("/api/v1/namespaces/default/pods/no-such-pod")
        data = resp.json()
        assert data["apiVersion"] == "v1"
        assert data["kind"] == "Status"
        assert data["code"] == 404

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

    def test_list_pods_has_resource_version(self, k8s_client: TestClient):
        resp = k8s_client.get("/api/v1/namespaces/default/pods")
        data = resp.json()
        assert "resourceVersion" in data["metadata"]

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

    def test_delete_pod_returns_succeeded_phase(self, k8s_client: TestClient):
        k8s_client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "phase-pod"}, "spec": {}},
        )
        resp = k8s_client.delete("/api/v1/namespaces/default/pods/phase-pod")
        assert resp.json()["status"]["phase"] == "Succeeded"

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

    def test_create_pod_no_name(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {}, "spec": {}},
        )
        assert resp.status_code == 400

    def test_pod_status_subresource(self, k8s_client: TestClient):
        k8s_client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "status-pod"}, "spec": {}},
        )
        resp = k8s_client.get("/api/v1/namespaces/default/pods/status-pod/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"]["phase"] == "Running"
        assert data["metadata"]["name"] == "status-pod"

    def test_pod_status_not_found(self, k8s_client: TestClient):
        resp = k8s_client.get("/api/v1/namespaces/default/pods/missing/status")
        assert resp.status_code == 404

    def test_same_pod_name_different_namespaces(self, k8s_client: TestClient):
        k8s_client.post("/api/v1/namespaces", json={"metadata": {"name": "ns-x"}})
        k8s_client.post("/api/v1/namespaces", json={"metadata": {"name": "ns-y"}})
        k8s_client.post(
            "/api/v1/namespaces/ns-x/pods",
            json={"metadata": {"name": "same-name"}, "spec": {}},
        )
        k8s_client.post(
            "/api/v1/namespaces/ns-y/pods",
            json={"metadata": {"name": "same-name"}, "spec": {}},
        )
        resp_x = k8s_client.get("/api/v1/namespaces/ns-x/pods/same-name")
        resp_y = k8s_client.get("/api/v1/namespaces/ns-y/pods/same-name")
        assert resp_x.status_code == 200
        assert resp_y.status_code == 200
        # Different UIDs
        assert resp_x.json()["metadata"]["uid"] != resp_y.json()["metadata"]["uid"]

    def test_many_pods(self, k8s_client: TestClient):
        """Create 100+ pods and verify they all appear in listing."""
        for i in range(120):
            resp = k8s_client.post(
                "/api/v1/namespaces/default/pods",
                json={"metadata": {"name": f"pod-{i}"}, "spec": {}},
            )
            assert resp.status_code == 201
        resp = k8s_client.get("/api/v1/namespaces/default/pods")
        assert len(resp.json()["items"]) == 120


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

    def test_create_service_defaults_to_clusterip(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/api/v1/namespaces/default/services",
            json={"metadata": {"name": "default-type"}, "spec": {}},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["spec"]["type"] == "ClusterIP"

    def test_create_service_nodeport(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/api/v1/namespaces/default/services",
            json={
                "metadata": {"name": "np-svc"},
                "spec": {
                    "type": "NodePort",
                    "ports": [{"port": 80, "targetPort": 8080, "nodePort": 30080}],
                },
            },
        )
        assert resp.status_code == 201
        assert resp.json()["spec"]["type"] == "NodePort"

    def test_create_service_loadbalancer(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/api/v1/namespaces/default/services",
            json={
                "metadata": {"name": "lb-svc"},
                "spec": {"type": "LoadBalancer", "ports": [{"port": 443}]},
            },
        )
        assert resp.status_code == 201
        assert resp.json()["spec"]["type"] == "LoadBalancer"

    def test_create_service_with_labels(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/api/v1/namespaces/default/services",
            json={
                "metadata": {"name": "labeled-svc", "labels": {"app": "backend"}},
                "spec": {},
            },
        )
        assert resp.status_code == 201
        assert resp.json()["metadata"]["labels"] == {"app": "backend"}

    def test_create_service_with_selector(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/api/v1/namespaces/default/services",
            json={
                "metadata": {"name": "sel-svc"},
                "spec": {"selector": {"app": "web"}, "ports": [{"port": 80}]},
            },
        )
        assert resp.status_code == 201
        assert resp.json()["spec"]["selector"] == {"app": "web"}

    def test_create_service_has_resource_version(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/api/v1/namespaces/default/services",
            json={"metadata": {"name": "rv-svc"}, "spec": {}},
        )
        assert "resourceVersion" in resp.json()["metadata"]

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

    def test_list_services_namespace_isolation(self, k8s_client: TestClient):
        k8s_client.post("/api/v1/namespaces", json={"metadata": {"name": "svc-ns-a"}})
        k8s_client.post(
            "/api/v1/namespaces/svc-ns-a/services",
            json={"metadata": {"name": "svc-in-a"}, "spec": {}},
        )
        k8s_client.post(
            "/api/v1/namespaces/default/services",
            json={"metadata": {"name": "svc-in-default"}, "spec": {}},
        )
        resp = k8s_client.get("/api/v1/namespaces/svc-ns-a/services")
        names = [s["metadata"]["name"] for s in resp.json()["items"]]
        assert "svc-in-a" in names
        assert "svc-in-default" not in names

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

    def test_get_service_not_found(self, k8s_client: TestClient):
        resp = k8s_client.get("/api/v1/namespaces/default/services/missing")
        assert resp.status_code == 404

    def test_create_duplicate_service(self, k8s_client: TestClient):
        k8s_client.post(
            "/api/v1/namespaces/default/services",
            json={"metadata": {"name": "dup-svc"}, "spec": {}},
        )
        resp = k8s_client.post(
            "/api/v1/namespaces/default/services",
            json={"metadata": {"name": "dup-svc"}, "spec": {}},
        )
        assert resp.status_code == 409


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

    def test_create_deployment_default_replicas(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={"metadata": {"name": "default-rep"}, "spec": {}},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["status"]["replicas"] == 1
        assert data["status"]["readyReplicas"] == 1
        assert data["status"]["availableReplicas"] == 1

    def test_create_deployment_with_labels(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={
                "metadata": {"name": "labeled-dep", "labels": {"app": "api"}},
                "spec": {},
            },
        )
        assert resp.status_code == 201
        assert resp.json()["metadata"]["labels"] == {"app": "api"}

    def test_create_deployment_has_resource_version(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={"metadata": {"name": "rv-dep"}, "spec": {}},
        )
        assert "resourceVersion" in resp.json()["metadata"]

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

    def test_list_deployments_namespace_isolation(self, k8s_client: TestClient):
        k8s_client.post("/api/v1/namespaces", json={"metadata": {"name": "dep-ns-a"}})
        k8s_client.post(
            "/apis/apps/v1/namespaces/dep-ns-a/deployments",
            json={"metadata": {"name": "dep-in-a"}, "spec": {}},
        )
        k8s_client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={"metadata": {"name": "dep-in-default"}, "spec": {}},
        )
        resp = k8s_client.get("/apis/apps/v1/namespaces/dep-ns-a/deployments")
        names = [d["metadata"]["name"] for d in resp.json()["items"]]
        assert "dep-in-a" in names
        assert "dep-in-default" not in names

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

    def test_get_deployment_not_found(self, k8s_client: TestClient):
        resp = k8s_client.get("/apis/apps/v1/namespaces/default/deployments/missing")
        assert resp.status_code == 404

    def test_create_deployment_no_name(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={"metadata": {}, "spec": {}},
        )
        assert resp.status_code == 400

    def test_create_duplicate_deployment(self, k8s_client: TestClient):
        k8s_client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={"metadata": {"name": "dup-dep"}, "spec": {}},
        )
        resp = k8s_client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={"metadata": {"name": "dup-dep"}, "spec": {}},
        )
        assert resp.status_code == 409

    def test_create_service_no_name(self, k8s_client: TestClient):
        resp = k8s_client.post(
            "/api/v1/namespaces/default/services",
            json={"metadata": {}, "spec": {}},
        )
        assert resp.status_code == 400


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

    def test_is_running_false_when_not_started(self):
        server = K8sMockServer()
        assert server.is_running is False

    def test_stop_idempotent(self):
        """Calling stop on a never-started server should not raise."""
        server = K8sMockServer()
        server.stop()  # Should not raise
        server.stop()  # Should not raise again

    def test_get_app_returns_same_instance(self):
        """get_app should return the same Starlette app on repeated calls."""
        server = K8sMockServer()
        app1 = server.get_app()
        app2 = server.get_app()
        assert app1 is app2

    def test_port_reset_on_stop(self):
        """Port should be reset to 0 after stop."""
        server = K8sMockServer()
        server.port = 12345  # Simulate having been started
        server.stop()
        assert server.port == 0

    def test_start_and_stop(self):
        """Integration: start a real server and stop it."""
        server = K8sMockServer()
        port = server.start("test-lifecycle", port=0)
        assert port > 0
        assert server.is_running

        server.stop()
        assert server.is_running is False
        assert server.port == 0

    def test_multiple_servers_different_ports(self):
        """Two servers must bind to different ports."""
        s1 = K8sMockServer()
        s2 = K8sMockServer()
        try:
            p1 = s1.start("cluster-1", port=0)
            p2 = s2.start("cluster-2", port=0)
            assert p1 != p2
            assert p1 > 0
            assert p2 > 0
        finally:
            s1.stop()
            s2.stop()
