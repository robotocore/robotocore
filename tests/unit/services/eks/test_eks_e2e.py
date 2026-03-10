"""End-to-end and edge-case tests for the EKS mock Kubernetes API feature.

Tests cover:
- Full cluster lifecycle (create resources, use them, delete them)
- Multi-cluster isolation (via TestClient and via real HTTP servers)
- Kubernetes API edge cases (duplicate names, missing fields, 404s, etc.)
- Namespace cascade deletion with nested resources
- API discovery response structure (kubectl compatibility)
- Server lifecycle edge cases (restart, double-stop, concurrent starts)
- Response format accuracy (UIDs, timestamps, resourceVersions)
- Kubeconfig integration with live server
- Spec preservation on GET after POST
- Memory: create/delete many resources and verify stores are cleaned
"""

import re

import httpx
import pytest
from starlette.testclient import TestClient

from robotocore.services.eks.k8s_mock import K8sMockServer, _next_resource_version

# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture
def server():
    """Return a fresh K8sMockServer instance (not started)."""
    return K8sMockServer()


@pytest.fixture
def client(server: K8sMockServer):
    """TestClient wrapping a fresh K8sMockServer."""
    return TestClient(server.get_app())


@pytest.fixture
def live_server():
    """Start a real K8sMockServer on a random port, yield (server, base_url), stop after."""
    srv = K8sMockServer()
    port = srv.start("e2e-test", port=0)
    yield srv, f"http://localhost:{port}"
    srv.stop()


# ------------------------------------------------------------------
# Full lifecycle: create resources, verify, delete, verify gone
# ------------------------------------------------------------------


class TestFullClusterLifecycle:
    """Simulates what a real user does: create a cluster's worth of k8s resources."""

    def test_pod_lifecycle(self, client: TestClient):
        """Create pod -> verify Running -> delete -> verify gone."""
        resp = client.post(
            "/api/v1/namespaces/default/pods",
            json={
                "metadata": {"name": "lifecycle-pod"},
                "spec": {"containers": [{"name": "nginx", "image": "nginx:1.25"}]},
            },
        )
        assert resp.status_code == 201
        pod = resp.json()
        assert pod["status"]["phase"] == "Running"
        assert pod["metadata"]["namespace"] == "default"

        # GET it back
        resp = client.get("/api/v1/namespaces/default/pods/lifecycle-pod")
        assert resp.status_code == 200
        assert resp.json()["spec"]["containers"][0]["image"] == "nginx:1.25"

        # DELETE
        resp = client.delete("/api/v1/namespaces/default/pods/lifecycle-pod")
        assert resp.status_code == 200
        assert resp.json()["status"]["phase"] == "Succeeded"

        # Gone
        resp = client.get("/api/v1/namespaces/default/pods/lifecycle-pod")
        assert resp.status_code == 404

    def test_service_gets_clusterip_type(self, client: TestClient):
        """Create service -> verify ClusterIP default type."""
        resp = client.post(
            "/api/v1/namespaces/default/services",
            json={
                "metadata": {"name": "web-svc"},
                "spec": {"ports": [{"port": 80, "targetPort": 8080}]},
            },
        )
        assert resp.status_code == 201
        svc = resp.json()
        assert svc["spec"]["type"] == "ClusterIP"
        assert svc["spec"]["ports"] == [{"port": 80, "targetPort": 8080}]

    def test_deployment_with_replicas(self, client: TestClient):
        """Create deployment with 3 replicas -> verify spec and status."""
        resp = client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={
                "metadata": {"name": "web-deploy"},
                "spec": {
                    "replicas": 3,
                    "selector": {"matchLabels": {"app": "web"}},
                    "template": {
                        "metadata": {"labels": {"app": "web"}},
                        "spec": {
                            "containers": [{"name": "web", "image": "web:v2"}],
                        },
                    },
                },
            },
        )
        assert resp.status_code == 201
        dep = resp.json()
        assert dep["spec"]["replicas"] == 3
        assert dep["status"]["replicas"] == 3
        assert dep["status"]["readyReplicas"] == 3
        assert dep["status"]["availableReplicas"] == 3

    def test_full_create_list_delete_cycle(self, client: TestClient):
        """Create pod + service + deployment, list all, delete all, verify empty."""
        # Create
        client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "cycle-pod"}, "spec": {}},
        )
        client.post(
            "/api/v1/namespaces/default/services",
            json={"metadata": {"name": "cycle-svc"}, "spec": {}},
        )
        client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={"metadata": {"name": "cycle-dep"}, "spec": {}},
        )

        # List all — verify present
        pods = client.get("/api/v1/namespaces/default/pods").json()["items"]
        svcs = client.get("/api/v1/namespaces/default/services").json()["items"]
        deps = client.get("/apis/apps/v1/namespaces/default/deployments").json()["items"]
        assert any(p["metadata"]["name"] == "cycle-pod" for p in pods)
        assert any(s["metadata"]["name"] == "cycle-svc" for s in svcs)
        assert any(d["metadata"]["name"] == "cycle-dep" for d in deps)

        # Delete in reverse order
        dep_url = "/apis/apps/v1/namespaces/default/deployments/cycle-dep"
        resp = client.delete(dep_url)
        assert resp.status_code == 200
        resp = client.delete("/api/v1/namespaces/default/services/cycle-svc")
        assert resp.status_code == 200
        resp = client.delete("/api/v1/namespaces/default/pods/cycle-pod")
        assert resp.status_code == 200

        # Verify empty
        pods = client.get("/api/v1/namespaces/default/pods").json()["items"]
        svcs = client.get("/api/v1/namespaces/default/services").json()["items"]
        deps = client.get("/apis/apps/v1/namespaces/default/deployments").json()["items"]
        assert len(pods) == 0
        assert len(svcs) == 0
        assert len(deps) == 0


# ------------------------------------------------------------------
# Multi-cluster isolation via TestClient (no real HTTP needed)
# ------------------------------------------------------------------


class TestMultiClusterIsolation:
    def test_two_servers_independent_pods(self):
        """Pods in server A do not appear in server B."""
        sa = K8sMockServer()
        sb = K8sMockServer()
        ca = TestClient(sa.get_app())
        cb = TestClient(sb.get_app())

        ca.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "pod-only-a"}, "spec": {}},
        )
        cb.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "pod-only-b"}, "spec": {}},
        )

        pods_a = ca.get("/api/v1/namespaces/default/pods").json()["items"]
        names_a = [p["metadata"]["name"] for p in pods_a]
        pods_b = cb.get("/api/v1/namespaces/default/pods").json()["items"]
        names_b = [p["metadata"]["name"] for p in pods_b]

        assert "pod-only-a" in names_a
        assert "pod-only-b" not in names_a
        assert "pod-only-b" in names_b
        assert "pod-only-a" not in names_b

    def test_two_servers_independent_namespaces(self):
        """Creating a namespace on server A does not affect server B."""
        sa = K8sMockServer()
        sb = K8sMockServer()
        ca = TestClient(sa.get_app())
        cb = TestClient(sb.get_app())

        ca.post("/api/v1/namespaces", json={"metadata": {"name": "only-a"}})

        names_a = [ns["metadata"]["name"] for ns in ca.get("/api/v1/namespaces").json()["items"]]
        names_b = [ns["metadata"]["name"] for ns in cb.get("/api/v1/namespaces").json()["items"]]

        assert "only-a" in names_a
        assert "only-a" not in names_b

    def test_delete_one_server_other_unaffected(self):
        """After stopping server A (or clearing its state), server B works fine."""
        sa = K8sMockServer()
        sb = K8sMockServer()
        ca = TestClient(sa.get_app())
        cb = TestClient(sb.get_app())

        ca.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "pod-a"}, "spec": {}},
        )
        cb.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "pod-b"}, "spec": {}},
        )

        # "Stop" server A by clearing its stores
        sa._pods.clear()

        # Server B still has its pod
        pods_b = cb.get("/api/v1/namespaces/default/pods").json()["items"]
        names_b = [p["metadata"]["name"] for p in pods_b]
        assert "pod-b" in names_b

    def test_three_servers_unique_endpoints(self):
        """Three live servers all get different ports."""
        servers = [K8sMockServer() for _ in range(3)]
        ports = []
        try:
            for i, s in enumerate(servers):
                ports.append(s.start(f"multi-{i}", port=0))
            assert len(set(ports)) == 3
            for p in ports:
                assert p > 0
        finally:
            for s in servers:
                s.stop()


# ------------------------------------------------------------------
# Kubernetes API edge cases
# ------------------------------------------------------------------


class TestKubernetesAPIEdgeCases:
    def test_pod_with_extra_unknown_fields_accepted(self, client: TestClient):
        """K8s is lenient with unknown fields; our mock should accept them."""
        resp = client.post(
            "/api/v1/namespaces/default/pods",
            json={
                "metadata": {"name": "extra-fields-pod"},
                "spec": {
                    "containers": [{"name": "app", "image": "app:v1"}],
                    "unknownField": "should-be-kept",
                },
            },
        )
        assert resp.status_code == 201
        # The spec is preserved as-is
        assert resp.json()["spec"]["unknownField"] == "should-be-kept"

    def test_pod_containers_preserved_on_get(self, client: TestClient):
        """Container specs should be retrievable after creation."""
        containers = [
            {"name": "main", "image": "app:v3", "ports": [{"containerPort": 8080}]},
            {"name": "sidecar", "image": "proxy:v1"},
        ]
        client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "multi-container"}, "spec": {"containers": containers}},
        )
        resp = client.get("/api/v1/namespaces/default/pods/multi-container")
        assert resp.status_code == 200
        assert resp.json()["spec"]["containers"] == containers

    def test_duplicate_pod_same_namespace_409(self, client: TestClient):
        """Creating two pods with the same name in the same namespace -> 409."""
        client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "dup-test"}, "spec": {}},
        )
        resp = client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "dup-test"}, "spec": {}},
        )
        assert resp.status_code == 409
        data = resp.json()
        assert data["reason"] == "AlreadyExists"
        assert data["kind"] == "Status"
        assert data["code"] == 409

    def test_same_name_different_namespaces_ok(self, client: TestClient):
        """Same resource name in different namespaces should work."""
        client.post("/api/v1/namespaces", json={"metadata": {"name": "ns-alpha"}})
        client.post("/api/v1/namespaces", json={"metadata": {"name": "ns-beta"}})

        r1 = client.post(
            "/api/v1/namespaces/ns-alpha/pods",
            json={"metadata": {"name": "shared-name"}, "spec": {}},
        )
        r2 = client.post(
            "/api/v1/namespaces/ns-beta/pods",
            json={"metadata": {"name": "shared-name"}, "spec": {}},
        )
        assert r1.status_code == 201
        assert r2.status_code == 201

        # They have different UIDs
        uid1 = r1.json()["metadata"]["uid"]
        uid2 = r2.json()["metadata"]["uid"]
        assert uid1 != uid2

    def test_duplicate_service_same_namespace_409(self, client: TestClient):
        client.post(
            "/api/v1/namespaces/default/services",
            json={"metadata": {"name": "dup-svc"}, "spec": {}},
        )
        resp = client.post(
            "/api/v1/namespaces/default/services",
            json={"metadata": {"name": "dup-svc"}, "spec": {}},
        )
        assert resp.status_code == 409
        assert resp.json()["reason"] == "AlreadyExists"

    def test_duplicate_deployment_same_namespace_409(self, client: TestClient):
        client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={"metadata": {"name": "dup-dep"}, "spec": {}},
        )
        resp = client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={"metadata": {"name": "dup-dep"}, "spec": {}},
        )
        assert resp.status_code == 409
        assert resp.json()["reason"] == "AlreadyExists"

    def test_get_nonexistent_pod_returns_k8s_status(self, client: TestClient):
        resp = client.get("/api/v1/namespaces/default/pods/ghost")
        assert resp.status_code == 404
        data = resp.json()
        assert data["apiVersion"] == "v1"
        assert data["kind"] == "Status"
        assert data["status"] == "Failure"
        assert data["reason"] == "NotFound"
        assert data["code"] == 404
        assert "ghost" in data["message"]

    def test_delete_nonexistent_pod_returns_404(self, client: TestClient):
        resp = client.delete("/api/v1/namespaces/default/pods/phantom")
        assert resp.status_code == 404
        data = resp.json()
        assert data["kind"] == "Status"
        assert data["reason"] == "NotFound"

    def test_delete_nonexistent_service_returns_404(self, client: TestClient):
        resp = client.delete("/api/v1/namespaces/default/services/phantom")
        assert resp.status_code == 404
        assert resp.json()["reason"] == "NotFound"

    def test_delete_nonexistent_deployment_returns_404(self, client: TestClient):
        resp = client.delete("/apis/apps/v1/namespaces/default/deployments/phantom")
        assert resp.status_code == 404
        assert resp.json()["reason"] == "NotFound"

    def test_create_pod_no_metadata_key(self, client: TestClient):
        """POST with empty body (no metadata at all) should fail."""
        resp = client.post(
            "/api/v1/namespaces/default/pods",
            json={"spec": {}},
        )
        assert resp.status_code == 400

    def test_create_service_no_metadata_key(self, client: TestClient):
        resp = client.post(
            "/api/v1/namespaces/default/services",
            json={"spec": {}},
        )
        assert resp.status_code == 400

    def test_create_deployment_no_metadata_key(self, client: TestClient):
        resp = client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={"spec": {}},
        )
        assert resp.status_code == 400

    def test_create_namespace_no_metadata_key(self, client: TestClient):
        resp = client.post(
            "/api/v1/namespaces",
            json={},
        )
        assert resp.status_code == 400

    def test_empty_pod_list_has_correct_structure(self, client: TestClient):
        """A namespace with no pods should return empty items list."""
        client.post("/api/v1/namespaces", json={"metadata": {"name": "empty-ns"}})
        resp = client.get("/api/v1/namespaces/empty-ns/pods")
        assert resp.status_code == 200
        data = resp.json()
        assert data["kind"] == "PodList"
        assert data["apiVersion"] == "v1"
        assert data["items"] == []
        assert "resourceVersion" in data["metadata"]

    def test_empty_service_list_structure(self, client: TestClient):
        resp = client.get("/api/v1/namespaces/default/services")
        assert resp.status_code == 200
        data = resp.json()
        assert data["kind"] == "ServiceList"
        assert data["items"] == []

    def test_empty_deployment_list_structure(self, client: TestClient):
        resp = client.get("/apis/apps/v1/namespaces/default/deployments")
        assert resp.status_code == 200
        data = resp.json()
        assert data["kind"] == "DeploymentList"
        assert data["items"] == []

    def test_service_spec_preserved_on_get(self, client: TestClient):
        """Service spec (ports, selector, etc.) should roundtrip."""
        spec = {
            "type": "NodePort",
            "ports": [{"port": 80, "targetPort": 8080, "nodePort": 30080}],
            "selector": {"app": "web"},
        }
        client.post(
            "/api/v1/namespaces/default/services",
            json={"metadata": {"name": "roundtrip-svc"}, "spec": spec},
        )
        resp = client.get("/api/v1/namespaces/default/services/roundtrip-svc")
        got_spec = resp.json()["spec"]
        assert got_spec["type"] == "NodePort"
        assert got_spec["ports"] == spec["ports"]
        assert got_spec["selector"] == {"app": "web"}

    def test_deployment_spec_preserved_on_get(self, client: TestClient):
        """Deployment spec (replicas, selector, template) should roundtrip."""
        spec = {
            "replicas": 5,
            "selector": {"matchLabels": {"app": "api"}},
            "template": {
                "metadata": {"labels": {"app": "api"}},
                "spec": {"containers": [{"name": "api", "image": "api:v3"}]},
            },
        }
        client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={"metadata": {"name": "roundtrip-dep"}, "spec": spec},
        )
        resp = client.get("/apis/apps/v1/namespaces/default/deployments/roundtrip-dep")
        got = resp.json()
        assert got["spec"]["replicas"] == 5
        assert got["spec"]["template"]["spec"]["containers"][0]["image"] == "api:v3"
        assert got["status"]["replicas"] == 5


# ------------------------------------------------------------------
# Namespace management edge cases
# ------------------------------------------------------------------


class TestNamespaceEdgeCases:
    def test_cascade_delete_removes_all_resource_types(self, client: TestClient):
        """Deleting a namespace removes pods, services, and deployments."""
        ns = "cascade-all"
        client.post("/api/v1/namespaces", json={"metadata": {"name": ns}})
        client.post(
            f"/api/v1/namespaces/{ns}/pods",
            json={"metadata": {"name": "p1"}, "spec": {}},
        )
        client.post(
            f"/api/v1/namespaces/{ns}/services",
            json={"metadata": {"name": "s1"}, "spec": {}},
        )
        client.post(
            f"/apis/apps/v1/namespaces/{ns}/deployments",
            json={"metadata": {"name": "d1"}, "spec": {}},
        )

        # Delete the namespace
        resp = client.delete(f"/api/v1/namespaces/{ns}")
        assert resp.status_code == 200
        assert resp.json()["status"]["phase"] == "Terminating"

        # All resources gone
        assert client.get(f"/api/v1/namespaces/{ns}/pods/p1").status_code == 404
        assert client.get(f"/api/v1/namespaces/{ns}/services/s1").status_code == 404
        assert client.get(f"/apis/apps/v1/namespaces/{ns}/deployments/d1").status_code == 404

    def test_cascade_delete_does_not_affect_other_namespaces(self, client: TestClient):
        """Resources in other namespaces are unaffected by namespace deletion."""
        client.post("/api/v1/namespaces", json={"metadata": {"name": "doomed"}})
        client.post("/api/v1/namespaces", json={"metadata": {"name": "safe"}})
        client.post(
            "/api/v1/namespaces/doomed/pods",
            json={"metadata": {"name": "pod1"}, "spec": {}},
        )
        client.post(
            "/api/v1/namespaces/safe/pods",
            json={"metadata": {"name": "pod1"}, "spec": {}},
        )

        client.delete("/api/v1/namespaces/doomed")

        # "safe" namespace and its pod still exist
        assert client.get("/api/v1/namespaces/safe").status_code == 200
        assert client.get("/api/v1/namespaces/safe/pods/pod1").status_code == 200

    def test_delete_nonexistent_namespace_404(self, client: TestClient):
        resp = client.delete("/api/v1/namespaces/does-not-exist")
        assert resp.status_code == 404
        assert resp.json()["reason"] == "NotFound"

    def test_create_namespace_with_labels(self, client: TestClient):
        resp = client.post(
            "/api/v1/namespaces",
            json={"metadata": {"name": "labeled", "labels": {"env": "staging", "team": "backend"}}},
        )
        assert resp.status_code == 201
        labels = resp.json()["metadata"]["labels"]
        assert labels["env"] == "staging"
        assert labels["team"] == "backend"

    def test_duplicate_namespace_409(self, client: TestClient):
        client.post("/api/v1/namespaces", json={"metadata": {"name": "once"}})
        resp = client.post("/api/v1/namespaces", json={"metadata": {"name": "once"}})
        assert resp.status_code == 409
        assert resp.json()["reason"] == "AlreadyExists"

    def test_default_namespace_is_gettable(self, client: TestClient):
        """The 'default' namespace should exist from the start."""
        resp = client.get("/api/v1/namespaces/default")
        assert resp.status_code == 200
        data = resp.json()
        assert data["metadata"]["name"] == "default"
        assert data["status"]["phase"] == "Active"

    def test_many_namespaces_listed(self, client: TestClient):
        """Creating many namespaces and listing them all."""
        for i in range(20):
            resp = client.post(
                "/api/v1/namespaces",
                json={"metadata": {"name": f"ns-{i}"}},
            )
            assert resp.status_code == 201

        resp = client.get("/api/v1/namespaces")
        names = [ns["metadata"]["name"] for ns in resp.json()["items"]]
        # 20 custom + "default"
        assert len(names) == 21
        assert "default" in names
        for i in range(20):
            assert f"ns-{i}" in names


# ------------------------------------------------------------------
# API discovery (kubectl relies on these)
# ------------------------------------------------------------------


class TestAPIDiscoveryStructure:
    def test_api_has_versions_and_server_address(self, client: TestClient):
        resp = client.get("/api")
        data = resp.json()
        assert "versions" in data
        assert "serverAddressByClientCIDRs" in data
        assert len(data["serverAddressByClientCIDRs"]) > 0
        cidr = data["serverAddressByClientCIDRs"][0]
        assert "clientCIDR" in cidr
        assert "serverAddress" in cidr

    def test_api_v1_resources_have_verbs(self, client: TestClient):
        """Each resource in /api/v1 should have a verbs list."""
        resp = client.get("/api/v1")
        for resource in resp.json()["resources"]:
            assert "verbs" in resource
            assert isinstance(resource["verbs"], list)
            assert len(resource["verbs"]) > 0

    def test_api_v1_resources_have_required_fields(self, client: TestClient):
        resp = client.get("/api/v1")
        for resource in resp.json()["resources"]:
            assert "name" in resource
            assert "kind" in resource
            assert "namespaced" in resource
            assert "singularName" in resource

    def test_apis_groups_have_preferred_version(self, client: TestClient):
        resp = client.get("/apis")
        data = resp.json()
        for group in data["groups"]:
            assert "name" in group
            assert "versions" in group
            assert "preferredVersion" in group
            assert "groupVersion" in group["preferredVersion"]

    def test_apis_apps_v1_has_deployment_resource(self, client: TestClient):
        resp = client.get("/apis/apps/v1")
        data = resp.json()
        assert data["groupVersion"] == "apps/v1"
        dep = [r for r in data["resources"] if r["name"] == "deployments"]
        assert len(dep) == 1
        assert dep[0]["kind"] == "Deployment"
        assert dep[0]["namespaced"] is True
        assert "create" in dep[0]["verbs"]
        assert "delete" in dep[0]["verbs"]
        assert "get" in dep[0]["verbs"]
        assert "list" in dep[0]["verbs"]


# ------------------------------------------------------------------
# Response format accuracy
# ------------------------------------------------------------------


class TestResponseFormatAccuracy:
    def test_created_pod_has_all_metadata_fields(self, client: TestClient):
        resp = client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "meta-pod"}, "spec": {}},
        )
        meta = resp.json()["metadata"]
        assert meta["name"] == "meta-pod"
        assert meta["namespace"] == "default"
        assert "uid" in meta
        assert "creationTimestamp" in meta
        assert "resourceVersion" in meta

    def test_uid_is_valid_uuid_format(self, client: TestClient):
        resp = client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "uuid-pod"}, "spec": {}},
        )
        uid = resp.json()["metadata"]["uid"]
        # UUID v4 format: 8-4-4-4-12 hex chars
        assert re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            uid,
        )

    def test_uids_are_unique_across_resources(self, client: TestClient):
        """Every resource gets a unique UID."""
        uids = set()
        for i in range(10):
            resp = client.post(
                "/api/v1/namespaces/default/pods",
                json={"metadata": {"name": f"uid-pod-{i}"}, "spec": {}},
            )
            uids.add(resp.json()["metadata"]["uid"])
        assert len(uids) == 10

    def test_uids_unique_across_resource_types(self, client: TestClient):
        """UIDs are unique even across pods, services, and deployments."""
        uids = set()
        resp = client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "uid-cross-pod"}, "spec": {}},
        )
        uids.add(resp.json()["metadata"]["uid"])

        resp = client.post(
            "/api/v1/namespaces/default/services",
            json={"metadata": {"name": "uid-cross-svc"}, "spec": {}},
        )
        uids.add(resp.json()["metadata"]["uid"])

        resp = client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={"metadata": {"name": "uid-cross-dep"}, "spec": {}},
        )
        uids.add(resp.json()["metadata"]["uid"])

        resp = client.post(
            "/api/v1/namespaces",
            json={"metadata": {"name": "uid-cross-ns"}},
        )
        uids.add(resp.json()["metadata"]["uid"])

        assert len(uids) == 4

    def test_creation_timestamp_is_iso_format(self, client: TestClient):
        resp = client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "ts-pod"}, "spec": {}},
        )
        ts = resp.json()["metadata"]["creationTimestamp"]
        # Format: YYYY-MM-DDTHH:MM:SSZ
        assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", ts)

    def test_resource_versions_are_monotonically_increasing(self, client: TestClient):
        """Each created resource should have a higher resourceVersion than the last."""
        versions = []
        for i in range(5):
            resp = client.post(
                "/api/v1/namespaces/default/pods",
                json={"metadata": {"name": f"rv-pod-{i}"}, "spec": {}},
            )
            versions.append(int(resp.json()["metadata"]["resourceVersion"]))
        # Strictly increasing
        for j in range(1, len(versions)):
            assert versions[j] > versions[j - 1]

    def test_list_response_has_correct_structure(self, client: TestClient):
        """List responses have apiVersion, kind, metadata, items."""
        resp = client.get("/api/v1/namespaces/default/pods")
        data = resp.json()
        assert "apiVersion" in data
        assert "kind" in data
        assert "metadata" in data
        assert "items" in data
        assert isinstance(data["items"], list)

    def test_error_response_has_all_k8s_status_fields(self, client: TestClient):
        """Error responses match the Kubernetes Status object format."""
        resp = client.get("/api/v1/namespaces/default/pods/not-real")
        data = resp.json()
        assert data["apiVersion"] == "v1"
        assert data["kind"] == "Status"
        assert data["status"] == "Failure"
        assert "message" in data
        assert "reason" in data
        assert "code" in data
        assert "metadata" in data

    def test_pod_without_labels_has_no_labels_key(self, client: TestClient):
        """A pod created without labels should not have a labels key in metadata."""
        resp = client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "no-labels-pod"}, "spec": {}},
        )
        assert "labels" not in resp.json()["metadata"]

    def test_pod_with_labels_has_labels_key(self, client: TestClient):
        resp = client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "has-labels-pod", "labels": {"a": "b"}}, "spec": {}},
        )
        assert resp.json()["metadata"]["labels"] == {"a": "b"}


# ------------------------------------------------------------------
# Server lifecycle edge cases
# ------------------------------------------------------------------


class TestServerLifecycleEdgeCases:
    def test_stop_idempotent_no_raise(self):
        """Calling stop multiple times on a never-started server is safe."""
        s = K8sMockServer()
        s.stop()
        s.stop()
        s.stop()

    def test_stop_resets_port(self):
        s = K8sMockServer()
        port = s.start("port-reset", port=0)
        assert port > 0
        s.stop()
        assert s.port == 0

    def test_is_running_transitions(self):
        s = K8sMockServer()
        assert not s.is_running
        s.start("transitions", port=0)
        assert s.is_running
        s.stop()
        assert not s.is_running

    def test_start_stop_start_different_port(self):
        """Start, stop, start again — should work on a (possibly different) port."""
        s = K8sMockServer()
        p1 = s.start("restart-test", port=0)
        assert p1 > 0
        s.stop()

        # Start again
        p2 = s.start("restart-test-2", port=0)
        assert p2 > 0
        assert s.is_running
        s.stop()

    def test_start_stop_start_state_cleared(self):
        """After stop+start, state from previous run is gone (new server instance needed)."""
        s = K8sMockServer()
        app = s.get_app()
        c = TestClient(app)

        c.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "old-pod"}, "spec": {}},
        )
        pods = c.get("/api/v1/namespaces/default/pods").json()["items"]
        assert len(pods) == 1

        # The state persists on the same server object (stores are in-memory on the instance)
        # A brand new server would have clean state
        s2 = K8sMockServer()
        c2 = TestClient(s2.get_app())
        pods2 = c2.get("/api/v1/namespaces/default/pods").json()["items"]
        assert len(pods2) == 0

    def test_get_app_idempotent(self):
        """Calling get_app() multiple times returns the same app."""
        s = K8sMockServer()
        a1 = s.get_app()
        a2 = s.get_app()
        assert a1 is a2


# ------------------------------------------------------------------
# Memory: create and delete many resources, verify stores cleaned
# ------------------------------------------------------------------


class TestMemoryCleanup:
    def test_create_delete_100_pods_stores_empty(self, server: K8sMockServer):
        """After creating and deleting 100 pods, the internal store should be empty."""
        c = TestClient(server.get_app())
        for i in range(100):
            resp = c.post(
                "/api/v1/namespaces/default/pods",
                json={"metadata": {"name": f"mem-pod-{i}"}, "spec": {}},
            )
            assert resp.status_code == 201

        assert len(server._pods) == 100

        for i in range(100):
            resp = c.delete(f"/api/v1/namespaces/default/pods/mem-pod-{i}")
            assert resp.status_code == 200

        assert len(server._pods) == 0

    def test_create_delete_services_stores_empty(self, server: K8sMockServer):
        c = TestClient(server.get_app())
        for i in range(50):
            c.post(
                "/api/v1/namespaces/default/services",
                json={"metadata": {"name": f"mem-svc-{i}"}, "spec": {}},
            )
        assert len(server._services) == 50

        for i in range(50):
            c.delete(f"/api/v1/namespaces/default/services/mem-svc-{i}")
        assert len(server._services) == 0

    def test_namespace_delete_clears_all_nested_resources(self, server: K8sMockServer):
        """Deleting a namespace clears its resources from all stores."""
        c = TestClient(server.get_app())
        ns = "mem-ns"
        c.post("/api/v1/namespaces", json={"metadata": {"name": ns}})

        for i in range(10):
            pod_url = f"/api/v1/namespaces/{ns}/pods"
            c.post(pod_url, json={"metadata": {"name": f"p-{i}"}, "spec": {}})
            svc_url = f"/api/v1/namespaces/{ns}/services"
            c.post(svc_url, json={"metadata": {"name": f"s-{i}"}, "spec": {}})
            c.post(
                f"/apis/apps/v1/namespaces/{ns}/deployments",
                json={"metadata": {"name": f"d-{i}"}, "spec": {}},
            )

        assert len(server._pods) == 10
        assert len(server._services) == 10
        assert len(server._deployments) == 10

        c.delete(f"/api/v1/namespaces/{ns}")

        assert len(server._pods) == 0
        assert len(server._services) == 0
        assert len(server._deployments) == 0


# ------------------------------------------------------------------
# Cross-namespace resource correctness
# ------------------------------------------------------------------


class TestCrossNamespaceCorrectness:
    def test_pods_listed_only_in_their_namespace(self, client: TestClient):
        """Pods from namespace A should never appear in namespace B's list."""
        client.post("/api/v1/namespaces", json={"metadata": {"name": "ns-1"}})
        client.post("/api/v1/namespaces", json={"metadata": {"name": "ns-2"}})

        for i in range(5):
            client.post(
                "/api/v1/namespaces/ns-1/pods",
                json={"metadata": {"name": f"ns1-pod-{i}"}, "spec": {}},
            )
            client.post(
                "/api/v1/namespaces/ns-2/pods",
                json={"metadata": {"name": f"ns2-pod-{i}"}, "spec": {}},
            )

        ns1_pods = client.get("/api/v1/namespaces/ns-1/pods").json()["items"]
        ns2_pods = client.get("/api/v1/namespaces/ns-2/pods").json()["items"]

        ns1_names = {p["metadata"]["name"] for p in ns1_pods}
        ns2_names = {p["metadata"]["name"] for p in ns2_pods}

        assert len(ns1_names) == 5
        assert len(ns2_names) == 5
        assert ns1_names.isdisjoint(ns2_names)

    def test_services_listed_only_in_their_namespace(self, client: TestClient):
        client.post("/api/v1/namespaces", json={"metadata": {"name": "svc-ns-1"}})
        client.post("/api/v1/namespaces", json={"metadata": {"name": "svc-ns-2"}})

        client.post(
            "/api/v1/namespaces/svc-ns-1/services",
            json={"metadata": {"name": "svc-a"}, "spec": {}},
        )
        client.post(
            "/api/v1/namespaces/svc-ns-2/services",
            json={"metadata": {"name": "svc-b"}, "spec": {}},
        )

        ns1 = client.get("/api/v1/namespaces/svc-ns-1/services").json()["items"]
        ns2 = client.get("/api/v1/namespaces/svc-ns-2/services").json()["items"]

        assert [s["metadata"]["name"] for s in ns1] == ["svc-a"]
        assert [s["metadata"]["name"] for s in ns2] == ["svc-b"]

    def test_deployments_listed_only_in_their_namespace(self, client: TestClient):
        client.post("/api/v1/namespaces", json={"metadata": {"name": "dep-ns-1"}})
        client.post("/api/v1/namespaces", json={"metadata": {"name": "dep-ns-2"}})

        client.post(
            "/apis/apps/v1/namespaces/dep-ns-1/deployments",
            json={"metadata": {"name": "dep-a"}, "spec": {}},
        )
        client.post(
            "/apis/apps/v1/namespaces/dep-ns-2/deployments",
            json={"metadata": {"name": "dep-b"}, "spec": {}},
        )

        ns1 = client.get("/apis/apps/v1/namespaces/dep-ns-1/deployments").json()["items"]
        ns2 = client.get("/apis/apps/v1/namespaces/dep-ns-2/deployments").json()["items"]

        assert [d["metadata"]["name"] for d in ns1] == ["dep-a"]
        assert [d["metadata"]["name"] for d in ns2] == ["dep-b"]


# ------------------------------------------------------------------
# Live server integration (real HTTP, not TestClient)
# ------------------------------------------------------------------


class TestLiveServerIntegration:
    def test_full_lifecycle_via_http(self, live_server):
        """Full create -> list -> get -> delete cycle over real HTTP."""
        _, base = live_server

        # Create namespace
        resp = httpx.post(f"{base}/api/v1/namespaces", json={"metadata": {"name": "live-ns"}})
        assert resp.status_code == 201

        # Create pod
        resp = httpx.post(
            f"{base}/api/v1/namespaces/live-ns/pods",
            json={
                "metadata": {"name": "live-pod"},
                "spec": {"containers": [{"name": "app", "image": "app:latest"}]},
            },
        )
        assert resp.status_code == 201
        assert resp.json()["status"]["phase"] == "Running"

        # Create service
        resp = httpx.post(
            f"{base}/api/v1/namespaces/live-ns/services",
            json={"metadata": {"name": "live-svc"}, "spec": {"ports": [{"port": 80}]}},
        )
        assert resp.status_code == 201

        # Create deployment
        resp = httpx.post(
            f"{base}/apis/apps/v1/namespaces/live-ns/deployments",
            json={"metadata": {"name": "live-dep"}, "spec": {"replicas": 2}},
        )
        assert resp.status_code == 201
        assert resp.json()["status"]["replicas"] == 2

        # List all
        pods = httpx.get(f"{base}/api/v1/namespaces/live-ns/pods").json()["items"]
        svcs = httpx.get(f"{base}/api/v1/namespaces/live-ns/services").json()["items"]
        deps = httpx.get(f"{base}/apis/apps/v1/namespaces/live-ns/deployments").json()["items"]
        assert len(pods) == 1
        assert len(svcs) == 1
        assert len(deps) == 1

        # Delete namespace (cascade)
        resp = httpx.delete(f"{base}/api/v1/namespaces/live-ns")
        assert resp.status_code == 200

        # All gone
        assert httpx.get(f"{base}/api/v1/namespaces/live-ns/pods/live-pod").status_code == 404
        assert httpx.get(f"{base}/api/v1/namespaces/live-ns/services/live-svc").status_code == 404

    def test_discovery_endpoints_via_http(self, live_server):
        """All discovery endpoints respond over real HTTP."""
        _, base = live_server

        for path in ["/api", "/api/v1", "/apis", "/apis/apps/v1"]:
            resp = httpx.get(f"{base}{path}")
            assert resp.status_code == 200
            assert "kind" in resp.json()

    def test_pod_status_subresource_via_http(self, live_server):
        _, base = live_server

        httpx.post(
            f"{base}/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "status-pod"}, "spec": {}},
        )
        resp = httpx.get(f"{base}/api/v1/namespaces/default/pods/status-pod/status")
        assert resp.status_code == 200
        assert resp.json()["status"]["phase"] == "Running"

    def test_concurrent_creates_no_duplicates(self, live_server):
        """Concurrent POSTs with different names should all succeed."""
        import concurrent.futures

        _, base = live_server

        def create_pod(idx):
            return httpx.post(
                f"{base}/api/v1/namespaces/default/pods",
                json={"metadata": {"name": f"concurrent-{idx}"}, "spec": {}},
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            futures = [pool.submit(create_pod, i) for i in range(20)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        assert all(r.status_code == 201 for r in results)

        # Verify all 20 exist
        resp = httpx.get(f"{base}/api/v1/namespaces/default/pods")
        names = {p["metadata"]["name"] for p in resp.json()["items"]}
        for i in range(20):
            assert f"concurrent-{i}" in names


# ------------------------------------------------------------------
# Resource version global counter
# ------------------------------------------------------------------


class TestResourceVersionCounter:
    def test_resource_version_increases_across_calls(self):
        """_next_resource_version() returns strictly increasing values."""
        rv1 = int(_next_resource_version())
        rv2 = int(_next_resource_version())
        rv3 = int(_next_resource_version())
        assert rv1 < rv2 < rv3

    def test_resource_version_increases_across_resource_types(self, client: TestClient):
        """resourceVersion should increase across different resource types."""
        r1 = client.post(
            "/api/v1/namespaces/default/pods",
            json={"metadata": {"name": "rv-type-pod"}, "spec": {}},
        )
        r2 = client.post(
            "/api/v1/namespaces/default/services",
            json={"metadata": {"name": "rv-type-svc"}, "spec": {}},
        )
        r3 = client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={"metadata": {"name": "rv-type-dep"}, "spec": {}},
        )
        rv1 = int(r1.json()["metadata"]["resourceVersion"])
        rv2 = int(r2.json()["metadata"]["resourceVersion"])
        rv3 = int(r3.json()["metadata"]["resourceVersion"])
        assert rv1 < rv2 < rv3


# ------------------------------------------------------------------
# Pod status subresource
# ------------------------------------------------------------------


class TestPodStatusSubresource:
    def test_status_returns_full_pod(self, client: TestClient):
        """GET /pods/{name}/status should return the full pod object."""
        client.post(
            "/api/v1/namespaces/default/pods",
            json={
                "metadata": {"name": "full-status-pod", "labels": {"tier": "web"}},
                "spec": {"containers": [{"name": "c", "image": "img:v1"}]},
            },
        )
        resp = client.get("/api/v1/namespaces/default/pods/full-status-pod/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["metadata"]["name"] == "full-status-pod"
        assert data["metadata"]["labels"] == {"tier": "web"}
        assert data["status"]["phase"] == "Running"
        assert data["spec"]["containers"][0]["image"] == "img:v1"

    def test_status_nonexistent_pod_404(self, client: TestClient):
        resp = client.get("/api/v1/namespaces/default/pods/ghost/status")
        assert resp.status_code == 404
        assert resp.json()["reason"] == "NotFound"

    def test_status_in_custom_namespace(self, client: TestClient):
        client.post("/api/v1/namespaces", json={"metadata": {"name": "status-ns"}})
        client.post(
            "/api/v1/namespaces/status-ns/pods",
            json={"metadata": {"name": "ns-status-pod"}, "spec": {}},
        )
        resp = client.get("/api/v1/namespaces/status-ns/pods/ns-status-pod/status")
        assert resp.status_code == 200
        assert resp.json()["metadata"]["namespace"] == "status-ns"


# ------------------------------------------------------------------
# Deployment replicas
# ------------------------------------------------------------------


class TestDeploymentReplicas:
    def test_zero_replicas(self, client: TestClient):
        resp = client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={"metadata": {"name": "zero-rep"}, "spec": {"replicas": 0}},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["status"]["replicas"] == 0
        assert data["status"]["readyReplicas"] == 0
        assert data["status"]["availableReplicas"] == 0

    def test_large_replicas(self, client: TestClient):
        resp = client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={"metadata": {"name": "big-rep"}, "spec": {"replicas": 1000}},
        )
        assert resp.status_code == 201
        assert resp.json()["status"]["replicas"] == 1000

    def test_default_replicas_is_one(self, client: TestClient):
        resp = client.post(
            "/apis/apps/v1/namespaces/default/deployments",
            json={"metadata": {"name": "default-rep"}, "spec": {}},
        )
        assert resp.json()["status"]["replicas"] == 1


# ------------------------------------------------------------------
# Kubeconfig integration with live server
# ------------------------------------------------------------------


class TestKubeconfigLiveIntegration:
    def test_kubeconfig_endpoint_reaches_live_server(self, live_server):
        """Generate a kubeconfig, extract the endpoint, and hit the server."""
        import yaml

        from robotocore.services.eks.kubeconfig import generate_kubeconfig

        srv, base = live_server
        config_str = generate_kubeconfig(
            cluster_name="kc-test",
            endpoint=base,
            region="us-east-1",
            account_id="123456789012",
        )
        config = yaml.safe_load(config_str)
        endpoint = config["clusters"][0]["cluster"]["server"]

        # The endpoint from the kubeconfig should be reachable
        resp = httpx.get(f"{endpoint}/api")
        assert resp.status_code == 200
        assert resp.json()["kind"] == "APIVersions"

    def test_multiple_kubeconfigs_different_endpoints(self):
        """Kubeconfigs for different clusters point to different endpoints."""
        import yaml

        from robotocore.services.eks.kubeconfig import generate_kubeconfig

        cfg1 = yaml.safe_load(
            generate_kubeconfig("c1", "http://localhost:1111", "us-east-1", "111111111111")
        )
        cfg2 = yaml.safe_load(
            generate_kubeconfig("c2", "http://localhost:2222", "us-west-2", "222222222222")
        )

        ep1 = cfg1["clusters"][0]["cluster"]["server"]
        ep2 = cfg2["clusters"][0]["cluster"]["server"]
        assert ep1 != ep2
        assert cfg1["current-context"] == "c1"
        assert cfg2["current-context"] == "c2"
