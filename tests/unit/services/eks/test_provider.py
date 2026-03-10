"""Tests for the EKS native provider (provider.py)."""

import threading

from robotocore.services.eks.k8s_mock import K8sMockServer
from robotocore.services.eks.provider import (
    _k8s_servers,
    _lock,
    _server_key,
)


class TestServerKey:
    def test_key_includes_region_account_name(self):
        key = _server_key("us-east-1", "123456789012", "my-cluster")
        assert key == "us-east-1:123456789012:my-cluster"

    def test_different_regions_different_keys(self):
        k1 = _server_key("us-east-1", "123456789012", "cluster")
        k2 = _server_key("us-west-2", "123456789012", "cluster")
        assert k1 != k2

    def test_different_accounts_different_keys(self):
        k1 = _server_key("us-east-1", "111111111111", "cluster")
        k2 = _server_key("us-east-1", "222222222222", "cluster")
        assert k1 != k2


class TestServerRegistry:
    """Tests for the module-level server registry."""

    def setup_method(self):
        """Clean the global registry before each test."""
        with _lock:
            # Stop any lingering servers
            for server in _k8s_servers.values():
                server.stop()
            _k8s_servers.clear()

    def teardown_method(self):
        """Clean up after each test."""
        with _lock:
            for server in _k8s_servers.values():
                server.stop()
            _k8s_servers.clear()

    def test_registry_starts_empty(self):
        with _lock:
            assert len(_k8s_servers) == 0

    def test_can_register_and_retrieve_server(self):
        server = K8sMockServer()
        key = _server_key("us-east-1", "123456789012", "test")
        with _lock:
            _k8s_servers[key] = server
        with _lock:
            assert _k8s_servers[key] is server

    def test_registry_is_thread_safe(self):
        """Multiple threads writing to the registry should not corrupt it."""
        errors = []

        def register(i: int):
            try:
                server = K8sMockServer()
                key = _server_key("us-east-1", "123456789012", f"cluster-{i}")
                with _lock:
                    _k8s_servers[key] = server
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=register, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        with _lock:
            assert len(_k8s_servers) == 20


class TestK8sMockServerIntegration:
    """Tests that exercise the K8sMockServer with real HTTP (start/stop)."""

    def test_start_stop_lifecycle(self):
        server = K8sMockServer()
        port = server.start("lifecycle-test", port=0)
        assert port > 0
        assert server.is_running

        server.stop()
        assert not server.is_running

    def test_server_responds_to_http(self):
        """Start a real server and verify it responds to HTTP requests."""
        import httpx

        server = K8sMockServer()
        try:
            port = server.start("http-test", port=0)
            resp = httpx.get(f"http://localhost:{port}/api")
            assert resp.status_code == 200
            data = resp.json()
            assert data["kind"] == "APIVersions"
        finally:
            server.stop()

    def test_create_pod_via_http(self):
        """Full lifecycle: start server, create a pod, list pods, verify."""
        import httpx

        server = K8sMockServer()
        try:
            port = server.start("pod-http-test", port=0)
            base = f"http://localhost:{port}"

            # Create a pod
            resp = httpx.post(
                f"{base}/api/v1/namespaces/default/pods",
                json={
                    "metadata": {"name": "http-pod"},
                    "spec": {"containers": [{"name": "app", "image": "app:v1"}]},
                },
            )
            assert resp.status_code == 201
            assert resp.json()["metadata"]["name"] == "http-pod"

            # List pods
            resp = httpx.get(f"{base}/api/v1/namespaces/default/pods")
            assert resp.status_code == 200
            names = [p["metadata"]["name"] for p in resp.json()["items"]]
            assert "http-pod" in names

            # Get pod
            resp = httpx.get(f"{base}/api/v1/namespaces/default/pods/http-pod")
            assert resp.status_code == 200
            assert resp.json()["status"]["phase"] == "Running"

            # Delete pod
            resp = httpx.delete(f"{base}/api/v1/namespaces/default/pods/http-pod")
            assert resp.status_code == 200

            # Verify gone
            resp = httpx.get(f"{base}/api/v1/namespaces/default/pods/http-pod")
            assert resp.status_code == 404
        finally:
            server.stop()

    def test_two_clusters_independent_state(self):
        """Two running servers have completely independent state."""
        import httpx

        s1 = K8sMockServer()
        s2 = K8sMockServer()
        try:
            p1 = s1.start("cluster-a", port=0)
            p2 = s2.start("cluster-b", port=0)

            # Create a pod only on cluster-a
            httpx.post(
                f"http://localhost:{p1}/api/v1/namespaces/default/pods",
                json={"metadata": {"name": "only-on-a"}, "spec": {}},
            )

            # cluster-a has it
            resp = httpx.get(f"http://localhost:{p1}/api/v1/namespaces/default/pods")
            names_a = [p["metadata"]["name"] for p in resp.json()["items"]]
            assert "only-on-a" in names_a

            # cluster-b does not
            resp = httpx.get(f"http://localhost:{p2}/api/v1/namespaces/default/pods")
            names_b = [p["metadata"]["name"] for p in resp.json()["items"]]
            assert "only-on-a" not in names_b
        finally:
            s1.stop()
            s2.stop()
