"""Semantic integration tests for Cloud Pods -- end-to-end flows with local backend."""

import time

import pytest
from starlette.testclient import TestClient

from robotocore.state.cloud_pods import CloudPodsManager
from robotocore.state.manager import StateManager


@pytest.fixture
def state_manager(tmp_path):
    """Create a StateManager with a temp state dir and a mock native handler."""
    mgr = StateManager(state_dir=str(tmp_path / "state"))

    # Register a trivial native handler so we can verify state round-trips
    _store: dict = {"counter": 0}

    def save_fn():
        return dict(_store)

    def load_fn(data):
        _store.clear()
        _store.update(data)

    mgr.register_native_handler("test-service", save_fn, load_fn)
    return mgr, _store


@pytest.fixture
def pods_manager(tmp_path):
    """Create a CloudPodsManager with local backend."""
    backend = tmp_path / "pods"
    backend.mkdir()
    return CloudPodsManager(backend=str(backend))


class TestEndToEndSaveLoadVerify:
    def test_save_list_load_roundtrip(self, pods_manager, state_manager):
        """Save pod -> list -> load -> verify state restored."""
        mgr, store = state_manager
        store["counter"] = 42

        pods_manager.save_pod("roundtrip", state_manager=mgr)

        # Verify it appears in the list
        pods = pods_manager.list_pods()
        assert "roundtrip" in pods

        # Modify state
        store["counter"] = 0

        # Load and verify restoration
        pods_manager.load_pod("roundtrip", state_manager=mgr)
        # The native handler's load_fn was called, so store should be restored
        assert store["counter"] == 42

    def test_multiple_versions_load_specific(self, pods_manager, state_manager):
        """Save multiple versions -> load specific version -> correct state."""
        mgr, store = state_manager

        store["counter"] = 10
        pods_manager.save_pod("versioned", state_manager=mgr)
        time.sleep(0.01)

        store["counter"] = 20
        pods_manager.save_pod("versioned", state_manager=mgr)

        versions = pods_manager.list_pod_versions("versioned")
        assert len(versions) == 2

        # Load first version (counter=10)
        store["counter"] = 999
        pods_manager.load_pod(
            "versioned",
            state_manager=mgr,
            version=versions[0]["version"],
        )
        assert store["counter"] == 10

    def test_selective_services_only_saves_those(self, pods_manager, state_manager):
        """save_pod with services filter records the filter in metadata."""
        mgr, store = state_manager
        store["counter"] = 5

        pods_manager.save_pod(
            "selective",
            state_manager=mgr,
            services=["s3", "dynamodb"],
        )

        info = pods_manager.pod_info("selective")
        assert info.services_filter == ["s3", "dynamodb"]


class TestManagementAPIEndpoints:
    """Test the /_robotocore/pods/* management endpoints."""

    @pytest.fixture
    def app_client(self, tmp_path, monkeypatch):
        """Create a test client with cloud pods configured."""
        backend = tmp_path / "pods"
        backend.mkdir()
        state_dir = tmp_path / "state"
        state_dir.mkdir()

        monkeypatch.setenv("CLOUD_PODS_BACKEND", str(backend))
        monkeypatch.setenv("ROBOTOCORE_STATE_DIR", str(state_dir))

        # Re-import to pick up env vars -- use a fresh module
        from robotocore.gateway.app import app

        return TestClient(app, raise_server_exceptions=False)

    def test_save_then_list_then_load(self, app_client):
        """POST save -> GET list -> POST load cycle."""
        # Save
        resp = app_client.post(
            "/_robotocore/pods/save",
            json={"name": "api-test"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "saved"
        assert data["name"] == "api-test"

        # List
        resp = app_client.get("/_robotocore/pods")
        assert resp.status_code == 200
        data = resp.json()
        assert "api-test" in data["pods"]

        # Load
        resp = app_client.post(
            "/_robotocore/pods/load",
            json={"name": "api-test"},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "loaded"

    def test_delete_then_list_empty(self, app_client):
        """POST save -> DELETE -> GET list shows empty."""
        app_client.post("/_robotocore/pods/save", json={"name": "del-test"})
        resp = app_client.delete("/_robotocore/pods/del-test")
        assert resp.status_code == 200
        assert resp.json()["status"] == "deleted"

        resp = app_client.get("/_robotocore/pods")
        assert "del-test" not in resp.json()["pods"]

    def test_pod_info_endpoint(self, app_client):
        """GET /_robotocore/pods/{name} returns info + versions."""
        app_client.post("/_robotocore/pods/save", json={"name": "info-test"})

        resp = app_client.get("/_robotocore/pods/info-test")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "info-test"
        assert "versions" in data
        assert "size_bytes" in data
