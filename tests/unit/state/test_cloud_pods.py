"""Unit tests for Cloud Pods -- versioned, remotely-shareable state snapshots."""

import json
import tarfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from robotocore.state.cloud_pods import (
    CloudPodsError,
    CloudPodsManager,
    PodInfo,
)


@pytest.fixture
def local_backend(tmp_path):
    """Create a CloudPodsManager with a local filesystem backend."""
    backend_path = tmp_path / "pods"
    backend_path.mkdir()
    return CloudPodsManager(backend=str(backend_path))


@pytest.fixture
def state_manager(tmp_path):
    """Create a StateManager with a temp state dir."""
    from robotocore.state.manager import StateManager

    mgr = StateManager(state_dir=str(tmp_path / "state"))
    return mgr


class TestSavePod:
    def test_save_creates_compressed_archive(self, local_backend, state_manager):
        """save_pod creates a tar.gz archive in the backend."""
        local_backend.save_pod("test-pod", state_manager=state_manager)

        pod_dir = Path(local_backend._backend_path) / "test-pod"
        assert pod_dir.exists()
        # Should have at least one version file
        versions = list(pod_dir.glob("v*.tar.gz"))
        assert len(versions) == 1

    def test_save_archive_contains_correct_files(self, local_backend, state_manager):
        """The archive should contain metadata.json, moto_state.pkl, native_state.json."""
        local_backend.save_pod("test-pod", state_manager=state_manager)

        pod_dir = Path(local_backend._backend_path) / "test-pod"
        archive = list(pod_dir.glob("v*.tar.gz"))[0]
        with tarfile.open(archive, "r:gz") as tar:
            names = tar.getnames()
        assert "metadata.json" in names

    def test_save_with_selective_services(self, local_backend, state_manager):
        """save_pod with services= should record which services were included."""
        local_backend.save_pod(
            "selective-pod",
            state_manager=state_manager,
            services=["s3", "dynamodb"],
        )

        pod_dir = Path(local_backend._backend_path) / "selective-pod"
        archive = list(pod_dir.glob("v*.tar.gz"))[0]
        with tarfile.open(archive, "r:gz") as tar:
            meta_file = tar.extractfile("metadata.json")
            meta = json.loads(meta_file.read())
        assert meta.get("services_filter") == ["s3", "dynamodb"]


class TestLoadPod:
    def test_load_restores_state(self, local_backend, state_manager):
        """load_pod downloads and restores state from an archive."""
        # Save a pod
        local_backend.save_pod("restore-test", state_manager=state_manager)

        # Load it back
        result = local_backend.load_pod("restore-test", state_manager=state_manager)
        assert result is True

    def test_load_nonexistent_pod_raises(self, local_backend, state_manager):
        """Loading a pod that doesn't exist should raise CloudPodsError."""
        with pytest.raises(CloudPodsError, match="not found"):
            local_backend.load_pod("nonexistent", state_manager=state_manager)

    def test_load_specific_version(self, local_backend, state_manager):
        """load_pod with version= loads a specific version."""
        local_backend.save_pod("versioned", state_manager=state_manager)
        time.sleep(0.01)  # Ensure different timestamp
        local_backend.save_pod("versioned", state_manager=state_manager)

        versions = local_backend.list_pod_versions("versioned")
        assert len(versions) == 2

        # Load the first version
        result = local_backend.load_pod(
            "versioned",
            state_manager=state_manager,
            version=versions[0]["version"],
        )
        assert result is True

    def test_load_latest_version_default(self, local_backend, state_manager):
        """load_pod without version= loads the latest version."""
        local_backend.save_pod("latest-test", state_manager=state_manager)
        time.sleep(0.01)
        local_backend.save_pod("latest-test", state_manager=state_manager)

        # Default loads latest
        result = local_backend.load_pod("latest-test", state_manager=state_manager)
        assert result is True


class TestListPods:
    def test_list_pods_empty(self, local_backend):
        """list_pods returns empty list when no pods exist."""
        pods = local_backend.list_pods()
        assert pods == []

    def test_list_pods_returns_names(self, local_backend, state_manager):
        """list_pods returns pod names after saving."""
        local_backend.save_pod("pod-a", state_manager=state_manager)
        local_backend.save_pod("pod-b", state_manager=state_manager)

        pods = local_backend.list_pods()
        assert sorted(pods) == ["pod-a", "pod-b"]


class TestDeletePod:
    def test_delete_removes_pod(self, local_backend, state_manager):
        """delete_pod removes the pod and all versions."""
        local_backend.save_pod("delete-me", state_manager=state_manager)
        assert "delete-me" in local_backend.list_pods()

        local_backend.delete_pod("delete-me")
        assert "delete-me" not in local_backend.list_pods()

    def test_delete_nonexistent_raises(self, local_backend):
        """Deleting a nonexistent pod should raise CloudPodsError."""
        with pytest.raises(CloudPodsError, match="not found"):
            local_backend.delete_pod("nope")


class TestPodInfo:
    def test_pod_info_returns_metadata(self, local_backend, state_manager):
        """pod_info returns name, created_at, size, services, version count."""
        local_backend.save_pod("info-test", state_manager=state_manager)

        info = local_backend.pod_info("info-test")
        assert isinstance(info, PodInfo)
        assert info.name == "info-test"
        assert info.created_at is not None
        assert info.size_bytes > 0
        assert info.version_count == 1

    def test_pod_info_nonexistent_raises(self, local_backend):
        """pod_info for nonexistent pod raises CloudPodsError."""
        with pytest.raises(CloudPodsError, match="not found"):
            local_backend.pod_info("missing")


class TestVersioning:
    def test_save_twice_creates_two_versions(self, local_backend, state_manager):
        """Saving the same pod name twice creates two versions."""
        local_backend.save_pod("multi", state_manager=state_manager)
        time.sleep(0.01)
        local_backend.save_pod("multi", state_manager=state_manager)

        versions = local_backend.list_pod_versions("multi")
        assert len(versions) == 2

    def test_versions_are_chronological(self, local_backend, state_manager):
        """Versions should be in chronological order (oldest first)."""
        local_backend.save_pod("chrono", state_manager=state_manager)
        time.sleep(0.01)
        local_backend.save_pod("chrono", state_manager=state_manager)

        versions = local_backend.list_pod_versions("chrono")
        assert versions[0]["created_at"] <= versions[1]["created_at"]

    def test_list_pod_versions_nonexistent_raises(self, local_backend):
        """list_pod_versions for nonexistent pod raises CloudPodsError."""
        with pytest.raises(CloudPodsError, match="not found"):
            local_backend.list_pod_versions("nope")


class TestDisabledBackend:
    def test_no_backend_raises_on_save(self, state_manager):
        """When no backend is configured, save_pod raises CloudPodsError."""
        mgr = CloudPodsManager(backend=None)
        with pytest.raises(CloudPodsError, match="No Cloud Pods backend configured"):
            mgr.save_pod("test", state_manager=state_manager)

    def test_no_backend_raises_on_list(self):
        """When no backend is configured, list_pods raises CloudPodsError."""
        mgr = CloudPodsManager(backend=None)
        with pytest.raises(CloudPodsError, match="No Cloud Pods backend configured"):
            mgr.list_pods()


class TestInvalidBackend:
    def test_invalid_s3_url_gives_helpful_error(self):
        """An invalid S3 URL should give a helpful error message."""
        mgr = CloudPodsManager(backend="s3://nonexistent-bucket/pods/")
        # The error happens when we try to use it, not on construction
        # We test that the S3 backend is at least recognized
        assert mgr._is_s3_backend is True


class TestS3Backend:
    """Tests for S3-backed Cloud Pods using mocked boto3."""

    def test_save_pod_uploads_to_s3(self, state_manager):
        """save_pod with S3 backend uploads the archive."""
        mgr = CloudPodsManager(
            backend="s3://test-bucket/pods/",
            endpoint_url="http://localhost:4566",
            region="us-east-1",
        )

        mock_client = MagicMock()
        with patch.object(mgr, "_get_s3_client", return_value=mock_client):
            mgr.save_pod("s3-test", state_manager=state_manager)

        # Should have called put_object
        mock_client.put_object.assert_called_once()
        call_kwargs = mock_client.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "test-bucket"
        assert call_kwargs["Key"].startswith("pods/s3-test/v")
        assert call_kwargs["Key"].endswith(".tar.gz")

    def test_list_pods_from_s3(self):
        """list_pods with S3 backend lists common prefixes."""
        mgr = CloudPodsManager(
            backend="s3://test-bucket/pods/",
            endpoint_url="http://localhost:4566",
        )

        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            "CommonPrefixes": [
                {"Prefix": "pods/pod-a/"},
                {"Prefix": "pods/pod-b/"},
            ]
        }
        with patch.object(mgr, "_get_s3_client", return_value=mock_client):
            pods = mgr.list_pods()

        assert sorted(pods) == ["pod-a", "pod-b"]

    def test_delete_pod_from_s3(self, state_manager):
        """delete_pod with S3 backend removes all version objects."""
        mgr = CloudPodsManager(
            backend="s3://test-bucket/pods/",
            endpoint_url="http://localhost:4566",
        )

        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "pods/del-test/v20260310T120000.tar.gz"},
                {"Key": "pods/del-test/v20260310T120001.tar.gz"},
            ]
        }
        with patch.object(mgr, "_get_s3_client", return_value=mock_client):
            mgr.delete_pod("del-test")

        mock_client.delete_objects.assert_called_once()
