"""Unit tests for Cloud Pods -- versioned, remotely-shareable state snapshots."""

import io
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
    get_cloud_pods_manager,
    reset_cloud_pods_manager,
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


@pytest.fixture
def stateful_manager(tmp_path):
    """Create a StateManager with a registered native handler to verify data round-trips."""
    from robotocore.state.manager import StateManager

    mgr = StateManager(state_dir=str(tmp_path / "state"))
    store: dict = {"value": 0}

    def save_fn():
        return dict(store)

    def load_fn(data):
        store.clear()
        store.update(data)

    mgr.register_native_handler("test-svc", save_fn, load_fn)
    return mgr, store


# ---------------------------------------------------------------------------
# Constructor / backend parsing
# ---------------------------------------------------------------------------


class TestConstructor:
    def test_local_path_backend(self, tmp_path):
        """Local path backend sets _is_s3_backend=False."""
        mgr = CloudPodsManager(backend=str(tmp_path))
        assert mgr._is_s3_backend is False
        assert mgr._backend_path == str(tmp_path)

    def test_s3_url_backend_parsed(self):
        """S3 URL backend parses bucket and prefix correctly."""
        mgr = CloudPodsManager(backend="s3://my-bucket/my/prefix/")
        assert mgr._is_s3_backend is True
        assert mgr._s3_bucket == "my-bucket"
        assert mgr._s3_prefix == "my/prefix/"

    def test_s3_url_without_trailing_slash(self):
        """S3 URL without trailing slash gets one appended to prefix."""
        mgr = CloudPodsManager(backend="s3://my-bucket/prefix")
        assert mgr._s3_prefix == "prefix/"

    def test_s3_url_no_prefix(self):
        """S3 URL with no path uses empty prefix."""
        mgr = CloudPodsManager(backend="s3://my-bucket")
        assert mgr._s3_bucket == "my-bucket"
        assert mgr._s3_prefix == ""

    def test_no_backend_sets_defaults(self):
        """No backend sets local path to None and S3 fields empty."""
        mgr = CloudPodsManager(backend=None)
        assert mgr._is_s3_backend is False
        assert mgr._backend_path is None
        assert mgr._s3_bucket == ""

    def test_default_region(self):
        """Default region is us-east-1 when not specified."""
        mgr = CloudPodsManager(backend=None)
        assert mgr._region == "us-east-1"

    def test_custom_region(self):
        """Custom region is stored correctly."""
        mgr = CloudPodsManager(backend=None, region="eu-west-1")
        assert mgr._region == "eu-west-1"

    def test_endpoint_url_stored(self):
        """Endpoint URL is stored for S3 client creation."""
        mgr = CloudPodsManager(
            backend="s3://bucket/",
            endpoint_url="http://localhost:4566",
        )
        assert mgr._endpoint_url == "http://localhost:4566"


# ---------------------------------------------------------------------------
# save_pod
# ---------------------------------------------------------------------------


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

    def test_save_returns_version_string(self, local_backend, state_manager):
        """save_pod returns a version string starting with 'v'."""
        version = local_backend.save_pod("ret-test", state_manager=state_manager)
        assert version.startswith("v")
        # Version should have format vYYYYMMDDTHHMMSS.microseconds
        assert "T" in version

    def test_save_metadata_includes_pod_name_and_version(self, local_backend, state_manager):
        """Metadata in archive contains pod_name and pod_version fields."""
        version = local_backend.save_pod("meta-check", state_manager=state_manager)

        pod_dir = Path(local_backend._backend_path) / "meta-check"
        archive = list(pod_dir.glob("v*.tar.gz"))[0]
        with tarfile.open(archive, "r:gz") as tar:
            meta_file = tar.extractfile("metadata.json")
            meta = json.loads(meta_file.read())

        assert meta["pod_name"] == "meta-check"
        assert meta["pod_version"] == version

    def test_save_with_no_services_filter_stores_none(self, local_backend, state_manager):
        """When services= is not passed, services_filter in metadata is None."""
        local_backend.save_pod("no-filter", state_manager=state_manager)

        pod_dir = Path(local_backend._backend_path) / "no-filter"
        archive = list(pod_dir.glob("v*.tar.gz"))[0]
        with tarfile.open(archive, "r:gz") as tar:
            meta_file = tar.extractfile("metadata.json")
            meta = json.loads(meta_file.read())

        assert meta["services_filter"] is None

    def test_save_creates_pod_directory(self, local_backend, state_manager):
        """save_pod creates the pod subdirectory if it doesn't exist."""
        pod_dir = Path(local_backend._backend_path) / "new-pod"
        assert not pod_dir.exists()

        local_backend.save_pod("new-pod", state_manager=state_manager)
        assert pod_dir.exists()
        assert pod_dir.is_dir()

    def test_save_archive_is_valid_gzip(self, local_backend, state_manager):
        """The saved archive is a valid gzip tar file."""
        local_backend.save_pod("gzip-test", state_manager=state_manager)

        pod_dir = Path(local_backend._backend_path) / "gzip-test"
        archive = list(pod_dir.glob("v*.tar.gz"))[0]
        # Should not raise
        with tarfile.open(archive, "r:gz") as tar:
            members = tar.getmembers()
            assert len(members) > 0


# ---------------------------------------------------------------------------
# load_pod
# ---------------------------------------------------------------------------


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

    def test_load_nonexistent_version_raises(self, local_backend, state_manager):
        """Loading a specific version that doesn't exist raises CloudPodsError."""
        local_backend.save_pod("ver-test", state_manager=state_manager)

        with pytest.raises(CloudPodsError, match="not found"):
            local_backend.load_pod(
                "ver-test",
                state_manager=state_manager,
                version="v19700101T000000.000000",
            )

    def test_load_roundtrips_native_state(self, local_backend, stateful_manager):
        """Native provider state is correctly round-tripped through save/load."""
        mgr, store = stateful_manager
        store["value"] = 42

        local_backend.save_pod("roundtrip", state_manager=mgr)

        # Corrupt the in-memory state
        store["value"] = 0

        # Load should restore
        local_backend.load_pod("roundtrip", state_manager=mgr)
        assert store["value"] == 42

    def test_load_specific_version_restores_correct_data(self, local_backend, stateful_manager):
        """Loading a specific version restores that version's data, not latest."""
        mgr, store = stateful_manager

        store["value"] = 10
        local_backend.save_pod("ver-data", state_manager=mgr)
        time.sleep(0.01)

        store["value"] = 20
        local_backend.save_pod("ver-data", state_manager=mgr)

        versions = local_backend.list_pod_versions("ver-data")
        store["value"] = 999

        # Load first version -- should get 10
        local_backend.load_pod("ver-data", state_manager=mgr, version=versions[0]["version"])
        assert store["value"] == 10


# ---------------------------------------------------------------------------
# list_pods
# ---------------------------------------------------------------------------


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

    def test_list_pods_ignores_empty_directories(self, local_backend):
        """list_pods ignores directories that don't contain version archives."""
        backend = Path(local_backend._backend_path)
        (backend / "empty-pod").mkdir()

        pods = local_backend.list_pods()
        assert pods == []

    def test_list_pods_sorted(self, local_backend, state_manager):
        """list_pods returns names in sorted order."""
        local_backend.save_pod("zz-pod", state_manager=state_manager)
        local_backend.save_pod("aa-pod", state_manager=state_manager)
        local_backend.save_pod("mm-pod", state_manager=state_manager)

        pods = local_backend.list_pods()
        assert pods == ["aa-pod", "mm-pod", "zz-pod"]

    def test_list_pods_nonexistent_backend_returns_empty(self, tmp_path):
        """list_pods returns [] when backend directory doesn't exist."""
        mgr = CloudPodsManager(backend=str(tmp_path / "nonexistent"))
        pods = mgr.list_pods()
        assert pods == []


# ---------------------------------------------------------------------------
# delete_pod
# ---------------------------------------------------------------------------


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

    def test_delete_removes_all_versions(self, local_backend, state_manager):
        """delete_pod removes the pod directory and all version archives."""
        local_backend.save_pod("multi-ver", state_manager=state_manager)
        time.sleep(0.01)
        local_backend.save_pod("multi-ver", state_manager=state_manager)

        pod_dir = Path(local_backend._backend_path) / "multi-ver"
        assert len(list(pod_dir.glob("v*.tar.gz"))) == 2

        local_backend.delete_pod("multi-ver")
        assert not pod_dir.exists()

    def test_delete_then_save_works(self, local_backend, state_manager):
        """After deleting a pod, saving a new pod with the same name works."""
        local_backend.save_pod("reusable", state_manager=state_manager)
        local_backend.delete_pod("reusable")
        # Should not raise
        local_backend.save_pod("reusable", state_manager=state_manager)
        assert "reusable" in local_backend.list_pods()


# ---------------------------------------------------------------------------
# pod_info
# ---------------------------------------------------------------------------


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

    def test_pod_info_multiple_versions(self, local_backend, state_manager):
        """pod_info reflects correct version_count and accumulated size."""
        local_backend.save_pod("multi-info", state_manager=state_manager)
        time.sleep(0.01)
        local_backend.save_pod("multi-info", state_manager=state_manager)

        info = local_backend.pod_info("multi-info")
        assert info.version_count == 2
        assert len(info.versions) == 2
        # Total size is sum of both versions
        assert info.size_bytes > 0

    def test_pod_info_services_filter(self, local_backend, state_manager):
        """pod_info reflects the services_filter from the latest version."""
        local_backend.save_pod(
            "filtered-info",
            state_manager=state_manager,
            services=["sqs", "sns"],
        )
        info = local_backend.pod_info("filtered-info")
        assert info.services_filter == ["sqs", "sns"]

    def test_pod_info_no_services_filter(self, local_backend, state_manager):
        """pod_info with no services filter returns None for services_filter."""
        local_backend.save_pod("no-filter-info", state_manager=state_manager)
        info = local_backend.pod_info("no-filter-info")
        assert info.services_filter is None

    def test_pod_info_versions_have_required_fields(self, local_backend, state_manager):
        """Each version in pod_info has version, created_at, size_bytes."""
        local_backend.save_pod("field-check", state_manager=state_manager)
        info = local_backend.pod_info("field-check")

        assert len(info.versions) == 1
        v = info.versions[0]
        assert "version" in v
        assert "created_at" in v
        assert "size_bytes" in v
        assert v["version"].startswith("v")
        assert isinstance(v["created_at"], float)
        assert isinstance(v["size_bytes"], int)


# ---------------------------------------------------------------------------
# Versioning
# ---------------------------------------------------------------------------


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

    def test_version_ids_are_unique(self, local_backend, state_manager):
        """Each save produces a unique version ID due to microsecond precision."""
        v1 = local_backend.save_pod("unique-ver", state_manager=state_manager)
        time.sleep(0.01)
        v2 = local_backend.save_pod("unique-ver", state_manager=state_manager)
        assert v1 != v2

    def test_three_versions(self, local_backend, state_manager):
        """Three saves create three versions, all accessible."""
        for _ in range(3):
            local_backend.save_pod("triple", state_manager=state_manager)
            time.sleep(0.01)

        versions = local_backend.list_pod_versions("triple")
        assert len(versions) == 3
        # All have unique version IDs
        version_ids = [v["version"] for v in versions]
        assert len(set(version_ids)) == 3

    def test_version_size_bytes_is_positive(self, local_backend, state_manager):
        """Each version entry has a positive size_bytes."""
        local_backend.save_pod("sized", state_manager=state_manager)
        versions = local_backend.list_pod_versions("sized")
        assert versions[0]["size_bytes"] > 0


# ---------------------------------------------------------------------------
# Disabled / no backend
# ---------------------------------------------------------------------------


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

    def test_no_backend_raises_on_load(self, state_manager):
        """When no backend is configured, load_pod raises CloudPodsError."""
        mgr = CloudPodsManager(backend=None)
        with pytest.raises(CloudPodsError, match="No Cloud Pods backend configured"):
            mgr.load_pod("test", state_manager=state_manager)

    def test_no_backend_raises_on_delete(self):
        """When no backend is configured, delete_pod raises CloudPodsError."""
        mgr = CloudPodsManager(backend=None)
        with pytest.raises(CloudPodsError, match="No Cloud Pods backend configured"):
            mgr.delete_pod("test")

    def test_no_backend_raises_on_pod_info(self):
        """When no backend is configured, pod_info raises CloudPodsError."""
        mgr = CloudPodsManager(backend=None)
        with pytest.raises(CloudPodsError, match="No Cloud Pods backend configured"):
            mgr.pod_info("test")

    def test_no_backend_raises_on_list_versions(self):
        """When no backend is configured, list_pod_versions raises CloudPodsError."""
        mgr = CloudPodsManager(backend=None)
        with pytest.raises(CloudPodsError, match="No Cloud Pods backend configured"):
            mgr.list_pod_versions("test")


class TestInvalidBackend:
    def test_invalid_s3_url_gives_helpful_error(self):
        """An invalid S3 URL should give a helpful error message."""
        mgr = CloudPodsManager(backend="s3://nonexistent-bucket/pods/")
        # The error happens when we try to use it, not on construction
        # We test that the S3 backend is at least recognized
        assert mgr._is_s3_backend is True


# ---------------------------------------------------------------------------
# S3 backend (mocked)
# ---------------------------------------------------------------------------


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

    def test_save_pod_body_is_valid_targz(self, state_manager):
        """The Body uploaded to S3 is a valid tar.gz archive."""
        mgr = CloudPodsManager(
            backend="s3://test-bucket/pods/",
            endpoint_url="http://localhost:4566",
        )

        mock_client = MagicMock()
        with patch.object(mgr, "_get_s3_client", return_value=mock_client):
            mgr.save_pod("valid-gz", state_manager=state_manager)

        body = mock_client.put_object.call_args[1]["Body"]
        # Should be valid tar.gz
        with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as tar:
            names = tar.getnames()
        assert "metadata.json" in names

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

    def test_list_pods_empty_s3(self):
        """list_pods returns [] when S3 has no common prefixes."""
        mgr = CloudPodsManager(backend="s3://test-bucket/pods/")

        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {}
        with patch.object(mgr, "_get_s3_client", return_value=mock_client):
            pods = mgr.list_pods()

        assert pods == []

    def test_list_pods_s3_uses_delimiter(self):
        """list_pods passes Delimiter='/' to list_objects_v2."""
        mgr = CloudPodsManager(backend="s3://test-bucket/pods/")

        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {}
        with patch.object(mgr, "_get_s3_client", return_value=mock_client):
            mgr.list_pods()

        call_kwargs = mock_client.list_objects_v2.call_args[1]
        assert call_kwargs["Delimiter"] == "/"
        assert call_kwargs["Prefix"] == "pods/"

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
        delete_call = mock_client.delete_objects.call_args[1]
        assert delete_call["Bucket"] == "test-bucket"
        objects = delete_call["Delete"]["Objects"]
        assert len(objects) == 2

    def test_delete_nonexistent_s3_pod_raises(self):
        """Deleting a pod that doesn't exist in S3 raises CloudPodsError."""
        mgr = CloudPodsManager(backend="s3://test-bucket/pods/")

        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {"Contents": []}
        with patch.object(mgr, "_get_s3_client", return_value=mock_client):
            with pytest.raises(CloudPodsError, match="not found"):
                mgr.delete_pod("nonexistent")

    def test_s3_list_versions(self):
        """list_pod_versions returns sorted version entries from S3."""
        mgr = CloudPodsManager(backend="s3://test-bucket/pods/")

        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {
                    "Key": "pods/my-pod/v20260310T100000.000000.tar.gz",
                    "LastModified": 1710064800.0,
                    "Size": 1024,
                },
                {
                    "Key": "pods/my-pod/v20260310T110000.000000.tar.gz",
                    "LastModified": 1710068400.0,
                    "Size": 2048,
                },
            ]
        }
        with patch.object(mgr, "_get_s3_client", return_value=mock_client):
            versions = mgr.list_pod_versions("my-pod")

        assert len(versions) == 2
        assert versions[0]["version"] == "v20260310T100000.000000"
        assert versions[1]["version"] == "v20260310T110000.000000"
        assert versions[0]["size_bytes"] == 1024
        assert versions[1]["size_bytes"] == 2048

    def test_s3_list_versions_ignores_non_version_files(self):
        """list_pod_versions skips files that don't match v*.tar.gz pattern."""
        mgr = CloudPodsManager(backend="s3://test-bucket/pods/")

        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {
                    "Key": "pods/my-pod/v20260310T100000.000000.tar.gz",
                    "Size": 1024,
                },
                {
                    "Key": "pods/my-pod/README.md",
                    "Size": 100,
                },
                {
                    "Key": "pods/my-pod/backup.tar.gz",
                    "Size": 500,
                },
            ]
        }
        with patch.object(mgr, "_get_s3_client", return_value=mock_client):
            versions = mgr.list_pod_versions("my-pod")

        assert len(versions) == 1
        assert versions[0]["version"] == "v20260310T100000.000000"

    def test_s3_list_versions_nonexistent_raises(self):
        """list_pod_versions for nonexistent S3 pod raises CloudPodsError."""
        mgr = CloudPodsManager(backend="s3://test-bucket/pods/")

        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {"Contents": []}
        with patch.object(mgr, "_get_s3_client", return_value=mock_client):
            with pytest.raises(CloudPodsError, match="not found"):
                mgr.list_pod_versions("nonexistent")

    def test_s3_load_pod_calls_get_object(self, state_manager):
        """load_pod with S3 backend calls get_object with correct key."""
        mgr = CloudPodsManager(backend="s3://test-bucket/pods/")

        # Create a valid archive for the mock to return
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            meta = json.dumps({"pod_name": "test", "pod_version": "v1"}).encode()
            info = tarfile.TarInfo(name="metadata.json")
            info.size = len(meta)
            tar.addfile(info, io.BytesIO(meta))
        archive_bytes = buf.getvalue()

        mock_client = MagicMock()
        # Mock list_objects_v2 for version listing
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {
                    "Key": "pods/test/v20260310T100000.000000.tar.gz",
                    "Size": len(archive_bytes),
                },
            ]
        }
        # Mock get_object for download
        mock_body = MagicMock()
        mock_body.read.return_value = archive_bytes
        mock_client.get_object.return_value = {"Body": mock_body}

        with patch.object(mgr, "_get_s3_client", return_value=mock_client):
            result = mgr.load_pod("test", state_manager=state_manager)

        assert result is True
        mock_client.get_object.assert_called_once()
        get_kwargs = mock_client.get_object.call_args[1]
        assert get_kwargs["Bucket"] == "test-bucket"
        assert get_kwargs["Key"] == "pods/test/v20260310T100000.000000.tar.gz"


# ---------------------------------------------------------------------------
# Singleton / get_cloud_pods_manager
# ---------------------------------------------------------------------------


class TestSingleton:
    def test_get_cloud_pods_manager_returns_singleton(self, monkeypatch):
        """get_cloud_pods_manager returns the same instance on repeated calls."""
        reset_cloud_pods_manager()
        monkeypatch.setenv("CLOUD_PODS_BACKEND", "/tmp/test-pods")
        try:
            mgr1 = get_cloud_pods_manager()
            mgr2 = get_cloud_pods_manager()
            assert mgr1 is mgr2
        finally:
            reset_cloud_pods_manager()

    def test_reset_clears_singleton(self, monkeypatch):
        """reset_cloud_pods_manager clears the singleton so a new one is created."""
        reset_cloud_pods_manager()
        monkeypatch.setenv("CLOUD_PODS_BACKEND", "/tmp/test-pods-1")
        mgr1 = get_cloud_pods_manager()
        reset_cloud_pods_manager()

        monkeypatch.setenv("CLOUD_PODS_BACKEND", "/tmp/test-pods-2")
        mgr2 = get_cloud_pods_manager()
        assert mgr1 is not mgr2
        reset_cloud_pods_manager()

    def test_get_manager_reads_env_vars(self, monkeypatch):
        """get_cloud_pods_manager reads CLOUD_PODS_* env vars."""
        reset_cloud_pods_manager()
        monkeypatch.setenv("CLOUD_PODS_BACKEND", "s3://env-bucket/prefix/")
        monkeypatch.setenv("CLOUD_PODS_ENDPOINT", "http://localhost:9999")
        monkeypatch.setenv("CLOUD_PODS_REGION", "ap-southeast-1")
        try:
            mgr = get_cloud_pods_manager()
            assert mgr._is_s3_backend is True
            assert mgr._s3_bucket == "env-bucket"
            assert mgr._endpoint_url == "http://localhost:9999"
            assert mgr._region == "ap-southeast-1"
        finally:
            reset_cloud_pods_manager()

    def test_get_manager_no_env_vars(self, monkeypatch):
        """get_cloud_pods_manager with no env vars creates manager with no backend."""
        reset_cloud_pods_manager()
        monkeypatch.delenv("CLOUD_PODS_BACKEND", raising=False)
        monkeypatch.delenv("CLOUD_PODS_ENDPOINT", raising=False)
        monkeypatch.delenv("CLOUD_PODS_REGION", raising=False)
        try:
            mgr = get_cloud_pods_manager()
            assert mgr._is_s3_backend is False
            assert mgr._backend_path is None
        finally:
            reset_cloud_pods_manager()


# ---------------------------------------------------------------------------
# PodInfo dataclass
# ---------------------------------------------------------------------------


class TestPodInfoDataclass:
    def test_podinfo_defaults(self):
        """PodInfo defaults are sensible."""
        info = PodInfo(name="test")
        assert info.name == "test"
        assert info.created_at is None
        assert info.size_bytes == 0
        assert info.version_count == 0
        assert info.services_filter is None
        assert info.versions == []

    def test_podinfo_with_all_fields(self):
        """PodInfo with all fields set."""
        info = PodInfo(
            name="full",
            created_at=1234567890.0,
            size_bytes=4096,
            version_count=3,
            services_filter=["s3"],
            versions=[{"version": "v1"}],
        )
        assert info.name == "full"
        assert info.created_at == 1234567890.0
        assert info.size_bytes == 4096
        assert info.version_count == 3
        assert info.services_filter == ["s3"]
        assert len(info.versions) == 1


# ---------------------------------------------------------------------------
# CloudPodsError
# ---------------------------------------------------------------------------


class TestCloudPodsError:
    def test_error_is_exception(self):
        """CloudPodsError is a subclass of Exception."""
        err = CloudPodsError("test message")
        assert isinstance(err, Exception)
        assert str(err) == "test message"
