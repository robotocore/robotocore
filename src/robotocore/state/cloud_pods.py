"""Cloud Pods -- versioned, remotely-shareable state snapshots.

Push/pull emulator state snapshots to a remote S3-compatible store or local
filesystem path. This is a transport layer on top of the existing StateManager
save/load system.

Configuration via environment variables:
    CLOUD_PODS_BACKEND   -- S3 bucket URL (s3://bucket/prefix/) or local path
    CLOUD_PODS_ENDPOINT  -- S3 endpoint URL (for MinIO, robotocore, LocalStack)
    CLOUD_PODS_REGION    -- AWS region for the S3 bucket (default: us-east-1)
"""

import io
import json
import logging
import os
import shutil
import tarfile
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class CloudPodsError(Exception):
    """Raised for Cloud Pods operational errors."""


@dataclass
class PodInfo:
    """Metadata about a cloud pod."""

    name: str
    created_at: float | None = None
    size_bytes: int = 0
    version_count: int = 0
    services_filter: list[str] | None = None
    versions: list[dict] = field(default_factory=list)


class CloudPodsManager:
    """Manages versioned state snapshots in a local or S3-compatible backend."""

    def __init__(
        self,
        backend: str | None = None,
        endpoint_url: str | None = None,
        region: str | None = None,
    ) -> None:
        self._backend_raw = backend
        self._endpoint_url = endpoint_url
        self._region = region or "us-east-1"

        # Parse backend URL
        if backend and backend.startswith("s3://"):
            self._is_s3_backend = True
            parsed = urlparse(backend)
            self._s3_bucket = parsed.netloc
            self._s3_prefix = parsed.path.lstrip("/")
            if self._s3_prefix and not self._s3_prefix.endswith("/"):
                self._s3_prefix += "/"
            self._backend_path: str | None = None
        elif backend:
            self._is_s3_backend = False
            self._backend_path = backend
            self._s3_bucket = ""
            self._s3_prefix = ""
        else:
            self._is_s3_backend = False
            self._backend_path = None
            self._s3_bucket = ""
            self._s3_prefix = ""

    def _require_backend(self) -> None:
        """Raise if no backend is configured."""
        if not self._backend_raw:
            raise CloudPodsError(
                "No Cloud Pods backend configured. "
                "Set CLOUD_PODS_BACKEND to an S3 URL or local path."
            )

    def _get_s3_client(self):
        """Create a boto3 S3 client for the configured backend."""
        import boto3

        kwargs: dict = {"region_name": self._region}
        if self._endpoint_url:
            kwargs["endpoint_url"] = self._endpoint_url
        return boto3.client("s3", **kwargs)

    # -----------------------------------------------------------------------
    # Pod operations
    # -----------------------------------------------------------------------

    def save_pod(
        self,
        name: str,
        state_manager=None,
        services: list[str] | None = None,
    ) -> str:
        """Snapshot current state, compress, and upload to backend.

        Returns the version identifier (timestamp string).
        """
        self._require_backend()

        if state_manager is None:
            from robotocore.state.manager import get_state_manager

            state_manager = get_state_manager()

        # Use microsecond precision to avoid collisions on rapid saves
        timestamp = time.strftime("%Y%m%dT%H%M%S") + f".{int(time.time() * 1000000) % 1000000:06d}"
        version = f"v{timestamp}"

        # Save state to a temp directory
        with tempfile.TemporaryDirectory() as tmpdir:
            state_manager.save(path=tmpdir, services=services)

            # Inject pod-specific metadata
            meta_path = Path(tmpdir) / "metadata.json"
            if meta_path.exists():
                meta = json.loads(meta_path.read_text())
            else:
                meta = {}
            meta["pod_name"] = name
            meta["pod_version"] = version
            meta["services_filter"] = services
            meta_path.write_text(json.dumps(meta, indent=2, default=str))

            # Compress
            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w:gz") as tar:
                for item in Path(tmpdir).iterdir():
                    tar.add(str(item), arcname=item.name)
            archive_bytes = buf.getvalue()

        # Upload / write
        if self._is_s3_backend:
            self._s3_put(name, version, archive_bytes)
        else:
            self._local_put(name, version, archive_bytes)

        logger.info("Saved pod '%s' version %s (%d bytes)", name, version, len(archive_bytes))
        return version

    def load_pod(
        self,
        name: str,
        state_manager=None,
        version: str | None = None,
    ) -> bool:
        """Download a pod from backend and restore state.

        Args:
            name: Pod name.
            state_manager: StateManager to restore into.
            version: Specific version to load (default: latest).

        Returns True on success.
        """
        self._require_backend()

        if state_manager is None:
            from robotocore.state.manager import get_state_manager

            state_manager = get_state_manager()

        if version is None:
            # Find latest
            versions = self.list_pod_versions(name)
            if not versions:
                raise CloudPodsError(f"Pod '{name}' not found or has no versions")
            version = versions[-1]["version"]

        # Download / read
        if self._is_s3_backend:
            archive_bytes = self._s3_get(name, version)
        else:
            archive_bytes = self._local_get(name, version)

        # Extract and load
        with tempfile.TemporaryDirectory() as tmpdir:
            with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tar:
                tar.extractall(tmpdir, filter="data")

            tmp_path = Path(tmpdir)
            moto_path = tmp_path / "moto_state.pkl"
            native_path = tmp_path / "native_state.json"

            if moto_path.exists():
                state_manager._load_moto_state(moto_path)
            if native_path.exists():
                state_manager._load_native_state(native_path)

        logger.info("Loaded pod '%s' version %s", name, version)
        return True

    def list_pods(self) -> list[str]:
        """List available pod names in the backend."""
        self._require_backend()

        if self._is_s3_backend:
            return self._s3_list_pods()
        return self._local_list_pods()

    def delete_pod(self, name: str) -> None:
        """Remove a pod and all its versions from the backend."""
        self._require_backend()

        if self._is_s3_backend:
            self._s3_delete_pod(name)
        else:
            self._local_delete_pod(name)

        logger.info("Deleted pod '%s'", name)

    def pod_info(self, name: str) -> PodInfo:
        """Get metadata about a pod: name, created_at, size, versions."""
        self._require_backend()

        versions = self.list_pod_versions(name)
        if not versions:
            raise CloudPodsError(f"Pod '{name}' not found")

        total_size = sum(v.get("size_bytes", 0) for v in versions)
        created_at = versions[0].get("created_at") if versions else None

        # Read metadata from latest version to get services_filter
        services_filter = None
        if self._is_s3_backend:
            archive_bytes = self._s3_get(name, versions[-1]["version"])
        else:
            archive_bytes = self._local_get(name, versions[-1]["version"])

        with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tar:
            try:
                meta_file = tar.extractfile("metadata.json")
                if meta_file:
                    meta = json.loads(meta_file.read())
                    services_filter = meta.get("services_filter")
            except KeyError:
                logger.debug("No metadata.json in pod archive %r", name)

        return PodInfo(
            name=name,
            created_at=created_at,
            size_bytes=total_size,
            version_count=len(versions),
            services_filter=services_filter,
            versions=versions,
        )

    def list_pod_versions(self, name: str) -> list[dict]:
        """List versions of a pod, oldest first.

        Each entry: {"version": "v20260310T120000", "created_at": <float>, "size_bytes": <int>}
        """
        self._require_backend()

        if self._is_s3_backend:
            return self._s3_list_versions(name)
        return self._local_list_versions(name)

    # -----------------------------------------------------------------------
    # Local filesystem backend
    # -----------------------------------------------------------------------

    def _local_put(self, name: str, version: str, data: bytes) -> None:
        pod_dir = Path(self._backend_path) / name
        pod_dir.mkdir(parents=True, exist_ok=True)
        (pod_dir / f"{version}.tar.gz").write_bytes(data)

    def _local_get(self, name: str, version: str) -> bytes:
        archive = Path(self._backend_path) / name / f"{version}.tar.gz"
        if not archive.exists():
            raise CloudPodsError(f"Pod '{name}' version '{version}' not found")
        return archive.read_bytes()

    def _local_list_pods(self) -> list[str]:
        backend = Path(self._backend_path)
        if not backend.exists():
            return []
        return sorted(d.name for d in backend.iterdir() if d.is_dir() and any(d.glob("v*.tar.gz")))

    def _local_delete_pod(self, name: str) -> None:
        pod_dir = Path(self._backend_path) / name
        if not pod_dir.exists():
            raise CloudPodsError(f"Pod '{name}' not found")
        shutil.rmtree(pod_dir)

    def _local_list_versions(self, name: str) -> list[dict]:
        pod_dir = Path(self._backend_path) / name
        if not pod_dir.exists():
            raise CloudPodsError(f"Pod '{name}' not found")
        versions = []
        for f in sorted(pod_dir.glob("v*.tar.gz")):
            version_id = f.name.removesuffix(".tar.gz")  # e.g. "v20260310T120000.123456"
            stat = f.stat()
            created = stat.st_mtime
            versions.append(
                {
                    "version": version_id,
                    "created_at": created,
                    "size_bytes": stat.st_size,
                }
            )
        return versions

    # -----------------------------------------------------------------------
    # S3 backend
    # -----------------------------------------------------------------------

    def _s3_put(self, name: str, version: str, data: bytes) -> None:
        client = self._get_s3_client()
        key = f"{self._s3_prefix}{name}/{version}.tar.gz"
        client.put_object(Bucket=self._s3_bucket, Key=key, Body=data)

    def _s3_get(self, name: str, version: str) -> bytes:
        client = self._get_s3_client()
        key = f"{self._s3_prefix}{name}/{version}.tar.gz"
        try:
            resp = client.get_object(Bucket=self._s3_bucket, Key=key)
            return resp["Body"].read()
        except client.exceptions.NoSuchKey:
            raise CloudPodsError(f"Pod '{name}' version '{version}' not found")

    def _s3_list_pods(self) -> list[str]:
        client = self._get_s3_client()
        resp = client.list_objects_v2(
            Bucket=self._s3_bucket,
            Prefix=self._s3_prefix,
            Delimiter="/",
        )
        pods = []
        for cp in resp.get("CommonPrefixes", []):
            prefix = cp["Prefix"]
            # Remove the base prefix and trailing slash
            name = prefix[len(self._s3_prefix) :].rstrip("/")
            if name:
                pods.append(name)
        return sorted(pods)

    def _s3_delete_pod(self, name: str) -> None:
        client = self._get_s3_client()
        prefix = f"{self._s3_prefix}{name}/"
        resp = client.list_objects_v2(Bucket=self._s3_bucket, Prefix=prefix)
        objects = resp.get("Contents", [])
        if not objects:
            raise CloudPodsError(f"Pod '{name}' not found")
        client.delete_objects(
            Bucket=self._s3_bucket,
            Delete={"Objects": [{"Key": obj["Key"]} for obj in objects]},
        )

    def _s3_list_versions(self, name: str) -> list[dict]:
        client = self._get_s3_client()
        prefix = f"{self._s3_prefix}{name}/"
        resp = client.list_objects_v2(Bucket=self._s3_bucket, Prefix=prefix)
        objects = resp.get("Contents", [])
        if not objects:
            raise CloudPodsError(f"Pod '{name}' not found")

        versions = []
        for obj in sorted(objects, key=lambda o: o["Key"]):
            key = obj["Key"]
            filename = key.rsplit("/", 1)[-1]
            if not filename.startswith("v") or not filename.endswith(".tar.gz"):
                continue
            version_id = filename.removesuffix(".tar.gz")
            versions.append(
                {
                    "version": version_id,
                    "created_at": obj.get("LastModified", time.time()),
                    "size_bytes": obj.get("Size", 0),
                }
            )
        return versions


# -----------------------------------------------------------------------
# Module-level singleton
# -----------------------------------------------------------------------

_manager: CloudPodsManager | None = None


def get_cloud_pods_manager() -> CloudPodsManager:
    """Get or create the global CloudPodsManager from env vars."""
    global _manager
    if _manager is None:
        backend = os.environ.get("CLOUD_PODS_BACKEND")
        endpoint_url = os.environ.get("CLOUD_PODS_ENDPOINT")
        region = os.environ.get("CLOUD_PODS_REGION")
        _manager = CloudPodsManager(
            backend=backend,
            endpoint_url=endpoint_url,
            region=region,
        )
    return _manager


def reset_cloud_pods_manager() -> None:
    """Reset the singleton (for testing)."""
    global _manager
    _manager = None
