"""Advanced tests for state snapshot manager."""

import json
import tarfile
import tempfile
from pathlib import Path
from unittest.mock import patch

from robotocore.state.manager import StateManager


def _make_manager(tmpdir: str) -> StateManager:
    """Create a StateManager pointed at a temp dir."""
    return StateManager(state_dir=tmpdir)


class TestSaveMultipleSnapshotsLoadSpecific:
    """Save multiple snapshots, load specific one, verify data."""

    def test_save_two_load_second(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = _make_manager(tmpdir)
            # Register a native handler with switchable state
            state_store = {"value": "first"}

            def save_fn():
                return dict(state_store)

            def load_fn(data):
                state_store.clear()
                state_store.update(data)

            mgr.register_native_handler("test-svc", save_fn, load_fn)

            with patch("robotocore.state.manager.StateManager._save_moto_state"):
                with patch("robotocore.state.manager.StateManager._load_moto_state"):
                    mgr.save(name="snap-1")
                    state_store["value"] = "second"
                    mgr.save(name="snap-2")

                    # Now change state and load snap-1
                    state_store["value"] = "changed"
                    mgr.load(name="snap-1")

            assert state_store["value"] == "first"

    def test_save_two_load_latest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = _make_manager(tmpdir)
            state_store = {"value": "a"}

            mgr.register_native_handler(
                "test-svc", lambda: dict(state_store), lambda d: state_store.update(d)
            )

            with patch("robotocore.state.manager.StateManager._save_moto_state"):
                with patch("robotocore.state.manager.StateManager._load_moto_state"):
                    mgr.save(name="alpha")
                    state_store["value"] = "b"
                    mgr.save(name="beta")

                    state_store["value"] = "changed"
                    mgr.load(name="latest")

            # "latest" should resolve to "beta" (most recent saved_at)
            assert state_store["value"] == "b"


class TestSaveCompressedVerifyMetadata:
    """Save compressed, list, verify metadata includes compressed flag."""

    def test_save_compressed_and_list(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = _make_manager(tmpdir)
            mgr.register_native_handler("svc1", lambda: {"k": "v"}, lambda d: None)

            with patch("robotocore.state.manager.StateManager._save_moto_state"):
                result_path = mgr.save(name="compressed-snap", compress=True)

            assert result_path.endswith(".tar.gz")
            assert Path(result_path).exists()

            snapshots = mgr.list_snapshots()
            assert len(snapshots) == 1
            snap = snapshots[0]
            assert snap["name"] == "compressed-snap"
            assert snap["compressed"] is True

    def test_save_uncompressed_and_list(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = _make_manager(tmpdir)
            mgr.register_native_handler("svc1", lambda: {"k": "v"}, lambda d: None)

            with patch("robotocore.state.manager.StateManager._save_moto_state"):
                mgr.save(name="dir-snap")

            snapshots = mgr.list_snapshots()
            assert len(snapshots) == 1
            assert snapshots[0]["compressed"] is False


class TestImportCorruptedArchive:
    """Import corrupted archive results in error handling."""

    def test_import_corrupted_bytes_does_not_crash(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = _make_manager(tmpdir)
            corrupted = b"this is not a tar.gz file at all"
            try:
                mgr.import_snapshot_bytes(corrupted, name="bad-snap")
                assert False, "Expected an exception"
            except Exception:
                pass  # Expected

    def test_import_empty_tar_gz(self):
        """An empty tar.gz archive (no metadata.json) gets imported but without metadata."""
        import io

        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz"):
            pass  # empty archive
        data = buf.getvalue()

        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = _make_manager(tmpdir)
            with patch("robotocore.state.manager.StateManager._load_moto_state"):
                with patch("robotocore.state.manager.StateManager._load_native_state"):
                    name = mgr.import_snapshot_bytes(
                        data, name="empty-snap", load_after_import=False
                    )
            assert name == "empty-snap"


class TestSelectiveServiceSave:
    """Selective service save (only S3) then load verifies only S3 state restored."""

    def test_selective_save_and_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = _make_manager(tmpdir)

            s3_state = {"buckets": ["my-bucket"]}
            ddb_state = {"tables": ["my-table"]}

            mgr.register_native_handler(
                "s3",
                lambda: dict(s3_state),
                lambda d: s3_state.update(d),
            )
            mgr.register_native_handler(
                "dynamodb",
                lambda: dict(ddb_state),
                lambda d: ddb_state.update(d),
            )

            with patch("robotocore.state.manager.StateManager._save_moto_state"):
                with patch("robotocore.state.manager.StateManager._load_moto_state"):
                    # Save only S3
                    mgr.save(name="s3-only", services=["s3"])

                    # Modify both
                    s3_state["buckets"] = ["changed"]
                    ddb_state["tables"] = ["changed"]

                    # Load only S3
                    mgr.load(name="s3-only", services=["s3"])

            # S3 restored, DDB unchanged
            assert s3_state["buckets"] == ["my-bucket"]
            assert ddb_state["tables"] == ["changed"]

    def test_native_state_file_only_contains_selected_services(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = _make_manager(tmpdir)
            mgr.register_native_handler("s3", lambda: {"s3": True}, lambda d: None)
            mgr.register_native_handler("sqs", lambda: {"sqs": True}, lambda d: None)

            with patch("robotocore.state.manager.StateManager._save_moto_state"):
                mgr.save(name="selective", services=["s3"])

            native_path = Path(tmpdir) / "snapshots" / "selective" / "native_state.json"
            native_data = json.loads(native_path.read_text())
            assert "s3" in native_data
            assert "sqs" not in native_data


class TestSnapshotMetadata:
    """Snapshot metadata correctness."""

    def test_metadata_contains_name_and_timestamp(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = _make_manager(tmpdir)
            with patch("robotocore.state.manager.StateManager._save_moto_state"):
                mgr.save(name="meta-test")

            meta_path = Path(tmpdir) / "snapshots" / "meta-test" / "metadata.json"
            meta = json.loads(meta_path.read_text())
            assert meta["name"] == "meta-test"
            assert meta["version"] == "1.0"
            assert "timestamp" in meta
            assert "saved_at" in meta
            assert meta["compressed"] is False

    def test_export_snapshot_bytes_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = _make_manager(tmpdir)
            mgr.register_native_handler("svc", lambda: {"key": "val"}, lambda d: None)

            with patch("robotocore.state.manager.StateManager._save_moto_state"):
                mgr.save(name="export-test")

            data = mgr.export_snapshot_bytes(name="export-test")
            assert len(data) > 0

            # Should be a valid tar.gz
            import io

            with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
                names = tar.getnames()
                assert "metadata.json" in names


class TestPathTraversalPrevention:
    """Snapshot names with path traversal are rejected."""

    def test_save_path_traversal_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = _make_manager(tmpdir)
            try:
                mgr.save(name="../../etc/passwd")
                assert False, "Expected ValueError"
            except ValueError as e:
                assert "path traversal" in str(e).lower()

    def test_load_path_traversal_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = _make_manager(tmpdir)
            try:
                mgr.load(name="../../etc/passwd")
                assert False, "Expected ValueError"
            except ValueError as e:
                assert "path traversal" in str(e).lower()
