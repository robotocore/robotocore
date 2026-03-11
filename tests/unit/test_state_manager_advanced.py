"""Advanced tests for state manager: snapshots, compression, export/import bytes,
path traversal protection, restore_on_startup, and find_latest_snapshot."""

import os
import tarfile
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from robotocore.state.manager import StateManager


class TestCompressedSave:
    def test_save_compressed_creates_archive(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc", lambda: {"k": "v"}, lambda d: None)
        result = mgr.save(name="compressed-snap", compress=True)
        assert result.endswith(".tar.gz")
        assert Path(result).exists()
        # The uncompressed directory should have been cleaned up
        assert not (tmp_path / "snapshots" / "compressed-snap").exists()

    def test_compressed_archive_contains_files(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc", lambda: {"data": 1}, lambda d: None)
        result = mgr.save(name="check-contents", compress=True)
        with tarfile.open(result, "r:gz") as tar:
            names = tar.getnames()
        assert "metadata.json" in names
        assert "native_state.json" in names
        assert "moto_state.pkl" in names

    def test_load_compressed_snapshot(self, tmp_path):
        loaded = {}
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc", lambda: {"val": 42}, lambda d: loaded.update(d))
        mgr.save(name="load-compressed", compress=True)
        # Clear and reload
        loaded.clear()
        success = mgr.load(name="load-compressed")
        assert success is True
        assert loaded.get("val") == 42

    def test_list_snapshots_includes_compressed(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.save(name="dir-snap")
        mgr.save(name="gz-snap", compress=True)
        snaps = mgr.list_snapshots()
        names = {s["name"] for s in snaps}
        assert "dir-snap" in names
        assert "gz-snap" in names
        compressed_flags = {s["name"]: s["compressed"] for s in snaps}
        assert compressed_flags["dir-snap"] is False
        assert compressed_flags["gz-snap"] is True


class TestExportImportSnapshotBytes:
    def test_export_current_state_as_bytes(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc", lambda: {"x": 1}, lambda d: None)
        data = mgr.export_snapshot_bytes()
        assert isinstance(data, bytes)
        assert len(data) > 0
        # Should be valid tar.gz
        import io

        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
            assert "metadata.json" in tar.getnames()

    def test_export_named_snapshot_directory(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.save(name="exportable")
        data = mgr.export_snapshot_bytes(name="exportable")
        assert isinstance(data, bytes)
        assert len(data) > 0

    def test_export_named_snapshot_compressed(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.save(name="exportable-gz", compress=True)
        data = mgr.export_snapshot_bytes(name="exportable-gz")
        assert isinstance(data, bytes)
        assert len(data) > 0

    def test_export_nonexistent_snapshot_raises(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        with pytest.raises(ValueError, match="not found"):
            mgr.export_snapshot_bytes(name="nonexistent")

    def test_import_bytes_creates_snapshot(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc", lambda: {"a": 1}, lambda d: None)
        data = mgr.export_snapshot_bytes()

        mgr2 = StateManager(state_dir=str(tmp_path / "import-dest"))
        mgr2.register_native_handler("svc", lambda: {}, lambda d: None)
        name = mgr2.import_snapshot_bytes(data, name="imported", load_after_import=False)
        assert name == "imported"
        snaps = mgr2.list_snapshots()
        assert any(s["name"] == "imported" for s in snaps)

    def test_import_bytes_loads_by_default(self, tmp_path):
        loaded = {}
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc", lambda: {"val": 99}, lambda d: None)
        data = mgr.export_snapshot_bytes()

        mgr2 = StateManager(state_dir=str(tmp_path / "dest"))
        mgr2.register_native_handler("svc", lambda: {}, lambda d: loaded.update(d))
        mgr2.import_snapshot_bytes(data, name="auto-load")
        assert loaded.get("val") == 99

    def test_import_bytes_no_state_dir_raises(self):
        mgr = StateManager()
        with pytest.raises(ValueError, match="No state directory"):
            mgr.import_snapshot_bytes(b"data")

    def test_import_uses_metadata_name(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc", lambda: {}, lambda d: None)
        mgr.save(name="meta-name")
        data = mgr.export_snapshot_bytes(name="meta-name")

        mgr2 = StateManager(state_dir=str(tmp_path / "dest"))
        mgr2.register_native_handler("svc", lambda: {}, lambda d: None)
        name = mgr2.import_snapshot_bytes(data, load_after_import=False)
        assert name == "meta-name"


class TestPathTraversal:
    def test_save_path_traversal_rejected(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        with pytest.raises(ValueError, match="path traversal"):
            mgr.save(name="../../etc/passwd")

    def test_load_path_traversal_rejected(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        with pytest.raises(ValueError, match="path traversal"):
            mgr.load(name="../../etc/passwd")


class TestRestoreOnStartup:
    def test_no_env_var_does_nothing(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("ROBOTOCORE_RESTORE_SNAPSHOT", None)
            result = mgr.restore_on_startup()
        assert result is False

    def test_env_var_triggers_restore(self, tmp_path):
        loaded = {}
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc", lambda: {"k": "v"}, lambda d: loaded.update(d))
        mgr.save(name="auto-snap")

        with patch.dict(os.environ, {"ROBOTOCORE_RESTORE_SNAPSHOT": "auto-snap"}):
            result = mgr.restore_on_startup()
        assert result is True
        assert loaded.get("k") == "v"

    def test_env_var_missing_snapshot(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        with patch.dict(os.environ, {"ROBOTOCORE_RESTORE_SNAPSHOT": "nonexistent"}):
            result = mgr.restore_on_startup()
        assert result is False


class TestFindLatestSnapshot:
    def test_latest_by_timestamp(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.save(name="first")
        time.sleep(0.05)  # Ensure different saved_at
        mgr.save(name="second")
        name = mgr._find_latest_snapshot()
        assert name == "second"

    def test_latest_no_snapshots(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        assert mgr._find_latest_snapshot() is None

    def test_load_latest_resolves(self, tmp_path):
        loaded = {}
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc", lambda: {"v": 1}, lambda d: loaded.update(d))
        mgr.save(name="old")
        time.sleep(0.05)
        mgr.register_native_handler("svc", lambda: {"v": 2}, lambda d: loaded.update(d))
        mgr.save(name="new")

        loaded.clear()
        success = mgr.load(name="latest")
        assert success is True
        assert loaded.get("v") == 2


class TestSelectiveLoad:
    def test_load_specific_services_only(self, tmp_path):
        loaded_svc1 = {}
        loaded_svc2 = {}
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc1", lambda: {"a": 1}, lambda d: loaded_svc1.update(d))
        mgr.register_native_handler("svc2", lambda: {"b": 2}, lambda d: loaded_svc2.update(d))
        mgr.save()

        loaded_svc1.clear()
        loaded_svc2.clear()
        mgr.load(services=["svc1"])
        assert loaded_svc1.get("a") == 1
        assert loaded_svc2 == {}  # svc2 not loaded
