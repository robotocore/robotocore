"""Tests for snapshot export/import feature."""

import tarfile
import tempfile
from io import BytesIO
from pathlib import Path

from robotocore.state.manager import StateManager


class TestExportSnapshotBytes:
    def test_export_current_state(self):
        """Exporting without a name should save and compress current state."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            data = manager.export_snapshot_bytes()
            assert len(data) > 0
            # Should be a valid tar.gz
            with tarfile.open(fileobj=BytesIO(data), mode="r:gz") as tar:
                names = tar.getnames()
                assert "metadata.json" in names

    def test_export_named_snapshot_directory(self):
        """Exporting a named directory snapshot should compress it."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            manager.save(name="export-dir")
            data = manager.export_snapshot_bytes(name="export-dir")
            assert len(data) > 0
            with tarfile.open(fileobj=BytesIO(data), mode="r:gz") as tar:
                names = tar.getnames()
                assert "metadata.json" in names

    def test_export_named_compressed_snapshot(self):
        """Exporting an already-compressed snapshot should return the archive bytes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            path = manager.save(name="export-gz", compress=True)
            data = manager.export_snapshot_bytes(name="export-gz")
            # Should match the file on disk
            assert data == Path(path).read_bytes()

    def test_export_nonexistent_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            try:
                manager.export_snapshot_bytes(name="nope")
                assert False, "Should have raised ValueError"
            except ValueError as e:
                assert "not found" in str(e).lower()


class TestImportSnapshotBytes:
    def test_import_roundtrip(self):
        """Export then import should preserve native state."""
        saved_data = {"key": "value", "count": 42}
        loaded_data = {}

        def save_fn():
            return saved_data

        def load_fn(data):
            loaded_data.update(data)

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            manager.register_native_handler("test-svc", save_fn, load_fn)
            exported = manager.export_snapshot_bytes()

            # Reset and import
            loaded_data.clear()
            name = manager.import_snapshot_bytes(data=exported, name="imported")
            assert name == "imported"
            assert loaded_data == saved_data

    def test_import_with_auto_name(self):
        """Importing without a name should use metadata name or generate one."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            manager.save(name="original")
            exported = manager.export_snapshot_bytes(name="original")

            # Import to a new state dir without specifying name
            with tempfile.TemporaryDirectory() as tmpdir2:
                manager2 = StateManager(state_dir=tmpdir2)
                name = manager2.import_snapshot_bytes(data=exported)
                # Should use the name from metadata
                assert name == "original"

    def test_import_without_load(self):
        """import with load_after_import=False should save but not load."""
        loaded = []

        def save_fn():
            return {"x": 1}

        def load_fn(data):
            loaded.append(data)

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            manager.register_native_handler("svc", save_fn, load_fn)
            exported = manager.export_snapshot_bytes()

            loaded.clear()
            manager.import_snapshot_bytes(data=exported, name="no-load", load_after_import=False)
            # Should not have called load_fn
            assert len(loaded) == 0

            # But the snapshot should exist
            snapshots = manager.list_snapshots()
            names = [s["name"] for s in snapshots]
            assert "no-load" in names

    def test_import_no_state_dir_raises(self):
        manager = StateManager(state_dir=None)
        try:
            manager.import_snapshot_bytes(data=b"fake", name="x")
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "state directory" in str(e).lower()


class TestExportImportCrossInstance:
    def test_transfer_between_managers(self):
        """Simulate sharing a snapshot between two emulator instances."""
        original_data = {"tables": ["users", "orders"]}
        received_data = {}

        def save_fn():
            return original_data

        def load_fn(data):
            received_data.update(data)

        with tempfile.TemporaryDirectory() as tmpdir1:
            manager1 = StateManager(state_dir=tmpdir1)
            manager1.register_native_handler("dynamodb", save_fn, load_fn)
            snapshot_bytes = manager1.export_snapshot_bytes()

        # "Transfer" to a completely separate instance
        with tempfile.TemporaryDirectory() as tmpdir2:
            manager2 = StateManager(state_dir=tmpdir2)
            manager2.register_native_handler("dynamodb", save_fn, load_fn)
            name = manager2.import_snapshot_bytes(data=snapshot_bytes, name="shared")
            assert name == "shared"
            assert received_data == original_data

    def test_archive_contains_expected_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            data = manager.export_snapshot_bytes()
            with tarfile.open(fileobj=BytesIO(data), mode="r:gz") as tar:
                names = set(tar.getnames())
                assert "metadata.json" in names
                assert "moto_state.pkl" in names
                assert "native_state.json" in names
