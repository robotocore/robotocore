"""Tests for state snapshot compression feature."""

import json
import tempfile
from pathlib import Path

from robotocore.state.manager import StateManager


class TestCompressedSave:
    def test_save_compressed_creates_tar_gz(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            path = manager.save(name="comp-test", compress=True)
            assert path.endswith(".tar.gz")
            assert Path(path).exists()
            # The uncompressed directory should be removed
            assert not (Path(tmpdir) / "snapshots" / "comp-test").is_dir()

    def test_save_uncompressed_creates_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            path = manager.save(name="uncomp-test", compress=False)
            assert Path(path).is_dir()
            assert (Path(path) / "metadata.json").exists()

    def test_compressed_metadata_has_flag(self):
        """Metadata inside the archive should have compressed=True."""
        import tarfile

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            path = manager.save(name="meta-flag", compress=True)
            with tarfile.open(path, "r:gz") as tar:
                f = tar.extractfile("metadata.json")
                assert f is not None
                meta = json.loads(f.read())
                assert meta["compressed"] is True
                assert meta["name"] == "meta-flag"


class TestCompressedLoad:
    def test_load_compressed_snapshot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            manager.save(name="load-comp", compress=True)
            # Now load it
            success = manager.load(name="load-comp")
            assert success

    def test_load_uncompressed_snapshot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            manager.save(name="load-uncomp", compress=False)
            success = manager.load(name="load-uncomp")
            assert success

    def test_load_nonexistent_compressed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            success = manager.load(name="nope")
            assert not success


class TestCompressedDataIntegrity:
    def test_native_state_roundtrip_compressed(self):
        """Save native state compressed, load it, verify data survived."""
        saved_data = {"items": [1, 2, 3], "config": {"key": "value"}}
        loaded_data = {}

        def save_fn():
            return saved_data

        def load_fn(data):
            loaded_data.update(data)

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            manager.register_native_handler("test-svc", save_fn, load_fn)
            manager.save(name="integrity", compress=True)

            # Reset loaded_data and load
            loaded_data.clear()
            manager.load(name="integrity")
            assert loaded_data == saved_data

    def test_native_state_roundtrip_uncompressed(self):
        """Sanity check: same test uncompressed."""
        saved_data = {"items": [4, 5, 6]}
        loaded_data = {}

        def save_fn():
            return saved_data

        def load_fn(data):
            loaded_data.update(data)

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            manager.register_native_handler("test-svc", save_fn, load_fn)
            manager.save(name="integrity-uncomp")
            loaded_data.clear()
            manager.load(name="integrity-uncomp")
            assert loaded_data == saved_data


class TestListSnapshotsMixed:
    def test_list_shows_both_compressed_and_uncompressed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            manager.save(name="snap-dir", compress=False)
            manager.save(name="snap-gz", compress=True)
            snapshots = manager.list_snapshots()
            names = {s["name"] for s in snapshots}
            assert "snap-dir" in names
            assert "snap-gz" in names

    def test_list_compressed_has_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            manager.save(name="meta-comp", compress=True)
            snapshots = manager.list_snapshots()
            snap = next(s for s in snapshots if s["name"] == "meta-comp")
            assert snap["compressed"] is True
            assert "timestamp" in snap
            assert "saved_at" in snap
