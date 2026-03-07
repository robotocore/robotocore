"""Unit tests for state persistence snapshot features."""

import json
import tempfile
from pathlib import Path

from robotocore.state.manager import StateManager


class TestNamedSnapshots:
    def test_save_named_snapshot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            path = manager.save(name="test-snap")
            assert "snapshots/test-snap" in path
            assert Path(path, "metadata.json").exists()

    def test_list_snapshots_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            assert manager.list_snapshots() == []

    def test_list_snapshots_after_save(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            manager.save(name="snap-a")
            manager.save(name="snap-b")
            snapshots = manager.list_snapshots()
            names = [s["name"] for s in snapshots]
            assert "snap-a" in names
            assert "snap-b" in names

    def test_load_named_snapshot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            manager.save(name="restore-test")
            success = manager.load(name="restore-test")
            assert success

    def test_load_nonexistent_snapshot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            success = manager.load(name="does-not-exist")
            assert not success

    def test_snapshot_metadata_includes_name(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            path = manager.save(name="meta-test")
            meta = json.loads(Path(path, "metadata.json").read_text())
            assert meta["name"] == "meta-test"
            assert "timestamp" in meta


class TestSelectiveSave:
    def test_save_with_services_filter(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            # Should not raise even with services filter
            path = manager.save(services=["s3", "dynamodb"])
            meta = json.loads(Path(path, "metadata.json").read_text())
            # moto_services in metadata should only contain filtered services
            # (if they exist in registry)
            for svc in meta.get("moto_services", []):
                assert svc in ("s3", "dynamodb")
