"""Tests for state manager bug fixes.

Validates fixes for real bugs found during code audit:
- Path traversal in snapshot names allowed writing outside snapshots dir
- load(services=[...]) ignored the services filter
- list_snapshots() omitted native_services from output
"""

import pytest

from robotocore.state.manager import StateManager


class TestPathTraversalInSnapshotNames:
    """Fixed: snapshot names with '..' are now rejected with ValueError."""

    def test_path_traversal_save_rejected(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        with pytest.raises(ValueError, match="path traversal"):
            mgr.save(name="../escape")

    def test_path_traversal_load_rejected(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        with pytest.raises(ValueError, match="path traversal"):
            mgr.load(name="..")

    def test_normal_snapshot_name_works(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc1", lambda: {"x": 1}, lambda d: None)
        result = mgr.save(name="my-snapshot")
        assert "my-snapshot" in result


class TestSelectiveLoadFiltersServices:
    """Fixed: load(services=[...]) now only loads the requested services."""

    def test_selective_load_only_loads_requested_services(self, tmp_path):
        svc1_loaded = {}
        svc2_loaded = {}

        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler(
            "svc1", lambda: {"data": "one"}, lambda d: svc1_loaded.update(d)
        )
        mgr.register_native_handler(
            "svc2", lambda: {"data": "two"}, lambda d: svc2_loaded.update(d)
        )
        mgr.save()

        svc1_loaded.clear()
        svc2_loaded.clear()

        mgr2 = StateManager(state_dir=str(tmp_path))
        mgr2.register_native_handler("svc1", lambda: {}, lambda d: svc1_loaded.update(d))
        mgr2.register_native_handler("svc2", lambda: {}, lambda d: svc2_loaded.update(d))
        mgr2.load(services=["svc1"])

        assert svc1_loaded == {"data": "one"}
        assert svc2_loaded == {}

    def test_load_named_snapshot_with_services_filter(self, tmp_path):
        svc1_loaded = {}
        svc2_loaded = {}

        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler(
            "svc1", lambda: {"val": "first"}, lambda d: svc1_loaded.update(d)
        )
        mgr.register_native_handler(
            "svc2", lambda: {"val": "second"}, lambda d: svc2_loaded.update(d)
        )
        mgr.save(name="full_snap")

        svc1_loaded.clear()
        svc2_loaded.clear()

        mgr2 = StateManager(state_dir=str(tmp_path))
        mgr2.register_native_handler("svc1", lambda: {}, lambda d: svc1_loaded.update(d))
        mgr2.register_native_handler("svc2", lambda: {}, lambda d: svc2_loaded.update(d))
        mgr2.load(name="full_snap", services=["svc1"])

        assert svc1_loaded == {"val": "first"}
        assert svc2_loaded == {}


class TestListSnapshotsIncludesNativeServices:
    """Fixed: list_snapshots() now includes native_services in output."""

    def test_list_snapshots_includes_native_services(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("my_native_svc", lambda: {"x": 1}, lambda d: None)
        mgr.save(name="snap1")

        snaps = mgr.list_snapshots()
        assert len(snaps) == 1
        snap = snaps[0]
        assert "native_services" in snap
        assert "my_native_svc" in snap["native_services"]
