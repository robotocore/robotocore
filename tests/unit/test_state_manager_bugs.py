"""Failing tests for correctness bugs in state/manager.py.

Each test targets a specific bug. All tests are expected to FAIL against the
current implementation.
"""

from datetime import UTC, datetime
from pathlib import Path

from robotocore.state.manager import StateManager


class TestSelectiveLoadIgnoresServicesParam:
    """Bug: load(services=[...]) accepts a services list but never filters by it.

    The `services` param is accepted by load() but is never forwarded to
    _load_moto_state() or _load_native_state().  A selective load therefore
    restores ALL services instead of only the requested subset.
    """

    def test_selective_load_only_loads_requested_services(self, tmp_path):
        """Saving two services, then loading only one should NOT restore the other."""
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

        # Clear the dicts so we can detect what load() restores
        svc1_loaded.clear()
        svc2_loaded.clear()

        mgr2 = StateManager(state_dir=str(tmp_path))
        mgr2.register_native_handler("svc1", lambda: {}, lambda d: svc1_loaded.update(d))
        mgr2.register_native_handler("svc2", lambda: {}, lambda d: svc2_loaded.update(d))
        mgr2.load(services=["svc1"])

        # svc1 should be loaded
        assert svc1_loaded == {"data": "one"}
        # svc2 should NOT have been loaded -- but due to the bug, it IS loaded
        assert svc2_loaded == {}, f"svc2 was loaded even though services=['svc1']: {svc2_loaded}"


class TestPathTraversalInSnapshotNames:
    """Bug: snapshot names are used directly in path construction without
    sanitization.  Names containing '..' can write/read outside the
    snapshots directory.
    """

    def test_path_traversal_save_writes_outside_snapshots_dir(self, tmp_path):
        """A snapshot name with '..' should be rejected, not silently write
        files outside the snapshots directory."""
        mgr = StateManager(state_dir=str(tmp_path))

        # This name resolves: tmp_path/snapshots/../../escape = tmp_path/../escape
        # which is OUTSIDE the state_dir entirely.
        malicious_name = "../escape"

        # A correct implementation should raise ValueError (or similar).
        # The current code silently creates directories outside state_dir.
        try:
            result_path = mgr.save(name=malicious_name)
            resolved = Path(result_path).resolve()
            snapshots_dir = (tmp_path / "snapshots").resolve()
            # The saved path must be INSIDE the snapshots directory
            assert str(resolved).startswith(str(snapshots_dir)), (
                f"Path traversal: snapshot saved to {resolved} which is outside {snapshots_dir}"
            )
        except (ValueError, OSError):
            pass  # Raising an error is also acceptable

    def test_path_traversal_load_reads_outside_snapshots_dir(self, tmp_path):
        """A snapshot name with '..' should not load state from outside
        the snapshots directory."""
        mgr = StateManager(state_dir=str(tmp_path))
        # Save legitimate state at the state_dir root
        mgr.save()
        # Also create a named snapshot so the snapshots/ dir exists
        mgr.save(name="legit")

        # Now try to load with a traversal name: snapshots/../.. resolves
        # to tmp_path's parent. But snapshots/.. resolves to tmp_path itself,
        # which has metadata.json from the save() above.
        mgr2 = StateManager(state_dir=str(tmp_path))
        try:
            result = mgr2.load(name="..")
            # If it doesn't raise, it should return False (reject the name)
            assert result is False, (
                "Path traversal in load name should be rejected, "
                "but it loaded state from outside snapshots/"
            )
        except (ValueError, OSError):
            pass  # Raising an error is also acceptable


class TestJsonSerializationDataLoss:
    """Bug: _save_native_state uses json.dumps(default=str) which silently
    converts bytes, datetimes, sets, and other non-JSON types to strings.
    On load, the original types are lost.
    """

    def test_bytes_roundtrip_loses_type(self, tmp_path):
        """Bytes values become strings after save/load roundtrip."""
        loaded = {}
        original = {"binary_data": b"\x00\x01\x02\xff"}

        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc", lambda: original, lambda d: loaded.update(d))
        mgr.save()

        loaded.clear()
        mgr2 = StateManager(state_dir=str(tmp_path))
        mgr2.register_native_handler("svc", lambda: {}, lambda d: loaded.update(d))
        mgr2.load()

        # The loaded value should be bytes, same as the original.
        # Due to default=str, it becomes a string like "b'\\x00\\x01\\x02\\xff'"
        assert isinstance(loaded["binary_data"], bytes), (
            f"Expected bytes, got {type(loaded['binary_data'])}: {loaded['binary_data']!r}"
        )

    def test_datetime_roundtrip_loses_type(self, tmp_path):
        """Datetime values become strings after save/load roundtrip."""
        loaded = {}
        now = datetime(2025, 6, 15, 12, 0, 0, tzinfo=UTC)
        original = {"created_at": now}

        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc", lambda: original, lambda d: loaded.update(d))
        mgr.save()

        loaded.clear()
        mgr2 = StateManager(state_dir=str(tmp_path))
        mgr2.register_native_handler("svc", lambda: {}, lambda d: loaded.update(d))
        mgr2.load()

        assert isinstance(loaded["created_at"], datetime), (
            f"Expected datetime, got {type(loaded['created_at'])}: {loaded['created_at']!r}"
        )

    def test_set_roundtrip_loses_type(self, tmp_path):
        """Set values become lists (via str()) after save/load roundtrip."""
        loaded = {}
        original = {"tags": {1, 2, 3}}

        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc", lambda: original, lambda d: loaded.update(d))
        mgr.save()

        loaded.clear()
        mgr2 = StateManager(state_dir=str(tmp_path))
        mgr2.register_native_handler("svc", lambda: {}, lambda d: loaded.update(d))
        mgr2.load()

        # With default=str, sets become their str() repr, e.g. "{1, 2, 3}"
        # which is neither a set nor a list
        assert isinstance(loaded["tags"], set), (
            f"Expected set, got {type(loaded['tags'])}: {loaded['tags']!r}"
        )


class TestNamedSnapshotListMissingNativeServices:
    """Bug: list_snapshots() reports moto_services from metadata but omits
    native_services.  Users cannot see which native providers were saved in
    a snapshot.
    """

    def test_list_snapshots_includes_native_services(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("my_native_svc", lambda: {"x": 1}, lambda d: None)
        mgr.save(name="snap1")

        snaps = mgr.list_snapshots()
        assert len(snaps) == 1
        snap = snaps[0]
        # The snapshot listing should include native services
        assert "native_services" in snap, f"Snapshot listing missing 'native_services' key: {snap}"
        assert "my_native_svc" in snap["native_services"]


class TestLoadNamedSnapshotDoesNotFilterServices:
    """Bug: load(name='x', services=['svc1']) ignores the services filter.

    Even when loading a named snapshot with a services filter, ALL services
    in the snapshot are restored.
    """

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
        # Load only svc1 from the snapshot
        mgr2.load(name="full_snap", services=["svc1"])

        assert svc1_loaded == {"val": "first"}
        assert svc2_loaded == {}, f"svc2 should not have been loaded but got: {svc2_loaded}"
