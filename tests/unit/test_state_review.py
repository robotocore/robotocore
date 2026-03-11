"""Failing tests for state management edge cases.

Each test documents correct behavior that is currently missing or broken.
These tests should all FAIL until the corresponding fixes are applied.
"""

import io
import json
import pickle
import tarfile

import pytest

from robotocore.state.hooks import HookType, StateHookRegistry
from robotocore.state.manager import StateManager


class TestSaveLoadRoundTripFidelity:
    """Save/load should preserve all data exactly."""

    def test_native_state_with_nested_types_preserved(self, tmp_path):
        """Correct behavior: nested data types (tuples, sets, bytes) should survive
        save/load round-trip. Currently, JSON serialization with default=str silently
        corrupts non-JSON-native types (tuples become lists, sets become strings, bytes
        become strings).
        """
        original = {
            "tuple_val": (1, 2, 3),
            "set_val": {4, 5, 6},
            "bytes_val": b"hello",
        }
        loaded = {}

        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc", lambda: original, lambda d: loaded.update(d))
        mgr.save()
        loaded.clear()
        mgr.load()

        # Tuples should survive (they get converted to lists by JSON)
        assert isinstance(loaded["tuple_val"], tuple), (
            "Tuples are silently converted to lists during JSON round-trip"
        )
        # Sets should survive (they get converted to strings by default=str)
        assert isinstance(loaded["set_val"], set), (
            "Sets are silently corrupted by default=str serializer"
        )
        # Bytes should survive
        assert isinstance(loaded["bytes_val"], bytes), (
            "Bytes are silently converted to strings by default=str serializer"
        )

    def test_native_state_integer_keys_preserved(self, tmp_path):
        """Correct behavior: integer dict keys should survive round-trip.
        JSON converts all keys to strings, losing the int type.
        """
        original = {1: "one", 2: "two"}
        loaded = {}

        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc", lambda: original, lambda d: loaded.update(d))
        mgr.save()
        loaded.clear()
        mgr.load()

        # Integer keys become string keys in JSON
        assert 1 in loaded, "Integer dict keys are converted to strings by JSON serialization"


class TestSelectivePersistenceEdgeCases:
    """Save/load with service filters should be precise."""

    def test_selective_save_does_not_overwrite_other_services(self, tmp_path):
        """Correct behavior: saving services=["svc2"] over an existing snapshot
        that already has svc1 state should NOT destroy svc1's saved state.
        Currently, native_state.json is written from scratch, overwriting the
        previous file entirely.
        """
        loaded_svc1 = {}
        loaded_svc2 = {}

        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler(
            "svc1", lambda: {"val": "original"}, lambda d: loaded_svc1.update(d)
        )
        mgr.register_native_handler(
            "svc2", lambda: {"val": "updated"}, lambda d: loaded_svc2.update(d)
        )

        # First: save everything
        mgr.save()
        # Second: save only svc2 (should not destroy svc1's data)
        mgr.save(services=["svc2"])

        # Now load everything -- svc1 should still be there from first save
        loaded_svc1.clear()
        loaded_svc2.clear()
        mgr.load()

        assert loaded_svc1 == {"val": "original"}, (
            "Selective save overwrites the entire native_state.json, destroying "
            "previously saved service state"
        )

    def test_reset_with_services_filter_only_resets_specified(self, tmp_path):
        """Correct behavior: reset(services=["svc1"]) should only reset svc1,
        leaving svc2 untouched. Currently reset() ignores the services parameter
        and resets everything.
        """
        svc1_state = {"data": "svc1_original"}
        svc2_state = {"data": "svc2_original"}

        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler(
            "svc1",
            lambda: dict(svc1_state),
            lambda d: svc1_state.update(d) if d else svc1_state.clear(),
        )
        mgr.register_native_handler(
            "svc2",
            lambda: dict(svc2_state),
            lambda d: svc2_state.update(d) if d else svc2_state.clear(),
        )

        mgr.reset(services=["svc1"])

        # svc1 should be reset (empty)
        assert svc1_state == {} or svc1_state.get("data") is None, "svc1 should be reset"
        # svc2 should be untouched
        assert svc2_state == {"data": "svc2_original"}, (
            "reset(services=['svc1']) also resets svc2 because services param is ignored"
        )


class TestConcurrentSaveLoad:
    """Thread safety of save/load operations."""

    def test_save_is_atomic_or_uses_lock(self, tmp_path):
        """Correct behavior: save() should be protected by the lock, not just
        save_debounced(). Currently only save_debounced() acquires _save_lock.
        """
        mgr = StateManager(state_dir=str(tmp_path))
        assert hasattr(mgr, "_save_lock"), "Manager has a save lock"

        # Verify that save() acquires the lock (it currently doesn't)
        lock_acquired_during_save = []

        original_save_moto = mgr._save_moto_state

        def instrumented_save_moto(*args, **kwargs):
            lock_acquired_during_save.append(mgr._save_lock.locked())
            return original_save_moto(*args, **kwargs)

        mgr._save_moto_state = instrumented_save_moto
        mgr.save()

        assert any(lock_acquired_during_save), (
            "save() does not acquire _save_lock, making concurrent saves unsafe"
        )


class TestSnapshotNamingConflicts:
    """Overwriting named snapshots should be safe and predictable."""

    def test_save_snapshot_with_empty_name_raises(self, tmp_path):
        """Correct behavior: empty string snapshot name should be rejected,
        not create a snapshot at state_dir/snapshots//.
        """
        mgr = StateManager(state_dir=str(tmp_path))
        with pytest.raises(ValueError):
            mgr.save(name="")

    def test_save_snapshot_with_slash_in_name_rejected(self, tmp_path):
        """Correct behavior: snapshot names with slashes should be rejected
        to prevent creating nested directories under snapshots/.
        """
        mgr = StateManager(state_dir=str(tmp_path))
        with pytest.raises(ValueError):
            mgr.save(name="parent/child")


class TestCorruptedSnapshotHandling:
    """Corrupted data should produce clean errors, not crashes."""

    def test_load_corrupted_pickle_returns_false(self, tmp_path):
        """Correct behavior: if moto_state.pkl is corrupted, load() should
        return False or raise a clear error, not silently succeed with no state.
        Currently it catches the exception and returns True anyway.
        """
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.save()

        # Corrupt the pickle file
        (tmp_path / "moto_state.pkl").write_bytes(b"corrupted data")

        mgr2 = StateManager(state_dir=str(tmp_path))
        result = mgr2.load()
        # Currently this returns True even though Moto state failed to load
        assert result is False, (
            "load() returns True even when moto_state.pkl is corrupted, "
            "silently losing all Moto state"
        )

    def test_load_corrupted_native_json_returns_false(self, tmp_path):
        """Correct behavior: if native_state.json is corrupted/invalid JSON,
        load() should return False, not crash with JSONDecodeError.
        """
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.save()

        # Corrupt the native state JSON
        (tmp_path / "native_state.json").write_text("not json {{{")

        mgr2 = StateManager(state_dir=str(tmp_path))
        result = mgr2.load()
        # Should handle gracefully instead of crashing
        assert result is False, (
            "load() crashes with JSONDecodeError when native_state.json is corrupted"
        )

    def test_load_pickle_with_dangerous_class_is_safe(self, tmp_path):
        """Correct behavior: loading a pickle that contains malicious objects
        should be rejected. Currently pickle.load is used with no restrictions,
        which is a security risk (arbitrary code execution via __reduce__).
        """
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.save()

        # Create a malicious pickle that would execute code
        class MaliciousReducer:
            def __reduce__(self):
                return (eval, ("__import__('os').getpid()",))

        malicious_state = {"s3": {"default": {"us-east-1": MaliciousReducer()}}}
        (tmp_path / "moto_state.pkl").write_bytes(
            pickle.dumps(malicious_state, protocol=pickle.HIGHEST_PROTOCOL)
        )

        # Should either use restricted unpickler or reject untrusted pickles
        mgr2 = StateManager(state_dir=str(tmp_path))
        with pytest.raises(Exception):
            mgr2.load()

    def test_import_tar_gz_with_path_traversal_in_filenames(self, tmp_path):
        """Correct behavior: tar.gz archives containing files with path
        traversal (e.g., '../../etc/passwd') should be rejected. Currently
        extractall() is used with noqa: S202, which disables the Bandit
        warning but does not fix the vulnerability.
        """
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            info = tarfile.TarInfo(name="../../etc/evil")
            info.size = 4
            tar.addfile(info, io.BytesIO(b"evil"))
            meta = json.dumps({"name": "evil", "version": "1.0"}).encode()
            meta_info = tarfile.TarInfo(name="metadata.json")
            meta_info.size = len(meta)
            tar.addfile(meta_info, io.BytesIO(meta))

        mgr = StateManager(state_dir=str(tmp_path))
        with pytest.raises(Exception):
            mgr.import_snapshot_bytes(buf.getvalue(), name="evil-snap", load_after_import=False)


class TestHooksFiringOrder:
    """Hooks should fire in the correct order and with correct context."""

    def test_load_hooks_fire_for_compressed_snapshot(self, tmp_path):
        """Correct behavior: hooks should fire even when loading compressed
        snapshots. Currently _load_compressed does NOT fire hooks.
        """
        registry = StateHookRegistry()
        hook_calls = []
        registry.register(HookType.BEFORE_LOAD, lambda ctx: hook_calls.append("before"))
        registry.register(HookType.AFTER_LOAD, lambda ctx: hook_calls.append("after"))

        mgr = StateManager(state_dir=str(tmp_path), hook_registry=registry)
        mgr.save(name="compressed-hook", compress=True)
        hook_calls.clear()

        mgr.load(name="compressed-hook")

        assert "before" in hook_calls, (
            "_load_compressed() bypasses hooks -- before_load never fires"
        )
        assert "after" in hook_calls, "_load_compressed() bypasses hooks -- after_load never fires"


class TestListSnapshotsEdgeCases:
    """Edge cases in listing snapshots."""

    def test_list_snapshots_with_corrupted_metadata(self, tmp_path):
        """Correct behavior: if a snapshot directory has a corrupted
        metadata.json, list_snapshots() should still list it (with minimal
        info) rather than crash.
        """
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.save(name="good-snap")

        # Create a snapshot directory with corrupted metadata
        bad_snap = tmp_path / "snapshots" / "bad-snap"
        bad_snap.mkdir(parents=True)
        (bad_snap / "metadata.json").write_text("not json {{{")

        try:
            snaps = mgr.list_snapshots()
        except json.JSONDecodeError:
            pytest.fail(
                "list_snapshots() crashes with JSONDecodeError when a "
                "snapshot has corrupted metadata.json"
            )

        names = [s["name"] for s in snaps]
        assert "good-snap" in names
        assert "bad-snap" in names


class TestCompressedSnapshotEdgeCases:
    """Edge cases specific to compressed snapshots."""

    def test_save_compressed_then_overwrite_uncompressed(self, tmp_path):
        """Correct behavior: saving 'snap' as compressed, then saving 'snap'
        as uncompressed should clean up the old .tar.gz to avoid having both
        formats coexist for the same name.
        """
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc", lambda: {"v": 1}, lambda d: None)

        # Save compressed first
        mgr.save(name="mixed", compress=True)
        archive = tmp_path / "snapshots" / "mixed.tar.gz"
        assert archive.exists()

        # Now save uncompressed with same name
        mgr.register_native_handler("svc", lambda: {"v": 2}, lambda d: None)
        mgr.save(name="mixed", compress=False)
        snap_dir = tmp_path / "snapshots" / "mixed"
        assert snap_dir.is_dir()

        # Old .tar.gz should be cleaned up
        assert not archive.exists(), (
            "Old .tar.gz archive is not cleaned up when saving uncompressed "
            "with same name, causing list_snapshots() to show stale data"
        )


class TestMetadataVersioning:
    """Metadata format versioning."""

    def test_load_snapshot_with_unknown_version(self, tmp_path):
        """Correct behavior: loading a snapshot with version '99.0' (a future
        format) should either fail clearly or handle gracefully, not silently
        load potentially incompatible data. Currently version is written but
        never checked on load.
        """
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.save()

        # Change metadata version to a future version
        meta_path = tmp_path / "metadata.json"
        meta = json.loads(meta_path.read_text())
        meta["version"] = "99.0"
        meta_path.write_text(json.dumps(meta))

        mgr2 = StateManager(state_dir=str(tmp_path))
        with pytest.raises(Exception):
            mgr2.load()
