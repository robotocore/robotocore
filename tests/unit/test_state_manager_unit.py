"""Unit tests for the state manager."""

import json

from robotocore.state.manager import StateManager


class TestStateManagerInit:
    def test_no_state_dir(self):
        mgr = StateManager()
        assert mgr.state_dir is None

    def test_with_state_dir(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        assert mgr.state_dir == tmp_path


class TestNativeHandlers:
    def test_register_and_export(self):
        mgr = StateManager()
        state = {"key": "value"}
        mgr.register_native_handler("test-svc", lambda: state, lambda d: None)
        export = mgr.export_json()
        assert export["native_state"]["test-svc"] == {"key": "value"}

    def test_import_json_calls_load(self):
        loaded = {}
        mgr = StateManager()
        mgr.register_native_handler("test-svc", lambda: {}, lambda d: loaded.update(d))
        mgr.import_json({"native_state": {"test-svc": {"imported": True}}})
        assert loaded["imported"] is True

    def test_import_ignores_unregistered(self):
        mgr = StateManager()
        # Should not raise
        mgr.import_json({"native_state": {"unknown": {"data": 1}}})

    def test_export_handles_error(self):
        def _raise():
            raise RuntimeError("boom")

        mgr = StateManager()
        mgr.register_native_handler("bad", _raise, lambda d: None)
        export = mgr.export_json()
        # Should not crash; bad service just skipped
        assert "bad" not in export["native_state"]


class TestSaveLoad:
    def test_save_creates_files(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("test-svc", lambda: {"k": "v"}, lambda d: None)
        mgr.save()
        assert "metadata.json" in [f.name for f in tmp_path.iterdir()]
        assert "native_state.json" in [f.name for f in tmp_path.iterdir()]
        assert "moto_state.pkl" in [f.name for f in tmp_path.iterdir()]

    def test_save_metadata_contents(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.save()
        meta = json.loads((tmp_path / "metadata.json").read_text())
        assert meta["version"] == "1.0"
        assert "timestamp" in meta
        assert "saved_at" in meta

    def test_save_native_state_contents(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc1", lambda: {"x": 1}, lambda d: None)
        mgr.save()
        state = json.loads((tmp_path / "native_state.json").read_text())
        assert state["svc1"]["x"] == 1

    def test_named_snapshot(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        result = mgr.save(name="snap1")
        assert "snapshots/snap1" in result
        snap_dir = tmp_path / "snapshots" / "snap1"
        assert snap_dir.exists()
        assert (snap_dir / "metadata.json").exists()

    def test_list_snapshots(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.save(name="alpha")
        mgr.save(name="beta")
        snaps = mgr.list_snapshots()
        names = [s["name"] for s in snaps]
        assert "alpha" in names
        assert "beta" in names

    def test_list_snapshots_no_dir(self):
        mgr = StateManager()
        assert mgr.list_snapshots() == []

    def test_load_nonexistent(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path / "nope"))
        assert mgr.load() is False

    def test_load_no_metadata(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        # Directory exists but no metadata.json
        assert mgr.load() is False

    def test_save_and_load_native_roundtrip(self, tmp_path):
        loaded_state = {}
        save_data = {"counter": 42}
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("test-svc", lambda: save_data, lambda d: loaded_state.update(d))
        mgr.save()
        mgr.load()
        assert loaded_state["counter"] == 42

    def test_save_no_dir_raises(self):
        mgr = StateManager()
        try:
            mgr.save()
            assert False, "Should have raised"
        except ValueError as e:
            assert "No state directory" in str(e)


class TestSaveDebounced:
    def test_first_save_always_runs(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        assert mgr.save_debounced() is True

    def test_second_save_debounced(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr._debounce_interval = 10.0  # large interval
        mgr.save_debounced()
        assert mgr.save_debounced() is False


class TestSelectiveSave:
    def test_save_specific_services(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc1", lambda: {"a": 1}, lambda d: None)
        mgr.register_native_handler("svc2", lambda: {"b": 2}, lambda d: None)
        mgr.save(services=["svc1"])
        state = json.loads((tmp_path / "native_state.json").read_text())
        assert "svc1" in state
        assert "svc2" not in state


class TestReset:
    def test_reset_calls_native_load_with_empty(self):
        loaded = {}
        mgr = StateManager()
        mgr.register_native_handler("test", lambda: {}, lambda d: loaded.update({"called": True}))
        mgr.reset()
        assert loaded.get("called") is True
