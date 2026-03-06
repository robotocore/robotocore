"""Tests for state persistence manager."""

import json
import os
import tempfile

import pytest


class TestStateManager:
    def test_save_creates_files(self, tmp_path):
        from robotocore.state.manager import StateManager

        mgr = StateManager(state_dir=str(tmp_path))
        mgr.save()

        assert (tmp_path / "metadata.json").exists()
        assert (tmp_path / "moto_state.pkl").exists()
        assert (tmp_path / "native_state.json").exists()

        meta = json.loads((tmp_path / "metadata.json").read_text())
        assert meta["version"] == "1.0"
        assert "timestamp" in meta

    def test_save_and_load_metadata(self, tmp_path):
        from robotocore.state.manager import StateManager

        mgr = StateManager(state_dir=str(tmp_path))
        mgr.save()

        meta = json.loads((tmp_path / "metadata.json").read_text())
        assert meta["version"] == "1.0"
        assert isinstance(meta["moto_services"], list)
        assert isinstance(meta["native_services"], list)

    def test_load_nonexistent_returns_false(self, tmp_path):
        from robotocore.state.manager import StateManager

        mgr = StateManager(state_dir=str(tmp_path / "nonexistent"))
        assert mgr.load() is False

    def test_load_without_metadata_returns_false(self, tmp_path):
        from robotocore.state.manager import StateManager

        mgr = StateManager(state_dir=str(tmp_path))
        # Directory exists but no metadata.json
        assert mgr.load() is False

    def test_save_no_dir_raises(self):
        from robotocore.state.manager import StateManager

        mgr = StateManager()
        with pytest.raises(ValueError, match="No state directory"):
            mgr.save()

    def test_save_with_explicit_path(self, tmp_path):
        from robotocore.state.manager import StateManager

        mgr = StateManager()  # No default dir
        mgr.save(path=tmp_path)
        assert (tmp_path / "metadata.json").exists()

    def test_native_handler_round_trip(self, tmp_path):
        from robotocore.state.manager import StateManager

        saved_data = {"queues": ["q1", "q2"], "count": 42}
        loaded = {}

        def save_fn():
            return saved_data

        def load_fn(data):
            loaded.update(data)

        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("test_service", save_fn, load_fn)
        mgr.save()

        # Load into a new manager
        mgr2 = StateManager(state_dir=str(tmp_path))
        mgr2.register_native_handler("test_service", save_fn, load_fn)
        mgr2.load()

        assert loaded == saved_data

    def test_reset_calls_load_with_empty(self):
        from robotocore.state.manager import StateManager

        reset_called = {}

        def save_fn():
            return {"data": 1}

        def load_fn(data):
            reset_called["data"] = data

        mgr = StateManager()
        mgr.register_native_handler("test", save_fn, load_fn)
        mgr.reset()
        assert reset_called["data"] == {}

    def test_singleton(self):
        from robotocore.state.manager import get_state_manager

        m1 = get_state_manager()
        m2 = get_state_manager()
        assert m1 is m2
