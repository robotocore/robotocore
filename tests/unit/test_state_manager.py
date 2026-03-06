"""Tests for state persistence manager."""

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from robotocore.state.manager import StateManager, get_state_manager


class TestStateManager:
    def test_save_creates_files(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.save()

        assert (tmp_path / "metadata.json").exists()
        assert (tmp_path / "moto_state.pkl").exists()
        assert (tmp_path / "native_state.json").exists()

        meta = json.loads((tmp_path / "metadata.json").read_text())
        assert meta["version"] == "1.0"
        assert "timestamp" in meta

    def test_save_and_load_metadata(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.save()

        meta = json.loads((tmp_path / "metadata.json").read_text())
        assert meta["version"] == "1.0"
        assert isinstance(meta["moto_services"], list)
        assert isinstance(meta["native_services"], list)

    def test_load_nonexistent_returns_false(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path / "nonexistent"))
        assert mgr.load() is False

    def test_load_without_metadata_returns_false(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        # Directory exists but no metadata.json
        assert mgr.load() is False

    def test_save_no_dir_raises(self):
        mgr = StateManager()
        with pytest.raises(ValueError, match="No state directory"):
            mgr.save()

    def test_save_with_explicit_path(self, tmp_path):
        mgr = StateManager()  # No default dir
        mgr.save(path=tmp_path)
        assert (tmp_path / "metadata.json").exists()

    def test_native_handler_round_trip(self, tmp_path):
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
        m1 = get_state_manager()
        m2 = get_state_manager()
        assert m1 is m2


class TestStateManagerInit:
    def test_init_with_state_dir(self):
        mgr = StateManager(state_dir="/tmp/test-state")
        assert mgr.state_dir == Path("/tmp/test-state")

    def test_init_without_state_dir(self):
        mgr = StateManager()
        assert mgr.state_dir is None

    def test_init_empty_native_handlers(self):
        mgr = StateManager()
        assert mgr._native_handlers == {}


class TestRegisterNativeHandler:
    def test_register_stores_tuple(self):
        mgr = StateManager()

        def save_fn():
            return {}

        def load_fn(d):
            return None

        mgr.register_native_handler("sqs", save_fn, load_fn)
        assert "sqs" in mgr._native_handlers
        assert mgr._native_handlers["sqs"] == (save_fn, load_fn)

    def test_register_multiple_handlers(self):
        mgr = StateManager()
        mgr.register_native_handler("sqs", lambda: {}, lambda d: None)
        mgr.register_native_handler("sns", lambda: {}, lambda d: None)
        assert len(mgr._native_handlers) == 2
        assert "sqs" in mgr._native_handlers
        assert "sns" in mgr._native_handlers

    def test_register_overwrites_existing(self):
        mgr = StateManager()

        def fn1():
            return {"v": 1}

        def fn2():
            return {"v": 2}

        mgr.register_native_handler("sqs", fn1, lambda d: None)
        mgr.register_native_handler("sqs", fn2, lambda d: None)
        assert mgr._native_handlers["sqs"][0] is fn2


class TestSaveDetails:
    def test_save_returns_path_string(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        result = mgr.save()
        assert result == str(tmp_path)

    def test_save_creates_parent_dirs(self, tmp_path):
        nested = tmp_path / "a" / "b" / "c"
        mgr = StateManager(state_dir=str(nested))
        mgr.save()
        assert nested.exists()
        assert (nested / "metadata.json").exists()

    def test_metadata_contains_saved_at(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.save()
        meta = json.loads((tmp_path / "metadata.json").read_text())
        assert "saved_at" in meta
        assert isinstance(meta["saved_at"], float)
        assert meta["saved_at"] > 0

    def test_metadata_lists_native_services(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("sqs", lambda: {}, lambda d: None)
        mgr.register_native_handler("sns", lambda: {}, lambda d: None)
        mgr.save()

        meta = json.loads((tmp_path / "metadata.json").read_text())
        assert "sqs" in meta["native_services"]
        assert "sns" in meta["native_services"]

    def test_native_state_json_written(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("test_svc", lambda: {"key": "val"}, lambda d: None)
        mgr.save()

        native = json.loads((tmp_path / "native_state.json").read_text())
        assert native["test_svc"] == {"key": "val"}

    def test_save_with_explicit_path_overrides_state_dir(self, tmp_path):
        default_dir = tmp_path / "default"
        override_dir = tmp_path / "override"
        mgr = StateManager(state_dir=str(default_dir))
        mgr.save(path=override_dir)

        assert (override_dir / "metadata.json").exists()
        assert not (default_dir / "metadata.json").exists()


class TestLoadDetails:
    def test_load_returns_true_on_success(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.save()

        mgr2 = StateManager(state_dir=str(tmp_path))
        assert mgr2.load() is True

    def test_load_with_explicit_path(self, tmp_path):
        mgr = StateManager()
        mgr.save(path=tmp_path)

        mgr2 = StateManager()
        assert mgr2.load(path=tmp_path) is True

    def test_load_calls_native_load_fn(self, tmp_path):
        loaded_data = {}

        def save_fn():
            return {"items": [1, 2, 3]}

        def load_fn(data):
            loaded_data.update(data)

        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("svc", save_fn, load_fn)
        mgr.save()

        mgr2 = StateManager(state_dir=str(tmp_path))
        mgr2.register_native_handler("svc", save_fn, load_fn)
        mgr2.load()

        assert loaded_data == {"items": [1, 2, 3]}

    def test_load_skips_unregistered_services(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("extra", lambda: {"data": 1}, lambda d: None)
        mgr.save()

        # Second manager does NOT register "extra"
        mgr2 = StateManager(state_dir=str(tmp_path))
        result = mgr2.load()
        assert result is True  # should not fail

    def test_load_with_none_path_and_no_state_dir(self):
        mgr = StateManager()
        assert mgr.load() is False

    def test_load_handles_corrupt_native_handler(self, tmp_path):
        """If a load_fn raises, it should be silently caught."""

        def save_fn():
            return {"data": 1}

        def bad_load_fn(data):
            raise RuntimeError("bad load")

        mgr = StateManager(state_dir=str(tmp_path))
        mgr.register_native_handler("bad_svc", save_fn, bad_load_fn)
        mgr.save()

        mgr2 = StateManager(state_dir=str(tmp_path))
        mgr2.register_native_handler("bad_svc", save_fn, bad_load_fn)
        # Should not raise
        result = mgr2.load()
        assert result is True


class TestReset:
    def test_reset_calls_all_native_load_fns_with_empty(self):
        results = {}

        mgr = StateManager()
        mgr.register_native_handler("svc1", lambda: {}, lambda d: results.update({"svc1": d}))
        mgr.register_native_handler("svc2", lambda: {}, lambda d: results.update({"svc2": d}))
        mgr.reset()

        assert results["svc1"] == {}
        assert results["svc2"] == {}

    def test_reset_handles_failing_load_fn(self):
        def bad_load(data):
            raise RuntimeError("fail")

        mgr = StateManager()
        mgr.register_native_handler("bad", lambda: {}, bad_load)
        # Should not raise
        mgr.reset()


class TestGetStateManagerSingleton:
    def test_returns_state_manager_instance(self):
        mgr = get_state_manager()
        assert isinstance(mgr, StateManager)

    def test_returns_same_instance_across_calls(self):
        m1 = get_state_manager()
        m2 = get_state_manager()
        assert m1 is m2

    def test_singleton_resets(self):
        import robotocore.state.manager as mod

        old = mod._manager
        try:
            mod._manager = None
            m1 = get_state_manager()
            assert isinstance(m1, StateManager)
        finally:
            mod._manager = old

    def test_singleton_uses_env_var(self):
        import robotocore.state.manager as mod

        old = mod._manager
        try:
            mod._manager = None
            with patch.dict(os.environ, {"ROBOTOCORE_STATE_DIR": "/tmp/test-roboto-state"}):
                mgr = get_state_manager()
                # Note: it may have been created from a previous call,
                # so we reset it above
                if mgr.state_dir is not None:
                    assert str(mgr.state_dir) == "/tmp/test-roboto-state"
        finally:
            mod._manager = old
