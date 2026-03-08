"""Tests for state manager persistence enhancements: debounce, export, import."""

import time

from robotocore.state.manager import StateManager


class TestSaveDebounced:
    def test_first_save_executes(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr._debounce_interval = 0.5
        result = mgr.save_debounced()
        assert result is True
        assert (tmp_path / "metadata.json").exists()

    def test_rapid_saves_debounced(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr._debounce_interval = 10  # very long interval
        first = mgr.save_debounced()
        second = mgr.save_debounced()
        assert first is True
        assert second is False  # debounced

    def test_save_after_interval(self, tmp_path):
        mgr = StateManager(state_dir=str(tmp_path))
        mgr._debounce_interval = 0.01  # very short
        mgr.save_debounced()
        time.sleep(0.02)
        result = mgr.save_debounced()
        assert result is True

    def test_debounce_interval_default(self):
        mgr = StateManager()
        assert mgr._debounce_interval == 1.0


class TestExportJson:
    def test_empty_export(self):
        mgr = StateManager()
        data = mgr.export_json()
        assert data["version"] == "1.0"
        assert "exported_at" in data
        assert data["native_state"] == {}

    def test_export_with_native_handler(self):
        mgr = StateManager()
        mgr.register_native_handler(
            "sqs",
            lambda: {"queues": ["q1"]},
            lambda d: None,
        )
        data = mgr.export_json()
        assert data["native_state"]["sqs"] == {"queues": ["q1"]}

    def test_export_multiple_services(self):
        mgr = StateManager()
        mgr.register_native_handler("sqs", lambda: {"q": 1}, lambda d: None)
        mgr.register_native_handler("sns", lambda: {"t": 2}, lambda d: None)
        data = mgr.export_json()
        assert "sqs" in data["native_state"]
        assert "sns" in data["native_state"]

    def test_export_handles_failing_handler(self):
        def bad_save():
            raise RuntimeError("fail")

        mgr = StateManager()
        mgr.register_native_handler("bad", bad_save, lambda d: None)
        data = mgr.export_json()
        assert "bad" not in data["native_state"]


class TestImportJson:
    def test_import_loads_native_state(self):
        loaded = {}

        mgr = StateManager()
        mgr.register_native_handler("sqs", lambda: {}, lambda d: loaded.update(d))

        mgr.import_json(
            {
                "version": "1.0",
                "native_state": {"sqs": {"queues": ["q1", "q2"]}},
            }
        )
        assert loaded == {"queues": ["q1", "q2"]}

    def test_import_skips_unregistered(self):
        loaded = {}
        mgr = StateManager()
        mgr.register_native_handler("sqs", lambda: {}, lambda d: loaded.update(d))

        # Import data for a service we don't have registered
        mgr.import_json(
            {
                "native_state": {"unknown": {"data": 1}, "sqs": {"x": 1}},
            }
        )
        assert loaded == {"x": 1}

    def test_import_empty_native_state(self):
        mgr = StateManager()
        # Should not raise
        mgr.import_json({"native_state": {}})

    def test_import_missing_native_state_key(self):
        mgr = StateManager()
        # Should not raise
        mgr.import_json({})

    def test_import_handles_failing_handler(self):
        def bad_load(data):
            raise RuntimeError("fail")

        mgr = StateManager()
        mgr.register_native_handler("bad", lambda: {}, bad_load)
        # Should not raise
        mgr.import_json({"native_state": {"bad": {"data": 1}}})

    def test_round_trip_export_import(self):
        loaded = {}

        mgr = StateManager()
        mgr.register_native_handler(
            "sqs",
            lambda: {"queues": ["q1"]},
            lambda d: loaded.update(d),
        )

        exported = mgr.export_json()
        mgr.import_json(exported)
        assert loaded == {"queues": ["q1"]}
