"""Unit tests for configurable snapshot save/load strategies."""

import json
import os
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from robotocore.state.change_tracker import ChangeTracker
from robotocore.state.manager import (
    InvalidStrategyError,
    SnapshotLoadStrategy,
    SnapshotSaveStrategy,
    StateManager,
    _parse_load_strategy,
    _parse_save_strategy,
)


class TestDefaultStrategies:
    """Test default strategy values."""

    def test_default_save_strategy_is_on_shutdown(self) -> None:
        manager = StateManager()
        assert manager.save_strategy == SnapshotSaveStrategy.ON_SHUTDOWN

    def test_default_load_strategy_is_on_startup(self) -> None:
        manager = StateManager()
        assert manager.load_strategy == SnapshotLoadStrategy.ON_STARTUP


class TestSaveStrategyFromEnv:
    """Test save strategy selection from environment variables."""

    def test_on_shutdown_strategy(self) -> None:
        with patch.dict(os.environ, {"SNAPSHOT_SAVE_STRATEGY": "on_shutdown"}):
            manager = StateManager()
            assert manager.save_strategy == SnapshotSaveStrategy.ON_SHUTDOWN

    def test_on_request_strategy(self) -> None:
        with patch.dict(os.environ, {"SNAPSHOT_SAVE_STRATEGY": "on_request"}):
            manager = StateManager()
            assert manager.save_strategy == SnapshotSaveStrategy.ON_REQUEST

    def test_scheduled_strategy(self) -> None:
        with patch.dict(os.environ, {"SNAPSHOT_SAVE_STRATEGY": "scheduled"}):
            manager = StateManager()
            assert manager.save_strategy == SnapshotSaveStrategy.SCHEDULED

    def test_manual_strategy(self) -> None:
        with patch.dict(os.environ, {"SNAPSHOT_SAVE_STRATEGY": "manual"}):
            manager = StateManager()
            assert manager.save_strategy == SnapshotSaveStrategy.MANUAL

    def test_invalid_strategy_raises_helpful_error(self) -> None:
        with patch.dict(os.environ, {"SNAPSHOT_SAVE_STRATEGY": "bogus"}):
            with pytest.raises(InvalidStrategyError, match="bogus"):
                StateManager()


class TestLoadStrategyFromEnv:
    """Test load strategy selection from environment variables."""

    def test_on_startup_strategy(self) -> None:
        with patch.dict(os.environ, {"SNAPSHOT_LOAD_STRATEGY": "on_startup"}):
            manager = StateManager()
            assert manager.load_strategy == SnapshotLoadStrategy.ON_STARTUP

    def test_on_request_strategy(self) -> None:
        with patch.dict(os.environ, {"SNAPSHOT_LOAD_STRATEGY": "on_request"}):
            manager = StateManager()
            assert manager.load_strategy == SnapshotLoadStrategy.ON_REQUEST

    def test_manual_strategy(self) -> None:
        with patch.dict(os.environ, {"SNAPSHOT_LOAD_STRATEGY": "manual"}):
            manager = StateManager()
            assert manager.load_strategy == SnapshotLoadStrategy.MANUAL

    def test_invalid_load_strategy_raises_helpful_error(self) -> None:
        with patch.dict(os.environ, {"SNAPSHOT_LOAD_STRATEGY": "bogus"}):
            with pytest.raises(InvalidStrategyError, match="bogus"):
                StateManager()


class TestOnShutdownSaveStrategy:
    """Test on_shutdown save strategy behavior."""

    def test_saves_on_shutdown_signal(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.ON_SHUTDOWN
        manager.on_shutdown()
        assert (tmp_path / "metadata.json").exists()

    def test_does_not_auto_save_on_request(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.ON_SHUTDOWN
        manager.on_mutating_request()
        assert not (tmp_path / "metadata.json").exists()


class TestScheduledSaveStrategy:
    """Test scheduled save strategy behavior."""

    def test_scheduled_saves_at_configured_interval(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.SCHEDULED
        manager.change_tracker.mark_dirty()

        # Simulate the scheduled save tick
        manager._do_scheduled_save()
        assert (tmp_path / "metadata.json").exists()

    def test_scheduled_skips_save_when_state_unchanged(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.SCHEDULED
        # Don't mark dirty
        manager._do_scheduled_save()
        assert not (tmp_path / "metadata.json").exists()

    def test_flush_interval_configuration(self) -> None:
        with patch.dict(os.environ, {"SNAPSHOT_FLUSH_INTERVAL": "30"}):
            manager = StateManager()
            assert manager.flush_interval == 30.0

    def test_flush_interval_default(self) -> None:
        manager = StateManager()
        assert manager.flush_interval == 15.0


class TestOnRequestSaveStrategy:
    """Test on_request save strategy behavior."""

    def test_triggers_after_mutating_request(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.ON_REQUEST
        manager.change_tracker.mark_dirty()
        # Force past debounce
        manager._last_save_time = 0.0
        manager.on_mutating_request()
        assert (tmp_path / "metadata.json").exists()

    def test_debounces_max_once_per_second(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.ON_REQUEST
        manager.change_tracker.mark_dirty()
        # Force past debounce for first save
        manager._last_save_time = 0.0
        manager.on_mutating_request()
        assert (tmp_path / "metadata.json").exists()

        # Remove the file and try again immediately -- should be debounced
        (tmp_path / "metadata.json").unlink()
        manager.change_tracker.mark_dirty()
        manager.on_mutating_request()
        assert not (tmp_path / "metadata.json").exists()


class TestOnRequestLoadStrategy:
    """Test on_request load strategy behavior."""

    def test_loads_on_first_request(self, tmp_path: Path) -> None:
        # Pre-create state
        manager = StateManager(state_dir=str(tmp_path))
        manager.save()
        manager2 = StateManager(state_dir=str(tmp_path))
        manager2.load_strategy = SnapshotLoadStrategy.ON_REQUEST
        assert not manager2._lazy_loaded
        manager2.on_first_request()
        assert manager2._lazy_loaded

    def test_does_not_reload_after_first_load(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save()
        manager2 = StateManager(state_dir=str(tmp_path))
        manager2.load_strategy = SnapshotLoadStrategy.ON_REQUEST
        manager2.on_first_request()
        assert manager2._lazy_loaded
        # Call again -- should be a no-op
        with patch.object(manager2, "load") as mock_load:
            manager2.on_first_request()
            mock_load.assert_not_called()


class TestManualStrategy:
    """Test manual strategy behavior."""

    def test_manual_save_does_not_auto_save(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.MANUAL
        manager.change_tracker.mark_dirty()
        manager.on_mutating_request()
        assert not (tmp_path / "metadata.json").exists()

    def test_manual_save_does_not_save_on_shutdown(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.MANUAL
        manager.on_shutdown()
        assert not (tmp_path / "metadata.json").exists()

    def test_manual_load_does_not_auto_load(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save()
        manager2 = StateManager(state_dir=str(tmp_path))
        manager2.load_strategy = SnapshotLoadStrategy.MANUAL
        manager2.on_first_request()
        assert not manager2._lazy_loaded


class TestChangeTracker:
    """Test change tracker dirty flag behavior."""

    def test_dirty_after_mutation(self) -> None:
        tracker = ChangeTracker()
        assert not tracker.is_dirty
        tracker.mark_dirty()
        assert tracker.is_dirty

    def test_clean_after_save(self) -> None:
        tracker = ChangeTracker()
        tracker.mark_dirty()
        assert tracker.is_dirty
        tracker.mark_clean()
        assert not tracker.is_dirty

    def test_not_dirty_initially(self) -> None:
        tracker = ChangeTracker()
        assert not tracker.is_dirty

    def test_not_dirty_after_read_request(self) -> None:
        tracker = ChangeTracker()
        tracker.on_request("GET", "/some-path")
        assert not tracker.is_dirty

    def test_not_dirty_after_head_request(self) -> None:
        tracker = ChangeTracker()
        tracker.on_request("HEAD", "/some-path")
        assert not tracker.is_dirty

    def test_dirty_after_post_request(self) -> None:
        tracker = ChangeTracker()
        tracker.on_request("POST", "/some-path")
        assert tracker.is_dirty

    def test_dirty_after_put_request(self) -> None:
        tracker = ChangeTracker()
        tracker.on_request("PUT", "/some-path")
        assert tracker.is_dirty

    def test_dirty_after_delete_request(self) -> None:
        tracker = ChangeTracker()
        tracker.on_request("DELETE", "/some-path")
        assert tracker.is_dirty

    def test_not_dirty_for_robotocore_management_paths(self) -> None:
        tracker = ChangeTracker()
        tracker.on_request("POST", "/_robotocore/state/save")
        assert not tracker.is_dirty

    def test_thread_safety(self) -> None:
        tracker = ChangeTracker()
        errors: list[str] = []

        def mark_many():
            try:
                for _ in range(100):
                    tracker.mark_dirty()
                    tracker.mark_clean()
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=mark_many) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors

    def test_not_dirty_after_options_request(self) -> None:
        tracker = ChangeTracker()
        tracker.on_request("OPTIONS", "/some-path")
        assert not tracker.is_dirty

    def test_dirty_after_patch_request(self) -> None:
        tracker = ChangeTracker()
        tracker.on_request("PATCH", "/some-path")
        assert tracker.is_dirty

    def test_case_insensitive_method_matching(self) -> None:
        """on_request uppercases the method, so 'get' should be read-only."""
        tracker = ChangeTracker()
        tracker.on_request("get", "/some-path")
        assert not tracker.is_dirty

    def test_management_path_post_not_dirty(self) -> None:
        """POST to any /_robotocore/ subpath should not set dirty."""
        tracker = ChangeTracker()
        tracker.on_request("POST", "/_robotocore/chaos/rules")
        assert not tracker.is_dirty

    def test_management_path_delete_not_dirty(self) -> None:
        tracker = ChangeTracker()
        tracker.on_request("DELETE", "/_robotocore/chaos/rules/abc")
        assert not tracker.is_dirty

    def test_non_management_post_dirty(self) -> None:
        """POST to a non-management path should set dirty."""
        tracker = ChangeTracker()
        tracker.on_request("POST", "/sqs/some-queue")
        assert tracker.is_dirty

    def test_mark_dirty_idempotent(self) -> None:
        tracker = ChangeTracker()
        tracker.mark_dirty()
        tracker.mark_dirty()
        assert tracker.is_dirty

    def test_mark_clean_idempotent(self) -> None:
        tracker = ChangeTracker()
        tracker.mark_clean()
        assert not tracker.is_dirty


# ---------------------------------------------------------------------------
# Parse strategy helper functions
# ---------------------------------------------------------------------------


class TestParseSaveStrategy:
    """Test _parse_save_strategy directly."""

    def test_all_valid_values(self) -> None:
        assert _parse_save_strategy("on_shutdown") == SnapshotSaveStrategy.ON_SHUTDOWN
        assert _parse_save_strategy("on_request") == SnapshotSaveStrategy.ON_REQUEST
        assert _parse_save_strategy("scheduled") == SnapshotSaveStrategy.SCHEDULED
        assert _parse_save_strategy("manual") == SnapshotSaveStrategy.MANUAL

    def test_invalid_value_lists_all_valid(self) -> None:
        with pytest.raises(InvalidStrategyError, match="on_shutdown") as exc_info:
            _parse_save_strategy("nope")
        # The error message should list all valid values
        msg = str(exc_info.value)
        assert "on_request" in msg
        assert "scheduled" in msg
        assert "manual" in msg

    def test_empty_string_raises(self) -> None:
        with pytest.raises(InvalidStrategyError):
            _parse_save_strategy("")

    def test_case_sensitive(self) -> None:
        """Strategy parsing is case-sensitive -- ON_SHUTDOWN != on_shutdown."""
        with pytest.raises(InvalidStrategyError):
            _parse_save_strategy("ON_SHUTDOWN")


class TestParseLoadStrategy:
    """Test _parse_load_strategy directly."""

    def test_all_valid_values(self) -> None:
        assert _parse_load_strategy("on_startup") == SnapshotLoadStrategy.ON_STARTUP
        assert _parse_load_strategy("on_request") == SnapshotLoadStrategy.ON_REQUEST
        assert _parse_load_strategy("manual") == SnapshotLoadStrategy.MANUAL

    def test_invalid_value_lists_all_valid(self) -> None:
        with pytest.raises(InvalidStrategyError, match="nope") as exc_info:
            _parse_load_strategy("nope")
        msg = str(exc_info.value)
        assert "on_startup" in msg
        assert "on_request" in msg
        assert "manual" in msg

    def test_empty_string_raises(self) -> None:
        with pytest.raises(InvalidStrategyError):
            _parse_load_strategy("")


# ---------------------------------------------------------------------------
# Enum values
# ---------------------------------------------------------------------------


class TestStrategyEnumValues:
    """Verify enum .value strings match expected wire format."""

    def test_save_strategy_values(self) -> None:
        assert SnapshotSaveStrategy.ON_SHUTDOWN.value == "on_shutdown"
        assert SnapshotSaveStrategy.ON_REQUEST.value == "on_request"
        assert SnapshotSaveStrategy.SCHEDULED.value == "scheduled"
        assert SnapshotSaveStrategy.MANUAL.value == "manual"

    def test_load_strategy_values(self) -> None:
        assert SnapshotLoadStrategy.ON_STARTUP.value == "on_startup"
        assert SnapshotLoadStrategy.ON_REQUEST.value == "on_request"
        assert SnapshotLoadStrategy.MANUAL.value == "manual"

    def test_save_strategy_count(self) -> None:
        assert len(SnapshotSaveStrategy) == 4

    def test_load_strategy_count(self) -> None:
        assert len(SnapshotLoadStrategy) == 3

    def test_invalid_strategy_is_value_error(self) -> None:
        assert issubclass(InvalidStrategyError, ValueError)


# ---------------------------------------------------------------------------
# Scheduled saver lifecycle
# ---------------------------------------------------------------------------


class TestScheduledSaverLifecycle:
    """Test start/stop of the scheduled saver thread."""

    def test_start_creates_daemon_thread(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.SCHEDULED
        manager.flush_interval = 60.0  # Long interval so it won't fire during test
        try:
            manager.start_scheduled_saver()
            assert manager._scheduler_thread is not None
            assert manager._scheduler_thread.is_alive()
            assert manager._scheduler_thread.daemon
            assert manager._scheduler_thread.name == "snapshot-scheduler"
        finally:
            manager.stop_scheduled_saver()

    def test_start_is_idempotent(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.flush_interval = 60.0
        try:
            manager.start_scheduled_saver()
            first_thread = manager._scheduler_thread
            manager.start_scheduled_saver()
            assert manager._scheduler_thread is first_thread
        finally:
            manager.stop_scheduled_saver()

    def test_stop_sets_thread_to_none(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.flush_interval = 60.0
        manager.start_scheduled_saver()
        manager.stop_scheduled_saver()
        assert manager._scheduler_thread is None

    def test_stop_without_start_is_noop(self) -> None:
        manager = StateManager()
        manager.stop_scheduled_saver()  # Should not raise
        assert manager._scheduler_thread is None

    def test_on_shutdown_stops_scheduler(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.SCHEDULED
        manager.flush_interval = 60.0
        manager.start_scheduled_saver()
        assert manager._scheduler_thread is not None
        manager.on_shutdown()
        assert manager._scheduler_thread is None


class TestScheduledSaveDoesNotSaveWhenNoStateDir:
    """Test _do_scheduled_save with no state_dir configured."""

    def test_skips_save_when_no_state_dir(self) -> None:
        manager = StateManager()  # no state_dir
        manager.change_tracker.mark_dirty()
        manager._do_scheduled_save()
        # Should not raise, just skip
        assert manager.change_tracker.is_dirty  # still dirty since no save occurred

    def test_clears_dirty_after_successful_save(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.change_tracker.mark_dirty()
        manager._do_scheduled_save()
        assert not manager.change_tracker.is_dirty


# ---------------------------------------------------------------------------
# save_debounced
# ---------------------------------------------------------------------------


class TestSaveDebounced:
    """Test debounce logic for save_debounced."""

    def test_returns_true_when_save_performed(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager._last_save_time = 0.0
        result = manager.save_debounced()
        assert result is True
        assert (tmp_path / "metadata.json").exists()

    def test_returns_false_when_debounced(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager._last_save_time = 0.0
        manager.save_debounced()
        # Immediately try again
        result = manager.save_debounced()
        assert result is False

    def test_updates_last_save_time(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager._last_save_time = 0.0
        before = time.monotonic()
        manager.save_debounced()
        assert manager._last_save_time >= before

    def test_debounce_interval_configurable(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager._debounce_interval = 0.0  # No debounce
        manager._last_save_time = 0.0
        assert manager.save_debounced() is True
        assert manager.save_debounced() is True  # Should succeed immediately


# ---------------------------------------------------------------------------
# on_mutating_request skips when not dirty
# ---------------------------------------------------------------------------


class TestOnMutatingRequestSkipsClean:
    """Test that on_mutating_request does nothing when state is clean."""

    def test_skips_when_not_dirty(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.ON_REQUEST
        manager._last_save_time = 0.0
        # Don't mark dirty
        manager.on_mutating_request()
        assert not (tmp_path / "metadata.json").exists()

    def test_skips_for_non_on_request_strategy(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.SCHEDULED
        manager.change_tracker.mark_dirty()
        manager._last_save_time = 0.0
        manager.on_mutating_request()
        assert not (tmp_path / "metadata.json").exists()


# ---------------------------------------------------------------------------
# on_first_request with various strategies
# ---------------------------------------------------------------------------


class TestOnFirstRequestStrategies:
    """Test on_first_request behavior with different load strategies."""

    def test_noop_for_on_startup_strategy(self) -> None:
        manager = StateManager()
        manager.load_strategy = SnapshotLoadStrategy.ON_STARTUP
        manager.on_first_request()
        assert not manager._lazy_loaded

    def test_noop_for_manual_strategy(self) -> None:
        manager = StateManager()
        manager.load_strategy = SnapshotLoadStrategy.MANUAL
        manager.on_first_request()
        assert not manager._lazy_loaded

    def test_on_request_sets_lazy_loaded_even_when_load_fails(self) -> None:
        """If load fails (e.g. no state dir), _lazy_loaded should still be True."""
        manager = StateManager()  # no state_dir
        manager.load_strategy = SnapshotLoadStrategy.ON_REQUEST
        manager.on_first_request()
        assert manager._lazy_loaded


# ---------------------------------------------------------------------------
# on_shutdown with various strategies
# ---------------------------------------------------------------------------


class TestOnShutdownStrategies:
    """Test on_shutdown behavior across all save strategies."""

    def test_on_request_strategy_does_not_save_on_shutdown(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.ON_REQUEST
        manager.on_shutdown()
        assert not (tmp_path / "metadata.json").exists()

    def test_scheduled_strategy_does_not_save_on_shutdown(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.SCHEDULED
        manager.on_shutdown()
        assert not (tmp_path / "metadata.json").exists()

    def test_on_shutdown_without_state_dir_does_not_raise(self) -> None:
        manager = StateManager()  # no state_dir
        manager.save_strategy = SnapshotSaveStrategy.ON_SHUTDOWN
        manager.on_shutdown()  # Should not raise


# ---------------------------------------------------------------------------
# Flush interval edge cases
# ---------------------------------------------------------------------------


class TestFlushIntervalEdgeCases:
    """Test flush interval parsing from env."""

    def test_fractional_interval(self) -> None:
        with patch.dict(os.environ, {"SNAPSHOT_FLUSH_INTERVAL": "0.5"}):
            manager = StateManager()
            assert manager.flush_interval == 0.5

    def test_large_interval(self) -> None:
        with patch.dict(os.environ, {"SNAPSHOT_FLUSH_INTERVAL": "3600"}):
            manager = StateManager()
            assert manager.flush_interval == 3600.0


# ---------------------------------------------------------------------------
# Combined strategy + env var interactions
# ---------------------------------------------------------------------------


class TestCombinedStrategies:
    """Test both save and load strategies configured simultaneously."""

    def test_both_strategies_from_env(self) -> None:
        with patch.dict(
            os.environ,
            {
                "SNAPSHOT_SAVE_STRATEGY": "scheduled",
                "SNAPSHOT_LOAD_STRATEGY": "on_request",
                "SNAPSHOT_FLUSH_INTERVAL": "5",
            },
        ):
            manager = StateManager()
            assert manager.save_strategy == SnapshotSaveStrategy.SCHEDULED
            assert manager.load_strategy == SnapshotLoadStrategy.ON_REQUEST
            assert manager.flush_interval == 5.0

    def test_save_env_set_load_default(self) -> None:
        with patch.dict(os.environ, {"SNAPSHOT_SAVE_STRATEGY": "manual"}, clear=False):
            # Ensure SNAPSHOT_LOAD_STRATEGY is not set
            env = os.environ.copy()
            env.pop("SNAPSHOT_LOAD_STRATEGY", None)
            with patch.dict(os.environ, env, clear=True):
                os.environ["SNAPSHOT_SAVE_STRATEGY"] = "manual"
                manager = StateManager()
                assert manager.save_strategy == SnapshotSaveStrategy.MANUAL
                assert manager.load_strategy == SnapshotLoadStrategy.ON_STARTUP


# ---------------------------------------------------------------------------
# StateManager initialization
# ---------------------------------------------------------------------------


class TestStateManagerInit:
    """Test StateManager constructor behavior."""

    def test_state_dir_none_by_default(self) -> None:
        manager = StateManager()
        assert manager.state_dir is None

    def test_state_dir_set_from_constructor(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        assert manager.state_dir == tmp_path

    def test_change_tracker_initialized(self) -> None:
        manager = StateManager()
        assert isinstance(manager.change_tracker, ChangeTracker)
        assert not manager.change_tracker.is_dirty

    def test_lazy_loaded_false_initially(self) -> None:
        manager = StateManager()
        assert manager._lazy_loaded is False

    def test_debounce_interval_default(self) -> None:
        manager = StateManager()
        assert manager._debounce_interval == 1.0

    def test_scheduler_thread_none_initially(self) -> None:
        manager = StateManager()
        assert manager._scheduler_thread is None


# ---------------------------------------------------------------------------
# Native handler registration with strategies
# ---------------------------------------------------------------------------


class TestNativeHandlersWithStrategies:
    """Test that native handler save/load works with strategy-triggered saves."""

    def test_on_request_save_includes_native_state(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.ON_REQUEST
        manager._last_save_time = 0.0

        # Register a native handler
        native_state = {"key": "value"}
        manager.register_native_handler(
            "test-service",
            save_fn=lambda: native_state,
            load_fn=lambda data: None,
        )

        manager.change_tracker.mark_dirty()
        manager.on_mutating_request()

        # Verify native state was saved
        native_path = tmp_path / "native_state.json"
        assert native_path.exists()
        saved = json.loads(native_path.read_text())
        assert saved["test-service"] == {"key": "value"}

    def test_scheduled_save_includes_native_state(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.SCHEDULED

        native_state = {"items": [1, 2, 3]}
        manager.register_native_handler(
            "my-service",
            save_fn=lambda: native_state,
            load_fn=lambda data: None,
        )

        manager.change_tracker.mark_dirty()
        manager._do_scheduled_save()

        native_path = tmp_path / "native_state.json"
        assert native_path.exists()
        saved = json.loads(native_path.read_text())
        assert saved["my-service"] == {"items": [1, 2, 3]}

    def test_on_shutdown_save_includes_native_state(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.ON_SHUTDOWN

        native_state = {"count": 42}
        manager.register_native_handler(
            "counter-svc",
            save_fn=lambda: native_state,
            load_fn=lambda data: None,
        )

        manager.on_shutdown()

        native_path = tmp_path / "native_state.json"
        assert native_path.exists()
        saved = json.loads(native_path.read_text())
        assert saved["counter-svc"] == {"count": 42}


# ---------------------------------------------------------------------------
# Metadata written by strategy-triggered saves
# ---------------------------------------------------------------------------


class TestMetadataInStrategySaves:
    """Verify metadata.json content when saves are triggered by strategies."""

    def test_on_shutdown_metadata_has_version(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.ON_SHUTDOWN
        manager.on_shutdown()
        meta = json.loads((tmp_path / "metadata.json").read_text())
        assert meta["version"] == "1.0"
        assert "timestamp" in meta
        assert "saved_at" in meta

    def test_scheduled_save_metadata_has_timestamp(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.change_tracker.mark_dirty()
        before = time.time()
        manager._do_scheduled_save()
        meta = json.loads((tmp_path / "metadata.json").read_text())
        assert meta["saved_at"] >= before

    def test_on_request_save_metadata_has_no_name(self, tmp_path: Path) -> None:
        """Automatic saves don't have a snapshot name."""
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.ON_REQUEST
        manager._last_save_time = 0.0
        manager.change_tracker.mark_dirty()
        manager.on_mutating_request()
        meta = json.loads((tmp_path / "metadata.json").read_text())
        assert meta["name"] is None


# ---------------------------------------------------------------------------
# _do_scheduled_save error handling
# ---------------------------------------------------------------------------


class TestScheduledSaveErrorHandling:
    """Test that _do_scheduled_save handles errors gracefully."""

    def test_does_not_raise_on_save_error(self, tmp_path: Path) -> None:
        manager = StateManager(state_dir=str(tmp_path))
        manager.change_tracker.mark_dirty()

        # Make save fail by patching _save_moto_state to raise
        with patch.object(manager, "save", side_effect=RuntimeError("disk full")):
            manager._do_scheduled_save()  # Should not raise

        # Dirty flag should NOT be cleared since save failed
        assert manager.change_tracker.is_dirty
