"""Unit tests for configurable snapshot save/load strategies."""

import os
import threading
from pathlib import Path
from unittest.mock import patch

import pytest

from robotocore.state.change_tracker import ChangeTracker
from robotocore.state.manager import (
    InvalidStrategyError,
    SnapshotLoadStrategy,
    SnapshotSaveStrategy,
    StateManager,
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
