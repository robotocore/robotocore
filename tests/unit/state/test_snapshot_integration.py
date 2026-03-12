"""Semantic integration tests for snapshot strategies.

These tests verify the end-to-end behavior of snapshot strategies with
simulated state changes, without requiring a running server.
"""

import time
from pathlib import Path

from robotocore.state.manager import (
    SnapshotLoadStrategy,
    SnapshotSaveStrategy,
    StateManager,
)


class TestScheduledStrategyIntegration:
    """Test scheduled strategy end-to-end."""

    def test_create_resources_wait_interval_verify_state_written(self, tmp_path: Path) -> None:
        """Simulate: state changes -> scheduled tick -> file appears."""
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.SCHEDULED
        manager.flush_interval = 0.1  # 100ms for test speed

        # Simulate creating resources (marks dirty)
        manager.change_tracker.mark_dirty()

        # Start the scheduler, let it tick once
        manager.start_scheduled_saver()
        try:
            # Wait for the scheduled save to fire and dirty flag to clear.
            # Both conditions must be met: file written AND mark_clean() called.
            # These happen sequentially in the same thread but we poll from
            # another thread, so we must wait for both.
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if (tmp_path / "metadata.json").exists() and not manager.change_tracker.is_dirty:
                    break
                time.sleep(0.05)
            assert (tmp_path / "metadata.json").exists(), "Scheduled save did not fire"
            assert not manager.change_tracker.is_dirty, "Dirty flag not cleared after save"
        finally:
            manager.stop_scheduled_saver()


class TestOnRequestSaveIntegration:
    """Test on_request save strategy end-to-end."""

    def test_mutating_request_triggers_save(self, tmp_path: Path) -> None:
        """Simulate: POST request -> state saved to disk."""
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.ON_REQUEST
        manager._last_save_time = 0.0  # Bypass debounce

        # Simulate a mutating AWS request
        manager.change_tracker.on_request("POST", "/sqs")
        manager.on_mutating_request()

        assert (tmp_path / "metadata.json").exists()


class TestOnRequestLoadIntegration:
    """Test on_request load strategy end-to-end."""

    def test_state_file_exists_first_request_loads(self, tmp_path: Path) -> None:
        """Pre-existing state file + first request -> state loaded."""
        # Create state file
        writer = StateManager(state_dir=str(tmp_path))
        writer.save()

        # New manager with on_request load strategy
        reader = StateManager(state_dir=str(tmp_path))
        reader.load_strategy = SnapshotLoadStrategy.ON_REQUEST
        assert not reader._lazy_loaded

        reader.on_first_request()
        assert reader._lazy_loaded


class TestManualStrategyIntegration:
    """Test manual strategy end-to-end."""

    def test_no_auto_save_explicit_save_writes_file(self, tmp_path: Path) -> None:
        """Manual strategy: mutations don't auto-save, explicit save() does."""
        manager = StateManager(state_dir=str(tmp_path))
        manager.save_strategy = SnapshotSaveStrategy.MANUAL

        # Simulate mutations
        manager.change_tracker.mark_dirty()
        manager.on_mutating_request()
        assert not (tmp_path / "metadata.json").exists()

        # Simulate shutdown -- should NOT save in manual mode
        manager.on_shutdown()
        assert not (tmp_path / "metadata.json").exists()

        # Explicit save via API
        manager.save()
        assert (tmp_path / "metadata.json").exists()
