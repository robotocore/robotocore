"""Tests for auto-restore on startup feature."""

import tempfile
from unittest import mock

from robotocore.state.manager import StateManager


class TestAutoRestore:
    def test_restore_on_startup_with_env_var(self):
        """ROBOTOCORE_RESTORE_SNAPSHOT=<name> should restore that snapshot."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            manager.save(name="startup-snap")

            with mock.patch.dict("os.environ", {"ROBOTOCORE_RESTORE_SNAPSHOT": "startup-snap"}):
                result = manager.restore_on_startup()
            assert result is True

    def test_restore_on_startup_latest(self):
        """ROBOTOCORE_RESTORE_SNAPSHOT=latest should restore the most recent snapshot."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            manager.save(name="old-snap")
            manager.save(name="new-snap")

            with mock.patch.dict("os.environ", {"ROBOTOCORE_RESTORE_SNAPSHOT": "latest"}):
                result = manager.restore_on_startup()
            assert result is True

    def test_restore_on_startup_no_env_var(self):
        """Without ROBOTOCORE_RESTORE_SNAPSHOT, nothing is restored."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            with mock.patch.dict("os.environ", {}, clear=False):
                # Ensure the env var is not set
                import os

                os.environ.pop("ROBOTOCORE_RESTORE_SNAPSHOT", None)
                result = manager.restore_on_startup()
            assert result is False

    def test_restore_on_startup_missing_snapshot(self):
        """Restoring a non-existent snapshot should return False."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            with mock.patch.dict("os.environ", {"ROBOTOCORE_RESTORE_SNAPSHOT": "nonexistent"}):
                result = manager.restore_on_startup()
            assert result is False

    def test_restore_on_startup_latest_no_snapshots(self):
        """'latest' with no snapshots should return False."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            with mock.patch.dict("os.environ", {"ROBOTOCORE_RESTORE_SNAPSHOT": "latest"}):
                result = manager.restore_on_startup()
            assert result is False

    def test_restore_compressed_snapshot(self):
        """Auto-restore should work with compressed snapshots."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            manager.save(name="comp-startup", compress=True)

            with mock.patch.dict("os.environ", {"ROBOTOCORE_RESTORE_SNAPSHOT": "comp-startup"}):
                result = manager.restore_on_startup()
            assert result is True


class TestFindLatest:
    def test_find_latest_by_saved_at(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            manager.save(name="first")
            import time

            time.sleep(0.01)  # ensure distinct timestamps
            manager.save(name="second")

            latest = manager._find_latest_snapshot()
            assert latest == "second"

    def test_find_latest_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            assert manager._find_latest_snapshot() is None

    def test_load_latest_keyword(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir)
            manager.save(name="alpha")
            import time

            time.sleep(0.01)
            manager.save(name="beta")

            success = manager.load(name="latest")
            assert success
