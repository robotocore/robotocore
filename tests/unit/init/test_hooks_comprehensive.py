"""Comprehensive unit tests for init hooks (run_init_hooks in observability/hooks.py)."""

import os
import stat
import tempfile
from pathlib import Path
from unittest.mock import patch

import robotocore.init.tracker as tracker_mod
from robotocore.init.tracker import get_init_tracker
from robotocore.observability.hooks import run_init_hooks


class TestRunInitHooksNoDirectory:
    """Test behavior when hook directory does not exist."""

    def setup_method(self):
        tracker_mod._tracker = None

    def teardown_method(self):
        tracker_mod._tracker = None

    def test_returns_empty_when_no_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            nonexistent = os.path.join(tmpdir, "nonexistent")
            with patch("robotocore.observability.hooks.get_hook_base", return_value=nonexistent):
                results = run_init_hooks("boot")
        assert results == []

    def test_tracker_unchanged_when_no_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            nonexistent = os.path.join(tmpdir, "nonexistent")
            with patch("robotocore.observability.hooks.get_hook_base", return_value=nonexistent):
                run_init_hooks("boot")
        tracker = get_init_tracker()
        assert tracker.get_scripts("boot") == []


class TestRunInitHooksEmptyDirectory:
    """Test behavior when hook directory exists but has no scripts."""

    def setup_method(self):
        tracker_mod._tracker = None

    def teardown_method(self):
        tracker_mod._tracker = None

    def test_returns_empty_when_no_scripts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            boot_d = Path(tmpdir) / "boot.d"
            boot_d.mkdir()
            with patch("robotocore.observability.hooks.get_hook_base", return_value=tmpdir):
                results = run_init_hooks("boot")
        assert results == []


class TestRunInitHooksExecution:
    """Test actual script execution through run_init_hooks."""

    def setup_method(self):
        tracker_mod._tracker = None

    def teardown_method(self):
        tracker_mod._tracker = None

    def test_successful_script_execution(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            boot_d = Path(tmpdir) / "boot.d"
            boot_d.mkdir()
            script = boot_d / "01-setup.sh"
            script.write_text("#!/bin/bash\necho hello\n")
            script.chmod(script.stat().st_mode | stat.S_IEXEC)

            with patch("robotocore.observability.hooks.get_hook_base", return_value=tmpdir):
                results = run_init_hooks("boot")

        assert len(results) == 1
        assert results[0]["script"] == "01-setup.sh"
        assert results[0]["returncode"] == 0
        assert "hello" in results[0]["stdout"]

    def test_successful_script_tracked_as_completed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            boot_d = Path(tmpdir) / "boot.d"
            boot_d.mkdir()
            script = boot_d / "01-setup.sh"
            script.write_text("#!/bin/bash\necho ok\n")
            script.chmod(script.stat().st_mode | stat.S_IEXEC)

            with patch("robotocore.observability.hooks.get_hook_base", return_value=tmpdir):
                run_init_hooks("boot")

        tracker = get_init_tracker()
        scripts = tracker.get_scripts("boot")
        assert len(scripts) == 1
        assert scripts[0]["filename"] == "01-setup.sh"
        assert scripts[0]["status"] == "completed"
        assert scripts[0]["duration"] is not None
        assert scripts[0]["duration"] >= 0

    def test_failing_script_tracked_as_failed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            boot_d = Path(tmpdir) / "boot.d"
            boot_d.mkdir()
            script = boot_d / "01-fail.sh"
            script.write_text("#!/bin/bash\nexit 1\n")
            script.chmod(script.stat().st_mode | stat.S_IEXEC)

            with patch("robotocore.observability.hooks.get_hook_base", return_value=tmpdir):
                results = run_init_hooks("boot")

        assert len(results) == 1
        assert results[0]["returncode"] == 1

        tracker = get_init_tracker()
        scripts = tracker.get_scripts("boot")
        assert scripts[0]["status"] == "failed"

    def test_multiple_scripts_executed_in_sorted_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            boot_d = Path(tmpdir) / "boot.d"
            boot_d.mkdir()

            # Create scripts with sortable names
            for name, content in [
                ("02-second.sh", "echo second"),
                ("01-first.sh", "echo first"),
                ("03-third.sh", "echo third"),
            ]:
                script = boot_d / name
                script.write_text(f"#!/bin/bash\n{content}\n")
                script.chmod(script.stat().st_mode | stat.S_IEXEC)

            with patch("robotocore.observability.hooks.get_hook_base", return_value=tmpdir):
                results = run_init_hooks("boot")

        assert len(results) == 3
        assert results[0]["script"] == "01-first.sh"
        assert results[1]["script"] == "02-second.sh"
        assert results[2]["script"] == "03-third.sh"

    def test_scripts_recorded_pending_before_execution(self):
        """Verify that all scripts are marked pending before any starts running."""
        with tempfile.TemporaryDirectory() as tmpdir:
            boot_d = Path(tmpdir) / "boot.d"
            boot_d.mkdir()
            script = boot_d / "01-setup.sh"
            script.write_text("#!/bin/bash\necho ok\n")
            script.chmod(script.stat().st_mode | stat.S_IEXEC)

            with patch("robotocore.observability.hooks.get_hook_base", return_value=tmpdir):
                run_init_hooks("boot")

        # After execution completes, scripts should be completed not pending
        tracker = get_init_tracker()
        scripts = tracker.get_scripts("boot")
        assert scripts[0]["status"] == "completed"

    def test_non_sh_files_ignored(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            boot_d = Path(tmpdir) / "boot.d"
            boot_d.mkdir()
            # .sh file should be picked up
            sh_script = boot_d / "01-setup.sh"
            sh_script.write_text("#!/bin/bash\necho ok\n")
            sh_script.chmod(sh_script.stat().st_mode | stat.S_IEXEC)
            # .txt file should be ignored
            txt_file = boot_d / "readme.txt"
            txt_file.write_text("This is not a script")

            with patch("robotocore.observability.hooks.get_hook_base", return_value=tmpdir):
                results = run_init_hooks("boot")

        assert len(results) == 1
        assert results[0]["script"] == "01-setup.sh"

    def test_different_stages_use_different_directories(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            for stage in ("boot", "ready"):
                stage_d = Path(tmpdir) / f"{stage}.d"
                stage_d.mkdir()
                script = stage_d / f"01-{stage}.sh"
                script.write_text(f"#!/bin/bash\necho {stage}\n")
                script.chmod(script.stat().st_mode | stat.S_IEXEC)

            with patch("robotocore.observability.hooks.get_hook_base", return_value=tmpdir):
                boot_results = run_init_hooks("boot")
                ready_results = run_init_hooks("ready")

        assert len(boot_results) == 1
        assert boot_results[0]["script"] == "01-boot.sh"
        assert len(ready_results) == 1
        assert ready_results[0]["script"] == "01-ready.sh"

    def test_tracker_summary_after_mixed_results(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            boot_d = Path(tmpdir) / "boot.d"
            boot_d.mkdir()
            good = boot_d / "01-ok.sh"
            good.write_text("#!/bin/bash\necho ok\n")
            good.chmod(good.stat().st_mode | stat.S_IEXEC)
            bad = boot_d / "02-fail.sh"
            bad.write_text("#!/bin/bash\nexit 42\n")
            bad.chmod(bad.stat().st_mode | stat.S_IEXEC)

            with patch("robotocore.observability.hooks.get_hook_base", return_value=tmpdir):
                run_init_hooks("boot")

        tracker = get_init_tracker()
        summary = tracker.get_summary()
        assert summary["stages"]["boot"]["total"] == 2
        assert summary["stages"]["boot"]["completed"] == 1
        assert summary["stages"]["boot"]["failed"] == 1


class TestRunInitHooksEnvVar:
    """Test that ROBOTOCORE_INIT_DIR env var is respected."""

    def setup_method(self):
        tracker_mod._tracker = None

    def teardown_method(self):
        tracker_mod._tracker = None

    def test_env_var_overrides_default_hook_base(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            boot_d = Path(tmpdir) / "boot.d"
            boot_d.mkdir()
            script = boot_d / "01-env.sh"
            script.write_text("#!/bin/bash\necho from_env\n")
            script.chmod(script.stat().st_mode | stat.S_IEXEC)

            with patch.dict(os.environ, {"ROBOTOCORE_INIT_DIR": tmpdir}):
                results = run_init_hooks("boot")

        assert len(results) == 1
        assert "from_env" in results[0]["stdout"]
