"""Tests for init hook execution."""

import os
import stat
from unittest.mock import patch

from robotocore.observability.hooks import get_hook_base, run_init_hooks


class TestGetHookBase:
    def test_default_base(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("ROBOTOCORE_INIT_DIR", None)
            assert get_hook_base() == "/etc/robotocore/init"

    def test_custom_base(self):
        with patch.dict(os.environ, {"ROBOTOCORE_INIT_DIR": "/custom/init"}):
            assert get_hook_base() == "/custom/init"


class TestRunInitHooks:
    def test_no_directory(self, tmp_path):
        with patch(
            "robotocore.observability.hooks.get_hook_base",
            return_value=str(tmp_path / "nonexistent"),
        ):
            results = run_init_hooks("boot")
        assert results == []

    def test_empty_directory(self, tmp_path):
        boot_dir = tmp_path / "boot.d"
        boot_dir.mkdir()
        with patch(
            "robotocore.observability.hooks.get_hook_base",
            return_value=str(tmp_path),
        ):
            results = run_init_hooks("boot")
        assert results == []

    def test_executes_scripts_in_order(self, tmp_path):
        boot_dir = tmp_path / "boot.d"
        boot_dir.mkdir()

        # Create two scripts
        script1 = boot_dir / "01_first.sh"
        script1.write_text("#!/bin/bash\necho 'first'")
        script1.chmod(script1.stat().st_mode | stat.S_IEXEC)

        script2 = boot_dir / "02_second.sh"
        script2.write_text("#!/bin/bash\necho 'second'")
        script2.chmod(script2.stat().st_mode | stat.S_IEXEC)

        with patch(
            "robotocore.observability.hooks.get_hook_base",
            return_value=str(tmp_path),
        ):
            results = run_init_hooks("boot")

        assert len(results) == 2
        assert results[0]["script"] == "01_first.sh"
        assert results[0]["returncode"] == 0
        assert "first" in results[0]["stdout"]
        assert results[1]["script"] == "02_second.sh"
        assert results[1]["returncode"] == 0
        assert "second" in results[1]["stdout"]

    def test_captures_nonzero_exit(self, tmp_path):
        boot_dir = tmp_path / "boot.d"
        boot_dir.mkdir()

        script = boot_dir / "01_fail.sh"
        script.write_text("#!/bin/bash\nexit 1")
        script.chmod(script.stat().st_mode | stat.S_IEXEC)

        with patch(
            "robotocore.observability.hooks.get_hook_base",
            return_value=str(tmp_path),
        ):
            results = run_init_hooks("boot")

        assert len(results) == 1
        assert results[0]["returncode"] == 1

    def test_only_runs_sh_files(self, tmp_path):
        boot_dir = tmp_path / "boot.d"
        boot_dir.mkdir()

        # .sh file
        sh_file = boot_dir / "run.sh"
        sh_file.write_text("#!/bin/bash\necho 'yes'")
        sh_file.chmod(sh_file.stat().st_mode | stat.S_IEXEC)

        # .txt file should be ignored
        txt_file = boot_dir / "ignore.txt"
        txt_file.write_text("should not run")

        with patch(
            "robotocore.observability.hooks.get_hook_base",
            return_value=str(tmp_path),
        ):
            results = run_init_hooks("boot")

        assert len(results) == 1
        assert results[0]["script"] == "run.sh"

    def test_different_stages(self, tmp_path):
        for stage in ("boot", "ready", "shutdown"):
            stage_dir = tmp_path / f"{stage}.d"
            stage_dir.mkdir()
            script = stage_dir / "test.sh"
            script.write_text(f"#!/bin/bash\necho '{stage}'")
            script.chmod(script.stat().st_mode | stat.S_IEXEC)

        for stage in ("boot", "ready", "shutdown"):
            with patch(
                "robotocore.observability.hooks.get_hook_base",
                return_value=str(tmp_path),
            ):
                results = run_init_hooks(stage)
            assert len(results) == 1
            assert stage in results[0]["stdout"]

    def test_timeout_handling(self, tmp_path):
        boot_dir = tmp_path / "boot.d"
        boot_dir.mkdir()

        # Script that would take too long (we patch timeout to be very short)
        script = boot_dir / "slow.sh"
        script.write_text("#!/bin/bash\nsleep 100")
        script.chmod(script.stat().st_mode | stat.S_IEXEC)

        with (
            patch(
                "robotocore.observability.hooks.get_hook_base",
                return_value=str(tmp_path),
            ),
            patch(
                "subprocess.run",
                side_effect=__import__("subprocess").TimeoutExpired(cmd="bash", timeout=30),
            ),
        ):
            results = run_init_hooks("boot")

        assert len(results) == 1
        assert results[0]["returncode"] == -1
        assert "Timeout" in results[0]["stderr"]
