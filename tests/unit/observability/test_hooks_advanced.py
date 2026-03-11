"""Advanced tests for init hooks: script execution, timeouts, ordering,
error handling, and env var configuration."""

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
        with patch.dict(os.environ, {"ROBOTOCORE_INIT_DIR": "/custom/hooks"}):
            assert get_hook_base() == "/custom/hooks"


class TestRunInitHooks:
    def test_nonexistent_directory_returns_empty(self, tmp_path):
        with patch.dict(os.environ, {"ROBOTOCORE_INIT_DIR": str(tmp_path / "nonexistent")}):
            results = run_init_hooks("boot")
        assert results == []

    def test_empty_directory_returns_empty(self, tmp_path):
        hook_dir = tmp_path / "boot.d"
        hook_dir.mkdir(parents=True)
        with patch.dict(os.environ, {"ROBOTOCORE_INIT_DIR": str(tmp_path)}):
            results = run_init_hooks("boot")
        assert results == []

    def test_successful_script_execution(self, tmp_path):
        hook_dir = tmp_path / "ready.d"
        hook_dir.mkdir(parents=True)
        script = hook_dir / "01-test.sh"
        script.write_text("#!/bin/bash\necho 'hello from hook'")
        script.chmod(stat.S_IRWXU)

        with patch.dict(os.environ, {"ROBOTOCORE_INIT_DIR": str(tmp_path)}):
            results = run_init_hooks("ready")

        assert len(results) == 1
        assert results[0]["script"] == "01-test.sh"
        assert results[0]["returncode"] == 0
        assert "hello from hook" in results[0]["stdout"]

    def test_failing_script_captured(self, tmp_path):
        hook_dir = tmp_path / "boot.d"
        hook_dir.mkdir(parents=True)
        script = hook_dir / "01-fail.sh"
        script.write_text("#!/bin/bash\necho 'error' >&2\nexit 1")
        script.chmod(stat.S_IRWXU)

        with patch.dict(os.environ, {"ROBOTOCORE_INIT_DIR": str(tmp_path)}):
            results = run_init_hooks("boot")

        assert len(results) == 1
        assert results[0]["returncode"] == 1
        assert "error" in results[0]["stderr"]

    def test_scripts_run_in_sorted_order(self, tmp_path):
        hook_dir = tmp_path / "boot.d"
        hook_dir.mkdir(parents=True)
        for name in ["03-third.sh", "01-first.sh", "02-second.sh"]:
            script = hook_dir / name
            script.write_text(f"#!/bin/bash\necho '{name}'")
            script.chmod(stat.S_IRWXU)

        with patch.dict(os.environ, {"ROBOTOCORE_INIT_DIR": str(tmp_path)}):
            results = run_init_hooks("boot")

        assert len(results) == 3
        assert results[0]["script"] == "01-first.sh"
        assert results[1]["script"] == "02-second.sh"
        assert results[2]["script"] == "03-third.sh"

    def test_all_sh_files_run_including_underscored(self, tmp_path):
        """The hooks module runs ALL *.sh files, including underscore-prefixed ones."""
        hook_dir = tmp_path / "boot.d"
        hook_dir.mkdir(parents=True)
        helper = hook_dir / "_helper.sh"
        helper.write_text("#!/bin/bash\necho 'helper ran'")
        helper.chmod(stat.S_IRWXU)
        regular = hook_dir / "01-run.sh"
        regular.write_text("#!/bin/bash\necho 'running'")
        regular.chmod(stat.S_IRWXU)

        with patch.dict(os.environ, {"ROBOTOCORE_INIT_DIR": str(tmp_path)}):
            results = run_init_hooks("boot")

        assert len(results) == 2
        scripts = [r["script"] for r in results]
        assert "01-run.sh" in scripts
        assert "_helper.sh" in scripts

    def test_non_sh_files_skipped(self, tmp_path):
        hook_dir = tmp_path / "boot.d"
        hook_dir.mkdir(parents=True)
        # .py file should be skipped
        (hook_dir / "test.py").write_text("print('hi')")
        # .sh file should run
        sh = hook_dir / "01-test.sh"
        sh.write_text("#!/bin/bash\necho 'ok'")
        sh.chmod(stat.S_IRWXU)

        with patch.dict(os.environ, {"ROBOTOCORE_INIT_DIR": str(tmp_path)}):
            results = run_init_hooks("boot")

        assert len(results) == 1
        assert results[0]["script"] == "01-test.sh"

    def test_different_stages(self, tmp_path):
        for stage in ["boot", "ready", "shutdown"]:
            hook_dir = tmp_path / f"{stage}.d"
            hook_dir.mkdir(parents=True)
            script = hook_dir / f"01-{stage}.sh"
            script.write_text(f"#!/bin/bash\necho '{stage}'")
            script.chmod(stat.S_IRWXU)

        with patch.dict(os.environ, {"ROBOTOCORE_INIT_DIR": str(tmp_path)}):
            for stage in ["boot", "ready", "shutdown"]:
                results = run_init_hooks(stage)
                assert len(results) == 1
                assert stage in results[0]["stdout"]
