"""Tests for Lambda hot reload — mount paths and file change detection."""

import os
import time

import pytest

from robotocore.services.lambda_.hot_reload import (
    FileWatcher,
    get_mount_path,
    is_hot_reload_enabled,
    is_hot_reload_for_function,
)


class TestGetMountPath:
    def test_returns_none_when_env_not_set(self, monkeypatch):
        monkeypatch.delenv("LAMBDA_CODE_MOUNT_DIR", raising=False)
        assert get_mount_path("myfunc") is None

    def test_returns_none_when_dir_missing(self, monkeypatch, tmp_path):
        monkeypatch.setenv("LAMBDA_CODE_MOUNT_DIR", str(tmp_path))
        assert get_mount_path("nonexistent_func") is None

    def test_returns_path_when_dir_exists(self, monkeypatch, tmp_path):
        func_dir = tmp_path / "myfunc"
        func_dir.mkdir()
        monkeypatch.setenv("LAMBDA_CODE_MOUNT_DIR", str(tmp_path))
        result = get_mount_path("myfunc")
        assert result == str(func_dir)

    def test_returns_none_for_empty_env(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_CODE_MOUNT_DIR", "")
        assert get_mount_path("myfunc") is None


class TestIsHotReloadEnabled:
    def test_disabled_by_default(self, monkeypatch):
        monkeypatch.delenv("LAMBDA_HOT_RELOAD", raising=False)
        assert is_hot_reload_enabled() is False

    def test_enabled_with_1(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_HOT_RELOAD", "1")
        assert is_hot_reload_enabled() is True

    def test_enabled_with_true(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_HOT_RELOAD", "true")
        assert is_hot_reload_enabled() is True

    def test_enabled_with_yes(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_HOT_RELOAD", "yes")
        assert is_hot_reload_enabled() is True

    def test_disabled_with_zero(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_HOT_RELOAD", "0")
        assert is_hot_reload_enabled() is False


class TestIsHotReloadForFunction:
    def test_false_when_no_env_vars(self):
        assert is_hot_reload_for_function(None) is False
        assert is_hot_reload_for_function({}) is False

    def test_true_when_marker_present(self):
        assert is_hot_reload_for_function({"__ROBOTOCORE_HOT_RELOAD__": "1"}) is True

    def test_false_when_marker_absent(self):
        assert is_hot_reload_for_function({"FOO": "bar"}) is False


class TestFileWatcher:
    def test_first_check_returns_false(self, tmp_path):
        watcher = FileWatcher()
        handler = tmp_path / "handler.py"
        handler.write_text("x = 1")
        assert watcher.check_for_changes("myfunc", str(tmp_path)) is False

    def test_detects_mtime_change(self, tmp_path):
        watcher = FileWatcher()
        handler = tmp_path / "handler.py"
        handler.write_text("x = 1")

        # First check — baseline
        assert watcher.check_for_changes("myfunc", str(tmp_path)) is False

        # Modify file with a future mtime to ensure detection
        handler.write_text("x = 2")
        future_time = time.time() + 10
        os.utime(handler, (future_time, future_time))

        assert watcher.check_for_changes("myfunc", str(tmp_path)) is True

    def test_no_change_returns_false(self, tmp_path):
        watcher = FileWatcher()
        handler = tmp_path / "handler.py"
        handler.write_text("x = 1")

        watcher.check_for_changes("myfunc", str(tmp_path))
        assert watcher.check_for_changes("myfunc", str(tmp_path)) is False

    def test_scan_directory_recursive(self, tmp_path):
        watcher = FileWatcher()
        subdir = tmp_path / "pkg"
        subdir.mkdir()
        (subdir / "mod.py").write_text("y = 2")
        (tmp_path / "handler.py").write_text("x = 1")

        # Set known mtimes
        future = time.time() + 100
        os.utime(subdir / "mod.py", (future, future))

        mtime = watcher._scan_directory(str(tmp_path))
        assert mtime == pytest.approx(future, abs=1)

    def test_ignores_non_code_files(self, tmp_path):
        watcher = FileWatcher()
        (tmp_path / "data.txt").write_text("not code")
        (tmp_path / "image.png").write_bytes(b"\x89PNG")

        mtime = watcher._scan_directory(str(tmp_path))
        assert mtime == 0.0

    def test_watches_js_files(self, tmp_path):
        watcher = FileWatcher()
        js_file = tmp_path / "index.js"
        js_file.write_text("exports.handler = () => {}")

        mtime = watcher._scan_directory(str(tmp_path))
        assert mtime > 0

    def test_invalidate_clears_tracking(self, tmp_path):
        watcher = FileWatcher()
        handler = tmp_path / "handler.py"
        handler.write_text("x = 1")

        watcher.check_for_changes("myfunc", str(tmp_path))
        watcher.invalidate("myfunc")

        # After invalidation, first check returns False again (baseline)
        assert watcher.check_for_changes("myfunc", str(tmp_path)) is False
