"""Tests for Lambda hot reload — mount paths and file change detection."""

import os
import time

import pytest

from robotocore.services.lambda_.hot_reload import (
    _WATCHED_EXTENSIONS,
    FileWatcher,
    get_mount_dir,
    get_mount_path,
    is_hot_reload_enabled,
    is_hot_reload_for_function,
)


class TestGetMountDir:
    def test_returns_none_when_not_set(self, monkeypatch):
        monkeypatch.delenv("LAMBDA_CODE_MOUNT_DIR", raising=False)
        assert get_mount_dir() is None

    def test_returns_dir_when_set(self, monkeypatch, tmp_path):
        monkeypatch.setenv("LAMBDA_CODE_MOUNT_DIR", str(tmp_path))
        assert get_mount_dir() == str(tmp_path)

    def test_returns_none_for_empty_string(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_CODE_MOUNT_DIR", "")
        assert get_mount_dir() is None


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

    def test_returns_none_when_path_is_file(self, monkeypatch, tmp_path):
        """A file (not directory) should not match."""
        (tmp_path / "myfunc").write_text("not a dir")
        monkeypatch.setenv("LAMBDA_CODE_MOUNT_DIR", str(tmp_path))
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

    def test_disabled_with_random_string(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_HOT_RELOAD", "maybe")
        assert is_hot_reload_enabled() is False

    def test_handles_whitespace(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_HOT_RELOAD", "  1  ")
        assert is_hot_reload_enabled() is True


class TestIsHotReloadForFunction:
    def test_false_when_no_env_vars(self):
        assert is_hot_reload_for_function(None) is False
        assert is_hot_reload_for_function({}) is False

    def test_true_when_marker_present(self):
        assert is_hot_reload_for_function({"__ROBOTOCORE_HOT_RELOAD__": "1"}) is True

    def test_false_when_marker_absent(self):
        assert is_hot_reload_for_function({"FOO": "bar"}) is False

    def test_marker_value_doesnt_matter(self):
        """Any value for the marker key enables hot reload."""
        assert is_hot_reload_for_function({"__ROBOTOCORE_HOT_RELOAD__": ""}) is True
        assert is_hot_reload_for_function({"__ROBOTOCORE_HOT_RELOAD__": "false"}) is True


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

        # First check -- baseline
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

    # --- New tests ---

    def test_detects_mtime_going_backwards(self, tmp_path):
        """Git checkout can set mtimes to older values -- should detect change."""
        watcher = FileWatcher()
        handler = tmp_path / "handler.py"
        handler.write_text("x = 1")

        # Set a future mtime as baseline
        future = time.time() + 100
        os.utime(handler, (future, future))
        assert watcher.check_for_changes("myfunc", str(tmp_path)) is False

        # Now set it to an older time (simulating git checkout)
        past = time.time() - 100
        os.utime(handler, (past, past))
        assert watcher.check_for_changes("myfunc", str(tmp_path)) is True

    def test_deeply_nested_directory(self, tmp_path):
        """FileWatcher should scan deeply nested directory structures."""
        deep = tmp_path / "a" / "b" / "c" / "d" / "e"
        deep.mkdir(parents=True)
        deep_file = deep / "handler.py"
        deep_file.write_text("deep = True")

        watcher = FileWatcher()
        assert watcher.check_for_changes("deep_func", str(tmp_path)) is False

        # Modify the deeply nested file
        future = time.time() + 10
        os.utime(deep_file, (future, future))
        assert watcher.check_for_changes("deep_func", str(tmp_path)) is True

    def test_deleted_mount_dir(self, tmp_path):
        """FileWatcher should handle a directory that gets deleted."""
        func_dir = tmp_path / "myfunc"
        func_dir.mkdir()
        (func_dir / "handler.py").write_text("x = 1")

        watcher = FileWatcher()
        assert watcher.check_for_changes("myfunc", str(func_dir)) is False

        # Delete the directory
        import shutil

        shutil.rmtree(func_dir)

        # Should return True (mtime changed from >0 to 0.0 because dir is gone)
        assert watcher.check_for_changes("myfunc", str(func_dir)) is True

    def test_multiple_functions_same_watcher(self, tmp_path):
        """Different functions are tracked independently."""
        watcher = FileWatcher()

        func_a = tmp_path / "func_a"
        func_a.mkdir()
        (func_a / "handler.py").write_text("a")

        func_b = tmp_path / "func_b"
        func_b.mkdir()
        (func_b / "handler.py").write_text("b")

        # Baseline for both
        watcher.check_for_changes("func_a", str(func_a))
        watcher.check_for_changes("func_b", str(func_b))

        # Modify only func_a
        future = time.time() + 10
        os.utime(func_a / "handler.py", (future, future))

        assert watcher.check_for_changes("func_a", str(func_a)) is True
        assert watcher.check_for_changes("func_b", str(func_b)) is False

    def test_ignores_pyc_files(self, tmp_path):
        """__pycache__ and .pyc files should not trigger changes."""
        watcher = FileWatcher()
        (tmp_path / "handler.py").write_text("x = 1")
        pycache = tmp_path / "__pycache__"
        pycache.mkdir()
        (pycache / "handler.cpython-312.pyc").write_bytes(b"\x00\x00")

        # Baseline
        watcher.check_for_changes("myfunc", str(tmp_path))

        # Touch only the pyc file
        future = time.time() + 10
        os.utime(pycache / "handler.cpython-312.pyc", (future, future))

        assert watcher.check_for_changes("myfunc", str(tmp_path)) is False

    def test_watches_all_code_extensions(self, tmp_path):
        """All file types in _WATCHED_EXTENSIONS should be scanned."""
        watcher = FileWatcher()
        for ext in _WATCHED_EXTENSIONS:
            (tmp_path / f"file{ext}").write_text(f"code for {ext}")

        mtime = watcher._scan_directory(str(tmp_path))
        assert mtime > 0

    def test_empty_directory(self, tmp_path):
        """Empty directory should have mtime 0.0."""
        watcher = FileWatcher()
        mtime = watcher._scan_directory(str(tmp_path))
        assert mtime == 0.0

    def test_nonexistent_directory(self):
        """Nonexistent directory should return mtime 0.0 without error."""
        watcher = FileWatcher()
        mtime = watcher._scan_directory("/nonexistent/path/that/does/not/exist")
        assert mtime == 0.0

    def test_invalidate_nonexistent_function(self):
        """Invalidating a non-tracked function should not raise."""
        watcher = FileWatcher()
        watcher.invalidate("never_tracked")  # Should not raise

    def test_symlink_to_file(self, tmp_path):
        """Symlinks to code files should be scanned."""
        real_file = tmp_path / "real_handler.py"
        real_file.write_text("real = True")
        link_file = tmp_path / "handler.py"
        link_file.symlink_to(real_file)

        watcher = FileWatcher()
        mtime = watcher._scan_directory(str(tmp_path))
        assert mtime > 0

    def test_broken_symlink_handled(self, tmp_path):
        """Broken symlinks should be silently skipped."""
        broken = tmp_path / "broken.py"
        broken.symlink_to(tmp_path / "nonexistent.py")

        watcher = FileWatcher()
        # Should not raise
        mtime = watcher._scan_directory(str(tmp_path))
        # Broken symlink should be skipped, so mtime stays 0
        assert mtime == 0.0

    def test_hidden_directories_skipped(self, tmp_path):
        """Hidden directories (starting with .) should be skipped."""
        watcher = FileWatcher()
        hidden = tmp_path / ".git"
        hidden.mkdir()
        (hidden / "config.py").write_text("internal")

        (tmp_path / "handler.py").write_text("visible")

        # Baseline
        watcher.check_for_changes("myfunc", str(tmp_path))

        # Modify only the hidden file
        future = time.time() + 10
        os.utime(hidden / "config.py", (future, future))

        assert watcher.check_for_changes("myfunc", str(tmp_path)) is False

    def test_rapid_successive_changes(self, tmp_path):
        """Rapid successive changes should all be detected."""
        watcher = FileWatcher()
        handler = tmp_path / "handler.py"
        handler.write_text("v0")

        watcher.check_for_changes("myfunc", str(tmp_path))

        # Make three rapid changes with increasing mtimes
        for i in range(1, 4):
            future = time.time() + (10 * i)
            handler.write_text(f"v{i}")
            os.utime(handler, (future, future))
            assert watcher.check_for_changes("myfunc", str(tmp_path)) is True

    def test_change_detection_after_rapid_no_change(self, tmp_path):
        """After several no-change checks, a real change is still detected."""
        watcher = FileWatcher()
        handler = tmp_path / "handler.py"
        handler.write_text("x = 1")

        watcher.check_for_changes("myfunc", str(tmp_path))

        # Multiple no-change checks
        for _ in range(5):
            assert watcher.check_for_changes("myfunc", str(tmp_path)) is False

        # Now make a change
        future = time.time() + 10
        handler.write_text("x = 2")
        os.utime(handler, (future, future))
        assert watcher.check_for_changes("myfunc", str(tmp_path)) is True
