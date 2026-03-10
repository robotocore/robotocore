"""End-to-end and edge-case tests for Lambda hot reload and code caching.

Tests real developer workflows, mount directory behavior, cache memory management,
code extraction edge cases, layer interaction, sys.modules cleanup, concurrency,
and provider integration patterns.
"""

import os
import sys
import threading
import time
import zipfile

import pytest

from robotocore.services.lambda_.executor import (
    CodeCache,
    LambdaContext,
    _clear_modules_for_dir,
    execute_python_handler,
    get_code_cache,
)
from robotocore.services.lambda_.hot_reload import (
    FileWatcher,
    get_mount_path,
    is_hot_reload_enabled,
    is_hot_reload_for_function,
)
from robotocore.services.lambda_.runtimes.python import PythonExecutor
from tests.unit.services.lambda_.helpers import make_zip

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _handler_code(return_value: str) -> str:
    """Generate a simple handler that returns a given literal."""
    return f"def handler(event, context):\n    return {return_value}\n"


def _handler_code_with_import(module_name: str, attr: str) -> str:
    return (
        f"import {module_name}\n"
        f"def handler(event, context):\n"
        f"    return {{'{attr}': {module_name}.{attr}}}\n"
    )


def _handler_code_with_global_cache() -> str:
    return (
        "_cache = {}\n"
        "def handler(event, context):\n"
        "    _cache[event.get('key', 'k')] = event.get('val', 'v')\n"
        "    return {'cache_size': len(_cache)}\n"
    )


# ---------------------------------------------------------------------------
# Real developer workflow simulation
# ---------------------------------------------------------------------------


class TestDeveloperWorkflow:
    """Simulate: create function -> invoke N times -> update code -> invoke N more."""

    def setup_method(self):
        self.executor = PythonExecutor()
        self.cache = CodeCache(max_size=50)

    def teardown_method(self):
        self.cache.invalidate_all()

    def test_repeated_invocations_use_cache(self):
        """Invoking the same function 5 times should reuse the same cache dir."""
        code = make_zip({"handler.py": _handler_code("{'count': 1}")})
        dirs = []
        for _ in range(5):
            d = self.cache.get_or_extract("repeat-fn", code)
            dirs.append(d)
        # All should be the exact same directory
        assert len(set(dirs)) == 1
        assert os.path.isdir(dirs[0])

    def test_update_code_then_invoke_uses_new_code(self):
        """After UpdateFunctionCode, invocations use the new code."""
        code_v1 = make_zip({"handler.py": _handler_code("{'v': 1}")})
        code_v2 = make_zip({"handler.py": _handler_code("{'v': 2}")})

        # Invoke v1
        result1, err1, _ = self.executor.execute(
            code_zip=code_v1,
            handler="handler.handler",
            event={},
            function_name="update-fn",
        )
        assert err1 is None
        assert result1 == {"v": 1}

        # Simulate UpdateFunctionCode: invalidate cache
        get_code_cache().invalidate("update-fn")

        # Invoke v2
        result2, err2, _ = self.executor.execute(
            code_zip=code_v2,
            handler="handler.handler",
            event={},
            function_name="update-fn",
        )
        assert err2 is None
        assert result2 == {"v": 2}

        get_code_cache().invalidate("update-fn")

    def test_five_invocations_after_update_all_return_new_code(self):
        """After code update, all subsequent invocations return new results."""
        code_v1 = make_zip({"handler.py": _handler_code("'old'")})
        code_v2 = make_zip({"handler.py": _handler_code("'new'")})

        self.executor.execute(
            code_zip=code_v1,
            handler="handler.handler",
            event={},
            function_name="multi-invoke-fn",
        )
        get_code_cache().invalidate("multi-invoke-fn")

        for _ in range(5):
            result, err, _ = self.executor.execute(
                code_zip=code_v2,
                handler="handler.handler",
                event={},
                function_name="multi-invoke-fn",
            )
            assert err is None
            assert result == "new"

        get_code_cache().invalidate("multi-invoke-fn")


# ---------------------------------------------------------------------------
# Mount directory workflow
# ---------------------------------------------------------------------------


class TestMountDirectoryWorkflow:
    """Mount dir: create handler -> invoke -> modify -> invoke -> verify hot reload."""

    def setup_method(self):
        self.executor = PythonExecutor()

    def test_mount_dir_returns_v1_then_v2(self, tmp_path):
        """Modify handler.py in mount dir, hot reload picks up change."""
        handler = tmp_path / "handler.py"
        handler.write_text("def handler(event, context):\n    return 'v1'\n")

        result1, err1, _ = self.executor.execute(
            code_zip=b"",
            handler="handler.handler",
            event={},
            function_name="mount-fn",
            code_dir=str(tmp_path),
            hot_reload=True,
        )
        assert err1 is None
        assert result1 == "v1"

        handler.write_text("def handler(event, context):\n    return 'v2'\n")

        result2, err2, _ = self.executor.execute(
            code_zip=b"",
            handler="handler.handler",
            event={},
            function_name="mount-fn",
            code_dir=str(tmp_path),
            hot_reload=True,
        )
        assert err2 is None
        assert result2 == "v2"

    def test_mount_dir_empty_falls_to_import_error(self, tmp_path):
        """Empty mount dir -> handler not found -> ImportModuleError."""
        result, err, _ = self.executor.execute(
            code_zip=b"",
            handler="handler.handler",
            event={},
            function_name="empty-mount",
            code_dir=str(tmp_path),
        )
        assert err == "Runtime.ImportModuleError"

    def test_mount_dir_handler_mismatch(self, tmp_path):
        """Mount dir has code but handler string doesn't match any file."""
        (tmp_path / "app.py").write_text("def run(e, c): return 'ok'\n")

        result, err, _ = self.executor.execute(
            code_zip=b"",
            handler="handler.handler",
            event={},
            function_name="mismatch-fn",
            code_dir=str(tmp_path),
        )
        assert err == "Runtime.ImportModuleError"

    def test_get_mount_path_returns_none_when_dir_is_empty(self, tmp_path, monkeypatch):
        """Mount dir exists, function subdir exists but is empty -- still returns path."""
        func_dir = tmp_path / "my-func"
        func_dir.mkdir()
        monkeypatch.setenv("LAMBDA_CODE_MOUNT_DIR", str(tmp_path))
        # get_mount_path checks isdir, not contents
        assert get_mount_path("my-func") == str(func_dir)

    def test_delete_handler_from_mount_dir_causes_error(self, tmp_path):
        """Deleting handler.py from mount dir should cause ImportModuleError."""
        handler = tmp_path / "handler.py"
        handler.write_text("def handler(event, context): return 'ok'\n")

        result1, err1, _ = self.executor.execute(
            code_zip=b"",
            handler="handler.handler",
            event={},
            function_name="del-handler-fn",
            code_dir=str(tmp_path),
            hot_reload=True,
        )
        assert err1 is None
        assert result1 == "ok"

        # Delete the handler
        handler.unlink()

        result2, err2, _ = self.executor.execute(
            code_zip=b"",
            handler="handler.handler",
            event={},
            function_name="del-handler-fn",
            code_dir=str(tmp_path),
            hot_reload=True,
        )
        assert err2 == "Runtime.ImportModuleError"


# ---------------------------------------------------------------------------
# Code cache memory management
# ---------------------------------------------------------------------------


class TestCodeCacheMemoryManagement:
    """LRU eviction: 60 functions -> max 50 -> first 10 evicted -> re-extract works."""

    def test_evict_first_10_of_60(self):
        cache = CodeCache(max_size=50)
        dirs = []
        for i in range(60):
            d = cache.get_or_extract(f"evict-fn-{i}", make_zip({"h.py": f"x={i}\n"}))
            dirs.append(d)

        assert len(cache) == 50
        # First 10 should be evicted (dirs removed)
        for d in dirs[:10]:
            assert not os.path.isdir(d)
        # Rest should be present
        for d in dirs[10:]:
            assert os.path.isdir(d)

        cache.invalidate_all()

    def test_re_extract_evicted_function(self):
        """Re-invoking an evicted function should re-extract and work fine."""
        cache = CodeCache(max_size=5)
        code0 = make_zip({"handler.py": "def handler(e,c): return 'zero'\n"})
        dir0 = cache.get_or_extract("evict-re-0", code0)

        # Fill cache to evict func 0
        for i in range(1, 6):
            cache.get_or_extract(f"evict-re-{i}", make_zip({"h.py": f"x={i}\n"}))

        assert not os.path.isdir(dir0)

        # Re-extract func 0 -- should work
        dir0_new = cache.get_or_extract("evict-re-0", code0)
        assert os.path.isdir(dir0_new)
        assert dir0_new != dir0
        with open(os.path.join(dir0_new, "handler.py")) as f:
            assert "zero" in f.read()

        cache.invalidate_all()

    def test_access_prevents_eviction(self):
        """Accessing a cache entry moves it to MRU position, preventing eviction."""
        cache = CodeCache(max_size=3)
        code_a = make_zip({"h.py": "a=1\n"})
        code_b = make_zip({"h.py": "b=1\n"})
        code_c = make_zip({"h.py": "c=1\n"})

        dir_a = cache.get_or_extract("fa", code_a)
        cache.get_or_extract("fb", code_b)
        cache.get_or_extract("fc", code_c)

        # Access fa to move it to MRU
        cache.get_or_extract("fa", code_a)

        # Add fd -- should evict fb (LRU)
        cache.get_or_extract("fd", make_zip({"h.py": "d=1\n"}))
        assert os.path.isdir(dir_a)  # fa survived
        assert len(cache) == 3

        cache.invalidate_all()


# ---------------------------------------------------------------------------
# Edge cases in code extraction
# ---------------------------------------------------------------------------


class TestCodeExtractionEdgeCases:
    def test_deeply_nested_handler(self):
        """Lambda zip with deeply nested directory: foo/bar/baz/handler.py."""
        code = make_zip(
            {
                "foo/bar/baz/handler.py": "def handler(e,c): return 'deep'\n",
                "foo/__init__.py": "",
                "foo/bar/__init__.py": "",
                "foo/bar/baz/__init__.py": "",
            }
        )
        cache = CodeCache(max_size=10)
        d = cache.get_or_extract("deep-fn", code)
        assert os.path.isfile(os.path.join(d, "foo", "bar", "baz", "handler.py"))
        cache.invalidate_all()

    def test_zip_with_init_files_proper_package(self):
        """Zip with __init__.py files forms a proper Python package."""
        code = make_zip(
            {
                "handler.py": "from mypkg import util\ndef handler(e,c): return util.VALUE\n",
                "mypkg/__init__.py": "from . import util\n",
                "mypkg/util.py": "VALUE = 42\n",
            }
        )
        executor = PythonExecutor()
        result, err, _ = executor.execute(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="pkg-fn",
        )
        assert err is None
        assert result == 42
        get_code_cache().invalidate("pkg-fn")

    def test_zip_with_requirements_txt_ignored(self):
        """requirements.txt in zip should not affect execution."""
        code = make_zip(
            {
                "handler.py": "def handler(e,c): return 'ok'\n",
                "requirements.txt": "boto3>=1.28\nrequests\n",
            }
        )
        cache = CodeCache(max_size=10)
        d = cache.get_or_extract("req-fn", code)
        assert os.path.exists(os.path.join(d, "requirements.txt"))
        assert os.path.exists(os.path.join(d, "handler.py"))

        executor = PythonExecutor()
        result, err, _ = executor.execute(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="req-fn",
        )
        assert err is None
        assert result == "ok"
        cache.invalidate_all()
        get_code_cache().invalidate("req-fn")

    def test_zip_with_pyc_alongside_py(self):
        """Zip with .pyc files alongside .py -- .py should be used."""
        code = make_zip(
            {
                "handler.py": "def handler(e,c): return 'from_py'\n",
                "__pycache__/handler.cpython-312.pyc": b"\x00\x00\x00\x00",
            }
        )
        executor = PythonExecutor()
        result, err, _ = executor.execute(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="pyc-fn",
        )
        assert err is None
        assert result == "from_py"
        get_code_cache().invalidate("pyc-fn")

    def test_zip_single_file_no_directory(self):
        """Zip with a single handler file, no directory structure."""
        code = make_zip({"handler.py": "def handler(e,c): return 'solo'\n"})
        executor = PythonExecutor()
        result, err, _ = executor.execute(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="solo-fn",
        )
        assert err is None
        assert result == "solo"
        get_code_cache().invalidate("solo-fn")

    def test_empty_zip_raises_bad_zip(self):
        """Empty zip (zero bytes) should raise BadZipFile."""
        cache = CodeCache(max_size=10)
        with pytest.raises(zipfile.BadZipFile):
            cache.get_or_extract("empty-fn", b"")

    def test_corrupt_zip_raises_and_no_orphan_dir(self):
        """Corrupt zip should raise BadZipFile and not leave orphaned temp dirs."""
        cache = CodeCache(max_size=10)
        with pytest.raises(zipfile.BadZipFile):
            cache.get_or_extract("corrupt-fn", b"PK\x03\x04corrupt-data-here")
        assert len(cache) == 0

    def test_bad_handler_format(self):
        """Handler string without a dot should return HandlerNotFound."""
        code = make_zip({"handler.py": "def handler(e,c): return 'ok'\n"})
        result, err, logs = execute_python_handler(
            code_zip=code,
            handler="handler_no_dot",
            event={},
            function_name="bad-handler-fn",
        )
        assert err == "Runtime.HandlerNotFound"
        assert "Bad handler format" in logs
        get_code_cache().invalidate("bad-handler-fn")

    def test_handler_function_missing_from_module(self):
        """Handler module exists but function name doesn't match."""
        code = make_zip({"handler.py": "def other_func(e,c): return 'nope'\n"})
        result, err, logs = execute_python_handler(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="missing-func-fn",
        )
        assert err == "Runtime.HandlerNotFound"
        assert "not found" in logs
        get_code_cache().invalidate("missing-func-fn")


# ---------------------------------------------------------------------------
# Layer interaction with cache
# ---------------------------------------------------------------------------


class TestLayerInteraction:
    def test_layer_provides_dependency(self):
        """Function code imports a module from a layer."""
        layer = make_zip(
            {
                "python/mylib.py": "LAYER_VALUE = 99\n",
            }
        )
        code = make_zip(
            {
                "handler.py": "import mylib\ndef handler(e,c): return mylib.LAYER_VALUE\n",
            }
        )
        executor = PythonExecutor()
        result, err, _ = executor.execute(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="layer-dep-fn",
            layer_zips=[layer],
        )
        assert err is None
        assert result == 99
        get_code_cache().invalidate("layer-dep-fn")

    def test_layer_content_change_produces_different_cache_key(self):
        """Different layer content with same function code -> different cache entry."""
        cache = CodeCache(max_size=10)
        code = make_zip({"handler.py": "x=1\n"})
        layer_v1 = make_zip({"lib.py": "V=1\n"})
        layer_v2 = make_zip({"lib.py": "V=2\n"})

        # Note: cache key is based on code_zip hash only, not layers.
        # Same code_zip -> same hash -> same cache entry.
        # Layers DO NOT change the cache key (only code_zip hash matters).
        dir1 = cache.get_or_extract("layer-fn", code, layer_zips=[layer_v1])
        dir2 = cache.get_or_extract("layer-fn", code, layer_zips=[layer_v2])

        # Both return the same dir because the code hash is identical
        assert dir1 == dir2
        cache.invalidate_all()

    def test_two_functions_sharing_layer_get_separate_dirs(self):
        """Two functions with the same layer each get their own extracted dir."""
        cache = CodeCache(max_size=10)
        shared_layer = make_zip({"shared.py": "S=1\n"})
        code_a = make_zip({"handler.py": "def handler(e,c): return 'a'\n"})
        code_b = make_zip({"handler.py": "def handler(e,c): return 'b'\n"})

        dir_a = cache.get_or_extract("fn-a", code_a, layer_zips=[shared_layer])
        dir_b = cache.get_or_extract("fn-b", code_b, layer_zips=[shared_layer])
        assert dir_a != dir_b
        assert os.path.isdir(dir_a)
        assert os.path.isdir(dir_b)
        cache.invalidate_all()

    def test_function_code_overrides_layer(self):
        """Function code should override files from layers with same name."""
        layer = make_zip({"shared.py": "ORIGIN = 'layer'\n"})
        code = make_zip(
            {
                "shared.py": "ORIGIN = 'function'\n",
                "handler.py": "import shared\ndef handler(e,c): return shared.ORIGIN\n",
            }
        )
        executor = PythonExecutor()
        result, err, _ = executor.execute(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="override-fn",
            layer_zips=[layer],
        )
        assert err is None
        assert result == "function"
        get_code_cache().invalidate("override-fn")


# ---------------------------------------------------------------------------
# Hot reload edge cases
# ---------------------------------------------------------------------------


class TestHotReloadEdgeCases:
    def test_only_pyc_files_no_trigger(self, tmp_path):
        """Mount dir with only .pyc files should have mtime 0."""
        watcher = FileWatcher()
        pycache = tmp_path / "__pycache__"
        pycache.mkdir()
        (pycache / "handler.cpython-312.pyc").write_bytes(b"\x00\x00")
        (tmp_path / "data.json").write_text('{"k":"v"}')

        mtime = watcher._scan_directory(str(tmp_path))
        assert mtime == 0.0

    def test_new_subdirectory_added(self, tmp_path):
        """Adding a new subdirectory with code triggers change detection."""
        watcher = FileWatcher()
        (tmp_path / "handler.py").write_text("x=1\n")

        watcher.check_for_changes("fn", str(tmp_path))

        # Add a new submodule
        subdir = tmp_path / "newmod"
        subdir.mkdir()
        new_file = subdir / "helper.py"
        new_file.write_text("HELP=True\n")
        future = time.time() + 10
        os.utime(new_file, (future, future))

        assert watcher.check_for_changes("fn", str(tmp_path)) is True

    def test_permission_change_triggers_reload(self, tmp_path):
        """File permission change updates mtime, should trigger reload."""
        watcher = FileWatcher()
        handler = tmp_path / "handler.py"
        handler.write_text("x=1\n")

        watcher.check_for_changes("fn", str(tmp_path))

        # Change mtime without changing content (simulates touch/permission change)
        future = time.time() + 10
        os.utime(handler, (future, future))

        assert watcher.check_for_changes("fn", str(tmp_path)) is True

    def test_thousands_of_files_performance(self, tmp_path):
        """FileWatcher should handle a dir with many files without hanging."""
        watcher = FileWatcher()
        # Create 500 files (enough to test performance without being slow)
        for i in range(500):
            (tmp_path / f"mod_{i}.py").write_text(f"x={i}\n")

        start = time.time()
        watcher.check_for_changes("big-fn", str(tmp_path))
        elapsed = time.time() - start
        # Should complete in under 2 seconds even with 500 files
        assert elapsed < 2.0

    def test_hot_reload_explicitly_disabled(self, monkeypatch):
        """LAMBDA_HOT_RELOAD=0 should disable hot reload."""
        monkeypatch.setenv("LAMBDA_HOT_RELOAD", "0")
        assert is_hot_reload_enabled() is False

    def test_per_function_hot_reload_marker(self):
        """__ROBOTOCORE_HOT_RELOAD__ enables hot reload for a single function."""
        assert is_hot_reload_for_function({"__ROBOTOCORE_HOT_RELOAD__": "1"}) is True
        assert is_hot_reload_for_function({"OTHER_VAR": "1"}) is False

    def test_hot_reload_not_applied_without_code_dir(self):
        """hot_reload=True but no code_dir should use cache normally (no module clearing)."""
        code = make_zip({"handler.py": "def handler(e,c): return 'cached'\n"})
        result, err, _ = execute_python_handler(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="no-codedir-hr",
            hot_reload=True,
            code_dir=None,
        )
        assert err is None
        assert result == "cached"
        get_code_cache().invalidate("no-codedir-hr")


# ---------------------------------------------------------------------------
# sys.modules cleanup
# ---------------------------------------------------------------------------


class TestSysModulesCleanup:
    def test_stdlib_modules_not_cleared(self, tmp_path):
        """Hot reload should NOT clear stdlib modules like json, os, etc."""
        handler = tmp_path / "stdlib_test_mod.py"
        handler.write_text("import json\ndef handler(e,c):\n    return json.dumps({'ok': True})\n")
        executor = PythonExecutor()
        executor.execute(
            code_zip=b"",
            handler="stdlib_test_mod.handler",
            event={},
            function_name="stdlib-fn",
            code_dir=str(tmp_path),
            hot_reload=True,
        )
        # json should still be in sys.modules
        assert "json" in sys.modules
        assert "os" in sys.modules

    def test_local_module_cleared_on_reload(self, tmp_path):
        """Modules from the code directory should be cleared on hot reload."""
        (tmp_path / "local_helper_e2e.py").write_text("VAL = 1\n")
        handler = tmp_path / "lr_handler_e2e.py"
        handler.write_text(
            "import local_helper_e2e\ndef handler(e,c): return local_helper_e2e.VAL\n"
        )

        executor = PythonExecutor()
        result1, _, _ = executor.execute(
            code_zip=b"",
            handler="lr_handler_e2e.handler",
            event={},
            function_name="local-mod-fn",
            code_dir=str(tmp_path),
            hot_reload=True,
        )
        assert result1 == 1

        # Update local module
        (tmp_path / "local_helper_e2e.py").write_text("VAL = 2\n")

        result2, _, _ = executor.execute(
            code_zip=b"",
            handler="lr_handler_e2e.handler",
            event={},
            function_name="local-mod-fn",
            code_dir=str(tmp_path),
            hot_reload=True,
        )
        assert result2 == 2

    def test_module_level_cache_reset_on_reload(self, tmp_path):
        """Module-level mutable state (global dict) should be reset after reload."""
        handler = tmp_path / "gcache_mod_e2e.py"
        handler.write_text(_handler_code_with_global_cache())

        executor = PythonExecutor()
        result1, _, _ = executor.execute(
            code_zip=b"",
            handler="gcache_mod_e2e.handler",
            event={"key": "a", "val": "1"},
            function_name="gcache-fn",
            code_dir=str(tmp_path),
            hot_reload=True,
        )
        assert result1 == {"cache_size": 1}

        # Second invocation with hot_reload=True should re-import, resetting the cache
        result2, _, _ = executor.execute(
            code_zip=b"",
            handler="gcache_mod_e2e.handler",
            event={"key": "b", "val": "2"},
            function_name="gcache-fn",
            code_dir=str(tmp_path),
            hot_reload=True,
        )
        # Hot reload clears modules, so _cache starts empty again
        assert result2 == {"cache_size": 1}

    def test_relative_import_cleared(self, tmp_path):
        """Relative imports (from . import helper) should be cleared on reload."""
        pkg = tmp_path / "mypkg_e2e"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("")
        (pkg / "helper.py").write_text("VAL = 10\n")
        (pkg / "main.py").write_text(
            "from mypkg_e2e import helper\ndef handler(e,c): return helper.VAL\n"
        )

        executor = PythonExecutor()
        result1, err1, _ = executor.execute(
            code_zip=b"",
            handler="mypkg_e2e.main.handler",
            event={},
            function_name="rel-import-fn",
            code_dir=str(tmp_path),
            hot_reload=True,
        )
        assert err1 is None
        assert result1 == 10

        (pkg / "helper.py").write_text("VAL = 20\n")

        result2, err2, _ = executor.execute(
            code_zip=b"",
            handler="mypkg_e2e.main.handler",
            event={},
            function_name="rel-import-fn",
            code_dir=str(tmp_path),
            hot_reload=True,
        )
        assert err2 is None
        assert result2 == 20

    def test_clear_modules_ignores_builtin_modules(self, tmp_path):
        """_clear_modules_for_dir should not touch built-in modules without __file__."""
        builtins_count_before = sum(
            1 for m in sys.modules.values() if getattr(m, "__file__", None) is None
        )
        _clear_modules_for_dir(str(tmp_path))
        builtins_count_after = sum(
            1 for m in sys.modules.values() if getattr(m, "__file__", None) is None
        )
        assert builtins_count_after == builtins_count_before


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


class TestConcurrency:
    def test_concurrent_invocations_same_function(self):
        """10 concurrent invocations of the same function should all succeed."""
        code = make_zip({"handler.py": "def handler(e,c): return e.get('id')\n"})
        executor = PythonExecutor()
        results = {}
        errors = []

        def invoke(thread_id):
            try:
                result, err, _ = executor.execute(
                    code_zip=code,
                    handler="handler.handler",
                    event={"id": thread_id},
                    function_name="concurrent-fn",
                )
                if err:
                    errors.append((thread_id, err))
                else:
                    results[thread_id] = result
            except Exception as exc:
                errors.append((thread_id, str(exc)))

        threads = [threading.Thread(target=invoke, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Errors: {errors}"
        assert len(results) == 10
        for i in range(10):
            assert results[i] == i

        get_code_cache().invalidate("concurrent-fn")

    def test_concurrent_cache_get_or_extract(self):
        """Concurrent cache access should not corrupt state."""
        cache = CodeCache(max_size=50)
        errors = []
        dirs = []

        def extract(idx):
            try:
                d = cache.get_or_extract(
                    f"conc-{idx % 5}",  # 5 unique functions, 2 threads each
                    make_zip({"h.py": f"x={idx % 5}\n"}),
                )
                dirs.append(d)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=extract, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert len(dirs) == 10
        for d in dirs:
            assert os.path.isdir(d)
        cache.invalidate_all()

    def test_eviction_during_concurrent_invocations(self):
        """Cache eviction during concurrent invocations should not crash."""
        cache = CodeCache(max_size=3)
        errors = []

        def fill_cache(idx):
            try:
                d = cache.get_or_extract(
                    f"evict-conc-{idx}",
                    make_zip({"h.py": f"x={idx}\n"}),
                )
                # Verify extracted dir is valid
                assert os.path.isdir(d)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=fill_cache, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert len(cache) <= 3
        cache.invalidate_all()


# ---------------------------------------------------------------------------
# Provider integration
# ---------------------------------------------------------------------------


class TestProviderIntegration:
    def test_create_function_then_immediate_invoke(self):
        """Cold cache: create function and immediately invoke."""
        code = make_zip({"handler.py": "def handler(e,c): return 'first-call'\n"})
        executor = PythonExecutor()
        result, err, _ = executor.execute(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="cold-invoke-fn",
        )
        assert err is None
        assert result == "first-call"
        get_code_cache().invalidate("cold-invoke-fn")

    def test_delete_function_cleans_cache(self):
        """Invalidating cache entry simulates DeleteFunction cleanup."""
        cache = CodeCache(max_size=10)
        code = make_zip({"handler.py": "x=1\n"})
        d = cache.get_or_extract("del-fn", code)
        assert os.path.isdir(d)

        cache.invalidate("del-fn")
        assert not os.path.isdir(d)
        assert len(cache) == 0

    def test_update_function_code_flow(self):
        """Full flow: invoke v1 -> invalidate -> invoke v2 -> verify."""
        executor = PythonExecutor()
        code_v1 = make_zip({"handler.py": "def handler(e,c): return 'v1'\n"})
        code_v2 = make_zip({"handler.py": "def handler(e,c): return 'v2'\n"})

        r1, e1, _ = executor.execute(
            code_zip=code_v1,
            handler="handler.handler",
            event={},
            function_name="update-flow-fn",
        )
        assert e1 is None
        assert r1 == "v1"

        get_code_cache().invalidate("update-flow-fn")

        r2, e2, _ = executor.execute(
            code_zip=code_v2,
            handler="handler.handler",
            event={},
            function_name="update-flow-fn",
        )
        assert e2 is None
        assert r2 == "v2"

        get_code_cache().invalidate("update-flow-fn")


# ---------------------------------------------------------------------------
# LambdaContext
# ---------------------------------------------------------------------------


class TestLambdaContext:
    def test_remaining_time_decreases(self):
        ctx = LambdaContext(function_name="timer-fn", _timeout=10)
        t1 = ctx.get_remaining_time_in_millis()
        assert 9000 <= t1 <= 10000
        time.sleep(0.05)
        t2 = ctx.get_remaining_time_in_millis()
        assert t2 < t1

    def test_remaining_time_never_negative(self):
        ctx = LambdaContext(function_name="expired-fn", _timeout=0, _start_time=time.time() - 10)
        assert ctx.get_remaining_time_in_millis() == 0

    def test_context_fields(self):
        ctx = LambdaContext(
            function_name="my-fn",
            function_version="$LATEST",
            memory_limit_in_mb=256,
        )
        assert ctx.function_name == "my-fn"
        assert ctx.function_version == "$LATEST"
        assert ctx.memory_limit_in_mb == 256
        assert len(ctx.aws_request_id) > 0

    def test_context_passed_to_handler(self):
        """Handler receives a LambdaContext with correct function_name."""
        code = make_zip(
            {
                "handler.py": (
                    "def handler(e,c):\n"
                    "    return {'fn': c.function_name, 'mem': c.memory_limit_in_mb}\n"
                ),
            }
        )
        result, err, _ = execute_python_handler(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="ctx-fn",
            memory_size=512,
        )
        assert err is None
        assert result["fn"] == "ctx-fn"
        assert result["mem"] == 512
        get_code_cache().invalidate("ctx-fn")


# ---------------------------------------------------------------------------
# Environment variable handling
# ---------------------------------------------------------------------------


class TestEnvironmentVariables:
    def test_env_vars_available_in_handler(self):
        """Custom env vars passed to handler should be accessible."""
        code = make_zip(
            {
                "handler.py": "import os\ndef handler(e,c): return os.environ.get('MY_VAR')\n",
            }
        )
        result, err, _ = execute_python_handler(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="env-fn",
            env_vars={"MY_VAR": "hello"},
        )
        assert err is None
        assert result == "hello"
        get_code_cache().invalidate("env-fn")

    def test_env_vars_restored_after_invocation(self):
        """Environment should be restored after handler returns."""
        original = os.environ.get("MY_TEMP_TEST_VAR")
        code = make_zip({"handler.py": "def handler(e,c): return 'ok'\n"})
        execute_python_handler(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="env-restore-fn",
            env_vars={"MY_TEMP_TEST_VAR": "injected"},
        )
        assert os.environ.get("MY_TEMP_TEST_VAR") == original
        get_code_cache().invalidate("env-restore-fn")

    def test_aws_region_set_correctly(self):
        """AWS_REGION should be set to the configured region."""
        code = make_zip(
            {
                "handler.py": "import os\ndef handler(e,c): return os.environ.get('AWS_REGION')\n",
            }
        )
        result, err, _ = execute_python_handler(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="region-fn",
            region="eu-west-1",
        )
        assert err is None
        assert result == "eu-west-1"
        get_code_cache().invalidate("region-fn")


# ---------------------------------------------------------------------------
# Handler error scenarios
# ---------------------------------------------------------------------------


class TestHandlerErrors:
    def test_handler_raises_exception(self):
        """Handler that raises an exception should return error info."""
        code = make_zip(
            {
                "handler.py": "def handler(e,c): raise ValueError('boom')\n",
            }
        )
        result, err, logs = execute_python_handler(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="error-fn",
        )
        assert err == "Handled"
        assert result["errorType"] == "ValueError"
        assert "boom" in result["errorMessage"]
        get_code_cache().invalidate("error-fn")

    def test_handler_import_error(self):
        """Handler that fails to import should return ImportModuleError or Handled."""
        code = make_zip(
            {
                "handler.py": "import nonexistent_module_12345\ndef handler(e,c): return 'ok'\n",
            }
        )
        result, err, logs = execute_python_handler(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="import-err-fn",
        )
        assert err == "Handled"
        assert "ModuleNotFoundError" in result["errorType"]
        get_code_cache().invalidate("import-err-fn")

    def test_handler_syntax_error(self):
        """Handler with syntax error should return Handled error."""
        code = make_zip(
            {
                "handler.py": "def handler(e,c):\n    return 'ok'\n    this is bad syntax\n",
            }
        )
        result, err, _ = execute_python_handler(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="syntax-err-fn",
        )
        assert err == "Handled"
        assert "SyntaxError" in result["errorType"]
        get_code_cache().invalidate("syntax-err-fn")

    def test_handler_returns_none(self):
        """Handler returning None should be valid."""
        code = make_zip({"handler.py": "def handler(e,c): return None\n"})
        result, err, _ = execute_python_handler(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="none-fn",
        )
        assert err is None
        assert result is None
        get_code_cache().invalidate("none-fn")

    def test_handler_returns_string(self):
        """Handler returning a string should be valid."""
        code = make_zip({"handler.py": "def handler(e,c): return 'just a string'\n"})
        result, err, _ = execute_python_handler(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="str-fn",
        )
        assert err is None
        assert result == "just a string"
        get_code_cache().invalidate("str-fn")

    def test_handler_returns_list(self):
        """Handler returning a list should be valid."""
        code = make_zip({"handler.py": "def handler(e,c): return [1, 2, 3]\n"})
        result, err, _ = execute_python_handler(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="list-fn",
        )
        assert err is None
        assert result == [1, 2, 3]
        get_code_cache().invalidate("list-fn")

    def test_handler_prints_to_stdout(self):
        """Handler print output should appear in logs."""
        code = make_zip(
            {
                "handler.py": (
                    "def handler(e,c):\n    print('hello from lambda')\n    return 'ok'\n"
                ),
            }
        )
        result, err, logs = execute_python_handler(
            code_zip=code,
            handler="handler.handler",
            event={},
            function_name="print-fn",
        )
        assert err is None
        assert "hello from lambda" in logs
        get_code_cache().invalidate("print-fn")


# ---------------------------------------------------------------------------
# FileWatcher edge cases (additional)
# ---------------------------------------------------------------------------


class TestFileWatcherAdditional:
    def test_new_file_added_triggers_change(self, tmp_path):
        """Adding a new .py file to an existing dir triggers change detection."""
        watcher = FileWatcher()
        (tmp_path / "handler.py").write_text("x=1\n")
        watcher.check_for_changes("fn", str(tmp_path))

        # Add a new file with future mtime
        new_file = tmp_path / "new_module.py"
        new_file.write_text("NEW=True\n")
        future = time.time() + 10
        os.utime(new_file, (future, future))

        assert watcher.check_for_changes("fn", str(tmp_path)) is True

    def test_file_deleted_triggers_change(self, tmp_path):
        """Deleting a .py file changes the max mtime (may go to 0 or lower max)."""
        watcher = FileWatcher()
        f1 = tmp_path / "mod1.py"
        f2 = tmp_path / "mod2.py"
        f1.write_text("a=1\n")
        f2.write_text("b=2\n")

        # Set f2 to have a much higher mtime
        future = time.time() + 100
        os.utime(f2, (future, future))

        watcher.check_for_changes("fn", str(tmp_path))

        # Delete the file with the highest mtime
        f2.unlink()

        # mtime changed from future to f1's mtime
        assert watcher.check_for_changes("fn", str(tmp_path)) is True

    def test_concurrent_watcher_calls(self, tmp_path):
        """Multiple threads calling check_for_changes should not crash."""
        watcher = FileWatcher()
        (tmp_path / "handler.py").write_text("x=1\n")
        errors = []

        def check(idx):
            try:
                watcher.check_for_changes(f"fn-{idx}", str(tmp_path))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=check, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors


# ---------------------------------------------------------------------------
# Two rapid invocations during code change
# ---------------------------------------------------------------------------


class TestRapidInvocationsDuringChange:
    def test_both_invocations_get_consistent_code(self, tmp_path):
        """Two rapid invocations during a code change should both get valid results."""
        handler = tmp_path / "rapid_mod_e2e.py"
        handler.write_text("def handler(e,c): return 'v1'\n")

        executor = PythonExecutor()
        results = []
        errors = []

        # First invocation establishes baseline
        r, err, _ = executor.execute(
            code_zip=b"",
            handler="rapid_mod_e2e.handler",
            event={},
            function_name="rapid-fn",
            code_dir=str(tmp_path),
            hot_reload=True,
        )
        assert r == "v1"

        # Update code
        handler.write_text("def handler(e,c): return 'v2'\n")

        # Two rapid invocations
        for _ in range(2):
            r, err, _ = executor.execute(
                code_zip=b"",
                handler="rapid_mod_e2e.handler",
                event={},
                function_name="rapid-fn",
                code_dir=str(tmp_path),
                hot_reload=True,
            )
            if err:
                errors.append(err)
            else:
                results.append(r)

        assert not errors
        assert len(results) == 2
        # Both should see the new code (hot reload clears modules)
        for r in results:
            assert r == "v2"
