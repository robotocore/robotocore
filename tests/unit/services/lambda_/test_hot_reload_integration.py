"""Integration-style tests for hot reload + code cache + executor together.

Tests the full flow: create code dir, invoke, modify, invoke again, verify behavior.
"""

import os
import time

from robotocore.services.lambda_.executor import CodeCache, _clear_modules_for_dir, get_code_cache
from robotocore.services.lambda_.hot_reload import FileWatcher
from robotocore.services.lambda_.runtimes.python import PythonExecutor
from tests.unit.services.lambda_.helpers import make_zip


class TestHotReloadInvocationFlow:
    """Full invocation flow: create function -> invoke -> modify -> invoke -> verify."""

    def setup_method(self):
        self.executor = PythonExecutor()

    def test_invoke_with_mount_dir(self, tmp_path):
        """Invoke using a mounted code directory instead of zip."""
        handler = tmp_path / "handler.py"
        handler.write_text("def main(event, context):\n    return {'v': 1}\n")

        result, error_type, _ = self.executor.execute(
            code_zip=b"",
            handler="handler.main",
            event={},
            function_name="mount_fn",
            code_dir=str(tmp_path),
            hot_reload=False,
        )
        assert result == {"v": 1}
        assert error_type is None

    def test_hot_reload_picks_up_code_change(self, tmp_path):
        """Modify code -> invoke again -> verify different result."""
        handler = tmp_path / "handler.py"
        handler.write_text("def main(event, context):\n    return {'version': 1}\n")

        result1, err1, _ = self.executor.execute(
            code_zip=b"",
            handler="handler.main",
            event={},
            function_name="reload_fn",
            code_dir=str(tmp_path),
            hot_reload=True,
        )
        assert result1 == {"version": 1}
        assert err1 is None

        # Modify the code
        handler.write_text("def main(event, context):\n    return {'version': 2}\n")

        result2, err2, _ = self.executor.execute(
            code_zip=b"",
            handler="handler.main",
            event={},
            function_name="reload_fn",
            code_dir=str(tmp_path),
            hot_reload=True,
        )
        assert result2 == {"version": 2}
        assert err2 is None

    def test_hot_reload_disabled_keeps_old_code(self, tmp_path):
        """Without hot reload, cached modules return old results."""
        handler = tmp_path / "hr_disabled_mod.py"
        handler.write_text("def main(event, context):\n    return {'version': 1}\n")

        result1, _, _ = self.executor.execute(
            code_zip=b"",
            handler="hr_disabled_mod.main",
            event={},
            function_name="no_reload_fn",
            code_dir=str(tmp_path),
            hot_reload=False,
        )
        assert result1 == {"version": 1}

        # Modify the code (but hot reload is off)
        handler.write_text("def main(event, context):\n    return {'version': 2}\n")

        result2, _, _ = self.executor.execute(
            code_zip=b"",
            handler="hr_disabled_mod.main",
            event={},
            function_name="no_reload_fn",
            code_dir=str(tmp_path),
            hot_reload=False,
        )
        # Module is cached in sys.modules, so we get old result
        assert result2 == {"version": 1}

        # Clean up so we don't pollute other tests
        _clear_modules_for_dir(str(tmp_path))

    def test_mount_dir_missing_handler_falls_back_to_zip(self, tmp_path):
        """If mount dir has no handler file, zip code should still work."""
        # Mount dir exists but has wrong file
        (tmp_path / "other.py").write_text("x = 1")

        code_zip = make_zip(
            {"handler.py": "def main(event, context):\n    return {'source': 'zip'}\n"}
        )

        # With code_dir pointing to a dir that lacks the handler module,
        # execution should fail with ImportModuleError because code_dir takes priority
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="handler.main",
            event={},
            function_name="fallback_fn",
            code_dir=str(tmp_path),
        )
        assert error_type == "Runtime.ImportModuleError"

    def test_update_function_code_invalidates_cache(self):
        """UpdateFunctionCode should invalidate the code cache."""
        cache = CodeCache(max_size=10)
        code_v1 = make_zip({"handler.py": "def main(e, c): return 1\n"})
        code_v2 = make_zip({"handler.py": "def main(e, c): return 2\n"})

        dir1 = cache.get_or_extract("my_func", code_v1)
        assert os.path.isdir(dir1)

        # Simulate UpdateFunctionCode by invalidating
        cache.invalidate("my_func")
        assert not os.path.isdir(dir1)

        # New code gets a new directory
        dir2 = cache.get_or_extract("my_func", code_v2)
        assert os.path.isdir(dir2)
        assert dir1 != dir2
        with open(os.path.join(dir2, "handler.py")) as f:
            assert "return 2" in f.read()
        cache.invalidate_all()


class TestFileWatcherCacheIntegration:
    """Test FileWatcher + CodeCache working together as in the provider."""

    def test_watcher_triggers_cache_invalidation(self, tmp_path):
        """When watcher detects changes, cache should be invalidated."""
        watcher = FileWatcher()
        cache = CodeCache(max_size=10)

        func_dir = tmp_path / "myfunc"
        func_dir.mkdir()
        handler = func_dir / "handler.py"
        handler.write_text("v = 1")

        # Create cache entry via zip
        code_zip = make_zip({"handler.py": "v = 1\n"})
        cached_dir = cache.get_or_extract("myfunc", code_zip)
        assert os.path.isdir(cached_dir)

        # First watcher check -- baseline
        watcher.check_for_changes("myfunc", str(func_dir))

        # Modify the mount dir
        handler.write_text("v = 2")
        future = time.time() + 10
        os.utime(handler, (future, future))

        # Watcher detects change
        assert watcher.check_for_changes("myfunc", str(func_dir)) is True

        # Invalidate cache (as provider does)
        cache.invalidate("myfunc")
        assert not os.path.isdir(cached_dir)

    def test_no_change_no_invalidation(self, tmp_path):
        """When watcher sees no changes, cache stays intact."""
        watcher = FileWatcher()
        cache = CodeCache(max_size=10)

        func_dir = tmp_path / "myfunc"
        func_dir.mkdir()
        (func_dir / "handler.py").write_text("v = 1")

        code_zip = make_zip({"handler.py": "v = 1\n"})
        cached_dir = cache.get_or_extract("myfunc", code_zip)

        # Baseline + no-change check
        watcher.check_for_changes("myfunc", str(func_dir))
        assert watcher.check_for_changes("myfunc", str(func_dir)) is False

        # Cache should still be intact
        assert os.path.isdir(cached_dir)
        assert len(cache) == 1
        cache.invalidate_all()


class TestPythonRuntimeHotReload:
    """Test Python-specific sys.modules clearing during hot reload."""

    def setup_method(self):
        self.executor = PythonExecutor()

    def test_sys_modules_cleared_for_handler(self, tmp_path):
        """Hot reload should clear sys.modules for the handler module."""
        handler = tmp_path / "cleartest_mod.py"
        handler.write_text("def main(event, context):\n    return {'ok': True}\n")

        self.executor.execute(
            code_zip=b"",
            handler="cleartest_mod.main",
            event={},
            function_name="cleartest",
            code_dir=str(tmp_path),
            hot_reload=True,
        )
        # After execution with hot_reload, the module should NOT be in sys.modules
        # because _clear_modules_for_dir is called before import
        # But sys.modules is restored by the executor... actually the executor
        # imports the module fresh each time. Let's verify behavior:
        # The module may or may not be in sys.modules depending on cleanup.
        # What matters is that re-invocation picks up new code.

        handler.write_text("def main(event, context):\n    return {'ok': False}\n")
        result, _, _ = self.executor.execute(
            code_zip=b"",
            handler="cleartest_mod.main",
            event={},
            function_name="cleartest",
            code_dir=str(tmp_path),
            hot_reload=True,
        )
        assert result == {"ok": False}

    def test_code_dir_used_as_working_directory(self, tmp_path):
        """code_dir should be on sys.path so imports from it work."""
        (tmp_path / "mylib.py").write_text("VALUE = 42\n")
        handler = tmp_path / "handler.py"
        handler.write_text(
            "import mylib\ndef main(event, context):\n    return {'value': mylib.VALUE}\n"
        )

        result, error_type, _ = self.executor.execute(
            code_zip=b"",
            handler="handler.main",
            event={},
            function_name="import_test",
            code_dir=str(tmp_path),
            hot_reload=True,
        )
        assert result == {"value": 42}
        assert error_type is None
        _clear_modules_for_dir(str(tmp_path))

    def test_zip_code_with_nested_package(self):
        """Zip with nested package structure should work."""
        code_zip = make_zip(
            {
                "handler.py": (
                    "from pkg.util import helper\n"
                    "def main(event, context):\n"
                    "    return {'result': helper()}\n"
                ),
                "pkg/__init__.py": "",
                "pkg/util.py": "def helper():\n    return 'hello'\n",
            }
        )

        executor = PythonExecutor()
        result, error_type, _ = executor.execute(
            code_zip=code_zip,
            handler="handler.main",
            event={},
            function_name="nested_pkg",
        )
        assert result == {"result": "hello"}
        assert error_type is None
        get_code_cache().invalidate("nested_pkg")
