"""Tests for Lambda CodeCache — extracted zip caching with LRU eviction."""

import io
import os
import shutil
import threading
import zipfile

import pytest

from robotocore.services.lambda_.executor import CodeCache, _clear_modules_for_dir


def _make_zip(content: str = "x = 1\n", filename: str = "handler.py") -> bytes:
    """Create a minimal zip with a single Python file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, content)
    return buf.getvalue()


def _make_multi_file_zip(files: dict[str, str]) -> bytes:
    """Create a zip with multiple files. Keys are paths, values are content."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for path, content in files.items():
            zf.writestr(path, content)
    return buf.getvalue()


class TestCodeCache:
    def test_cache_hit_returns_same_dir(self):
        cache = CodeCache(max_size=10)
        code = _make_zip("a = 1")
        dir1 = cache.get_or_extract("myfunc", code)
        dir2 = cache.get_or_extract("myfunc", code)
        assert dir1 == dir2
        assert os.path.isdir(dir1)
        cache.invalidate_all()

    def test_cache_miss_different_code(self):
        cache = CodeCache(max_size=10)
        code_a = _make_zip("a = 1")
        code_b = _make_zip("b = 2")
        dir_a = cache.get_or_extract("myfunc", code_a)
        dir_b = cache.get_or_extract("myfunc", code_b)
        assert dir_a != dir_b
        assert os.path.isdir(dir_a)
        assert os.path.isdir(dir_b)
        cache.invalidate_all()

    def test_invalidate_removes_cached_dir(self):
        cache = CodeCache(max_size=10)
        code = _make_zip("a = 1")
        cached_dir = cache.get_or_extract("myfunc", code)
        assert os.path.isdir(cached_dir)
        cache.invalidate("myfunc")
        assert not os.path.isdir(cached_dir)
        assert len(cache) == 0

    def test_invalidate_all(self):
        cache = CodeCache(max_size=10)
        dir1 = cache.get_or_extract("func1", _make_zip("a"))
        dir2 = cache.get_or_extract("func2", _make_zip("b"))
        assert len(cache) == 2
        cache.invalidate_all()
        assert len(cache) == 0
        assert not os.path.isdir(dir1)
        assert not os.path.isdir(dir2)

    def test_lru_eviction(self):
        cache = CodeCache(max_size=3)
        dirs = []
        for i in range(4):
            d = cache.get_or_extract(f"func{i}", _make_zip(f"x = {i}"))
            dirs.append(d)
        # First entry should have been evicted
        assert len(cache) == 3
        assert not os.path.isdir(dirs[0])
        # Others should still exist
        for d in dirs[1:]:
            assert os.path.isdir(d)
        cache.invalidate_all()

    def test_lru_access_resets_order(self):
        cache = CodeCache(max_size=3)
        code_a = _make_zip("a")
        code_b = _make_zip("b")
        code_c = _make_zip("c")
        dir_a = cache.get_or_extract("fa", code_a)
        cache.get_or_extract("fb", code_b)
        cache.get_or_extract("fc", code_c)
        # Access "fa" again to move it to end (most recently used)
        cache.get_or_extract("fa", code_a)
        # Now adding a 4th entry should evict "fb" (least recently used)
        cache.get_or_extract("fd", _make_zip("d"))
        assert os.path.isdir(dir_a)  # "fa" was recently used, should survive
        assert len(cache) == 3
        cache.invalidate_all()

    def test_hash_based_keying(self):
        cache = CodeCache(max_size=10)
        code1 = _make_zip("version = 1")
        code2 = _make_zip("version = 2")
        dir1 = cache.get_or_extract("myfunc", code1)
        dir2 = cache.get_or_extract("myfunc", code2)
        # Different content -> different hash -> different dirs
        assert dir1 != dir2
        cache.invalidate_all()

    def test_extracted_files_present(self):
        cache = CodeCache(max_size=10)
        code = _make_zip("result = 42", "handler.py")
        d = cache.get_or_extract("myfunc", code)
        assert os.path.exists(os.path.join(d, "handler.py"))
        with open(os.path.join(d, "handler.py")) as f:
            assert "result = 42" in f.read()
        cache.invalidate_all()

    def test_layers_extracted(self):
        cache = CodeCache(max_size=10)
        code = _make_zip("main = 1", "main.py")
        layer = _make_zip("util = 1", "util.py")
        d = cache.get_or_extract("myfunc", code, layer_zips=[layer])
        assert os.path.exists(os.path.join(d, "main.py"))
        assert os.path.exists(os.path.join(d, "util.py"))
        cache.invalidate_all()

    def test_stale_entry_recovered(self, tmp_path):
        """If the cached dir is deleted externally, cache recovers gracefully."""
        cache = CodeCache(max_size=10)
        code = _make_zip("a = 1")
        dir1 = cache.get_or_extract("myfunc", code)
        # Simulate external deletion
        shutil.rmtree(dir1)
        # Should detect stale entry and re-extract
        dir2 = cache.get_or_extract("myfunc", code)
        assert os.path.isdir(dir2)
        assert dir1 != dir2  # New directory created
        cache.invalidate_all()

    # --- New tests ---

    def test_concurrent_get_or_extract(self):
        """Thread safety: concurrent get_or_extract from multiple threads."""
        cache = CodeCache(max_size=50)
        code = _make_zip("concurrent = True")
        results = []
        errors = []

        def extract_in_thread():
            try:
                d = cache.get_or_extract("shared_func", code)
                results.append(d)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=extract_in_thread) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Errors in threads: {errors}"
        assert len(results) == 10
        # All threads should get the same directory (cache hit after first extraction)
        # or a valid directory at minimum
        for d in results:
            assert os.path.isdir(d)
        cache.invalidate_all()

    def test_concurrent_different_functions(self):
        """Thread safety: concurrent extraction of different functions."""
        cache = CodeCache(max_size=50)
        results = {}
        errors = []

        def extract_func(name, content):
            try:
                d = cache.get_or_extract(name, _make_zip(content))
                results[name] = d
            except Exception as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=extract_func, args=(f"func_{i}", f"x = {i}")) for i in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert len(results) == 10
        # Each function should have a unique directory
        assert len(set(results.values())) == 10
        cache.invalidate_all()

    def test_eviction_over_50_entries(self):
        """LRU eviction: create 60 entries, verify only 50 remain."""
        cache = CodeCache(max_size=50)
        dirs = []
        for i in range(60):
            d = cache.get_or_extract(f"f{i}", _make_zip(f"v = {i}"))
            dirs.append(d)

        assert len(cache) == 50
        # First 10 entries should have been evicted
        for d in dirs[:10]:
            assert not os.path.isdir(d), f"Expected evicted: {d}"
        # Last 50 should still exist
        for d in dirs[10:]:
            assert os.path.isdir(d), f"Expected to exist: {d}"
        cache.invalidate_all()

    def test_zip_with_subdirectories(self):
        """Zip file with subdirectories and nested modules."""
        code = _make_multi_file_zip(
            {
                "handler.py": "from pkg.util import helper\ndef main(): return helper()",
                "pkg/__init__.py": "",
                "pkg/util.py": "def helper(): return 42",
                "pkg/sub/__init__.py": "",
                "pkg/sub/deep.py": "DEEP = True",
            }
        )
        cache = CodeCache(max_size=10)
        d = cache.get_or_extract("nested_func", code)
        assert os.path.exists(os.path.join(d, "handler.py"))
        assert os.path.exists(os.path.join(d, "pkg", "__init__.py"))
        assert os.path.exists(os.path.join(d, "pkg", "util.py"))
        assert os.path.exists(os.path.join(d, "pkg", "sub", "deep.py"))
        cache.invalidate_all()

    def test_zero_byte_zip_raises(self):
        """Zero-byte zip file should raise BadZipFile."""
        cache = CodeCache(max_size=10)
        with pytest.raises(zipfile.BadZipFile):
            cache.get_or_extract("bad_func", b"")

    def test_invalid_zip_raises(self):
        """Random bytes (not a zip) should raise BadZipFile."""
        cache = CodeCache(max_size=10)
        with pytest.raises(zipfile.BadZipFile):
            cache.get_or_extract("bad_func", b"this is not a zip file")

    def test_bad_zip_cleans_up_tmpdir(self):
        """When zip extraction fails, the tmpdir should be cleaned up."""
        cache = CodeCache(max_size=10)
        with pytest.raises(zipfile.BadZipFile):
            cache.get_or_extract("bad_func", b"not-a-zip")
        # Cache should have no entries for this function
        assert len(cache) == 0

    def test_code_hash_changes_single_file_mod(self):
        """Hash changes when only one file in a multi-file zip changes."""
        code_v1 = _make_multi_file_zip(
            {
                "handler.py": "VERSION = 1",
                "config.py": "DB = 'localhost'",
            }
        )
        code_v2 = _make_multi_file_zip(
            {
                "handler.py": "VERSION = 2",  # Only this changed
                "config.py": "DB = 'localhost'",
            }
        )
        cache = CodeCache(max_size=10)
        dir1 = cache.get_or_extract("myfunc", code_v1)
        dir2 = cache.get_or_extract("myfunc", code_v2)
        assert dir1 != dir2
        # Verify the content is correct in each
        with open(os.path.join(dir1, "handler.py")) as f:
            assert "VERSION = 1" in f.read()
        with open(os.path.join(dir2, "handler.py")) as f:
            assert "VERSION = 2" in f.read()
        cache.invalidate_all()

    def test_invalidate_nonexistent_function(self):
        """Invalidating a function not in the cache should not raise."""
        cache = CodeCache(max_size=10)
        cache.invalidate("nonexistent")  # Should not raise
        assert len(cache) == 0

    def test_invalidate_only_target_function(self):
        """Invalidating one function leaves others intact."""
        cache = CodeCache(max_size=10)
        dir_a = cache.get_or_extract("func_a", _make_zip("a = 1"))
        dir_b = cache.get_or_extract("func_b", _make_zip("b = 2"))
        cache.invalidate("func_a")
        assert not os.path.isdir(dir_a)
        assert os.path.isdir(dir_b)
        assert len(cache) == 1
        cache.invalidate_all()

    def test_layer_overridden_by_function_code(self):
        """Function code should override layer files with same name."""
        layer = _make_zip("ORIGIN = 'layer'", "shared.py")
        code = _make_zip("ORIGIN = 'function'", "shared.py")
        cache = CodeCache(max_size=10)
        d = cache.get_or_extract("myfunc", code, layer_zips=[layer])
        with open(os.path.join(d, "shared.py")) as f:
            assert "ORIGIN = 'function'" in f.read()
        cache.invalidate_all()

    def test_bad_layer_zip_does_not_prevent_extraction(self):
        """A bad layer zip should be skipped, but function code still extracts."""
        cache = CodeCache(max_size=10)
        code = _make_zip("main = True", "handler.py")
        bad_layer = b"not-a-zip"
        d = cache.get_or_extract("myfunc", code, layer_zips=[bad_layer])
        assert os.path.exists(os.path.join(d, "handler.py"))
        cache.invalidate_all()

    def test_multiple_code_versions_same_function(self):
        """Multiple code versions for the same function all cached."""
        cache = CodeCache(max_size=10)
        dirs = []
        for i in range(5):
            d = cache.get_or_extract("myfunc", _make_zip(f"v = {i}"))
            dirs.append(d)
        # All 5 should be different and present (same function name, different hashes)
        assert len(set(dirs)) == 5
        assert len(cache) == 5
        cache.invalidate_all()


class TestClearModulesForDir:
    def test_clears_modules_in_dir(self, tmp_path):
        """Modules from code_dir should be removed from sys.modules."""
        import sys
        import types

        mod = types.ModuleType("test_clear_mod_1")
        mod.__file__ = str(tmp_path / "test_clear_mod_1.py")
        sys.modules["test_clear_mod_1"] = mod

        _clear_modules_for_dir(str(tmp_path))
        assert "test_clear_mod_1" not in sys.modules

    def test_does_not_clear_unrelated_modules(self, tmp_path):
        """Modules outside code_dir should NOT be removed."""
        import sys
        import types

        mod = types.ModuleType("test_clear_mod_2")
        mod.__file__ = "/some/other/path/test_clear_mod_2.py"
        sys.modules["test_clear_mod_2"] = mod

        _clear_modules_for_dir(str(tmp_path))
        assert "test_clear_mod_2" in sys.modules
        del sys.modules["test_clear_mod_2"]

    def test_handles_modules_without_file(self, tmp_path):
        """Modules with no __file__ attribute should be skipped."""
        import sys
        import types

        mod = types.ModuleType("test_clear_mod_3")
        # No __file__ attribute
        sys.modules["test_clear_mod_3"] = mod

        _clear_modules_for_dir(str(tmp_path))
        # Should not crash and module should remain
        assert "test_clear_mod_3" in sys.modules
        del sys.modules["test_clear_mod_3"]

    def test_clears_nested_module(self, tmp_path):
        """Nested modules (pkg.sub.mod) should be cleared if under code_dir."""
        import sys
        import types

        subdir = tmp_path / "pkg" / "sub"
        subdir.mkdir(parents=True)

        mod = types.ModuleType("pkg.sub.nested_mod")
        mod.__file__ = str(subdir / "nested_mod.py")
        sys.modules["pkg.sub.nested_mod"] = mod

        _clear_modules_for_dir(str(tmp_path))
        assert "pkg.sub.nested_mod" not in sys.modules

    def test_handles_none_file_attribute(self, tmp_path):
        """Modules with __file__ = None should be skipped."""
        import sys
        import types

        mod = types.ModuleType("test_clear_none_file")
        mod.__file__ = None
        sys.modules["test_clear_none_file"] = mod

        _clear_modules_for_dir(str(tmp_path))
        assert "test_clear_none_file" in sys.modules
        del sys.modules["test_clear_none_file"]
