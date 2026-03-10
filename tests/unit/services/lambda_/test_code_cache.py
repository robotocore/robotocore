"""Tests for Lambda CodeCache — extracted zip caching with LRU eviction."""

import io
import os
import zipfile

from robotocore.services.lambda_.executor import CodeCache


def _make_zip(content: str = "x = 1\n", filename: str = "handler.py") -> bytes:
    """Create a minimal zip with a single Python file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, content)
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
        import shutil

        shutil.rmtree(dir1)
        # Should detect stale entry and re-extract
        dir2 = cache.get_or_extract("myfunc", code)
        assert os.path.isdir(dir2)
        assert dir1 != dir2  # New directory created
        cache.invalidate_all()
