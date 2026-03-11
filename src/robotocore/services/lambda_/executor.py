"""Lambda function executor — runs Python Lambda code in-process.

For Python runtimes, executes the handler function directly without Docker.
For other runtimes, falls back to Moto's Docker-based execution.

Includes a CodeCache that avoids re-extracting zips on every invocation.
"""

import base64
import ctypes
import hashlib
import importlib.util
import io
import logging
import os
import shutil
import sys
import tempfile
import threading
import time
import traceback
import uuid
import zipfile
from collections import OrderedDict
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Lock for thread-safe sys.path and os.environ manipulation
_env_path_lock = threading.Lock()

# Thread-local storage for per-invocation environment isolation
_thread_local = threading.local()


class _ThreadLocalEnviron:
    """A proxy for os.environ that uses thread-local overrides.

    When a thread sets a thread-local environ dict via `install()`, all
    `os.environ` accesses in that thread (get, __getitem__, etc.) are
    routed to the thread-local copy. Other threads are unaffected.
    """

    def __init__(self, real_environ: os._Environ) -> None:
        object.__setattr__(self, "_real", real_environ)

    def _current(self) -> os._Environ | dict:
        return getattr(_thread_local, "environ", self._real)

    def install(self, env: dict) -> None:
        """Set a thread-local environ for the calling thread."""
        _thread_local.environ = env

    def uninstall(self) -> None:
        """Remove the thread-local environ, reverting to the real one."""
        _thread_local.environ = None

    # Dict-like interface delegating to the current environ
    def __getitem__(self, key: str) -> str:
        return self._current()[key]

    def __setitem__(self, key: str, value: str) -> None:
        self._current()[key] = value

    def __delitem__(self, key: str) -> None:
        del self._current()[key]

    def __contains__(self, key: object) -> bool:
        return key in self._current()

    def __iter__(self):
        return iter(self._current())

    def __len__(self) -> int:
        return len(self._current())

    def get(self, key: str, default: str | None = None) -> str | None:
        return self._current().get(key, default)

    def keys(self):
        return self._current().keys()

    def values(self):
        return self._current().values()

    def items(self):
        return self._current().items()

    def update(self, *args, **kwargs) -> None:
        self._current().update(*args, **kwargs)

    def copy(self) -> dict:
        return dict(self._current())

    def clear(self) -> None:
        self._current().clear()

    def pop(self, key: str, *args):
        return self._current().pop(key, *args)

    def setdefault(self, key: str, default: str = "") -> str:
        return self._current().setdefault(key, default)

    def __repr__(self) -> str:
        return repr(self._current())


# Install the thread-local environ proxy so that handler code using
# os.environ.get() is automatically isolated per-thread.
_real_environ = os.environ
_tl_environ = _ThreadLocalEnviron(_real_environ)
os.environ = _tl_environ  # type: ignore[assignment]


def get_layer_zips(fn, account_id: str, region: str) -> list[bytes]:
    """Extract layer zip bytes from a Moto LambdaFunction's layers."""
    layer_zips = []
    layers = getattr(fn, "layers", None) or []
    if not layers:
        return layer_zips

    try:
        from moto.backends import get_backend
        from moto.core import DEFAULT_ACCOUNT_ID

        acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID
        backend = get_backend("lambda")[acct][region]

        for layer_ref in layers:
            layer_arn = (
                layer_ref
                if isinstance(layer_ref, str)
                else getattr(layer_ref, "arn", str(layer_ref))
            )
            try:
                # Parse layer ARN: arn:aws:lambda:region:account:layer:name:version
                parts = layer_arn.split(":")
                if len(parts) >= 8:
                    layer_name = parts[6]
                    version = int(parts[7])
                    layer_ver = backend.get_layer_version(layer_name, version)
                    if layer_ver and hasattr(layer_ver, "code"):
                        code = layer_ver.code
                        if isinstance(code, dict) and "ZipFile" in code:
                            zip_data = code["ZipFile"]
                            if isinstance(zip_data, str):
                                zip_data = base64.b64decode(zip_data)
                            layer_zips.append(zip_data)
                        elif hasattr(layer_ver, "code_bytes") and layer_ver.code_bytes:
                            layer_zips.append(layer_ver.code_bytes)
            except Exception:
                pass
    except Exception:
        pass

    return layer_zips


class CodeCache:
    """Cache of extracted Lambda code directories.

    Maps (function_name, code_sha256) to an extracted temp directory.
    Uses LRU eviction when the cache exceeds max_size entries.
    Thread-safe. Evicted directories are not deleted immediately — they are
    kept on disk until explicitly cleaned up, preventing race conditions where
    an evicted directory is still in use by another thread.
    """

    def __init__(self, max_size: int = 50) -> None:
        self._max_size = max_size
        self._cache: OrderedDict[tuple[str, str], str] = OrderedDict()
        self._lock = threading.Lock()
        # Reference counts for directories currently in use
        self._refcounts: dict[str, int] = {}
        # Directories evicted from cache but not yet cleaned up
        self._evicted: set[str] = set()

    def get_or_extract(
        self,
        function_name: str,
        code_zip: bytes,
        layer_zips: list[bytes] | None = None,
    ) -> str:
        """Return a cached tmpdir for the given code, or extract a new one.

        The returned directory must NOT be deleted by the caller — the cache
        manages its lifecycle.

        Extraction happens outside the lock to avoid blocking concurrent cache
        lookups during slow I/O.
        """
        code_hash = hashlib.sha256(code_zip).hexdigest()
        key = (function_name, code_hash)

        with self._lock:
            if key in self._cache:
                # Move to end (most recently used)
                self._cache.move_to_end(key)
                path = self._cache[key]
                if os.path.isdir(path):
                    return path
                # Dir was deleted externally — remove stale entry
                del self._cache[key]

        # Extract outside the lock — I/O can be slow for large zips
        tmpdir = tempfile.mkdtemp(prefix="lambda_cache_")
        try:
            if layer_zips:
                for layer_zip in layer_zips:
                    try:
                        with zipfile.ZipFile(io.BytesIO(layer_zip)) as zf:
                            zf.extractall(tmpdir)
                    except (zipfile.BadZipFile, OSError):
                        logger.warning("CodeCache: failed to extract layer zip")
            with zipfile.ZipFile(io.BytesIO(code_zip)) as zf:
                zf.extractall(tmpdir)
        except (zipfile.BadZipFile, OSError) as exc:
            # Clean up on extraction failure
            shutil.rmtree(tmpdir, ignore_errors=True)
            raise zipfile.BadZipFile(f"Failed to extract Lambda code: {exc}") from exc

        with self._lock:
            # Another thread may have extracted the same key while we were outside the lock.
            # If so, discard our extraction and use the existing one.
            if key in self._cache:
                existing = self._cache[key]
                if os.path.isdir(existing):
                    shutil.rmtree(tmpdir, ignore_errors=True)
                    self._cache.move_to_end(key)
                    return existing
                del self._cache[key]

            self._cache[key] = tmpdir

            # Evict LRU entries if over capacity
            while len(self._cache) > self._max_size:
                evicted_key, evicted_path = self._cache.popitem(last=False)
                logger.debug("CodeCache: evicting %s", evicted_key)
                # Defer directory deletion — the dir may still be in use
                # by another thread. It will be cleaned up later.
                self._evicted.add(evicted_path)

            return tmpdir

    def acquire(self, path: str) -> None:
        """Increment reference count for a directory (mark as in-use)."""
        with self._lock:
            self._refcounts[path] = self._refcounts.get(path, 0) + 1

    def release(self, path: str) -> None:
        """Decrement reference count for a directory.

        If the directory was evicted from cache and has no more references,
        it is deleted from disk.
        """
        with self._lock:
            count = self._refcounts.get(path, 0) - 1
            if count <= 0:
                self._refcounts.pop(path, None)
                # Clean up evicted dirs that are no longer referenced
                if path in self._evicted:
                    self._evicted.discard(path)
                    shutil.rmtree(path, ignore_errors=True)
            else:
                self._refcounts[path] = count

    def cleanup_evicted(self) -> None:
        """Clean up evicted directories that have no active references."""
        with self._lock:
            to_clean = {p for p in self._evicted if self._refcounts.get(p, 0) <= 0}
            for path in to_clean:
                self._evicted.discard(path)
                shutil.rmtree(path, ignore_errors=True)

    def invalidate(self, function_name: str) -> None:
        """Remove all cached entries for a function and clean up their tmpdirs."""
        with self._lock:
            keys_to_remove = [k for k in self._cache if k[0] == function_name]
            for key in keys_to_remove:
                path = self._cache.pop(key)
                if self._refcounts.get(path, 0) <= 0:
                    shutil.rmtree(path, ignore_errors=True)
                    self._refcounts.pop(path, None)
        # Also clear function-scoped module cache in sys.modules
        prefix = f"_lambda_{function_name}."
        for name in list(sys.modules.keys()):
            if name.startswith(prefix):
                sys.modules.pop(name, None)

    def invalidate_all(self) -> None:
        """Remove all cached entries and clean up all tmpdirs."""
        with self._lock:
            for path in self._cache.values():
                if self._refcounts.get(path, 0) <= 0:
                    shutil.rmtree(path, ignore_errors=True)
            self._cache.clear()
            self._refcounts.clear()
        # Also clear all function-scoped module cache entries
        for name in list(sys.modules.keys()):
            if name.startswith("_lambda_"):
                sys.modules.pop(name, None)

    def __len__(self) -> int:
        with self._lock:
            return len(self._cache)


# Module-level singleton
_code_cache = CodeCache()


def get_code_cache() -> CodeCache:
    """Return the global CodeCache singleton."""
    return _code_cache


@dataclass
class LambdaContext:
    """Mock AWS Lambda context object."""

    function_name: str
    function_version: str = "$LATEST"
    memory_limit_in_mb: int | str = 128
    invoked_function_arn: str = ""
    aws_request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    log_group_name: str = ""
    log_stream_name: str = ""
    _timeout: int = 3
    _start_time: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        # AWS Lambda Python runtime exposes memory_limit_in_mb as a string
        self.memory_limit_in_mb = str(self.memory_limit_in_mb)

    def get_remaining_time_in_millis(self) -> int:
        elapsed = time.time() - self._start_time
        remaining = max(0, self._timeout - elapsed)
        return int(remaining * 1000)


def _clear_modules_for_dir(code_dir: str) -> None:
    """Remove cached modules that live inside code_dir from sys.modules.

    This forces fresh imports on the next invocation when hot reload is active.
    Also removes __pycache__ directories (bytecode cache) and clears importlib's
    finder caches so that SourceFileLoader re-reads the source files.
    """
    import importlib
    import linecache

    norm_dir = os.path.abspath(code_dir) + os.sep
    to_remove = []
    # Copy items() to a list to avoid RuntimeError if another thread modifies sys.modules
    for name, mod in list(sys.modules.items()):
        mod_file = getattr(mod, "__file__", None)
        if mod_file:
            try:
                norm_file = os.path.abspath(mod_file)
                if norm_file.startswith(norm_dir):
                    to_remove.append(name)
            except (ValueError, TypeError):
                pass
    # Also clear function-scoped module cache keys (_lambda_* entries)
    for name in list(sys.modules.keys()):
        if name.startswith("_lambda_"):
            mod = sys.modules[name]
            mod_file = getattr(mod, "__file__", None)
            if mod_file:
                try:
                    if os.path.abspath(mod_file).startswith(norm_dir):
                        to_remove.append(name)
                except (ValueError, TypeError):
                    pass
    for name in to_remove:
        sys.modules.pop(name, None)

    # Remove __pycache__ directories so SourceFileLoader doesn't use stale bytecode
    for dirpath, dirnames, _ in os.walk(code_dir):
        for dirname in dirnames:
            if dirname == "__pycache__":
                pycache_path = os.path.join(dirpath, dirname)
                shutil.rmtree(pycache_path, ignore_errors=True)

    # Clear linecache for files in the code dir
    keys_to_clear = [k for k in list(linecache.cache) if os.path.abspath(k).startswith(norm_dir)]
    for k in keys_to_clear:
        linecache.cache.pop(k, None)

    # Invalidate importlib finder caches so loaders re-read source files
    importlib.invalidate_caches()


def _format_aws_stacktrace(tb_str: str) -> list[str]:
    """Format a Python traceback into AWS Lambda stackTrace format.

    AWS Lambda's stackTrace is a list of frame description strings, excluding
    the 'Traceback (most recent call last):' header and the final exception line.
    """
    lines = tb_str.strip().split("\n")
    frames = []
    for line in lines:
        # Skip the "Traceback (most recent call last):" header
        if line.startswith("Traceback"):
            continue
        # Skip the final exception line (e.g., "ValueError: test error")
        # These lines are not indented and contain the exception info
        if not line.startswith(" ") and ":" in line and not line.startswith("  "):
            continue
        if line.strip():
            frames.append(line)
    return frames


def execute_python_handler(
    code_zip: bytes,
    handler: str,
    event: dict,
    function_name: str,
    timeout: int = 3,
    memory_size: int = 128,
    env_vars: dict | None = None,
    region: str = "us-east-1",
    account_id: str = "123456789012",
    layer_zips: list[bytes] | None = None,
    code_dir: str | None = None,
    hot_reload: bool = False,
) -> tuple[dict | str | None, str | None, str]:
    """Execute a Python Lambda handler in-process.

    Args:
        code_dir: Pre-extracted or mounted code directory. When provided, skips
            zip extraction entirely. If None, uses the CodeCache.
        hot_reload: When True and code_dir is set, clears cached modules before
            importing to pick up code changes.

    Returns (result, error_type, logs).
    """
    # Parse handler: "module.function" or "dir/module.function"
    parts = handler.rsplit(".", 1)
    if len(parts) != 2:
        return None, "Runtime.HandlerNotFound", f"Bad handler format: {handler}"
    module_path, func_name = parts

    # Determine working directory
    if code_dir:
        # Use provided directory directly (mount dir or pre-extracted)
        tmpdir = code_dir
    else:
        # Use code cache instead of extracting every time
        tmpdir = _code_cache.get_or_extract(function_name, code_zip, layer_zips)

    # Acquire a reference to prevent eviction during execution
    _code_cache.acquire(tmpdir)
    try:
        # Hot reload: clear cached modules so we get fresh imports
        if hot_reload and code_dir:
            _clear_modules_for_dir(tmpdir)

        # Build context
        context = LambdaContext(
            function_name=function_name,
            memory_limit_in_mb=memory_size,
            invoked_function_arn=(f"arn:aws:lambda:{region}:{account_id}:function:{function_name}"),
            log_group_name=f"/aws/lambda/{function_name}",
            log_stream_name=(f"{time.strftime('%Y/%m/%d')}/[$LATEST]{uuid.uuid4().hex[:32]}"),
            _timeout=timeout,
        )

        request_id = context.aws_request_id

        # Set up environment and sys.path under a lock for thread safety
        with _env_path_lock:
            old_env = os.environ.copy()
            old_path = sys.path[:]
            if env_vars:
                os.environ.update(env_vars)
            os.environ["AWS_LAMBDA_FUNCTION_NAME"] = function_name
            os.environ["AWS_REGION"] = region
            os.environ["AWS_DEFAULT_REGION"] = region
            os.environ["AWS_ACCOUNT_ID"] = account_id
            sys.path.insert(0, tmpdir)
            # AWS Lambda layers put Python code in python/ subdirectory
            python_subdir = os.path.join(tmpdir, "python")
            if os.path.isdir(python_subdir):
                sys.path.insert(1, python_subdir)
            # Snapshot the env/path for this invocation
            invocation_env = os.environ.copy()
            invocation_path = sys.path[:]
            # Restore immediately so other threads aren't affected
            os.environ.clear()
            os.environ.update(old_env)
            sys.path[:] = old_path

        # Load the module
        module_file = os.path.join(tmpdir, module_path.replace(".", "/") + ".py")
        if not os.path.exists(module_file):
            # Try without directory nesting
            module_file = os.path.join(tmpdir, module_path.replace("/", ".") + ".py")
        if not os.path.exists(module_file):
            # Try just the filename
            module_file = os.path.join(tmpdir, module_path + ".py")

        if not os.path.exists(module_file):
            return None, "Runtime.ImportModuleError", f"Cannot find module: {module_path}"

        # Use a function-scoped key so different Lambda functions don't collide
        modules_key = f"_lambda_{function_name}.{module_path}"

        # Run handler in a separate thread with timeout enforcement
        handler_result: list = []  # [(result, error_type, logs)]

        def _run_handler() -> None:
            handler_logs = io.StringIO()
            # Install a thread-local environ so concurrent invocations
            # don't overwrite each other's variables.
            _tl_environ.install(dict(invocation_env))
            old_stdout = sys.stdout
            old_stderr = sys.stderr
            try:
                sys.stdout = handler_logs
                sys.stderr = handler_logs

                # START log line (AWS parity)
                start_line = f"START RequestId: {request_id} Version: {context.function_version}\n"
                handler_logs.write(start_line)

                # Load module with sys.path temporarily modified under a lock
                # to prevent concurrent threads from corrupting each other's paths
                with _env_path_lock:
                    saved_path = sys.path[:]
                    sys.path[:] = invocation_path
                    try:
                        if not hot_reload and modules_key in sys.modules:
                            module = sys.modules[modules_key]
                        else:
                            spec = importlib.util.spec_from_file_location(module_path, module_file)
                            module = importlib.util.module_from_spec(spec)
                            spec.loader.exec_module(module)
                            sys.modules[modules_key] = module
                    finally:
                        sys.path[:] = saved_path

                handler_func = getattr(module, func_name, None)
                if handler_func is None:
                    handler_result.append(
                        (
                            None,
                            "Runtime.HandlerNotFound",
                            f"Handler function '{func_name}' not found in {module_path}",
                        )
                    )
                    return

                result = handler_func(event, context)

                # END/REPORT log lines
                end_line = f"END RequestId: {request_id}\n"
                report_line = (
                    f"REPORT RequestId: {request_id}\t"
                    f"Duration: 0 ms\tBilled Duration: 100 ms\t"
                    f"Memory Size: {context.memory_limit_in_mb} MB\t"
                    f"Max Memory Used: 0 MB\n"
                )
                handler_logs.write(end_line)
                handler_logs.write(report_line)

                handler_result.append((result, None, handler_logs.getvalue()))
            except Exception as e:
                tb = traceback.format_exc()
                handler_logs.write(tb)
                # END/REPORT log lines even on error
                end_line = f"END RequestId: {request_id}\n"
                report_line = (
                    f"REPORT RequestId: {request_id}\t"
                    f"Duration: 0 ms\tBilled Duration: 100 ms\t"
                    f"Memory Size: {context.memory_limit_in_mb} MB\t"
                    f"Max Memory Used: 0 MB\n"
                )
                handler_logs.write(end_line)
                handler_logs.write(report_line)

                error_result = {
                    "errorMessage": str(e),
                    "errorType": type(e).__name__,
                    "stackTrace": _format_aws_stacktrace(tb),
                }
                handler_result.append((error_result, "Handled", handler_logs.getvalue()))
            finally:
                sys.stdout = old_stdout
                sys.stderr = old_stderr
                # Remove thread-local environ
                _tl_environ.uninstall()

        handler_thread = threading.Thread(target=_run_handler, daemon=True)
        handler_thread.start()
        handler_thread.join(timeout=timeout)

        if handler_thread.is_alive():
            # Handler exceeded timeout — attempt to interrupt it
            try:
                tid = handler_thread.ident
                if tid is not None:
                    ctypes.pythonapi.PyThreadState_SetAsyncExc(
                        ctypes.c_ulong(tid), ctypes.py_object(SystemExit)
                    )
            except Exception:
                pass
            # Don't wait forever for the interrupted thread
            handler_thread.join(timeout=1)

            timeout_logs = (
                f"START RequestId: {request_id} "
                f"Version: {context.function_version}\n"
                f"{time.strftime('%Y-%m-%dT%H:%M:%S')} "
                f"{request_id} Task timed out after {timeout}.00 seconds\n"
                f"END RequestId: {request_id}\n"
                f"REPORT RequestId: {request_id}\t"
                f"Duration: {timeout * 1000} ms\tBilled Duration: {timeout * 1000} ms\t"
                f"Memory Size: {context.memory_limit_in_mb} MB\t"
                f"Max Memory Used: 0 MB\n"
            )
            error_result = {
                "errorMessage": f"{time.strftime('%Y-%m-%dT%H:%M:%S')} "
                f"{request_id} Task timed out after {timeout}.00 seconds",
                "errorType": "Task.TimedOut",
            }
            return error_result, "Task.TimedOut", timeout_logs

        if handler_result:
            return handler_result[0]

        # Should not reach here
        return None, "Runtime.Unknown", "Handler thread completed without result"
    finally:
        _code_cache.release(tmpdir)
        # Do NOT clean up tmpdir when using cache or mount dir — managed externally
