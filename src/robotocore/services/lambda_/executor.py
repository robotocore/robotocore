"""Lambda function executor --- runs Python Lambda code in-process.

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

# Locks for thread-safe access to global mutable state
_env_lock = threading.Lock()
_path_lock = threading.Lock()

# Thread-local storage for per-invocation environment variables
_thread_local = threading.local()


class _ThreadLocalEnviron:
    """A proxy around os.environ that supports per-thread overrides.

    When a thread has set a thread-local environment (via _thread_local.env),
    all reads go to that thread-local dict. Otherwise, reads fall through to
    the real os.environ.
    """

    def __init__(self, real_environ: os._Environ) -> None:
        # Store in object __dict__ directly to avoid __setattr__ issues
        object.__setattr__(self, "_real", real_environ)

    def _get_local(self) -> dict | None:
        return getattr(_thread_local, "env", None)

    def __getitem__(self, key: str) -> str:
        local = self._get_local()
        if local is not None:
            return local[key]
        return self._real[key]

    def __setitem__(self, key: str, value: str) -> None:
        local = self._get_local()
        if local is not None:
            local[key] = value
        else:
            self._real[key] = value

    def __delitem__(self, key: str) -> None:
        local = self._get_local()
        if local is not None:
            del local[key]
        else:
            del self._real[key]

    def __contains__(self, key: object) -> bool:
        local = self._get_local()
        if local is not None:
            return key in local
        return key in self._real

    def __iter__(self):
        local = self._get_local()
        if local is not None:
            return iter(local)
        return iter(self._real)

    def __len__(self) -> int:
        local = self._get_local()
        if local is not None:
            return len(local)
        return len(self._real)

    def get(self, key: str, default: str | None = None) -> str | None:
        local = self._get_local()
        if local is not None:
            return local.get(key, default)
        return self._real.get(key, default)

    def keys(self):
        local = self._get_local()
        if local is not None:
            return local.keys()
        return self._real.keys()

    def values(self):
        local = self._get_local()
        if local is not None:
            return local.values()
        return self._real.values()

    def items(self):
        local = self._get_local()
        if local is not None:
            return local.items()
        return self._real.items()

    def copy(self) -> dict:
        local = self._get_local()
        if local is not None:
            return dict(local)
        return dict(self._real)

    def update(self, *args, **kwargs) -> None:
        local = self._get_local()
        if local is not None:
            local.update(*args, **kwargs)
        else:
            self._real.update(*args, **kwargs)

    def clear(self) -> None:
        local = self._get_local()
        if local is not None:
            local.clear()
        else:
            self._real.clear()

    def pop(self, key: str, *args):
        local = self._get_local()
        if local is not None:
            return local.pop(key, *args)
        return self._real.pop(key, *args)

    def __repr__(self) -> str:
        local = self._get_local()
        if local is not None:
            return f"_ThreadLocalEnviron(thread-local, {len(local)} vars)"
        return repr(self._real)


def _install_thread_local_environ() -> None:
    """Install the thread-local environ proxy if not already installed."""
    if not isinstance(os.environ, _ThreadLocalEnviron):
        os.environ = _ThreadLocalEnviron(os.environ)  # type: ignore[assignment]


# Install on module load
_install_thread_local_environ()


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
            if isinstance(layer_ref, str):
                layer_arn = layer_ref
            elif isinstance(layer_ref, dict):
                layer_arn = layer_ref.get("Arn", "")
            else:
                layer_arn = getattr(layer_ref, "arn", str(layer_ref))
            try:
                # Parse layer ARN: arn:aws:lambda:region:account:layer:name:version
                parts = layer_arn.split(":")
                if len(parts) >= 8:
                    layer_name = parts[6]
                    version = int(parts[7])
                    layer_ver = backend.get_layer_version(layer_name, version)
                    if layer_ver:
                        # Try content dict first (Moto stores layer code in .content)
                        content = getattr(layer_ver, "content", None) or getattr(
                            layer_ver, "code", None
                        )
                        if isinstance(content, dict) and "ZipFile" in content:
                            zip_data = content["ZipFile"]
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
    Thread-safe. Entries with active references are not evicted.
    """

    def __init__(self, max_size: int = 50) -> None:
        self._max_size = max_size
        self._cache: OrderedDict[tuple[str, str], str] = OrderedDict()
        self._lock = threading.Lock()
        # Reference counts: how many threads are currently using each path
        self._refcounts: dict[str, int] = {}
        # Directories deferred from cleanup because they were in use during eviction
        self._pending_cleanup: set[str] = set()

    def acquire_ref(self, path: str) -> None:
        """Increment the reference count for a cache directory."""
        with self._lock:
            self._refcounts[path] = self._refcounts.get(path, 0) + 1

    def release_ref(self, path: str) -> None:
        """Decrement the reference count for a cache directory."""
        to_cleanup = []
        with self._lock:
            count = self._refcounts.get(path, 0)
            if count <= 1:
                self._refcounts.pop(path, None)
                # If this path was pending cleanup and now has no refs, clean it up
                if path in self._pending_cleanup:
                    self._pending_cleanup.discard(path)
                    to_cleanup.append(path)
            else:
                self._refcounts[path] = count - 1
        # Delete outside the lock to avoid blocking
        for p in to_cleanup:
            shutil.rmtree(p, ignore_errors=True)

    def cleanup_evicted(self) -> None:
        """Clean up evicted directories that are no longer referenced.

        Directories deferred during eviction (because they had active refs)
        are deleted here once their refcount drops to zero.
        """
        to_cleanup = []
        with self._lock:
            for path in list(self._pending_cleanup):
                if self._refcounts.get(path, 0) == 0:
                    self._pending_cleanup.discard(path)
                    to_cleanup.append(path)
        for p in to_cleanup:
            shutil.rmtree(p, ignore_errors=True)

    def get_or_extract(
        self,
        function_name: str,
        code_zip: bytes,
        layer_zips: list[bytes] | None = None,
    ) -> str:
        """Return a cached tmpdir for the given code, or extract a new one.

        The returned directory must NOT be deleted by the caller --- the cache
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
                    self._refcounts[path] = self._refcounts.get(path, 0) + 1
                    return path
                # Dir was deleted externally --- remove stale entry
                del self._cache[key]

        # Extract outside the lock --- I/O can be slow for large zips
        tmpdir = tempfile.mkdtemp(prefix="lambda_cache_")
        try:
            if layer_zips:
                for layer_zip in layer_zips:
                    try:
                        with zipfile.ZipFile(io.BytesIO(layer_zip)) as zf:
                            for name in zf.namelist():
                                # Normalize separators and check for traversal
                                normalized = name.replace("\\", "/")
                                if normalized.startswith("/") or ".." in normalized.split("/"):
                                    raise ValueError(f"Unsafe path in ZIP archive: {name}")
                            zf.extractall(tmpdir)
                    except (zipfile.BadZipFile, OSError):
                        logger.warning("CodeCache: failed to extract layer zip")
            with zipfile.ZipFile(io.BytesIO(code_zip)) as zf:
                for name in zf.namelist():
                    # Normalize separators and check for traversal
                    normalized = name.replace("\\", "/")
                    if normalized.startswith("/") or ".." in normalized.split("/"):
                        raise ValueError(f"Unsafe path in ZIP archive: {name}")
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
                    self._refcounts[existing] = self._refcounts.get(existing, 0) + 1
                    return existing
                del self._cache[key]

            self._cache[key] = tmpdir
            self._refcounts[tmpdir] = self._refcounts.get(tmpdir, 0) + 1

            # Evict LRU entries if over capacity, but don't delete directories
            # that may still be in use by other threads. Evicted directories are
            # moved to _pending_cleanup and deleted later when safe.
            while len(self._cache) > self._max_size:
                evicted_key, evicted_path = self._cache.popitem(last=False)
                logger.debug("CodeCache: evicting %s", evicted_key)
                if self._refcounts.get(evicted_path, 0) == 0:
                    shutil.rmtree(evicted_path, ignore_errors=True)
                else:
                    # Directory is in use; defer cleanup
                    self._pending_cleanup.add(evicted_path)

            return tmpdir

    def invalidate(self, function_name: str) -> None:
        """Remove all cached entries for a function and clean up their tmpdirs.

        Force-cleans directories regardless of refcount (invalidation is explicit).
        """
        with self._lock:
            keys_to_remove = [k for k in self._cache if k[0] == function_name]
            for key in keys_to_remove:
                path = self._cache.pop(key)
                self._refcounts.pop(path, None)
                self._pending_cleanup.discard(path)
                shutil.rmtree(path, ignore_errors=True)
        # Also clear function-scoped module cache in sys.modules
        prefix = f"_lambda_{function_name}."
        for name in list(sys.modules.keys()):
            if name.startswith(prefix):
                sys.modules.pop(name, None)

    def invalidate_all(self) -> None:
        """Remove all cached entries and clean up all tmpdirs.

        Force-cleans directories regardless of refcount (invalidation is explicit).
        """
        with self._lock:
            for path in self._cache.values():
                shutil.rmtree(path, ignore_errors=True)
            self._cache.clear()
            self._refcounts.clear()
            # Also clean up any pending cleanup dirs
            for path in self._pending_cleanup:
                shutil.rmtree(path, ignore_errors=True)
            self._pending_cleanup.clear()
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
        # AWS Lambda Python runtime returns memory_limit_in_mb as a string
        self.memory_limit_in_mb = str(self.memory_limit_in_mb)

    def get_remaining_time_in_millis(self) -> int:
        elapsed = time.time() - self._start_time
        remaining = max(0, self._timeout - elapsed)
        return int(remaining * 1000)


def _clear_plain_modules_for_dir(code_dir: str) -> None:
    """Remove plain-named (non _lambda_* namespaced) modules from sys.modules whose
    __file__ lives inside code_dir.  Called before every execution to prevent
    helper modules (e.g. 'shared') from leaking across Lambda invocations.
    The function-scoped _lambda_* entries are intentionally left in place so that
    hot_reload=False caching still works for the top-level handler module.
    """
    norm_dir = os.path.abspath(code_dir) + os.sep
    to_remove = []
    for name, mod in list(sys.modules.items()):
        if name.startswith("_lambda_"):
            continue
        mod_file = getattr(mod, "__file__", None)
        if mod_file:
            try:
                if os.path.abspath(mod_file).startswith(norm_dir):
                    to_remove.append(name)
            except (ValueError, TypeError):
                pass
    for name in to_remove:
        sys.modules.pop(name, None)


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
            except (ValueError, TypeError) as exc:
                logger.debug("Could not resolve module path for %s: %s", name, exc)
    # Also clear function-scoped module cache keys (_lambda_* entries)
    for name in list(sys.modules.keys()):
        if name.startswith("_lambda_"):
            mod = sys.modules[name]
            mod_file = getattr(mod, "__file__", None)
            if mod_file:
                try:
                    if os.path.abspath(mod_file).startswith(norm_dir):
                        to_remove.append(name)
                except (ValueError, TypeError) as exc:
                    logger.debug("Could not resolve module path for %s: %s", name, exc)
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


def _format_stacktrace(tb_string: str) -> list[str]:
    """Format a traceback string into AWS Lambda stackTrace format.

    AWS Lambda's stackTrace field omits the 'Traceback (most recent call last):'
    header and the final exception line. It contains only the frame entries.
    """
    lines = tb_string.strip().split("\n")
    result = []
    for line in lines:
        # Skip the "Traceback (most recent call last):" header
        if line.startswith("Traceback"):
            continue
        # Skip the final exception line (e.g. "ValueError: test error")
        # These lines are not indented and contain the exception info
        # Frame entries from traceback are indented with spaces
        result.append(line)
    return result


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
    logs_output = io.StringIO()

    # Parse handler: "module.function" or "dir/module.function"
    parts = handler.rsplit(".", 1)
    if len(parts) != 2:
        msg = f"Bad handler format: {handler}"
        return (
            {"errorMessage": msg, "errorType": "Runtime.HandlerNotFound"},
            "Runtime.HandlerNotFound",
            msg,
        )
    module_path, func_name = parts

    # Determine working directory
    if code_dir:
        # Use provided directory directly (mount dir or pre-extracted)
        tmpdir = code_dir
    else:
        # Use code cache instead of extracting every time
        tmpdir = _code_cache.get_or_extract(function_name, code_zip, layer_zips)

    # get_or_extract auto-acquires a ref; we release it in the finally block
    try:
        # Always clear plain-named (non-namespaced) modules from this tmpdir.
        # Modules like "shared" are stored without a function-scoped prefix and
        # can pollute subsequent Lambda executions in the same process (test workers).
        # The _lambda_* namespaced entries are left intact to preserve hot_reload=False
        # caching behaviour for the top-level handler module.
        _clear_plain_modules_for_dir(tmpdir)
        if hot_reload and code_dir:
            _clear_modules_for_dir(code_dir)

        # Build per-invocation environment snapshot
        # Start from the real environ, then overlay Lambda-specific vars
        real_env = os.environ._real if isinstance(os.environ, _ThreadLocalEnviron) else os.environ
        invocation_env = dict(real_env)
        if env_vars:
            invocation_env.update(env_vars)
        invocation_env["AWS_LAMBDA_FUNCTION_NAME"] = function_name
        invocation_env["AWS_REGION"] = region
        invocation_env["AWS_DEFAULT_REGION"] = region
        invocation_env["AWS_ACCOUNT_ID"] = account_id
        # Ensure dummy credentials are present so boto3 doesn't raise NoCredentialsError
        # when Lambda functions call back to the local emulator. The emulator doesn't
        # validate credentials, but boto3 refuses to sign a request without them.
        invocation_env.setdefault("AWS_ACCESS_KEY_ID", "testing")
        invocation_env.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
        invocation_env.setdefault("AWS_SESSION_TOKEN", "testing")

        # Set up sys.path with lock for thread safety.
        # Track exactly which entries we add so we can remove them later
        # without corrupting other threads' additions.
        added_paths = []
        with _path_lock:
            sys.path.insert(0, tmpdir)
            added_paths.append(tmpdir)
            # AWS Lambda layers put Python code in python/ subdirectory
            python_subdir = os.path.join(tmpdir, "python")
            if os.path.isdir(python_subdir):
                sys.path.insert(1, python_subdir)
                added_paths.append(python_subdir)

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

        # Add START log line like real AWS Lambda
        logs_output.write(f"START RequestId: {request_id} Version: $LATEST\n")

        # Load the module
        module_file = os.path.join(tmpdir, module_path.replace(".", "/") + ".py")
        if not os.path.exists(module_file):
            # Try without directory nesting
            module_file = os.path.join(tmpdir, module_path.replace("/", ".") + ".py")
        if not os.path.exists(module_file):
            # Try just the filename
            module_file = os.path.join(tmpdir, module_path + ".py")

        if not os.path.exists(module_file):
            msg = f"Cannot find module: {module_path}"
            return (
                {"errorMessage": msg, "errorType": "Runtime.ImportModuleError"},
                "Runtime.ImportModuleError",
                msg,
            )

        # Use a function-scoped key so different Lambda functions don't collide
        modules_key = f"_lambda_{function_name}.{module_path}"

        # Capture print output
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = logs_output
        sys.stderr = logs_output

        try:
            # Reuse cached module when hot_reload is off (matches real Lambda behavior:
            # modules persist across invocations within the same execution environment)
            if not hot_reload and modules_key in sys.modules:
                module = sys.modules[modules_key]
            else:
                spec = importlib.util.spec_from_file_location(module_path, module_file)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                sys.modules[modules_key] = module
            handler_func = getattr(module, func_name, None)
            if handler_func is None:
                msg = f"Handler function '{func_name}' not found in {module_path}"
                return (
                    {"errorMessage": msg, "errorType": "Runtime.HandlerNotFound"},
                    "Runtime.HandlerNotFound",
                    msg,
                )

            # Execute handler with timeout enforcement and env var isolation
            handler_result = [None]
            handler_error = [None]

            def _run_handler():
                # Set thread-local environment so os.environ reads in
                # this thread see invocation-specific values
                _thread_local.env = dict(invocation_env)
                try:
                    handler_result[0] = handler_func(event, context)
                except Exception as exc:
                    handler_error[0] = exc
                finally:
                    _thread_local.env = None

            worker = threading.Thread(target=_run_handler, daemon=True)
            worker.start()
            worker.join(timeout=timeout)

            if worker.is_alive():
                # Timeout: try to kill the thread
                try:
                    tid = worker.ident
                    if tid is not None:
                        ctypes.pythonapi.PyThreadState_SetAsyncExc(
                            ctypes.c_ulong(tid), ctypes.py_object(SystemExit)
                        )
                except Exception:
                    pass
                # Add END/REPORT log lines
                elapsed_ms = timeout * 1000
                logs_output.write(f"END RequestId: {request_id}\n")
                logs_output.write(
                    f"REPORT RequestId: {request_id}"
                    f"\tDuration: {elapsed_ms:.2f} ms"
                    f"\tBilled Duration: {elapsed_ms} ms"
                    f"\tMemory Size: {memory_size} MB"
                    f"\tMax Memory Used: {memory_size} MB\n"
                )
                ts = time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                error_msg = f"{ts} {request_id} Task timed out after {timeout:.2f} seconds"
                error_result = {
                    "errorMessage": error_msg,
                    "errorType": "Task.TimedOut",
                }
                return error_result, "Task.TimedOut", logs_output.getvalue()

            if handler_error[0] is not None:
                e = handler_error[0]
                tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                logs_output.write(tb)
                # Add END/REPORT log lines
                elapsed_s = time.time() - context._start_time
                elapsed_ms = elapsed_s * 1000
                logs_output.write(f"END RequestId: {request_id}\n")
                logs_output.write(
                    f"REPORT RequestId: {request_id}"
                    f"\tDuration: {elapsed_ms:.2f} ms"
                    f"\tBilled Duration: {int(elapsed_ms) + 1} ms"
                    f"\tMemory Size: {memory_size} MB"
                    f"\tMax Memory Used: {memory_size} MB\n"
                )
                error_result = {
                    "errorMessage": str(e),
                    "errorType": type(e).__name__,
                    "stackTrace": _format_stacktrace(tb),
                }
                return error_result, "Handled", logs_output.getvalue()

            # Success: add END/REPORT log lines
            elapsed_s = time.time() - context._start_time
            elapsed_ms = elapsed_s * 1000
            logs_output.write(f"END RequestId: {request_id}\n")
            logs_output.write(
                f"REPORT RequestId: {request_id}"
                f"\tDuration: {elapsed_ms:.2f} ms"
                f"\tBilled Duration: {int(elapsed_ms) + 1} ms"
                f"\tMemory Size: {memory_size} MB"
                f"\tMax Memory Used: {memory_size} MB\n"
            )
            return handler_result[0], None, logs_output.getvalue()
        except Exception as e:
            # Catch module-level errors (SyntaxError, ImportError, etc.)
            tb = traceback.format_exc()
            logs_output.write(tb)
            error_result = {
                "errorMessage": str(e),
                "errorType": type(e).__name__,
                "stackTrace": _format_stacktrace(tb),
            }
            return error_result, "Handled", logs_output.getvalue()
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
    finally:
        # Remove only the paths we added (thread-safe: doesn't affect other threads)
        with _path_lock:
            for p in added_paths:
                try:
                    sys.path.remove(p)
                except ValueError:
                    pass
        # Release the cache reference
        _code_cache.release_ref(tmpdir)
        # Do NOT clean up tmpdir when using cache or mount dir --- managed externally
