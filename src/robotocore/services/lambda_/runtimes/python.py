"""Python runtime executor.

Two execution paths:

* **In-process** (fast path): when the function's requested runtime matches
  the host Python (e.g. ``runtime="python3.12"`` on a host running 3.12),
  the handler is dispatched in-process via ``execute_python_handler``. This
  preserves robotocore's existing layer handling, hot reload, code caching,
  and ~zero per-invocation overhead.

* **Subprocess** (faithful per-version): when the requested runtime differs
  from the host (e.g. ``runtime="python3.10"`` on a 3.12 host), we exec the
  matching versioned binary (``/usr/local/bin/python3.10``) with
  ``bootstraps/bootstrap.py``. The handler runs under the requested Python's
  exact interpreter, syntax, and stdlib — at the cost of subprocess startup.

The Dockerfile installs python3.10, python3.11, and python3.13 alongside the
image's base python3.12. If the requested binary isn't on $PATH we fall back
to in-process with a warning rather than failing the invocation outright.
"""

import logging
import os
import shutil
import sys

from robotocore.services.lambda_.executor import execute_python_handler
from robotocore.services.lambda_.runtimes.base import (
    build_env,
    extract_code,
    run_subprocess,
)

BOOTSTRAP_PY = os.path.join(os.path.dirname(__file__), "bootstraps", "bootstrap.py")

logger = logging.getLogger(__name__)

# Lambda runtime identifier → (major, minor) of the matching CPython.
# Runtimes absent from this map (e.g. python3.7) trigger a warning when used.
_RUNTIME_BINARY: dict[str, tuple[int, int]] = {
    "python3.8": (3, 8),
    "python3.9": (3, 9),
    "python3.10": (3, 10),
    "python3.11": (3, 11),
    "python3.12": (3, 12),
    "python3.13": (3, 13),
}


class PythonExecutor:
    def __init__(self, runtime: str = "") -> None:
        self._runtime = runtime
        self._mismatch_warned = False

    def execute(
        self,
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
    ) -> tuple[dict | str | list | None, str | None, str]:
        # Subprocess path for cross-version requests (e.g. python3.10 on a
        # python3.12 host) when the versioned binary is installed.
        sub_bin = self._resolve_subprocess_binary()
        if sub_bin is not None:
            return self._execute_via_subprocess(
                sub_bin,
                code_zip=code_zip,
                handler=handler,
                event=event,
                function_name=function_name,
                timeout=timeout,
                memory_size=memory_size,
                env_vars=env_vars,
                region=region,
                account_id=account_id,
                layer_zips=layer_zips,
                code_dir=code_dir,
            )

        # In-process path: host Python matches OR no versioned binary available.
        self._warn_if_mismatch()
        return execute_python_handler(
            code_zip=code_zip,
            handler=handler,
            event=event,
            function_name=function_name,
            timeout=timeout,
            memory_size=memory_size,
            env_vars=env_vars,
            region=region,
            account_id=account_id,
            layer_zips=layer_zips,
            code_dir=code_dir,
            hot_reload=hot_reload,
        )

    def _resolve_subprocess_binary(self) -> str | None:
        """Return the path to a versioned python binary when one is required.

        Returns ``None`` when we should fall through to in-process — either
        because the runtime matches the host Python, or no runtime was
        specified, or the versioned binary isn't installed (and fault-in
        couldn't fetch it).
        """
        if not self._runtime:
            return None
        expected = _RUNTIME_BINARY.get(self._runtime)
        if expected is None:
            return None  # Unknown runtime — in-process with warning.
        host = (sys.version_info.major, sys.version_info.minor)
        if expected == host:
            return None  # Matching version — keep the in-process fast path.
        # The runtime ID doubles as the binary name (python3.10 → python3.10).
        path = shutil.which(self._runtime)
        if path:
            return path
        # Versioned binary not present — try fault-in install.
        from robotocore.services.lambda_.runtimes import install as _install

        if _install.ensure_installed(self._runtime):
            return shutil.which(self._runtime)
        return None

    def _execute_via_subprocess(
        self,
        python_bin: str,
        *,
        code_zip: bytes,
        handler: str,
        event: dict,
        function_name: str,
        timeout: int,
        memory_size: int,
        env_vars: dict | None,
        region: str,
        account_id: str,
        layer_zips: list[bytes] | None,
        code_dir: str | None,
    ) -> tuple[dict | str | list | None, str | None, str]:
        tmpdir = extract_code(code_zip, layer_zips, code_dir=code_dir, function_name=function_name)
        env = build_env(function_name, region, account_id, timeout, memory_size, handler, env_vars)
        # Make the user code + any layer paths importable from bootstrap.py.
        python_path_parts = [tmpdir]
        # Layers convention: `python/` at the layer root contains modules.
        py_layer_dir = os.path.join(tmpdir, "python")
        if os.path.isdir(py_layer_dir):
            python_path_parts.append(py_layer_dir)
        env["PYTHONPATH"] = os.pathsep.join(python_path_parts)
        # Surface a millisecond deadline so context.get_remaining_time_in_millis
        # is meaningful (the in-process path computes this differently).
        env["AWS_LAMBDA_TIMEOUT_MS"] = str(timeout * 1000)

        cmd = [python_bin, BOOTSTRAP_PY]
        return run_subprocess(cmd, event, tmpdir, env, timeout)

    def _warn_if_mismatch(self) -> None:
        """Warn once if the in-process Python doesn't match the requested runtime."""
        if self._mismatch_warned or not self._runtime:
            return
        expected = _RUNTIME_BINARY.get(self._runtime)
        host = (sys.version_info.major, sys.version_info.minor)
        if expected is None:
            logger.warning(
                "Unknown python runtime %r — executing in host Python %d.%d. Supported: %s",
                self._runtime,
                host[0],
                host[1],
                ", ".join(sorted(_RUNTIME_BINARY)),
            )
        elif expected != host:
            logger.warning(
                "Python runtime %r requested but neither host Python %d.%d nor "
                "versioned binary %r is available — falling back to in-process "
                "(handler may see wrong stdlib/syntax).",
                self._runtime,
                host[0],
                host[1],
                self._runtime,
            )
        self._mismatch_warned = True

    # Backwards-compatible alias used by existing tests.
    _check_version_match = _warn_if_mismatch
