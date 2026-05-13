"""Node.js runtime executor — runs handler via subprocess."""

import logging
import os
import shutil

from robotocore.services.lambda_.runtimes.base import (
    build_env,
    extract_code,
    run_subprocess,
)

BOOTSTRAP_JS = os.path.join(os.path.dirname(__file__), "bootstraps", "bootstrap.js")

logger = logging.getLogger(__name__)

# Maps Lambda runtime identifiers to the versioned node binary name in the image.
# The Dockerfile installs node18, node20, node22 alongside a default "node" (→ 20).
# Runtimes absent from this map (e.g. nodejs16.x) fall back to plain "node" with
# a warning so divergence from the requested version is visible in logs.
_RUNTIME_BINARY: dict[str, str] = {
    "nodejs18.x": "node18",
    "nodejs20.x": "node20",
    "nodejs22.x": "node22",
}


class NodejsExecutor:
    def __init__(self, runtime: str = "") -> None:
        self._runtime = runtime

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
        node_bin = self._resolve_binary()
        if not node_bin:
            return None, "Runtime.InvalidRuntime", "Node.js not installed"

        tmpdir = extract_code(code_zip, layer_zips, code_dir=code_dir, function_name=function_name)
        env = build_env(function_name, region, account_id, timeout, memory_size, handler, env_vars)
        node_modules = os.path.join(tmpdir, "node_modules")
        node_path_parts = [tmpdir]
        if os.path.isdir(node_modules):
            node_path_parts.append(node_modules)
        nodejs_dir = os.path.join(tmpdir, "nodejs")
        if os.path.isdir(nodejs_dir):
            node_path_parts.append(nodejs_dir)
            nodejs_modules = os.path.join(nodejs_dir, "node_modules")
            if os.path.isdir(nodejs_modules):
                node_path_parts.append(nodejs_modules)
        env["NODE_PATH"] = os.pathsep.join(node_path_parts)

        cmd = [node_bin, BOOTSTRAP_JS, handler]
        return run_subprocess(cmd, event, tmpdir, env, timeout)

    def _resolve_binary(self) -> str | None:
        """Return the node binary path, preferring the version-specific one.

        Attempts fault-in install when a known versioned binary is missing
        (see ``runtimes/install.py``). Logs a warning when we fall back to
        the default ``node`` so the Node-version mismatch is never silent.
        """
        versioned = _RUNTIME_BINARY.get(self._runtime)
        if versioned:
            path = shutil.which(versioned)
            if path:
                return path
            from robotocore.services.lambda_.runtimes import install as _install

            if _install.ensure_installed(self._runtime):
                path = shutil.which(versioned)
                if path:
                    return path
            logger.warning(
                "Versioned node binary %r for runtime %r not on $PATH and "
                "fault-in install unavailable — falling back to default "
                "'node' (the executed Node version will not match the "
                "function's declared runtime).",
                versioned,
                self._runtime,
            )
        elif self._runtime:
            logger.warning(
                "No versioned node binary for runtime %r — falling back to 'node'. Supported: %s",
                self._runtime,
                ", ".join(sorted(_RUNTIME_BINARY)),
            )
        return shutil.which("node")
