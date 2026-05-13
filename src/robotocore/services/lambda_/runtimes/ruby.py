"""Ruby runtime executor — runs handler via subprocess."""

import logging
import os
import shutil

from robotocore.services.lambda_.runtimes.base import (
    build_env,
    extract_code,
    run_subprocess,
)

BOOTSTRAP_RB = os.path.join(os.path.dirname(__file__), "bootstraps", "bootstrap.rb")

logger = logging.getLogger(__name__)

# Maps Lambda runtime identifiers to the versioned ruby binary name in the image.
# The Dockerfile installs ruby3.2, ruby3.3, ruby3.4 alongside a default "ruby".
# Runtimes absent from this map fall back to plain "ruby" with a warning so
# divergence from the requested version is visible in logs.
_RUNTIME_BINARY: dict[str, str] = {
    "ruby3.2": "ruby3.2",
    "ruby3.3": "ruby3.3",
    "ruby3.4": "ruby3.4",
}


class RubyExecutor:
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
        ruby_bin = self._resolve_binary()
        if not ruby_bin:
            return None, "Runtime.InvalidRuntime", "Ruby not installed"

        tmpdir = extract_code(code_zip, layer_zips, code_dir=code_dir, function_name=function_name)
        env = build_env(function_name, region, account_id, timeout, memory_size, handler, env_vars)
        # Add load paths for layers (ruby/ subdirectory)
        rubyopt_parts = []
        ruby_dir = os.path.join(tmpdir, "ruby")
        if os.path.isdir(ruby_dir):
            rubyopt_parts.append(f"-I{ruby_dir}")
        rubyopt_parts.append(f"-I{tmpdir}")
        if rubyopt_parts:
            env["RUBYOPT"] = " ".join(rubyopt_parts)
        env["GEM_PATH"] = os.path.join(tmpdir, "ruby", "gems")

        cmd = [ruby_bin, BOOTSTRAP_RB]
        return run_subprocess(cmd, event, tmpdir, env, timeout)

    def _resolve_binary(self) -> str | None:
        """Return the ruby binary path, preferring the version-specific one."""
        versioned = _RUNTIME_BINARY.get(self._runtime)
        if versioned:
            path = shutil.which(versioned)
            if path:
                return path
        if self._runtime and self._runtime not in _RUNTIME_BINARY:
            logger.warning(
                "No versioned ruby binary for runtime %r — falling back to 'ruby'. Supported: %s",
                self._runtime,
                ", ".join(sorted(_RUNTIME_BINARY)),
            )
        return shutil.which("ruby")
