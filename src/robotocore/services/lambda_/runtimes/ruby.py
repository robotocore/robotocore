"""Ruby runtime executor — runs handler via subprocess."""

import os
import shutil

from robotocore.services.lambda_.runtimes.base import (
    build_env,
    extract_code,
    run_subprocess,
)

BOOTSTRAP_RB = os.path.join(os.path.dirname(__file__), "bootstraps", "bootstrap.rb")


class RubyExecutor:
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
    ) -> tuple[dict | str | list | None, str | None, str]:
        ruby_bin = shutil.which("ruby")
        if not ruby_bin:
            return None, "Runtime.InvalidRuntime", "Ruby not installed"

        tmpdir = extract_code(code_zip, layer_zips)
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
