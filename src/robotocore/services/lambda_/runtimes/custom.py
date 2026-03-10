"""Custom runtime executor — runs the `bootstrap` binary from the code zip.

Used for provided, provided.al2, provided.al2023 runtimes (Go, Rust, etc.).
The code zip must contain a `bootstrap` executable.
"""

import os
import stat

from robotocore.services.lambda_.runtimes.base import (
    build_env,
    extract_code,
    run_subprocess,
)


class CustomRuntimeExecutor:
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
        tmpdir = extract_code(code_zip, layer_zips)
        env = build_env(function_name, region, account_id, timeout, memory_size, handler, env_vars)

        # Look for bootstrap executable
        bootstrap_path = os.path.join(tmpdir, "bootstrap")
        if not os.path.exists(bootstrap_path):
            # Some packages name it after the handler
            handler_base = handler.split(".")[0] if "." in handler else handler
            bootstrap_path = os.path.join(tmpdir, handler_base)
        if not os.path.exists(bootstrap_path):
            return (
                None,
                "Runtime.InvalidEntrypoint",
                "No 'bootstrap' executable found in code package",
            )

        # Ensure it's executable
        st = os.stat(bootstrap_path)
        os.chmod(bootstrap_path, st.st_mode | stat.S_IEXEC)

        cmd = [bootstrap_path]
        return run_subprocess(cmd, event, tmpdir, env, timeout)
