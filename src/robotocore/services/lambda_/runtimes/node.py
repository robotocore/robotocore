"""Node.js runtime executor — runs handler via subprocess."""

import os
import shutil

from robotocore.services.lambda_.runtimes.base import (
    build_env,
    extract_code,
    run_subprocess,
)

BOOTSTRAP_JS = os.path.join(os.path.dirname(__file__), "bootstraps", "bootstrap.js")


class NodejsExecutor:
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
        node_bin = shutil.which("node")
        if not node_bin:
            return None, "Runtime.InvalidRuntime", "Node.js not installed"

        tmpdir = extract_code(code_zip, layer_zips)
        env = build_env(function_name, region, account_id, timeout, memory_size, handler, env_vars)
        # Add node_modules from the extracted code to NODE_PATH
        node_modules = os.path.join(tmpdir, "node_modules")
        node_path_parts = [tmpdir]
        if os.path.isdir(node_modules):
            node_path_parts.append(node_modules)
        # Layers may put Node modules in nodejs/node_modules
        nodejs_dir = os.path.join(tmpdir, "nodejs")
        if os.path.isdir(nodejs_dir):
            node_path_parts.append(nodejs_dir)
            nodejs_modules = os.path.join(nodejs_dir, "node_modules")
            if os.path.isdir(nodejs_modules):
                node_path_parts.append(nodejs_modules)
        env["NODE_PATH"] = os.pathsep.join(node_path_parts)

        cmd = [node_bin, BOOTSTRAP_JS, handler]
        return run_subprocess(cmd, event, tmpdir, env, timeout)
