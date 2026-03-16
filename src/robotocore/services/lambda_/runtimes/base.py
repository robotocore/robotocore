"""Base subprocess executor shared by Node.js, Ruby, Java, .NET, and custom runtimes."""

import json
import logging
import os
import shutil
import subprocess

logger = logging.getLogger(__name__)


def extract_code(
    code_zip: bytes,
    layer_zips: list[bytes] | None = None,
    code_dir: str | None = None,
    function_name: str = "__subprocess__",
) -> str:
    """Extract code zip (and layers) to a temp directory. Returns the path.

    If code_dir is provided (e.g., from a mount directory), returns it directly
    without extracting. Caller is responsible for cleanup via shutil.rmtree()
    only when code_dir was NOT provided.

    Layers are extracted first so function code can override layer files.
    """
    if code_dir:
        return code_dir

    from robotocore.services.lambda_.executor import get_code_cache

    return get_code_cache().get_or_extract(
        function_name=function_name,
        code_zip=code_zip,
        layer_zips=layer_zips,
    )


def build_env(
    function_name: str,
    region: str,
    account_id: str,
    timeout: int,
    memory_size: int,
    handler: str,
    env_vars: dict | None = None,
) -> dict[str, str]:
    """Build the environment dict for a Lambda subprocess."""
    env = os.environ.copy()
    if env_vars:
        env.update(env_vars)
    env["AWS_LAMBDA_FUNCTION_NAME"] = function_name
    env["AWS_REGION"] = region
    env["AWS_DEFAULT_REGION"] = region
    env["AWS_ACCOUNT_ID"] = account_id
    env["AWS_LAMBDA_FUNCTION_MEMORY_SIZE"] = str(memory_size)
    env["AWS_LAMBDA_FUNCTION_TIMEOUT"] = str(timeout)
    env["_HANDLER"] = handler
    # Prevent SDK calls from hitting real AWS
    env.setdefault("AWS_ACCESS_KEY_ID", "testing")
    env.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    return env


def run_subprocess(
    cmd: list[str],
    event: dict,
    tmpdir: str,
    env: dict[str, str],
    timeout: int,
) -> tuple[dict | str | list | None, str | None, str]:
    """Run a subprocess with event JSON on stdin, parse result from stdout.

    Protocol: subprocess reads JSON event from stdin, writes JSON result to stdout,
    writes logs to stderr. Exit code 0 = success, non-zero = error.

    Returns (result, error_type, logs).
    """
    event_json = json.dumps(event)
    try:
        proc = subprocess.run(
            cmd,
            input=event_json,
            capture_output=True,
            text=True,
            timeout=timeout + 2,  # small grace period beyond Lambda timeout
            cwd=tmpdir,
            env=env,
        )
    except subprocess.TimeoutExpired:
        error_result = {
            "errorMessage": f"Task timed out after {timeout}.00 seconds",
            "errorType": "Task.TimedOut",
        }
        return error_result, "Task.TimedOut", f"Function timed out after {timeout}s"
    except FileNotFoundError as e:
        return None, "Runtime.InvalidRuntime", f"Runtime not found: {e}"

    logs = proc.stderr
    stdout = proc.stdout.strip()

    if proc.returncode != 0:
        # Check if stdout has a structured error
        if stdout:
            try:
                error_obj = json.loads(stdout)
                if isinstance(error_obj, dict) and "errorMessage" in error_obj:
                    return error_obj, "Handled", logs
            except json.JSONDecodeError as exc:
                logger.debug("run_subprocess: loads failed (non-fatal): %s", exc)
        return (
            {
                "errorMessage": stdout or logs.split("\n")[-1] if logs else "Unknown error",
                "errorType": "Runtime.ExitError",
                "stackTrace": logs.strip().split("\n") if logs else [],
            },
            "Unhandled",
            logs,
        )

    if not stdout:
        return None, None, logs

    try:
        result = json.loads(stdout)
    except json.JSONDecodeError:
        # Return raw string if not valid JSON
        result = stdout

    return result, None, logs


def cleanup(tmpdir: str) -> None:
    """Remove the temporary directory."""
    shutil.rmtree(tmpdir, ignore_errors=True)
