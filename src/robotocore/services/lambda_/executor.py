"""Lambda function executor — runs Python Lambda code in-process.

For Python runtimes, executes the handler function directly without Docker.
For other runtimes, falls back to Moto's Docker-based execution.
"""

import base64
import importlib.util
import io
import os
import sys
import tempfile
import time
import traceback
import uuid
import zipfile
from dataclasses import dataclass, field


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


@dataclass
class LambdaContext:
    """Mock AWS Lambda context object."""

    function_name: str
    function_version: str = "$LATEST"
    memory_limit_in_mb: int = 128
    invoked_function_arn: str = ""
    aws_request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    log_group_name: str = ""
    log_stream_name: str = ""
    _timeout: int = 3
    _start_time: float = field(default_factory=time.time)

    def get_remaining_time_in_millis(self) -> int:
        elapsed = time.time() - self._start_time
        remaining = max(0, self._timeout - elapsed)
        return int(remaining * 1000)


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
) -> tuple[dict | str | None, str | None, str]:
    """Execute a Python Lambda handler in-process.

    Returns (result, error_type, logs).
    """
    logs_output = io.StringIO()

    # Parse handler: "module.function" or "dir/module.function"
    parts = handler.rsplit(".", 1)
    if len(parts) != 2:
        return None, "Runtime.HandlerNotFound", f"Bad handler format: {handler}"
    module_path, func_name = parts

    # Extract zip to temp dir
    tmpdir = tempfile.mkdtemp(prefix="lambda_")
    try:
        # Extract layers first (so function code can override layer files)
        if layer_zips:
            for layer_zip in layer_zips:
                try:
                    with zipfile.ZipFile(io.BytesIO(layer_zip)) as zf:
                        zf.extractall(tmpdir)
                except Exception:
                    pass  # Skip invalid layer zips

        with zipfile.ZipFile(io.BytesIO(code_zip)) as zf:
            zf.extractall(tmpdir)

        # Set up environment
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

        # Build context
        context = LambdaContext(
            function_name=function_name,
            memory_limit_in_mb=memory_size,
            invoked_function_arn=f"arn:aws:lambda:{region}:{account_id}:function:{function_name}",
            log_group_name=f"/aws/lambda/{function_name}",
            log_stream_name=f"{time.strftime('%Y/%m/%d')}/[$LATEST]{uuid.uuid4().hex[:32]}",
            _timeout=timeout,
        )

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

        spec = importlib.util.spec_from_file_location(module_path, module_file)
        module = importlib.util.module_from_spec(spec)

        # Capture print output
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = logs_output
        sys.stderr = logs_output

        try:
            spec.loader.exec_module(module)
            handler_func = getattr(module, func_name, None)
            if handler_func is None:
                return (
                    None,
                    "Runtime.HandlerNotFound",
                    f"Handler function '{func_name}' not found in {module_path}",
                )

            result = handler_func(event, context)
            return result, None, logs_output.getvalue()
        except Exception as e:
            tb = traceback.format_exc()
            logs_output.write(tb)
            error_result = {
                "errorMessage": str(e),
                "errorType": type(e).__name__,
                "stackTrace": tb.strip().split("\n"),
            }
            return error_result, "Handled", logs_output.getvalue()
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
    finally:
        # Restore environment
        os.environ.clear()
        os.environ.update(old_env)
        sys.path[:] = old_path
        # Clean up temp dir
        import shutil

        shutil.rmtree(tmpdir, ignore_errors=True)
