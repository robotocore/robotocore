"""Lambda Python subprocess bootstrap.

Used when the function's declared runtime (python3.10/3.11/3.13) doesn't match
the host Python that robotocore itself runs on (typically python3.12). The
parent process spawns the versioned binary with this bootstrap, reads the
event from stdin, and reads the result + handled-error JSON from stdout.

Contract (matches bootstrap.js / bootstrap.rb):
  * ``_HANDLER`` env var: ``module.function`` or ``path/module.function``
  * stdin: JSON-encoded event
  * stdout: JSON-encoded result, OR a ``{"errorMessage", "errorType"}``
    object when the handler raised
  * stderr: free-form logs (also captured by run_subprocess)
  * exit code: 0 on success, 1 on handled error, other on runtime failure

The bootstrap doesn't import Lambda-specific machinery — it's intentionally
minimal so it can run on whichever Python version the user requested without
requiring robotocore's deps to be installed under that Python.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import time
import traceback


def _build_context() -> object:
    """Lambda invocation context — a minimal duck-typed object."""
    deadline_ms = int(time.time() * 1000) + int(os.environ.get("AWS_LAMBDA_TIMEOUT_MS", "3000"))

    class Context:
        function_name = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "")
        function_version = os.environ.get("AWS_LAMBDA_FUNCTION_VERSION", "$LATEST")
        invoked_function_arn = os.environ.get("AWS_LAMBDA_FUNCTION_ARN", "")
        memory_limit_in_mb = os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "128")
        aws_request_id = os.environ.get("AWS_LAMBDA_REQUEST_ID", "")
        log_group_name = os.environ.get("AWS_LAMBDA_LOG_GROUP_NAME", "")
        log_stream_name = os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME", "")

        @staticmethod
        def get_remaining_time_in_millis() -> int:
            return max(0, deadline_ms - int(time.time() * 1000))

    return Context()


def _resolve_handler(handler: str):
    """Parse ``module.function`` (last dot splits) and import."""
    module_path, _, func_name = handler.rpartition(".")
    if not module_path:
        raise ValueError(f"Bad handler format: {handler!r}")
    # Allow path/to/module.func — convert path separators to dots.
    module_path = module_path.replace("/", ".")
    module = importlib.import_module(module_path)
    if not hasattr(module, func_name):
        raise AttributeError(f"Handler {func_name!r} not found in module {module_path!r}")
    return getattr(module, func_name)


def main() -> int:
    handler = os.environ.get("_HANDLER", "")
    try:
        event_text = sys.stdin.read()
        event = json.loads(event_text) if event_text else {}
    except json.JSONDecodeError as exc:
        sys.stdout.write(
            json.dumps({"errorMessage": f"Invalid event JSON: {exc}", "errorType": "Unhandled"})
        )
        return 1

    try:
        fn = _resolve_handler(handler)
    except ModuleNotFoundError as exc:
        sys.stdout.write(
            json.dumps(
                {
                    "errorMessage": f"Cannot find module: {exc.name}",
                    "errorType": "Runtime.ImportModuleError",
                }
            )
        )
        return 1
    except (AttributeError, ValueError) as exc:
        sys.stdout.write(
            json.dumps({"errorMessage": str(exc), "errorType": "Runtime.HandlerNotFound"})
        )
        return 1

    context = _build_context()
    try:
        result = fn(event, context)
    except Exception as exc:  # noqa: BLE001 — Lambda surfaces ANY handler error
        traceback.print_exc(file=sys.stderr)
        sys.stdout.write(
            json.dumps(
                {
                    "errorMessage": str(exc),
                    "errorType": type(exc).__name__,
                    "stackTrace": traceback.format_exception(type(exc), exc, exc.__traceback__),
                }
            )
        )
        return 1

    sys.stdout.write(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
