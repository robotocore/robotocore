"""Lambda runtime executors — one per language family.

Each executor implements the same interface: given a code zip + handler + event,
execute the function and return (result, error_type, logs).

Runtime registry maps AWS runtime strings (e.g., "python3.12", "nodejs20.x")
to the appropriate executor.
"""

from __future__ import annotations

import logging
from typing import Protocol

logger = logging.getLogger(__name__)


class RuntimeExecutor(Protocol):
    """Interface every runtime executor must implement."""

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
        """Execute a Lambda handler. Returns (result, error_type, logs)."""
        ...


# Lazy-initialized executor singletons
_executors: dict[str, RuntimeExecutor] = {}


def _get_executor(runtime_family: str) -> RuntimeExecutor:
    """Get or create the executor singleton for a runtime family."""
    if runtime_family not in _executors:
        if runtime_family == "python":
            from robotocore.services.lambda_.runtimes.python import PythonExecutor

            _executors[runtime_family] = PythonExecutor()
        elif runtime_family == "nodejs":
            from robotocore.services.lambda_.runtimes.node import NodejsExecutor

            _executors[runtime_family] = NodejsExecutor()
        elif runtime_family == "ruby":
            from robotocore.services.lambda_.runtimes.ruby import RubyExecutor

            _executors[runtime_family] = RubyExecutor()
        elif runtime_family == "java":
            from robotocore.services.lambda_.runtimes.java import JavaExecutor

            _executors[runtime_family] = JavaExecutor()
        elif runtime_family == "dotnet":
            from robotocore.services.lambda_.runtimes.dotnet import DotnetExecutor

            _executors[runtime_family] = DotnetExecutor()
        elif runtime_family == "custom":
            from robotocore.services.lambda_.runtimes.custom import CustomRuntimeExecutor

            _executors[runtime_family] = CustomRuntimeExecutor()
        else:
            raise ValueError(f"Unknown runtime family: {runtime_family}")
    return _executors[runtime_family]


def runtime_to_family(runtime: str) -> str:
    """Map an AWS runtime string to its executor family.

    Examples:
        "python3.12" -> "python"
        "nodejs20.x" -> "nodejs"
        "java21" -> "java"
        "dotnet8" -> "dotnet"
        "ruby3.3" -> "ruby"
        "provided.al2023" -> "custom"
        "provided" -> "custom"
    """
    if not runtime:
        return "custom"
    r = runtime.lower()
    if r.startswith("python"):
        return "python"
    if r.startswith("nodejs") or r.startswith("node"):
        return "nodejs"
    if r.startswith("ruby"):
        return "ruby"
    if r.startswith("java"):
        return "java"
    if r.startswith("dotnet") or r.startswith(".net"):
        return "dotnet"
    if r.startswith("provided") or r.startswith("go"):
        return "custom"
    logger.warning("Unknown runtime %r, falling back to custom", runtime)
    return "custom"


def get_executor_for_runtime(runtime: str) -> RuntimeExecutor:
    """Get the executor for a given AWS runtime string."""
    family = runtime_to_family(runtime)
    return _get_executor(family)


def clear_executor_cache() -> None:
    """Clear cached executor singletons (for testing)."""
    _executors.clear()
