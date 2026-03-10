"""Canary script executor for CloudWatch Synthetics.

Executes canary handler code (Python runtimes only). For Node.js runtimes,
returns mock success results. Captures execution result: success/failure,
duration, and error messages.
"""

import base64
import importlib
import logging
import sys
import tempfile
import threading
import traceback
import types
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# Maximum run history per canary (configurable)
MAX_RUNS_PER_CANARY = 100


@dataclass
class CanaryRunResult:
    """Result of a single canary execution."""

    run_id: str
    canary_name: str
    status: str  # "PASSED" or "FAILED"
    start_time: datetime
    end_time: datetime
    duration_ms: float
    error_message: str = ""
    state_reason_code: str = ""

    def to_dict(self) -> dict:
        return {
            "Id": self.run_id,
            "Name": self.canary_name,
            "Status": {
                "State": self.status,
                "StateReason": self.error_message,
                "StateReasonCode": self.state_reason_code,
            },
            "Timeline": {
                "Started": self.start_time.isoformat(timespec="milliseconds") + "Z",
                "Completed": self.end_time.isoformat(timespec="milliseconds") + "Z",
            },
            "ArtifactS3Location": "s3://cw-syn-results/canary",
        }


# Per-canary run storage: (account_id, region, canary_name) -> list of runs
_run_store: dict[tuple[str, str, str], list[CanaryRunResult]] = {}
_store_lock = threading.Lock()


def store_run(account_id: str, region: str, canary_name: str, result: CanaryRunResult) -> None:
    """Store a canary run result, keeping at most MAX_RUNS_PER_CANARY."""
    key = (account_id, region, canary_name)
    with _store_lock:
        if key not in _run_store:
            _run_store[key] = []
        runs = _run_store[key]
        runs.append(result)
        if len(runs) > MAX_RUNS_PER_CANARY:
            _run_store[key] = runs[-MAX_RUNS_PER_CANARY:]


def get_runs(account_id: str, region: str, canary_name: str) -> list[CanaryRunResult]:
    """Get stored runs for a canary."""
    key = (account_id, region, canary_name)
    with _store_lock:
        return list(_run_store.get(key, []))


def clear_runs(account_id: str, region: str, canary_name: str) -> None:
    """Clear stored runs for a canary."""
    key = (account_id, region, canary_name)
    with _store_lock:
        _run_store.pop(key, None)


def execute_canary(
    canary_name: str,
    runtime_version: str,
    handler: str,
    code_content: str | None,
    timeout_seconds: int = 60,
    run_id: str | None = None,
) -> CanaryRunResult:
    """Execute a canary handler and return the run result.

    For Python runtimes (syn-python-*), actually executes the handler code.
    For Node.js runtimes (syn-nodejs-*), returns a mock success.

    Args:
        canary_name: Name of the canary.
        runtime_version: e.g. "syn-python-selenium-3.0" or "syn-nodejs-puppeteer-9.1"
        handler: Handler string like "module_name.handler_function"
        code_content: Base64-encoded zip content or inline script.
        timeout_seconds: Max execution time.
        run_id: Optional run ID (generated if not provided).
    """
    import uuid

    if run_id is None:
        run_id = str(uuid.uuid4())

    start_time = datetime.now(tz=UTC)

    # Node.js runtimes: mock success
    if runtime_version.startswith("syn-nodejs"):
        end_time = datetime.now(tz=UTC)
        duration_ms = (end_time - start_time).total_seconds() * 1000
        return CanaryRunResult(
            run_id=run_id,
            canary_name=canary_name,
            status="PASSED",
            start_time=start_time,
            end_time=end_time,
            duration_ms=duration_ms,
            state_reason_code="CANARY_SUCCESS",
        )

    # Python runtimes: attempt actual execution
    return _execute_python_canary(
        canary_name=canary_name,
        handler=handler,
        code_content=code_content,
        timeout_seconds=timeout_seconds,
        run_id=run_id,
        start_time=start_time,
    )


def _execute_python_canary(
    canary_name: str,
    handler: str,
    code_content: str | None,
    timeout_seconds: int,
    run_id: str,
    start_time: datetime,
) -> CanaryRunResult:
    """Execute a Python canary handler."""
    if not code_content:
        end_time = datetime.now(tz=UTC)
        return CanaryRunResult(
            run_id=run_id,
            canary_name=canary_name,
            status="FAILED",
            start_time=start_time,
            end_time=end_time,
            duration_ms=(end_time - start_time).total_seconds() * 1000,
            error_message="No code content provided",
            state_reason_code="CANARY_FAILURE",
        )

    # Parse handler: "module_name.function_name"
    parts = handler.rsplit(".", 1) if handler else []
    if len(parts) != 2:
        end_time = datetime.now(tz=UTC)
        return CanaryRunResult(
            run_id=run_id,
            canary_name=canary_name,
            status="FAILED",
            start_time=start_time,
            end_time=end_time,
            duration_ms=(end_time - start_time).total_seconds() * 1000,
            error_message=f"Invalid handler format: {handler}. Expected module.function",
            state_reason_code="CANARY_FAILURE",
        )

    module_name, function_name = parts

    result_holder: list[CanaryRunResult] = []
    exception_holder: list[Exception] = []

    def _run_handler() -> None:
        try:
            # Try to decode base64 content and write to temp file
            try:
                script_bytes = base64.b64decode(code_content)
            except Exception:
                # If not base64, treat as raw script text
                script_bytes = code_content.encode("utf-8") if code_content else b""

            with tempfile.TemporaryDirectory() as tmpdir:
                script_path = Path(tmpdir) / f"{module_name}.py"
                script_path.write_bytes(script_bytes)

                # Add tmpdir to sys.path temporarily
                sys.path.insert(0, tmpdir)
                try:
                    # Load the module
                    spec = importlib.util.spec_from_file_location(module_name, script_path)
                    if spec is None or spec.loader is None:
                        raise ImportError(f"Cannot load module from {script_path}")
                    mod = types.ModuleType(module_name)
                    spec.loader.exec_module(mod)

                    # Get the handler function
                    handler_fn = getattr(mod, function_name, None)
                    if handler_fn is None:
                        raise AttributeError(
                            f"Handler function '{function_name}' "
                            f"not found in module '{module_name}'"
                        )

                    # Execute the handler
                    handler_fn()

                    end_time = datetime.now(tz=UTC)
                    result_holder.append(
                        CanaryRunResult(
                            run_id=run_id,
                            canary_name=canary_name,
                            status="PASSED",
                            start_time=start_time,
                            end_time=end_time,
                            duration_ms=(end_time - start_time).total_seconds() * 1000,
                            state_reason_code="CANARY_SUCCESS",
                        )
                    )
                finally:
                    sys.path.remove(tmpdir)
                    # Clean up module from sys.modules
                    sys.modules.pop(module_name, None)

        except Exception as exc:
            exception_holder.append(exc)

    # Run in a thread with timeout
    thread = threading.Thread(target=_run_handler, daemon=True)
    thread.start()
    thread.join(timeout=timeout_seconds)

    if thread.is_alive():
        # Timeout
        end_time = datetime.now(tz=UTC)
        return CanaryRunResult(
            run_id=run_id,
            canary_name=canary_name,
            status="FAILED",
            start_time=start_time,
            end_time=end_time,
            duration_ms=(end_time - start_time).total_seconds() * 1000,
            error_message=f"Canary execution timed out after {timeout_seconds}s",
            state_reason_code="CANARY_FAILURE",
        )

    if exception_holder:
        end_time = datetime.now(tz=UTC)
        exc = exception_holder[0]
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        return CanaryRunResult(
            run_id=run_id,
            canary_name=canary_name,
            status="FAILED",
            start_time=start_time,
            end_time=end_time,
            duration_ms=(end_time - start_time).total_seconds() * 1000,
            error_message=f"{type(exc).__name__}: {exc}\n{tb}",
            state_reason_code="CANARY_FAILURE",
        )

    if result_holder:
        return result_holder[0]

    # Should not happen, but just in case
    end_time = datetime.now(tz=UTC)
    return CanaryRunResult(
        run_id=run_id,
        canary_name=canary_name,
        status="FAILED",
        start_time=start_time,
        end_time=end_time,
        duration_ms=(end_time - start_time).total_seconds() * 1000,
        error_message="Unknown execution error",
        state_reason_code="CANARY_FAILURE",
    )


def publish_canary_metrics(
    canary_name: str,
    result: CanaryRunResult,
    account_id: str,
    region: str,
) -> None:
    """Publish canary run metrics to CloudWatch via Moto backend.

    Publishes:
      - CloudWatchSynthetics/CanaryName/SuccessPercent
      - CloudWatchSynthetics/CanaryName/Duration
    """
    try:
        from moto.backends import get_backend

        cw_backend = get_backend("cloudwatch")[account_id][region]

        success_value = 100.0 if result.status == "PASSED" else 0.0
        now = datetime.now(tz=UTC)

        # Build metric datum objects compatible with Moto's put_metric_data
        # Moto expects FakeMetricDatum-compatible args
        cw_backend.put_metric_data(
            namespace="CloudWatchSynthetics",
            metric_data=[
                {
                    "MetricName": "SuccessPercent",
                    "Dimensions": [{"Name": "CanaryName", "Value": canary_name}],
                    "Value": success_value,
                    "Unit": "Percent",
                    "Timestamp": now.isoformat(),
                },
                {
                    "MetricName": "Duration",
                    "Dimensions": [{"Name": "CanaryName", "Value": canary_name}],
                    "Value": result.duration_ms,
                    "Unit": "Milliseconds",
                    "Timestamp": now.isoformat(),
                },
            ],
        )
        logger.debug(
            "Published metrics for canary %s: success=%s, duration=%.1fms",
            canary_name,
            result.status,
            result.duration_ms,
        )
    except Exception:
        logger.exception("Failed to publish metrics for canary %s", canary_name)
