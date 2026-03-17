"""Canary scheduler for CloudWatch Synthetics.

Background thread that runs canaries on their configured schedule.
Supports `rate(N minutes)` and cron expressions.
"""

import logging
import re
import threading
import time
import uuid

from moto.backends import get_backend

from robotocore.services.synthetics.executor import (
    CanaryRunResult,
    execute_canary,
    publish_canary_metrics,
    store_run,
)

logger = logging.getLogger(__name__)

# Minimum check interval in seconds
CHECK_INTERVAL = 5

# Singleton scheduler
_scheduler: "CanaryScheduler | None" = None
_scheduler_lock = threading.Lock()


def get_canary_scheduler() -> "CanaryScheduler":
    """Return the global CanaryScheduler singleton."""
    global _scheduler
    with _scheduler_lock:
        if _scheduler is None:
            _scheduler = CanaryScheduler()
        return _scheduler


def parse_rate_seconds(expression: str) -> int | None:
    """Parse a rate() expression into seconds.

    Examples:
        rate(5 minutes) -> 300
        rate(1 hour) -> 3600
        rate(0 minutes) -> None (disabled)
    """
    m = re.match(r"rate\(\s*(\d+)\s+(minute|minutes|hour|hours|day|days)\s*\)", expression)
    if not m:
        return None
    value = int(m.group(1))
    unit = m.group(2).rstrip("s")  # normalize to singular
    if value == 0:
        return None
    multipliers = {"minute": 60, "hour": 3600, "day": 86400}
    return value * multipliers.get(unit, 60)


def parse_cron_minutes(expression: str) -> int | None:
    """Parse a simple cron() expression to extract approximate interval in seconds.

    Supports patterns like:
        cron(0/5 * * * ? *)  -> every 5 minutes
        cron(0 * * * ? *)    -> every hour

    For complex cron patterns, defaults to 5 minutes.
    """
    m = re.match(r"cron\(\s*(.+?)\s*\)", expression)
    if not m:
        return None
    fields = m.group(1).split()
    if len(fields) < 5:
        return 300  # default 5 minutes

    minute_field = fields[0]
    # "0/N" means every N minutes
    step_match = re.match(r"(\d+)/(\d+)", minute_field)
    if step_match:
        step = int(step_match.group(2))
        return max(step * 60, 60)

    # "*/N" also means every N minutes
    step_match = re.match(r"\*/(\d+)", minute_field)
    if step_match:
        step = int(step_match.group(1))
        return max(step * 60, 60)

    # Single number in minutes, * in hours means hourly at that minute
    if minute_field.isdigit() and fields[1] == "*":
        return 3600

    # Default to 5 minutes for complex patterns
    return 300


class CanaryScheduler:
    """Background scheduler that executes canaries on their configured schedule."""

    def __init__(self) -> None:
        self._running = False
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        # Track last execution time per canary: (account, region, name) -> timestamp
        self._last_run: dict[tuple[str, str, str], float] = {}

    def start(self) -> None:
        """Start the scheduler background thread."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(
                target=self._run_loop, daemon=True, name="synthetics-scheduler"
            )
            self._thread.start()
            logger.info("Synthetics canary scheduler started (check_interval=%ds)", CHECK_INTERVAL)

    def stop(self) -> None:
        """Stop the scheduler."""
        with self._lock:
            self._running = False

    def is_running(self) -> bool:
        with self._lock:
            return self._running

    def _run_loop(self) -> None:
        """Main scheduling loop."""
        while self._running:
            try:
                self._check_canaries()
            except Exception:
                logger.exception("Error in synthetics scheduler loop")
            time.sleep(CHECK_INTERVAL)

    def _check_canaries(self) -> None:
        """Check all canaries across all accounts/regions for due executions."""
        try:
            syn_backends = get_backend("synthetics")
        except Exception:  # noqa: BLE001
            return

        now = time.monotonic()

        for account_id in list(syn_backends.keys()):
            account_backends = syn_backends[account_id]
            for region_name in list(account_backends.keys()):
                try:
                    backend = account_backends[region_name]
                except (KeyError, TypeError):
                    continue

                for canary in list(backend.canaries.values()):
                    if canary.state != "RUNNING":
                        continue

                    key = (account_id, region_name, canary.name)
                    schedule_expr = canary.schedule.get("Expression", "")
                    interval = parse_rate_seconds(schedule_expr) or parse_cron_minutes(
                        schedule_expr
                    )
                    if interval is None:
                        continue

                    last = self._last_run.get(key, 0)
                    if now - last >= interval:
                        self._last_run[key] = now
                        self._execute_canary(canary, account_id, region_name)

    def _execute_canary(self, canary, account_id: str, region: str) -> None:
        """Execute a single canary and store results."""
        try:
            code = canary.code or {}
            handler = code.get("Handler", "")
            # Inline script content or S3 reference
            code_content = code.get("Script", code.get("ZipFile"))

            timeout = 60
            run_config = canary.run_config
            if isinstance(run_config, dict):
                timeout = run_config.get("TimeoutInSeconds", 60)

            run_id = str(uuid.uuid4())
            result = execute_canary(
                canary_name=canary.name,
                runtime_version=canary.runtime_version,
                handler=handler,
                code_content=code_content,
                timeout_seconds=timeout,
                run_id=run_id,
            )

            # Store the run result
            store_run(account_id, region, canary.name, result)

            # Also update the Moto backend's canary run list
            _update_moto_canary_run(canary, result)

            # Publish CloudWatch metrics
            publish_canary_metrics(canary.name, result, account_id, region)

            logger.debug(
                "Canary %s executed: %s (%.1fms)",
                canary.name,
                result.status,
                result.duration_ms,
            )

        except Exception:
            logger.exception("Failed to execute canary %s", canary.name)

    def trigger_immediate(self, canary, account_id: str, region: str) -> CanaryRunResult:
        """Trigger an immediate canary execution (used by StartCanary)."""
        code = canary.code or {}
        handler = code.get("Handler", "")
        code_content = code.get("Script", code.get("ZipFile"))

        timeout = 60
        run_config = canary.run_config
        if isinstance(run_config, dict):
            timeout = run_config.get("TimeoutInSeconds", 60)

        run_id = str(uuid.uuid4())
        result = execute_canary(
            canary_name=canary.name,
            runtime_version=canary.runtime_version,
            handler=handler,
            code_content=code_content,
            timeout_seconds=timeout,
            run_id=run_id,
        )

        store_run(account_id, region, canary.name, result)
        _update_moto_canary_run(canary, result)
        publish_canary_metrics(canary.name, result, account_id, region)

        # Track execution time for scheduler
        key = (account_id, region, canary.name)
        self._last_run[key] = time.monotonic()

        return result


def _update_moto_canary_run(canary, result: CanaryRunResult) -> None:
    """Update the Moto canary object with a new run record."""
    from moto.synthetics.models import CanaryRun

    run = CanaryRun(canary_name=canary.name, status=result.status)
    run.id = result.run_id
    canary.runs.append(run)
    canary.last_run = run
