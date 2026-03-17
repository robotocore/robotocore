"""Init hook support for Robotocore.

Supports running shell scripts at different lifecycle stages:
    /etc/robotocore/init/boot.d/    - Scripts run at startup
    /etc/robotocore/init/ready.d/   - Scripts run when server is ready
    /etc/robotocore/init/shutdown.d/ - Scripts run at shutdown

Scripts are executed in sorted order. Only *.sh files are executed.
Each script has a 30-second timeout.
"""

import logging
import os
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_HOOK_BASE = "/etc/robotocore/init"


def get_hook_base() -> str:
    """Return the base directory for init hooks."""
    return os.environ.get("ROBOTOCORE_INIT_DIR", DEFAULT_HOOK_BASE)


def run_init_hooks(stage: str) -> list[dict]:
    """Run all init hook scripts for the given stage.

    Args:
        stage: One of 'boot', 'ready', 'shutdown'

    Returns:
        List of dicts with script name, return code, and any output.
    """
    hook_dir = Path(get_hook_base()) / f"{stage}.d"
    if not hook_dir.exists():
        logger.debug("No hook directory found: %s", hook_dir)
        return []

    from robotocore.init.tracker import get_init_tracker

    tracker = get_init_tracker()

    results = []
    scripts = sorted(hook_dir.glob("*.sh"))
    if not scripts:
        logger.debug("No scripts found in %s", hook_dir)
        return []

    logger.info("Running %d %s hook(s) from %s", len(scripts), stage, hook_dir)

    # Record all scripts as pending first
    for script in scripts:
        if script.is_file():
            tracker.record_pending(script.name, stage)

    for script in scripts:
        if not script.is_file():
            continue
        logger.info("Running hook: %s", script.name)
        tracker.record_start(script.name, stage)
        start_time = __import__("time").monotonic()
        try:
            result = subprocess.run(
                ["bash", str(script)],
                timeout=30,
                capture_output=True,
                text=True,
            )
            elapsed = __import__("time").monotonic() - start_time
            results.append(
                {
                    "script": script.name,
                    "returncode": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                }
            )
            if result.returncode != 0:
                logger.warning(
                    "Hook %s exited with code %d: %s",
                    script.name,
                    result.returncode,
                    result.stderr,
                )
                tracker.record_failure(script.name, stage, error=result.stderr, duration=elapsed)
            else:
                logger.info("Hook %s completed successfully", script.name)
                tracker.record_complete(script.name, stage, duration=elapsed)
        except subprocess.TimeoutExpired:
            elapsed = __import__("time").monotonic() - start_time
            logger.error("Hook %s timed out after 30 seconds", script.name)
            results.append(
                {
                    "script": script.name,
                    "returncode": -1,
                    "stdout": "",
                    "stderr": "Timeout after 30 seconds",
                }
            )
            tracker.record_failure(
                script.name, stage, error="Timeout after 30 seconds", duration=elapsed
            )
        except Exception as exc:  # noqa: BLE001
            elapsed = __import__("time").monotonic() - start_time
            logger.error("Hook %s failed: %s", script.name, exc)
            results.append(
                {
                    "script": script.name,
                    "returncode": -1,
                    "stdout": "",
                    "stderr": str(exc),
                }
            )
            tracker.record_failure(script.name, stage, error=str(exc), duration=elapsed)

    return results
