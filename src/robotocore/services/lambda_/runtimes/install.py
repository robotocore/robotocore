"""On-demand fault-in installer for Lambda runtimes.

When an executor's ``_resolve_binary()`` looks up a versioned binary
(``ruby3.3``, ``java17``, ``python3.10``, etc.) and doesn't find one on
``$PATH``, it asks this module to install it. We download from each
language's trusted source, extract into ``/var/lib/robotocore/runtimes/``,
and write a wrapper at ``/usr/local/bin/<rt>``. Subsequent invocations
find the wrapper via ``shutil.which`` and skip this path entirely.

Key properties:

* **Idempotent**: an ``.installed`` marker per runtime makes re-checks
  O(1).
* **Concurrency-safe**: ``flock``-based per-runtime file lock so two
  Lambda invocations that race to use the same new runtime only
  download once.
* **Blocking**: the invocation that triggers fault-in waits (the
  alternative — failing or silently downgrading — masks the version
  mismatch). First call may take 30s–2min; logs report progress.
* **Opt-out**: ``ROBOTOCORE_RUNTIME_FAULTIN=disabled`` keeps installs
  off (useful in air-gapped CI).

The per-language ``InstallPlan`` subclasses live in this module too —
each knows how to fetch its tarball/image, extract the right files,
and write a wrapper script. They never import each other, so adding a
new family is a small, contained patch.
"""

from __future__ import annotations

import contextlib
import fcntl
import logging
import os
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ config

_DEFAULT_CACHE_DIR = "/var/lib/robotocore/runtimes"
CACHE_DIR = Path(os.environ.get("ROBOTOCORE_RUNTIME_CACHE_DIR", _DEFAULT_CACHE_DIR))
WRAPPER_BIN_DIR = Path(os.environ.get("ROBOTOCORE_RUNTIME_BIN_DIR", "/usr/local/bin"))
DOWNLOAD_TIMEOUT_S = int(os.environ.get("ROBOTOCORE_RUNTIME_DOWNLOAD_TIMEOUT", "300"))
FAULTIN_DISABLED = os.environ.get("ROBOTOCORE_RUNTIME_FAULTIN", "").lower() in {
    "disabled",
    "0",
    "false",
    "off",
}


# ------------------------------------------------------------------ plan API


@dataclass
class InstallPlan:
    """Per-runtime installer. Subclasses implement ``install()``."""

    runtime: str  # AWS runtime identifier (e.g. "ruby3.3")
    family: str  # "ruby", "java", "python", "nodejs", "dotnet"
    prefix: Path  # /var/lib/robotocore/runtimes/ruby-3.3 (filled by subclass)
    binary_name: str  # /usr/local/bin/<binary_name> wrapper

    def is_installed(self) -> bool:
        return (self.prefix / ".installed").is_file()

    def install(self) -> None:  # pragma: no cover - override in subclass
        raise NotImplementedError

    # ---- shared helpers for subclasses ----

    def _mark_installed(self) -> None:
        self.prefix.mkdir(parents=True, exist_ok=True)
        (self.prefix / ".installed").write_text(f"installed at {time.time():.0f}\n")

    def _write_wrapper(self, body: str) -> None:
        """Drop a /usr/local/bin/<rt> shell script that exec's the right binary."""
        WRAPPER_BIN_DIR.mkdir(parents=True, exist_ok=True)
        wrapper = WRAPPER_BIN_DIR / self.binary_name
        wrapper.write_text(body)
        wrapper.chmod(0o755)


# ------------------------------------------------------------------ registry

# Lazy-populated to avoid heavyweight imports at module load. Each language
# module registers its plans via _register() when first needed.
_PLANS: dict[str, InstallPlan] = {}


def _register(plan: InstallPlan) -> None:
    _PLANS[plan.runtime] = plan


def _load_plans() -> None:
    """Import the language-specific plan modules once on first use."""
    if _PLANS:
        return
    # Importing these modules registers their plans via top-level calls.
    from robotocore.services.lambda_.runtimes import (  # noqa: F401 — side effects
        install_dotnet,
        install_java,
        install_node,
        install_python,
        install_ruby,
    )


# ------------------------------------------------------------------ locking


@contextlib.contextmanager
def _install_lock(runtime: str) -> Iterator[None]:
    """Per-runtime exclusive lock so concurrent invocations don't double-install.

    Falls back to a no-op lock when the cache dir isn't writable (the
    caller's ``install()`` will then fail just-as-defensively). This
    matters in dev environments where ``/var/lib/robotocore`` isn't
    accessible — the test suite and host-only runs hit this path.
    """
    lock_dir = CACHE_DIR / ".locks"
    try:
        lock_dir.mkdir(parents=True, exist_ok=True)
        lock_path = lock_dir / f"{runtime}.lock"
        lock_path.touch(exist_ok=True)
        fd = os.open(lock_path, os.O_RDWR)
    except OSError as exc:
        logger.debug("Lock dir %s unwritable, proceeding lock-less: %s", lock_dir, exc)
        yield
        return
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError as exc:
            logger.debug("Unlock failed: %s", exc)
        os.close(fd)


# ------------------------------------------------------------------ public API


def get_plan(runtime: str) -> InstallPlan | None:
    """Return the install plan for a runtime, or None if not registered."""
    _load_plans()
    return _PLANS.get(runtime)


def list_plans() -> dict[str, InstallPlan]:
    """All registered plans keyed by AWS runtime identifier."""
    _load_plans()
    return dict(_PLANS)


def is_installed(runtime: str) -> bool:
    """True when the runtime's binary is already fault-in installed."""
    plan = get_plan(runtime)
    return plan is not None and plan.is_installed()


def ensure_installed(runtime: str) -> bool:
    """Install ``runtime`` if it isn't already. Returns True when ready.

    Blocking: the caller's invocation pauses until the install finishes.
    First-call cost is 30s–2min depending on family + cache state.
    Subsequent calls return True immediately after a single
    ``.installed`` stat.
    """
    if FAULTIN_DISABLED:
        return False
    plan = get_plan(runtime)
    if plan is None:
        return False
    try:
        if plan.is_installed():
            return True
    except OSError as exc:
        logger.debug("is_installed check failed for %r: %s", runtime, exc)
        return False
    logger.info("Fault-in installing Lambda runtime %r — first invocation will pause", runtime)
    start = time.monotonic()
    try:
        with _install_lock(runtime):
            # Re-check under the lock — another invocation may have just finished.
            if plan.is_installed():
                return True
            plan.install()
    except Exception as exc:  # noqa: BLE001 — install fail is recoverable
        logger.warning("Fault-in install of %r failed: %s", runtime, exc)
        return False
    elapsed = time.monotonic() - start
    logger.info("Fault-in install of %r completed in %.1fs", runtime, elapsed)
    return True
