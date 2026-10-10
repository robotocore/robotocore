"""Shared fixtures for compatibility tests.

These suites talk to a long-lived robotocore process over HTTP (unlike
`tests/integration`, which boots an in-process server per module). Choose the
target with `ENDPOINT_URL` (defaults to `http://localhost:4566`); at collection
time the server must be fresh — see `pytest_collection_modifyitems` below for the guard.
"""

import logging
import os
import shutil
import sys

import boto3
import pytest
import requests
from botocore.config import Config

logger = logging.getLogger(__name__)

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")

# Cached result of the runtimes probe — set only on a successful HTTP 200 response
# so a transient "server not ready" during module collection won't permanently cache
# an empty set and cause entire test modules to skip incorrectly.
_runtimes_cache: frozenset[str] | None = None


def _server_available_runtimes() -> frozenset[str]:
    """Fetch available runtime families from the server, caching only on success."""
    global _runtimes_cache
    if _runtimes_cache is not None:
        return _runtimes_cache
    try:
        resp = requests.get(f"{ENDPOINT_URL}/_robotocore/runtimes", timeout=5)
        if resp.ok:
            _runtimes_cache = frozenset(resp.json().get("available", []))
            return _runtimes_cache
    except Exception:
        logger.debug("Could not reach /_robotocore/runtimes (server may not be up)", exc_info=True)
    return frozenset()


def skip_if_runtime_unavailable(
    family: str, *, also_requires: str | None = None
) -> pytest.MarkDecorator:
    """Return a pytest skip mark when *family* is absent from the running server.

    Use as a module-level pytestmark so tests are skipped (not errored) when
    the server does not have the required runtime binary installed.

    also_requires: optional local binary name (e.g. "javac") that the tests need
    on the test-runner host itself (e.g. for local compilation). If absent locally,
    the module is skipped even when the server supports the runtime.
    """
    available = _server_available_runtimes()
    if family not in available:
        return pytest.mark.skip(reason=f"Runtime '{family}' not available in server")
    if also_requires is not None and shutil.which(also_requires) is None:
        return pytest.mark.skip(
            reason=f"Local '{also_requires}' not found on PATH (required for test compilation)"
        )
    return pytest.mark.skipif(False, reason=f"Runtime '{family}' available")


def make_client(service_name: str, **kwargs):
    config_kwargs = {}
    if service_name == "s3":
        config_kwargs["s3"] = {"addressing_style": "path"}

    return boto3.client(
        service_name,
        endpoint_url=ENDPOINT_URL,
        region_name=kwargs.pop("region_name", "us-east-1"),
        aws_access_key_id=kwargs.pop("aws_access_key_id", "testing"),
        aws_secret_access_key=kwargs.pop("aws_secret_access_key", "testing"),
        config=Config(**config_kwargs),
        **kwargs,
    )


# NOTE: there is deliberately NO session-scoped admin-state teardown here.
# Under pytest-xdist every worker runs its own pytest session against the one
# shared server; a session-end reset from a *finishing* worker would wipe
# capacity profiles and chaos overrides that a still-running worker's tests
# depend on — the InsufficientInstanceCapacity pair failed in CI exactly that
# way (forensics showed chaos_override=None profile_count=0 mid-test). Tests
# whose fixtures mutate the admin plane must clean up per test (see
# test_ec2_capacity_profiles.py's `_clear_capacity_state`). The CI job's server
# dies when the job ends, so leftovers do not outlive the stage.


def _target_server_is_warm() -> bool:
    """True when ENDPOINT_URL points at a server older than the freshness limit.

    Unreachable servers are never "warm" — the suites themselves will fail loudly.
    ``ROBOTOCORE_COMPAT_ALLOW_WARM_SERVER=1`` opts out entirely.
    """
    if os.environ.get("ROBOTOCORE_COMPAT_ALLOW_WARM_SERVER", "0") == "1":
        return False
    max_uptime = float(os.environ.get("ROBOTOCORE_COMPAT_MAX_UPTIME", "600"))
    try:
        resp = requests.get(f"{ENDPOINT_URL}/_robotocore/health", timeout=5)
        uptime = float(resp.json().get("uptime_seconds", 0))
    except Exception:
        return False  # unreachable: let the tests fail themselves, loudly
    return uptime > max_uptime


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip compat tests, politely, when they target a server someone else owns.

    These suites are only meaningful against a robotocore whose state starts
    empty (CI boots a fresh server immediately before the run). Pointing them
    at a long-running robotocore produces phantom failures indistinguishable
    from real defects — but aborting every whole-tree run because
    localhost:4566 happens to be busy would be worse for a repo whose unit
    suites are serverless. When the guard trips, compat items are dropped and
    one warning is emitted; `scripts/local-ci.sh compat` and CI boot their own
    fresh server and are unaffected.
    """
    if not _target_server_is_warm():
        return
    compat_tests = [item for item in items if "/tests/compatibility/" in str(item.fspath)]
    kept = [item for item in items if item not in compat_tests]
    if compat_tests:
        items[:] = kept
        print(
            f"WARNING: skipped {len(compat_tests)} compatibility tests — the server at "
            f"{ENDPOINT_URL} is warm (uptime above the freshness limit), and these "
            "suites only compare against a freshly started robotocore. Boot one per "
            "run (scripts/local-ci.sh) or set ROBOTOCORE_COMPAT_ALLOW_WARM_SERVER=1.",
            sys.stderr,
        )
