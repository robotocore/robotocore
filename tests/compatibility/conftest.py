"""Shared fixtures for compatibility tests."""

import logging
import os
import shutil

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


@pytest.fixture(autouse=True, scope="session")
def _clear_chaos_rules_at_session_end():
    """Safety net: clear all chaos rules when a compat test session ends.

    If chaos tests somehow leak into a compat shard (e.g., due to CI
    misconfiguration), this prevents leftover rules from poisoning
    subsequent test runs on the same server.
    """
    yield
    try:
        resp = requests.post(f"{ENDPOINT_URL}/_robotocore/chaos/rules/clear", timeout=5)
        if resp.ok and resp.json().get("count", 0) > 0:
            logger.warning(
                "Session teardown cleared %d leftover chaos rules — "
                "chaos tests may have leaked into this shard",
                resp.json()["count"],
            )
    except Exception:
        pass  # server may already be stopped
