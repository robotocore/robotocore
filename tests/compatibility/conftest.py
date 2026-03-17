"""Shared fixtures for compatibility tests."""

import logging
import os

import boto3
import pytest
import requests
from botocore.config import Config

logger = logging.getLogger(__name__)

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")


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
