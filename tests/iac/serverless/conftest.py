"""Serverless Framework-specific fixtures for IaC tests."""

from __future__ import annotations

import shutil

import pytest

from tests.iac.helpers.tool_runner import ServerlessRunner


@pytest.fixture(scope="session")
def serverless_available():
    """Skip all Serverless tests if the binary is not installed."""
    if shutil.which("serverless") is None and shutil.which("sls") is None:
        pytest.skip("serverless/sls CLI not found on PATH")


@pytest.fixture(scope="module")
def sls_runner(serverless_available) -> ServerlessRunner:
    """Provide a ServerlessRunner instance."""
    return ServerlessRunner()
