"""CDK-specific fixtures for IaC tests."""

from __future__ import annotations

import shutil

import pytest

from tests.iac.helpers.tool_runner import CdkRunner


@pytest.fixture(scope="session")
def cdk_available():
    """Skip all CDK tests if ``cdk`` or ``node`` binaries are not installed."""
    if shutil.which("node") is None:
        pytest.skip("node not found on PATH (required for CDK)")
    if shutil.which("cdk") is None:
        pytest.skip("cdk CLI not found on PATH")


@pytest.fixture(scope="module")
def cdk_runner(cdk_available) -> CdkRunner:
    """Provide a CdkRunner instance."""
    return CdkRunner()
