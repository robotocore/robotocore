"""Pulumi-specific fixtures for IaC tests."""

from __future__ import annotations

import shutil

import pytest

from tests.iac.helpers.tool_runner import PulumiRunner


@pytest.fixture(scope="session")
def pulumi_available():
    """Skip all Pulumi tests if the ``pulumi`` binary is not installed."""
    if shutil.which("pulumi") is None:
        pytest.skip("pulumi CLI not found on PATH")


@pytest.fixture(scope="module")
def pulumi_runner(pulumi_available) -> PulumiRunner:
    """Provide a PulumiRunner instance."""
    return PulumiRunner()
