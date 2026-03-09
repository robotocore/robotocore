"""SAM-specific fixtures for IaC tests."""

from __future__ import annotations

import shutil

import pytest

from tests.iac.helpers.tool_runner import SamRunner


@pytest.fixture(scope="session")
def sam_available():
    """Skip all SAM tests if the ``sam`` binary is not installed."""
    if shutil.which("sam") is None:
        pytest.skip("sam CLI not found on PATH")


@pytest.fixture(scope="module")
def sam_runner(sam_available) -> SamRunner:
    """Provide a SamRunner instance."""
    return SamRunner()
