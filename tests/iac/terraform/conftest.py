"""Terraform-specific fixtures for IaC tests."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from tests.iac.conftest import ENDPOINT_URL
from tests.iac.helpers.endpoint_config import generate_terraform_provider_override
from tests.iac.helpers.tool_runner import TerraformRunner


@pytest.fixture(scope="session")
def terraform_available():
    """Skip all Terraform tests if the ``terraform`` binary is not installed."""
    if shutil.which("terraform") is None:
        pytest.skip("terraform CLI not found on PATH")


@pytest.fixture(scope="module")
def tf_runner() -> TerraformRunner:
    """Provide a TerraformRunner instance."""
    return TerraformRunner()


@pytest.fixture(scope="module")
def terraform_dir(request, tmp_path_factory, test_run_id, terraform_available, tf_runner):
    """Prepare a Terraform scenario directory.

    Copies the scenario source files (located next to the test module) into a
    temporary directory, writes ``provider_override.tf``, runs ``terraform init``,
    and yields the working directory.  On teardown, runs ``terraform destroy``.
    """
    # Scenario source lives alongside the test file
    src_dir = Path(request.fspath).parent
    work_dir = tmp_path_factory.mktemp(f"tf-{test_run_id}")

    # Copy all .tf files and supporting source files into the work dir
    for tf_file in src_dir.glob("*.tf"):
        shutil.copy2(tf_file, work_dir / tf_file.name)
    for py_file in src_dir.glob("*.py"):
        if py_file.name.startswith("test_"):
            continue  # Don't copy test files into the Terraform work dir
        shutil.copy2(py_file, work_dir / py_file.name)

    # Write provider override pointing at robotocore
    generate_terraform_provider_override(work_dir, ENDPOINT_URL)

    # Initialise
    result = tf_runner.init(work_dir)
    if result.returncode != 0:
        pytest.fail(f"terraform init failed:\n{result.stderr}")

    yield work_dir

    # Teardown: best-effort destroy
    tf_runner.destroy(work_dir, auto_approve=True)
