"""Terraform drift oracle: apply each stack to the twin, then plan.

A plan after apply that shows changes means the twin wrote state the
AWS provider does not recognize as matching — the provider's schema and
refresh logic act as a semantic oracle no hand-written expectation can
match: it names the exact field that diverges.

Requires the `terraform` CLI (skipped cleanly when absent) and network
access for `terraform init` (provider download — not live AWS). Runs in
the iac lane; each stack gets its own temp workspace and fresh names.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import uuid
from pathlib import Path

import pytest

from tests.iac.conftest import ENDPOINT_URL

STACKS = [
    d.name
    for d in sorted(Path(__file__).parent.iterdir())
    if d.is_dir() and (d / "main.tf").exists()
]

REGION = "us-east-1"

terraform_available = pytest.mark.skipif(
    shutil.which("terraform") is None, reason="terraform CLI not on PATH"
)

# The drift oracle is OPT-IN (`make drift`): several stacks still trip
# documented twin gaps at apply time, so the default iac lane (which
# runs `pytest tests/iac/terraform/`) must not collect it.
pytestmark = [pytest.mark.drift_oracle]

# The twin answers everything in one account; the provider must not
# resolve credentials through real AWS bootstrapping.
OVERRIDE_TF = """
provider "aws" {
  region                      = "{region}"
  access_key                  = "{account}"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_region_validation      = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true

  endpoints {
    ec2            = "{endpoint}"
    s3             = "{endpoint}"
    sqs            = "{endpoint}"
    sns            = "{endpoint}"
    dynamodb       = "{endpoint}"
    iam            = "{endpoint}"
    sts            = "{endpoint}"
    lambda         = "{endpoint}"
    kms            = "{endpoint}"
    secretsmanager = "{endpoint}"
    ssm            = "{endpoint}"
    logs           = "{endpoint}"
    events         = "{endpoint}"
    cloudwatch     = "{endpoint}"
    ecr            = "{endpoint}"
    ecs            = "{endpoint}"
    stepfunctions  = "{endpoint}"
    acm            = "{endpoint}"
    apigateway     = "{endpoint}"
    cloudformation = "{endpoint}"
  }
}
"""


def _run(cmd: list[str], cwd: str, env: dict) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True, timeout=600)


def _stack_env() -> dict:
    env = dict(os.environ)
    env["TF_IN_AUTOMATION"] = "1"
    env["TF_INPUT"] = "0"
    # A shared provider cache keeps `terraform init` cheap across stacks.
    cache = Path(tempfile.gettempdir()) / "tf-plugin-cache"
    cache.mkdir(exist_ok=True)
    env["TF_PLUGIN_CACHE_DIR"] = str(cache)
    return env


def _workspace(stack: Path, unique_suffix: str, tmp_path_factory) -> tuple[str, dict]:
    work = tmp_path_factory.mktemp(f"tfdrift-{stack.name}")
    # Copy the WHOLE stack dir: stacks reference local files (lambda
    # sources for archive_file) relative to the workspace root.
    shutil.copytree(stack, work, dirs_exist_ok=True)
    shutil.rmtree(work / "__pycache__", ignore_errors=True)
    # A unique 12-digit account per stack: the twin scopes all state by
    # the access key's account id, so stacks (and re-runs) cannot see
    # each other's resources.
    account = str(700000000000 + (int(unique_suffix, 16) % 99999999999))
    # Token replace, not str.format — the HCL block is full of braces.
    override = (
        OVERRIDE_TF.replace("{region}", REGION)
        .replace("{endpoint}", ENDPOINT_URL)
        .replace("{account}", account)
    )
    (work / "zz_override.tf").write_text(override)
    # Resource names: rewrite every string default in variables.tf to a
    # per-run suffixed value via auto.tfvars — static defaults collide
    # across stacks and runs (S3 bucket names are globally unique in AWS).
    vars_tf = work / "variables.tf"
    if vars_tf.exists():
        defaults = re.findall(
            r'variable\s+"([^"]+)"\s*\{[^}]*?default\s*=\s*"([^"]*)"', vars_tf.read_text(), re.S
        )
        # Suffix ONLY name-like variables: a "vpc_cidr" or AMI default is
        # a VALUE, and rewriting it breaks the stack ("invalid CIDR
        # address"). Names are what collide (S3 bucket names are globally
        # unique in AWS).
        if defaults:
            lines = []
            for name, default in defaults:
                if not (name.endswith("name") or "_name" in name or name == "name"):
                    continue
                if not default:
                    continue
                lines.append(f'{name} = "{default}-{unique_suffix[:6]}"')
            if lines:
                (work / "zz.auto.tfvars").write_text("\n".join(lines) + "\n")
    env = _stack_env()
    init = _run(["terraform", "init", "-input=false"], str(work), env)
    assert init.returncode == 0, f"terraform init failed:\n{init.stderr[-2000:]}"
    return str(work), env


def _plan_changes(work: str, env: dict) -> tuple[int, str]:
    """`terraform plan -detailed-exitcode`: 0 = no changes, 2 = changes,
    other = error."""
    plan = _run(
        ["terraform", "plan", "-detailed-exitcode", "-input=false", "-no-color"],
        work,
        env,
    )
    return plan.returncode, plan.stdout + plan.stderr


@terraform_available
@pytest.mark.parametrize("stack_name", STACKS)
@pytest.mark.iac
def test_apply_then_plan_shows_no_drift(stack_name, tmp_path_factory, request):
    stack = Path(__file__).parent / stack_name
    suffix = uuid.uuid4().hex[:8]
    work, env = _workspace(stack, suffix, tmp_path_factory)

    apply_result = _run(
        ["terraform", "apply", "-auto-approve", "-input=false", "-no-color"],
        work,
        env,
    )
    if apply_result.returncode != 0:
        pytest.fail(
            f"terraform apply failed (the twin rejected a resource or the "
            f"provider could not drive it — a fidelity finding either way):\n"
            f"{apply_result.stdout[-3000:]}\n{apply_result.stderr[-1000:]}"
        )

    code, output = _plan_changes(work, env)
    if code == 2:
        pytest.fail(
            "terraform plan after apply shows DRIFT — the twin's response "
            "does not satisfy the provider's refresh logic:\n" + output[-4000:]
        )
    elif code != 0:
        pytest.fail(f"terraform plan errored:\n{output[-3000:]}")


@terraform_available
def test_stacks_exist():
    """The oracle has stacks to chew on — guards against the dir moving."""
    assert len(STACKS) >= 3, f"only {STACKS} found"
