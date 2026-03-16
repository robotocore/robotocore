"""Robust CloudFormation stack deployment for SAM/Serverless/advanced tests.

Replaces the copy-pasted polling loops with a single reusable deployer
that uses CloudFormationRunner under the hood.
"""

from __future__ import annotations

from tests.iac.conftest import make_client
from tests.iac.helpers.tool_runner import CloudFormationRunner


def deploy_and_yield(
    stack_name: str,
    template_body: str,
    timeout: int = 120,
) -> dict:
    """Deploy a CFN stack and return the stack description.

    Unlike the fixture-based deploy_stack, this is a plain function that can
    be called from any fixture or test.  Returns the stack dict on success.
    Raises RuntimeError on failure (never silently skips).
    """
    client = make_client("cloudformation")
    runner = CloudFormationRunner(client)
    return runner.deploy_stack(stack_name, template_body, timeout=timeout)


def delete_stack(stack_name: str) -> None:
    """Delete a CFN stack (best-effort)."""
    client = make_client("cloudformation")
    runner = CloudFormationRunner(client)
    try:
        runner.delete_stack(stack_name)
    except Exception:
        pass  # best-effort cleanup


def get_stack_outputs(stack: dict) -> dict[str, str]:
    """Extract outputs from a stack description as {key: value}."""
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
