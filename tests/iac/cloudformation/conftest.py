"""CloudFormation-specific fixtures for IaC tests."""

from __future__ import annotations

import pytest

from tests.iac.helpers.tool_runner import CloudFormationRunner


@pytest.fixture(scope="module")
def cfn_runner(cloudformation) -> CloudFormationRunner:
    """Provide a CloudFormationRunner backed by the session's boto3 client."""
    return CloudFormationRunner(cloudformation)


@pytest.fixture
def deploy_stack(cfn_runner, test_run_id):
    """Deploy a CloudFormation stack and delete it on teardown.

    Usage::

        def test_something(deploy_stack):
            stack = deploy_stack("my-template", template_body)
            assert stack["StackStatus"] == "CREATE_COMPLETE"
    """
    created_stacks: list[str] = []

    def _deploy(name_suffix: str, template_body: str, params: dict | None = None) -> dict:
        stack_name = f"{test_run_id}-{name_suffix}"
        stack = cfn_runner.deploy_stack(stack_name, template_body, params)
        created_stacks.append(stack_name)
        return stack

    yield _deploy

    # Teardown: delete all stacks created during the test
    for name in reversed(created_stacks):
        try:
            cfn_runner.delete_stack(name)
        except Exception:
            pass  # best-effort cleanup
