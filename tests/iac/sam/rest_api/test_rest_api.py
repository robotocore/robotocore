"""IaC test: sam - rest_api."""

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.stack_deployer import delete_stack, deploy_and_yield, get_stack_outputs

pytestmark = pytest.mark.iac


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    template = (Path(__file__).parent / "template.yaml").read_text()
    stack_name = f"{test_run_id}-sam-rest-api"
    stack = deploy_and_yield(stack_name, template)
    yield stack
    delete_stack(stack_name)


class TestRestApi:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_function_exists(self, deployed_stack, ensure_server):
        outputs = get_stack_outputs(deployed_stack)
        function_name = outputs.get("FunctionName")
        assert function_name is not None, "FunctionName output missing"

        lam = make_client("lambda")
        resp = lam.get_function(FunctionName=function_name)
        assert resp["Configuration"]["FunctionName"] == function_name

    def test_function_runtime(self, deployed_stack, ensure_server):
        outputs = get_stack_outputs(deployed_stack)
        function_name = outputs.get("FunctionName")
        assert function_name is not None

        lam = make_client("lambda")
        resp = lam.get_function(FunctionName=function_name)
        assert resp["Configuration"]["Runtime"] == "python3.12"
