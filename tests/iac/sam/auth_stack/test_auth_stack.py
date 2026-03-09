"""IaC test: sam - auth_stack."""

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.stack_deployer import delete_stack, deploy_and_yield, get_stack_outputs

pytestmark = pytest.mark.iac


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    template = (Path(__file__).parent / "template.yaml").read_text()
    stack_name = f"{test_run_id}-sam-auth-stack"
    stack = deploy_and_yield(stack_name, template)
    yield stack
    delete_stack(stack_name)


class TestAuthStack:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_user_pool_exists(self, deployed_stack, ensure_server):
        outputs = get_stack_outputs(deployed_stack)
        pool_id = outputs.get("UserPoolId")
        assert pool_id is not None, "UserPoolId output missing"

        cognito = make_client("cognito-idp")
        resp = cognito.describe_user_pool(UserPoolId=pool_id)
        pool = resp["UserPool"]
        assert pool["Id"] == pool_id
        assert pool["Policies"]["PasswordPolicy"]["MinimumLength"] == 8

    def test_user_pool_client_exists(self, deployed_stack, ensure_server):
        outputs = get_stack_outputs(deployed_stack)
        pool_id = outputs.get("UserPoolId")
        client_id = outputs.get("UserPoolClientId")
        assert pool_id is not None, "UserPoolId output missing"
        assert client_id is not None, "UserPoolClientId output missing"

        cognito = make_client("cognito-idp")
        resp = cognito.describe_user_pool_client(UserPoolId=pool_id, ClientId=client_id)
        client = resp["UserPoolClient"]
        assert client["ClientId"] == client_id
        assert "ALLOW_USER_PASSWORD_AUTH" in client.get("ExplicitAuthFlows", [])
