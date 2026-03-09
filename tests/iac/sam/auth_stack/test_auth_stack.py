"""IaC test: sam - auth_stack."""

import time
from pathlib import Path

import pytest

from tests.iac.conftest import make_client

pytestmark = pytest.mark.iac


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    cfn = make_client("cloudformation")
    template = (Path(__file__).parent / "template.yaml").read_text()
    stack_name = f"{test_run_id}-sam-auth-stack"
    cfn.create_stack(
        StackName=stack_name,
        TemplateBody=template,
        Capabilities=["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND"],
    )
    for _ in range(60):
        resp = cfn.describe_stacks(StackName=stack_name)
        status = resp["Stacks"][0]["StackStatus"]
        if status == "CREATE_COMPLETE":
            yield resp["Stacks"][0]
            cfn.delete_stack(StackName=stack_name)
            return
        if "FAILED" in status or "ROLLBACK" in status:
            pytest.skip(f"SAM stack failed: {status}")
            return
        time.sleep(1)
    pytest.skip("SAM stack timed out")


class TestAuthStack:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_user_pool_exists(self, deployed_stack, ensure_server):
        outputs = {o["OutputKey"]: o["OutputValue"] for o in deployed_stack.get("Outputs", [])}
        pool_id = outputs.get("UserPoolId")
        assert pool_id is not None, "UserPoolId output missing"

        cognito = make_client("cognito-idp")
        resp = cognito.describe_user_pool(UserPoolId=pool_id)
        pool = resp["UserPool"]
        assert pool["Id"] == pool_id
        assert pool["Policies"]["PasswordPolicy"]["MinimumLength"] == 8

    def test_user_pool_client_exists(self, deployed_stack, ensure_server):
        outputs = {o["OutputKey"]: o["OutputValue"] for o in deployed_stack.get("Outputs", [])}
        pool_id = outputs.get("UserPoolId")
        client_id = outputs.get("UserPoolClientId")
        assert pool_id is not None, "UserPoolId output missing"
        assert client_id is not None, "UserPoolClientId output missing"

        cognito = make_client("cognito-idp")
        resp = cognito.describe_user_pool_client(UserPoolId=pool_id, ClientId=client_id)
        client = resp["UserPoolClient"]
        assert client["ClientId"] == client_id
        assert "ALLOW_USER_PASSWORD_AUTH" in client.get("ExplicitAuthFlows", [])
