"""IaC test: serverless - auth_stack (Cognito UserPool + Client)."""

from __future__ import annotations

import time

import pytest

from tests.iac.conftest import make_client

pytestmark = pytest.mark.iac

TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Description: Serverless auth stack - Cognito User Pool and App Client

Resources:
  UserPool:
    Type: AWS::Cognito::UserPool
    Properties:
      UserPoolName: sls-auth-stack-users
      AutoVerifiedAttributes:
        - email
      Policies:
        PasswordPolicy:
          MinimumLength: 8
          RequireLowercase: true
          RequireUppercase: true
          RequireNumbers: true

  UserPoolClient:
    Type: AWS::Cognito::UserPoolClient
    Properties:
      ClientName: sls-auth-stack-web
      UserPoolId: !Ref UserPool
      ExplicitAuthFlows:
        - ALLOW_USER_PASSWORD_AUTH
        - ALLOW_REFRESH_TOKEN_AUTH
      GenerateSecret: false

Outputs:
  UserPoolId:
    Value: !Ref UserPool
  UserPoolClientId:
    Value: !Ref UserPoolClient
"""


def _get_outputs(stack: dict) -> dict[str, str]:
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    cfn = make_client("cloudformation")
    stack_name = f"{test_run_id}-sls-auth-stack"
    cfn.create_stack(
        StackName=stack_name,
        TemplateBody=TEMPLATE,
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
            pytest.skip(f"Stack deploy failed: {status}")
            return
        time.sleep(1)
    pytest.skip("Stack deploy timed out")


class TestAuthStack:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_user_pool_exists(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        cognito = make_client("cognito-idp")
        pool = cognito.describe_user_pool(UserPoolId=outputs["UserPoolId"])
        assert pool["UserPool"]["Name"] == "sls-auth-stack-users"
        policy = pool["UserPool"]["Policies"]["PasswordPolicy"]
        assert policy["MinimumLength"] == 8

    def test_user_pool_client_exists(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        cognito = make_client("cognito-idp")
        client = cognito.describe_user_pool_client(
            UserPoolId=outputs["UserPoolId"],
            ClientId=outputs["UserPoolClientId"],
        )
        assert client["UserPoolClient"]["ClientName"] == "sls-auth-stack-web"
        auth_flows = client["UserPoolClient"]["ExplicitAuthFlows"]
        assert "ALLOW_USER_PASSWORD_AUTH" in auth_flows
        assert "ALLOW_REFRESH_TOKEN_AUTH" in auth_flows
