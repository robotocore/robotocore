"""IaC test: serverless - auth_stack (Cognito UserPool + Client)."""

from __future__ import annotations

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.stack_deployer import delete_stack, deploy_and_yield, get_stack_outputs

pytestmark = pytest.mark.iac

TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Description: Serverless auth stack - Cognito User Pool and App Client

Resources:
  UserPool:
    Type: AWS::Cognito::UserPool
    Properties:
      UserPoolName: !Sub "${AWS::StackName}-users"
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
      ClientName: !Sub "${AWS::StackName}-web"
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
  UserPoolName:
    Value: !Sub "${AWS::StackName}-users"
  ClientName:
    Value: !Sub "${AWS::StackName}-web"
"""


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    stack_name = f"{test_run_id}-sls-auth-stack"
    stack = deploy_and_yield(stack_name, TEMPLATE)
    yield stack
    delete_stack(stack_name)


class TestAuthStack:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_user_pool_exists(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        cognito = make_client("cognito-idp")
        pool = cognito.describe_user_pool(UserPoolId=outputs["UserPoolId"])
        assert pool["UserPool"]["Name"] == outputs["UserPoolName"]
        policy = pool["UserPool"]["Policies"]["PasswordPolicy"]
        assert policy["MinimumLength"] == 8

    def test_user_pool_client_exists(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        cognito = make_client("cognito-idp")
        client = cognito.describe_user_pool_client(
            UserPoolId=outputs["UserPoolId"],
            ClientId=outputs["UserPoolClientId"],
        )
        assert client["UserPoolClient"]["ClientName"] == outputs["ClientName"]
        auth_flows = client["UserPoolClient"]["ExplicitAuthFlows"]
        assert "ALLOW_USER_PASSWORD_AUTH" in auth_flows
        assert "ALLOW_REFRESH_TOKEN_AUTH" in auth_flows
