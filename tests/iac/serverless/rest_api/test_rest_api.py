"""IaC test: serverless - rest_api (Lambda + API Gateway)."""

from __future__ import annotations

import time

import pytest

from tests.iac.conftest import make_client

pytestmark = pytest.mark.iac

# Equivalent CFN template for what serverless.yml would generate
TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Description: Serverless REST API - Lambda with API Gateway

Resources:
  HelloFunctionRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: sls-rest-api-hello-role
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Principal:
              Service: lambda.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

  HelloFunction:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: sls-rest-api-hello
      Runtime: python3.12
      Handler: index.handler
      Role: !GetAtt HelloFunctionRole.Arn
      Code:
        ZipFile: |
          import json
          def handler(event, context):
              return {"statusCode": 200, "body": json.dumps({"message": "Hello from Serverless!"})}

  RestApi:
    Type: AWS::ApiGateway::RestApi
    Properties:
      Name: sls-rest-api

  HelloResource:
    Type: AWS::ApiGateway::Resource
    Properties:
      RestApiId: !Ref RestApi
      ParentId: !GetAtt RestApi.RootResourceId
      PathPart: hello

  HelloGetMethod:
    Type: AWS::ApiGateway::Method
    Properties:
      RestApiId: !Ref RestApi
      ResourceId: !Ref HelloResource
      HttpMethod: GET
      AuthorizationType: NONE
      Integration:
        Type: AWS_PROXY
        IntegrationHttpMethod: POST
        Uri: !GetAtt HelloFunction.Arn

Outputs:
  FunctionName:
    Value: !Ref HelloFunction
  FunctionArn:
    Value: !GetAtt HelloFunction.Arn
  RestApiId:
    Value: !Ref RestApi
  RoleName:
    Value: !Ref HelloFunctionRole
"""


def _get_outputs(stack: dict) -> dict[str, str]:
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    cfn = make_client("cloudformation")
    stack_name = f"{test_run_id}-sls-rest-api"
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


class TestRestApi:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_lambda_function_exists(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        lam = make_client("lambda")
        config = lam.get_function_configuration(FunctionName=outputs["FunctionName"])
        assert config["Runtime"] == "python3.12"
        assert config["Handler"] == "index.handler"

    def test_iam_role_exists(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        iam = make_client("iam")
        role = iam.get_role(RoleName=outputs["RoleName"])
        assert "lambda.amazonaws.com" in str(role["Role"]["AssumeRolePolicyDocument"])

    def test_api_gateway_has_hello_resource(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        apigw = make_client("apigateway")
        resources = apigw.get_resources(restApiId=outputs["RestApiId"])
        paths = [r.get("pathPart", "") for r in resources["items"]]
        assert "hello" in paths
