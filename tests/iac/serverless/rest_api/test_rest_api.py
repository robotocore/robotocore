"""IaC test: serverless - rest_api (Lambda + API Gateway)."""

from __future__ import annotations

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.stack_deployer import delete_stack, deploy_and_yield, get_stack_outputs

pytestmark = pytest.mark.iac

# Equivalent CFN template for what serverless.yml would generate
TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Description: Serverless REST API - Lambda with API Gateway

Resources:
  HelloFunctionRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub "${AWS::StackName}-hello-role"
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
      FunctionName: !Sub "${AWS::StackName}-hello"
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
      Name: !Sub "${AWS::StackName}-api"

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


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    stack_name = f"{test_run_id}-sls-rest-api"
    stack = deploy_and_yield(stack_name, TEMPLATE)
    yield stack
    delete_stack(stack_name)


class TestRestApi:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_lambda_function_exists(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        lam = make_client("lambda")
        config = lam.get_function_configuration(FunctionName=outputs["FunctionName"])
        assert config["Runtime"] == "python3.12"
        assert config["Handler"] == "index.handler"

    def test_iam_role_exists(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        iam = make_client("iam")
        role = iam.get_role(RoleName=outputs["RoleName"])
        assert "lambda.amazonaws.com" in str(role["Role"]["AssumeRolePolicyDocument"])

    def test_api_gateway_has_hello_resource(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        apigw = make_client("apigateway")
        resources = apigw.get_resources(restApiId=outputs["RestApiId"])
        paths = [r.get("pathPart", "") for r in resources["items"]]
        assert "hello" in paths
