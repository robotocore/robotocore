"""IaC test: serverless - event_pipeline (SQS + Lambda + DynamoDB)."""

from __future__ import annotations

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.stack_deployer import delete_stack, deploy_and_yield, get_stack_outputs

pytestmark = pytest.mark.iac

TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Description: Serverless event pipeline - SQS queue, Lambda processor, DynamoDB table

Resources:
  EventQueue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: !Sub "${AWS::StackName}-events"

  EventsTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: !Sub "${AWS::StackName}-events"
      AttributeDefinitions:
        - AttributeName: id
          AttributeType: S
      KeySchema:
        - AttributeName: id
          KeyType: HASH
      BillingMode: PAY_PER_REQUEST

  ProcessorRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub "${AWS::StackName}-processor-role"
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Principal:
              Service: lambda.amazonaws.com
            Action: sts:AssumeRole

  ProcessorFunction:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: !Sub "${AWS::StackName}-processor"
      Runtime: python3.12
      Handler: index.process
      Role: !GetAtt ProcessorRole.Arn
      Code:
        ZipFile: |
          import json
          def process(event, context):
              return {"statusCode": 200, "processed": len(event.get("Records", []))}

Outputs:
  QueueUrl:
    Value: !Ref EventQueue
  TableName:
    Value: !Ref EventsTable
  FunctionName:
    Value: !Ref ProcessorFunction
"""


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    stack_name = f"{test_run_id}-sls-event-pipeline"
    stack = deploy_and_yield(stack_name, TEMPLATE)
    yield stack
    delete_stack(stack_name)


class TestEventPipeline:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_sqs_queue_exists(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        sqs = make_client("sqs")
        attrs = sqs.get_queue_attributes(QueueUrl=outputs["QueueUrl"], AttributeNames=["QueueArn"])
        assert "QueueArn" in attrs["Attributes"]

    def test_dynamodb_table_exists(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        ddb = make_client("dynamodb")
        desc = ddb.describe_table(TableName=outputs["TableName"])
        assert desc["Table"]["TableStatus"] == "ACTIVE"
        assert desc["Table"]["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"

    def test_lambda_function_exists(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        lam = make_client("lambda")
        config = lam.get_function_configuration(FunctionName=outputs["FunctionName"])
        assert config["Runtime"] == "python3.12"
        assert config["Handler"] == "index.process"
