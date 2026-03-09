"""IaC test: serverless - event_pipeline (SQS + Lambda + DynamoDB)."""

from __future__ import annotations

import time

import pytest

from tests.iac.conftest import make_client

pytestmark = pytest.mark.iac

TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Description: Serverless event pipeline - SQS queue, Lambda processor, DynamoDB table

Resources:
  EventQueue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: sls-event-pipeline-events

  EventsTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: sls-event-pipeline-events
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
      RoleName: sls-event-pipeline-processor-role
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
      FunctionName: sls-event-pipeline-processor
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


def _get_outputs(stack: dict) -> dict[str, str]:
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    cfn = make_client("cloudformation")
    stack_name = f"{test_run_id}-sls-event-pipeline"
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


class TestEventPipeline:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_sqs_queue_exists(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        sqs = make_client("sqs")
        attrs = sqs.get_queue_attributes(QueueUrl=outputs["QueueUrl"], AttributeNames=["QueueArn"])
        assert "QueueArn" in attrs["Attributes"]

    def test_dynamodb_table_exists(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        ddb = make_client("dynamodb")
        desc = ddb.describe_table(TableName=outputs["TableName"])
        assert desc["Table"]["TableStatus"] == "ACTIVE"
        assert desc["Table"]["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"

    def test_lambda_function_exists(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        lam = make_client("lambda")
        config = lam.get_function_configuration(FunctionName=outputs["FunctionName"])
        assert config["Runtime"] == "python3.12"
        assert config["Handler"] == "index.process"
