"""IaC test: serverless - cicd_pipeline (S3 artifacts + IAM role + SNS)."""

from __future__ import annotations

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.stack_deployer import delete_stack, deploy_and_yield, get_stack_outputs

pytestmark = pytest.mark.iac

TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Description: Serverless CI/CD pipeline - S3 artifacts, IAM role, SNS notifications

Resources:
  ArtifactBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${AWS::StackName}-artifacts"

  NotificationTopic:
    Type: AWS::SNS::Topic
    Properties:
      TopicName: !Sub "${AWS::StackName}-notifications"

  PipelineRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub "${AWS::StackName}-role"
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Principal:
              Service: codepipeline.amazonaws.com
            Action: sts:AssumeRole
      Policies:
        - PolicyName: PipelinePolicy
          PolicyDocument:
            Version: "2012-10-17"
            Statement:
              - Effect: Allow
                Action:
                  - s3:GetObject
                  - s3:PutObject
                  - sns:Publish
                Resource: "*"

Outputs:
  BucketName:
    Value: !Ref ArtifactBucket
  TopicArn:
    Value: !Ref NotificationTopic
  RoleArn:
    Value: !GetAtt PipelineRole.Arn
  RoleName:
    Value: !Ref PipelineRole
"""


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    stack_name = f"{test_run_id}-sls-cicd-pipeline"
    stack = deploy_and_yield(stack_name, TEMPLATE)
    yield stack
    delete_stack(stack_name)


class TestCicdPipeline:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_artifact_bucket_exists(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        s3 = make_client("s3")
        resp = s3.head_bucket(Bucket=outputs["BucketName"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_sns_topic_exists(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        sns = make_client("sns")
        attrs = sns.get_topic_attributes(TopicArn=outputs["TopicArn"])
        assert outputs["TopicArn"] == attrs["Attributes"]["TopicArn"]

    def test_iam_role_exists(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        iam = make_client("iam")
        role = iam.get_role(RoleName=outputs["RoleName"])
        assert "codepipeline.amazonaws.com" in str(role["Role"]["AssumeRolePolicyDocument"])
        assert outputs["RoleArn"].startswith("arn:aws:iam::")
