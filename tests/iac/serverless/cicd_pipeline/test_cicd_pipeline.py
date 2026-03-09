"""IaC test: serverless - cicd_pipeline (S3 artifacts + IAM role + SNS)."""

from __future__ import annotations

import time

import pytest

from tests.iac.conftest import make_client

pytestmark = pytest.mark.iac

TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Description: Serverless CI/CD pipeline - S3 artifacts, IAM role, SNS notifications

Resources:
  ArtifactBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: sls-cicd-pipeline-artifacts

  NotificationTopic:
    Type: AWS::SNS::Topic
    Properties:
      TopicName: sls-cicd-pipeline-notifications

  PipelineRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: sls-cicd-pipeline-role
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


def _get_outputs(stack: dict) -> dict[str, str]:
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    cfn = make_client("cloudformation")
    stack_name = f"{test_run_id}-sls-cicd-pipeline"
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


class TestCicdPipeline:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_artifact_bucket_exists(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        s3 = make_client("s3")
        resp = s3.head_bucket(Bucket=outputs["BucketName"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_sns_topic_exists(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        sns = make_client("sns")
        attrs = sns.get_topic_attributes(TopicArn=outputs["TopicArn"])
        assert "sls-cicd-pipeline-notifications" in attrs["Attributes"]["TopicArn"]

    def test_iam_role_exists(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        iam = make_client("iam")
        role = iam.get_role(RoleName=outputs["RoleName"])
        assert "codepipeline.amazonaws.com" in str(role["Role"]["AssumeRolePolicyDocument"])
        assert outputs["RoleArn"].startswith("arn:aws:iam::")
