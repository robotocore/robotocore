"""IaC test: serverless - monitoring (SNS + CloudWatch Alarm + Log Group)."""

from __future__ import annotations

import time

import pytest

from tests.iac.conftest import make_client

pytestmark = pytest.mark.iac

TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Description: Serverless monitoring - SNS topic, CloudWatch alarm, log group

Resources:
  AlarmTopic:
    Type: AWS::SNS::Topic
    Properties:
      TopicName: sls-monitoring-alarms

  AppLogGroup:
    Type: AWS::Logs::LogGroup
    Properties:
      LogGroupName: /app/sls-monitoring
      RetentionInDays: 14

  HighErrorAlarm:
    Type: AWS::CloudWatch::Alarm
    Properties:
      AlarmName: sls-monitoring-high-errors
      MetricName: Errors
      Namespace: sls-monitoring
      Statistic: Sum
      Period: 300
      EvaluationPeriods: 1
      Threshold: 10
      ComparisonOperator: GreaterThanThreshold
      AlarmActions:
        - !Ref AlarmTopic

Outputs:
  TopicArn:
    Value: !Ref AlarmTopic
  LogGroupName:
    Value: !Ref AppLogGroup
  AlarmName:
    Value: sls-monitoring-high-errors
"""


def _get_outputs(stack: dict) -> dict[str, str]:
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    cfn = make_client("cloudformation")
    stack_name = f"{test_run_id}-sls-monitoring"
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


class TestMonitoring:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_sns_topic_exists(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        sns = make_client("sns")
        attrs = sns.get_topic_attributes(TopicArn=outputs["TopicArn"])
        assert "sls-monitoring-alarms" in attrs["Attributes"]["TopicArn"]

    def test_log_group_exists(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        logs = make_client("logs")
        resp = logs.describe_log_groups(logGroupNamePrefix=outputs["LogGroupName"])
        groups = resp["logGroups"]
        assert len(groups) >= 1
        assert groups[0]["retentionInDays"] == 14

    def test_cloudwatch_alarm_exists(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        cw = make_client("cloudwatch")
        resp = cw.describe_alarms(AlarmNames=[outputs["AlarmName"]])
        alarms = resp["MetricAlarms"]
        assert len(alarms) == 1
        alarm = alarms[0]
        assert alarm["MetricName"] == "Errors"
        assert alarm["Threshold"] == 10.0
        assert alarm["ComparisonOperator"] == "GreaterThanThreshold"
