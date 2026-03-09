"""IaC test: serverless - monitoring (SNS + CloudWatch Alarm + Log Group)."""

from __future__ import annotations

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.stack_deployer import delete_stack, deploy_and_yield, get_stack_outputs

pytestmark = pytest.mark.iac

TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Description: Serverless monitoring - SNS topic, CloudWatch alarm, log group

Resources:
  AlarmTopic:
    Type: AWS::SNS::Topic
    Properties:
      TopicName: !Sub "${AWS::StackName}-alarms"

  AppLogGroup:
    Type: AWS::Logs::LogGroup
    Properties:
      LogGroupName: !Sub "/app/${AWS::StackName}"
      RetentionInDays: 14

  HighErrorAlarm:
    Type: AWS::CloudWatch::Alarm
    Properties:
      AlarmName: !Sub "${AWS::StackName}-high-errors"
      MetricName: Errors
      Namespace: !Sub "${AWS::StackName}"
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
    Value: !Sub "${AWS::StackName}-high-errors"
"""


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    stack_name = f"{test_run_id}-sls-monitoring"
    stack = deploy_and_yield(stack_name, TEMPLATE)
    yield stack
    delete_stack(stack_name)


class TestMonitoring:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_sns_topic_exists(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        sns = make_client("sns")
        attrs = sns.get_topic_attributes(TopicArn=outputs["TopicArn"])
        assert outputs["TopicArn"] == attrs["Attributes"]["TopicArn"]

    def test_log_group_exists(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        logs = make_client("logs")
        resp = logs.describe_log_groups(logGroupNamePrefix=outputs["LogGroupName"])
        groups = resp["logGroups"]
        assert len(groups) >= 1
        assert groups[0]["retentionInDays"] == 14

    def test_cloudwatch_alarm_exists(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        cw = make_client("cloudwatch")
        resp = cw.describe_alarms(AlarmNames=[outputs["AlarmName"]])
        alarms = resp["MetricAlarms"]
        assert len(alarms) == 1
        alarm = alarms[0]
        assert alarm["MetricName"] == "Errors"
        assert alarm["Threshold"] == 10.0
        assert alarm["ComparisonOperator"] == "GreaterThanThreshold"
