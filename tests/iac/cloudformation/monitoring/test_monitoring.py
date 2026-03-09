"""IaC test: cloudformation - monitoring.

Deploys an SNS Topic, CloudWatch Alarm (CPUUtilization > 80), and
CloudWatch LogGroup (7-day retention). Validates all resources.
"""

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.resource_validator import (
    assert_cloudwatch_alarm_exists,
    assert_log_group_exists,
    assert_sns_topic_exists,
)

pytestmark = pytest.mark.iac

TEMPLATE = (Path(__file__).parent / "template.yaml").read_text()


class TestMonitoring:
    """CloudFormation monitoring stack with SNS, CloudWatch Alarm, and LogGroup."""

    def test_deploy_and_validate(self, deploy_stack):
        stack = deploy_stack("monitoring", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        # Extract outputs
        outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
        assert "TopicArn" in outputs
        assert "AlarmName" in outputs
        assert "LogGroupName" in outputs

        topic_arn = outputs["TopicArn"]
        alarm_name = outputs["AlarmName"]
        log_group_name = outputs["LogGroupName"]

        # Validate SNS topic
        sns = make_client("sns")
        topic_attrs = assert_sns_topic_exists(sns, topic_arn)
        assert topic_attrs["TopicArn"] == topic_arn

        # Validate CloudWatch alarm
        cloudwatch = make_client("cloudwatch")
        alarm = assert_cloudwatch_alarm_exists(cloudwatch, alarm_name)
        assert alarm["MetricName"] == "CPUUtilization"
        assert alarm["Namespace"] == "AWS/EC2"
        assert alarm["Statistic"] == "Average"
        assert alarm["Period"] == 300
        assert alarm["EvaluationPeriods"] == 2
        assert alarm["Threshold"] == 80.0
        assert alarm["ComparisonOperator"] == "GreaterThanThreshold"
        assert topic_arn in alarm.get("AlarmActions", [])

        # Validate CloudWatch log group
        logs = make_client("logs")
        log_group = assert_log_group_exists(logs, log_group_name)
        assert log_group["retentionInDays"] == 7
