"""IaC test: terraform - monitoring.

Validates SNS topic, CloudWatch alarm, and log group creation via Terraform.
"""

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.resource_validator import (
    assert_cloudwatch_alarm_exists,
    assert_log_group_exists,
    assert_sns_topic_exists,
)

pytestmark = pytest.mark.iac


class TestMonitoring:
    """Terraform monitoring stack: SNS + CloudWatch alarm + log group."""

    def test_apply_succeeds(self, terraform_dir, tf_runner):
        result = tf_runner.apply(terraform_dir)
        assert result.returncode == 0, f"terraform apply failed:\n{result.stderr}"

    def test_sns_topic_exists(self, terraform_dir, tf_runner):
        tf_runner.apply(terraform_dir)
        outputs = tf_runner.output(terraform_dir)
        topic_arn = outputs["topic_arn"]["value"]

        sns = make_client("sns")
        attrs = assert_sns_topic_exists(sns, topic_arn)
        assert attrs["TopicArn"] == topic_arn

    def test_alarm_configuration(self, terraform_dir, tf_runner):
        tf_runner.apply(terraform_dir)
        outputs = tf_runner.output(terraform_dir)
        alarm_name = outputs["alarm_name"]["value"]
        topic_arn = outputs["topic_arn"]["value"]

        cloudwatch = make_client("cloudwatch")
        alarm = assert_cloudwatch_alarm_exists(cloudwatch, alarm_name)
        assert alarm["MetricName"] == "CPUUtilization"
        assert alarm["Namespace"] == "AWS/EC2"
        assert alarm["Threshold"] == 80.0
        assert alarm["Period"] == 300
        assert alarm["EvaluationPeriods"] == 2
        assert alarm["Statistic"] == "Average"
        assert alarm["ComparisonOperator"] == "GreaterThanThreshold"
        assert topic_arn in alarm["AlarmActions"]

    def test_log_group_exists(self, terraform_dir, tf_runner):
        tf_runner.apply(terraform_dir)
        outputs = tf_runner.output(terraform_dir)
        log_group_name = outputs["log_group_name"]["value"]

        logs = make_client("logs")
        group = assert_log_group_exists(logs, log_group_name)
        assert group["logGroupName"] == log_group_name
        assert group["retentionInDays"] == 7
