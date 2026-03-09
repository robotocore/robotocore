"""IaC test: pulumi - monitoring.

Validates CloudWatch alarm, SNS topic, and log group creation via Pulumi.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.iac.conftest import ACCOUNT_ID, REGION, make_client
from tests.iac.helpers.functional_validator import (
    publish_metric_and_check_alarm,
    put_log_event_and_query,
    subscribe_sns_to_sqs_and_publish,
)
from tests.iac.helpers.resource_validator import (
    assert_cloudwatch_alarm_exists,
    assert_log_group_exists,
    assert_sns_topic_exists,
)

pytestmark = pytest.mark.iac

SCENARIO_DIR = Path(__file__).parent


@pytest.fixture(scope="module")
def stack_outputs(pulumi_runner):
    """Deploy the monitoring stack and return Pulumi outputs."""
    result = pulumi_runner.up(SCENARIO_DIR)
    if result.returncode != 0:
        pytest.fail(f"pulumi up failed:\n{result.stderr}")
    yield pulumi_runner.stack_output(SCENARIO_DIR)
    pulumi_runner.destroy(SCENARIO_DIR)


class TestMonitoring:
    """Pulumi monitoring stack: SNS + CloudWatch alarm + log group."""

    def test_alarm_created(self, stack_outputs):
        alarm_name = stack_outputs["alarm_name"]
        topic_arn = stack_outputs["topic_arn"]

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

    def test_log_group_created(self, stack_outputs):
        log_group_name = stack_outputs["log_group_name"]

        logs = make_client("logs")
        group = assert_log_group_exists(logs, log_group_name)
        assert group["logGroupName"] == log_group_name
        assert group["retentionInDays"] == 7

    def test_topic_created(self, stack_outputs):
        topic_arn = stack_outputs["topic_arn"]

        sns = make_client("sns")
        attrs = assert_sns_topic_exists(sns, topic_arn)
        assert attrs["TopicArn"] == topic_arn

    def test_publish_metric(self, stack_outputs):
        """Publish a metric and verify the alarm is still describable."""
        alarm_name = stack_outputs["alarm_name"]
        cw = make_client("cloudwatch")
        alarm = publish_metric_and_check_alarm(
            cw,
            "AWS/EC2",
            "CPUUtilization",
            alarm_name,
            90.0,
        )
        assert alarm["AlarmName"] == alarm_name

    def test_log_event_roundtrip(self, stack_outputs):
        """Put a log event and query it back."""
        log_group_name = stack_outputs["log_group_name"]
        logs = make_client("logs")
        events = put_log_event_and_query(
            logs,
            log_group_name,
            "test-stream",
            "functional test message",
        )
        assert any("functional test message" in e["message"] for e in events)

    def test_sns_to_sqs_notification(self, stack_outputs):
        """Subscribe an SQS queue to the SNS topic and publish a message."""
        topic_arn = stack_outputs["topic_arn"]
        sns = make_client("sns")
        sqs = make_client("sqs")
        q = sqs.create_queue(QueueName="mon-test-notify-queue")
        queue_url = q["QueueUrl"]
        queue_arn = f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:mon-test-notify-queue"
        msg = subscribe_sns_to_sqs_and_publish(
            sns,
            sqs,
            topic_arn,
            queue_arn,
            queue_url,
            "test alert",
        )
        assert msg is not None
