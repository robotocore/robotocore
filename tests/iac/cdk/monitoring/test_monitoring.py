"""IaC test: cdk - monitoring.

Deploys an SNS Topic, CloudWatch Alarm (CPUUtilization > 80), and
CloudWatch LogGroup (7-day retention). Validates all resources.
"""

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
)

pytestmark = pytest.mark.iac

SCENARIO_DIR = Path(__file__).parent


class TestMonitoring:
    """CDK monitoring stack with SNS, CloudWatch Alarm, and LogGroup."""

    @pytest.fixture(autouse=True)
    def deploy(self, cdk_runner):
        """Deploy the CDK app and tear it down after tests."""
        result = cdk_runner.deploy(SCENARIO_DIR, "MonitoringStack")
        assert result.returncode == 0, f"cdk deploy failed: {result.stderr}"
        yield
        cdk_runner.destroy(SCENARIO_DIR, "MonitoringStack")

    def test_alarm_created(self):
        """Verify CloudWatch alarm exists with correct configuration."""
        cloudwatch = make_client("cloudwatch")
        alarm = assert_cloudwatch_alarm_exists(cloudwatch, "monitoring-cpu-alarm")
        assert alarm["MetricName"] == "CPUUtilization"
        assert alarm["Namespace"] == "AWS/EC2"
        assert alarm["Threshold"] == 80.0
        assert alarm["ComparisonOperator"] == "GreaterThanThreshold"

    def test_log_group_created(self):
        """Verify CloudWatch log group exists with retention."""
        logs = make_client("logs")
        log_group = assert_log_group_exists(logs, "monitoring-app-logs")
        assert log_group["retentionInDays"] == 7

    def test_sns_topic_created(self):
        """Verify SNS topic exists."""
        sns = make_client("sns")
        resp = sns.list_topics()
        topic_arns = [t["TopicArn"] for t in resp["Topics"]]
        matching = [a for a in topic_arns if "monitoring-alarm-topic" in a]
        assert len(matching) >= 1, "SNS topic 'monitoring-alarm-topic' not found"

    def test_publish_metric(self):
        """Publish a metric and verify the alarm is still describable."""
        cw = make_client("cloudwatch")
        alarm = publish_metric_and_check_alarm(
            cw,
            "AWS/EC2",
            "CPUUtilization",
            "monitoring-cpu-alarm",
            90.0,
        )
        assert alarm["AlarmName"] == "monitoring-cpu-alarm"

    def test_log_event_roundtrip(self):
        """Put a log event and query it back."""
        logs = make_client("logs")
        events = put_log_event_and_query(
            logs,
            "monitoring-app-logs",
            "test-stream",
            "functional test message",
        )
        assert any("functional test message" in e["message"] for e in events)

    def test_sns_to_sqs_notification(self):
        """Subscribe an SQS queue to the SNS topic and publish a message."""
        sns = make_client("sns")
        sqs = make_client("sqs")

        # Find the monitoring topic ARN
        resp = sns.list_topics()
        topic_arns = [t["TopicArn"] for t in resp["Topics"]]
        matching = [a for a in topic_arns if "monitoring-alarm-topic" in a]
        assert len(matching) >= 1, "SNS topic 'monitoring-alarm-topic' not found"
        topic_arn = matching[0]

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
