"""IaC test: pulumi - monitoring.

Validates CloudWatch alarm, SNS topic, and log group creation.
Resources are created via boto3 (mirroring the Pulumi program).
"""

from __future__ import annotations

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


@pytest.fixture(scope="module")
def monitoring_resources(sns_client, cloudwatch_client, logs_client):
    """Create SNS topic, CloudWatch alarm, and log group via boto3."""
    # SNS topic
    topic = sns_client.create_topic(Name="monitoring-alerts")
    topic_arn = topic["TopicArn"]

    # CloudWatch alarm
    cloudwatch_client.put_metric_alarm(
        AlarmName="high-cpu-alarm",
        MetricName="CPUUtilization",
        Namespace="AWS/EC2",
        Statistic="Average",
        Period=300,
        EvaluationPeriods=2,
        Threshold=80.0,
        ComparisonOperator="GreaterThanThreshold",
        AlarmActions=[topic_arn],
    )

    # Log group
    logs_client.create_log_group(logGroupName="/app/monitoring")
    logs_client.put_retention_policy(logGroupName="/app/monitoring", retentionInDays=7)

    yield {
        "topic_arn": topic_arn,
        "alarm_name": "high-cpu-alarm",
        "log_group_name": "/app/monitoring",
    }

    # Cleanup
    cloudwatch_client.delete_alarms(AlarmNames=["high-cpu-alarm"])
    logs_client.delete_log_group(logGroupName="/app/monitoring")
    sns_client.delete_topic(TopicArn=topic_arn)


class TestMonitoring:
    """Pulumi monitoring stack: SNS + CloudWatch alarm + log group."""

    def test_alarm_created(self, monitoring_resources):
        alarm_name = monitoring_resources["alarm_name"]
        topic_arn = monitoring_resources["topic_arn"]

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

    def test_log_group_created(self, monitoring_resources):
        log_group_name = monitoring_resources["log_group_name"]

        logs = make_client("logs")
        group = assert_log_group_exists(logs, log_group_name)
        assert group["logGroupName"] == log_group_name
        assert group["retentionInDays"] == 7

    def test_topic_created(self, monitoring_resources):
        topic_arn = monitoring_resources["topic_arn"]

        sns = make_client("sns")
        attrs = assert_sns_topic_exists(sns, topic_arn)
        assert attrs["TopicArn"] == topic_arn

    def test_publish_metric(self, monitoring_resources):
        """Publish a metric and verify the alarm is still describable."""
        alarm_name = monitoring_resources["alarm_name"]
        cw = make_client("cloudwatch")
        alarm = publish_metric_and_check_alarm(
            cw,
            "AWS/EC2",
            "CPUUtilization",
            alarm_name,
            90.0,
        )
        assert alarm["AlarmName"] == alarm_name

    def test_log_event_roundtrip(self, monitoring_resources):
        """Put a log event and query it back."""
        log_group_name = monitoring_resources["log_group_name"]
        logs = make_client("logs")
        events = put_log_event_and_query(
            logs,
            log_group_name,
            "test-stream",
            "functional test message",
        )
        assert any("functional test message" in e["message"] for e in events)

    def test_sns_to_sqs_notification(self, monitoring_resources):
        """Subscribe an SQS queue to the SNS topic and publish a message."""
        topic_arn = monitoring_resources["topic_arn"]
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
