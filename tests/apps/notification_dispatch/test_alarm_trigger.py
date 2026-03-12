"""Test CloudWatch Alarm → SNS → SQS trigger for notification dispatch.

Verifies that a metric breaching a threshold fires an alarm that
delivers a notification through the SNS→SQS chain.
"""

import json
import uuid
from datetime import UTC, datetime

from tests.apps.conftest import wait_for_messages


class TestAlarmTrigger:
    """CloudWatch Alarm → SNS → SQS notification chain."""

    def test_alarm_triggers_sns_notification(self, sns, sqs, cloudwatch):
        """Put metric above threshold, wait for alarm, verify SQS message."""
        suffix = uuid.uuid4().hex[:8]
        topic_name = f"nd-alarm-topic-{suffix}"
        queue_name = f"nd-alarm-queue-{suffix}"
        alarm_name = f"nd-error-alarm-{suffix}"
        namespace = f"NotifDispatch/{suffix}"

        # Create SNS → SQS chain
        topic_resp = sns.create_topic(Name=topic_name)
        topic_arn = topic_resp["TopicArn"]

        queue_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = queue_resp["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

        # Create alarm
        cloudwatch.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace=namespace,
            MetricName="FailedDeliveries",
            Threshold=5.0,
            ComparisonOperator="GreaterThanThreshold",
            Period=60,
            EvaluationPeriods=1,
            Statistic="Sum",
            AlarmActions=[topic_arn],
            TreatMissingData="notBreaching",
        )

        # Push metric above threshold
        cloudwatch.put_metric_data(
            Namespace=namespace,
            MetricData=[
                {
                    "MetricName": "FailedDeliveries",
                    "Value": 10.0,
                    "Timestamp": datetime.now(UTC),
                    "Unit": "Count",
                }
            ],
        )

        # Wait for alarm evaluation (10s daemon interval + processing)
        messages = wait_for_messages(sqs, queue_url, timeout=30, expected=1)
        assert len(messages) >= 1, "No alarm notification received within 30s"

        body = json.loads(messages[0]["Body"])
        if "Message" in body:
            alarm_msg = json.loads(body["Message"])
        else:
            alarm_msg = body

        assert "ALARM" in str(alarm_msg)

        # Cleanup
        cloudwatch.delete_alarms(AlarmNames=[alarm_name])
        sqs.delete_queue(QueueUrl=queue_url)
        sns.delete_topic(TopicArn=topic_arn)
