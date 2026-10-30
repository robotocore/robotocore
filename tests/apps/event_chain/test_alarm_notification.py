"""Tests for CloudWatch Alarm → SNS → SQS notification chain.

Verifies that metric data triggers alarms which notify via SNS to SQS.
"""

import json
import time
from datetime import UTC, datetime

from tests.apps.conftest import wait_for_messages


class TestAlarmNotification:
    """CloudWatch Alarm → SNS → SQS tests."""

    def test_alarm_fires_on_threshold_breach(self, chain, unique_name, sqs, cloudwatch):
        """Put metric above threshold → alarm fires → SNS → SQS message."""
        topic_arn = chain.create_topic(f"alarm-topic-{unique_name}")
        queue_url, queue_arn = chain.create_queue(f"alarm-queue-{unique_name}")
        chain.subscribe_sqs_to_sns(topic_arn, queue_arn)

        namespace = f"TestApp/{unique_name}"
        metric_name = "ErrorRate"

        chain.create_alarm_to_sns(
            alarm_name=f"high-errors-{unique_name}",
            namespace=namespace,
            metric_name=metric_name,
            threshold=50.0,
            topic_arn=topic_arn,
            comparison="GreaterThanThreshold",
            period=60,
            evaluation_periods=1,
            statistic="Average",
        )

        # Push metric data above threshold
        cloudwatch.put_metric_data(
            Namespace=namespace,
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Value": 75.0,
                    "Timestamp": datetime.now(UTC),
                    "Unit": "Percent",
                }
            ],
        )

        # Wait for alarm daemon to evaluate (10s interval + processing)
        messages = wait_for_messages(sqs, queue_url, timeout=30, expected=1)
        assert len(messages) >= 1, "No alarm notification received within 30s"

        body = json.loads(messages[0]["Body"])
        # SNS wraps the message — parse inner message
        if "Message" in body:
            alarm_msg = json.loads(body["Message"])
        else:
            alarm_msg = body

        # The real-CloudWatch notification envelope the alarm consumers
        # parse — every envelope field rides the SNS-delivered message.
        for field in ("NewStateValue", "AlarmName", "NewStateReason", "OldStateValue"):
            assert field in alarm_msg, f"alarm envelope missing {field}: {alarm_msg}"
        assert alarm_msg.get("NewStateValue") == "ALARM" or "ALARM" in str(alarm_msg)
        assert alarm_msg["OldStateValue"] in ("OK", "ALARM", "INSUFFICIENT_DATA")

    def test_alarm_returns_to_ok(self, chain, unique_name, sqs, cloudwatch):
        """Force alarm to ALARM state, push metric below threshold → OK notification."""
        topic_arn = chain.create_topic(f"ok-topic-{unique_name}")
        queue_url, queue_arn = chain.create_queue(f"ok-queue-{unique_name}")
        chain.subscribe_sqs_to_sns(topic_arn, queue_arn)

        namespace = f"TestOK/{unique_name}"
        metric_name = "Latency"
        alarm_name = f"high-latency-{unique_name}"

        chain.create_alarm_to_sns(
            alarm_name=alarm_name,
            namespace=namespace,
            metric_name=metric_name,
            threshold=100.0,
            topic_arn=topic_arn,
            comparison="GreaterThanThreshold",
            period=60,
            evaluation_periods=1,
            statistic="Average",
        )

        # Force alarm into ALARM state via API
        cloudwatch.set_alarm_state(
            AlarmName=alarm_name,
            StateValue="ALARM",
            StateReason="Forced for test",
        )

        # Push only below-threshold metric so next evaluation transitions to OK
        cloudwatch.put_metric_data(
            Namespace=namespace,
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Value": 50.0,
                    "Timestamp": datetime.now(UTC),
                    "Unit": "Milliseconds",
                }
            ],
        )

        # Wait for OK state notification (alarm daemon sees below-threshold,
        # transitions to OK) — scan the delivered envelopes for the OK one:
        # the queue also brings the forced-ALARM notification itself.
        deadline = time.time() + 30
        alarm_msg = None
        while time.time() < deadline and alarm_msg is None:
            recv = sqs.receive_message(
                QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2
            )
            for m in recv.get("Messages", []):
                body = json.loads(m["Body"])
                inner = json.loads(body["Message"]) if "Message" in body else body
                if inner.get("NewStateValue") == "OK":
                    alarm_msg = inner
                    break

        assert alarm_msg is not None, "OK notification not received within 30s"

        # The envelope fields ride the OK transition's message too.
        for field in ("NewStateValue", "AlarmName", "NewStateReason", "OldStateValue"):
            assert field in alarm_msg, f"alarm envelope missing {field}: {alarm_msg}"
        assert alarm_msg["AlarmName"] == alarm_name
        assert alarm_msg["OldStateValue"] == "ALARM"
        assert alarm_msg["NewStateReason"]
