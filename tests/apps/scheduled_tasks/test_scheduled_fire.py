"""Test EventBridge scheduled rule → SQS.

Verifies that a rate-based schedule rule fires and delivers to an SQS target,
proving the EventBridge scheduler daemon works.
"""

import json
import uuid

from tests.apps.conftest import wait_for_messages


class TestScheduledFire:
    """EventBridge scheduled rule → SQS target."""

    def test_rate_rule_fires_to_sqs(self, events, sqs):
        """rate(1 minute) rule → wait for scheduler daemon → verify SQS message."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"sched-rule-{suffix}"
        queue_name = f"sched-queue-{suffix}"

        # Create SQS queue
        queue_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = queue_resp["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        # Create scheduled rule (rate(1 minute) — scheduler checks every 5s)
        events.put_rule(
            Name=rule_name,
            ScheduleExpression="rate(1 minute)",
            State="ENABLED",
            Description="Test scheduled rule",
        )
        events.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    "Id": "sqs-target",
                    "Arn": queue_arn,
                    "Input": json.dumps({"scheduled": True, "rule": rule_name}),
                }
            ],
        )

        # Wait for scheduler daemon to fire (5s check interval, up to 15s)
        messages = wait_for_messages(sqs, queue_url, timeout=15, expected=1)
        assert len(messages) >= 1, "Scheduled rule did not fire within 15s"

        body = json.loads(messages[0]["Body"])
        assert body.get("scheduled") is True
        assert body.get("rule") == rule_name

        # Cleanup
        events.remove_targets(Rule=rule_name, Ids=["sqs-target"])
        events.delete_rule(Name=rule_name)
        sqs.delete_queue(QueueUrl=queue_url)
