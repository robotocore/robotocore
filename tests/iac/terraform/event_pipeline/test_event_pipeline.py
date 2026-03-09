"""IaC test: terraform - event_pipeline.

Validates EventBridge rule, SQS queue, and SNS topic creation.
Resources are created via boto3 (mirroring the Terraform program).
"""

from __future__ import annotations

import json

import pytest

from tests.iac.conftest import ACCOUNT_ID, REGION
from tests.iac.helpers.functional_validator import send_and_receive_sqs

pytestmark = pytest.mark.iac


@pytest.fixture(scope="module")
def pipeline_resources(sqs_client, sns_client, events_client):
    """Create SQS queue, SNS topic, EventBridge rule via boto3."""
    # SQS queue
    q = sqs_client.create_queue(QueueName="tf-event-pipeline-queue")
    queue_url = q["QueueUrl"]
    queue_arn = f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:tf-event-pipeline-queue"

    # SNS topic
    topic = sns_client.create_topic(Name="tf-event-pipeline-topic")
    topic_arn = topic["TopicArn"]

    # SNS -> SQS subscription
    sub = sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
    )
    subscription_arn = sub["SubscriptionArn"]

    # EventBridge rule
    rule_name = "tf-event-pipeline-rule"
    events_client.put_rule(
        Name=rule_name,
        Description="Captures custom app events",
        EventPattern=json.dumps(
            {
                "source": ["my.app"],
                "detail-type": ["AppEvent"],
            }
        ),
        State="ENABLED",
    )

    # EventBridge target -> SQS
    events_client.put_targets(
        Rule=rule_name,
        Targets=[
            {
                "Id": "queue-target",
                "Arn": queue_arn,
            }
        ],
    )

    yield {
        "queue_url": queue_url,
        "queue_arn": queue_arn,
        "topic_arn": topic_arn,
        "rule_name": rule_name,
        "subscription_arn": subscription_arn,
    }

    # Cleanup
    events_client.remove_targets(Rule=rule_name, Ids=["queue-target"])
    events_client.delete_rule(Name=rule_name)
    sns_client.unsubscribe(SubscriptionArn=subscription_arn)
    sns_client.delete_topic(TopicArn=topic_arn)
    sqs_client.delete_queue(QueueUrl=queue_url)


class TestEventPipeline:
    """Validate event pipeline resources created by Terraform."""

    def test_queue_created(self, pipeline_resources, sqs_client):
        """Verify the SQS queue exists."""
        queue_url = pipeline_resources["queue_url"]
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["QueueArn"],
        )
        assert "Attributes" in attrs
        assert attrs["Attributes"]["QueueArn"] == pipeline_resources["queue_arn"]

    def test_topic_created(self, pipeline_resources, sns_client):
        """Verify the SNS topic exists."""
        topic_arn = pipeline_resources["topic_arn"]
        resp = sns_client.get_topic_attributes(TopicArn=topic_arn)
        assert resp["Attributes"]["TopicArn"] == topic_arn

    def test_rule_created(self, pipeline_resources, events_client):
        """Verify the EventBridge rule exists with correct event pattern."""
        rule_name = pipeline_resources["rule_name"]
        resp = events_client.describe_rule(Name=rule_name)
        assert resp["Name"] == rule_name
        assert resp["State"] == "ENABLED"
        assert "my.app" in resp["EventPattern"]

    def test_sqs_message_roundtrip(self, pipeline_resources, sqs_client):
        """Send a message to the SQS queue and receive it back."""
        queue_url = pipeline_resources["queue_url"]
        msg = send_and_receive_sqs(sqs_client, queue_url, '{"test": "message"}')
        assert msg["Body"] == '{"test": "message"}'
