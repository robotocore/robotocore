"""Cross-region integration tests verifying SNS→SQS and EventBridge→SQS
delivery works when the source and target are in different regions.

These tests exercise the bug fixes from Phase 1B and 1C.
"""

import json
import time
import uuid


class TestSNSCrossRegionDelivery:
    """Phase 1B fix: SNS topic in one region delivering to SQS queue in another."""

    def test_sns_to_sqs_cross_region(self, make_boto_client):
        suffix = uuid.uuid4().hex[:8]

        # Create SNS topic in us-east-1
        sns = make_boto_client("sns", region_name="us-east-1")
        topic_resp = sns.create_topic(Name=f"cross-topic-{suffix}")
        topic_arn = topic_resp["TopicArn"]

        # Create SQS queue in us-west-2
        sqs = make_boto_client("sqs", region_name="us-west-2")
        q_resp = sqs.create_queue(QueueName=f"cross-queue-{suffix}")
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        # Subscribe the us-west-2 queue to the us-east-1 topic
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

        # Publish a message
        sns.publish(TopicArn=topic_arn, Message="cross-region test message")

        # Give time for delivery
        time.sleep(1)

        # Receive from the us-west-2 queue
        recv = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
        )
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1, "Message should be delivered cross-region"

        body = json.loads(msgs[0]["Body"])
        assert body["Message"] == "cross-region test message"

        # Cleanup
        sns.delete_topic(TopicArn=topic_arn)
        sqs.delete_queue(QueueUrl=queue_url)


class TestEventBridgeCrossRegionDelivery:
    """Phase 1C fix: EventBridge rule in one region targeting SQS queue in another."""

    def test_eventbridge_to_sqs_cross_region(self, make_boto_client):
        suffix = uuid.uuid4().hex[:8]

        # Create SQS queue in eu-west-1
        sqs = make_boto_client("sqs", region_name="eu-west-1")
        q_resp = sqs.create_queue(QueueName=f"eb-cross-{suffix}")
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        # Create EventBridge rule in us-east-1
        events = make_boto_client("events", region_name="us-east-1")
        events.put_rule(
            Name=f"cross-rule-{suffix}",
            EventPattern=json.dumps({"source": ["cross.test"]}),
        )
        events.put_targets(
            Rule=f"cross-rule-{suffix}",
            Targets=[{"Id": "cross-sqs", "Arn": queue_arn}],
        )

        # Put event in us-east-1
        events.put_events(
            Entries=[
                {
                    "Source": "cross.test",
                    "DetailType": "CrossRegionTest",
                    "Detail": json.dumps({"region": "cross"}),
                }
            ]
        )

        # Give time for delivery
        time.sleep(1)

        # Receive from the eu-west-1 queue
        recv = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
        )
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1, "Event should be delivered to cross-region SQS queue"

        # Cleanup
        events.remove_targets(Rule=f"cross-rule-{suffix}", Ids=["cross-sqs"])
        events.delete_rule(Name=f"cross-rule-{suffix}")
        sqs.delete_queue(QueueUrl=queue_url)
