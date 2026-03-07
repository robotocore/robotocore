"""SNS compatibility tests."""

import json

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def sns():
    return make_client("sns")


@pytest.fixture
def sqs():
    return make_client("sqs")


@pytest.fixture
def topic_arn(sns):
    response = sns.create_topic(Name="test-topic")
    arn = response["TopicArn"]
    yield arn
    sns.delete_topic(TopicArn=arn)


class TestSNSTopicOperations:
    def test_create_topic(self, sns):
        response = sns.create_topic(Name="test-topic")
        assert "TopicArn" in response
        sns.delete_topic(TopicArn=response["TopicArn"])

    def test_create_topic_idempotent(self, sns):
        arn1 = sns.create_topic(Name="idempotent-topic")["TopicArn"]
        arn2 = sns.create_topic(Name="idempotent-topic")["TopicArn"]
        assert arn1 == arn2
        sns.delete_topic(TopicArn=arn1)

    def test_list_topics(self, sns, topic_arn):
        response = sns.list_topics()
        arns = [t["TopicArn"] for t in response["Topics"]]
        assert topic_arn in arns

    def test_get_topic_attributes(self, sns, topic_arn):
        response = sns.get_topic_attributes(TopicArn=topic_arn)
        assert response["Attributes"]["TopicArn"] == topic_arn

    def test_set_topic_attributes(self, sns, topic_arn):
        sns.set_topic_attributes(
            TopicArn=topic_arn,
            AttributeName="DisplayName",
            AttributeValue="My Display Name",
        )
        attrs = sns.get_topic_attributes(TopicArn=topic_arn)["Attributes"]
        assert attrs["DisplayName"] == "My Display Name"

    def test_publish(self, sns, topic_arn):
        response = sns.publish(TopicArn=topic_arn, Message="hello")
        assert "MessageId" in response

    def test_delete_topic(self, sns):
        arn = sns.create_topic(Name="delete-me")["TopicArn"]
        sns.delete_topic(TopicArn=arn)
        topics = sns.list_topics()["Topics"]
        arns = [t["TopicArn"] for t in topics]
        assert arn not in arns


class TestSNSSubscriptions:
    def test_subscribe_email(self, sns, topic_arn):
        sub = sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint="test@example.com")
        assert "SubscriptionArn" in sub

    def test_list_subscriptions_by_topic(self, sns, topic_arn):
        sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint="a@b.com")
        subs = sns.list_subscriptions_by_topic(TopicArn=topic_arn)
        assert len(subs["Subscriptions"]) >= 1

    def test_list_subscriptions(self, sns, topic_arn):
        sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint="a@b.com")
        subs = sns.list_subscriptions()
        assert len(subs["Subscriptions"]) >= 1

    def test_unsubscribe(self, sns, topic_arn):
        sub_arn = sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint="unsub@test.com")[
            "SubscriptionArn"
        ]
        sns.unsubscribe(SubscriptionArn=sub_arn)
        subs = sns.list_subscriptions_by_topic(TopicArn=topic_arn)
        sub_arns = [s["SubscriptionArn"] for s in subs["Subscriptions"]]
        assert sub_arn not in sub_arns


class TestSNSToSQSDelivery:
    def test_publish_delivers_to_sqs(self, sns, sqs):
        # Create SQS queue
        q_url = sqs.create_queue(QueueName="sns-delivery-test")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]

        # Create SNS topic and subscribe SQS
        topic_arn = sns.create_topic(Name="delivery-topic")["TopicArn"]
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)

        # Publish
        sns.publish(TopicArn=topic_arn, Message="hello from sns")

        # Receive from SQS
        recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2)
        msgs = recv.get("Messages", [])
        assert len(msgs) == 1
        body = json.loads(msgs[0]["Body"])
        assert body["Type"] == "Notification"
        assert body["Message"] == "hello from sns"

        sqs.delete_queue(QueueUrl=q_url)
        sns.delete_topic(TopicArn=topic_arn)

    def test_raw_message_delivery(self, sns, sqs):
        q_url = sqs.create_queue(QueueName="sns-raw-test")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]

        topic_arn = sns.create_topic(Name="raw-topic")["TopicArn"]
        sub_arn = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)[
            "SubscriptionArn"
        ]
        sns.set_subscription_attributes(
            SubscriptionArn=sub_arn,
            AttributeName="RawMessageDelivery",
            AttributeValue="true",
        )

        sns.publish(TopicArn=topic_arn, Message="raw message")

        recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2)
        msgs = recv.get("Messages", [])
        assert len(msgs) == 1
        # Raw delivery means no JSON wrapper
        assert msgs[0]["Body"] == "raw message"

        sqs.delete_queue(QueueUrl=q_url)
        sns.delete_topic(TopicArn=topic_arn)

    def test_multiple_subscribers(self, sns, sqs):
        q1_url = sqs.create_queue(QueueName="sns-multi-1")["QueueUrl"]
        q2_url = sqs.create_queue(QueueName="sns-multi-2")["QueueUrl"]
        q1_arn = sqs.get_queue_attributes(QueueUrl=q1_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]
        q2_arn = sqs.get_queue_attributes(QueueUrl=q2_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        topic_arn = sns.create_topic(Name="multi-sub-topic")["TopicArn"]
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q1_arn)
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q2_arn)

        sns.publish(TopicArn=topic_arn, Message="broadcast")

        r1 = sqs.receive_message(QueueUrl=q1_url, WaitTimeSeconds=2)
        r2 = sqs.receive_message(QueueUrl=q2_url, WaitTimeSeconds=2)
        assert len(r1.get("Messages", [])) == 1
        assert len(r2.get("Messages", [])) == 1

        sqs.delete_queue(QueueUrl=q1_url)
        sqs.delete_queue(QueueUrl=q2_url)
        sns.delete_topic(TopicArn=topic_arn)


class TestSNSTags:
    def test_tag_resource(self, sns, topic_arn):
        """Tag a topic and list tags."""
        sns.tag_resource(
            ResourceArn=topic_arn,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )
        response = sns.list_tags_for_resource(ResourceArn=topic_arn)
        tag_map = {t["Key"]: t["Value"] for t in response["Tags"]}
        assert tag_map["env"] == "test"
        assert tag_map["team"] == "platform"

    def test_untag_resource(self, sns, topic_arn):
        """Untag a topic."""
        sns.tag_resource(
            ResourceArn=topic_arn,
            Tags=[{"Key": "k1", "Value": "v1"}, {"Key": "k2", "Value": "v2"}],
        )
        sns.untag_resource(ResourceArn=topic_arn, TagKeys=["k1"])
        response = sns.list_tags_for_resource(ResourceArn=topic_arn)
        tag_map = {t["Key"]: t["Value"] for t in response["Tags"]}
        assert "k1" not in tag_map
        assert tag_map["k2"] == "v2"


class TestSNSPublishBatch:
    def test_publish_batch(self, sns, topic_arn):
        """Publish a batch of messages."""
        response = sns.publish_batch(
            TopicArn=topic_arn,
            PublishBatchRequestEntries=[
                {"Id": "msg1", "Message": "hello"},
                {"Id": "msg2", "Message": "world"},
                {"Id": "msg3", "Message": "batch"},
            ],
        )
        # Verify the response has the expected structure
        assert "Successful" in response
        assert "Failed" in response
        total = len(response["Successful"]) + len(response["Failed"])
        assert total == 3


class TestSNSMessageAttributes:
    def test_publish_with_message_attributes(self, sns, sqs):
        """Publish with message attributes and verify delivery."""
        q_url = sqs.create_queue(QueueName="sns-attrs-test")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(
            QueueUrl=q_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]

        topic_arn = sns.create_topic(Name="attrs-topic")["TopicArn"]
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)

        sns.publish(
            TopicArn=topic_arn,
            Message="with attrs",
            MessageAttributes={
                "color": {"DataType": "String", "StringValue": "blue"},
                "count": {"DataType": "Number", "StringValue": "42"},
            },
        )

        recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2)
        msgs = recv.get("Messages", [])
        assert len(msgs) == 1
        body = json.loads(msgs[0]["Body"])
        assert body["Message"] == "with attrs"

        sqs.delete_queue(QueueUrl=q_url)
        sns.delete_topic(TopicArn=topic_arn)


class TestSNSSubscriptionAttributes:
    def test_get_subscription_attributes(self, sns, topic_arn):
        """Get attributes of a subscription."""
        sub = sns.subscribe(
            TopicArn=topic_arn, Protocol="email", Endpoint="attrs@test.com"
        )
        sub_arn = sub["SubscriptionArn"]
        attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn)
        assert "Attributes" in attrs
        assert attrs["Attributes"]["TopicArn"] == topic_arn
        assert attrs["Attributes"]["Protocol"] == "email"

    def test_set_subscription_filter_policy(self, sns, topic_arn):
        """Set and verify a filter policy on a subscription."""
        sub = sns.subscribe(
            TopicArn=topic_arn, Protocol="email", Endpoint="filter@test.com"
        )
        sub_arn = sub["SubscriptionArn"]

        filter_policy = json.dumps({"color": ["blue", "red"]})
        sns.set_subscription_attributes(
            SubscriptionArn=sub_arn,
            AttributeName="FilterPolicy",
            AttributeValue=filter_policy,
        )

        attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn)
        assert "FilterPolicy" in attrs["Attributes"]
        policy = json.loads(attrs["Attributes"]["FilterPolicy"])
        assert "blue" in policy["color"]


class TestSNSFIFOTopic:
    def test_create_fifo_topic(self, sns):
        """Create a FIFO topic."""
        response = sns.create_topic(
            Name="test-topic.fifo",
            Attributes={"FifoTopic": "true"},
        )
        assert response["TopicArn"].endswith(".fifo")
        sns.delete_topic(TopicArn=response["TopicArn"])

    def test_publish_to_fifo_topic(self, sns):
        """Publish to a FIFO topic with group and dedup."""
        arn = sns.create_topic(
            Name="pub-fifo.fifo",
            Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "true"},
        )["TopicArn"]

        response = sns.publish(
            TopicArn=arn,
            Message="fifo message",
            MessageGroupId="group1",
        )
        assert "MessageId" in response
        sns.delete_topic(TopicArn=arn)
