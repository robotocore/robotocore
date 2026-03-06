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
