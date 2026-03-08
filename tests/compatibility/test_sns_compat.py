"""SNS compatibility tests."""

import json
import uuid

import pytest
from botocore.exceptions import ClientError

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


class TestSNSAttributeOperations:
    def test_set_and_get_topic_attributes(self, sns):
        arn = sns.create_topic(Name="attr-topic")["TopicArn"]
        sns.set_topic_attributes(
            TopicArn=arn,
            AttributeName="DisplayName",
            AttributeValue="My Test Display",
        )
        attrs = sns.get_topic_attributes(TopicArn=arn)["Attributes"]
        assert attrs["DisplayName"] == "My Test Display"
        sns.delete_topic(TopicArn=arn)

    def test_set_and_get_subscription_attributes(self, sns):
        arn = sns.create_topic(Name="sub-attr-topic")["TopicArn"]
        sub_arn = sns.subscribe(
            TopicArn=arn,
            Protocol="sqs",
            Endpoint="arn:aws:sqs:us-east-1:123456789012:test",
        )["SubscriptionArn"]
        sns.set_subscription_attributes(
            SubscriptionArn=sub_arn,
            AttributeName="RawMessageDelivery",
            AttributeValue="true",
        )
        attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn)["Attributes"]
        assert attrs["RawMessageDelivery"] == "true"
        sns.unsubscribe(SubscriptionArn=sub_arn)
        sns.delete_topic(TopicArn=arn)

    def test_list_subscriptions_by_topic_filtered(self, sns):
        arn = sns.create_topic(Name="list-sub-topic")["TopicArn"]
        sns.subscribe(
            TopicArn=arn,
            Protocol="sqs",
            Endpoint="arn:aws:sqs:us-east-1:123456789012:test",
        )
        subs = sns.list_subscriptions_by_topic(TopicArn=arn)
        sub_endpoints = [s["Endpoint"] for s in subs["Subscriptions"]]
        assert "arn:aws:sqs:us-east-1:123456789012:test" in sub_endpoints
        sns.delete_topic(TopicArn=arn)


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


class TestSNSFifoTopics:
    def test_create_fifo_topic(self, sns):
        suffix = uuid.uuid4().hex[:8]
        name = f"fifo-topic-{suffix}.fifo"
        try:
            response = sns.create_topic(
                Name=name,
                Attributes={
                    "FifoTopic": "true",
                    "ContentBasedDeduplication": "true",
                },
            )
            arn = response["TopicArn"]
            assert arn.endswith(".fifo")
            attrs = sns.get_topic_attributes(TopicArn=arn)["Attributes"]
            assert attrs["FifoTopic"] == "true"
            assert attrs["ContentBasedDeduplication"] == "true"
        finally:
            sns.delete_topic(TopicArn=response["TopicArn"])

    def test_publish_to_fifo_topic(self, sns):
        suffix = uuid.uuid4().hex[:8]
        name = f"fifo-pub-{suffix}.fifo"
        arn = sns.create_topic(
            Name=name,
            Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "true"},
        )["TopicArn"]
        try:
            response = sns.publish(
                TopicArn=arn,
                Message="fifo message",
                MessageGroupId="group1",
            )
            assert "MessageId" in response
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_fifo_topic_with_dedup_id(self, sns):
        suffix = uuid.uuid4().hex[:8]
        name = f"fifo-dedup-{suffix}.fifo"
        arn = sns.create_topic(
            Name=name,
            Attributes={"FifoTopic": "true"},
        )["TopicArn"]
        try:
            response = sns.publish(
                TopicArn=arn,
                Message="dedup message",
                MessageGroupId="group1",
                MessageDeduplicationId="dedup-123",
            )
            assert "MessageId" in response
        finally:
            sns.delete_topic(TopicArn=arn)


class TestSNSFilterPolicy:
    def test_subscription_filter_policy_string_match(self, sns, sqs):
        suffix = uuid.uuid4().hex[:8]
        q_url = sqs.create_queue(QueueName=f"filter-str-{suffix}")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]
        topic_arn = sns.create_topic(Name=f"filter-str-{suffix}")["TopicArn"]
        try:
            sub_arn = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)[
                "SubscriptionArn"
            ]
            sns.set_subscription_attributes(
                SubscriptionArn=sub_arn,
                AttributeName="FilterPolicy",
                AttributeValue=json.dumps({"color": ["blue"]}),
            )
            # Matching message
            sns.publish(
                TopicArn=topic_arn,
                Message="blue msg",
                MessageAttributes={"color": {"DataType": "String", "StringValue": "blue"}},
            )
            recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=3)
            assert len(recv.get("Messages", [])) == 1

            # Non-matching message
            sns.publish(
                TopicArn=topic_arn,
                Message="red msg",
                MessageAttributes={"color": {"DataType": "String", "StringValue": "red"}},
            )
            recv2 = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=1)
            assert len(recv2.get("Messages", [])) == 0
        finally:
            sqs.delete_queue(QueueUrl=q_url)
            sns.delete_topic(TopicArn=topic_arn)

    def test_subscription_filter_policy_prefix(self, sns, sqs):
        suffix = uuid.uuid4().hex[:8]
        q_url = sqs.create_queue(QueueName=f"filter-pfx-{suffix}")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]
        topic_arn = sns.create_topic(Name=f"filter-pfx-{suffix}")["TopicArn"]
        try:
            sub_arn = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)[
                "SubscriptionArn"
            ]
            sns.set_subscription_attributes(
                SubscriptionArn=sub_arn,
                AttributeName="FilterPolicy",
                AttributeValue=json.dumps({"event": [{"prefix": "order"}]}),
            )
            sns.publish(
                TopicArn=topic_arn,
                Message="order event",
                MessageAttributes={"event": {"DataType": "String", "StringValue": "order.created"}},
            )
            recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=3)
            assert len(recv.get("Messages", [])) == 1
        finally:
            sqs.delete_queue(QueueUrl=q_url)
            sns.delete_topic(TopicArn=topic_arn)

    def test_subscription_filter_policy_numeric(self, sns, sqs):
        suffix = uuid.uuid4().hex[:8]
        q_url = sqs.create_queue(QueueName=f"filter-num-{suffix}")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]
        topic_arn = sns.create_topic(Name=f"filter-num-{suffix}")["TopicArn"]
        try:
            sub_arn = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)[
                "SubscriptionArn"
            ]
            sns.set_subscription_attributes(
                SubscriptionArn=sub_arn,
                AttributeName="FilterPolicy",
                AttributeValue=json.dumps({"price": [{"numeric": [">=", 100]}]}),
            )
            sns.publish(
                TopicArn=topic_arn,
                Message="expensive item",
                MessageAttributes={"price": {"DataType": "Number", "StringValue": "150"}},
            )
            recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=3)
            assert len(recv.get("Messages", [])) == 1
        finally:
            sqs.delete_queue(QueueUrl=q_url)
            sns.delete_topic(TopicArn=topic_arn)


class TestSNSTopicTags:
    def test_tag_resource(self, sns):
        suffix = uuid.uuid4().hex[:8]
        arn = sns.create_topic(Name=f"tag-topic-{suffix}")["TopicArn"]
        try:
            sns.tag_resource(
                ResourceArn=arn,
                Tags=[
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "platform"},
                ],
            )
            tags = sns.list_tags_for_resource(ResourceArn=arn)["Tags"]
            tag_map = {t["Key"]: t["Value"] for t in tags}
            assert tag_map["env"] == "test"
            assert tag_map["team"] == "platform"
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_untag_resource(self, sns):
        suffix = uuid.uuid4().hex[:8]
        arn = sns.create_topic(Name=f"untag-topic-{suffix}")["TopicArn"]
        try:
            sns.tag_resource(
                ResourceArn=arn,
                Tags=[{"Key": "env", "Value": "test"}, {"Key": "keep", "Value": "yes"}],
            )
            sns.untag_resource(ResourceArn=arn, TagKeys=["env"])
            tags = sns.list_tags_for_resource(ResourceArn=arn)["Tags"]
            keys = [t["Key"] for t in tags]
            assert "env" not in keys
            assert "keep" in keys
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_list_tags_for_resource_empty(self, sns):
        suffix = uuid.uuid4().hex[:8]
        arn = sns.create_topic(Name=f"notags-{suffix}")["TopicArn"]
        try:
            tags = sns.list_tags_for_resource(ResourceArn=arn)["Tags"]
            assert tags == []
        finally:
            sns.delete_topic(TopicArn=arn)


class TestSNSTopicAndSubscriptionAttributes:
    def test_get_topic_attributes_fields(self, sns):
        suffix = uuid.uuid4().hex[:8]
        arn = sns.create_topic(Name=f"attrs-{suffix}")["TopicArn"]
        try:
            attrs = sns.get_topic_attributes(TopicArn=arn)["Attributes"]
            assert "TopicArn" in attrs
            assert "Owner" in attrs
            assert "SubscriptionsConfirmed" in attrs
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_set_topic_display_name(self, sns):
        suffix = uuid.uuid4().hex[:8]
        arn = sns.create_topic(Name=f"display-{suffix}")["TopicArn"]
        try:
            sns.set_topic_attributes(
                TopicArn=arn,
                AttributeName="DisplayName",
                AttributeValue="My Topic Display",
            )
            attrs = sns.get_topic_attributes(TopicArn=arn)["Attributes"]
            assert attrs["DisplayName"] == "My Topic Display"
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_set_topic_policy(self, sns):
        suffix = uuid.uuid4().hex[:8]
        arn = sns.create_topic(Name=f"policy-{suffix}")["TopicArn"]
        try:
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "SNS:Publish",
                            "Resource": arn,
                        }
                    ],
                }
            )
            sns.set_topic_attributes(
                TopicArn=arn,
                AttributeName="Policy",
                AttributeValue=policy,
            )
            attrs = sns.get_topic_attributes(TopicArn=arn)["Attributes"]
            returned_policy = json.loads(attrs["Policy"])
            assert returned_policy["Version"] == "2012-10-17"
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_get_subscription_attributes(self, sns, sqs):
        suffix = uuid.uuid4().hex[:8]
        q_url = sqs.create_queue(QueueName=f"sub-attrs-{suffix}")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]
        topic_arn = sns.create_topic(Name=f"sub-attrs-{suffix}")["TopicArn"]
        try:
            sub_arn = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)[
                "SubscriptionArn"
            ]
            attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn)["Attributes"]
            assert attrs["Protocol"] == "sqs"
            assert attrs["Endpoint"] == q_arn
            assert attrs["TopicArn"] == topic_arn
            assert "SubscriptionArn" in attrs
        finally:
            sqs.delete_queue(QueueUrl=q_url)
            sns.delete_topic(TopicArn=topic_arn)

    def test_set_subscription_raw_message_delivery(self, sns, sqs):
        suffix = uuid.uuid4().hex[:8]
        q_url = sqs.create_queue(QueueName=f"sub-raw-{suffix}")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]
        topic_arn = sns.create_topic(Name=f"sub-raw-{suffix}")["TopicArn"]
        try:
            sub_arn = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)[
                "SubscriptionArn"
            ]
            sns.set_subscription_attributes(
                SubscriptionArn=sub_arn,
                AttributeName="RawMessageDelivery",
                AttributeValue="true",
            )
            attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn)["Attributes"]
            assert attrs["RawMessageDelivery"] == "true"
        finally:
            sqs.delete_queue(QueueUrl=q_url)
            sns.delete_topic(TopicArn=topic_arn)


class TestSNSPublishFeatures:
    def test_publish_with_message_attributes(self, sns):
        suffix = uuid.uuid4().hex[:8]
        arn = sns.create_topic(Name=f"msg-attrs-{suffix}")["TopicArn"]
        try:
            response = sns.publish(
                TopicArn=arn,
                Message="message with attributes",
                MessageAttributes={
                    "name": {"DataType": "String", "StringValue": "test-name"},
                    "count": {"DataType": "Number", "StringValue": "42"},
                },
            )
            assert "MessageId" in response
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_publish_batch(self, sns):
        suffix = uuid.uuid4().hex[:8]
        arn = sns.create_topic(Name=f"batch-{suffix}")["TopicArn"]
        try:
            response = sns.publish_batch(
                TopicArn=arn,
                PublishBatchRequestEntries=[
                    {"Id": "msg1", "Message": "first message"},
                    {"Id": "msg2", "Message": "second message"},
                    {"Id": "msg3", "Message": "third message"},
                ],
            )
            assert len(response.get("Successful", [])) == 3
            assert len(response.get("Failed", [])) == 0
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_publish_with_subject(self, sns):
        suffix = uuid.uuid4().hex[:8]
        arn = sns.create_topic(Name=f"subject-{suffix}")["TopicArn"]
        try:
            response = sns.publish(
                TopicArn=arn,
                Message="message with subject",
                Subject="Test Subject",
            )
            assert "MessageId" in response
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_publish_message_structure_json(self, sns):
        suffix = uuid.uuid4().hex[:8]
        arn = sns.create_topic(Name=f"json-struct-{suffix}")["TopicArn"]
        try:
            response = sns.publish(
                TopicArn=arn,
                Message=json.dumps({"default": "default message", "sqs": "sqs message"}),
                MessageStructure="json",
            )
            assert "MessageId" in response
        finally:
            sns.delete_topic(TopicArn=arn)


class TestSNSPlatformApplications:
    def test_create_and_list_platform_application(self, sns):
        suffix = uuid.uuid4().hex[:8]
        name = f"test-app-{suffix}"
        try:
            response = sns.create_platform_application(
                Name=name,
                Platform="GCM",
                Attributes={"PlatformCredential": "test-api-key"},
            )
            app_arn = response["PlatformApplicationArn"]
            assert app_arn is not None

            apps = sns.list_platform_applications()
            arns = [a["PlatformApplicationArn"] for a in apps["PlatformApplications"]]
            assert app_arn in arns
        finally:
            sns.delete_platform_application(PlatformApplicationArn=app_arn)

    def test_delete_platform_application(self, sns):
        suffix = uuid.uuid4().hex[:8]
        name = f"del-app-{suffix}"
        response = sns.create_platform_application(
            Name=name,
            Platform="GCM",
            Attributes={"PlatformCredential": "test-api-key"},
        )
        app_arn = response["PlatformApplicationArn"]
        sns.delete_platform_application(PlatformApplicationArn=app_arn)
        apps = sns.list_platform_applications()
        arns = [a["PlatformApplicationArn"] for a in apps["PlatformApplications"]]
        assert app_arn not in arns


class TestSNSSubscriptionAttributes:
    def test_set_and_get_filter_policy(self, sns, sqs):
        """SetSubscriptionAttributes + GetSubscriptionAttributes with filter policy."""
        topic_arn = sns.create_topic(Name="filter-policy-topic")["TopicArn"]
        q_url = sqs.create_queue(QueueName="filter-policy-queue")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]

        sub_arn = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)[
            "SubscriptionArn"
        ]

        filter_policy = json.dumps({"event_type": ["order_placed"]})
        sns.set_subscription_attributes(
            SubscriptionArn=sub_arn,
            AttributeName="FilterPolicy",
            AttributeValue=filter_policy,
        )

        attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn)["Attributes"]
        assert json.loads(attrs["FilterPolicy"]) == {"event_type": ["order_placed"]}
        assert attrs["TopicArn"] == topic_arn
        assert attrs["Protocol"] == "sqs"

        sqs.delete_queue(QueueUrl=q_url)
        sns.delete_topic(TopicArn=topic_arn)

    def test_get_subscription_attributes_basic(self, sns, sqs):
        """GetSubscriptionAttributes returns standard fields."""
        topic_arn = sns.create_topic(Name="get-sub-attrs-topic")["TopicArn"]
        q_url = sqs.create_queue(QueueName="get-sub-attrs-queue")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]

        sub_arn = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)[
            "SubscriptionArn"
        ]

        attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn)["Attributes"]
        assert attrs["SubscriptionArn"] == sub_arn
        assert attrs["TopicArn"] == topic_arn
        assert attrs["Protocol"] == "sqs"
        assert attrs["Endpoint"] == q_arn
        assert "Owner" in attrs

        sqs.delete_queue(QueueUrl=q_url)
        sns.delete_topic(TopicArn=topic_arn)


class TestSNSMessageAttributes:
    def test_tag_untag_list_tags(self, sns, topic_arn):
        """Test TagResource, UntagResource, ListTagsForResource on topics."""
        sns.tag_resource(
            ResourceArn=topic_arn,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )
        tags_resp = sns.list_tags_for_resource(ResourceArn=topic_arn)
        tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
        assert tags["env"] == "test"
        assert tags["team"] == "platform"

        # Untag one key
        sns.untag_resource(ResourceArn=topic_arn, TagKeys=["team"])
        tags_resp = sns.list_tags_for_resource(ResourceArn=topic_arn)
        keys = [t["Key"] for t in tags_resp["Tags"]]
        assert "env" in keys
        assert "team" not in keys


class TestSNSSubscriptionAttributesExtended:
    def test_set_subscription_filter_policy(self, sns, sqs):
        """Test SetSubscriptionAttributes with FilterPolicy."""
        q_url = sqs.create_queue(QueueName="sns-filter-test")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]
        topic_arn = sns.create_topic(Name="filter-topic")["TopicArn"]
        try:
            sub_arn = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)[
                "SubscriptionArn"
            ]
            filter_policy = json.dumps({"event_type": ["order_placed"]})
            sns.set_subscription_attributes(
                SubscriptionArn=sub_arn,
                AttributeName="FilterPolicy",
                AttributeValue=filter_policy,
            )
            attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn)
            assert "FilterPolicy" in attrs["Attributes"]
            assert json.loads(attrs["Attributes"]["FilterPolicy"]) == {
                "event_type": ["order_placed"]
            }
        finally:
            sns.delete_topic(TopicArn=topic_arn)
            sqs.delete_queue(QueueUrl=q_url)

    def test_list_topics_pagination(self, sns):
        """Test ListTopics pagination by creating many topics."""
        created_arns = []
        try:
            for i in range(5):
                arn = sns.create_topic(Name=f"page-topic-{i}")["TopicArn"]
                created_arns.append(arn)
            # List all topics — should include our topics
            all_arns = []
            response = sns.list_topics()
            all_arns.extend([t["TopicArn"] for t in response["Topics"]])
            while "NextToken" in response:
                response = sns.list_topics(NextToken=response["NextToken"])
                all_arns.extend([t["TopicArn"] for t in response["Topics"]])
            for arn in created_arns:
                assert arn in all_arns
        finally:
            for arn in created_arns:
                sns.delete_topic(TopicArn=arn)

    def test_confirm_subscription_with_dummy_token(self, sns, topic_arn):
        """ConfirmSubscription with an invalid token should raise an error."""
        with pytest.raises(Exception):
            sns.confirm_subscription(
                TopicArn=topic_arn,
                Token="invalid-dummy-token-12345",
            )


class TestSNSPlatformApplication:
    def test_create_and_delete_platform_application(self, sns):
        """Test CreatePlatformApplication and DeletePlatformApplication."""
        response = sns.create_platform_application(
            Name="test-platform-app",
            Platform="GCM",
            Attributes={"PlatformCredential": "test-api-key"},
        )
        arn = response["PlatformApplicationArn"]
        assert arn is not None
        sns.delete_platform_application(PlatformApplicationArn=arn)


class TestSNSTopicAttributesExtended:
    def test_get_topic_attributes_all_fields(self, sns, topic_arn):
        """Test GetTopicAttributes returns all expected fields."""
        attrs = sns.get_topic_attributes(TopicArn=topic_arn)["Attributes"]
        assert "TopicArn" in attrs
        assert "Owner" in attrs
        assert "DisplayName" in attrs
        assert "SubscriptionsConfirmed" in attrs
        assert "SubscriptionsPending" in attrs
        assert "SubscriptionsDeleted" in attrs


class TestSNSSubscribeProtocols:
    def test_subscribe_sqs_protocol(self, sns, sqs, topic_arn):
        """Test Subscribe with SQS protocol."""
        q_url = sqs.create_queue(QueueName="sns-proto-sqs")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]
        try:
            sub = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)
            assert "SubscriptionArn" in sub
            # SQS subscriptions are auto-confirmed
            assert sub["SubscriptionArn"] != "PendingConfirmation"
        finally:
            sqs.delete_queue(QueueUrl=q_url)

    def test_subscribe_lambda_protocol(self, sns, topic_arn):
        """Test Subscribe with lambda protocol."""
        lambda_arn = "arn:aws:lambda:us-east-1:123456789012:function:my-function"
        sub = sns.subscribe(TopicArn=topic_arn, Protocol="lambda", Endpoint=lambda_arn)
        assert "SubscriptionArn" in sub

    def test_subscribe_https_protocol(self, sns, topic_arn):
        """Test Subscribe with https protocol."""
        sub = sns.subscribe(
            TopicArn=topic_arn,
            Protocol="https",
            Endpoint="https://example.com/sns-endpoint",
        )
        assert "SubscriptionArn" in sub


class TestSNSExtendedOperations:
    """Extended SNS operations for higher coverage."""

    @pytest.fixture
    def sns(self):
        from tests.compatibility.conftest import make_client

        return make_client("sns")

    @pytest.fixture
    def topic_arn(self, sns):
        import uuid

        name = f"ext-topic-{uuid.uuid4().hex[:8]}"
        resp = sns.create_topic(Name=name)
        arn = resp["TopicArn"]
        yield arn
        sns.delete_topic(TopicArn=arn)

    def test_set_get_topic_attributes(self, sns, topic_arn):
        sns.set_topic_attributes(
            TopicArn=topic_arn,
            AttributeName="DisplayName",
            AttributeValue="My Display Name",
        )
        resp = sns.get_topic_attributes(TopicArn=topic_arn)
        assert resp["Attributes"]["DisplayName"] == "My Display Name"

    def test_tag_untag_topic(self, sns, topic_arn):
        sns.tag_resource(
            ResourceArn=topic_arn,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )
        resp = sns.list_tags_for_resource(ResourceArn=topic_arn)
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags["env"] == "test"

        sns.untag_resource(ResourceArn=topic_arn, TagKeys=["team"])
        resp = sns.list_tags_for_resource(ResourceArn=topic_arn)
        keys = [t["Key"] for t in resp["Tags"]]
        assert "team" not in keys

    def test_set_subscription_attributes(self, sns, topic_arn):
        from tests.compatibility.conftest import make_client

        sqs = make_client("sqs")
        q = sqs.create_queue(QueueName=f"sns-sub-attr-{__import__('uuid').uuid4().hex[:8]}")
        q_url = q["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]
        try:
            sub = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)
            sub_arn = sub["SubscriptionArn"]
            if sub_arn != "pending confirmation":
                sns.set_subscription_attributes(
                    SubscriptionArn=sub_arn,
                    AttributeName="RawMessageDelivery",
                    AttributeValue="true",
                )
                attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn)
                assert attrs["Attributes"]["RawMessageDelivery"] == "true"
        finally:
            sqs.delete_queue(QueueUrl=q_url)

    def test_publish_with_message_attributes(self, sns, topic_arn):
        resp = sns.publish(
            TopicArn=topic_arn,
            Message="attributed message",
            MessageAttributes={
                "event_type": {"DataType": "String", "StringValue": "order.created"},
                "priority": {"DataType": "Number", "StringValue": "1"},
            },
        )
        assert "MessageId" in resp

    def test_publish_batch(self, sns, topic_arn):
        resp = sns.publish_batch(
            TopicArn=topic_arn,
            PublishBatchRequestEntries=[
                {"Id": "msg1", "Message": "first"},
                {"Id": "msg2", "Message": "second"},
                {"Id": "msg3", "Message": "third"},
            ],
        )
        assert len(resp.get("Successful", [])) == 3
        assert resp.get("Failed", []) == []

    def test_create_topic_with_tags(self, sns):
        import uuid

        name = f"tagged-topic-{uuid.uuid4().hex[:8]}"
        resp = sns.create_topic(
            Name=name,
            Tags=[{"Key": "env", "Value": "staging"}],
        )
        arn = resp["TopicArn"]
        try:
            tags = sns.list_tags_for_resource(ResourceArn=arn)
            tag_map = {t["Key"]: t["Value"] for t in tags["Tags"]}
            assert tag_map["env"] == "staging"
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_create_topic_with_attributes(self, sns):
        import uuid

        name = f"attr-topic-{uuid.uuid4().hex[:8]}"
        resp = sns.create_topic(
            Name=name,
            Attributes={"DisplayName": "Attributed Topic"},
        )
        arn = resp["TopicArn"]
        try:
            attrs = sns.get_topic_attributes(TopicArn=arn)
            assert attrs["Attributes"]["DisplayName"] == "Attributed Topic"
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_list_subscriptions(self, sns, topic_arn):
        resp = sns.list_subscriptions()
        assert "Subscriptions" in resp

    def test_list_subscriptions_by_topic(self, sns, topic_arn):
        from tests.compatibility.conftest import make_client

        sqs = make_client("sqs")
        q = sqs.create_queue(QueueName=f"sns-list-sub-{__import__('uuid').uuid4().hex[:8]}")
        q_url = q["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]
        try:
            sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)
            resp = sns.list_subscriptions_by_topic(TopicArn=topic_arn)
            assert len(resp["Subscriptions"]) >= 1
            protocols = [s["Protocol"] for s in resp["Subscriptions"]]
            assert "sqs" in protocols
        finally:
            sqs.delete_queue(QueueUrl=q_url)

    def test_confirm_subscription_invalid_token(self, sns, topic_arn):
        """ConfirmSubscription with invalid token should raise error."""
        with pytest.raises(ClientError):
            sns.confirm_subscription(TopicArn=topic_arn, Token="invalid-token-value")

    def test_create_fifo_topic(self, sns):
        import uuid

        name = f"fifo-topic-{uuid.uuid4().hex[:8]}.fifo"
        resp = sns.create_topic(
            Name=name,
            Attributes={"FifoTopic": "true"},
        )
        arn = resp["TopicArn"]
        try:
            attrs = sns.get_topic_attributes(TopicArn=arn)
            assert attrs["Attributes"].get("FifoTopic") == "true"
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_publish_to_fifo_topic(self, sns):
        import uuid

        name = f"pub-fifo-{uuid.uuid4().hex[:8]}.fifo"
        resp = sns.create_topic(
            Name=name,
            Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "true"},
        )
        arn = resp["TopicArn"]
        try:
            pub_resp = sns.publish(
                TopicArn=arn,
                Message="fifo message",
                MessageGroupId="group1",
            )
            assert "MessageId" in pub_resp
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_subscribe_with_filter_policy(self, sns, topic_arn):
        from tests.compatibility.conftest import make_client

        sqs = make_client("sqs")
        q = sqs.create_queue(QueueName=f"sns-filter-{__import__('uuid').uuid4().hex[:8]}")
        q_url = q["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]
        try:
            sub = sns.subscribe(
                TopicArn=topic_arn,
                Protocol="sqs",
                Endpoint=q_arn,
                Attributes={
                    "FilterPolicy": json.dumps({"event_type": ["order.created"]}),
                },
            )
            sub_arn = sub["SubscriptionArn"]
            if sub_arn != "pending confirmation":
                attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn)
                fp = json.loads(attrs["Attributes"]["FilterPolicy"])
                assert fp["event_type"] == ["order.created"]
        finally:
            sqs.delete_queue(QueueUrl=q_url)

    def test_publish_with_subject(self, sns, topic_arn):
        resp = sns.publish(
            TopicArn=topic_arn,
            Subject="Test Subject",
            Message="Message with subject",
        )
        assert "MessageId" in resp

    def test_set_topic_policy(self, sns, topic_arn):
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": "SNS:Publish",
                        "Resource": topic_arn,
                    }
                ],
            }
        )
        sns.set_topic_attributes(
            TopicArn=topic_arn,
            AttributeName="Policy",
            AttributeValue=policy,
        )
        attrs = sns.get_topic_attributes(TopicArn=topic_arn)
        parsed = json.loads(attrs["Attributes"]["Policy"])
        assert len(parsed["Statement"]) >= 1


class TestSNSGapStubs:
    """Tests for gap operations: SMS sandbox phone numbers, origination numbers, sandbox status."""

    @pytest.fixture
    def sns(self):
        return make_client("sns")

    def test_list_sms_sandbox_phone_numbers(self, sns):
        resp = sns.list_sms_sandbox_phone_numbers()
        assert "PhoneNumbers" in resp

    def test_list_origination_numbers(self, sns):
        resp = sns.list_origination_numbers()
        assert "PhoneNumbers" in resp

    def test_get_sms_sandbox_account_status(self, sns):
        resp = sns.get_sms_sandbox_account_status()
        assert "IsInSandbox" in resp
        assert isinstance(resp["IsInSandbox"], bool)
