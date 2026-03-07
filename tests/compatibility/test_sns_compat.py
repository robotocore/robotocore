"""SNS compatibility tests."""

import json
import uuid

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


class TestSNSFIFOTopic:
    def test_create_fifo_topic(self, sns):
        """Create a FIFO topic."""
        uid = uuid.uuid4().hex[:8]
        arn = sns.create_topic(
            Name=f"fifo-{uid}.fifo",
            Attributes={
                "FifoTopic": "true",
                "ContentBasedDeduplication": "true",
            },
        )["TopicArn"]
        try:
            assert arn.endswith(".fifo")
            attrs = sns.get_topic_attributes(TopicArn=arn)["Attributes"]
            assert attrs.get("FifoTopic") == "true"
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_publish_to_fifo_topic(self, sns):
        """Publish a message to a FIFO topic with MessageGroupId."""
        uid = uuid.uuid4().hex[:8]
        arn = sns.create_topic(
            Name=f"fifo-pub-{uid}.fifo",
            Attributes={
                "FifoTopic": "true",
                "ContentBasedDeduplication": "true",
            },
        )["TopicArn"]
        try:
            resp = sns.publish(
                TopicArn=arn,
                Message="fifo message",
                MessageGroupId="group1",
            )
            assert "MessageId" in resp
            assert "SequenceNumber" in resp
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_fifo_topic_idempotent_create(self, sns):
        """Creating same FIFO topic twice returns same ARN."""
        uid = uuid.uuid4().hex[:8]
        name = f"fifo-idem-{uid}.fifo"
        attrs = {"FifoTopic": "true", "ContentBasedDeduplication": "true"}
        arn1 = sns.create_topic(Name=name, Attributes=attrs)["TopicArn"]
        arn2 = sns.create_topic(Name=name, Attributes=attrs)["TopicArn"]
        try:
            assert arn1 == arn2
        finally:
            sns.delete_topic(TopicArn=arn1)


class TestSNSFilterPolicy:
    def test_string_match_filter(self, sns, sqs):
        """Filter policy with exact string match."""
        uid = uuid.uuid4().hex[:8]
        topic_arn = sns.create_topic(Name=f"filter-str-{uid}")["TopicArn"]
        q_url = sqs.create_queue(QueueName=f"filter-str-q-{uid}")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(
            QueueUrl=q_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        sub_arn = sns.subscribe(
            TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn
        )["SubscriptionArn"]
        sns.set_subscription_attributes(
            SubscriptionArn=sub_arn,
            AttributeName="FilterPolicy",
            AttributeValue=json.dumps({"color": ["blue"]}),
        )
        try:
            # Matching message
            sns.publish(
                TopicArn=topic_arn,
                Message="match",
                MessageAttributes={
                    "color": {"DataType": "String", "StringValue": "blue"},
                },
            )
            recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2)
            assert len(recv.get("Messages", [])) == 1

            # Drain
            for m in recv.get("Messages", []):
                sqs.delete_message(QueueUrl=q_url, ReceiptHandle=m["ReceiptHandle"])

            # Non-matching message
            sns.publish(
                TopicArn=topic_arn,
                Message="no-match",
                MessageAttributes={
                    "color": {"DataType": "String", "StringValue": "red"},
                },
            )
            import time

            time.sleep(1)
            recv2 = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2)
            assert len(recv2.get("Messages", [])) == 0
        finally:
            sqs.delete_queue(QueueUrl=q_url)
            sns.delete_topic(TopicArn=topic_arn)

    def test_prefix_filter(self, sns, sqs):
        """Filter policy with prefix matching."""
        uid = uuid.uuid4().hex[:8]
        topic_arn = sns.create_topic(Name=f"filter-pfx-{uid}")["TopicArn"]
        q_url = sqs.create_queue(QueueName=f"filter-pfx-q-{uid}")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(
            QueueUrl=q_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        sub_arn = sns.subscribe(
            TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn
        )["SubscriptionArn"]
        sns.set_subscription_attributes(
            SubscriptionArn=sub_arn,
            AttributeName="FilterPolicy",
            AttributeValue=json.dumps({"region": [{"prefix": "us-"}]}),
        )
        try:
            # Matching
            sns.publish(
                TopicArn=topic_arn,
                Message="match",
                MessageAttributes={
                    "region": {"DataType": "String", "StringValue": "us-east-1"},
                },
            )
            recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2)
            assert len(recv.get("Messages", [])) == 1

            for m in recv.get("Messages", []):
                sqs.delete_message(QueueUrl=q_url, ReceiptHandle=m["ReceiptHandle"])

            # Non-matching
            sns.publish(
                TopicArn=topic_arn,
                Message="no-match",
                MessageAttributes={
                    "region": {"DataType": "String", "StringValue": "eu-west-1"},
                },
            )
            import time

            time.sleep(1)
            recv2 = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2)
            assert len(recv2.get("Messages", [])) == 0
        finally:
            sqs.delete_queue(QueueUrl=q_url)
            sns.delete_topic(TopicArn=topic_arn)

    def test_numeric_filter(self, sns, sqs):
        """Filter policy with numeric matching."""
        uid = uuid.uuid4().hex[:8]
        topic_arn = sns.create_topic(Name=f"filter-num-{uid}")["TopicArn"]
        q_url = sqs.create_queue(QueueName=f"filter-num-q-{uid}")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(
            QueueUrl=q_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        sub_arn = sns.subscribe(
            TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn
        )["SubscriptionArn"]
        sns.set_subscription_attributes(
            SubscriptionArn=sub_arn,
            AttributeName="FilterPolicy",
            AttributeValue=json.dumps({"price": [{"numeric": [">=", 100]}]}),
        )
        try:
            # Matching (150 >= 100)
            sns.publish(
                TopicArn=topic_arn,
                Message="expensive",
                MessageAttributes={
                    "price": {"DataType": "Number", "StringValue": "150"},
                },
            )
            recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2)
            assert len(recv.get("Messages", [])) == 1

            for m in recv.get("Messages", []):
                sqs.delete_message(QueueUrl=q_url, ReceiptHandle=m["ReceiptHandle"])

            # Non-matching (50 < 100)
            sns.publish(
                TopicArn=topic_arn,
                Message="cheap",
                MessageAttributes={
                    "price": {"DataType": "Number", "StringValue": "50"},
                },
            )
            import time

            time.sleep(1)
            recv2 = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2)
            assert len(recv2.get("Messages", [])) == 0
        finally:
            sqs.delete_queue(QueueUrl=q_url)
            sns.delete_topic(TopicArn=topic_arn)

    def test_filter_policy_readable(self, sns, topic_arn):
        """Setting a filter policy should be readable via get_subscription_attributes."""
        sub_arn = sns.subscribe(
            TopicArn=topic_arn, Protocol="email", Endpoint="fp@test.com"
        )["SubscriptionArn"]
        policy = {"color": ["blue", "green"]}
        sns.set_subscription_attributes(
            SubscriptionArn=sub_arn,
            AttributeName="FilterPolicy",
            AttributeValue=json.dumps(policy),
        )
        attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn)["Attributes"]
        assert "FilterPolicy" in attrs
        read_policy = json.loads(attrs["FilterPolicy"])
        assert read_policy == policy


class TestSNSMessageAttributes:
    def test_publish_with_message_attributes_to_sqs(self, sns, sqs):
        """Message attributes should be delivered to SQS with raw delivery."""
        uid = uuid.uuid4().hex[:8]
        topic_arn = sns.create_topic(Name=f"attrs-{uid}")["TopicArn"]
        q_url = sqs.create_queue(QueueName=f"attrs-q-{uid}")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(
            QueueUrl=q_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        sub_arn = sns.subscribe(
            TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn
        )["SubscriptionArn"]
        sns.set_subscription_attributes(
            SubscriptionArn=sub_arn,
            AttributeName="RawMessageDelivery",
            AttributeValue="true",
        )
        try:
            sns.publish(
                TopicArn=topic_arn,
                Message="with-attrs",
                MessageAttributes={
                    "color": {"DataType": "String", "StringValue": "blue"},
                    "count": {"DataType": "Number", "StringValue": "5"},
                },
            )
            import time

            time.sleep(1)
            recv = sqs.receive_message(
                QueueUrl=q_url, WaitTimeSeconds=2, MessageAttributeNames=["All"]
            )
            msgs = recv.get("Messages", [])
            assert len(msgs) == 1
            attrs = msgs[0].get("MessageAttributes", {})
            assert attrs["color"]["StringValue"] == "blue"
            assert attrs["count"]["StringValue"] == "5"
        finally:
            sqs.delete_queue(QueueUrl=q_url)
            sns.delete_topic(TopicArn=topic_arn)

    def test_publish_with_subject(self, sns, sqs):
        """Publish with Subject and verify in SQS notification envelope."""
        uid = uuid.uuid4().hex[:8]
        topic_arn = sns.create_topic(Name=f"subject-{uid}")["TopicArn"]
        q_url = sqs.create_queue(QueueName=f"subject-q-{uid}")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(
            QueueUrl=q_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)
        try:
            sns.publish(
                TopicArn=topic_arn,
                Message="hello",
                Subject="Test Subject Line",
            )
            recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2)
            msgs = recv.get("Messages", [])
            assert len(msgs) == 1
            body = json.loads(msgs[0]["Body"])
            assert body["Subject"] == "Test Subject Line"
            assert body["Message"] == "hello"
        finally:
            sqs.delete_queue(QueueUrl=q_url)
            sns.delete_topic(TopicArn=topic_arn)


class TestSNSTagging:
    def test_tag_topic(self, sns):
        """Tag a topic and list tags."""
        uid = uuid.uuid4().hex[:8]
        arn = sns.create_topic(Name=f"tag-{uid}")["TopicArn"]
        try:
            sns.tag_resource(
                ResourceArn=arn,
                Tags=[
                    {"Key": "env", "Value": "test"},
                    {"Key": "project", "Value": "robotocore"},
                ],
            )
            tags = sns.list_tags_for_resource(ResourceArn=arn)["Tags"]
            tag_map = {t["Key"]: t["Value"] for t in tags}
            assert tag_map["env"] == "test"
            assert tag_map["project"] == "robotocore"
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_untag_topic(self, sns):
        """Tag then untag a topic."""
        uid = uuid.uuid4().hex[:8]
        arn = sns.create_topic(Name=f"untag-{uid}")["TopicArn"]
        try:
            sns.tag_resource(
                ResourceArn=arn,
                Tags=[
                    {"Key": "a", "Value": "1"},
                    {"Key": "b", "Value": "2"},
                ],
            )
            sns.untag_resource(ResourceArn=arn, TagKeys=["a"])
            tags = sns.list_tags_for_resource(ResourceArn=arn)["Tags"]
            tag_keys = [t["Key"] for t in tags]
            assert "a" not in tag_keys
            assert "b" in tag_keys
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_tag_overwrite(self, sns):
        """Tagging with same key overwrites value."""
        uid = uuid.uuid4().hex[:8]
        arn = sns.create_topic(Name=f"tag-over-{uid}")["TopicArn"]
        try:
            sns.tag_resource(
                ResourceArn=arn, Tags=[{"Key": "env", "Value": "dev"}]
            )
            sns.tag_resource(
                ResourceArn=arn, Tags=[{"Key": "env", "Value": "prod"}]
            )
            tags = sns.list_tags_for_resource(ResourceArn=arn)["Tags"]
            tag_map = {t["Key"]: t["Value"] for t in tags}
            assert tag_map["env"] == "prod"
        finally:
            sns.delete_topic(TopicArn=arn)


class TestSNSPublishBatch:
    def test_publish_batch(self, sns):
        """PublishBatch sends multiple messages at once."""
        uid = uuid.uuid4().hex[:8]
        arn = sns.create_topic(Name=f"batch-{uid}")["TopicArn"]
        try:
            resp = sns.publish_batch(
                TopicArn=arn,
                PublishBatchRequestEntries=[
                    {"Id": "msg1", "Message": "hello1"},
                    {"Id": "msg2", "Message": "hello2"},
                    {"Id": "msg3", "Message": "hello3"},
                ],
            )
            assert len(resp["Successful"]) == 3
            assert len(resp.get("Failed", [])) == 0
            msg_ids = {s["Id"] for s in resp["Successful"]}
            assert msg_ids == {"msg1", "msg2", "msg3"}
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_publish_batch_delivers_to_sqs(self, sns, sqs):
        """PublishBatch messages should arrive in subscribed SQS queue."""
        uid = uuid.uuid4().hex[:8]
        topic_arn = sns.create_topic(Name=f"batch-dlv-{uid}")["TopicArn"]
        q_url = sqs.create_queue(QueueName=f"batch-dlv-q-{uid}")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(
            QueueUrl=q_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)
        try:
            sns.publish_batch(
                TopicArn=topic_arn,
                PublishBatchRequestEntries=[
                    {"Id": "a", "Message": "batch-a"},
                    {"Id": "b", "Message": "batch-b"},
                ],
            )
            import time

            time.sleep(1)
            msgs = []
            for _ in range(3):
                recv = sqs.receive_message(
                    QueueUrl=q_url, MaxNumberOfMessages=10, WaitTimeSeconds=2
                )
                msgs.extend(recv.get("Messages", []))
                if len(msgs) >= 2:
                    break
            assert len(msgs) >= 2
            bodies = set()
            for m in msgs:
                body = json.loads(m["Body"])
                bodies.add(body["Message"])
            assert "batch-a" in bodies
            assert "batch-b" in bodies
        finally:
            sqs.delete_queue(QueueUrl=q_url)
            sns.delete_topic(TopicArn=topic_arn)

    def test_publish_batch_with_attributes(self, sns):
        """PublishBatch with message attributes."""
        uid = uuid.uuid4().hex[:8]
        arn = sns.create_topic(Name=f"batch-attr-{uid}")["TopicArn"]
        try:
            resp = sns.publish_batch(
                TopicArn=arn,
                PublishBatchRequestEntries=[
                    {
                        "Id": "msg1",
                        "Message": "with-attr",
                        "MessageAttributes": {
                            "color": {
                                "DataType": "String",
                                "StringValue": "blue",
                            },
                        },
                    },
                ],
            )
            assert len(resp["Successful"]) == 1
        finally:
            sns.delete_topic(TopicArn=arn)


class TestSNSSubscriptionAttributes:
    def test_get_subscription_attributes(self, sns, topic_arn):
        """Get subscription attributes returns expected keys."""
        sub_arn = sns.subscribe(
            TopicArn=topic_arn, Protocol="email", Endpoint="sub-attrs@test.com"
        )["SubscriptionArn"]
        attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn)["Attributes"]
        assert attrs["Protocol"] == "email"
        assert attrs["TopicArn"] == topic_arn
        assert attrs["Endpoint"] == "sub-attrs@test.com"
        assert "SubscriptionArn" in attrs

    def test_set_subscription_raw_delivery(self, sns, topic_arn, sqs):
        """Set RawMessageDelivery on a subscription."""
        uid = uuid.uuid4().hex[:8]
        q_url = sqs.create_queue(QueueName=f"raw-attr-{uid}")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(
            QueueUrl=q_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        sub_arn = sns.subscribe(
            TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn
        )["SubscriptionArn"]
        try:
            sns.set_subscription_attributes(
                SubscriptionArn=sub_arn,
                AttributeName="RawMessageDelivery",
                AttributeValue="true",
            )
            attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn)["Attributes"]
            assert attrs["RawMessageDelivery"] == "true"
        finally:
            sqs.delete_queue(QueueUrl=q_url)

    def test_list_subscriptions_by_topic_multiple(self, sns):
        """list_subscriptions_by_topic returns all subscriptions for a topic."""
        uid = uuid.uuid4().hex[:8]
        arn = sns.create_topic(Name=f"multi-sub-{uid}")["TopicArn"]
        try:
            sns.subscribe(TopicArn=arn, Protocol="email", Endpoint="a@test.com")
            sns.subscribe(TopicArn=arn, Protocol="email", Endpoint="b@test.com")
            sns.subscribe(TopicArn=arn, Protocol="email", Endpoint="c@test.com")
            subs = sns.list_subscriptions_by_topic(TopicArn=arn)["Subscriptions"]
            assert len(subs) >= 3
            endpoints = {s["Endpoint"] for s in subs}
            assert "a@test.com" in endpoints
            assert "b@test.com" in endpoints
            assert "c@test.com" in endpoints
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_subscribe_multiple_protocols(self, sns, topic_arn):
        """Subscribe with different protocols to same topic."""
        sub1 = sns.subscribe(
            TopicArn=topic_arn, Protocol="email", Endpoint="multi@test.com"
        )
        sub2 = sns.subscribe(
            TopicArn=topic_arn, Protocol="email-json", Endpoint="multi-json@test.com"
        )
        assert sub1["SubscriptionArn"] != sub2["SubscriptionArn"]
        subs = sns.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
        protocols = {s["Protocol"] for s in subs}
        assert "email" in protocols
        assert "email-json" in protocols


class TestSNSMultipleSubscribers:
    def test_three_sqs_subscribers(self, sns, sqs):
        """Three SQS queues subscribed to same topic all receive the message."""
        uid = uuid.uuid4().hex[:8]
        topic_arn = sns.create_topic(Name=f"tri-sub-{uid}")["TopicArn"]
        queues = []
        try:
            for i in range(3):
                q_url = sqs.create_queue(QueueName=f"tri-sub-q{i}-{uid}")["QueueUrl"]
                q_arn = sqs.get_queue_attributes(
                    QueueUrl=q_url, AttributeNames=["QueueArn"]
                )["Attributes"]["QueueArn"]
                sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)
                queues.append(q_url)

            sns.publish(TopicArn=topic_arn, Message="to-all-three")

            for q_url in queues:
                recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2)
                msgs = recv.get("Messages", [])
                assert len(msgs) == 1
                body = json.loads(msgs[0]["Body"])
                assert body["Message"] == "to-all-three"
        finally:
            for q_url in queues:
                sqs.delete_queue(QueueUrl=q_url)
            sns.delete_topic(TopicArn=topic_arn)

    def test_mixed_raw_and_wrapped_subscribers(self, sns, sqs):
        """One raw and one wrapped subscriber to same topic."""
        uid = uuid.uuid4().hex[:8]
        topic_arn = sns.create_topic(Name=f"mixed-{uid}")["TopicArn"]
        q_raw_url = sqs.create_queue(QueueName=f"mixed-raw-{uid}")["QueueUrl"]
        q_wrap_url = sqs.create_queue(QueueName=f"mixed-wrap-{uid}")["QueueUrl"]
        q_raw_arn = sqs.get_queue_attributes(
            QueueUrl=q_raw_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        q_wrap_arn = sqs.get_queue_attributes(
            QueueUrl=q_wrap_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]

        sub_raw = sns.subscribe(
            TopicArn=topic_arn, Protocol="sqs", Endpoint=q_raw_arn
        )["SubscriptionArn"]
        sns.set_subscription_attributes(
            SubscriptionArn=sub_raw,
            AttributeName="RawMessageDelivery",
            AttributeValue="true",
        )
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_wrap_arn)

        try:
            sns.publish(TopicArn=topic_arn, Message="mixed-message")

            recv_raw = sqs.receive_message(QueueUrl=q_raw_url, WaitTimeSeconds=2)
            recv_wrap = sqs.receive_message(QueueUrl=q_wrap_url, WaitTimeSeconds=2)

            # Raw subscriber gets plain message
            assert recv_raw["Messages"][0]["Body"] == "mixed-message"

            # Wrapped subscriber gets JSON envelope
            body = json.loads(recv_wrap["Messages"][0]["Body"])
            assert body["Type"] == "Notification"
            assert body["Message"] == "mixed-message"
        finally:
            sqs.delete_queue(QueueUrl=q_raw_url)
            sqs.delete_queue(QueueUrl=q_wrap_url)
            sns.delete_topic(TopicArn=topic_arn)
