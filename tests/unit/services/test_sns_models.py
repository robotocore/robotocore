"""Tests for robotocore.services.sns.models."""

import json

from robotocore.services.sns.models import SnsStore, SnsSubscription, SnsTopic


class TestSnsSubscription:
    def test_matches_filter_no_policy(self):
        sub = SnsSubscription(
            subscription_arn="arn:sub:1",
            topic_arn="arn:topic:1",
            protocol="sqs",
            endpoint="arn:sqs:q1",
        )
        assert sub.matches_filter({"Color": {"Value": "red"}}) is True

    def test_matches_filter_exact_match(self):
        sub = SnsSubscription(
            subscription_arn="arn:sub:1",
            topic_arn="arn:topic:1",
            protocol="sqs",
            endpoint="arn:sqs:q1",
            filter_policy={"Color": ["red", "blue"]},
        )
        assert sub.matches_filter({"Color": {"Value": "red"}}) is True
        assert sub.matches_filter({"Color": {"Value": "green"}}) is False

    def test_matches_filter_missing_key(self):
        sub = SnsSubscription(
            subscription_arn="arn:sub:1",
            topic_arn="arn:topic:1",
            protocol="sqs",
            endpoint="arn:sqs:q1",
            filter_policy={"Color": ["red"]},
        )
        assert sub.matches_filter({}) is False

    def test_matches_filter_scalar_value(self):
        sub = SnsSubscription(
            subscription_arn="arn:sub:1",
            topic_arn="arn:topic:1",
            protocol="sqs",
            endpoint="arn:sqs:q1",
            filter_policy={"Status": "active"},
        )
        assert sub.matches_filter({"Status": {"Value": "active"}}) is True
        assert sub.matches_filter({"Status": {"Value": "inactive"}}) is False

    def test_matches_filter_string_value_key(self):
        sub = SnsSubscription(
            subscription_arn="arn:sub:1",
            topic_arn="arn:topic:1",
            protocol="sqs",
            endpoint="arn:sqs:q1",
            filter_policy={"K": ["v"]},
        )
        # Falls back to StringValue when Value is absent
        assert sub.matches_filter({"K": {"StringValue": "v"}}) is True
        assert sub.matches_filter({"K": {"StringValue": "other"}}) is False

    def test_matches_filter_multiple_keys(self):
        sub = SnsSubscription(
            subscription_arn="arn:sub:1",
            topic_arn="arn:topic:1",
            protocol="sqs",
            endpoint="arn:sqs:q1",
            filter_policy={"A": ["1"], "B": ["2"]},
        )
        assert sub.matches_filter({"A": {"Value": "1"}, "B": {"Value": "2"}}) is True
        assert sub.matches_filter({"A": {"Value": "1"}, "B": {"Value": "9"}}) is False


class TestSnsTopic:
    def test_is_fifo(self):
        t = SnsTopic(arn="a", name="my-topic.fifo", region="r", account_id="a")
        assert t.is_fifo is True

    def test_is_not_fifo(self):
        t = SnsTopic(arn="a", name="my-topic", region="r", account_id="a")
        assert t.is_fifo is False

    def test_defaults(self):
        t = SnsTopic(arn="a", name="n", region="r", account_id="a")
        assert t.attributes == {}
        assert t.subscriptions == []
        assert t.tags == {}


class TestSnsStore:
    def make_store(self) -> SnsStore:
        return SnsStore()

    # -- Topic CRUD --
    def test_create_topic(self):
        store = self.make_store()
        t = store.create_topic("my-topic", "us-east-1", "123")
        assert t.name == "my-topic"
        assert t.arn == "arn:aws:sns:us-east-1:123:my-topic"

    def test_create_topic_idempotent(self):
        store = self.make_store()
        t1 = store.create_topic("t", "us-east-1", "123")
        t2 = store.create_topic("t", "us-east-1", "123")
        assert t1 is t2

    def test_get_topic(self):
        store = self.make_store()
        store.create_topic("t", "us-east-1", "123")
        assert store.get_topic("arn:aws:sns:us-east-1:123:t") is not None
        assert store.get_topic("arn:aws:sns:us-east-1:123:missing") is None

    def test_delete_topic(self):
        store = self.make_store()
        store.create_topic("t", "us-east-1", "123")
        assert store.delete_topic("arn:aws:sns:us-east-1:123:t") is True
        assert store.get_topic("arn:aws:sns:us-east-1:123:t") is None

    def test_delete_topic_nonexistent(self):
        store = self.make_store()
        assert store.delete_topic("arn:nope") is False

    def test_delete_topic_removes_subscriptions(self):
        store = self.make_store()
        store.create_topic("t", "us-east-1", "123")
        arn = "arn:aws:sns:us-east-1:123:t"
        sub = store.subscribe(arn, "sqs", "arn:aws:sqs:us-east-1:123:q")
        assert sub is not None
        store.delete_topic(arn)
        assert store.get_subscription(sub.subscription_arn) is None

    def test_list_topics(self):
        store = self.make_store()
        store.create_topic("a", "us-east-1", "123")
        store.create_topic("b", "us-east-1", "123")
        assert len(store.list_topics()) == 2

    # -- Subscription CRUD --
    def test_subscribe(self):
        store = self.make_store()
        store.create_topic("t", "us-east-1", "123")
        sub = store.subscribe("arn:aws:sns:us-east-1:123:t", "sqs", "arn:sqs:q")
        assert sub is not None
        assert sub.protocol == "sqs"
        assert sub.endpoint == "arn:sqs:q"
        assert sub.subscription_arn.startswith("arn:aws:sns:us-east-1:123:t:")

    def test_subscribe_nonexistent_topic(self):
        store = self.make_store()
        assert store.subscribe("arn:nope", "sqs", "arn:q") is None

    def test_subscribe_with_raw_delivery(self):
        store = self.make_store()
        store.create_topic("t", "us-east-1", "123")
        sub = store.subscribe(
            "arn:aws:sns:us-east-1:123:t",
            "sqs",
            "arn:q",
            attributes={"RawMessageDelivery": "true"},
        )
        assert sub.raw_message_delivery is True

    def test_subscribe_with_filter_policy_string(self):
        store = self.make_store()
        store.create_topic("t", "us-east-1", "123")
        policy = {"Color": ["red"]}
        sub = store.subscribe(
            "arn:aws:sns:us-east-1:123:t",
            "sqs",
            "arn:q",
            attributes={"FilterPolicy": json.dumps(policy)},
        )
        assert sub.filter_policy == policy

    def test_subscribe_with_filter_policy_dict(self):
        store = self.make_store()
        store.create_topic("t", "us-east-1", "123")
        policy = {"Color": ["red"]}
        sub = store.subscribe(
            "arn:aws:sns:us-east-1:123:t",
            "sqs",
            "arn:q",
            attributes={"FilterPolicy": policy},
        )
        assert sub.filter_policy == policy

    def test_unsubscribe(self):
        store = self.make_store()
        store.create_topic("t", "us-east-1", "123")
        sub = store.subscribe("arn:aws:sns:us-east-1:123:t", "sqs", "arn:q")
        assert store.unsubscribe(sub.subscription_arn) is True
        assert store.get_subscription(sub.subscription_arn) is None
        # Also removed from topic
        topic = store.get_topic("arn:aws:sns:us-east-1:123:t")
        assert len(topic.subscriptions) == 0

    def test_unsubscribe_nonexistent(self):
        store = self.make_store()
        assert store.unsubscribe("arn:nope") is False

    def test_get_subscription(self):
        store = self.make_store()
        store.create_topic("t", "us-east-1", "123")
        sub = store.subscribe("arn:aws:sns:us-east-1:123:t", "sqs", "arn:q")
        assert store.get_subscription(sub.subscription_arn) is sub
        assert store.get_subscription("arn:nope") is None

    def test_list_subscriptions_all(self):
        store = self.make_store()
        store.create_topic("t1", "us-east-1", "123")
        store.create_topic("t2", "us-east-1", "123")
        store.subscribe("arn:aws:sns:us-east-1:123:t1", "sqs", "arn:q1")
        store.subscribe("arn:aws:sns:us-east-1:123:t2", "sqs", "arn:q2")
        assert len(store.list_subscriptions()) == 2

    def test_list_subscriptions_by_topic(self):
        store = self.make_store()
        store.create_topic("t1", "us-east-1", "123")
        store.create_topic("t2", "us-east-1", "123")
        store.subscribe("arn:aws:sns:us-east-1:123:t1", "sqs", "arn:q1")
        store.subscribe("arn:aws:sns:us-east-1:123:t2", "sqs", "arn:q2")
        result = store.list_subscriptions(topic_arn="arn:aws:sns:us-east-1:123:t1")
        assert len(result) == 1
        assert result[0].endpoint == "arn:q1"

    def test_list_subscriptions_nonexistent_topic(self):
        store = self.make_store()
        assert store.list_subscriptions(topic_arn="arn:nope") == []

    def test_subscribe_owner_from_topic(self):
        store = self.make_store()
        store.create_topic("t", "us-east-1", "999888")
        sub = store.subscribe("arn:aws:sns:us-east-1:999888:t", "sqs", "arn:q")
        assert sub.owner == "999888"
