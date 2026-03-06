"""Unit tests for SNS completeness: FIFO topics, advanced filter policies,
HTTP delivery, Lambda delivery, Firehose delivery, platform applications,
message attributes, confirm subscription, topic policies and tags.
"""

import json
import time
import uuid
from unittest.mock import MagicMock, patch

import pytest

from robotocore.services.sns.models import (
    SnsStore,
    SnsSubscription,
    _check_anything_but,
    _check_cidr,
    _check_numeric,
    _matches_filter_policy,
    _matches_filter_value,
)
from robotocore.services.sns.provider import (
    SnsError,
    _confirm_subscription,
    _create_platform_application,
    _create_topic,
    _delete_platform_application,
    _deliver_to_subscriber,
    _get_platform_application_attributes,
    _get_subscription_attributes,
    _get_topic_attributes,
    _list_platform_applications,
    _list_tags_for_resource,
    _publish,
    _set_platform_application_attributes,
    _set_subscription_attributes,
    _set_topic_attributes,
    _tag_resource,
    _untag_resource,
)


def _make_sub(
    protocol="sqs",
    endpoint="arn:aws:sqs:us-east-1:123:q",
    filter_policy=None,
    confirmed=True,
):
    return SnsSubscription(
        subscription_arn=f"arn:sub:{uuid.uuid4()}",
        topic_arn="arn:aws:sns:us-east-1:123:topic",
        protocol=protocol,
        endpoint=endpoint,
        owner="123",
        filter_policy=filter_policy,
        confirmed=confirmed,
    )


# ==== FIFO Topics ====


class TestFifoTopics:
    def test_create_fifo_topic(self):
        store = SnsStore()
        mock_req = MagicMock()
        result = _create_topic(
            store,
            {
                "Name": "my-topic.fifo",
                "Attributes": {"FifoTopic": "true"},
            },
            "us-east-1", "123", mock_req,
        )
        assert result["TopicArn"].endswith(".fifo")
        topic = store.get_topic(result["TopicArn"])
        assert topic.is_fifo

    def test_fifo_topic_name_must_end_fifo(self):
        store = SnsStore()
        mock_req = MagicMock()
        with pytest.raises(SnsError) as exc:
            _create_topic(
                store,
                {
                    "Name": "bad-name",
                    "Attributes": {"FifoTopic": "true"},
                },
                "us-east-1", "123", mock_req,
            )
        assert "InvalidParameter" in exc.value.code

    def test_fifo_topic_content_based_dedup(self):
        store = SnsStore()
        store.create_topic("t.fifo", "us-east-1", "123", {
            "FifoTopic": "true",
            "ContentBasedDeduplication": "true",
        })
        topic = store.get_topic("arn:aws:sns:us-east-1:123:t.fifo")
        assert topic.content_based_dedup is True

    def test_fifo_dedup_skips_duplicate(self):
        store = SnsStore()
        topic = store.create_topic("t.fifo", "us-east-1", "123", {
            "FifoTopic": "true",
            "ContentBasedDeduplication": "true",
        })
        is_dup1, _ = topic.check_dedup("msg body", None, "g1")
        assert is_dup1 is False
        is_dup2, _ = topic.check_dedup("msg body", None, "g1")
        assert is_dup2 is True

    def test_fifo_dedup_explicit_id(self):
        store = SnsStore()
        topic = store.create_topic("t.fifo", "us-east-1", "123", {
            "FifoTopic": "true",
        })
        is_dup1, _ = topic.check_dedup("body1", "dedup-1", "g1")
        assert is_dup1 is False
        is_dup2, _ = topic.check_dedup("body2", "dedup-1", "g1")
        assert is_dup2 is True

    def test_fifo_dedup_expires(self):
        store = SnsStore()
        topic = store.create_topic("t.fifo", "us-east-1", "123", {
            "FifoTopic": "true",
        })
        topic.check_dedup("body", "d1", "g1")
        # Manually expire
        for k in list(topic._dedup_cache):
            topic._dedup_cache[k] = time.time() - 301
        is_dup, _ = topic.check_dedup("body", "d1", "g1")
        assert is_dup is False

    def test_fifo_publish_returns_sequence_number(self):
        store = SnsStore()
        store.create_topic("t.fifo", "us-east-1", "123", {
            "FifoTopic": "true",
            "ContentBasedDeduplication": "true",
        })
        mock_req = MagicMock()
        result = _publish(
            store,
            {
                "TopicArn": "arn:aws:sns:us-east-1:123:t.fifo",
                "Message": "hello",
                "MessageGroupId": "g1",
            },
            "us-east-1", "123", mock_req,
        )
        assert "SequenceNumber" in result

    def test_fifo_get_attributes_shows_fifo(self):
        store = SnsStore()
        store.create_topic("t.fifo", "us-east-1", "123", {
            "FifoTopic": "true",
        })
        mock_req = MagicMock()
        result = _get_topic_attributes(
            store,
            {"TopicArn": "arn:aws:sns:us-east-1:123:t.fifo"},
            "us-east-1", "123", mock_req,
        )
        assert result["Attributes"]["FifoTopic"] == "true"


# ==== Advanced Filter Policy ====


class TestFilterPolicyExactMatch:
    def test_string_list(self):
        assert _matches_filter_policy(
            {"Color": ["red", "blue"]},
            {"Color": {"Value": "red"}},
        ) is True

    def test_string_list_no_match(self):
        assert _matches_filter_policy(
            {"Color": ["red", "blue"]},
            {"Color": {"Value": "green"}},
        ) is False


class TestFilterPolicyPrefix:
    def test_prefix_match(self):
        assert _matches_filter_policy(
            {"Color": [{"prefix": "blu"}]},
            {"Color": {"Value": "blue"}},
        ) is True

    def test_prefix_no_match(self):
        assert _matches_filter_policy(
            {"Color": [{"prefix": "red"}]},
            {"Color": {"Value": "blue"}},
        ) is False


class TestFilterPolicyNumeric:
    def test_exact_numeric(self):
        assert _matches_filter_policy(
            {"Price": [{"numeric": ["=", 100]}]},
            {"Price": {"Value": "100"}},
        ) is True

    def test_range(self):
        assert _matches_filter_policy(
            {"Price": [{"numeric": [">=", 100, "<", 200]}]},
            {"Price": {"Value": "150"}},
        ) is True

    def test_range_out_of_bounds(self):
        assert _matches_filter_policy(
            {"Price": [{"numeric": [">=", 100, "<", 200]}]},
            {"Price": {"Value": "250"}},
        ) is False

    def test_greater_than(self):
        assert _matches_filter_policy(
            {"Price": [{"numeric": [">", 50]}]},
            {"Price": {"Value": "100"}},
        ) is True

    def test_less_than_or_equal(self):
        assert _matches_filter_policy(
            {"Price": [{"numeric": ["<=", 50]}]},
            {"Price": {"Value": "50"}},
        ) is True

    def test_non_numeric_value_fails(self):
        assert _matches_filter_policy(
            {"Price": [{"numeric": ["=", 100]}]},
            {"Price": {"Value": "not-a-number"}},
        ) is False


class TestFilterPolicyExists:
    def test_exists_true_with_key(self):
        assert _matches_filter_policy(
            {"Color": [{"exists": True}]},
            {"Color": {"Value": "red"}},
        ) is True

    def test_exists_true_without_key(self):
        assert _matches_filter_policy(
            {"Color": [{"exists": True}]},
            {},
        ) is False

    def test_exists_false_without_key(self):
        assert _matches_filter_policy(
            {"Color": [{"exists": False}]},
            {},
        ) is True

    def test_exists_false_with_key(self):
        assert _matches_filter_policy(
            {"Color": [{"exists": False}]},
            {"Color": {"Value": "red"}},
        ) is False


class TestFilterPolicyAnythingBut:
    def test_anything_but_list(self):
        assert _matches_filter_policy(
            {"Color": [{"anything-but": ["red", "blue"]}]},
            {"Color": {"Value": "green"}},
        ) is True

    def test_anything_but_list_matches_excluded(self):
        assert _matches_filter_policy(
            {"Color": [{"anything-but": ["red", "blue"]}]},
            {"Color": {"Value": "red"}},
        ) is False

    def test_anything_but_string(self):
        assert _matches_filter_policy(
            {"Color": [{"anything-but": "red"}]},
            {"Color": {"Value": "blue"}},
        ) is True

    def test_anything_but_prefix(self):
        assert _matches_filter_policy(
            {"Color": [{"anything-but": {"prefix": "red"}}]},
            {"Color": {"Value": "blue"}},
        ) is True

    def test_anything_but_prefix_excluded(self):
        assert _matches_filter_policy(
            {"Color": [{"anything-but": {"prefix": "red"}}]},
            {"Color": {"Value": "reddish"}},
        ) is False


class TestFilterPolicyCIDR:
    def test_cidr_match(self):
        assert _matches_filter_policy(
            {"SourceIP": [{"cidr": "10.0.0.0/8"}]},
            {"SourceIP": {"Value": "10.1.2.3"}},
        ) is True

    def test_cidr_no_match(self):
        assert _matches_filter_policy(
            {"SourceIP": [{"cidr": "10.0.0.0/8"}]},
            {"SourceIP": {"Value": "192.168.1.1"}},
        ) is False

    def test_cidr_invalid_ip(self):
        assert _matches_filter_policy(
            {"SourceIP": [{"cidr": "10.0.0.0/8"}]},
            {"SourceIP": {"Value": "not-an-ip"}},
        ) is False


class TestFilterPolicyCombined:
    def test_multiple_keys_all_must_match(self):
        assert _matches_filter_policy(
            {"Color": ["red"], "Size": [{"numeric": [">", 5]}]},
            {"Color": {"Value": "red"}, "Size": {"Value": "10"}},
        ) is True

    def test_multiple_keys_one_fails(self):
        assert _matches_filter_policy(
            {"Color": ["red"], "Size": [{"numeric": [">", 5]}]},
            {"Color": {"Value": "red"}, "Size": {"Value": "3"}},
        ) is False

    def test_or_within_value_list(self):
        """Multiple rules for same key: any match = pass."""
        assert _matches_filter_policy(
            {"Color": ["red", {"prefix": "blu"}]},
            {"Color": {"Value": "blue"}},
        ) is True


class TestSubscriptionMatchesFilter:
    def test_no_filter_matches_all(self):
        sub = _make_sub()
        assert sub.matches_filter({"Any": {"Value": "thing"}}) is True

    def test_with_filter(self):
        sub = _make_sub(filter_policy={"Color": ["red"]})
        assert sub.matches_filter({"Color": {"Value": "red"}}) is True
        assert sub.matches_filter({"Color": {"Value": "blue"}}) is False


# ==== HTTP/HTTPS Subscription Confirmation ====


class TestSubscriptionConfirmation:
    def test_http_sub_starts_unconfirmed(self):
        store = SnsStore()
        store.create_topic("t", "us-east-1", "123")
        sub = store.subscribe(
            "arn:aws:sns:us-east-1:123:t", "http",
            "http://example.com/hook",
        )
        assert sub.confirmed is False

    def test_confirm_subscription(self):
        store = SnsStore()
        store.create_topic("t", "us-east-1", "123")
        store.subscribe(
            "arn:aws:sns:us-east-1:123:t", "http",
            "http://example.com/hook",
        )
        mock_req = MagicMock()
        result = _confirm_subscription(
            store,
            {"TopicArn": "arn:aws:sns:us-east-1:123:t", "Token": "tok"},
            "us-east-1", "123", mock_req,
        )
        assert "SubscriptionArn" in result

    def test_confirm_nonexistent_topic(self):
        store = SnsStore()
        mock_req = MagicMock()
        with pytest.raises(SnsError):
            _confirm_subscription(
                store,
                {"TopicArn": "arn:nope", "Token": "tok"},
                "us-east-1", "123", mock_req,
            )

    def test_sqs_sub_auto_confirmed(self):
        store = SnsStore()
        store.create_topic("t", "us-east-1", "123")
        sub = store.subscribe(
            "arn:aws:sns:us-east-1:123:t", "sqs",
            "arn:aws:sqs:us-east-1:123:q",
        )
        assert sub.confirmed is True

    def test_get_sub_attrs_shows_pending(self):
        store = SnsStore()
        store.create_topic("t", "us-east-1", "123")
        sub = store.subscribe(
            "arn:aws:sns:us-east-1:123:t", "http",
            "http://example.com/hook",
        )
        mock_req = MagicMock()
        result = _get_subscription_attributes(
            store,
            {"SubscriptionArn": sub.subscription_arn},
            "us-east-1", "123", mock_req,
        )
        assert result["Attributes"]["PendingConfirmation"] == "true"


# ==== SNS -> Lambda Delivery ====


class TestSNSLambdaDelivery:
    def test_lambda_delivery_dispatches(self):
        sub = _make_sub(
            protocol="lambda",
            endpoint="arn:aws:lambda:us-east-1:123:function:f",
        )
        with patch("robotocore.services.sns.provider._deliver_to_lambda") as mock:
            _deliver_to_subscriber(
                sub, "msg", None, {}, "id", "topic", "us-east-1",
            )
            mock.assert_called_once()


# ==== SNS -> Firehose Delivery ====


class TestSNSFirehoseDelivery:
    def test_firehose_delivery_dispatches(self):
        sub = _make_sub(
            protocol="firehose",
            endpoint="arn:aws:firehose:us-east-1:123:deliverystream/stream",
        )
        with patch("robotocore.services.sns.provider._deliver_to_firehose") as mock:
            _deliver_to_subscriber(
                sub, "msg", None, {}, "id", "topic", "us-east-1",
            )
            mock.assert_called_once()


# ==== Platform Applications (stubs) ====


class TestPlatformApplications:
    def test_create_platform_app(self):
        store = SnsStore()
        mock_req = MagicMock()
        result = _create_platform_application(
            store,
            {
                "Name": "my-app",
                "Platform": "GCM",
                "Attributes": {"PlatformCredential": "api-key"},
            },
            "us-east-1", "123", mock_req,
        )
        assert "PlatformApplicationArn" in result
        assert "GCM" in result["PlatformApplicationArn"]

    def test_list_platform_apps(self):
        store = SnsStore()
        mock_req = MagicMock()
        _create_platform_application(
            store,
            {"Name": "a1", "Platform": "GCM"},
            "us-east-1", "123", mock_req,
        )
        _create_platform_application(
            store,
            {"Name": "a2", "Platform": "APNS"},
            "us-east-1", "123", mock_req,
        )
        result = _list_platform_applications(
            store, {}, "us-east-1", "123", mock_req,
        )
        assert len(result["PlatformApplications"]) == 2

    def test_delete_platform_app(self):
        store = SnsStore()
        mock_req = MagicMock()
        result = _create_platform_application(
            store,
            {"Name": "a1", "Platform": "GCM"},
            "us-east-1", "123", mock_req,
        )
        arn = result["PlatformApplicationArn"]
        _delete_platform_application(
            store, {"PlatformApplicationArn": arn},
            "us-east-1", "123", mock_req,
        )
        assert store.get_platform_application(arn) is None

    def test_get_platform_app_attributes(self):
        store = SnsStore()
        mock_req = MagicMock()
        result = _create_platform_application(
            store,
            {
                "Name": "a1", "Platform": "GCM",
                "Attributes": {"PlatformCredential": "key"},
            },
            "us-east-1", "123", mock_req,
        )
        arn = result["PlatformApplicationArn"]
        attrs = _get_platform_application_attributes(
            store, {"PlatformApplicationArn": arn},
            "us-east-1", "123", mock_req,
        )
        assert attrs["Attributes"]["PlatformCredential"] == "key"

    def test_set_platform_app_attributes(self):
        store = SnsStore()
        mock_req = MagicMock()
        result = _create_platform_application(
            store,
            {"Name": "a1", "Platform": "GCM"},
            "us-east-1", "123", mock_req,
        )
        arn = result["PlatformApplicationArn"]
        _set_platform_application_attributes(
            store,
            {
                "PlatformApplicationArn": arn,
                "Attributes": {"Enabled": "true"},
            },
            "us-east-1", "123", mock_req,
        )
        app = store.get_platform_application(arn)
        assert app.attributes["Enabled"] == "true"

    def test_get_nonexistent_app(self):
        store = SnsStore()
        mock_req = MagicMock()
        with pytest.raises(SnsError):
            _get_platform_application_attributes(
                store, {"PlatformApplicationArn": "arn:nope"},
                "us-east-1", "123", mock_req,
            )


# ==== Message Attributes ====


class TestSNSMessageAttributes:
    def test_publish_with_attributes(self):
        store = SnsStore()
        store.create_topic("t", "us-east-1", "123")
        mock_req = MagicMock()
        result = _publish(
            store,
            {
                "TopicArn": "arn:aws:sns:us-east-1:123:t",
                "Message": "hello",
                "MessageAttributes": {
                    "Color": {"DataType": "String", "StringValue": "red"},
                },
            },
            "us-east-1", "123", mock_req,
        )
        assert "MessageId" in result

    def test_publish_with_query_protocol_attrs(self):
        store = SnsStore()
        store.create_topic("t", "us-east-1", "123")
        mock_req = MagicMock()
        result = _publish(
            store,
            {
                "TopicArn": "arn:aws:sns:us-east-1:123:t",
                "Message": "hello",
                "MessageAttributes.entry.1.Name": "Color",
                "MessageAttributes.entry.1.Value.DataType": "String",
                "MessageAttributes.entry.1.Value.StringValue": "red",
            },
            "us-east-1", "123", mock_req,
        )
        assert "MessageId" in result


# ==== Topic Policies and Tags ====


class TestTopicPoliciesAndTags:
    def test_set_topic_policy(self):
        store = SnsStore()
        store.create_topic("t", "us-east-1", "123")
        arn = "arn:aws:sns:us-east-1:123:t"
        mock_req = MagicMock()
        policy = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": "*", "Action": "SNS:Publish"}],
        })
        _set_topic_attributes(
            store,
            {"TopicArn": arn, "AttributeName": "Policy", "AttributeValue": policy},
            "us-east-1", "123", mock_req,
        )
        result = _get_topic_attributes(
            store, {"TopicArn": arn}, "us-east-1", "123", mock_req,
        )
        assert result["Attributes"]["Policy"] == policy

    def test_tag_and_list_tags(self):
        store = SnsStore()
        store.create_topic("t", "us-east-1", "123")
        arn = "arn:aws:sns:us-east-1:123:t"
        mock_req = MagicMock()
        _tag_resource(
            store,
            {
                "ResourceArn": arn,
                "Tags": [
                    {"Key": "env", "Value": "prod"},
                    {"Key": "team", "Value": "backend"},
                ],
            },
            "us-east-1", "123", mock_req,
        )
        result = _list_tags_for_resource(
            store, {"ResourceArn": arn}, "us-east-1", "123", mock_req,
        )
        tags = {t["Key"]: t["Value"] for t in result["Tags"]}
        assert tags == {"env": "prod", "team": "backend"}

    def test_untag_resource(self):
        store = SnsStore()
        topic = store.create_topic("t", "us-east-1", "123")
        topic.tags = {"env": "dev", "team": "be"}
        arn = "arn:aws:sns:us-east-1:123:t"
        mock_req = MagicMock()
        _untag_resource(
            store,
            {"ResourceArn": arn, "TagKeys": ["env"]},
            "us-east-1", "123", mock_req,
        )
        result = _list_tags_for_resource(
            store, {"ResourceArn": arn}, "us-east-1", "123", mock_req,
        )
        keys = [t["Key"] for t in result["Tags"]]
        assert "env" not in keys
        assert "team" in keys

    def test_create_topic_with_tags(self):
        store = SnsStore()
        mock_req = MagicMock()
        result = _create_topic(
            store,
            {
                "Name": "t",
                "Tags": [{"Key": "k1", "Value": "v1"}],
            },
            "us-east-1", "123", mock_req,
        )
        topic = store.get_topic(result["TopicArn"])
        assert topic.tags["k1"] == "v1"


# ==== Filter Policy Helper Functions ====


class TestCheckNumeric:
    def test_equal(self):
        assert _check_numeric(["=", 5], "5") is True
        assert _check_numeric(["=", 5], "6") is False

    def test_greater_than(self):
        assert _check_numeric([">", 5], "6") is True
        assert _check_numeric([">", 5], "5") is False


class TestCheckAnythingBut:
    def test_list(self):
        assert _check_anything_but(["a", "b"], "c") is True
        assert _check_anything_but(["a", "b"], "a") is False

    def test_prefix(self):
        assert _check_anything_but({"prefix": "pre"}, "other") is True
        assert _check_anything_but({"prefix": "pre"}, "prefix") is False

    def test_single_string(self):
        assert _check_anything_but("x", "y") is True
        assert _check_anything_but("x", "x") is False


class TestCheckCidr:
    def test_valid_match(self):
        assert _check_cidr("192.168.0.0/16", "192.168.1.1") is True

    def test_no_match(self):
        assert _check_cidr("192.168.0.0/16", "10.0.0.1") is False

    def test_invalid(self):
        assert _check_cidr("bad", "1.1.1.1") is False


class TestMatchesFilterValue:
    def test_string_match(self):
        assert _matches_filter_value("red", "red") is True
        assert _matches_filter_value("red", "blue") is False

    def test_numeric_match(self):
        assert _matches_filter_value(42, "42") is True

    def test_dict_prefix(self):
        assert _matches_filter_value({"prefix": "bl"}, "blue") is True

    def test_dict_exists(self):
        assert _matches_filter_value({"exists": True}, "any") is True


# ==== Subscription Filter Policy Scope ====


class TestFilterPolicyScope:
    def test_set_filter_policy_scope(self):
        store = SnsStore()
        store.create_topic("t", "us-east-1", "123")
        arn = "arn:aws:sns:us-east-1:123:t"
        sub = store.subscribe(arn, "sqs", "arn:q")
        mock_req = MagicMock()
        _set_subscription_attributes(
            store,
            {
                "SubscriptionArn": sub.subscription_arn,
                "AttributeName": "FilterPolicyScope",
                "AttributeValue": "MessageBody",
            },
            "us-east-1", "123", mock_req,
        )
        assert sub.filter_policy_scope == "MessageBody"

    def test_get_sub_attrs_includes_scope(self):
        store = SnsStore()
        store.create_topic("t", "us-east-1", "123")
        arn = "arn:aws:sns:us-east-1:123:t"
        sub = store.subscribe(arn, "sqs", "arn:q")
        mock_req = MagicMock()
        result = _get_subscription_attributes(
            store,
            {"SubscriptionArn": sub.subscription_arn},
            "us-east-1", "123", mock_req,
        )
        assert result["Attributes"]["FilterPolicyScope"] == "MessageAttributes"


# ==== SnsStore Platform Application Model Tests ====


class TestSnsStorePlatformApps:
    def test_create_idempotent(self):
        store = SnsStore()
        a1 = store.create_platform_application("app", "GCM", "us-east-1", "123")
        a2 = store.create_platform_application("app", "GCM", "us-east-1", "123")
        assert a1 is a2

    def test_delete_returns_false_if_missing(self):
        store = SnsStore()
        assert store.delete_platform_application("arn:nope") is False

    def test_list_empty(self):
        store = SnsStore()
        assert store.list_platform_applications() == []
