"""Unit tests for the SNS native provider action functions."""

import json
from unittest.mock import MagicMock, patch

import pytest

from robotocore.services.sns.models import (
    SnsStore,
    SnsSubscription,
    _check_anything_but,
    _check_cidr,
    _check_numeric,
    _matches_filter_policy,
    _matches_filter_policy_on_body,
    _matches_filter_value,
)
from robotocore.services.sns.provider import (
    SnsError,
    _add_permission,
    _confirm_subscription,
    _create_platform_application,
    _create_platform_endpoint,
    _create_topic,
    _delete_endpoint,
    _delete_platform_application,
    _delete_topic,
    _deliver_to_sqs,
    _get_endpoint_attributes,
    _get_platform_application_attributes,
    _get_subscription_attributes,
    _get_topic_attributes,
    _list_endpoints_by_platform_application,
    _list_platform_applications,
    _list_subscriptions,
    _list_subscriptions_by_topic,
    _list_tags_for_resource,
    _list_topics,
    _parse_member_list,
    _publish,
    _publish_batch,
    _remove_permission,
    _set_endpoint_attributes,
    _set_platform_application_attributes,
    _set_subscription_attributes,
    _set_topic_attributes,
    _subscribe,
    _tag_resource,
    _unsubscribe,
    _untag_resource,
)

REGION = "us-east-1"
ACCOUNT_ID = "123456789012"
SQS_ARN = f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:my-queue"
SQS_ARN2 = f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:q2"
TOPIC_NOPE = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:nope"
LAMBDA_ARN = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:f1"
GCM_APP_NOPE = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:app/GCM/nope"


@pytest.fixture
def store():
    return SnsStore()


@pytest.fixture
def mock_request():
    return MagicMock()


@pytest.fixture
def topic_arn(store, mock_request):
    """Create a standard topic and return its ARN."""
    result = _create_topic(store, {"Name": "my-topic"}, REGION, ACCOUNT_ID, mock_request)
    return result["TopicArn"]


@pytest.fixture
def fifo_topic_arn(store, mock_request):
    """Create a FIFO topic and return its ARN."""
    result = _create_topic(
        store,
        {
            "Name": "my-topic.fifo",
            "Attributes": {"FifoTopic": "true", "ContentBasedDeduplication": "true"},
        },
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    return result["TopicArn"]


# ---- CreateTopic ----


def test_create_topic_basic(store, mock_request):
    result = _create_topic(store, {"Name": "test-topic"}, REGION, ACCOUNT_ID, mock_request)
    assert result["TopicArn"] == f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:test-topic"


def test_create_topic_idempotent(store, mock_request):
    r1 = _create_topic(store, {"Name": "dup"}, REGION, ACCOUNT_ID, mock_request)
    r2 = _create_topic(store, {"Name": "dup"}, REGION, ACCOUNT_ID, mock_request)
    assert r1["TopicArn"] == r2["TopicArn"]
    assert len(store.list_topics()) == 1


def test_create_topic_with_attributes(store, mock_request):
    params = {
        "Name": "attr-topic",
        "Attributes.entry.1.key": "DisplayName",
        "Attributes.entry.1.value": "My Display",
    }
    result = _create_topic(store, params, REGION, ACCOUNT_ID, mock_request)
    topic = store.get_topic(result["TopicArn"])
    assert topic.attributes["DisplayName"] == "My Display"


def test_create_topic_with_tags(store, mock_request):
    params = {
        "Name": "tagged",
        "Tags.member.1.Key": "env",
        "Tags.member.1.Value": "prod",
        "Tags.member.2.Key": "team",
        "Tags.member.2.Value": "platform",
    }
    result = _create_topic(store, params, REGION, ACCOUNT_ID, mock_request)
    topic = store.get_topic(result["TopicArn"])
    assert topic.tags == {"env": "prod", "team": "platform"}


def test_create_topic_json_tags(store, mock_request):
    params = {
        "Name": "json-tagged",
        "Tags": [{"Key": "k1", "Value": "v1"}],
    }
    result = _create_topic(store, params, REGION, ACCOUNT_ID, mock_request)
    topic = store.get_topic(result["TopicArn"])
    assert topic.tags == {"k1": "v1"}


def test_create_topic_default_policy(store, mock_request):
    result = _create_topic(store, {"Name": "policy-test"}, REGION, ACCOUNT_ID, mock_request)
    topic = store.get_topic(result["TopicArn"])
    policy = json.loads(topic.attributes["Policy"])
    assert policy["Version"] == "2008-10-17"
    assert len(policy["Statement"]) == 1
    assert "SNS:Publish" in policy["Statement"][0]["Action"]


def test_create_topic_fifo_valid(store, mock_request):
    params = {"Name": "my-topic.fifo", "Attributes": {"FifoTopic": "true"}}
    result = _create_topic(store, params, REGION, ACCOUNT_ID, mock_request)
    assert result["TopicArn"].endswith(".fifo")


def test_create_topic_fifo_invalid_name(store, mock_request):
    params = {"Name": "not-fifo", "Attributes": {"FifoTopic": "true"}}
    with pytest.raises(SnsError, match="FIFO topic name must end with .fifo"):
        _create_topic(store, params, REGION, ACCOUNT_ID, mock_request)


def test_create_topic_fifo_via_query_attrs(store, mock_request):
    params = {
        "Name": "q.fifo",
        "Attributes.entry.1.key": "FifoTopic",
        "Attributes.entry.1.value": "true",
    }
    result = _create_topic(store, params, REGION, ACCOUNT_ID, mock_request)
    assert result["TopicArn"].endswith(".fifo")


# ---- DeleteTopic ----


def test_delete_topic(store, mock_request, topic_arn):
    result = _delete_topic(store, {"TopicArn": topic_arn}, REGION, ACCOUNT_ID, mock_request)
    assert result == {}
    assert store.get_topic(topic_arn) is None


def test_delete_topic_nonexistent(store, mock_request):
    result = _delete_topic(
        store,
        {"TopicArn": TOPIC_NOPE},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert result == {}


def test_delete_topic_removes_subscriptions(store, mock_request, topic_arn):
    _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "sqs", "Endpoint": SQS_ARN},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert len(store.list_subscriptions()) == 1
    _delete_topic(store, {"TopicArn": topic_arn}, REGION, ACCOUNT_ID, mock_request)
    assert len(store.list_subscriptions()) == 0


# ---- ListTopics ----


def test_list_topics_empty(store, mock_request):
    result = _list_topics(store, {}, REGION, ACCOUNT_ID, mock_request)
    assert result == {"Topics": []}


def test_list_topics(store, mock_request, topic_arn):
    result = _list_topics(store, {}, REGION, ACCOUNT_ID, mock_request)
    assert len(result["Topics"]) == 1
    assert result["Topics"][0]["TopicArn"] == topic_arn


# ---- GetTopicAttributes ----


def test_get_topic_attributes(store, mock_request, topic_arn):
    result = _get_topic_attributes(store, {"TopicArn": topic_arn}, REGION, ACCOUNT_ID, mock_request)
    attrs = result["Attributes"]
    assert attrs["TopicArn"] == topic_arn
    assert attrs["Owner"] == ACCOUNT_ID
    assert attrs["SubscriptionsConfirmed"] == "0"


def test_get_topic_attributes_not_found(store, mock_request):
    with pytest.raises(SnsError, match="not found"):
        _get_topic_attributes(
            store,
            {"TopicArn": TOPIC_NOPE},
            REGION,
            ACCOUNT_ID,
            mock_request,
        )


def test_get_topic_attributes_fifo(store, mock_request, fifo_topic_arn):
    result = _get_topic_attributes(
        store,
        {"TopicArn": fifo_topic_arn},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    attrs = result["Attributes"]
    assert attrs["FifoTopic"] == "true"
    assert attrs["ContentBasedDeduplication"] == "true"


def test_get_topic_attributes_effective_delivery_policy(store, mock_request, topic_arn):
    result = _get_topic_attributes(store, {"TopicArn": topic_arn}, REGION, ACCOUNT_ID, mock_request)
    edp = json.loads(result["Attributes"]["EffectiveDeliveryPolicy"])
    assert edp["http"]["defaultHealthyRetryPolicy"]["numRetries"] == 3


# ---- SetTopicAttributes ----


def test_set_topic_attributes(store, mock_request, topic_arn):
    _set_topic_attributes(
        store,
        {"TopicArn": topic_arn, "AttributeName": "DisplayName", "AttributeValue": "New Name"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    topic = store.get_topic(topic_arn)
    assert topic.attributes["DisplayName"] == "New Name"


def test_set_topic_attributes_not_found(store, mock_request):
    with pytest.raises(SnsError, match="not found"):
        _set_topic_attributes(
            store,
            {"TopicArn": TOPIC_NOPE, "AttributeName": "X", "AttributeValue": "Y"},
            REGION,
            ACCOUNT_ID,
            mock_request,
        )


# ---- AddPermission / RemovePermission ----


def test_add_permission(store, mock_request, topic_arn):
    params = {
        "TopicArn": topic_arn,
        "Label": "test-perm",
        "AWSAccountId.member.1": "111111111111",
        "ActionName.member.1": "Publish",
    }
    _add_permission(store, params, REGION, ACCOUNT_ID, mock_request)
    topic = store.get_topic(topic_arn)
    policy = json.loads(topic.attributes["Policy"])
    stmts = [s for s in policy["Statement"] if s.get("Sid") == "test-perm"]
    assert len(stmts) == 1
    assert stmts[0]["Action"] == "SNS:Publish"


def test_add_permission_invalid_action(store, mock_request, topic_arn):
    params = {
        "TopicArn": topic_arn,
        "Label": "bad",
        "AWSAccountId.member.1": "111111111111",
        "ActionName.member.1": "InvalidAction",
    }
    with pytest.raises(SnsError, match="out of service scope"):
        _add_permission(store, params, REGION, ACCOUNT_ID, mock_request)


def test_add_permission_duplicate_label(store, mock_request, topic_arn):
    params = {
        "TopicArn": topic_arn,
        "Label": "dup-label",
        "AWSAccountId.member.1": "111111111111",
        "ActionName.member.1": "Publish",
    }
    _add_permission(store, params, REGION, ACCOUNT_ID, mock_request)
    with pytest.raises(SnsError, match="Statement already exists"):
        _add_permission(store, params, REGION, ACCOUNT_ID, mock_request)


def test_remove_permission(store, mock_request, topic_arn):
    add_params = {
        "TopicArn": topic_arn,
        "Label": "to-remove",
        "AWSAccountId.member.1": "111111111111",
        "ActionName.member.1": "Publish",
    }
    _add_permission(store, add_params, REGION, ACCOUNT_ID, mock_request)
    rm_params = {"TopicArn": topic_arn, "Label": "to-remove"}
    _remove_permission(store, rm_params, REGION, ACCOUNT_ID, mock_request)
    topic = store.get_topic(topic_arn)
    policy = json.loads(topic.attributes["Policy"])
    assert not any(s.get("Sid") == "to-remove" for s in policy["Statement"])


# ---- Subscribe ----


def test_subscribe_sqs(store, mock_request, topic_arn):
    result = _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "sqs", "Endpoint": SQS_ARN},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert "SubscriptionArn" in result
    assert result["SubscriptionArn"].startswith(topic_arn + ":")


def test_subscribe_lambda(store, mock_request, topic_arn):
    result = _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "lambda", "Endpoint": LAMBDA_ARN},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert "SubscriptionArn" in result


def test_subscribe_email(store, mock_request, topic_arn):
    result = _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "email", "Endpoint": "user@example.com"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    sub = store.get_subscription(result["SubscriptionArn"])
    assert sub.confirmed is True  # email auto-confirms in emulator


@patch("robotocore.services.sns.provider._send_subscription_confirmation")
def test_subscribe_http_starts_unconfirmed(mock_send, store, mock_request, topic_arn):
    result = _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "http", "Endpoint": "http://example.com/hook"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    sub = store.get_subscription(result["SubscriptionArn"])
    assert sub.confirmed is False
    mock_send.assert_called_once()


def test_subscribe_with_filter_policy(store, mock_request, topic_arn):
    fp = json.dumps({"color": ["red", "green"]})
    result = _subscribe(
        store,
        {
            "TopicArn": topic_arn,
            "Protocol": "sqs",
            "Endpoint": SQS_ARN2,
            "Attributes": {"FilterPolicy": fp},
        },
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    sub = store.get_subscription(result["SubscriptionArn"])
    assert sub.filter_policy == {"color": ["red", "green"]}


def test_subscribe_with_raw_delivery(store, mock_request, topic_arn):
    result = _subscribe(
        store,
        {
            "TopicArn": topic_arn,
            "Protocol": "sqs",
            "Endpoint": SQS_ARN,
            "Attributes": {"RawMessageDelivery": "true"},
        },
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    sub = store.get_subscription(result["SubscriptionArn"])
    assert sub.raw_message_delivery is True


def test_subscribe_nonexistent_topic(store, mock_request):
    with pytest.raises(SnsError, match="not found"):
        _subscribe(
            store,
            {"TopicArn": TOPIC_NOPE, "Protocol": "sqs", "Endpoint": SQS_ARN},
            REGION,
            ACCOUNT_ID,
            mock_request,
        )


def test_subscribe_query_protocol_attributes(store, mock_request, topic_arn):
    params = {
        "TopicArn": topic_arn,
        "Protocol": "sqs",
        "Endpoint": SQS_ARN,
        "Attributes.entry.1.key": "RawMessageDelivery",
        "Attributes.entry.1.value": "true",
    }
    result = _subscribe(store, params, REGION, ACCOUNT_ID, mock_request)
    sub = store.get_subscription(result["SubscriptionArn"])
    assert sub.raw_message_delivery is True


# ---- ConfirmSubscription ----


@patch("robotocore.services.sns.provider._send_subscription_confirmation")
def test_confirm_subscription(mock_send, store, mock_request, topic_arn):
    sub_result = _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "http", "Endpoint": "http://example.com/hook"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    sub = store.get_subscription(sub_result["SubscriptionArn"])
    assert sub.confirmed is False
    result = _confirm_subscription(
        store,
        {"TopicArn": topic_arn, "Token": "any-token"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert result["SubscriptionArn"] == sub_result["SubscriptionArn"]
    assert sub.confirmed is True


def test_confirm_subscription_no_pending(store, mock_request, topic_arn):
    with pytest.raises(SnsError, match="No pending subscription"):
        _confirm_subscription(
            store,
            {"TopicArn": topic_arn, "Token": "tok"},
            REGION,
            ACCOUNT_ID,
            mock_request,
        )


# ---- Unsubscribe ----


def test_unsubscribe(store, mock_request, topic_arn):
    sub_result = _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "sqs", "Endpoint": SQS_ARN},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    result = _unsubscribe(
        store,
        {"SubscriptionArn": sub_result["SubscriptionArn"]},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert result == {}
    assert store.get_subscription(sub_result["SubscriptionArn"]) is None


def test_unsubscribe_nonexistent(store, mock_request):
    result = _unsubscribe(
        store,
        {"SubscriptionArn": "arn:aws:sns:us-east-1:123456789012:topic:fake-sub"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert result == {}


# ---- ListSubscriptions ----


def test_list_subscriptions_empty(store, mock_request):
    result = _list_subscriptions(store, {}, REGION, ACCOUNT_ID, mock_request)
    assert result == {"Subscriptions": []}


def test_list_subscriptions(store, mock_request, topic_arn):
    _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "sqs", "Endpoint": SQS_ARN},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "email", "Endpoint": "a@b.com"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    result = _list_subscriptions(store, {}, REGION, ACCOUNT_ID, mock_request)
    assert len(result["Subscriptions"]) == 2
    protocols = {s["Protocol"] for s in result["Subscriptions"]}
    assert protocols == {"sqs", "email"}


# ---- ListSubscriptionsByTopic ----


def test_list_subscriptions_by_topic(store, mock_request, topic_arn):
    _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "sqs", "Endpoint": SQS_ARN},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    result = _list_subscriptions_by_topic(
        store,
        {"TopicArn": topic_arn},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert len(result["Subscriptions"]) == 1


# ---- GetSubscriptionAttributes ----


def test_get_subscription_attributes(store, mock_request, topic_arn):
    sub_result = _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "sqs", "Endpoint": SQS_ARN},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    result = _get_subscription_attributes(
        store,
        {"SubscriptionArn": sub_result["SubscriptionArn"]},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    attrs = result["Attributes"]
    assert attrs["Protocol"] == "sqs"
    assert attrs["RawMessageDelivery"] == "false"
    assert attrs["PendingConfirmation"] == "false"


def test_get_subscription_attributes_with_filter(store, mock_request, topic_arn):
    fp = json.dumps({"color": ["blue"]})
    sub_result = _subscribe(
        store,
        {
            "TopicArn": topic_arn,
            "Protocol": "sqs",
            "Endpoint": SQS_ARN,
            "Attributes": {"FilterPolicy": fp},
        },
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    result = _get_subscription_attributes(
        store,
        {"SubscriptionArn": sub_result["SubscriptionArn"]},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert json.loads(result["Attributes"]["FilterPolicy"]) == {"color": ["blue"]}


def test_get_subscription_attributes_not_found(store, mock_request):
    with pytest.raises(SnsError, match="not found"):
        _get_subscription_attributes(
            store,
            {"SubscriptionArn": "arn:aws:sns:us-east-1:123456789012:t:fake"},
            REGION,
            ACCOUNT_ID,
            mock_request,
        )


# ---- SetSubscriptionAttributes ----


def test_set_subscription_raw_delivery(store, mock_request, topic_arn):
    sub_result = _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "sqs", "Endpoint": SQS_ARN},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    _set_subscription_attributes(
        store,
        {
            "SubscriptionArn": sub_result["SubscriptionArn"],
            "AttributeName": "RawMessageDelivery",
            "AttributeValue": "true",
        },
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    sub = store.get_subscription(sub_result["SubscriptionArn"])
    assert sub.raw_message_delivery is True


def test_set_subscription_filter_policy(store, mock_request, topic_arn):
    sub_result = _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "sqs", "Endpoint": SQS_ARN},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    fp = json.dumps({"color": ["red"]})
    _set_subscription_attributes(
        store,
        {
            "SubscriptionArn": sub_result["SubscriptionArn"],
            "AttributeName": "FilterPolicy",
            "AttributeValue": fp,
        },
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    sub = store.get_subscription(sub_result["SubscriptionArn"])
    assert sub.filter_policy == {"color": ["red"]}


def test_set_subscription_filter_policy_scope(store, mock_request, topic_arn):
    sub_result = _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "sqs", "Endpoint": SQS_ARN},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    _set_subscription_attributes(
        store,
        {
            "SubscriptionArn": sub_result["SubscriptionArn"],
            "AttributeName": "FilterPolicyScope",
            "AttributeValue": "MessageBody",
        },
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    sub = store.get_subscription(sub_result["SubscriptionArn"])
    assert sub.filter_policy_scope == "MessageBody"


def test_set_subscription_attributes_not_found(store, mock_request):
    with pytest.raises(SnsError, match="not found"):
        _set_subscription_attributes(
            store,
            {"SubscriptionArn": "arn:fake", "AttributeName": "X", "AttributeValue": "Y"},
            REGION,
            ACCOUNT_ID,
            mock_request,
        )


# ---- Publish ----


@patch("robotocore.services.sns.provider._deliver_to_subscriber")
def test_publish_basic(mock_deliver, store, mock_request, topic_arn):
    _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "sqs", "Endpoint": SQS_ARN},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    result = _publish(
        store,
        {"TopicArn": topic_arn, "Message": "hello"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert "MessageId" in result
    mock_deliver.assert_called_once()
    call_args = mock_deliver.call_args
    assert call_args[0][1] == "hello"  # message


@patch("robotocore.services.sns.provider._deliver_to_subscriber")
def test_publish_with_subject(mock_deliver, store, mock_request, topic_arn):
    _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "sqs", "Endpoint": SQS_ARN},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    _publish(
        store,
        {"TopicArn": topic_arn, "Message": "body", "Subject": "subj"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    call_args = mock_deliver.call_args
    assert call_args[0][2] == "subj"  # subject


def test_publish_nonexistent_topic(store, mock_request):
    with pytest.raises(SnsError, match="not found"):
        _publish(
            store,
            {"TopicArn": TOPIC_NOPE, "Message": "x"},
            REGION,
            ACCOUNT_ID,
            mock_request,
        )


@patch("robotocore.services.sns.provider._deliver_to_subscriber")
def test_publish_with_message_attributes_query(mock_deliver, store, mock_request, topic_arn):
    _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "sqs", "Endpoint": SQS_ARN},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    params = {
        "TopicArn": topic_arn,
        "Message": "hi",
        "MessageAttributes.entry.1.Name": "color",
        "MessageAttributes.entry.1.Value.DataType": "String",
        "MessageAttributes.entry.1.Value.StringValue": "red",
    }
    _publish(store, params, REGION, ACCOUNT_ID, mock_request)
    call_args = mock_deliver.call_args
    msg_attrs = call_args[0][3]  # message_attributes
    assert msg_attrs["color"]["StringValue"] == "red"


@patch("robotocore.services.sns.provider._deliver_to_subscriber")
def test_publish_target_arn(mock_deliver, store, mock_request, topic_arn):
    _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "sqs", "Endpoint": SQS_ARN},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    result = _publish(
        store,
        {"TargetArn": topic_arn, "Message": "via target"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert "MessageId" in result


@patch("robotocore.services.sns.provider._deliver_to_subscriber")
def test_publish_skips_unconfirmed(mock_deliver, store, mock_request, topic_arn):
    """Unconfirmed subscribers should not receive messages."""
    # Manually add an unconfirmed subscription
    topic = store.get_topic(topic_arn)
    sub = SnsSubscription(
        subscription_arn=f"{topic_arn}:fake-sub",
        topic_arn=topic_arn,
        protocol="sqs",
        endpoint=SQS_ARN,
        confirmed=False,
    )
    topic.subscriptions.append(sub)
    _publish(
        store,
        {"TopicArn": topic_arn, "Message": "hello"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    mock_deliver.assert_not_called()


@patch("robotocore.services.sns.provider._deliver_to_subscriber")
def test_publish_filter_policy_blocks(mock_deliver, store, mock_request, topic_arn):
    """Subscriber with filter policy that doesn't match should not receive message."""
    fp = json.dumps({"color": ["blue"]})
    _subscribe(
        store,
        {
            "TopicArn": topic_arn,
            "Protocol": "sqs",
            "Endpoint": SQS_ARN,
            "Attributes": {"FilterPolicy": fp},
        },
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    _publish(
        store,
        {
            "TopicArn": topic_arn,
            "Message": "hi",
            "MessageAttributes": {"color": {"DataType": "String", "StringValue": "red"}},
        },
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    mock_deliver.assert_not_called()


@patch("robotocore.services.sns.provider._deliver_to_subscriber")
def test_publish_filter_policy_passes(mock_deliver, store, mock_request, topic_arn):
    fp = json.dumps({"color": ["red"]})
    _subscribe(
        store,
        {
            "TopicArn": topic_arn,
            "Protocol": "sqs",
            "Endpoint": SQS_ARN,
            "Attributes": {"FilterPolicy": fp},
        },
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    _publish(
        store,
        {
            "TopicArn": topic_arn,
            "Message": "hi",
            "MessageAttributes": {"color": {"DataType": "String", "StringValue": "red"}},
        },
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    mock_deliver.assert_called_once()


@patch("robotocore.services.sns.provider._deliver_to_subscriber")
def test_publish_message_structure_json(mock_deliver, store, mock_request, topic_arn):
    _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "sqs", "Endpoint": SQS_ARN},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    per_protocol = json.dumps({"default": "fallback", "sqs": "sqs-specific"})
    _publish(
        store,
        {"TopicArn": topic_arn, "Message": per_protocol, "MessageStructure": "json"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    call_args = mock_deliver.call_args
    assert call_args[0][1] == "sqs-specific"


@patch("robotocore.services.sns.provider._deliver_to_subscriber")
def test_publish_message_structure_json_uses_default(mock_deliver, store, mock_request, topic_arn):
    _subscribe(
        store,
        {"TopicArn": topic_arn, "Protocol": "email", "Endpoint": "a@b.com"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    per_protocol = json.dumps({"default": "fallback", "sqs": "sqs-only"})
    _publish(
        store,
        {"TopicArn": topic_arn, "Message": per_protocol, "MessageStructure": "json"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    call_args = mock_deliver.call_args
    assert call_args[0][1] == "fallback"


def test_publish_message_structure_json_invalid(store, mock_request, topic_arn):
    with pytest.raises(SnsError, match="JSON message body failed to parse"):
        _publish(
            store,
            {"TopicArn": topic_arn, "Message": "not-json", "MessageStructure": "json"},
            REGION,
            ACCOUNT_ID,
            mock_request,
        )


# ---- FIFO Publish ----


@patch("robotocore.services.sns.provider._deliver_to_subscriber")
def test_publish_fifo_requires_group_id(mock_deliver, store, mock_request, fifo_topic_arn):
    with pytest.raises(SnsError, match="MessageGroupId parameter is required"):
        _publish(
            store,
            {"TopicArn": fifo_topic_arn, "Message": "hello"},
            REGION,
            ACCOUNT_ID,
            mock_request,
        )


@patch("robotocore.services.sns.provider._deliver_to_subscriber")
def test_publish_fifo_with_content_dedup(mock_deliver, store, mock_request, fifo_topic_arn):
    result = _publish(
        store,
        {"TopicArn": fifo_topic_arn, "Message": "hello", "MessageGroupId": "g1"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert "MessageId" in result
    assert "SequenceNumber" in result


@patch("robotocore.services.sns.provider._deliver_to_subscriber")
def test_publish_fifo_dedup_same_message(mock_deliver, store, mock_request, fifo_topic_arn):
    _publish(
        store,
        {"TopicArn": fifo_topic_arn, "Message": "dup", "MessageGroupId": "g1"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    # Second identical message should be deduplicated (content-based)
    _publish(
        store,
        {"TopicArn": fifo_topic_arn, "Message": "dup", "MessageGroupId": "g1"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    # Only one delivery
    assert mock_deliver.call_count == 0  # no subscribers, but dedup check still runs


@patch("robotocore.services.sns.provider._deliver_to_subscriber")
def test_publish_fifo_no_content_dedup_requires_dedup_id(mock_deliver, store, mock_request):
    result = _create_topic(
        store,
        {"Name": "no-dedup.fifo", "Attributes": {"FifoTopic": "true"}},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    arn = result["TopicArn"]
    with pytest.raises(SnsError, match="ContentBasedDeduplication"):
        _publish(
            store,
            {"TopicArn": arn, "Message": "hello", "MessageGroupId": "g1"},
            REGION,
            ACCOUNT_ID,
            mock_request,
        )


# ---- PublishBatch ----


@patch("robotocore.services.sns.provider._deliver_to_subscriber")
def test_publish_batch_basic(mock_deliver, store, mock_request, topic_arn):
    params = {
        "TopicArn": topic_arn,
        "PublishBatchRequestEntries.member.1.Id": "a",
        "PublishBatchRequestEntries.member.1.Message": "msg-a",
        "PublishBatchRequestEntries.member.2.Id": "b",
        "PublishBatchRequestEntries.member.2.Message": "msg-b",
    }
    result = _publish_batch(store, params, REGION, ACCOUNT_ID, mock_request)
    assert len(result["Successful"]) == 2
    assert result["Failed"] == []
    ids = {s["Id"] for s in result["Successful"]}
    assert ids == {"a", "b"}


@patch("robotocore.services.sns.provider._deliver_to_subscriber")
def test_publish_batch_json_protocol(mock_deliver, store, mock_request, topic_arn):
    params = {
        "TopicArn": topic_arn,
        "PublishBatchRequestEntries": [
            {"Id": "x", "Message": "msg-x"},
            {"Id": "y", "Message": "msg-y"},
        ],
    }
    result = _publish_batch(store, params, REGION, ACCOUNT_ID, mock_request)
    assert len(result["Successful"]) == 2


def test_publish_batch_empty(store, mock_request, topic_arn):
    with pytest.raises(SnsError, match="doesn't contain any entries"):
        _publish_batch(store, {"TopicArn": topic_arn}, REGION, ACCOUNT_ID, mock_request)


def test_publish_batch_too_many(store, mock_request, topic_arn):
    params = {"TopicArn": topic_arn}
    for i in range(1, 12):
        params[f"PublishBatchRequestEntries.member.{i}.Id"] = f"id-{i}"
        params[f"PublishBatchRequestEntries.member.{i}.Message"] = f"msg-{i}"
    with pytest.raises(SnsError, match="more entries than permissible"):
        _publish_batch(store, params, REGION, ACCOUNT_ID, mock_request)


def test_publish_batch_duplicate_ids(store, mock_request, topic_arn):
    params = {
        "TopicArn": topic_arn,
        "PublishBatchRequestEntries": [
            {"Id": "same", "Message": "a"},
            {"Id": "same", "Message": "b"},
        ],
    }
    with pytest.raises(SnsError, match="same Id"):
        _publish_batch(store, params, REGION, ACCOUNT_ID, mock_request)


# ---- Tags ----


def test_tag_resource(store, mock_request, topic_arn):
    _tag_resource(
        store,
        {"ResourceArn": topic_arn, "Tags.member.1.Key": "env", "Tags.member.1.Value": "dev"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    result = _list_tags_for_resource(
        store,
        {"ResourceArn": topic_arn},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert {"Key": "env", "Value": "dev"} in result["Tags"]


def test_tag_resource_json(store, mock_request, topic_arn):
    _tag_resource(
        store,
        {"ResourceArn": topic_arn, "Tags": [{"Key": "team", "Value": "eng"}]},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    topic = store.get_topic(topic_arn)
    assert topic.tags["team"] == "eng"


def test_untag_resource(store, mock_request, topic_arn):
    _tag_resource(
        store,
        {
            "ResourceArn": topic_arn,
            "Tags": [
                {"Key": "k1", "Value": "v1"},
                {"Key": "k2", "Value": "v2"},
            ],
        },
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    _untag_resource(
        store,
        {"ResourceArn": topic_arn, "TagKeys.member.1": "k1"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    topic = store.get_topic(topic_arn)
    assert "k1" not in topic.tags
    assert topic.tags["k2"] == "v2"


def test_untag_resource_json(store, mock_request, topic_arn):
    _tag_resource(
        store,
        {"ResourceArn": topic_arn, "Tags": [{"Key": "x", "Value": "y"}]},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    _untag_resource(
        store,
        {"ResourceArn": topic_arn, "TagKeys": ["x"]},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    topic = store.get_topic(topic_arn)
    assert "x" not in topic.tags


def test_list_tags_not_found(store, mock_request):
    with pytest.raises(SnsError, match="not found"):
        _list_tags_for_resource(
            store,
            {"ResourceArn": TOPIC_NOPE},
            REGION,
            ACCOUNT_ID,
            mock_request,
        )


# ---- Platform Application ----


def test_create_platform_application(store, mock_request):
    result = _create_platform_application(
        store,
        {"Name": "my-app", "Platform": "GCM", "Attributes": {"PlatformCredential": "key123"}},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert "PlatformApplicationArn" in result
    assert "GCM" in result["PlatformApplicationArn"]


def test_list_platform_applications(store, mock_request):
    _create_platform_application(
        store,
        {"Name": "app1", "Platform": "APNS"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    result = _list_platform_applications(store, {}, REGION, ACCOUNT_ID, mock_request)
    assert len(result["PlatformApplications"]) == 1


def test_get_platform_application_attributes(store, mock_request):
    r = _create_platform_application(
        store,
        {"Name": "app2", "Platform": "GCM", "Attributes": {"Enabled": "true"}},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    result = _get_platform_application_attributes(
        store,
        {"PlatformApplicationArn": r["PlatformApplicationArn"]},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert result["Attributes"]["Enabled"] == "true"


def test_set_platform_application_attributes(store, mock_request):
    r = _create_platform_application(
        store,
        {"Name": "app3", "Platform": "GCM"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    _set_platform_application_attributes(
        store,
        {
            "PlatformApplicationArn": r["PlatformApplicationArn"],
            "Attributes.entry.1.key": "SuccessFeedbackRoleArn",
            "Attributes.entry.1.value": "arn:aws:iam::123:role/r",
        },
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    app = store.get_platform_application(r["PlatformApplicationArn"])
    assert app.attributes["SuccessFeedbackRoleArn"] == "arn:aws:iam::123:role/r"


def test_delete_platform_application(store, mock_request):
    r = _create_platform_application(
        store,
        {"Name": "app-del", "Platform": "GCM"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    _delete_platform_application(
        store,
        {"PlatformApplicationArn": r["PlatformApplicationArn"]},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert store.get_platform_application(r["PlatformApplicationArn"]) is None


# ---- Platform Endpoint ----


def test_create_platform_endpoint(store, mock_request):
    app = _create_platform_application(
        store,
        {"Name": "ep-app", "Platform": "GCM"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    result = _create_platform_endpoint(
        store,
        {"PlatformApplicationArn": app["PlatformApplicationArn"], "Token": "device-tok"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert "EndpointArn" in result


def test_create_platform_endpoint_no_app(store, mock_request):
    with pytest.raises(SnsError, match="does not exist"):
        _create_platform_endpoint(
            store,
            {"PlatformApplicationArn": GCM_APP_NOPE, "Token": "t"},
            REGION,
            ACCOUNT_ID,
            mock_request,
        )


def test_get_set_endpoint_attributes(store, mock_request):
    app = _create_platform_application(
        store,
        {"Name": "ep-app2", "Platform": "GCM"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    ep = _create_platform_endpoint(
        store,
        {"PlatformApplicationArn": app["PlatformApplicationArn"], "Token": "tok"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    result = _get_endpoint_attributes(
        store,
        {"EndpointArn": ep["EndpointArn"]},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert result["Attributes"]["Enabled"] == "true"

    _set_endpoint_attributes(
        store,
        {
            "EndpointArn": ep["EndpointArn"],
            "Attributes.entry.1.key": "Enabled",
            "Attributes.entry.1.value": "false",
        },
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    result2 = _get_endpoint_attributes(
        store,
        {"EndpointArn": ep["EndpointArn"]},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert result2["Attributes"]["Enabled"] == "false"


def test_delete_endpoint(store, mock_request):
    app = _create_platform_application(
        store,
        {"Name": "ep-app3", "Platform": "GCM"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    ep = _create_platform_endpoint(
        store,
        {"PlatformApplicationArn": app["PlatformApplicationArn"], "Token": "tok"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    _delete_endpoint(store, {"EndpointArn": ep["EndpointArn"]}, REGION, ACCOUNT_ID, mock_request)
    assert store.get_platform_endpoint(ep["EndpointArn"]) is None


def test_list_endpoints_by_platform_application(store, mock_request):
    app = _create_platform_application(
        store,
        {"Name": "ep-app4", "Platform": "APNS"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    _create_platform_endpoint(
        store,
        {"PlatformApplicationArn": app["PlatformApplicationArn"], "Token": "tok1"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    _create_platform_endpoint(
        store,
        {"PlatformApplicationArn": app["PlatformApplicationArn"], "Token": "tok2"},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    result = _list_endpoints_by_platform_application(
        store,
        {"PlatformApplicationArn": app["PlatformApplicationArn"]},
        REGION,
        ACCOUNT_ID,
        mock_request,
    )
    assert len(result["Endpoints"]) == 2


# ---- Deliver to SQS ----


@patch("robotocore.services.sns.provider.get_sqs_store")
def test_deliver_to_sqs_wrapped(mock_get_sqs_store):
    mock_queue = MagicMock()
    mock_sqs_store = MagicMock()
    mock_sqs_store.get_queue.return_value = mock_queue
    mock_get_sqs_store.return_value = mock_sqs_store

    sub = SnsSubscription(
        subscription_arn="arn:aws:sns:us-east-1:123456789012:t:sub1",
        topic_arn="arn:aws:sns:us-east-1:123456789012:t",
        protocol="sqs",
        endpoint="arn:aws:sqs:us-east-1:123456789012:my-queue",
        raw_message_delivery=False,
    )
    _deliver_to_sqs(
        sub,
        "hello world",
        "Test Subject",
        {},
        "msg-id-1",
        "arn:aws:sns:us-east-1:123456789012:t",
        "us-east-1",
    )
    mock_queue.put.assert_called_once()
    sqs_msg = mock_queue.put.call_args[0][0]
    body = json.loads(sqs_msg.body)
    assert body["Type"] == "Notification"
    assert body["Message"] == "hello world"
    assert body["Subject"] == "Test Subject"
    assert body["TopicArn"] == "arn:aws:sns:us-east-1:123456789012:t"


@patch("robotocore.services.sns.provider.get_sqs_store")
def test_deliver_to_sqs_raw(mock_get_sqs_store):
    mock_queue = MagicMock()
    mock_sqs_store = MagicMock()
    mock_sqs_store.get_queue.return_value = mock_queue
    mock_get_sqs_store.return_value = mock_sqs_store

    sub = SnsSubscription(
        subscription_arn="arn:aws:sns:us-east-1:123456789012:t:sub1",
        topic_arn="arn:aws:sns:us-east-1:123456789012:t",
        protocol="sqs",
        endpoint="arn:aws:sqs:us-east-1:123456789012:my-queue",
        raw_message_delivery=True,
    )
    _deliver_to_sqs(
        sub,
        "raw body",
        None,
        {"color": {"DataType": "String", "StringValue": "red"}},
        "msg-id-2",
        "arn:aws:sns:us-east-1:123456789012:t",
        "us-east-1",
    )
    mock_queue.put.assert_called_once()
    sqs_msg = mock_queue.put.call_args[0][0]
    assert sqs_msg.body == "raw body"
    assert sqs_msg.message_attributes["color"]["StringValue"] == "red"


@patch("robotocore.services.sns.provider.get_sqs_store")
def test_deliver_to_sqs_no_subject_in_wrapped(mock_get_sqs_store):
    """When no subject is provided, it should not appear in the notification."""
    mock_queue = MagicMock()
    mock_sqs_store = MagicMock()
    mock_sqs_store.get_queue.return_value = mock_queue
    mock_get_sqs_store.return_value = mock_sqs_store

    sub = SnsSubscription(
        subscription_arn="arn:aws:sns:us-east-1:123456789012:t:sub1",
        topic_arn="arn:aws:sns:us-east-1:123456789012:t",
        protocol="sqs",
        endpoint="arn:aws:sqs:us-east-1:123456789012:my-queue",
        raw_message_delivery=False,
    )
    _deliver_to_sqs(
        sub,
        "no subject msg",
        None,
        {},
        "msg-id-3",
        "arn:aws:sns:us-east-1:123456789012:t",
        "us-east-1",
    )
    sqs_msg = mock_queue.put.call_args[0][0]
    body = json.loads(sqs_msg.body)
    assert "Subject" not in body


@patch("robotocore.services.sns.provider.get_sqs_store")
def test_deliver_to_sqs_queue_not_found(mock_get_sqs_store):
    """Delivery to a non-existent queue should silently return."""
    mock_sqs_store = MagicMock()
    mock_sqs_store.get_queue.return_value = None
    mock_get_sqs_store.return_value = mock_sqs_store

    sub = SnsSubscription(
        subscription_arn="arn:aws:sns:us-east-1:123456789012:t:sub1",
        topic_arn="arn:aws:sns:us-east-1:123456789012:t",
        protocol="sqs",
        endpoint="arn:aws:sqs:us-east-1:123456789012:gone-queue",
        raw_message_delivery=False,
    )
    # Should not raise
    _deliver_to_sqs(
        sub,
        "msg",
        None,
        {},
        "msg-id-4",
        "arn:aws:sns:us-east-1:123456789012:t",
        "us-east-1",
    )


@patch("robotocore.services.sns.provider.get_sqs_store")
def test_deliver_to_sqs_with_message_attributes_in_notification(mock_get_sqs_store):
    mock_queue = MagicMock()
    mock_sqs_store = MagicMock()
    mock_sqs_store.get_queue.return_value = mock_queue
    mock_get_sqs_store.return_value = mock_sqs_store

    sub = SnsSubscription(
        subscription_arn="arn:aws:sns:us-east-1:123456789012:t:sub1",
        topic_arn="arn:aws:sns:us-east-1:123456789012:t",
        protocol="sqs",
        endpoint=SQS_ARN,
        raw_message_delivery=False,
    )
    attrs = {"event_type": {"DataType": "String", "StringValue": "order_created"}}
    _deliver_to_sqs(
        sub,
        "msg",
        None,
        attrs,
        "msg-id-5",
        "arn:aws:sns:us-east-1:123456789012:t",
        "us-east-1",
    )
    sqs_msg = mock_queue.put.call_args[0][0]
    body = json.loads(sqs_msg.body)
    assert "MessageAttributes" in body
    assert body["MessageAttributes"]["event_type"]["Type"] == "String"
    assert body["MessageAttributes"]["event_type"]["Value"] == "order_created"


@patch("robotocore.services.sns.provider.get_sqs_store")
def test_deliver_to_sqs_cross_region(mock_get_sqs_store):
    """SQS delivery should use the queue ARN's region, not the topic's region."""
    mock_queue = MagicMock()
    mock_sqs_store = MagicMock()
    mock_sqs_store.get_queue.return_value = mock_queue
    mock_get_sqs_store.return_value = mock_sqs_store

    sub = SnsSubscription(
        subscription_arn="arn:aws:sns:us-east-1:123456789012:t:sub1",
        topic_arn="arn:aws:sns:us-east-1:123456789012:t",
        protocol="sqs",
        endpoint="arn:aws:sqs:eu-west-1:123456789012:cross-region-q",
        raw_message_delivery=False,
    )
    _deliver_to_sqs(
        sub,
        "cross-region",
        None,
        {},
        "msg-id-6",
        "arn:aws:sns:us-east-1:123456789012:t",
        "us-east-1",
    )
    # Should have called get_sqs_store with "eu-west-1"
    mock_get_sqs_store.assert_called_with("eu-west-1", "123456789012")


# ---- Deliver to Lambda ----


@patch("robotocore.services.lambda_.invoke.invoke_lambda_async")
def test_deliver_to_lambda(mock_invoke):
    from robotocore.services.sns.provider import _deliver_to_lambda

    sub = SnsSubscription(
        subscription_arn="arn:aws:sns:us-east-1:123456789012:t:sub1",
        topic_arn="arn:aws:sns:us-east-1:123456789012:t",
        protocol="lambda",
        endpoint="arn:aws:lambda:us-east-1:123456789012:function:my-func",
    )
    _deliver_to_lambda(
        sub,
        "lambda msg",
        "Subject",
        {"k": {"DataType": "String", "StringValue": "v"}},
        "msg-id-7",
        "arn:aws:sns:us-east-1:123456789012:t",
        "us-east-1",
    )
    mock_invoke.assert_called_once()
    call_args = mock_invoke.call_args
    event = call_args[0][1]
    assert len(event["Records"]) == 1
    record = event["Records"][0]
    assert record["EventSource"] == "aws:sns"
    assert record["Sns"]["Message"] == "lambda msg"
    assert record["Sns"]["Subject"] == "Subject"
    assert record["Sns"]["MessageAttributes"]["k"]["Value"] == "v"


# ---- Deliver to HTTP ----


@patch("urllib.request.urlopen")
def test_deliver_to_http(mock_urlopen):
    from robotocore.services.sns.provider import _deliver_to_http

    sub = SnsSubscription(
        subscription_arn="arn:aws:sns:us-east-1:123456789012:t:sub1",
        topic_arn="arn:aws:sns:us-east-1:123456789012:t",
        protocol="http",
        endpoint="http://example.com/webhook",
    )
    _deliver_to_http(
        sub,
        "http msg",
        "Subject",
        {},
        "msg-id-8",
        "arn:aws:sns:us-east-1:123456789012:t",
        "us-east-1",
    )
    mock_urlopen.assert_called_once()
    req = mock_urlopen.call_args[0][0]
    body = json.loads(req.data.decode())
    assert body["Type"] == "Notification"
    assert body["Message"] == "http msg"
    assert req.get_header("X-amz-sns-message-type") == "Notification"


# ---- Filter Policy Matching (models) ----


def test_filter_exact_string():
    assert _matches_filter_value("red", "red") is True
    assert _matches_filter_value("red", "blue") is False


def test_filter_numeric():
    assert _matches_filter_value(42, "42") is True
    assert _matches_filter_value(42, "43") is False


def test_filter_prefix():
    assert _matches_filter_value({"prefix": "foo"}, "foobar") is True
    assert _matches_filter_value({"prefix": "foo"}, "barfoo") is False


def test_filter_suffix():
    assert _matches_filter_value({"suffix": ".jpg"}, "photo.jpg") is True
    assert _matches_filter_value({"suffix": ".jpg"}, "photo.png") is False


def test_filter_exists():
    assert _matches_filter_value({"exists": True}, "anything") is True
    assert _matches_filter_value({"exists": False}, "anything") is False


def test_filter_anything_but_string():
    assert _check_anything_but("red", "blue") is True
    assert _check_anything_but("red", "red") is False


def test_filter_anything_but_list():
    assert _check_anything_but(["red", "green"], "blue") is True
    assert _check_anything_but(["red", "green"], "red") is False


def test_filter_anything_but_prefix():
    assert _check_anything_but({"prefix": "foo"}, "bar") is True
    assert _check_anything_but({"prefix": "foo"}, "foobar") is False


def test_filter_numeric_range():
    assert _check_numeric([">=", 100, "<", 200], "150") is True
    assert _check_numeric([">=", 100, "<", 200], "50") is False
    assert _check_numeric([">=", 100, "<", 200], "200") is False


def test_filter_numeric_equals():
    assert _check_numeric(["=", 42], "42") is True
    assert _check_numeric(["=", 42], "43") is False


def test_filter_cidr():
    assert _check_cidr("10.0.0.0/8", "10.1.2.3") is True
    assert _check_cidr("10.0.0.0/8", "192.168.1.1") is False


def test_filter_policy_all_keys_match():
    policy = {"color": ["red"], "size": ["large"]}
    attrs = {
        "color": {"StringValue": "red"},
        "size": {"StringValue": "large"},
    }
    assert _matches_filter_policy(policy, attrs) is True


def test_filter_policy_missing_key():
    policy = {"color": ["red"], "size": ["large"]}
    attrs = {"color": {"StringValue": "red"}}
    assert _matches_filter_policy(policy, attrs) is False


def test_filter_policy_exists_false():
    policy = {"color": [{"exists": False}]}
    assert _matches_filter_policy(policy, {}) is True
    assert _matches_filter_policy(policy, {"color": {"StringValue": "red"}}) is False


def test_filter_policy_on_body():
    policy = {"category": ["electronics"]}
    body = {"category": "electronics", "price": 50}
    assert _matches_filter_policy_on_body(policy, body) is True


def test_filter_policy_on_body_no_match():
    policy = {"category": ["electronics"]}
    body = {"category": "clothing"}
    assert _matches_filter_policy_on_body(policy, body) is False


# ---- SnsSubscription.matches_filter ----


def test_subscription_matches_filter_no_policy():
    sub = SnsSubscription(
        subscription_arn="arn",
        topic_arn="arn",
        protocol="sqs",
        endpoint="arn",
    )
    assert sub.matches_filter({}) is True


def test_subscription_matches_filter_message_body_scope():
    sub = SnsSubscription(
        subscription_arn="arn",
        topic_arn="arn",
        protocol="sqs",
        endpoint="arn",
        filter_policy={"status": ["active"]},
        filter_policy_scope="MessageBody",
    )
    assert sub.matches_filter({}, message_body='{"status": "active"}') is True
    assert sub.matches_filter({}, message_body='{"status": "inactive"}') is False


def test_subscription_matches_filter_message_body_invalid_json():
    sub = SnsSubscription(
        subscription_arn="arn",
        topic_arn="arn",
        protocol="sqs",
        endpoint="arn",
        filter_policy={"status": ["active"]},
        filter_policy_scope="MessageBody",
    )
    assert sub.matches_filter({}, message_body="not json") is False


# ---- _parse_member_list ----


def test_parse_member_list_query():
    params = {"Ids.member.1": "a", "Ids.member.2": "b", "Ids.member.3": "c"}
    assert _parse_member_list(params, "Ids") == ["a", "b", "c"]


def test_parse_member_list_json():
    params = {"Ids": ["x", "y"]}
    assert _parse_member_list(params, "Ids") == ["x", "y"]


def test_parse_member_list_empty():
    assert _parse_member_list({}, "Ids") == []


# ---- SnsError ----


def test_sns_error():
    err = SnsError("NotFound", "not found msg", 404)
    assert err.code == "NotFound"
    assert err.message == "not found msg"
    assert err.status == 404


def test_sns_error_default_status():
    err = SnsError("InvalidParameter", "bad param")
    assert err.status == 400
