"""SNS query-protocol fidelity: XML escaping and topic attribute defaults."""

import os
import uuid

import boto3

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")


def _client(service: str, account: str = "123456789012", **kw):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT_URL,
        region_name=kw.pop("region_name", "us-east-1"),
        aws_access_key_id=kw.pop("aws_access_key_id", account),
        aws_secret_access_key="test",
        **kw,
    )


class TestSnsXmlEscaping:
    def test_subscription_endpoint_with_query_string_round_trips(self):
        sns = _client("sns", region_name="us-west-2")
        topic = sns.create_topic(Name=f"tf-compat-{uuid.uuid4().hex[:8]}")["TopicArn"]
        endpoint = "https://api.example.io/v2/alert_events/cloudwatch/x?token=a&service=b<c>"
        sub = sns.subscribe(
            TopicArn=topic, Protocol="https", Endpoint=endpoint, ReturnSubscriptionArn=True
        )["SubscriptionArn"]
        attrs = sns.get_subscription_attributes(SubscriptionArn=sub)["Attributes"]
        assert attrs["Endpoint"] == endpoint
        listed = sns.list_subscriptions_by_topic(TopicArn=topic)["Subscriptions"]
        assert listed[0]["Endpoint"] == endpoint


class TestSnsTopicDefaults:
    def test_display_name_empty_until_set(self):
        sns = _client("sns")
        topic = sns.create_topic(Name=f"tf-compat-{uuid.uuid4().hex[:8]}")["TopicArn"]
        assert sns.get_topic_attributes(TopicArn=topic)["Attributes"]["DisplayName"] == ""
