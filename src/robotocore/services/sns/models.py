"""In-memory SNS data models: topics, subscriptions, and message delivery."""

import json
import threading
import uuid
from dataclasses import dataclass, field


def _new_id() -> str:
    return str(uuid.uuid4())


@dataclass
class SnsSubscription:
    subscription_arn: str
    topic_arn: str
    protocol: str
    endpoint: str
    owner: str = "123456789012"
    confirmed: bool = True
    raw_message_delivery: bool = False
    filter_policy: dict | None = None
    attributes: dict = field(default_factory=dict)

    def matches_filter(self, message_attributes: dict) -> bool:
        if not self.filter_policy:
            return True
        for key, allowed_values in self.filter_policy.items():
            if key not in message_attributes:
                return False
            attr = message_attributes[key]
            value = attr.get("Value") or attr.get("StringValue", "")
            if isinstance(allowed_values, list):
                if value not in allowed_values:
                    return False
            elif value != allowed_values:
                return False
        return True


@dataclass
class SnsTopic:
    arn: str
    name: str
    region: str
    account_id: str
    attributes: dict = field(default_factory=dict)
    subscriptions: list[SnsSubscription] = field(default_factory=list)
    tags: dict = field(default_factory=dict)

    @property
    def is_fifo(self) -> bool:
        return self.name.endswith(".fifo")


class SnsStore:
    """Per-region SNS store managing topics and subscriptions."""

    def __init__(self):
        self.topics: dict[str, SnsTopic] = {}
        self.subscriptions: dict[str, SnsSubscription] = {}
        self.mutex = threading.RLock()

    def create_topic(
        self, name: str, region: str, account_id: str, attributes: dict | None = None
    ) -> SnsTopic:
        arn = f"arn:aws:sns:{region}:{account_id}:{name}"
        with self.mutex:
            if arn in self.topics:
                return self.topics[arn]
            topic = SnsTopic(
                arn=arn,
                name=name,
                region=region,
                account_id=account_id,
                attributes=attributes or {},
            )
            self.topics[arn] = topic
            return topic

    def get_topic(self, arn: str) -> SnsTopic | None:
        return self.topics.get(arn)

    def delete_topic(self, arn: str) -> bool:
        with self.mutex:
            topic = self.topics.pop(arn, None)
            if topic:
                for sub in topic.subscriptions:
                    self.subscriptions.pop(sub.subscription_arn, None)
                return True
            return False

    def list_topics(self) -> list[SnsTopic]:
        return list(self.topics.values())

    def subscribe(
        self,
        topic_arn: str,
        protocol: str,
        endpoint: str,
        attributes: dict | None = None,
    ) -> SnsSubscription | None:
        with self.mutex:
            topic = self.topics.get(topic_arn)
            if not topic:
                return None
            sub_id = _new_id()
            sub_arn = f"{topic_arn}:{sub_id}"
            sub = SnsSubscription(
                subscription_arn=sub_arn,
                topic_arn=topic_arn,
                protocol=protocol,
                endpoint=endpoint,
                owner=topic.account_id,
                attributes=attributes or {},
            )
            if attributes:
                if "RawMessageDelivery" in attributes:
                    sub.raw_message_delivery = attributes["RawMessageDelivery"].lower() == "true"
                if "FilterPolicy" in attributes:
                    sub.filter_policy = (
                        json.loads(attributes["FilterPolicy"])
                        if isinstance(attributes["FilterPolicy"], str)
                        else attributes["FilterPolicy"]
                    )
            topic.subscriptions.append(sub)
            self.subscriptions[sub_arn] = sub
            return sub

    def unsubscribe(self, subscription_arn: str) -> bool:
        with self.mutex:
            sub = self.subscriptions.pop(subscription_arn, None)
            if not sub:
                return False
            topic = self.topics.get(sub.topic_arn)
            if topic:
                topic.subscriptions = [
                    s for s in topic.subscriptions if s.subscription_arn != subscription_arn
                ]
            return True

    def get_subscription(self, arn: str) -> SnsSubscription | None:
        return self.subscriptions.get(arn)

    def list_subscriptions(self, topic_arn: str | None = None) -> list[SnsSubscription]:
        if topic_arn:
            topic = self.topics.get(topic_arn)
            return list(topic.subscriptions) if topic else []
        return list(self.subscriptions.values())
