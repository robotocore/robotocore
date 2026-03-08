"""In-memory SNS data models: topics, subscriptions, and message delivery."""

import hashlib
import ipaddress
import json
import threading
import time
import uuid
from dataclasses import dataclass, field


def _new_id() -> str:
    return str(uuid.uuid4())


def _matches_filter_value(rule, actual_value: str) -> bool:
    """Check if a single filter rule matches the actual attribute value.

    Supports: exact string match, prefix, numeric, exists, anything-but, cidr.
    """
    if isinstance(rule, str):
        return actual_value == rule
    if isinstance(rule, (int, float)):
        try:
            return float(actual_value) == float(rule)
        except (ValueError, TypeError):
            return False
    if isinstance(rule, dict):
        if "prefix" in rule:
            return actual_value.startswith(rule["prefix"])
        if "numeric" in rule:
            return _check_numeric(rule["numeric"], actual_value)
        if "exists" in rule:
            # The "exists" rule is handled at the policy level
            # If we're here, the key exists, so exists=true matches
            return rule["exists"] is True
        if "anything-but" in rule:
            return _check_anything_but(rule["anything-but"], actual_value)
        if "cidr" in rule:
            return _check_cidr(rule["cidr"], actual_value)
    return False


def _check_numeric(conditions: list, actual_value: str) -> bool:
    """Evaluate numeric filter conditions like [">=", 100, "<", 200]."""
    try:
        val = float(actual_value)
    except (ValueError, TypeError):
        return False

    i = 0
    while i < len(conditions):
        op = conditions[i]
        threshold = float(conditions[i + 1])
        if op == "=" and not (val == threshold):
            return False
        elif op == ">" and not (val > threshold):
            return False
        elif op == ">=" and not (val >= threshold):
            return False
        elif op == "<" and not (val < threshold):
            return False
        elif op == "<=" and not (val <= threshold):
            return False
        i += 2
    return True


def _check_anything_but(exclusion, actual_value: str) -> bool:
    """Check anything-but filter: value must not match any excluded values."""
    if isinstance(exclusion, list):
        return actual_value not in [str(v) for v in exclusion]
    if isinstance(exclusion, dict):
        if "prefix" in exclusion:
            return not actual_value.startswith(exclusion["prefix"])
    if isinstance(exclusion, str):
        return actual_value != exclusion
    if isinstance(exclusion, (int, float)):
        try:
            return float(actual_value) != float(exclusion)
        except (ValueError, TypeError):
            return True
    return True


def _check_cidr(cidr: str, actual_value: str) -> bool:
    """Check if actual_value IP is within the CIDR block."""
    try:
        network = ipaddress.ip_network(cidr, strict=False)
        addr = ipaddress.ip_address(actual_value)
        return addr in network
    except (ValueError, TypeError):
        return False


def _matches_filter_policy(filter_policy: dict, message_attributes: dict) -> bool:
    """Evaluate a complete filter policy against message attributes.

    Each key in the policy must be present in message_attributes (unless
    exists: false), and at least one rule in the value list must match.
    """
    for key, rules in filter_policy.items():
        if not isinstance(rules, list):
            rules = [rules]

        # Check for exists: false first
        has_exists_false = any(isinstance(r, dict) and r.get("exists") is False for r in rules)

        if key not in message_attributes:
            if has_exists_false:
                continue  # Key absent + exists:false = match
            return False  # Key missing = no match

        attr = message_attributes[key]
        value = attr.get("Value") or attr.get("StringValue", "")

        # Check if any rule matches
        matched = False
        for rule in rules:
            if isinstance(rule, dict) and rule.get("exists") is False:
                # Key is present but rule says exists:false -> this rule fails
                continue
            if _matches_filter_value(rule, value):
                matched = True
                break

        if not matched:
            return False

    return True


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
    filter_policy_scope: str = "MessageAttributes"
    attributes: dict = field(default_factory=dict)

    def matches_filter(self, message_attributes: dict) -> bool:
        if not self.filter_policy:
            return True
        return _matches_filter_policy(self.filter_policy, message_attributes)


@dataclass
class SnsTopic:
    arn: str
    name: str
    region: str
    account_id: str
    attributes: dict = field(default_factory=dict)
    subscriptions: list[SnsSubscription] = field(default_factory=list)
    tags: dict = field(default_factory=dict)
    # FIFO dedup tracking
    _dedup_cache: dict = field(default_factory=dict)

    DEDUP_INTERVAL = 300  # 5 minutes

    @property
    def is_fifo(self) -> bool:
        return self.name.endswith(".fifo")

    @property
    def content_based_dedup(self) -> bool:
        return self.attributes.get("ContentBasedDeduplication", "false").lower() == "true"

    def check_dedup(
        self, message: str, dedup_id: str | None, group_id: str | None
    ) -> tuple[bool, str | None]:
        """Check FIFO deduplication. Returns (is_duplicate, resolved_dedup_id)."""
        if not self.is_fifo:
            return False, None

        resolved_id = dedup_id
        if not resolved_id and self.content_based_dedup:
            resolved_id = hashlib.sha256(message.encode()).hexdigest()

        if resolved_id:
            self._clean_dedup_cache()
            if resolved_id in self._dedup_cache:
                return True, resolved_id
            self._dedup_cache[resolved_id] = time.time()

        return False, resolved_id

    def _clean_dedup_cache(self) -> None:
        now = time.time()
        expired = [k for k, t in self._dedup_cache.items() if now - t > self.DEDUP_INTERVAL]
        for k in expired:
            del self._dedup_cache[k]


@dataclass
class PlatformApplication:
    """Basic stub for SNS platform application (no actual push delivery)."""

    arn: str
    name: str
    platform: str
    attributes: dict = field(default_factory=dict)


@dataclass
class PlatformEndpoint:
    """SNS platform endpoint for push notifications."""

    arn: str
    application_arn: str
    token: str
    attributes: dict = field(default_factory=dict)
    enabled: bool = True
    custom_user_data: str = ""


class SnsStore:
    """Per-region SNS store managing topics and subscriptions."""

    def __init__(self):
        self.topics: dict[str, SnsTopic] = {}
        self.subscriptions: dict[str, SnsSubscription] = {}
        self.platform_applications: dict[str, PlatformApplication] = {}
        self.platform_endpoints: dict[str, PlatformEndpoint] = {}
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

            # HTTP/HTTPS subscriptions start unconfirmed
            confirmed = protocol not in ("http", "https")

            sub = SnsSubscription(
                subscription_arn=sub_arn,
                topic_arn=topic_arn,
                protocol=protocol,
                endpoint=endpoint,
                owner=topic.account_id,
                confirmed=confirmed,
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
                if "FilterPolicyScope" in attributes:
                    sub.filter_policy_scope = attributes["FilterPolicyScope"]
            topic.subscriptions.append(sub)
            self.subscriptions[sub_arn] = sub
            return sub

    def confirm_subscription(self, topic_arn: str, token: str) -> SnsSubscription | None:
        """Confirm a pending subscription (for HTTP/HTTPS)."""
        with self.mutex:
            topic = self.topics.get(topic_arn)
            if not topic:
                return None
            # Find first unconfirmed subscription for this topic
            for sub in topic.subscriptions:
                if not sub.confirmed:
                    sub.confirmed = True
                    return sub
            return None

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

    # --- Platform Applications (stubs) ---

    def create_platform_application(
        self,
        name: str,
        platform: str,
        region: str,
        account_id: str,
        attributes: dict | None = None,
    ) -> PlatformApplication:
        arn = f"arn:aws:sns:{region}:{account_id}:app/{platform}/{name}"
        with self.mutex:
            if arn in self.platform_applications:
                return self.platform_applications[arn]
            app = PlatformApplication(
                arn=arn,
                name=name,
                platform=platform,
                attributes=attributes or {},
            )
            self.platform_applications[arn] = app
            return app

    def get_platform_application(self, arn: str) -> PlatformApplication | None:
        return self.platform_applications.get(arn)

    def delete_platform_application(self, arn: str) -> bool:
        with self.mutex:
            return self.platform_applications.pop(arn, None) is not None

    def list_platform_applications(self) -> list[PlatformApplication]:
        return list(self.platform_applications.values())

    # --- Platform Endpoints ---

    def create_platform_endpoint(
        self,
        application_arn: str,
        token: str,
        custom_user_data: str = "",
        attributes: dict | None = None,
    ) -> PlatformEndpoint | None:
        app = self.platform_applications.get(application_arn)
        if not app:
            return None
        endpoint_id = str(uuid.uuid4())
        arn = application_arn.replace(":app/", ":endpoint/") + f"/{endpoint_id}"
        with self.mutex:
            ep = PlatformEndpoint(
                arn=arn,
                application_arn=application_arn,
                token=token,
                attributes=attributes or {},
                custom_user_data=custom_user_data,
            )
            ep.attributes.setdefault("Enabled", "true")
            ep.attributes.setdefault("Token", token)
            if custom_user_data:
                ep.attributes.setdefault("CustomUserData", custom_user_data)
            self.platform_endpoints[arn] = ep
            return ep

    def get_platform_endpoint(self, arn: str) -> PlatformEndpoint | None:
        return self.platform_endpoints.get(arn)

    def delete_platform_endpoint(self, arn: str) -> bool:
        with self.mutex:
            return self.platform_endpoints.pop(arn, None) is not None

    def list_endpoints_by_platform_application(
        self, application_arn: str
    ) -> list[PlatformEndpoint]:
        return [
            ep for ep in self.platform_endpoints.values() if ep.application_arn == application_arn
        ]
