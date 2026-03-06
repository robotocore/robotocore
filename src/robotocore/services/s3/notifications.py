"""S3 event notification support — fires events to SQS/SNS on object mutations."""

import json
import threading
import time
import uuid
from dataclasses import dataclass, field

from robotocore.services.sns.provider import _get_store as get_sns_store
from robotocore.services.sqs.models import SqsMessage
from robotocore.services.sqs.provider import _get_store as get_sqs_store
from robotocore.services.sqs.provider import _md5


@dataclass
class NotificationConfig:
    """Parsed bucket notification configuration."""

    queue_configs: list[dict] = field(default_factory=list)
    topic_configs: list[dict] = field(default_factory=list)
    lambda_configs: list[dict] = field(default_factory=list)


_bucket_notifications: dict[str, NotificationConfig] = {}
_lock = threading.Lock()


def set_notification_config(bucket: str, config: NotificationConfig) -> None:
    with _lock:
        _bucket_notifications[bucket] = config


def get_notification_config(bucket: str) -> NotificationConfig:
    with _lock:
        return _bucket_notifications.get(bucket, NotificationConfig())


def fire_event(
    event_name: str,
    bucket: str,
    key: str,
    region: str = "us-east-1",
    account_id: str = "123456789012",
    size: int = 0,
    etag: str = "",
) -> None:
    """Fire S3 event notifications for a bucket mutation."""
    config = get_notification_config(bucket)
    if not config.queue_configs and not config.topic_configs:
        return

    record = _build_event_record(event_name, bucket, key, region, account_id, size, etag)
    message = json.dumps({"Records": [record]})

    for qc in config.queue_configs:
        if _event_matches(event_name, qc.get("Events", []), key, qc.get("Filter")):
            _deliver_to_sqs(qc["QueueArn"], message, region)

    for tc in config.topic_configs:
        if _event_matches(event_name, tc.get("Events", []), key, tc.get("Filter")):
            _deliver_to_sns(tc["TopicArn"], message, region)


def _event_matches(event_name: str, events: list[str], key: str, filter_rules: dict | None) -> bool:
    matched = False
    for evt in events:
        if evt == "s3:*" or event_name.startswith(evt.rstrip("*")):
            matched = True
            break
    if not matched:
        return False

    if filter_rules:
        rules = filter_rules.get("Key", {}).get("FilterRules", [])
        for rule in rules:
            name = rule.get("Name", "")
            value = rule.get("Value", "")
            if name == "prefix" and not key.startswith(value):
                return False
            if name == "suffix" and not key.endswith(value):
                return False
    return True


def _build_event_record(
    event_name: str,
    bucket: str,
    key: str,
    region: str,
    account_id: str,
    size: int,
    etag: str,
) -> dict:
    return {
        "eventVersion": "2.1",
        "eventSource": "aws:s3",
        "awsRegion": region,
        "eventTime": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
        "eventName": event_name.split(":")[-1],
        "userIdentity": {"principalId": account_id},
        "requestParameters": {"sourceIPAddress": "127.0.0.1"},
        "responseElements": {},
        "s3": {
            "s3SchemaVersion": "1.0",
            "configurationId": str(uuid.uuid4()),
            "bucket": {
                "name": bucket,
                "ownerIdentity": {"principalId": account_id},
                "arn": f"arn:aws:s3:::{bucket}",
            },
            "object": {
                "key": key,
                "size": size,
                "eTag": etag,
                "sequencer": format(int(time.time() * 1000), "016X"),
            },
        },
    }


def _deliver_to_sqs(queue_arn: str, message: str, region: str) -> None:
    sqs_store = get_sqs_store(region)
    queue_name = queue_arn.rsplit(":", 1)[-1]
    queue = sqs_store.get_queue(queue_name)
    if not queue:
        return
    msg = SqsMessage(
        message_id=str(uuid.uuid4()),
        body=message,
        md5_of_body=_md5(message),
    )
    queue.put(msg)


def _deliver_to_sns(topic_arn: str, message: str, region: str) -> None:
    sns_store = get_sns_store(region)
    topic = sns_store.get_topic(topic_arn)
    if not topic:
        return
    # Publish to all SQS subscribers of this topic
    from robotocore.services.sns.provider import _deliver_to_subscriber

    for sub in topic.subscriptions:
        if sub.confirmed:
            _deliver_to_subscriber(
                sub, message, "S3 Notification", {}, str(uuid.uuid4()), topic_arn, region
            )
