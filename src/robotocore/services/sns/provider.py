"""Native SNS provider with cross-service SQS, Lambda, Firehose, and HTTP/HTTPS delivery."""

import hashlib
import json
import logging
import threading
import uuid
from collections.abc import Callable
from datetime import UTC

from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.sns.models import SnsStore, SnsSubscription
from robotocore.services.sqs.models import SqsMessage
from robotocore.services.sqs.provider import _get_store as get_sqs_store

logger = logging.getLogger(__name__)

_stores: dict[str, SnsStore] = {}
_store_lock = threading.Lock()


def _get_store(region: str = "us-east-1") -> SnsStore:
    with _store_lock:
        if region not in _stores:
            _stores[region] = SnsStore()
        return _stores[region]


def _md5(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()


def _new_id() -> str:
    return str(uuid.uuid4())


class SnsError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


async def handle_sns_request(request: Request, region: str, account_id: str) -> Response:
    """Handle an SNS API request."""
    body = await request.body()
    content_type = request.headers.get("content-type", "")
    target = request.headers.get("x-amz-target", "")

    if target and "application/x-amz-json" in content_type:
        action = target.split(".")[-1]
        params = json.loads(body) if body else {}
        use_json = True
    else:
        from urllib.parse import parse_qs

        if "x-www-form-urlencoded" in content_type:
            parsed = parse_qs(body.decode(), keep_blank_values=True)
        else:
            parsed = parse_qs(str(request.url.query), keep_blank_values=True)
        params = {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}
        action = params.get("Action", "")
        use_json = False

    store = _get_store(region)
    handler = _ACTION_MAP.get(action)
    if handler is None:
        # Fall back to Moto for operations we don't intercept
        from robotocore.providers.moto_bridge import forward_to_moto

        return await forward_to_moto(request, "sns")

    try:
        result = handler(store, params, region, account_id, request)
        if use_json:
            return _json_response(result)
        else:
            return _xml_response(action + "Response", result)
    except SnsError as e:
        return _error(e.code, e.message, e.status, use_json)
    except Exception as e:
        return _error("InternalError", str(e), 500, use_json)


# --- Actions ---


def _create_topic(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    name = params.get("Name", "")
    attributes = params.get("Attributes", {})
    tags = params.get("Tags", [])
    # Query protocol tags: Tags.member.N.Key / Tags.member.N.Value
    i = 1
    while f"Tags.member.{i}.Key" in params:
        tags.append(
            {
                "Key": params[f"Tags.member.{i}.Key"],
                "Value": params.get(f"Tags.member.{i}.Value", ""),
            }
        )
        i += 1
    # Query protocol attributes
    for key, value in params.items():
        if key.startswith("Attributes.entry.") and key.endswith(".key"):
            idx = key.split(".")[2]
            attributes[value] = params.get(f"Attributes.entry.{idx}.value", "")
    # Validate FIFO topic naming
    if attributes.get("FifoTopic", "false").lower() == "true":
        if not name.endswith(".fifo"):
            raise SnsError(
                "InvalidParameter",
                "FIFO topic name must end with .fifo",
            )
    topic = store.create_topic(name, region, account_id, attributes)
    # Apply tags
    if tags:
        for tag in tags:
            topic.tags[tag.get("Key", "")] = tag.get("Value", "")
    return {"TopicArn": topic.arn}


def _delete_topic(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    arn = params.get("TopicArn", "")
    store.delete_topic(arn)
    return {}


def _list_topics(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    topics = store.list_topics()
    return {"Topics": [{"TopicArn": t.arn} for t in topics]}


def _get_topic_attributes(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    arn = params.get("TopicArn", "")
    topic = store.get_topic(arn)
    if not topic:
        raise SnsError("NotFound", f"Topic {arn} not found", 404)
    # Start with user-set attributes, then overlay computed fields
    # so computed values cannot be overwritten by stale stored attributes
    attrs = dict(topic.attributes)
    computed = {
        "TopicArn": topic.arn,
        "Owner": topic.account_id,
        "DisplayName": topic.attributes.get("DisplayName", topic.name),
        "SubscriptionsConfirmed": str(len(topic.subscriptions)),
        "SubscriptionsPending": "0",
        "SubscriptionsDeleted": "0",
    }
    if topic.is_fifo:
        computed["FifoTopic"] = "true"
        computed["ContentBasedDeduplication"] = topic.attributes.get(
            "ContentBasedDeduplication", "false"
        )
    attrs.update(computed)
    return {"Attributes": attrs}


def _set_topic_attributes(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    arn = params.get("TopicArn", "")
    topic = store.get_topic(arn)
    if not topic:
        raise SnsError("NotFound", f"Topic {arn} not found", 404)
    attr_name = params.get("AttributeName", "")
    attr_value = params.get("AttributeValue", "")
    if attr_name:
        topic.attributes[attr_name] = attr_value
    return {}


def _subscribe(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    topic_arn = params.get("TopicArn", "")
    protocol = params.get("Protocol", "")
    endpoint = params.get("Endpoint", "")
    attributes = params.get("Attributes", {})
    # Query protocol attributes: Attributes.entry.N.key / Attributes.entry.N.value
    for key, value in params.items():
        if key.startswith("Attributes.entry.") and key.endswith(".key"):
            idx = key.split(".")[2]
            attributes[value] = params.get(f"Attributes.entry.{idx}.value", "")
    sub = store.subscribe(topic_arn, protocol, endpoint, attributes)
    if not sub:
        raise SnsError("NotFound", f"Topic {topic_arn} not found", 404)

    # For HTTP/HTTPS, send subscription confirmation
    if protocol in ("http", "https"):
        _send_subscription_confirmation(sub, topic_arn, region)

    return {"SubscriptionArn": sub.subscription_arn}


def _confirm_subscription(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    topic_arn = params.get("TopicArn", "")
    token = params.get("Token", "")
    sub = store.confirm_subscription(topic_arn, token)
    if not sub:
        raise SnsError("NotFound", f"No pending subscription for topic {topic_arn}", 404)
    return {"SubscriptionArn": sub.subscription_arn}


def _unsubscribe(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    arn = params.get("SubscriptionArn", "")
    store.unsubscribe(arn)
    return {}


def _list_subscriptions(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    subs = store.list_subscriptions()
    return {"Subscriptions": [_sub_to_dict(s) for s in subs]}


def _list_subscriptions_by_topic(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    topic_arn = params.get("TopicArn", "")
    subs = store.list_subscriptions(topic_arn)
    return {"Subscriptions": [_sub_to_dict(s) for s in subs]}


def _get_subscription_attributes(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    arn = params.get("SubscriptionArn", "")
    sub = store.get_subscription(arn)
    if not sub:
        raise SnsError("NotFound", f"Subscription {arn} not found", 404)
    attrs = {
        "SubscriptionArn": sub.subscription_arn,
        "TopicArn": sub.topic_arn,
        "Protocol": sub.protocol,
        "Endpoint": sub.endpoint,
        "Owner": sub.owner,
        "RawMessageDelivery": str(sub.raw_message_delivery).lower(),
        "PendingConfirmation": str(not sub.confirmed).lower(),
    }
    if sub.filter_policy:
        attrs["FilterPolicy"] = json.dumps(sub.filter_policy)
    if sub.filter_policy_scope:
        attrs["FilterPolicyScope"] = sub.filter_policy_scope
    return {"Attributes": attrs}


def _set_subscription_attributes(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    arn = params.get("SubscriptionArn", "")
    sub = store.get_subscription(arn)
    if not sub:
        raise SnsError("NotFound", f"Subscription {arn} not found", 404)
    attr_name = params.get("AttributeName", "")
    attr_value = params.get("AttributeValue", "")
    if attr_name == "RawMessageDelivery":
        sub.raw_message_delivery = attr_value.lower() == "true"
    elif attr_name == "FilterPolicy":
        sub.filter_policy = json.loads(attr_value) if attr_value else None
    elif attr_name == "FilterPolicyScope":
        sub.filter_policy_scope = attr_value
    sub.attributes[attr_name] = attr_value
    return {}


def _publish(store: SnsStore, params: dict, region: str, account_id: str, request: Request) -> dict:
    topic_arn = params.get("TopicArn", "")
    target_arn = params.get("TargetArn", "")
    arn = topic_arn or target_arn

    topic = store.get_topic(arn)
    if not topic:
        raise SnsError("NotFound", f"Topic {arn} not found", 404)

    message = params.get("Message", "")
    subject = params.get("Subject")
    message_attributes = params.get("MessageAttributes", {})
    message_group_id = params.get("MessageGroupId")
    message_dedup_id = params.get("MessageDeduplicationId")

    # Parse query protocol message attributes
    i = 1
    while f"MessageAttributes.entry.{i}.Name" in params:
        name = params[f"MessageAttributes.entry.{i}.Name"]
        data_type = params.get(f"MessageAttributes.entry.{i}.Value.DataType", "String")
        string_value = params.get(f"MessageAttributes.entry.{i}.Value.StringValue", "")
        message_attributes[name] = {
            "DataType": data_type,
            "StringValue": string_value,
        }
        i += 1

    message_id = _new_id()

    # FIFO topic validation and deduplication
    if topic.is_fifo:
        if not message_group_id:
            raise SnsError(
                "InvalidParameter",
                "The MessageGroupId parameter is required for FIFO topics.",
            )
        is_dup, _ = topic.check_dedup(message, message_dedup_id, message_group_id)
        if is_dup:
            return {"MessageId": message_id}

    # Deliver to subscribers
    for sub in topic.subscriptions:
        if not sub.confirmed:
            continue
        if not sub.matches_filter(message_attributes):
            continue
        _deliver_to_subscriber(sub, message, subject, message_attributes, message_id, arn, region)

    result = {"MessageId": message_id}
    if topic.is_fifo and message_group_id:
        result["SequenceNumber"] = str(int(time.time() * 1000000))
    return result


def _publish_batch(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    topic_arn = params.get("TopicArn", "")
    topic = store.get_topic(topic_arn)
    if not topic:
        raise SnsError("NotFound", f"Topic {topic_arn} not found", 404)

    # Parse entries from query params: PublishBatchRequestEntries.member.N.Field
    entries = []
    i = 1
    while f"PublishBatchRequestEntries.member.{i}.Id" in params:
        entry = {
            "Id": params[f"PublishBatchRequestEntries.member.{i}.Id"],
            "Message": params.get(f"PublishBatchRequestEntries.member.{i}.Message", ""),
        }
        subject = params.get(f"PublishBatchRequestEntries.member.{i}.Subject")
        if subject:
            entry["Subject"] = subject
        group_id = params.get(f"PublishBatchRequestEntries.member.{i}.MessageGroupId")
        if group_id:
            entry["MessageGroupId"] = group_id
        dedup_id = params.get(f"PublishBatchRequestEntries.member.{i}.MessageDeduplicationId")
        if dedup_id:
            entry["MessageDeduplicationId"] = dedup_id
        i += 1
        entries.append(entry)

    # Validate max 10 entries
    if len(entries) > 10:
        raise SnsError(
            "TooManyEntriesInBatchRequest",
            "The batch request contains more entries than permissible.",
        )

    # Validate unique entry IDs
    entry_ids = [e["Id"] for e in entries]
    if len(entry_ids) != len(set(entry_ids)):
        raise SnsError(
            "BatchEntryIdsNotDistinct",
            "Two or more batch entries in the request have the same Id.",
        )

    successful = []
    for entry in entries:
        msg_id = _new_id()
        message = entry.get("Message", "")
        subject = entry.get("Subject")
        message_attributes = entry.get("MessageAttributes", {})

        for sub in topic.subscriptions:
            if not sub.confirmed:
                continue
            if not sub.matches_filter(message_attributes):
                continue
            _deliver_to_subscriber(
                sub,
                message,
                subject,
                message_attributes,
                msg_id,
                topic_arn,
                region,
            )

        successful.append({"Id": entry.get("Id", ""), "MessageId": msg_id})

    return {"Successful": successful, "Failed": []}


def _tag_resource(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    arn = params.get("ResourceArn", "")
    topic = store.get_topic(arn)
    if not topic:
        raise SnsError("ResourceNotFound", f"Resource {arn} not found", 404)
    tags = params.get("Tags", [])
    # Query protocol tags
    i = 1
    while f"Tags.member.{i}.Key" in params:
        tags.append(
            {
                "Key": params[f"Tags.member.{i}.Key"],
                "Value": params.get(f"Tags.member.{i}.Value", ""),
            }
        )
        i += 1
    for tag in tags:
        topic.tags[tag.get("Key", "")] = tag.get("Value", "")
    return {}


def _untag_resource(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    arn = params.get("ResourceArn", "")
    topic = store.get_topic(arn)
    if not topic:
        raise SnsError("ResourceNotFound", f"Resource {arn} not found", 404)
    keys = params.get("TagKeys", [])
    # Query protocol
    i = 1
    while f"TagKeys.member.{i}" in params:
        keys.append(params[f"TagKeys.member.{i}"])
        i += 1
    for key in keys:
        topic.tags.pop(key, None)
    return {}


def _list_tags_for_resource(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    arn = params.get("ResourceArn", "")
    topic = store.get_topic(arn)
    tags = []
    if topic:
        tags = [{"Key": k, "Value": v} for k, v in topic.tags.items()]
    return {"Tags": tags}


# --- Platform Application stubs ---


def _create_platform_application(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    name = params.get("Name", "")
    platform = params.get("Platform", "")
    attributes = params.get("Attributes", {})
    # Query protocol attributes
    for key, value in params.items():
        if key.startswith("Attributes.entry.") and key.endswith(".key"):
            idx = key.split(".")[2]
            attributes[value] = params.get(f"Attributes.entry.{idx}.value", "")
    app = store.create_platform_application(name, platform, region, account_id, attributes)
    return {"PlatformApplicationArn": app.arn}


def _delete_platform_application(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    arn = params.get("PlatformApplicationArn", "")
    store.delete_platform_application(arn)
    return {}


def _list_platform_applications(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    apps = store.list_platform_applications()
    return {
        "PlatformApplications": [
            {"PlatformApplicationArn": a.arn, "Attributes": a.attributes} for a in apps
        ]
    }


def _get_platform_application_attributes(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    arn = params.get("PlatformApplicationArn", "")
    app = store.get_platform_application(arn)
    if not app:
        raise SnsError("NotFound", f"Platform application {arn} not found", 404)
    return {"Attributes": app.attributes}


def _set_platform_application_attributes(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    arn = params.get("PlatformApplicationArn", "")
    app = store.get_platform_application(arn)
    if not app:
        raise SnsError("NotFound", f"Platform application {arn} not found", 404)
    attributes = params.get("Attributes", {})
    for key, value in params.items():
        if key.startswith("Attributes.entry.") and key.endswith(".key"):
            idx = key.split(".")[2]
            attributes[value] = params.get(f"Attributes.entry.{idx}.value", "")
    app.attributes.update(attributes)
    return {}


# --- Cross-service delivery ---


def _deliver_to_subscriber(
    sub: SnsSubscription,
    message: str,
    subject: str | None,
    message_attributes: dict,
    message_id: str,
    topic_arn: str,
    region: str,
) -> None:
    if sub.protocol == "sqs":
        _deliver_to_sqs(
            sub,
            message,
            subject,
            message_attributes,
            message_id,
            topic_arn,
            region,
        )
    elif sub.protocol == "lambda":
        _deliver_to_lambda(
            sub,
            message,
            subject,
            message_attributes,
            message_id,
            topic_arn,
            region,
        )
    elif sub.protocol in ("http", "https"):
        _deliver_to_http(
            sub,
            message,
            subject,
            message_attributes,
            message_id,
            topic_arn,
            region,
        )
    elif sub.protocol == "firehose":
        _deliver_to_firehose(
            sub,
            message,
            subject,
            message_attributes,
            message_id,
            topic_arn,
            region,
        )
    # Other protocols (email, sms, etc.) are no-ops for now


def _deliver_to_sqs(
    sub: SnsSubscription,
    message: str,
    subject: str | None,
    message_attributes: dict,
    message_id: str,
    topic_arn: str,
    region: str,
) -> None:
    # Parse region from the SQS queue ARN (arn:aws:sqs:<region>:<account>:<name>)
    # to support cross-region delivery
    arn_parts = sub.endpoint.split(":")
    queue_region = arn_parts[3] if len(arn_parts) >= 6 else region
    sqs_store = get_sqs_store(queue_region)
    # Endpoint is queue ARN -- extract queue name
    queue_name = sub.endpoint.rsplit(":", 1)[-1]
    queue = sqs_store.get_queue(queue_name)
    if not queue:
        return

    if sub.raw_message_delivery:
        body = message
    else:
        # Build SNS notification JSON matching AWS format
        notification: dict = {
            "Type": "Notification",
            "MessageId": message_id,
            "TopicArn": topic_arn,
            "Message": message,
            "Timestamp": _iso_timestamp(),
            "SignatureVersion": "1",
            "Signature": "EXAMPLE",
            "SigningCertURL": (f"https://sns.{region}.amazonaws.com/SimpleNotificationService.pem"),
            "UnsubscribeURL": (
                f"https://sns.{region}.amazonaws.com/"
                f"?Action=Unsubscribe"
                f"&SubscriptionArn={sub.subscription_arn}"
            ),
        }
        # Only include Subject when provided (matches real AWS)
        if subject:
            notification["Subject"] = subject
        # Include MessageAttributes in the notification JSON (matches real AWS)
        if message_attributes:
            sns_attrs = {}
            for attr_name, attr_val in message_attributes.items():
                sns_attrs[attr_name] = {
                    "Type": attr_val.get("DataType", "String"),
                    "Value": attr_val.get("StringValue", attr_val.get("Value", "")),
                }
            notification["MessageAttributes"] = sns_attrs
        body = json.dumps(notification)

    sqs_msg = SqsMessage(
        message_id=str(uuid.uuid4()),
        body=body,
        md5_of_body=_md5(body),
    )
    # Forward message attributes if raw delivery
    if sub.raw_message_delivery and message_attributes:
        sqs_msg.message_attributes = dict(message_attributes)
    queue.put(sqs_msg)


def _deliver_to_lambda(
    sub: SnsSubscription,
    message: str,
    subject: str | None,
    message_attributes: dict,
    message_id: str,
    topic_arn: str,
    region: str,
) -> None:
    """Deliver an SNS message to a Lambda function subscriber."""
    from robotocore.services.lambda_.invoke import invoke_lambda_async

    endpoint = sub.endpoint
    try:
        arn_parts = endpoint.split(":")
        account_id = arn_parts[4] if len(arn_parts) >= 5 else "123456789012"
    except (IndexError, ValueError):
        logger.warning("SNS: Cannot parse Lambda ARN from endpoint: %s", endpoint)
        return

    # Build SNS-to-Lambda event payload (matches AWS format)
    sns_message_attributes = {}
    for attr_name, attr_val in message_attributes.items():
        sns_message_attributes[attr_name] = {
            "Type": attr_val.get("DataType", "String"),
            "Value": attr_val.get("StringValue", attr_val.get("Value", "")),
        }

    timestamp = _iso_timestamp()
    event = {
        "Records": [
            {
                "EventSource": "aws:sns",
                "EventVersion": "1.0",
                "EventSubscriptionArn": sub.subscription_arn,
                "Sns": {
                    "Type": "Notification",
                    "MessageId": message_id,
                    "TopicArn": topic_arn,
                    "Subject": subject or "",
                    "Message": message,
                    "Timestamp": timestamp,
                    "SignatureVersion": "1",
                    "Signature": "EXAMPLE",
                    "SigningCertUrl": (
                        f"https://sns.{region}.amazonaws.com/SimpleNotificationService.pem"
                    ),
                    "UnsubscribeUrl": (
                        f"https://sns.{region}.amazonaws.com/"
                        f"?Action=Unsubscribe"
                        f"&SubscriptionArn={sub.subscription_arn}"
                    ),
                    "MessageAttributes": sns_message_attributes,
                },
            }
        ],
    }

    invoke_lambda_async(endpoint, event, region, account_id)


def _deliver_to_http(
    sub: SnsSubscription,
    message: str,
    subject: str | None,
    message_attributes: dict,
    message_id: str,
    topic_arn: str,
    region: str,
) -> None:
    """Deliver an SNS message to an HTTP/HTTPS endpoint."""
    import urllib.error
    import urllib.request

    timestamp = _iso_timestamp()
    payload = json.dumps(
        {
            "Type": "Notification",
            "MessageId": message_id,
            "TopicArn": topic_arn,
            "Subject": subject or "",
            "Message": message,
            "Timestamp": timestamp,
            "SignatureVersion": "1",
            "Signature": "EXAMPLE",
            "SigningCertURL": (f"https://sns.{region}.amazonaws.com/SimpleNotificationService.pem"),
            "UnsubscribeURL": (
                f"https://sns.{region}.amazonaws.com/"
                f"?Action=Unsubscribe"
                f"&SubscriptionArn={sub.subscription_arn}"
            ),
            "MessageAttributes": {
                name: {
                    "Type": val.get("DataType", "String"),
                    "Value": val.get("StringValue", val.get("Value", "")),
                }
                for name, val in message_attributes.items()
            },
        }
    )

    try:
        req = urllib.request.Request(
            sub.endpoint,
            data=payload.encode("utf-8"),
            headers={
                "Content-Type": "text/plain; charset=UTF-8",
                "x-amz-sns-message-type": "Notification",
                "x-amz-sns-message-id": message_id,
                "x-amz-sns-topic-arn": topic_arn,
                "x-amz-sns-subscription-arn": sub.subscription_arn,
            },
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
        logger.debug("SNS: Delivered to HTTP endpoint %s", sub.endpoint)
    except Exception as e:
        logger.warning(
            "SNS: Failed to deliver to HTTP endpoint %s: %s",
            sub.endpoint,
            e,
        )


def _deliver_to_firehose(
    sub: SnsSubscription,
    message: str,
    subject: str | None,
    message_attributes: dict,
    message_id: str,
    topic_arn: str,
    region: str,
) -> None:
    """Deliver an SNS message to a Kinesis Firehose delivery stream."""
    try:
        from robotocore.services.firehose.provider import (
            _get_store as get_firehose_store,
        )

        firehose_store = get_firehose_store(region)
        # Endpoint is the delivery stream ARN
        stream_name = sub.endpoint.rsplit("/", 1)[-1]
        stream = firehose_store.get_stream(stream_name)
        if stream:
            record_data = json.dumps(
                {
                    "Type": "Notification",
                    "MessageId": message_id,
                    "TopicArn": topic_arn,
                    "Subject": subject or "",
                    "Message": message,
                }
            )
            stream.put_record(record_data.encode())
            logger.debug("SNS: Delivered to Firehose stream %s", stream_name)
    except ImportError:
        logger.warning(
            "SNS: Firehose provider not available for delivery to %s",
            sub.endpoint,
        )
    except Exception as e:
        logger.warning("SNS: Failed to deliver to Firehose %s: %s", sub.endpoint, e)


def _send_subscription_confirmation(sub: SnsSubscription, topic_arn: str, region: str) -> None:
    """Send a SubscriptionConfirmation message to HTTP/HTTPS endpoints."""
    import urllib.error
    import urllib.request

    token = _new_id()
    payload = json.dumps(
        {
            "Type": "SubscriptionConfirmation",
            "MessageId": _new_id(),
            "TopicArn": topic_arn,
            "Token": token,
            "Message": f"You have chosen to subscribe to the topic {topic_arn}.",
            "SubscribeURL": (
                f"https://sns.{region}.amazonaws.com/"
                f"?Action=ConfirmSubscription"
                f"&TopicArn={topic_arn}"
                f"&Token={token}"
            ),
            "Timestamp": _iso_timestamp(),
            "SignatureVersion": "1",
            "Signature": "EXAMPLE",
            "SigningCertURL": (f"https://sns.{region}.amazonaws.com/SimpleNotificationService.pem"),
        }
    )

    try:
        req = urllib.request.Request(
            sub.endpoint,
            data=payload.encode("utf-8"),
            headers={
                "Content-Type": "text/plain; charset=UTF-8",
                "x-amz-sns-message-type": "SubscriptionConfirmation",
                "x-amz-sns-topic-arn": topic_arn,
            },
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        logger.debug(
            "SNS: Failed to send subscription confirmation to %s: %s",
            sub.endpoint,
            e,
        )


def _iso_timestamp() -> str:
    from datetime import datetime

    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# Need time import for sequence numbers in FIFO publish
import time  # noqa: E402


def _sub_to_dict(sub: SnsSubscription) -> dict:
    return {
        "SubscriptionArn": sub.subscription_arn,
        "TopicArn": sub.topic_arn,
        "Protocol": sub.protocol,
        "Endpoint": sub.endpoint,
        "Owner": sub.owner,
    }


# --- Response helpers ---


def _json_response(data: dict) -> Response:
    return Response(
        content=json.dumps(data),
        status_code=200,
        media_type="application/x-amz-json-1.0",
    )


def _xml_response(action: str, data: dict) -> Response:
    # Fields that are maps and need entry/key/value serialization
    map_fields = {"Attributes", "Tags"}

    def dict_to_xml(d: dict) -> str:
        parts = []
        for k, v in d.items():
            if isinstance(v, list):
                parts.append(f"<{k}>")
                for item in v:
                    if isinstance(item, dict):
                        parts.append(f"<member>{dict_to_xml(item)}</member>")
                    else:
                        parts.append(f"<member>{item}</member>")
                parts.append(f"</{k}>")
            elif isinstance(v, dict) and k in map_fields:
                parts.append(f"<{k}>")
                for mk, mv in v.items():
                    parts.append(f"<entry><key>{mk}</key><value>{mv}</value></entry>")
                parts.append(f"</{k}>")
            elif isinstance(v, dict):
                parts.append(f"<{k}>{dict_to_xml(v)}</{k}>")
            else:
                parts.append(f"<{k}>{v}</{k}>")
        return "".join(parts)

    result_name = action.replace("Response", "Result")
    body_xml = dict_to_xml(data)
    xml = (
        f'<?xml version="1.0"?>'
        f'<{action} xmlns="http://sns.amazonaws.com/doc/2010-03-31/">'
        f"<{result_name}>{body_xml}</{result_name}>"
        f"<ResponseMetadata><RequestId>{_new_id()}</RequestId>"
        f"</ResponseMetadata>"
        f"</{action}>"
    )
    return Response(content=xml, status_code=200, media_type="text/xml")


def _error(code: str, message: str, status: int, use_json: bool) -> Response:
    if use_json:
        body = json.dumps({"__type": code, "message": message})
        return Response(
            content=body,
            status_code=status,
            media_type="application/x-amz-json-1.0",
        )
    xml = (
        f'<?xml version="1.0"?>'
        f'<ErrorResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">'
        f"<Error><Type>Sender</Type><Code>{code}</Code>"
        f"<Message>{message}</Message></Error>"
        f"<RequestId>{_new_id()}</RequestId>"
        f"</ErrorResponse>"
    )
    return Response(content=xml, status_code=status, media_type="text/xml")


_ACTION_MAP: dict[str, Callable] = {
    "CreateTopic": _create_topic,
    "DeleteTopic": _delete_topic,
    "ListTopics": _list_topics,
    "GetTopicAttributes": _get_topic_attributes,
    "SetTopicAttributes": _set_topic_attributes,
    "Subscribe": _subscribe,
    "ConfirmSubscription": _confirm_subscription,
    "Unsubscribe": _unsubscribe,
    "ListSubscriptions": _list_subscriptions,
    "ListSubscriptionsByTopic": _list_subscriptions_by_topic,
    "GetSubscriptionAttributes": _get_subscription_attributes,
    "SetSubscriptionAttributes": _set_subscription_attributes,
    "Publish": _publish,
    "PublishBatch": _publish_batch,
    "TagResource": _tag_resource,
    "UntagResource": _untag_resource,
    "ListTagsForResource": _list_tags_for_resource,
    "CreatePlatformApplication": _create_platform_application,
    "DeletePlatformApplication": _delete_platform_application,
    "ListPlatformApplications": _list_platform_applications,
    "GetPlatformApplicationAttributes": _get_platform_application_attributes,
    "SetPlatformApplicationAttributes": _set_platform_application_attributes,
}
