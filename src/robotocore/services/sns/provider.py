"""Native SNS provider with cross-service SQS, Lambda, and HTTP/HTTPS delivery."""

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
        return _error("InvalidAction", f"Unknown action: {action}", 400, use_json)

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
    # Query protocol attributes
    for key, value in params.items():
        if key.startswith("Attributes.entry.") and key.endswith(".key"):
            idx = key.split(".")[2]
            attributes[value] = params.get(f"Attributes.entry.{idx}.value", "")
    topic = store.create_topic(name, region, account_id, attributes)
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
    attrs = {
        "TopicArn": topic.arn,
        "Owner": topic.account_id,
        "DisplayName": topic.attributes.get("DisplayName", topic.name),
        "SubscriptionsConfirmed": str(len(topic.subscriptions)),
        "SubscriptionsPending": "0",
        "SubscriptionsDeleted": "0",
    }
    if topic.is_fifo:
        attrs["FifoTopic"] = "true"
        attrs["ContentBasedDeduplication"] = topic.attributes.get(
            "ContentBasedDeduplication", "false"
        )
    attrs.update(topic.attributes)
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
    sub = store.subscribe(topic_arn, protocol, endpoint, attributes)
    if not sub:
        raise SnsError("NotFound", f"Topic {topic_arn} not found", 404)
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
    }
    if sub.filter_policy:
        attrs["FilterPolicy"] = json.dumps(sub.filter_policy)
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
    # Parse query protocol message attributes
    i = 1
    while f"MessageAttributes.entry.{i}.Name" in params:
        name = params[f"MessageAttributes.entry.{i}.Name"]
        data_type = params.get(f"MessageAttributes.entry.{i}.Value.DataType", "String")
        string_value = params.get(f"MessageAttributes.entry.{i}.Value.StringValue", "")
        message_attributes[name] = {"DataType": data_type, "StringValue": string_value}
        i += 1

    message_id = _new_id()

    # Deliver to subscribers
    for sub in topic.subscriptions:
        if not sub.confirmed:
            continue
        if not sub.matches_filter(message_attributes):
            continue
        _deliver_to_subscriber(
            sub, message, subject, message_attributes, message_id, topic_arn, region
        )

    result = {"MessageId": message_id}
    return result


def _publish_batch(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    topic_arn = params.get("TopicArn", "")
    topic = store.get_topic(topic_arn)
    if not topic:
        raise SnsError("NotFound", f"Topic {topic_arn} not found", 404)

    entries = params.get("PublishBatchRequestEntries", [])
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
                sub, message, subject, message_attributes, msg_id, topic_arn, region
            )

        successful.append({"Id": entry.get("Id", ""), "MessageId": msg_id})

    return {"Successful": successful, "Failed": []}


def _tag_resource(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    arn = params.get("ResourceArn", "")
    topic = store.get_topic(arn)
    if topic:
        tags = params.get("Tags", [])
        for tag in tags:
            topic.tags[tag.get("Key", "")] = tag.get("Value", "")
    return {}


def _untag_resource(
    store: SnsStore, params: dict, region: str, account_id: str, request: Request
) -> dict:
    arn = params.get("ResourceArn", "")
    topic = store.get_topic(arn)
    if topic:
        keys = params.get("TagKeys", [])
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
        _deliver_to_sqs(sub, message, subject, message_attributes, message_id, topic_arn, region)
    elif sub.protocol == "lambda":
        _deliver_to_lambda(sub, message, subject, message_attributes, message_id, topic_arn, region)
    elif sub.protocol in ("http", "https"):
        _deliver_to_http(sub, message, subject, message_attributes, message_id, topic_arn, region)
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
    sqs_store = get_sqs_store(region)
    # Endpoint is queue ARN — extract queue name
    queue_name = sub.endpoint.rsplit(":", 1)[-1]
    queue = sqs_store.get_queue(queue_name)
    if not queue:
        return

    if sub.raw_message_delivery:
        body = message
    else:
        body = json.dumps(
            {
                "Type": "Notification",
                "MessageId": message_id,
                "TopicArn": topic_arn,
                "Subject": subject or "",
                "Message": message,
                "Timestamp": _iso_timestamp(),
                "SignatureVersion": "1",
                "Signature": "EXAMPLE",
                "SigningCertURL": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService.pem",
                "UnsubscribeURL": f"https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn={sub.subscription_arn}",
            }
        )

    sqs_msg = SqsMessage(
        message_id=str(uuid.uuid4()),
        body=body,
        md5_of_body=_md5(body),
    )
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
    """Deliver an SNS message to a Lambda function subscriber.

    Uses async dispatch via thread pool to avoid deadlocking the event loop
    when the Lambda function calls back to the server.
    """
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
                    "SigningCertUrl": f"https://sns.{region}.amazonaws.com/SimpleNotificationService.pem",
                    "UnsubscribeUrl": f"https://sns.{region}.amazonaws.com/?Action=Unsubscribe&SubscriptionArn={sub.subscription_arn}",
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
            "SigningCertURL": f"https://sns.{region}.amazonaws.com/SimpleNotificationService.pem",
            "UnsubscribeURL": f"https://sns.{region}.amazonaws.com/?Action=Unsubscribe&SubscriptionArn={sub.subscription_arn}",
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
        logger.warning("SNS: Failed to deliver to HTTP endpoint %s: %s", sub.endpoint, e)


def _iso_timestamp() -> str:
    from datetime import datetime

    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


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
        f"<ResponseMetadata><RequestId>{_new_id()}</RequestId></ResponseMetadata>"
        f"</{action}>"
    )
    return Response(content=xml, status_code=200, media_type="text/xml")


def _error(code: str, message: str, status: int, use_json: bool) -> Response:
    if use_json:
        body = json.dumps({"__type": code, "message": message})
        return Response(content=body, status_code=status, media_type="application/x-amz-json-1.0")
    xml = (
        f'<?xml version="1.0"?>'
        f'<ErrorResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">'
        f"<Error><Type>Sender</Type><Code>{code}</Code><Message>{message}</Message></Error>"
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
}
