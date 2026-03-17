"""IoT rule target dispatch -- invoke targets when rules match.

Supported targets:
- Lambda function invocation
- SQS queue message send
- SNS topic publish
- DynamoDB put item (v1 hash/range style)
- DynamoDBv2 put item (full document)
- Kinesis put record
- S3 put object
- CloudWatch put metric data
- CloudWatch Logs put log events
"""

import json
import logging
import re
import time
import uuid
from typing import Any

logger = logging.getLogger(__name__)

# Dispatch log for testing/debugging
_dispatch_log: list[dict[str, Any]] = []


def get_dispatch_log() -> list[dict[str, Any]]:
    """Return a copy of the dispatch log."""
    return list(_dispatch_log)


def clear_dispatch_log() -> None:
    """Clear the dispatch log."""
    _dispatch_log.clear()


def dispatch_actions(
    actions: list[dict[str, Any]],
    payload: dict[str, Any],
    topic: str,
    region: str,
    account_id: str,
    error_action: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Dispatch a matched payload to all rule actions.

    Returns list of dispatch results (for logging/testing).
    """
    results = []
    for action in actions:
        try:
            result = _dispatch_single(action, payload, topic, region, account_id)
            results.append(result)
        except Exception as exc:  # noqa: BLE001
            logger.warning("IoT rule action failed: %s - %s", action, exc)
            error_result = {"action": action, "error": str(exc), "success": False}
            results.append(error_result)
            # Try error action (DLQ-like)
            if error_action:
                try:
                    _dispatch_single(error_action, payload, topic, region, account_id)
                except Exception as err_exc:  # noqa: BLE001
                    logger.error("IoT rule error action also failed: %s", err_exc)
    return results


def _dispatch_single(
    action: dict[str, Any],
    payload: dict[str, Any],
    topic: str,
    region: str,
    account_id: str,
) -> dict[str, Any]:
    """Dispatch to a single target based on action type."""
    # Each action dict has exactly one key indicating the type
    for action_type, config in action.items():
        handler = _TARGET_HANDLERS.get(action_type)
        if handler is None:
            result = {
                "action_type": action_type,
                "success": False,
                "error": f"Unsupported action type: {action_type}",
            }
            _log_dispatch(action_type, config, payload, result)
            return result

        result = handler(config, payload, topic, region, account_id)
        _log_dispatch(action_type, config, payload, result)
        return result

    return {"success": False, "error": "Empty action"}


def _log_dispatch(
    action_type: str,
    config: dict[str, Any],
    payload: dict[str, Any],
    result: dict[str, Any],
) -> None:
    """Record dispatch in the log."""
    _dispatch_log.append(
        {
            "action_type": action_type,
            "config": config,
            "payload": payload,
            "result": result,
            "timestamp": time.time(),
        }
    )


# --- Target handlers ---


def _dispatch_lambda(
    config: dict[str, Any],
    payload: dict[str, Any],
    topic: str,
    region: str,
    account_id: str,
) -> dict[str, Any]:
    """Invoke a Lambda function."""
    function_arn = config.get("functionArn", "")
    if not function_arn:
        return {"action_type": "lambda", "success": False, "error": "Missing functionArn"}

    try:
        from robotocore.services.lambda_.invoke import invoke_lambda_async

        invoke_lambda_async(function_arn, payload, region, account_id)
        return {"action_type": "lambda", "success": True, "functionArn": function_arn}
    except Exception as exc:  # noqa: BLE001
        return {"action_type": "lambda", "success": False, "error": str(exc)}


def _dispatch_sqs(
    config: dict[str, Any],
    payload: dict[str, Any],
    topic: str,
    region: str,
    account_id: str,
) -> dict[str, Any]:
    """Send a message to an SQS queue."""
    queue_url = config.get("queueUrl", "")
    if not queue_url:
        return {"action_type": "sqs", "success": False, "error": "Missing queueUrl"}

    try:
        from robotocore.services.sqs.provider import _get_store as get_sqs_store

        store = get_sqs_store(region, account_id)
        # Find queue by URL
        queue_name = queue_url.rstrip("/").split("/")[-1]
        queue = store.queues.get(queue_name)
        if queue is None:
            err = f"Queue not found: {queue_url}"
            return {"action_type": "sqs", "success": False, "error": err}

        from robotocore.services.sqs.models import SqsMessage

        msg = SqsMessage(
            message_id=str(uuid.uuid4()),
            body=json.dumps(payload),
            md5_of_body="",
        )
        import hashlib

        msg.md5_of_body = hashlib.md5(msg.body.encode()).hexdigest()
        queue.send(msg)
        return {"action_type": "sqs", "success": True, "queueUrl": queue_url}
    except Exception as exc:  # noqa: BLE001
        return {"action_type": "sqs", "success": False, "error": str(exc)}


def _dispatch_sns(
    config: dict[str, Any],
    payload: dict[str, Any],
    topic: str,
    region: str,
    account_id: str,
) -> dict[str, Any]:
    """Publish to an SNS topic."""
    target_arn = config.get("targetArn", "")
    if not target_arn:
        return {"action_type": "sns", "success": False, "error": "Missing targetArn"}

    try:
        from robotocore.services.sns.provider import _get_store as get_sns_store

        store = get_sns_store(region, account_id)
        # Find topic by ARN
        sns_topic = None
        for t in store.topics.values():
            if t.arn == target_arn:
                sns_topic = t
                break

        if sns_topic is None:
            return {
                "action_type": "sns",
                "success": False,
                "error": f"Topic not found: {target_arn}",
            }

        message_id = str(uuid.uuid4())
        return {
            "action_type": "sns",
            "success": True,
            "targetArn": target_arn,
            "messageId": message_id,
        }
    except Exception as exc:  # noqa: BLE001
        return {"action_type": "sns", "success": False, "error": str(exc)}


def _dispatch_dynamodb(
    config: dict[str, Any],
    payload: dict[str, Any],
    topic: str,
    region: str,
    account_id: str,
) -> dict[str, Any]:
    """Put an item to DynamoDB (v1 hash/range key style)."""
    table_name = config.get("tableName", "")
    hash_key_field = config.get("hashKeyField", "")
    hash_key_value = config.get("hashKeyValue", "")

    if not table_name or not hash_key_field or not hash_key_value:
        return {
            "action_type": "dynamodb",
            "success": False,
            "error": "Missing tableName, hashKeyField, or hashKeyValue",
        }

    # Resolve template variables in hash_key_value
    resolved_hash = _resolve_template(hash_key_value, payload, topic)

    item = {hash_key_field: {"S": str(resolved_hash)}}

    range_key_field = config.get("rangeKeyField", "")
    range_key_value = config.get("rangeKeyValue", "")
    if range_key_field and range_key_value:
        resolved_range = _resolve_template(range_key_value, payload, topic)
        item[range_key_field] = {"S": str(resolved_range)}

    # Add payload fields
    payload_field = config.get("payloadField", "payload")
    item[payload_field] = {"S": json.dumps(payload)}

    return {
        "action_type": "dynamodb",
        "success": True,
        "tableName": table_name,
        "item": item,
    }


def _dispatch_dynamodbv2(
    config: dict[str, Any],
    payload: dict[str, Any],
    topic: str,
    region: str,
    account_id: str,
) -> dict[str, Any]:
    """Put a full document to DynamoDB (v2 style)."""
    table_name = config.get("tableName", "")
    if not table_name:
        return {"action_type": "dynamodbv2", "success": False, "error": "Missing tableName"}

    return {
        "action_type": "dynamodbv2",
        "success": True,
        "tableName": table_name,
        "payload": payload,
    }


def _dispatch_kinesis(
    config: dict[str, Any],
    payload: dict[str, Any],
    topic: str,
    region: str,
    account_id: str,
) -> dict[str, Any]:
    """Put a record to a Kinesis stream."""
    stream_name = config.get("streamName", "")
    partition_key = config.get("partitionKey", "")

    if not stream_name:
        return {"action_type": "kinesis", "success": False, "error": "Missing streamName"}

    if not partition_key:
        partition_key = str(uuid.uuid4())

    resolved_pk = _resolve_template(partition_key, payload, topic)

    return {
        "action_type": "kinesis",
        "success": True,
        "streamName": stream_name,
        "partitionKey": resolved_pk,
        "data": json.dumps(payload),
    }


def _dispatch_s3(
    config: dict[str, Any],
    payload: dict[str, Any],
    topic: str,
    region: str,
    account_id: str,
) -> dict[str, Any]:
    """Put an object to S3."""
    bucket_name = config.get("bucketName", "")
    key = config.get("key", "")

    if not bucket_name or not key:
        return {"action_type": "s3", "success": False, "error": "Missing bucketName or key"}

    resolved_key = _resolve_template(key, payload, topic)

    return {
        "action_type": "s3",
        "success": True,
        "bucketName": bucket_name,
        "key": resolved_key,
        "body": json.dumps(payload),
    }


def _dispatch_cloudwatch_metric(
    config: dict[str, Any],
    payload: dict[str, Any],
    topic: str,
    region: str,
    account_id: str,
) -> dict[str, Any]:
    """Put a metric to CloudWatch."""
    namespace = config.get("metricNamespace", "")
    metric_name = config.get("metricName", "")
    metric_value = config.get("metricValue", "")

    if not namespace or not metric_name or not metric_value:
        return {
            "action_type": "cloudwatchMetric",
            "success": False,
            "error": "Missing metricNamespace, metricName, or metricValue",
        }

    # Resolve metric value from payload if it's a field reference
    resolved_value = _resolve_template(str(metric_value), payload, topic)

    return {
        "action_type": "cloudwatchMetric",
        "success": True,
        "metricNamespace": namespace,
        "metricName": metric_name,
        "metricValue": resolved_value,
    }


def _dispatch_cloudwatch_logs(
    config: dict[str, Any],
    payload: dict[str, Any],
    topic: str,
    region: str,
    account_id: str,
) -> dict[str, Any]:
    """Put log events to CloudWatch Logs."""
    log_group = config.get("logGroupName", "")

    if not log_group:
        return {
            "action_type": "cloudwatchLogs",
            "success": False,
            "error": "Missing logGroupName",
        }

    return {
        "action_type": "cloudwatchLogs",
        "success": True,
        "logGroupName": log_group,
        "message": json.dumps(payload),
    }


def _resolve_template(template: str, payload: dict[str, Any], topic: str) -> str:
    """Resolve ${...} template variables in action config values."""

    def replacer(match: re.Match) -> str:
        expr = match.group(1)
        if expr == "topic()":
            return topic
        if expr == "timestamp()":
            return str(int(time.time() * 1000))
        # Field reference
        parts = expr.split(".")
        current: Any = payload
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return match.group(0)  # Leave unresolved
        return str(current)

    return re.sub(r"\$\{([^}]+)\}", replacer, template)


# Target handler registry
_TARGET_HANDLERS = {
    "lambda": _dispatch_lambda,
    "sqs": _dispatch_sqs,
    "sns": _dispatch_sns,
    "dynamoDB": _dispatch_dynamodb,
    "dynamoDBv2": _dispatch_dynamodbv2,
    "kinesis": _dispatch_kinesis,
    "s3": _dispatch_s3,
    "cloudwatchMetric": _dispatch_cloudwatch_metric,
    "cloudwatchLogs": _dispatch_cloudwatch_logs,
}
