"""Lambda invoke destinations — dispatches results to SQS, SNS, Lambda, EventBridge.

After an async Lambda invocation completes (or fails), the result is sent to the
configured OnSuccess or OnFailure destination.
"""

import json
import logging
import time
import uuid

logger = logging.getLogger(__name__)


def dispatch_destination(
    destination_arn: str,
    function_arn: str,
    payload: dict,
    is_success: bool,
    result: dict | str | None,
    error: str | None,
    region: str,
    account_id: str,
) -> None:
    """Send invocation result/failure to the configured destination.

    Args:
        destination_arn: ARN of the destination (SQS, SNS, Lambda, EventBridge)
        function_arn: ARN of the source Lambda function
        payload: Original invocation payload
        is_success: Whether the invocation succeeded
        result: Invocation result (on success) or error details (on failure)
        error: Error type string (on failure)
        region: AWS region
        account_id: AWS account ID
    """
    record = _build_destination_record(
        function_arn=function_arn,
        payload=payload,
        is_success=is_success,
        result=result,
        error=error,
    )

    try:
        if ":sqs:" in destination_arn:
            _send_to_sqs(destination_arn, record, region, account_id)
        elif ":sns:" in destination_arn:
            _send_to_sns(destination_arn, record, region, account_id)
        elif ":lambda:" in destination_arn:
            _send_to_lambda(destination_arn, record, region, account_id)
        elif ":events:" in destination_arn or ":event-bus" in destination_arn:
            _send_to_eventbridge(destination_arn, record, region, account_id)
        else:
            logger.warning("Unknown destination type for ARN: %s", destination_arn)
    except Exception:
        logger.exception("Failed to dispatch to destination %s", destination_arn)


def _build_destination_record(
    function_arn: str,
    payload: dict,
    is_success: bool,
    result: dict | str | None,
    error: str | None,
) -> dict:
    """Build the destination record matching AWS format."""
    record: dict = {
        "version": "1.0",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "requestContext": {
            "requestId": str(uuid.uuid4()),
            "functionArn": function_arn,
            "condition": "Success" if is_success else "RetriesExhausted",
            "approximateInvokeCount": 1,
        },
        "requestPayload": payload,
    }

    if is_success:
        record["responseContext"] = {"statusCode": 200, "executedVersion": "$LATEST"}
        record["responsePayload"] = result
    else:
        record["responseContext"] = {
            "statusCode": 200,
            "executedVersion": "$LATEST",
            "functionError": error or "Unhandled",
        }
        record["responsePayload"] = result if result else {"errorMessage": error or "Unknown"}

    return record


def _send_to_sqs(
    queue_arn: str, record: dict, region: str, account_id: str = "123456789012"
) -> None:
    """Send destination record to SQS queue."""
    import hashlib

    from robotocore.services.sqs.models import SqsMessage
    from robotocore.services.sqs.provider import _get_store

    queue_name = queue_arn.rsplit(":", 1)[-1]
    store = _get_store(region, account_id)
    queue = store.get_queue(queue_name)
    if queue:
        body = json.dumps(record)
        msg = SqsMessage(
            message_id=str(uuid.uuid4()),
            body=body,
            md5_of_body=hashlib.md5(body.encode()).hexdigest(),
        )
        queue.put(msg)
    else:
        logger.warning("Destination SQS queue not found: %s", queue_name)


def _send_to_sns(
    topic_arn: str, record: dict, region: str, account_id: str = "123456789012"
) -> None:
    """Send destination record to SNS topic."""
    from robotocore.services.sns.provider import (
        _deliver_to_subscriber,
        _get_store,
        _new_id,
    )

    store = _get_store(region, account_id)
    topic = store.get_topic(topic_arn)
    if topic:
        message = json.dumps(record)
        message_id = _new_id()
        for sub in topic.subscriptions:
            if sub.confirmed:
                _deliver_to_subscriber(
                    sub,
                    message,
                    "Lambda Invocation Result",
                    {},
                    message_id,
                    topic_arn,
                    region,
                )
    else:
        logger.warning("Destination SNS topic not found: %s", topic_arn)


def _send_to_lambda(function_arn: str, record: dict, region: str, account_id: str) -> None:
    """Send destination record to another Lambda function."""
    from robotocore.services.lambda_.invoke import invoke_lambda_async

    invoke_lambda_async(
        function_arn=function_arn,
        payload=record,
        region=region,
        account_id=account_id,
    )


def _send_to_eventbridge(event_bus_arn: str, record: dict, region: str, account_id: str) -> None:
    """Send destination record to EventBridge."""
    try:
        from robotocore.services.eventbridge.provider import get_store

        store = get_store(region)
        event = {
            "Source": "lambda",
            "DetailType": "Lambda Function Invocation Result - "
            + record["requestContext"]["condition"],
            "Detail": json.dumps(record),
            "EventBusName": "default",
        }
        store.put_events([event])
    except Exception:
        logger.exception("Failed to send to EventBridge: %s", event_bus_arn)
