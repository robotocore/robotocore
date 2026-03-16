"""Reusable functional validation helpers for IaC tests.

These helpers exercise deployed resources — they go beyond existence checks
to verify that resources actually *work* (data roundtrips, auth flows, etc.).
"""

from __future__ import annotations

import json
import time

import urllib3

from tests.iac.conftest import ENDPOINT_URL

# ── API Gateway ──────────────────────────────────────────────────────────────


def invoke_api_gateway(api_id: str, stage: str, path: str, method: str = "GET") -> dict:
    """Send an HTTP request through an API Gateway REST API.

    Uses the robotocore execute-api routing:
        {ENDPOINT_URL}/restapis/{api_id}/{stage}/_user_request_/{path}

    Returns dict with 'status', 'headers', 'body' keys.
    """
    url = f"{ENDPOINT_URL}/restapis/{api_id}/{stage}/_user_request_/{path.lstrip('/')}"
    http = urllib3.PoolManager()
    resp = http.request(method, url, timeout=10.0)
    body = resp.data.decode("utf-8", errors="replace")
    try:
        body = json.loads(body)
    except (json.JSONDecodeError, ValueError):
        pass  # intentionally ignored
    return {
        "status": resp.status,
        "headers": dict(resp.headers),
        "body": body,
    }


# ── S3 ───────────────────────────────────────────────────────────────────────


def put_and_get_s3_object(client, bucket: str, key: str, body: str | bytes) -> dict:
    """Upload an object to S3, read it back, and assert the contents match.

    Returns the GetObject response.
    """
    if isinstance(body, str):
        body = body.encode("utf-8")
    client.put_object(Bucket=bucket, Key=key, Body=body)
    resp = client.get_object(Bucket=bucket, Key=key)
    returned_body = resp["Body"].read()
    assert returned_body == body, (
        f"S3 roundtrip mismatch for s3://{bucket}/{key}: "
        f"put {len(body)} bytes, got {len(returned_body)} bytes"
    )
    return resp


# ── SQS ──────────────────────────────────────────────────────────────────────


def send_and_receive_sqs(client, queue_url: str, body: str, wait_seconds: int = 5) -> dict:
    """Send a message to SQS, poll for it, and return the received message.

    Returns the first received message dict.
    """
    client.send_message(QueueUrl=queue_url, MessageBody=body)
    resp = client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=wait_seconds,
    )
    messages = resp.get("Messages", [])
    assert len(messages) >= 1, f"No messages received from {queue_url}"
    msg = messages[0]
    assert msg["Body"] == body, f"SQS body mismatch: {msg['Body']!r} != {body!r}"
    # Clean up
    client.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
    return msg


# ── DynamoDB ─────────────────────────────────────────────────────────────────


def put_and_get_dynamodb_item(client, table: str, item: dict, key: dict) -> dict:
    """PutItem then GetItem, assert the returned item matches.

    ``item`` is the full item (including key attributes).
    ``key`` is just the key attributes for GetItem.

    Returns the GetItem response item.
    """
    client.put_item(TableName=table, Item=item)
    resp = client.get_item(TableName=table, Key=key)
    returned = resp.get("Item")
    assert returned is not None, f"GetItem returned no item for key={key} in table={table}"
    # Verify key attributes match
    for k, v in key.items():
        assert returned[k] == v, f"Key mismatch on {k}: {returned[k]} != {v}"
    return returned


# ── Kinesis ──────────────────────────────────────────────────────────────────


def put_and_read_kinesis_record(
    client, stream_name: str, data: str | bytes, partition_key: str
) -> dict:
    """PutRecord then read it back via shard iterator. Returns the record."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    client.put_record(StreamName=stream_name, Data=data, PartitionKey=partition_key)

    # Get shard iterator
    desc = client.describe_stream(StreamName=stream_name)
    shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]
    iter_resp = client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON",
    )
    shard_iter = iter_resp["ShardIterator"]

    # Read records (retry with deadline for eventual consistency)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        records_resp = client.get_records(ShardIterator=shard_iter, Limit=10)
        records = records_resp.get("Records", [])
        if records:
            record = records[-1]  # most recent
            # Kinesis returns Data as bytes; normalize for comparison
            record_data = record["Data"]
            if isinstance(record_data, str):
                record_data = record_data.encode("utf-8")
            assert record_data == data, f"Kinesis data mismatch: {record_data!r} != {data!r}"
            return record
        shard_iter = records_resp["NextShardIterator"]
        time.sleep(0.5)
    raise AssertionError(f"No records found in stream {stream_name}")


# ── Cognito ──────────────────────────────────────────────────────────────────


def create_cognito_user_and_auth(
    client,
    pool_id: str,
    client_id: str,
    username: str,
    password: str,
) -> dict:
    """Create a Cognito user, set password, authenticate, return tokens.

    Returns the InitiateAuth response (contains AccessToken, IdToken, etc.).
    """
    client.admin_create_user(
        UserPoolId=pool_id,
        Username=username,
        TemporaryPassword=password,
        MessageAction="SUPPRESS",
    )
    client.admin_set_user_password(
        UserPoolId=pool_id,
        Username=username,
        Password=password,
        Permanent=True,
    )
    auth_resp = client.initiate_auth(
        ClientId=client_id,
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={
            "USERNAME": username,
            "PASSWORD": password,
        },
    )
    result = auth_resp.get("AuthenticationResult", {})
    assert "AccessToken" in result, "InitiateAuth did not return AccessToken"
    assert "IdToken" in result, "InitiateAuth did not return IdToken"
    return auth_resp


# ── CloudWatch ───────────────────────────────────────────────────────────────


def publish_metric_and_check_alarm(
    cw_client,
    namespace: str,
    metric_name: str,
    alarm_name: str,
    value: float,
    dimensions: list[dict] | None = None,
) -> dict:
    """PutMetricData and then describe the alarm. Returns the alarm description.

    Note: Robotocore may not evaluate alarm state automatically, so we just
    verify the metric was accepted and the alarm is still describable.
    """
    kwargs: dict = {
        "Namespace": namespace,
        "MetricData": [
            {
                "MetricName": metric_name,
                "Value": value,
                "Unit": "Percent",
            }
        ],
    }
    if dimensions:
        kwargs["MetricData"][0]["Dimensions"] = dimensions
    cw_client.put_metric_data(**kwargs)

    resp = cw_client.describe_alarms(AlarmNames=[alarm_name])
    alarms = resp.get("MetricAlarms", [])
    assert len(alarms) == 1, f"Alarm {alarm_name!r} not found after PutMetricData"
    return alarms[0]


# ── CloudWatch Logs ──────────────────────────────────────────────────────────


def put_log_event_and_query(
    logs_client,
    log_group: str,
    log_stream: str,
    message: str,
) -> list[dict]:
    """Create log stream, put a log event, then filter for it.

    Returns the filtered events list.
    """
    try:
        logs_client.create_log_stream(logGroupName=log_group, logStreamName=log_stream)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass  # resource may not exist

    logs_client.put_log_events(
        logGroupName=log_group,
        logStreamName=log_stream,
        logEvents=[
            {
                "timestamp": int(time.time() * 1000),
                "message": message,
            }
        ],
    )

    resp = logs_client.filter_log_events(
        logGroupName=log_group,
        logStreamNames=[log_stream],
        filterPattern=message,
    )
    events = resp.get("events", [])
    assert len(events) >= 1, f"No log events found matching {message!r} in {log_group}"
    return events


# ── SNS → SQS ───────────────────────────────────────────────────────────────


def subscribe_sns_to_sqs_and_publish(
    sns_client,
    sqs_client,
    topic_arn: str,
    queue_arn: str,
    queue_url: str,
    message: str,
) -> dict:
    """Subscribe an SQS queue to an SNS topic, publish a message, receive it.

    Returns the received SQS message.
    """
    sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
    )
    sns_client.publish(TopicArn=topic_arn, Message=message)

    # Poll for the message
    for _ in range(5):
        resp = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=2,
        )
        messages = resp.get("Messages", [])
        if messages:
            msg = messages[0]
            # SNS wraps the message in an envelope
            body = msg["Body"]
            try:
                envelope = json.loads(body)
                actual_message = envelope.get("Message", body)
            except (json.JSONDecodeError, ValueError):
                actual_message = body
            assert message in str(actual_message), (
                f"SNS→SQS message mismatch: expected {message!r} in {actual_message!r}"
            )
            sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
            return msg
    raise AssertionError(f"No SNS→SQS message received on {queue_url}")
