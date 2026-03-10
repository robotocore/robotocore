"""
Notification Dispatch System Tests

Simulates a multi-channel notification platform that dispatches messages
across email, SMS, push, and webhook channels using SNS topics, SQS queues,
S3 templates, DynamoDB delivery tracking, and CloudWatch metrics/logs.
"""

import json
import time
from datetime import UTC, datetime

import pytest


def _receive_messages(sqs, queue_url, expected=1, timeout=10):
    """Poll queue until expected messages received or timeout."""
    messages = []
    deadline = time.time() + timeout
    while len(messages) < expected and time.time() < deadline:
        resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        messages.extend(resp.get("Messages", []))
    return messages


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def email_topic(sns, unique_name):
    resp = sns.create_topic(Name=f"notif-email-{unique_name}")
    arn = resp["TopicArn"]
    yield arn
    sns.delete_topic(TopicArn=arn)


@pytest.fixture
def sms_topic(sns, unique_name):
    resp = sns.create_topic(Name=f"notif-sms-{unique_name}")
    arn = resp["TopicArn"]
    yield arn
    sns.delete_topic(TopicArn=arn)


@pytest.fixture
def email_queue(sqs, sns, email_topic, unique_name):
    queue_name = f"email-delivery-{unique_name}"
    resp = sqs.create_queue(QueueName=queue_name)
    url = resp["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    sub = sns.subscribe(TopicArn=email_topic, Protocol="sqs", Endpoint=arn)
    sub_arn = sub["SubscriptionArn"]
    yield url, arn, sub_arn
    sqs.delete_queue(QueueUrl=url)


@pytest.fixture
def sms_queue(sqs, sns, sms_topic, unique_name):
    queue_name = f"sms-delivery-{unique_name}"
    resp = sqs.create_queue(QueueName=queue_name)
    url = resp["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    sub = sns.subscribe(TopicArn=sms_topic, Protocol="sqs", Endpoint=arn)
    sub_arn = sub["SubscriptionArn"]
    yield url, arn, sub_arn
    sqs.delete_queue(QueueUrl=url)


@pytest.fixture
def template_bucket(s3, unique_name):
    bucket = f"notif-templates-{unique_name}"
    s3.create_bucket(Bucket=bucket)
    yield bucket
    # Cleanup: delete all objects then bucket
    resp = s3.list_objects_v2(Bucket=bucket)
    for obj in resp.get("Contents", []):
        s3.delete_object(Bucket=bucket, Key=obj["Key"])
    s3.delete_bucket(Bucket=bucket)


@pytest.fixture
def delivery_table(dynamodb, unique_name):
    table_name = f"delivery-log-{unique_name}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "notification_id", "KeyType": "HASH"},
            {"AttributeName": "channel", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "notification_id", "AttributeType": "S"},
            {"AttributeName": "channel", "AttributeType": "S"},
            {"AttributeName": "recipient", "AttributeType": "S"},
            {"AttributeName": "delivery_status", "AttributeType": "S"},
            {"AttributeName": "sent_at", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-recipient",
                "KeySchema": [
                    {"AttributeName": "recipient", "KeyType": "HASH"},
                    {"AttributeName": "sent_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "by-status",
                "KeySchema": [
                    {"AttributeName": "delivery_status", "KeyType": "HASH"},
                    {"AttributeName": "sent_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


@pytest.fixture
def delivery_namespace(unique_name):
    return f"NotifDispatch/{unique_name}"


@pytest.fixture
def dispatch_log(logs, unique_name):
    group_name = f"/notif-dispatch/{unique_name}"
    stream_name = "dispatch-events"
    logs.create_log_group(logGroupName=group_name)
    logs.create_log_stream(logGroupName=group_name, logStreamName=stream_name)
    yield group_name, stream_name
    logs.delete_log_stream(logGroupName=group_name, logStreamName=stream_name)
    logs.delete_log_group(logGroupName=group_name)


# ---------------------------------------------------------------------------
# TestNotificationFanout (SNS + SQS)
# ---------------------------------------------------------------------------


class TestNotificationFanout:
    def test_single_channel_dispatch(self, sns, sqs, email_topic, email_queue):
        """Publish to email topic, receive from email queue."""
        queue_url, _queue_arn, _sub_arn = email_queue

        notification = {
            "notification_id": "NOTIF-001",
            "recipient": "alice@example.com",
            "subject": "Welcome!",
            "body": "Thanks for signing up.",
        }
        sns.publish(TopicArn=email_topic, Message=json.dumps(notification))

        messages = _receive_messages(sqs, queue_url)
        assert len(messages) >= 1

        # SNS wraps the message in an envelope
        body = json.loads(messages[0]["Body"])
        inner = json.loads(body["Message"]) if "Message" in body else body
        assert inner["notification_id"] == "NOTIF-001"
        assert inner["recipient"] == "alice@example.com"

    def test_multi_channel_dispatch(self, sns, sqs, email_topic, sms_topic, email_queue, sms_queue):
        """Same notification_id published to both channels."""
        email_url, _ea, _es = email_queue
        sms_url, _sa, _ss = sms_queue

        notification_id = "NOTIF-MULTI-002"
        for topic_arn, channel in [(email_topic, "email"), (sms_topic, "sms")]:
            sns.publish(
                TopicArn=topic_arn,
                Message=json.dumps(
                    {
                        "notification_id": notification_id,
                        "channel": channel,
                        "recipient": "bob@example.com",
                    }
                ),
            )

        email_msgs = _receive_messages(sqs, email_url)
        sms_msgs = _receive_messages(sqs, sms_url)
        assert len(email_msgs) >= 1
        assert len(sms_msgs) >= 1

        email_body = json.loads(email_msgs[0]["Body"])
        email_inner = json.loads(email_body["Message"]) if "Message" in email_body else email_body
        sms_body = json.loads(sms_msgs[0]["Body"])
        sms_inner = json.loads(sms_body["Message"]) if "Message" in sms_body else sms_body

        assert email_inner["notification_id"] == notification_id
        assert sms_inner["notification_id"] == notification_id

    def test_message_filtering(self, sns, sqs, email_topic, email_queue):
        """Set filter policy on subscription, verify it is applied."""
        queue_url, _queue_arn, sub_arn = email_queue

        # Set filter policy: only high priority messages
        filter_policy = json.dumps({"priority": ["high"]})
        sns.set_subscription_attributes(
            SubscriptionArn=sub_arn,
            AttributeName="FilterPolicy",
            AttributeValue=filter_policy,
        )

        # Verify the filter policy was set
        attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn)
        assert "Attributes" in attrs
        stored_policy = json.loads(attrs["Attributes"].get("FilterPolicy", "{}"))
        assert stored_policy == {"priority": ["high"]}

        # Publish a high-priority message with matching attribute
        sns.publish(
            TopicArn=email_topic,
            Message=json.dumps({"notification_id": "NOTIF-HIGH", "body": "Urgent alert"}),
            MessageAttributes={
                "priority": {"DataType": "String", "StringValue": "high"},
            },
        )

        # Receive: should get the high priority message
        messages = _receive_messages(sqs, queue_url, expected=1, timeout=5)
        assert len(messages) >= 1
        body = json.loads(messages[0]["Body"])
        inner = json.loads(body["Message"]) if "Message" in body else body
        assert inner["notification_id"] == "NOTIF-HIGH"

    def test_batch_notifications(self, sns, sqs, email_topic, email_queue):
        """Publish 5 messages to topic, receive all from queue."""
        queue_url, _queue_arn, _sub_arn = email_queue

        for i in range(5):
            sns.publish(
                TopicArn=email_topic,
                Message=json.dumps(
                    {"notification_id": f"NOTIF-BATCH-{i:03d}", "recipient": f"user-{i}@test.com"}
                ),
            )

        messages = _receive_messages(sqs, queue_url, expected=5, timeout=15)
        assert len(messages) == 5

        notification_ids = set()
        for msg in messages:
            body = json.loads(msg["Body"])
            inner = json.loads(body["Message"]) if "Message" in body else body
            notification_ids.add(inner["notification_id"])

        for i in range(5):
            assert f"NOTIF-BATCH-{i:03d}" in notification_ids


# ---------------------------------------------------------------------------
# TestNotificationTemplates (S3)
# ---------------------------------------------------------------------------


class TestNotificationTemplates:
    def test_store_template(self, s3, template_bucket):
        """Store and retrieve an HTML notification template."""
        key = "templates/welcome-email.html"
        html = "<html><body><h1>Welcome, {{name}}!</h1></body></html>"
        s3.put_object(Bucket=template_bucket, Key=key, Body=html.encode())

        resp = s3.get_object(Bucket=template_bucket, Key=key)
        content = resp["Body"].read().decode()
        assert content == html
        assert "{{name}}" in content

    def test_template_variants(self, s3, template_bucket):
        """Upload locale variants, list with prefix."""
        s3.put_object(
            Bucket=template_bucket,
            Key="templates/welcome-email/en.html",
            Body=b"<html>Welcome!</html>",
        )
        s3.put_object(
            Bucket=template_bucket,
            Key="templates/welcome-email/es.html",
            Body=b"<html>Bienvenido!</html>",
        )

        resp = s3.list_objects_v2(Bucket=template_bucket, Prefix="templates/welcome-email/")
        keys = [obj["Key"] for obj in resp.get("Contents", [])]
        assert len(keys) == 2
        assert "templates/welcome-email/en.html" in keys
        assert "templates/welcome-email/es.html" in keys

    def test_update_template(self, s3, template_bucket):
        """Overwriting a template returns the latest version."""
        key = "templates/promo.html"
        s3.put_object(Bucket=template_bucket, Key=key, Body=b"<html>Old promo</html>")
        s3.put_object(Bucket=template_bucket, Key=key, Body=b"<html>New promo</html>")

        resp = s3.get_object(Bucket=template_bucket, Key=key)
        content = resp["Body"].read().decode()
        assert content == "<html>New promo</html>"

    def test_template_metadata(self, s3, template_bucket):
        """PutObject with custom metadata, verify via HeadObject."""
        key = "templates/alert.html"
        metadata = {"version": "3", "author": "ops-team", "last-modified": "2026-01-15"}
        s3.put_object(
            Bucket=template_bucket,
            Key=key,
            Body=b"<html>Alert: {{message}}</html>",
            Metadata=metadata,
        )

        resp = s3.head_object(Bucket=template_bucket, Key=key)
        assert resp["Metadata"]["version"] == "3"
        assert resp["Metadata"]["author"] == "ops-team"
        assert resp["Metadata"]["last-modified"] == "2026-01-15"


# ---------------------------------------------------------------------------
# TestDeliveryTracking (DynamoDB)
# ---------------------------------------------------------------------------


class TestDeliveryTracking:
    def test_record_delivery(self, dynamodb, delivery_table):
        """PutItem and GetItem for a single delivery record."""
        item = {
            "notification_id": {"S": "NOTIF-D001"},
            "channel": {"S": "email"},
            "recipient": {"S": "alice@example.com"},
            "delivery_status": {"S": "sent"},
            "sent_at": {"S": "2026-03-08T10:00:00Z"},
            "message_preview": {"S": "Welcome to our platform!"},
        }
        dynamodb.put_item(TableName=delivery_table, Item=item)

        resp = dynamodb.get_item(
            TableName=delivery_table,
            Key={"notification_id": {"S": "NOTIF-D001"}, "channel": {"S": "email"}},
        )
        assert "Item" in resp
        assert resp["Item"]["recipient"]["S"] == "alice@example.com"
        assert resp["Item"]["delivery_status"]["S"] == "sent"
        assert resp["Item"]["message_preview"]["S"] == "Welcome to our platform!"

    def test_multi_channel_delivery_record(self, dynamodb, delivery_table):
        """Same notification_id with email and sms channels (different sort keys)."""
        base = {
            "notification_id": {"S": "NOTIF-MC001"},
            "recipient": {"S": "bob@example.com"},
            "delivery_status": {"S": "sent"},
            "sent_at": {"S": "2026-03-08T10:01:00Z"},
        }
        dynamodb.put_item(TableName=delivery_table, Item={**base, "channel": {"S": "email"}})
        dynamodb.put_item(TableName=delivery_table, Item={**base, "channel": {"S": "sms"}})

        email_resp = dynamodb.get_item(
            TableName=delivery_table,
            Key={"notification_id": {"S": "NOTIF-MC001"}, "channel": {"S": "email"}},
        )
        sms_resp = dynamodb.get_item(
            TableName=delivery_table,
            Key={"notification_id": {"S": "NOTIF-MC001"}, "channel": {"S": "sms"}},
        )
        assert "Item" in email_resp
        assert "Item" in sms_resp
        assert email_resp["Item"]["recipient"]["S"] == "bob@example.com"
        assert sms_resp["Item"]["recipient"]["S"] == "bob@example.com"

    def test_query_by_recipient(self, dynamodb, delivery_table):
        """Insert notifications for two users, query GSI by recipient."""
        for i in range(5):
            dynamodb.put_item(
                TableName=delivery_table,
                Item={
                    "notification_id": {"S": f"NOTIF-QR-A{i:02d}"},
                    "channel": {"S": "email"},
                    "recipient": {"S": "user-A"},
                    "delivery_status": {"S": "sent"},
                    "sent_at": {"S": f"2026-03-08T10:{i:02d}:00Z"},
                },
            )
        for i in range(3):
            dynamodb.put_item(
                TableName=delivery_table,
                Item={
                    "notification_id": {"S": f"NOTIF-QR-B{i:02d}"},
                    "channel": {"S": "email"},
                    "recipient": {"S": "user-B"},
                    "delivery_status": {"S": "sent"},
                    "sent_at": {"S": f"2026-03-08T11:{i:02d}:00Z"},
                },
            )

        resp = dynamodb.query(
            TableName=delivery_table,
            IndexName="by-recipient",
            KeyConditionExpression="recipient = :r",
            ExpressionAttributeValues={":r": {"S": "user-A"}},
        )
        assert resp["Count"] == 5

    def test_update_delivery_status(self, dynamodb, delivery_table):
        """PutItem then UpdateItem to change status to delivered."""
        dynamodb.put_item(
            TableName=delivery_table,
            Item={
                "notification_id": {"S": "NOTIF-UPD001"},
                "channel": {"S": "email"},
                "recipient": {"S": "carol@example.com"},
                "delivery_status": {"S": "sent"},
                "sent_at": {"S": "2026-03-08T12:00:00Z"},
            },
        )

        dynamodb.update_item(
            TableName=delivery_table,
            Key={"notification_id": {"S": "NOTIF-UPD001"}, "channel": {"S": "email"}},
            UpdateExpression="SET delivery_status = :s, delivered_at = :d",
            ExpressionAttributeValues={
                ":s": {"S": "delivered"},
                ":d": {"S": "2026-03-08T12:00:05Z"},
            },
        )

        resp = dynamodb.get_item(
            TableName=delivery_table,
            Key={"notification_id": {"S": "NOTIF-UPD001"}, "channel": {"S": "email"}},
        )
        assert resp["Item"]["delivery_status"]["S"] == "delivered"
        assert resp["Item"]["delivered_at"]["S"] == "2026-03-08T12:00:05Z"

    def test_query_failed_deliveries(self, dynamodb, delivery_table):
        """Insert mixed statuses, query GSI by-status for failures."""
        statuses = ["sent", "sent", "sent", "delivered", "delivered", "failed", "failed"]
        for i, status in enumerate(statuses):
            dynamodb.put_item(
                TableName=delivery_table,
                Item={
                    "notification_id": {"S": f"NOTIF-FAIL{i:02d}"},
                    "channel": {"S": "email"},
                    "recipient": {"S": f"user-{i}@test.com"},
                    "delivery_status": {"S": status},
                    "sent_at": {"S": f"2026-03-08T13:{i:02d}:00Z"},
                },
            )

        resp = dynamodb.query(
            TableName=delivery_table,
            IndexName="by-status",
            KeyConditionExpression="delivery_status = :s",
            ExpressionAttributeValues={":s": {"S": "failed"}},
        )
        assert resp["Count"] == 2


# ---------------------------------------------------------------------------
# TestDeliveryMetrics (CloudWatch + Logs)
# ---------------------------------------------------------------------------


class TestDeliveryMetrics:
    def test_publish_delivery_metrics(self, cloudwatch, delivery_namespace):
        """PutMetricData and GetMetricStatistics with dimensions."""
        cloudwatch.put_metric_data(
            Namespace=delivery_namespace,
            MetricData=[
                {
                    "MetricName": "NotificationsSent",
                    "Value": 100,
                    "Unit": "Count",
                    "Dimensions": [{"Name": "Channel", "Value": "email"}],
                },
                {
                    "MetricName": "DeliveryFailures",
                    "Value": 5,
                    "Unit": "Count",
                    "Dimensions": [{"Name": "Channel", "Value": "email"}],
                },
            ],
        )

        from datetime import timedelta

        now = datetime.now(UTC)
        resp = cloudwatch.get_metric_statistics(
            Namespace=delivery_namespace,
            MetricName="NotificationsSent",
            StartTime=(now - timedelta(hours=1)).isoformat(),
            EndTime=(now + timedelta(hours=1)).isoformat(),
            Period=3600,
            Statistics=["Sum"],
            Dimensions=[{"Name": "Channel", "Value": "email"}],
        )
        datapoints = resp.get("Datapoints", [])
        assert len(datapoints) >= 1
        assert datapoints[0]["Sum"] == 100.0

    def test_dispatch_audit_log(self, logs, dispatch_log):
        """PutLogEvents and GetLogEvents for dispatch audit trail."""
        group_name, stream_name = dispatch_log

        events = []
        base_ts = int(time.time() * 1000)
        for i in range(5):
            events.append(
                {
                    "timestamp": base_ts + i * 1000,
                    "message": json.dumps(
                        {
                            "notification_id": f"NOTIF-LOG-{i:03d}",
                            "channel": "email",
                            "recipient": f"user-{i}@test.com",
                            "status": "dispatched",
                        }
                    ),
                }
            )

        logs.put_log_events(
            logGroupName=group_name,
            logStreamName=stream_name,
            logEvents=events,
        )

        resp = logs.get_log_events(
            logGroupName=group_name,
            logStreamName=stream_name,
            startFromHead=True,
        )
        log_events = resp.get("events", [])
        assert len(log_events) == 5

        for evt in log_events:
            parsed = json.loads(evt["message"])
            assert "notification_id" in parsed
            assert parsed["status"] == "dispatched"

    def test_filter_failed_dispatches(self, logs, dispatch_log):
        """Put mixed success/failure events, filter for failures."""
        group_name, stream_name = dispatch_log

        base_ts = int(time.time() * 1000)
        events = []
        for i, status in enumerate(["success", "success", "failed", "success", "failed"]):
            events.append(
                {
                    "timestamp": base_ts + i * 1000,
                    "message": json.dumps(
                        {
                            "notification_id": f"NOTIF-FILT-{i:03d}",
                            "status": status,
                        }
                    ),
                }
            )

        logs.put_log_events(
            logGroupName=group_name,
            logStreamName=stream_name,
            logEvents=events,
        )

        resp = logs.filter_log_events(
            logGroupName=group_name,
            filterPattern="failed",
        )
        matched = resp.get("events", [])
        assert len(matched) == 2
        for evt in matched:
            parsed = json.loads(evt["message"])
            assert parsed["status"] == "failed"


# ---------------------------------------------------------------------------
# TestNotificationEndToEnd
# ---------------------------------------------------------------------------


class TestNotificationEndToEnd:
    def test_template_render_and_send(
        self, s3, sns, sqs, template_bucket, email_topic, email_queue
    ):
        """Load template from S3, render, publish to SNS, receive from SQS."""
        queue_url, _queue_arn, _sub_arn = email_queue

        # Store template
        template = "<html><body>Hello {{name}}, your code is {{code}}.</body></html>"
        s3.put_object(
            Bucket=template_bucket,
            Key="templates/verification.html",
            Body=template.encode(),
        )

        # Load and render
        resp = s3.get_object(Bucket=template_bucket, Key="templates/verification.html")
        raw = resp["Body"].read().decode()
        rendered = raw.replace("{{name}}", "Dana").replace("{{code}}", "ABC123")

        # Publish rendered message
        sns.publish(
            TopicArn=email_topic,
            Message=json.dumps(
                {
                    "notification_id": "NOTIF-RENDER-001",
                    "rendered_body": rendered,
                    "recipient": "dana@example.com",
                }
            ),
        )

        messages = _receive_messages(sqs, queue_url)
        assert len(messages) >= 1
        body = json.loads(messages[0]["Body"])
        inner = json.loads(body["Message"]) if "Message" in body else body
        assert "Dana" in inner["rendered_body"]
        assert "ABC123" in inner["rendered_body"]

    def test_full_notification_pipeline(
        self,
        s3,
        sns,
        sqs,
        dynamodb,
        cloudwatch,
        logs,
        template_bucket,
        email_topic,
        sms_topic,
        email_queue,
        sms_queue,
        delivery_table,
        delivery_namespace,
        dispatch_log,
    ):
        """End-to-end: template -> SNS -> SQS -> DynamoDB -> CloudWatch -> Logs."""
        email_url, _ea, _es = email_queue
        sms_url, _sa, _ss = sms_queue
        log_group, log_stream = dispatch_log
        notification_id = "NOTIF-E2E-001"

        # 1. Store and load template from S3
        template = "<html>Hi {{name}}, order {{order_id}} confirmed.</html>"
        s3.put_object(
            Bucket=template_bucket,
            Key="templates/order-confirm.html",
            Body=template.encode(),
        )
        resp = s3.get_object(Bucket=template_bucket, Key="templates/order-confirm.html")
        rendered = (
            resp["Body"]
            .read()
            .decode()
            .replace("{{name}}", "Eve")
            .replace("{{order_id}}", "ORD-999")
        )

        # 2. Publish to both channels
        for topic_arn, channel in [(email_topic, "email"), (sms_topic, "sms")]:
            sns.publish(
                TopicArn=topic_arn,
                Message=json.dumps(
                    {
                        "notification_id": notification_id,
                        "channel": channel,
                        "rendered_body": rendered,
                        "recipient": "eve@example.com",
                    }
                ),
            )

        # 3. Receive from both queues
        email_msgs = _receive_messages(sqs, email_url)
        sms_msgs = _receive_messages(sqs, sms_url)
        assert len(email_msgs) >= 1
        assert len(sms_msgs) >= 1

        # 4. Record delivery in DynamoDB for each channel
        for channel in ["email", "sms"]:
            dynamodb.put_item(
                TableName=delivery_table,
                Item={
                    "notification_id": {"S": notification_id},
                    "channel": {"S": channel},
                    "recipient": {"S": "eve@example.com"},
                    "delivery_status": {"S": "sent"},
                    "sent_at": {"S": "2026-03-08T14:00:00Z"},
                },
            )

        # 5. Update status to delivered
        for channel in ["email", "sms"]:
            dynamodb.update_item(
                TableName=delivery_table,
                Key={
                    "notification_id": {"S": notification_id},
                    "channel": {"S": channel},
                },
                UpdateExpression="SET delivery_status = :s, delivered_at = :d",
                ExpressionAttributeValues={
                    ":s": {"S": "delivered"},
                    ":d": {"S": "2026-03-08T14:00:03Z"},
                },
            )

        # 6. Publish metrics to CloudWatch
        cloudwatch.put_metric_data(
            Namespace=delivery_namespace,
            MetricData=[
                {
                    "MetricName": "NotificationsSent",
                    "Value": 2,
                    "Unit": "Count",
                    "Dimensions": [{"Name": "NotificationId", "Value": notification_id}],
                },
            ],
        )

        # 7. Log audit events
        base_ts = int(time.time() * 1000)
        audit_events = []
        for i, channel in enumerate(["email", "sms"]):
            audit_events.append(
                {
                    "timestamp": base_ts + i * 1000,
                    "message": json.dumps(
                        {
                            "notification_id": notification_id,
                            "channel": channel,
                            "recipient": "eve@example.com",
                            "status": "delivered",
                        }
                    ),
                }
            )
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            logEvents=audit_events,
        )

        # 8. Verify DynamoDB: query by notification_id for both channels
        for channel in ["email", "sms"]:
            resp = dynamodb.get_item(
                TableName=delivery_table,
                Key={
                    "notification_id": {"S": notification_id},
                    "channel": {"S": channel},
                },
            )
            assert resp["Item"]["delivery_status"]["S"] == "delivered"
            assert resp["Item"]["recipient"]["S"] == "eve@example.com"

        # 9. Verify metrics
        from datetime import timedelta

        now = datetime.now(UTC)
        resp = cloudwatch.get_metric_statistics(
            Namespace=delivery_namespace,
            MetricName="NotificationsSent",
            StartTime=(now - timedelta(hours=1)).isoformat(),
            EndTime=(now + timedelta(hours=1)).isoformat(),
            Period=3600,
            Statistics=["Sum"],
            Dimensions=[{"Name": "NotificationId", "Value": notification_id}],
        )
        datapoints = resp.get("Datapoints", [])
        assert len(datapoints) >= 1
        assert datapoints[0]["Sum"] == 2.0
