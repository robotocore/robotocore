"""
Fixtures for notification dispatch tests.

Provides a fully-configured NotificationService plus convenience fixtures
for channels, templates, and user preferences.
"""

import pytest

from .app import NotificationService
from .models import Channel, Template


@pytest.fixture
def notifier(sns, sqs, s3, dynamodb, cloudwatch, logs, unique_name):
    """Create a NotificationService with all AWS resources provisioned."""
    template_bucket = f"notif-templates-{unique_name}"
    delivery_table = f"notif-delivery-{unique_name}"
    preferences_table = f"notif-prefs-{unique_name}"
    schedule_table = f"notif-schedule-{unique_name}"
    metrics_namespace = f"NotifDispatch/{unique_name}"
    log_group = f"/notif-dispatch/{unique_name}"
    log_stream = "dispatch-events"

    # Create S3 bucket
    s3.create_bucket(Bucket=template_bucket)

    # Create delivery tracking table
    dynamodb.create_table(
        TableName=delivery_table,
        KeySchema=[
            {"AttributeName": "notification_id", "KeyType": "HASH"},
            {"AttributeName": "channel", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "notification_id", "AttributeType": "S"},
            {"AttributeName": "channel", "AttributeType": "S"},
            {"AttributeName": "user_id", "AttributeType": "S"},
            {"AttributeName": "sent_at", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-user",
                "KeySchema": [
                    {"AttributeName": "user_id", "KeyType": "HASH"},
                    {"AttributeName": "sent_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "by-channel",
                "KeySchema": [
                    {"AttributeName": "channel", "KeyType": "HASH"},
                    {"AttributeName": "sent_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    # Create preferences table
    dynamodb.create_table(
        TableName=preferences_table,
        KeySchema=[
            {"AttributeName": "user_id", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "user_id", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    # Create schedule table
    dynamodb.create_table(
        TableName=schedule_table,
        KeySchema=[
            {"AttributeName": "schedule_id", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "schedule_id", "AttributeType": "S"},
            {"AttributeName": "user_id", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-user",
                "KeySchema": [
                    {"AttributeName": "user_id", "KeyType": "HASH"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    # Create CloudWatch Logs
    logs.create_log_group(logGroupName=log_group)
    logs.create_log_stream(logGroupName=log_group, logStreamName=log_stream)

    svc = NotificationService(
        sns=sns,
        sqs=sqs,
        s3=s3,
        dynamodb=dynamodb,
        cloudwatch=cloudwatch,
        logs=logs,
        template_bucket=template_bucket,
        delivery_table=delivery_table,
        preferences_table=preferences_table,
        schedule_table=schedule_table,
        metrics_namespace=metrics_namespace,
        log_group=log_group,
        log_stream=log_stream,
    )

    yield svc

    svc.cleanup()


@pytest.fixture
def email_channel(notifier, unique_name):
    """Pre-configured email channel."""
    return notifier.create_channel(Channel.EMAIL, unique_name)


@pytest.fixture
def sms_channel(notifier, unique_name):
    """Pre-configured SMS channel."""
    return notifier.create_channel(Channel.SMS, unique_name)


@pytest.fixture
def sample_template(notifier):
    """Upload a sample welcome email template."""
    template = Template(
        template_id="welcome-email",
        name="Welcome Email",
        channel=Channel.EMAIL,
        subject="Welcome, {{name}}!",
        body="Hello {{name}}, thanks for joining {{company}}. Your account is ready.",
        variables=["name", "company"],
    )
    notifier.upload_template(template)
    return template


@pytest.fixture
def sms_template(notifier):
    """Upload a sample SMS template."""
    template = Template(
        template_id="verify-sms",
        name="Verification SMS",
        channel=Channel.SMS,
        subject="Verify",
        body="Your verification code is {{code}}. Expires in {{minutes}} minutes.",
        variables=["code", "minutes"],
    )
    notifier.upload_template(template)
    return template
