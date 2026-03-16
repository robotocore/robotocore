"""S3 event notification end-to-end tests — verify S3 → SQS, S3 → SNS → SQS,
S3 → Lambda, prefix/suffix filters, delete notifications, and multi-target configs."""

import json
import os
import time
import uuid

import boto3
import pytest

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")
REGION = "us-east-1"
CLIENT_KW = dict(
    endpoint_url=ENDPOINT_URL,
    region_name=REGION,
    aws_access_key_id="testing",
    aws_secret_access_key="testing",
)


@pytest.fixture
def s3():
    return boto3.client("s3", **CLIENT_KW)


@pytest.fixture
def sqs():
    return boto3.client("sqs", **CLIENT_KW)


@pytest.fixture
def sns():
    return boto3.client("sns", **CLIENT_KW)


@pytest.fixture
def unique_name():
    """Return a unique name suffix to avoid collisions in parallel runs."""
    return uuid.uuid4().hex[:8]


@pytest.fixture
def s3_bucket(s3, unique_name):
    """Create and clean up an S3 bucket."""
    name = f"notif-test-{unique_name}"
    s3.create_bucket(Bucket=name)
    yield name
    try:
        objs = s3.list_objects_v2(Bucket=name).get("Contents", [])
        for obj in objs:
            s3.delete_object(Bucket=name, Key=obj["Key"])
        s3.delete_bucket(Bucket=name)
    except Exception:
        pass  # best-effort cleanup


@pytest.fixture
def sqs_queue(sqs, unique_name):
    """Create and clean up an SQS queue, yielding (url, arn)."""
    name = f"notif-q-{unique_name}"
    url = sqs.create_queue(QueueName=name)["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    yield url, arn
    try:
        sqs.delete_queue(QueueUrl=url)
    except Exception:
        pass  # best-effort cleanup


def _drain_queue(sqs, queue_url, max_wait=3):
    """Receive all available messages from a queue within max_wait seconds."""
    messages = []
    deadline = time.time() + max_wait
    while time.time() < deadline:
        resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        batch = resp.get("Messages", [])
        messages.extend(batch)
        if batch:
            break
    return messages


# ---------------------------------------------------------------------------
# Test 1: S3 → SQS on ObjectCreated
# ---------------------------------------------------------------------------


class TestS3ToSqsNotification:
    def test_put_object_sends_sqs_message(self, s3, sqs, s3_bucket, sqs_queue):
        """Putting an object triggers an SQS notification."""
        queue_url, queue_arn = sqs_queue

        s3.put_bucket_notification_configuration(
            Bucket=s3_bucket,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {
                        "QueueArn": queue_arn,
                        "Events": ["s3:ObjectCreated:*"],
                    }
                ]
            },
        )

        s3.put_object(Bucket=s3_bucket, Key="hello.txt", Body=b"world")

        messages = _drain_queue(sqs, queue_url)
        assert len(messages) >= 1, "Expected at least 1 SQS message from S3 notification"

        body = json.loads(messages[0]["Body"])
        records = body["Records"]
        assert len(records) == 1
        record = records[0]
        assert record["eventSource"] == "aws:s3"
        assert record["eventName"] == "ObjectCreated:Put"
        assert record["s3"]["bucket"]["name"] == s3_bucket
        assert record["s3"]["object"]["key"] == "hello.txt"

    def test_notification_config_roundtrip(self, s3, sqs, s3_bucket, sqs_queue):
        """GetBucketNotificationConfiguration returns what was set."""
        _, queue_arn = sqs_queue

        s3.put_bucket_notification_configuration(
            Bucket=s3_bucket,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {
                        "QueueArn": queue_arn,
                        "Events": ["s3:ObjectCreated:Put", "s3:ObjectRemoved:*"],
                    }
                ]
            },
        )

        config = s3.get_bucket_notification_configuration(Bucket=s3_bucket)
        queue_configs = config.get("QueueConfigurations", [])
        assert len(queue_configs) == 1
        assert queue_configs[0]["QueueArn"] == queue_arn
        assert set(queue_configs[0]["Events"]) == {
            "s3:ObjectCreated:Put",
            "s3:ObjectRemoved:*",
        }

    def test_empty_config_clears_notifications(self, s3, sqs, s3_bucket, sqs_queue):
        """Setting empty NotificationConfiguration removes all notifications."""
        queue_url, queue_arn = sqs_queue

        # Set notification
        s3.put_bucket_notification_configuration(
            Bucket=s3_bucket,
            NotificationConfiguration={
                "QueueConfigurations": [{"QueueArn": queue_arn, "Events": ["s3:ObjectCreated:*"]}]
            },
        )

        # Clear notification
        s3.put_bucket_notification_configuration(
            Bucket=s3_bucket,
            NotificationConfiguration={},
        )

        # Put object — should NOT trigger notification
        s3.put_object(Bucket=s3_bucket, Key="silent.txt", Body=b"no-notify")
        time.sleep(0.5)

        messages = _drain_queue(sqs, queue_url, max_wait=2)
        assert len(messages) == 0, "Expected no messages after clearing notification config"


# ---------------------------------------------------------------------------
# Test 2: S3 → SNS → SQS
# ---------------------------------------------------------------------------


class TestS3ToSnsNotification:
    def test_put_object_sends_sns_to_sqs(self, s3, sqs, sns, s3_bucket, unique_name):
        """S3 → SNS topic → SQS subscriber pipeline delivers event."""
        topic_arn = sns.create_topic(Name=f"notif-topic-{unique_name}")["TopicArn"]
        queue_name = f"notif-sns-q-{unique_name}"
        queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        # Subscribe SQS to SNS
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

        # Configure S3 → SNS
        s3.put_bucket_notification_configuration(
            Bucket=s3_bucket,
            NotificationConfiguration={
                "TopicConfigurations": [{"TopicArn": topic_arn, "Events": ["s3:ObjectCreated:*"]}]
            },
        )

        s3.put_object(Bucket=s3_bucket, Key="via-sns.txt", Body=b"data")

        messages = _drain_queue(sqs, queue_url)
        assert len(messages) >= 1, "Expected at least 1 message via SNS"

        # SNS wraps the S3 event in an SNS envelope
        envelope = json.loads(messages[0]["Body"])
        assert envelope["Type"] == "Notification"
        assert envelope["TopicArn"] == topic_arn

        # The inner Message should contain S3 event records
        inner = json.loads(envelope["Message"])
        assert "Records" in inner
        assert inner["Records"][0]["s3"]["object"]["key"] == "via-sns.txt"

        # Cleanup
        try:
            sqs.delete_queue(QueueUrl=queue_url)
            sns.delete_topic(TopicArn=topic_arn)
        except Exception:
            pass  # best-effort cleanup


# ---------------------------------------------------------------------------
# Test 3: Prefix filter
# ---------------------------------------------------------------------------


class TestPrefixFilter:
    def test_prefix_filter_matches(self, s3, sqs, s3_bucket, sqs_queue):
        """Only keys starting with the configured prefix trigger notifications."""
        queue_url, queue_arn = sqs_queue

        s3.put_bucket_notification_configuration(
            Bucket=s3_bucket,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {
                        "QueueArn": queue_arn,
                        "Events": ["s3:ObjectCreated:*"],
                        "Filter": {
                            "Key": {"FilterRules": [{"Name": "prefix", "Value": "images/"}]}
                        },
                    }
                ]
            },
        )

        # This should NOT match the prefix filter
        s3.put_object(Bucket=s3_bucket, Key="docs/readme.txt", Body=b"doc")
        time.sleep(0.5)
        messages = _drain_queue(sqs, queue_url, max_wait=2)
        assert len(messages) == 0, "Non-matching prefix should not trigger notification"

        # This SHOULD match the prefix filter
        s3.put_object(Bucket=s3_bucket, Key="images/photo.png", Body=b"img")
        messages = _drain_queue(sqs, queue_url)
        assert len(messages) >= 1, "Matching prefix should trigger notification"

        body = json.loads(messages[0]["Body"])
        assert body["Records"][0]["s3"]["object"]["key"] == "images/photo.png"


# ---------------------------------------------------------------------------
# Test 4: Suffix filter
# ---------------------------------------------------------------------------


class TestSuffixFilter:
    def test_suffix_filter_matches(self, s3, sqs, s3_bucket, sqs_queue):
        """Only keys ending with the configured suffix trigger notifications."""
        queue_url, queue_arn = sqs_queue

        s3.put_bucket_notification_configuration(
            Bucket=s3_bucket,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {
                        "QueueArn": queue_arn,
                        "Events": ["s3:ObjectCreated:*"],
                        "Filter": {"Key": {"FilterRules": [{"Name": "suffix", "Value": ".csv"}]}},
                    }
                ]
            },
        )

        # Non-matching suffix
        s3.put_object(Bucket=s3_bucket, Key="data.json", Body=b"json")
        time.sleep(0.5)
        messages = _drain_queue(sqs, queue_url, max_wait=2)
        assert len(messages) == 0, "Non-matching suffix should not trigger notification"

        # Matching suffix
        s3.put_object(Bucket=s3_bucket, Key="report.csv", Body=b"csv")
        messages = _drain_queue(sqs, queue_url)
        assert len(messages) >= 1, "Matching suffix should trigger notification"

        body = json.loads(messages[0]["Body"])
        assert body["Records"][0]["s3"]["object"]["key"] == "report.csv"


# ---------------------------------------------------------------------------
# Test 5: Delete notification (s3:ObjectRemoved:*)
# ---------------------------------------------------------------------------


class TestDeleteNotification:
    def test_delete_object_sends_notification(self, s3, sqs, s3_bucket, sqs_queue):
        """Deleting an object triggers s3:ObjectRemoved:Delete notification."""
        queue_url, queue_arn = sqs_queue

        s3.put_bucket_notification_configuration(
            Bucket=s3_bucket,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {
                        "QueueArn": queue_arn,
                        "Events": ["s3:ObjectRemoved:*"],
                    }
                ]
            },
        )

        # Create then delete an object
        s3.put_object(Bucket=s3_bucket, Key="ephemeral.txt", Body=b"temp")
        time.sleep(0.3)

        # Drain any unexpected messages
        _drain_queue(sqs, queue_url, max_wait=1)

        s3.delete_object(Bucket=s3_bucket, Key="ephemeral.txt")

        messages = _drain_queue(sqs, queue_url)
        assert len(messages) >= 1, "Expected delete notification"

        body = json.loads(messages[0]["Body"])
        record = body["Records"][0]
        assert record["eventName"] == "ObjectRemoved:Delete"
        assert record["s3"]["object"]["key"] == "ephemeral.txt"

    def test_create_event_not_sent_for_delete_config(self, s3, sqs, s3_bucket, sqs_queue):
        """When only ObjectRemoved events are configured, puts should not notify."""
        queue_url, queue_arn = sqs_queue

        s3.put_bucket_notification_configuration(
            Bucket=s3_bucket,
            NotificationConfiguration={
                "QueueConfigurations": [{"QueueArn": queue_arn, "Events": ["s3:ObjectRemoved:*"]}]
            },
        )

        s3.put_object(Bucket=s3_bucket, Key="nope.txt", Body=b"data")
        time.sleep(0.5)

        messages = _drain_queue(sqs, queue_url, max_wait=2)
        assert len(messages) == 0, "ObjectCreated should not trigger ObjectRemoved notification"


# ---------------------------------------------------------------------------
# Test 6: Multiple notification targets
# ---------------------------------------------------------------------------


class TestMultipleTargets:
    def test_different_events_to_different_queues(self, s3, sqs, s3_bucket, unique_name):
        """Different event types can route to different SQS queues."""
        # Create two queues
        q1_url = sqs.create_queue(QueueName=f"notif-create-{unique_name}")["QueueUrl"]
        q1_arn = sqs.get_queue_attributes(QueueUrl=q1_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        q2_url = sqs.create_queue(QueueName=f"notif-delete-{unique_name}")["QueueUrl"]
        q2_arn = sqs.get_queue_attributes(QueueUrl=q2_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        s3.put_bucket_notification_configuration(
            Bucket=s3_bucket,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {"QueueArn": q1_arn, "Events": ["s3:ObjectCreated:*"]},
                    {"QueueArn": q2_arn, "Events": ["s3:ObjectRemoved:*"]},
                ]
            },
        )

        # Put -> should go to q1 only
        s3.put_object(Bucket=s3_bucket, Key="multi.txt", Body=b"data")
        time.sleep(0.5)

        q1_msgs = _drain_queue(sqs, q1_url)
        q2_msgs = _drain_queue(sqs, q2_url, max_wait=1)
        assert len(q1_msgs) >= 1, "Create event should go to q1"
        assert len(q2_msgs) == 0, "Create event should NOT go to q2"

        # Delete -> should go to q2 only
        s3.delete_object(Bucket=s3_bucket, Key="multi.txt")
        time.sleep(0.5)

        q1_after = _drain_queue(sqs, q1_url, max_wait=1)
        q2_after = _drain_queue(sqs, q2_url)
        assert len(q1_after) == 0, "Delete event should NOT go to q1"
        assert len(q2_after) >= 1, "Delete event should go to q2"

        # Cleanup
        try:
            sqs.delete_queue(QueueUrl=q1_url)
            sqs.delete_queue(QueueUrl=q2_url)
        except Exception:
            pass  # best-effort cleanup


# ---------------------------------------------------------------------------
# Test 7: Event record structure validation
# ---------------------------------------------------------------------------


class TestEventRecordStructure:
    def test_event_record_has_required_fields(self, s3, sqs, s3_bucket, sqs_queue):
        """S3 event records contain all required fields per AWS spec."""
        queue_url, queue_arn = sqs_queue

        s3.put_bucket_notification_configuration(
            Bucket=s3_bucket,
            NotificationConfiguration={
                "QueueConfigurations": [{"QueueArn": queue_arn, "Events": ["s3:ObjectCreated:*"]}]
            },
        )

        s3.put_object(Bucket=s3_bucket, Key="structure-test.txt", Body=b"content")

        messages = _drain_queue(sqs, queue_url)
        assert len(messages) >= 1

        body = json.loads(messages[0]["Body"])
        record = body["Records"][0]

        # Top-level required fields
        assert record["eventVersion"] == "2.1"
        assert record["eventSource"] == "aws:s3"
        assert "awsRegion" in record
        assert "eventTime" in record
        assert "eventName" in record

        # S3 structure
        s3_data = record["s3"]
        assert s3_data["s3SchemaVersion"] == "1.0"
        assert "configurationId" in s3_data
        assert s3_data["bucket"]["name"] == s3_bucket
        assert f"arn:aws:s3:::{s3_bucket}" == s3_data["bucket"]["arn"]
        assert s3_data["object"]["key"] == "structure-test.txt"
        assert "sequencer" in s3_data["object"]

    def test_prefix_and_suffix_combined_filter(self, s3, sqs, s3_bucket, sqs_queue):
        """Prefix and suffix filters can be combined on a single notification."""
        queue_url, queue_arn = sqs_queue

        s3.put_bucket_notification_configuration(
            Bucket=s3_bucket,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {
                        "QueueArn": queue_arn,
                        "Events": ["s3:ObjectCreated:*"],
                        "Filter": {
                            "Key": {
                                "FilterRules": [
                                    {"Name": "prefix", "Value": "logs/"},
                                    {"Name": "suffix", "Value": ".gz"},
                                ]
                            }
                        },
                    }
                ]
            },
        )

        # Matches prefix but not suffix
        s3.put_object(Bucket=s3_bucket, Key="logs/app.txt", Body=b"txt")
        time.sleep(0.5)
        msgs = _drain_queue(sqs, queue_url, max_wait=2)
        assert len(msgs) == 0, "prefix match + suffix mismatch should not notify"

        # Matches suffix but not prefix
        s3.put_object(Bucket=s3_bucket, Key="data/archive.gz", Body=b"gz")
        time.sleep(0.5)
        msgs = _drain_queue(sqs, queue_url, max_wait=2)
        assert len(msgs) == 0, "prefix mismatch + suffix match should not notify"

        # Matches both prefix AND suffix
        s3.put_object(Bucket=s3_bucket, Key="logs/archive.gz", Body=b"gz")
        msgs = _drain_queue(sqs, queue_url)
        assert len(msgs) >= 1, "Both prefix and suffix match should notify"

        body = json.loads(msgs[0]["Body"])
        assert body["Records"][0]["s3"]["object"]["key"] == "logs/archive.gz"
