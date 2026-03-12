"""Test S3 → SQS notification trigger for file processing.

Verifies that uploading a file to the processing bucket sends an
SQS notification, as a real file-processing pipeline would use.
"""

import json

from tests.apps.conftest import wait_for_messages


class TestS3Notification:
    """S3 bucket notification → SQS on file upload."""

    def test_upload_sends_sqs_notification(self, s3, sqs, unique_name):
        """Configure bucket notification, upload file, verify SQS message."""
        bucket_name = f"fp-notif-{unique_name}"
        queue_name = f"fp-queue-{unique_name}"

        s3.create_bucket(Bucket=bucket_name)
        resp = sqs.create_queue(QueueName=queue_name)
        queue_url = resp["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        # Configure S3 → SQS notification
        s3.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {
                        "QueueArn": queue_arn,
                        "Events": ["s3:ObjectCreated:*"],
                        "Filter": {
                            "Key": {
                                "FilterRules": [
                                    {"Name": "prefix", "Value": "incoming/"},
                                ]
                            }
                        },
                    }
                ]
            },
        )

        # Upload a file to the matching prefix
        s3.put_object(
            Bucket=bucket_name,
            Key="incoming/report.pdf",
            Body=b"fake-pdf-content",
        )

        messages = wait_for_messages(sqs, queue_url, timeout=10, expected=1)
        assert len(messages) >= 1, "No SQS notification received for S3 upload"

        body = json.loads(messages[0]["Body"])
        records = body.get("Records", [body])
        record = records[0] if records else body
        assert "s3" in str(record).lower()

        # Cleanup
        s3.delete_object(Bucket=bucket_name, Key="incoming/report.pdf")
        s3.delete_bucket(Bucket=bucket_name)
        sqs.delete_queue(QueueUrl=queue_url)
