"""SQS compatibility tests — verify robotocore matches LocalStack behavior."""

import os
import time

import boto3
import pytest

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")


@pytest.fixture
def sqs():
    return boto3.client(
        "sqs",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture
def queue_url(sqs):
    response = sqs.create_queue(QueueName="test-compat-queue")
    url = response["QueueUrl"]
    yield url
    sqs.delete_queue(QueueUrl=url)


@pytest.fixture
def fifo_queue_url(sqs):
    response = sqs.create_queue(
        QueueName="test-compat-fifo.fifo",
        Attributes={
            "FifoQueue": "true",
            "ContentBasedDeduplication": "true",
        },
    )
    url = response["QueueUrl"]
    yield url
    sqs.delete_queue(QueueUrl=url)


class TestSQSBasicOperations:
    def test_create_queue(self, sqs):
        response = sqs.create_queue(QueueName="test-create-queue")
        assert "QueueUrl" in response
        sqs.delete_queue(QueueUrl=response["QueueUrl"])

    def test_send_and_receive_message(self, sqs, queue_url):
        sqs.send_message(QueueUrl=queue_url, MessageBody="hello")
        response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
        messages = response.get("Messages", [])
        assert len(messages) == 1
        assert messages[0]["Body"] == "hello"

    def test_delete_message(self, sqs, queue_url):
        sqs.send_message(QueueUrl=queue_url, MessageBody="delete me")
        response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
        receipt = response["Messages"][0]["ReceiptHandle"]
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)

    def test_get_queue_attributes(self, sqs, queue_url):
        response = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])
        assert "Attributes" in response

    def test_list_queues(self, sqs, queue_url):
        response = sqs.list_queues()
        assert any("test-compat-queue" in url for url in response.get("QueueUrls", []))

    def test_get_queue_url(self, sqs, queue_url):
        response = sqs.get_queue_url(QueueName="test-compat-queue")
        assert "test-compat-queue" in response["QueueUrl"]

    def test_purge_queue(self, sqs, queue_url):
        sqs.send_message(QueueUrl=queue_url, MessageBody="purge me")
        sqs.purge_queue(QueueUrl=queue_url)
        response = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=0)
        assert len(response.get("Messages", [])) == 0

    def test_set_queue_attributes(self, sqs, queue_url):
        sqs.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes={"VisibilityTimeout": "60"},
        )
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["VisibilityTimeout"]
        )
        assert response["Attributes"]["VisibilityTimeout"] == "60"

    def test_message_attributes(self, sqs, queue_url):
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="with attrs",
            MessageAttributes={
                "color": {"DataType": "String", "StringValue": "blue"},
            },
        )
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MessageAttributeNames=["All"],
        )
        msg = response["Messages"][0]
        assert msg["MessageAttributes"]["color"]["StringValue"] == "blue"

    def test_receive_message_attributes(self, sqs, queue_url):
        sqs.send_message(QueueUrl=queue_url, MessageBody="attrs test")
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=["All"],
        )
        msg = response["Messages"][0]
        attrs = msg.get("Attributes", {})
        assert "ApproximateReceiveCount" in attrs
        assert "SentTimestamp" in attrs


class TestSQSBatchOperations:
    def test_send_message_batch(self, sqs, queue_url):
        response = sqs.send_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {"Id": "msg1", "MessageBody": "batch message 1"},
                {"Id": "msg2", "MessageBody": "batch message 2"},
                {"Id": "msg3", "MessageBody": "batch message 3"},
            ],
        )
        assert len(response["Successful"]) == 3
        assert len(response.get("Failed", [])) == 0

    def test_delete_message_batch(self, sqs, queue_url):
        sqs.send_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {"Id": "msg1", "MessageBody": "batch del 1"},
                {"Id": "msg2", "MessageBody": "batch del 2"},
            ],
        )
        recv = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
        entries = [
            {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]}
            for i, m in enumerate(recv["Messages"])
        ]
        response = sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
        assert len(response["Successful"]) == len(entries)

    def test_batch_receive_multiple(self, sqs, queue_url):
        for i in range(5):
            sqs.send_message(QueueUrl=queue_url, MessageBody=f"msg {i}")
        response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=5)
        assert len(response.get("Messages", [])) >= 1


class TestSQSVisibilityTimeout:
    def test_change_message_visibility(self, sqs, queue_url):
        sqs.send_message(QueueUrl=queue_url, MessageBody="vis test")
        recv = sqs.receive_message(QueueUrl=queue_url)
        receipt = recv["Messages"][0]["ReceiptHandle"]
        # Set visibility to 0 to make it immediately available again
        sqs.change_message_visibility(
            QueueUrl=queue_url, ReceiptHandle=receipt, VisibilityTimeout=0
        )
        # Should be receivable again
        recv2 = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=1)
        assert len(recv2.get("Messages", [])) == 1

    def test_message_not_visible_during_timeout(self, sqs):
        url = sqs.create_queue(
            QueueName="test-vis-timeout",
            Attributes={"VisibilityTimeout": "2"},
        )["QueueUrl"]
        try:
            sqs.send_message(QueueUrl=url, MessageBody="hidden")
            # First receive makes it invisible
            recv1 = sqs.receive_message(QueueUrl=url)
            assert len(recv1["Messages"]) == 1
            # Immediately try again — should get nothing
            recv2 = sqs.receive_message(QueueUrl=url, WaitTimeSeconds=0)
            assert len(recv2.get("Messages", [])) == 0
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_message_returns_after_visibility_timeout(self, sqs):
        url = sqs.create_queue(
            QueueName="test-vis-return",
            Attributes={"VisibilityTimeout": "1"},
        )["QueueUrl"]
        try:
            sqs.send_message(QueueUrl=url, MessageBody="comeback")
            recv1 = sqs.receive_message(QueueUrl=url)
            assert len(recv1["Messages"]) == 1
            # Wait for visibility timeout + background worker cycle
            time.sleep(3)
            recv2 = sqs.receive_message(QueueUrl=url, WaitTimeSeconds=1)
            assert len(recv2.get("Messages", [])) == 1
            assert recv2["Messages"][0]["Body"] == "comeback"
        finally:
            sqs.delete_queue(QueueUrl=url)


class TestSQSFIFO:
    def test_fifo_queue_creation(self, sqs, fifo_queue_url):
        attrs = sqs.get_queue_attributes(QueueUrl=fifo_queue_url, AttributeNames=["All"])
        assert attrs["Attributes"]["FifoQueue"] == "true"

    def test_fifo_message_ordering(self, sqs, fifo_queue_url):
        for i in range(5):
            sqs.send_message(
                QueueUrl=fifo_queue_url,
                MessageBody=f"fifo msg {i}",
                MessageGroupId="group1",
            )
        messages = []
        for _ in range(5):
            recv = sqs.receive_message(QueueUrl=fifo_queue_url)
            if recv.get("Messages"):
                msg = recv["Messages"][0]
                messages.append(msg["Body"])
                sqs.delete_message(
                    QueueUrl=fifo_queue_url,
                    ReceiptHandle=msg["ReceiptHandle"],
                )
        assert messages == [f"fifo msg {i}" for i in range(len(messages))]

    def test_fifo_deduplication(self, sqs, fifo_queue_url):
        # Content-based dedup is enabled on the fixture
        sqs.send_message(
            QueueUrl=fifo_queue_url,
            MessageBody="same content",
            MessageGroupId="group1",
        )
        sqs.send_message(
            QueueUrl=fifo_queue_url,
            MessageBody="same content",
            MessageGroupId="group1",
        )
        recv = sqs.receive_message(QueueUrl=fifo_queue_url, MaxNumberOfMessages=10)
        # Should only get one message due to dedup
        assert len(recv.get("Messages", [])) == 1

    def test_fifo_sequence_number(self, sqs, fifo_queue_url):
        sqs.send_message(
            QueueUrl=fifo_queue_url,
            MessageBody="seq test",
            MessageGroupId="group1",
        )
        recv = sqs.receive_message(QueueUrl=fifo_queue_url, AttributeNames=["All"])
        assert "SequenceNumber" in recv["Messages"][0]["Attributes"]

    def test_fifo_message_group_id(self, sqs, fifo_queue_url):
        sqs.send_message(
            QueueUrl=fifo_queue_url,
            MessageBody="group test",
            MessageGroupId="mygroup",
        )
        recv = sqs.receive_message(QueueUrl=fifo_queue_url, AttributeNames=["All"])
        assert recv["Messages"][0]["Attributes"]["MessageGroupId"] == "mygroup"


class TestSQSDeadLetterQueue:
    def test_dlq_redrive(self, sqs):
        # Create DLQ
        dlq_url = sqs.create_queue(QueueName="test-dlq")["QueueUrl"]
        dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        # Create source queue with redrive policy
        import json

        source_url = sqs.create_queue(
            QueueName="test-dlq-source",
            Attributes={
                "RedrivePolicy": json.dumps(
                    {
                        "deadLetterTargetArn": dlq_arn,
                        "maxReceiveCount": "2",
                    }
                ),
                "VisibilityTimeout": "1",
            },
        )["QueueUrl"]

        try:
            sqs.send_message(QueueUrl=source_url, MessageBody="will fail")

            # Receive twice (exceeding maxReceiveCount)
            for _ in range(3):
                recv = sqs.receive_message(QueueUrl=source_url, WaitTimeSeconds=2)
                if recv.get("Messages"):
                    pass  # Just receive, don't delete
                time.sleep(2)  # Wait for visibility timeout

            # Check DLQ has the message
            dlq_recv = sqs.receive_message(QueueUrl=dlq_url, WaitTimeSeconds=3)
            assert len(dlq_recv.get("Messages", [])) == 1
            assert dlq_recv["Messages"][0]["Body"] == "will fail"
        finally:
            sqs.delete_queue(QueueUrl=source_url)
            sqs.delete_queue(QueueUrl=dlq_url)
