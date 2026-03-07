"""SQS compatibility tests — verify robotocore matches LocalStack behavior."""

import json
import os
import time
import uuid

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

    @pytest.mark.xfail(reason="ChangeMessageVisibilityBatch not yet implemented")
    def test_change_message_visibility_batch(self, sqs, queue_url):
        # Send multiple messages
        sqs.send_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {"Id": "msg1", "MessageBody": "vis batch 1"},
                {"Id": "msg2", "MessageBody": "vis batch 2"},
            ],
        )
        recv = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
        messages = recv.get("Messages", [])
        assert len(messages) >= 1

        # Change visibility of all received messages
        entries = [
            {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"], "VisibilityTimeout": 0}
            for i, m in enumerate(messages)
        ]
        response = sqs.change_message_visibility_batch(QueueUrl=queue_url, Entries=entries)
        assert len(response["Successful"]) == len(entries)
        assert len(response.get("Failed", [])) == 0

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


class TestSQSPermissions:
    @pytest.mark.xfail(reason="AddPermission not yet implemented")
    def test_add_and_remove_permission(self, sqs, queue_url):
        sqs.add_permission(
            QueueUrl=queue_url,
            Label="test-permission",
            AWSAccountIds=["111111111111"],
            Actions=["SendMessage"],
        )
        # Verify the policy was added
        attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["Policy"])
        assert "Policy" in attrs["Attributes"]

        # Remove the permission
        sqs.remove_permission(QueueUrl=queue_url, Label="test-permission")


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


class TestSQSFifoAdvanced:
    """Advanced FIFO queue operations beyond basic ordering."""

    def test_fifo_message_deduplication_id(self, sqs):
        """MessageDeduplicationId prevents duplicate delivery."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"test-fifo-dedup-{uid}.fifo",
            Attributes={"FifoQueue": "true"},
        )["QueueUrl"]
        try:
            dedup_id = f"dedup-{uid}"
            sqs.send_message(
                QueueUrl=url,
                MessageBody="first",
                MessageGroupId="g1",
                MessageDeduplicationId=dedup_id,
            )
            sqs.send_message(
                QueueUrl=url,
                MessageBody="duplicate",
                MessageGroupId="g1",
                MessageDeduplicationId=dedup_id,
            )
            recv = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)
            assert len(recv.get("Messages", [])) == 1
            assert recv["Messages"][0]["Body"] == "first"
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_fifo_different_dedup_ids_both_delivered(self, sqs):
        """Different MessageDeduplicationIds are both delivered."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"test-fifo-diff-dedup-{uid}.fifo",
            Attributes={"FifoQueue": "true"},
        )["QueueUrl"]
        try:
            sqs.send_message(
                QueueUrl=url,
                MessageBody="msg-a",
                MessageGroupId="g1",
                MessageDeduplicationId="dedup-a",
            )
            sqs.send_message(
                QueueUrl=url,
                MessageBody="msg-b",
                MessageGroupId="g1",
                MessageDeduplicationId="dedup-b",
            )
            recv = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)
            assert len(recv.get("Messages", [])) == 2
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_fifo_multiple_groups_ordering(self, sqs):
        """Messages in different groups maintain per-group ordering."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"test-fifo-multigroup-{uid}.fifo",
            Attributes={
                "FifoQueue": "true",
                "ContentBasedDeduplication": "true",
            },
        )["QueueUrl"]
        try:
            for i in range(3):
                sqs.send_message(
                    QueueUrl=url,
                    MessageBody=f"groupA-{i}",
                    MessageGroupId="groupA",
                )
            for i in range(3):
                sqs.send_message(
                    QueueUrl=url,
                    MessageBody=f"groupB-{i}",
                    MessageGroupId="groupB",
                )
            messages = []
            for _ in range(10):
                recv = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)
                for m in recv.get("Messages", []):
                    messages.append(m["Body"])
                    sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
                if len(messages) >= 6:
                    break
            group_a = [m for m in messages if m.startswith("groupA")]
            group_b = [m for m in messages if m.startswith("groupB")]
            assert group_a == ["groupA-0", "groupA-1", "groupA-2"]
            assert group_b == ["groupB-0", "groupB-1", "groupB-2"]
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_fifo_content_based_dedup_attribute(self, sqs):
        """ContentBasedDeduplication attribute is queryable."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"test-fifo-cbd-{uid}.fifo",
            Attributes={
                "FifoQueue": "true",
                "ContentBasedDeduplication": "true",
            },
        )["QueueUrl"]
        try:
            attrs = sqs.get_queue_attributes(
                QueueUrl=url, AttributeNames=["ContentBasedDeduplication"]
            )
            assert attrs["Attributes"]["ContentBasedDeduplication"] == "true"
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_fifo_send_message_batch(self, sqs):
        """SendMessageBatch works with FIFO queues."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"test-fifo-batch-{uid}.fifo",
            Attributes={
                "FifoQueue": "true",
                "ContentBasedDeduplication": "true",
            },
        )["QueueUrl"]
        try:
            resp = sqs.send_message_batch(
                QueueUrl=url,
                Entries=[
                    {"Id": "1", "MessageBody": "fifo-batch-1", "MessageGroupId": "g1"},
                    {"Id": "2", "MessageBody": "fifo-batch-2", "MessageGroupId": "g1"},
                    {"Id": "3", "MessageBody": "fifo-batch-3", "MessageGroupId": "g1"},
                ],
            )
            assert len(resp["Successful"]) == 3
            for entry in resp["Successful"]:
                assert "MessageId" in entry
                assert "MD5OfMessageBody" in entry
        finally:
            sqs.delete_queue(QueueUrl=url)


class TestSQSDLQAdvanced:
    """Advanced dead-letter queue tests."""

    def test_dlq_redrive_policy_attribute(self, sqs):
        """RedrivePolicy can be read back from queue attributes."""
        uid = uuid.uuid4().hex[:8]
        dlq_url = sqs.create_queue(QueueName=f"test-dlq-attr-dlq-{uid}")["QueueUrl"]
        dlq_arn = sqs.get_queue_attributes(
            QueueUrl=dlq_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        policy = json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "3"})
        source_url = sqs.create_queue(
            QueueName=f"test-dlq-attr-src-{uid}",
            Attributes={"RedrivePolicy": policy},
        )["QueueUrl"]
        try:
            attrs = sqs.get_queue_attributes(
                QueueUrl=source_url, AttributeNames=["RedrivePolicy"]
            )
            returned_policy = json.loads(attrs["Attributes"]["RedrivePolicy"])
            assert returned_policy["deadLetterTargetArn"] == dlq_arn
            assert str(returned_policy["maxReceiveCount"]) == "3"
        finally:
            sqs.delete_queue(QueueUrl=source_url)
            sqs.delete_queue(QueueUrl=dlq_url)

    def test_dlq_message_body_preserved(self, sqs):
        """Message body and attributes are preserved when moved to DLQ."""
        uid = uuid.uuid4().hex[:8]
        dlq_url = sqs.create_queue(QueueName=f"test-dlq-body-dlq-{uid}")["QueueUrl"]
        dlq_arn = sqs.get_queue_attributes(
            QueueUrl=dlq_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        source_url = sqs.create_queue(
            QueueName=f"test-dlq-body-src-{uid}",
            Attributes={
                "RedrivePolicy": json.dumps(
                    {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "1"}
                ),
                "VisibilityTimeout": "1",
            },
        )["QueueUrl"]
        try:
            sqs.send_message(QueueUrl=source_url, MessageBody="dlq-preserve-test")
            # Receive once (maxReceiveCount=1), then let it expire
            sqs.receive_message(QueueUrl=source_url, WaitTimeSeconds=1)
            time.sleep(3)
            # Receive again to trigger DLQ move
            sqs.receive_message(QueueUrl=source_url, WaitTimeSeconds=1)
            time.sleep(2)
            dlq_recv = sqs.receive_message(QueueUrl=dlq_url, WaitTimeSeconds=3)
            assert len(dlq_recv.get("Messages", [])) == 1
            assert dlq_recv["Messages"][0]["Body"] == "dlq-preserve-test"
        finally:
            sqs.delete_queue(QueueUrl=source_url)
            sqs.delete_queue(QueueUrl=dlq_url)


class TestSQSMessageAttributes:
    """Message attribute types and retrieval."""

    def test_string_attribute(self, sqs, queue_url):
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="str-attr",
            MessageAttributes={
                "Name": {"DataType": "String", "StringValue": "Alice"},
            },
        )
        recv = sqs.receive_message(
            QueueUrl=queue_url, MessageAttributeNames=["All"]
        )
        attr = recv["Messages"][0]["MessageAttributes"]["Name"]
        assert attr["StringValue"] == "Alice"
        assert attr["DataType"] == "String"

    def test_number_attribute(self, sqs, queue_url):
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="num-attr",
            MessageAttributes={
                "Count": {"DataType": "Number", "StringValue": "42"},
            },
        )
        recv = sqs.receive_message(
            QueueUrl=queue_url, MessageAttributeNames=["All"]
        )
        attr = recv["Messages"][0]["MessageAttributes"]["Count"]
        assert attr["StringValue"] == "42"
        assert attr["DataType"] == "Number"

    def test_binary_attribute(self, sqs, queue_url):
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="bin-attr",
            MessageAttributes={
                "Data": {"DataType": "Binary", "BinaryValue": b"\x00\x01\x02\x03"},
            },
        )
        recv = sqs.receive_message(
            QueueUrl=queue_url, MessageAttributeNames=["All"]
        )
        attr = recv["Messages"][0]["MessageAttributes"]["Data"]
        assert attr["BinaryValue"] == b"\x00\x01\x02\x03"
        assert attr["DataType"] == "Binary"

    def test_multiple_attributes(self, sqs, queue_url):
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="multi-attr",
            MessageAttributes={
                "Color": {"DataType": "String", "StringValue": "red"},
                "Size": {"DataType": "Number", "StringValue": "10"},
                "Blob": {"DataType": "Binary", "BinaryValue": b"\xff"},
            },
        )
        recv = sqs.receive_message(
            QueueUrl=queue_url, MessageAttributeNames=["All"]
        )
        attrs = recv["Messages"][0]["MessageAttributes"]
        assert attrs["Color"]["StringValue"] == "red"
        assert attrs["Size"]["StringValue"] == "10"
        assert attrs["Blob"]["BinaryValue"] == b"\xff"

    def test_all_attributes_with_all_request(self, sqs, queue_url):
        """Requesting 'All' returns all message attributes."""
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="all-attr-req",
            MessageAttributes={
                "X": {"DataType": "String", "StringValue": "val-x"},
                "Y": {"DataType": "String", "StringValue": "val-y"},
            },
        )
        recv = sqs.receive_message(
            QueueUrl=queue_url, MessageAttributeNames=["All"]
        )
        attrs = recv["Messages"][0].get("MessageAttributes", {})
        assert "X" in attrs
        assert "Y" in attrs

    def test_custom_data_type(self, sqs, queue_url):
        """Custom data types like String.email are supported."""
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="custom-type",
            MessageAttributes={
                "Email": {"DataType": "String.email", "StringValue": "a@b.com"},
            },
        )
        recv = sqs.receive_message(
            QueueUrl=queue_url, MessageAttributeNames=["All"]
        )
        attr = recv["Messages"][0]["MessageAttributes"]["Email"]
        assert attr["StringValue"] == "a@b.com"
        assert attr["DataType"] == "String.email"


class TestSQSBatchAdvanced:
    """Advanced batch operations."""

    def test_send_batch_with_attributes(self, sqs, queue_url):
        """SendMessageBatch entries can have different attributes."""
        resp = sqs.send_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {
                    "Id": "m1",
                    "MessageBody": "batch-a",
                    "MessageAttributes": {
                        "Key": {"DataType": "String", "StringValue": "val1"},
                    },
                },
                {
                    "Id": "m2",
                    "MessageBody": "batch-b",
                    "MessageAttributes": {
                        "Key": {"DataType": "String", "StringValue": "val2"},
                    },
                },
            ],
        )
        assert len(resp["Successful"]) == 2

    def test_send_batch_with_delay(self, sqs, queue_url):
        """SendMessageBatch entries can have per-message DelaySeconds."""
        resp = sqs.send_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {"Id": "m1", "MessageBody": "no-delay", "DelaySeconds": 0},
                {"Id": "m2", "MessageBody": "delayed", "DelaySeconds": 1},
            ],
        )
        assert len(resp["Successful"]) == 2
        # The non-delayed message should be immediately available
        recv = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=0)
        assert len(recv.get("Messages", [])) >= 1

    def test_delete_batch_returns_ids(self, sqs, queue_url):
        """DeleteMessageBatch returns the Ids of successfully deleted messages."""
        sqs.send_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {"Id": "a", "MessageBody": "del-batch-1"},
                {"Id": "b", "MessageBody": "del-batch-2"},
            ],
        )
        recv = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
        entries = [
            {"Id": f"del-{i}", "ReceiptHandle": m["ReceiptHandle"]}
            for i, m in enumerate(recv["Messages"])
        ]
        resp = sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
        returned_ids = {e["Id"] for e in resp["Successful"]}
        expected_ids = {e["Id"] for e in entries}
        assert returned_ids == expected_ids


class TestSQSQueueAttributes:
    """Get and set standard queue attributes."""

    def test_set_and_get_delay_seconds(self, sqs):
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"test-delay-{uid}",
            Attributes={"DelaySeconds": "5"},
        )["QueueUrl"]
        try:
            attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["DelaySeconds"])
            assert attrs["Attributes"]["DelaySeconds"] == "5"
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_set_and_get_maximum_message_size(self, sqs):
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"test-maxsize-{uid}",
            Attributes={"MaximumMessageSize": "1024"},
        )["QueueUrl"]
        try:
            attrs = sqs.get_queue_attributes(
                QueueUrl=url, AttributeNames=["MaximumMessageSize"]
            )
            assert attrs["Attributes"]["MaximumMessageSize"] == "1024"
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_set_and_get_message_retention_period(self, sqs):
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"test-retention-{uid}",
            Attributes={"MessageRetentionPeriod": "3600"},
        )["QueueUrl"]
        try:
            attrs = sqs.get_queue_attributes(
                QueueUrl=url, AttributeNames=["MessageRetentionPeriod"]
            )
            assert attrs["Attributes"]["MessageRetentionPeriod"] == "3600"
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_set_and_get_receive_wait_time(self, sqs):
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"test-wait-{uid}",
            Attributes={"ReceiveMessageWaitTimeSeconds": "5"},
        )["QueueUrl"]
        try:
            attrs = sqs.get_queue_attributes(
                QueueUrl=url, AttributeNames=["ReceiveMessageWaitTimeSeconds"]
            )
            assert attrs["Attributes"]["ReceiveMessageWaitTimeSeconds"] == "5"
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_update_visibility_timeout(self, sqs):
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"test-update-vis-{uid}")["QueueUrl"]
        try:
            sqs.set_queue_attributes(
                QueueUrl=url, Attributes={"VisibilityTimeout": "120"}
            )
            attrs = sqs.get_queue_attributes(
                QueueUrl=url, AttributeNames=["VisibilityTimeout"]
            )
            assert attrs["Attributes"]["VisibilityTimeout"] == "120"
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_queue_arn_format(self, sqs):
        uid = uuid.uuid4().hex[:8]
        name = f"test-arn-{uid}"
        url = sqs.create_queue(QueueName=name)["QueueUrl"]
        try:
            attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])
            arn = attrs["Attributes"]["QueueArn"]
            assert arn.startswith("arn:aws:sqs:")
            assert arn.endswith(f":{name}")
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_get_all_attributes(self, sqs):
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"test-all-attrs-{uid}",
            Attributes={
                "VisibilityTimeout": "45",
                "DelaySeconds": "10",
                "MaximumMessageSize": "2048",
            },
        )["QueueUrl"]
        try:
            attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
            assert attrs["VisibilityTimeout"] == "45"
            assert attrs["DelaySeconds"] == "10"
            assert attrs["MaximumMessageSize"] == "2048"
            # These should always be present
            assert "QueueArn" in attrs
            assert "CreatedTimestamp" in attrs
        finally:
            sqs.delete_queue(QueueUrl=url)


class TestSQSPurgeQueue:
    """Purge queue operations."""

    def test_purge_removes_all_messages(self, sqs):
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"test-purge-all-{uid}")["QueueUrl"]
        try:
            for i in range(5):
                sqs.send_message(QueueUrl=url, MessageBody=f"purge-{i}")
            sqs.purge_queue(QueueUrl=url)
            recv = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
            assert len(recv.get("Messages", [])) == 0
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_purge_empty_queue(self, sqs):
        """Purging an empty queue does not error."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"test-purge-empty-{uid}")["QueueUrl"]
        try:
            sqs.purge_queue(QueueUrl=url)  # Should not raise
        finally:
            sqs.delete_queue(QueueUrl=url)


class TestSQSChangeVisibility:
    """ChangeMessageVisibility and batch variant."""

    def test_change_visibility_to_zero(self, sqs):
        """Setting visibility to 0 makes the message immediately available."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"test-chg-vis-{uid}",
            Attributes={"VisibilityTimeout": "30"},
        )["QueueUrl"]
        try:
            sqs.send_message(QueueUrl=url, MessageBody="vis-zero")
            recv = sqs.receive_message(QueueUrl=url)
            receipt = recv["Messages"][0]["ReceiptHandle"]
            sqs.change_message_visibility(
                QueueUrl=url, ReceiptHandle=receipt, VisibilityTimeout=0
            )
            recv2 = sqs.receive_message(QueueUrl=url, WaitTimeSeconds=1)
            assert len(recv2.get("Messages", [])) == 1
            assert recv2["Messages"][0]["Body"] == "vis-zero"
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_change_visibility_multiple_messages(self, sqs):
        """ChangeMessageVisibility on multiple messages individually."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"test-chg-vis-multi-{uid}",
            Attributes={"VisibilityTimeout": "30"},
        )["QueueUrl"]
        try:
            sqs.send_message(QueueUrl=url, MessageBody="multi-vis-1")
            sqs.send_message(QueueUrl=url, MessageBody="multi-vis-2")
            recv = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)
            for m in recv["Messages"]:
                sqs.change_message_visibility(
                    QueueUrl=url, ReceiptHandle=m["ReceiptHandle"], VisibilityTimeout=0
                )
            recv2 = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
            assert len(recv2.get("Messages", [])) >= 1
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_change_visibility_extends_timeout(self, sqs):
        """Extending visibility keeps message hidden."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"test-chg-vis-ext-{uid}",
            Attributes={"VisibilityTimeout": "2"},
        )["QueueUrl"]
        try:
            sqs.send_message(QueueUrl=url, MessageBody="extend-vis")
            recv = sqs.receive_message(QueueUrl=url)
            receipt = recv["Messages"][0]["ReceiptHandle"]
            # Extend to 30 seconds
            sqs.change_message_visibility(
                QueueUrl=url, ReceiptHandle=receipt, VisibilityTimeout=30
            )
            # Should not be visible after original 2s timeout
            time.sleep(3)
            recv2 = sqs.receive_message(QueueUrl=url, WaitTimeSeconds=0)
            assert len(recv2.get("Messages", [])) == 0
        finally:
            sqs.delete_queue(QueueUrl=url)


class TestSQSLongPolling:
    """Long polling behavior."""

    def test_long_poll_no_messages_returns_quickly(self, sqs):
        """WaitTimeSeconds=1 on empty queue returns empty within ~1s."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"test-longpoll-{uid}")["QueueUrl"]
        try:
            start = time.time()
            recv = sqs.receive_message(QueueUrl=url, WaitTimeSeconds=1)
            elapsed = time.time() - start
            assert len(recv.get("Messages", [])) == 0
            # Should take roughly 1s, not more than 5s
            assert elapsed < 5
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_long_poll_returns_immediately_with_messages(self, sqs):
        """Long poll returns immediately when messages exist."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"test-longpoll-msgs-{uid}")["QueueUrl"]
        try:
            sqs.send_message(QueueUrl=url, MessageBody="ready")
            start = time.time()
            recv = sqs.receive_message(QueueUrl=url, WaitTimeSeconds=5)
            elapsed = time.time() - start
            assert len(recv.get("Messages", [])) == 1
            assert elapsed < 3
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_queue_level_wait_time(self, sqs):
        """ReceiveMessageWaitTimeSeconds on queue is respected."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"test-queue-wait-{uid}",
            Attributes={"ReceiveMessageWaitTimeSeconds": "1"},
        )["QueueUrl"]
        try:
            start = time.time()
            recv = sqs.receive_message(QueueUrl=url)
            elapsed = time.time() - start
            assert len(recv.get("Messages", [])) == 0
            assert elapsed < 5
        finally:
            sqs.delete_queue(QueueUrl=url)


class TestSQSQueueTags:
    """Queue tagging operations."""

    def test_tag_queue(self, sqs):
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"test-tag-{uid}")["QueueUrl"]
        try:
            sqs.tag_queue(QueueUrl=url, Tags={"env": "test", "team": "backend"})
            resp = sqs.list_queue_tags(QueueUrl=url)
            tags = resp.get("Tags", {})
            assert tags["env"] == "test"
            assert tags["team"] == "backend"
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_untag_queue(self, sqs):
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"test-untag-{uid}")["QueueUrl"]
        try:
            sqs.tag_queue(QueueUrl=url, Tags={"a": "1", "b": "2", "c": "3"})
            sqs.untag_queue(QueueUrl=url, TagKeys=["b"])
            resp = sqs.list_queue_tags(QueueUrl=url)
            tags = resp.get("Tags", {})
            assert "a" in tags
            assert "b" not in tags
            assert "c" in tags
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_list_queue_tags_empty(self, sqs):
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"test-notags-{uid}")["QueueUrl"]
        try:
            resp = sqs.list_queue_tags(QueueUrl=url)
            assert resp.get("Tags") is None or resp.get("Tags") == {}
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_overwrite_tags(self, sqs):
        """Tagging with same key overwrites the value."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"test-overwrite-tag-{uid}")["QueueUrl"]
        try:
            sqs.tag_queue(QueueUrl=url, Tags={"key": "old"})
            sqs.tag_queue(QueueUrl=url, Tags={"key": "new"})
            resp = sqs.list_queue_tags(QueueUrl=url)
            assert resp["Tags"]["key"] == "new"
        finally:
            sqs.delete_queue(QueueUrl=url)


class TestSQSSystemAttributes:
    """Message system attributes (SentTimestamp, ApproximateReceiveCount, etc.)."""

    def test_sent_timestamp(self, sqs, queue_url):
        before = int(time.time() * 1000)
        sqs.send_message(QueueUrl=queue_url, MessageBody="ts-test")
        after = int(time.time() * 1000)
        recv = sqs.receive_message(QueueUrl=queue_url, AttributeNames=["SentTimestamp"])
        ts = int(recv["Messages"][0]["Attributes"]["SentTimestamp"])
        # Timestamp should be within a reasonable window
        assert before - 2000 <= ts <= after + 2000

    def test_approximate_receive_count(self, sqs):
        """ApproximateReceiveCount increments on each receive."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"test-recv-count-{uid}",
            Attributes={"VisibilityTimeout": "1"},
        )["QueueUrl"]
        try:
            sqs.send_message(QueueUrl=url, MessageBody="count-me")
            # First receive
            recv1 = sqs.receive_message(QueueUrl=url, AttributeNames=["ApproximateReceiveCount"])
            count1 = int(recv1["Messages"][0]["Attributes"]["ApproximateReceiveCount"])
            assert count1 == 1
            # Wait for visibility timeout
            time.sleep(2)
            # Second receive
            recv2 = sqs.receive_message(
                QueueUrl=url,
                AttributeNames=["ApproximateReceiveCount"],
                WaitTimeSeconds=2,
            )
            count2 = int(recv2["Messages"][0]["Attributes"]["ApproximateReceiveCount"])
            assert count2 == 2
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_approximate_first_receive_timestamp(self, sqs, queue_url):
        sqs.send_message(QueueUrl=queue_url, MessageBody="first-recv-ts")
        before = int(time.time() * 1000)
        recv = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=["ApproximateFirstReceiveTimestamp"],
        )
        after = int(time.time() * 1000)
        ts = int(recv["Messages"][0]["Attributes"]["ApproximateFirstReceiveTimestamp"])
        assert before - 2000 <= ts <= after + 2000

    def test_sender_id_present(self, sqs, queue_url):
        sqs.send_message(QueueUrl=queue_url, MessageBody="sender-test")
        recv = sqs.receive_message(QueueUrl=queue_url, AttributeNames=["SenderId"])
        assert "SenderId" in recv["Messages"][0]["Attributes"]

    def test_md5_of_body(self, sqs, queue_url):
        """MD5OfBody is returned on send and receive."""
        import hashlib

        body = "md5-test-body"
        send_resp = sqs.send_message(QueueUrl=queue_url, MessageBody=body)
        expected_md5 = hashlib.md5(body.encode()).hexdigest()
        assert send_resp["MD5OfMessageBody"] == expected_md5
        recv = sqs.receive_message(QueueUrl=queue_url)
        assert recv["Messages"][0]["MD5OfBody"] == expected_md5
    @pytest.mark.xfail(reason="ListDeadLetterSourceQueues not yet implemented")
    def test_list_dead_letter_source_queues(self, sqs):
        import json

        # Create DLQ
        dlq_url = sqs.create_queue(QueueName="test-list-dlq-target")["QueueUrl"]
        dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        # Create two source queues pointing to this DLQ
        source1_url = sqs.create_queue(
            QueueName="test-list-dlq-source1",
            Attributes={
                "RedrivePolicy": json.dumps(
                    {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "3"}
                )
            },
        )["QueueUrl"]
        source2_url = sqs.create_queue(
            QueueName="test-list-dlq-source2",
            Attributes={
                "RedrivePolicy": json.dumps(
                    {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "3"}
                )
            },
        )["QueueUrl"]

        try:
            response = sqs.list_dead_letter_source_queues(QueueUrl=dlq_url)
            source_urls = response.get("queueUrls", [])
            assert len(source_urls) == 2
            assert source1_url in source_urls
            assert source2_url in source_urls
        finally:
            sqs.delete_queue(QueueUrl=source1_url)
            sqs.delete_queue(QueueUrl=source2_url)
            sqs.delete_queue(QueueUrl=dlq_url)
