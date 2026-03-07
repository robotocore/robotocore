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

    def test_batch_receive_multiple(self, sqs, queue_url):
        for i in range(5):
            sqs.send_message(QueueUrl=queue_url, MessageBody=f"msg {i}")
        response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=5)
        assert len(response.get("Messages", [])) >= 1

    def test_send_batch_unique_message_ids(self, sqs):
        """Batch send returns unique MessageIds for each entry."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"batch-ids-{uid}")["QueueUrl"]
        try:
            response = sqs.send_message_batch(
                QueueUrl=url,
                Entries=[
                    {"Id": "a", "MessageBody": "msg-a"},
                    {"Id": "b", "MessageBody": "msg-b"},
                    {"Id": "c", "MessageBody": "msg-c"},
                ],
            )
            assert len(response["Successful"]) == 3
            message_ids = {s["MessageId"] for s in response["Successful"]}
            assert len(message_ids) == 3  # All unique
            entry_ids = {s["Id"] for s in response["Successful"]}
            assert entry_ids == {"a", "b", "c"}
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_batch_receive_and_delete_all(self, sqs):
        """Send batch, receive all, delete all in batch."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"batch-all-{uid}")["QueueUrl"]
        try:
            sqs.send_message_batch(
                QueueUrl=url,
                Entries=[
                    {"Id": str(i), "MessageBody": f"item-{i}"} for i in range(5)
                ],
            )
            all_msgs = []
            for _ in range(5):
                recv = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)
                all_msgs.extend(recv.get("Messages", []))
                if len(all_msgs) >= 5:
                    break
            assert len(all_msgs) >= 3  # at least most messages received
            delete_entries = [
                {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]}
                for i, m in enumerate(all_msgs)
            ]
            resp = sqs.delete_message_batch(QueueUrl=url, Entries=delete_entries)
            assert len(resp["Successful"]) == len(delete_entries)
        finally:
            sqs.delete_queue(QueueUrl=url)


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

    def test_change_visibility_extends_timeout(self, sqs):
        """Change visibility to a longer timeout, verify message stays hidden."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"vis-extend-{uid}",
            Attributes={"VisibilityTimeout": "1"},
        )["QueueUrl"]
        try:
            sqs.send_message(QueueUrl=url, MessageBody="extend-test")
            recv = sqs.receive_message(QueueUrl=url)
            receipt = recv["Messages"][0]["ReceiptHandle"]
            # Extend visibility to 10 seconds
            sqs.change_message_visibility(
                QueueUrl=url, ReceiptHandle=receipt, VisibilityTimeout=10
            )
            # Wait past original 1s timeout
            time.sleep(2)
            # Message should still be hidden
            recv2 = sqs.receive_message(QueueUrl=url, WaitTimeSeconds=0)
            assert len(recv2.get("Messages", [])) == 0
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_receive_increments_approximate_receive_count(self, sqs):
        """ApproximateReceiveCount should increase with each receive."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"recv-count-{uid}",
            Attributes={"VisibilityTimeout": "0"},
        )["QueueUrl"]
        try:
            sqs.send_message(QueueUrl=url, MessageBody="count-me")
            # First receive
            recv1 = sqs.receive_message(
                QueueUrl=url, AttributeNames=["All"], WaitTimeSeconds=2
            )
            count1 = int(recv1["Messages"][0]["Attributes"]["ApproximateReceiveCount"])
            assert count1 == 1
            # Second receive (visibility=0 so immediately available)
            time.sleep(0.5)
            recv2 = sqs.receive_message(
                QueueUrl=url, AttributeNames=["All"], WaitTimeSeconds=2
            )
            count2 = int(recv2["Messages"][0]["Attributes"]["ApproximateReceiveCount"])
            assert count2 == 2
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

    def test_fifo_explicit_deduplication_id(self, sqs):
        """FIFO queue without content-based dedup uses explicit MessageDeduplicationId."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"fifo-explicit-{uid}.fifo",
            Attributes={"FifoQueue": "true"},
        )["QueueUrl"]
        try:
            sqs.send_message(
                QueueUrl=url,
                MessageBody="body-one",
                MessageGroupId="g1",
                MessageDeduplicationId="dedup-same",
            )
            sqs.send_message(
                QueueUrl=url,
                MessageBody="body-two-different",
                MessageGroupId="g1",
                MessageDeduplicationId="dedup-same",
            )
            recv = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)
            msgs = recv.get("Messages", [])
            assert len(msgs) == 1
            assert msgs[0]["Body"] == "body-one"
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_fifo_different_dedup_ids_both_delivered(self, sqs):
        """Messages with different dedup IDs should both be delivered."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"fifo-diff-dedup-{uid}.fifo",
            Attributes={"FifoQueue": "true"},
        )["QueueUrl"]
        try:
            sqs.send_message(
                QueueUrl=url,
                MessageBody="first",
                MessageGroupId="g1",
                MessageDeduplicationId="dedup-a",
            )
            sqs.send_message(
                QueueUrl=url,
                MessageBody="second",
                MessageGroupId="g1",
                MessageDeduplicationId="dedup-b",
            )
            msgs = []
            for _ in range(3):
                recv = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)
                for m in recv.get("Messages", []):
                    msgs.append(m)
                    sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
                if len(msgs) >= 2:
                    break
            assert len(msgs) == 2
            bodies = [m["Body"] for m in msgs]
            assert "first" in bodies
            assert "second" in bodies
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_fifo_multiple_groups_ordering(self, sqs):
        """Messages in different groups maintain per-group ordering."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"fifo-multi-grp-{uid}.fifo",
            Attributes={
                "FifoQueue": "true",
                "ContentBasedDeduplication": "true",
            },
        )["QueueUrl"]
        try:
            # Send interleaved messages to two groups
            for i in range(3):
                sqs.send_message(
                    QueueUrl=url,
                    MessageBody=f"groupA-{i}",
                    MessageGroupId="groupA",
                )
                sqs.send_message(
                    QueueUrl=url,
                    MessageBody=f"groupB-{i}",
                    MessageGroupId="groupB",
                )

            all_msgs = []
            for _ in range(10):
                recv = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)
                for m in recv.get("Messages", []):
                    all_msgs.append(m)
                    sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
                if len(all_msgs) >= 6:
                    break

            group_a = [m["Body"] for m in all_msgs if m["Body"].startswith("groupA")]
            group_b = [m["Body"] for m in all_msgs if m["Body"].startswith("groupB")]
            # Each group should be in order
            assert group_a == sorted(group_a)
            assert group_b == sorted(group_b)
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_fifo_send_message_returns_sequence_number(self, sqs):
        """SendMessage on FIFO should return SequenceNumber."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"fifo-seq-ret-{uid}.fifo",
            Attributes={
                "FifoQueue": "true",
                "ContentBasedDeduplication": "true",
            },
        )["QueueUrl"]
        try:
            resp = sqs.send_message(
                QueueUrl=url,
                MessageBody="seq-return-test",
                MessageGroupId="g1",
            )
            assert "SequenceNumber" in resp
            assert resp["SequenceNumber"].isdigit()
        finally:
            sqs.delete_queue(QueueUrl=url)


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

    def test_dlq_redrive_policy_readable(self, sqs):
        """RedrivePolicy should be readable from queue attributes."""
        uid = uuid.uuid4().hex[:8]
        dlq_url = sqs.create_queue(QueueName=f"dlq-read-{uid}")["QueueUrl"]
        dlq_arn = sqs.get_queue_attributes(
            QueueUrl=dlq_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        policy = json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "5"})
        src_url = sqs.create_queue(
            QueueName=f"src-read-{uid}",
            Attributes={"RedrivePolicy": policy},
        )["QueueUrl"]
        try:
            attrs = sqs.get_queue_attributes(
                QueueUrl=src_url, AttributeNames=["RedrivePolicy"]
            )["Attributes"]
            read_policy = json.loads(attrs["RedrivePolicy"])
            assert read_policy["deadLetterTargetArn"] == dlq_arn
            assert str(read_policy["maxReceiveCount"]) == "5"
        finally:
            sqs.delete_queue(QueueUrl=src_url)
            sqs.delete_queue(QueueUrl=dlq_url)

    def test_dlq_message_preserves_body(self, sqs):
        """Message body should be preserved when moved to DLQ."""
        uid = uuid.uuid4().hex[:8]
        dlq_url = sqs.create_queue(QueueName=f"dlq-body-{uid}")["QueueUrl"]
        dlq_arn = sqs.get_queue_attributes(
            QueueUrl=dlq_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        src_url = sqs.create_queue(
            QueueName=f"src-body-{uid}",
            Attributes={
                "RedrivePolicy": json.dumps(
                    {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "1"}
                ),
                "VisibilityTimeout": "1",
            },
        )["QueueUrl"]
        try:
            original_body = f"preserve-me-{uid}"
            sqs.send_message(QueueUrl=src_url, MessageBody=original_body)
            # Receive once (maxReceiveCount=1, so next receive triggers DLQ)
            sqs.receive_message(QueueUrl=src_url, WaitTimeSeconds=1)
            time.sleep(2)
            # Receive again to trigger DLQ move
            sqs.receive_message(QueueUrl=src_url, WaitTimeSeconds=1)
            time.sleep(2)
            # Check DLQ
            dlq_recv = sqs.receive_message(QueueUrl=dlq_url, WaitTimeSeconds=3)
            assert len(dlq_recv.get("Messages", [])) == 1
            assert dlq_recv["Messages"][0]["Body"] == original_body
        finally:
            sqs.delete_queue(QueueUrl=src_url)
            sqs.delete_queue(QueueUrl=dlq_url)


class TestSQSPurge:
    def test_purge_removes_all_messages(self, sqs):
        """Purge should remove all messages from the queue."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"purge-all-{uid}")["QueueUrl"]
        try:
            for i in range(5):
                sqs.send_message(QueueUrl=url, MessageBody=f"purge-{i}")
            sqs.purge_queue(QueueUrl=url)
            recv = sqs.receive_message(QueueUrl=url, WaitTimeSeconds=1, MaxNumberOfMessages=10)
            assert len(recv.get("Messages", [])) == 0
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_purge_empty_queue(self, sqs):
        """Purging an already-empty queue should not error."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"purge-empty-{uid}")["QueueUrl"]
        try:
            sqs.purge_queue(QueueUrl=url)  # Should not raise
        finally:
            sqs.delete_queue(QueueUrl=url)


class TestSQSTagging:
    def test_tag_queue(self, sqs):
        """Tag a queue and list tags."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"tag-q-{uid}")["QueueUrl"]
        try:
            sqs.tag_queue(QueueUrl=url, Tags={"env": "test", "project": "robotocore"})
            tags = sqs.list_queue_tags(QueueUrl=url).get("Tags", {})
            assert tags["env"] == "test"
            assert tags["project"] == "robotocore"
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_untag_queue(self, sqs):
        """Tag then untag a queue."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"untag-q-{uid}")["QueueUrl"]
        try:
            sqs.tag_queue(QueueUrl=url, Tags={"a": "1", "b": "2"})
            sqs.untag_queue(QueueUrl=url, TagKeys=["a"])
            tags = sqs.list_queue_tags(QueueUrl=url).get("Tags", {})
            assert "a" not in tags
            assert tags["b"] == "2"
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_tag_overwrite(self, sqs):
        """Tagging with same key overwrites the value."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"tag-over-{uid}")["QueueUrl"]
        try:
            sqs.tag_queue(QueueUrl=url, Tags={"env": "dev"})
            sqs.tag_queue(QueueUrl=url, Tags={"env": "prod"})
            tags = sqs.list_queue_tags(QueueUrl=url).get("Tags", {})
            assert tags["env"] == "prod"
        finally:
            sqs.delete_queue(QueueUrl=url)


class TestSQSLongPolling:
    def test_long_poll_returns_immediately_when_message_exists(self, sqs):
        """Long poll should return immediately if message is already available."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"longpoll-{uid}")["QueueUrl"]
        try:
            sqs.send_message(QueueUrl=url, MessageBody="pre-sent")
            start = time.time()
            recv = sqs.receive_message(QueueUrl=url, WaitTimeSeconds=5)
            elapsed = time.time() - start
            assert len(recv.get("Messages", [])) == 1
            assert recv["Messages"][0]["Body"] == "pre-sent"
            # Should return well before the 5s timeout
            assert elapsed < 4.0
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_long_poll_returns_empty_after_timeout(self, sqs):
        """Long poll on empty queue should wait and return empty."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"longpoll-empty-{uid}")["QueueUrl"]
        try:
            start = time.time()
            recv = sqs.receive_message(QueueUrl=url, WaitTimeSeconds=1)
            elapsed = time.time() - start
            assert len(recv.get("Messages", [])) == 0
            # Should have waited at least ~1s
            assert elapsed >= 0.5
        finally:
            sqs.delete_queue(QueueUrl=url)


class TestSQSMessageAttributeTypes:
    def test_string_attribute(self, sqs, queue_url):
        """String type message attribute."""
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="string-attr",
            MessageAttributes={
                "name": {"DataType": "String", "StringValue": "hello"},
            },
        )
        recv = sqs.receive_message(
            QueueUrl=queue_url, MessageAttributeNames=["All"]
        )
        attr = recv["Messages"][0]["MessageAttributes"]["name"]
        assert attr["DataType"] == "String"
        assert attr["StringValue"] == "hello"

    def test_number_attribute(self, sqs, queue_url):
        """Number type message attribute."""
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="number-attr",
            MessageAttributes={
                "count": {"DataType": "Number", "StringValue": "42"},
            },
        )
        recv = sqs.receive_message(
            QueueUrl=queue_url, MessageAttributeNames=["All"]
        )
        attr = recv["Messages"][0]["MessageAttributes"]["count"]
        assert attr["DataType"] == "Number"
        assert attr["StringValue"] == "42"

    def test_binary_attribute(self, sqs, queue_url):
        """Binary type message attribute."""
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="binary-attr",
            MessageAttributes={
                "data": {"DataType": "Binary", "BinaryValue": b"\x00\x01\x02"},
            },
        )
        recv = sqs.receive_message(
            QueueUrl=queue_url, MessageAttributeNames=["All"]
        )
        attr = recv["Messages"][0]["MessageAttributes"]["data"]
        assert attr["DataType"] == "Binary"
        assert attr["BinaryValue"] == b"\x00\x01\x02"

    def test_multiple_attribute_types(self, sqs, queue_url):
        """Mix of String, Number, and Binary attributes."""
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="multi-attr",
            MessageAttributes={
                "name": {"DataType": "String", "StringValue": "test"},
                "count": {"DataType": "Number", "StringValue": "99"},
                "blob": {"DataType": "Binary", "BinaryValue": b"\xff"},
            },
        )
        recv = sqs.receive_message(
            QueueUrl=queue_url, MessageAttributeNames=["All"]
        )
        attrs = recv["Messages"][0]["MessageAttributes"]
        assert attrs["name"]["StringValue"] == "test"
        assert attrs["count"]["StringValue"] == "99"
        assert attrs["blob"]["BinaryValue"] == b"\xff"

    def test_custom_string_attribute(self, sqs, queue_url):
        """Custom type like String.json."""
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="custom-attr",
            MessageAttributes={
                "payload": {
                    "DataType": "String.json",
                    "StringValue": '{"key": "value"}',
                },
            },
        )
        recv = sqs.receive_message(
            QueueUrl=queue_url, MessageAttributeNames=["All"]
        )
        attr = recv["Messages"][0]["MessageAttributes"]["payload"]
        assert attr["DataType"] == "String.json"
        assert json.loads(attr["StringValue"]) == {"key": "value"}


class TestSQSQueueAttributes:
    def test_visibility_timeout_attribute(self, sqs):
        """Create queue with VisibilityTimeout and read it back."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"vis-attr-{uid}",
            Attributes={"VisibilityTimeout": "45"},
        )["QueueUrl"]
        try:
            attrs = sqs.get_queue_attributes(
                QueueUrl=url, AttributeNames=["VisibilityTimeout"]
            )["Attributes"]
            assert attrs["VisibilityTimeout"] == "45"
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_message_retention_period(self, sqs):
        """Create queue with MessageRetentionPeriod and read it back."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"retention-{uid}",
            Attributes={"MessageRetentionPeriod": "86400"},
        )["QueueUrl"]
        try:
            attrs = sqs.get_queue_attributes(
                QueueUrl=url, AttributeNames=["MessageRetentionPeriod"]
            )["Attributes"]
            assert attrs["MessageRetentionPeriod"] == "86400"
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_delay_seconds(self, sqs):
        """Create queue with DelaySeconds and read it back."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(
            QueueName=f"delay-{uid}",
            Attributes={"DelaySeconds": "10"},
        )["QueueUrl"]
        try:
            attrs = sqs.get_queue_attributes(
                QueueUrl=url, AttributeNames=["DelaySeconds"]
            )["Attributes"]
            assert attrs["DelaySeconds"] == "10"
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_queue_arn_attribute(self, sqs):
        """QueueArn should be present in attributes."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"arn-attr-{uid}")["QueueUrl"]
        try:
            attrs = sqs.get_queue_attributes(
                QueueUrl=url, AttributeNames=["QueueArn"]
            )["Attributes"]
            assert "QueueArn" in attrs
            assert "arn:aws:sqs:" in attrs["QueueArn"]
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_approximate_message_counts(self, sqs):
        """ApproximateNumberOfMessages should reflect sent messages."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"approx-{uid}")["QueueUrl"]
        try:
            sqs.send_message(QueueUrl=url, MessageBody="count-me")
            # Give the system a moment to update counts
            time.sleep(0.5)
            attrs = sqs.get_queue_attributes(
                QueueUrl=url,
                AttributeNames=["ApproximateNumberOfMessages"],
            )["Attributes"]
            count = int(attrs["ApproximateNumberOfMessages"])
            assert count >= 1
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_list_queues_with_prefix(self, sqs):
        """list_queues with QueueNamePrefix filters correctly."""
        uid = uuid.uuid4().hex[:8]
        prefix = f"pfx-{uid}"
        url1 = sqs.create_queue(QueueName=f"{prefix}-alpha")["QueueUrl"]
        url2 = sqs.create_queue(QueueName=f"{prefix}-beta")["QueueUrl"]
        url3 = sqs.create_queue(QueueName=f"other-{uid}")["QueueUrl"]
        try:
            resp = sqs.list_queues(QueueNamePrefix=prefix)
            urls = resp.get("QueueUrls", [])
            assert len(urls) == 2
            assert all(prefix in u for u in urls)
        finally:
            sqs.delete_queue(QueueUrl=url1)
            sqs.delete_queue(QueueUrl=url2)
            sqs.delete_queue(QueueUrl=url3)

    def test_set_multiple_attributes(self, sqs):
        """Set multiple queue attributes at once."""
        uid = uuid.uuid4().hex[:8]
        url = sqs.create_queue(QueueName=f"multi-attr-{uid}")["QueueUrl"]
        try:
            sqs.set_queue_attributes(
                QueueUrl=url,
                Attributes={
                    "VisibilityTimeout": "30",
                    "MessageRetentionPeriod": "172800",
                    "DelaySeconds": "5",
                },
            )
            attrs = sqs.get_queue_attributes(
                QueueUrl=url, AttributeNames=["All"]
            )["Attributes"]
            assert attrs["VisibilityTimeout"] == "30"
            assert attrs["MessageRetentionPeriod"] == "172800"
            assert attrs["DelaySeconds"] == "5"
        finally:
            sqs.delete_queue(QueueUrl=url)
