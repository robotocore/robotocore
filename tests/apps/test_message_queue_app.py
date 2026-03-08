"""
Message Queue Application Tests

Simulates an order processing system that uses SQS for reliable
message delivery, FIFO ordering, dead-letter queues, and batch operations.
"""

import json
import time

import pytest


@pytest.fixture
def order_queue(sqs, unique_name):
    queue_name = f"order-processing-{unique_name}"
    response = sqs.create_queue(
        QueueName=queue_name,
        Attributes={"VisibilityTimeout": "5"},
    )
    url = response["QueueUrl"]
    yield url
    sqs.delete_queue(QueueUrl=url)


@pytest.fixture
def fifo_queue(sqs, unique_name):
    queue_name = f"order-sequencing-{unique_name}.fifo"
    response = sqs.create_queue(
        QueueName=queue_name,
        Attributes={
            "FifoQueue": "true",
            "ContentBasedDeduplication": "true",
        },
    )
    url = response["QueueUrl"]
    yield url
    sqs.delete_queue(QueueUrl=url)


@pytest.fixture
def dlq_pair(sqs, unique_name):
    """Create a main queue with a dead-letter queue."""
    dlq_name = f"order-failures-{unique_name}"
    dlq_resp = sqs.create_queue(QueueName=dlq_name)
    dlq_url = dlq_resp["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]

    main_name = f"order-intake-{unique_name}"
    main_resp = sqs.create_queue(
        QueueName=main_name,
        Attributes={
            "VisibilityTimeout": "1",
            "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "1"}),
        },
    )
    main_url = main_resp["QueueUrl"]
    yield main_url, dlq_url
    sqs.delete_queue(QueueUrl=main_url)
    sqs.delete_queue(QueueUrl=dlq_url)


class TestMessageQueueApp:
    def test_send_and_receive_order(self, sqs, order_queue):
        """Send an order message, receive and verify its content."""
        order = {"order_id": "ORD-2024-001", "customer": "alice", "total": 99.95}
        sqs.send_message(QueueUrl=order_queue, MessageBody=json.dumps(order))

        response = sqs.receive_message(
            QueueUrl=order_queue, MaxNumberOfMessages=1, WaitTimeSeconds=5
        )
        messages = response["Messages"]
        assert len(messages) == 1
        received_order = json.loads(messages[0]["Body"])
        assert received_order["order_id"] == "ORD-2024-001"
        assert received_order["customer"] == "alice"
        assert received_order["total"] == 99.95

        sqs.delete_message(QueueUrl=order_queue, ReceiptHandle=messages[0]["ReceiptHandle"])

    def test_fifo_ordering(self, sqs, fifo_queue):
        """Send 5 sequenced messages to FIFO queue, verify order preserved."""
        group_id = "order-group-1"
        for i in range(5):
            sqs.send_message(
                QueueUrl=fifo_queue,
                MessageBody=json.dumps({"sequence": i, "step": f"step-{i}"}),
                MessageGroupId=group_id,
            )

        received = []
        for _ in range(5):
            resp = sqs.receive_message(
                QueueUrl=fifo_queue, MaxNumberOfMessages=1, WaitTimeSeconds=5
            )
            if "Messages" in resp:
                msg = resp["Messages"][0]
                received.append(json.loads(msg["Body"]))
                sqs.delete_message(QueueUrl=fifo_queue, ReceiptHandle=msg["ReceiptHandle"])

        sequences = [m["sequence"] for m in received]
        assert sequences == [0, 1, 2, 3, 4]

    def test_dead_letter_queue(self, sqs, dlq_pair):
        """Message that exceeds maxReceiveCount lands on DLQ."""
        main_url, dlq_url = dlq_pair

        sqs.send_message(
            QueueUrl=main_url,
            MessageBody=json.dumps({"order_id": "ORD-FAIL-001", "status": "bad"}),
        )

        # Receive once (maxReceiveCount=1), don't delete — let visibility expire
        resp = sqs.receive_message(QueueUrl=main_url, MaxNumberOfMessages=1, WaitTimeSeconds=5)
        assert len(resp.get("Messages", [])) == 1

        # Wait for visibility timeout (1s) + margin, then try to receive again
        # to trigger the DLQ redrive
        time.sleep(3)

        # This second receive triggers the move to DLQ after maxReceiveCount exceeded
        sqs.receive_message(QueueUrl=main_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)

        # Give time for redrive to complete
        time.sleep(2)

        # Message should now be on DLQ
        dlq_resp = sqs.receive_message(QueueUrl=dlq_url, MaxNumberOfMessages=1, WaitTimeSeconds=5)
        assert len(dlq_resp.get("Messages", [])) == 1
        body = json.loads(dlq_resp["Messages"][0]["Body"])
        assert body["order_id"] == "ORD-FAIL-001"

    def test_batch_send_receive(self, sqs, order_queue):
        """Send 10 messages via batch, receive all."""
        entries = [
            {
                "Id": str(i),
                "MessageBody": json.dumps({"item_id": f"ITEM-{i:03d}", "qty": i + 1}),
            }
            for i in range(10)
        ]

        sqs.send_message_batch(QueueUrl=order_queue, Entries=entries)

        all_messages = []
        for _ in range(5):
            resp = sqs.receive_message(
                QueueUrl=order_queue, MaxNumberOfMessages=10, WaitTimeSeconds=5
            )
            msgs = resp.get("Messages", [])
            all_messages.extend(msgs)
            for msg in msgs:
                sqs.delete_message(QueueUrl=order_queue, ReceiptHandle=msg["ReceiptHandle"])
            if len(all_messages) >= 10:
                break

        assert len(all_messages) == 10
        item_ids = {json.loads(m["Body"])["item_id"] for m in all_messages}
        assert item_ids == {f"ITEM-{i:03d}" for i in range(10)}

    def test_message_attributes(self, sqs, order_queue):
        """Send message with typed attributes, verify they're preserved."""
        sqs.send_message(
            QueueUrl=order_queue,
            MessageBody=json.dumps({"order_id": "ORD-ATTR-001"}),
            MessageAttributes={
                "priority": {"DataType": "String", "StringValue": "high"},
                "retry_count": {"DataType": "Number", "StringValue": "0"},
                "source_system": {"DataType": "String", "StringValue": "web-checkout"},
            },
        )

        resp = sqs.receive_message(
            QueueUrl=order_queue,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
            MessageAttributeNames=["All"],
        )
        msg = resp["Messages"][0]
        attrs = msg["MessageAttributes"]
        assert attrs["priority"]["StringValue"] == "high"
        assert attrs["retry_count"]["StringValue"] == "0"
        assert attrs["source_system"]["StringValue"] == "web-checkout"
