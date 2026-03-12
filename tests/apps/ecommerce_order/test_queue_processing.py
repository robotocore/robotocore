"""
Tests for SQS FIFO queue processing.

Covers: submit to queue, FIFO ordering, dead-letter queue, batch processing.
"""

import json
import time

from .models import Order


class TestSubmitToQueue:
    """Verify orders are correctly placed on the FIFO queue."""

    def test_submit_and_consume(self, order_processor, sample_order):
        """Submit order to queue, consume it, verify data integrity."""
        order_processor.submit_order(sample_order)
        processed = order_processor.process_next_order(wait_seconds=5)

        assert processed is not None
        assert processed.order_id == sample_order.order_id
        assert processed.customer_id == sample_order.customer_id
        assert len(processed.items) == 3
        assert processed.status == "PROCESSING"

    def test_no_messages_returns_none(self, order_processor):
        """process_next_order returns None when queue is empty."""
        result = order_processor.process_next_order(wait_seconds=1)
        assert result is None


class TestFifoOrdering:
    """Verify FIFO ordering guarantees per message group."""

    def test_same_customer_orders_processed_in_order(
        self, order_processor, sample_items, sample_address, unique_suffix
    ):
        """Orders from the same customer arrive in submission order."""
        customer_id = f"CUST-fifo-{unique_suffix}"
        order_ids = []

        for i in range(3):
            oid = f"ORD-fifo-{unique_suffix}-{i:03d}"
            order_ids.append(oid)
            order = Order(
                order_id=oid,
                customer_id=customer_id,
                items=[sample_items[0]],
                shipping_address=sample_address,
            )
            order_processor.submit_order(order)

        received_ids = []
        for _ in range(3):
            processed = order_processor.process_next_order(wait_seconds=5)
            assert processed is not None
            received_ids.append(processed.order_id)

        assert received_ids == order_ids


class TestDeadLetterQueue:
    """Verify DLQ behavior for failed messages."""

    def test_unprocessed_message_goes_to_dlq(
        self, sqs, order_processor, sample_order, dead_letter_queue
    ):
        """Message that exceeds maxReceiveCount lands in the DLQ."""
        # Send directly to queue (bypassing processor for control)
        body = json.dumps(sample_order.to_dict(), default=str)
        sqs.send_message(
            QueueUrl=order_processor.order_queue_url,
            MessageBody=body,
            MessageGroupId=sample_order.customer_id,
            MessageDeduplicationId=sample_order.order_id,
        )

        # Receive once without deleting (vis timeout = 1s, maxReceiveCount = 1)
        resp = sqs.receive_message(
            QueueUrl=order_processor.order_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
        )
        assert len(resp.get("Messages", [])) == 1

        # Wait for visibility timeout, then trigger redrive
        time.sleep(3)
        sqs.receive_message(
            QueueUrl=order_processor.order_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1,
        )
        time.sleep(2)

        # Check DLQ
        dlq_resp = sqs.receive_message(
            QueueUrl=dead_letter_queue,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
        )
        dlq_msgs = dlq_resp.get("Messages", [])
        assert len(dlq_msgs) == 1
        dlq_body = json.loads(dlq_msgs[0]["Body"])
        assert dlq_body["order_id"] == sample_order.order_id


class TestBatchProcessing:
    """Verify batch processing of multiple orders."""

    def test_process_batch_of_orders(
        self, order_processor, sample_items, sample_address, unique_suffix
    ):
        """Submit 5 orders, process all in batch."""
        submitted_ids = []
        for i in range(5):
            oid = f"ORD-batch-{unique_suffix}-{i:03d}"
            submitted_ids.append(oid)
            order = Order(
                order_id=oid,
                customer_id=f"CUST-batch-{unique_suffix}-{i}",
                items=[sample_items[0]],
                shipping_address=sample_address,
            )
            order_processor.submit_order(order)

        processed = order_processor.process_batch(max_messages=5, wait_seconds=5)
        assert len(processed) == 5

        processed_ids = [o.order_id for o in processed]
        assert set(processed_ids) == set(submitted_ids)

    def test_batch_returns_empty_on_empty_queue(self, order_processor):
        """Batch processing returns empty list when queue is empty."""
        processed = order_processor.process_batch(max_messages=3, wait_seconds=1)
        assert processed == []
