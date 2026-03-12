"""
Tests for receipt generation and retrieval.

Covers: generate receipt, verify contents, retrieve by order, list by customer.
"""

from .models import Order


class TestGenerateReceipt:
    """Verify receipt generation and S3 storage."""

    def test_generate_receipt_stored_in_s3(self, order_processor, sample_order):
        """Generate a receipt and verify it exists in S3."""
        order_processor.submit_order(sample_order)
        order_processor.process_next_order(wait_seconds=5)

        receipt = order_processor.generate_receipt(sample_order.order_id)
        assert receipt.order_id == sample_order.order_id
        assert receipt.s3_key.startswith("receipts/")
        assert receipt.s3_key.endswith(f"/{sample_order.order_id}.json")

    def test_receipt_contains_order_details(self, order_processor, sample_order):
        """Receipt document contains full order details."""
        order_processor.submit_order(sample_order)
        order_processor.process_next_order(wait_seconds=5)

        order_processor.generate_receipt(sample_order.order_id)

        doc = order_processor.get_receipt(sample_order.order_id)
        assert doc is not None
        assert doc["order_id"] == sample_order.order_id
        assert doc["customer_id"] == sample_order.customer_id
        assert len(doc["items"]) == 3
        assert "subtotal" in doc
        assert "tax" in doc
        assert "total" in doc
        assert "generated_at" in doc

    def test_receipt_total_matches_order(self, order_processor, sample_order):
        """Receipt total matches the order's calculated total."""
        order_processor.submit_order(sample_order)
        order_processor.process_next_order(wait_seconds=5)

        receipt = order_processor.generate_receipt(sample_order.order_id)
        assert receipt.total == sample_order.total


class TestRetrieveReceipt:
    """Verify receipt retrieval."""

    def test_retrieve_receipt_by_order_id(self, order_processor, sample_order):
        """Retrieve a stored receipt by order ID."""
        order_processor.submit_order(sample_order)
        order_processor.process_next_order(wait_seconds=5)
        order_processor.generate_receipt(sample_order.order_id)

        doc = order_processor.get_receipt(sample_order.order_id)
        assert doc is not None
        assert doc["order_id"] == sample_order.order_id

    def test_retrieve_nonexistent_receipt(self, order_processor):
        """Retrieving a nonexistent receipt returns None."""
        doc = order_processor.get_receipt("ORD-NONEXISTENT-999")
        assert doc is None


class TestListReceipts:
    """Verify listing receipts for a customer."""

    def test_list_receipts_for_customer(
        self, order_processor, sample_items, sample_address, unique_suffix
    ):
        """List all receipts for a specific customer."""
        customer_id = f"CUST-receipts-{unique_suffix}"

        for i in range(3):
            order = Order(
                order_id=f"ORD-rcpt-{unique_suffix}-{i}",
                customer_id=customer_id,
                items=[sample_items[0]],
                shipping_address=sample_address,
            )
            order_processor.submit_order(order)
            order_processor.process_next_order(wait_seconds=5)
            order_processor.generate_receipt(order.order_id)

        receipts = order_processor.list_receipts_for_customer(customer_id)
        assert len(receipts) == 3
        for r in receipts:
            assert r["customer_id"] == customer_id

    def test_list_receipts_empty_for_unknown_customer(self, order_processor):
        """Unknown customer has no receipts."""
        receipts = order_processor.list_receipts_for_customer("CUST-UNKNOWN-999")
        assert receipts == []
