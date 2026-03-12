"""
Tests for payment processing.

Covers: credential retrieval, successful payment, failed payment, refunds.
"""

import pytest

from .app import InvalidTransitionError


class TestPaymentCredentials:
    """Verify payment credentials from Secrets Manager."""

    def test_retrieve_credentials(self, order_processor):
        """Payment credentials can be retrieved and parsed."""
        creds = order_processor._get_payment_credentials()
        assert creds["gateway_url"] == "https://payments.example.com/v2/charge"
        assert creds["api_key"] == "sk_test_abc123"
        assert creds["merchant_id"] == "merch_9876"

    def test_rotate_credentials(self, order_processor):
        """Credentials can be rotated with a new API key."""
        order_processor.rotate_payment_credentials("sk_test_new_key_789")
        creds = order_processor._get_payment_credentials()
        assert creds["api_key"] == "sk_test_new_key_789"
        assert creds["merchant_id"] == "merch_9876"  # unchanged


class TestSuccessfulPayment:
    """Verify successful payment processing."""

    def test_process_payment_transitions_to_paid(self, order_processor, sample_order):
        """Payment processing transitions order from PROCESSING to PAID."""
        order_processor.submit_order(sample_order)
        order_processor.process_next_order(wait_seconds=5)

        result = order_processor.process_payment(sample_order.order_id)
        assert result.status == "success"
        assert result.transaction_id.startswith("TXN-")
        assert result.amount == sample_order.total

        # Verify order status
        order = order_processor.get_order(sample_order.order_id)
        assert order.status == "PAID"
        assert order.payment_result is not None
        assert order.payment_result.status == "success"

    def test_payment_amount_matches_order_total(self, order_processor, sample_order):
        """Payment amount matches the calculated order total."""
        order_processor.submit_order(sample_order)
        order_processor.process_next_order(wait_seconds=5)

        result = order_processor.process_payment(sample_order.order_id)
        assert result.amount == sample_order.total


class TestFailedPayment:
    """Verify failed payment behavior."""

    def test_failed_payment_keeps_pending(self, order_processor, sample_order):
        """Failed payment leaves order in PAYMENT_PENDING status."""
        order_processor.submit_order(sample_order)
        order_processor.process_next_order(wait_seconds=5)

        # First move to PAYMENT_PENDING
        order_processor.update_order_status(sample_order.order_id, "PAYMENT_PENDING")

        result = order_processor.simulate_failed_payment(sample_order.order_id)
        assert result.status == "failed"

        order = order_processor.get_order(sample_order.order_id)
        assert order.status == "PAYMENT_PENDING"  # not advanced


class TestRefund:
    """Verify refund processing."""

    def test_refund_completed_order(self, order_processor, sample_order):
        """A completed order can be refunded."""
        order_processor.submit_order(sample_order)
        order_processor.process_next_order(wait_seconds=5)
        order_processor.complete_order(sample_order.order_id)

        refund = order_processor.refund_order(sample_order.order_id)
        assert refund.status == "refunded"
        assert refund.transaction_id.startswith("REFUND-")
        assert refund.amount == sample_order.total

        order = order_processor.get_order(sample_order.order_id)
        assert order.status == "REFUNDED"

    def test_cannot_refund_processing_order(self, order_processor, sample_order):
        """Cannot refund an order that is still processing."""
        order_processor.submit_order(sample_order)
        order_processor.process_next_order(wait_seconds=5)

        with pytest.raises(InvalidTransitionError, match="must be COMPLETED"):
            order_processor.refund_order(sample_order.order_id)
