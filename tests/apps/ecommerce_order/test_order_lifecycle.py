"""
Tests for order lifecycle state transitions.

Covers: submit, process, cancel, complete, and full end-to-end lifecycle.
"""

import pytest

from .app import InvalidTransitionError


class TestOrderSubmission:
    """Verify orders can be submitted and retrieved."""

    def test_submit_order_sets_submitted_status(self, order_processor, sample_order):
        """Submit an order and verify it gets SUBMITTED status."""
        order_processor.submit_order(sample_order)
        order = order_processor.process_next_order(wait_seconds=5)
        assert order is not None
        assert order.status == "PROCESSING"
        # The original was SUBMITTED, process_next_order transitions to PROCESSING

    def test_submit_and_retrieve(self, order_processor, sample_order):
        """Submit, process from queue, then retrieve from DynamoDB."""
        order_processor.submit_order(sample_order)
        processed = order_processor.process_next_order(wait_seconds=5)
        assert processed is not None

        retrieved = order_processor.get_order(sample_order.order_id)
        assert retrieved is not None
        assert retrieved.order_id == sample_order.order_id
        assert retrieved.customer_id == sample_order.customer_id
        assert retrieved.status == "PROCESSING"
        assert len(retrieved.items) == 3


class TestStatusTransitions:
    """Verify status transitions through the order lifecycle."""

    def test_processing_to_payment_pending(self, order_processor, sample_order):
        """Transition from PROCESSING to PAYMENT_PENDING."""
        order_processor.submit_order(sample_order)
        order_processor.process_next_order(wait_seconds=5)

        updated = order_processor.update_order_status(sample_order.order_id, "PAYMENT_PENDING")
        assert updated.status == "PAYMENT_PENDING"

    def test_full_lifecycle_progression(self, order_processor, sample_order):
        """Walk through all states: SUBMITTED -> PROCESSING -> ... -> COMPLETED."""
        order_processor.submit_order(sample_order)
        order_processor.process_next_order(wait_seconds=5)

        # PROCESSING -> PAYMENT_PENDING -> PAID -> SHIPPED -> DELIVERED -> COMPLETED
        order_processor.update_order_status(sample_order.order_id, "PAYMENT_PENDING")
        order_processor.update_order_status(sample_order.order_id, "PAID")

        order_processor.ship_order(sample_order.order_id, "1Z999AA10123456784")

        order_processor.update_order_status(sample_order.order_id, "DELIVERED")
        final = order_processor.update_order_status(sample_order.order_id, "COMPLETED")

        assert final.status == "COMPLETED"

    def test_invalid_transition_raises(self, order_processor, sample_order):
        """Cannot skip states (e.g., PROCESSING -> SHIPPED)."""
        order_processor.submit_order(sample_order)
        order_processor.process_next_order(wait_seconds=5)

        with pytest.raises(InvalidTransitionError):
            order_processor.update_order_status(sample_order.order_id, "SHIPPED")


class TestOrderCancellation:
    """Verify cancellation rules."""

    def test_cancel_submitted_order(self, order_processor, sample_order, sample_items):
        """Can cancel an order that is still in PROCESSING (just pulled from queue)."""
        order_processor.submit_order(sample_order)
        order_processor.process_next_order(wait_seconds=5)

        cancelled = order_processor.cancel_order(sample_order.order_id)
        assert cancelled.status == "CANCELLED"

    def test_cannot_cancel_shipped_order(self, order_processor, sample_order):
        """Cannot cancel an order that has been shipped."""
        order_processor.submit_order(sample_order)
        order_processor.process_next_order(wait_seconds=5)

        # Move to SHIPPED
        order_processor.update_order_status(sample_order.order_id, "PAYMENT_PENDING")
        order_processor.update_order_status(sample_order.order_id, "PAID")
        order_processor.ship_order(sample_order.order_id, "TRACK123")

        with pytest.raises(InvalidTransitionError, match="Cannot cancel"):
            order_processor.cancel_order(sample_order.order_id)

    def test_cannot_cancel_paid_order(self, order_processor, sample_order):
        """Cannot cancel an order after payment."""
        order_processor.submit_order(sample_order)
        order_processor.process_next_order(wait_seconds=5)
        order_processor.update_order_status(sample_order.order_id, "PAYMENT_PENDING")
        order_processor.update_order_status(sample_order.order_id, "PAID")

        with pytest.raises(InvalidTransitionError, match="Cannot cancel"):
            order_processor.cancel_order(sample_order.order_id)


class TestCompleteOrder:
    """Test the complete_order convenience method."""

    def test_complete_order_end_to_end(self, order_processor, sample_order):
        """complete_order runs the full lifecycle automatically."""
        order_processor.submit_order(sample_order)
        order_processor.process_next_order(wait_seconds=5)

        final = order_processor.complete_order(sample_order.order_id)
        assert final.status == "COMPLETED"

        # Verify in DynamoDB
        stored = order_processor.get_order(sample_order.order_id)
        assert stored is not None
        assert stored.status == "COMPLETED"
        assert stored.tracking_number is not None
