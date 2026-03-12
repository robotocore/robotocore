"""
Tests for order querying and statistics.

Covers: query by customer, query by status, date range, order history, statistics.
"""

from decimal import Decimal

from .models import Order


class TestQueryByCustomer:
    """Query orders using the by-customer GSI."""

    def test_query_orders_by_customer(
        self, order_processor, sample_items, sample_address, unique_suffix
    ):
        """Query returns only orders for the specified customer."""
        customer_a = f"CUST-qa-{unique_suffix}"
        customer_b = f"CUST-qb-{unique_suffix}"

        for i in range(3):
            order = Order(
                order_id=f"ORD-qa-{unique_suffix}-{i}",
                customer_id=customer_a,
                items=[sample_items[0]],
                shipping_address=sample_address,
                created_at=f"2026-03-08T{10 + i:02d}:00:00Z",
            )
            order_processor.submit_order(order)
            order_processor.process_next_order(wait_seconds=5)

        for i in range(2):
            order = Order(
                order_id=f"ORD-qb-{unique_suffix}-{i}",
                customer_id=customer_b,
                items=[sample_items[1]],
                shipping_address=sample_address,
                created_at=f"2026-03-08T{10 + i:02d}:00:00Z",
            )
            order_processor.submit_order(order)
            order_processor.process_next_order(wait_seconds=5)

        results = order_processor.query_orders_by_customer(customer_a)
        assert len(results) == 3
        for r in results:
            assert r.customer_id == customer_a


class TestQueryByStatus:
    """Query orders using the by-status GSI."""

    def test_query_orders_by_status(
        self, order_processor, sample_items, sample_address, unique_suffix
    ):
        """Query returns only orders with the specified status."""
        # Create 3 orders, complete 2 of them
        for i in range(3):
            order = Order(
                order_id=f"ORD-qs-{unique_suffix}-{i}",
                customer_id=f"CUST-qs-{unique_suffix}-{i}",
                items=[sample_items[0]],
                shipping_address=sample_address,
                created_at=f"2026-03-08T{10 + i:02d}:00:00Z",
            )
            order_processor.submit_order(order)
            order_processor.process_next_order(wait_seconds=5)

        # Complete first 2
        for i in range(2):
            oid = f"ORD-qs-{unique_suffix}-{i}"
            order_processor.complete_order(oid)

        processing = order_processor.query_orders_by_status("PROCESSING")
        completed = order_processor.query_orders_by_status("COMPLETED")

        assert len(processing) == 1
        assert len(completed) == 2


class TestQueryByDateRange:
    """Query orders within a date range."""

    def test_query_by_date_range(
        self, order_processor, sample_items, sample_address, unique_suffix
    ):
        """Query orders within a specific date range."""
        for i in range(5):
            order = Order(
                order_id=f"ORD-dr-{unique_suffix}-{i}",
                customer_id=f"CUST-dr-{unique_suffix}",
                items=[sample_items[0]],
                shipping_address=sample_address,
                created_at=f"2026-03-{10 + i:02d}T12:00:00Z",
            )
            order_processor.submit_order(order)
            order_processor.process_next_order(wait_seconds=5)

        # Query for March 11-13 only
        results = order_processor.query_orders_by_date_range(
            "PROCESSING",
            "2026-03-11T00:00:00Z",
            "2026-03-13T23:59:59Z",
        )
        assert len(results) == 3  # March 11, 12, 13


class TestOrderHistory:
    """Test order history retrieval."""

    def test_order_history_sorted_by_date(
        self, order_processor, sample_items, sample_address, unique_suffix
    ):
        """Order history returns orders sorted by creation date."""
        customer = f"CUST-hist-{unique_suffix}"

        for i in range(4):
            order = Order(
                order_id=f"ORD-hist-{unique_suffix}-{i}",
                customer_id=customer,
                items=[sample_items[0]],
                shipping_address=sample_address,
                created_at=f"2026-03-{10 + i:02d}T12:00:00Z",
            )
            order_processor.submit_order(order)
            order_processor.process_next_order(wait_seconds=5)

        history = order_processor.get_order_history(customer)
        assert len(history) == 4
        dates = [o.created_at for o in history]
        assert dates == sorted(dates)


class TestOrderStatistics:
    """Test aggregate statistics."""

    def test_order_statistics(self, order_processor, sample_items, sample_address, unique_suffix):
        """Statistics reflect orders in the table."""
        for i in range(3):
            order = Order(
                order_id=f"ORD-stat-{unique_suffix}-{i}",
                customer_id=f"CUST-stat-{unique_suffix}",
                items=[sample_items[0]],
                shipping_address=sample_address,
                created_at=f"2026-03-{10 + i:02d}T12:00:00Z",
            )
            order_processor.submit_order(order)
            order_processor.process_next_order(wait_seconds=5)

        stats = order_processor.get_order_statistics()
        assert stats.total_orders == 3
        assert stats.total_revenue > Decimal("0")
        assert stats.avg_order_value > Decimal("0")
        assert stats.orders_by_status.get("PROCESSING") == 3

    def test_popular_products(self, order_processor, sample_items, sample_address, unique_suffix):
        """Popular products aggregates quantity across orders."""
        for i in range(3):
            order = Order(
                order_id=f"ORD-pop-{unique_suffix}-{i}",
                customer_id=f"CUST-pop-{unique_suffix}",
                items=sample_items,
                shipping_address=sample_address,
                created_at=f"2026-03-{10 + i:02d}T12:00:00Z",
            )
            order_processor.submit_order(order)
            order_processor.process_next_order(wait_seconds=5)

        products = order_processor.get_popular_products()
        assert products["WIDGET-001"] == 6  # 2 * 3 orders
        assert products["GADGET-042"] == 3  # 1 * 3 orders
        assert products["GIZMO-007"] == 9  # 3 * 3 orders
