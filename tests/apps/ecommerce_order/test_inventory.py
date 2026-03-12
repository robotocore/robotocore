"""
Tests for inventory management.

Covers: stock tracking, decrement on order, insufficient stock, bulk updates.
"""

from decimal import Decimal

import pytest

from .app import InsufficientStockError
from .models import Order, OrderItem


class TestStockTracking:
    """Verify inventory add/get operations."""

    def test_add_and_get_stock(self, order_processor):
        """Add inventory for a product and verify count."""
        order_processor.add_inventory("WIDGET-001", "Premium Widget", 100)
        assert order_processor.get_stock("WIDGET-001") == 100

    def test_get_stock_nonexistent(self, order_processor):
        """Nonexistent product returns 0 stock."""
        assert order_processor.get_stock("NONEXISTENT-999") == 0

    def test_update_stock_overwrites(self, order_processor):
        """Adding inventory with same product_id overwrites."""
        order_processor.add_inventory("WIDGET-001", "Premium Widget", 100)
        order_processor.add_inventory("WIDGET-001", "Premium Widget", 50)
        assert order_processor.get_stock("WIDGET-001") == 50


class TestDecrementOnOrder:
    """Verify stock decrements when orders are placed."""

    def test_decrement_reduces_stock(self, order_processor):
        """Decrementing stock reduces the count."""
        order_processor.add_inventory("WIDGET-001", "Premium Widget", 100)
        new_stock = order_processor.decrement_inventory("WIDGET-001", 10)
        assert new_stock == 90
        assert order_processor.get_stock("WIDGET-001") == 90

    def test_check_and_reserve_for_order(self, order_processor, sample_order, unique_suffix):
        """check_and_reserve_inventory decrements stock for all items."""
        # Set up inventory for all items
        for item in sample_order.items:
            order_processor.add_inventory(item.product_id, item.product_name, 50)

        order_processor.check_and_reserve_inventory(sample_order)

        # Verify each item's stock was reduced
        assert order_processor.get_stock("WIDGET-001") == 48  # 50 - 2
        assert order_processor.get_stock("GADGET-042") == 49  # 50 - 1
        assert order_processor.get_stock("GIZMO-007") == 47  # 50 - 3


class TestInsufficientStock:
    """Verify insufficient stock is rejected."""

    def test_decrement_below_zero_raises(self, order_processor):
        """Cannot decrement more than available stock."""
        order_processor.add_inventory("WIDGET-001", "Premium Widget", 5)
        with pytest.raises(InsufficientStockError, match="requested 10, available 5"):
            order_processor.decrement_inventory("WIDGET-001", 10)

    def test_order_rejected_when_stock_insufficient(
        self, order_processor, sample_address, unique_suffix
    ):
        """Order is rejected if any item has insufficient stock."""
        order_processor.add_inventory("LOW-STOCK", "Low Stock Item", 1)
        order = Order(
            order_id=f"ORD-lowstock-{unique_suffix}",
            customer_id=f"CUST-lowstock-{unique_suffix}",
            items=[
                OrderItem(
                    product_id="LOW-STOCK",
                    product_name="Low Stock Item",
                    quantity=5,
                    unit_price=Decimal("10.00"),
                )
            ],
            shipping_address=sample_address,
        )

        with pytest.raises(InsufficientStockError, match="requested 5, available 1"):
            order_processor.check_and_reserve_inventory(order)

        # Stock should be unchanged
        assert order_processor.get_stock("LOW-STOCK") == 1


class TestBulkInventory:
    """Verify bulk inventory operations."""

    def test_bulk_add_inventory(self, order_processor):
        """Add inventory for multiple products at once."""
        products = [
            {"product_id": "BULK-A", "product_name": "Bulk A", "quantity": 100},
            {"product_id": "BULK-B", "product_name": "Bulk B", "quantity": 200},
            {"product_id": "BULK-C", "product_name": "Bulk C", "quantity": 300},
        ]
        order_processor.bulk_add_inventory(products)

        assert order_processor.get_stock("BULK-A") == 100
        assert order_processor.get_stock("BULK-B") == 200
        assert order_processor.get_stock("BULK-C") == 300
