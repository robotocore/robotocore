"""
Tests for coupon and discount management.

Covers: apply coupon, usage tracking, expired coupons, max uses enforcement.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from .app import CouponError
from .models import Coupon, Order, OrderItem


class TestApplyCoupon:
    """Verify coupon application to orders."""

    def test_apply_coupon_reduces_total(self, order_processor, sample_order, unique_suffix):
        """Applying a coupon reduces the order total."""
        coupon = Coupon(
            code=f"SAVE20-{unique_suffix}",
            discount_percent=20,
            max_uses=100,
        )
        order_processor.create_coupon(coupon)

        original_total = sample_order.total
        order_processor.apply_coupon(sample_order, coupon.code)

        assert sample_order.discount_percent == 20
        assert sample_order.coupon_code == coupon.code
        assert sample_order.total < original_total

    def test_discount_amount_calculated_correctly(self, order_processor, unique_suffix):
        """Discount amount matches expected percentage of subtotal."""
        coupon = Coupon(
            code=f"HALF-{unique_suffix}",
            discount_percent=50,
            max_uses=10,
        )
        order_processor.create_coupon(coupon)

        order = Order(
            order_id=f"ORD-disc-{unique_suffix}",
            customer_id=f"CUST-disc-{unique_suffix}",
            items=[
                OrderItem(
                    product_id="TEST-001",
                    product_name="Test Item",
                    quantity=1,
                    unit_price=Decimal("100.00"),
                )
            ],
        )
        order_processor.apply_coupon(order, coupon.code)

        assert order.discount_amount == Decimal("50.00")
        assert order.subtotal == Decimal("100.00")


class TestCouponUsageTracking:
    """Verify coupon usage is tracked."""

    def test_usage_incremented_on_apply(self, order_processor, unique_suffix):
        """Applying a coupon increments current_uses."""
        coupon = Coupon(
            code=f"TRACK-{unique_suffix}",
            discount_percent=10,
            max_uses=5,
        )
        order_processor.create_coupon(coupon)

        order = Order(
            order_id=f"ORD-track-{unique_suffix}",
            customer_id=f"CUST-track-{unique_suffix}",
            items=[
                OrderItem(
                    product_id="X",
                    product_name="X",
                    quantity=1,
                    unit_price=Decimal("10.00"),
                )
            ],
        )
        order_processor.apply_coupon(order, coupon.code)

        updated = order_processor.get_coupon(coupon.code)
        assert updated.current_uses == 1


class TestExpiredCoupon:
    """Verify expired coupons are rejected."""

    def test_expired_coupon_rejected(self, order_processor, unique_suffix):
        """Cannot apply an expired coupon."""
        yesterday = (datetime.now(UTC) - timedelta(days=1)).isoformat()
        coupon = Coupon(
            code=f"EXPIRED-{unique_suffix}",
            discount_percent=10,
            max_uses=100,
            expires_at=yesterday,
        )
        order_processor.create_coupon(coupon)

        order = Order(
            order_id=f"ORD-exp-{unique_suffix}",
            customer_id=f"CUST-exp-{unique_suffix}",
            items=[
                OrderItem(
                    product_id="X",
                    product_name="X",
                    quantity=1,
                    unit_price=Decimal("10.00"),
                )
            ],
        )

        with pytest.raises(CouponError, match="expired"):
            order_processor.apply_coupon(order, coupon.code)


class TestMaxUsesEnforced:
    """Verify max uses limit is enforced."""

    def test_coupon_rejected_at_max_uses(self, order_processor, unique_suffix):
        """Coupon is rejected when max_uses is reached."""
        coupon = Coupon(
            code=f"LIMITED-{unique_suffix}",
            discount_percent=15,
            max_uses=2,
        )
        order_processor.create_coupon(coupon)

        # Use twice
        for i in range(2):
            order = Order(
                order_id=f"ORD-lim-{unique_suffix}-{i}",
                customer_id=f"CUST-lim-{unique_suffix}",
                items=[
                    OrderItem(
                        product_id="X",
                        product_name="X",
                        quantity=1,
                        unit_price=Decimal("10.00"),
                    )
                ],
            )
            order_processor.apply_coupon(order, coupon.code)

        # Third use should fail
        order3 = Order(
            order_id=f"ORD-lim-{unique_suffix}-2",
            customer_id=f"CUST-lim-{unique_suffix}",
            items=[
                OrderItem(
                    product_id="X",
                    product_name="X",
                    quantity=1,
                    unit_price=Decimal("10.00"),
                )
            ],
        )
        with pytest.raises(CouponError, match="maximum uses"):
            order_processor.apply_coupon(order3, coupon.code)

    def test_nonexistent_coupon_rejected(self, order_processor, sample_order):
        """Applying a nonexistent coupon raises CouponError."""
        with pytest.raises(CouponError, match="not found"):
            order_processor.apply_coupon(sample_order, "DOESNOTEXIST")
