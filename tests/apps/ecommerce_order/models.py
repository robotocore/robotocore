"""
Data models for the e-commerce order processing application.

Pure data classes with no AWS dependencies.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal


@dataclass
class OrderItem:
    """A single item in an order."""

    product_id: str
    product_name: str
    quantity: int
    unit_price: Decimal

    @property
    def line_total(self) -> Decimal:
        return self.unit_price * self.quantity

    def to_dict(self) -> dict:
        return {
            "product_id": self.product_id,
            "product_name": self.product_name,
            "quantity": self.quantity,
            "unit_price": str(self.unit_price),
        }

    @classmethod
    def from_dict(cls, data: dict) -> OrderItem:
        return cls(
            product_id=data["product_id"],
            product_name=data["product_name"],
            quantity=int(data["quantity"]),
            unit_price=Decimal(data["unit_price"]),
        )


@dataclass
class ShippingAddress:
    """Shipping address for an order."""

    name: str
    street: str
    city: str
    state: str
    zip_code: str
    country: str = "US"

    def validate(self) -> list[str]:
        """Return list of validation errors (empty if valid)."""
        errors = []
        if not self.name or not self.name.strip():
            errors.append("Name is required")
        if not self.street or not self.street.strip():
            errors.append("Street is required")
        if not self.city or not self.city.strip():
            errors.append("City is required")
        if not self.state or not self.state.strip():
            errors.append("State is required")
        if not self.zip_code or not self.zip_code.strip():
            errors.append("Zip code is required")
        if self.zip_code and len(self.zip_code.replace("-", "").replace(" ", "")) not in (5, 9):
            errors.append("Zip code must be 5 or 9 digits")
        return errors

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "street": self.street,
            "city": self.city,
            "state": self.state,
            "zip_code": self.zip_code,
            "country": self.country,
        }

    @classmethod
    def from_dict(cls, data: dict) -> ShippingAddress:
        return cls(**data)


@dataclass
class PaymentResult:
    """Result of a payment attempt."""

    transaction_id: str
    status: str  # "success", "failed", "pending"
    amount: Decimal
    processed_at: str

    def to_dict(self) -> dict:
        return {
            "transaction_id": self.transaction_id,
            "status": self.status,
            "amount": str(self.amount),
            "processed_at": self.processed_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> PaymentResult:
        return cls(
            transaction_id=data["transaction_id"],
            status=data["status"],
            amount=Decimal(data["amount"]),
            processed_at=data["processed_at"],
        )


@dataclass
class Receipt:
    """Receipt document metadata."""

    order_id: str
    s3_key: str
    generated_at: str
    total: Decimal

    def to_dict(self) -> dict:
        return {
            "order_id": self.order_id,
            "s3_key": self.s3_key,
            "generated_at": self.generated_at,
            "total": str(self.total),
        }


@dataclass
class Coupon:
    """Discount coupon."""

    code: str
    discount_percent: int
    max_uses: int
    current_uses: int = 0
    expires_at: str | None = None

    @property
    def is_valid(self) -> bool:
        if self.current_uses >= self.max_uses:
            return False
        if self.expires_at:
            expiry = datetime.fromisoformat(self.expires_at)
            if expiry < datetime.now(UTC):
                return False
        return True

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "discount_percent": self.discount_percent,
            "max_uses": self.max_uses,
            "current_uses": self.current_uses,
            "expires_at": self.expires_at or "",
        }

    @classmethod
    def from_dict(cls, data: dict) -> Coupon:
        return cls(
            code=data["code"],
            discount_percent=int(data["discount_percent"]),
            max_uses=int(data["max_uses"]),
            current_uses=int(data.get("current_uses", 0)),
            expires_at=data.get("expires_at") or None,
        )


@dataclass
class Order:
    """An e-commerce order."""

    order_id: str = field(default_factory=lambda: f"ORD-{uuid.uuid4().hex[:8]}")
    customer_id: str = ""
    items: list[OrderItem] = field(default_factory=list)
    status: str = "SUBMITTED"
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    shipping_address: ShippingAddress | None = None
    payment_result: PaymentResult | None = None
    coupon_code: str | None = None
    discount_percent: int = 0
    tracking_number: str | None = None

    # Valid status transitions
    VALID_TRANSITIONS = {
        "SUBMITTED": ["PROCESSING", "CANCELLED"],
        "PROCESSING": ["PAYMENT_PENDING", "CANCELLED"],
        "PAYMENT_PENDING": ["PAID", "PROCESSING"],
        "PAID": ["SHIPPED"],
        "SHIPPED": ["DELIVERED"],
        "DELIVERED": ["COMPLETED"],
        "COMPLETED": ["REFUNDED"],
        "CANCELLED": [],
        "REFUNDED": [],
    }

    CANCELLABLE_STATUSES = {"SUBMITTED", "PROCESSING"}

    @property
    def subtotal(self) -> Decimal:
        return sum((item.line_total for item in self.items), Decimal("0"))

    @property
    def discount_amount(self) -> Decimal:
        if self.discount_percent > 0:
            return (self.subtotal * Decimal(self.discount_percent) / Decimal(100)).quantize(
                Decimal("0.01")
            )
        return Decimal("0")

    @property
    def tax(self) -> Decimal:
        taxable = self.subtotal - self.discount_amount
        return (taxable * Decimal("0.08")).quantize(Decimal("0.01"))

    @property
    def shipping_cost(self) -> Decimal:
        if self.subtotal - self.discount_amount >= Decimal("50"):
            return Decimal("0")
        return Decimal("5.99")

    @property
    def total(self) -> Decimal:
        return self.subtotal - self.discount_amount + self.tax + self.shipping_cost

    def can_transition_to(self, new_status: str) -> bool:
        return new_status in self.VALID_TRANSITIONS.get(self.status, [])

    def can_cancel(self) -> bool:
        return self.status in self.CANCELLABLE_STATUSES

    def to_dict(self) -> dict:
        result = {
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "items": [item.to_dict() for item in self.items],
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "subtotal": str(self.subtotal),
            "tax": str(self.tax),
            "shipping_cost": str(self.shipping_cost),
            "total": str(self.total),
            "discount_percent": self.discount_percent,
            "coupon_code": self.coupon_code or "",
        }
        if self.shipping_address:
            result["shipping_address"] = self.shipping_address.to_dict()
        if self.payment_result:
            result["payment_result"] = self.payment_result.to_dict()
        if self.tracking_number:
            result["tracking_number"] = self.tracking_number
        return result


@dataclass
class OrderStats:
    """Aggregate order statistics."""

    total_orders: int
    total_revenue: Decimal
    avg_order_value: Decimal
    orders_by_status: dict[str, int]
