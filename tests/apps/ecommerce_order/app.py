"""
E-Commerce Order Processing Application.

Simulates an online store's order processing pipeline using AWS services:
- SQS FIFO: Order ingestion queue (exactly-once, ordered per customer)
- DynamoDB: Order storage, inventory tracking, coupon management
- Secrets Manager: Payment gateway credentials
- SNS: Order confirmation notifications
- S3: Receipt document storage

This module uses only boto3 — no robotocore or moto imports.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal

from .models import (
    Coupon,
    Order,
    OrderItem,
    OrderStats,
    PaymentResult,
    Receipt,
    ShippingAddress,
)


class OrderProcessingError(Exception):
    """Raised when order processing fails."""


class InsufficientStockError(Exception):
    """Raised when inventory is insufficient for an order."""


class InvalidTransitionError(Exception):
    """Raised when an invalid status transition is attempted."""


class CouponError(Exception):
    """Raised when a coupon cannot be applied."""


class OrderProcessor:
    """
    Manages the full lifecycle of e-commerce orders.

    Coordinates across SQS (queue), DynamoDB (storage), Secrets Manager (credentials),
    SNS (notifications), and S3 (receipts).
    """

    TAX_RATE = Decimal("0.08")
    FREE_SHIPPING_THRESHOLD = Decimal("50")
    SHIPPING_COST = Decimal("5.99")

    def __init__(
        self,
        sqs_client,
        dynamodb_client,
        secretsmanager_client,
        sns_client,
        s3_client,
        order_queue_url: str,
        dlq_url: str,
        orders_table: str,
        inventory_table: str,
        coupons_table: str,
        payment_secret_name: str,
        confirmation_topic_arn: str,
        receipt_bucket: str,
    ):
        self.sqs = sqs_client
        self.dynamodb = dynamodb_client
        self.secretsmanager = secretsmanager_client
        self.sns = sns_client
        self.s3 = s3_client
        self.order_queue_url = order_queue_url
        self.dlq_url = dlq_url
        self.orders_table = orders_table
        self.inventory_table = inventory_table
        self.coupons_table = coupons_table
        self.payment_secret_name = payment_secret_name
        self.confirmation_topic_arn = confirmation_topic_arn
        self.receipt_bucket = receipt_bucket

    # -----------------------------------------------------------------------
    # Order Submission (SQS FIFO)
    # -----------------------------------------------------------------------

    def submit_order(self, order: Order) -> str:
        """
        Submit an order to the FIFO queue for processing.

        Uses customer_id as MessageGroupId for per-customer ordering.
        Returns the SQS MessageId.
        """
        errors = self._validate_order(order)
        if errors:
            raise OrderProcessingError(f"Invalid order: {'; '.join(errors)}")

        resp = self.sqs.send_message(
            QueueUrl=self.order_queue_url,
            MessageBody=json.dumps(order.to_dict(), default=str),
            MessageGroupId=order.customer_id,
            MessageDeduplicationId=order.order_id,
        )
        return resp["MessageId"]

    def _validate_order(self, order: Order) -> list[str]:
        """Validate an order before submission."""
        errors = []
        if not order.customer_id:
            errors.append("Customer ID is required")
        if not order.items:
            errors.append("Order must have at least one item")
        for item in order.items:
            if item.quantity <= 0:
                errors.append(f"Item {item.product_id}: quantity must be positive")
            if item.unit_price <= 0:
                errors.append(f"Item {item.product_id}: price must be positive")
        if order.shipping_address:
            addr_errors = order.shipping_address.validate()
            errors.extend(addr_errors)
        return errors

    # -----------------------------------------------------------------------
    # Queue Processing
    # -----------------------------------------------------------------------

    def process_next_order(self, wait_seconds: int = 5) -> Order | None:
        """
        Consume and process the next order from the queue.

        Returns the processed Order or None if no message available.
        """
        resp = self.sqs.receive_message(
            QueueUrl=self.order_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=wait_seconds,
        )
        messages = resp.get("Messages", [])
        if not messages:
            return None

        msg = messages[0]
        try:
            order_data = json.loads(msg["Body"])
            order = self._reconstruct_order(order_data)

            # Transition to PROCESSING
            order.status = "PROCESSING"
            order.updated_at = datetime.now(UTC).isoformat()

            # Store in DynamoDB
            self._store_order(order)

            # Delete from queue (successful processing)
            self.sqs.delete_message(
                QueueUrl=self.order_queue_url,
                ReceiptHandle=msg["ReceiptHandle"],
            )
            return order
        except Exception as e:
            raise OrderProcessingError(f"Failed to process order: {e}") from e

    def process_batch(self, max_messages: int = 5, wait_seconds: int = 5) -> list[Order]:
        """
        Process a batch of orders from the queue.

        Returns list of successfully processed orders.
        """
        processed = []
        for _ in range(max_messages):
            order = self.process_next_order(wait_seconds=wait_seconds)
            if order is None:
                break
            processed.append(order)
        return processed

    def _reconstruct_order(self, data: dict) -> Order:
        """Reconstruct an Order from its dict representation."""
        items = [OrderItem.from_dict(i) for i in data.get("items", [])]
        address = None
        if data.get("shipping_address"):
            address = ShippingAddress.from_dict(data["shipping_address"])

        order = Order(
            order_id=data["order_id"],
            customer_id=data["customer_id"],
            items=items,
            status=data.get("status", "SUBMITTED"),
            created_at=data.get("created_at", datetime.now(UTC).isoformat()),
            updated_at=data.get("updated_at", datetime.now(UTC).isoformat()),
            shipping_address=address,
            coupon_code=data.get("coupon_code") or None,
            discount_percent=int(data.get("discount_percent", 0)),
        )
        return order

    # -----------------------------------------------------------------------
    # Order Storage (DynamoDB)
    # -----------------------------------------------------------------------

    def _store_order(self, order: Order) -> None:
        """Write an order to DynamoDB."""
        item = {
            "order_id": {"S": order.order_id},
            "customer_id": {"S": order.customer_id},
            "status": {"S": order.status},
            "items": {"S": json.dumps([i.to_dict() for i in order.items])},
            "created_at": {"S": order.created_at},
            "updated_at": {"S": order.updated_at},
            "subtotal": {"S": str(order.subtotal)},
            "tax": {"S": str(order.tax)},
            "shipping_cost": {"S": str(order.shipping_cost)},
            "total": {"S": str(order.total)},
            "discount_percent": {"N": str(order.discount_percent)},
        }
        if order.shipping_address:
            item["shipping_address"] = {"S": json.dumps(order.shipping_address.to_dict())}
        if order.coupon_code:
            item["coupon_code"] = {"S": order.coupon_code}
        if order.tracking_number:
            item["tracking_number"] = {"S": order.tracking_number}
        if order.payment_result:
            item["payment_result"] = {"S": json.dumps(order.payment_result.to_dict())}

        self.dynamodb.put_item(TableName=self.orders_table, Item=item)

    def get_order(self, order_id: str) -> Order | None:
        """Retrieve an order by ID from DynamoDB."""
        resp = self.dynamodb.get_item(
            TableName=self.orders_table,
            Key={"order_id": {"S": order_id}},
        )
        item = resp.get("Item")
        if not item:
            return None
        return self._item_to_order(item)

    def _item_to_order(self, item: dict) -> Order:
        """Convert a DynamoDB item to an Order."""
        items_data = json.loads(item["items"]["S"])
        order_items = [OrderItem.from_dict(i) for i in items_data]

        address = None
        if "shipping_address" in item:
            address = ShippingAddress.from_dict(json.loads(item["shipping_address"]["S"]))

        payment = None
        if "payment_result" in item:
            payment = PaymentResult.from_dict(json.loads(item["payment_result"]["S"]))

        return Order(
            order_id=item["order_id"]["S"],
            customer_id=item["customer_id"]["S"],
            items=order_items,
            status=item["status"]["S"],
            created_at=item["created_at"]["S"],
            updated_at=item["updated_at"]["S"],
            shipping_address=address,
            payment_result=payment,
            coupon_code=item.get("coupon_code", {}).get("S"),
            discount_percent=int(item.get("discount_percent", {}).get("N", "0")),
            tracking_number=item.get("tracking_number", {}).get("S"),
        )

    def update_order_status(self, order_id: str, new_status: str) -> Order:
        """
        Transition an order to a new status.

        Raises InvalidTransitionError if the transition is not allowed.
        """
        order = self.get_order(order_id)
        if order is None:
            raise OrderProcessingError(f"Order {order_id} not found")

        if not order.can_transition_to(new_status):
            raise InvalidTransitionError(f"Cannot transition from {order.status} to {new_status}")

        now = datetime.now(UTC).isoformat()
        update_expr = "SET #s = :new_status, updated_at = :now"
        expr_values = {
            ":new_status": {"S": new_status},
            ":now": {"S": now},
            ":current": {"S": order.status},
        }

        self.dynamodb.update_item(
            TableName=self.orders_table,
            Key={"order_id": {"S": order_id}},
            UpdateExpression=update_expr,
            ConditionExpression="#s = :current",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues=expr_values,
        )

        order.status = new_status
        order.updated_at = now
        return order

    # -----------------------------------------------------------------------
    # Order Lifecycle
    # -----------------------------------------------------------------------

    def cancel_order(self, order_id: str) -> Order:
        """
        Cancel an order. Only SUBMITTED and PROCESSING orders can be cancelled.
        """
        order = self.get_order(order_id)
        if order is None:
            raise OrderProcessingError(f"Order {order_id} not found")
        if not order.can_cancel():
            raise InvalidTransitionError(f"Cannot cancel order in {order.status} status")
        return self.update_order_status(order_id, "CANCELLED")

    def process_payment(self, order_id: str) -> PaymentResult:
        """
        Process payment for an order.

        Reads payment credentials from Secrets Manager, simulates payment,
        updates order status.
        """
        order = self.get_order(order_id)
        if order is None:
            raise OrderProcessingError(f"Order {order_id} not found")

        # Ensure order is in correct state for payment
        if order.status == "PROCESSING":
            self.update_order_status(order_id, "PAYMENT_PENDING")
        elif order.status != "PAYMENT_PENDING":
            raise InvalidTransitionError(
                f"Cannot process payment for order in {order.status} status"
            )

        # Get payment credentials
        creds = self._get_payment_credentials()

        # Simulate payment (always succeeds if creds are valid)
        payment = PaymentResult(
            transaction_id=f"TXN-{uuid.uuid4().hex[:12]}",
            status="success" if creds.get("api_key") else "failed",
            amount=order.total,
            processed_at=datetime.now(UTC).isoformat(),
        )

        # Update order with payment result
        now = datetime.now(UTC).isoformat()
        update_expr = "SET payment_result = :pr, #s = :new_status, updated_at = :now"
        new_status = "PAID" if payment.status == "success" else "PAYMENT_PENDING"
        self.dynamodb.update_item(
            TableName=self.orders_table,
            Key={"order_id": {"S": order_id}},
            UpdateExpression=update_expr,
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":pr": {"S": json.dumps(payment.to_dict())},
                ":new_status": {"S": new_status},
                ":now": {"S": now},
            },
        )

        return payment

    def simulate_failed_payment(self, order_id: str) -> PaymentResult:
        """
        Simulate a failed payment for an order (for testing).
        """
        order = self.get_order(order_id)
        if order is None:
            raise OrderProcessingError(f"Order {order_id} not found")

        if order.status == "PROCESSING":
            self.update_order_status(order_id, "PAYMENT_PENDING")

        payment = PaymentResult(
            transaction_id=f"TXN-{uuid.uuid4().hex[:12]}",
            status="failed",
            amount=order.total,
            processed_at=datetime.now(UTC).isoformat(),
        )

        now = datetime.now(UTC).isoformat()
        self.dynamodb.update_item(
            TableName=self.orders_table,
            Key={"order_id": {"S": order_id}},
            UpdateExpression="SET payment_result = :pr, updated_at = :now",
            ExpressionAttributeValues={
                ":pr": {"S": json.dumps(payment.to_dict())},
                ":now": {"S": now},
            },
        )
        # Status stays PAYMENT_PENDING on failure
        return payment

    def ship_order(self, order_id: str, tracking_number: str) -> Order:
        """Mark an order as shipped with a tracking number."""
        order = self.get_order(order_id)
        if order is None:
            raise OrderProcessingError(f"Order {order_id} not found")

        if order.status != "PAID":
            raise InvalidTransitionError(
                f"Cannot ship order in {order.status} status (must be PAID)"
            )

        now = datetime.now(UTC).isoformat()
        self.dynamodb.update_item(
            TableName=self.orders_table,
            Key={"order_id": {"S": order_id}},
            UpdateExpression="SET #s = :shipped, tracking_number = :tn, updated_at = :now",
            ConditionExpression="#s = :paid",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":shipped": {"S": "SHIPPED"},
                ":tn": {"S": tracking_number},
                ":paid": {"S": "PAID"},
                ":now": {"S": now},
            },
        )

        order.status = "SHIPPED"
        order.tracking_number = tracking_number
        order.updated_at = now
        return order

    def complete_order(self, order_id: str) -> Order:
        """
        Run the full lifecycle: PROCESSING -> PAYMENT -> PAID -> SHIPPED -> DELIVERED -> COMPLETED.
        Returns the final order.
        """
        # Process payment
        self.process_payment(order_id)

        # Ship
        tracking = f"1Z{uuid.uuid4().hex[:16].upper()}"
        self.ship_order(order_id, tracking)

        # Deliver
        self.update_order_status(order_id, "DELIVERED")

        # Complete
        return self.update_order_status(order_id, "COMPLETED")

    def refund_order(self, order_id: str) -> PaymentResult:
        """
        Process a refund for a completed order.

        Transitions status to REFUNDED and creates a refund payment result.
        """
        order = self.get_order(order_id)
        if order is None:
            raise OrderProcessingError(f"Order {order_id} not found")

        if order.status != "COMPLETED":
            raise InvalidTransitionError(
                f"Cannot refund order in {order.status} status (must be COMPLETED)"
            )

        refund = PaymentResult(
            transaction_id=f"REFUND-{uuid.uuid4().hex[:12]}",
            status="refunded",
            amount=order.total,
            processed_at=datetime.now(UTC).isoformat(),
        )

        now = datetime.now(UTC).isoformat()
        self.dynamodb.update_item(
            TableName=self.orders_table,
            Key={"order_id": {"S": order_id}},
            UpdateExpression="SET #s = :refunded, payment_result = :pr, updated_at = :now",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":refunded": {"S": "REFUNDED"},
                ":pr": {"S": json.dumps(refund.to_dict())},
                ":now": {"S": now},
            },
        )

        return refund

    # -----------------------------------------------------------------------
    # Payment Credentials (Secrets Manager)
    # -----------------------------------------------------------------------

    def _get_payment_credentials(self) -> dict:
        """Retrieve payment gateway credentials from Secrets Manager."""
        resp = self.secretsmanager.get_secret_value(SecretId=self.payment_secret_name)
        return json.loads(resp["SecretString"])

    def rotate_payment_credentials(self, new_api_key: str) -> None:
        """Update the payment gateway API key."""
        creds = self._get_payment_credentials()
        creds["api_key"] = new_api_key
        self.secretsmanager.update_secret(
            SecretId=self.payment_secret_name,
            SecretString=json.dumps(creds),
        )

    # -----------------------------------------------------------------------
    # Order Notifications (SNS)
    # -----------------------------------------------------------------------

    def send_confirmation(self, order_id: str) -> str:
        """
        Send an order confirmation notification via SNS.

        Returns the SNS MessageId.
        """
        order = self.get_order(order_id)
        if order is None:
            raise OrderProcessingError(f"Order {order_id} not found")

        message = {
            "order_id": order.order_id,
            "customer_id": order.customer_id,
            "status": order.status,
            "total": str(order.total),
            "item_count": len(order.items),
        }

        resp = self.sns.publish(
            TopicArn=self.confirmation_topic_arn,
            Message=json.dumps(message),
            Subject=f"Order Confirmation: {order.order_id}",
            MessageAttributes={
                "order_status": {"DataType": "String", "StringValue": order.status},
                "customer_id": {"DataType": "String", "StringValue": order.customer_id},
            },
        )
        return resp["MessageId"]

    # -----------------------------------------------------------------------
    # Receipt Generation (S3)
    # -----------------------------------------------------------------------

    def generate_receipt(self, order_id: str) -> Receipt:
        """
        Generate a receipt document and store it in S3.

        Returns the Receipt metadata.
        """
        order = self.get_order(order_id)
        if order is None:
            raise OrderProcessingError(f"Order {order_id} not found")

        now = datetime.now(UTC)
        date_path = now.strftime("%Y/%m")
        s3_key = f"receipts/{date_path}/{order.order_id}.json"

        receipt_doc = {
            "order_id": order.order_id,
            "customer_id": order.customer_id,
            "items": [item.to_dict() for item in order.items],
            "subtotal": str(order.subtotal),
            "discount_percent": order.discount_percent,
            "discount_amount": str(order.discount_amount),
            "tax": str(order.tax),
            "shipping_cost": str(order.shipping_cost),
            "total": str(order.total),
            "status": order.status,
            "created_at": order.created_at,
            "generated_at": now.isoformat(),
        }
        if order.payment_result:
            receipt_doc["payment"] = order.payment_result.to_dict()

        self.s3.put_object(
            Bucket=self.receipt_bucket,
            Key=s3_key,
            Body=json.dumps(receipt_doc, indent=2),
            ContentType="application/json",
        )

        return Receipt(
            order_id=order.order_id,
            s3_key=s3_key,
            generated_at=now.isoformat(),
            total=order.total,
        )

    def get_receipt(self, order_id: str) -> dict | None:
        """
        Retrieve a receipt document from S3 by order ID.

        Searches all date prefixes for the receipt.
        """
        resp = self.s3.list_objects_v2(
            Bucket=self.receipt_bucket,
            Prefix="receipts/",
        )
        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(f"/{order_id}.json"):
                result = self.s3.get_object(Bucket=self.receipt_bucket, Key=obj["Key"])
                return json.loads(result["Body"].read())
        return None

    def list_receipts_for_customer(self, customer_id: str) -> list[dict]:
        """List all receipts for a customer by scanning receipt contents."""
        receipts = []
        resp = self.s3.list_objects_v2(Bucket=self.receipt_bucket, Prefix="receipts/")
        for obj in resp.get("Contents", []):
            result = self.s3.get_object(Bucket=self.receipt_bucket, Key=obj["Key"])
            doc = json.loads(result["Body"].read())
            if doc.get("customer_id") == customer_id:
                receipts.append(doc)
        return receipts

    # -----------------------------------------------------------------------
    # Order Queries (DynamoDB GSIs)
    # -----------------------------------------------------------------------

    def query_orders_by_customer(self, customer_id: str) -> list[Order]:
        """Query orders for a specific customer using the by-customer GSI."""
        resp = self.dynamodb.query(
            TableName=self.orders_table,
            IndexName="by-customer",
            KeyConditionExpression="customer_id = :cid",
            ExpressionAttributeValues={":cid": {"S": customer_id}},
        )
        return [self._item_to_order(item) for item in resp.get("Items", [])]

    def query_orders_by_status(self, status: str) -> list[Order]:
        """Query orders with a specific status using the by-status GSI."""
        resp = self.dynamodb.query(
            TableName=self.orders_table,
            IndexName="by-status",
            KeyConditionExpression="#s = :status",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":status": {"S": status}},
        )
        return [self._item_to_order(item) for item in resp.get("Items", [])]

    def query_orders_by_date_range(
        self, status: str, start_date: str, end_date: str
    ) -> list[Order]:
        """
        Query orders by status within a date range using the by-status GSI.

        The by-status GSI has status as hash key and created_at as range key.
        """
        resp = self.dynamodb.query(
            TableName=self.orders_table,
            IndexName="by-status",
            KeyConditionExpression="#s = :status AND created_at BETWEEN :start AND :end",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":status": {"S": status},
                ":start": {"S": start_date},
                ":end": {"S": end_date},
            },
        )
        return [self._item_to_order(item) for item in resp.get("Items", [])]

    def get_order_history(self, customer_id: str) -> list[Order]:
        """Get full order history for a customer, sorted by creation date."""
        orders = self.query_orders_by_customer(customer_id)
        return sorted(orders, key=lambda o: o.created_at)

    # -----------------------------------------------------------------------
    # Inventory Management (DynamoDB)
    # -----------------------------------------------------------------------

    def add_inventory(self, product_id: str, product_name: str, quantity: int) -> None:
        """Add or update inventory for a product."""
        self.dynamodb.put_item(
            TableName=self.inventory_table,
            Item={
                "product_id": {"S": product_id},
                "product_name": {"S": product_name},
                "stock": {"N": str(quantity)},
                "updated_at": {"S": datetime.now(UTC).isoformat()},
            },
        )

    def get_stock(self, product_id: str) -> int:
        """Get current stock level for a product."""
        resp = self.dynamodb.get_item(
            TableName=self.inventory_table,
            Key={"product_id": {"S": product_id}},
        )
        item = resp.get("Item")
        if not item:
            return 0
        return int(item["stock"]["N"])

    def decrement_inventory(self, product_id: str, quantity: int) -> int:
        """
        Decrement inventory atomically. Returns new stock level.

        Raises InsufficientStockError if stock would go below zero.
        """
        current = self.get_stock(product_id)
        if current < quantity:
            raise InsufficientStockError(
                f"Product {product_id}: requested {quantity}, available {current}"
            )

        resp = self.dynamodb.update_item(
            TableName=self.inventory_table,
            Key={"product_id": {"S": product_id}},
            UpdateExpression="SET stock = stock - :qty, updated_at = :now",
            ConditionExpression="stock >= :qty",
            ExpressionAttributeValues={
                ":qty": {"N": str(quantity)},
                ":now": {"S": datetime.now(UTC).isoformat()},
            },
            ReturnValues="ALL_NEW",
        )
        return int(resp["Attributes"]["stock"]["N"])

    def bulk_add_inventory(self, products: list[dict]) -> None:
        """
        Add inventory for multiple products.

        Each dict should have: product_id, product_name, quantity.
        """
        for product in products:
            self.add_inventory(
                product_id=product["product_id"],
                product_name=product["product_name"],
                quantity=product["quantity"],
            )

    def check_and_reserve_inventory(self, order: Order) -> bool:
        """
        Check inventory for all items in an order and decrement if available.

        Returns True if all items reserved, raises InsufficientStockError otherwise.
        """
        # First check all items
        for item in order.items:
            stock = self.get_stock(item.product_id)
            if stock < item.quantity:
                raise InsufficientStockError(
                    f"Product {item.product_id} ({item.product_name}): "
                    f"requested {item.quantity}, available {stock}"
                )

        # Then decrement all
        for item in order.items:
            self.decrement_inventory(item.product_id, item.quantity)

        return True

    # -----------------------------------------------------------------------
    # Coupon / Discount Management (DynamoDB)
    # -----------------------------------------------------------------------

    def create_coupon(self, coupon: Coupon) -> None:
        """Store a coupon in DynamoDB."""
        item = {
            "code": {"S": coupon.code},
            "discount_percent": {"N": str(coupon.discount_percent)},
            "max_uses": {"N": str(coupon.max_uses)},
            "current_uses": {"N": str(coupon.current_uses)},
        }
        if coupon.expires_at:
            item["expires_at"] = {"S": coupon.expires_at}

        self.dynamodb.put_item(TableName=self.coupons_table, Item=item)

    def get_coupon(self, code: str) -> Coupon | None:
        """Retrieve a coupon by code."""
        resp = self.dynamodb.get_item(
            TableName=self.coupons_table,
            Key={"code": {"S": code}},
        )
        item = resp.get("Item")
        if not item:
            return None
        return Coupon(
            code=item["code"]["S"],
            discount_percent=int(item["discount_percent"]["N"]),
            max_uses=int(item["max_uses"]["N"]),
            current_uses=int(item["current_uses"]["N"]),
            expires_at=item.get("expires_at", {}).get("S") or None,
        )

    def apply_coupon(self, order: Order, coupon_code: str) -> Order:
        """
        Apply a coupon to an order.

        Validates the coupon, increments usage, and updates the order.
        """
        coupon = self.get_coupon(coupon_code)
        if coupon is None:
            raise CouponError(f"Coupon '{coupon_code}' not found")

        if not coupon.is_valid:
            if coupon.current_uses >= coupon.max_uses:
                raise CouponError(f"Coupon '{coupon_code}' has reached maximum uses")
            raise CouponError(f"Coupon '{coupon_code}' has expired")

        # Increment usage atomically
        self.dynamodb.update_item(
            TableName=self.coupons_table,
            Key={"code": {"S": coupon_code}},
            UpdateExpression="SET current_uses = current_uses + :one",
            ConditionExpression="current_uses < max_uses",
            ExpressionAttributeValues={":one": {"N": "1"}},
        )

        # Update the order
        order.coupon_code = coupon_code
        order.discount_percent = coupon.discount_percent

        # Persist to DynamoDB if order exists
        if order.status != "SUBMITTED":
            now = datetime.now(UTC).isoformat()
            self.dynamodb.update_item(
                TableName=self.orders_table,
                Key={"order_id": {"S": order.order_id}},
                UpdateExpression=(
                    "SET coupon_code = :cc, discount_percent = :dp, "
                    "updated_at = :now, "
                    "subtotal = :sub, tax = :tax, shipping_cost = :ship, total = :total"
                ),
                ExpressionAttributeValues={
                    ":cc": {"S": coupon_code},
                    ":dp": {"N": str(coupon.discount_percent)},
                    ":now": {"S": now},
                    ":sub": {"S": str(order.subtotal)},
                    ":tax": {"S": str(order.tax)},
                    ":ship": {"S": str(order.shipping_cost)},
                    ":total": {"S": str(order.total)},
                },
            )

        return order

    # -----------------------------------------------------------------------
    # Order Totals
    # -----------------------------------------------------------------------

    def calculate_order_totals(self, order: Order) -> dict:
        """
        Calculate detailed totals for an order.

        Returns dict with subtotal, discount, tax, shipping, total.
        """
        return {
            "subtotal": order.subtotal,
            "discount_percent": order.discount_percent,
            "discount_amount": order.discount_amount,
            "tax": order.tax,
            "shipping_cost": order.shipping_cost,
            "total": order.total,
        }

    # -----------------------------------------------------------------------
    # Order Statistics
    # -----------------------------------------------------------------------

    def get_order_statistics(self) -> OrderStats:
        """
        Calculate aggregate order statistics by scanning the orders table.

        Returns OrderStats with total orders, revenue, average value, and status counts.
        """
        resp = self.dynamodb.scan(TableName=self.orders_table)
        items = resp.get("Items", [])

        total_orders = len(items)
        total_revenue = Decimal("0")
        orders_by_status: dict[str, int] = {}

        for item in items:
            total_str = item.get("total", {}).get("S", "0")
            total_revenue += Decimal(total_str)
            status = item.get("status", {}).get("S", "UNKNOWN")
            orders_by_status[status] = orders_by_status.get(status, 0) + 1

        avg_value = total_revenue / total_orders if total_orders > 0 else Decimal("0")

        return OrderStats(
            total_orders=total_orders,
            total_revenue=total_revenue,
            avg_order_value=avg_value.quantize(Decimal("0.01")),
            orders_by_status=orders_by_status,
        )

    def get_popular_products(self) -> dict[str, int]:
        """
        Get product order counts by scanning all orders.

        Returns dict mapping product_id to total quantity ordered.
        """
        resp = self.dynamodb.scan(TableName=self.orders_table)
        product_counts: dict[str, int] = {}

        for item in resp.get("Items", []):
            items_json = item.get("items", {}).get("S", "[]")
            order_items = json.loads(items_json)
            for oi in order_items:
                pid = oi["product_id"]
                qty = int(oi["quantity"])
                product_counts[pid] = product_counts.get(pid, 0) + qty

        return product_counts

    # -----------------------------------------------------------------------
    # Shipping Address Validation
    # -----------------------------------------------------------------------

    @staticmethod
    def validate_shipping_address(address: ShippingAddress) -> list[str]:
        """Validate a shipping address, returning a list of errors."""
        return address.validate()
