"""
Fixtures for e-commerce order processing tests.

Creates all AWS resources (queues, tables, topics, buckets, secrets)
and provides a fully-configured OrderProcessor instance.
"""

import json
import uuid
from decimal import Decimal

import pytest

from .app import OrderProcessor
from .models import Order, OrderItem, ShippingAddress


@pytest.fixture
def unique_suffix():
    """Short unique suffix for resource names."""
    return uuid.uuid4().hex[:8]


@pytest.fixture
def dead_letter_queue(sqs, unique_suffix):
    """SQS FIFO dead-letter queue."""
    name = f"order-dlq-{unique_suffix}.fifo"
    resp = sqs.create_queue(
        QueueName=name,
        Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "false"},
    )
    url = resp["QueueUrl"]
    yield url
    sqs.delete_queue(QueueUrl=url)


@pytest.fixture
def order_queue(sqs, unique_suffix, dead_letter_queue):
    """SQS FIFO order queue with DLQ."""
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dead_letter_queue, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]

    name = f"orders-{unique_suffix}.fifo"
    resp = sqs.create_queue(
        QueueName=name,
        Attributes={
            "FifoQueue": "true",
            "ContentBasedDeduplication": "true",
            "VisibilityTimeout": "1",
            "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "1"}),
        },
    )
    url = resp["QueueUrl"]
    yield url
    sqs.delete_queue(QueueUrl=url)


@pytest.fixture
def orders_table(dynamodb, unique_suffix):
    """DynamoDB orders table with GSIs for status and customer queries."""
    name = f"orders-{unique_suffix}"
    dynamodb.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "order_id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "order_id", "AttributeType": "S"},
            {"AttributeName": "status", "AttributeType": "S"},
            {"AttributeName": "created_at", "AttributeType": "S"},
            {"AttributeName": "customer_id", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-status",
                "KeySchema": [
                    {"AttributeName": "status", "KeyType": "HASH"},
                    {"AttributeName": "created_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "by-customer",
                "KeySchema": [
                    {"AttributeName": "customer_id", "KeyType": "HASH"},
                    {"AttributeName": "created_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    yield name
    dynamodb.delete_table(TableName=name)


@pytest.fixture
def inventory_table(dynamodb, unique_suffix):
    """DynamoDB inventory table."""
    name = f"inventory-{unique_suffix}"
    dynamodb.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "product_id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "product_id", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    yield name
    dynamodb.delete_table(TableName=name)


@pytest.fixture
def coupons_table(dynamodb, unique_suffix):
    """DynamoDB coupons table."""
    name = f"coupons-{unique_suffix}"
    dynamodb.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "code", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "code", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    yield name
    dynamodb.delete_table(TableName=name)


@pytest.fixture
def payment_credentials(secretsmanager, unique_suffix):
    """Secrets Manager secret with payment gateway credentials."""
    name = f"payment-creds-{unique_suffix}"
    creds = {
        "gateway_url": "https://payments.example.com/v2/charge",
        "api_key": "sk_test_abc123",
        "merchant_id": "merch_9876",
    }
    secretsmanager.create_secret(Name=name, SecretString=json.dumps(creds))
    yield name
    secretsmanager.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)


@pytest.fixture
def confirmation_topic(sns, unique_suffix):
    """SNS topic for order confirmations."""
    name = f"order-confirm-{unique_suffix}"
    resp = sns.create_topic(Name=name)
    arn = resp["TopicArn"]
    yield arn
    sns.delete_topic(TopicArn=arn)


@pytest.fixture
def receipt_bucket(s3, unique_suffix):
    """S3 bucket for receipt storage."""
    name = f"receipts-{unique_suffix}"
    s3.create_bucket(Bucket=name)
    yield name
    try:
        objects = s3.list_objects_v2(Bucket=name).get("Contents", [])
        for obj in objects:
            s3.delete_object(Bucket=name, Key=obj["Key"])
    except Exception:
        pass  # best-effort cleanup
    s3.delete_bucket(Bucket=name)


@pytest.fixture
def order_processor(
    sqs,
    dynamodb,
    secretsmanager,
    sns,
    s3,
    order_queue,
    dead_letter_queue,
    orders_table,
    inventory_table,
    coupons_table,
    payment_credentials,
    confirmation_topic,
    receipt_bucket,
):
    """Fully configured OrderProcessor with all AWS resources created."""
    return OrderProcessor(
        sqs_client=sqs,
        dynamodb_client=dynamodb,
        secretsmanager_client=secretsmanager,
        sns_client=sns,
        s3_client=s3,
        order_queue_url=order_queue,
        dlq_url=dead_letter_queue,
        orders_table=orders_table,
        inventory_table=inventory_table,
        coupons_table=coupons_table,
        payment_secret_name=payment_credentials,
        confirmation_topic_arn=confirmation_topic,
        receipt_bucket=receipt_bucket,
    )


@pytest.fixture
def sample_items():
    """Three sample order items."""
    return [
        OrderItem(
            product_id="WIDGET-001",
            product_name="Premium Widget",
            quantity=2,
            unit_price=Decimal("19.99"),
        ),
        OrderItem(
            product_id="GADGET-042",
            product_name="Super Gadget",
            quantity=1,
            unit_price=Decimal("49.99"),
        ),
        OrderItem(
            product_id="GIZMO-007",
            product_name="Mega Gizmo",
            quantity=3,
            unit_price=Decimal("9.99"),
        ),
    ]


@pytest.fixture
def sample_address():
    """A valid shipping address."""
    return ShippingAddress(
        name="Alice Smith",
        street="123 Main St",
        city="Springfield",
        state="IL",
        zip_code="62701",
        country="US",
    )


@pytest.fixture
def sample_order(sample_items, sample_address, unique_suffix):
    """A pre-built order with 3 items and a shipping address."""
    return Order(
        order_id=f"ORD-sample-{unique_suffix}",
        customer_id=f"CUST-alice-{unique_suffix}",
        items=sample_items,
        shipping_address=sample_address,
    )
