"""
E-Commerce Order Processing Application Tests

Simulates an online store's order processing pipeline:
- Orders arrive via SQS FIFO queue (exactly-once, ordered per customer)
- Order data stored in DynamoDB (with GSIs for querying by status and customer)
- Payment API credentials stored in Secrets Manager
- Order confirmations sent via SNS
- PDF receipts archived to S3
"""

import json
import time
import uuid
from datetime import UTC, datetime

import pytest
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def dead_letter_queue(sqs, unique_name):
    """SQS FIFO dead-letter queue linked to the order queue."""
    dlq_name = f"order-dlq-{unique_name}.fifo"
    resp = sqs.create_queue(
        QueueName=dlq_name,
        Attributes={
            "FifoQueue": "true",
            "ContentBasedDeduplication": "false",
        },
    )
    dlq_url = resp["QueueUrl"]
    yield dlq_url
    sqs.delete_queue(QueueUrl=dlq_url)


@pytest.fixture
def order_queue(sqs, unique_name, dead_letter_queue):
    """SQS FIFO queue with content-based dedup, linked to a DLQ."""
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dead_letter_queue, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]

    queue_name = f"orders-{unique_name}.fifo"
    resp = sqs.create_queue(
        QueueName=queue_name,
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
def orders_table(dynamodb, unique_name):
    """DynamoDB table with GSIs for by-status and by-customer queries."""
    table_name = f"orders-{unique_name}"
    dynamodb.create_table(
        TableName=table_name,
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
    yield table_name
    dynamodb.delete_table(TableName=table_name)


@pytest.fixture
def payment_credentials(secretsmanager, unique_name):
    """SecretsManager secret holding payment gateway credentials."""
    secret_name = f"payment-creds-{unique_name}"
    creds = {
        "gateway_url": "https://payments.example.com/v2/charge",
        "api_key": "sk_test_abc123original",
        "merchant_id": "merch_9876",
    }
    secretsmanager.create_secret(
        Name=secret_name,
        SecretString=json.dumps(creds),
    )
    yield secret_name
    secretsmanager.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)


@pytest.fixture
def confirmation_topic(sns, unique_name):
    """SNS topic for order confirmation notifications."""
    topic_name = f"order-confirm-{unique_name}"
    resp = sns.create_topic(Name=topic_name)
    arn = resp["TopicArn"]
    yield arn
    sns.delete_topic(TopicArn=arn)


@pytest.fixture
def receipt_bucket(s3, unique_name):
    """S3 bucket for archiving order receipts."""
    bucket = f"receipts-{unique_name}"
    s3.create_bucket(Bucket=bucket)
    yield bucket
    # Cleanup: delete all objects then bucket
    try:
        objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
        for obj in objects:
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
    except Exception:
        pass
    s3.delete_bucket(Bucket=bucket)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_order(order_id=None, customer_id=None, status="pending"):
    """Build a sample order dict."""
    now = datetime.now(UTC).isoformat()
    return {
        "order_id": order_id or f"ORD-{uuid.uuid4().hex[:8]}",
        "customer_id": customer_id or f"CUST-{uuid.uuid4().hex[:6]}",
        "status": status,
        "items": [
            {"sku": "WIDGET-001", "qty": 2, "price": "19.99"},
            {"sku": "GADGET-042", "qty": 1, "price": "49.99"},
        ],
        "total": "89.97",
        "created_at": now,
    }


def _put_order(dynamodb, table, order):
    """Write an order dict to DynamoDB using native attribute types."""
    dynamodb.put_item(
        TableName=table,
        Item={
            "order_id": {"S": order["order_id"]},
            "customer_id": {"S": order["customer_id"]},
            "status": {"S": order["status"]},
            "items": {"S": json.dumps(order["items"])},
            "total": {"S": order["total"]},
            "created_at": {"S": order["created_at"]},
        },
    )


def _subscribe_sqs_to_sns(sns, sqs, topic_arn, queue_url):
    """Subscribe an SQS queue to an SNS topic and return subscription ARN."""
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    resp = sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
    )
    return resp["SubscriptionArn"]


def _receive_one(sqs, queue_url, wait=5):
    """Receive a single message from a queue, returning the message dict or None."""
    resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=wait)
    msgs = resp.get("Messages", [])
    return msgs[0] if msgs else None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestOrderIngestion:
    """SQS FIFO queue + DLQ tests."""

    def test_submit_single_order(self, sqs, order_queue):
        """Send a single order to the FIFO queue, receive and verify."""
        order = _make_order(order_id="ORD-SINGLE-001", customer_id="CUST-alice")
        sqs.send_message(
            QueueUrl=order_queue,
            MessageBody=json.dumps(order),
            MessageGroupId=order["customer_id"],
            MessageDeduplicationId=order["order_id"],
        )

        msg = _receive_one(sqs, order_queue)
        assert msg is not None
        body = json.loads(msg["Body"])
        assert body["order_id"] == "ORD-SINGLE-001"
        assert body["customer_id"] == "CUST-alice"
        assert body["status"] == "pending"
        assert len(body["items"]) == 2
        sqs.delete_message(QueueUrl=order_queue, ReceiptHandle=msg["ReceiptHandle"])

    def test_fifo_ordering_per_customer(self, sqs, order_queue):
        """Send 5 orders for the same customer, verify arrival order."""
        customer = "CUST-fifo-test"
        order_ids = [f"ORD-SEQ-{i:03d}" for i in range(5)]

        for oid in order_ids:
            sqs.send_message(
                QueueUrl=order_queue,
                MessageBody=json.dumps({"order_id": oid, "seq": oid}),
                MessageGroupId=customer,
                MessageDeduplicationId=oid,
            )

        received_ids = []
        for _ in range(5):
            msg = _receive_one(sqs, order_queue)
            assert msg is not None
            received_ids.append(json.loads(msg["Body"])["order_id"])
            sqs.delete_message(QueueUrl=order_queue, ReceiptHandle=msg["ReceiptHandle"])

        assert received_ids == order_ids

    def test_deduplication(self, sqs, order_queue):
        """Send same order twice with identical dedup ID, verify only 1 delivered."""
        body = json.dumps({"order_id": "ORD-DEDUP-001", "item": "widget"})
        dedup_id = "ORD-DEDUP-001"
        group_id = "CUST-dedup"

        sqs.send_message(
            QueueUrl=order_queue,
            MessageBody=body,
            MessageGroupId=group_id,
            MessageDeduplicationId=dedup_id,
        )
        sqs.send_message(
            QueueUrl=order_queue,
            MessageBody=body,
            MessageGroupId=group_id,
            MessageDeduplicationId=dedup_id,
        )

        # Receive all available messages
        messages = []
        for _ in range(3):
            resp = sqs.receive_message(
                QueueUrl=order_queue, MaxNumberOfMessages=10, WaitTimeSeconds=2
            )
            batch = resp.get("Messages", [])
            messages.extend(batch)
            for m in batch:
                sqs.delete_message(QueueUrl=order_queue, ReceiptHandle=m["ReceiptHandle"])
            if not batch:
                break

        assert len(messages) == 1
        assert json.loads(messages[0]["Body"])["order_id"] == "ORD-DEDUP-001"

    def test_dead_letter_after_max_receives(self, sqs, order_queue, dead_letter_queue):
        """Message exceeding maxReceiveCount=1 should land in DLQ."""
        body = json.dumps({"order_id": "ORD-DLQ-001", "failing": True})
        sqs.send_message(
            QueueUrl=order_queue,
            MessageBody=body,
            MessageGroupId="CUST-dlq",
            MessageDeduplicationId="ORD-DLQ-001",
        )

        # Receive once without deleting (maxReceiveCount=1, vis timeout=1s)
        resp = sqs.receive_message(QueueUrl=order_queue, MaxNumberOfMessages=1, WaitTimeSeconds=5)
        assert len(resp.get("Messages", [])) == 1

        # Wait for visibility timeout to expire, then trigger redrive
        time.sleep(3)
        sqs.receive_message(QueueUrl=order_queue, MaxNumberOfMessages=1, WaitTimeSeconds=1)
        time.sleep(2)

        # Message should now be on the DLQ
        dlq_msg = _receive_one(sqs, dead_letter_queue, wait=5)
        assert dlq_msg is not None
        dlq_body = json.loads(dlq_msg["Body"])
        assert dlq_body["order_id"] == "ORD-DLQ-001"


class TestOrderStorage:
    """DynamoDB table + GSI tests."""

    def test_create_order(self, dynamodb, orders_table):
        """Create an order and read it back."""
        order = _make_order(order_id="ORD-CREATE-001", customer_id="CUST-bob")
        _put_order(dynamodb, orders_table, order)

        result = dynamodb.get_item(
            TableName=orders_table,
            Key={"order_id": {"S": "ORD-CREATE-001"}},
        )
        item = result["Item"]
        assert item["order_id"]["S"] == "ORD-CREATE-001"
        assert item["customer_id"]["S"] == "CUST-bob"
        assert item["status"]["S"] == "pending"
        assert item["total"]["S"] == "89.97"
        items_list = json.loads(item["items"]["S"])
        assert len(items_list) == 2
        assert items_list[0]["sku"] == "WIDGET-001"

    def test_query_orders_by_status(self, dynamodb, orders_table):
        """Insert orders with different statuses, query GSI by-status."""
        base_time = "2026-03-08T"
        orders = []
        for i, status in enumerate(
            ["pending", "pending", "pending", "shipped", "shipped", "delivered"]
        ):
            o = _make_order(
                order_id=f"ORD-STATUS-{i:03d}",
                customer_id=f"CUST-{i}",
                status=status,
            )
            o["created_at"] = f"{base_time}{10 + i:02d}:00:00Z"
            orders.append(o)
            _put_order(dynamodb, orders_table, o)

        resp = dynamodb.query(
            TableName=orders_table,
            IndexName="by-status",
            KeyConditionExpression="#s = :status",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":status": {"S": "pending"}},
        )
        assert resp["Count"] == 3
        for item in resp["Items"]:
            assert item["status"]["S"] == "pending"

    def test_query_orders_by_customer(self, dynamodb, orders_table):
        """Insert orders for 2 customers, query GSI by-customer."""
        for i in range(3):
            o = _make_order(order_id=f"ORD-ALICE-{i}", customer_id="CUST-alice")
            o["created_at"] = f"2026-03-08T{10 + i}:00:00Z"
            _put_order(dynamodb, orders_table, o)
        for i in range(2):
            o = _make_order(order_id=f"ORD-BOB-{i}", customer_id="CUST-bob")
            o["created_at"] = f"2026-03-08T{10 + i}:00:00Z"
            _put_order(dynamodb, orders_table, o)

        resp = dynamodb.query(
            TableName=orders_table,
            IndexName="by-customer",
            KeyConditionExpression="customer_id = :cid",
            ExpressionAttributeValues={":cid": {"S": "CUST-alice"}},
        )
        assert resp["Count"] == 3
        for item in resp["Items"]:
            assert item["customer_id"]["S"] == "CUST-alice"

    def test_update_order_status(self, dynamodb, orders_table):
        """Create a pending order, update to shipped with tracking number."""
        order = _make_order(order_id="ORD-UPD-001", customer_id="CUST-carol")
        _put_order(dynamodb, orders_table, order)

        dynamodb.update_item(
            TableName=orders_table,
            Key={"order_id": {"S": "ORD-UPD-001"}},
            UpdateExpression="SET #s = :shipped, tracking_number = :tn",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":shipped": {"S": "shipped"},
                ":tn": {"S": "1Z999AA10123456784"},
            },
        )

        result = dynamodb.get_item(
            TableName=orders_table,
            Key={"order_id": {"S": "ORD-UPD-001"}},
        )
        item = result["Item"]
        assert item["status"]["S"] == "shipped"
        assert item["tracking_number"]["S"] == "1Z999AA10123456784"

    def test_conditional_update(self, dynamodb, orders_table):
        """Conditional update succeeds when condition is met, fails otherwise."""
        order = _make_order(order_id="ORD-COND-001", customer_id="CUST-dave")
        _put_order(dynamodb, orders_table, order)

        # First update: pending -> cancelled (should succeed)
        dynamodb.update_item(
            TableName=orders_table,
            Key={"order_id": {"S": "ORD-COND-001"}},
            UpdateExpression="SET #s = :cancelled",
            ConditionExpression="#s = :pending",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":cancelled": {"S": "cancelled"},
                ":pending": {"S": "pending"},
            },
        )

        result = dynamodb.get_item(
            TableName=orders_table,
            Key={"order_id": {"S": "ORD-COND-001"}},
        )
        assert result["Item"]["status"]["S"] == "cancelled"

        # Second update: try to cancel again (should fail — status is now cancelled)
        with pytest.raises(ClientError) as exc_info:
            dynamodb.update_item(
                TableName=orders_table,
                Key={"order_id": {"S": "ORD-COND-001"}},
                UpdateExpression="SET #s = :cancelled",
                ConditionExpression="#s = :pending",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":cancelled": {"S": "cancelled"},
                    ":pending": {"S": "pending"},
                },
            )
        assert exc_info.value.response["Error"]["Code"] == "ConditionalCheckFailedException"


class TestPaymentIntegration:
    """SecretsManager tests for payment credentials."""

    def test_retrieve_payment_credentials(self, secretsmanager, payment_credentials):
        """Retrieve and parse payment gateway credentials."""
        resp = secretsmanager.get_secret_value(SecretId=payment_credentials)
        creds = json.loads(resp["SecretString"])
        assert creds["gateway_url"] == "https://payments.example.com/v2/charge"
        assert creds["api_key"] == "sk_test_abc123original"
        assert creds["merchant_id"] == "merch_9876"

    def test_rotate_api_key(self, secretsmanager, payment_credentials):
        """Update the secret with a new API key and verify."""
        new_creds = {
            "gateway_url": "https://payments.example.com/v2/charge",
            "api_key": "sk_test_xyz789rotated",
            "merchant_id": "merch_9876",
        }
        secretsmanager.update_secret(
            SecretId=payment_credentials,
            SecretString=json.dumps(new_creds),
        )

        resp = secretsmanager.get_secret_value(SecretId=payment_credentials)
        creds = json.loads(resp["SecretString"])
        assert creds["api_key"] == "sk_test_xyz789rotated"

    def test_credential_versioning(self, secretsmanager, payment_credentials):
        """Create, update twice, verify version stages."""
        # First update
        secretsmanager.update_secret(
            SecretId=payment_credentials,
            SecretString=json.dumps({"api_key": "v2"}),
        )
        # Second update
        secretsmanager.update_secret(
            SecretId=payment_credentials,
            SecretString=json.dumps({"api_key": "v3"}),
        )

        desc = secretsmanager.describe_secret(SecretId=payment_credentials)
        version_stages = desc.get("VersionIdsToStages", {})
        # There should be at least one version marked AWSCURRENT
        current_versions = [vid for vid, stages in version_stages.items() if "AWSCURRENT" in stages]
        assert len(current_versions) >= 1

        # Verify the current value is the latest
        resp = secretsmanager.get_secret_value(SecretId=payment_credentials)
        creds = json.loads(resp["SecretString"])
        assert creds["api_key"] == "v3"


class TestOrderNotifications:
    """SNS + SQS subscription tests."""

    @pytest.fixture
    def subscriber_queue(self, sqs, unique_name):
        """A standard SQS queue to receive SNS notifications."""
        name = f"order-notify-sub-{unique_name}"
        resp = sqs.create_queue(QueueName=name)
        url = resp["QueueUrl"]
        yield url
        sqs.delete_queue(QueueUrl=url)

    def test_publish_order_confirmation(self, sns, sqs, confirmation_topic, subscriber_queue):
        """Publish order confirmation to SNS, receive via SQS subscriber."""
        _subscribe_sqs_to_sns(sns, sqs, confirmation_topic, subscriber_queue)

        order_msg = {
            "order_id": "ORD-CONFIRM-001",
            "status": "confirmed",
            "total": "89.97",
        }
        sns.publish(
            TopicArn=confirmation_topic,
            Message=json.dumps(order_msg),
            Subject="Order Confirmation",
        )

        msg = _receive_one(sqs, subscriber_queue, wait=5)
        assert msg is not None
        # SNS wraps the message in an envelope
        envelope = json.loads(msg["Body"])
        inner = json.loads(envelope["Message"])
        assert inner["order_id"] == "ORD-CONFIRM-001"
        assert inner["status"] == "confirmed"

    def test_message_attributes(self, sns, sqs, confirmation_topic, subscriber_queue):
        """Publish with MessageAttributes, verify they reach the subscriber."""
        _subscribe_sqs_to_sns(sns, sqs, confirmation_topic, subscriber_queue)

        sns.publish(
            TopicArn=confirmation_topic,
            Message=json.dumps({"order_id": "ORD-ATTR-001"}),
            MessageAttributes={
                "order_type": {"DataType": "String", "StringValue": "express"},
                "priority": {"DataType": "String", "StringValue": "high"},
            },
        )

        msg = _receive_one(sqs, subscriber_queue, wait=5)
        assert msg is not None
        envelope = json.loads(msg["Body"])
        # SNS includes MessageAttributes in the envelope
        attrs = envelope.get("MessageAttributes", {})
        assert attrs["order_type"]["Value"] == "express"
        assert attrs["priority"]["Value"] == "high"

    def test_multiple_subscribers(self, sns, sqs, confirmation_topic, unique_name):
        """Publish once, verify message arrives in both subscriber queues."""
        queues = []
        for i in range(2):
            name = f"multi-sub-{unique_name}-{i}"
            resp = sqs.create_queue(QueueName=name)
            queues.append(resp["QueueUrl"])
            _subscribe_sqs_to_sns(sns, sqs, confirmation_topic, resp["QueueUrl"])

        try:
            sns.publish(
                TopicArn=confirmation_topic,
                Message=json.dumps({"order_id": "ORD-MULTI-001"}),
            )

            for queue_url in queues:
                msg = _receive_one(sqs, queue_url, wait=5)
                assert msg is not None
                envelope = json.loads(msg["Body"])
                inner = json.loads(envelope["Message"])
                assert inner["order_id"] == "ORD-MULTI-001"
        finally:
            for q in queues:
                sqs.delete_queue(QueueUrl=q)


class TestOrderReceipts:
    """S3 receipt storage and end-to-end tests."""

    def test_store_receipt(self, s3, receipt_bucket):
        """Upload a JSON receipt to S3 and download to verify content."""
        receipt = {
            "order_id": "ORD-RCPT-001",
            "customer": "alice",
            "items": [{"sku": "WIDGET-001", "qty": 2}],
            "total": "39.98",
            "paid_at": "2026-03-08T12:00:00Z",
        }
        key = "receipts/2026/03/ORD-RCPT-001.json"
        s3.put_object(
            Bucket=receipt_bucket,
            Key=key,
            Body=json.dumps(receipt),
            ContentType="application/json",
        )

        obj = s3.get_object(Bucket=receipt_bucket, Key=key)
        downloaded = json.loads(obj["Body"].read())
        assert downloaded["order_id"] == "ORD-RCPT-001"
        assert downloaded["total"] == "39.98"
        assert downloaded["customer"] == "alice"

    def test_list_receipts_by_month(self, s3, receipt_bucket):
        """Upload receipts across months, verify prefix-based listing."""
        for month, oid in [("01", "A"), ("01", "B"), ("02", "C"), ("03", "D")]:
            s3.put_object(
                Bucket=receipt_bucket,
                Key=f"receipts/2026/{month}/ORD-{oid}.json",
                Body=json.dumps({"order_id": f"ORD-{oid}"}),
            )

        resp = s3.list_objects_v2(Bucket=receipt_bucket, Prefix="receipts/2026/01/")
        assert resp["KeyCount"] == 2
        keys = {o["Key"] for o in resp["Contents"]}
        assert keys == {
            "receipts/2026/01/ORD-A.json",
            "receipts/2026/01/ORD-B.json",
        }

    def test_full_order_lifecycle(
        self,
        sqs,
        dynamodb,
        secretsmanager,
        sns,
        s3,
        orders_table,
        order_queue,
        payment_credentials,
        confirmation_topic,
        receipt_bucket,
        unique_name,
    ):
        """End-to-end: submit order -> store -> confirm -> notify -> archive."""
        order_id = f"ORD-E2E-{uuid.uuid4().hex[:6]}"
        customer_id = "CUST-lifecycle"

        # 1. Read payment credentials from SecretsManager
        creds_resp = secretsmanager.get_secret_value(SecretId=payment_credentials)
        creds = json.loads(creds_resp["SecretString"])
        assert "api_key" in creds

        # 2. Submit order to SQS FIFO
        order = _make_order(order_id=order_id, customer_id=customer_id)
        sqs.send_message(
            QueueUrl=order_queue,
            MessageBody=json.dumps(order),
            MessageGroupId=customer_id,
            MessageDeduplicationId=order_id,
        )

        # 3. Receive order from queue
        msg = _receive_one(sqs, order_queue, wait=5)
        assert msg is not None
        received_order = json.loads(msg["Body"])
        assert received_order["order_id"] == order_id
        sqs.delete_message(QueueUrl=order_queue, ReceiptHandle=msg["ReceiptHandle"])

        # 4. Store order in DynamoDB
        _put_order(dynamodb, orders_table, received_order)

        # 5. Update status to confirmed
        dynamodb.update_item(
            TableName=orders_table,
            Key={"order_id": {"S": order_id}},
            UpdateExpression="SET #s = :confirmed",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":confirmed": {"S": "confirmed"}},
        )

        # 6. Publish SNS notification — subscribe a queue first
        notify_queue_name = f"e2e-notify-{unique_name}"
        notify_resp = sqs.create_queue(QueueName=notify_queue_name)
        notify_queue_url = notify_resp["QueueUrl"]
        try:
            _subscribe_sqs_to_sns(sns, sqs, confirmation_topic, notify_queue_url)

            sns.publish(
                TopicArn=confirmation_topic,
                Message=json.dumps({"order_id": order_id, "status": "confirmed"}),
            )

            # 7. Receive notification
            notify_msg = _receive_one(sqs, notify_queue_url, wait=5)
            assert notify_msg is not None
            envelope = json.loads(notify_msg["Body"])
            inner = json.loads(envelope["Message"])
            assert inner["order_id"] == order_id
            assert inner["status"] == "confirmed"
        finally:
            sqs.delete_queue(QueueUrl=notify_queue_url)

        # 8. Archive receipt to S3
        receipt = {
            "order_id": order_id,
            "customer_id": customer_id,
            "total": received_order["total"],
            "confirmed_at": datetime.now(UTC).isoformat(),
        }
        receipt_key = f"receipts/2026/03/{order_id}.json"
        s3.put_object(
            Bucket=receipt_bucket,
            Key=receipt_key,
            Body=json.dumps(receipt),
        )

        # 9. Verify receipt in S3
        obj = s3.get_object(Bucket=receipt_bucket, Key=receipt_key)
        stored_receipt = json.loads(obj["Body"].read())
        assert stored_receipt["order_id"] == order_id

        # 10. Verify final state in DynamoDB
        final = dynamodb.get_item(
            TableName=orders_table,
            Key={"order_id": {"S": order_id}},
        )
        assert final["Item"]["status"]["S"] == "confirmed"
        assert final["Item"]["customer_id"]["S"] == customer_id
