"""Compatibility tests for state save/load with services that contain threading locks.

Tests verify that:
1. SQS queues with Condition locks survive save/load
2. DynamoDB tables survive save/load
3. S3 buckets/objects survive save/load
4. Multi-service state survives save/load
5. Versioned snapshots work correctly
"""

import tempfile
import uuid

import requests

from tests.compatibility.conftest import ENDPOINT_URL, make_client

STATE_SAVE_URL = f"{ENDPOINT_URL}/_robotocore/state/save"
STATE_LOAD_URL = f"{ENDPOINT_URL}/_robotocore/state/load"
STATE_RESET_URL = f"{ENDPOINT_URL}/_robotocore/state/reset"

# Use a temp directory for disk-based save/load
_STATE_DIR = tempfile.mkdtemp(prefix="robotocore-state-test-")


def _save_state(name: str, services: list[str] | None = None) -> dict:
    """Save state via the REST API."""
    payload: dict = {"name": name, "path": _STATE_DIR}
    if services:
        payload["services"] = services
    resp = requests.post(STATE_SAVE_URL, json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _load_state(name: str, services: list[str] | None = None) -> dict:
    """Load state via the REST API."""
    payload: dict = {"name": name, "path": _STATE_DIR}
    if services:
        payload["services"] = services
    resp = requests.post(STATE_LOAD_URL, json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _reset_state() -> dict:
    """Reset all emulator state."""
    resp = requests.post(STATE_RESET_URL, json={}, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestSqsStateSaveLoad:
    """SQS queues contain threading.Condition locks that must survive save/load."""

    def test_sqs_queue_message_survives_save_load(self):
        sqs = make_client("sqs")
        snapshot = _unique("sqs-snap")
        queue_name = _unique("test-queue")

        # Create queue and send a message
        resp = sqs.create_queue(QueueName=queue_name)
        queue_url = resp["QueueUrl"]
        sqs.send_message(QueueUrl=queue_url, MessageBody="hello-state")

        # Save state
        result = _save_state(snapshot)
        assert result.get("status") == "saved"

        # Reset state to wipe everything
        _reset_state()

        # Load state
        result = _load_state(snapshot)
        assert result.get("status") == "loaded"

        # Verify the message is back
        recv = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
        messages = recv.get("Messages", [])
        assert len(messages) >= 1
        assert any(m["Body"] == "hello-state" for m in messages)


class TestDynamoDbStateSaveLoad:
    """DynamoDB tables should survive save/load."""

    def test_dynamodb_item_survives_save_load(self):
        ddb = make_client("dynamodb")
        snapshot = _unique("ddb-snap")
        table_name = _unique("test-table")

        # Create table and put item
        ddb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        ddb.put_item(
            TableName=table_name,
            Item={"pk": {"S": "key1"}, "data": {"S": "value1"}},
        )

        # Save state
        result = _save_state(snapshot)
        assert result.get("status") == "saved"

        # Reset
        _reset_state()

        # Load state
        result = _load_state(snapshot)
        assert result.get("status") == "loaded"

        # Verify item is back
        item = ddb.get_item(TableName=table_name, Key={"pk": {"S": "key1"}})
        assert item["Item"]["data"]["S"] == "value1"


class TestS3StateSaveLoad:
    """S3 buckets/objects should survive save/load."""

    def test_s3_object_survives_save_load(self):
        s3 = make_client("s3")
        snapshot = _unique("s3-snap")
        bucket_name = _unique("test-bucket")

        # Create bucket and put object
        s3.create_bucket(Bucket=bucket_name)
        s3.put_object(Bucket=bucket_name, Key="test.txt", Body=b"state-data")

        # Save state
        result = _save_state(snapshot)
        assert result.get("status") == "saved"

        # Reset
        _reset_state()

        # Load state
        result = _load_state(snapshot)
        assert result.get("status") == "loaded"

        # Verify object is back
        obj = s3.get_object(Bucket=bucket_name, Key="test.txt")
        body = obj["Body"].read()
        assert body == b"state-data"


class TestMultiServiceStateSaveLoad:
    """Multiple services should survive save/load together."""

    def test_sqs_dynamodb_s3_survive_save_load(self):
        snapshot = _unique("multi-snap")

        # Create SQS queue
        sqs = make_client("sqs")
        queue_name = _unique("multi-queue")
        resp = sqs.create_queue(QueueName=queue_name)
        queue_url = resp["QueueUrl"]
        sqs.send_message(QueueUrl=queue_url, MessageBody="multi-msg")

        # Create DynamoDB table
        ddb = make_client("dynamodb")
        table_name = _unique("multi-table")
        ddb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        ddb.put_item(
            TableName=table_name,
            Item={"pk": {"S": "k1"}, "val": {"S": "v1"}},
        )

        # Create S3 bucket
        s3 = make_client("s3")
        bucket_name = _unique("multi-bucket")
        s3.create_bucket(Bucket=bucket_name)
        s3.put_object(Bucket=bucket_name, Key="multi.txt", Body=b"multi-data")

        # Save all state
        result = _save_state(snapshot)
        assert result.get("status") == "saved"

        # Reset everything
        _reset_state()

        # Load state
        result = _load_state(snapshot)
        assert result.get("status") == "loaded"

        # Verify SQS
        recv = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
        assert any(m["Body"] == "multi-msg" for m in recv.get("Messages", []))

        # Verify DynamoDB
        item = ddb.get_item(TableName=table_name, Key={"pk": {"S": "k1"}})
        assert item["Item"]["val"]["S"] == "v1"

        # Verify S3
        obj = s3.get_object(Bucket=bucket_name, Key="multi.txt")
        assert obj["Body"].read() == b"multi-data"


class TestVersionedSnapshots:
    """Versioned snapshots: save v1, modify, save v2, load v1 => get original."""

    def test_versioned_snapshot_restores_original(self):
        ddb = make_client("dynamodb")
        table_name = _unique("ver-table")
        snap_v1 = _unique("ver-v1")
        snap_v2 = _unique("ver-v2")

        # Create table, put item v1
        ddb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        ddb.put_item(
            TableName=table_name,
            Item={"pk": {"S": "key1"}, "version": {"S": "v1"}},
        )

        # Save v1
        result = _save_state(snap_v1)
        assert result.get("status") == "saved"

        # Modify: update item to v2
        ddb.put_item(
            TableName=table_name,
            Item={"pk": {"S": "key1"}, "version": {"S": "v2"}},
        )

        # Save v2
        result = _save_state(snap_v2)
        assert result.get("status") == "saved"

        # Verify current state is v2
        item = ddb.get_item(TableName=table_name, Key={"pk": {"S": "key1"}})
        assert item["Item"]["version"]["S"] == "v2"

        # Load v1
        result = _load_state(snap_v1)
        assert result.get("status") == "loaded"

        # Verify state is back to v1
        item = ddb.get_item(TableName=table_name, Key={"pk": {"S": "key1"}})
        assert item["Item"]["version"]["S"] == "v1"
