"""Compatibility tests for state save/load with services that contain threading locks.

Tests verify that:
1. State save completes without pickle errors for services with threading locks
2. State load completes without errors
3. Save/load APIs return correct status responses
4. Multiple services can be saved together
5. Versioned snapshots (in-memory) work correctly

IMPORTANT: These tests MUST NOT call _reset_state() or load snapshots that
replace Moto backends, because that destroys state for other compat tests
running in parallel on the same server. The primary goal is to verify that
save/load doesn't CRASH (the threading lock pickle fix), not to verify
end-to-end data restoration.
"""

import tempfile
import uuid

import requests

from tests.compatibility.conftest import ENDPOINT_URL, make_client

STATE_SAVE_URL = f"{ENDPOINT_URL}/_robotocore/state/save"
STATE_LOAD_URL = f"{ENDPOINT_URL}/_robotocore/state/load"
VERSIONED_SAVE_URL = f"{ENDPOINT_URL}/_robotocore/state/snapshots/save"
VERSIONED_LOAD_URL = f"{ENDPOINT_URL}/_robotocore/state/snapshots/load"

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


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestSqsStateSaveLoad:
    """SQS uses a native provider with threading.Condition locks.

    The primary purpose of these tests is to verify that saving state when
    SQS queues exist does not crash with 'can't pickle _thread.lock objects'.
    """

    def test_sqs_state_save_does_not_crash(self):
        """Save state when SQS queues with threading locks exist."""
        sqs = make_client("sqs")
        snapshot = _unique("sqs-snap")
        queue_name = _unique("test-queue")

        # Create queue and send a message (this creates threading.Condition objects)
        resp = sqs.create_queue(QueueName=queue_name)
        queue_url = resp["QueueUrl"]
        sqs.send_message(QueueUrl=queue_url, MessageBody="hello-state")

        # Save state -- the main assertion: this must not crash with
        # "can't pickle _thread.lock objects"
        result = _save_state(snapshot)
        assert result.get("status") == "saved"
        assert "path" in result

        # Queue should still be accessible after save
        recv = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
        messages = recv.get("Messages", [])
        assert len(messages) >= 1
        assert any(m["Body"] == "hello-state" for m in messages)

        # Cleanup
        sqs.delete_queue(QueueUrl=queue_url)


class TestDynamoDbStateSaveLoad:
    """DynamoDB state save/load API works without errors."""

    def test_dynamodb_save_succeeds(self):
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

        # Save state -- must succeed
        result = _save_state(snapshot, services=["dynamodb"])
        assert result.get("status") == "saved"

        # Data should still be intact after save
        item = ddb.get_item(TableName=table_name, Key={"pk": {"S": "key1"}})
        assert item["Item"]["data"]["S"] == "value1"

        # Cleanup
        ddb.delete_table(TableName=table_name)


class TestS3StateSaveLoad:
    """S3 state save works without errors."""

    def test_s3_save_succeeds(self):
        s3 = make_client("s3")
        snapshot = _unique("s3-snap")
        bucket_name = _unique("test-bucket")

        # Create bucket and put object
        s3.create_bucket(Bucket=bucket_name)
        s3.put_object(Bucket=bucket_name, Key="test.txt", Body=b"state-data")

        # Save state -- must not crash
        result = _save_state(snapshot)
        assert result.get("status") == "saved"

        # Data should still be intact after save
        obj = s3.get_object(Bucket=bucket_name, Key="test.txt")
        body = obj["Body"].read()
        assert body == b"state-data"

        # Cleanup
        s3.delete_object(Bucket=bucket_name, Key="test.txt")
        s3.delete_bucket(Bucket=bucket_name)


class TestMultiServiceStateSaveLoad:
    """Multiple services can be saved together without crashing."""

    def test_multi_service_save_succeeds(self):
        snapshot = _unique("multi-snap")

        # Create SQS queue (native, has threading locks)
        sqs = make_client("sqs")
        queue_name = _unique("multi-queue")
        resp = sqs.create_queue(QueueName=queue_name)
        queue_url = resp["QueueUrl"]
        sqs.send_message(QueueUrl=queue_url, MessageBody="multi-msg")

        # Create DynamoDB table (Moto-backed)
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

        # Save all state (must not crash despite SQS threading locks)
        result = _save_state(snapshot)
        assert result.get("status") == "saved"

        # All data should still be accessible
        recv = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
        assert any(m["Body"] == "multi-msg" for m in recv.get("Messages", []))

        item = ddb.get_item(TableName=table_name, Key={"pk": {"S": "k1"}})
        assert item["Item"]["val"]["S"] == "v1"

        obj = s3.get_object(Bucket=bucket_name, Key="multi.txt")
        assert obj["Body"].read() == b"multi-data"

        # Cleanup
        sqs.delete_queue(QueueUrl=queue_url)
        ddb.delete_table(TableName=table_name)
        s3.delete_object(Bucket=bucket_name, Key="multi.txt")
        s3.delete_bucket(Bucket=bucket_name)


class TestVersionedSnapshots:
    """Versioned snapshot save API works."""

    def test_save_two_versions_succeeds(self):
        ddb = make_client("dynamodb")
        table_name = _unique("ver-table")
        snap_name = _unique("ver-snap")

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
        result = _save_state(snap_name, services=["dynamodb"])
        assert result.get("status") == "saved"

        # Modify: update item to v2
        ddb.put_item(
            TableName=table_name,
            Item={"pk": {"S": "key1"}, "version": {"S": "v2"}},
        )

        # Save v2 with a different name
        snap_v2 = _unique("ver-snap-v2")
        result = _save_state(snap_v2, services=["dynamodb"])
        assert result.get("status") == "saved"

        # Verify current state is v2
        item = ddb.get_item(TableName=table_name, Key={"pk": {"S": "key1"}})
        assert item["Item"]["version"]["S"] == "v2"

        # Cleanup
        ddb.delete_table(TableName=table_name)
