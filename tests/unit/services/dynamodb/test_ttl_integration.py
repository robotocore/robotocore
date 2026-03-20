"""Integration tests for DynamoDB TTL — end-to-end scenarios with Moto backends."""

import os
import time

import pytest

from moto import mock_aws
from robotocore.services.dynamodb.ttl import scan_and_remove_expired_items


@pytest.fixture(autouse=True)
def _mock_aws():
    with mock_aws():
        yield


@pytest.fixture(autouse=True)
def _ttl_enabled():
    """Ensure TTL removal is enabled via env var."""
    old = os.environ.get("DYNAMODB_REMOVE_EXPIRED_ITEMS")
    os.environ["DYNAMODB_REMOVE_EXPIRED_ITEMS"] = "true"
    yield
    if old is None:
        os.environ.pop("DYNAMODB_REMOVE_EXPIRED_ITEMS", None)
    else:
        os.environ["DYNAMODB_REMOVE_EXPIRED_ITEMS"] = old


def _get_backend(region: str = "us-east-1"):
    from moto.backends import get_backend  # noqa: I001
    from moto.core import DEFAULT_ACCOUNT_ID

    return get_backend("dynamodb")[DEFAULT_ACCOUNT_ID][region]


def _create_table(
    table_name: str = "TestTable",
    enable_ttl: bool = False,
    ttl_attr: str = "expiry",
    enable_streams: bool = False,
    region: str = "us-east-1",
):
    backend = _get_backend(region)
    streams = None
    if enable_streams:
        streams = {"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"}
    backend.create_table(
        table_name,
        schema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        throughput=None,
        attr=[{"AttributeName": "pk", "AttributeType": "S"}],
        global_indexes=None,
        indexes=None,
        streams=streams,
        billing_mode="PAY_PER_REQUEST",
        sse_specification=None,
        tags=[],
        deletion_protection_enabled=None,
        warm_throughput=None,
    )
    if enable_ttl:
        backend.update_time_to_live(table_name, {"Enabled": True, "AttributeName": ttl_attr})
    return backend


def _count_items(backend, table_name: str) -> int:
    table = backend.get_table(table_name)
    count = 0
    for key, val in table.items.items():
        if isinstance(val, dict):
            count += len(val)
        else:
            count += 1
    return count


class TestEndToEndTTLRemoval:
    """End-to-end tests: create table -> enable TTL -> put items -> scan -> verify."""

    def test_expired_item_removed(self):
        """Create table, enable TTL, put expired item, scan -> item removed."""
        backend = _create_table(enable_ttl=True, ttl_attr="expiry")
        past = str(int(time.time()) - 3600)
        backend.put_item("TestTable", {"pk": {"S": "item1"}, "expiry": {"N": past}})

        assert _count_items(backend, "TestTable") == 1
        scan_and_remove_expired_items()
        assert _count_items(backend, "TestTable") == 0

    def test_future_item_kept(self):
        """Create table, enable TTL, put future-expiry item, scan -> item still exists."""
        backend = _create_table(enable_ttl=True, ttl_attr="expiry")
        future = str(int(time.time()) + 3600)
        backend.put_item("TestTable", {"pk": {"S": "item1"}, "expiry": {"N": future}})

        scan_and_remove_expired_items()
        assert _count_items(backend, "TestTable") == 1

    def test_no_ttl_enabled_items_kept(self):
        """Create table without TTL, put expired item, scan -> item still exists."""
        backend = _create_table(enable_ttl=False)
        past = str(int(time.time()) - 3600)
        backend.put_item("TestTable", {"pk": {"S": "item1"}, "expiry": {"N": past}})

        scan_and_remove_expired_items()
        assert _count_items(backend, "TestTable") == 1

    def test_stream_event_on_ttl_removal(self):
        """TTL removal should emit a REMOVE stream event with Service userIdentity."""
        from robotocore.services.dynamodbstreams.hooks import get_store

        backend = _create_table(enable_ttl=True, ttl_attr="expiry", enable_streams=True)
        past = str(int(time.time()) - 3600)
        backend.put_item("TestTable", {"pk": {"S": "item1"}, "expiry": {"N": past}})

        scan_and_remove_expired_items()
        assert _count_items(backend, "TestTable") == 0

        # Check the stream store for the REMOVE event
        store = get_store("us-east-1")
        all_records = []
        for arn, records in store._hook_records.items():
            all_records.extend(records)

        # Find REMOVE events
        remove_events = [r for r in all_records if r.event_name == "REMOVE"]
        assert len(remove_events) >= 1

        evt = remove_events[-1]
        assert evt.event_name == "REMOVE"
        # Check userIdentity is set for TTL-triggered removal
        assert evt.dynamodb.get("userIdentity") == {
            "type": "Service",
            "principalId": "dynamodb.amazonaws.com",
        }

    def test_stream_event_user_identity(self):
        """TTL removal stream events must have userIdentity type=Service."""
        from robotocore.services.dynamodbstreams.hooks import get_store

        backend = _create_table(
            table_name="IdentityTable",
            enable_ttl=True,
            ttl_attr="ttl_field",
            enable_streams=True,
        )
        past = str(int(time.time()) - 3600)
        backend.put_item("IdentityTable", {"pk": {"S": "x"}, "ttl_field": {"N": past}})

        scan_and_remove_expired_items()

        store = get_store("us-east-1")
        all_records = []
        for arn, records in store._hook_records.items():
            all_records.extend(records)

        remove_events = [r for r in all_records if r.event_name == "REMOVE"]
        assert len(remove_events) >= 1
        identity = remove_events[-1].dynamodb.get("userIdentity")
        assert identity is not None
        assert identity["type"] == "Service"
        assert identity["principalId"] == "dynamodb.amazonaws.com"

    def test_mixed_items_only_expired_removed(self):
        """Multiple items: some expired, some not -- only expired removed."""
        backend = _create_table(enable_ttl=True, ttl_attr="expiry")
        past = str(int(time.time()) - 3600)
        future = str(int(time.time()) + 3600)

        backend.put_item("TestTable", {"pk": {"S": "expired1"}, "expiry": {"N": past}})
        backend.put_item("TestTable", {"pk": {"S": "expired2"}, "expiry": {"N": past}})
        backend.put_item("TestTable", {"pk": {"S": "alive"}, "expiry": {"N": future}})
        backend.put_item("TestTable", {"pk": {"S": "no_ttl"}, "other": {"S": "value"}})

        assert _count_items(backend, "TestTable") == 4
        scan_and_remove_expired_items()
        assert _count_items(backend, "TestTable") == 2  # alive + no_ttl remain

    def test_different_ttl_attribute_names_per_table(self):
        """Each table can have a different TTL attribute name."""
        backend = _create_table(table_name="Table1", enable_ttl=True, ttl_attr="exp_at")
        _create_table(table_name="Table2", enable_ttl=True, ttl_attr="delete_after")
        past = str(int(time.time()) - 3600)

        backend.put_item("Table1", {"pk": {"S": "item1"}, "exp_at": {"N": past}})
        backend.put_item("Table2", {"pk": {"S": "item2"}, "delete_after": {"N": past}})

        scan_and_remove_expired_items()
        assert _count_items(backend, "Table1") == 0
        assert _count_items(backend, "Table2") == 0
