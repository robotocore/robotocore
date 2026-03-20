"""Unit tests for DynamoDB TTL (Time-to-Live) automatic item removal."""

import os
import time

import pytest

from moto import mock_aws
from robotocore.services.dynamodb.ttl import (
    TTLScanner,
    scan_and_remove_expired_items,
)


@pytest.fixture(autouse=True)
def _mock_aws():
    with mock_aws():
        yield


@pytest.fixture()
def _ttl_enabled():
    """Ensure TTL removal is enabled via env var."""
    old = os.environ.get("DYNAMODB_REMOVE_EXPIRED_ITEMS")
    os.environ["DYNAMODB_REMOVE_EXPIRED_ITEMS"] = "true"
    yield
    if old is None:
        os.environ.pop("DYNAMODB_REMOVE_EXPIRED_ITEMS", None)
    else:
        os.environ["DYNAMODB_REMOVE_EXPIRED_ITEMS"] = old


@pytest.fixture()
def _ttl_disabled():
    """Ensure TTL removal is disabled via env var."""
    old = os.environ.get("DYNAMODB_REMOVE_EXPIRED_ITEMS")
    os.environ["DYNAMODB_REMOVE_EXPIRED_ITEMS"] = "false"
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
    region: str = "us-east-1",
    table_name: str = "TestTable",
    enable_ttl: bool = False,
    ttl_attr: str = "expiry",
):
    backend = _get_backend(region)
    backend.create_table(
        table_name,
        schema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        throughput=None,
        attr=[{"AttributeName": "pk", "AttributeType": "S"}],
        global_indexes=None,
        indexes=None,
        streams=None,
        billing_mode="PAY_PER_REQUEST",
        sse_specification=None,
        tags=[],
        deletion_protection_enabled=None,
        warm_throughput=None,
    )
    if enable_ttl:
        backend.update_time_to_live(table_name, {"Enabled": True, "AttributeName": ttl_attr})
    return backend


def _put_item(backend, table_name: str, item: dict):
    backend.put_item(table_name, item)


def _get_item(backend, table_name: str, key: dict):
    from moto.dynamodb.models.dynamo_type import DynamoType

    hash_key = DynamoType(key["pk"])
    table = backend.get_table(table_name)
    return table.items.get(hash_key)


def _count_items(backend, table_name: str) -> int:
    table = backend.get_table(table_name)
    count = 0
    for key, val in table.items.items():
        if isinstance(val, dict):
            count += len(val)
        else:
            count += 1
    return count


class TestTTLScannerIdentifiesExpiredItems:
    """Test that TTL scanner correctly identifies and removes expired items."""

    @pytest.mark.usefixtures("_ttl_enabled")
    def test_removes_expired_items(self):
        """Items with TTL attribute < current time should be removed."""
        backend = _create_table(enable_ttl=True, ttl_attr="expiry")
        past = str(int(time.time()) - 3600)
        _put_item(backend, "TestTable", {"pk": {"S": "item1"}, "expiry": {"N": past}})

        assert _count_items(backend, "TestTable") == 1
        scan_and_remove_expired_items()
        assert _count_items(backend, "TestTable") == 0

    @pytest.mark.usefixtures("_ttl_enabled")
    def test_keeps_non_expired_items(self):
        """Items with TTL attribute > current time should NOT be removed."""
        backend = _create_table(enable_ttl=True, ttl_attr="expiry")
        future = str(int(time.time()) + 3600)
        _put_item(backend, "TestTable", {"pk": {"S": "item1"}, "expiry": {"N": future}})

        scan_and_remove_expired_items()
        assert _count_items(backend, "TestTable") == 1

    @pytest.mark.usefixtures("_ttl_enabled")
    def test_ignores_items_without_ttl_attribute(self):
        """Items without the TTL attribute should NOT be removed."""
        backend = _create_table(enable_ttl=True, ttl_attr="expiry")
        _put_item(backend, "TestTable", {"pk": {"S": "item1"}, "other": {"S": "value"}})

        scan_and_remove_expired_items()
        assert _count_items(backend, "TestTable") == 1

    @pytest.mark.usefixtures("_ttl_enabled")
    def test_ignores_items_where_ttl_not_number(self):
        """Items where TTL attribute is not a Number type should NOT be removed."""
        backend = _create_table(enable_ttl=True, ttl_attr="expiry")
        _put_item(
            backend,
            "TestTable",
            {"pk": {"S": "item1"}, "expiry": {"S": "not-a-number"}},
        )

        scan_and_remove_expired_items()
        assert _count_items(backend, "TestTable") == 1

    @pytest.mark.usefixtures("_ttl_enabled")
    def test_only_processes_tables_with_ttl_enabled(self):
        """Tables with TTL enabled should have items scanned."""
        backend_ttl = _create_table(table_name="WithTTL", enable_ttl=True, ttl_attr="expiry")
        past = str(int(time.time()) - 3600)
        _put_item(backend_ttl, "WithTTL", {"pk": {"S": "item1"}, "expiry": {"N": past}})

        # Also create a table without TTL
        backend_no_ttl = _create_table(table_name="NoTTL", enable_ttl=False)
        _put_item(backend_no_ttl, "NoTTL", {"pk": {"S": "item1"}, "expiry": {"N": past}})

        scan_and_remove_expired_items()
        assert _count_items(backend_ttl, "WithTTL") == 0
        assert _count_items(backend_no_ttl, "NoTTL") == 1

    @pytest.mark.usefixtures("_ttl_enabled")
    def test_skips_tables_without_ttl_configuration(self):
        """Tables that never had TTL configured should be skipped entirely."""
        backend = _create_table(enable_ttl=False)
        past = str(int(time.time()) - 3600)
        _put_item(backend, "TestTable", {"pk": {"S": "item1"}, "expiry": {"N": past}})

        scan_and_remove_expired_items()
        assert _count_items(backend, "TestTable") == 1

    @pytest.mark.usefixtures("_ttl_enabled")
    def test_handles_empty_tables(self):
        """Empty tables should be scanned without error."""
        _create_table(enable_ttl=True, ttl_attr="expiry")
        # Should not raise
        scan_and_remove_expired_items()

    @pytest.mark.usefixtures("_ttl_enabled")
    def test_handles_table_deletion_during_scan(self):
        """If a table is deleted during scan, skip gracefully."""
        backend = _create_table(enable_ttl=True, ttl_attr="expiry")
        past = str(int(time.time()) - 3600)
        _put_item(backend, "TestTable", {"pk": {"S": "item1"}, "expiry": {"N": past}})

        # Delete the table before scanning
        backend.delete_table("TestTable")

        # Should not raise
        scan_and_remove_expired_items()

    @pytest.mark.usefixtures("_ttl_disabled")
    def test_disabled_via_env_var(self):
        """When DYNAMODB_REMOVE_EXPIRED_ITEMS=false, no items should be removed."""
        backend = _create_table(enable_ttl=True, ttl_attr="expiry")
        past = str(int(time.time()) - 3600)
        _put_item(backend, "TestTable", {"pk": {"S": "item1"}, "expiry": {"N": past}})

        scan_and_remove_expired_items()
        assert _count_items(backend, "TestTable") == 1

    @pytest.mark.usefixtures("_ttl_enabled")
    def test_handles_multiple_tables(self):
        """Scanner should process all tables with TTL enabled."""
        backend = _create_table(table_name="Table1", enable_ttl=True, ttl_attr="exp")
        _create_table(table_name="Table2", enable_ttl=True, ttl_attr="ttl")
        past = str(int(time.time()) - 3600)
        _put_item(backend, "Table1", {"pk": {"S": "a"}, "exp": {"N": past}})
        _put_item(backend, "Table2", {"pk": {"S": "b"}, "ttl": {"N": past}})

        scan_and_remove_expired_items()
        assert _count_items(backend, "Table1") == 0
        assert _count_items(backend, "Table2") == 0

    @pytest.mark.usefixtures("_ttl_enabled")
    def test_removes_from_correct_table(self):
        """Expired items should be removed from their own table, not others."""
        backend = _create_table(table_name="Table1", enable_ttl=True, ttl_attr="expiry")
        _create_table(table_name="Table2", enable_ttl=True, ttl_attr="expiry")
        past = str(int(time.time()) - 3600)
        future = str(int(time.time()) + 3600)

        _put_item(backend, "Table1", {"pk": {"S": "expired"}, "expiry": {"N": past}})
        _put_item(backend, "Table2", {"pk": {"S": "alive"}, "expiry": {"N": future}})

        scan_and_remove_expired_items()
        assert _count_items(backend, "Table1") == 0
        assert _count_items(backend, "Table2") == 1


class TestTTLScannerThread:
    """Test TTL scanner daemon thread lifecycle."""

    def test_starts_and_stops_cleanly(self):
        scanner = TTLScanner(interval=0.1)
        scanner.start()
        assert scanner.is_alive()
        scanner.stop()
        scanner.join(timeout=2)
        assert not scanner.is_alive()

    def test_stop_is_idempotent(self):
        scanner = TTLScanner(interval=0.1)
        scanner.start()
        scanner.stop()
        scanner.join(timeout=2)
        # Calling stop again should not raise
        scanner.stop()
