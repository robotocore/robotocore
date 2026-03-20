"""Tests for DynamoDB Global Tables replication engine."""

import time

import pytest

from moto import mock_aws
from robotocore.services.dynamodb.replication import (
    _add_replication_attrs,
    _item_to_dict,
    backfill_replica,
    create_replica_table,
    delete_replica_table,
    get_replica_regions,
    replicate_write,
)


@pytest.fixture(autouse=True)
def _mock_aws():
    with mock_aws():
        yield


def _create_table_in_region(region: str, table_name: str = "TestTable"):
    """Helper to create a DynamoDB table in a specific region via Moto backend."""
    from moto.backends import get_backend  # noqa: I001
    from moto.core import DEFAULT_ACCOUNT_ID

    backend = get_backend("dynamodb")[DEFAULT_ACCOUNT_ID][region]
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
    return backend


def _put_item(region: str, table_name: str, item: dict):
    """Put an item into a Moto DynamoDB backend."""
    from moto.backends import get_backend  # noqa: I001
    from moto.core import DEFAULT_ACCOUNT_ID

    backend = get_backend("dynamodb")[DEFAULT_ACCOUNT_ID][region]
    backend.put_item(table_name, item)


def _get_item(region: str, table_name: str, key: dict):
    """Get an item from a Moto DynamoDB backend."""
    from moto.backends import get_backend  # noqa: I001
    from moto.core import DEFAULT_ACCOUNT_ID

    backend = get_backend("dynamodb")[DEFAULT_ACCOUNT_ID][region]
    return backend.get_item(table_name, key)


class TestGetReplicaRegions:
    def test_returns_empty_for_non_global_table(self):
        gt = {}
        assert get_replica_regions("NotGlobal", "123456789012", gt) == []

    def test_returns_regions(self):
        gt = {
            ("acct", "MyTable"): {
                "ReplicationGroup": [
                    {"RegionName": "us-east-1"},
                    {"RegionName": "eu-west-1"},
                ]
            }
        }
        regions = get_replica_regions("MyTable", "acct", gt)
        assert set(regions) == {"us-east-1", "eu-west-1"}


class TestAddReplicationAttrs:
    def test_adds_system_attributes(self):
        item = {"pk": {"S": "abc"}, "data": {"N": "42"}}
        now = time.time()
        result = _add_replication_attrs(item, "us-east-1", now)
        assert result["aws:rep:deleting"] == {"BOOL": False}
        assert result["aws:rep:updateregion"] == {"S": "us-east-1"}
        assert result["aws:rep:updatetime"] == {"N": str(now)}
        # Original item should not be modified
        assert "aws:rep:deleting" not in item

    def test_preserves_original_attrs(self):
        item = {"pk": {"S": "x"}, "name": {"S": "test"}}
        result = _add_replication_attrs(item, "eu-west-1", 0.0)
        assert result["pk"] == {"S": "x"}
        assert result["name"] == {"S": "test"}


class TestItemToDict:
    def test_none_returns_none(self):
        assert _item_to_dict(None) is None

    def test_dict_returns_dict(self):
        d = {"pk": {"S": "abc"}}
        assert _item_to_dict(d) == d


class TestCreateReplicaTable:
    def test_creates_table_in_target_region(self):
        _create_table_in_region("us-east-1", "ReplicaTest")

        success = create_replica_table("ReplicaTest", "us-east-1", "eu-west-1", "123456789012")
        assert success

        # Verify table exists in target region
        from moto.backends import get_backend  # noqa: I001
        from moto.core import DEFAULT_ACCOUNT_ID

        target = get_backend("dynamodb")[DEFAULT_ACCOUNT_ID]["eu-west-1"]
        table = target.get_table("ReplicaTest")
        assert table is not None

    def test_skips_if_table_already_exists(self):
        _create_table_in_region("us-east-1", "AlreadyExists")
        _create_table_in_region("eu-west-1", "AlreadyExists")

        # Should succeed without error (idempotent)
        success = create_replica_table("AlreadyExists", "us-east-1", "eu-west-1", "123456789012")
        assert success

    def test_fails_if_source_not_found(self):
        success = create_replica_table("NoSuchTable", "us-east-1", "eu-west-1", "123456789012")
        assert not success


class TestDeleteReplicaTable:
    def test_deletes_table(self):
        _create_table_in_region("eu-west-1", "ToDelete")
        success = delete_replica_table("ToDelete", "eu-west-1", "123456789012")
        assert success

    def test_handles_missing_table(self):
        # Should not raise, returns False
        result = delete_replica_table("NoSuchTable", "eu-west-1", "123456789012")
        assert result is False


class TestReplicateWrite:
    def test_replicate_put_item(self):
        _create_table_in_region("us-east-1", "GlobalTbl")
        _create_table_in_region("eu-west-1", "GlobalTbl")

        gt = {
            ("123456789012", "GlobalTbl"): {
                "ReplicationGroup": [
                    {"RegionName": "us-east-1"},
                    {"RegionName": "eu-west-1"},
                ]
            }
        }

        body = {"TableName": "GlobalTbl", "Item": {"pk": {"S": "item1"}, "val": {"N": "99"}}}
        replicate_write("GlobalTbl", "PutItem", body, "us-east-1", "123456789012", gt)

        # Check item exists in eu-west-1 with replication attrs
        from moto.backends import get_backend  # noqa: I001
        from moto.core import DEFAULT_ACCOUNT_ID

        target = get_backend("dynamodb")[DEFAULT_ACCOUNT_ID]["eu-west-1"]
        item = target.get_item("GlobalTbl", {"pk": {"S": "item1"}})
        assert item is not None

    def test_replicate_delete_item(self):
        _create_table_in_region("us-east-1", "DelTbl")
        _create_table_in_region("eu-west-1", "DelTbl")

        # Put an item in both regions first
        _put_item("us-east-1", "DelTbl", {"pk": {"S": "del1"}})
        _put_item("eu-west-1", "DelTbl", {"pk": {"S": "del1"}})

        gt = {
            ("123456789012", "DelTbl"): {
                "ReplicationGroup": [
                    {"RegionName": "us-east-1"},
                    {"RegionName": "eu-west-1"},
                ]
            }
        }

        body = {"TableName": "DelTbl", "Key": {"pk": {"S": "del1"}}}
        replicate_write("DelTbl", "DeleteItem", body, "us-east-1", "123456789012", gt)

        # The item should be gone in eu-west-1
        from moto.backends import get_backend  # noqa: I001
        from moto.core import DEFAULT_ACCOUNT_ID

        target = get_backend("dynamodb")[DEFAULT_ACCOUNT_ID]["eu-west-1"]
        item = target.get_item("DelTbl", {"pk": {"S": "del1"}})
        assert item is None

    def test_no_replication_for_non_global_table(self):
        _create_table_in_region("us-east-1", "LocalOnly")
        _create_table_in_region("eu-west-1", "LocalOnly")

        gt = {}  # empty — not a global table
        body = {"TableName": "LocalOnly", "Item": {"pk": {"S": "x"}}}
        replicate_write("LocalOnly", "PutItem", body, "us-east-1", "123456789012", gt)

        # Should not have replicated
        from moto.backends import get_backend  # noqa: I001
        from moto.core import DEFAULT_ACCOUNT_ID

        target = get_backend("dynamodb")[DEFAULT_ACCOUNT_ID]["eu-west-1"]
        item = target.get_item("LocalOnly", {"pk": {"S": "x"}})
        assert item is None

    def test_does_not_replicate_to_source_region(self):
        """Replication should skip the source region."""
        _create_table_in_region("us-east-1", "SrcTbl")

        gt = {
            ("123456789012", "SrcTbl"): {
                "ReplicationGroup": [
                    {"RegionName": "us-east-1"},
                ]
            }
        }

        body = {"TableName": "SrcTbl", "Item": {"pk": {"S": "x"}}}
        # Should not error — just a no-op
        replicate_write("SrcTbl", "PutItem", body, "us-east-1", "123456789012", gt)


class TestBackfillReplica:
    def test_backfill_copies_items(self):
        _create_table_in_region("us-east-1", "BfTbl")
        _create_table_in_region("eu-west-1", "BfTbl")

        _put_item("us-east-1", "BfTbl", {"pk": {"S": "a"}})
        _put_item("us-east-1", "BfTbl", {"pk": {"S": "b"}})

        count = backfill_replica("BfTbl", "us-east-1", "eu-west-1", "123456789012")
        assert count == 2

        # Items should exist in target
        from moto.backends import get_backend  # noqa: I001
        from moto.core import DEFAULT_ACCOUNT_ID

        target = get_backend("dynamodb")[DEFAULT_ACCOUNT_ID]["eu-west-1"]
        assert target.get_item("BfTbl", {"pk": {"S": "a"}}) is not None
        assert target.get_item("BfTbl", {"pk": {"S": "b"}}) is not None

    def test_backfill_empty_table(self):
        _create_table_in_region("us-east-1", "EmptyBf")
        _create_table_in_region("eu-west-1", "EmptyBf")

        count = backfill_replica("EmptyBf", "us-east-1", "eu-west-1", "123456789012")
        assert count == 0
