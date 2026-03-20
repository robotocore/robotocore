"""End-to-end tests for DynamoDB Global Tables lifecycle."""

import pytest

from moto import mock_aws
from robotocore.services.dynamodb.provider import (
    _create_global_table,
    _delete_global_table,
    _describe_global_table,
    _DynamoDBError,
    _global_tables,
    _global_tables_lock,
    _list_global_tables,
    _update_global_table,
)


@pytest.fixture(autouse=True)
def _clean_global_tables():
    """Reset global tables between tests."""
    with _global_tables_lock:
        _global_tables.clear()
    yield
    with _global_tables_lock:
        _global_tables.clear()


@pytest.fixture(autouse=True)
def _mock_aws_env():
    with mock_aws():
        yield


def _create_moto_table(region: str, table_name: str):
    """Create a DynamoDB table in Moto backend."""
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


def _get_moto_item(region: str, table_name: str, key: dict):
    from moto.backends import get_backend  # noqa: I001
    from moto.core import DEFAULT_ACCOUNT_ID

    backend = get_backend("dynamodb")[DEFAULT_ACCOUNT_ID][region]
    return backend.get_item(table_name, key)


def _put_moto_item(region: str, table_name: str, item: dict):
    from moto.backends import get_backend  # noqa: I001
    from moto.core import DEFAULT_ACCOUNT_ID

    backend = get_backend("dynamodb")[DEFAULT_ACCOUNT_ID][region]
    backend.put_item(table_name, item)


class TestGlobalTableLifecycle:
    """Full lifecycle: create -> describe -> list -> update -> delete."""

    def test_create_global_table(self):
        _create_moto_table("us-east-1", "Orders")
        result = _create_global_table(
            {
                "GlobalTableName": "Orders",
                "ReplicationGroup": [
                    {"RegionName": "us-east-1"},
                    {"RegionName": "eu-west-1"},
                ],
            },
            region="us-east-1",
            account_id="123456789012",
        )
        desc = result["GlobalTableDescription"]
        assert desc["GlobalTableName"] == "Orders"
        assert desc["GlobalTableStatus"] == "ACTIVE"
        assert len(desc["ReplicationGroup"]) == 2

    def test_create_duplicate_raises(self):
        _create_moto_table("us-east-1", "Dup")
        _create_global_table(
            {"GlobalTableName": "Dup", "ReplicationGroup": [{"RegionName": "us-east-1"}]},
            region="us-east-1",
            account_id="123456789012",
        )
        with pytest.raises(_DynamoDBError, match="already exists"):
            _create_global_table(
                {"GlobalTableName": "Dup", "ReplicationGroup": [{"RegionName": "us-east-1"}]},
                region="us-east-1",
                account_id="123456789012",
            )

    def test_describe_global_table(self):
        _create_moto_table("us-east-1", "Desc")
        _create_global_table(
            {"GlobalTableName": "Desc", "ReplicationGroup": [{"RegionName": "us-east-1"}]},
            region="us-east-1",
            account_id="123456789012",
        )
        result = _describe_global_table(
            {"GlobalTableName": "Desc"}, region="us-east-1", account_id="123456789012"
        )
        assert result["GlobalTableDescription"]["GlobalTableName"] == "Desc"

    def test_describe_nonexistent_raises(self):
        with pytest.raises(_DynamoDBError, match="does not exist"):
            _describe_global_table(
                {"GlobalTableName": "Nope"}, region="us-east-1", account_id="123456789012"
            )

    def test_list_global_tables(self):
        _create_moto_table("us-east-1", "Tbl1")
        _create_moto_table("us-east-1", "Tbl2")
        _create_global_table(
            {"GlobalTableName": "Tbl1", "ReplicationGroup": [{"RegionName": "us-east-1"}]},
            region="us-east-1",
            account_id="123456789012",
        )
        _create_global_table(
            {"GlobalTableName": "Tbl2", "ReplicationGroup": [{"RegionName": "us-east-1"}]},
            region="us-east-1",
            account_id="123456789012",
        )
        result = _list_global_tables({}, region="us-east-1", account_id="123456789012")
        names = [gt["GlobalTableName"] for gt in result["GlobalTables"]]
        assert "Tbl1" in names
        assert "Tbl2" in names

    def test_list_filters_by_region(self):
        _create_moto_table("us-east-1", "FilterTbl")
        _create_global_table(
            {
                "GlobalTableName": "FilterTbl",
                "ReplicationGroup": [{"RegionName": "us-east-1"}],
            },
            region="us-east-1",
            account_id="123456789012",
        )
        # Filter by a region that has no replicas
        result = _list_global_tables(
            {"RegionName": "ap-southeast-1"}, region="us-east-1", account_id="123456789012"
        )
        assert len(result["GlobalTables"]) == 0


class TestUpdateGlobalTable:
    def test_add_replica(self):
        _create_moto_table("us-east-1", "UpdTbl")
        _create_global_table(
            {"GlobalTableName": "UpdTbl", "ReplicationGroup": [{"RegionName": "us-east-1"}]},
            region="us-east-1",
            account_id="123456789012",
        )
        result = _update_global_table(
            {
                "GlobalTableName": "UpdTbl",
                "ReplicaUpdates": [{"Create": {"RegionName": "eu-west-1"}}],
            },
            region="us-east-1",
            account_id="123456789012",
        )
        regions = {r["RegionName"] for r in result["GlobalTableDescription"]["ReplicationGroup"]}
        assert "eu-west-1" in regions

        # Verify the table was created in the new region
        from moto.backends import get_backend  # noqa: I001
        from moto.core import DEFAULT_ACCOUNT_ID

        target = get_backend("dynamodb")[DEFAULT_ACCOUNT_ID]["eu-west-1"]
        assert target.get_table("UpdTbl") is not None

    def test_remove_replica(self):
        _create_moto_table("us-east-1", "RmTbl")
        _create_moto_table("eu-west-1", "RmTbl")
        _create_global_table(
            {
                "GlobalTableName": "RmTbl",
                "ReplicationGroup": [
                    {"RegionName": "us-east-1"},
                    {"RegionName": "eu-west-1"},
                ],
            },
            region="us-east-1",
            account_id="123456789012",
        )
        result = _update_global_table(
            {
                "GlobalTableName": "RmTbl",
                "ReplicaUpdates": [{"Delete": {"RegionName": "eu-west-1"}}],
            },
            region="us-east-1",
            account_id="123456789012",
        )
        regions = {r["RegionName"] for r in result["GlobalTableDescription"]["ReplicationGroup"]}
        assert "eu-west-1" not in regions

    def test_add_duplicate_replica_raises(self):
        _create_moto_table("us-east-1", "DupRep")
        _create_global_table(
            {
                "GlobalTableName": "DupRep",
                "ReplicationGroup": [{"RegionName": "us-east-1"}],
            },
            region="us-east-1",
            account_id="123456789012",
        )
        with pytest.raises(_DynamoDBError, match="already exists"):
            _update_global_table(
                {
                    "GlobalTableName": "DupRep",
                    "ReplicaUpdates": [{"Create": {"RegionName": "us-east-1"}}],
                },
                region="us-east-1",
                account_id="123456789012",
            )

    def test_remove_nonexistent_replica_raises(self):
        _create_moto_table("us-east-1", "NoRep")
        _create_global_table(
            {
                "GlobalTableName": "NoRep",
                "ReplicationGroup": [{"RegionName": "us-east-1"}],
            },
            region="us-east-1",
            account_id="123456789012",
        )
        with pytest.raises(_DynamoDBError, match="does not exist"):
            _update_global_table(
                {
                    "GlobalTableName": "NoRep",
                    "ReplicaUpdates": [{"Delete": {"RegionName": "ap-northeast-1"}}],
                },
                region="us-east-1",
                account_id="123456789012",
            )

    def test_update_nonexistent_global_table_raises(self):
        with pytest.raises(_DynamoDBError, match="does not exist"):
            _update_global_table(
                {
                    "GlobalTableName": "Ghost",
                    "ReplicaUpdates": [{"Create": {"RegionName": "us-west-2"}}],
                },
                region="us-east-1",
                account_id="123456789012",
            )

    def test_add_replica_backfills_existing_items(self):
        """Adding a new replica should copy existing items."""
        _create_moto_table("us-east-1", "BfTbl")
        _put_moto_item("us-east-1", "BfTbl", {"pk": {"S": "item1"}, "val": {"N": "100"}})
        _put_moto_item("us-east-1", "BfTbl", {"pk": {"S": "item2"}, "val": {"N": "200"}})

        _create_global_table(
            {"GlobalTableName": "BfTbl", "ReplicationGroup": [{"RegionName": "us-east-1"}]},
            region="us-east-1",
            account_id="123456789012",
        )
        _update_global_table(
            {
                "GlobalTableName": "BfTbl",
                "ReplicaUpdates": [{"Create": {"RegionName": "eu-west-1"}}],
            },
            region="us-east-1",
            account_id="123456789012",
        )

        # Both items should be in eu-west-1
        item1 = _get_moto_item("eu-west-1", "BfTbl", {"pk": {"S": "item1"}})
        item2 = _get_moto_item("eu-west-1", "BfTbl", {"pk": {"S": "item2"}})
        assert item1 is not None
        assert item2 is not None


class TestDeleteGlobalTable:
    def test_delete_global_table(self):
        _create_moto_table("us-east-1", "DelGt")
        _create_global_table(
            {"GlobalTableName": "DelGt", "ReplicationGroup": [{"RegionName": "us-east-1"}]},
            region="us-east-1",
            account_id="123456789012",
        )
        result = _delete_global_table(
            {"GlobalTableName": "DelGt"}, region="us-east-1", account_id="123456789012"
        )
        assert result["GlobalTableDescription"]["GlobalTableStatus"] == "DELETING"

        # Should be gone from the store
        with pytest.raises(_DynamoDBError, match="does not exist"):
            _describe_global_table(
                {"GlobalTableName": "DelGt"}, region="us-east-1", account_id="123456789012"
            )

    def test_delete_nonexistent_raises(self):
        with pytest.raises(_DynamoDBError, match="does not exist"):
            _delete_global_table(
                {"GlobalTableName": "Nope"}, region="us-east-1", account_id="123456789012"
            )

    def test_delete_with_multiple_replicas(self):
        _create_moto_table("us-east-1", "MultiDel")
        _create_global_table(
            {
                "GlobalTableName": "MultiDel",
                "ReplicationGroup": [
                    {"RegionName": "us-east-1"},
                    {"RegionName": "eu-west-1"},
                ],
            },
            region="us-east-1",
            account_id="123456789012",
        )
        result = _delete_global_table(
            {"GlobalTableName": "MultiDel"}, region="us-east-1", account_id="123456789012"
        )
        assert result["GlobalTableDescription"]["GlobalTableStatus"] == "DELETING"


class TestCreateReplicaOnGlobalTableCreation:
    def test_create_global_table_creates_replica_tables(self):
        """Creating a global table with multiple regions should create tables in all regions."""
        _create_moto_table("us-east-1", "AutoRep")
        _create_global_table(
            {
                "GlobalTableName": "AutoRep",
                "ReplicationGroup": [
                    {"RegionName": "us-east-1"},
                    {"RegionName": "eu-west-1"},
                    {"RegionName": "ap-southeast-1"},
                ],
            },
            region="us-east-1",
            account_id="123456789012",
        )

        from moto.backends import get_backend  # noqa: I001
        from moto.core import DEFAULT_ACCOUNT_ID

        eu_table = get_backend("dynamodb")[DEFAULT_ACCOUNT_ID]["eu-west-1"].get_table("AutoRep")
        ap_table = get_backend("dynamodb")[DEFAULT_ACCOUNT_ID]["ap-southeast-1"].get_table(
            "AutoRep"
        )
        assert eu_table is not None
        assert ap_table is not None


class TestAccountIsolation:
    def test_different_accounts_independent(self):
        """Global tables from different accounts should be independent."""
        _create_moto_table("us-east-1", "Shared")
        _create_global_table(
            {"GlobalTableName": "Shared", "ReplicationGroup": [{"RegionName": "us-east-1"}]},
            region="us-east-1",
            account_id="111111111111",
        )

        # Different account should be able to create the same name
        _create_global_table(
            {"GlobalTableName": "Shared", "ReplicationGroup": [{"RegionName": "us-east-1"}]},
            region="us-east-1",
            account_id="222222222222",
        )

        # List for account 1 should only show its table
        result = _list_global_tables({}, region="us-east-1", account_id="111111111111")
        assert len(result["GlobalTables"]) == 1
