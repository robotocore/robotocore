"""Advanced tests for DynamoDB global table replication engine."""

from unittest.mock import MagicMock, patch

from robotocore.services.dynamodb.replication import (
    _add_replication_attrs,
    _item_to_dict,
    _replicating,
    _replication_lock,
    get_replica_regions,
    replicate_write,
)


def _make_global_tables(account_id, table_name, regions):
    """Build a global_tables dict for testing."""
    return {
        (account_id, table_name): {
            "ReplicationGroup": [{"RegionName": r} for r in regions],
        }
    }


class TestReplicationWithConditionalWrites:
    """Replication with conditional writes (PutItem with ConditionExpression)."""

    def test_put_item_replicates_to_other_region(self):
        gt = _make_global_tables("123456789012", "my-table", ["us-east-1", "eu-west-1"])
        mock_backend = MagicMock()

        with patch("moto.backends.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            replicate_write(
                table_name="my-table",
                operation="PutItem",
                body={"Item": {"pk": {"S": "key1"}, "data": {"S": "val1"}}},
                source_region="us-east-1",
                account_id="123456789012",
                global_tables=gt,
            )

        # Should replicate to eu-west-1 only (not back to us-east-1)
        mock_backend.put_item.assert_called_once()
        call_args = mock_backend.put_item.call_args[0]
        assert call_args[0] == "my-table"
        item = call_args[1]
        assert item["pk"] == {"S": "key1"}
        # Should have replication attributes
        assert "aws:rep:updateregion" in item
        assert item["aws:rep:updateregion"]["S"] == "us-east-1"

    def test_delete_item_replicates(self):
        gt = _make_global_tables("123456789012", "t", ["us-east-1", "us-west-2"])
        mock_backend = MagicMock()

        with patch("moto.backends.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            replicate_write(
                table_name="t",
                operation="DeleteItem",
                body={"Key": {"pk": {"S": "k1"}}},
                source_region="us-east-1",
                account_id="123456789012",
                global_tables=gt,
            )

        mock_backend.delete_item.assert_called_once_with("t", {"pk": {"S": "k1"}})


class TestBatchWritesReplicate:
    """Batch writes replicate all items via multiple replicate_write calls."""

    def test_multiple_puts_all_replicated(self):
        gt = _make_global_tables("123456789012", "batch-tbl", ["us-east-1", "ap-southeast-1"])
        mock_backend = MagicMock()

        with patch("moto.backends.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend

            for i in range(5):
                replicate_write(
                    table_name="batch-tbl",
                    operation="PutItem",
                    body={"Item": {"pk": {"S": f"key-{i}"}}},
                    source_region="us-east-1",
                    account_id="123456789012",
                    global_tables=gt,
                )

        assert mock_backend.put_item.call_count == 5


class TestReplicationLoopPrevention:
    """Replication loop prevention: replicated writes don't trigger further replication."""

    def test_replicating_set_prevents_recursion(self):
        """When a region is in _replicating, writes to it are skipped."""
        gt = _make_global_tables("123456789012", "loop-tbl", ["us-east-1", "eu-west-1"])
        mock_backend = MagicMock()

        # Pre-mark eu-west-1 as being replicated to
        repl_key = ("loop-tbl", "eu-west-1", "123456789012")
        with _replication_lock:
            _replicating.add(repl_key)

        try:
            with patch("moto.backends.get_backend") as mock_gb:
                mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = (
                    mock_backend
                )
                replicate_write(
                    table_name="loop-tbl",
                    operation="PutItem",
                    body={"Item": {"pk": {"S": "x"}}},
                    source_region="us-east-1",
                    account_id="123456789012",
                    global_tables=gt,
                )

            # No replication should have occurred since eu-west-1 was already in _replicating
            mock_backend.put_item.assert_not_called()
        finally:
            with _replication_lock:
                _replicating.discard(repl_key)

    def test_replication_key_cleaned_up_after_write(self):
        """After replication completes, the key is removed from _replicating."""
        gt = _make_global_tables("123456789012", "cleanup-tbl", ["us-east-1", "eu-west-1"])
        mock_backend = MagicMock()

        with patch("moto.backends.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            replicate_write(
                table_name="cleanup-tbl",
                operation="PutItem",
                body={"Item": {"pk": {"S": "x"}}},
                source_region="us-east-1",
                account_id="123456789012",
                global_tables=gt,
            )

        # After replicate_write, the key should be removed from _replicating
        repl_key = ("cleanup-tbl", "eu-west-1", "123456789012")
        assert repl_key not in _replicating


class TestReplicationAttributes:
    """Replication adds aws:rep:* attributes."""

    def test_add_replication_attrs(self):
        item = {"pk": {"S": "k1"}, "data": {"N": "42"}}
        result = _add_replication_attrs(item, "us-east-1", 1234567890.0)
        assert result["aws:rep:deleting"] == {"BOOL": False}
        assert result["aws:rep:updatetime"] == {"N": "1234567890.0"}
        assert result["aws:rep:updateregion"] == {"S": "us-east-1"}
        # Original item should not be modified
        assert "aws:rep:deleting" not in item

    def test_add_replication_attrs_preserves_existing(self):
        item = {"pk": {"S": "k1"}, "extra": {"S": "val"}}
        result = _add_replication_attrs(item, "eu-west-1", 999.0)
        assert result["pk"] == {"S": "k1"}
        assert result["extra"] == {"S": "val"}


class TestGetReplicaRegions:
    """Get replica regions from global tables config."""

    def test_returns_all_regions(self):
        gt = _make_global_tables("acct", "tbl", ["us-east-1", "eu-west-1", "ap-southeast-1"])
        regions = get_replica_regions("tbl", "acct", gt)
        assert set(regions) == {"us-east-1", "eu-west-1", "ap-southeast-1"}

    def test_missing_table_returns_empty(self):
        gt = {}
        regions = get_replica_regions("nonexistent", "acct", gt)
        assert regions == []


class TestReplicationSkipsSourceRegion:
    """Replication skips the source region itself."""

    def test_no_self_replication(self):
        gt = _make_global_tables("123456789012", "tbl", ["us-east-1", "eu-west-1", "ap-south-1"])

        # replicate_write should call _replicate_to_region for each non-source region.
        # Verify by patching _replicate_to_region directly.
        with patch(
            "robotocore.services.dynamodb.replication._replicate_to_region"
        ) as mock_replicate:
            replicate_write(
                table_name="tbl",
                operation="PutItem",
                body={"Item": {"pk": {"S": "x"}}},
                source_region="us-east-1",
                account_id="123456789012",
                global_tables=gt,
            )

        # Should replicate to eu-west-1 and ap-south-1, not us-east-1
        assert mock_replicate.call_count == 2
        called_regions = {c[1]["target_region"] for c in mock_replicate.call_args_list}
        assert called_regions == {"eu-west-1", "ap-south-1"}


class TestItemToDict:
    """_item_to_dict conversion edge cases."""

    def test_dict_passthrough(self):
        assert _item_to_dict({"pk": {"S": "k1"}}) == {"pk": {"S": "k1"}}

    def test_none_returns_none(self):
        assert _item_to_dict(None) is None

    def test_object_with_attrs(self):
        mock_item = MagicMock()
        mock_attr = MagicMock()
        mock_attr.to_json.return_value = '{"S": "hello"}'
        mock_item.attrs = {"name": mock_attr}
        result = _item_to_dict(mock_item)
        assert result is not None
        assert result["name"] == {"S": "hello"}

    def test_object_without_attrs_returns_none(self):
        """An object with no attrs attribute returns None."""
        obj = object()
        assert _item_to_dict(obj) is None


class TestReplicationFailureHandling:
    """Replication failures are logged but don't crash."""

    def test_backend_exception_swallowed(self):
        gt = _make_global_tables("123456789012", "tbl", ["us-east-1", "eu-west-1"])

        with patch(
            "moto.backends.get_backend",
            side_effect=RuntimeError("backend down"),
        ):
            # Should not raise
            replicate_write(
                table_name="tbl",
                operation="PutItem",
                body={"Item": {"pk": {"S": "x"}}},
                source_region="us-east-1",
                account_id="123456789012",
                global_tables=gt,
            )
