"""DynamoDB Global Tables cross-region replication engine.

When a global table is created, this module ensures:
1. The table is created in each replica region
2. Writes (PutItem, UpdateItem, DeleteItem) are replicated to all replica regions
3. System attributes (aws:rep:*) are added to replicated items
"""

import json
import logging
import threading
import time

logger = logging.getLogger(__name__)

# Lock for thread-safe replication
_replication_lock = threading.Lock()

# Track which tables are currently being replicated to avoid infinite loops
_replicating: set[tuple[str, str, str]] = set()  # (table_name, region, account_id)


def get_replica_regions(
    table_name: str, account_id: str, global_tables: dict[tuple[str, str], dict]
) -> list[str]:
    """Get all replica regions for a global table."""
    key = (account_id, table_name)
    gt = global_tables.get(key)
    if not gt:
        return []
    return [r["RegionName"] for r in gt.get("ReplicationGroup", [])]


def replicate_write(
    table_name: str,
    operation: str,
    body: dict,
    source_region: str,
    account_id: str,
    global_tables: dict[tuple[str, str], dict],
) -> None:
    """Replicate a write operation to all other replica regions.

    Args:
        table_name: The DynamoDB table name.
        operation: The DynamoDB operation (PutItem, UpdateItem, DeleteItem).
        body: The original request body.
        source_region: The region where the write originated.
        account_id: The AWS account ID.
        global_tables: Reference to the global tables store.
    """
    replica_regions = get_replica_regions(table_name, account_id, global_tables)
    if not replica_regions:
        return

    for target_region in replica_regions:
        if target_region == source_region:
            continue

        # Check if we're already replicating to this target to avoid loops
        repl_key = (table_name, target_region, account_id)
        with _replication_lock:
            if repl_key in _replicating:
                continue
            _replicating.add(repl_key)

        try:
            _replicate_to_region(
                table_name=table_name,
                operation=operation,
                body=body,
                target_region=target_region,
                source_region=source_region,
                account_id=account_id,
            )
        except Exception:  # noqa: BLE001
            logger.debug(
                "Failed to replicate %s to %s for table %s",
                operation,
                target_region,
                table_name,
                exc_info=True,
            )
        finally:
            with _replication_lock:
                _replicating.discard(repl_key)


def _replicate_to_region(
    table_name: str,
    operation: str,
    body: dict,
    target_region: str,
    source_region: str,
    account_id: str,
) -> None:
    """Replicate a single write to a target region via Moto backend."""
    from moto.backends import get_backend  # noqa: I001
    from moto.core import DEFAULT_ACCOUNT_ID

    acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID
    backend = get_backend("dynamodb")[acct][target_region]

    now = time.time()

    if operation == "PutItem":
        item = body.get("Item", {})
        item_with_rep = _add_replication_attrs(item, source_region, now)
        backend.put_item(table_name, item_with_rep)
        logger.debug("Replicated PutItem to %s for table %s", target_region, table_name)

    elif operation == "DeleteItem":
        key = body.get("Key", {})
        backend.delete_item(table_name, key)
        logger.debug("Replicated DeleteItem to %s for table %s", target_region, table_name)

    elif operation == "UpdateItem":
        # UpdateItem is more complex — replicate by reading the updated item
        # from the source region and putting it in the target
        _replicate_updated_item(
            table_name=table_name,
            key=body.get("Key", {}),
            source_region=source_region,
            target_region=target_region,
            account_id=account_id,
            now=now,
        )


def _replicate_updated_item(
    table_name: str,
    key: dict,
    source_region: str,
    target_region: str,
    account_id: str,
    now: float,
) -> None:
    """Replicate an UpdateItem by reading from source and putting to target."""
    from moto.backends import get_backend  # noqa: I001
    from moto.core import DEFAULT_ACCOUNT_ID

    acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID

    # Read the item from source region
    source_backend = get_backend("dynamodb")[acct][source_region]
    item = source_backend.get_item(table_name, key)
    if item is None:
        return

    # Convert DynamoItem to dict if needed
    item_dict = _item_to_dict(item)
    if item_dict:
        item_with_rep = _add_replication_attrs(item_dict, source_region, now)
        target_backend = get_backend("dynamodb")[acct][target_region]
        target_backend.put_item(table_name, item_with_rep)
        logger.debug("Replicated UpdateItem to %s for table %s", target_region, table_name)


def _item_to_dict(item) -> dict | None:
    """Convert a Moto DynamoItem to a DynamoDB-format dict."""
    if item is None:
        return None
    if isinstance(item, dict):
        return item
    # Moto DynamoItem has an .attrs dict of DynamoType objects
    if hasattr(item, "attrs"):
        result = {}
        for attr_name, attr_value in item.attrs.items():
            if hasattr(attr_value, "to_json"):
                val = attr_value.to_json()
                if isinstance(val, str):
                    result[attr_name] = json.loads(val)
                elif isinstance(val, dict):
                    result[attr_name] = val
                else:
                    result[attr_name] = {"S": str(val)}
            elif isinstance(attr_value, dict):
                result[attr_name] = attr_value
            else:
                result[attr_name] = {"S": str(attr_value)}
        return result
    return None


def _add_replication_attrs(item: dict, source_region: str, timestamp: float) -> dict:
    """Add aws:rep:* system attributes to a replicated item."""
    item = dict(item)  # shallow copy
    item["aws:rep:deleting"] = {"BOOL": False}
    item["aws:rep:updatetime"] = {"N": str(timestamp)}
    item["aws:rep:updateregion"] = {"S": source_region}
    return item


def create_replica_table(
    table_name: str,
    source_region: str,
    target_region: str,
    account_id: str,
) -> bool:
    """Create a replica table in the target region by copying the source table's schema.

    Returns True if the table was created successfully.
    """
    from moto.backends import get_backend  # noqa: I001
    from moto.core import DEFAULT_ACCOUNT_ID

    acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID

    try:
        source_backend = get_backend("dynamodb")[acct][source_region]
        source_table = source_backend.get_table(table_name)
        if source_table is None:
            logger.warning("Source table %s not found in %s", table_name, source_region)
            return False

        target_backend = get_backend("dynamodb")[acct][target_region]

        # Check if table already exists in target
        try:
            existing = target_backend.get_table(table_name)
        except Exception:  # noqa: BLE001
            existing = None
        if existing is not None:
            logger.debug(
                "Table %s already exists in %s, skipping creation",
                table_name,
                target_region,
            )
            return True

        # Build the create_table params from source table schema
        key_schema = []
        attr_defs = []

        if source_table.hash_key_attr:
            key_schema.append(
                {
                    "AttributeName": source_table.hash_key_attr,
                    "KeyType": "HASH",
                }
            )
            attr_defs.append(
                {
                    "AttributeName": source_table.hash_key_attr,
                    "AttributeType": source_table.hash_key_type or "S",
                }
            )

        if source_table.range_key_attr:
            key_schema.append(
                {
                    "AttributeName": source_table.range_key_attr,
                    "KeyType": "RANGE",
                }
            )
            attr_defs.append(
                {
                    "AttributeName": source_table.range_key_attr,
                    "AttributeType": source_table.range_key_type or "S",
                }
            )

        target_backend.create_table(
            table_name,
            schema=key_schema,
            throughput=None,
            attr=attr_defs,
            global_indexes=None,
            indexes=None,
            streams=None,
            billing_mode="PAY_PER_REQUEST",
            sse_specification=None,
            tags=[],
            deletion_protection_enabled=None,
            warm_throughput=None,
        )

        logger.info(
            "Created replica table %s in %s (from %s)",
            table_name,
            target_region,
            source_region,
        )
        return True

    except Exception:  # noqa: BLE001
        logger.warning(
            "Failed to create replica table %s in %s",
            table_name,
            target_region,
            exc_info=True,
        )
        return False


def backfill_replica(
    table_name: str,
    source_region: str,
    target_region: str,
    account_id: str,
) -> int:
    """Copy all items from source table to target table.

    Returns the number of items copied.
    """
    from moto.backends import get_backend  # noqa: I001
    from moto.core import DEFAULT_ACCOUNT_ID

    acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID
    count = 0
    now = time.time()

    try:
        source_backend = get_backend("dynamodb")[acct][source_region]
        target_backend = get_backend("dynamodb")[acct][target_region]

        source_table = source_backend.get_table(table_name)
        if source_table is None:
            return 0

        # Iterate all items in the source table
        for item_hash_key, items in source_table.items.items():
            if isinstance(items, dict):
                for _range_key, item in items.items():
                    item_dict = _item_to_dict(item)
                    if item_dict:
                        item_with_rep = _add_replication_attrs(item_dict, source_region, now)
                        target_backend.put_item(table_name, item_with_rep)
                        count += 1
            else:
                item_dict = _item_to_dict(items)
                if item_dict:
                    item_with_rep = _add_replication_attrs(item_dict, source_region, now)
                    target_backend.put_item(table_name, item_with_rep)
                    count += 1

        logger.info(
            "Backfilled %d items from %s:%s to %s",
            count,
            source_region,
            table_name,
            target_region,
        )

    except Exception:  # noqa: BLE001
        logger.warning(
            "Failed to backfill replica %s from %s to %s",
            table_name,
            source_region,
            target_region,
            exc_info=True,
        )

    return count


def delete_replica_table(
    table_name: str,
    target_region: str,
    account_id: str,
) -> bool:
    """Delete a replica table from a region.

    Returns True if deleted successfully.
    """
    from moto.backends import get_backend  # noqa: I001
    from moto.core import DEFAULT_ACCOUNT_ID

    acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID

    try:
        target_backend = get_backend("dynamodb")[acct][target_region]
        target_backend.delete_table(table_name)
        logger.info("Deleted replica table %s in %s", table_name, target_region)
        return True
    except Exception:  # noqa: BLE001
        logger.debug(
            "Failed to delete replica table %s in %s",
            table_name,
            target_region,
            exc_info=True,
        )
        return False
