"""DynamoDB provider -- wraps Moto with stream mutation hooks."""

import json
import logging
import threading

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto

logger = logging.getLogger(__name__)

# Operations that mutate items and should trigger stream events
_MUTATION_OPS = {
    "DynamoDB_20120810.PutItem",
    "DynamoDB_20120810.UpdateItem",
    "DynamoDB_20120810.DeleteItem",
    "DynamoDB_20120810.BatchWriteItem",
    "DynamoDB_20120810.TransactWriteItems",
}

# In-memory global tables store, keyed by (account_id, name) for proper isolation
_global_tables: dict[tuple[str, str], dict] = {}
_global_tables_lock = threading.Lock()


async def handle_dynamodb_request(request: Request, region: str, account_id: str) -> Response:
    """Handle DynamoDB requests, forwarding to Moto and firing stream hooks on mutations."""
    body_bytes = await request.body()
    target = request.headers.get("x-amz-target", "")
    op = target.split(".")[-1] if "." in target else ""

    # Intercept operations not implemented in Moto
    intercepted = _INTERCEPT_OPS.get(op)
    if intercepted:
        try:
            params = json.loads(body_bytes) if body_bytes else {}
            result = intercepted(params, region, account_id)
            return Response(
                content=json.dumps(result),
                status_code=200,
                media_type="application/x-amz-json-1.0",
            )
        except _DynamoDBError as e:
            return Response(
                content=json.dumps({"__type": e.code, "message": e.message}),
                status_code=400,
                media_type="application/x-amz-json-1.0",
            )

    # Forward to Moto
    response = await forward_to_moto(request, "dynamodb", account_id=account_id)

    # Only fire hooks on successful mutations
    if target in _MUTATION_OPS and 200 <= response.status_code < 300:
        try:
            _fire_stream_hooks(target, body_bytes, response, region, account_id)
        except Exception:  # noqa: BLE001
            logger.debug("Failed to fire stream hooks for %s", target, exc_info=True)

        # Replicate writes to global table replicas
        try:
            _replicate_mutation(target, body_bytes, region, account_id)
        except Exception:  # noqa: BLE001
            logger.debug("Failed to replicate mutation for %s", target, exc_info=True)

    return response


def _fire_stream_hooks(
    target: str, body_bytes: bytes, response: Response, region: str, account_id: str
) -> None:
    """Parse the request and fire appropriate stream hooks."""
    from robotocore.services.dynamodbstreams.hooks import notify_table_change

    body = json.loads(body_bytes)
    op = target.split(".")[-1]

    if op == "PutItem":
        table_name = body.get("TableName", "")
        item = body.get("Item", {})
        keys = _extract_keys_from_item(table_name, item, region, account_id)
        # Determine INSERT vs MODIFY: if the Moto response contains Attributes,
        # an existing item was overwritten (MODIFY). Otherwise it's a new item (INSERT).
        resp_body = json.loads(response.body) if response.body else {}
        event_name = "MODIFY" if resp_body.get("Attributes") else "INSERT"
        old_image = resp_body.get("Attributes")
        notify_table_change(
            table_name=table_name,
            event_name=event_name,
            keys=keys,
            new_image=item,
            old_image=old_image,
            region=region,
            account_id=account_id,
        )

    elif op == "DeleteItem":
        table_name = body.get("TableName", "")
        keys = body.get("Key", {})
        notify_table_change(
            table_name=table_name,
            event_name="REMOVE",
            keys=keys,
            new_image=None,
            old_image=None,
            region=region,
            account_id=account_id,
        )

    elif op == "UpdateItem":
        table_name = body.get("TableName", "")
        keys = body.get("Key", {})
        notify_table_change(
            table_name=table_name,
            event_name="MODIFY",
            keys=keys,
            new_image=None,
            old_image=None,
            region=region,
            account_id=account_id,
        )

    elif op == "BatchWriteItem":
        request_items = body.get("RequestItems", {})
        for table_name, requests in request_items.items():
            for req in requests:
                if "PutRequest" in req:
                    item = req["PutRequest"].get("Item", {})
                    keys = _extract_keys_from_item(table_name, item, region, account_id)
                    # Check if item already exists to determine INSERT vs MODIFY
                    event_name = "INSERT"
                    old_image = None
                    try:
                        existing = _get_existing_item(table_name, keys, region, account_id)
                        if existing:
                            event_name = "MODIFY"
                            old_image = existing
                    except Exception as exc:  # noqa: BLE001
                        logger.debug(
                            "_fire_stream_hooks: _get_existing_item failed (non-fatal): %s", exc
                        )
                    notify_table_change(
                        table_name=table_name,
                        event_name=event_name,
                        keys=keys,
                        new_image=item,
                        old_image=old_image,
                        region=region,
                        account_id=account_id,
                    )
                elif "DeleteRequest" in req:
                    keys = req["DeleteRequest"].get("Key", {})
                    notify_table_change(
                        table_name=table_name,
                        event_name="REMOVE",
                        keys=keys,
                        new_image=None,
                        old_image=None,
                        region=region,
                        account_id=account_id,
                    )

    elif op == "TransactWriteItems":
        for item in body.get("TransactItems", []):
            if "Put" in item:
                put = item["Put"]
                table_name = put.get("TableName", "")
                put_item = put.get("Item", {})
                keys = _extract_keys_from_item(table_name, put_item, region, account_id)
                notify_table_change(
                    table_name=table_name,
                    event_name="INSERT",
                    keys=keys,
                    new_image=put_item,
                    old_image=None,
                    region=region,
                    account_id=account_id,
                )
            elif "Delete" in item:
                delete = item["Delete"]
                table_name = delete.get("TableName", "")
                keys = delete.get("Key", {})
                notify_table_change(
                    table_name=table_name,
                    event_name="REMOVE",
                    keys=keys,
                    new_image=None,
                    old_image=None,
                    region=region,
                    account_id=account_id,
                )
            elif "Update" in item:
                update = item["Update"]
                table_name = update.get("TableName", "")
                keys = update.get("Key", {})
                # Try to get current item image for stream consumers
                new_image = None
                old_image = None
                try:
                    existing = _get_existing_item(table_name, keys, region, account_id)
                    if existing:
                        # After the update, the item has been modified
                        new_image = existing  # Best effort: current state after update
                        old_image = existing  # Approximate: we don't have pre-update state
                except Exception as exc:  # noqa: BLE001
                    logger.debug(
                        "_fire_stream_hooks: _get_existing_item failed (non-fatal): %s", exc
                    )
                notify_table_change(
                    table_name=table_name,
                    event_name="MODIFY",
                    keys=keys,
                    new_image=new_image,
                    old_image=old_image,
                    region=region,
                    account_id=account_id,
                )


def _replicate_mutation(target: str, body_bytes: bytes, region: str, account_id: str) -> None:
    """Replicate a mutation to all global table replicas."""
    from robotocore.services.dynamodb.replication import replicate_write

    body = json.loads(body_bytes)
    op = target.split(".")[-1]
    table_name = body.get("TableName", "")

    if op in ("PutItem", "UpdateItem", "DeleteItem"):
        replicate_write(
            table_name=table_name,
            operation=op,
            body=body,
            source_region=region,
            account_id=account_id,
            global_tables=_global_tables,
        )
    elif op == "BatchWriteItem":
        for tbl_name, requests in body.get("RequestItems", {}).items():
            for req in requests:
                if "PutRequest" in req:
                    replicate_write(
                        table_name=tbl_name,
                        operation="PutItem",
                        body={"TableName": tbl_name, "Item": req["PutRequest"]["Item"]},
                        source_region=region,
                        account_id=account_id,
                        global_tables=_global_tables,
                    )
                elif "DeleteRequest" in req:
                    replicate_write(
                        table_name=tbl_name,
                        operation="DeleteItem",
                        body={"TableName": tbl_name, "Key": req["DeleteRequest"]["Key"]},
                        source_region=region,
                        account_id=account_id,
                        global_tables=_global_tables,
                    )
    elif op == "TransactWriteItems":
        for item in body.get("TransactItems", []):
            if "Put" in item:
                put = item["Put"]
                replicate_write(
                    table_name=put.get("TableName", ""),
                    operation="PutItem",
                    body={"TableName": put.get("TableName", ""), "Item": put.get("Item", {})},
                    source_region=region,
                    account_id=account_id,
                    global_tables=_global_tables,
                )
            elif "Delete" in item:
                delete = item["Delete"]
                replicate_write(
                    table_name=delete.get("TableName", ""),
                    operation="DeleteItem",
                    body={
                        "TableName": delete.get("TableName", ""),
                        "Key": delete.get("Key", {}),
                    },
                    source_region=region,
                    account_id=account_id,
                    global_tables=_global_tables,
                )
            elif "Update" in item:
                update = item["Update"]
                replicate_write(
                    table_name=update.get("TableName", ""),
                    operation="UpdateItem",
                    body={
                        "TableName": update.get("TableName", ""),
                        "Key": update.get("Key", {}),
                    },
                    source_region=region,
                    account_id=account_id,
                    global_tables=_global_tables,
                )


def _get_existing_item(table_name: str, keys: dict, region: str, account_id: str) -> dict | None:
    """Try to get an existing item from Moto's backend by key."""
    try:
        from moto.backends import get_backend
        from moto.core import DEFAULT_ACCOUNT_ID

        acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID
        backend = get_backend("dynamodb")[acct][region]
        result = backend.get_item(table_name, keys)
        if result and hasattr(result, "to_json"):
            return result.to_json().get("Attributes", result.to_json())
    except Exception as exc:  # noqa: BLE001
        logger.debug("_get_existing_item: get_item failed (non-fatal): %s", exc)
    return None


def _extract_keys_from_item(table_name: str, item: dict, region: str, account_id: str) -> dict:
    """Extract just the key attributes from a full item, using Moto's table key schema."""
    try:
        from moto.backends import get_backend
        from moto.core import DEFAULT_ACCOUNT_ID

        acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID
        backend = get_backend("dynamodb")[acct][region]
        table = backend.get_table(table_name)
        if table:
            keys = {}
            if table.hash_key_attr:
                keys[table.hash_key_attr] = item.get(table.hash_key_attr, {})
            if table.range_key_attr is not None:
                keys[table.range_key_attr] = item.get(table.range_key_attr, {})
            if keys:
                return keys
    except Exception as exc:  # noqa: BLE001
        logger.debug("_extract_keys_from_item: get_table failed (non-fatal): %s", exc)
    return item  # Fallback: return full item as keys


# ---------------------------------------------------------------------------
# Intercepted operations (not implemented in Moto)
# ---------------------------------------------------------------------------


class _DynamoDBError(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message


def _create_global_table(params: dict, region: str, account_id: str) -> dict:
    from robotocore.services.dynamodb.replication import create_replica_table

    name = params.get("GlobalTableName", "")
    replication_group = params.get("ReplicationGroup", [])
    key = (account_id, name)
    with _global_tables_lock:
        if key in _global_tables:
            raise _DynamoDBError(
                "GlobalTableAlreadyExistsException",
                f"Global table with name '{name}' already exists",
            )
        import time

        desc = {
            "GlobalTableName": name,
            "ReplicationGroup": replication_group,
            "GlobalTableArn": f"arn:aws:dynamodb::{account_id}:global-table/{name}",
            "CreationDateTime": time.time(),
            "GlobalTableStatus": "ACTIVE",
        }
        _global_tables[key] = desc

    # Create replica tables in each region (outside the lock to avoid deadlock)
    for replica in replication_group:
        target_region = replica["RegionName"]
        if target_region != region:
            create_replica_table(name, region, target_region, account_id)

    return {"GlobalTableDescription": desc}


def _describe_global_table(params: dict, region: str, account_id: str) -> dict:
    name = params.get("GlobalTableName", "")
    key = (account_id, name)
    with _global_tables_lock:
        if key not in _global_tables:
            raise _DynamoDBError(
                "GlobalTableNotFoundException",
                f"Global table with name '{name}' does not exist",
            )
        return {"GlobalTableDescription": _global_tables[key]}


def _list_global_tables(params: dict, region: str, account_id: str) -> dict:
    region_filter = params.get("RegionName")
    limit = params.get("Limit", 100)
    last_evaluated = params.get("LastEvaluatedGlobalTableName")

    with _global_tables_lock:
        # Filter to this account's tables only
        account_tables = [
            gt for (acct, _name), gt in sorted(_global_tables.items()) if acct == account_id
        ]

    # Filter by region if requested
    if region_filter:
        account_tables = [
            gt
            for gt in account_tables
            if any(r.get("RegionName") == region_filter for r in gt.get("ReplicationGroup", []))
        ]

    # Pagination: skip past last_evaluated
    if last_evaluated:
        found = False
        filtered = []
        for gt in account_tables:
            if found:
                filtered.append(gt)
            if gt["GlobalTableName"] == last_evaluated:
                found = True
        account_tables = filtered

    # Check if there are more results before slicing
    total_count = len(account_tables)

    # Apply limit
    account_tables = account_tables[:limit]

    result: dict = {
        "GlobalTables": [
            {"GlobalTableName": gt["GlobalTableName"], "ReplicationGroup": gt["ReplicationGroup"]}
            for gt in account_tables
        ]
    }

    # Add pagination token if there are more results
    if len(account_tables) == limit and total_count > limit:
        result["LastEvaluatedGlobalTableName"] = account_tables[-1]["GlobalTableName"]

    return result


def _table_exists(table_name: str, region: str, account_id: str) -> bool:
    """Check if a DynamoDB table exists in Moto's backend."""
    if not table_name:
        return False
    try:
        from moto.backends import get_backend
        from moto.core import DEFAULT_ACCOUNT_ID

        acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID
        backend = get_backend("dynamodb")[acct][region]
        table = backend.get_table(table_name)
        return table is not None
    except Exception:  # noqa: BLE001
        return False


def _describe_table_replica_auto_scaling(params: dict, region: str, account_id: str) -> dict:
    table_name = params.get("TableName", "")
    if not _table_exists(table_name, region, account_id):
        raise _DynamoDBError(
            "ResourceNotFoundException",
            f"Requested resource not found: Table: {table_name} not found",
        )
    return {
        "TableAutoScalingDescription": {
            "TableName": table_name,
            "TableStatus": "ACTIVE",
            "Replicas": [
                {
                    "RegionName": region,
                    "ReplicaStatus": "ACTIVE",
                    "ReplicaAutoScalingPolicies": [],
                }
            ],
        }
    }


def _update_global_table(params: dict, region: str, account_id: str) -> dict:
    """Add or remove replicas from a global table."""
    from robotocore.services.dynamodb.replication import (
        backfill_replica,
        create_replica_table,
        delete_replica_table,
    )

    name = params.get("GlobalTableName", "")
    updates = params.get("ReplicaUpdates", [])
    key = (account_id, name)

    with _global_tables_lock:
        if key not in _global_tables:
            raise _DynamoDBError(
                "GlobalTableNotFoundException",
                f"Global table with name '{name}' does not exist",
            )
        gt = _global_tables[key]
        current_regions = {r["RegionName"] for r in gt.get("ReplicationGroup", [])}

        for update in updates:
            if "Create" in update:
                new_region = update["Create"]["RegionName"]
                if new_region in current_regions:
                    raise _DynamoDBError(
                        "ReplicaAlreadyExistsException",
                        f"Replica already exists in region '{new_region}'",
                    )
                # Determine a source region for creating the replica
                source_region = next(iter(current_regions)) if current_regions else region
                # Create the table in the new region
                create_replica_table(name, source_region, new_region, account_id)
                # Backfill existing items
                backfill_replica(name, source_region, new_region, account_id)
                gt["ReplicationGroup"].append({"RegionName": new_region})
                current_regions.add(new_region)

            elif "Delete" in update:
                del_region = update["Delete"]["RegionName"]
                if del_region not in current_regions:
                    raise _DynamoDBError(
                        "ReplicaNotFoundException",
                        f"Replica does not exist in region '{del_region}'",
                    )
                delete_replica_table(name, del_region, account_id)
                gt["ReplicationGroup"] = [
                    r for r in gt["ReplicationGroup"] if r["RegionName"] != del_region
                ]
                current_regions.discard(del_region)

        _global_tables[key] = gt

    return {"GlobalTableDescription": gt}


def _delete_global_table(params: dict, region: str, account_id: str) -> dict:
    """Delete a global table and all its replicas."""
    from robotocore.services.dynamodb.replication import delete_replica_table

    name = params.get("GlobalTableName", "")
    key = (account_id, name)

    with _global_tables_lock:
        if key not in _global_tables:
            raise _DynamoDBError(
                "GlobalTableNotFoundException",
                f"Global table with name '{name}' does not exist",
            )
        gt = _global_tables.pop(key)

    # Delete replica tables in all regions
    for replica in gt.get("ReplicationGroup", []):
        delete_replica_table(name, replica["RegionName"], account_id)

    gt["GlobalTableStatus"] = "DELETING"
    return {"GlobalTableDescription": gt}


_INTERCEPT_OPS: dict = {
    "CreateGlobalTable": _create_global_table,
    "DescribeGlobalTable": _describe_global_table,
    "ListGlobalTables": _list_global_tables,
    "UpdateGlobalTable": _update_global_table,
    "DeleteGlobalTable": _delete_global_table,
    "DescribeTableReplicaAutoScaling": _describe_table_replica_auto_scaling,
}
