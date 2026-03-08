"""DynamoDB provider -- wraps Moto with stream mutation hooks."""

import json
import logging

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

# In-memory global tables store
_global_tables: dict[str, dict] = {}


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
    response = await forward_to_moto(request, "dynamodb")

    # Only fire hooks on successful mutations
    if target in _MUTATION_OPS and 200 <= response.status_code < 300:
        try:
            _fire_stream_hooks(target, body_bytes, response, region, account_id)
        except Exception:
            logger.debug("Failed to fire stream hooks for %s", target, exc_info=True)

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
                    notify_table_change(
                        table_name=table_name,
                        event_name="INSERT",
                        keys=keys,
                        new_image=item,
                        old_image=None,
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
                notify_table_change(
                    table_name=table_name,
                    event_name="MODIFY",
                    keys=keys,
                    new_image=None,
                    old_image=None,
                    region=region,
                    account_id=account_id,
                )


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
    except Exception:
        pass
    return item  # Fallback: return full item as keys


# ---------------------------------------------------------------------------
# Intercepted operations (not implemented in Moto)
# ---------------------------------------------------------------------------


class _DynamoDBError(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message


def _create_global_table(params: dict, region: str, account_id: str) -> dict:
    name = params.get("GlobalTableName", "")
    replication_group = params.get("ReplicationGroup", [])
    if name in _global_tables:
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
    _global_tables[name] = desc
    return {"GlobalTableDescription": desc}


def _describe_global_table(params: dict, region: str, account_id: str) -> dict:
    name = params.get("GlobalTableName", "")
    if name not in _global_tables:
        raise _DynamoDBError(
            "GlobalTableNotFoundException",
            f"Global table with name '{name}' does not exist",
        )
    return {"GlobalTableDescription": _global_tables[name]}


def _list_global_tables(params: dict, region: str, account_id: str) -> dict:
    tables = [
        {"GlobalTableName": gt["GlobalTableName"], "ReplicationGroup": gt["ReplicationGroup"]}
        for gt in _global_tables.values()
    ]
    return {"GlobalTables": tables}


def _describe_table_replica_auto_scaling(params: dict, region: str, account_id: str) -> dict:
    table_name = params.get("TableName", "")
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


_INTERCEPT_OPS: dict = {
    "CreateGlobalTable": _create_global_table,
    "DescribeGlobalTable": _describe_global_table,
    "ListGlobalTables": _list_global_tables,
    "DescribeTableReplicaAutoScaling": _describe_table_replica_auto_scaling,
}
