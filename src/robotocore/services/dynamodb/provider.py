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


async def handle_dynamodb_request(request: Request, region: str, account_id: str) -> Response:
    """Handle DynamoDB requests, forwarding to Moto and firing stream hooks on mutations."""
    body_bytes = await request.body()
    target = request.headers.get("x-amz-target", "")

    # Forward to Moto
    response = await forward_to_moto(request, "dynamodb")

    # Only fire hooks on successful mutations
    if target in _MUTATION_OPS and 200 <= response.status_code < 300:
        try:
            _fire_stream_hooks(target, body_bytes, region, account_id)
        except Exception:
            logger.debug("Failed to fire stream hooks for %s", target, exc_info=True)

    return response


def _fire_stream_hooks(target: str, body_bytes: bytes, region: str, account_id: str) -> None:
    """Parse the request and fire appropriate stream hooks."""
    from robotocore.services.dynamodbstreams.hooks import notify_table_change

    body = json.loads(body_bytes)
    op = target.split(".")[-1]

    if op == "PutItem":
        table_name = body.get("TableName", "")
        item = body.get("Item", {})
        keys = _extract_keys_from_item(table_name, item, region, account_id)
        notify_table_change(
            table_name=table_name,
            event_name="INSERT",  # Could be MODIFY if item existed, but INSERT is close enough
            keys=keys,
            new_image=item,
            old_image=None,
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
