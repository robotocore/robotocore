"""Hooks for capturing DynamoDB mutations and recording stream events.

This module exposes `notify_table_change()` which should be called when
DynamoDB put_item/delete_item/update_item operations occur on tables
with streaming enabled.

It also exposes `get_store()` so the Lambda ESM engine can poll for records.
"""

import logging
import threading

from robotocore.services.dynamodbstreams.models import DynamoDBStreamsStore

logger = logging.getLogger(__name__)

_stores: dict[tuple[str, str], DynamoDBStreamsStore] = {}
_stores_lock = threading.Lock()


def get_store(region: str = "us-east-1", account_id: str = "123456789012") -> DynamoDBStreamsStore:
    with _stores_lock:
        key = (account_id, region)
        if key not in _stores:
            _stores[key] = DynamoDBStreamsStore()
        return _stores[key]


def notify_table_change(
    table_name: str,
    event_name: str,
    keys: dict,
    new_image: dict | None,
    old_image: dict | None,
    region: str,
    account_id: str,
) -> None:
    """Notify the DynamoDB Streams subsystem of a table mutation.

    Args:
        table_name: Name of the DynamoDB table.
        event_name: One of INSERT, MODIFY, REMOVE.
        keys: The key attributes of the affected item.
        new_image: The item after the mutation (None for REMOVE).
        old_image: The item before the mutation (None for INSERT).
        region: AWS region.
        account_id: AWS account ID.
    """
    try:
        from moto.backends import get_backend
        from moto.core import DEFAULT_ACCOUNT_ID

        acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID
        backend = get_backend("dynamodb")[acct][region]
        table = backend.get_table(table_name)

        if not table or not table.latest_stream_label:
            return  # No stream enabled on this table

        stream_arn = f"{table.table_arn}/stream/{table.latest_stream_label}"
        view_type = table.stream_specification.get("StreamViewType", "NEW_AND_OLD_IMAGES")

        store = get_store(region, account_id)
        store.record_change(
            table_name=table_name,
            event_name=event_name,
            keys=keys,
            new_image=new_image,
            old_image=old_image,
            region=region,
            account_id=account_id,
            stream_arn=stream_arn,
            view_type=view_type,
        )

    except Exception:  # noqa: BLE001
        logger.debug("Could not record stream change for table %s", table_name, exc_info=True)
