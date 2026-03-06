"""Hooks for capturing DynamoDB mutations and recording stream events.

This module exposes `notify_table_change()` which should be called when
DynamoDB put_item/delete_item/update_item operations occur on tables
with streaming enabled. For now it is a public interface that will be
wired up when DynamoDB interceptors are added.
"""

import logging

logger = logging.getLogger(__name__)


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
    from robotocore.services.dynamodbstreams.models import DynamoDBStreamsStore

    # Look up the stream ARN for this table from Moto's DynamoDB backend
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

        # For now, log the change. The actual stream records are already
        # captured by Moto's StreamShard.add() during DynamoDB operations.
        # This hook provides an additional capture point for future use
        # (e.g., Lambda event source mapping polling).
        logger.debug(
            "DynamoDB stream change: table=%s event=%s stream=%s",
            table_name,
            event_name,
            stream_arn,
        )

    except Exception:
        logger.debug("Could not record stream change for table %s", table_name, exc_info=True)
