"""In-memory models for DynamoDB Streams.

These models provide a native view of stream data. The actual stream records
are stored by Moto's DynamoDB backend (in StreamShard objects on each table).
This module provides helper types and a store that bridges to Moto's data.
"""

import threading
import time
from dataclasses import dataclass, field


@dataclass
class StreamRecord:
    """A single change record in a DynamoDB stream."""

    event_id: str
    event_name: str  # INSERT, MODIFY, REMOVE
    dynamodb: dict  # Keys, NewImage, OldImage, SequenceNumber, etc.
    event_source_arn: str
    aws_region: str
    event_source: str = "aws:dynamodb"
    event_version: str = "1.1"


@dataclass
class StreamShard:
    """A shard within a DynamoDB stream."""

    shard_id: str
    records: list[StreamRecord] = field(default_factory=list)
    parent_shard_id: str | None = None
    sequence_counter: int = 0


@dataclass
class DynamoDBStream:
    """Represents a DynamoDB stream attached to a table."""

    stream_arn: str
    table_name: str
    stream_label: str
    status: str = "ENABLED"
    shards: list[StreamShard] = field(default_factory=list)
    view_type: str = "NEW_AND_OLD_IMAGES"
    key_schema: list[dict] = field(default_factory=list)
    creation_time: float = field(default_factory=time.time)


class DynamoDBStreamsStore:
    """In-memory store for DynamoDB Streams.

    This store bridges to Moto's DynamoDB backend to read stream data
    from tables that have streaming enabled.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # Local record of changes pushed via hooks (for future use)
        self._hook_records: dict[str, list[StreamRecord]] = {}

    def record_change(
        self,
        table_name: str,
        event_name: str,
        keys: dict,
        new_image: dict | None,
        old_image: dict | None,
        region: str,
        account_id: str,
        stream_arn: str,
        view_type: str = "NEW_AND_OLD_IMAGES",
    ) -> None:
        """Record a change event for a table's stream.

        Called when DynamoDB mutations happen (put_item, delete_item, update_item).
        Appends a record that can later be consumed by Lambda event source mappings.
        """
        with self._lock:
            if stream_arn not in self._hook_records:
                self._hook_records[stream_arn] = []

            records = self._hook_records[stream_arn]
            seq = len(records) + 1
            seq_str = str(seq).zfill(21)

            dynamodb_payload: dict = {
                "Keys": keys,
                "SequenceNumber": seq_str,
                "SizeBytes": 0,
                "StreamViewType": view_type,
            }
            if view_type in ("NEW_IMAGE", "NEW_AND_OLD_IMAGES") and new_image:
                dynamodb_payload["NewImage"] = new_image
            if view_type in ("OLD_IMAGE", "NEW_AND_OLD_IMAGES") and old_image:
                dynamodb_payload["OldImage"] = old_image

            record = StreamRecord(
                event_id=f"{seq_str}",
                event_name=event_name,
                dynamodb=dynamodb_payload,
                event_source_arn=stream_arn,
                aws_region=region,
            )
            records.append(record)
