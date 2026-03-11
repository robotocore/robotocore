"""DynamoDB TTL (Time-to-Live) automatic item removal engine.

Background daemon thread that periodically scans DynamoDB tables for expired TTL items
and removes them, matching AWS behavior. When streams are enabled, emits REMOVE events
with userIdentity: {type: "Service", principalId: "dynamodb.amazonaws.com"}.

Controlled by:
    DYNAMODB_REMOVE_EXPIRED_ITEMS  (default "true") -- enable/disable TTL removal
"""

import logging
import os
import threading
import time

logger = logging.getLogger(__name__)

# Scan interval in seconds (not configurable -- fast for local dev)
_SCAN_INTERVAL = 60

# Default account/region pairs to scan
_DEFAULT_ACCOUNT_ID = "123456789012"
_DEFAULT_REGIONS = [
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "eu-west-1",
    "eu-central-1",
    "ap-southeast-1",
    "ap-northeast-1",
]


def _is_ttl_enabled() -> bool:
    """Check if TTL removal is enabled via environment variable."""
    return os.environ.get("DYNAMODB_REMOVE_EXPIRED_ITEMS", "true").lower() in (
        "true",
        "1",
        "yes",
    )


def scan_and_remove_expired_items() -> int:
    """Scan all DynamoDB tables across all accounts/regions for expired TTL items.

    Returns the total number of items removed.
    """
    if not _is_ttl_enabled():
        return 0

    total_removed = 0

    try:
        from moto.backends import get_backend
    except ImportError:
        logger.debug("Moto not available, skipping TTL scan")
        return 0

    try:
        dynamodb_backends = get_backend("dynamodb")
    except Exception:
        logger.debug("Could not get dynamodb backend", exc_info=True)
        return 0

    # Iterate all account/region combos that have backends
    for account_id, regions in dynamodb_backends.items():
        if not isinstance(regions, dict):
            continue
        for region, backend in regions.items():
            try:
                removed = _scan_backend(backend, region, account_id)
                total_removed += removed
            except Exception:
                logger.debug(
                    "Error scanning TTL for account=%s region=%s",
                    account_id,
                    region,
                    exc_info=True,
                )

    if total_removed > 0:
        logger.info("TTL scan removed %d expired items", total_removed)

    return total_removed


def _scan_backend(backend, region: str, account_id: str) -> int:
    """Scan a single Moto DynamoDB backend for expired TTL items."""
    removed = 0
    now = int(time.time())

    # Get a snapshot of table names to avoid mutation during iteration
    try:
        table_names = list(backend.tables.keys())
    except Exception:
        return 0

    for table_name in table_names:
        try:
            table = backend.get_table(table_name)
        except Exception:
            # Table may have been deleted between listing and access
            continue

        if table is None:
            continue

        # Check if TTL is enabled on this table
        ttl_config = table.ttl
        if ttl_config.get("TimeToLiveStatus") != "ENABLED":
            continue

        ttl_attr = ttl_config.get("AttributeName")
        if not ttl_attr:
            continue

        removed += _remove_expired_items(
            backend, table, table_name, ttl_attr, now, region, account_id
        )

    return removed


def _remove_expired_items(
    backend,
    table,
    table_name: str,
    ttl_attr: str,
    now: int,
    region: str,
    account_id: str,
) -> int:
    """Remove expired items from a single table. Returns count of items removed."""
    removed = 0

    # Collect expired item keys first to avoid mutating dict during iteration
    expired_keys: list[tuple] = []  # list of (hash_key, range_key_or_None)

    try:
        for hash_key, val in list(table.items.items()):
            if isinstance(val, dict):
                # Table has range key -- val is {range_key: Item}
                for range_key, item in list(val.items()):
                    if _is_item_expired(item, ttl_attr, now):
                        expired_keys.append((hash_key, range_key))
            else:
                # Hash-only table -- val is the Item directly
                if _is_item_expired(val, ttl_attr, now):
                    expired_keys.append((hash_key, None))
    except Exception:
        logger.debug("Error iterating items in table %s", table_name, exc_info=True)
        return 0

    # Delete expired items
    for hash_key, range_key in expired_keys:
        try:
            table.delete_item(hash_key, range_key)
            removed += 1

            # Emit stream event if streams are enabled
            _emit_ttl_stream_event(
                backend, table, table_name, hash_key, range_key, region, account_id
            )

            logger.debug(
                "TTL removed item from table %s (hash=%s, range=%s)",
                table_name,
                hash_key,
                range_key,
            )
        except Exception:
            logger.debug("Failed to delete expired item from %s", table_name, exc_info=True)

    return removed


def _is_item_expired(item, ttl_attr: str, now: int) -> bool:
    """Check if an item's TTL attribute indicates it has expired.

    AWS ignores TTL values more than 5 years in the past -- such values are
    treated as non-TTL data (e.g., small integers used as IDs) and the item
    is NOT expired.
    """
    if not hasattr(item, "attrs"):
        return False

    attr = item.attrs.get(ttl_attr)
    if attr is None:
        return False

    # TTL attribute must be a Number type
    if attr.type != "N":
        return False

    try:
        ttl_value = int(float(str(attr.value)))
    except (ValueError, TypeError):
        return False

    # AWS ignores TTL values more than 5 years in the past
    five_years_seconds = 5 * 365 * 24 * 3600
    if now - ttl_value > five_years_seconds:
        return False

    return ttl_value <= now


def _emit_ttl_stream_event(
    backend,
    table,
    table_name: str,
    hash_key,
    range_key,
    region: str,
    account_id: str,
) -> None:
    """Emit a REMOVE stream event for a TTL-deleted item, with Service userIdentity."""
    if not table.latest_stream_label:
        return  # No stream on this table

    try:
        from robotocore.services.dynamodbstreams.hooks import get_store

        stream_arn = f"{table.table_arn}/stream/{table.latest_stream_label}"
        view_type = table.stream_specification.get("StreamViewType", "NEW_AND_OLD_IMAGES")

        # Build keys dict in DynamoDB format
        keys: dict = {}
        if hasattr(hash_key, "to_json"):
            key_json = hash_key.to_json()
            if table.hash_key_attr:
                keys[table.hash_key_attr] = key_json
        if range_key is not None and hasattr(range_key, "to_json"):
            key_json = range_key.to_json()
            if table.range_key_attr:
                keys[table.range_key_attr] = key_json

        store = get_store(region)

        # Build the dynamodb payload with userIdentity for TTL-triggered removal
        seq = 0
        with store._lock:
            if stream_arn not in store._hook_records:
                store._hook_records[stream_arn] = []
            records = store._hook_records[stream_arn]
            seq = len(records) + 1
            seq_str = str(seq).zfill(21)

            from robotocore.services.dynamodbstreams.models import StreamRecord

            dynamodb_payload: dict = {
                "Keys": keys,
                "SequenceNumber": seq_str,
                "SizeBytes": 0,
                "StreamViewType": view_type,
                "userIdentity": {
                    "type": "Service",
                    "principalId": "dynamodb.amazonaws.com",
                },
            }

            record = StreamRecord(
                event_id=seq_str,
                event_name="REMOVE",
                dynamodb=dynamodb_payload,
                event_source_arn=stream_arn,
                aws_region=region,
            )
            records.append(record)

    except Exception:
        logger.debug("Failed to emit TTL stream event for %s", table_name, exc_info=True)


class TTLScanner:
    """Background daemon thread that periodically scans for expired TTL items."""

    def __init__(self, interval: float = _SCAN_INTERVAL):
        self._interval = interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the TTL scanner daemon thread."""
        if self._thread is not None and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="ttl-scanner")
        self._thread.start()
        logger.info("DynamoDB TTL scanner started (interval=%ss)", self._interval)

    def stop(self) -> None:
        """Signal the scanner thread to stop."""
        self._stop_event.set()

    def join(self, timeout: float | None = None) -> None:
        """Wait for the scanner thread to finish."""
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def is_alive(self) -> bool:
        """Check if the scanner thread is running."""
        return self._thread is not None and self._thread.is_alive()

    def _run(self) -> None:
        """Main loop: scan periodically until stopped."""
        while not self._stop_event.is_set():
            try:
                scan_and_remove_expired_items()
            except Exception:
                logger.debug("TTL scan iteration failed", exc_info=True)
            self._stop_event.wait(self._interval)


# Module-level singleton
_scanner: TTLScanner | None = None


def get_ttl_scanner() -> TTLScanner:
    """Get or create the global TTL scanner singleton."""
    global _scanner
    if _scanner is None:
        _scanner = TTLScanner()
    return _scanner
