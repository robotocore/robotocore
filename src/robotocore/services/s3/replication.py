"""S3 replication engine — copies objects to destination buckets based on replication rules."""

import logging
from concurrent.futures import ThreadPoolExecutor

from moto.backends import get_backend

from robotocore.services.s3.notifications import fire_event

logger = logging.getLogger(__name__)

_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="s3-repl")


def maybe_replicate(bucket: str, key: str, region: str, account_id: str) -> None:
    """Check replication config and submit copy jobs for matching rules."""
    try:
        backend = get_backend("s3")[account_id][region]
        replication = getattr(backend, "get_bucket_replication", lambda b: None)(bucket)
        if not replication:
            return
        for rule in replication.get("Rule", []):
            if rule.get("Status") != "Enabled":
                continue
            if not key.startswith(_get_rule_prefix(rule)):
                continue
            dest_bucket = _parse_dest_bucket(rule)
            if dest_bucket:
                _executor.submit(
                    _replicate_object,
                    bucket,
                    key,
                    dest_bucket,
                    region,
                    account_id,
                    rule.get("ID", ""),
                )
    except Exception:
        logger.exception("Error checking replication config for bucket %s", bucket)


def _replicate_object(
    src_bucket: str,
    key: str,
    dest_bucket: str,
    region: str,
    account_id: str,
    rule_id: str,
) -> None:
    """Copy object using Moto backend directly and fire replication event."""
    try:
        backend = get_backend("s3")[account_id][region]
        src_key = backend.get_object(src_bucket, key)
        if src_key is None:
            return
        backend.copy_object(src_key, dest_bucket, key)
        fire_event(
            "s3:Replication:OperationReplicatedAfterThreshold",
            src_bucket,
            key,
            region,
            account_id,
        )
    except Exception:
        logger.exception(
            "Replication failed: %s/%s -> %s (rule %s)", src_bucket, key, dest_bucket, rule_id
        )


def _get_rule_prefix(rule: dict) -> str:
    """Extract prefix from old-style Prefix field or new-style Filter.Prefix."""
    # New style: Filter -> Prefix
    f = rule.get("Filter", {})
    if isinstance(f, dict):
        prefix = f.get("Prefix")
        if prefix is not None:
            return prefix
        # Tag/And filters — no prefix match needed, replicate all
        and_filter = f.get("And", {})
        if isinstance(and_filter, dict):
            prefix = and_filter.get("Prefix")
            if prefix is not None:
                return prefix
    # Old style: top-level Prefix
    return rule.get("Prefix", "")


def _parse_dest_bucket(rule: dict) -> str | None:
    """Extract destination bucket name from replication rule ARN."""
    dest = rule.get("Destination", {})
    if not isinstance(dest, dict):
        return None
    bucket_arn = dest.get("Bucket", "")
    if not bucket_arn:
        return None
    # Strip arn:aws:s3::: prefix
    return bucket_arn.split(":::")[-1] if ":::" in bucket_arn else bucket_arn
