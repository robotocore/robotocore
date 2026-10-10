"""Guard: every S3 sub-resource config handler answers NoSuchBucket for a
bucket moto does not hold, matching AWS's per-bucket behavior instead of
accepting configs for nonexistent buckets (which also leaked across accounts
when a later account created a same-named bucket that inherited them)."""

from starlette.responses import Response


def _require_bucket(bucket: str, region: str, account_id: str) -> Response | None:
    """404 NoSuchBucket when the bucket does not exist in the caller's backend."""
    from moto.backends import get_backend  # noqa: I001
    from moto.s3.exceptions import MissingBucket as MotoNoSuchBucket

    try:
        backend = get_backend("s3")[account_id][region or "us-east-1"]
        backend.get_bucket(bucket)
        return None
    except MotoNoSuchBucket:
        pass
    from xml.sax.saxutils import escape as xml_escape

    return Response(
        content=(
            "<Error><Code>NoSuchBucket</Code>"
            f"<Message>The specified bucket does not exist: {xml_escape(bucket)}</Message>"
            f"<BucketName>{xml_escape(bucket)}</BucketName></Error>"
        ),
        status_code=404,
        media_type="application/xml",
    )
