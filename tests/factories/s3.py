"""S3 test data factories with automatic cleanup.

Provides context managers for creating S3 buckets and objects that are
automatically cleaned up after the test.

Usage:
    from tests.factories.s3 import bucket, bucket_with_objects

    def test_put_get(s3):
        with bucket(s3) as bucket_name:
            s3.put_object(Bucket=bucket_name, Key="test.txt", Body=b"hello")
            response = s3.get_object(Bucket=bucket_name, Key="test.txt")
            assert response["Body"].read() == b"hello"

    def test_list_objects(s3):
        with bucket_with_objects(s3, keys=["a.txt", "b.txt", "c.txt"]) as bucket_name:
            response = s3.list_objects_v2(Bucket=bucket_name)
            assert len(response["Contents"]) == 3
"""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from botocore.exceptions import ClientError

from . import unique_name

__all__ = ["bucket", "bucket_with_objects"]


def _delete_all_objects(client: Any, bucket_name: str) -> None:
    """Delete all objects in a bucket (required before bucket deletion)."""
    try:
        # List and delete all objects
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name):
            if "Contents" in page:
                objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})

        # List and delete all object versions (for versioned buckets)
        try:
            paginator = client.get_paginator("list_object_versions")
            for page in paginator.paginate(Bucket=bucket_name):
                objects = []
                for version in page.get("Versions", []):
                    objects.append({"Key": version["Key"], "VersionId": version["VersionId"]})
                for marker in page.get("DeleteMarkers", []):
                    objects.append({"Key": marker["Key"], "VersionId": marker["VersionId"]})
                if objects:
                    client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
        except ClientError:
            pass  # Versioning may not be enabled
    except ClientError:
        pass  # Best effort


@contextmanager
def bucket(client: Any, name: str | None = None) -> Generator[str, None, None]:
    """Create an S3 bucket with automatic cleanup.

    Args:
        client: boto3 S3 client
        name: Optional bucket name (auto-generated if not provided)

    Yields:
        Bucket name

    Example:
        with bucket(s3) as bucket_name:
            s3.put_object(Bucket=bucket_name, Key="test.txt", Body=b"hello")
    """
    bucket_name = name or unique_name("test-bucket")

    client.create_bucket(Bucket=bucket_name)

    try:
        yield bucket_name
    finally:
        try:
            _delete_all_objects(client, bucket_name)
            client.delete_bucket(Bucket=bucket_name)
        except ClientError:
            pass  # Best effort cleanup


@contextmanager
def bucket_with_objects(
    client: Any,
    keys: list[str] | None = None,
    count: int | None = None,
    body: bytes = b"test content",
    name: str | None = None,
) -> Generator[str, None, None]:
    """Create an S3 bucket pre-populated with objects.

    Args:
        client: boto3 S3 client
        keys: List of object keys to create
        count: Number of objects to create (if keys not provided)
        body: Content for each object (default: b"test content")
        name: Optional bucket name (auto-generated if not provided)

    Yields:
        Bucket name

    Example:
        with bucket_with_objects(s3, keys=["a.txt", "b.txt"]) as bucket_name:
            response = s3.list_objects_v2(Bucket=bucket_name)
            assert len(response["Contents"]) == 2

        with bucket_with_objects(s3, count=10) as bucket_name:
            response = s3.list_objects_v2(Bucket=bucket_name)
            assert len(response["Contents"]) == 10
    """
    if keys is None:
        keys = [f"object-{i}.txt" for i in range(count or 5)]

    with bucket(client, name=name) as bucket_name:
        for key in keys:
            client.put_object(Bucket=bucket_name, Key=key, Body=body)
        yield bucket_name
