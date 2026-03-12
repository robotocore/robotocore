"""
Fixtures for the file processing application tests.

Inherits shared fixtures (s3, dynamodb, unique_name, ...) from the parent
``tests/apps/conftest.py`` automatically via pytest's fixture resolution.
"""

from __future__ import annotations

import pytest

from .app import FileProcessingService


@pytest.fixture
def file_bucket(s3, unique_name):
    """Create an S3 bucket for file storage, clean up after the test."""
    bucket = f"file-processing-{unique_name}"
    s3.create_bucket(Bucket=bucket)
    yield bucket
    # Cleanup: delete all object versions, delete markers, then bucket
    try:
        paginator = s3.get_paginator("list_object_versions")
        for page in paginator.paginate(Bucket=bucket):
            for v in page.get("Versions", []):
                s3.delete_object(Bucket=bucket, Key=v["Key"], VersionId=v["VersionId"])
            for dm in page.get("DeleteMarkers", []):
                s3.delete_object(Bucket=bucket, Key=dm["Key"], VersionId=dm["VersionId"])
    except Exception:
        pass
    try:
        objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
        for obj in objects:
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
    except Exception:
        pass
    s3.delete_bucket(Bucket=bucket)


@pytest.fixture
def metadata_table(dynamodb, unique_name):
    """Create a DynamoDB table with GSIs for content_type, status, and tags."""
    table_name = f"file-meta-{unique_name}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "file_id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "file_id", "AttributeType": "S"},
            {"AttributeName": "content_type", "AttributeType": "S"},
            {"AttributeName": "status", "AttributeType": "S"},
            {"AttributeName": "uploaded_at", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "content-type-index",
                "KeySchema": [
                    {"AttributeName": "content_type", "KeyType": "HASH"},
                    {"AttributeName": "uploaded_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "status-index",
                "KeySchema": [
                    {"AttributeName": "status", "KeyType": "HASH"},
                    {"AttributeName": "uploaded_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


@pytest.fixture
def file_service(s3, dynamodb, file_bucket, metadata_table):
    """Fully initialized FileProcessingService instance."""
    return FileProcessingService(
        s3_client=s3,
        dynamodb_client=dynamodb,
        bucket=file_bucket,
        table_name=metadata_table,
    )


@pytest.fixture
def sample_files() -> dict[str, bytes]:
    """A dict of realistic sample file contents keyed by filename."""
    return {
        "readme.txt": b"This is a plain-text readme for the project.",
        "config.json": b'{"version": 1, "debug": false, "features": ["auth", "search"]}',
        "data.csv": b"id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com\n",
        "logo.png": b"\x89PNG\r\n\x1a\n" + b"\x00" * 256,  # fake PNG header + padding
        "report.pdf": b"%PDF-1.4 fake pdf content for testing purposes " * 5,
    }
