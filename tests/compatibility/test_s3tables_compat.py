"""S3 Tables compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def s3tables():
    return make_client("s3tables")


class TestS3TablesOperations:
    def test_list_table_buckets_empty(self, s3tables):
        resp = s3tables.list_table_buckets()
        assert "tableBuckets" in resp

    def test_create_table_bucket(self, s3tables):
        name = f"test-{uuid.uuid4().hex[:8]}"
        resp = s3tables.create_table_bucket(name=name)
        assert "arn" in resp
        assert name in resp["arn"]

    def test_list_table_buckets_after_create(self, s3tables):
        name = f"test-{uuid.uuid4().hex[:8]}"
        s3tables.create_table_bucket(name=name)
        resp = s3tables.list_table_buckets()
        names = [b["name"] for b in resp["tableBuckets"]]
        assert name in names
