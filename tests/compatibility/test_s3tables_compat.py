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

    def test_get_namespace(self, s3tables):
        bucket_name = f"test-{uuid.uuid4().hex[:8]}"
        bucket_resp = s3tables.create_table_bucket(name=bucket_name)
        bucket_arn = bucket_resp["arn"]
        ns_name = f"ns-{uuid.uuid4().hex[:8]}"
        try:
            s3tables.create_namespace(tableBucketARN=bucket_arn, namespace=[ns_name])
            resp = s3tables.get_namespace(tableBucketARN=bucket_arn, namespace=ns_name)
            assert "namespace" in resp
            assert resp["namespace"] == [ns_name]
        finally:
            s3tables.delete_namespace(tableBucketARN=bucket_arn, namespace=ns_name)

    def test_delete_namespace(self, s3tables):
        bucket_name = f"test-{uuid.uuid4().hex[:8]}"
        bucket_resp = s3tables.create_table_bucket(name=bucket_name)
        bucket_arn = bucket_resp["arn"]
        ns_name = f"ns-{uuid.uuid4().hex[:8]}"
        s3tables.create_namespace(tableBucketARN=bucket_arn, namespace=[ns_name])
        s3tables.delete_namespace(tableBucketARN=bucket_arn, namespace=ns_name)
        with pytest.raises(s3tables.exceptions.ClientError) as exc:
            s3tables.get_namespace(tableBucketARN=bucket_arn, namespace=ns_name)
        assert exc.value.response["Error"]["Code"] in (
            "NotFoundException",
            "ResourceNotFoundException",
            "NoSuchEntity",
        )
