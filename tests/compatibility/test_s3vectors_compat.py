"""S3 Vectors compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def s3vectors():
    return make_client("s3vectors")


class TestS3VectorsOperations:
    def test_list_vector_buckets(self, s3vectors):
        response = s3vectors.list_vector_buckets()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "vectorBuckets" in response


class TestS3VectorsIndexOperations:
    """Tests for S3 Vectors Index operations."""

    @pytest.fixture
    def client(self):
        return make_client("s3vectors")

    @pytest.fixture
    def vector_bucket(self, client):
        bucket_name = "test-index-ops-bucket"
        client.create_vector_bucket(vectorBucketName=bucket_name)
        yield bucket_name
        # cleanup: delete any leftover indexes, then bucket
        try:
            resp = client.list_indexes(vectorBucketName=bucket_name)
            for idx in resp.get("indexes", []):
                client.delete_index(vectorBucketName=bucket_name, indexName=idx["indexName"])
        except Exception:
            pass
        try:
            client.delete_vector_bucket(vectorBucketName=bucket_name)
        except Exception:
            pass

    def test_create_index(self, client, vector_bucket):
        """CreateIndex creates a vector index."""
        resp = client.create_index(
            vectorBucketName=vector_bucket,
            indexName="test-create-idx",
            dataType="float32",
            dimension=4,
            distanceMetric="euclidean",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_index(self, client, vector_bucket):
        """GetIndex returns index details."""
        client.create_index(
            vectorBucketName=vector_bucket,
            indexName="test-get-idx",
            dataType="float32",
            dimension=8,
            distanceMetric="cosine",
        )
        resp = client.get_index(vectorBucketName=vector_bucket, indexName="test-get-idx")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["index"]["indexName"] == "test-get-idx"
        assert resp["index"]["dimension"] == 8
        assert resp["index"]["distanceMetric"] == "cosine"
        assert resp["index"]["dataType"] == "float32"

    def test_get_index_nonexistent(self, client, vector_bucket):
        """GetIndex on nonexistent index returns error."""
        with pytest.raises(client.exceptions.ClientError) as exc:
            client.get_index(vectorBucketName=vector_bucket, indexName="no-such-index")
        err = exc.value.response["Error"]["Code"]
        assert err in ("ResourceNotFoundException", "NotFoundException", "NoSuchEntity")
