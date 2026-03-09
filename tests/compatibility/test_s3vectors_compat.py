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

    def test_delete_index(self, client, vector_bucket):
        """DeleteIndex removes a vector index."""
        client.create_index(
            vectorBucketName=vector_bucket,
            indexName="test-del-idx",
            dataType="float32",
            dimension=4,
            distanceMetric="euclidean",
        )
        resp = client.delete_index(vectorBucketName=vector_bucket, indexName="test-del-idx")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_indexes(self, client, vector_bucket):
        """ListIndexes returns indexes in a bucket."""
        client.create_index(
            vectorBucketName=vector_bucket,
            indexName="test-list-idx",
            dataType="float32",
            dimension=4,
            distanceMetric="euclidean",
        )
        resp = client.list_indexes(vectorBucketName=vector_bucket)
        assert "indexes" in resp
        names = [i["indexName"] for i in resp["indexes"]]
        assert "test-list-idx" in names


class TestS3VectorsBucketOperations:
    """Tests for S3 Vectors Bucket CRUD operations."""

    @pytest.fixture
    def client(self):
        return make_client("s3vectors")

    def test_create_vector_bucket(self, client):
        """CreateVectorBucket creates a bucket."""
        name = "test-create-vb"
        resp = client.create_vector_bucket(vectorBucketName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # cleanup
        client.delete_vector_bucket(vectorBucketName=name)

    def test_delete_vector_bucket(self, client):
        """DeleteVectorBucket removes a bucket."""
        name = "test-delete-vb"
        client.create_vector_bucket(vectorBucketName=name)
        resp = client.delete_vector_bucket(vectorBucketName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_vector_bucket(self, client):
        """GetVectorBucket returns bucket details."""
        name = "test-get-vb"
        client.create_vector_bucket(vectorBucketName=name)
        resp = client.get_vector_bucket(vectorBucketName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["vectorBucket"]["vectorBucketName"] == name
        client.delete_vector_bucket(vectorBucketName=name)

    def test_get_vector_bucket_nonexistent(self, client):
        """GetVectorBucket for nonexistent bucket raises error."""
        with pytest.raises(client.exceptions.ClientError) as exc:
            client.get_vector_bucket(vectorBucketName="no-such-bucket-xyz")
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "NotFoundException",
        )

    def test_delete_vector_bucket_policy_nonexistent(self, client):
        """DeleteVectorBucketPolicy for nonexistent bucket raises error."""
        with pytest.raises(client.exceptions.ClientError) as exc:
            client.delete_vector_bucket_policy(vectorBucketName="no-such-bucket-xyz")
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "NotFoundException",
        )

    def test_get_vector_bucket_policy_nonexistent(self, client):
        """GetVectorBucketPolicy for nonexistent bucket raises error."""
        with pytest.raises(client.exceptions.ClientError) as exc:
            client.get_vector_bucket_policy(vectorBucketName="no-such-bucket-xyz")
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "NotFoundException",
        )

    def test_put_and_get_vector_bucket_policy(self, client):
        """PutVectorBucketPolicy sets a policy, GetVectorBucketPolicy reads it."""
        name = "test-policy-vb"
        client.create_vector_bucket(vectorBucketName=name)
        import json

        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3vectors:*",
                        "Resource": "*",
                    }
                ],
            }
        )
        resp = client.put_vector_bucket_policy(vectorBucketName=name, policy=policy)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        get_resp = client.get_vector_bucket_policy(vectorBucketName=name)
        assert "policy" in get_resp
        client.delete_vector_bucket(vectorBucketName=name)


class TestS3VectorsListVectors:
    """Tests for ListVectors operation."""

    @pytest.fixture
    def client(self):
        return make_client("s3vectors")

    @pytest.fixture
    def vector_bucket_with_index(self, client):
        bucket_name = "test-listvec-bucket"
        index_name = "test-listvec-idx"
        client.create_vector_bucket(vectorBucketName=bucket_name)
        client.create_index(
            vectorBucketName=bucket_name,
            indexName=index_name,
            dataType="float32",
            dimension=4,
            distanceMetric="euclidean",
        )
        yield bucket_name, index_name
        try:
            client.delete_index(vectorBucketName=bucket_name, indexName=index_name)
        except Exception:
            pass
        try:
            client.delete_vector_bucket(vectorBucketName=bucket_name)
        except Exception:
            pass

    def test_list_vectors_empty(self, client, vector_bucket_with_index):
        """ListVectors on empty index returns empty list."""
        bucket, index = vector_bucket_with_index
        resp = client.list_vectors(vectorBucketName=bucket, indexName=index)
        assert "vectors" in resp
        assert isinstance(resp["vectors"], list)

    def test_list_vectors_nonexistent_bucket(self, client):
        """ListVectors for nonexistent bucket raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            client.list_vectors(
                vectorBucketName="no-such-bucket-xyz",
                indexName="no-such-index",
            )
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "NotFoundException",
            "ValidationException",
        )
