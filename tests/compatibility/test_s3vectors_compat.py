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
