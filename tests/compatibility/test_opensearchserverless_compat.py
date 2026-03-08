"""OpenSearch Serverless compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def opensearchserverless():
    return make_client("opensearchserverless")


class TestOpenSearchServerlessOperations:
    def test_list_collections(self, opensearchserverless):
        """ListCollections returns a list of collection summaries."""
        response = opensearchserverless.list_collections()
        assert "collectionSummaries" in response
        assert isinstance(response["collectionSummaries"], list)

    def test_list_collections_with_filter(self, opensearchserverless):
        """ListCollections accepts a filter parameter."""
        response = opensearchserverless.list_collections(collectionFilters={"status": "ACTIVE"})
        assert "collectionSummaries" in response

    def test_list_collections_status_code(self, opensearchserverless):
        """ListCollections returns HTTP 200."""
        response = opensearchserverless.list_collections()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestOpensearchserverlessAutoCoverage:
    """Auto-generated coverage tests for opensearchserverless."""

    @pytest.fixture
    def client(self):
        return make_client("opensearchserverless")

    def test_batch_get_collection(self, client):
        """BatchGetCollection returns a response."""
        resp = client.batch_get_collection()
        assert "collectionDetails" in resp
