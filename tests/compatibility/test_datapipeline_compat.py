"""Data Pipeline compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def datapipeline():
    return make_client("datapipeline")


class TestDataPipelineOperations:
    def test_list_pipelines(self, datapipeline):
        """ListPipelines returns a list of pipeline IDs."""
        response = datapipeline.list_pipelines()
        assert "pipelineIdList" in response
        assert isinstance(response["pipelineIdList"], list)

    def test_list_pipelines_has_marker(self, datapipeline):
        """ListPipelines returns hasMoreResults field."""
        response = datapipeline.list_pipelines()
        assert "hasMoreResults" in response

    def test_list_pipelines_status_code(self, datapipeline):
        """ListPipelines returns HTTP 200."""
        response = datapipeline.list_pipelines()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
