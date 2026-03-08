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

    def test_create_pipeline(self, datapipeline):
        """CreatePipeline returns a pipelineId."""
        resp = datapipeline.create_pipeline(
            name="test-pipeline-create",
            uniqueId="test-unique-create",
        )
        pipeline_id = resp["pipelineId"]
        try:
            assert pipeline_id
            assert isinstance(pipeline_id, str)
        finally:
            datapipeline.delete_pipeline(pipelineId=pipeline_id)

    def test_delete_pipeline(self, datapipeline):
        """DeletePipeline removes a pipeline."""
        resp = datapipeline.create_pipeline(
            name="test-pipeline-delete",
            uniqueId="test-unique-delete",
        )
        pipeline_id = resp["pipelineId"]
        del_resp = datapipeline.delete_pipeline(pipelineId=pipeline_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify it's gone from the list
        pipelines = datapipeline.list_pipelines()["pipelineIdList"]
        assert all(p["id"] != pipeline_id for p in pipelines)

    def test_describe_pipelines(self, datapipeline):
        """DescribePipelines returns pipeline metadata."""
        resp = datapipeline.create_pipeline(
            name="test-pipeline-describe",
            uniqueId="test-unique-describe",
        )
        pipeline_id = resp["pipelineId"]
        try:
            result = datapipeline.describe_pipelines(pipelineIds=[pipeline_id])
            assert "pipelineDescriptionList" in result
            descriptions = result["pipelineDescriptionList"]
            assert len(descriptions) == 1
            assert descriptions[0]["pipelineId"] == pipeline_id
            assert descriptions[0]["name"] == "test-pipeline-describe"
        finally:
            datapipeline.delete_pipeline(pipelineId=pipeline_id)
