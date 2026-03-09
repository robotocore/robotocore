"""Data Pipeline compatibility tests."""

import uuid

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


class TestDataPipelineDefinition:
    """Tests for put/get pipeline definition."""

    def _create_pipeline(self, datapipeline):
        uid = uuid.uuid4().hex[:8]
        resp = datapipeline.create_pipeline(
            name=f"test-pipeline-{uid}",
            uniqueId=f"test-unique-{uid}",
        )
        return resp["pipelineId"]

    def test_put_pipeline_definition(self, datapipeline):
        """PutPipelineDefinition stores a pipeline definition."""
        pipeline_id = self._create_pipeline(datapipeline)
        try:
            resp = datapipeline.put_pipeline_definition(
                pipelineId=pipeline_id,
                pipelineObjects=[
                    {
                        "id": "Default",
                        "name": "Default",
                        "fields": [{"key": "type", "stringValue": "Default"}],
                    }
                ],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "errored" in resp
        finally:
            datapipeline.delete_pipeline(pipelineId=pipeline_id)

    def test_get_pipeline_definition(self, datapipeline):
        """GetPipelineDefinition retrieves a stored definition."""
        pipeline_id = self._create_pipeline(datapipeline)
        try:
            datapipeline.put_pipeline_definition(
                pipelineId=pipeline_id,
                pipelineObjects=[
                    {
                        "id": "Default",
                        "name": "Default",
                        "fields": [{"key": "type", "stringValue": "Default"}],
                    }
                ],
            )
            resp = datapipeline.get_pipeline_definition(pipelineId=pipeline_id)
            assert "pipelineObjects" in resp
            assert len(resp["pipelineObjects"]) >= 1
        finally:
            datapipeline.delete_pipeline(pipelineId=pipeline_id)

    def test_activate_pipeline(self, datapipeline):
        """ActivatePipeline activates a pipeline."""
        pipeline_id = self._create_pipeline(datapipeline)
        try:
            datapipeline.put_pipeline_definition(
                pipelineId=pipeline_id,
                pipelineObjects=[
                    {
                        "id": "Default",
                        "name": "Default",
                        "fields": [{"key": "type", "stringValue": "Default"}],
                    }
                ],
            )
            resp = datapipeline.activate_pipeline(pipelineId=pipeline_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            datapipeline.delete_pipeline(pipelineId=pipeline_id)

    def test_created_pipeline_in_list(self, datapipeline):
        """A created pipeline appears in list_pipelines."""
        pipeline_id = self._create_pipeline(datapipeline)
        try:
            resp = datapipeline.list_pipelines()
            ids = [p["id"] for p in resp["pipelineIdList"]]
            assert pipeline_id in ids
        finally:
            datapipeline.delete_pipeline(pipelineId=pipeline_id)

    def test_describe_objects(self, datapipeline):
        """DescribeObjects returns objects for a pipeline."""
        pipeline_id = self._create_pipeline(datapipeline)
        try:
            datapipeline.put_pipeline_definition(
                pipelineId=pipeline_id,
                pipelineObjects=[
                    {
                        "id": "Default",
                        "name": "Default",
                        "fields": [{"key": "type", "stringValue": "Default"}],
                    }
                ],
            )
            resp = datapipeline.describe_objects(
                pipelineId=pipeline_id,
                objectIds=["Default"],
            )
            assert "pipelineObjects" in resp
        finally:
            datapipeline.delete_pipeline(pipelineId=pipeline_id)


class TestDataPipelineTaskOperations:
    """Tests for task runner operations."""

    def test_poll_for_task(self, datapipeline):
        """PollForTask returns a response with taskObject key."""
        resp = datapipeline.poll_for_task(
            workerGroup="test-worker-group",
        )
        assert "taskObject" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_report_task_runner_heartbeat(self, datapipeline):
        """ReportTaskRunnerHeartbeat returns a terminate flag."""
        resp = datapipeline.report_task_runner_heartbeat(
            taskrunnerId="test-runner-id",
        )
        assert "terminate" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_report_task_progress(self, datapipeline):
        """ReportTaskProgress returns canceled flag."""
        resp = datapipeline.report_task_progress(
            taskId="test-task-id",
        )
        assert "canceled" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_set_task_status(self, datapipeline):
        """SetTaskStatus sets a task's status."""
        resp = datapipeline.set_task_status(
            taskId="test-task-id",
            taskStatus="FINISHED",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
