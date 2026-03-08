"""OSIS (OpenSearch Ingestion) compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def osis():
    return make_client("osis")


@pytest.fixture
def pipeline(osis):
    name = f"test-{uuid.uuid4().hex[:8]}"
    resp = osis.create_pipeline(
        PipelineName=name,
        MinUnits=1,
        MaxUnits=1,
        PipelineConfigurationBody='version: "2"',
    )
    return resp["Pipeline"]


class TestOSISOperations:
    def test_list_pipelines_empty(self, osis):
        resp = osis.list_pipelines()
        assert "Pipelines" in resp

    def test_create_pipeline(self, osis):
        name = f"test-{uuid.uuid4().hex[:8]}"
        resp = osis.create_pipeline(
            PipelineName=name,
            MinUnits=1,
            MaxUnits=1,
            PipelineConfigurationBody='version: "2"',
        )
        pipeline = resp["Pipeline"]
        assert pipeline["PipelineName"] == name
        assert "PipelineArn" in pipeline

    def test_get_pipeline(self, osis, pipeline):
        resp = osis.get_pipeline(PipelineName=pipeline["PipelineName"])
        assert resp["Pipeline"]["PipelineName"] == pipeline["PipelineName"]
        assert resp["Pipeline"]["PipelineArn"] == pipeline["PipelineArn"]

    def test_list_pipelines_after_create(self, osis, pipeline):
        resp = osis.list_pipelines()
        names = [p["PipelineName"] for p in resp["Pipelines"]]
        assert pipeline["PipelineName"] in names

    def test_list_tags_for_resource(self, osis, pipeline):
        resp = osis.list_tags_for_resource(Arn=pipeline["PipelineArn"])
        assert "Tags" in resp
        assert isinstance(resp["Tags"], list)
