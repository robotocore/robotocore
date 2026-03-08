"""CodePipeline compatibility tests."""

import json
import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def codepipeline():
    return make_client("codepipeline")


@pytest.fixture
def iam():
    return make_client("iam")


TRUST_POLICY = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "codepipeline.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
)


@pytest.fixture
def pipeline_role_arn(iam):
    role_name = _unique("cp-role")
    resp = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=TRUST_POLICY,
        Path="/",
    )
    arn = resp["Role"]["Arn"]
    yield arn
    iam.delete_role(RoleName=role_name)


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _pipeline_def(name, role_arn):
    return {
        "name": name,
        "roleArn": role_arn,
        "artifactStore": {
            "type": "S3",
            "location": "test-artifact-bucket",
        },
        "stages": [
            {
                "name": "Source",
                "actions": [
                    {
                        "name": "SourceAction",
                        "actionTypeId": {
                            "category": "Source",
                            "owner": "AWS",
                            "provider": "S3",
                            "version": "1",
                        },
                        "outputArtifacts": [{"name": "SourceOutput"}],
                        "configuration": {
                            "S3Bucket": "source-bucket",
                            "S3ObjectKey": "source.zip",
                        },
                    }
                ],
            },
            {
                "name": "Deploy",
                "actions": [
                    {
                        "name": "DeployAction",
                        "actionTypeId": {
                            "category": "Deploy",
                            "owner": "AWS",
                            "provider": "S3",
                            "version": "1",
                        },
                        "inputArtifacts": [{"name": "SourceOutput"}],
                        "configuration": {
                            "BucketName": "deploy-bucket",
                            "Extract": "false",
                        },
                    }
                ],
            },
        ],
    }


@pytest.fixture
def pipeline(codepipeline, pipeline_role_arn):
    name = _unique("test-pipe")
    resp = codepipeline.create_pipeline(pipeline=_pipeline_def(name, pipeline_role_arn))
    yield resp["pipeline"]
    try:
        codepipeline.delete_pipeline(name=name)
    except ClientError:
        pass


class TestCodePipelineOperations:
    def test_create_pipeline(self, codepipeline, pipeline_role_arn):
        name = _unique("test-pipe")
        resp = codepipeline.create_pipeline(pipeline=_pipeline_def(name, pipeline_role_arn))
        assert resp["pipeline"]["name"] == name
        assert len(resp["pipeline"]["stages"]) == 2
        codepipeline.delete_pipeline(name=name)

    def test_get_pipeline(self, codepipeline, pipeline):
        resp = codepipeline.get_pipeline(name=pipeline["name"])
        assert resp["pipeline"]["name"] == pipeline["name"]
        assert "metadata" in resp

    def test_list_pipelines(self, codepipeline, pipeline):
        resp = codepipeline.list_pipelines()
        names = [p["name"] for p in resp["pipelines"]]
        assert pipeline["name"] in names

    def test_list_tags_for_resource(self, codepipeline, pipeline):
        get_resp = codepipeline.get_pipeline(name=pipeline["name"])
        arn = get_resp["metadata"]["pipelineArn"]
        resp = codepipeline.list_tags_for_resource(resourceArn=arn)
        assert "tags" in resp

    def test_tag_and_untag_resource(self, codepipeline, pipeline):
        get_resp = codepipeline.get_pipeline(name=pipeline["name"])
        arn = get_resp["metadata"]["pipelineArn"]

        # Tag
        codepipeline.tag_resource(
            resourceArn=arn,
            tags=[{"key": "env", "value": "test"}],
        )
        resp = codepipeline.list_tags_for_resource(resourceArn=arn)
        tag_map = {t["key"]: t["value"] for t in resp["tags"]}
        assert tag_map["env"] == "test"

        # Untag
        codepipeline.untag_resource(resourceArn=arn, tagKeys=["env"])
        resp = codepipeline.list_tags_for_resource(resourceArn=arn)
        tag_keys = [t["key"] for t in resp["tags"]]
        assert "env" not in tag_keys

    def test_update_pipeline(self, codepipeline, pipeline):
        updated = pipeline.copy()
        updated["stages"][1]["actions"][0]["configuration"]["Extract"] = "true"
        resp = codepipeline.update_pipeline(pipeline=updated)
        config = resp["pipeline"]["stages"][1]["actions"][0]["configuration"]
        assert config["Extract"] == "true"

    def test_delete_pipeline(self, codepipeline, pipeline_role_arn):
        name = _unique("test-pipe")
        codepipeline.create_pipeline(pipeline=_pipeline_def(name, pipeline_role_arn))
        codepipeline.delete_pipeline(name=name)
        # Verify it's gone
        resp = codepipeline.list_pipelines()
        names = [p["name"] for p in resp["pipelines"]]
        assert name not in names

    def test_get_nonexistent_pipeline(self, codepipeline):
        with pytest.raises(ClientError) as exc_info:
            codepipeline.get_pipeline(name="does-not-exist")
        assert exc_info.value.response["Error"]["Code"] == "PipelineNotFoundException"
