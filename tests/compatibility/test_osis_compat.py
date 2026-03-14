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

    def test_delete_pipeline(self, osis):
        name = f"test-{uuid.uuid4().hex[:8]}"
        osis.create_pipeline(
            PipelineName=name,
            MinUnits=1,
            MaxUnits=1,
            PipelineConfigurationBody='version: "2"',
        )
        resp = osis.delete_pipeline(PipelineName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_stop_pipeline(self, osis, pipeline):
        resp = osis.stop_pipeline(PipelineName=pipeline["PipelineName"])
        assert resp["Pipeline"]["PipelineName"] == pipeline["PipelineName"]

    def test_start_pipeline_after_stop(self, osis, pipeline):
        osis.stop_pipeline(PipelineName=pipeline["PipelineName"])
        resp = osis.start_pipeline(PipelineName=pipeline["PipelineName"])
        assert resp["Pipeline"]["PipelineName"] == pipeline["PipelineName"]

    def test_update_pipeline(self, osis, pipeline):
        resp = osis.update_pipeline(
            PipelineName=pipeline["PipelineName"],
            MinUnits=1,
            MaxUnits=2,
        )
        assert resp["Pipeline"]["PipelineName"] == pipeline["PipelineName"]
        assert resp["Pipeline"]["MaxUnits"] == 2

    def test_tag_resource(self, osis, pipeline):
        """TagResource adds tags to a pipeline."""
        resp = osis.tag_resource(
            Arn=pipeline["PipelineArn"],
            Tags=[{"Key": "env", "Value": "test"}],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_untag_resource(self, osis, pipeline):
        """UntagResource removes tags from a pipeline."""
        osis.tag_resource(
            Arn=pipeline["PipelineArn"],
            Tags=[{"Key": "env", "Value": "test"}],
        )
        resp = osis.untag_resource(
            Arn=pipeline["PipelineArn"],
            TagKeys=["env"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_tag_and_list_tags(self, osis, pipeline):
        """TagResource + ListTagsForResource roundtrip."""
        osis.tag_resource(
            Arn=pipeline["PipelineArn"],
            Tags=[{"Key": "project", "Value": "robotocore"}],
        )
        resp = osis.list_tags_for_resource(Arn=pipeline["PipelineArn"])
        tag_keys = [t["Key"] for t in resp["Tags"]]
        assert "project" in tag_keys


class TestOSISResourcePolicy:
    """Tests for OSIS Resource Policy operations."""

    def test_get_resource_policy_nonexistent(self, osis):
        """GetResourcePolicy for nonexistent ARN raises error."""
        from botocore.exceptions import ClientError

        fake_arn = "arn:aws:osis:us-east-1:123456789012:pipeline/no-such-pipe"
        with pytest.raises(ClientError) as exc:
            osis.get_resource_policy(ResourceArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_resource_policy_nonexistent(self, osis):
        """DeleteResourcePolicy for nonexistent ARN raises error."""
        from botocore.exceptions import ClientError

        fake_arn = "arn:aws:osis:us-east-1:123456789012:pipeline/no-such-pipe"
        with pytest.raises(ClientError) as exc:
            osis.delete_resource_policy(ResourceArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_pipeline_nonexistent(self, osis):
        """GetPipeline for nonexistent pipeline raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            osis.get_pipeline(PipelineName="no-such-pipeline")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_pipeline_nonexistent(self, osis):
        """DeletePipeline for nonexistent pipeline raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            osis.delete_pipeline(PipelineName="no-such-pipeline")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestOSISPutResourcePolicy:
    """Tests for OSIS PutResourcePolicy operation."""

    def test_put_resource_policy(self, osis, pipeline):
        """PutResourcePolicy sets a policy on a pipeline."""
        import json

        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                        "Action": "osis:*",
                        "Resource": pipeline["PipelineArn"],
                    }
                ],
            }
        )
        resp = osis.put_resource_policy(
            ResourceArn=pipeline["PipelineArn"],
            Policy=policy,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_and_get_resource_policy(self, osis, pipeline):
        """PutResourcePolicy then GetResourcePolicy roundtrip."""
        import json

        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                        "Action": "osis:*",
                        "Resource": pipeline["PipelineArn"],
                    }
                ],
            }
        )
        osis.put_resource_policy(
            ResourceArn=pipeline["PipelineArn"],
            Policy=policy,
        )
        get_resp = osis.get_resource_policy(ResourceArn=pipeline["PipelineArn"])
        assert "Policy" in get_resp

    def test_put_then_delete_resource_policy(self, osis, pipeline):
        """PutResourcePolicy then DeleteResourcePolicy succeeds."""
        import json

        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                        "Action": "osis:*",
                        "Resource": pipeline["PipelineArn"],
                    }
                ],
            }
        )
        osis.put_resource_policy(
            ResourceArn=pipeline["PipelineArn"],
            Policy=policy,
        )
        del_resp = osis.delete_resource_policy(ResourceArn=pipeline["PipelineArn"])
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
