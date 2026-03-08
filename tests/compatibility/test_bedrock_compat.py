"""Bedrock compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def bedrock():
    return make_client("bedrock")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _create_job(bedrock, job_name=None, model_name=None):
    """Helper to create a model customization job."""
    job_name = job_name or _unique("job")
    model_name = model_name or _unique("model")
    r = bedrock.create_model_customization_job(
        jobName=job_name,
        customModelName=model_name,
        roleArn="arn:aws:iam::123456789012:role/test",
        baseModelIdentifier="amazon.titan-text-express-v1",
        trainingDataConfig={"s3Uri": "s3://test-bucket/train.jsonl"},
        outputDataConfig={"s3Uri": "s3://test-bucket/output/"},
        hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
    )
    return r["jobArn"], job_name, model_name


class TestBedrockCustomModelOperations:
    """Tests for custom model CRUD operations."""

    def test_list_custom_models_returns_list(self, bedrock):
        r = bedrock.list_custom_models()
        assert "modelSummaries" in r
        assert isinstance(r["modelSummaries"], list)

    def test_create_and_get_custom_model(self, bedrock):
        model_name = _unique("model")
        _create_job(bedrock, model_name=model_name)

        # The custom model should appear in the list
        r = bedrock.list_custom_models()
        names = [m["modelName"] for m in r["modelSummaries"]]
        assert model_name in names

        # Get the custom model by name
        r2 = bedrock.get_custom_model(modelIdentifier=model_name)
        assert r2["modelName"] == model_name
        assert "modelArn" in r2
        assert "baseModelArn" in r2

    def test_get_custom_model_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.get_custom_model(modelIdentifier="nonexistent-model")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_custom_model(self, bedrock):
        model_name = _unique("model")
        _create_job(bedrock, model_name=model_name)

        # Verify it exists
        r = bedrock.get_custom_model(modelIdentifier=model_name)
        assert r["modelName"] == model_name

        # Delete it
        bedrock.delete_custom_model(modelIdentifier=model_name)

        # Verify it's gone
        with pytest.raises(ClientError) as exc:
            bedrock.get_custom_model(modelIdentifier=model_name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_custom_model_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.delete_custom_model(modelIdentifier="nonexistent-model")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBedrockModelCustomizationJobs:
    """Tests for model customization job operations."""

    def test_list_model_customization_jobs_returns_list(self, bedrock):
        r = bedrock.list_model_customization_jobs()
        assert "modelCustomizationJobSummaries" in r
        assert isinstance(r["modelCustomizationJobSummaries"], list)

    def test_create_model_customization_job(self, bedrock):
        job_name = _unique("job")
        model_name = _unique("model")
        job_arn, _, _ = _create_job(bedrock, job_name=job_name, model_name=model_name)

        assert "arn:aws:bedrock:" in job_arn
        assert "model-customization-job" in job_arn

    def test_get_model_customization_job_by_name(self, bedrock):
        job_name = _unique("job")
        model_name = _unique("model")
        _create_job(bedrock, job_name=job_name, model_name=model_name)

        r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        assert r["jobName"] == job_name
        assert "outputModelName" in r
        assert "jobArn" in r
        assert "status" in r

    def test_get_model_customization_job_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.get_model_customization_job(jobIdentifier="nonexistent-job")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_model_customization_jobs_with_filter(self, bedrock):
        job_name = _unique("job")
        _create_job(bedrock, job_name=job_name)

        r = bedrock.list_model_customization_jobs(statusEquals="InProgress")
        assert "modelCustomizationJobSummaries" in r
        job_names = [j["jobName"] for j in r["modelCustomizationJobSummaries"]]
        assert job_name in job_names

    def test_stop_model_customization_job(self, bedrock):
        job_arn, job_name, _ = _create_job(bedrock)

        bedrock.stop_model_customization_job(jobIdentifier=job_arn)

        r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        assert r["status"] in ("Stopping", "Stopped")


class TestBedrockLoggingConfiguration:
    """Tests for model invocation logging configuration."""

    def test_put_and_get_logging_config(self, bedrock):
        bedrock.put_model_invocation_logging_configuration(
            loggingConfig={
                "textDataDeliveryEnabled": True,
                "imageDataDeliveryEnabled": False,
                "embeddingDataDeliveryEnabled": True,
            }
        )

        r = bedrock.get_model_invocation_logging_configuration()
        cfg = r["loggingConfig"]
        assert cfg["textDataDeliveryEnabled"] is True
        assert cfg["imageDataDeliveryEnabled"] is False
        assert cfg["embeddingDataDeliveryEnabled"] is True

    def test_update_logging_config(self, bedrock):
        bedrock.put_model_invocation_logging_configuration(
            loggingConfig={
                "textDataDeliveryEnabled": True,
                "imageDataDeliveryEnabled": True,
                "embeddingDataDeliveryEnabled": True,
            }
        )

        # Overwrite with new values
        bedrock.put_model_invocation_logging_configuration(
            loggingConfig={
                "textDataDeliveryEnabled": False,
                "imageDataDeliveryEnabled": False,
                "embeddingDataDeliveryEnabled": False,
            }
        )

        r = bedrock.get_model_invocation_logging_configuration()
        cfg = r["loggingConfig"]
        assert cfg["textDataDeliveryEnabled"] is False
        assert cfg["imageDataDeliveryEnabled"] is False
        assert cfg["embeddingDataDeliveryEnabled"] is False


class TestBedrockTags:
    """Tests for tagging operations on Bedrock resources."""

    def test_tag_and_list_tags(self, bedrock):
        job_arn, _, _ = _create_job(bedrock)

        bedrock.tag_resource(
            resourceARN=job_arn,
            tags=[
                {"key": "env", "value": "test"},
                {"key": "project", "value": "robotocore"},
            ],
        )

        r = bedrock.list_tags_for_resource(resourceARN=job_arn)
        tags = {t["key"]: t["value"] for t in r["tags"]}
        assert tags["env"] == "test"
        assert tags["project"] == "robotocore"

    def test_untag_resource(self, bedrock):
        job_arn, _, _ = _create_job(bedrock)

        bedrock.tag_resource(
            resourceARN=job_arn,
            tags=[
                {"key": "env", "value": "test"},
                {"key": "project", "value": "robotocore"},
            ],
        )

        bedrock.untag_resource(resourceARN=job_arn, tagKeys=["env"])

        r = bedrock.list_tags_for_resource(resourceARN=job_arn)
        tags = {t["key"]: t["value"] for t in r["tags"]}
        assert "env" not in tags
        assert tags["project"] == "robotocore"

    def test_list_tags_empty(self, bedrock):
        job_arn, _, _ = _create_job(bedrock)

        r = bedrock.list_tags_for_resource(resourceARN=job_arn)
        assert r["tags"] == []

    def test_list_tags_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.list_tags_for_resource(
                resourceARN="arn:aws:bedrock:us-east-1:123456789012:custom-model/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBedrockAutoCoverage:
    """Auto-generated coverage tests for bedrock."""

    @pytest.fixture
    def client(self):
        return make_client("bedrock")

    def test_delete_model_invocation_logging_configuration(self, client):
        """DeleteModelInvocationLoggingConfiguration returns a response."""
        client.delete_model_invocation_logging_configuration()
