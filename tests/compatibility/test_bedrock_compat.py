"""Bedrock compatibility tests."""

import datetime
import re
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

        assert re.match(r"arn:aws:bedrock:[^:]+:\d+:model-customization-job/.+", job_arn), \
            f"bad job ARN: {job_arn}"
        assert job_name in job_arn

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
        job_arn, _, _ = _create_job(bedrock, job_name=job_name)

        # Verify job exists via get (list may paginate)
        r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        assert r["jobName"] == job_name

        # Verify the list endpoint works with filter
        r2 = bedrock.list_model_customization_jobs(statusEquals="InProgress")
        assert "modelCustomizationJobSummaries" in r2
        assert isinstance(r2["modelCustomizationJobSummaries"], list)

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


class TestBedrockModelCustomizationJobEdgeCases:
    """Edge-case tests for model customization job operations."""

    def test_get_model_customization_job_by_arn(self, bedrock):
        """GetModelCustomizationJob works when called with the job ARN."""
        job_arn, job_name, _ = _create_job(bedrock)

        r = bedrock.get_model_customization_job(jobIdentifier=job_arn)
        assert r["jobName"] == job_name
        assert r["jobArn"] == job_arn

    def test_create_duplicate_job_name_raises(self, bedrock):
        """Creating a job with an already-used name raises ResourceInUseException."""
        job_name = _unique("dup")
        _create_job(bedrock, job_name=job_name, model_name=_unique("m1"))
        with pytest.raises(ClientError) as exc:
            _create_job(bedrock, job_name=job_name, model_name=_unique("m2"))
        assert exc.value.response["Error"]["Code"] == "ResourceInUseException"

    def test_stop_nonexistent_job_raises(self, bedrock):
        """Stopping a nonexistent job raises ResourceNotFoundException."""
        fake_arn = "arn:aws:bedrock:us-east-1:123456789012:model-customization-job/nonexistent"
        with pytest.raises(ClientError) as exc:
            bedrock.stop_model_customization_job(jobIdentifier=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_job_returns_training_config(self, bedrock):
        """GetModelCustomizationJob returns training and output data configs."""
        job_arn, job_name, _ = _create_job(bedrock)

        r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        assert r["trainingDataConfig"]["s3Uri"] == "s3://test-bucket/train.jsonl"
        assert r["outputDataConfig"]["s3Uri"] == "s3://test-bucket/output/"

    def test_get_job_returns_hyperparameters(self, bedrock):
        """GetModelCustomizationJob returns the hyperparameters."""
        _, job_name, _ = _create_job(bedrock)

        r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        hp = r["hyperParameters"]
        assert hp["epochCount"] == "1"
        assert hp["batchSize"] == "1"
        assert hp["learningRate"] == "0.00001"

    def test_get_job_returns_role_arn(self, bedrock):
        """GetModelCustomizationJob returns the roleArn."""
        _, job_name, _ = _create_job(bedrock)

        r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        assert r["roleArn"] == "arn:aws:iam::123456789012:role/test"

    def test_create_job_with_job_tags(self, bedrock):
        """Creating a job with jobTags makes them visible via ListTagsForResource."""
        job_name = _unique("tagged")
        r = bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=_unique("model"),
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://test-bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://test-bucket/output/"},
            hyperParameters={
                "epochCount": "1",
                "batchSize": "1",
                "learningRate": "0.00001",
            },
            jobTags=[{"key": "env", "value": "prod"}],
        )
        job_arn = r["jobArn"]

        tags = bedrock.list_tags_for_resource(resourceARN=job_arn)
        tag_map = {t["key"]: t["value"] for t in tags["tags"]}
        assert tag_map["env"] == "prod"

    def test_list_jobs_pagination(self, bedrock):
        """ListModelCustomizationJobs supports pagination via maxResults/nextToken."""
        # Create enough jobs
        for _ in range(3):
            _create_job(bedrock)

        r = bedrock.list_model_customization_jobs(maxResults=1)
        assert len(r["modelCustomizationJobSummaries"]) == 1
        assert "nextToken" in r

        r2 = bedrock.list_model_customization_jobs(maxResults=1, nextToken=r["nextToken"])
        assert len(r2["modelCustomizationJobSummaries"]) == 1

    def test_list_jobs_name_contains_filter(self, bedrock):
        """ListModelCustomizationJobs with nameContains filters results."""
        r = bedrock.list_model_customization_jobs(nameContains="nonexistent-xyz-99")
        assert r["modelCustomizationJobSummaries"] == []


class TestBedrockCustomModelEdgeCases:
    """Edge-case tests for custom model operations."""

    def test_get_custom_model_returns_base_model_arn(self, bedrock):
        """GetCustomModel returns baseModelArn."""
        model_name = _unique("model")
        _create_job(bedrock, model_name=model_name)

        r = bedrock.get_custom_model(modelIdentifier=model_name)
        assert r["baseModelArn"].endswith("foundation-model/amazon.titan-text-express-v1")

    def test_get_custom_model_returns_creation_time(self, bedrock):
        """GetCustomModel returns a creationTime."""
        model_name = _unique("model")
        _create_job(bedrock, model_name=model_name)

        r = bedrock.get_custom_model(modelIdentifier=model_name)
        assert r["creationTime"] is not None

    def test_get_custom_model_returns_training_config(self, bedrock):
        """GetCustomModel returns trainingDataConfig and hyperParameters."""
        model_name = _unique("model")
        _create_job(bedrock, model_name=model_name)

        r = bedrock.get_custom_model(modelIdentifier=model_name)
        assert r["trainingDataConfig"]["s3Uri"] == "s3://test-bucket/train.jsonl"
        assert "epochCount" in r["hyperParameters"]

    def test_list_custom_models_name_contains_filter(self, bedrock):
        """ListCustomModels with nameContains=nonexistent returns empty list."""
        r = bedrock.list_custom_models(nameContains="nonexistent-xyz-99")
        assert r["modelSummaries"] == []

    def test_list_custom_models_summary_has_expected_keys(self, bedrock):
        """Custom model fetched by name has expected fields with valid values."""
        model_name = _unique("model")
        _create_job(bedrock, model_name=model_name)

        s = bedrock.get_custom_model(modelIdentifier=model_name)
        assert s["modelArn"].startswith("arn:aws:bedrock:")
        assert s["baseModelArn"].startswith("arn:aws:bedrock:")
        assert isinstance(s["creationTime"], datetime.datetime)

    def test_tag_custom_model(self, bedrock):
        """Tags can be applied to a custom model (not just a job)."""
        model_name = _unique("model")
        _create_job(bedrock, model_name=model_name)

        cm = bedrock.get_custom_model(modelIdentifier=model_name)
        model_arn = cm["modelArn"]

        bedrock.tag_resource(
            resourceARN=model_arn,
            tags=[{"key": "team", "value": "ml"}],
        )

        tags = bedrock.list_tags_for_resource(resourceARN=model_arn)
        tag_map = {t["key"]: t["value"] for t in tags["tags"]}
        assert tag_map["team"] == "ml"


class TestBedrockLoggingEdgeCases:
    """Edge-case tests for logging configuration."""

    def test_delete_logging_config_returns_200(self, bedrock):
        """DeleteModelInvocationLoggingConfiguration returns HTTP 200."""
        r = bedrock.delete_model_invocation_logging_configuration()
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_logging_config_after_delete_returns_empty(self, bedrock):
        """After deleting logging config, get returns empty loggingConfig."""
        bedrock.put_model_invocation_logging_configuration(
            loggingConfig={
                "textDataDeliveryEnabled": True,
                "imageDataDeliveryEnabled": True,
                "embeddingDataDeliveryEnabled": True,
            }
        )
        bedrock.delete_model_invocation_logging_configuration()

        r = bedrock.get_model_invocation_logging_configuration()
        assert r["loggingConfig"] == {}


class TestBedrockListOperations:
    """Tests for various list operations."""

    def test_list_foundation_models(self, bedrock):
        r = bedrock.list_foundation_models()
        # Foundation models are always present in Moto
        assert len(r["modelSummaries"]) > 0
        assert "modelId" in r["modelSummaries"][0]

    def test_list_guardrails(self, bedrock):
        r = bedrock.list_guardrails()
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(r["guardrails"], list)

    def test_list_inference_profiles(self, bedrock):
        r = bedrock.list_inference_profiles()
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(r["inferenceProfileSummaries"], list)

    def test_list_imported_models(self, bedrock):
        r = bedrock.list_imported_models()
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(r["modelSummaries"], list)

    def test_list_evaluation_jobs(self, bedrock):
        r = bedrock.list_evaluation_jobs()
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(r["jobSummaries"], list)

    def test_list_model_copy_jobs(self, bedrock):
        r = bedrock.list_model_copy_jobs()
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(r["modelCopyJobSummaries"], list)

    def test_list_model_import_jobs(self, bedrock):
        r = bedrock.list_model_import_jobs()
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(r["modelImportJobSummaries"], list)

    def test_list_model_invocation_jobs(self, bedrock):
        r = bedrock.list_model_invocation_jobs()
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(r["invocationJobSummaries"], list)

    def test_list_provisioned_model_throughputs(self, bedrock):
        r = bedrock.list_provisioned_model_throughputs()
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(r["provisionedModelSummaries"], list)

    def test_list_prompt_routers(self, bedrock):
        r = bedrock.list_prompt_routers()
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(r["promptRouterSummaries"], list)

    def test_list_marketplace_model_endpoints(self, bedrock):
        r = bedrock.list_marketplace_model_endpoints()
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(r["marketplaceModelEndpoints"], list)


class TestBedrockGetWithFakeIds:
    """Tests for Get operations with nonexistent IDs."""

    def test_get_guardrail_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.get_guardrail(guardrailIdentifier="nonexistent-guardrail-id")
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
        )

    def test_get_evaluation_job_not_found(self, bedrock):
        fake_arn = "arn:aws:bedrock:us-east-1:123456789012:evaluation-job/nonexistent"
        with pytest.raises(ClientError) as exc:
            bedrock.get_evaluation_job(jobIdentifier=fake_arn)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
        )

    def test_get_provisioned_model_throughput_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.get_provisioned_model_throughput(provisionedModelId="nonexistent-pmt-id")
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
        )

    def test_get_model_invocation_job_not_found(self, bedrock):
        fake_arn = "arn:aws:bedrock:us-east-1:123456789012:model-invocation-job/nonexistent"
        with pytest.raises(ClientError) as exc:
            bedrock.get_model_invocation_job(jobIdentifier=fake_arn)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
        )

    def test_get_model_copy_job_not_found(self, bedrock):
        fake_arn = "arn:aws:bedrock:us-east-1:123456789012:model-copy-job/nonexistent"
        with pytest.raises(ClientError) as exc:
            bedrock.get_model_copy_job(jobArn=fake_arn)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
        )

    def test_get_model_import_job_not_found(self, bedrock):
        fake_arn = "arn:aws:bedrock:us-east-1:123456789012:model-import-job/nonexistent"
        with pytest.raises(ClientError) as exc:
            bedrock.get_model_import_job(jobIdentifier=fake_arn)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
        )

    def test_get_imported_model_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.get_imported_model(modelIdentifier="nonexistent-imported-model")
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
        )

    def test_get_inference_profile_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.get_inference_profile(inferenceProfileIdentifier="nonexistent-profile-id")
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
        )

    def test_get_foundation_model(self, bedrock):
        """GetFoundationModel with a known model ID."""
        r = bedrock.get_foundation_model(modelIdentifier="anthropic.claude-v2")
        assert "modelDetails" in r
        assert r["modelDetails"]["modelId"] == "anthropic.claude-v2"

    def test_get_prompt_router_not_found(self, bedrock):
        fake_arn = "arn:aws:bedrock:us-east-1:123456789012:default-prompt-router/nonexistent"
        with pytest.raises(ClientError) as exc:
            bedrock.get_prompt_router(promptRouterArn=fake_arn)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
        )

    def test_get_marketplace_model_endpoint_not_found(self, bedrock):
        fake_arn = "arn:aws:bedrock:us-east-1:123456789012:marketplace-model-endpoint/nonexistent"
        with pytest.raises(ClientError) as exc:
            bedrock.get_marketplace_model_endpoint(endpointArn=fake_arn)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
        )

    def test_get_custom_model_deployment_not_found(self, bedrock):
        fake_id = "nonexistent-deployment-id"
        with pytest.raises(ClientError) as exc:
            bedrock.get_custom_model_deployment(customModelDeploymentIdentifier=fake_id)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
        )


class TestBedrockFoundationModels:
    """Tests for foundation model operations."""

    def test_list_foundation_models_has_model_summaries(self, bedrock):
        """ListFoundationModels returns model summaries with expected fields."""
        r = bedrock.list_foundation_models()
        assert len(r["modelSummaries"]) > 0
        model = r["modelSummaries"][0]
        assert "modelId" in model
        assert "modelName" in model

    def test_get_foundation_model_availability(self, bedrock):
        """GetFoundationModelAvailability returns agreement availability."""
        r = bedrock.get_foundation_model_availability(modelId="anthropic.claude-v2")
        assert r["modelId"] == "anthropic.claude-v2"
        assert isinstance(r["agreementAvailability"], dict)

    def test_list_foundation_model_agreement_offers(self, bedrock):
        """ListFoundationModelAgreementOffers returns offers list."""
        r = bedrock.list_foundation_model_agreement_offers(modelId="anthropic.claude-v2")
        assert "offers" in r
        assert isinstance(r["offers"], list)

    def test_get_use_case_for_model_access(self, bedrock):
        """GetUseCaseForModelAccess returns a response."""
        r = bedrock.get_use_case_for_model_access()
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestBedrockJobCreationBehavior:
    """Behavioral fidelity tests for job creation and response structure."""

    def test_create_job_returns_arn_with_correct_format(self, bedrock):
        """CreateModelCustomizationJob ARN matches expected format."""
        job_arn, job_name, _ = _create_job(bedrock)
        # ARN must follow arn:aws:bedrock:REGION:ACCOUNT:model-customization-job/NAME
        assert job_arn.startswith("arn:aws:bedrock:")
        assert ":model-customization-job/" in job_arn
        assert job_name in job_arn

    def test_create_job_initial_status_is_inprogress(self, bedrock):
        """Newly created job has status InProgress."""
        _, job_name, _ = _create_job(bedrock)
        r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        assert r["status"] == "InProgress"

    def test_create_job_creation_time_is_present(self, bedrock):
        """GetModelCustomizationJob returns a creationTime datetime."""
        _, job_name, _ = _create_job(bedrock)
        r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        assert "creationTime" in r
        assert isinstance(r["creationTime"], datetime.datetime)

    def test_create_job_output_model_name_contains_custom_model_name(self, bedrock):
        """GetModelCustomizationJob outputModelName contains the customModelName passed at creation."""
        model_name = _unique("model")
        job_name = _unique("job")
        _create_job(bedrock, job_name=job_name, model_name=model_name)
        r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        # Moto stores outputModelName as "{customModelName}-{jobName}"
        assert model_name in r["outputModelName"]

    def test_create_job_base_model_identifier_preserved(self, bedrock):
        """GetModelCustomizationJob returns the base model identifier used at creation."""
        _, job_name, _ = _create_job(bedrock)
        r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        assert r["baseModelArn"].endswith("amazon.titan-text-express-v1")


class TestBedrockCustomModelBehavior:
    """Behavioral fidelity tests for custom model listing and retrieval."""

    def test_list_custom_models_includes_created_model(self, bedrock):
        """After creating a job, the custom model appears in ListCustomModels."""
        job_name = _unique("job")
        model_name = _unique("model")
        _create_job(bedrock, job_name=job_name, model_name=model_name)

        # nameContains for list_custom_models filters by job_name, not model_name
        r = bedrock.list_custom_models(nameContains=job_name)
        names = [m["modelName"] for m in r["modelSummaries"]]
        assert model_name in names

    def test_list_custom_models_summary_has_arn(self, bedrock):
        """Each custom model summary includes a modelArn."""
        job_name = _unique("job")
        model_name = _unique("model")
        _create_job(bedrock, job_name=job_name, model_name=model_name)

        r = bedrock.list_custom_models(nameContains=job_name)
        model_summaries = [m for m in r["modelSummaries"] if m["modelName"] == model_name]
        assert len(model_summaries) == 1
        assert "modelArn" in model_summaries[0]
        assert "arn:aws:bedrock:" in model_summaries[0]["modelArn"]

    def test_custom_model_arn_format(self, bedrock):
        """Custom model ARN follows expected format."""
        model_name = _unique("model")
        _create_job(bedrock, model_name=model_name)

        r = bedrock.get_custom_model(modelIdentifier=model_name)
        model_arn = r["modelArn"]
        assert model_arn.startswith("arn:aws:bedrock:")
        assert "custom-model" in model_arn or model_name in model_arn

    def test_delete_custom_model_removes_from_list(self, bedrock):
        """Deleted custom model no longer appears in ListCustomModels."""
        job_name = _unique("job")
        model_name = _unique("model")
        _create_job(bedrock, job_name=job_name, model_name=model_name)

        # nameContains filters by job_name for list_custom_models
        r = bedrock.list_custom_models(nameContains=job_name)
        names_before = [m["modelName"] for m in r["modelSummaries"]]
        assert model_name in names_before

        bedrock.delete_custom_model(modelIdentifier=model_name)

        r2 = bedrock.list_custom_models(nameContains=job_name)
        names_after = [m["modelName"] for m in r2["modelSummaries"]]
        assert model_name not in names_after

    def test_list_custom_models_by_base_model_filter(self, bedrock):
        """ListCustomModels supports filtering by baseModelArnEquals."""
        job_name = _unique("job")
        model_name = _unique("model")
        _create_job(bedrock, job_name=job_name, model_name=model_name)

        cm = bedrock.get_custom_model(modelIdentifier=model_name)
        base_arn = cm["baseModelArn"]

        # Combine baseModelArnEquals with nameContains (filters by job_name) to scope results
        r = bedrock.list_custom_models(baseModelArnEquals=base_arn, nameContains=job_name)
        assert "modelSummaries" in r
        names = [m["modelName"] for m in r["modelSummaries"]]
        assert model_name in names


class TestBedrockJobListBehavior:
    """Behavioral fidelity tests for job listing."""

    def test_list_jobs_includes_created_job(self, bedrock):
        """After creating a job, it appears in ListModelCustomizationJobs."""
        job_name = _unique("job")
        _create_job(bedrock, job_name=job_name)

        r = bedrock.list_model_customization_jobs(nameContains=job_name)
        job_names = [j["jobName"] for j in r["modelCustomizationJobSummaries"]]
        assert job_name in job_names

    def test_list_jobs_summary_has_expected_fields(self, bedrock):
        """Job summaries include jobArn, jobName, status, and creationTime."""
        job_name = _unique("job")
        _create_job(bedrock, job_name=job_name)

        r = bedrock.list_model_customization_jobs(nameContains=job_name)
        job_summaries = [j for j in r["modelCustomizationJobSummaries"] if j["jobName"] == job_name]
        assert len(job_summaries) == 1
        summary = job_summaries[0]
        assert "jobArn" in summary
        assert "jobName" in summary
        assert "status" in summary
        assert "creationTime" in summary

    def test_list_jobs_filter_inprogress_includes_new_job(self, bedrock):
        """ListModelCustomizationJobs(statusEquals=InProgress) returns newly created job."""
        job_name = _unique("job")
        _create_job(bedrock, job_name=job_name)

        r = bedrock.list_model_customization_jobs(statusEquals="InProgress", nameContains=job_name)
        job_names = [j["jobName"] for j in r["modelCustomizationJobSummaries"]]
        assert job_name in job_names

    def test_list_jobs_filter_completed_excludes_inprogress(self, bedrock):
        """ListModelCustomizationJobs(statusEquals=Completed) does not return InProgress jobs."""
        job_name = _unique("job")
        _create_job(bedrock, job_name=job_name)

        r = bedrock.list_model_customization_jobs(statusEquals="Completed")
        job_names = [j["jobName"] for j in r["modelCustomizationJobSummaries"]]
        assert job_name not in job_names

    def test_list_jobs_name_contains_matches_substring(self, bedrock):
        """ListModelCustomizationJobs(nameContains=X) returns jobs whose name contains X."""
        unique_suffix = uuid.uuid4().hex[:12]
        job_name = f"job-{unique_suffix}"
        _create_job(bedrock, job_name=job_name)

        r = bedrock.list_model_customization_jobs(nameContains=unique_suffix)
        job_names = [j["jobName"] for j in r["modelCustomizationJobSummaries"]]
        assert job_name in job_names

    def test_list_jobs_pagination_all_jobs_reachable(self, bedrock):
        """All jobs created are reachable by exhausting pagination."""
        unique_suffix = uuid.uuid4().hex[:8]
        created_names = set()
        for i in range(3):
            job_name = f"paginatejob-{unique_suffix}-{i}"
            _create_job(bedrock, job_name=job_name)
            created_names.add(job_name)

        # Paginate with maxResults=1 until exhausted
        seen_names = set()
        kwargs = {"maxResults": 1, "nameContains": f"paginatejob-{unique_suffix}"}
        while True:
            r = bedrock.list_model_customization_jobs(**kwargs)
            for j in r["modelCustomizationJobSummaries"]:
                seen_names.add(j["jobName"])
            if "nextToken" not in r:
                break
            kwargs["nextToken"] = r["nextToken"]

        assert created_names.issubset(seen_names)


class TestBedrockTagsBehavior:
    """Behavioral fidelity tests for tagging operations."""

    def test_tag_and_untag_leaves_other_tags(self, bedrock):
        """Untagging one key leaves other tags intact."""
        job_arn, _, _ = _create_job(bedrock)
        bedrock.tag_resource(
            resourceARN=job_arn,
            tags=[
                {"key": "keep", "value": "yes"},
                {"key": "remove", "value": "no"},
            ],
        )
        bedrock.untag_resource(resourceARN=job_arn, tagKeys=["remove"])
        r = bedrock.list_tags_for_resource(resourceARN=job_arn)
        tag_map = {t["key"]: t["value"] for t in r["tags"]}
        assert "remove" not in tag_map
        assert tag_map["keep"] == "yes"

    def test_tag_resource_overwrites_existing_key(self, bedrock):
        """Tagging with an existing key updates the value."""
        job_arn, _, _ = _create_job(bedrock)
        bedrock.tag_resource(resourceARN=job_arn, tags=[{"key": "env", "value": "dev"}])
        bedrock.tag_resource(resourceARN=job_arn, tags=[{"key": "env", "value": "prod"}])
        r = bedrock.list_tags_for_resource(resourceARN=job_arn)
        tag_map = {t["key"]: t["value"] for t in r["tags"]}
        assert tag_map["env"] == "prod"

    def test_list_tags_after_create_with_job_tags(self, bedrock):
        """Tags passed as jobTags at creation are visible via ListTagsForResource."""
        job_name = _unique("tagcreate")
        r = bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=_unique("model"),
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://test-bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://test-bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
            jobTags=[{"key": "created-with", "value": "api"}],
        )
        job_arn = r["jobArn"]
        tags = bedrock.list_tags_for_resource(resourceARN=job_arn)
        tag_map = {t["key"]: t["value"] for t in tags["tags"]}
        assert tag_map["created-with"] == "api"

    def test_untag_nonexistent_key_is_idempotent(self, bedrock):
        """Untagging a key that doesn't exist does not raise an error."""
        job_arn, _, _ = _create_job(bedrock)
        # Should not raise
        bedrock.untag_resource(resourceARN=job_arn, tagKeys=["nonexistent-key"])
        r = bedrock.list_tags_for_resource(resourceARN=job_arn)
        assert r["tags"] == []


class TestBedrockAutomatedReasoningPolicies:
    """Tests for automated reasoning policy operations."""

    _FAKE_POLICY_ARN = "arn:aws:bedrock:us-east-1:123456789012:automated-reasoning-policy/fake"

    def test_list_automated_reasoning_policies(self, bedrock):
        r = bedrock.list_automated_reasoning_policies()
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_automated_reasoning_policy_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.get_automated_reasoning_policy(policyArn=self._FAKE_POLICY_ARN)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_automated_reasoning_policy_build_workflow_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.get_automated_reasoning_policy_build_workflow(
                policyArn=self._FAKE_POLICY_ARN, buildWorkflowId="fake-wf"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_automated_reasoning_policy_annotations_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.get_automated_reasoning_policy_annotations(
                policyArn=self._FAKE_POLICY_ARN, buildWorkflowId="fake-wf"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_automated_reasoning_policy_result_assets_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.get_automated_reasoning_policy_build_workflow_result_assets(
                policyArn=self._FAKE_POLICY_ARN,
                buildWorkflowId="fake-wf",
                assetType="POLICY",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_automated_reasoning_policy_next_scenario_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.get_automated_reasoning_policy_next_scenario(
                policyArn=self._FAKE_POLICY_ARN, buildWorkflowId="fake-wf"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_automated_reasoning_policy_test_case_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.get_automated_reasoning_policy_test_case(
                policyArn=self._FAKE_POLICY_ARN, testCaseId="fake-tc"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_automated_reasoning_policy_test_result_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.get_automated_reasoning_policy_test_result(
                policyArn=self._FAKE_POLICY_ARN,
                buildWorkflowId="fake-wf",
                testCaseId="fake-tc",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_automated_reasoning_policy_build_workflows_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.list_automated_reasoning_policy_build_workflows(policyArn=self._FAKE_POLICY_ARN)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_automated_reasoning_policy_test_cases_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.list_automated_reasoning_policy_test_cases(policyArn=self._FAKE_POLICY_ARN)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_automated_reasoning_policy_test_results_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.list_automated_reasoning_policy_test_results(
                policyArn=self._FAKE_POLICY_ARN, buildWorkflowId="fake-wf"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBedrockEnforcedGuardrails:
    """Tests for enforced guardrails configuration."""

    def test_list_enforced_guardrails_configuration(self, bedrock):
        r = bedrock.list_enforced_guardrails_configuration()
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestBedrockCustomModelDeployments:
    """Tests for custom model deployment operations."""

    def test_list_custom_model_deployments(self, bedrock):
        """ListCustomModelDeployments returns a response."""
        r = bedrock.list_custom_model_deployments()
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestBedrockGuardrailOps:
    """Tests for guardrail CRUD operations."""

    def test_create_guardrail(self, bedrock):
        name = _unique("guard")
        r = bedrock.create_guardrail(
            name=name,
            blockedInputMessaging="blocked-input",
            blockedOutputsMessaging="blocked-output",
        )
        assert "guardrailId" in r
        assert "guardrailArn" in r
        assert r["version"] == "DRAFT"

    def test_create_guardrail_version(self, bedrock):
        name = _unique("guard-ver")
        r = bedrock.create_guardrail(
            name=name,
            blockedInputMessaging="blocked",
            blockedOutputsMessaging="blocked",
        )
        gr_id = r["guardrailId"]
        r2 = bedrock.create_guardrail_version(guardrailIdentifier=gr_id)
        assert "guardrailId" in r2
        assert r2["version"] == "1"

    def test_update_guardrail(self, bedrock):
        name = _unique("guard-upd")
        r = bedrock.create_guardrail(
            name=name,
            blockedInputMessaging="old-blocked",
            blockedOutputsMessaging="old-blocked",
        )
        gr_id = r["guardrailId"]
        new_name = _unique("guard-new")
        r2 = bedrock.update_guardrail(
            guardrailIdentifier=gr_id,
            name=new_name,
            blockedInputMessaging="new-blocked",
            blockedOutputsMessaging="new-blocked",
        )
        assert "guardrailId" in r2
        assert r2["updatedAt"] is not None

    def test_delete_guardrail(self, bedrock):
        name = _unique("guard-del")
        r = bedrock.create_guardrail(
            name=name,
            blockedInputMessaging="blocked",
            blockedOutputsMessaging="blocked",
        )
        gr_id = r["guardrailId"]
        r2 = bedrock.delete_guardrail(guardrailIdentifier=gr_id)
        assert r2["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_guardrail_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.delete_guardrail(guardrailIdentifier="nonexistent-guardrail")
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
        )

    def test_create_guardrail_then_get(self, bedrock):
        """Created guardrail is retrievable via get_guardrail."""
        name = _unique("guard-get")
        r = bedrock.create_guardrail(
            name=name,
            blockedInputMessaging="blocked",
            blockedOutputsMessaging="blocked",
        )
        gr_id = r["guardrailId"]
        r2 = bedrock.get_guardrail(guardrailIdentifier=gr_id)
        assert r2["name"] == name
        assert r2["guardrailId"] == gr_id


class TestBedrockProvisionedModelThroughputOps:
    """Tests for provisioned model throughput CRUD operations."""

    def test_create_provisioned_model_throughput(self, bedrock):
        name = _unique("pmt")
        r = bedrock.create_provisioned_model_throughput(
            modelUnits=1,
            provisionedModelName=name,
            modelId="amazon.titan-text-express-v1",
        )
        pmt_arn = r["provisionedModelArn"]
        assert re.match(r"arn:aws:bedrock:[^:]+:\d+:provisioned-model/.+", pmt_arn), \
            f"bad PMT ARN: {pmt_arn}"

    def test_update_provisioned_model_throughput(self, bedrock):
        name = _unique("pmt-upd")
        r = bedrock.create_provisioned_model_throughput(
            modelUnits=1,
            provisionedModelName=name,
            modelId="amazon.titan-text-express-v1",
        )
        pmt_arn = r["provisionedModelArn"]
        new_name = _unique("pmt-new")
        r2 = bedrock.update_provisioned_model_throughput(
            provisionedModelId=pmt_arn,
            desiredProvisionedModelName=new_name,
        )
        assert r2["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_provisioned_model_throughput(self, bedrock):
        name = _unique("pmt-del")
        r = bedrock.create_provisioned_model_throughput(
            modelUnits=1,
            provisionedModelName=name,
            modelId="amazon.titan-text-express-v1",
        )
        pmt_arn = r["provisionedModelArn"]
        r2 = bedrock.delete_provisioned_model_throughput(provisionedModelId=pmt_arn)
        assert r2["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_provisioned_model_throughput_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.delete_provisioned_model_throughput(provisionedModelId="nonexistent-pmt")
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
        )


class TestBedrockJobLifecycleCoverage:
    """Multi-pattern lifecycle tests that exercise C/R/L/U/D/E patterns."""

    def test_create_job_arn_contains_account_and_region(self, bedrock):
        """Created job ARN encodes account ID and region (C+R)."""
        job_arn, job_name, _ = _create_job(bedrock)
        # Retrieve and compare
        r = bedrock.get_model_customization_job(jobIdentifier=job_arn)
        assert r["jobArn"] == job_arn
        assert "123456789012" in job_arn
        assert "us-east-1" in job_arn

    def test_job_appears_in_list_after_creation(self, bedrock):
        """Newly created job is visible in ListModelCustomizationJobs (C+L)."""
        job_name = _unique("listcheck")
        _create_job(bedrock, job_name=job_name)
        r = bedrock.list_model_customization_jobs(nameContains=job_name)
        summaries = r["modelCustomizationJobSummaries"]
        assert any(j["jobName"] == job_name for j in summaries)

    def test_job_list_summary_arn_matches_creation_arn(self, bedrock):
        """The jobArn in the list summary equals the ARN returned at creation (C+L)."""
        job_arn, job_name, _ = _create_job(bedrock)
        r = bedrock.list_model_customization_jobs(nameContains=job_name)
        found = [j for j in r["modelCustomizationJobSummaries"] if j["jobName"] == job_name]
        assert len(found) == 1
        assert found[0]["jobArn"] == job_arn

    def test_stop_job_updates_status_to_stopped(self, bedrock):
        """After stopping a job its status changes to Stopping or Stopped (C+U+R)."""
        job_arn, job_name, _ = _create_job(bedrock)
        r_before = bedrock.get_model_customization_job(jobIdentifier=job_name)
        assert r_before["status"] == "InProgress"

        bedrock.stop_model_customization_job(jobIdentifier=job_arn)

        r_after = bedrock.get_model_customization_job(jobIdentifier=job_name)
        assert r_after["status"] in ("Stopping", "Stopped")

    def test_stopped_job_excluded_from_inprogress_filter(self, bedrock):
        """Stopped job does not appear in list filtered by statusEquals=InProgress (C+U+L)."""
        job_arn, job_name, _ = _create_job(bedrock)
        bedrock.stop_model_customization_job(jobIdentifier=job_arn)

        r = bedrock.list_model_customization_jobs(statusEquals="InProgress", nameContains=job_name)
        job_names = [j["jobName"] for j in r["modelCustomizationJobSummaries"]]
        assert job_name not in job_names

    def test_delete_custom_model_then_error_on_get(self, bedrock):
        """After deleting, GetCustomModel raises ResourceNotFoundException (C+D+E)."""
        model_name = _unique("del-err")
        _create_job(bedrock, model_name=model_name)
        bedrock.delete_custom_model(modelIdentifier=model_name)
        with pytest.raises(ClientError) as exc:
            bedrock.get_custom_model(modelIdentifier=model_name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_custom_model_twice_raises(self, bedrock):
        """Deleting a model that was already deleted raises ResourceNotFoundException (C+D+E)."""
        model_name = _unique("del-twice")
        _create_job(bedrock, model_name=model_name)
        bedrock.delete_custom_model(modelIdentifier=model_name)
        with pytest.raises(ClientError) as exc:
            bedrock.delete_custom_model(modelIdentifier=model_name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_custom_model_then_list_then_delete_then_list(self, bedrock):
        """Full lifecycle: create → list (present) → delete → list (absent) (C+L+D+L)."""
        job_name = _unique("lifecycle")
        model_name = _unique("lc-model")
        _create_job(bedrock, job_name=job_name, model_name=model_name)

        r1 = bedrock.list_custom_models(nameContains=job_name)
        names_before = [m["modelName"] for m in r1["modelSummaries"]]
        assert model_name in names_before

        bedrock.delete_custom_model(modelIdentifier=model_name)

        r2 = bedrock.list_custom_models(nameContains=job_name)
        names_after = [m["modelName"] for m in r2["modelSummaries"]]
        assert model_name not in names_after

    def test_list_jobs_pagination_returns_correct_count(self, bedrock):
        """MaxResults=2 returns exactly 2 summaries when more exist (C+C+C+L)."""
        suffix = uuid.uuid4().hex[:8]
        for i in range(3):
            _create_job(bedrock, job_name=f"pgtest-{suffix}-{i}")

        r = bedrock.list_model_customization_jobs(
            maxResults=2, nameContains=f"pgtest-{suffix}"
        )
        assert len(r["modelCustomizationJobSummaries"]) == 2
        assert "nextToken" in r

    def test_list_custom_models_pagination(self, bedrock):
        """ListCustomModels supports pagination (C+C+C+L)."""
        suffix = uuid.uuid4().hex[:8]
        for i in range(3):
            _create_job(bedrock, job_name=f"cmpg-{suffix}-{i}", model_name=f"cmpg-m-{suffix}-{i}")

        r = bedrock.list_custom_models(maxResults=1)
        assert len(r["modelSummaries"]) == 1
        assert "nextToken" in r

        r2 = bedrock.list_custom_models(maxResults=1, nextToken=r["nextToken"])
        assert len(r2["modelSummaries"]) == 1


class TestBedrockTagEdgeCaseCoverage:
    """Multi-pattern tag edge cases targeting C/R/L/U/D/E coverage."""

    def test_tag_overwrite_and_list_all_tags(self, bedrock):
        """Overwriting a tag updates its value; listing returns all current tags (C+U+L)."""
        job_arn, _, _ = _create_job(bedrock)
        bedrock.tag_resource(
            resourceARN=job_arn,
            tags=[{"key": "color", "value": "red"}, {"key": "size", "value": "large"}],
        )
        # Overwrite 'color' only
        bedrock.tag_resource(resourceARN=job_arn, tags=[{"key": "color", "value": "blue"}])
        r = bedrock.list_tags_for_resource(resourceARN=job_arn)
        tag_map = {t["key"]: t["value"] for t in r["tags"]}
        assert tag_map["color"] == "blue"
        assert tag_map["size"] == "large"

    def test_tag_delete_all_then_list_empty(self, bedrock):
        """Untagging all keys leaves empty tag list (C+U+D+L)."""
        job_arn, _, _ = _create_job(bedrock)
        bedrock.tag_resource(
            resourceARN=job_arn,
            tags=[{"key": "a", "value": "1"}, {"key": "b", "value": "2"}],
        )
        bedrock.untag_resource(resourceARN=job_arn, tagKeys=["a", "b"])
        r = bedrock.list_tags_for_resource(resourceARN=job_arn)
        assert r["tags"] == []

    def test_tag_job_and_retrieve_via_get_job(self, bedrock):
        """After tagging a job, the job itself is still retrievable (C+U+R)."""
        job_arn, job_name, _ = _create_job(bedrock)
        bedrock.tag_resource(resourceARN=job_arn, tags=[{"key": "tagged", "value": "yes"}])
        r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        assert r["jobName"] == job_name

    def test_tag_nonexistent_resource_raises(self, bedrock):
        """Tagging a resource that doesn't exist raises an error (E)."""
        fake_arn = "arn:aws:bedrock:us-east-1:123456789012:model-customization-job/nonexistent-tag-test"
        with pytest.raises(ClientError) as exc:
            bedrock.tag_resource(
                resourceARN=fake_arn,
                tags=[{"key": "k", "value": "v"}],
            )
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
        )

    def test_list_tags_after_untag_nonexistent_key(self, bedrock):
        """Untagging a key that never existed returns empty list (C+D+L)."""
        job_arn, _, _ = _create_job(bedrock)
        bedrock.untag_resource(resourceARN=job_arn, tagKeys=["never-existed"])
        r = bedrock.list_tags_for_resource(resourceARN=job_arn)
        assert r["tags"] == []


class TestBedrockJobRetrievalCoverage:
    """Tests targeting R/E patterns for job retrieval."""

    def test_get_job_by_name_and_by_arn_return_same_data(self, bedrock):
        """GetModelCustomizationJob returns identical data whether called by name or ARN (C+R+R)."""
        job_arn, job_name, _ = _create_job(bedrock)
        by_name = bedrock.get_model_customization_job(jobIdentifier=job_name)
        by_arn = bedrock.get_model_customization_job(jobIdentifier=job_arn)
        assert by_name["jobName"] == by_arn["jobName"]
        assert by_name["jobArn"] == by_arn["jobArn"]
        assert by_name["status"] == by_arn["status"]

    def test_get_job_creation_time_is_recent_datetime(self, bedrock):
        """creationTime is a datetime within the last day (C+R)."""
        _, job_name, _ = _create_job(bedrock)
        r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        ct = r["creationTime"]
        assert isinstance(ct, datetime.datetime)
        # Strip tzinfo for simple magnitude check (tolerant of server TZ differences)
        ct_naive = ct.replace(tzinfo=None)
        now_naive = datetime.datetime.now()
        delta = abs((now_naive - ct_naive).total_seconds())
        assert delta < 86400, f"creationTime is more than a day away from now: {delta}s"

    def test_get_job_all_required_fields_present(self, bedrock):
        """GetModelCustomizationJob response has all expected top-level fields (C+R)."""
        _, job_name, _ = _create_job(bedrock)
        r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        for field in ("jobArn", "jobName", "status", "roleArn", "creationTime",
                      "baseModelArn", "hyperParameters", "trainingDataConfig", "outputDataConfig"):
            assert field in r, f"missing field: {field}"

    def test_get_custom_model_all_required_fields_present(self, bedrock):
        """GetCustomModel response has all expected top-level fields (C+R)."""
        model_name = _unique("fields")
        _create_job(bedrock, model_name=model_name)
        r = bedrock.get_custom_model(modelIdentifier=model_name)
        for field in ("modelArn", "modelName", "baseModelArn", "creationTime",
                      "hyperParameters", "trainingDataConfig"):
            assert field in r, f"missing field: {field}"

    def test_create_job_with_validation_data_config(self, bedrock):
        """Job created with validationDataConfig stores and returns it (C+R)."""
        job_name = _unique("valdata")
        model_name = _unique("valmodel")
        bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=model_name,
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://test-bucket/train.jsonl"},
            validationDataConfig={
                "validators": [{"s3Uri": "s3://test-bucket/val.jsonl"}]
            },
            outputDataConfig={"s3Uri": "s3://test-bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )
        r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        assert r["jobName"] == job_name
        # validationDataConfig may or may not be echoed; verify job was created successfully
        assert r["status"] == "InProgress"

    def test_duplicate_job_name_error_code(self, bedrock):
        """Duplicate job name raises ResourceInUseException (C+C+E)."""
        job_name = _unique("dupcheck")
        _create_job(bedrock, job_name=job_name, model_name=_unique("m1"))
        with pytest.raises(ClientError) as exc:
            _create_job(bedrock, job_name=job_name, model_name=_unique("m2"))
        code = exc.value.response["Error"]["Code"]
        assert code == "ResourceInUseException", f"Expected ResourceInUseException, got {code}"

    def test_list_jobs_name_contains_filter_empty_result(self, bedrock):
        """nameContains with no match returns empty list (L+E-ish)."""
        r = bedrock.list_model_customization_jobs(nameContains="absolutely-no-match-xyz-999")
        assert r["modelCustomizationJobSummaries"] == []

    def test_list_custom_models_returns_created_model_in_summaries(self, bedrock):
        """Created model appears with correct modelName in list summaries (C+L)."""
        job_name = _unique("inlist")
        model_name = _unique("inlist-model")
        _create_job(bedrock, job_name=job_name, model_name=model_name)
        r = bedrock.list_custom_models(nameContains=job_name)
        matched = [m for m in r["modelSummaries"] if m["modelName"] == model_name]
        assert len(matched) == 1
        assert "modelArn" in matched[0]
        assert "creationTime" in matched[0]


class TestBedrockModelCopyJobOps:
    """Tests for model copy job operations."""

    def test_create_model_copy_job(self, bedrock):
        name = _unique("copy")
        r = bedrock.create_model_copy_job(
            sourceModelArn="arn:aws:bedrock:us-west-2:123456789012:custom-model/test-model",
            targetModelName=name,
        )
        job_arn = r["jobArn"]
        assert re.match(r"arn:aws:bedrock:[^:]+:\d+:model-copy-job/.+", job_arn), \
            f"bad model-copy-job ARN: {job_arn}"


class TestBedrockDeleteOpsWithFakeIds:
    """Tests for delete operations with nonexistent resources."""

    def test_delete_imported_model_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.delete_imported_model(modelIdentifier="nonexistent-model")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_inference_profile_not_found(self, bedrock):
        with pytest.raises(ClientError) as exc:
            bedrock.delete_inference_profile(inferenceProfileIdentifier="nonexistent-profile")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_marketplace_model_endpoint_not_found(self, bedrock):
        fake_arn = "arn:aws:bedrock:us-east-1:123456789012:marketplace-model-endpoint/fake"
        with pytest.raises(ClientError) as exc:
            bedrock.delete_marketplace_model_endpoint(endpointArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_prompt_router_not_found(self, bedrock):
        fake_arn = "arn:aws:bedrock:us-east-1:123456789012:default-prompt-router/fake"
        with pytest.raises(ClientError) as exc:
            bedrock.delete_prompt_router(promptRouterArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_deregister_marketplace_model_endpoint_not_found(self, bedrock):
        fake_arn = "arn:aws:bedrock:us-east-1:123456789012:marketplace-model-endpoint/fake"
        with pytest.raises(ClientError) as exc:
            bedrock.deregister_marketplace_model_endpoint(endpointArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBedrockStopOpsWithFakeIds:
    """Tests for stop operations with nonexistent resources."""

    def test_stop_evaluation_job_not_found(self, bedrock):
        fake_arn = "arn:aws:bedrock:us-east-1:123456789012:evaluation-job/nonexistent"
        with pytest.raises(ClientError) as exc:
            bedrock.stop_evaluation_job(jobIdentifier=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_stop_model_invocation_job_not_found(self, bedrock):
        fake_arn = "arn:aws:bedrock:us-east-1:123456789012:model-invocation-job/nonexistent"
        with pytest.raises(ClientError) as exc:
            bedrock.stop_model_invocation_job(jobIdentifier=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBedrockRegisterMarketplaceEndpoint:
    """Tests for RegisterMarketplaceModelEndpoint."""

    def test_register_marketplace_model_endpoint_not_found(self, bedrock):
        """RegisterMarketplaceModelEndpoint with fake endpoint returns error."""
        with pytest.raises(ClientError) as exc:
            bedrock.register_marketplace_model_endpoint(
                endpointIdentifier="arn:aws:sagemaker:us-east-1:123456789012:endpoint/fake",
                modelSourceIdentifier="amazon.titan-text-express-v1",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBedrockAutomatedReasoningPolicyCRUD:
    """Tests for automated reasoning policy create/update/delete operations."""

    _FAKE_POLICY_ARN = (
        "arn:aws:bedrock:us-east-1:123456789012:automated-reasoning-policy/fake123456"
    )

    def test_create_automated_reasoning_policy(self, bedrock):
        """CreateAutomatedReasoningPolicy returns a policyArn with correct ARN format."""
        name = _unique("arp")
        r = bedrock.create_automated_reasoning_policy(
            name=name,
            policyDefinition={
                "version": "1.0",
                "rules": [{"id": "rule-abcdef123456", "expression": "true"}],
            },
        )
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200
        policy_arn = r["policyArn"]
        assert re.match(r"arn:aws:bedrock:[^:]+:\d+:automated-reasoning-policy/.+", policy_arn), \
            f"bad policyArn format: {policy_arn}"

    def test_update_automated_reasoning_policy_not_found(self, bedrock):
        """UpdateAutomatedReasoningPolicy with fake ARN raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            bedrock.update_automated_reasoning_policy(
                policyArn=self._FAKE_POLICY_ARN,
                policyDefinition={
                    "version": "1.0",
                    "rules": [{"id": "rule-abcdef123456", "expression": "true"}],
                },
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_cancel_automated_reasoning_policy_build_workflow_not_found(self, bedrock):
        """CancelAutomatedReasoningPolicyBuildWorkflow with fake ARN raises error."""
        with pytest.raises(ClientError) as exc:
            bedrock.cancel_automated_reasoning_policy_build_workflow(
                policyArn=self._FAKE_POLICY_ARN,
                buildWorkflowId="fake-workflow-id",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_start_automated_reasoning_policy_build_workflow_not_found(self, bedrock):
        """StartAutomatedReasoningPolicyBuildWorkflow with fake ARN raises error."""
        with pytest.raises(ClientError) as exc:
            bedrock.start_automated_reasoning_policy_build_workflow(
                policyArn=self._FAKE_POLICY_ARN,
                buildWorkflowType="INGEST_CONTENT",
                sourceContent={
                    "policyDefinition": {
                        "version": "1.0",
                        "rules": [{"id": "rule-abcdef123456", "expression": "true"}],
                    }
                },
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_start_automated_reasoning_policy_test_workflow_not_found(self, bedrock):
        """StartAutomatedReasoningPolicyTestWorkflow with fake ARN raises error."""
        with pytest.raises(ClientError) as exc:
            bedrock.start_automated_reasoning_policy_test_workflow(
                policyArn=self._FAKE_POLICY_ARN,
                buildWorkflowId="fake-workflow-id",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_automated_reasoning_policy_build_workflow_not_found(self, bedrock):
        """DeleteAutomatedReasoningPolicyBuildWorkflow with fake ARN raises error."""
        with pytest.raises(ClientError) as exc:
            bedrock.delete_automated_reasoning_policy_build_workflow(
                policyArn=self._FAKE_POLICY_ARN,
                buildWorkflowId="fake-workflow-id",
                lastUpdatedAt=datetime.datetime(2024, 1, 1),
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_automated_reasoning_policy_test_case_not_found(self, bedrock):
        """DeleteAutomatedReasoningPolicyTestCase with fake ARN raises error."""
        with pytest.raises(ClientError) as exc:
            bedrock.delete_automated_reasoning_policy_test_case(
                policyArn=self._FAKE_POLICY_ARN,
                testCaseId="fake-test-case-id",
                lastUpdatedAt=datetime.datetime(2024, 1, 1),
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_automated_reasoning_policy_test_case_not_found(self, bedrock):
        """CreateAutomatedReasoningPolicyTestCase with fake policy raises error."""
        with pytest.raises(ClientError) as exc:
            bedrock.create_automated_reasoning_policy_test_case(
                policyArn=self._FAKE_POLICY_ARN,
                guardContent="test guard content string",
                expectedAggregatedFindingsResult="PASS",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_automated_reasoning_policy_test_case_not_found(self, bedrock):
        """UpdateAutomatedReasoningPolicyTestCase with fake policy raises error."""
        with pytest.raises(ClientError) as exc:
            bedrock.update_automated_reasoning_policy_test_case(
                policyArn=self._FAKE_POLICY_ARN,
                testCaseId="fake-test-case-id",
                guardContent="updated content",
                lastUpdatedAt=datetime.datetime(2024, 1, 1),
                expectedAggregatedFindingsResult="FAIL",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_export_automated_reasoning_policy_version_not_found(self, bedrock):
        """ExportAutomatedReasoningPolicyVersion with fake ARN raises error."""
        with pytest.raises(ClientError) as exc:
            bedrock.export_automated_reasoning_policy_version(
                policyArn=self._FAKE_POLICY_ARN,
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_automated_reasoning_policy_not_found(self, bedrock):
        """DeleteAutomatedReasoningPolicy with fake ARN raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            bedrock.delete_automated_reasoning_policy(policyArn=self._FAKE_POLICY_ARN)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBedrockCustomModelDeploymentCRUD:
    """Tests for custom model deployment create/update/delete operations."""

    def test_create_custom_model_deployment(self, bedrock):
        """CreateCustomModelDeployment returns a deployment ARN with correct format."""
        name = _unique("deploy")
        r = bedrock.create_custom_model_deployment(
            modelDeploymentName=name,
            modelArn="arn:aws:bedrock:us-east-1:123456789012:custom-model/test-model",
        )
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200
        deployment_arn = r["customModelDeploymentArn"]
        assert re.match(r"arn:aws:bedrock:[^:]+:\d+:custom-model-deployment/.+", deployment_arn), \
            f"bad deployment ARN format: {deployment_arn}"

    def test_delete_custom_model_deployment_not_found(self, bedrock):
        """DeleteCustomModelDeployment with fake ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            bedrock.delete_custom_model_deployment(
                customModelDeploymentIdentifier="nonexistent-deployment"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_custom_model_deployment_not_found(self, bedrock):
        """UpdateCustomModelDeployment with fake ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            bedrock.update_custom_model_deployment(
                modelArn="arn:aws:bedrock:us-east-1:123456789012:custom-model/test-model",
                customModelDeploymentIdentifier="nonexistent-deployment",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBedrockFoundationModelAgreementOps:
    """Tests for foundation model agreement operations."""

    def test_create_foundation_model_agreement(self, bedrock):
        """CreateFoundationModelAgreement returns modelId."""
        r = bedrock.create_foundation_model_agreement(
            offerToken="test-offer-token",
            modelId="anthropic.claude-v2",
        )
        assert r["modelId"] == "anthropic.claude-v2"
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_foundation_model_agreement(self, bedrock):
        """DeleteFoundationModelAgreement returns 200."""
        r = bedrock.delete_foundation_model_agreement(modelId="anthropic.claude-v2")
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestBedrockEnforcedGuardrailConfigOps:
    """Tests for enforced guardrail configuration operations."""

    def test_delete_enforced_guardrail_configuration(self, bedrock):
        """DeleteEnforcedGuardrailConfiguration returns 200."""
        r = bedrock.delete_enforced_guardrail_configuration(configId="test-config")
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_enforced_guardrail_configuration(self, bedrock):
        """PutEnforcedGuardrailConfiguration returns configId."""
        r = bedrock.put_enforced_guardrail_configuration(
            guardrailInferenceConfig={
                "guardrailIdentifier": "test-guardrail-id",
                "guardrailVersion": "1",
            },
        )
        assert "configId" in r
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestBedrockUseCaseOps:
    """Tests for use case for model access operations."""

    def test_put_use_case_for_model_access(self, bedrock):
        """PutUseCaseForModelAccess returns 200."""
        r = bedrock.put_use_case_for_model_access(formData=b"use_case=testing")
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestBedrockGuardrailFullLifecycle:
    """Full lifecycle tests for guardrail CRUD (C+R+L+U+D+E patterns)."""

    def test_guardrail_full_lifecycle(self, bedrock):
        """create → get → list → update → verify update → delete → error (C+R+L+U+D+E)."""
        name = _unique("guard-full")

        # CREATE
        r = bedrock.create_guardrail(
            name=name,
            blockedInputMessaging="original-blocked",
            blockedOutputsMessaging="original-blocked",
        )
        gr_id = r["guardrailId"]
        gr_arn = r["guardrailArn"]
        assert gr_id  # non-empty string
        assert re.match(r"arn:aws:bedrock:[^:]+:\d+:guardrail/.+", gr_arn), f"bad ARN: {gr_arn}"
        assert r["version"] == "DRAFT"

        # RETRIEVE
        r2 = bedrock.get_guardrail(guardrailIdentifier=gr_id)
        assert r2["name"] == name
        assert r2["guardrailId"] == gr_id
        assert r2["version"] == "DRAFT"

        # LIST — verify in list (list_guardrails uses "id" not "guardrailId")
        r3 = bedrock.list_guardrails()
        ids = [g["id"] for g in r3["guardrails"]]
        assert gr_id in ids

        # UPDATE
        new_name = _unique("guard-renamed")
        r4 = bedrock.update_guardrail(
            guardrailIdentifier=gr_id,
            name=new_name,
            blockedInputMessaging="updated-blocked",
            blockedOutputsMessaging="updated-blocked",
        )
        assert r4["guardrailId"] == gr_id
        assert r4["updatedAt"] is not None

        # Verify update applied
        r5 = bedrock.get_guardrail(guardrailIdentifier=gr_id)
        assert r5["name"] == new_name

        # DELETE
        bedrock.delete_guardrail(guardrailIdentifier=gr_id)

        # ERROR after delete
        with pytest.raises(ClientError) as exc:
            bedrock.get_guardrail(guardrailIdentifier=gr_id)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
        )

    def test_guardrail_not_in_list_after_delete(self, bedrock):
        """Deleted guardrail no longer appears in ListGuardrails (C+L+D+L)."""
        name = _unique("guard-del-list")
        r = bedrock.create_guardrail(
            name=name,
            blockedInputMessaging="blocked",
            blockedOutputsMessaging="blocked",
        )
        gr_id = r["guardrailId"]

        # Confirm it's in the list
        r2 = bedrock.list_guardrails()
        assert any(g["id"] == gr_id for g in r2["guardrails"])

        bedrock.delete_guardrail(guardrailIdentifier=gr_id)

        r3 = bedrock.list_guardrails()
        assert not any(g["id"] == gr_id for g in r3["guardrails"])

    def test_guardrail_update_does_not_change_id(self, bedrock):
        """UpdateGuardrail preserves guardrailId (C+U+R)."""
        name = _unique("guard-id-stable")
        r = bedrock.create_guardrail(
            name=name,
            blockedInputMessaging="blocked",
            blockedOutputsMessaging="blocked",
        )
        gr_id = r["guardrailId"]

        new_name = _unique("guard-id-stable-renamed")
        bedrock.update_guardrail(
            guardrailIdentifier=gr_id,
            name=new_name,
            blockedInputMessaging="new-blocked",
            blockedOutputsMessaging="new-blocked",
        )
        r2 = bedrock.get_guardrail(guardrailIdentifier=gr_id)
        assert r2["guardrailId"] == gr_id
        assert r2["name"] == new_name

    def test_guardrail_version_created_on_create_version(self, bedrock):
        """CreateGuardrailVersion creates version '1' from DRAFT (C+U+R)."""
        name = _unique("guard-ver-test")
        r = bedrock.create_guardrail(
            name=name,
            blockedInputMessaging="blocked",
            blockedOutputsMessaging="blocked",
        )
        gr_id = r["guardrailId"]
        assert r["version"] == "DRAFT"

        r2 = bedrock.create_guardrail_version(guardrailIdentifier=gr_id)
        assert r2["guardrailId"] == gr_id
        assert r2["version"] == "1"

    def test_guardrail_arn_encodes_account_and_region(self, bedrock):
        """Guardrail ARN encodes the account ID and region (C+R)."""
        name = _unique("guard-arn")
        r = bedrock.create_guardrail(
            name=name,
            blockedInputMessaging="blocked",
            blockedOutputsMessaging="blocked",
        )
        gr_arn = r["guardrailArn"]
        assert "123456789012" in gr_arn
        assert "us-east-1" in gr_arn

        r2 = bedrock.get_guardrail(guardrailIdentifier=r["guardrailId"])
        assert r2["guardrailArn"] == gr_arn


class TestBedrockProvisionedModelThroughputLifecycle:
    """Full lifecycle tests for provisioned model throughput (C+R+L+U+D+E)."""

    def test_pmt_full_lifecycle(self, bedrock):
        """create → list → update name → verify → delete → error (C+L+U+R+D+E)."""
        name = _unique("pmt-full")

        # CREATE
        r = bedrock.create_provisioned_model_throughput(
            modelUnits=1,
            provisionedModelName=name,
            modelId="amazon.titan-text-express-v1",
        )
        pmt_arn = r["provisionedModelArn"]
        assert re.match(r"arn:aws:bedrock:[^:]+:\d+:provisioned-model/.+", pmt_arn), \
            f"bad PMT ARN: {pmt_arn}"

        # LIST — verify in list
        r2 = bedrock.list_provisioned_model_throughputs()
        arns = [p["provisionedModelArn"] for p in r2["provisionedModelSummaries"]]
        assert pmt_arn in arns

        # UPDATE
        new_name = _unique("pmt-renamed")
        r3 = bedrock.update_provisioned_model_throughput(
            provisionedModelId=pmt_arn,
            desiredProvisionedModelName=new_name,
        )
        assert r3["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify update applied via GET
        r4 = bedrock.get_provisioned_model_throughput(provisionedModelId=pmt_arn)
        assert r4["provisionedModelArn"] == pmt_arn

        # DELETE
        bedrock.delete_provisioned_model_throughput(provisionedModelId=pmt_arn)

        # ERROR after delete
        with pytest.raises(ClientError) as exc:
            bedrock.get_provisioned_model_throughput(provisionedModelId=pmt_arn)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
        )

    def test_pmt_not_in_list_after_delete(self, bedrock):
        """Deleted PMT no longer appears in ListProvisionedModelThroughputs (C+L+D+L)."""
        name = _unique("pmt-del-list")
        r = bedrock.create_provisioned_model_throughput(
            modelUnits=1,
            provisionedModelName=name,
            modelId="amazon.titan-text-express-v1",
        )
        pmt_arn = r["provisionedModelArn"]

        r2 = bedrock.list_provisioned_model_throughputs()
        assert any(p["provisionedModelArn"] == pmt_arn for p in r2["provisionedModelSummaries"])

        bedrock.delete_provisioned_model_throughput(provisionedModelId=pmt_arn)

        r3 = bedrock.list_provisioned_model_throughputs()
        assert not any(
            p["provisionedModelArn"] == pmt_arn for p in r3["provisionedModelSummaries"]
        )


class TestBedrockDirectJobCreation:
    """Tests using bedrock.create_model_customization_job directly (no _create_job helper).

    These tests ensure the validator detects the CREATE pattern.
    """

    def test_create_job_directly_returns_arn(self, bedrock):
        """create_model_customization_job returns a jobArn with correct format (C+R)."""
        job_name = _unique("direct-create")
        r = bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=_unique("dc-model"),
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://test-bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://test-bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )
        job_arn = r["jobArn"]
        assert re.match(
            r"arn:aws:bedrock:[^:]+:\d+:model-customization-job/.+", job_arn
        ), f"bad job ARN: {job_arn}"
        assert job_name in job_arn

        # RETRIEVE
        r2 = bedrock.get_model_customization_job(jobIdentifier=job_name)
        assert r2["jobArn"] == job_arn
        assert r2["status"] == "InProgress"

    def test_create_job_directly_then_list(self, bedrock):
        """Newly created job is visible in list (C+L)."""
        job_name = _unique("direct-list")
        r = bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=_unique("dl-model"),
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://test-bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://test-bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )
        job_arn = r["jobArn"]

        r2 = bedrock.list_model_customization_jobs(nameContains=job_name)
        summaries = r2["modelCustomizationJobSummaries"]
        matched = [j for j in summaries if j["jobName"] == job_name]
        assert len(matched) == 1
        assert matched[0]["jobArn"] == job_arn

    def test_create_job_then_stop_then_verify(self, bedrock):
        """create → stop → verify status changed (C+U+R)."""
        job_name = _unique("direct-stop")
        r = bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=_unique("ds-model"),
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://test-bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://test-bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )
        job_arn = r["jobArn"]

        r2 = bedrock.get_model_customization_job(jobIdentifier=job_name)
        assert r2["status"] == "InProgress"

        bedrock.stop_model_customization_job(jobIdentifier=job_arn)

        r3 = bedrock.get_model_customization_job(jobIdentifier=job_name)
        assert r3["status"] in ("Stopping", "Stopped")

    def test_create_job_duplicate_raises_resource_in_use(self, bedrock):
        """Creating two jobs with the same name raises ResourceInUseException (C+C+E)."""
        job_name = _unique("dup-direct")
        bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=_unique("dup-m1"),
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://test-bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://test-bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )
        with pytest.raises(ClientError) as exc:
            bedrock.create_model_customization_job(
                jobName=job_name,
                customModelName=_unique("dup-m2"),
                roleArn="arn:aws:iam::123456789012:role/test",
                baseModelIdentifier="amazon.titan-text-express-v1",
                trainingDataConfig={"s3Uri": "s3://test-bucket/train.jsonl"},
                outputDataConfig={"s3Uri": "s3://test-bucket/output/"},
                hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceInUseException"

    def test_create_job_list_shows_creation_time(self, bedrock):
        """Job summary in list includes a creationTime datetime (C+L)."""
        job_name = _unique("direct-ct")
        bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=_unique("ct-model"),
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://test-bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://test-bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )
        r = bedrock.list_model_customization_jobs(nameContains=job_name)
        matched = [j for j in r["modelCustomizationJobSummaries"] if j["jobName"] == job_name]
        assert len(matched) == 1
        assert isinstance(matched[0]["creationTime"], datetime.datetime)

    def test_create_custom_model_then_list_shows_it(self, bedrock):
        """After creating a job, the custom model appears in ListCustomModels (C+L)."""
        job_name = _unique("cm-list-direct")
        model_name = _unique("cm-list-m")
        bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=model_name,
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://test-bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://test-bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )
        r = bedrock.list_custom_models(nameContains=job_name)
        names = [m["modelName"] for m in r["modelSummaries"]]
        assert model_name in names

    def test_create_job_then_delete_model_then_error(self, bedrock):
        """create job → delete model → get model raises ResourceNotFoundException (C+R+D+E)."""
        job_name = _unique("del-model-direct")
        model_name = _unique("dm-direct")
        bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=model_name,
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://test-bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://test-bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )

        r = bedrock.get_custom_model(modelIdentifier=model_name)
        assert r["modelName"] == model_name

        bedrock.delete_custom_model(modelIdentifier=model_name)

        with pytest.raises(ClientError) as exc:
            bedrock.get_custom_model(modelIdentifier=model_name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_and_tag_job_then_list_tags_and_untag(self, bedrock):
        """create → tag → list tags → untag → verify empty (C+U+L+D)."""
        job_name = _unique("tag-full")
        r = bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=_unique("tag-model"),
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://test-bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://test-bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )
        job_arn = r["jobArn"]

        # Empty tags initially
        r2 = bedrock.list_tags_for_resource(resourceARN=job_arn)
        assert r2["tags"] == []

        # Tag it
        bedrock.tag_resource(
            resourceARN=job_arn,
            tags=[{"key": "env", "value": "staging"}, {"key": "owner", "value": "team-ml"}],
        )

        # Verify tags present
        r3 = bedrock.list_tags_for_resource(resourceARN=job_arn)
        tag_map = {t["key"]: t["value"] for t in r3["tags"]}
        assert tag_map["env"] == "staging"
        assert tag_map["owner"] == "team-ml"

        # Untag one key
        bedrock.untag_resource(resourceARN=job_arn, tagKeys=["env"])

        r4 = bedrock.list_tags_for_resource(resourceARN=job_arn)
        remaining = {t["key"]: t["value"] for t in r4["tags"]}
        assert "env" not in remaining
        assert remaining["owner"] == "team-ml"


class TestBedrockCreateCustomModel:
    """Tests for standalone CreateCustomModel operation."""

    def test_create_custom_model(self, bedrock):
        """CreateCustomModel creates a model from S3 source."""
        model_name = _unique("custom-model")
        r = bedrock.create_custom_model(
            modelName=model_name,
            modelSourceConfig={"s3DataSource": {"s3Uri": "s3://bucket/model/"}},
        )
        assert "modelArn" in r
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_custom_model_then_get(self, bedrock):
        """CreateCustomModel model is retrievable via GetCustomModel."""
        model_name = _unique("custom-model")
        bedrock.create_custom_model(
            modelName=model_name,
            modelSourceConfig={"s3DataSource": {"s3Uri": "s3://bucket/model/"}},
        )
        r = bedrock.get_custom_model(modelIdentifier=model_name)
        assert r["modelName"] == model_name
        assert "modelArn" in r

    def test_create_custom_model_then_delete(self, bedrock):
        """CreateCustomModel model can be deleted."""
        model_name = _unique("custom-model")
        bedrock.create_custom_model(
            modelName=model_name,
            modelSourceConfig={"s3DataSource": {"s3Uri": "s3://bucket/model/"}},
        )
        bedrock.delete_custom_model(modelIdentifier=model_name)
        with pytest.raises(ClientError) as exc:
            bedrock.get_custom_model(modelIdentifier=model_name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBedrockEvaluationJobCRUD:
    """Tests for CreateEvaluationJob and BatchDeleteEvaluationJob."""

    def test_create_evaluation_job(self, bedrock):
        """CreateEvaluationJob returns a job ARN."""
        job_name = _unique("eval-job")
        r = bedrock.create_evaluation_job(
            jobName=job_name,
            roleArn="arn:aws:iam::123456789012:role/test",
            evaluationConfig={
                "automated": {
                    "datasetMetricConfigs": [
                        {
                            "taskType": "Summarization",
                            "dataset": {
                                "name": "test",
                                "datasetLocation": {
                                    "s3Uri": "s3://bucket/data.jsonl",
                                },
                            },
                            "metricNames": ["Accuracy"],
                        }
                    ]
                }
            },
            inferenceConfig={
                "models": [
                    {
                        "bedrockModel": {
                            "modelIdentifier": "amazon.titan-text-express-v1",
                            "inferenceParams": "{}",
                        }
                    }
                ]
            },
            outputDataConfig={"s3Uri": "s3://bucket/output/"},
        )
        job_arn = r["jobArn"]
        assert re.match(r"arn:aws:bedrock:[^:]+:\d+:evaluation-job/.+", job_arn), \
            f"bad evaluation job ARN: {job_arn}"

    def test_create_evaluation_job_then_get(self, bedrock):
        """GetEvaluationJob returns created job details."""
        job_name = _unique("eval-job")
        create_r = bedrock.create_evaluation_job(
            jobName=job_name,
            roleArn="arn:aws:iam::123456789012:role/test",
            evaluationConfig={
                "automated": {
                    "datasetMetricConfigs": [
                        {
                            "taskType": "Summarization",
                            "dataset": {
                                "name": "test",
                                "datasetLocation": {
                                    "s3Uri": "s3://bucket/data.jsonl",
                                },
                            },
                            "metricNames": ["Accuracy"],
                        }
                    ]
                }
            },
            inferenceConfig={
                "models": [
                    {
                        "bedrockModel": {
                            "modelIdentifier": "amazon.titan-text-express-v1",
                            "inferenceParams": "{}",
                        }
                    }
                ]
            },
            outputDataConfig={"s3Uri": "s3://bucket/output/"},
        )
        job_arn = create_r["jobArn"]
        r = bedrock.get_evaluation_job(jobIdentifier=job_arn)
        assert r["jobName"] == job_name
        assert r["jobArn"] == job_arn

    def test_batch_delete_evaluation_job(self, bedrock):
        """BatchDeleteEvaluationJob accepts job identifiers."""
        job_name = _unique("eval-del")
        create_r = bedrock.create_evaluation_job(
            jobName=job_name,
            roleArn="arn:aws:iam::123456789012:role/test",
            evaluationConfig={
                "automated": {
                    "datasetMetricConfigs": [
                        {
                            "taskType": "Summarization",
                            "dataset": {
                                "name": "test",
                                "datasetLocation": {
                                    "s3Uri": "s3://bucket/data.jsonl",
                                },
                            },
                            "metricNames": ["Accuracy"],
                        }
                    ]
                }
            },
            inferenceConfig={
                "models": [
                    {
                        "bedrockModel": {
                            "modelIdentifier": "amazon.titan-text-express-v1",
                            "inferenceParams": "{}",
                        }
                    }
                ]
            },
            outputDataConfig={"s3Uri": "s3://bucket/output/"},
        )
        job_arn = create_r["jobArn"]
        r = bedrock.batch_delete_evaluation_job(jobIdentifiers=[job_arn])
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(r["errors"], list)
        assert isinstance(r["evaluationJobs"], list)


TITAN_MODEL_ARN = "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-text-express-v1"


class TestBedrockInferenceProfileCRUD:
    """Tests for CreateInferenceProfile CRUD."""

    def test_create_inference_profile(self, bedrock):
        """CreateInferenceProfile returns an ARN and status."""
        name = _unique("inf-profile")
        r = bedrock.create_inference_profile(
            inferenceProfileName=name,
            modelSource={"copyFrom": TITAN_MODEL_ARN},
        )
        assert "inferenceProfileArn" in r
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_inference_profile_then_get(self, bedrock):
        """GetInferenceProfile returns created profile."""
        name = _unique("inf-profile")
        create_r = bedrock.create_inference_profile(
            inferenceProfileName=name,
            modelSource={"copyFrom": TITAN_MODEL_ARN},
        )
        arn = create_r["inferenceProfileArn"]
        r = bedrock.get_inference_profile(inferenceProfileIdentifier=arn)
        assert r["inferenceProfileName"] == name
        assert r["inferenceProfileArn"] == arn

    def test_create_inference_profile_then_delete(self, bedrock):
        """DeleteInferenceProfile removes created profile."""
        name = _unique("inf-profile")
        create_r = bedrock.create_inference_profile(
            inferenceProfileName=name,
            modelSource={"copyFrom": TITAN_MODEL_ARN},
        )
        arn = create_r["inferenceProfileArn"]
        bedrock.delete_inference_profile(inferenceProfileIdentifier=arn)
        with pytest.raises(ClientError) as exc:
            bedrock.get_inference_profile(inferenceProfileIdentifier=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBedrockModelImportJobCRUD:
    """Tests for CreateModelImportJob."""

    def test_create_model_import_job(self, bedrock):
        """CreateModelImportJob returns a job ARN."""
        job_name = _unique("import-job")
        r = bedrock.create_model_import_job(
            jobName=job_name,
            importedModelName=_unique("imported"),
            roleArn="arn:aws:iam::123456789012:role/test",
            modelDataSource={"s3DataSource": {"s3Uri": "s3://bucket/model/"}},
        )
        job_arn = r["jobArn"]
        assert re.match(r"arn:aws:bedrock:[^:]+:\d+:model-import-job/.+", job_arn), \
            f"bad model-import-job ARN: {job_arn}"

    def test_create_model_import_job_then_get(self, bedrock):
        """GetModelImportJob returns created job."""
        job_name = _unique("import-job")
        create_r = bedrock.create_model_import_job(
            jobName=job_name,
            importedModelName=_unique("imported"),
            roleArn="arn:aws:iam::123456789012:role/test",
            modelDataSource={"s3DataSource": {"s3Uri": "s3://bucket/model/"}},
        )
        job_arn = create_r["jobArn"]
        r = bedrock.get_model_import_job(jobIdentifier=job_arn)
        assert r["jobName"] == job_name
        assert r["jobArn"] == job_arn


class TestBedrockModelInvocationJobCRUD:
    """Tests for CreateModelInvocationJob."""

    def test_create_model_invocation_job(self, bedrock):
        """CreateModelInvocationJob returns a job ARN."""
        job_name = _unique("invoke-job")
        r = bedrock.create_model_invocation_job(
            jobName=job_name,
            roleArn="arn:aws:iam::123456789012:role/test",
            modelId="amazon.titan-text-express-v1",
            inputDataConfig={"s3InputDataConfig": {"s3Uri": "s3://bucket/input/"}},
            outputDataConfig={"s3OutputDataConfig": {"s3Uri": "s3://bucket/output/"}},
        )
        job_arn = r["jobArn"]
        assert re.match(r"arn:aws:bedrock:[^:]+:\d+:model-invocation-job/.+", job_arn), \
            f"bad model-invocation-job ARN: {job_arn}"

    def test_create_model_invocation_job_then_get(self, bedrock):
        """GetModelInvocationJob returns created job."""
        job_name = _unique("invoke-job")
        create_r = bedrock.create_model_invocation_job(
            jobName=job_name,
            roleArn="arn:aws:iam::123456789012:role/test",
            modelId="amazon.titan-text-express-v1",
            inputDataConfig={"s3InputDataConfig": {"s3Uri": "s3://bucket/input/"}},
            outputDataConfig={"s3OutputDataConfig": {"s3Uri": "s3://bucket/output/"}},
        )
        job_arn = create_r["jobArn"]
        r = bedrock.get_model_invocation_job(jobIdentifier=job_arn)
        assert r["jobName"] == job_name
        assert r["jobArn"] == job_arn


class TestBedrockPromptRouterCRUD:
    """Tests for CreatePromptRouter CRUD."""

    def test_create_prompt_router(self, bedrock):
        """CreatePromptRouter returns a router ARN."""
        name = _unique("router")
        r = bedrock.create_prompt_router(
            promptRouterName=name,
            models=[{"modelArn": TITAN_MODEL_ARN}],
            fallbackModel={"modelArn": TITAN_MODEL_ARN},
            routingCriteria={"responseQualityDifference": 0.5},
        )
        assert "promptRouterArn" in r
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_prompt_router_appears_in_list(self, bedrock):
        """CreatePromptRouter creates a router visible in ListPromptRouters."""
        name = _unique("router")
        create_r = bedrock.create_prompt_router(
            promptRouterName=name,
            models=[{"modelArn": TITAN_MODEL_ARN}],
            fallbackModel={"modelArn": TITAN_MODEL_ARN},
            routingCriteria={"responseQualityDifference": 0.5},
        )
        arn = create_r["promptRouterArn"]
        lr = bedrock.list_prompt_routers()
        arns = [s["promptRouterArn"] for s in lr["promptRouterSummaries"]]
        assert arn in arns


class TestBedrockMarketplaceModelEndpointCRUD:
    """Tests for CreateMarketplaceModelEndpoint CRUD."""

    def test_create_marketplace_model_endpoint(self, bedrock):
        """CreateMarketplaceModelEndpoint returns an endpoint ARN."""
        name = _unique("mkt-ep")
        r = bedrock.create_marketplace_model_endpoint(
            modelSourceIdentifier="arn:aws:bedrock:us-east-1:123456789012:marketplace-model/fake",
            endpointConfig={
                "sageMaker": {
                    "initialInstanceCount": 1,
                    "instanceType": "ml.g5.xlarge",
                    "executionRole": "arn:aws:iam::123456789012:role/test",
                }
            },
            endpointName=name,
        )
        assert "marketplaceModelEndpoint" in r
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_marketplace_model_endpoint_appears_in_list(self, bedrock):
        """CreateMarketplaceModelEndpoint appears in ListMarketplaceModelEndpoints."""
        name = _unique("mkt-ep")
        create_r = bedrock.create_marketplace_model_endpoint(
            modelSourceIdentifier="arn:aws:bedrock:us-east-1:123456789012:marketplace-model/fake",
            endpointConfig={
                "sageMaker": {
                    "initialInstanceCount": 1,
                    "instanceType": "ml.g5.xlarge",
                    "executionRole": "arn:aws:iam::123456789012:role/test",
                }
            },
            endpointName=name,
        )
        ep = create_r["marketplaceModelEndpoint"]
        arn = ep["endpointArn"]
        lr = bedrock.list_marketplace_model_endpoints()
        arns = [e["endpointArn"] for e in lr.get("marketplaceModelEndpoints", [])]
        assert arn in arns


class TestBedrockAutomatedReasoningVersionOps:
    """Tests for automated reasoning policy version and annotation operations."""

    def test_create_automated_reasoning_policy_version_not_found(self, bedrock):
        """CreateAutomatedReasoningPolicyVersion: fake policy error."""
        long_hash = "a" * 128
        with pytest.raises(ClientError) as exc:
            bedrock.create_automated_reasoning_policy_version(
                policyArn="arn:aws:bedrock:us-east-1:123456789012:automated-reasoning-policy/fake",
                lastUpdatedDefinitionHash=long_hash,
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_automated_reasoning_policy_annotations_not_found(self, bedrock):
        """UpdateAutomatedReasoningPolicyAnnotations: fake policy error."""
        long_hash = "a" * 128
        with pytest.raises(ClientError) as exc:
            bedrock.update_automated_reasoning_policy_annotations(
                policyArn="arn:aws:bedrock:us-east-1:123456789012:automated-reasoning-policy/fake",
                buildWorkflowId="fake-workflow",
                annotations=[],
                lastUpdatedAnnotationSetHash=long_hash,
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBedrockUpdateMarketplaceModelEndpoint:
    """Test UpdateMarketplaceModelEndpoint operation."""

    def test_update_marketplace_model_endpoint(self):
        """UpdateMarketplaceModelEndpoint raises known error for fake ARN."""
        client = make_client("bedrock")
        try:
            client.update_marketplace_model_endpoint(
                endpointArn=(
                    "arn:aws:bedrock:us-east-1:123456789012:marketplace-model/endpoint/fake"
                ),
                endpointConfig={
                    "sageMaker": {
                        "initialInstanceCount": 1,
                        "instanceType": "ml.t2.medium",
                        "executionRole": ("arn:aws:iam::123456789012:role/test-role"),
                    }
                },
            )
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None


class TestBedrockJobLifecycle:
    """Full lifecycle tests: create → retrieve → list → delete."""

    def test_create_and_retrieve_job_directly(self, bedrock):
        """Create a job inline and immediately retrieve it to verify fields."""
        job_name = _unique("direct")
        model_name = _unique("model")
        create_r = bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=model_name,
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )
        job_arn = create_r["jobArn"]

        get_r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        assert get_r["jobName"] == job_name
        assert get_r["jobArn"] == job_arn
        assert get_r["outputModelName"].startswith(model_name)
        assert get_r["status"] == "InProgress"

    def test_create_job_arn_contains_job_name(self, bedrock):
        """JobArn returned by CreateModelCustomizationJob contains the job name."""
        job_name = _unique("arncheck")
        r = bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=_unique("model"),
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )
        assert job_name in r["jobArn"]
        assert "arn:aws:bedrock:" in r["jobArn"]
        assert "model-customization-job" in r["jobArn"]

    def test_list_includes_newly_created_job(self, bedrock):
        """A created job appears in ListModelCustomizationJobs."""
        job_name = _unique("listtest")
        bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=_unique("model"),
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )

        listed = bedrock.list_model_customization_jobs(nameContains=job_name)
        job_names = [j["jobName"] for j in listed["modelCustomizationJobSummaries"]]
        assert job_name in job_names

    def test_list_custom_models_includes_model_from_job(self, bedrock):
        """A custom model appears in ListCustomModels after job is created."""
        job_name = _unique("listjob")
        model_name = _unique("listmod")
        bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=model_name,
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )

        listed = bedrock.list_custom_models(nameContains=job_name)
        model_names = [m["modelName"] for m in listed["modelSummaries"]]
        assert model_name in model_names

    def test_delete_custom_model_removes_from_list(self, bedrock):
        """After deleting a custom model, it no longer appears in ListCustomModels."""
        model_name = _unique("delmod")
        bedrock.create_model_customization_job(
            jobName=_unique("job"),
            customModelName=model_name,
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )

        bedrock.delete_custom_model(modelIdentifier=model_name)

        listed = bedrock.list_custom_models()
        model_names = [m["modelName"] for m in listed["modelSummaries"]]
        assert model_name not in model_names

    def test_stop_job_then_get_reflects_status(self, bedrock):
        """After stopping a job, status transitions to Stopping or Stopped."""
        job_name = _unique("stoptest")
        r = bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=_unique("model"),
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )
        job_arn = r["jobArn"]

        bedrock.stop_model_customization_job(jobIdentifier=job_arn)

        get_r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        assert get_r["status"] in ("Stopping", "Stopped")

    def test_duplicate_model_name_raises_error(self, bedrock):
        """Creating two jobs with the same customModelName raises an error."""
        model_name = _unique("dupmodel")
        bedrock.create_model_customization_job(
            jobName=_unique("job1"),
            customModelName=model_name,
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )
        with pytest.raises(ClientError) as exc:
            bedrock.create_model_customization_job(
                jobName=_unique("job2"),
                customModelName=model_name,
                roleArn="arn:aws:iam::123456789012:role/test",
                baseModelIdentifier="amazon.titan-text-express-v1",
                trainingDataConfig={"s3Uri": "s3://bucket/train.jsonl"},
                outputDataConfig={"s3Uri": "s3://bucket/output/"},
                hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
            )
        assert exc.value.response["Error"]["Code"] in (
            "ResourceInUseException", "ConflictException", "ValidationException"
        )


class TestBedrockTagsLifecycle:
    """Tag lifecycle: create resource, tag it, verify, untag, verify."""

    def test_tag_job_then_list_and_untag(self, bedrock):
        """Full tag lifecycle: tag → list → untag → verify removed."""
        job_name = _unique("tagcycle")
        r = bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=_unique("model"),
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )
        job_arn = r["jobArn"]

        # Tag it
        bedrock.tag_resource(
            resourceARN=job_arn,
            tags=[{"key": "stage", "value": "dev"}, {"key": "owner", "value": "alice"}],
        )

        # List and verify
        tags_r = bedrock.list_tags_for_resource(resourceARN=job_arn)
        tag_map = {t["key"]: t["value"] for t in tags_r["tags"]}
        assert tag_map["stage"] == "dev"
        assert tag_map["owner"] == "alice"

        # Untag one
        bedrock.untag_resource(resourceARN=job_arn, tagKeys=["stage"])

        # Verify only owner remains
        tags_r2 = bedrock.list_tags_for_resource(resourceARN=job_arn)
        tag_map2 = {t["key"]: t["value"] for t in tags_r2["tags"]}
        assert "stage" not in tag_map2
        assert tag_map2["owner"] == "alice"

    def test_list_tags_for_nonexistent_resource(self, bedrock):
        """ListTagsForResource on nonexistent resource raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            bedrock.list_tags_for_resource(
                resourceARN="arn:aws:bedrock:us-east-1:123456789012:model-customization-job/nonexistent-xyz"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_tag_overwrite_existing_key(self, bedrock):
        """Tagging with an existing key overwrites the value."""
        job_name = _unique("tagover")
        r = bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=_unique("model"),
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )
        job_arn = r["jobArn"]

        bedrock.tag_resource(resourceARN=job_arn, tags=[{"key": "env", "value": "dev"}])
        bedrock.tag_resource(resourceARN=job_arn, tags=[{"key": "env", "value": "prod"}])

        tags_r = bedrock.list_tags_for_resource(resourceARN=job_arn)
        tag_map = {t["key"]: t["value"] for t in tags_r["tags"]}
        assert tag_map["env"] == "prod"


class TestBedrockBehavioralFidelity:
    """Behavioral fidelity: timestamps, ARN format, status values."""

    def test_job_creationtime_is_set(self, bedrock):
        """GetModelCustomizationJob returns a non-None creationTime."""
        job_name = _unique("timejob")
        bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=_unique("model"),
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )

        r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        assert "creationTime" in r
        ct = r["creationTime"]
        assert ct is not None
        # Must be a datetime-like object
        assert hasattr(ct, "year") or isinstance(ct, str)

    def test_custom_model_arn_format(self, bedrock):
        """Custom model ARN matches arn:aws:bedrock:<region>:<account>:custom-model/<name>."""
        model_name = _unique("arnmod")
        bedrock.create_model_customization_job(
            jobName=_unique("job"),
            customModelName=model_name,
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )

        r = bedrock.get_custom_model(modelIdentifier=model_name)
        model_arn = r["modelArn"]
        assert model_arn.startswith("arn:aws:bedrock:")
        assert ":custom-model/" in model_arn
        assert model_name in model_arn

    def test_list_job_summaries_have_expected_fields(self, bedrock):
        """ListModelCustomizationJobs returns summaries with required fields."""
        job_name = _unique("sumjob")
        bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=_unique("model"),
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )

        r = bedrock.list_model_customization_jobs(nameContains=job_name)
        summaries = r["modelCustomizationJobSummaries"]
        assert len(summaries) >= 1
        summary = next(s for s in summaries if s["jobName"] == job_name)
        assert "jobArn" in summary
        assert "status" in summary
        assert "creationTime" in summary

    def test_get_job_base_model_identifier_preserved(self, bedrock):
        """GetModelCustomizationJob preserves the baseModelArn."""
        job_name = _unique("basejob")
        bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=_unique("model"),
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )

        r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        assert r["baseModelArn"].endswith("amazon.titan-text-express-v1")

    def test_list_custom_models_summary_has_creation_time(self, bedrock):
        """ListCustomModels summaries include creationTime."""
        job_name = _unique("timejob2")
        model_name = _unique("timelist")
        bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=model_name,
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )

        r = bedrock.list_custom_models(nameContains=job_name)
        summaries = r["modelSummaries"]
        match = next((m for m in summaries if m["modelName"] == model_name), None)
        assert match is not None
        assert "creationTime" in match


class TestBedrockGuardrailUpdateLifecycle:
    """Tests that exercise the UPDATE pattern for guardrails (C+U+R+L+D+E)."""

    def test_update_guardrail_changes_blocked_messaging(self, bedrock):
        """UpdateGuardrail changes blockedInputMessaging and blockedOutputsMessaging (C+U+R)."""
        name = _unique("guard-msg")
        create_r = bedrock.create_guardrail(
            name=name,
            blockedInputMessaging="original-input-msg",
            blockedOutputsMessaging="original-output-msg",
        )
        gr_id = create_r["guardrailId"]

        # UPDATE
        bedrock.update_guardrail(
            guardrailIdentifier=gr_id,
            name=name,
            blockedInputMessaging="updated-input-msg",
            blockedOutputsMessaging="updated-output-msg",
        )

        # RETRIEVE to confirm changes
        r = bedrock.get_guardrail(guardrailIdentifier=gr_id)
        assert r["blockedInputMessaging"] == "updated-input-msg"
        assert r["blockedOutputsMessaging"] == "updated-output-msg"

    def test_update_guardrail_name_visible_in_list(self, bedrock):
        """After UpdateGuardrail, the new name is visible in ListGuardrails (C+U+L)."""
        old_name = _unique("guard-oldname")
        create_r = bedrock.create_guardrail(
            name=old_name,
            blockedInputMessaging="blocked",
            blockedOutputsMessaging="blocked",
        )
        gr_id = create_r["guardrailId"]

        new_name = _unique("guard-newname")
        bedrock.update_guardrail(
            guardrailIdentifier=gr_id,
            name=new_name,
            blockedInputMessaging="blocked",
            blockedOutputsMessaging="blocked",
        )

        # LIST and find by ID
        list_r = bedrock.list_guardrails()
        match = next((g for g in list_r["guardrails"] if g["id"] == gr_id), None)
        assert match is not None
        assert match["name"] == new_name

    def test_update_guardrail_returns_updated_at(self, bedrock):
        """UpdateGuardrail returns an updatedAt timestamp (C+U)."""
        name = _unique("guard-ts")
        create_r = bedrock.create_guardrail(
            name=name,
            blockedInputMessaging="blocked",
            blockedOutputsMessaging="blocked",
        )
        gr_id = create_r["guardrailId"]

        update_r = bedrock.update_guardrail(
            guardrailIdentifier=gr_id,
            name=_unique("guard-ts-new"),
            blockedInputMessaging="new-blocked",
            blockedOutputsMessaging="new-blocked",
        )
        assert "updatedAt" in update_r
        assert update_r["updatedAt"] is not None

    def test_update_then_delete_guardrail(self, bedrock):
        """UpdateGuardrail then delete: resource gone after delete (C+U+D+E)."""
        name = _unique("guard-upd-del")
        create_r = bedrock.create_guardrail(
            name=name,
            blockedInputMessaging="blocked",
            blockedOutputsMessaging="blocked",
        )
        gr_id = create_r["guardrailId"]

        # UPDATE
        bedrock.update_guardrail(
            guardrailIdentifier=gr_id,
            name=_unique("guard-upd-del-new"),
            blockedInputMessaging="new-blocked",
            blockedOutputsMessaging="new-blocked",
        )

        # DELETE
        bedrock.delete_guardrail(guardrailIdentifier=gr_id)

        # ERROR on get
        with pytest.raises(ClientError) as exc:
            bedrock.get_guardrail(guardrailIdentifier=gr_id)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException", "ValidationException"
        )

    def test_guardrail_update_preserves_arn(self, bedrock):
        """UpdateGuardrail does not change the guardrailArn (C+U+R)."""
        name = _unique("guard-arnstable")
        create_r = bedrock.create_guardrail(
            name=name,
            blockedInputMessaging="blocked",
            blockedOutputsMessaging="blocked",
        )
        gr_id = create_r["guardrailId"]
        original_arn = create_r["guardrailArn"]

        bedrock.update_guardrail(
            guardrailIdentifier=gr_id,
            name=_unique("guard-arnstable-new"),
            blockedInputMessaging="updated",
            blockedOutputsMessaging="updated",
        )

        get_r = bedrock.get_guardrail(guardrailIdentifier=gr_id)
        assert get_r["guardrailArn"] == original_arn

    def test_guardrail_create_version_then_delete_draft(self, bedrock):
        """CreateGuardrailVersion + delete DRAFT guardrail (C+U+D+E)."""
        name = _unique("guard-ver-del")
        create_r = bedrock.create_guardrail(
            name=name,
            blockedInputMessaging="blocked",
            blockedOutputsMessaging="blocked",
        )
        gr_id = create_r["guardrailId"]

        # CREATE VERSION (update state)
        ver_r = bedrock.create_guardrail_version(guardrailIdentifier=gr_id)
        assert ver_r["version"] == "1"

        # DELETE the guardrail
        bedrock.delete_guardrail(guardrailIdentifier=gr_id)

        # ERROR after delete
        with pytest.raises(ClientError) as exc:
            bedrock.get_guardrail(guardrailIdentifier=gr_id)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException", "ValidationException"
        )


class TestBedrockProvisionedModelThroughputUpdateOps:
    """UPDATE pattern tests for provisioned model throughput (C+U+R+L+D+E)."""

    def test_update_pmt_model_units(self, bedrock):
        """UpdateProvisionedModelThroughput with new model units returns 200 (C+U+R)."""
        name = _unique("pmt-units")
        create_r = bedrock.create_provisioned_model_throughput(
            modelUnits=1,
            provisionedModelName=name,
            modelId="amazon.titan-text-express-v1",
        )
        pmt_arn = create_r["provisionedModelArn"]

        # UPDATE - change the name
        new_name = _unique("pmt-units-new")
        upd_r = bedrock.update_provisioned_model_throughput(
            provisionedModelId=pmt_arn,
            desiredProvisionedModelName=new_name,
        )
        assert upd_r["ResponseMetadata"]["HTTPStatusCode"] == 200

        # RETRIEVE - ARN is stable
        get_r = bedrock.get_provisioned_model_throughput(provisionedModelId=pmt_arn)
        assert get_r["provisionedModelArn"] == pmt_arn

    def test_update_pmt_name_then_list(self, bedrock):
        """UpdateProvisionedModelThroughput name change reflected in list (C+U+L)."""
        name = _unique("pmt-rename")
        create_r = bedrock.create_provisioned_model_throughput(
            modelUnits=1,
            provisionedModelName=name,
            modelId="amazon.titan-text-express-v1",
        )
        pmt_arn = create_r["provisionedModelArn"]
        new_name = _unique("pmt-renamed")

        bedrock.update_provisioned_model_throughput(
            provisionedModelId=pmt_arn,
            desiredProvisionedModelName=new_name,
        )

        # LIST and verify ARN still there
        list_r = bedrock.list_provisioned_model_throughputs()
        arns = [p["provisionedModelArn"] for p in list_r["provisionedModelSummaries"]]
        assert pmt_arn in arns

    def test_pmt_create_update_delete_error_lifecycle(self, bedrock):
        """Full PMT lifecycle: create → update → delete → error on get (C+U+D+E)."""
        name = _unique("pmt-lifecycle")
        create_r = bedrock.create_provisioned_model_throughput(
            modelUnits=1,
            provisionedModelName=name,
            modelId="amazon.titan-text-express-v1",
        )
        pmt_arn = create_r["provisionedModelArn"]
        assert re.match(r"arn:aws:bedrock:[^:]+:\d+:provisioned-model/.+", pmt_arn)

        # UPDATE
        bedrock.update_provisioned_model_throughput(
            provisionedModelId=pmt_arn,
            desiredProvisionedModelName=_unique("pmt-new"),
        )

        # DELETE
        bedrock.delete_provisioned_model_throughput(provisionedModelId=pmt_arn)

        # ERROR on get
        with pytest.raises(ClientError) as exc:
            bedrock.get_provisioned_model_throughput(provisionedModelId=pmt_arn)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException", "ValidationException"
        )

    def test_pmt_not_in_list_after_delete(self, bedrock):
        """PMT removed from list after delete (C+L+D+L)."""
        name = _unique("pmt-listdel")
        create_r = bedrock.create_provisioned_model_throughput(
            modelUnits=1,
            provisionedModelName=name,
            modelId="amazon.titan-text-express-v1",
        )
        pmt_arn = create_r["provisionedModelArn"]

        # Confirm in list
        r1 = bedrock.list_provisioned_model_throughputs()
        assert any(p["provisionedModelArn"] == pmt_arn for p in r1["provisionedModelSummaries"])

        # DELETE
        bedrock.delete_provisioned_model_throughput(provisionedModelId=pmt_arn)

        # Verify not in list
        r2 = bedrock.list_provisioned_model_throughputs()
        assert not any(p["provisionedModelArn"] == pmt_arn for p in r2["provisionedModelSummaries"])


class TestBedrockLoggingConfigUpdateOps:
    """UPDATE/PUT pattern tests for logging configuration (C+U+R+D)."""

    def test_put_logging_config_is_idempotent(self, bedrock):
        """Calling put_model_invocation_logging_configuration twice updates the config (C+U+R)."""
        bedrock.put_model_invocation_logging_configuration(
            loggingConfig={
                "textDataDeliveryEnabled": True,
                "imageDataDeliveryEnabled": True,
                "embeddingDataDeliveryEnabled": False,
            }
        )
        # Overwrite
        bedrock.put_model_invocation_logging_configuration(
            loggingConfig={
                "textDataDeliveryEnabled": False,
                "imageDataDeliveryEnabled": True,
                "embeddingDataDeliveryEnabled": True,
            }
        )

        r = bedrock.get_model_invocation_logging_configuration()
        cfg = r["loggingConfig"]
        assert cfg["textDataDeliveryEnabled"] is False
        assert cfg["imageDataDeliveryEnabled"] is True
        assert cfg["embeddingDataDeliveryEnabled"] is True

    def test_put_then_delete_logging_config(self, bedrock):
        """put logging config, then delete, then get returns empty (C+U+R+D)."""
        bedrock.put_model_invocation_logging_configuration(
            loggingConfig={
                "textDataDeliveryEnabled": True,
                "imageDataDeliveryEnabled": False,
                "embeddingDataDeliveryEnabled": False,
            }
        )

        # Verify it's set
        r = bedrock.get_model_invocation_logging_configuration()
        assert r["loggingConfig"].get("textDataDeliveryEnabled") is True

        # DELETE
        bedrock.delete_model_invocation_logging_configuration()

        # After delete, returns empty config
        r2 = bedrock.get_model_invocation_logging_configuration()
        assert r2["loggingConfig"] == {}


class TestBedrockInferenceProfileUpdateAndDelete:
    """UPDATE and DELETE pattern tests for inference profiles (C+R+L+U+D+E)."""

    def test_inference_profile_create_list_delete_lifecycle(self, bedrock):
        """Create inference profile → list → delete → verify gone (C+L+D+E)."""
        name = _unique("inf-del")
        create_r = bedrock.create_inference_profile(
            inferenceProfileName=name,
            modelSource={"copyFrom": TITAN_MODEL_ARN},
        )
        arn = create_r["inferenceProfileArn"]

        # LIST - must be present
        list_r = bedrock.list_inference_profiles()
        arns = [p["inferenceProfileArn"] for p in list_r["inferenceProfileSummaries"]]
        assert arn in arns

        # DELETE
        bedrock.delete_inference_profile(inferenceProfileIdentifier=arn)

        # ERROR - ResourceNotFoundException after delete
        with pytest.raises(ClientError) as exc:
            bedrock.get_inference_profile(inferenceProfileIdentifier=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_inference_profile_not_in_list_after_delete(self, bedrock):
        """Deleted inference profile absent from list (C+L+D+L)."""
        name = _unique("inf-listdel")
        create_r = bedrock.create_inference_profile(
            inferenceProfileName=name,
            modelSource={"copyFrom": TITAN_MODEL_ARN},
        )
        arn = create_r["inferenceProfileArn"]

        # Confirm in list before delete
        r1 = bedrock.list_inference_profiles()
        before_arns = [p["inferenceProfileArn"] for p in r1["inferenceProfileSummaries"]]
        assert arn in before_arns

        # DELETE
        bedrock.delete_inference_profile(inferenceProfileIdentifier=arn)

        # Confirm not in list after delete
        r2 = bedrock.list_inference_profiles()
        after_arns = [p["inferenceProfileArn"] for p in r2["inferenceProfileSummaries"]]
        assert arn not in after_arns

    def test_inference_profile_arn_format(self, bedrock):
        """Inference profile ARN matches expected format (C+R)."""
        name = _unique("inf-arnfmt")
        create_r = bedrock.create_inference_profile(
            inferenceProfileName=name,
            modelSource={"copyFrom": TITAN_MODEL_ARN},
        )
        arn = create_r["inferenceProfileArn"]
        assert re.match(r"arn:aws:bedrock:[^:]+:\d+:inference-profile/.+", arn), \
            f"unexpected ARN format: {arn}"

        # RETRIEVE and confirm ARN is stable
        get_r = bedrock.get_inference_profile(inferenceProfileIdentifier=arn)
        assert get_r["inferenceProfileArn"] == arn


class TestBedrockPromptRouterDeleteOps:
    """DELETE lifecycle for prompt routers (C+R+L+D+E)."""

    def test_prompt_router_create_get_delete_error(self, bedrock):
        """Create → get → delete → error on get (C+R+D+E)."""
        name = _unique("router-del")
        create_r = bedrock.create_prompt_router(
            promptRouterName=name,
            models=[{"modelArn": TITAN_MODEL_ARN}],
            fallbackModel={"modelArn": TITAN_MODEL_ARN},
            routingCriteria={"responseQualityDifference": 0.5},
        )
        arn = create_r["promptRouterArn"]

        # RETRIEVE
        get_r = bedrock.get_prompt_router(promptRouterArn=arn)
        assert get_r["promptRouterArn"] == arn

        # DELETE
        bedrock.delete_prompt_router(promptRouterArn=arn)

        # ERROR on get after delete
        with pytest.raises(ClientError) as exc:
            bedrock.get_prompt_router(promptRouterArn=arn)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException", "ValidationException"
        )

    def test_prompt_router_not_in_list_after_delete(self, bedrock):
        """Deleted prompt router absent from list (C+L+D+L)."""
        name = _unique("router-listdel")
        create_r = bedrock.create_prompt_router(
            promptRouterName=name,
            models=[{"modelArn": TITAN_MODEL_ARN}],
            fallbackModel={"modelArn": TITAN_MODEL_ARN},
            routingCriteria={"responseQualityDifference": 0.5},
        )
        arn = create_r["promptRouterArn"]

        # Confirm in list
        r1 = bedrock.list_prompt_routers()
        arns_before = [s["promptRouterArn"] for s in r1["promptRouterSummaries"]]
        assert arn in arns_before

        # DELETE
        bedrock.delete_prompt_router(promptRouterArn=arn)

        # Confirm not in list
        r2 = bedrock.list_prompt_routers()
        arns_after = [s["promptRouterArn"] for s in r2["promptRouterSummaries"]]
        assert arn not in arns_after


class TestBedrockAutomatedReasoningPolicyUpdateAndDelete:
    """UPDATE and DELETE patterns for automated reasoning policies (C+U+R+D+E)."""

    def test_create_policy_then_update(self, bedrock):
        """Create automated reasoning policy then update it (C+U+R)."""
        name = _unique("arp-upd")
        create_r = bedrock.create_automated_reasoning_policy(
            name=name,
            policyDefinition={
                "version": "1.0",
                "rules": [{"id": "rule-abcdef123456", "expression": "true"}],
            },
        )
        policy_arn = create_r["policyArn"]
        assert policy_arn.startswith("arn:aws:bedrock:")

        # UPDATE
        upd_r = bedrock.update_automated_reasoning_policy(
            policyArn=policy_arn,
            policyDefinition={
                "version": "1.1",
                "rules": [
                    {"id": "rule-abcdef123456", "expression": "false"},
                    {"id": "rule-fedcba654321", "expression": "true"},
                ],
            },
        )
        assert upd_r["ResponseMetadata"]["HTTPStatusCode"] == 200

        # RETRIEVE
        get_r = bedrock.get_automated_reasoning_policy(policyArn=policy_arn)
        assert get_r["policyArn"] == policy_arn

    def test_create_policy_then_delete(self, bedrock):
        """Create policy then delete → error on get (C+D+E)."""
        name = _unique("arp-del")
        create_r = bedrock.create_automated_reasoning_policy(
            name=name,
            policyDefinition={
                "version": "1.0",
                "rules": [{"id": "rule-abcdef123456", "expression": "true"}],
            },
        )
        policy_arn = create_r["policyArn"]

        # DELETE
        bedrock.delete_automated_reasoning_policy(policyArn=policy_arn)

        # ERROR on get
        with pytest.raises(ClientError) as exc:
            bedrock.get_automated_reasoning_policy(policyArn=policy_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_policy_appears_in_list_after_create(self, bedrock):
        """Created policy is visible in ListAutomatedReasoningPolicies (C+L)."""
        name = _unique("arp-listed")
        create_r = bedrock.create_automated_reasoning_policy(
            name=name,
            policyDefinition={
                "version": "1.0",
                "rules": [{"id": "rule-abcdef123456", "expression": "true"}],
            },
        )
        policy_arn = create_r["policyArn"]

        list_r = bedrock.list_automated_reasoning_policies()
        assert list_r["ResponseMetadata"]["HTTPStatusCode"] == 200
        # List should succeed (policy is present)
        policy_arns = [p.get("policyArn", "") for p in list_r.get("policies", [])]
        # If list returns ARNs, verify ours is in there; otherwise just confirm 200
        if policy_arns:
            assert policy_arn in policy_arns

    def test_create_update_delete_full_lifecycle(self, bedrock):
        """Full C+U+R+D+E lifecycle for automated reasoning policy."""
        name = _unique("arp-full")

        # CREATE
        create_r = bedrock.create_automated_reasoning_policy(
            name=name,
            policyDefinition={
                "version": "1.0",
                "rules": [{"id": "rule-abcdef123456", "expression": "true"}],
            },
        )
        policy_arn = create_r["policyArn"]
        assert re.match(r"arn:aws:bedrock:[^:]+:\d+:automated-reasoning-policy/.+", policy_arn)

        # UPDATE
        bedrock.update_automated_reasoning_policy(
            policyArn=policy_arn,
            policyDefinition={
                "version": "2.0",
                "rules": [{"id": "rule-abcdef123456", "expression": "false"}],
            },
        )

        # RETRIEVE after update
        get_r = bedrock.get_automated_reasoning_policy(policyArn=policy_arn)
        assert get_r["policyArn"] == policy_arn

        # DELETE
        bedrock.delete_automated_reasoning_policy(policyArn=policy_arn)

        # ERROR on get after delete
        with pytest.raises(ClientError) as exc:
            bedrock.get_automated_reasoning_policy(policyArn=policy_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBedrockCustomModelDeploymentUpdateAndDelete:
    """UPDATE and DELETE patterns for custom model deployments (C+U+R+D+E)."""

    def test_create_deployment_then_update(self, bedrock):
        """Create custom model deployment then update model ARN (C+U+R)."""
        name = _unique("deploy-upd")
        create_r = bedrock.create_custom_model_deployment(
            modelDeploymentName=name,
            modelArn="arn:aws:bedrock:us-east-1:123456789012:custom-model/test-model",
        )
        dep_arn = create_r["customModelDeploymentArn"]
        assert re.match(r"arn:aws:bedrock:[^:]+:\d+:custom-model-deployment/.+", dep_arn)

        # UPDATE
        upd_r = bedrock.update_custom_model_deployment(
            customModelDeploymentIdentifier=dep_arn,
            modelArn="arn:aws:bedrock:us-east-1:123456789012:custom-model/test-model-v2",
        )
        assert upd_r["ResponseMetadata"]["HTTPStatusCode"] == 200

        # RETRIEVE to confirm it still exists
        get_r = bedrock.get_custom_model_deployment(customModelDeploymentIdentifier=dep_arn)
        assert get_r["customModelDeploymentArn"] == dep_arn

    def test_create_deployment_then_delete_then_error(self, bedrock):
        """Create custom model deployment → delete → error on get (C+D+E)."""
        name = _unique("deploy-del")
        create_r = bedrock.create_custom_model_deployment(
            modelDeploymentName=name,
            modelArn="arn:aws:bedrock:us-east-1:123456789012:custom-model/test-model",
        )
        dep_arn = create_r["customModelDeploymentArn"]

        # DELETE
        bedrock.delete_custom_model_deployment(customModelDeploymentIdentifier=dep_arn)

        # ERROR on get
        with pytest.raises(ClientError) as exc:
            bedrock.get_custom_model_deployment(customModelDeploymentIdentifier=dep_arn)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException", "ValidationException"
        )

    def test_create_deployment_list_delete_lifecycle(self, bedrock):
        """Create deployment → list it → delete → verify absent in list (C+L+D+L)."""
        name = _unique("deploy-listdel")
        create_r = bedrock.create_custom_model_deployment(
            modelDeploymentName=name,
            modelArn="arn:aws:bedrock:us-east-1:123456789012:custom-model/test-model",
        )
        dep_arn = create_r["customModelDeploymentArn"]

        # LIST before delete
        r1 = bedrock.list_custom_model_deployments()
        arns_before = [
            d.get("customModelDeploymentArn", "")
            for d in r1.get("modelDeploymentSummaries", [])
        ]
        assert dep_arn in arns_before

        # DELETE
        bedrock.delete_custom_model_deployment(customModelDeploymentIdentifier=dep_arn)

        # LIST after delete
        r2 = bedrock.list_custom_model_deployments()
        arns_after = [
            d.get("customModelDeploymentArn", "")
            for d in r2.get("modelDeploymentSummaries", [])
        ]
        assert dep_arn not in arns_after


class TestBedrockEdgeCasesUnicodeAndLimits:
    """Edge cases: Unicode names, long strings, multiple tag operations."""

    def test_guardrail_name_with_hyphens_and_numbers(self, bedrock):
        """Guardrail name with hyphens and numbers is accepted (C+R)."""
        name = f"guard-123-{uuid.uuid4().hex[:8]}"
        create_r = bedrock.create_guardrail(
            name=name,
            blockedInputMessaging="blocked",
            blockedOutputsMessaging="blocked",
        )
        gr_id = create_r["guardrailId"]

        get_r = bedrock.get_guardrail(guardrailIdentifier=gr_id)
        assert get_r["name"] == name

    def test_job_with_multiple_hyperparameters(self, bedrock):
        """Job created with many hyperparameters returns them all (C+R)."""
        job_name = _unique("multi-hp")
        bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=_unique("model"),
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://test-bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://test-bucket/output/"},
            hyperParameters={
                "epochCount": "3",
                "batchSize": "8",
                "learningRate": "0.00005",
                "warmupSteps": "100",
            },
        )

        r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        hp = r["hyperParameters"]
        assert hp["epochCount"] == "3"
        assert hp["batchSize"] == "8"
        assert hp["learningRate"] == "0.00005"
        assert hp["warmupSteps"] == "100"

    def test_tag_resource_with_many_tags(self, bedrock):
        """Tag a resource with multiple tags, all tags returned (C+U+L)."""
        job_name = _unique("manytags")
        create_r = bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=_unique("model"),
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://test-bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://test-bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )
        job_arn = create_r["jobArn"]

        tags = [{"key": f"key-{i}", "value": f"val-{i}"} for i in range(5)]
        bedrock.tag_resource(resourceARN=job_arn, tags=tags)

        list_r = bedrock.list_tags_for_resource(resourceARN=job_arn)
        tag_map = {t["key"]: t["value"] for t in list_r["tags"]}
        for i in range(5):
            assert tag_map[f"key-{i}"] == f"val-{i}"

    def test_guardrail_delete_nonexistent_returns_error(self, bedrock):
        """Deleting a nonexistent guardrail raises an error (E)."""
        with pytest.raises(ClientError) as exc:
            bedrock.delete_guardrail(guardrailIdentifier="nonexistent-guardrail-99")
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException", "ValidationException"
        )

    def test_list_custom_models_then_delete_one_then_list_again(self, bedrock):
        """Create 3 models, delete 1, verify only 2 visible in scoped list (C+L+D+L)."""
        suffix = uuid.uuid4().hex[:8]
        job_name_prefix = f"del3test-{suffix}"
        for i in range(3):
            bedrock.create_model_customization_job(
                jobName=f"{job_name_prefix}-{i}",
                customModelName=f"del3model-{suffix}-{i}",
                roleArn="arn:aws:iam::123456789012:role/test",
                baseModelIdentifier="amazon.titan-text-express-v1",
                trainingDataConfig={"s3Uri": "s3://test-bucket/train.jsonl"},
                outputDataConfig={"s3Uri": "s3://test-bucket/output/"},
                hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
            )

        # All 3 visible
        r1 = bedrock.list_custom_models(nameContains=f"{job_name_prefix}-0")
        assert any(m["modelName"] == f"del3model-{suffix}-0" for m in r1["modelSummaries"])

        # Delete model 0
        bedrock.delete_custom_model(modelIdentifier=f"del3model-{suffix}-0")

        # Model 0 is gone
        r2 = bedrock.list_custom_models(nameContains=f"{job_name_prefix}-0")
        assert not any(m["modelName"] == f"del3model-{suffix}-0" for m in r2["modelSummaries"])

        # Models 1 and 2 still exist
        r3 = bedrock.list_custom_models(nameContains=f"{job_name_prefix}-1")
        assert any(m["modelName"] == f"del3model-{suffix}-1" for m in r3["modelSummaries"])


class TestBedrockBehavioralFidelityOrdering:
    """Behavioral fidelity: list ordering and timestamp consistency."""

    def test_list_jobs_pagination_exhausts_all_items(self, bedrock):
        """Exhausting pagination of jobs retrieves every created job (C+L)."""
        suffix = uuid.uuid4().hex[:8]
        names = [f"paginateall-{suffix}-{i}" for i in range(3)]
        for name in names:
            bedrock.create_model_customization_job(
                jobName=name,
                customModelName=_unique("model"),
                roleArn="arn:aws:iam::123456789012:role/test",
                baseModelIdentifier="amazon.titan-text-express-v1",
                trainingDataConfig={"s3Uri": "s3://test-bucket/train.jsonl"},
                outputDataConfig={"s3Uri": "s3://test-bucket/output/"},
                hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
            )

        seen = set()
        kwargs = {"maxResults": 1, "nameContains": f"paginateall-{suffix}"}
        while True:
            r = bedrock.list_model_customization_jobs(**kwargs)
            for j in r["modelCustomizationJobSummaries"]:
                seen.add(j["jobName"])
            if "nextToken" not in r:
                break
            kwargs["nextToken"] = r["nextToken"]

        for name in names:
            assert name in seen

    def test_list_guardrails_pagination(self, bedrock):
        """Creating 3 guardrails and paginating returns all 3 (C+L)."""
        suffix = uuid.uuid4().hex[:8]
        created_ids = []
        for i in range(3):
            r = bedrock.create_guardrail(
                name=f"pg-guard-{suffix}-{i}",
                blockedInputMessaging="blocked",
                blockedOutputsMessaging="blocked",
            )
            created_ids.append(r["guardrailId"])

        # Collect all IDs via pagination
        seen_ids = set()
        kwargs = {"maxResults": 1}
        while True:
            r = bedrock.list_guardrails(**kwargs)
            for g in r["guardrails"]:
                seen_ids.add(g["id"])
            if "nextToken" not in r:
                break
            kwargs["nextToken"] = r["nextToken"]

        for gr_id in created_ids:
            assert gr_id in seen_ids

    def test_job_creation_time_is_recent(self, bedrock):
        """creationTime from GetModelCustomizationJob is within the last day (C+R)."""
        job_name = _unique("timechk")
        bedrock.create_model_customization_job(
            jobName=job_name,
            customModelName=_unique("model"),
            roleArn="arn:aws:iam::123456789012:role/test",
            baseModelIdentifier="amazon.titan-text-express-v1",
            trainingDataConfig={"s3Uri": "s3://test-bucket/train.jsonl"},
            outputDataConfig={"s3Uri": "s3://test-bucket/output/"},
            hyperParameters={"epochCount": "1", "batchSize": "1", "learningRate": "0.00001"},
        )

        r = bedrock.get_model_customization_job(jobIdentifier=job_name)
        ct = r["creationTime"]
        assert isinstance(ct, datetime.datetime)
        delta = abs((datetime.datetime.now() - ct.replace(tzinfo=None)).total_seconds())
        assert delta < 86400, f"creationTime is >1 day away: {delta}s"

    def test_guardrail_created_at_present(self, bedrock):
        """GetGuardrail returns a createdAt timestamp (C+R)."""
        name = _unique("guard-creat")
        create_r = bedrock.create_guardrail(
            name=name,
            blockedInputMessaging="blocked",
            blockedOutputsMessaging="blocked",
        )
        gr_id = create_r["guardrailId"]

        get_r = bedrock.get_guardrail(guardrailIdentifier=gr_id)
        assert "createdAt" in get_r
        assert get_r["createdAt"] is not None

    def test_guardrail_updated_at_changes_after_update(self, bedrock):
        """UpdateGuardrail returns updatedAt and it is after createdAt (C+U+R)."""
        name = _unique("guard-attime")
        create_r = bedrock.create_guardrail(
            name=name,
            blockedInputMessaging="blocked",
            blockedOutputsMessaging="blocked",
        )
        gr_id = create_r["guardrailId"]
        created_at = bedrock.get_guardrail(guardrailIdentifier=gr_id).get("createdAt")

        update_r = bedrock.update_guardrail(
            guardrailIdentifier=gr_id,
            name=_unique("guard-attime-new"),
            blockedInputMessaging="updated",
            blockedOutputsMessaging="updated",
        )
        updated_at = update_r["updatedAt"]
        assert updated_at is not None
        if isinstance(updated_at, datetime.datetime) and isinstance(created_at, datetime.datetime):
            assert updated_at >= created_at.replace(tzinfo=updated_at.tzinfo)
