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
        assert "role/test" in r["roleArn"]

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
        assert "foundation-model/amazon.titan-text-express-v1" in r["baseModelArn"]

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
        """Custom model fetched by name has expected fields."""
        model_name = _unique("model")
        _create_job(bedrock, model_name=model_name)

        s = bedrock.get_custom_model(modelIdentifier=model_name)
        assert "modelArn" in s
        assert "baseModelArn" in s
        assert "creationTime" in s

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
        assert "modelSummaries" in r
        assert isinstance(r["modelSummaries"], list)

    def test_list_guardrails(self, bedrock):
        r = bedrock.list_guardrails()
        assert "guardrails" in r
        assert isinstance(r["guardrails"], list)

    def test_list_inference_profiles(self, bedrock):
        r = bedrock.list_inference_profiles()
        assert "inferenceProfileSummaries" in r
        assert isinstance(r["inferenceProfileSummaries"], list)

    def test_list_imported_models(self, bedrock):
        r = bedrock.list_imported_models()
        assert "modelSummaries" in r
        assert isinstance(r["modelSummaries"], list)

    def test_list_evaluation_jobs(self, bedrock):
        r = bedrock.list_evaluation_jobs()
        assert "jobSummaries" in r
        assert isinstance(r["jobSummaries"], list)

    def test_list_model_copy_jobs(self, bedrock):
        r = bedrock.list_model_copy_jobs()
        assert "modelCopyJobSummaries" in r
        assert isinstance(r["modelCopyJobSummaries"], list)

    def test_list_model_import_jobs(self, bedrock):
        r = bedrock.list_model_import_jobs()
        assert "modelImportJobSummaries" in r
        assert isinstance(r["modelImportJobSummaries"], list)

    def test_list_model_invocation_jobs(self, bedrock):
        r = bedrock.list_model_invocation_jobs()
        assert "invocationJobSummaries" in r
        assert isinstance(r["invocationJobSummaries"], list)

    def test_list_provisioned_model_throughputs(self, bedrock):
        r = bedrock.list_provisioned_model_throughputs()
        assert "provisionedModelSummaries" in r
        assert isinstance(r["provisionedModelSummaries"], list)

    def test_list_prompt_routers(self, bedrock):
        r = bedrock.list_prompt_routers()
        assert "promptRouterSummaries" in r
        assert isinstance(r["promptRouterSummaries"], list)

    def test_list_marketplace_model_endpoints(self, bedrock):
        r = bedrock.list_marketplace_model_endpoints()
        assert "marketplaceModelEndpoints" in r
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
        assert "modelId" in r
        assert "agreementAvailability" in r

    def test_list_foundation_model_agreement_offers(self, bedrock):
        """ListFoundationModelAgreementOffers returns offers list."""
        r = bedrock.list_foundation_model_agreement_offers(modelId="anthropic.claude-v2")
        assert "offers" in r
        assert isinstance(r["offers"], list)

    def test_get_use_case_for_model_access(self, bedrock):
        """GetUseCaseForModelAccess returns a response."""
        r = bedrock.get_use_case_for_model_access()
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200


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
