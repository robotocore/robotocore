"""SageMaker compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def sagemaker():
    return make_client("sagemaker")


def _uid(prefix="test"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestSageMakerListOperations:
    def test_list_endpoints_empty(self, sagemaker):
        response = sagemaker.list_endpoints()
        assert "Endpoints" in response
        assert isinstance(response["Endpoints"], list)

    def test_list_models_empty(self, sagemaker):
        response = sagemaker.list_models()
        assert "Models" in response
        assert isinstance(response["Models"], list)

    def test_list_training_jobs_empty(self, sagemaker):
        response = sagemaker.list_training_jobs()
        assert "TrainingJobSummaries" in response
        assert isinstance(response["TrainingJobSummaries"], list)

    def test_list_notebook_instances_empty(self, sagemaker):
        response = sagemaker.list_notebook_instances()
        assert "NotebookInstances" in response
        assert isinstance(response["NotebookInstances"], list)

    def test_list_experiments_empty(self, sagemaker):
        response = sagemaker.list_experiments()
        assert "ExperimentSummaries" in response
        assert isinstance(response["ExperimentSummaries"], list)


class TestSageMakerModelCRUD:
    def test_create_and_describe_model(self, sagemaker):
        name = _uid("model")
        resp = sagemaker.create_model(
            ModelName=name,
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            PrimaryContainer={
                "Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
            },
        )
        assert "ModelArn" in resp
        try:
            desc = sagemaker.describe_model(ModelName=name)
            assert desc["ModelName"] == name
        finally:
            sagemaker.delete_model(ModelName=name)

    def test_list_models_after_create(self, sagemaker):
        name = _uid("model")
        sagemaker.create_model(
            ModelName=name,
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            PrimaryContainer={
                "Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
            },
        )
        try:
            resp = sagemaker.list_models()
            names = [m["ModelName"] for m in resp["Models"]]
            assert name in names
        finally:
            sagemaker.delete_model(ModelName=name)

    def test_delete_model(self, sagemaker):
        name = _uid("model")
        sagemaker.create_model(
            ModelName=name,
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            PrimaryContainer={
                "Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
            },
        )
        sagemaker.delete_model(ModelName=name)
        resp = sagemaker.list_models()
        names = [m["ModelName"] for m in resp["Models"]]
        assert name not in names


class TestSageMakerExperimentCRUD:
    def test_create_and_describe_experiment(self, sagemaker):
        name = _uid("exp")
        resp = sagemaker.create_experiment(ExperimentName=name)
        assert "ExperimentArn" in resp
        try:
            desc = sagemaker.describe_experiment(ExperimentName=name)
            assert desc["ExperimentName"] == name
        finally:
            sagemaker.delete_experiment(ExperimentName=name)

    def test_list_experiments_after_create(self, sagemaker):
        name = _uid("exp")
        sagemaker.create_experiment(ExperimentName=name)
        try:
            resp = sagemaker.list_experiments()
            names = [e["ExperimentName"] for e in resp["ExperimentSummaries"]]
            assert name in names
        finally:
            sagemaker.delete_experiment(ExperimentName=name)

    def test_delete_experiment(self, sagemaker):
        name = _uid("exp")
        sagemaker.create_experiment(ExperimentName=name)
        sagemaker.delete_experiment(ExperimentName=name)
        resp = sagemaker.list_experiments()
        names = [e["ExperimentName"] for e in resp["ExperimentSummaries"]]
        assert name not in names


class TestSageMakerNotebookInstance:
    def test_create_and_describe_notebook_instance(self, sagemaker):
        name = _uid("nb")
        resp = sagemaker.create_notebook_instance(
            NotebookInstanceName=name,
            InstanceType="ml.t2.medium",
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        assert "NotebookInstanceArn" in resp
        try:
            desc = sagemaker.describe_notebook_instance(NotebookInstanceName=name)
            assert desc["NotebookInstanceName"] == name
            assert desc["InstanceType"] == "ml.t2.medium"
        finally:
            sagemaker.stop_notebook_instance(NotebookInstanceName=name)
            sagemaker.delete_notebook_instance(NotebookInstanceName=name)

    def test_list_notebook_instances_after_create(self, sagemaker):
        name = _uid("nb")
        sagemaker.create_notebook_instance(
            NotebookInstanceName=name,
            InstanceType="ml.t2.medium",
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        try:
            resp = sagemaker.list_notebook_instances()
            names = [n["NotebookInstanceName"] for n in resp["NotebookInstances"]]
            assert name in names
        finally:
            sagemaker.stop_notebook_instance(NotebookInstanceName=name)
            sagemaker.delete_notebook_instance(NotebookInstanceName=name)


class TestSageMakerTrainingJob:
    def test_create_and_describe_training_job(self, sagemaker):
        name = _uid("tj")
        resp = sagemaker.create_training_job(
            TrainingJobName=name,
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            AlgorithmSpecification={
                "TrainingImage": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
                "TrainingInputMode": "File",
            },
            OutputDataConfig={"S3OutputPath": "s3://my-bucket/output"},
            ResourceConfig={
                "InstanceType": "ml.m4.xlarge",
                "InstanceCount": 1,
                "VolumeSizeInGB": 10,
            },
            StoppingCondition={"MaxRuntimeInSeconds": 3600},
        )
        assert "TrainingJobArn" in resp
        desc = sagemaker.describe_training_job(TrainingJobName=name)
        assert desc["TrainingJobName"] == name
        assert desc["TrainingJobStatus"] in ("InProgress", "Completed", "Failed", "Stopping")

    def test_list_training_jobs_after_create(self, sagemaker):
        name = _uid("tj")
        sagemaker.create_training_job(
            TrainingJobName=name,
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            AlgorithmSpecification={
                "TrainingImage": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
                "TrainingInputMode": "File",
            },
            OutputDataConfig={"S3OutputPath": "s3://my-bucket/output"},
            ResourceConfig={
                "InstanceType": "ml.m4.xlarge",
                "InstanceCount": 1,
                "VolumeSizeInGB": 10,
            },
            StoppingCondition={"MaxRuntimeInSeconds": 3600},
        )
        resp = sagemaker.list_training_jobs()
        names = [j["TrainingJobName"] for j in resp["TrainingJobSummaries"]]
        assert name in names


class TestSagemakerAutoCoverage:
    """Auto-generated coverage tests for sagemaker."""

    @pytest.fixture
    def client(self):
        return make_client("sagemaker")

    def test_list_auto_ml_jobs(self, client):
        """ListAutoMLJobs returns a response."""
        resp = client.list_auto_ml_jobs()
        assert "AutoMLJobSummaries" in resp

    def test_list_clusters(self, client):
        """ListClusters returns a response."""
        resp = client.list_clusters()
        assert "ClusterSummaries" in resp

    def test_list_compilation_jobs(self, client):
        """ListCompilationJobs returns a response."""
        resp = client.list_compilation_jobs()
        assert "CompilationJobSummaries" in resp

    def test_list_data_quality_job_definitions(self, client):
        """ListDataQualityJobDefinitions returns a response."""
        resp = client.list_data_quality_job_definitions()
        assert "JobDefinitionSummaries" in resp

    def test_list_domains(self, client):
        """ListDomains returns a response."""
        resp = client.list_domains()
        assert "Domains" in resp

    def test_list_endpoint_configs(self, client):
        """ListEndpointConfigs returns a response."""
        resp = client.list_endpoint_configs()
        assert "EndpointConfigs" in resp

    def test_list_hyper_parameter_tuning_jobs(self, client):
        """ListHyperParameterTuningJobs returns a response."""
        resp = client.list_hyper_parameter_tuning_jobs()
        assert "HyperParameterTuningJobSummaries" in resp

    def test_list_model_bias_job_definitions(self, client):
        """ListModelBiasJobDefinitions returns a response."""
        resp = client.list_model_bias_job_definitions()
        assert "JobDefinitionSummaries" in resp

    def test_list_model_cards(self, client):
        """ListModelCards returns a response."""
        resp = client.list_model_cards()
        assert "ModelCardSummaries" in resp

    def test_list_model_explainability_job_definitions(self, client):
        """ListModelExplainabilityJobDefinitions returns a response."""
        resp = client.list_model_explainability_job_definitions()
        assert "JobDefinitionSummaries" in resp

    def test_list_model_package_groups(self, client):
        """ListModelPackageGroups returns a response."""
        resp = client.list_model_package_groups()
        assert "ModelPackageGroupSummaryList" in resp

    def test_list_model_packages(self, client):
        """ListModelPackages returns a response."""
        resp = client.list_model_packages()
        assert "ModelPackageSummaryList" in resp

    def test_list_model_quality_job_definitions(self, client):
        """ListModelQualityJobDefinitions returns a response."""
        resp = client.list_model_quality_job_definitions()
        assert "JobDefinitionSummaries" in resp

    def test_list_pipelines(self, client):
        """ListPipelines returns a response."""
        resp = client.list_pipelines()
        assert "PipelineSummaries" in resp

    def test_list_processing_jobs(self, client):
        """ListProcessingJobs returns a response."""
        resp = client.list_processing_jobs()
        assert "ProcessingJobSummaries" in resp

    def test_list_transform_jobs(self, client):
        """ListTransformJobs returns a response."""
        resp = client.list_transform_jobs()
        assert "TransformJobSummaries" in resp

    def test_list_trial_components(self, client):
        """ListTrialComponents returns a response."""
        resp = client.list_trial_components()
        assert "TrialComponentSummaries" in resp

    def test_list_trials(self, client):
        """ListTrials returns a response."""
        resp = client.list_trials()
        assert "TrialSummaries" in resp


class TestSageMakerListOpsNoParams:
    """List operations that require no parameters and return empty lists."""

    @pytest.fixture
    def client(self):
        return make_client("sagemaker")

    def test_list_actions(self, client):
        resp = client.list_actions()
        assert isinstance(resp["ActionSummaries"], list)

    def test_list_algorithms(self, client):
        resp = client.list_algorithms()
        assert isinstance(resp["AlgorithmSummaryList"], list)

    def test_list_app_image_configs(self, client):
        resp = client.list_app_image_configs()
        assert isinstance(resp["AppImageConfigs"], list)

    def test_list_apps(self, client):
        resp = client.list_apps()
        assert isinstance(resp["Apps"], list)

    def test_list_artifacts(self, client):
        resp = client.list_artifacts()
        assert isinstance(resp["ArtifactSummaries"], list)

    def test_list_associations(self, client):
        resp = client.list_associations()
        assert isinstance(resp["AssociationSummaries"], list)

    def test_list_cluster_scheduler_configs(self, client):
        resp = client.list_cluster_scheduler_configs()
        assert isinstance(resp["ClusterSchedulerConfigSummaries"], list)

    def test_list_code_repositories(self, client):
        resp = client.list_code_repositories()
        assert isinstance(resp["CodeRepositorySummaryList"], list)

    def test_list_compute_quotas(self, client):
        resp = client.list_compute_quotas()
        assert isinstance(resp["ComputeQuotaSummaries"], list)

    def test_list_contexts(self, client):
        resp = client.list_contexts()
        assert isinstance(resp["ContextSummaries"], list)

    def test_list_device_fleets(self, client):
        resp = client.list_device_fleets()
        assert isinstance(resp["DeviceFleetSummaries"], list)

    def test_list_edge_deployment_plans(self, client):
        resp = client.list_edge_deployment_plans()
        assert isinstance(resp["EdgeDeploymentPlanSummaries"], list)

    def test_list_edge_packaging_jobs(self, client):
        resp = client.list_edge_packaging_jobs()
        assert isinstance(resp["EdgePackagingJobSummaries"], list)

    def test_list_feature_groups(self, client):
        resp = client.list_feature_groups()
        assert isinstance(resp["FeatureGroupSummaries"], list)

    def test_list_flow_definitions(self, client):
        resp = client.list_flow_definitions()
        assert isinstance(resp["FlowDefinitionSummaries"], list)

    def test_list_hubs(self, client):
        resp = client.list_hubs()
        assert isinstance(resp["HubSummaries"], list)

    def test_list_human_task_uis(self, client):
        resp = client.list_human_task_uis()
        assert isinstance(resp["HumanTaskUiSummaries"], list)

    def test_list_images(self, client):
        resp = client.list_images()
        assert isinstance(resp.get("Images", []), list)

    def test_list_inference_components(self, client):
        resp = client.list_inference_components()
        assert isinstance(resp["InferenceComponents"], list)

    def test_list_inference_experiments(self, client):
        resp = client.list_inference_experiments()
        assert isinstance(resp["InferenceExperiments"], list)

    def test_list_inference_recommendations_jobs(self, client):
        resp = client.list_inference_recommendations_jobs()
        assert isinstance(resp["InferenceRecommendationsJobs"], list)

    def test_list_labeling_jobs(self, client):
        resp = client.list_labeling_jobs()
        assert isinstance(resp["LabelingJobSummaryList"], list)

    def test_list_lineage_groups(self, client):
        resp = client.list_lineage_groups()
        assert isinstance(resp["LineageGroupSummaries"], list)

    def test_list_mlflow_tracking_servers(self, client):
        resp = client.list_mlflow_tracking_servers()
        assert isinstance(resp["TrackingServerSummaries"], list)

    def test_list_model_metadata(self, client):
        resp = client.list_model_metadata()
        assert isinstance(resp["ModelMetadataSummaries"], list)

    def test_list_monitoring_schedules(self, client):
        resp = client.list_monitoring_schedules()
        assert isinstance(resp["MonitoringScheduleSummaries"], list)

    def test_list_notebook_instance_lifecycle_configs(self, client):
        resp = client.list_notebook_instance_lifecycle_configs()
        assert isinstance(resp["NotebookInstanceLifecycleConfigs"], list)

    def test_list_optimization_jobs(self, client):
        resp = client.list_optimization_jobs()
        assert isinstance(resp["OptimizationJobSummaries"], list)

    def test_list_projects(self, client):
        resp = client.list_projects()
        assert isinstance(resp["ProjectSummaryList"], list)

    def test_list_resource_catalogs(self, client):
        resp = client.list_resource_catalogs()
        assert isinstance(resp["ResourceCatalogs"], list)

    def test_list_spaces(self, client):
        resp = client.list_spaces()
        assert isinstance(resp["Spaces"], list)

    def test_list_studio_lifecycle_configs(self, client):
        resp = client.list_studio_lifecycle_configs()
        assert isinstance(resp["StudioLifecycleConfigs"], list)

    def test_list_subscribed_workteams(self, client):
        resp = client.list_subscribed_workteams()
        assert isinstance(resp["SubscribedWorkteams"], list)

    def test_list_training_plans(self, client):
        resp = client.list_training_plans()
        assert isinstance(resp["TrainingPlanSummaries"], list)

    def test_list_user_profiles(self, client):
        resp = client.list_user_profiles()
        assert isinstance(resp["UserProfiles"], list)

    def test_list_workforces(self, client):
        resp = client.list_workforces()
        assert isinstance(resp["Workforces"], list)

    def test_list_workteams(self, client):
        resp = client.list_workteams()
        assert isinstance(resp["Workteams"], list)


class TestSageMakerListOpsWithParams:
    """List operations that require parameters."""

    @pytest.fixture
    def client(self):
        return make_client("sagemaker")

    def test_list_aliases(self, client):
        resp = client.list_aliases(ImageName="nonexistent")
        assert isinstance(resp["SageMakerImageVersionAliases"], list)

    def test_list_candidates_for_auto_ml_job(self, client):
        resp = client.list_candidates_for_auto_ml_job(AutoMLJobName="nonexistent")
        assert isinstance(resp["Candidates"], list)

    def test_list_devices(self, client):
        resp = client.list_devices(DeviceFleetName="nonexistent")
        assert isinstance(resp["DeviceSummaries"], list)

    def test_list_hub_contents(self, client):
        resp = client.list_hub_contents(HubName="nonexistent", HubContentType="Model")
        assert isinstance(resp["HubContentSummaries"], list)

    def test_list_hub_content_versions(self, client):
        resp = client.list_hub_content_versions(
            HubName="h", HubContentName="n", HubContentType="Model"
        )
        assert isinstance(resp["HubContentSummaries"], list)

    def test_list_image_versions(self, client):
        resp = client.list_image_versions(ImageName="nonexistent")
        assert isinstance(resp.get("ImageVersions", []), list)

    def test_list_inference_recommendations_job_steps(self, client):
        resp = client.list_inference_recommendations_job_steps(JobName="nonexistent")
        assert isinstance(resp["Steps"], list)

    def test_list_labeling_jobs_for_workteam(self, client):
        arn = "arn:aws:sagemaker:us-east-1:123456789012:workteam/default/nonexistent"
        resp = client.list_labeling_jobs_for_workteam(WorkteamArn=arn)
        assert isinstance(resp["LabelingJobSummaryList"], list)

    def test_list_model_card_export_jobs(self, client):
        resp = client.list_model_card_export_jobs(ModelCardName="nonexistent")
        assert isinstance(resp["ModelCardExportJobSummaries"], list)

    def test_list_monitoring_alerts(self, client):
        resp = client.list_monitoring_alerts(MonitoringScheduleName="nonexistent")
        assert isinstance(resp["MonitoringAlertSummaries"], list)

    def test_list_monitoring_alert_history(self, client):
        resp = client.list_monitoring_alert_history(MonitoringScheduleName="nonexistent")
        assert isinstance(resp["MonitoringAlertHistory"], list)

    def test_list_monitoring_executions(self, client):
        resp = client.list_monitoring_executions(MonitoringScheduleName="nonexistent")
        assert isinstance(resp["MonitoringExecutionSummaries"], list)

    def test_list_pipeline_execution_steps(self, client):
        arn = "arn:aws:sagemaker:us-east-1:123456789012:pipeline/nonexistent/execution/abc"
        resp = client.list_pipeline_execution_steps(PipelineExecutionArn=arn)
        assert isinstance(resp["PipelineExecutionSteps"], list)

    def test_list_pipeline_versions(self, client):
        resp = client.list_pipeline_versions(PipelineName="nonexistent")
        assert isinstance(resp["PipelineVersionSummaries"], list)

    def test_list_stage_devices(self, client):
        resp = client.list_stage_devices(EdgeDeploymentPlanName="nonexistent", StageName="stage1")
        assert isinstance(resp["DeviceDeploymentSummaries"], list)

    def test_list_training_jobs_for_hpt_job(self, client):
        resp = client.list_training_jobs_for_hyper_parameter_tuning_job(
            HyperParameterTuningJobName="nonexistent"
        )
        assert isinstance(resp["TrainingJobSummaries"], list)


class TestSageMakerDescribeOpsValidationError:
    """Describe operations that return ValidationException for nonexistent resources."""

    @pytest.fixture
    def client(self):
        return make_client("sagemaker")

    def test_describe_action(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_action(ActionName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_algorithm(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_algorithm(AlgorithmName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_app(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_app(
                DomainId="nonexistent",
                UserProfileName="nonexistent",
                AppType="JupyterServer",
                AppName="nonexistent",
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_app_image_config(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_app_image_config(AppImageConfigName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_artifact(self, client):
        arn = "arn:aws:sagemaker:us-east-1:123456789012:artifact/nonexistent"
        with pytest.raises(ClientError) as exc:
            client.describe_artifact(ArtifactArn=arn)
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_code_repository(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_code_repository(CodeRepositoryName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_context(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_context(ContextName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_device_fleet(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_device_fleet(DeviceFleetName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_edge_deployment_plan(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_edge_deployment_plan(EdgeDeploymentPlanName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_flow_definition(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_flow_definition(FlowDefinitionName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_hub(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_hub(HubName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_human_task_ui(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_human_task_ui(HumanTaskUiName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_image(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_image(ImageName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_inference_component(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_inference_component(InferenceComponentName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_inference_experiment(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_inference_experiment(Name="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_labeling_job(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_labeling_job(LabelingJobName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_mlflow_tracking_server(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_mlflow_tracking_server(TrackingServerName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_mlflow_app(self, client):
        arn = "arn:aws:sagemaker:us-east-1:123456789012:mlflow-tracking-server/nonexistent"
        with pytest.raises(ClientError) as exc:
            client.describe_mlflow_app(Arn=arn)
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_monitoring_schedule(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_monitoring_schedule(MonitoringScheduleName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_optimization_job(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_optimization_job(OptimizationJobName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_partner_app(self, client):
        arn = "arn:aws:sagemaker:us-east-1:123456789012:partner-app/nonexistent"
        with pytest.raises(ClientError) as exc:
            client.describe_partner_app(Arn=arn)
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_project(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_project(ProjectName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_space(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_space(DomainId="nonexistent", SpaceName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_studio_lifecycle_config(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_studio_lifecycle_config(StudioLifecycleConfigName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_user_profile(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_user_profile(DomainId="nonexistent", UserProfileName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_workforce(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_workforce(WorkforceName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_workteam(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_workteam(WorkteamName="nonexistent")
        assert exc.value.response["Error"]["Code"] == "ValidationException"


class TestSageMakerGetOperations:
    """Get operations."""

    @pytest.fixture
    def client(self):
        return make_client("sagemaker")

    def test_get_sagemaker_servicecatalog_portfolio_status(self, client):
        resp = client.get_sagemaker_servicecatalog_portfolio_status()
        assert "Status" in resp
        assert resp["Status"] in ("Enabled", "Disabled")

    def test_get_device_fleet_report(self, client):
        resp = client.get_device_fleet_report(DeviceFleetName="nonexistent")
        assert "DeviceFleetName" in resp
        assert "DeviceFleetArn" in resp

    def test_get_lineage_group_policy(self, client):
        resp = client.get_lineage_group_policy(LineageGroupName="nonexistent")
        assert "LineageGroupArn" in resp

    def test_get_model_package_group_policy(self, client):
        resp = client.get_model_package_group_policy(ModelPackageGroupName="nonexistent")
        assert "ResourcePolicy" in resp

    def test_get_scaling_configuration_recommendation(self, client):
        resp = client.get_scaling_configuration_recommendation(
            InferenceRecommendationsJobName="nonexistent"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_search_suggestions(self, client):
        resp = client.get_search_suggestions(Resource="TrainingJob")
        assert "PropertyNameSuggestions" in resp
        assert isinstance(resp["PropertyNameSuggestions"], list)


class TestSageMakerDescribeOpsReturnData:
    """Describe operations that return data even for nonexistent resources."""

    @pytest.fixture
    def client(self):
        return make_client("sagemaker")

    def test_describe_device(self, client):
        resp = client.describe_device(DeviceName="nonexistent", DeviceFleetName="nonexistent")
        assert "DeviceName" in resp
        assert "DeviceFleetName" in resp

    def test_describe_edge_packaging_job(self, client):
        resp = client.describe_edge_packaging_job(EdgePackagingJobName="nonexistent")
        assert "EdgePackagingJobName" in resp
        assert "EdgePackagingJobStatus" in resp

    def test_describe_inference_recommendations_job(self, client):
        resp = client.describe_inference_recommendations_job(JobName="nonexistent")
        assert "JobName" in resp
        assert "Status" in resp

    def test_describe_lineage_group(self, client):
        resp = client.describe_lineage_group(LineageGroupName="nonexistent")
        assert "LineageGroupName" in resp
        assert "LineageGroupArn" in resp

    def test_describe_subscribed_workteam(self, client):
        arn = "arn:aws:sagemaker:us-east-1:123456789012:workteam/default/nonexistent"
        resp = client.describe_subscribed_workteam(WorkteamArn=arn)
        assert "SubscribedWorkteam" in resp

    def test_describe_hub_content(self, client):
        resp = client.describe_hub_content(
            HubName="nonexistent",
            HubContentName="nonexistent",
            HubContentType="Model",
        )
        assert "HubContentName" in resp
        assert "HubContentType" in resp

    def test_describe_image_version(self, client):
        resp = client.describe_image_version(ImageName="nonexistent")
        assert "ImageVersionArn" in resp
        assert "ImageVersionStatus" in resp

    def test_describe_model_card_export_job(self, client):
        arn = "arn:aws:sagemaker:us-east-1:123456789012:model-card/x/export-job/y"
        resp = client.describe_model_card_export_job(ModelCardExportJobArn=arn)
        assert "ModelCardExportJobArn" in resp
        assert "Status" in resp

    def test_describe_training_plan(self, client):
        resp = client.describe_training_plan(TrainingPlanName="nonexistent")
        assert "TrainingPlanArn" in resp
        assert "Status" in resp

    def test_describe_cluster_scheduler_config(self, client):
        resp = client.describe_cluster_scheduler_config(ClusterSchedulerConfigId="nonexistent")
        assert "ClusterSchedulerConfigId" in resp
        assert "Status" in resp

    def test_describe_compute_quota(self, client):
        resp = client.describe_compute_quota(ComputeQuotaId="nonexistent")
        assert "ComputeQuotaId" in resp
        assert "Status" in resp

    def test_describe_cluster_event(self, client):
        resp = client.describe_cluster_event(ClusterName="nonexistent", EventId="nonexistent")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_feature_metadata(self, client):
        resp = client.describe_feature_metadata(
            FeatureGroupName="nonexistent", FeatureName="nonexistent"
        )
        assert "FeatureGroupName" in resp
        assert "FeatureName" in resp

    def test_describe_reserved_capacity(self, client):
        arn = "arn:aws:sagemaker:us-east-1:123456789012:reserved-capacity/x"
        resp = client.describe_reserved_capacity(ReservedCapacityArn=arn)
        assert "ReservedCapacityArn" in resp
        assert "Status" in resp


class TestSageMakerListOpsEmptyResponse:
    """List operations that return empty responses (no list key)."""

    @pytest.fixture
    def client(self):
        return make_client("sagemaker")

    def test_list_mlflow_apps(self, client):
        resp = client.list_mlflow_apps()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_partner_apps(self, client):
        resp = client.list_partner_apps()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_cluster_events(self, client):
        resp = client.list_cluster_events(ClusterName="nonexistent")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_ultra_servers_by_reserved_capacity(self, client):
        arn = "arn:aws:sagemaker:us-east-1:123456789012:reserved-capacity/x"
        resp = client.list_ultra_servers_by_reserved_capacity(ReservedCapacityArn=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerDescribeOperations:
    """Tests for Describe operations with fake resource IDs."""

    @pytest.fixture
    def client(self):
        return make_client("sagemaker")

    def test_describe_auto_ml_job_v2_not_found(self, client):
        """DescribeAutoMLJobV2 returns ResourceNotFound for fake job."""
        with pytest.raises(ClientError) as exc:
            client.describe_auto_ml_job_v2(AutoMLJobName="fake-job-nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFound"

    def test_describe_cluster_not_found(self, client):
        """DescribeCluster returns error for fake cluster."""
        with pytest.raises(ClientError) as exc:
            client.describe_cluster(ClusterName="fake-cluster-nonexistent")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    def test_describe_cluster_node_not_found(self, client):
        """DescribeClusterNode returns error for fake cluster/node."""
        with pytest.raises(ClientError) as exc:
            client.describe_cluster_node(
                ClusterName="fake-cluster-nonexistent",
                NodeId="fake-node-nonexistent",
            )
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    def test_describe_compilation_job_not_found(self, client):
        """DescribeCompilationJob returns ResourceNotFound for fake job."""
        with pytest.raises(ClientError) as exc:
            client.describe_compilation_job(CompilationJobName="fake-job-nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFound"

    def test_describe_data_quality_job_definition_not_found(self, client):
        """DescribeDataQualityJobDefinition returns ResourceNotFound."""
        with pytest.raises(ClientError) as exc:
            client.describe_data_quality_job_definition(JobDefinitionName="fake-jobdef-nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFound"

    def test_describe_domain_not_found(self, client):
        """DescribeDomain returns error for fake domain."""
        with pytest.raises(ClientError) as exc:
            client.describe_domain(DomainId="fake-domain-nonexistent")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    def test_describe_endpoint_not_found(self, client):
        """DescribeEndpoint returns error for fake endpoint."""
        with pytest.raises(ClientError) as exc:
            client.describe_endpoint(EndpointName="fake-endpoint-nonexistent")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    def test_describe_endpoint_config_not_found(self, client):
        """DescribeEndpointConfig returns error for fake config."""
        with pytest.raises(ClientError) as exc:
            client.describe_endpoint_config(EndpointConfigName="fake-config-nonexistent")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    def test_describe_hyper_parameter_tuning_job_not_found(self, client):
        """DescribeHyperParameterTuningJob returns ResourceNotFound."""
        with pytest.raises(ClientError) as exc:
            client.describe_hyper_parameter_tuning_job(
                HyperParameterTuningJobName="fake-job-nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFound"

    def test_describe_model_bias_job_definition_not_found(self, client):
        """DescribeModelBiasJobDefinition returns ResourceNotFound."""
        with pytest.raises(ClientError) as exc:
            client.describe_model_bias_job_definition(JobDefinitionName="fake-jobdef-nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFound"

    def test_describe_model_card_not_found(self, client):
        """DescribeModelCard returns ResourceNotFound for fake card."""
        with pytest.raises(ClientError) as exc:
            client.describe_model_card(ModelCardName="fake-card-nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFound"

    def test_describe_model_explainability_job_definition_not_found(self, client):
        """DescribeModelExplainabilityJobDefinition returns ResourceNotFound."""
        with pytest.raises(ClientError) as exc:
            client.describe_model_explainability_job_definition(
                JobDefinitionName="fake-jobdef-nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFound"

    def test_describe_model_package_not_found(self, client):
        """DescribeModelPackage returns error for fake package."""
        with pytest.raises(ClientError) as exc:
            client.describe_model_package(ModelPackageName="fake-pkg-nonexistent")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    def test_describe_model_package_group_not_found(self, client):
        """DescribeModelPackageGroup returns error for fake group."""
        with pytest.raises(ClientError) as exc:
            client.describe_model_package_group(ModelPackageGroupName="fake-group-nonexistent")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    def test_describe_model_quality_job_definition_not_found(self, client):
        """DescribeModelQualityJobDefinition returns ResourceNotFound."""
        with pytest.raises(ClientError) as exc:
            client.describe_model_quality_job_definition(
                JobDefinitionName="fake-jobdef-nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFound"


class TestSageMakerEndpointConfigCRUD:
    """EndpointConfig CRUD tests."""

    def _create_model(self, sagemaker, name):
        sagemaker.create_model(
            ModelName=name,
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            PrimaryContainer={
                "Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
            },
        )

    def test_create_and_describe_endpoint_config(self, sagemaker):
        model_name = _uid("model")
        ec_name = _uid("ec")
        self._create_model(sagemaker, model_name)
        try:
            resp = sagemaker.create_endpoint_config(
                EndpointConfigName=ec_name,
                ProductionVariants=[
                    {
                        "VariantName": "v1",
                        "ModelName": model_name,
                        "InitialInstanceCount": 1,
                        "InstanceType": "ml.m4.xlarge",
                    }
                ],
            )
            assert "EndpointConfigArn" in resp
            desc = sagemaker.describe_endpoint_config(EndpointConfigName=ec_name)
            assert desc["EndpointConfigName"] == ec_name
            assert len(desc["ProductionVariants"]) == 1
            assert desc["ProductionVariants"][0]["ModelName"] == model_name
        finally:
            sagemaker.delete_endpoint_config(EndpointConfigName=ec_name)
            sagemaker.delete_model(ModelName=model_name)

    def test_list_endpoint_configs_after_create(self, sagemaker):
        model_name = _uid("model")
        ec_name = _uid("ec")
        self._create_model(sagemaker, model_name)
        try:
            sagemaker.create_endpoint_config(
                EndpointConfigName=ec_name,
                ProductionVariants=[
                    {
                        "VariantName": "v1",
                        "ModelName": model_name,
                        "InitialInstanceCount": 1,
                        "InstanceType": "ml.m4.xlarge",
                    }
                ],
            )
            resp = sagemaker.list_endpoint_configs()
            names = [c["EndpointConfigName"] for c in resp["EndpointConfigs"]]
            assert ec_name in names
        finally:
            sagemaker.delete_endpoint_config(EndpointConfigName=ec_name)
            sagemaker.delete_model(ModelName=model_name)

    def test_delete_endpoint_config(self, sagemaker):
        model_name = _uid("model")
        ec_name = _uid("ec")
        self._create_model(sagemaker, model_name)
        try:
            sagemaker.create_endpoint_config(
                EndpointConfigName=ec_name,
                ProductionVariants=[
                    {
                        "VariantName": "v1",
                        "ModelName": model_name,
                        "InitialInstanceCount": 1,
                        "InstanceType": "ml.m4.xlarge",
                    }
                ],
            )
            sagemaker.delete_endpoint_config(EndpointConfigName=ec_name)
            resp = sagemaker.list_endpoint_configs()
            names = [c["EndpointConfigName"] for c in resp["EndpointConfigs"]]
            assert ec_name not in names
        finally:
            sagemaker.delete_model(ModelName=model_name)


class TestSageMakerEndpointCRUD:
    """Endpoint CRUD tests (requires model + endpoint config)."""

    def _setup_model_and_config(self, sagemaker, uid):
        model_name = f"model-{uid}"
        ec_name = f"ec-{uid}"
        sagemaker.create_model(
            ModelName=model_name,
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            PrimaryContainer={
                "Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
            },
        )
        sagemaker.create_endpoint_config(
            EndpointConfigName=ec_name,
            ProductionVariants=[
                {
                    "VariantName": "v1",
                    "ModelName": model_name,
                    "InitialInstanceCount": 1,
                    "InstanceType": "ml.m4.xlarge",
                }
            ],
        )
        return model_name, ec_name

    def _cleanup(self, sagemaker, ep_name, ec_name, model_name):
        try:
            sagemaker.delete_endpoint(EndpointName=ep_name)
        except Exception:
            pass
        sagemaker.delete_endpoint_config(EndpointConfigName=ec_name)
        sagemaker.delete_model(ModelName=model_name)

    def test_create_and_describe_endpoint(self, sagemaker):
        uid = uuid.uuid4().hex[:8]
        model_name, ec_name = self._setup_model_and_config(sagemaker, uid)
        ep_name = f"ep-{uid}"
        try:
            resp = sagemaker.create_endpoint(EndpointName=ep_name, EndpointConfigName=ec_name)
            assert "EndpointArn" in resp
            desc = sagemaker.describe_endpoint(EndpointName=ep_name)
            assert desc["EndpointName"] == ep_name
            assert desc["EndpointConfigName"] == ec_name
            assert "EndpointStatus" in desc
        finally:
            self._cleanup(sagemaker, ep_name, ec_name, model_name)

    def test_list_endpoints_after_create(self, sagemaker):
        uid = uuid.uuid4().hex[:8]
        model_name, ec_name = self._setup_model_and_config(sagemaker, uid)
        ep_name = f"ep-{uid}"
        try:
            sagemaker.create_endpoint(EndpointName=ep_name, EndpointConfigName=ec_name)
            resp = sagemaker.list_endpoints()
            names = [e["EndpointName"] for e in resp["Endpoints"]]
            assert ep_name in names
        finally:
            self._cleanup(sagemaker, ep_name, ec_name, model_name)

    def test_delete_endpoint(self, sagemaker):
        uid = uuid.uuid4().hex[:8]
        model_name, ec_name = self._setup_model_and_config(sagemaker, uid)
        ep_name = f"ep-{uid}"
        try:
            sagemaker.create_endpoint(EndpointName=ep_name, EndpointConfigName=ec_name)
            sagemaker.delete_endpoint(EndpointName=ep_name)
            resp = sagemaker.list_endpoints()
            names = [e["EndpointName"] for e in resp["Endpoints"]]
            assert ep_name not in names
        finally:
            sagemaker.delete_endpoint_config(EndpointConfigName=ec_name)
            sagemaker.delete_model(ModelName=model_name)


class TestSageMakerTransformJobCRUD:
    """TransformJob CRUD tests."""

    def test_create_and_describe_transform_job(self, sagemaker):
        name = _uid("tj")
        resp = sagemaker.create_transform_job(
            TransformJobName=name,
            ModelName="fake-model",
            TransformInput={
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": "s3://bucket/input",
                    }
                },
                "ContentType": "text/csv",
            },
            TransformOutput={"S3OutputPath": "s3://bucket/output"},
            TransformResources={"InstanceType": "ml.m4.xlarge", "InstanceCount": 1},
        )
        assert "TransformJobArn" in resp
        desc = sagemaker.describe_transform_job(TransformJobName=name)
        assert desc["TransformJobName"] == name
        assert desc["TransformJobStatus"] in ("InProgress", "Completed", "Failed", "Stopping")

    def test_list_transform_jobs_after_create(self, sagemaker):
        name = _uid("tj")
        sagemaker.create_transform_job(
            TransformJobName=name,
            ModelName="fake-model",
            TransformInput={
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": "s3://bucket/input",
                    }
                },
                "ContentType": "text/csv",
            },
            TransformOutput={"S3OutputPath": "s3://bucket/output"},
            TransformResources={"InstanceType": "ml.m4.xlarge", "InstanceCount": 1},
        )
        resp = sagemaker.list_transform_jobs()
        names = [j["TransformJobName"] for j in resp["TransformJobSummaries"]]
        assert name in names


class TestSageMakerHyperParameterTuningJobCRUD:
    """HyperParameterTuningJob CRUD tests."""

    def _create_hpt_job(self, sagemaker, name):
        return sagemaker.create_hyper_parameter_tuning_job(
            HyperParameterTuningJobName=name,
            HyperParameterTuningJobConfig={
                "Strategy": "Bayesian",
                "ResourceLimits": {
                    "MaxNumberOfTrainingJobs": 10,
                    "MaxParallelTrainingJobs": 2,
                },
            },
            TrainingJobDefinition={
                "RoleArn": "arn:aws:iam::123456789012:role/SageMakerRole",
                "AlgorithmSpecification": {
                    "TrainingImage": "123456789012.dkr.ecr.us-east-1.amazonaws.com/img:latest",
                    "TrainingInputMode": "File",
                },
                "OutputDataConfig": {"S3OutputPath": "s3://bucket/out"},
                "ResourceConfig": {
                    "InstanceType": "ml.m4.xlarge",
                    "InstanceCount": 1,
                    "VolumeSizeInGB": 10,
                },
                "StoppingCondition": {"MaxRuntimeInSeconds": 3600},
            },
        )

    def test_create_and_describe_hpt_job(self, sagemaker):
        name = _uid("hpt")
        resp = self._create_hpt_job(sagemaker, name)
        assert "HyperParameterTuningJobArn" in resp
        desc = sagemaker.describe_hyper_parameter_tuning_job(HyperParameterTuningJobName=name)
        assert desc["HyperParameterTuningJobName"] == name
        assert desc["HyperParameterTuningJobStatus"] in (
            "InProgress",
            "Completed",
            "Failed",
            "Stopping",
        )

    def test_list_hpt_jobs_after_create(self, sagemaker):
        name = _uid("hpt")
        self._create_hpt_job(sagemaker, name)
        resp = sagemaker.list_hyper_parameter_tuning_jobs()
        names = [j["HyperParameterTuningJobName"] for j in resp["HyperParameterTuningJobSummaries"]]
        assert name in names


class TestSageMakerProcessingJobCRUD:
    """ProcessingJob CRUD tests."""

    def _create_processing_job(self, sagemaker, name):
        return sagemaker.create_processing_job(
            ProcessingJobName=name,
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            ProcessingResources={
                "ClusterConfig": {
                    "InstanceCount": 1,
                    "InstanceType": "ml.m4.xlarge",
                    "VolumeSizeInGB": 10,
                }
            },
            AppSpecification={
                "ImageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/img:latest"
            },
        )

    def test_create_and_describe_processing_job(self, sagemaker):
        name = _uid("pj")
        resp = self._create_processing_job(sagemaker, name)
        assert "ProcessingJobArn" in resp
        desc = sagemaker.describe_processing_job(ProcessingJobName=name)
        assert desc["ProcessingJobName"] == name
        assert desc["ProcessingJobStatus"] in (
            "InProgress",
            "Completed",
            "Failed",
            "Stopping",
        )

    def test_list_processing_jobs_after_create(self, sagemaker):
        name = _uid("pj")
        self._create_processing_job(sagemaker, name)
        resp = sagemaker.list_processing_jobs()
        names = [j["ProcessingJobName"] for j in resp["ProcessingJobSummaries"]]
        assert name in names


class TestSageMakerCompilationJobCRUD:
    """CompilationJob CRUD tests."""

    def _create_compilation_job(self, sagemaker, name):
        return sagemaker.create_compilation_job(
            CompilationJobName=name,
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            InputConfig={
                "S3Uri": "s3://bucket/model.tar.gz",
                "DataInputConfig": '{"input":[1,3,224,224]}',
                "Framework": "PYTORCH",
            },
            OutputConfig={
                "S3OutputLocation": "s3://bucket/out",
                "TargetDevice": "ml_m4",
            },
            StoppingCondition={"MaxRuntimeInSeconds": 900},
        )

    def test_create_and_describe_compilation_job(self, sagemaker):
        name = _uid("cj")
        resp = self._create_compilation_job(sagemaker, name)
        assert "CompilationJobArn" in resp
        desc = sagemaker.describe_compilation_job(CompilationJobName=name)
        assert desc["CompilationJobName"] == name
        assert desc["CompilationJobStatus"] in (
            "INPROGRESS",
            "COMPLETED",
            "FAILED",
            "STARTING",
            "STOPPING",
            "STOPPED",
        )

    def test_list_compilation_jobs_after_create(self, sagemaker):
        name = _uid("cj")
        self._create_compilation_job(sagemaker, name)
        resp = sagemaker.list_compilation_jobs()
        names = [j["CompilationJobName"] for j in resp["CompilationJobSummaries"]]
        assert name in names


class TestSageMakerModelPackageGroupCRUD:
    """ModelPackageGroup CRUD tests (delete not implemented)."""

    def test_create_and_describe_model_package_group(self, sagemaker):
        name = _uid("mpg")
        resp = sagemaker.create_model_package_group(
            ModelPackageGroupName=name,
            ModelPackageGroupDescription="test group",
        )
        assert "ModelPackageGroupArn" in resp
        desc = sagemaker.describe_model_package_group(ModelPackageGroupName=name)
        assert desc["ModelPackageGroupName"] == name

    def test_list_model_package_groups_after_create(self, sagemaker):
        name = _uid("mpg")
        sagemaker.create_model_package_group(
            ModelPackageGroupName=name,
            ModelPackageGroupDescription="test group",
        )
        resp = sagemaker.list_model_package_groups()
        names = [g["ModelPackageGroupName"] for g in resp["ModelPackageGroupSummaryList"]]
        assert name in names


class TestSageMakerModelPackageCRUD:
    """ModelPackage CRUD tests (delete not implemented)."""

    def test_create_and_describe_model_package(self, sagemaker):
        name = _uid("mp")
        resp = sagemaker.create_model_package(
            ModelPackageName=name,
            ModelPackageDescription="test package",
        )
        assert "ModelPackageArn" in resp
        desc = sagemaker.describe_model_package(ModelPackageName=name)
        assert desc["ModelPackageName"] == name
        assert "ModelPackageStatus" in desc

    def test_list_model_packages_after_create(self, sagemaker):
        name = _uid("mp")
        sagemaker.create_model_package(
            ModelPackageName=name,
            ModelPackageDescription="test package",
        )
        resp = sagemaker.list_model_packages()
        arns = [p["ModelPackageArn"] for p in resp["ModelPackageSummaryList"]]
        assert any(name in arn for arn in arns)

    def test_create_versioned_model_package(self, sagemaker):
        group_name = _uid("mpg")
        sagemaker.create_model_package_group(
            ModelPackageGroupName=group_name,
            ModelPackageGroupDescription="test",
        )
        resp = sagemaker.create_model_package(
            ModelPackageGroupName=group_name,
            ModelPackageDescription="versioned pkg",
        )
        assert "ModelPackageArn" in resp
        assert group_name in resp["ModelPackageArn"]


class TestSageMakerModelCardCRUD:
    """ModelCard CRUD tests."""

    def test_create_and_describe_model_card(self, sagemaker):
        name = _uid("mc")
        resp = sagemaker.create_model_card(
            ModelCardName=name,
            Content='{"model_overview":{}}',
            ModelCardStatus="Draft",
        )
        assert "ModelCardArn" in resp
        try:
            desc = sagemaker.describe_model_card(ModelCardName=name)
            assert desc["ModelCardName"] == name
            assert desc["ModelCardStatus"] == "Draft"
        finally:
            sagemaker.delete_model_card(ModelCardName=name)

    def test_list_model_cards_after_create(self, sagemaker):
        name = _uid("mc")
        sagemaker.create_model_card(
            ModelCardName=name,
            Content='{"model_overview":{}}',
            ModelCardStatus="Draft",
        )
        try:
            resp = sagemaker.list_model_cards()
            names = [c["ModelCardName"] for c in resp["ModelCardSummaries"]]
            assert name in names
        finally:
            sagemaker.delete_model_card(ModelCardName=name)

    def test_delete_model_card(self, sagemaker):
        name = _uid("mc")
        sagemaker.create_model_card(
            ModelCardName=name,
            Content='{"model_overview":{}}',
            ModelCardStatus="Draft",
        )
        sagemaker.delete_model_card(ModelCardName=name)
        resp = sagemaker.list_model_cards()
        names = [c["ModelCardName"] for c in resp["ModelCardSummaries"]]
        assert name not in names


class TestSageMakerDomainCRUD:
    """Domain CRUD tests."""

    def test_create_and_describe_domain(self, sagemaker):
        name = _uid("dom")
        resp = sagemaker.create_domain(
            DomainName=name,
            AuthMode="IAM",
            DefaultUserSettings={"ExecutionRole": "arn:aws:iam::123456789012:role/SageMakerRole"},
            SubnetIds=["subnet-12345"],
            VpcId="vpc-12345",
        )
        assert "DomainArn" in resp
        try:
            # Extract domain ID from ARN
            domain_id = resp["DomainArn"].split("/")[-1]
            desc = sagemaker.describe_domain(DomainId=domain_id)
            assert desc["DomainName"] == name
            assert "DomainId" in desc
        finally:
            sagemaker.delete_domain(DomainId=domain_id)

    def test_list_domains_after_create(self, sagemaker):
        name = _uid("dom")
        resp = sagemaker.create_domain(
            DomainName=name,
            AuthMode="IAM",
            DefaultUserSettings={"ExecutionRole": "arn:aws:iam::123456789012:role/SageMakerRole"},
            SubnetIds=["subnet-12345"],
            VpcId="vpc-12345",
        )
        domain_id = resp["DomainArn"].split("/")[-1]
        try:
            resp = sagemaker.list_domains()
            domain_names = [d["DomainName"] for d in resp["Domains"]]
            assert name in domain_names
        finally:
            sagemaker.delete_domain(DomainId=domain_id)

    def test_delete_domain(self, sagemaker):
        name = _uid("dom")
        resp = sagemaker.create_domain(
            DomainName=name,
            AuthMode="IAM",
            DefaultUserSettings={"ExecutionRole": "arn:aws:iam::123456789012:role/SageMakerRole"},
            SubnetIds=["subnet-12345"],
            VpcId="vpc-12345",
        )
        domain_id = resp["DomainArn"].split("/")[-1]
        sagemaker.delete_domain(DomainId=domain_id)
        resp = sagemaker.list_domains()
        ids = [d.get("DomainId") for d in resp["Domains"]]
        assert domain_id not in ids


class TestSageMakerTrialCRUD:
    """Trial CRUD tests (requires experiment)."""

    def test_create_and_describe_trial(self, sagemaker):
        exp_name = _uid("exp")
        trial_name = _uid("trial")
        sagemaker.create_experiment(ExperimentName=exp_name)
        try:
            resp = sagemaker.create_trial(TrialName=trial_name, ExperimentName=exp_name)
            assert "TrialArn" in resp
            desc = sagemaker.describe_trial(TrialName=trial_name)
            assert desc["TrialName"] == trial_name
            assert desc["ExperimentName"] == exp_name
        finally:
            sagemaker.delete_trial(TrialName=trial_name)
            sagemaker.delete_experiment(ExperimentName=exp_name)

    def test_list_trials_after_create(self, sagemaker):
        exp_name = _uid("exp")
        trial_name = _uid("trial")
        sagemaker.create_experiment(ExperimentName=exp_name)
        try:
            sagemaker.create_trial(TrialName=trial_name, ExperimentName=exp_name)
            resp = sagemaker.list_trials()
            names = [t["TrialName"] for t in resp["TrialSummaries"]]
            assert trial_name in names
        finally:
            sagemaker.delete_trial(TrialName=trial_name)
            sagemaker.delete_experiment(ExperimentName=exp_name)

    def test_delete_trial(self, sagemaker):
        exp_name = _uid("exp")
        trial_name = _uid("trial")
        sagemaker.create_experiment(ExperimentName=exp_name)
        try:
            sagemaker.create_trial(TrialName=trial_name, ExperimentName=exp_name)
            sagemaker.delete_trial(TrialName=trial_name)
            resp = sagemaker.list_trials()
            names = [t["TrialName"] for t in resp["TrialSummaries"]]
            assert trial_name not in names
        finally:
            sagemaker.delete_experiment(ExperimentName=exp_name)


class TestSageMakerTagsCRUD:
    """Tags CRUD tests."""

    def test_add_and_list_tags(self, sagemaker):
        name = _uid("model")
        sagemaker.create_model(
            ModelName=name,
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            PrimaryContainer={
                "Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
            },
        )
        arn = f"arn:aws:sagemaker:us-east-1:123456789012:model/{name}"
        try:
            resp = sagemaker.add_tags(
                ResourceArn=arn,
                Tags=[
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "ml"},
                ],
            )
            assert "Tags" in resp
            tags_resp = sagemaker.list_tags(ResourceArn=arn)
            tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tags["env"] == "test"
            assert tags["team"] == "ml"
        finally:
            sagemaker.delete_model(ModelName=name)

    def test_delete_tags(self, sagemaker):
        name = _uid("model")
        sagemaker.create_model(
            ModelName=name,
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            PrimaryContainer={
                "Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
            },
        )
        arn = f"arn:aws:sagemaker:us-east-1:123456789012:model/{name}"
        try:
            sagemaker.add_tags(
                ResourceArn=arn,
                Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "ml"}],
            )
            sagemaker.delete_tags(ResourceArn=arn, TagKeys=["env"])
            tags_resp = sagemaker.list_tags(ResourceArn=arn)
            keys = [t["Key"] for t in tags_resp["Tags"]]
            assert "env" not in keys
            assert "team" in keys
        finally:
            sagemaker.delete_model(ModelName=name)

    def test_add_tags_to_experiment(self, sagemaker):
        name = _uid("exp")
        resp = sagemaker.create_experiment(ExperimentName=name)
        arn = resp["ExperimentArn"]
        try:
            sagemaker.add_tags(ResourceArn=arn, Tags=[{"Key": "purpose", "Value": "testing"}])
            tags_resp = sagemaker.list_tags(ResourceArn=arn)
            tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tags["purpose"] == "testing"
        finally:
            sagemaker.delete_experiment(ExperimentName=name)


class TestSageMakerNotebookInstanceExtended:
    """Extended NotebookInstance tests for stop and delete lifecycle."""

    def test_stop_notebook_instance(self, sagemaker):
        name = _uid("nb")
        sagemaker.create_notebook_instance(
            NotebookInstanceName=name,
            InstanceType="ml.t2.medium",
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        try:
            sagemaker.stop_notebook_instance(NotebookInstanceName=name)
            desc = sagemaker.describe_notebook_instance(NotebookInstanceName=name)
            assert desc["NotebookInstanceStatus"] == "Stopped"
        finally:
            sagemaker.delete_notebook_instance(NotebookInstanceName=name)

    def test_delete_notebook_instance(self, sagemaker):
        name = _uid("nb")
        sagemaker.create_notebook_instance(
            NotebookInstanceName=name,
            InstanceType="ml.t2.medium",
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        sagemaker.stop_notebook_instance(NotebookInstanceName=name)
        sagemaker.delete_notebook_instance(NotebookInstanceName=name)
        resp = sagemaker.list_notebook_instances()
        names = [n["NotebookInstanceName"] for n in resp["NotebookInstances"]]
        assert name not in names

    def test_start_notebook_instance(self, sagemaker):
        name = _uid("nb")
        sagemaker.create_notebook_instance(
            NotebookInstanceName=name,
            InstanceType="ml.t2.medium",
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        try:
            sagemaker.stop_notebook_instance(NotebookInstanceName=name)
            sagemaker.start_notebook_instance(NotebookInstanceName=name)
            desc = sagemaker.describe_notebook_instance(NotebookInstanceName=name)
            assert desc["NotebookInstanceStatus"] == "InService"
        finally:
            sagemaker.stop_notebook_instance(NotebookInstanceName=name)
            sagemaker.delete_notebook_instance(NotebookInstanceName=name)


class TestSageMakerPipelineCRUD:
    """Pipeline CRUD tests."""

    def _create_pipeline(self, sagemaker, name):
        return sagemaker.create_pipeline(
            PipelineName=name,
            PipelineDefinition='{"Version":"2020-12-01","Steps":[]}',
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )

    def test_create_and_describe_pipeline(self, sagemaker):
        name = _uid("pipe")
        resp = self._create_pipeline(sagemaker, name)
        assert "PipelineArn" in resp
        try:
            desc = sagemaker.describe_pipeline(PipelineName=name)
            assert desc["PipelineName"] == name
            assert "PipelineArn" in desc
        finally:
            sagemaker.delete_pipeline(PipelineName=name)

    def test_list_pipelines_after_create(self, sagemaker):
        name = _uid("pipe")
        self._create_pipeline(sagemaker, name)
        try:
            resp = sagemaker.list_pipelines()
            names = [p["PipelineName"] for p in resp["PipelineSummaries"]]
            assert name in names
        finally:
            sagemaker.delete_pipeline(PipelineName=name)

    def test_delete_pipeline(self, sagemaker):
        name = _uid("pipe")
        self._create_pipeline(sagemaker, name)
        sagemaker.delete_pipeline(PipelineName=name)
        resp = sagemaker.list_pipelines()
        names = [p["PipelineName"] for p in resp["PipelineSummaries"]]
        assert name not in names

    def test_update_pipeline(self, sagemaker):
        name = _uid("pipe")
        self._create_pipeline(sagemaker, name)
        try:
            resp = sagemaker.update_pipeline(PipelineName=name, PipelineDescription="updated desc")
            assert "PipelineArn" in resp
            desc = sagemaker.describe_pipeline(PipelineName=name)
            assert desc["PipelineDescription"] == "updated desc"
        finally:
            sagemaker.delete_pipeline(PipelineName=name)

    def test_describe_pipeline_not_found(self, sagemaker):
        with pytest.raises(ClientError) as exc:
            sagemaker.describe_pipeline(PipelineName="fake-pipeline-nonexistent")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


class TestSageMakerPipelineExecution:
    """Pipeline execution tests."""

    def _create_pipeline(self, sagemaker, name):
        sagemaker.create_pipeline(
            PipelineName=name,
            PipelineDefinition='{"Version":"2020-12-01","Steps":[]}',
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )

    def test_start_pipeline_execution(self, sagemaker):
        name = _uid("pipe")
        self._create_pipeline(sagemaker, name)
        try:
            resp = sagemaker.start_pipeline_execution(PipelineName=name)
            assert "PipelineExecutionArn" in resp
        finally:
            sagemaker.delete_pipeline(PipelineName=name)

    def test_describe_pipeline_execution(self, sagemaker):
        name = _uid("pipe")
        self._create_pipeline(sagemaker, name)
        try:
            exec_resp = sagemaker.start_pipeline_execution(PipelineName=name)
            exec_arn = exec_resp["PipelineExecutionArn"]
            desc = sagemaker.describe_pipeline_execution(PipelineExecutionArn=exec_arn)
            assert "PipelineExecutionStatus" in desc
            assert desc["PipelineExecutionArn"] == exec_arn
        finally:
            sagemaker.delete_pipeline(PipelineName=name)

    def test_list_pipeline_executions(self, sagemaker):
        name = _uid("pipe")
        self._create_pipeline(sagemaker, name)
        try:
            sagemaker.start_pipeline_execution(PipelineName=name)
            resp = sagemaker.list_pipeline_executions(PipelineName=name)
            assert "PipelineExecutionSummaries" in resp
            assert len(resp["PipelineExecutionSummaries"]) >= 1
        finally:
            sagemaker.delete_pipeline(PipelineName=name)

    def test_describe_pipeline_definition_for_execution(self, sagemaker):
        name = _uid("pipe")
        self._create_pipeline(sagemaker, name)
        try:
            exec_resp = sagemaker.start_pipeline_execution(PipelineName=name)
            exec_arn = exec_resp["PipelineExecutionArn"]
            desc = sagemaker.describe_pipeline_definition_for_execution(
                PipelineExecutionArn=exec_arn
            )
            assert "PipelineDefinition" in desc
        finally:
            sagemaker.delete_pipeline(PipelineName=name)

    def test_list_pipeline_parameters_for_execution(self, sagemaker):
        name = _uid("pipe")
        self._create_pipeline(sagemaker, name)
        try:
            exec_resp = sagemaker.start_pipeline_execution(PipelineName=name)
            exec_arn = exec_resp["PipelineExecutionArn"]
            resp = sagemaker.list_pipeline_parameters_for_execution(PipelineExecutionArn=exec_arn)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            sagemaker.delete_pipeline(PipelineName=name)

    def test_describe_pipeline_execution_not_found(self, sagemaker):
        with pytest.raises(ClientError) as exc:
            sagemaker.describe_pipeline_execution(
                PipelineExecutionArn="arn:aws:sagemaker:us-east-1:123456789012:pipeline/fake/execution/fake"
            )
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


class TestSageMakerDataQualityJobDefinitionCRUD:
    """DataQualityJobDefinition CRUD tests."""

    def _create_dq_job_def(self, sagemaker, name):
        return sagemaker.create_data_quality_job_definition(
            JobDefinitionName=name,
            DataQualityAppSpecification={
                "ImageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/img:latest"
            },
            DataQualityJobInput={
                "EndpointInput": {
                    "EndpointName": "fake-ep",
                    "LocalPath": "/opt/ml/input",
                }
            },
            DataQualityJobOutputConfig={
                "MonitoringOutputs": [
                    {
                        "S3Output": {
                            "S3Uri": "s3://bucket/out",
                            "LocalPath": "/opt/ml/output",
                            "S3UploadMode": "EndOfJob",
                        }
                    }
                ]
            },
            JobResources={
                "ClusterConfig": {
                    "InstanceCount": 1,
                    "InstanceType": "ml.m4.xlarge",
                    "VolumeSizeInGB": 10,
                }
            },
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )

    def test_create_and_describe_dq_job_definition(self, sagemaker):
        name = _uid("dq")
        resp = self._create_dq_job_def(sagemaker, name)
        assert "JobDefinitionArn" in resp
        try:
            desc = sagemaker.describe_data_quality_job_definition(JobDefinitionName=name)
            assert desc["JobDefinitionName"] == name
            assert "JobDefinitionArn" in desc
        finally:
            sagemaker.delete_data_quality_job_definition(JobDefinitionName=name)

    def test_list_dq_job_definitions_after_create(self, sagemaker):
        name = _uid("dq")
        self._create_dq_job_def(sagemaker, name)
        try:
            resp = sagemaker.list_data_quality_job_definitions()
            names = [j["MonitoringJobDefinitionName"] for j in resp["JobDefinitionSummaries"]]
            assert name in names
        finally:
            sagemaker.delete_data_quality_job_definition(JobDefinitionName=name)

    def test_delete_dq_job_definition(self, sagemaker):
        name = _uid("dq")
        self._create_dq_job_def(sagemaker, name)
        sagemaker.delete_data_quality_job_definition(JobDefinitionName=name)
        resp = sagemaker.list_data_quality_job_definitions()
        names = [j["MonitoringJobDefinitionName"] for j in resp["JobDefinitionSummaries"]]
        assert name not in names


class TestSageMakerModelQualityJobDefinitionCRUD:
    """ModelQualityJobDefinition CRUD tests."""

    def _create_mq_job_def(self, sagemaker, name):
        return sagemaker.create_model_quality_job_definition(
            JobDefinitionName=name,
            ModelQualityAppSpecification={
                "ImageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/img:latest",
                "ProblemType": "BinaryClassification",
            },
            ModelQualityJobInput={
                "EndpointInput": {
                    "EndpointName": "fake-ep",
                    "LocalPath": "/opt/ml/input",
                },
                "GroundTruthS3Input": {"S3Uri": "s3://bucket/gt"},
            },
            ModelQualityJobOutputConfig={
                "MonitoringOutputs": [
                    {
                        "S3Output": {
                            "S3Uri": "s3://bucket/out",
                            "LocalPath": "/opt/ml/output",
                            "S3UploadMode": "EndOfJob",
                        }
                    }
                ]
            },
            JobResources={
                "ClusterConfig": {
                    "InstanceCount": 1,
                    "InstanceType": "ml.m4.xlarge",
                    "VolumeSizeInGB": 10,
                }
            },
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )

    def test_create_and_describe_mq_job_definition(self, sagemaker):
        name = _uid("mq")
        resp = self._create_mq_job_def(sagemaker, name)
        assert "JobDefinitionArn" in resp
        try:
            desc = sagemaker.describe_model_quality_job_definition(JobDefinitionName=name)
            assert desc["JobDefinitionName"] == name
        finally:
            sagemaker.delete_model_quality_job_definition(JobDefinitionName=name)

    def test_list_mq_job_definitions_after_create(self, sagemaker):
        name = _uid("mq")
        self._create_mq_job_def(sagemaker, name)
        try:
            resp = sagemaker.list_model_quality_job_definitions()
            names = [j["MonitoringJobDefinitionName"] for j in resp["JobDefinitionSummaries"]]
            assert name in names
        finally:
            sagemaker.delete_model_quality_job_definition(JobDefinitionName=name)

    def test_delete_mq_job_definition(self, sagemaker):
        name = _uid("mq")
        self._create_mq_job_def(sagemaker, name)
        sagemaker.delete_model_quality_job_definition(JobDefinitionName=name)
        resp = sagemaker.list_model_quality_job_definitions()
        names = [j["MonitoringJobDefinitionName"] for j in resp["JobDefinitionSummaries"]]
        assert name not in names


class TestSageMakerModelExplainabilityJobDefinitionCRUD:
    """ModelExplainabilityJobDefinition CRUD tests."""

    def _create_me_job_def(self, sagemaker, name):
        return sagemaker.create_model_explainability_job_definition(
            JobDefinitionName=name,
            ModelExplainabilityAppSpecification={
                "ImageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/img:latest",
                "ConfigUri": "s3://bucket/config",
            },
            ModelExplainabilityJobInput={
                "EndpointInput": {
                    "EndpointName": "fake-ep",
                    "LocalPath": "/opt/ml/input",
                }
            },
            ModelExplainabilityJobOutputConfig={
                "MonitoringOutputs": [
                    {
                        "S3Output": {
                            "S3Uri": "s3://bucket/out",
                            "LocalPath": "/opt/ml/output",
                            "S3UploadMode": "EndOfJob",
                        }
                    }
                ]
            },
            JobResources={
                "ClusterConfig": {
                    "InstanceCount": 1,
                    "InstanceType": "ml.m4.xlarge",
                    "VolumeSizeInGB": 10,
                }
            },
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )

    def test_create_and_describe_me_job_definition(self, sagemaker):
        name = _uid("me")
        resp = self._create_me_job_def(sagemaker, name)
        assert "JobDefinitionArn" in resp
        try:
            desc = sagemaker.describe_model_explainability_job_definition(JobDefinitionName=name)
            assert desc["JobDefinitionName"] == name
        finally:
            sagemaker.delete_model_explainability_job_definition(JobDefinitionName=name)

    def test_list_me_job_definitions_after_create(self, sagemaker):
        name = _uid("me")
        self._create_me_job_def(sagemaker, name)
        try:
            resp = sagemaker.list_model_explainability_job_definitions()
            names = [j["MonitoringJobDefinitionName"] for j in resp["JobDefinitionSummaries"]]
            assert name in names
        finally:
            sagemaker.delete_model_explainability_job_definition(JobDefinitionName=name)

    def test_delete_me_job_definition(self, sagemaker):
        name = _uid("me")
        self._create_me_job_def(sagemaker, name)
        sagemaker.delete_model_explainability_job_definition(JobDefinitionName=name)
        resp = sagemaker.list_model_explainability_job_definitions()
        names = [j["MonitoringJobDefinitionName"] for j in resp["JobDefinitionSummaries"]]
        assert name not in names


class TestSageMakerModelBiasJobDefinitionCRUD:
    """ModelBiasJobDefinition CRUD tests."""

    def _create_mb_job_def(self, sagemaker, name):
        return sagemaker.create_model_bias_job_definition(
            JobDefinitionName=name,
            ModelBiasAppSpecification={
                "ImageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/img:latest",
                "ConfigUri": "s3://bucket/config",
            },
            ModelBiasJobInput={
                "EndpointInput": {
                    "EndpointName": "fake-ep",
                    "LocalPath": "/opt/ml/input",
                },
                "GroundTruthS3Input": {"S3Uri": "s3://bucket/gt"},
            },
            ModelBiasJobOutputConfig={
                "MonitoringOutputs": [
                    {
                        "S3Output": {
                            "S3Uri": "s3://bucket/out",
                            "LocalPath": "/opt/ml/output",
                            "S3UploadMode": "EndOfJob",
                        }
                    }
                ]
            },
            JobResources={
                "ClusterConfig": {
                    "InstanceCount": 1,
                    "InstanceType": "ml.m4.xlarge",
                    "VolumeSizeInGB": 10,
                }
            },
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )

    def test_create_and_describe_mb_job_definition(self, sagemaker):
        name = _uid("mb")
        resp = self._create_mb_job_def(sagemaker, name)
        assert "JobDefinitionArn" in resp
        try:
            desc = sagemaker.describe_model_bias_job_definition(JobDefinitionName=name)
            assert desc["JobDefinitionName"] == name
        finally:
            sagemaker.delete_model_bias_job_definition(JobDefinitionName=name)

    def test_list_mb_job_definitions_after_create(self, sagemaker):
        name = _uid("mb")
        self._create_mb_job_def(sagemaker, name)
        try:
            resp = sagemaker.list_model_bias_job_definitions()
            names = [j["MonitoringJobDefinitionName"] for j in resp["JobDefinitionSummaries"]]
            assert name in names
        finally:
            sagemaker.delete_model_bias_job_definition(JobDefinitionName=name)

    def test_delete_mb_job_definition(self, sagemaker):
        name = _uid("mb")
        self._create_mb_job_def(sagemaker, name)
        sagemaker.delete_model_bias_job_definition(JobDefinitionName=name)
        resp = sagemaker.list_model_bias_job_definitions()
        names = [j["MonitoringJobDefinitionName"] for j in resp["JobDefinitionSummaries"]]
        assert name not in names


class TestSageMakerTrialComponentCRUD:
    """TrialComponent CRUD tests."""

    def test_create_and_describe_trial_component(self, sagemaker):
        name = _uid("tc")
        resp = sagemaker.create_trial_component(TrialComponentName=name)
        assert "TrialComponentArn" in resp
        try:
            desc = sagemaker.describe_trial_component(TrialComponentName=name)
            assert desc["TrialComponentName"] == name
        finally:
            sagemaker.delete_trial_component(TrialComponentName=name)

    def test_list_trial_components_after_create(self, sagemaker):
        name = _uid("tc")
        sagemaker.create_trial_component(TrialComponentName=name)
        try:
            resp = sagemaker.list_trial_components()
            names = [c["TrialComponentName"] for c in resp["TrialComponentSummaries"]]
            assert name in names
        finally:
            sagemaker.delete_trial_component(TrialComponentName=name)

    def test_delete_trial_component(self, sagemaker):
        name = _uid("tc")
        sagemaker.create_trial_component(TrialComponentName=name)
        sagemaker.delete_trial_component(TrialComponentName=name)
        resp = sagemaker.list_trial_components()
        names = [c["TrialComponentName"] for c in resp["TrialComponentSummaries"]]
        assert name not in names

    def test_update_trial_component(self, sagemaker):
        name = _uid("tc")
        sagemaker.create_trial_component(TrialComponentName=name)
        try:
            resp = sagemaker.update_trial_component(
                TrialComponentName=name, DisplayName="Updated TC"
            )
            assert "TrialComponentArn" in resp
        finally:
            sagemaker.delete_trial_component(TrialComponentName=name)

    def test_associate_and_disassociate_trial_component(self, sagemaker):
        exp_name = _uid("exp")
        trial_name = _uid("trial")
        tc_name = _uid("tc")
        sagemaker.create_experiment(ExperimentName=exp_name)
        try:
            sagemaker.create_trial(TrialName=trial_name, ExperimentName=exp_name)
            sagemaker.create_trial_component(TrialComponentName=tc_name)
            resp = sagemaker.associate_trial_component(
                TrialComponentName=tc_name, TrialName=trial_name
            )
            assert "TrialComponentArn" in resp
            resp2 = sagemaker.disassociate_trial_component(
                TrialComponentName=tc_name, TrialName=trial_name
            )
            assert "TrialComponentArn" in resp2
        finally:
            sagemaker.delete_trial_component(TrialComponentName=tc_name)
            sagemaker.delete_trial(TrialName=trial_name)
            sagemaker.delete_experiment(ExperimentName=exp_name)

    def test_describe_trial_component_not_found(self, sagemaker):
        with pytest.raises(ClientError) as exc:
            sagemaker.describe_trial_component(TrialComponentName="fake-tc-nonexistent")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


class TestSageMakerModelCardExtended:
    """Extended ModelCard tests."""

    def test_update_model_card(self, sagemaker):
        name = _uid("mc")
        sagemaker.create_model_card(
            ModelCardName=name,
            Content='{"model_overview":{}}',
            ModelCardStatus="Draft",
        )
        try:
            resp = sagemaker.update_model_card(ModelCardName=name, ModelCardStatus="PendingReview")
            assert "ModelCardArn" in resp
            desc = sagemaker.describe_model_card(ModelCardName=name)
            assert desc["ModelCardStatus"] == "PendingReview"
        finally:
            sagemaker.delete_model_card(ModelCardName=name)

    def test_list_model_card_versions(self, sagemaker):
        name = _uid("mc")
        sagemaker.create_model_card(
            ModelCardName=name,
            Content='{"model_overview":{}}',
            ModelCardStatus="Draft",
        )
        try:
            resp = sagemaker.list_model_card_versions(ModelCardName=name)
            assert "ModelCardVersionSummaryList" in resp
            assert len(resp["ModelCardVersionSummaryList"]) >= 1
        finally:
            sagemaker.delete_model_card(ModelCardName=name)


class TestSageMakerModelPackageExtended:
    """Extended ModelPackage tests."""

    def test_update_model_package(self, sagemaker):
        name = _uid("mp")
        resp = sagemaker.create_model_package(
            ModelPackageName=name,
            ModelPackageDescription="test",
        )
        mp_arn = resp["ModelPackageArn"]
        resp = sagemaker.update_model_package(
            ModelPackageArn=mp_arn, ModelApprovalStatus="Approved"
        )
        assert "ModelPackageArn" in resp
        desc = sagemaker.describe_model_package(ModelPackageName=name)
        assert desc["ModelApprovalStatus"] == "Approved"


class TestSageMakerEndpointExtended:
    """Extended endpoint tests."""

    def _setup(self, sagemaker, uid):
        model_name = f"model-{uid}"
        ec_name = f"ec-{uid}"
        ep_name = f"ep-{uid}"
        sagemaker.create_model(
            ModelName=model_name,
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            PrimaryContainer={
                "Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
            },
        )
        sagemaker.create_endpoint_config(
            EndpointConfigName=ec_name,
            ProductionVariants=[
                {
                    "VariantName": "v1",
                    "ModelName": model_name,
                    "InitialInstanceCount": 1,
                    "InstanceType": "ml.m4.xlarge",
                }
            ],
        )
        sagemaker.create_endpoint(EndpointName=ep_name, EndpointConfigName=ec_name)
        return model_name, ec_name, ep_name

    def test_update_endpoint_weights_and_capacities(self, sagemaker):
        uid = uuid.uuid4().hex[:8]
        model_name, ec_name, ep_name = self._setup(sagemaker, uid)
        try:
            resp = sagemaker.update_endpoint_weights_and_capacities(
                EndpointName=ep_name,
                DesiredWeightsAndCapacities=[{"VariantName": "v1", "DesiredWeight": 1.0}],
            )
            assert "EndpointArn" in resp
        finally:
            sagemaker.delete_endpoint(EndpointName=ep_name)
            sagemaker.delete_endpoint_config(EndpointConfigName=ec_name)
            sagemaker.delete_model(ModelName=model_name)


class TestSageMakerSearch:
    """Search operation tests."""

    def test_search_experiments(self, sagemaker):
        resp = sagemaker.search(Resource="Experiment")
        assert "Results" in resp
        assert isinstance(resp["Results"], list)

    def test_search_training_jobs(self, sagemaker):
        resp = sagemaker.search(Resource="TrainingJob")
        assert "Results" in resp
        assert isinstance(resp["Results"], list)

    def test_search_endpoints(self, sagemaker):
        resp = sagemaker.search(Resource="Endpoint")
        assert "Results" in resp
        assert isinstance(resp["Results"], list)

    def test_search_pipelines(self, sagemaker):
        resp = sagemaker.search(Resource="Pipeline")
        assert "Results" in resp
        assert isinstance(resp["Results"], list)

    def test_search_model_packages(self, sagemaker):
        resp = sagemaker.search(Resource="ModelPackage")
        assert "Results" in resp
        assert isinstance(resp["Results"], list)

    def test_search_model_package_groups(self, sagemaker):
        resp = sagemaker.search(Resource="ModelPackageGroup")
        assert "Results" in resp
        assert isinstance(resp["Results"], list)


class TestSageMakerDescribeNotFound:
    """Describe operations return errors for non-existent resources."""

    def test_describe_model_not_found(self, sagemaker):
        with pytest.raises(ClientError) as exc:
            sagemaker.describe_model(ModelName="fake-model-nonexistent")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    def test_describe_training_job_not_found(self, sagemaker):
        with pytest.raises(ClientError) as exc:
            sagemaker.describe_training_job(TrainingJobName="fake-tj-nonexistent")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    def test_describe_transform_job_not_found(self, sagemaker):
        with pytest.raises(ClientError) as exc:
            sagemaker.describe_transform_job(TransformJobName="fake-xf-nonexistent")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    def test_describe_processing_job_not_found(self, sagemaker):
        with pytest.raises(ClientError) as exc:
            sagemaker.describe_processing_job(ProcessingJobName="fake-pj-nonexistent")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    def test_describe_notebook_instance_not_found(self, sagemaker):
        with pytest.raises(ClientError) as exc:
            sagemaker.describe_notebook_instance(NotebookInstanceName="fake-nb-nonexistent")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    def test_describe_trial_not_found(self, sagemaker):
        with pytest.raises(ClientError) as exc:
            sagemaker.describe_trial(TrialName="fake-trial-nonexistent")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


class TestSageMakerNotebookLifecycleConfigCRUD:
    """NotebookInstanceLifecycleConfig CRUD tests."""

    def test_create_and_describe_lifecycle_config(self, sagemaker):
        name = _uid("lc")
        resp = sagemaker.create_notebook_instance_lifecycle_config(
            NotebookInstanceLifecycleConfigName=name,
            OnCreate=[{"Content": "IyEvYmluL2Jhc2gKZWNobyBoZWxsbw=="}],
        )
        assert "NotebookInstanceLifecycleConfigArn" in resp
        try:
            desc = sagemaker.describe_notebook_instance_lifecycle_config(
                NotebookInstanceLifecycleConfigName=name
            )
            assert desc["NotebookInstanceLifecycleConfigName"] == name
            assert "NotebookInstanceLifecycleConfigArn" in desc
        finally:
            sagemaker.delete_notebook_instance_lifecycle_config(
                NotebookInstanceLifecycleConfigName=name
            )

    def test_delete_lifecycle_config(self, sagemaker):
        name = _uid("lc")
        sagemaker.create_notebook_instance_lifecycle_config(
            NotebookInstanceLifecycleConfigName=name,
            OnCreate=[{"Content": "IyEvYmluL2Jhc2gKZWNobyBoZWxsbw=="}],
        )
        resp = sagemaker.delete_notebook_instance_lifecycle_config(
            NotebookInstanceLifecycleConfigName=name
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify deletion: describe should fail
        with pytest.raises(ClientError):
            sagemaker.describe_notebook_instance_lifecycle_config(
                NotebookInstanceLifecycleConfigName=name
            )

    def test_describe_lifecycle_config_not_found(self, sagemaker):
        with pytest.raises(ClientError) as exc:
            sagemaker.describe_notebook_instance_lifecycle_config(
                NotebookInstanceLifecycleConfigName="fake-lc-nonexistent"
            )
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] >= 400


class TestSageMakerFeatureGroupCRUD:
    """FeatureGroup create and describe tests."""

    def test_create_and_describe_feature_group(self, sagemaker):
        name = _uid("fg")
        resp = sagemaker.create_feature_group(
            FeatureGroupName=name,
            RecordIdentifierFeatureName="record_id",
            EventTimeFeatureName="event_time",
            FeatureDefinitions=[
                {"FeatureName": "record_id", "FeatureType": "String"},
                {"FeatureName": "event_time", "FeatureType": "String"},
                {"FeatureName": "feature1", "FeatureType": "Integral"},
            ],
            OfflineStoreConfig={"S3StorageConfig": {"S3Uri": "s3://bucket/feature-store"}},
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        assert "FeatureGroupArn" in resp
        desc = sagemaker.describe_feature_group(FeatureGroupName=name)
        assert desc["FeatureGroupName"] == name
        assert len(desc["FeatureDefinitions"]) == 3
        assert "FeatureGroupStatus" in desc


class TestSageMakerClusterOperations:
    """Cluster operations tests.

    Note: create_cluster has a Moto bug (creates resource but also raises
    ResourceInUse), so we test delete_cluster and list_cluster_nodes against
    clusters that were already created despite the error.
    """

    def _force_create_cluster(self, sagemaker, name):
        """Create a cluster, ignoring the ResourceInUse bug in Moto."""
        try:
            sagemaker.create_cluster(
                ClusterName=name,
                InstanceGroups=[
                    {
                        "InstanceCount": 1,
                        "InstanceGroupName": "worker-group",
                        "InstanceType": "ml.m5.xlarge",
                        "LifeCycleConfig": {
                            "SourceS3Uri": "s3://sagemaker-bucket/lifecycle",
                            "OnCreate": "on_create.sh",
                        },
                        "ExecutionRole": "arn:aws:iam::123456789012:role/SageMakerRole",
                    }
                ],
            )
        except ClientError:
            pass  # Moto bug: creates cluster then raises ResourceInUse

    def test_delete_cluster(self, sagemaker):
        name = _uid("cl")
        self._force_create_cluster(sagemaker, name)
        # Verify it exists
        desc = sagemaker.describe_cluster(ClusterName=name)
        assert desc["ClusterName"] == name
        # Delete it
        del_resp = sagemaker.delete_cluster(ClusterName=name)
        assert "ClusterArn" in del_resp

    def test_list_cluster_nodes(self, sagemaker):
        name = _uid("cl")
        self._force_create_cluster(sagemaker, name)
        try:
            resp = sagemaker.list_cluster_nodes(ClusterName=name)
            assert "ClusterNodeSummaries" in resp
            assert isinstance(resp["ClusterNodeSummaries"], list)
        finally:
            sagemaker.delete_cluster(ClusterName=name)


class TestSageMakerAutoMLJobV2:
    """AutoMLJobV2 create and stop tests."""

    def test_create_auto_ml_job_v2(self, sagemaker):
        name = _uid("aml")
        resp = sagemaker.create_auto_ml_job_v2(
            AutoMLJobName=name,
            AutoMLJobInputDataConfig=[
                {
                    "ChannelType": "training",
                    "DataSource": {
                        "S3DataSource": {
                            "S3DataType": "S3Prefix",
                            "S3Uri": "s3://bucket/data",
                        }
                    },
                }
            ],
            OutputDataConfig={"S3OutputPath": "s3://bucket/output"},
            AutoMLProblemTypeConfig={
                "TabularJobConfig": {
                    "TargetAttributeName": "target",
                    "ProblemType": "BinaryClassification",
                    "CompletionCriteria": {
                        "MaxCandidates": 10,
                    },
                }
            },
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        assert "AutoMLJobArn" in resp

    def test_stop_auto_ml_job(self, sagemaker):
        name = _uid("aml")
        sagemaker.create_auto_ml_job_v2(
            AutoMLJobName=name,
            AutoMLJobInputDataConfig=[
                {
                    "ChannelType": "training",
                    "DataSource": {
                        "S3DataSource": {
                            "S3DataType": "S3Prefix",
                            "S3Uri": "s3://bucket/data",
                        }
                    },
                }
            ],
            OutputDataConfig={"S3OutputPath": "s3://bucket/output"},
            AutoMLProblemTypeConfig={
                "TabularJobConfig": {
                    "TargetAttributeName": "target",
                    "ProblemType": "BinaryClassification",
                    "CompletionCriteria": {
                        "MaxCandidates": 10,
                    },
                }
            },
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        resp = sagemaker.stop_auto_ml_job(AutoMLJobName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerDeleteOperations:
    """Delete operations for compilation job and hyper parameter tuning job."""

    def test_delete_compilation_job(self, sagemaker):
        name = _uid("cj")
        sagemaker.create_compilation_job(
            CompilationJobName=name,
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            InputConfig={
                "S3Uri": "s3://bucket/model.tar.gz",
                "DataInputConfig": '{"input":[1,3,224,224]}',
                "Framework": "PYTORCH",
            },
            OutputConfig={
                "S3OutputLocation": "s3://bucket/out",
                "TargetDevice": "ml_m4",
            },
            StoppingCondition={"MaxRuntimeInSeconds": 900},
        )
        resp = sagemaker.delete_compilation_job(CompilationJobName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_hyper_parameter_tuning_job(self, sagemaker):
        name = _uid("hpt")
        sagemaker.create_hyper_parameter_tuning_job(
            HyperParameterTuningJobName=name,
            HyperParameterTuningJobConfig={
                "Strategy": "Bayesian",
                "ResourceLimits": {
                    "MaxNumberOfTrainingJobs": 10,
                    "MaxParallelTrainingJobs": 2,
                },
            },
            TrainingJobDefinition={
                "StaticHyperParameters": {"epochs": "10"},
                "AlgorithmSpecification": {
                    "TrainingImage": "123456789012.dkr.ecr.us-east-1.amazonaws.com/img:latest",
                    "TrainingInputMode": "File",
                },
                "RoleArn": "arn:aws:iam::123456789012:role/SageMakerRole",
                "InputDataConfig": [
                    {
                        "ChannelName": "train",
                        "DataSource": {
                            "S3DataSource": {
                                "S3DataType": "S3Prefix",
                                "S3Uri": "s3://bucket/train",
                            }
                        },
                    }
                ],
                "OutputDataConfig": {"S3OutputPath": "s3://bucket/output"},
                "ResourceConfig": {
                    "InstanceType": "ml.m4.xlarge",
                    "InstanceCount": 1,
                    "VolumeSizeInGB": 10,
                },
                "StoppingCondition": {"MaxRuntimeInSeconds": 3600},
            },
        )
        resp = sagemaker.delete_hyper_parameter_tuning_job(HyperParameterTuningJobName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerSearchFiltered:
    """Search with filters and additional resource types."""

    def test_search_experiment_with_filter(self, sagemaker):
        """Search Experiment with SearchExpression filter."""
        name = _uid("exp")
        sagemaker.create_experiment(ExperimentName=name)
        try:
            resp = sagemaker.search(
                Resource="Experiment",
                SearchExpression={
                    "Filters": [
                        {
                            "Name": "ExperimentName",
                            "Operator": "Equals",
                            "Value": name,
                        }
                    ]
                },
            )
            assert "Results" in resp
            results = resp["Results"]
            assert len(results) >= 1
            matched = results[0]["Experiment"]["ExperimentName"]
            assert matched == name
        finally:
            sagemaker.delete_experiment(ExperimentName=name)

    def test_search_feature_group(self, sagemaker):
        """Search FeatureGroup resource type returns results."""
        resp = sagemaker.search(Resource="FeatureGroup")
        assert "Results" in resp
        assert isinstance(resp["Results"], list)

    def test_search_project_resource(self, sagemaker):
        """Search Project resource type returns results list."""
        resp = sagemaker.search(Resource="Project")
        assert "Results" in resp
        assert isinstance(resp["Results"], list)


class TestSageMakerListFiltered:
    """List operations with filter parameters."""

    def test_list_models_with_name_contains_param(self, sagemaker):
        """list_models accepts NameContains parameter without error."""
        name = _uid("mdl")
        sagemaker.create_model(
            ModelName=name,
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            PrimaryContainer={
                "Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
            },
        )
        try:
            resp = sagemaker.list_models(NameContains=name)
            assert "Models" in resp
            names = [m["ModelName"] for m in resp["Models"]]
            assert name in names
        finally:
            sagemaker.delete_model(ModelName=name)

    def test_list_training_jobs_status_filter(self, sagemaker):
        """list_training_jobs with StatusEquals filters by status."""
        resp = sagemaker.list_training_jobs(StatusEquals="Completed")
        assert "TrainingJobSummaries" in resp
        assert isinstance(resp["TrainingJobSummaries"], list)

    def test_list_endpoints_sorted(self, sagemaker):
        """list_endpoints with SortBy and SortOrder."""
        resp = sagemaker.list_endpoints(SortBy="Name", SortOrder="Ascending")
        assert "Endpoints" in resp
        assert isinstance(resp["Endpoints"], list)


class TestSageMakerTagsOnEndpoint:
    """Tags on endpoint resources."""

    def test_add_and_list_tags_on_endpoint(self, sagemaker):
        """add_tags and list_tags work on endpoint ARNs."""
        model_name = _uid("model")
        ec_name = _uid("ec")
        ep_name = _uid("ep")
        sagemaker.create_model(
            ModelName=model_name,
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            PrimaryContainer={
                "Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
            },
        )
        sagemaker.create_endpoint_config(
            EndpointConfigName=ec_name,
            ProductionVariants=[
                {
                    "VariantName": "v1",
                    "ModelName": model_name,
                    "InitialInstanceCount": 1,
                    "InstanceType": "ml.m4.xlarge",
                }
            ],
        )
        sagemaker.create_endpoint(EndpointName=ep_name, EndpointConfigName=ec_name)
        try:
            ep_arn = sagemaker.describe_endpoint(EndpointName=ep_name)["EndpointArn"]
            sagemaker.add_tags(
                ResourceArn=ep_arn,
                Tags=[{"Key": "env", "Value": "staging"}],
            )
            tags_resp = sagemaker.list_tags(ResourceArn=ep_arn)
            tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tags["env"] == "staging"
        finally:
            sagemaker.delete_endpoint(EndpointName=ep_name)
            sagemaker.delete_endpoint_config(EndpointConfigName=ec_name)
            sagemaker.delete_model(ModelName=model_name)

    def test_delete_tags_on_endpoint(self, sagemaker):
        """delete_tags removes tags from an endpoint."""
        model_name = _uid("model")
        ec_name = _uid("ec")
        ep_name = _uid("ep")
        sagemaker.create_model(
            ModelName=model_name,
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            PrimaryContainer={
                "Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
            },
        )
        sagemaker.create_endpoint_config(
            EndpointConfigName=ec_name,
            ProductionVariants=[
                {
                    "VariantName": "v1",
                    "ModelName": model_name,
                    "InitialInstanceCount": 1,
                    "InstanceType": "ml.m4.xlarge",
                }
            ],
        )
        sagemaker.create_endpoint(EndpointName=ep_name, EndpointConfigName=ec_name)
        try:
            ep_arn = sagemaker.describe_endpoint(EndpointName=ep_name)["EndpointArn"]
            sagemaker.add_tags(
                ResourceArn=ep_arn,
                Tags=[
                    {"Key": "env", "Value": "staging"},
                    {"Key": "team", "Value": "ml"},
                ],
            )
            sagemaker.delete_tags(ResourceArn=ep_arn, TagKeys=["env"])
            tags_resp = sagemaker.list_tags(ResourceArn=ep_arn)
            keys = [t["Key"] for t in tags_resp["Tags"]]
            assert "env" not in keys
            assert "team" in keys
        finally:
            sagemaker.delete_endpoint(EndpointName=ep_name)
            sagemaker.delete_endpoint_config(EndpointConfigName=ec_name)
            sagemaker.delete_model(ModelName=model_name)


class TestSageMakerCreateWithTags:
    """Create resources with inline Tags parameter."""

    def test_create_model_with_tags(self, sagemaker):
        """create_model with Tags attaches tags at creation time."""
        name = _uid("model")
        resp = sagemaker.create_model(
            ModelName=name,
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            PrimaryContainer={
                "Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
            },
            Tags=[{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "ds"}],
        )
        arn = resp["ModelArn"]
        try:
            tags_resp = sagemaker.list_tags(ResourceArn=arn)
            tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tags["env"] == "prod"
            assert tags["team"] == "ds"
        finally:
            sagemaker.delete_model(ModelName=name)

    def test_create_notebook_with_tags(self, sagemaker):
        """create_notebook_instance with Tags attaches tags at creation time."""
        name = _uid("nb")
        sagemaker.create_notebook_instance(
            NotebookInstanceName=name,
            InstanceType="ml.t2.medium",
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            Tags=[{"Key": "team", "Value": "ml"}],
        )
        try:
            desc = sagemaker.describe_notebook_instance(NotebookInstanceName=name)
            arn = desc["NotebookInstanceArn"]
            tags_resp = sagemaker.list_tags(ResourceArn=arn)
            tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tags["team"] == "ml"
        finally:
            sagemaker.stop_notebook_instance(NotebookInstanceName=name)
            sagemaker.delete_notebook_instance(NotebookInstanceName=name)

    def test_create_endpoint_config_with_tags(self, sagemaker):
        """create_endpoint_config with Tags attaches tags at creation time."""
        model_name = _uid("model")
        ec_name = _uid("ec")
        sagemaker.create_model(
            ModelName=model_name,
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            PrimaryContainer={
                "Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
            },
        )
        resp = sagemaker.create_endpoint_config(
            EndpointConfigName=ec_name,
            ProductionVariants=[
                {
                    "VariantName": "v1",
                    "ModelName": model_name,
                    "InitialInstanceCount": 1,
                    "InstanceType": "ml.m4.xlarge",
                }
            ],
            Tags=[{"Key": "cost-center", "Value": "123"}],
        )
        arn = resp["EndpointConfigArn"]
        try:
            tags_resp = sagemaker.list_tags(ResourceArn=arn)
            tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tags["cost-center"] == "123"
        finally:
            sagemaker.delete_endpoint_config(EndpointConfigName=ec_name)
            sagemaker.delete_model(ModelName=model_name)


class TestSageMakerEndpointConfigDetails:
    """Deeper assertions on endpoint config describe."""

    def test_describe_endpoint_config_production_variants(self, sagemaker):
        """describe_endpoint_config returns ProductionVariants with correct fields."""
        model_name = _uid("model")
        ec_name = _uid("ec")
        sagemaker.create_model(
            ModelName=model_name,
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            PrimaryContainer={
                "Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
            },
        )
        sagemaker.create_endpoint_config(
            EndpointConfigName=ec_name,
            ProductionVariants=[
                {
                    "VariantName": "primary",
                    "ModelName": model_name,
                    "InitialInstanceCount": 2,
                    "InstanceType": "ml.m5.xlarge",
                }
            ],
        )
        try:
            desc = sagemaker.describe_endpoint_config(EndpointConfigName=ec_name)
            assert desc["EndpointConfigName"] == ec_name
            assert "EndpointConfigArn" in desc
            pvs = desc["ProductionVariants"]
            assert len(pvs) == 1
            pv = pvs[0]
            assert pv["VariantName"] == "primary"
            assert pv["ModelName"] == model_name
            assert pv["InitialInstanceCount"] == 2
            assert pv["InstanceType"] == "ml.m5.xlarge"
        finally:
            sagemaker.delete_endpoint_config(EndpointConfigName=ec_name)
            sagemaker.delete_model(ModelName=model_name)


class TestSageMakerTagsOnPipeline:
    """Tags CRUD on Pipeline resources."""

    def _create_pipeline(self, sagemaker, name):
        return sagemaker.create_pipeline(
            PipelineName=name,
            PipelineDefinition='{"Version":"2020-12-01","Steps":[]}',
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )

    def test_add_and_list_tags_on_pipeline(self, sagemaker):
        """add_tags and list_tags work on pipeline ARNs."""
        name = _uid("pipe")
        resp = self._create_pipeline(sagemaker, name)
        arn = resp["PipelineArn"]
        try:
            sagemaker.add_tags(
                ResourceArn=arn,
                Tags=[
                    {"Key": "env", "Value": "dev"},
                    {"Key": "team", "Value": "ml"},
                ],
            )
            tags_resp = sagemaker.list_tags(ResourceArn=arn)
            tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tags["env"] == "dev"
            assert tags["team"] == "ml"
        finally:
            sagemaker.delete_pipeline(PipelineName=name)

    def test_delete_tags_on_pipeline(self, sagemaker):
        """delete_tags removes tags from a pipeline."""
        name = _uid("pipe")
        resp = self._create_pipeline(sagemaker, name)
        arn = resp["PipelineArn"]
        try:
            sagemaker.add_tags(
                ResourceArn=arn,
                Tags=[
                    {"Key": "env", "Value": "dev"},
                    {"Key": "team", "Value": "ml"},
                ],
            )
            sagemaker.delete_tags(ResourceArn=arn, TagKeys=["env"])
            tags_resp = sagemaker.list_tags(ResourceArn=arn)
            keys = [t["Key"] for t in tags_resp["Tags"]]
            assert "env" not in keys
            assert "team" in keys
        finally:
            sagemaker.delete_pipeline(PipelineName=name)

    def test_create_pipeline_with_inline_tags(self, sagemaker):
        """create_pipeline with Tags attaches tags at creation time."""
        name = _uid("pipe")
        resp = sagemaker.create_pipeline(
            PipelineName=name,
            PipelineDefinition='{"Version":"2020-12-01","Steps":[]}',
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            Tags=[{"Key": "created-by", "Value": "test"}],
        )
        arn = resp["PipelineArn"]
        try:
            tags_resp = sagemaker.list_tags(ResourceArn=arn)
            tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tags["created-by"] == "test"
        finally:
            sagemaker.delete_pipeline(PipelineName=name)


class TestSageMakerTagsOnTrainingJob:
    """Tags CRUD on TrainingJob resources."""

    def _create_training_job(self, sagemaker, name):
        return sagemaker.create_training_job(
            TrainingJobName=name,
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            AlgorithmSpecification={
                "TrainingImage": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
                "TrainingInputMode": "File",
            },
            OutputDataConfig={"S3OutputPath": "s3://my-bucket/output"},
            ResourceConfig={
                "InstanceType": "ml.m4.xlarge",
                "InstanceCount": 1,
                "VolumeSizeInGB": 10,
            },
            StoppingCondition={"MaxRuntimeInSeconds": 3600},
        )

    def test_add_and_list_tags_on_training_job(self, sagemaker):
        """add_tags and list_tags work on training job ARNs."""
        name = _uid("tj")
        resp = self._create_training_job(sagemaker, name)
        arn = resp["TrainingJobArn"]
        sagemaker.add_tags(
            ResourceArn=arn,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "ml"},
            ],
        )
        tags_resp = sagemaker.list_tags(ResourceArn=arn)
        tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
        assert tags["env"] == "test"
        assert tags["team"] == "ml"

    def test_delete_tags_on_training_job(self, sagemaker):
        """delete_tags removes tags from a training job."""
        name = _uid("tj")
        resp = self._create_training_job(sagemaker, name)
        arn = resp["TrainingJobArn"]
        sagemaker.add_tags(
            ResourceArn=arn,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "ml"},
            ],
        )
        sagemaker.delete_tags(ResourceArn=arn, TagKeys=["env"])
        tags_resp = sagemaker.list_tags(ResourceArn=arn)
        keys = [t["Key"] for t in tags_resp["Tags"]]
        assert "env" not in keys
        assert "team" in keys


class TestSageMakerTagsOnModelPackageGroup:
    """Tags on ModelPackageGroup resources."""

    def test_add_and_list_tags_on_model_package_group(self, sagemaker):
        """add_tags and list_tags work on model package group ARNs."""
        name = _uid("mpg")
        resp = sagemaker.create_model_package_group(
            ModelPackageGroupName=name,
            ModelPackageGroupDescription="test group",
        )
        arn = resp["ModelPackageGroupArn"]
        sagemaker.add_tags(
            ResourceArn=arn,
            Tags=[
                {"Key": "env", "Value": "staging"},
                {"Key": "owner", "Value": "data-team"},
            ],
        )
        tags_resp = sagemaker.list_tags(ResourceArn=arn)
        tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
        assert tags["env"] == "staging"
        assert tags["owner"] == "data-team"

    def test_delete_tags_on_model_package_group(self, sagemaker):
        """delete_tags removes tags from a model package group."""
        name = _uid("mpg")
        resp = sagemaker.create_model_package_group(
            ModelPackageGroupName=name,
            ModelPackageGroupDescription="test group",
        )
        arn = resp["ModelPackageGroupArn"]
        sagemaker.add_tags(
            ResourceArn=arn,
            Tags=[
                {"Key": "env", "Value": "staging"},
                {"Key": "owner", "Value": "data-team"},
            ],
        )
        sagemaker.delete_tags(ResourceArn=arn, TagKeys=["env"])
        tags_resp = sagemaker.list_tags(ResourceArn=arn)
        keys = [t["Key"] for t in tags_resp["Tags"]]
        assert "env" not in keys
        assert "owner" in keys


class TestSageMakerSearchAdditional:
    """Search with additional resource types not covered elsewhere."""

    def test_search_feature_group_resource(self, sagemaker):
        """Search FeatureGroup returns Results list."""
        resp = sagemaker.search(Resource="FeatureGroup")
        assert "Results" in resp
        assert isinstance(resp["Results"], list)

    def test_search_experiment_trial_component(self, sagemaker):
        """Search ExperimentTrialComponent returns Results list."""
        resp = sagemaker.search(Resource="ExperimentTrialComponent")
        assert "Results" in resp
        assert isinstance(resp["Results"], list)


class TestSageMakerListEndpointsFiltered:
    """List endpoints with various filter parameters."""

    def test_list_endpoints_name_contains(self, sagemaker):
        """list_endpoints with NameContains filters correctly."""
        model_name = _uid("model")
        ec_name = _uid("ec")
        ep_name = _uid("ep-flt")
        sagemaker.create_model(
            ModelName=model_name,
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            PrimaryContainer={
                "Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
            },
        )
        sagemaker.create_endpoint_config(
            EndpointConfigName=ec_name,
            ProductionVariants=[
                {
                    "VariantName": "v1",
                    "ModelName": model_name,
                    "InitialInstanceCount": 1,
                    "InstanceType": "ml.m4.xlarge",
                }
            ],
        )
        sagemaker.create_endpoint(EndpointName=ep_name, EndpointConfigName=ec_name)
        try:
            resp = sagemaker.list_endpoints(NameContains=ep_name[:10])
            names = [e["EndpointName"] for e in resp["Endpoints"]]
            assert ep_name in names
        finally:
            sagemaker.delete_endpoint(EndpointName=ep_name)
            sagemaker.delete_endpoint_config(EndpointConfigName=ec_name)
            sagemaker.delete_model(ModelName=model_name)

    def test_list_endpoints_max_results(self, sagemaker):
        """list_endpoints with MaxResults returns limited results."""
        resp = sagemaker.list_endpoints(MaxResults=5)
        assert "Endpoints" in resp
        assert isinstance(resp["Endpoints"], list)

    def test_list_endpoints_name_contains_no_match(self, sagemaker):
        """list_endpoints with non-matching NameContains returns empty list."""
        resp = sagemaker.list_endpoints(NameContains="zzz-nonexistent-zzz")
        assert "Endpoints" in resp
        assert len(resp["Endpoints"]) == 0


class TestSageMakerListEndpointConfigsFiltered:
    """List endpoint configs with filter parameters."""

    def test_list_endpoint_configs_name_contains(self, sagemaker):
        """list_endpoint_configs with NameContains filters correctly."""
        resp = sagemaker.list_endpoint_configs(NameContains="zzz-nonexistent-zzz")
        assert "EndpointConfigs" in resp
        assert len(resp["EndpointConfigs"]) == 0

    def test_list_endpoint_configs_max_results(self, sagemaker):
        """list_endpoint_configs with MaxResults returns bounded results."""
        resp = sagemaker.list_endpoint_configs(MaxResults=5)
        assert "EndpointConfigs" in resp
        assert isinstance(resp["EndpointConfigs"], list)

    def test_list_endpoint_configs_sorted(self, sagemaker):
        """list_endpoint_configs with SortBy and SortOrder."""
        resp = sagemaker.list_endpoint_configs(SortBy="Name", SortOrder="Descending")
        assert "EndpointConfigs" in resp
        assert isinstance(resp["EndpointConfigs"], list)


class TestSageMakerListModelsFiltered:
    """List models with additional filter parameters."""

    def test_list_models_max_results(self, sagemaker):
        """list_models with MaxResults returns bounded results."""
        resp = sagemaker.list_models(MaxResults=5)
        assert "Models" in resp
        assert isinstance(resp["Models"], list)

    def test_list_models_sorted_descending(self, sagemaker):
        """list_models with SortBy=Name, SortOrder=Descending."""
        resp = sagemaker.list_models(SortBy="Name", SortOrder="Descending")
        assert "Models" in resp
        assert isinstance(resp["Models"], list)

    def test_list_models_creation_time_filter(self, sagemaker):
        """list_models with CreationTimeAfter returns models."""
        from datetime import datetime

        resp = sagemaker.list_models(CreationTimeAfter=datetime(2020, 1, 1))
        assert "Models" in resp
        assert isinstance(resp["Models"], list)


class TestSageMakerListNotebookInstancesFiltered:
    """List notebook instances with filter parameters."""

    def test_list_notebook_instances_status_filter(self, sagemaker):
        """list_notebook_instances with StatusEquals filter."""
        resp = sagemaker.list_notebook_instances(StatusEquals="InService")
        assert "NotebookInstances" in resp
        assert isinstance(resp["NotebookInstances"], list)

    def test_list_notebook_instances_name_contains(self, sagemaker):
        """list_notebook_instances with NameContains filter."""
        resp = sagemaker.list_notebook_instances(NameContains="zzz-nonexistent-zzz")
        assert "NotebookInstances" in resp
        assert len(resp["NotebookInstances"]) == 0


class TestSageMakerListTrainingJobsFiltered:
    """List training jobs with additional filter parameters."""

    def test_list_training_jobs_name_contains(self, sagemaker):
        """list_training_jobs with NameContains filter."""
        resp = sagemaker.list_training_jobs(NameContains="zzz-nonexistent-zzz")
        assert "TrainingJobSummaries" in resp
        assert len(resp["TrainingJobSummaries"]) == 0

    def test_list_training_jobs_sorted_ascending(self, sagemaker):
        """list_training_jobs with SortBy=Name, SortOrder=Ascending."""
        resp = sagemaker.list_training_jobs(SortBy="Name", SortOrder="Ascending")
        assert "TrainingJobSummaries" in resp
        assert isinstance(resp["TrainingJobSummaries"], list)


class TestSageMakerListProcessingJobsFiltered:
    """List processing jobs with additional filter parameters."""

    def test_list_processing_jobs_status_filter(self, sagemaker):
        """list_processing_jobs with StatusEquals filter."""
        resp = sagemaker.list_processing_jobs(StatusEquals="Completed")
        assert "ProcessingJobSummaries" in resp
        assert isinstance(resp["ProcessingJobSummaries"], list)

    def test_list_processing_jobs_name_contains(self, sagemaker):
        """list_processing_jobs with NameContains filter."""
        resp = sagemaker.list_processing_jobs(NameContains="zzz-nonexistent-zzz")
        assert "ProcessingJobSummaries" in resp
        assert len(resp["ProcessingJobSummaries"]) == 0

    def test_list_processing_jobs_sorted(self, sagemaker):
        """list_processing_jobs with SortBy and SortOrder."""
        resp = sagemaker.list_processing_jobs(SortBy="Name", SortOrder="Ascending")
        assert "ProcessingJobSummaries" in resp
        assert isinstance(resp["ProcessingJobSummaries"], list)


class TestSageMakerListCompilationJobsFiltered:
    """List compilation jobs with filter parameters."""

    def test_list_compilation_jobs_sorted(self, sagemaker):
        """list_compilation_jobs with SortBy and SortOrder."""
        resp = sagemaker.list_compilation_jobs(SortBy="Name", SortOrder="Ascending")
        assert "CompilationJobSummaries" in resp
        assert isinstance(resp["CompilationJobSummaries"], list)


class TestSageMakerListTransformJobsFiltered:
    """List transform jobs with filter parameters."""

    def test_list_transform_jobs_sorted(self, sagemaker):
        """list_transform_jobs with SortBy and SortOrder."""
        resp = sagemaker.list_transform_jobs(SortBy="Name", SortOrder="Ascending")
        assert "TransformJobSummaries" in resp
        assert isinstance(resp["TransformJobSummaries"], list)


class TestSageMakerListHPTJobsFiltered:
    """List hyper parameter tuning jobs with filter parameters."""

    def test_list_hpt_jobs_sorted(self, sagemaker):
        """list_hyper_parameter_tuning_jobs with SortBy and SortOrder."""
        resp = sagemaker.list_hyper_parameter_tuning_jobs(SortBy="Name", SortOrder="Ascending")
        assert "HyperParameterTuningJobSummaries" in resp
        assert isinstance(resp["HyperParameterTuningJobSummaries"], list)


class TestSageMakerListExperimentsSorted:
    """List experiments with sort parameters."""

    def test_list_experiments_sorted(self, sagemaker):
        """list_experiments with SortBy and SortOrder."""
        resp = sagemaker.list_experiments(SortBy="Name", SortOrder="Ascending")
        assert "ExperimentSummaries" in resp
        assert isinstance(resp["ExperimentSummaries"], list)


class TestSageMakerListTrialsSorted:
    """List trials with sort parameters."""

    def test_list_trials_sorted(self, sagemaker):
        """list_trials with SortBy and SortOrder."""
        resp = sagemaker.list_trials(SortBy="Name", SortOrder="Ascending")
        assert "TrialSummaries" in resp
        assert isinstance(resp["TrialSummaries"], list)


class TestSageMakerListTrialComponentsSorted:
    """List trial components with sort parameters."""

    def test_list_trial_components_sorted(self, sagemaker):
        """list_trial_components with SortBy and SortOrder."""
        resp = sagemaker.list_trial_components(SortBy="Name", SortOrder="Ascending")
        assert "TrialComponentSummaries" in resp
        assert isinstance(resp["TrialComponentSummaries"], list)


class TestSageMakerListModelCardsSorted:
    """List model cards with sort parameters."""

    def test_list_model_cards_sorted(self, sagemaker):
        """list_model_cards with SortBy and SortOrder."""
        resp = sagemaker.list_model_cards(SortBy="Name", SortOrder="Ascending")
        assert "ModelCardSummaries" in resp
        assert isinstance(resp["ModelCardSummaries"], list)


class TestSageMakerListPipelinesSorted:
    """List pipelines with sort parameters."""

    def test_list_pipelines_sorted(self, sagemaker):
        """list_pipelines with SortBy and SortOrder."""
        resp = sagemaker.list_pipelines(SortBy="Name", SortOrder="Ascending")
        assert "PipelineSummaries" in resp
        assert isinstance(resp["PipelineSummaries"], list)


class TestSageMakerListDQJobDefsSorted:
    """List data quality job definitions with sort parameters."""

    def test_list_dq_job_definitions_sorted(self, sagemaker):
        """list_data_quality_job_definitions with SortBy and SortOrder."""
        resp = sagemaker.list_data_quality_job_definitions(SortBy="Name", SortOrder="Ascending")
        assert "JobDefinitionSummaries" in resp
        assert isinstance(resp["JobDefinitionSummaries"], list)


class TestSageMakerListModelPackageGroupsSorted:
    """List model package groups with sort parameters."""

    def test_list_model_package_groups_sorted(self, sagemaker):
        """list_model_package_groups with SortBy and SortOrder."""
        resp = sagemaker.list_model_package_groups(SortBy="Name", SortOrder="Ascending")
        assert "ModelPackageGroupSummaryList" in resp
        assert isinstance(resp["ModelPackageGroupSummaryList"], list)


class TestSageMakerListModelPackagesSorted:
    """List model packages with sort parameters."""

    def test_list_model_packages_sorted(self, sagemaker):
        """list_model_packages with SortBy and SortOrder."""
        resp = sagemaker.list_model_packages(SortBy="Name", SortOrder="Ascending")
        assert "ModelPackageSummaryList" in resp
        assert isinstance(resp["ModelPackageSummaryList"], list)


class TestSageMakerTagsOnTrial:
    """Tags CRUD on Trial resources."""

    def test_add_and_list_tags_on_trial(self, sagemaker):
        """add_tags and list_tags work on trial ARNs."""
        exp_name = _uid("exp")
        trial_name = _uid("trial")
        sagemaker.create_experiment(ExperimentName=exp_name)
        sagemaker.create_trial(TrialName=trial_name, ExperimentName=exp_name)
        try:
            trial_arn = sagemaker.describe_trial(TrialName=trial_name)["TrialArn"]
            sagemaker.add_tags(
                ResourceArn=trial_arn,
                Tags=[{"Key": "stage", "Value": "dev"}],
            )
            tags_resp = sagemaker.list_tags(ResourceArn=trial_arn)
            tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tags["stage"] == "dev"
        finally:
            sagemaker.delete_trial(TrialName=trial_name)
            sagemaker.delete_experiment(ExperimentName=exp_name)

    def test_delete_tags_on_trial(self, sagemaker):
        """delete_tags removes tags from a trial."""
        exp_name = _uid("exp")
        trial_name = _uid("trial")
        sagemaker.create_experiment(ExperimentName=exp_name)
        sagemaker.create_trial(TrialName=trial_name, ExperimentName=exp_name)
        try:
            trial_arn = sagemaker.describe_trial(TrialName=trial_name)["TrialArn"]
            sagemaker.add_tags(
                ResourceArn=trial_arn,
                Tags=[
                    {"Key": "stage", "Value": "dev"},
                    {"Key": "team", "Value": "ml"},
                ],
            )
            sagemaker.delete_tags(ResourceArn=trial_arn, TagKeys=["stage"])
            tags_resp = sagemaker.list_tags(ResourceArn=trial_arn)
            keys = [t["Key"] for t in tags_resp["Tags"]]
            assert "stage" not in keys
            assert "team" in keys
        finally:
            sagemaker.delete_trial(TrialName=trial_name)
            sagemaker.delete_experiment(ExperimentName=exp_name)


class TestSageMakerTagsOnTrialComponent:
    """Tags CRUD on TrialComponent resources."""

    def test_add_and_list_tags_on_trial_component(self, sagemaker):
        """add_tags and list_tags work on trial component ARNs."""
        tc_name = _uid("tc")
        resp = sagemaker.create_trial_component(TrialComponentName=tc_name)
        tc_arn = resp["TrialComponentArn"]
        try:
            sagemaker.add_tags(
                ResourceArn=tc_arn,
                Tags=[{"Key": "version", "Value": "1"}],
            )
            tags_resp = sagemaker.list_tags(ResourceArn=tc_arn)
            tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tags["version"] == "1"
        finally:
            sagemaker.delete_trial_component(TrialComponentName=tc_name)

    def test_delete_tags_on_trial_component(self, sagemaker):
        """delete_tags removes tags from a trial component."""
        tc_name = _uid("tc")
        resp = sagemaker.create_trial_component(TrialComponentName=tc_name)
        tc_arn = resp["TrialComponentArn"]
        try:
            sagemaker.add_tags(
                ResourceArn=tc_arn,
                Tags=[
                    {"Key": "version", "Value": "1"},
                    {"Key": "owner", "Value": "alice"},
                ],
            )
            sagemaker.delete_tags(ResourceArn=tc_arn, TagKeys=["version"])
            tags_resp = sagemaker.list_tags(ResourceArn=tc_arn)
            keys = [t["Key"] for t in tags_resp["Tags"]]
            assert "version" not in keys
            assert "owner" in keys
        finally:
            sagemaker.delete_trial_component(TrialComponentName=tc_name)


class TestSageMakerTagsOnModelCard:
    """Tags on ModelCard resources via create_model_card."""

    def test_create_model_card_with_tags(self, sagemaker):
        """create_model_card with Tags attaches tags at creation time."""
        name = _uid("mc")
        resp = sagemaker.create_model_card(
            ModelCardName=name,
            Content='{"model_overview":{"model_id":"test"}}',
            ModelCardStatus="Draft",
            Tags=[{"Key": "dept", "Value": "ai"}, {"Key": "env", "Value": "test"}],
        )
        arn = resp["ModelCardArn"]
        try:
            tags_resp = sagemaker.list_tags(ResourceArn=arn)
            tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tags["dept"] == "ai"
            assert tags["env"] == "test"
        finally:
            sagemaker.delete_model_card(ModelCardName=name)


class TestSageMakerTagsOnDQJobDef:
    """Tags on DataQualityJobDefinition via create with Tags."""

    def test_create_dq_job_definition_with_tags(self, sagemaker):
        """create_data_quality_job_definition with Tags attaches tags."""
        name = _uid("dq")
        resp = sagemaker.create_data_quality_job_definition(
            JobDefinitionName=name,
            DataQualityAppSpecification={
                "ImageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
            },
            DataQualityJobInput={
                "EndpointInput": {
                    "EndpointName": "fake-ep",
                    "LocalPath": "/opt/ml/processing/input",
                }
            },
            DataQualityJobOutputConfig={
                "MonitoringOutputs": [
                    {
                        "S3Output": {
                            "S3Uri": "s3://bucket/output",
                            "LocalPath": "/opt/ml/processing/output",
                        }
                    }
                ]
            },
            JobResources={
                "ClusterConfig": {
                    "InstanceCount": 1,
                    "InstanceType": "ml.m5.xlarge",
                    "VolumeSizeInGB": 10,
                }
            },
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            Tags=[{"Key": "dq-env", "Value": "test"}],
        )
        arn = resp["JobDefinitionArn"]
        try:
            tags_resp = sagemaker.list_tags(ResourceArn=arn)
            tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tags["dq-env"] == "test"
        finally:
            sagemaker.delete_data_quality_job_definition(JobDefinitionName=name)


class TestSageMakerTagsOnDomain:
    """Tags on Domain resources via create with Tags."""

    def test_create_domain_with_tags(self, sagemaker):
        """create_domain with Tags attaches tags at creation time."""
        name = _uid("dom")
        resp = sagemaker.create_domain(
            DomainName=name,
            AuthMode="IAM",
            DefaultUserSettings={"ExecutionRole": "arn:aws:iam::123456789012:role/SageMakerRole"},
            SubnetIds=["subnet-12345"],
            VpcId="vpc-12345",
            Tags=[{"Key": "env", "Value": "dev"}],
        )
        arn = resp["DomainArn"]
        try:
            tags_resp = sagemaker.list_tags(ResourceArn=arn)
            tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tags["env"] == "dev"
        finally:
            dom_id = arn.split("/")[-1]
            sagemaker.delete_domain(DomainId=dom_id)


class TestSageMakerAutoMLJobV2Detailed:
    """Detailed AutoMLJobV2 tests."""

    def test_describe_auto_ml_job_v2_fields(self, sagemaker):
        """describe_auto_ml_job_v2 returns expected fields."""
        name = _uid("aml")
        sagemaker.create_auto_ml_job_v2(
            AutoMLJobName=name,
            AutoMLJobInputDataConfig=[
                {
                    "ChannelType": "training",
                    "DataSource": {
                        "S3DataSource": {
                            "S3DataType": "S3Prefix",
                            "S3Uri": "s3://bucket/data",
                        }
                    },
                }
            ],
            OutputDataConfig={"S3OutputPath": "s3://bucket/output"},
            AutoMLProblemTypeConfig={
                "TabularJobConfig": {
                    "TargetAttributeName": "target",
                    "ProblemType": "BinaryClassification",
                    "CompletionCriteria": {"MaxCandidates": 10},
                }
            },
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        desc = sagemaker.describe_auto_ml_job_v2(AutoMLJobName=name)
        assert desc["AutoMLJobName"] == name
        assert "AutoMLJobArn" in desc
        assert desc["AutoMLJobStatus"] in ("InProgress", "Completed", "Failed", "Stopping")
        assert "CreationTime" in desc
        assert "OutputDataConfig" in desc
        assert "RoleArn" in desc

    def test_list_auto_ml_jobs_sorted(self, sagemaker):
        """list_auto_ml_jobs with SortBy and SortOrder."""
        resp = sagemaker.list_auto_ml_jobs(SortBy="Name", SortOrder="Ascending")
        assert "AutoMLJobSummaries" in resp
        assert isinstance(resp["AutoMLJobSummaries"], list)

    def test_list_auto_ml_jobs_status_filter(self, sagemaker):
        """list_auto_ml_jobs with StatusEquals filter."""
        resp = sagemaker.list_auto_ml_jobs(StatusEquals="InProgress")
        assert "AutoMLJobSummaries" in resp
        assert isinstance(resp["AutoMLJobSummaries"], list)


class TestSageMakerLifecycleConfigDetailed:
    """Detailed NotebookInstanceLifecycleConfig tests."""

    def test_lifecycle_config_on_create_and_on_start(self, sagemaker):
        """create lifecycle config with both OnCreate and OnStart scripts."""
        name = _uid("lc")
        resp = sagemaker.create_notebook_instance_lifecycle_config(
            NotebookInstanceLifecycleConfigName=name,
            OnCreate=[{"Content": "IyEvYmluL2Jhc2gKZWNobyBvbkNyZWF0ZQ=="}],
            OnStart=[{"Content": "IyEvYmluL2Jhc2gKZWNobyBvblN0YXJ0"}],
        )
        assert "NotebookInstanceLifecycleConfigArn" in resp
        try:
            desc = sagemaker.describe_notebook_instance_lifecycle_config(
                NotebookInstanceLifecycleConfigName=name
            )
            assert len(desc["OnCreate"]) == 1
            assert len(desc["OnStart"]) == 1
        finally:
            sagemaker.delete_notebook_instance_lifecycle_config(
                NotebookInstanceLifecycleConfigName=name
            )


class TestSageMakerModelVpcConfig:
    """Model with VpcConfig."""

    def test_create_model_with_vpc_config(self, sagemaker):
        """create_model with VpcConfig stores the config."""
        name = _uid("model")
        sagemaker.create_model(
            ModelName=name,
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            PrimaryContainer={
                "Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
            },
            VpcConfig={
                "SecurityGroupIds": ["sg-12345"],
                "Subnets": ["subnet-12345"],
            },
        )
        try:
            desc = sagemaker.describe_model(ModelName=name)
            assert "VpcConfig" in desc
            assert "sg-12345" in desc["VpcConfig"]["SecurityGroupIds"]
            assert "subnet-12345" in desc["VpcConfig"]["Subnets"]
        finally:
            sagemaker.delete_model(ModelName=name)


class TestSageMakerMultiVariantEndpointConfig:
    """Endpoint config with multiple production variants."""

    def test_endpoint_config_multiple_variants(self, sagemaker):
        """create_endpoint_config with two variants returns both in describe."""
        m1 = _uid("model")
        m2 = _uid("model")
        ec = _uid("ec")
        sagemaker.create_model(
            ModelName=m1,
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            PrimaryContainer={
                "Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
            },
        )
        sagemaker.create_model(
            ModelName=m2,
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            PrimaryContainer={
                "Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
            },
        )
        try:
            sagemaker.create_endpoint_config(
                EndpointConfigName=ec,
                ProductionVariants=[
                    {
                        "VariantName": "v1",
                        "ModelName": m1,
                        "InitialInstanceCount": 1,
                        "InstanceType": "ml.m4.xlarge",
                        "InitialVariantWeight": 0.7,
                    },
                    {
                        "VariantName": "v2",
                        "ModelName": m2,
                        "InitialInstanceCount": 1,
                        "InstanceType": "ml.m4.xlarge",
                        "InitialVariantWeight": 0.3,
                    },
                ],
            )
            desc = sagemaker.describe_endpoint_config(EndpointConfigName=ec)
            pvs = desc["ProductionVariants"]
            assert len(pvs) == 2
            variant_names = {pv["VariantName"] for pv in pvs}
            assert variant_names == {"v1", "v2"}
        finally:
            sagemaker.delete_endpoint_config(EndpointConfigName=ec)
            sagemaker.delete_model(ModelName=m1)
            sagemaker.delete_model(ModelName=m2)


class TestSageMakerDescribeNotFoundAdditional:
    """Describe operations return proper errors for non-existent resources."""

    def test_describe_processing_job_not_found_error_code(self, sagemaker):
        """DescribeProcessingJob returns ValidationException for fake job."""
        with pytest.raises(ClientError) as exc:
            sagemaker.describe_processing_job(ProcessingJobName="fake-pj-notfound-zzz")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_hyper_parameter_tuning_job_not_found_error_code(self, sagemaker):
        """DescribeHyperParameterTuningJob returns ResourceNotFound for fake job."""
        with pytest.raises(ClientError) as exc:
            sagemaker.describe_hyper_parameter_tuning_job(
                HyperParameterTuningJobName="fake-hpt-notfound-zzz"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFound"

    def test_describe_compilation_job_not_found_error_code(self, sagemaker):
        """DescribeCompilationJob returns ResourceNotFound for fake job."""
        with pytest.raises(ClientError) as exc:
            sagemaker.describe_compilation_job(CompilationJobName="fake-cj-notfound-zzz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFound"

    def test_describe_transform_job_not_found_error_code(self, sagemaker):
        """DescribeTransformJob returns ValidationException for fake job."""
        with pytest.raises(ClientError) as exc:
            sagemaker.describe_transform_job(TransformJobName="fake-tj-notfound-zzz")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_describe_lifecycle_config_not_found_error_code(self, sagemaker):
        """DescribeNotebookInstanceLifecycleConfig returns error for fake config."""
        with pytest.raises(ClientError) as exc:
            sagemaker.describe_notebook_instance_lifecycle_config(
                NotebookInstanceLifecycleConfigName="fake-lc-notfound-zzz"
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_delete_domain_not_found(self, sagemaker):
        """DeleteDomain returns ValidationException for fake domain."""
        with pytest.raises(ClientError) as exc:
            sagemaker.delete_domain(DomainId="d-nonexistent999")
        assert exc.value.response["Error"]["Code"] == "ValidationException"


class TestSageMakerStopOperations:
    """Tests for Stop* operations."""

    def test_stop_training_job(self, sagemaker):
        """StopTrainingJob accepts a job name."""
        resp = sagemaker.stop_training_job(TrainingJobName="fake-tj-stop-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_stop_transform_job(self, sagemaker):
        """StopTransformJob accepts a job name."""
        resp = sagemaker.stop_transform_job(TransformJobName="fake-transform-stop-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_stop_processing_job(self, sagemaker):
        """StopProcessingJob accepts a job name."""
        resp = sagemaker.stop_processing_job(ProcessingJobName="fake-pj-stop-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_stop_hyper_parameter_tuning_job(self, sagemaker):
        """StopHyperParameterTuningJob accepts a job name."""
        resp = sagemaker.stop_hyper_parameter_tuning_job(
            HyperParameterTuningJobName="fake-hpt-stop-zzz"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_stop_compilation_job(self, sagemaker):
        """StopCompilationJob accepts a job name."""
        resp = sagemaker.stop_compilation_job(CompilationJobName="fake-cj-stop-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_stop_labeling_job(self, sagemaker):
        """StopLabelingJob accepts a job name."""
        resp = sagemaker.stop_labeling_job(LabelingJobName="fake-lj-stop-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_stop_monitoring_schedule(self, sagemaker):
        """StopMonitoringSchedule accepts a schedule name."""
        resp = sagemaker.stop_monitoring_schedule(MonitoringScheduleName="fake-ms-stop-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_stop_optimization_job(self, sagemaker):
        """StopOptimizationJob accepts a job name."""
        resp = sagemaker.stop_optimization_job(OptimizationJobName="fake-oj-stop-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_stop_edge_packaging_job(self, sagemaker):
        """StopEdgePackagingJob accepts a job name."""
        resp = sagemaker.stop_edge_packaging_job(EdgePackagingJobName="fake-epj-stop-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_stop_inference_recommendations_job(self, sagemaker):
        """StopInferenceRecommendationsJob accepts a job name."""
        resp = sagemaker.stop_inference_recommendations_job(JobName="fake-irj-stop-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_stop_edge_deployment_stage(self, sagemaker):
        """StopEdgeDeploymentStage accepts plan and stage names."""
        resp = sagemaker.stop_edge_deployment_stage(
            EdgeDeploymentPlanName="fake-edp-stop-zzz", StageName="fake-stage"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_stop_mlflow_tracking_server(self, sagemaker):
        """StopMlflowTrackingServer returns TrackingServerArn."""
        resp = sagemaker.stop_mlflow_tracking_server(TrackingServerName="fake-mts-stop-zzz")
        assert "TrackingServerArn" in resp

    def test_stop_inference_experiment(self, sagemaker):
        """StopInferenceExperiment returns InferenceExperimentArn."""
        resp = sagemaker.stop_inference_experiment(
            Name="fake-ie-stop-zzz",
            ModelVariantActions={"variant1": "Retain"},
            DesiredModelVariants=[
                {
                    "ModelName": "model1",
                    "VariantName": "v1",
                    "InfrastructureConfig": {
                        "InfrastructureType": "RealTimeInference",
                        "RealTimeInferenceConfig": {
                            "InstanceType": "ml.m5.large",
                            "InstanceCount": 1,
                        },
                    },
                }
            ],
        )
        assert "InferenceExperimentArn" in resp


class TestSageMakerStartOperations:
    """Tests for Start* operations."""

    def test_start_edge_deployment_stage(self, sagemaker):
        """StartEdgeDeploymentStage accepts plan and stage names."""
        resp = sagemaker.start_edge_deployment_stage(
            EdgeDeploymentPlanName="fake-edp-start-zzz", StageName="fake-stage"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_start_inference_experiment(self, sagemaker):
        """StartInferenceExperiment returns InferenceExperimentArn."""
        resp = sagemaker.start_inference_experiment(Name="fake-ie-start-zzz")
        assert "InferenceExperimentArn" in resp

    def test_start_mlflow_tracking_server(self, sagemaker):
        """StartMlflowTrackingServer returns TrackingServerArn."""
        resp = sagemaker.start_mlflow_tracking_server(TrackingServerName="fake-mts-start-zzz")
        assert "TrackingServerArn" in resp

    def test_start_monitoring_schedule(self, sagemaker):
        """StartMonitoringSchedule accepts a schedule name."""
        resp = sagemaker.start_monitoring_schedule(MonitoringScheduleName="fake-ms-start-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerUpdateOperationsNotFound:
    """Tests for Update* operations that return errors for non-existent resources."""

    def test_update_workforce_not_found(self, sagemaker):
        """UpdateWorkforce returns ValidationException for non-existent workforce."""
        with pytest.raises(ClientError) as exc:
            sagemaker.update_workforce(WorkforceName="fake-wf-update-zzz")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_update_action_not_found(self, sagemaker):
        """UpdateAction returns ValidationException for non-existent action."""
        with pytest.raises(ClientError) as exc:
            sagemaker.update_action(ActionName="fake-action-update-zzz")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_update_artifact_not_found(self, sagemaker):
        """UpdateArtifact returns ValidationException for non-existent artifact."""
        with pytest.raises(ClientError) as exc:
            sagemaker.update_artifact(
                ArtifactArn="arn:aws:sagemaker:us-east-1:123456789012:artifact/fake-zzz"
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_update_context_not_found(self, sagemaker):
        """UpdateContext returns ValidationException for non-existent context."""
        with pytest.raises(ClientError) as exc:
            sagemaker.update_context(ContextName="fake-ctx-update-zzz")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_update_image_not_found(self, sagemaker):
        """UpdateImage returns ValidationException for non-existent image."""
        with pytest.raises(ClientError) as exc:
            sagemaker.update_image(ImageName="fake-img-update-zzz")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_update_project_not_found(self, sagemaker):
        """UpdateProject returns ValidationException for non-existent project."""
        with pytest.raises(ClientError) as exc:
            sagemaker.update_project(ProjectName="fake-proj-update-zzz")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_update_app_image_config_not_found(self, sagemaker):
        """UpdateAppImageConfig returns ValidationException for non-existent config."""
        with pytest.raises(ClientError) as exc:
            sagemaker.update_app_image_config(AppImageConfigName="fake-aic-update-zzz")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_update_device_fleet_not_found(self, sagemaker):
        """UpdateDeviceFleet returns ValidationException for non-existent fleet."""
        with pytest.raises(ClientError) as exc:
            sagemaker.update_device_fleet(
                DeviceFleetName="fake-df-update-zzz",
                OutputConfig={"S3OutputLocation": "s3://bucket/out"},
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_update_hub_not_found(self, sagemaker):
        """UpdateHub returns ValidationException for non-existent hub."""
        with pytest.raises(ClientError) as exc:
            sagemaker.update_hub(HubName="fake-hub-update-zzz")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_update_inference_experiment_not_found(self, sagemaker):
        """UpdateInferenceExperiment returns ValidationException."""
        with pytest.raises(ClientError) as exc:
            sagemaker.update_inference_experiment(
                Name="fake-ie-update-zzz",
                Schedule={"StartTime": "2026-01-01T00:00:00Z"},
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_update_monitoring_schedule_not_found(self, sagemaker):
        """UpdateMonitoringSchedule returns ValidationException."""
        with pytest.raises(ClientError) as exc:
            sagemaker.update_monitoring_schedule(
                MonitoringScheduleName="fake-ms-update-zzz",
                MonitoringScheduleConfig={"MonitoringJobDefinitionName": "fake"},
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_update_space_not_found(self, sagemaker):
        """UpdateSpace returns ValidationException for non-existent space."""
        with pytest.raises(ClientError) as exc:
            sagemaker.update_space(DomainId="d-fakeupdate999", SpaceName="fake-space-update")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_update_user_profile_not_found(self, sagemaker):
        """UpdateUserProfile returns ValidationException."""
        with pytest.raises(ClientError) as exc:
            sagemaker.update_user_profile(
                DomainId="d-fakeupdate999", UserProfileName="fake-up-update"
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_update_inference_component_not_found(self, sagemaker):
        """UpdateInferenceComponent returns ValidationException."""
        with pytest.raises(ClientError) as exc:
            sagemaker.update_inference_component(InferenceComponentName="fake-ic-update-zzz")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_update_mlflow_tracking_server_not_found(self, sagemaker):
        """UpdateMlflowTrackingServer returns ValidationException."""
        with pytest.raises(ClientError) as exc:
            sagemaker.update_mlflow_tracking_server(TrackingServerName="fake-mts-update-zzz")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_update_partner_app_not_found(self, sagemaker):
        """UpdatePartnerApp returns ValidationException."""
        with pytest.raises(ClientError) as exc:
            sagemaker.update_partner_app(
                Arn="arn:aws:sagemaker:us-east-1:123456789012:partner-app/fake-pa-zzz"
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"


class TestSageMakerUpdateOperationsSuccess:
    """Tests for Update* operations that succeed even with non-existent resources."""

    def test_update_experiment(self, sagemaker):
        """UpdateExperiment accepts a non-existent experiment name."""
        resp = sagemaker.update_experiment(ExperimentName="fake-exp-update-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_trial(self, sagemaker):
        """UpdateTrial accepts a non-existent trial name."""
        resp = sagemaker.update_trial(TrialName="fake-trial-update-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_notebook_instance(self, sagemaker):
        """UpdateNotebookInstance accepts a non-existent instance name."""
        resp = sagemaker.update_notebook_instance(NotebookInstanceName="fake-nb-update-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_notebook_instance_lifecycle_config(self, sagemaker):
        """UpdateNotebookInstanceLifecycleConfig accepts non-existent config."""
        resp = sagemaker.update_notebook_instance_lifecycle_config(
            NotebookInstanceLifecycleConfigName="fake-lc-update-zzz"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_domain(self, sagemaker):
        """UpdateDomain returns DomainArn."""
        resp = sagemaker.update_domain(DomainId="d-fakeupdate999")
        assert "DomainArn" in resp

    def test_update_image_version(self, sagemaker):
        """UpdateImageVersion returns ImageVersionArn."""
        resp = sagemaker.update_image_version(ImageName="fake-img-ver-zzz", Version=1)
        assert "ImageVersionArn" in resp

    def test_update_training_job(self, sagemaker):
        """UpdateTrainingJob returns TrainingJobArn."""
        resp = sagemaker.update_training_job(TrainingJobName="fake-tj-update-zzz")
        assert "TrainingJobArn" in resp

    def test_update_pipeline_execution(self, sagemaker):
        """UpdatePipelineExecution returns PipelineExecutionArn."""
        resp = sagemaker.update_pipeline_execution(
            PipelineExecutionArn="arn:aws:sagemaker:us-east-1:123456789012:pipeline/fake/execution/fake"
        )
        assert "PipelineExecutionArn" in resp

    def test_update_feature_group(self, sagemaker):
        """UpdateFeatureGroup accepts a non-existent feature group name."""
        resp = sagemaker.update_feature_group(FeatureGroupName="fake-fg-update-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_feature_metadata(self, sagemaker):
        """UpdateFeatureMetadata accepts non-existent feature group."""
        resp = sagemaker.update_feature_metadata(
            FeatureGroupName="fake-fg-meta-zzz", FeatureName="feat1"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_cluster(self, sagemaker):
        """UpdateCluster returns ClusterArn."""
        resp = sagemaker.update_cluster(
            ClusterName="fake-cluster-update-zzz",
            InstanceGroups=[
                {
                    "InstanceCount": 1,
                    "InstanceGroupName": "grp1",
                    "InstanceType": "ml.m5.large",
                    "LifeCycleConfig": {
                        "SourceS3Uri": "s3://bucket/path",
                        "OnCreate": "script.sh",
                    },
                    "ExecutionRole": "arn:aws:iam::123456789012:role/Role",
                }
            ],
        )
        assert "ClusterArn" in resp

    def test_update_cluster_software(self, sagemaker):
        """UpdateClusterSoftware returns ClusterArn."""
        resp = sagemaker.update_cluster_software(ClusterName="fake-cluster-sw-zzz")
        assert "ClusterArn" in resp

    def test_update_cluster_scheduler_config(self, sagemaker):
        """UpdateClusterSchedulerConfig returns ClusterSchedulerConfigArn."""
        resp = sagemaker.update_cluster_scheduler_config(
            ClusterSchedulerConfigId="fake-csc-update-zzz",
            TargetVersion=1,
            SchedulerConfig={"PriorityClasses": [{"Name": "default", "Weight": 1}]},
        )
        assert "ClusterSchedulerConfigArn" in resp

    def test_update_compute_quota(self, sagemaker):
        """UpdateComputeQuota returns ComputeQuotaArn."""
        resp = sagemaker.update_compute_quota(ComputeQuotaId="fake-cq-update-zzz", TargetVersion=1)
        assert "ComputeQuotaArn" in resp

    def test_update_devices(self, sagemaker):
        """UpdateDevices accepts device fleet and devices."""
        resp = sagemaker.update_devices(
            DeviceFleetName="fake-df-update-zzz",
            Devices=[{"DeviceName": "dev1"}],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_hub_content(self, sagemaker):
        """UpdateHubContent returns HubArn and HubContentArn."""
        resp = sagemaker.update_hub_content(
            HubName="fake-hub-hc-zzz",
            HubContentName="fake-hc",
            HubContentType="Model",
            HubContentVersion="1.0.0",
        )
        assert "HubArn" in resp
        assert "HubContentArn" in resp

    def test_update_inference_component_runtime_config(self, sagemaker):
        """UpdateInferenceComponentRuntimeConfig accepts non-existent component."""
        resp = sagemaker.update_inference_component_runtime_config(
            InferenceComponentName="fake-icrc-update-zzz",
            DesiredRuntimeConfig={"CopyCount": 1},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_monitoring_alert(self, sagemaker):
        """UpdateMonitoringAlert returns MonitoringScheduleArn."""
        resp = sagemaker.update_monitoring_alert(
            MonitoringScheduleName="fake-ms-alert-zzz",
            MonitoringAlertName="fake-alert",
            DatapointsToAlert=1,
            EvaluationPeriod=1,
        )
        assert "MonitoringScheduleArn" in resp


class TestSageMakerBatchOperations:
    """Tests for Batch* operations."""

    def test_batch_delete_cluster_nodes(self, sagemaker):
        """BatchDeleteClusterNodes accepts cluster name and node IDs."""
        resp = sagemaker.batch_delete_cluster_nodes(
            ClusterName="fake-cluster-bd-zzz", NodeIds=["mi-fake123"]
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_batch_reboot_cluster_nodes(self, sagemaker):
        """BatchRebootClusterNodes accepts cluster name and node IDs."""
        resp = sagemaker.batch_reboot_cluster_nodes(
            ClusterName="fake-cluster-reboot-zzz", NodeIds=["mi-fake123"]
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_batch_replace_cluster_nodes(self, sagemaker):
        """BatchReplaceClusterNodes accepts cluster name and node IDs."""
        resp = sagemaker.batch_replace_cluster_nodes(
            ClusterName="fake-cluster-replace-zzz", NodeIds=["mi-fake123"]
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerActionCRUD:
    def test_create_action(self, sagemaker):
        name = _uid("action")
        resp = sagemaker.create_action(
            ActionName=name,
            Source={"SourceUri": "s3://my-bucket/my-source"},
            ActionType="ModelDeployment",
        )
        assert "ActionArn" in resp
        sagemaker.delete_action(ActionName=name)

    def test_delete_action(self, sagemaker):
        name = _uid("action")
        sagemaker.create_action(
            ActionName=name,
            Source={"SourceUri": "s3://my-bucket/del-source"},
            ActionType="ModelDeployment",
        )
        resp = sagemaker.delete_action(ActionName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerAppImageConfigCRUD:
    def test_create_app_image_config(self, sagemaker):
        name = _uid("appimg")
        resp = sagemaker.create_app_image_config(AppImageConfigName=name)
        assert "AppImageConfigArn" in resp
        sagemaker.delete_app_image_config(AppImageConfigName=name)

    def test_delete_app_image_config(self, sagemaker):
        name = _uid("appimg")
        sagemaker.create_app_image_config(AppImageConfigName=name)
        resp = sagemaker.delete_app_image_config(AppImageConfigName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerArtifactCRUD:
    def test_create_artifact(self, sagemaker):
        resp = sagemaker.create_artifact(
            Source={"SourceUri": f"s3://my-bucket/{_uid('artifact')}"},
            ArtifactType="Dataset",
        )
        assert "ArtifactArn" in resp
        sagemaker.delete_artifact(ArtifactArn=resp["ArtifactArn"])

    def test_delete_artifact(self, sagemaker):
        resp = sagemaker.create_artifact(
            Source={"SourceUri": f"s3://my-bucket/{_uid('art-del')}"},
            ArtifactType="Dataset",
        )
        del_resp = sagemaker.delete_artifact(ArtifactArn=resp["ArtifactArn"])
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerContextCRUD:
    def test_create_context(self, sagemaker):
        name = _uid("ctx")
        resp = sagemaker.create_context(
            ContextName=name,
            Source={"SourceUri": "s3://my-bucket/my-ctx-source"},
            ContextType="Endpoint",
        )
        assert "ContextArn" in resp
        sagemaker.delete_context(ContextName=name)

    def test_delete_context(self, sagemaker):
        name = _uid("ctx")
        sagemaker.create_context(
            ContextName=name,
            Source={"SourceUri": "s3://my-bucket/del-ctx"},
            ContextType="Endpoint",
        )
        resp = sagemaker.delete_context(ContextName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerCodeRepositoryCRUD:
    def test_create_code_repository(self, sagemaker):
        name = _uid("repo")
        resp = sagemaker.create_code_repository(
            CodeRepositoryName=name,
            GitConfig={"RepositoryUrl": "https://github.com/example/repo.git"},
        )
        assert "CodeRepositoryArn" in resp
        sagemaker.delete_code_repository(CodeRepositoryName=name)

    def test_delete_code_repository(self, sagemaker):
        name = _uid("repo")
        sagemaker.create_code_repository(
            CodeRepositoryName=name,
            GitConfig={"RepositoryUrl": "https://github.com/example/del-repo.git"},
        )
        resp = sagemaker.delete_code_repository(CodeRepositoryName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerImageCRUD:
    def test_create_image(self, sagemaker):
        name = _uid("img")
        resp = sagemaker.create_image(
            ImageName=name,
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        assert "ImageArn" in resp
        sagemaker.delete_image(ImageName=name)

    def test_delete_image(self, sagemaker):
        name = _uid("img")
        sagemaker.create_image(
            ImageName=name,
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        resp = sagemaker.delete_image(ImageName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerProjectCRUD:
    def test_create_project(self, sagemaker):
        name = _uid("proj")
        resp = sagemaker.create_project(
            ProjectName=name,
            ServiceCatalogProvisioningDetails={"ProductId": "prod-abc123"},
        )
        assert "ProjectArn" in resp
        sagemaker.delete_project(ProjectName=name)

    def test_delete_project(self, sagemaker):
        name = _uid("proj")
        sagemaker.create_project(
            ProjectName=name,
            ServiceCatalogProvisioningDetails={"ProductId": "prod-abc123"},
        )
        resp = sagemaker.delete_project(ProjectName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerWorkforceCRUD:
    def test_create_workforce(self, sagemaker):
        name = _uid("wf")
        resp = sagemaker.create_workforce(WorkforceName=name)
        assert "WorkforceArn" in resp
        sagemaker.delete_workforce(WorkforceName=name)

    def test_delete_workforce(self, sagemaker):
        name = _uid("wf")
        sagemaker.create_workforce(WorkforceName=name)
        resp = sagemaker.delete_workforce(WorkforceName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerStudioLifecycleConfigCRUD:
    def test_create_studio_lifecycle_config(self, sagemaker):
        name = _uid("slc")
        resp = sagemaker.create_studio_lifecycle_config(
            StudioLifecycleConfigName=name,
            StudioLifecycleConfigContent="echo hello",
            StudioLifecycleConfigAppType="JupyterServer",
        )
        assert "StudioLifecycleConfigArn" in resp
        sagemaker.delete_studio_lifecycle_config(StudioLifecycleConfigName=name)

    def test_delete_studio_lifecycle_config(self, sagemaker):
        name = _uid("slc")
        sagemaker.create_studio_lifecycle_config(
            StudioLifecycleConfigName=name,
            StudioLifecycleConfigContent="echo hello",
            StudioLifecycleConfigAppType="JupyterServer",
        )
        resp = sagemaker.delete_studio_lifecycle_config(StudioLifecycleConfigName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerDeviceFleetCRUD:
    def test_create_device_fleet(self, sagemaker):
        name = _uid("df")
        resp = sagemaker.create_device_fleet(
            DeviceFleetName=name,
            OutputConfig={"S3OutputLocation": "s3://my-bucket/device-output"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        sagemaker.delete_device_fleet(DeviceFleetName=name)

    def test_delete_device_fleet(self, sagemaker):
        name = _uid("df")
        sagemaker.create_device_fleet(
            DeviceFleetName=name,
            OutputConfig={"S3OutputLocation": "s3://my-bucket/device-output"},
        )
        resp = sagemaker.delete_device_fleet(DeviceFleetName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerHubCRUD:
    def test_create_hub(self, sagemaker):
        name = _uid("hub")
        resp = sagemaker.create_hub(
            HubName=name,
            HubDescription="Test hub",
        )
        assert "HubArn" in resp
        sagemaker.delete_hub(HubName=name)

    def test_delete_hub(self, sagemaker):
        name = _uid("hub")
        sagemaker.create_hub(HubName=name, HubDescription="Del hub")
        resp = sagemaker.delete_hub(HubName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerHumanTaskUiCRUD:
    def test_create_human_task_ui(self, sagemaker):
        name = _uid("htui")
        resp = sagemaker.create_human_task_ui(
            HumanTaskUiName=name,
            UiTemplate={"Content": "<html>task template</html>"},
        )
        assert "HumanTaskUiArn" in resp
        sagemaker.delete_human_task_ui(HumanTaskUiName=name)

    def test_delete_human_task_ui(self, sagemaker):
        name = _uid("htui")
        sagemaker.create_human_task_ui(
            HumanTaskUiName=name,
            UiTemplate={"Content": "<html>del template</html>"},
        )
        resp = sagemaker.delete_human_task_ui(HumanTaskUiName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerFlowDefinitionCRUD:
    def test_create_flow_definition(self, sagemaker):
        name = _uid("flow")
        resp = sagemaker.create_flow_definition(
            FlowDefinitionName=name,
            OutputConfig={"S3OutputPath": "s3://my-bucket/flow-output"},
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            HumanLoopConfig={
                "WorkteamArn": "arn:aws:sagemaker:us-east-1:123456789012:workteam/default/test",
                "HumanTaskUiArn": "arn:aws:sagemaker:us-east-1:123456789012:human-task-ui/test",
                "TaskTitle": "Test Task",
                "TaskDescription": "Test",
                "TaskCount": 1,
            },
        )
        assert "FlowDefinitionArn" in resp
        sagemaker.delete_flow_definition(FlowDefinitionName=name)

    def test_delete_flow_definition(self, sagemaker):
        name = _uid("flow")
        sagemaker.create_flow_definition(
            FlowDefinitionName=name,
            OutputConfig={"S3OutputPath": "s3://my-bucket/flow-output"},
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            HumanLoopConfig={
                "WorkteamArn": "arn:aws:sagemaker:us-east-1:123456789012:workteam/default/test",
                "HumanTaskUiArn": "arn:aws:sagemaker:us-east-1:123456789012:human-task-ui/test",
                "TaskTitle": "Test Task",
                "TaskDescription": "Test",
                "TaskCount": 1,
            },
        )
        resp = sagemaker.delete_flow_definition(FlowDefinitionName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerMonitoringScheduleCRUD:
    def test_create_monitoring_schedule(self, sagemaker):
        name = _uid("ms")
        resp = sagemaker.create_monitoring_schedule(
            MonitoringScheduleName=name,
            MonitoringScheduleConfig={
                "MonitoringJobDefinition": {
                    "MonitoringInputs": [
                        {
                            "EndpointInput": {
                                "EndpointName": "fake-endpoint",
                                "LocalPath": "/opt/ml/input",
                            }
                        }
                    ],
                    "MonitoringOutputConfig": {
                        "MonitoringOutputs": [
                            {
                                "S3Output": {
                                    "S3Uri": "s3://my-bucket/mon-output",
                                    "LocalPath": "/opt/ml/output",
                                }
                            }
                        ]
                    },
                    "MonitoringResources": {
                        "ClusterConfig": {
                            "InstanceCount": 1,
                            "InstanceType": "ml.m4.xlarge",
                            "VolumeSizeInGB": 10,
                        }
                    },
                    "MonitoringAppSpecification": {
                        "ImageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
                    },
                    "RoleArn": "arn:aws:iam::123456789012:role/SageMakerRole",
                }
            },
        )
        assert "MonitoringScheduleArn" in resp
        sagemaker.delete_monitoring_schedule(MonitoringScheduleName=name)

    def test_delete_monitoring_schedule(self, sagemaker):
        name = _uid("ms")
        sagemaker.create_monitoring_schedule(
            MonitoringScheduleName=name,
            MonitoringScheduleConfig={
                "MonitoringJobDefinition": {
                    "MonitoringInputs": [
                        {
                            "EndpointInput": {
                                "EndpointName": "fake-endpoint",
                                "LocalPath": "/opt/ml/input",
                            }
                        }
                    ],
                    "MonitoringOutputConfig": {
                        "MonitoringOutputs": [
                            {
                                "S3Output": {
                                    "S3Uri": "s3://my-bucket/mon-output",
                                    "LocalPath": "/opt/ml/output",
                                }
                            }
                        ]
                    },
                    "MonitoringResources": {
                        "ClusterConfig": {
                            "InstanceCount": 1,
                            "InstanceType": "ml.m4.xlarge",
                            "VolumeSizeInGB": 10,
                        }
                    },
                    "MonitoringAppSpecification": {
                        "ImageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
                    },
                    "RoleArn": "arn:aws:iam::123456789012:role/SageMakerRole",
                }
            },
        )
        resp = sagemaker.delete_monitoring_schedule(MonitoringScheduleName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerDeleteAlgorithm:
    def test_delete_algorithm_no_error(self, sagemaker):
        """DeleteAlgorithm with fake name should not raise (server returns 200)."""
        resp = sagemaker.delete_algorithm(AlgorithmName="fake-algo-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerDeleteAssociation:
    def test_delete_association(self, sagemaker):
        """Create two artifacts, associate them, then delete the association."""
        src = sagemaker.create_artifact(
            Source={"SourceUri": f"s3://my-bucket/{_uid('assoc-src')}"},
            ArtifactType="Dataset",
        )
        dst = sagemaker.create_artifact(
            Source={"SourceUri": f"s3://my-bucket/{_uid('assoc-dst')}"},
            ArtifactType="Model",
        )
        sagemaker.add_association(
            SourceArn=src["ArtifactArn"],
            DestinationArn=dst["ArtifactArn"],
        )
        resp = sagemaker.delete_association(
            SourceArn=src["ArtifactArn"],
            DestinationArn=dst["ArtifactArn"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        sagemaker.delete_artifact(ArtifactArn=src["ArtifactArn"])
        sagemaker.delete_artifact(ArtifactArn=dst["ArtifactArn"])


class TestSageMakerDeleteFeatureGroup:
    def test_delete_feature_group_no_error(self, sagemaker):
        """DeleteFeatureGroup with fake name returns 200."""
        resp = sagemaker.delete_feature_group(FeatureGroupName="fake-fg-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerDeleteModelPackage:
    def test_delete_model_package(self, sagemaker):
        grp_name = _uid("mpg")
        sagemaker.create_model_package_group(ModelPackageGroupName=grp_name)
        pkg = sagemaker.create_model_package(ModelPackageGroupName=grp_name)
        resp = sagemaker.delete_model_package(ModelPackageName=pkg["ModelPackageArn"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        sagemaker.delete_model_package_group(ModelPackageGroupName=grp_name)


class TestSageMakerDeleteModelPackageGroup:
    def test_delete_model_package_group(self, sagemaker):
        name = _uid("mpg")
        sagemaker.create_model_package_group(ModelPackageGroupName=name)
        resp = sagemaker.delete_model_package_group(ModelPackageGroupName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerDeleteModelPackageGroupPolicy:
    def test_delete_model_package_group_policy(self, sagemaker):
        name = _uid("mpgp")
        sagemaker.create_model_package_group(ModelPackageGroupName=name)
        try:
            resp = sagemaker.delete_model_package_group_policy(ModelPackageGroupName=name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            sagemaker.delete_model_package_group(ModelPackageGroupName=name)


class TestSageMakerDeleteInferenceExperiment:
    def test_delete_inference_experiment_no_error(self, sagemaker):
        """DeleteInferenceExperiment with fake name returns 200."""
        resp = sagemaker.delete_inference_experiment(Name="fake-ie-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerDeleteProcessingJob:
    def test_delete_processing_job_no_error(self, sagemaker):
        """DeleteProcessingJob with fake name returns 200."""
        resp = sagemaker.delete_processing_job(ProcessingJobName="fake-pj-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerDeleteTrainingJob:
    def test_delete_training_job_no_error(self, sagemaker):
        """DeleteTrainingJob with fake name returns 200."""
        resp = sagemaker.delete_training_job(TrainingJobName="fake-tj-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerDeleteWorkteam:
    def test_delete_workteam(self, sagemaker):
        resp = sagemaker.delete_workteam(WorkteamName="fake-wt-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerEdgeDeploymentPlanCRUD:
    def test_create_edge_deployment_plan(self, sagemaker):
        name = _uid("edp")
        df_name = _uid("df")
        sagemaker.create_device_fleet(
            DeviceFleetName=df_name,
            OutputConfig={"S3OutputLocation": "s3://my-bucket/device-output"},
        )
        try:
            resp = sagemaker.create_edge_deployment_plan(
                EdgeDeploymentPlanName=name,
                DeviceFleetName=df_name,
                ModelConfigs=[
                    {
                        "ModelHandle": "model-handle",
                        "EdgePackagingJobName": "fake-epj",
                    }
                ],
            )
            assert "EdgeDeploymentPlanArn" in resp
            sagemaker.delete_edge_deployment_plan(EdgeDeploymentPlanName=name)
        finally:
            sagemaker.delete_device_fleet(DeviceFleetName=df_name)

    def test_delete_edge_deployment_plan(self, sagemaker):
        name = _uid("edp")
        df_name = _uid("df")
        sagemaker.create_device_fleet(
            DeviceFleetName=df_name,
            OutputConfig={"S3OutputLocation": "s3://my-bucket/device-output"},
        )
        sagemaker.create_edge_deployment_plan(
            EdgeDeploymentPlanName=name,
            DeviceFleetName=df_name,
            ModelConfigs=[
                {
                    "ModelHandle": "model-handle",
                    "EdgePackagingJobName": "fake-epj",
                }
            ],
        )
        resp = sagemaker.delete_edge_deployment_plan(EdgeDeploymentPlanName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        sagemaker.delete_device_fleet(DeviceFleetName=df_name)


class TestSageMakerEdgeDeploymentStageCRUD:
    def test_create_edge_deployment_stage(self, sagemaker):
        edp_name = _uid("edp")
        df_name = _uid("df")
        sagemaker.create_device_fleet(
            DeviceFleetName=df_name,
            OutputConfig={"S3OutputLocation": "s3://my-bucket/device-output"},
        )
        sagemaker.create_edge_deployment_plan(
            EdgeDeploymentPlanName=edp_name,
            DeviceFleetName=df_name,
            ModelConfigs=[
                {
                    "ModelHandle": "model-handle",
                    "EdgePackagingJobName": "fake-epj",
                }
            ],
        )
        try:
            resp = sagemaker.create_edge_deployment_stage(
                EdgeDeploymentPlanName=edp_name,
                Stages=[
                    {
                        "StageName": "stage1",
                        "DeviceSelectionConfig": {
                            "DeviceSubsetType": "PERCENTAGE",
                            "Percentage": 100,
                        },
                    }
                ],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            sagemaker.delete_edge_deployment_plan(EdgeDeploymentPlanName=edp_name)
            sagemaker.delete_device_fleet(DeviceFleetName=df_name)

    def test_delete_edge_deployment_stage(self, sagemaker):
        edp_name = _uid("edp")
        df_name = _uid("df")
        sagemaker.create_device_fleet(
            DeviceFleetName=df_name,
            OutputConfig={"S3OutputLocation": "s3://my-bucket/device-output"},
        )
        sagemaker.create_edge_deployment_plan(
            EdgeDeploymentPlanName=edp_name,
            DeviceFleetName=df_name,
            ModelConfigs=[
                {
                    "ModelHandle": "model-handle",
                    "EdgePackagingJobName": "fake-epj",
                }
            ],
            Stages=[
                {
                    "StageName": "stage1",
                    "DeviceSelectionConfig": {
                        "DeviceSubsetType": "PERCENTAGE",
                        "Percentage": 100,
                    },
                }
            ],
        )
        resp = sagemaker.delete_edge_deployment_stage(
            EdgeDeploymentPlanName=edp_name,
            StageName="stage1",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        sagemaker.delete_edge_deployment_plan(EdgeDeploymentPlanName=edp_name)
        sagemaker.delete_device_fleet(DeviceFleetName=df_name)


class TestSageMakerEdgePackagingJobCRUD:
    def test_create_edge_packaging_job(self, sagemaker):
        name = _uid("epj")
        resp = sagemaker.create_edge_packaging_job(
            EdgePackagingJobName=name,
            CompilationJobName="fake-compilation-job",
            ModelName="fake-model",
            ModelVersion="1.0",
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            OutputConfig={"S3OutputLocation": "s3://my-bucket/edge-output"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerMiscOperations:
    """Tests for Send*, Add*, Search*, Put*, Register*, Deregister*, Enable*, Disable*."""

    def test_add_association(self, sagemaker):
        """AddAssociation returns SourceArn and DestinationArn."""
        resp = sagemaker.add_association(
            SourceArn="arn:aws:sagemaker:us-east-1:123456789012:experiment/fake-src",
            DestinationArn="arn:aws:sagemaker:us-east-1:123456789012:experiment/fake-dst",
        )
        assert "SourceArn" in resp
        assert "DestinationArn" in resp

    def test_put_model_package_group_policy(self, sagemaker):
        """PutModelPackageGroupPolicy returns ModelPackageGroupArn."""
        resp = sagemaker.put_model_package_group_policy(
            ModelPackageGroupName="fake-mpg-policy-zzz",
            ResourcePolicy='{"Version":"2012-10-17","Statement":[]}',
        )
        assert "ModelPackageGroupArn" in resp

    def test_enable_sagemaker_servicecatalog_portfolio(self, sagemaker):
        """EnableSagemakerServicecatalogPortfolio succeeds."""
        resp = sagemaker.enable_sagemaker_servicecatalog_portfolio()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disable_sagemaker_servicecatalog_portfolio(self, sagemaker):
        """DisableSagemakerServicecatalogPortfolio succeeds."""
        resp = sagemaker.disable_sagemaker_servicecatalog_portfolio()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_register_devices(self, sagemaker):
        """RegisterDevices accepts device fleet and devices."""
        resp = sagemaker.register_devices(
            DeviceFleetName="fake-df-reg-zzz",
            Devices=[{"DeviceName": "dev1"}],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_deregister_devices(self, sagemaker):
        """DeregisterDevices accepts device fleet and device names."""
        resp = sagemaker.deregister_devices(
            DeviceFleetName="fake-df-dereg-zzz",
            DeviceNames=["dev1"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_send_pipeline_execution_step_failure(self, sagemaker):
        """SendPipelineExecutionStepFailure returns PipelineExecutionArn."""
        resp = sagemaker.send_pipeline_execution_step_failure(CallbackToken="fake-token-zzz")
        assert "PipelineExecutionArn" in resp

    def test_send_pipeline_execution_step_success(self, sagemaker):
        """SendPipelineExecutionStepSuccess returns PipelineExecutionArn."""
        resp = sagemaker.send_pipeline_execution_step_success(CallbackToken="fake-token-zzz")
        assert "PipelineExecutionArn" in resp

    def test_search_training_plan_offerings(self, sagemaker):
        """SearchTrainingPlanOfferings returns TrainingPlanOfferings."""
        resp = sagemaker.search_training_plan_offerings(
            InstanceType="ml.p4d.24xlarge",
            InstanceCount=1,
            DurationHours=1,
            StartTimeAfter="2026-01-01T00:00:00Z",
            EndTimeBefore="2026-12-31T00:00:00Z",
        )
        assert "TrainingPlanOfferings" in resp
        assert isinstance(resp["TrainingPlanOfferings"], list)


class TestSageMakerImageVersionCRUD:
    def test_create_image_version(self, sagemaker):
        img_name = _uid("img")
        sagemaker.create_image(
            ImageName=img_name,
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        try:
            resp = sagemaker.create_image_version(
                BaseImage="123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
                ImageName=img_name,
            )
            assert "ImageVersionArn" in resp
        finally:
            sagemaker.delete_image(ImageName=img_name)

    def test_delete_image_version(self, sagemaker):
        img_name = _uid("img")
        sagemaker.create_image(
            ImageName=img_name,
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        sagemaker.create_image_version(
            BaseImage="123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
            ImageName=img_name,
        )
        resp = sagemaker.delete_image_version(ImageName=img_name, Version=1)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        sagemaker.delete_image(ImageName=img_name)


class TestSageMakerPresignedNotebookInstanceUrl:
    def test_create_presigned_notebook_instance_url(self, sagemaker):
        name = _uid("nb")
        sagemaker.create_notebook_instance(
            NotebookInstanceName=name,
            InstanceType="ml.t2.medium",
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        try:
            resp = sagemaker.create_presigned_notebook_instance_url(NotebookInstanceName=name)
            assert "AuthorizedUrl" in resp
        finally:
            sagemaker.stop_notebook_instance(NotebookInstanceName=name)
            sagemaker.delete_notebook_instance(NotebookInstanceName=name)


class TestSageMakerMlflowTrackingServerCRUD:
    def test_create_mlflow_tracking_server(self, sagemaker):
        name = _uid("mlf")
        resp = sagemaker.create_mlflow_tracking_server(
            TrackingServerName=name,
            ArtifactStoreUri="s3://my-bucket/mlflow",
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        assert "TrackingServerArn" in resp
        sagemaker.delete_mlflow_tracking_server(TrackingServerName=name)

    def test_delete_mlflow_tracking_server(self, sagemaker):
        name = _uid("mlf")
        sagemaker.create_mlflow_tracking_server(
            TrackingServerName=name,
            ArtifactStoreUri="s3://my-bucket/mlflow",
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        resp = sagemaker.delete_mlflow_tracking_server(TrackingServerName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerDeleteInferenceComponent:
    def test_delete_inference_component_no_error(self, sagemaker):
        resp = sagemaker.delete_inference_component(InferenceComponentName="fake-ic-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerDeleteOptimizationJob:
    def test_delete_optimization_job_no_error(self, sagemaker):
        resp = sagemaker.delete_optimization_job(OptimizationJobName="fake-oj-zzz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerDeleteHubContent:
    def test_delete_hub_content(self, sagemaker):
        name = _uid("hub")
        sagemaker.create_hub(HubName=name, HubDescription="Test hub for content")
        try:
            resp = sagemaker.delete_hub_content(
                HubName=name,
                HubContentName="fake-content",
                HubContentType="Model",
                HubContentVersion="1.0.0",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            sagemaker.delete_hub(HubName=name)


class TestSageMakerDeleteHubContentReference:
    def test_delete_hub_content_reference(self, sagemaker):
        name = _uid("hub")
        sagemaker.create_hub(HubName=name, HubDescription="Test hub for ref")
        try:
            resp = sagemaker.delete_hub_content_reference(
                HubName=name,
                HubContentName="fake-ref",
                HubContentType="Model",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            sagemaker.delete_hub(HubName=name)


class TestSageMakerUserProfileCRUD:
    def test_create_user_profile(self, sagemaker):
        domain_name = _uid("dom")
        dom = sagemaker.create_domain(
            DomainName=domain_name,
            AuthMode="IAM",
            DefaultUserSettings={"ExecutionRole": "arn:aws:iam::123456789012:role/SageMakerRole"},
            SubnetIds=["subnet-12345"],
            VpcId="vpc-12345",
        )
        domain_id = dom["DomainArn"].split("/")[-1]
        try:
            up_name = _uid("up")
            resp = sagemaker.create_user_profile(
                DomainId=domain_id,
                UserProfileName=up_name,
            )
            assert "UserProfileArn" in resp
            sagemaker.delete_user_profile(DomainId=domain_id, UserProfileName=up_name)
        finally:
            sagemaker.delete_domain(DomainId=domain_id)

    def test_delete_user_profile(self, sagemaker):
        domain_name = _uid("dom")
        dom = sagemaker.create_domain(
            DomainName=domain_name,
            AuthMode="IAM",
            DefaultUserSettings={"ExecutionRole": "arn:aws:iam::123456789012:role/SageMakerRole"},
            SubnetIds=["subnet-12345"],
            VpcId="vpc-12345",
        )
        domain_id = dom["DomainArn"].split("/")[-1]
        up_name = _uid("up")
        sagemaker.create_user_profile(DomainId=domain_id, UserProfileName=up_name)
        resp = sagemaker.delete_user_profile(DomainId=domain_id, UserProfileName=up_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        sagemaker.delete_domain(DomainId=domain_id)


class TestSageMakerSpaceCRUD:
    def test_create_space(self, sagemaker):
        domain_name = _uid("dom")
        dom = sagemaker.create_domain(
            DomainName=domain_name,
            AuthMode="IAM",
            DefaultUserSettings={"ExecutionRole": "arn:aws:iam::123456789012:role/SageMakerRole"},
            SubnetIds=["subnet-12345"],
            VpcId="vpc-12345",
        )
        domain_id = dom["DomainArn"].split("/")[-1]
        try:
            sp_name = _uid("sp")
            resp = sagemaker.create_space(
                DomainId=domain_id,
                SpaceName=sp_name,
            )
            assert "SpaceArn" in resp
            sagemaker.delete_space(DomainId=domain_id, SpaceName=sp_name)
        finally:
            sagemaker.delete_domain(DomainId=domain_id)

    def test_delete_space(self, sagemaker):
        domain_name = _uid("dom")
        dom = sagemaker.create_domain(
            DomainName=domain_name,
            AuthMode="IAM",
            DefaultUserSettings={"ExecutionRole": "arn:aws:iam::123456789012:role/SageMakerRole"},
            SubnetIds=["subnet-12345"],
            VpcId="vpc-12345",
        )
        domain_id = dom["DomainArn"].split("/")[-1]
        sp_name = _uid("sp")
        sagemaker.create_space(DomainId=domain_id, SpaceName=sp_name)
        resp = sagemaker.delete_space(DomainId=domain_id, SpaceName=sp_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        sagemaker.delete_domain(DomainId=domain_id)


class TestSageMakerAppCRUD:
    def test_create_app(self, sagemaker):
        domain_name = _uid("dom")
        dom = sagemaker.create_domain(
            DomainName=domain_name,
            AuthMode="IAM",
            DefaultUserSettings={"ExecutionRole": "arn:aws:iam::123456789012:role/SageMakerRole"},
            SubnetIds=["subnet-12345"],
            VpcId="vpc-12345",
        )
        domain_id = dom["DomainArn"].split("/")[-1]
        up_name = _uid("up")
        sagemaker.create_user_profile(DomainId=domain_id, UserProfileName=up_name)
        try:
            app_name = _uid("app")
            resp = sagemaker.create_app(
                DomainId=domain_id,
                UserProfileName=up_name,
                AppType="JupyterServer",
                AppName=app_name,
            )
            assert "AppArn" in resp
            sagemaker.delete_app(
                DomainId=domain_id,
                UserProfileName=up_name,
                AppType="JupyterServer",
                AppName=app_name,
            )
        finally:
            sagemaker.delete_user_profile(DomainId=domain_id, UserProfileName=up_name)
            sagemaker.delete_domain(DomainId=domain_id)

    def test_delete_app(self, sagemaker):
        domain_name = _uid("dom")
        dom = sagemaker.create_domain(
            DomainName=domain_name,
            AuthMode="IAM",
            DefaultUserSettings={"ExecutionRole": "arn:aws:iam::123456789012:role/SageMakerRole"},
            SubnetIds=["subnet-12345"],
            VpcId="vpc-12345",
        )
        domain_id = dom["DomainArn"].split("/")[-1]
        up_name = _uid("up")
        sagemaker.create_user_profile(DomainId=domain_id, UserProfileName=up_name)
        app_name = _uid("app")
        sagemaker.create_app(
            DomainId=domain_id,
            UserProfileName=up_name,
            AppType="JupyterServer",
            AppName=app_name,
        )
        resp = sagemaker.delete_app(
            DomainId=domain_id,
            UserProfileName=up_name,
            AppType="JupyterServer",
            AppName=app_name,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        sagemaker.delete_user_profile(DomainId=domain_id, UserProfileName=up_name)
        sagemaker.delete_domain(DomainId=domain_id)


class TestSageMakerPresignedDomainUrl:
    def test_create_presigned_domain_url(self, sagemaker):
        domain_name = _uid("dom")
        dom = sagemaker.create_domain(
            DomainName=domain_name,
            AuthMode="IAM",
            DefaultUserSettings={"ExecutionRole": "arn:aws:iam::123456789012:role/SageMakerRole"},
            SubnetIds=["subnet-12345"],
            VpcId="vpc-12345",
        )
        domain_id = dom["DomainArn"].split("/")[-1]
        up_name = _uid("up")
        sagemaker.create_user_profile(DomainId=domain_id, UserProfileName=up_name)
        try:
            resp = sagemaker.create_presigned_domain_url(
                DomainId=domain_id,
                UserProfileName=up_name,
            )
            assert "AuthorizedUrl" in resp
        finally:
            sagemaker.delete_user_profile(DomainId=domain_id, UserProfileName=up_name)
            sagemaker.delete_domain(DomainId=domain_id)


class TestSageMakerModelCardExportJobCRUD:
    def test_create_model_card_export_job(self, sagemaker):
        card_name = _uid("mc")
        sagemaker.create_model_card(
            ModelCardName=card_name,
            Content='{"model_overview": {}}',
            ModelCardStatus="Draft",
        )
        try:
            resp = sagemaker.create_model_card_export_job(
                ModelCardName=card_name,
                ModelCardExportJobName=_uid("mcxj"),
                OutputConfig={"S3OutputPath": "s3://my-bucket/mc-export"},
            )
            assert "ModelCardExportJobArn" in resp
        finally:
            sagemaker.delete_model_card(ModelCardName=card_name)


class TestSageMakerInferenceRecommendationsJobCRUD:
    def test_create_inference_recommendations_job(self, sagemaker):
        name = _uid("irj")
        resp = sagemaker.create_inference_recommendations_job(
            JobName=name,
            JobType="Default",
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            InputConfig={
                "ModelPackageVersionArn": (
                    "arn:aws:sagemaker:us-east-1:123456789012:model-package/fake-pkg/1"
                ),
            },
        )
        assert "JobArn" in resp


class TestSageMakerOptimizationJobCRUD:
    def test_create_optimization_job(self, sagemaker):
        name = _uid("oj")
        resp = sagemaker.create_optimization_job(
            OptimizationJobName=name,
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            ModelSource={
                "S3": {"S3Uri": "s3://my-bucket/model.tar.gz"},
            },
            DeploymentInstanceType="ml.g5.xlarge",
            OptimizationConfigs=[
                {"ModelQuantizationConfig": {"Image": "fake-image"}},
            ],
            OutputConfig={"S3OutputLocation": "s3://my-bucket/opt-output"},
            StoppingCondition={"MaxRuntimeInSeconds": 3600},
        )
        assert "OptimizationJobArn" in resp
        sagemaker.delete_optimization_job(OptimizationJobName=name)


class TestSageMakerInferenceComponentCRUD:
    def test_create_inference_component(self, sagemaker):
        # Need an endpoint config + endpoint first
        model_name = _uid("model")
        sagemaker.create_model(
            ModelName=model_name,
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            PrimaryContainer={
                "Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
            },
        )
        epc_name = _uid("epc")
        sagemaker.create_endpoint_config(
            EndpointConfigName=epc_name,
            ProductionVariants=[
                {
                    "VariantName": "v1",
                    "ModelName": model_name,
                    "InstanceType": "ml.m4.xlarge",
                    "InitialInstanceCount": 1,
                }
            ],
        )
        ep_name = _uid("ep")
        sagemaker.create_endpoint(EndpointName=ep_name, EndpointConfigName=epc_name)
        try:
            ic_name = _uid("ic")
            resp = sagemaker.create_inference_component(
                InferenceComponentName=ic_name,
                EndpointName=ep_name,
                VariantName="v1",
                Specification={
                    "ModelName": model_name,
                    "ComputeResourceRequirements": {"MinMemoryRequiredInMb": 512},
                },
                RuntimeConfig={"CopyCount": 1},
            )
            assert "InferenceComponentArn" in resp
            sagemaker.delete_inference_component(InferenceComponentName=ic_name)
        finally:
            sagemaker.delete_endpoint(EndpointName=ep_name)
            sagemaker.delete_endpoint_config(EndpointConfigName=epc_name)
            sagemaker.delete_model(ModelName=model_name)


class TestSageMakerUpdateCodeRepository:
    def test_update_code_repository(self, sagemaker):
        name = _uid("repo")
        sagemaker.create_code_repository(
            CodeRepositoryName=name,
            GitConfig={"RepositoryUrl": "https://github.com/example/repo.git"},
        )
        try:
            resp = sagemaker.update_code_repository(
                CodeRepositoryName=name,
                GitConfig={
                    "SecretArn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:fake"
                },
            )
            assert "CodeRepositoryArn" in resp
        finally:
            sagemaker.delete_code_repository(CodeRepositoryName=name)


class TestSageMakerUpdateWorkteam:
    def test_update_workteam_not_found(self, sagemaker):
        with pytest.raises(ClientError) as exc:
            sagemaker.update_workteam(
                WorkteamName="fake-wt-update",
                Description="updated description",
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"


class TestSageMakerQueryLineage:
    def test_query_lineage_empty(self, sagemaker):
        resp = sagemaker.query_lineage()
        assert (
            "Vertices" in resp
            or "Edges" in resp
            or resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        )


class TestSageMakerCreatePresignedMlflowTrackingServerUrl:
    def test_create_presigned_mlflow_tracking_server_url(self, sagemaker):
        name = _uid("mlf")
        sagemaker.create_mlflow_tracking_server(
            TrackingServerName=name,
            ArtifactStoreUri="s3://my-bucket/mlflow",
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        try:
            resp = sagemaker.create_presigned_mlflow_tracking_server_url(
                TrackingServerName=name,
            )
            assert "AuthorizedUrl" in resp
        finally:
            sagemaker.delete_mlflow_tracking_server(TrackingServerName=name)


class TestSageMakerCreateMlflowApp:
    def test_create_and_delete_mlflow_app(self, sagemaker):
        name = _uid("mla")
        resp = sagemaker.create_mlflow_app(
            Name=name,
            ArtifactStoreUri="s3://my-bucket/mlflow-app",
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        assert "Arn" in resp
        arn = resp["Arn"]
        del_resp = sagemaker.delete_mlflow_app(Arn=arn)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerUpdateMlflowApp:
    def test_update_mlflow_app_not_found(self, sagemaker):
        with pytest.raises(ClientError) as exc:
            sagemaker.update_mlflow_app(
                Arn="arn:aws:sagemaker:us-east-1:123456789012:mlflow-app/fake-app",
            )
        assert exc.value.response["Error"]["Code"] in ("ValidationException", "ResourceNotFound")


class TestSageMakerCreatePartnerApp:
    def test_create_and_delete_partner_app(self, sagemaker):
        name = _uid("pa")
        resp = sagemaker.create_partner_app(
            Name=name,
            Type="lakera-guard",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            Tier="Standard",
            AuthType="IAM",
        )
        assert "Arn" in resp
        arn = resp["Arn"]
        del_resp = sagemaker.delete_partner_app(Arn=arn)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerCreatePartnerAppPresignedUrl:
    def test_create_partner_app_presigned_url(self, sagemaker):
        name = _uid("pa")
        resp = sagemaker.create_partner_app(
            Name=name,
            Type="lakera-guard",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
            Tier="Standard",
            AuthType="IAM",
        )
        arn = resp["Arn"]
        try:
            url_resp = sagemaker.create_partner_app_presigned_url(Arn=arn)
            assert url_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            sagemaker.delete_partner_app(Arn=arn)


class TestSageMakerCreateTrainingPlan:
    def test_create_training_plan(self, sagemaker):
        name = _uid("tp")
        resp = sagemaker.create_training_plan(
            TrainingPlanName=name,
            TrainingPlanOfferingId="trn1-32xl-1mo",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerCreateHubContentPresignedUrls:
    def test_create_hub_content_presigned_urls(self, sagemaker):
        hub_name = _uid("hub")
        sagemaker.create_hub(HubName=hub_name, HubDescription="Test hub for presigned urls")
        try:
            resp = sagemaker.create_hub_content_presigned_urls(
                HubName=hub_name,
                HubContentType="Model",
                HubContentName="fake-model",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            sagemaker.delete_hub(HubName=hub_name)


class TestSageMakerCreateHubContentReference:
    def test_create_hub_content_reference(self, sagemaker):
        hub_name = _uid("hub")
        sagemaker.create_hub(HubName=hub_name, HubDescription="Test hub for ref")
        try:
            resp = sagemaker.create_hub_content_reference(
                HubName=hub_name,
                SageMakerPublicHubContentArn="arn:aws:sagemaker:us-east-1:123456789012:hub-content/fake/Model/fake-model/1.0.0",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            sagemaker.delete_hub(HubName=hub_name)


class TestSageMakerUpdateHubContentReference:
    def test_update_hub_content_reference(self, sagemaker):
        hub_name = _uid("hub")
        sagemaker.create_hub(HubName=hub_name, HubDescription="Test hub for update ref")
        try:
            resp = sagemaker.update_hub_content_reference(
                HubName=hub_name,
                HubContentName="fake-ref",
                HubContentType="Model",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            sagemaker.delete_hub(HubName=hub_name)


class TestSageMakerImportHubContent:
    def test_import_hub_content(self, sagemaker):
        hub_name = _uid("hub")
        sagemaker.create_hub(HubName=hub_name, HubDescription="Test hub for import")
        try:
            resp = sagemaker.import_hub_content(
                HubContentName="imported-model",
                HubContentType="Model",
                DocumentSchemaVersion="1.0.0",
                HubName=hub_name,
                HubContentDocument="{}",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            sagemaker.delete_hub(HubName=hub_name)


class TestSageMakerClusterSchedulerConfig:
    def test_create_and_delete_cluster_scheduler_config(self, sagemaker):
        name = _uid("csc")
        resp = sagemaker.create_cluster_scheduler_config(
            Name=name,
            ClusterArn="arn:aws:sagemaker:us-east-1:123456789012:cluster/fake-cluster",
            SchedulerConfig={"PriorityClasses": []},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        del_resp = sagemaker.delete_cluster_scheduler_config(
            ClusterSchedulerConfigId=name,
        )
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerComputeQuota:
    def test_create_and_delete_compute_quota(self, sagemaker):
        name = _uid("cq")
        resp = sagemaker.create_compute_quota(
            Name=name,
            ClusterArn="arn:aws:sagemaker:us-east-1:123456789012:cluster/fake-cluster",
            ComputeQuotaConfig={"ComputeQuotaResources": []},
            ComputeQuotaTarget={
                "FairShareWeight": 1,
                "TeamName": "test-team",
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        del_resp = sagemaker.delete_compute_quota(ComputeQuotaId=name)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerUpdatePipelineVersion:
    def test_update_pipeline_version(self, sagemaker):
        resp = sagemaker.update_pipeline_version(
            PipelineArn="arn:aws:sagemaker:us-east-1:123456789012:pipeline/fake-pipeline",
            PipelineVersionId=1,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerAttachDetachClusterNodeVolume:
    def test_attach_cluster_node_volume(self, sagemaker):
        resp = sagemaker.attach_cluster_node_volume(
            ClusterArn="arn:aws:sagemaker:us-east-1:123456789012:cluster/fake-cluster",
            NodeId="fake-node-id",
            VolumeId="vol-fake12345",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_detach_cluster_node_volume(self, sagemaker):
        resp = sagemaker.detach_cluster_node_volume(
            ClusterArn="arn:aws:sagemaker:us-east-1:123456789012:cluster/fake-cluster",
            NodeId="fake-node-id",
            VolumeId="vol-fake12345",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerStartSession:
    def test_start_session(self, sagemaker):
        resp = sagemaker.start_session(
            ResourceIdentifier="arn:aws:sagemaker:us-east-1:123456789012:image-version/fake/1",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSageMakerCreatePresignedMlflowAppUrl:
    def test_create_presigned_mlflow_app_url(self, sagemaker):
        name = _uid("mla")
        resp = sagemaker.create_mlflow_app(
            Name=name,
            ArtifactStoreUri="s3://my-bucket/mlflow-presigned",
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        arn = resp["Arn"]
        try:
            url_resp = sagemaker.create_presigned_mlflow_app_url(Arn=arn)
            assert "AuthorizedUrl" in url_resp
        finally:
            sagemaker.delete_mlflow_app(Arn=arn)


class TestSageMakerRenderUiTemplate:
    def test_render_ui_template(self, sagemaker):
        resp = sagemaker.render_ui_template(
            UiTemplate={"Content": "<html><body>Test</body></html>"},
            Task={"Input": '{"key": "value"}'},
            RoleArn="arn:aws:iam::123456789012:role/SageMakerRole",
        )
        assert "RenderedContent" in resp or resp["ResponseMetadata"]["HTTPStatusCode"] == 200
