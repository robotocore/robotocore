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
