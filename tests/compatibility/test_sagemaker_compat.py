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
