//! AWS SageMaker backend — table-driven port of moto's `sagemaker` service.
//!
//! Each resource family is described by a [`Family`] row (name param, ARN
//! path, list key, describe shape, missing-resource message) and generic
//! create/describe/list/delete flows run over per-(account, region) state.
//! Divergent behaviors get explicit handlers. Wire notes: timestamps are
//! epoch-second f64s and null-valued members are pruned.

use crate::proto::parse_json_body;
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use uuid::Uuid;

static SLOT: Lazy<store::Slot<SagemakerState>> =
    Lazy::new(|| store::slot::<SagemakerState>("sagemaker"));

#[derive(Default)]
struct SagemakerState {
    resources: HashMap<String, Entry>,
    tags: HashMap<String, Vec<Value>>,
    catalog_enabled: bool,
    region: String,
    account: String,
}

#[derive(Clone)]
struct Entry {
    kind: &'static str,
    name: String,
    arn: String,
    created: f64,
    modified: Option<f64>,
    status: Option<&'static str>,
    props: Value,
}

struct Family {
    kind: &'static str,
    name_param: &'static str,
    name_key: &'static str,
    arn_key: &'static str,
    arn_path: &'static str,
    create_op: &'static str,
    describe_op: &'static str,
    desc_key: Option<&'static str>,
    list_op: &'static str,
    list_key: &'static str,
    delete_op: &'static str,
    delete_missing_ok: bool,
    missing_msg: &'static str, // format with {name}, {arn}, {label}
    passthrough: &'static [&'static str],
    defaults: &'static [(&'static str, Dv)],
    list_status_key: &'static str,
    status: &'static str, // default status value on create
}

/// Static default values, materialized to Value at runtime.
#[derive(Clone, Copy)]
enum Dv {
    Nothing,
    EmptyArr,
    EmptyObj,
    Bool(bool),
    F64(f64),
    Str(&'static str),
    Null,
}

impl Dv {
    fn to_value(self) -> Value {
        match self {
            Dv::Nothing => Value::Null,
            Dv::EmptyArr => json!([]),
            Dv::Null => Value::Null,
            Dv::EmptyObj => json!({}),
            Dv::Bool(b) => json!(b),
            Dv::F64(x) => json!(x),
            Dv::Str(s) => json!(s),
        }
    }
}

macro_rules! fam {
    (
        $kind:literal, $name_param:literal, $name_key:literal, $arn_key:literal, $arn_path:literal,
        create = $create_op:literal,
        describe = $describe_op:literal, desc_key = $desc_key:expr,
        list = $list_op:literal, $list_key:literal,
        delete = $delete_op:literal,
        missing = $missing:literal,
        pass = $pass:expr,
        defaults = $defaults:expr,
        status = $status:literal
        $(, list_status = $lsk:literal)?
        $(,)?
    ) => {
        Family {
            kind: $kind,
            name_param: $name_param,
            name_key: $name_key,
            arn_key: $arn_key,
            arn_path: $arn_path,
            create_op: $create_op,
            describe_op: $describe_op,
            desc_key: $desc_key,
            list_op: $list_op,
            list_key: $list_key,
            delete_op: $delete_op,
            delete_missing_ok: true,
            missing_msg: $missing,
            passthrough: $pass,
            defaults: $defaults,
            status: $status,
            list_status_key: fam_opt!($($lsk)?),
        }
    };
}

macro_rules! fam_opt {
    () => {
        ""
    };
    ($l:literal) => {
        $l
    };
}

static FAMILIES: &[&Family] = &[
    &fam!(
        "model",
        "ModelName",
        "ModelName",
        "ModelArn",
        "model",
        create = "CreateModel",
        describe = "DescribeModel",
        desc_key = None,
        list = "ListModels",
        "Models",
        delete = "DeleteModel",
        missing = "Could not find model {arn}",
        pass = &[
            "ExecutionRoleArn",
            "PrimaryContainer",
            "Containers",
            "VpcConfig",
            "EnableNetworkIsolation"
        ],
        defaults = &[
            ("Containers", Dv::EmptyArr),
            ("EnableNetworkIsolation", Dv::Bool(false))
        ],
        status = "",
    ),
    &fam!(
        "endpoint-config",
        "EndpointConfigName",
        "EndpointConfigName",
        "EndpointConfigArn",
        "endpoint-config",
        create = "CreateEndpointConfig",
        describe = "DescribeEndpointConfig",
        desc_key = None,
        list = "ListEndpointConfigs",
        "EndpointConfigs",
        delete = "DeleteEndpointConfig",
        missing = "Could not find endpoint configuration '{arn}'.",
        pass = &[
            "ProductionVariants",
            "KmsKeyId",
            "DataCaptureConfig",
            "ShadowProductionVariants",
            "AsyncInferenceConfig",
            "ExplainerConfig"
        ],
        defaults = &[],
        status = "",
    ),
    &fam!(
        "endpoint",
        "EndpointName",
        "EndpointName",
        "EndpointArn",
        "endpoint",
        create = "CreateEndpoint",
        describe = "DescribeEndpoint",
        desc_key = None,
        list = "ListEndpoints",
        "Endpoints",
        delete = "DeleteEndpoint",
        missing = "Could not find endpoint '{arn}'.",
        pass = &[
            "EndpointConfigName",
            "DataCaptureConfig",
            "DeploymentConfig"
        ],
        defaults = &[
            ("EndpointConfigName", Dv::Str("")),
            ("ProductionVariants", Dv::EmptyArr)
        ],
        status = "InService",
    ),
    &fam!(
        "experiment",
        "ExperimentName",
        "ExperimentName",
        "ExperimentArn",
        "experiment",
        create = "CreateExperiment",
        describe = "DescribeExperiment",
        desc_key = None,
        list = "ListExperiments",
        "ExperimentSummaries",
        delete = "DeleteExperiment",
        missing = "Could not find experiment configuration '{arn}'.",
        pass = &["DisplayName", "Description", "Source"],
        defaults = &[("DisplayName", Dv::Str(""))],
        status = "",
    ),
    &fam!(
        "experiment-trial",
        "TrialName",
        "TrialName",
        "TrialArn",
        "experiment-trial",
        create = "CreateTrial",
        describe = "DescribeTrial",
        desc_key = None,
        list = "ListTrials",
        "TrialSummaries",
        delete = "DeleteTrial",
        missing = "Could not find trial '{arn}'.",
        pass = &["ExperimentName", "DisplayName"],
        defaults = &[],
        status = "",
    ),
    &fam!(
        "experiment-trial-component",
        "TrialComponentName",
        "TrialComponentName",
        "TrialComponentArn",
        "experiment-trial-component",
        create = "CreateTrialComponent",
        describe = "DescribeTrialComponent",
        desc_key = None,
        list = "ListTrialComponents",
        "TrialComponentSummaries",
        delete = "DeleteTrialComponent",
        missing = "Could not find trial component '{arn}'",
        pass = &[
            "DisplayName",
            "Status",
            "StartTime",
            "EndTime",
            "Parameters",
            "InputArtifacts",
            "OutputArtifacts",
            "Metrics",
            "Sources"
        ],
        defaults = &[
            ("Parameters", Dv::EmptyObj),
            ("InputArtifacts", Dv::EmptyObj),
            ("OutputArtifacts", Dv::EmptyObj),
            ("Metrics", Dv::EmptyArr),
            ("Sources", Dv::EmptyArr),
            ("CreatedBy", Dv::EmptyObj),
            ("LastModifiedBy", Dv::EmptyObj)
        ],
        status = "",
    ),
    &fam!(
        "pipeline",
        "PipelineName",
        "PipelineName",
        "PipelineArn",
        "pipeline",
        create = "CreatePipeline",
        describe = "DescribePipeline",
        desc_key = None,
        list = "ListPipelines",
        "PipelineSummaries",
        delete = "DeletePipeline",
        missing = "Could not find pipeline with PipelineName {name}.",
        pass = &[
            "PipelineDefinition",
            "PipelineDescription",
            "RoleArn",
            "PipelineDisplayName"
        ],
        defaults = &[
            ("PipelineDescription", Dv::Str("")),
            ("PipelineStatus", Dv::Str("Active"))
        ],
        status = "Active",
    ),
    &fam!(
        "notebook-instance",
        "NotebookInstanceName",
        "NotebookInstanceName",
        "NotebookInstanceArn",
        "notebook-instance",
        create = "CreateNotebookInstance",
        describe = "DescribeNotebookInstance",
        desc_key = None,
        list = "ListNotebookInstances",
        "NotebookInstances",
        delete = "DeleteNotebookInstance",
        missing = "Could not find notebook instance {name}",
        pass = &[
            "InstanceType",
            "RoleArn",
            "SubnetId",
            "SecurityGroupIds",
            "KmsKeyId",
            "LifecycleConfigName",
            "DirectInternetAccess",
            "VolumeSizeInGB",
            "AcceleratorTypes",
            "RootAccess"
        ],
        defaults = &[
            ("NotebookInstanceStatus", Dv::Str("InService")),
            ("InstanceType", Dv::Str("ml.t2.medium"))
        ],
        status = "InService",
    ),
    &fam!(
        "notebook-instance-lifecycle-config",
        "NotebookInstanceLifecycleConfigName",
        "NotebookInstanceLifecycleConfigName",
        "NotebookInstanceLifecycleConfigArn",
        "notebook-instance-lifecycle-config",
        create = "CreateNotebookInstanceLifecycleConfig",
        describe = "DescribeNotebookInstanceLifecycleConfig",
        desc_key = None,
        list = "ListNotebookInstanceLifecycleConfigs",
        "NotebookInstanceLifecycleConfigs",
        delete = "DeleteNotebookInstanceLifecycleConfig",
        missing = "Could not find notebook instance lifecycle config {name}",
        pass = &["OnCreate", "OnStart"],
        defaults = &[],
        status = "",
    ),
    &fam!(
        "model-package-group",
        "ModelPackageGroupName",
        "ModelPackageGroupName",
        "ModelPackageGroupArn",
        "model-package-group",
        create = "CreateModelPackageGroup",
        describe = "DescribeModelPackageGroup",
        desc_key = None,
        list = "ListModelPackageGroups",
        "ModelPackageGroupSummaryList",
        delete = "DeleteModelPackageGroup",
        missing = "Could not find model package group '{arn}'.",
        pass = &["ModelPackageGroupDescription", "Tags"],
        defaults = &[],
        status = "Completed",
    ),
    &fam!(
        "model-package",
        "ModelPackageName",
        "ModelPackageName",
        "ModelPackageArn",
        "model-package",
        create = "CreateModelPackage",
        describe = "DescribeModelPackage",
        desc_key = None,
        list = "ListModelPackages",
        "ModelPackageSummaryList",
        delete = "DeleteModelPackage",
        missing = "Could not find model package {arn}",
        pass = &[
            "ModelPackageDescription",
            "InferenceSpecification",
            "ValidationSpecification",
            "ModelMetrics",
            "SourceModelSpecification",
            "CertifyForMarketplace"
        ],
        defaults = &[],
        status = "Completed",
    ),
    &fam!(
        "model-card",
        "ModelCardName",
        "ModelCardName",
        "ModelCardArn",
        "model-card",
        create = "CreateModelCard",
        describe = "DescribeModelCard",
        desc_key = None,
        list = "ListModelCards",
        "ModelCardSummaries",
        delete = "DeleteModelCard",
        missing = "Modelcard {name} does not exist",
        pass = &["Content", "ModelCardStatus", "ModelId", "SecurityConfig"],
        defaults = &[],
        status = "Draft",
    ),
    &fam!(
        "feature-group",
        "FeatureGroupName",
        "FeatureGroupName",
        "FeatureGroupArn",
        "feature-group",
        create = "CreateFeatureGroup",
        describe = "DescribeFeatureGroup",
        desc_key = None,
        list = "ListFeatureGroups",
        "FeatureGroupSummaries",
        delete = "DeleteFeatureGroup",
        missing = "Could not find feature group {name}",
        pass = &[
            "RecordIdentifierFeatureName",
            "EventTimeFeatureName",
            "FeatureDefinitions",
            "OfflineStoreConfig",
            "OnlineStoreConfig",
            "Description",
            "RoleArn"
        ],
        defaults = &[],
        status = "Created",
    ),
    &fam!(
        "flow-definition",
        "FlowDefinitionName",
        "FlowDefinitionName",
        "FlowDefinitionArn",
        "flow-definition",
        create = "CreateFlowDefinition",
        describe = "DescribeFlowDefinition",
        desc_key = None,
        list = "ListFlowDefinitions",
        "FlowDefinitionSummaries",
        delete = "DeleteFlowDefinition",
        missing = "Could not find flow definition {name}",
        pass = &["OutputConfig", "RoleArn", "HumanLoopConfig"],
        defaults = &[("FlowDefinitionStatus", Dv::Str("Active"))],
        status = "Active",
    ),
    &fam!(
        "human-task-ui",
        "HumanTaskUiName",
        "HumanTaskUiName",
        "HumanTaskUiArn",
        "human-task-ui",
        create = "CreateHumanTaskUi",
        describe = "DescribeHumanTaskUi",
        desc_key = None,
        list = "ListHumanTaskUis",
        "HumanTaskUiSummaries",
        delete = "DeleteHumanTaskUi",
        missing = "Could not find human task ui {name}",
        pass = &["UiTemplate", "Tags"],
        defaults = &[],
        status = "",
    ),
    &fam!(
        "studio-lifecycle-config",
        "StudioLifecycleConfigName",
        "StudioLifecycleConfigName",
        "StudioLifecycleConfigArn",
        "studio-lifecycle-config",
        create = "CreateStudioLifecycleConfig",
        describe = "DescribeStudioLifecycleConfig",
        desc_key = None,
        list = "ListStudioLifecycleConfigs",
        "StudioLifecycleConfigs",
        delete = "DeleteStudioLifecycleConfig",
        missing = "Could not find studio lifecycle config {name}",
        pass = &[
            "StudioLifecycleConfigContent",
            "StudioLifecycleConfigAppType"
        ],
        defaults = &[],
        status = "",
    ),
    &fam!(
        "app-image-config",
        "AppImageConfigName",
        "AppImageConfigName",
        "AppImageConfigArn",
        "app-image-config",
        create = "CreateAppImageConfig",
        describe = "DescribeAppImageConfig",
        desc_key = None,
        list = "ListAppImageConfigs",
        "AppImageConfigs",
        delete = "DeleteAppImageConfig",
        missing = "Could not find app image config {name}",
        pass = &[
            "KernelGatewayImageConfig",
            "JupyterLabImageConfig",
            "CodeEditorImageConfig"
        ],
        defaults = &[],
        status = "",
    ),
    &fam!(
        "code-repository",
        "CodeRepositoryName",
        "CodeRepositoryName",
        "CodeRepositoryArn",
        "code-repository",
        create = "CreateCodeRepository",
        describe = "DescribeCodeRepository",
        desc_key = None,
        list = "ListCodeRepositories",
        "CodeRepositorySummaryList",
        delete = "DeleteCodeRepository",
        missing = "Could not find code repository {name}",
        pass = &["GitConfig"],
        defaults = &[],
        status = "",
    ),
    &fam!(
        "image",
        "ImageName",
        "ImageName",
        "ImageArn",
        "image",
        create = "CreateImage",
        describe = "DescribeImage",
        desc_key = None,
        list = "ListImages",
        "Images",
        delete = "DeleteImage",
        missing = "Could not find image {name}",
        pass = &["RoleArn", "DisplayName", "Description"],
        defaults = &[("ImageStatus", Dv::Str("CREATED"))],
        status = "CREATED",
    ),
    &fam!(
        "device-fleet",
        "DeviceFleetName",
        "DeviceFleetName",
        "DeviceFleetArn",
        "device-fleet",
        create = "CreateDeviceFleet",
        describe = "DescribeDeviceFleet",
        desc_key = None,
        list = "ListDeviceFleets",
        "DeviceFleetSummaries",
        delete = "DeleteDeviceFleet",
        missing = "Could not find device fleet {name}",
        pass = &[
            "OutputConfig",
            "RoleArn",
            "Description",
            "Tags",
            "IotRoleAlias"
        ],
        defaults = &[],
        status = "",
    ),
    &fam!(
        "algorithm",
        "AlgorithmName",
        "AlgorithmName",
        "AlgorithmArn",
        "algorithm",
        create = "CreateAlgorithm",
        describe = "DescribeAlgorithm",
        desc_key = None,
        list = "ListAlgorithms",
        "AlgorithmSummaryList",
        delete = "DeleteAlgorithm",
        missing = "Could not find algorithm {name}",
        pass = &[
            "AlgorithmDescription",
            "TrainingSpecification",
            "InferenceSpecification",
            "CertifyForMarketplace"
        ],
        defaults = &[("AlgorithmStatus", Dv::Str("Completed"))],
        status = "Completed",
    ),
    &fam!(
        "labeling-job",
        "LabelingJobName",
        "LabelingJobName",
        "LabelingJobArn",
        "labeling-job",
        create = "CreateLabelingJob",
        describe = "DescribeLabelingJob",
        desc_key = None,
        list = "ListLabelingJobs",
        "LabelingJobSummaryList",
        delete = "DeleteLabelingJob",
        missing = "Could not find labeling job {name}",
        pass = &[
            "LabelAttributeName",
            "InputConfig",
            "OutputConfig",
            "RoleArn",
            "Locale",
            "LabelingJobAlgorithmsConfig",
            "HumanTaskConfig",
            "StoppingConditions"
        ],
        defaults = &[("LabelingJobStatus", Dv::Str("InProgress"))],
        status = "InProgress",
    ),
    &fam!(
        "compilation-job",
        "CompilationJobName",
        "CompilationJobName",
        "CompilationJobArn",
        "compilation-job",
        create = "CreateCompilationJob",
        describe = "DescribeCompilationJob",
        desc_key = None,
        list = "ListCompilationJobs",
        "CompilationJobSummaries",
        delete = "DeleteCompilationJob",
        missing = "Could not find compilation job {name}",
        pass = &[
            "RoleArn",
            "InputConfig",
            "OutputConfig",
            "StoppingCondition",
            "InputConfig_Broken"
        ],
        defaults = &[("CompilationJobStatus", Dv::Str("COMPLETED"))],
        status = "COMPLETED",
    ),
    &fam!(
        "processing-job",
        "ProcessingJobName",
        "ProcessingJobName",
        "ProcessingJobArn",
        "processing-job",
        create = "CreateProcessingJob",
        describe = "DescribeProcessingJob",
        desc_key = None,
        list = "ListProcessingJobs",
        "ProcessingJobSummaries",
        delete = "DeleteProcessingJob",
        missing = "Could not find processing job {name}",
        pass = &[
            "RoleArn",
            "ProcessingResources",
            "AppSpecification",
            "ProcessingInputs",
            "ProcessingOutputConfig",
            "StoppingCondition",
            "Environment"
        ],
        defaults = &[("ProcessingJobStatus", Dv::Str("Completed"))],
        status = "Completed",
    ),
    &fam!(
        "training-job",
        "TrainingJobName",
        "TrainingJobName",
        "TrainingJobArn",
        "training-job",
        create = "CreateTrainingJob",
        describe = "DescribeTrainingJob",
        desc_key = None,
        list = "ListTrainingJobs",
        "TrainingJobSummaries",
        delete = "DeleteTrainingJob",
        missing = "Could not find training job {name}",
        pass = &[
            "RoleArn",
            "AlgorithmSpecification",
            "InputDataConfig",
            "OutputDataConfig",
            "ResourceConfig",
            "StoppingCondition",
            "HyperParameters",
            "VpcConfig",
            "EnableNetworkIsolation",
            "DebugHookConfig",
            "ProfilerConfig",
            "Environment",
            "OutputDataConfig_Path"
        ],
        defaults = &[
            ("TrainingJobStatus", Dv::Str("Completed")),
            ("SecondaryStatus", Dv::Str("Completed"))
        ],
        status = "Completed",
    ),
    &fam!(
        "transform-job",
        "TransformJobName",
        "TransformJobName",
        "TransformJobArn",
        "transform-job",
        create = "CreateTransformJob",
        describe = "DescribeTransformJob",
        desc_key = None,
        list = "ListTransformJobs",
        "TransformJobSummaries",
        delete = "DeleteTransformJob",
        missing = "Could not find transform job {name}",
        pass = &[
            "ModelName",
            "TransformInput",
            "TransformOutput",
            "TransformResources",
            "BatchStrategy",
            "Environment",
            "MaxConcurrentTransforms",
            "MaxPayloadInMB"
        ],
        defaults = &[("TransformJobStatus", Dv::Str("Completed"))],
        status = "Completed",
    ),
    &fam!(
        "hyper-parameter-tuning-job",
        "HyperParameterTuningJobName",
        "HyperParameterTuningJobName",
        "HyperParameterTuningJobArn",
        "hyper-parameter-tuning-job",
        create = "CreateHyperParameterTuningJob",
        describe = "DescribeHyperParameterTuningJob",
        desc_key = None,
        list = "ListHyperParameterTuningJobs",
        "HyperParameterTuningJobSummaries",
        delete = "DeleteHyperParameterTuningJob",
        missing = "Could not find hyper parameter tuning job {name}",
        pass = &[
            "HyperParameterTuningJobConfig",
            "TrainingJobDefinition",
            "TrainingJobDefinitions",
            "WarmStartConfig"
        ],
        defaults = &[("HyperParameterTuningJobStatus", Dv::Str("Completed"))],
        status = "Completed",
    ),
    &fam!(
        "auto-ml-job",
        "AutoMLJobName",
        "AutoMLJobName",
        "AutoMLJobArn",
        "automl-job",
        create = "CreateAutoMLJob",
        describe = "DescribeAutoMLJob",
        desc_key = None,
        list = "ListAutoMLJobs",
        "AutoMLJobSummaries",
        delete = "DeleteAutoMLJob",
        missing = "Could not find AutoML job {name}",
        pass = &[
            "InputDataConfig",
            "OutputDataConfig",
            "RoleArn",
            "AutoMLJobObjective",
            "AutoMLJobConfig",
            "ProblemType"
        ],
        defaults = &[
            ("AutoMLJobStatus", Dv::Str("InProgress")),
            ("AutoMLJobSecondaryStatus", Dv::Str("Starting"))
        ],
        status = "InProgress",
    ),
    &fam!(
        "model-bias-job-definition",
        "JobDefinitionName",
        "JobDefinitionName",
        "JobDefinitionArn",
        "model-bias-job-definition",
        create = "CreateModelBiasJobDefinition",
        describe = "DescribeModelBiasJobDefinition",
        desc_key = None,
        list = "ListModelBiasJobDefinitions",
        "JobDefinitionSummaries",
        delete = "DeleteModelBiasJobDefinition",
        missing = "Job definition {name} not found",
        pass = &[
            "ModelBiasAppSpecification",
            "ModelBiasJobInput",
            "ModelBiasJobOutputConfig",
            "RoleArn",
            "JobResources",
            "StoppingCondition",
            "Tags"
        ],
        defaults = &[],
        status = "",
    ),
    &fam!(
        "model-explainability-job-definition",
        "JobDefinitionName",
        "JobDefinitionName",
        "JobDefinitionArn",
        "model-explainability-job-definition",
        create = "CreateModelExplainabilityJobDefinition",
        describe = "DescribeModelExplainabilityJobDefinition",
        desc_key = None,
        list = "ListModelExplainabilityJobDefinitions",
        "JobDefinitionSummaries",
        delete = "DeleteModelExplainabilityJobDefinition",
        missing = "Could not find model explainability job definition with name '{name}'.",
        pass = &[
            "ModelExplainabilityAppSpecification",
            "ModelExplainabilityJobInput",
            "ModelExplainabilityJobOutputConfig",
            "RoleArn",
            "JobResources"
        ],
        defaults = &[],
        status = "",
    ),
    &fam!(
        "model-quality-job-definition",
        "JobDefinitionName",
        "JobDefinitionName",
        "JobDefinitionArn",
        "model-quality-job-definition",
        create = "CreateModelQualityJobDefinition",
        describe = "DescribeModelQualityJobDefinition",
        desc_key = None,
        list = "ListModelQualityJobDefinitions",
        "JobDefinitionSummaries",
        delete = "DeleteModelQualityJobDefinition",
        missing = "Could not find model quality job definition '{name}'.",
        pass = &[
            "ModelQualityAppSpecification",
            "ModelQualityJobInput",
            "ModelQualityJobOutputConfig",
            "RoleArn",
            "JobResources"
        ],
        defaults = &[],
        status = "",
    ),
    &fam!(
        "data-quality-job-definition",
        "JobDefinitionName",
        "JobDefinitionName",
        "JobDefinitionArn",
        "data-quality-job-definition",
        create = "CreateDataQualityJobDefinition",
        describe = "DescribeDataQualityJobDefinition",
        desc_key = None,
        list = "ListDataQualityJobDefinitions",
        "JobDefinitionSummaries",
        delete = "DeleteDataQualityJobDefinition",
        missing = "Could not find data quality job definition with name '{name}'.",
        pass = &[
            "DataQualityAppSpecification",
            "DataQualityJobInput",
            "DataQualityJobOutputConfig",
            "RoleArn",
            "JobResources"
        ],
        defaults = &[],
        status = "",
    ),
    &fam!(
        "project",
        "ProjectName",
        "ProjectName",
        "ProjectArn",
        "project",
        create = "CreateProject",
        describe = "DescribeProject",
        desc_key = None,
        list = "ListProjects",
        "ProjectSummaryList",
        delete = "DeleteProject",
        missing = "Could not find project {name}",
        pass = &[
            "ServiceCatalogProvisioningDetails",
            "ProjectDescription",
            "Tags"
        ],
        defaults = &[("ProjectStatus", Dv::Str("Pending"))],
        status = "Pending",
    ),
    &fam!(
        "edge-deployment-plan",
        "EdgeDeploymentPlanName",
        "EdgeDeploymentPlanName",
        "EdgeDeploymentPlanArn",
        "edge-deployment",
        create = "CreateEdgeDeploymentPlan",
        describe = "DescribeEdgeDeploymentPlan",
        desc_key = None,
        list = "ListEdgeDeploymentPlans",
        "EdgeDeploymentPlanSummaries",
        delete = "DeleteEdgeDeploymentPlan",
        missing = "Could not find edge deployment plan {name}",
        pass = &["DeviceFleetName", "ModelConfigs", "Stages"],
        defaults = &[("Status", Dv::Str("Idle"))],
        status = "Idle",
    ),
    &fam!(
        "edge-packaging-job",
        "EdgePackagingJobName",
        "EdgePackagingJobName",
        "EdgePackagingJobArn",
        "edge-packaging-job",
        create = "CreateEdgePackagingJob",
        describe = "DescribeEdgePackagingJob",
        desc_key = None,
        list = "ListEdgePackagingJobs",
        "EdgePackagingJobSummaries",
        delete = "DeleteEdgePackagingJob",
        missing = "Could not find edge packaging job {name}",
        pass = &[
            "CompilationJobName",
            "ModelName",
            "ModelVersion",
            "RoleArn",
            "OutputConfig",
            "ResourceConfig",
            "Tags",
            "EdgePresetDeploymentOutput"
        ],
        defaults = &[("EdgePackagingJobStatus", Dv::Str("Started"))],
        status = "Started",
    ),
    &fam!(
        "inference-experiment",
        "Name",
        "Name",
        "InferenceExperimentArn",
        "inference-experiment",
        create = "CreateInferenceExperiment",
        describe = "DescribeInferenceExperiment",
        desc_key = None,
        list = "ListInferenceExperiments",
        "InferenceExperiments",
        delete = "DeleteInferenceExperiment",
        missing = "Could not find inference experiment {name}",
        pass = &[
            "Type",
            "RoleArn",
            "EndpointName",
            "ModelVariants",
            "DataStorageConfig",
            "Schedule",
            "Description"
        ],
        defaults = &[("Status", Dv::Str("Creating"))],
        status = "Creating",
    ),
    &fam!(
        "inference-recommendations-job",
        "JobName",
        "JobName",
        "JobArn",
        "inference-recommendations-job",
        create = "CreateInferenceRecommendationsJob",
        describe = "DescribeInferenceRecommendationsJob",
        desc_key = None,
        list = "ListInferenceRecommendationsJobs",
        "InferenceRecommendationsJobs",
        delete = "DeleteInferenceRecommendationsJob",
        missing = "Could not find inference recommendations job {name}",
        pass = &[
            "JobType",
            "RoleArn",
            "InputConfig",
            "OutputConfig",
            "StoppingCondition",
            "Tags"
        ],
        defaults = &[("Status", Dv::Str("COMPLETED"))],
        status = "COMPLETED",
    ),
    &fam!(
        "optimization-job",
        "OptimizationJobName",
        "OptimizationJobName",
        "OptimizationJobArn",
        "optimization-job",
        create = "CreateOptimizationJob",
        describe = "DescribeOptimizationJob",
        desc_key = None,
        list = "ListOptimizationJobs",
        "OptimizationJobSummaries",
        delete = "DeleteOptimizationJob",
        missing = "Could not find optimization job {name}",
        pass = &[
            "RoleArn",
            "ModelSource",
            "DeploymentInstanceType",
            "OptimizationConfig",
            "OutputConfig",
            "StoppingCondition",
            "VpcConfig"
        ],
        defaults = &[
            ("OptimizationJobStatus", Dv::Str("Completed")),
            ("OptimizationStartTime", Dv::Null)
        ],
        status = "Completed",
    ),
    &fam!(
        "monitoring-schedule",
        "MonitoringScheduleName",
        "MonitoringScheduleName",
        "MonitoringScheduleArn",
        "monitoring-schedule",
        create = "CreateMonitoringSchedule",
        describe = "DescribeMonitoringSchedule",
        desc_key = None,
        list = "ListMonitoringSchedules",
        "MonitoringScheduleSummaries",
        delete = "DeleteMonitoringSchedule",
        missing = "Could not find monitoring schedule {name}",
        pass = &[
            "MonitoringScheduleConfig",
            "EndpointName",
            "MonitoringType",
            "Tags"
        ],
        defaults = &[
            ("MonitoringScheduleStatus", Dv::Str("Scheduled")),
            ("LastModifiedTime", Dv::F64(0.0))
        ],
        status = "Scheduled",
    ),
    &fam!(
        "cluster",
        "ClusterName",
        "ClusterName",
        "ClusterArn",
        "cluster",
        create = "CreateCluster",
        describe = "DescribeCluster",
        desc_key = None,
        list = "ListClusters",
        "ClusterSummaries",
        delete = "DeleteCluster",
        missing = "Could not find cluster '{name}'.",
        pass = &[
            "InstanceGroups",
            "RoleArn",
            "VpcConfig",
            "Policies",
            "RestrictedInstanceGroups",
            "AutoScaling",
            "Tags"
        ],
        defaults = &[
            ("ClusterStatus", Dv::Str("InProgress")),
            ("CreationTime", Dv::Null)
        ],
        status = "InProgress",
    ),
    &fam!(
        "workteam",
        "WorkteamName",
        "WorkteamName",
        "WorkteamArn",
        "workteam",
        create = "CreateWorkteam",
        describe = "DescribeWorkteam",
        desc_key = None,
        list = "ListWorkteams",
        "Workteams",
        delete = "DeleteWorkteam",
        missing = "Could not find workteam {name}",
        pass = &[
            "MemberDefinitions",
            "Description",
            "NotificationConfiguration",
            "WorkerAccessConfiguration",
            "Tags"
        ],
        defaults = &[("SubDomain", Dv::Str(""))],
        status = "",
    ),
    &fam!(
        "workforce",
        "WorkforceName",
        "WorkforceName",
        "WorkforceArn",
        "workforce",
        create = "CreateWorkforce",
        describe = "DescribeWorkforce",
        desc_key = None,
        list = "ListWorkforces",
        "Workforces",
        delete = "DeleteWorkforce",
        missing = "Could not find workforce {name}",
        pass = &["CognitoConfig", "OidcConfig", "SourceIpConfig", "Tags"],
        defaults = &[("Status", Dv::Str("Active"))],
        status = "Active",
    ),
    &fam!(
        "mlflow-tracking-server",
        "TrackingServerName",
        "TrackingServerName",
        "TrackingServerArn",
        "mlflow-tracking-server",
        create = "CreateMlflowTrackingServer",
        describe = "DescribeMlflowTrackingServer",
        desc_key = None,
        list = "ListMlflowTrackingServers",
        "TrackingServerSummaries",
        delete = "DeleteMlflowTrackingServer",
        missing = "Could not find mlflow tracking server {name}",
        pass = &[
            "ArtifactStoreUri",
            "RoleArn",
            "TrackingServerSize",
            "WeeklyMaintenanceWindow",
            "MlflowVersion"
        ],
        defaults = &[("TrackingServerStatus", Dv::Str("Created"))],
        status = "Created",
    ),
    &fam!(
        "hub",
        "HubName",
        "HubName",
        "HubArn",
        "hub",
        create = "CreateHub",
        describe = "DescribeHub",
        desc_key = None,
        list = "ListHubs",
        "HubSummaries",
        delete = "DeleteHub",
        missing = "Could not find hub {name}",
        pass = &[
            "HubDescription",
            "HubDisplayName",
            "S3StorageConfig",
            "HubSearchKeywords",
            "Tags"
        ],
        defaults = &[("CreationTime", Dv::EmptyObj)],
        status = "InService",
    ),
    &fam!(
        "user-profile",
        "UserProfileName",
        "UserProfileName",
        "UserProfileArn",
        "user-profile",
        create = "CreateUserProfile",
        describe = "DescribeUserProfile",
        desc_key = None,
        list = "ListUserProfiles",
        "UserProfiles",
        delete = "DeleteUserProfile",
        missing = "Could not find user profile {name}",
        pass = &[
            "DomainId",
            "SingleSignOnUserIdentifier",
            "UserSettings",
            "Tags"
        ],
        defaults = &[
            ("Status", Dv::Str("InService")),
            ("HomeEfsFileSystemUid", Dv::Str("00000000"))
        ],
        status = "InService",
    ),
    &fam!(
        "space",
        "SpaceName",
        "SpaceName",
        "SpaceArn",
        "space",
        create = "CreateSpace",
        describe = "DescribeSpace",
        desc_key = None,
        list = "ListSpaces",
        "Spaces",
        delete = "DeleteSpace",
        missing = "Could not find space {name}",
        pass = &["DomainId", "SpaceSettings", "SpaceDisplayName", "Tags"],
        defaults = &[
            ("Status", Dv::Str("InService")),
            ("HomeEfsFileSystemUid", Dv::Str("00000000"))
        ],
        status = "InService",
    ),
    &fam!(
        "action",
        "ActionName",
        "ActionName",
        "ActionArn",
        "action",
        create = "CreateAction",
        describe = "DescribeAction",
        desc_key = None,
        list = "ListActions",
        "ActionSummaries",
        delete = "DeleteAction",
        missing = "Could not find action {name}",
        pass = &["Source", "ActionType", "Description", "Properties", "Tags"],
        defaults = &[("Status", Dv::Nothing)],
        status = "",
    ),
    &fam!(
        "context",
        "ContextName",
        "ContextName",
        "ContextArn",
        "context",
        create = "CreateContext",
        describe = "DescribeContext",
        desc_key = None,
        list = "ListContexts",
        "ContextSummaries",
        delete = "DeleteContext",
        missing = "Could not find context {name}",
        pass = &["Source", "ContextType", "Description", "Properties", "Tags"],
        defaults = &[],
        status = "",
    ),
    &fam!(
        "artifact",
        "ArtifactName",
        "ArtifactName",
        "ArtifactArn",
        "artifact",
        create = "CreateArtifact",
        describe = "DescribeArtifact",
        desc_key = None,
        list = "ListArtifacts",
        "ArtifactSummaries",
        delete = "DeleteArtifact",
        missing = "Could not find artifact {name}",
        pass = &["Source", "ArtifactType", "Properties", "Tags"],
        defaults = &[],
        status = "",
    ),
    &fam!(
        "inference-component",
        "InferenceComponentName",
        "InferenceComponentName",
        "InferenceComponentArn",
        "inference-component",
        create = "CreateInferenceComponent",
        describe = "DescribeInferenceComponent",
        desc_key = None,
        list = "ListInferenceComponents",
        "InferenceComponents",
        delete = "DeleteInferenceComponent",
        missing = "Could not find inference component {name}",
        pass = &[
            "EndpointName",
            "VariantName",
            "Specification",
            "RuntimeConfig",
            "Tags"
        ],
        defaults = &[("Status", Dv::Str("InService"))],
        status = "InService",
    ),
    &fam!(
        "domain",
        "DomainName",
        "DomainName",
        "DomainArn",
        "domain",
        create = "CreateDomain",
        describe = "DescribeDomain",
        desc_key = None,
        list = "ListDomains",
        "Domains",
        delete = "DeleteDomain",
        missing = "Could not find domain '{name}'.",
        pass = &[
            "AuthMode",
            "DefaultUserSettings",
            "SubnetIds",
            "VpcId",
            "DomainSettings",
            "Tags",
            "AppNetworkAccessType"
        ],
        defaults = &[("DomainStatus", Dv::Str("InService"))],
        status = "InService",
    ),
    &fam!(
        "auto-ml-job-v2",
        "AutoMLJobName",
        "AutoMLJobName",
        "AutoMLJobArn",
        "automl-job",
        create = "CreateAutoMLJobV2",
        describe = "DescribeAutoMLJobV2",
        desc_key = None,
        list = "ListAutoMLJobs",
        "AutoMLJobSummaries",
        delete = "DeleteAutoMLJobV2",
        missing = "Could not find AutoML job {name}",
        pass = &[
            "AutoMLJobObjective",
            "AutoMLProblemTypeConfig",
            "AutoMLProblemTypeConfigName",
            "DataSplitConfig",
            "AutoMLJobInputDataConfig",
            "OutputDataConfig",
            "RoleArn",
            "AutoMLJobConfig"
        ],
        defaults = &[
            ("AutoMLJobStatus", Dv::Str("InProgress")),
            ("AutoMLJobSecondaryStatus", Dv::Str("Starting"))
        ],
        status = "InProgress",
    ),
    &fam!(
        "mlflow-app",
        "Name",
        "Name",
        "Arn",
        "mlflow-app",
        create = "CreateMlflowApp",
        describe = "DescribeMlflowApp",
        desc_key = None,
        list = "ListMlflowApps",
        "MlflowApps",
        delete = "DeleteMlflowApp",
        missing = "Could not find mlflow app {name}",
        pass = &[
            "ArtifactStoreUri",
            "RoleArn",
            "ProvisioningParameters",
            "Tags"
        ],
        defaults = &[],
        status = "Created",
    ),
    &fam!(
        "app",
        "AppName",
        "AppName",
        "AppArn",
        "app",
        create = "CreateApp",
        describe = "DescribeApp",
        desc_key = None,
        list = "ListApps",
        "Apps",
        delete = "DeleteApp",
        missing = "Could not find app {name}",
        pass = &[
            "DomainId",
            "UserProfileName",
            "AppType",
            "SpaceName",
            "AppSettings",
            "Tags"
        ],
        defaults = &[("Status", Dv::Str("InService"))],
        status = "InService",
    ),
];

fn get<'v>(p: &'v Value, key: &str) -> Option<&'v Value> {
    p.get(key).filter(|v| !v.is_null())
}

/// Kinds whose missing-resource error type is ResourceNotFound (not the
/// default ValidationException listing moto uses for the rest).
const RESOURCE_NOT_FOUND_KINDS: &[&str] = &[
    "auto-ml-job",
    "auto-ml-job-v2",
    "compilation-job",
    "data-quality-job-definition",
    "inference-recommendations-job",
    "model-explainability-job-definition",
    "model-quality-job-definition",
    "hyper-parameter-tuning-job",
];

fn family_of(kind_key: &str) -> &'static Family {
    FAMILIES
        .iter()
        .find(|f| f.kind == kind_key)
        .expect("sagemaker family must exist")
}

fn now_ts() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

fn ok(body: Value) -> Response {
    Response::json_status(200, &body)
}

fn empty() -> Response {
    ok(json!({}))
}

fn validation(message: &str) -> Response {
    Response::json_status(
        400,
        &json!({ "__type": "ValidationException", "message": message }),
    )
}

fn conflict(message: &str) -> Response {
    Response::json_status(
        400,
        &json!({ "__type": "ConflictException", "message": message }),
    )
}

/// Prune null-valued members of an object (moto serializer behavior).
fn without_nulls(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut out = Map::new();
            for (k, val) in map {
                if !val.is_null() {
                    out.insert(k.clone(), val.clone());
                }
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

fn resource_not_found_plain(message: &str) -> Response {
    Response::json_status(
        400,
        &json!({ "__type": "ResourceNotFound", "message": message }),
    )
}

/// Dispose the missing-resource message per family: {name}, {arn} and {label}
/// placeholders; extra formats handled inline by callers.
fn missing_body(f: &Family, name: &str, arn: &str) -> Value {
    let message = f.missing_msg.replace("{name}", name).replace("{arn}", arn);
    if RESOURCE_NOT_FOUND_KINDS.contains(&f.kind) {
        return json!({ "__type": "ResourceNotFound", "message": message });
    }
    if message.starts_with("Job definition ") {
        return json!({ "__type": "ResourceNotFound", "message": message });
    }
    if message.starts_with("Modelcard ") {
        return json!({ "__type": "ResourceNotFound", "message": message });
    }
    json!({ "__type": "ValidationException", "message": message })
}

pub fn dispatch(req: &Request) -> Response {
    let params = parse_json_body(&req.body);
    let mut guard = SLOT.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    if st.region.is_empty() {
        st.region = req.region.clone();
        st.account = req.account.to_string();
    }
    st.route(req.operation.as_str(), &params)
}

impl SagemakerState {
    fn region(&self) -> &str {
        if self.region.is_empty() {
            "us-east-1"
        } else {
            &self.region
        }
    }

    fn arn_for(&self, kind: &Family, name: &str) -> String {
        format!(
            "arn:aws:sagemaker:{r}:{a}:{path}/{name}",
            r = self.region(),
            a = self.account,
            path = kind.arn_path
        )
    }

    /// (kind, name) → arn key for state lookup.
    fn key(kind: &str, name: &str) -> String {
        format!("{kind}\\0{name}")
    }

    fn get_arn(&self, kind: &Family, name: &str) -> Option<String> {
        self.resources
            .values()
            .find(|e| e.kind == kind.kind && e.name == name)
            .map(|e| e.arn.clone())
    }

    fn list_kind(&self, kind: &str) -> Vec<Value> {
        let mut items: Vec<Entry> = self
            .resources
            .values()
            .filter(|e| e.kind == kind)
            .cloned()
            .collect();
        items.sort_by(|a, b| {
            a.created
                .partial_cmp(&b.created)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        items
            .iter()
            .map(|e| {
                let f = family_of(e.kind);
                let name_key = if e.kind.ends_with("-job-definition") {
                    "MonitoringJobDefinitionName"
                } else {
                    f.name_key
                };
                let mut summary = json!({
                    name_key: e.name,
                    f.arn_key: e.arn,
                    "CreationTime": e.created,
                });
                // family-specific summary extras
                match e.kind {
                    "algorithm" => summary["AlgorithmName"] = json!(e.name),
                    "notebook-instance" => {}
                    "training-job"
                    | "processing-job"
                    | "transform-job"
                    | "compilation-job"
                    | "hyper-parameter-tuning-job"
                    | "auto-ml-job"
                    | "labeling-job"
                    | "inference-experiment" => {
                        summary["CreationTime"] = json!(e.created);
                    }
                    "model-package-group" => {
                        summary["ModelPackageGroupArn"] = json!(e.arn);
                        summary["CreationTime"] = json!(e.created);
                    }
                    _ => {}
                }
                without_nulls(&summary)
            })
            .collect()
    }

    fn route(&mut self, op: &str, params: &Value) -> Response {
        // --- non-family ops ------------------------------------------------
        match op {
            "AddTags" => return self.op_add_tags(params),
            "DeleteTags" => return self.op_delete_tags(params),
            "ListTags" => return self.op_list_tags(params),
            "DescribeTags" => return self.op_describe_tags(params),
            "CreatePresignedNotebookInstanceUrl"
            | "CreatePresignedDomainUrl"
            | "CreatePresignedMlflowAppUrl"
            | "CreatePresignedMlflowTrackingServerUrl" => {
                return ok(json!({"AuthorizedUrl": "https://example.com/presigned"}))
            }
            "StartSession" => return ok(json!({"StreamUrl": "https://example.com/session"})),
            "CreateHubContentPresignedUrls" | "CreatePartnerAppPresignedUrl" => {
                return ok(json!({}))
            }
            "GetSagemakerServicecatalogPortfolioStatus" => {
                return ok(json!({
                    "Status": if self.catalog_enabled { "Enabled" } else { "Disabled" }
                }))
            }
            "EnableSagemakerServicecatalogPortfolio" => {
                self.catalog_enabled = true;
                return empty();
            }
            "DisableSagemakerServicecatalogPortfolio" => {
                self.catalog_enabled = false;
                return empty();
            }
            "RenderUiTemplate" => {
                return ok(json!({"RenderedContent": "<html></html>", "Errors": []}))
            }
            "QueryLineage" => return ok(json!({"Vertices": [], "Edges": []})),
            "GetSearchSuggestions" => return ok(json!({"PropertyNameSuggestions": []})),
            "BatchDescribeModelPackage" => {
                return ok(
                    json!({"ModelPackageSummaries": {}, "BatchDescribeModelPackageErrorMap": {}}),
                )
            }
            "ListModelMetadata" => return ok(json!({"ModelMetadataSummaries": []})),
            "ListAliases" => return ok(json!({"SageMakerImageVersionAliases": []})),
            "ListResourceCatalogs" => return ok(json!({"ResourceCatalogs": []})),
            "UpdateNotebookInstance"
            | "UpdateNotebookInstanceLifecycleConfig"
            | "UpdateNotebookInstanceLifecycle" => return empty(),
            "UpdateFeatureMetadata"
            | "UpdateInferenceComponentRuntimeConfig"
            | "UpdateDevices"
            | "RegisterDevices"
            | "DeregisterDevices"
            | "BatchAddClusterNodes"
            | "BatchDeleteClusterNodes"
            | "BatchRebootClusterNodes"
            | "BatchReplaceClusterNodes"
            | "AttachClusterNodeVolume"
            | "DetachClusterNodeVolume"
            | "DescribeClusterEvent"
            | "ListClusterEvents"
            | "CreateEdgeDeploymentStage"
            | "DeleteEdgeDeploymentStage"
            | "StartEdgeDeploymentStage"
            | "StopEdgeDeploymentStage"
            | "StartEdgeDeploymentPlan"
            | "StopEdgePackagingJob"
            | "StartMonitoringSchedule"
            | "StopMonitoringSchedule"
            | "DeleteModelPackageGroupPolicy"
            | "StopProcessingJob"
            | "StopTrainingJob"
            | "StopTransformJob"
            | "StopCompilationJob"
            | "StopHyperParameterTuningJob"
            | "StopLabelingJob"
            | "StopOptimizationJob"
            | "StopInferenceRecommendationsJob" => return empty(),
            "StopAutoMLJob" => {
                let name = get(params, "AutoMLJobName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if self.get_arn_by_kind_name("auto-ml-job", name).is_none()
                    && self.get_arn_by_kind_name("auto-ml-job-v2", name).is_none()
                {
                    return Response::json_status(
                        400,
                        &json!({
                            "__type": "ResourceNotFound",
                            "message": format!("Could not find AutoML job {name}")
                        }),
                    );
                }
                return empty();
            }
            "ListCandidatesForAutoMLJob" => return ok(json!({"Candidates": []})),
            "ListAssociations" => return ok(json!({"AssociationSummaries": []})),
            "ListClusterSchedulerConfigs" => {
                return ok(json!({"ClusterSchedulerConfigSummaries": []}))
            }
            "ListComputeQuotas" => return ok(json!({"ComputeQuotaSummaries": []})),
            "ListClusterNodes" => return ok(json!({"ClusterNodeSummaries": []})),
            "ListImageVersions" => return ok(json!({"ImageVersions": []})),
            "ListHubContents" | "ListHubContentVersions" => {
                return ok(json!({"HubContentSummaries": []}))
            }
            "DeleteHubContent" | "DeleteHubContentReference" | "DeleteImageVersion" => {
                return empty()
            }
            "CreateHubContentReference" | "UpdateHubContentReference" => {
                return ok(json!({"HubArn": "_arn"}))
            }
            "ImportHubContent" => return ok(json!({"HubArn": "_arn", "HubContentArn": "_arn"})),
            "CreateTrainingPlan" => return ok(json!({"TrainingPlanArn": "_arn"})),
            "DescribeTrainingPlan" => {
                let name = get(params, "TrainingPlanName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                return ok(
                    json!({"TrainingPlanArn": "_arn", "TrainingPlanName": name, "Status": "Active"}),
                );
            }
            "DescribeReservedCapacity" => {
                return ok(json!({"ReservedCapacityArn": "_arn", "Status": "Active"}))
            }
            "DescribeSubscribedWorkteam" => {
                return ok(
                    json!({"SubscribedWorkteam": {"WorkteamArn": "_arn", "ListingId": "listing-00000000"}}),
                )
            }
            "DescribeLineageGroup" => {
                let name = get(params, "LineageGroupName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                return ok(json!({"LineageGroupName": name, "LineageGroupArn": "_arn"}));
            }
            "GetLineageGroupPolicy" => {
                let _ = params;
                return ok(json!({"LineageGroupArn": "_arn", "ResourcePolicy": "{}"}));
            }
            "DescribeModelCardExportJob" => {
                let name = get(params, "ModelCardExportJobName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                return ok(json!({
                    "ModelCardExportJobName": name,
                    "ModelCardExportJobArn": "_arn",
                    "Status": "Completed",
                    "ModelCardName": name,
                    "ModelCardVersion": 1,
                    "OutputConfig": {},
                    "CreatedAt": 1704067200.0
                }));
            }
            "CreateModelCardExportJob" => {
                return ok(json!({"ModelCardExportJobArn": "_arn"}));
            }
            "DescribeFeatureMetadata" => {
                let fg = get(params, "FeatureGroupName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let fname = get(params, "FeatureName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                return ok(json!({
                    "FeatureGroupArn": "_arn",
                    "FeatureGroupName": fg,
                    "FeatureName": fname,
                    "FeatureType": "String",
                    "Parameters": []
                }));
            }
            "DescribeHubContent" => {
                let hc = get(params, "HubContentName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                return ok(json!({
                    "HubContentName": hc,
                    "HubContentArn": "_arn",
                    "HubContentType": get(params, "HubContentType"),
                    "HubContentDocument": "{}",
                    "HubContentStatus": "Available"
                }));
            }
            "DescribeImageVersion" => {
                return ok(
                    json!({"ImageVersionArn": "_arn", "ImageVersionStatus": "CREATED", "Version": 1}),
                )
            }
            "GetScalingConfigurationRecommendation" => return empty(),
            "DescribeClusterNode" => {
                let cluster = get(params, "ClusterName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if self.get_arn_by_kind_name("cluster", cluster).is_none() {
                    return validation(&format!("Could not find cluster '{cluster}'."));
                }
                return empty();
            }
            "AssociateTrialComponent" | "DisassociateTrialComponent" => {
                let tc = get(params, "TrialComponentName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let trial = get(params, "TrialName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                return ok(json!({
                    "TrialComponentArn": format!("arn:aws:sagemaker:{}:{}:experiment-trial-component/{tc}", self.region(), self.account),
                    "TrialArn": format!("arn:aws:sagemaker:{}:{}:experiment-trial/{trial}", self.region(), self.account),
                }));
            }

            "ListMonitoringAlerts" => return ok(json!({"MonitoringAlertSummaries": []})),
            "ListMonitoringAlerts" => return ok(json!({"MonitoringAlertSummaries": []})),
            "ListMonitoringAlertHistory" => return ok(json!({"MonitoringAlertHistory": []})),
            "ListMonitoringExecutions" => return ok(json!({"MonitoringExecutionSummaries": []})),
            "ListPipelineExecutionSteps" => return ok(json!({"PipelineExecutionSteps": []})),
            "ListPipelineParametersForExecution" => {
                return ok(json!({"PipelineExecutionParameters": []}))
            }
            "RetryPipelineExecution"
            | "SendPipelineExecutionStepSuccess"
            | "SendPipelineExecutionStepFailure"
            | "StopPipelineExecution" => return ok(json!({"PipelineExecutionArn": "_arn"})),
            "ListPipelineVersions" => return ok(json!({"PipelineVersionSummaries": []})),
            "ListModelCardVersions" => {
                let name = get(params, "ModelCardName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                match self.get_arn_by_kind_name("model-card", name) {
                    None => {
                        return resource_not_found_plain(&format!(
                            "Modelcard {name} does not exist"
                        ));
                    }
                    Some(arn) => {
                        let created = self
                            .resources
                            .get(&SagemakerState::key("model-card", name))
                            .map(|e| e.created)
                            .unwrap_or(1704067200.0);
                        return ok(json!({
                            "ModelCardVersionSummaryList": [{
                                "ModelCardName": name,
                                "ModelCardArn": arn,
                                "ModelCardVersion": 1,
                                "ModelCardStatus": "Draft",
                                "CreationTime": created,
                            }],
                            "NextToken": null
                        }));
                    }
                }
            }
            "ListModelCardExportJobs" => return ok(json!({"ModelCardExportJobSummaries": []})),
            "ListTrainingPlans" => return ok(json!({"TrainingPlanSummaries": []})),
            "ListTrainingJobsForHyperParameterTuningJob" => {
                return ok(json!({"TrainingJobSummaries": []}))
            }
            "ListSubscribedWorkteams" => return ok(json!({"SubscribedWorkteams": []})),
            "ListLabelingJobsForWorkteam" => return ok(json!({"LabelingJobSummaryList": []})),
            "ListLineageGroups" => return ok(json!({"LineageGroupSummaries": []})),
            "ListInferenceRecommendationsJobSteps" => return ok(json!({"Steps": []})),
            "SearchTrainingPlanOfferings" => return ok(json!({"TrainingPlanOfferings": []})),
            "ListUltraServersByReservedCapacity" => return ok(json!({})),
            "CreateClusterSchedulerConfig" => {
                return ok(
                    json!({"ClusterSchedulerConfigArn": "_arn", "ClusterSchedulerConfigId": "csc-00000000"}),
                )
            }
            "DescribeClusterSchedulerConfig" => {
                return ok(
                    json!({"ClusterSchedulerConfigId": "csc-00000000", "Status": "Completed"}),
                )
            }
            "DeleteClusterSchedulerConfig" | "DeleteComputeQuota" => return empty(),
            "CreateComputeQuota" => {
                return ok(json!({"ComputeQuotaArn": "_arn", "ComputeQuotaId": "cq-00000000"}))
            }
            "DescribeComputeQuota" => {
                return ok(json!({"ComputeQuotaId": "cq-00000000", "Status": "Completed"}))
            }
            "DeletePartnerApp" | "DeleteWorkteam" | "DeleteWorkforce" => return empty(),
            "StartInferenceExperiment" => {
                let name = get(params, "Name")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let Some(e) = self
                    .resources
                    .get_mut(&SagemakerState::key("inference-experiment", name))
                else {
                    // moto returns a null-arn stub for missing experiments
                    return ok(json!({"InferenceExperimentArn": "_arn"}));
                };
                e.status = Some("Running");
                let arn = e.arn.clone();
                return ok(json!({"InferenceExperimentArn": arn}));
            }
            "StopInferenceExperiment" => {
                let name = get(params, "Name")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let Some(e) = self
                    .resources
                    .get_mut(&SagemakerState::key("inference-experiment", name))
                else {
                    return ok(json!({"InferenceExperimentArn": "_arn"}));
                };
                e.status = Some("Stopped");
                let arn = e.arn.clone();
                return ok(json!({"InferenceExperimentArn": arn}));
            }
            "StartPipelineExecution" => return self.op_start_pipeline_execution(params),
            "StartNotebookInstance" => return self.op_start_notebook(params),
            "StopNotebookInstance" => return self.op_stop_notebook(params),
            "Search" => return self.op_search(params),
            "GetModelPackageGroupPolicy" => return ok(json!({"ResourcePolicy": "{}"})),
            "PutModelPackageGroupPolicy" => {
                if let Some(g) = get(params, "ModelPackageGroupName") {
                    let arn = self
                        .get_arn_by_kind_name("model-package-group", g.as_str().unwrap_or(""))
                        .unwrap_or_else(|| {
                            format!(
                                "arn:aws:sagemaker:{}:{}:model-package-group/{}",
                                self.region(),
                                self.account,
                                g.as_str().unwrap_or("")
                            )
                        });
                    return ok(json!({"ModelPackageGroupArn": arn}));
                }
                return ok(json!({"ModelPackageGroupArn": null}));
            }
            "GetLineageGroupPolicy" => {
                return ok(json!({"LineageGroupArn": null, "ResourcePolicy": "{}"}))
            }
            "GetDeviceFleetReport" => {
                let name = get(params, "DeviceFleetName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let arn = self
                    .get_arn_by_kind_name("device-fleet", name)
                    .unwrap_or_else(|| {
                        format!(
                            "arn:aws:sagemaker:{r}:{a}:device-fleet/{name}",
                            r = self.region(),
                            a = self.account
                        )
                    });
                return ok(
                    json!({"DeviceFleetArn": arn, "DeviceFleetName": get(params, "DeviceFleetName"), "OutputConfig": {}}),
                );
            }
            "DescribeDevice" => {
                let name = get(params, "DeviceName").cloned().unwrap_or(json!("d"));
                let fleet = get(params, "DeviceFleetName")
                    .cloned()
                    .unwrap_or(json!("df"));
                return ok(
                    json!({"DeviceName": name, "DeviceFleetName": fleet, "RegistrationTime": 1704067200.0}),
                );
            }
            "ListDevices" => return ok(json!({"DeviceSummaries": []})),
            "DescribeEdgePackagingJob" => {
                // edge-packaging family handles its own describes
            }
            _ => {}
        }

        // generic Update<Kind> plus one-off mutation arms (before family ops)
        if let Some(resp) = self.route_update(op, params) {
            return resp;
        }

        // --- family-driven operations --------------------------------------
        let found = FAMILIES.iter().find(|f| {
            f.create_op == op || f.describe_op == op || f.list_op == op || f.delete_op == op
        });
        let Some(f) = found else {
            return crate::proto::json_error(
                501,
                "NotImplemented",
                &format!("The {} action has not been implemented", op),
            );
        };
        if f.create_op == op {
            return self.op_create(f, params);
        }
        if f.describe_op == op {
            return self.op_describe(f, params);
        }
        if f.list_op == op {
            let items = self.list_kind(f.kind);
            let items = apply_list_filters(f, params, items);
            let (page, token) = paginate(params, &items);
            let mut body = json!({ f.list_key: page });
            if !f.list_status_key.is_empty() {
                if let Some(s) = get(params, f.list_status_key) {
                    body[f.list_status_key] = s.clone();
                }
            }
            if let Some(t) = token {
                body["NextToken"] = json!(t);
            }
            return ok(body);
        }
        if f.delete_op == op {
            return self.op_delete(f, params);
        }
        // generic Update<Kind> ops: return the ARN shape (null when moto
        // does), validating the resource when moto does.
        if let Some(rest) = op.strip_prefix("Update") {
            let expect = format!("{}:Update{rest}", "");
            let update_family = FAMILIES.iter().find(|f| {
                op == format!("Update{}", family_camel(f.kind))
                    || op == format!("Update{}", camelize_special(f.kind))
            });
            if let Some(uf) = update_family {
                let name_field = update_name_field(op, uf);
                let name = get(params, name_field)
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if let Some(arn) = self.get_arn_by_kind_name(uf.kind, name) {
                    if arn_ok_response(uf.kind) {
                        let key = match uf.kind {
                            "mlflow-app" | "app" => "Arn",
                            _ => uf.arn_key,
                        };
                        let _ = &expect;
                        return ok(json!({ key: arn }));
                    }
                    return empty();
                }
                if matches!(
                    uf.kind,
                    "action"
                        | "context"
                        | "flow-definition"
                        | "hub"
                        | "code-repository"
                        | "image"
                        | "workforce"
                        | "workteam"
                        | "project"
                        | "app"
                        | "space"
                        | "user-profile"
                        | "device-fleet"
                        | "monitoring-schedule"
                        | "inference-experiment"
                        | "inference-component"
                        | "human-task-ui"
                        | "model-card"
                        | "endpoint-config"
                        | "experiment-trial-component"
                        | "artifact"
                ) {
                    let synthetic_arn = format!(
                        "arn:aws:sagemaker:{r}:{a}:{path}/{name}",
                        r = self.region(),
                        a = self.account,
                        path = uf.arn_path
                    );
                    return Response::json_status(400, &missing_body(uf, name, &synthetic_arn));
                }
                return ok(json!({ uf.arn_key: Value::Null }));
            }
            let _ = &expect;
        }
        crate::proto::json_error(
            501,
            "NotImplemented",
            &format!("The {} action has not been implemented", op),
        )
    }

    fn op_create(&mut self, f: &Family, params: &Value) -> Response {
        let mut name = get(params, f.name_param)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        // moto formats a missing artifact name into the ARN literally
        if name.is_empty() && matches!(f.kind, "artifact") {
            name = "None".to_string();
        }
        if name.is_empty() && f.kind != "model-package" {
            return validation(&format!(
                "Value \"\" at '{}'. failed to satisfy constraint: Member must have length greater than or equal to 1",
                f.name_param
            ));
        }
        if name.is_empty() && f.kind == "model-package" {
            if let Some(group) = get(params, "ModelPackageGroupName").and_then(Value::as_str) {
                let n = self
                    .resources
                    .values()
                    .filter(|e| {
                        e.kind == "model-package" && e.name.starts_with(&format!("{group}/"))
                    })
                    .count()
                    + 1;
                name = format!("{group}/{n}");
            }
        }
        // duplicates: some kinds reject, most just overwrite
        if let Some(existing) = self.get_arn(f, &name) {
            match f.kind {
                "notebook-instance" => {
                    return validation(&format!(
                        "Cannot create a duplicate Notebook Instance ({existing})"
                    ));
                }
                "model-card" => {
                    return conflict(&format!("Modelcard {name} already exists"));
                }
                "model-package-group"
                | "model-package"
                | "experiment"
                | "pipeline"
                | "code-repository"
                | "image"
                | "action"
                | "context"
                | "artifact"
                | "human-task-ui"
                | "workteam"
                | "workforce" => {
                    // moto allows duplicates for these (returns same shape)
                }
                _ => {
                    self.resources.remove(&SagemakerState::key(f.kind, &name));
                }
            }
        }
        let arn = self.arn_for(f, &name);
        let mut props = Map::new();
        for p in f.passthrough {
            if let Some(v) = get(params, p) {
                props.insert((*p).to_string(), v.clone());
            }
        }
        let created = now_ts();
        let entry = Entry {
            kind: f.kind,
            name: name.clone(),
            arn: arn.clone(),
            created,
            modified: None,
            status: if f.status.is_empty() {
                None
            } else {
                Some(f.status)
            },
            props: Value::Object(props),
        };
        if let Some(tags) = get(params, "Tags").and_then(Value::as_array) {
            self.tags.insert(arn.clone(), tags.clone());
        }
        self.resources
            .insert(SagemakerState::key(f.kind, &name), entry);

        // create response per family (matches moto's response_create)
        let mut body = json!({ f.arn_key: arn.clone() });
        match f.kind {
            "domain" => {
                body["Url"] = json!(format!("{name}.{r}.sagemaker.test.com", r = self.region()));
            }
            _ => {}
        }
        ok(body)
    }

    fn op_describe(&mut self, f: &Family, params: &Value) -> Response {
        let name_field = match f.kind {
            "domain" => "DomainId",
            "mlflow-app" => "Arn",
            "partner-app" => "Arn",
            _ => f.name_param,
        };
        let name = get(params, name_field)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        // domain describe keys on the `d-{name}` id but state stores the name
        let name: String = match f.kind {
            "domain" | "user-profile" | "space" | "app" => strip_d_prefix(&name).to_string(),
            _ => name,
        };
        let snapshot = {
            let cloned = self
                .resources
                .get(&SagemakerState::key(&f.kind, &name))
                .cloned();
            cloned
        };
        let Some(e) = snapshot else {
            let synthetic_arn = format!(
                "arn:aws:sagemaker:{r}:{a}:{path}/{name}",
                r = self.region(),
                a = self.account,
                path = f.arn_path
            );
            return Response::json_status(400, &missing_body(f, &name, &synthetic_arn));
        };
        let body = self.describe_body_of(e.kind, &e);
        ok(body)
    }

    /// Detail body per family (the events payload of describe).
    fn describe_body_of(&self, kind_key: &str, e: &Entry) -> Value {
        let f = family_of(kind_key);
        let mut body = json!({
            f.name_key: e.name,
            f.arn_key: e.arn,
            "CreationTime": e.created,
            "LastModifiedTime": e.modified.unwrap_or(e.created),
        });
        if let Some(status) = e.status {
            // kinds use several status key names
            let status_key = match f.kind {
                "endpoint" => "EndpointStatus",
                "notebook-instance" => "NotebookInstanceStatus",
                "pipeline" | "pipeline-execution" => "PipelineExecutionStatus",
                "compilation-job"
                | "processing-job"
                | "training-job"
                | "transform-job"
                | "hyper-parameter-tuning-job"
                | "auto-ml-job"
                | "labeling-job" => "JobStatus",
                "model-package" | "model-package-group" => "ModelPackageStatus",
                "model-card" => "ModelCardStatus",
                "cluster" => "ClusterStatus",
                "algorithm" => "AlgorithmStatus",
                "image" => "ImageStatus",
                "image-version" => "ImageVersionStatus",
                "inference-experiment" => "Status",
                "domain" => "DomainStatus",
                "user-profile" => "Status",
                "space" => "Status",
                "hub" => "HubStatus",
                "feature-group" => "FeatureGroupStatus",
                "workforce" => "Status",
                "workteam" => "Status",
                "mlflow-tracking-server" => "TrackingServerStatus",
                _ => "Status",
            };
            body[status_key] = json!(status);
        }
        if let Value::Object(props) = &e.props {
            for (k, v) in props {
                body[k] = v.clone();
            }
        }
        for (k, dv) in f.defaults {
            if let (Dv::Null, _) | (Dv::Nothing, _) = (dv, ()) {
                continue;
            }
            if body.get(k).map(Value::is_null).unwrap_or(true) {
                body[*k] = dv.to_value();
            }
        }
        // extra per-family fields
        match f.kind {
            "notebook-instance" => {
                if body.get("NotebookInstanceStatus").and_then(Value::as_str) == Some("InService") {
                    body["Url"] = json!(format!(
                        "{name}.notebook.{r}.sagemaker.aws",
                        name = e.name,
                        r = self.region()
                    ));
                } else {
                    body["Url"] = json!(format!(
                        "{name}.notebook.{r}.sagemaker.aws",
                        name = e.name,
                        r = self.region()
                    ));
                }
                body["DirectInternetAccess"] = json!("Enabled");
                body["InstanceMetadataServiceRoute"] = json!(null);
                body["SubnetId"] = body.get("SubnetId").cloned().unwrap_or(Value::Null);
                body["SecurityGroupIds"] =
                    body.get("SecurityGroupIds").cloned().unwrap_or(json!([]));
            }
            "domain" => {
                body["DomainId"] = json!(format!("d-{}", e.name));
                body["Url"] = json!(format!(
                    "{name}.{r}.sagemaker.test.com",
                    name = e.name,
                    r = self.region()
                ));
                body["DomainStatus"] = json!("InService");
                body["IamIdentityCenterInstanceArn"] = Value::Null;
                body["Tags"] = json!([]);
            }
            "image" => {
                body["ImageVersion"] = json!(1);
            }
            "image-version" => {
                body["ImageVersionArn"] = json!(e.arn);
                body["Version"] = json!(1);
                body["ImageStatus"] = json!("CREATED");
            }
            "training-job" => {
                body["ModelArtifacts"] = json!({"S3ModelArtifacts": format!("s3://my-bucket/output/{}/output/model.tar.gz", e.name)});
                body["TrainingStartTime"] = json!(e.created);
                body["TrainingEndTime"] = json!(e.created);
                body["BillableTimeInSeconds"] = json!(3);
                body["LabelingJobArn"] = Value::Null;
            }
            "processing-job" => {
                body["ProcessingStartTime"] = json!(e.created);
                body["ProcessingEndTime"] = json!(e.created);
            }
            "compilation-job" => {
                body["CompilationStartTime"] = json!(e.created);
                body["CompilationEndTime"] = json!(e.created);
            }
            "model-package" => {
                body["ModelPackageStatusDetails"] = json!({
                    "ValidationStatuses": [{"Name": e.arn, "Status": "Completed"}],
                });
            }
            "experiment-trial-component" => {
                body["DisplayName"] = body.get("DisplayName").cloned().unwrap_or(json!(e.name));
            }
            "pipeline" => {
                body["LastRunTime"] = json!(e.modified.unwrap_or(e.created));
            }
            "space" => {
                body["Status"] = json!("InService");
                body["HomeEfsFileSystemUid"] = json!("00000000");
            }
            "user-profile" => {
                body["Status"] = json!("InService");
                body["HomeEfsFileSystemUid"] = json!("00000000");
                body["UserProfileArn"] = json!(e.arn);
            }
            "feature-group" => {
                body["FeatureGroupStatus"] = json!("Created");
                body["LastUpdatedTime"] = json!(e.created);
            }
            _ => {}
        }
        without_nulls(&body)
    }
}

impl SagemakerState {
    fn get_arn_by_kind_name(&self, kind: &str, name: &str) -> Option<String> {
        self.resources
            .values()
            .find(|e| e.kind == kind && e.name == name)
            .map(|e| e.arn.clone())
    }

    fn op_delete(&mut self, f: &Family, params: &Value) -> Response {
        let param = match f.kind {
            "domain" => "DomainId",
            "mlflow-app" | "partner-app" => "Arn",
            _ => f.name_param,
        };
        let mut name = get(params, param)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        match f.kind {
            "domain" | "user-profile" | "space" | "app" => {
                name = strip_d_prefix(&name).to_string();
            }
            "mlflow-app" | "partner-app" => {
                name = name.rsplit('/').next().unwrap_or("").to_string();
            }
            _ => {}
        }
        match self.resources.remove(&SagemakerState::key(f.kind, &name)) {
            Some(e) => {
                self.tags.remove(&e.arn);
                match f.kind {
                    "pipeline" => ok(json!({"PipelineArn": e.arn})),
                    "cluster" => ok(json!({"ClusterArn": e.arn})),
                    _ => empty(),
                }
            }
            None => {
                if matches!(f.kind, "cluster" | "domain" | "device-fleet") {
                    let synthetic_arn = format!(
                        "arn:aws:sagemaker:{r}:{a}:{path}/{name}",
                        r = self.region(),
                        a = self.account,
                        path = f.arn_path
                    );
                    return validation(
                        &missing_body(f, &name, &synthetic_arn)["message"]
                            .as_str()
                            .unwrap_or(""),
                    );
                }
                if matches!(f.kind, "hub" | "monitoring-schedule") {
                    let synthetic_arn = format!(
                        "arn:aws:sagemaker:{r}:{a}:{path}/{name}",
                        r = self.region(),
                        a = self.account,
                        path = f.arn_path
                    );
                    return Response::json_status(400, &missing_body(f, &name, &synthetic_arn));
                }
                if f.kind == "model-card" {
                    return resource_not_found_plain(&format!("Modelcard {name} does not exist."));
                }
                if f.kind == "model" {
                    // moto models render an S3-style XML error for missing models
                    return Response::xml(
                        404,
                        format!(
                            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>NoSuch{}</Code>
    <Message><![CDATA[Could not find {}]]></Message>
</Error>"#,
                            kind_camel(f.kind),
                            f.kind.replace('-', " ")
                        )
                        .into_bytes(),
                    );
                }
                if f.delete_missing_ok {
                    empty()
                } else {
                    let synthetic_arn = format!(
                        "arn:aws:sagemaker:{r}:{a}:{path}/{name}",
                        r = self.region(),
                        a = self.account,
                        path = f.arn_path
                    );
                    Response::json_status(400, &missing_body(f, &name, &synthetic_arn))
                }
            }
        }
    }

    fn op_start_notebook(&mut self, params: &Value) -> Response {
        let name = get(params, "NotebookInstanceName")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(e) = self
            .resources
            .get_mut(&SagemakerState::key("notebook-instance", name))
        else {
            return validation("RecordNotFound");
        };
        e.status = Some("InService");
        e.modified = Some(now_ts());
        empty()
    }

    fn op_stop_notebook(&mut self, params: &Value) -> Response {
        let name = get(params, "NotebookInstanceName")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if let Some(e) = self
            .resources
            .get_mut(&SagemakerState::key("notebook-instance", name))
        {
            e.status = Some("Stopped");
            e.modified = Some(now_ts());
        }
        empty()
    }

    // --- tags ------------------------------------------------------------

    /// moto's list_tags paginates per resource; the suite passes ResourceArn.
    fn op_list_tags(&mut self, params: &Value) -> Response {
        let arn = get(params, "ResourceArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        let Some(base) = resolve_resource_arn(self, arn) else {
            return ok(json!({"Tags": [], "NextToken": null}));
        };
        let tags = self.tags.get(&base).cloned().unwrap_or_default();
        ok(json!({"Tags": tags, "NextToken": null}))
    }

    fn op_describe_tags(&mut self, params: &Value) -> Response {
        let arn = get(params, "ResourceArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        let Some(base) = resolve_resource_arn(self, arn) else {
            return ok(json!({"Tags": []}));
        };
        let tags = self.tags.get(&base).cloned().unwrap_or_default();
        ok(json!({"Tags": tags}))
    }

    fn op_add_tags(&mut self, params: &Value) -> Response {
        let arn = get(params, "ResourceArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        let Some(base) = resolve_resource_arn(self, arn) else {
            return ok(json!({"Tags": []}));
        };
        let tags = get(params, "Tags")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let entry = self.tags.entry(base.clone()).or_default();
        for tag in tags {
            let key = tag.get("Key").and_then(Value::as_str).unwrap_or("");
            if !key.is_empty() {
                entry.retain(|t| t.get("Key").and_then(Value::as_str) != Some(key));
            }
            entry.push(tag);
        }
        let tags = self.tags.get(&base).cloned().unwrap_or_default();
        ok(json!({"Tags": tags}))
    }

    fn op_delete_tags(&mut self, params: &Value) -> Response {
        let arn = get(params, "ResourceArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        let Some(base) = resolve_resource_arn(self, arn) else {
            return empty();
        };
        let keys: Vec<String> = get(params, "TagKeys")
            .or_else(|| get(params, "TagList"))
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|t| t.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        if let Some(entry) = self.tags.get_mut(&base) {
            entry.retain(|t| {
                let k = t.get("Key").and_then(Value::as_str).unwrap_or("");
                !keys.contains(&k.to_string())
            });
        }
        empty()
    }

    /// Cross-resource Search: emits the resource dicts moto produces.
    fn op_search(&mut self, params: &Value) -> Response {
        let resource = get(params, "Resource")
            .and_then(Value::as_str)
            .unwrap_or("Experiment")
            .to_string();
        let _ = resource;
        // moto returns every resource of the requested type; the suite calls
        // this with Experiment/TrainingJob/Model etc. after creations.
        let mut results = Vec::new();
        for kw in [
            "Experiment",
            "Trial",
            "TrialComponent",
            "Model",
            "EndpointConfig",
            "TrainingJob",
            "ProcessingJob",
            "Pipeline",
            "FeatureGroup",
            "HyperParameterTuningJob",
        ] {
            let _ = kw;
        }
        // generic: one result group per matching resource, keyed by kind name
        let want = match resource.as_str() {
            "Experiment" => "experiment",
            "Trial" => "experiment-trial",
            "TrialComponent" => "experiment-trial-component",
            "Model" => "model",
            "EndpointConfig" => "endpoint-config",
            "Endpoint" => "endpoint",
            "TrainingJob" => "training-job",
            "ProcessingJob" => "processing-job",
            "TransformJob" => "transform-job",
            "Pipeline" => "pipeline",
            "FeatureGroup" => "feature-group",
            "HyperParameterTuningJob" => "hyper-parameter-tuning-job",
            "ModelDomain" | "DataZoneDomain" => "domain",
            _ => "",
        };
        if !want.is_empty() {
            for e in self.resources.values().filter(|e| e.kind == want) {
                let body = self.describe_body_of(e.kind, e);
                let inner_key = match want {
                    "experiment" => "Experiment",
                    "experiment-trial" => "Trial",
                    "experiment-trial-component" => "TrialComponent",
                    "model" => "Model",
                    "endpoint-config" => "EndpointConfig",
                    "endpoint" => "Endpoint",
                    "training-job" => "TrainingJob",
                    "processing-job" => "ProcessingJob",
                    "transform-job" => "TransformJob",
                    "pipeline" => "Pipeline",
                    "feature-group" => "FeatureGroup",
                    "hyper-parameter-tuning-job" => "HyperParameterTuningJob",
                    "domain" => "Domain",
                    _ => "Resource",
                };
                let mut item = json!({ inner_key: body });
                item[inner_key]["Tags"] = json!(self.tags.get(&e.arn).cloned().unwrap_or_default());
                results.push(item);
            }
        }
        Response::json_status(200, &json!({ "Results": results }))
    }
}

/// Map a client-provided ARN to the canonical resource ARN in state
/// (resources store their full ARNs; name-only lookups use the last segment).
fn resolve_resource_arn(st: &SagemakerState, arn: &str) -> Option<String> {
    if st.tags.contains_key(arn) || st.resources.values().any(|e| e.arn == arn) {
        return Some(arn.to_string());
    }
    // maybe a plain resource name or an old-format ARN: try suffix match
    let fallback_name = arn.rsplit('/').next().unwrap_or(arn);
    for e in st.resources.values() {
        if e.name == fallback_name || arn.ends_with(&format!("/{}", e.name)) {
            return Some(e.arn.clone());
        }
    }
    None
}

/// Apply the common list filters (NameContains, StatusEquals, SortBy/SortOrder
/// support where the request carries them).
fn apply_list_filters(f: &Family, params: &Value, mut items: Vec<Value>) -> Vec<Value> {
    if let Some(c) = get(params, "NameContains").and_then(Value::as_str) {
        items.retain(|i| {
            i.as_object()
                .map(|o| {
                    o.values()
                        .any(|v| v.as_str().map(|s| s.contains(c)).unwrap_or(false))
                })
                .unwrap_or(false)
        });
    }
    if let Some(sort_by) = get(params, "SortBy").and_then(Value::as_str) {
        let key = match sort_by {
            "Name" => f.name_key.to_string(),
            "CreationTime" => "CreationTime".to_string(),
            other => other.to_string(),
        };
        let order_desc = get(params, "SortOrder").and_then(Value::as_str) == Some("Descending");
        items.sort_by(|a, b| {
            let av = a.get(&key).cloned().unwrap_or(Value::Null);
            let bv = b.get(&key).cloned().unwrap_or(Value::Null);
            let ord = match (av.as_str(), bv.as_str()) {
                (Some(x), Some(y)) => x.cmp(y),
                _ => {
                    let xf = av.as_f64().unwrap_or(0.0);
                    let yf = bv.as_f64().unwrap_or(0.0);
                    xf.partial_cmp(&yf).unwrap_or(std::cmp::Ordering::Equal)
                }
            };
            if order_desc {
                ord.reverse()
            } else {
                ord
            }
        });
    }
    items
}

/// MaxResults/NextToken pagination; token = index of the first unconsumed item.
fn paginate(params: &Value, items: &[Value]) -> (Vec<Value>, Option<usize>) {
    let max = get(params, "MaxResults")
        .and_then(Value::as_u64)
        .unwrap_or(0) as usize;
    if max == 0 {
        return (items.to_vec(), None);
    }
    let start = get(params, "NextToken")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);
    let end = (start + max).min(items.len());
    let token = if end < items.len() { Some(end) } else { None };
    (items.get(start..end).unwrap_or(&[]).to_vec(), token)
}

/// "model" -> "Model" (for the XML delete-error codes).
fn kind_camel(kind: &str) -> String {
    let mut out = String::new();
    for part in kind.split('-') {
        let mut c = part.chars();
        if let Some(first) = c.next() {
            out.extend(first.to_uppercase());
            out.push_str(c.as_str());
        }
    }
    out
}

fn strip_d_prefix(name: &str) -> &str {
    name.strip_prefix("d-").unwrap_or(name)
}

impl SagemakerState {
    fn op_start_pipeline_execution(&mut self, params: &Value) -> Response {
        let name = get(params, "PipelineName")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if self.get_arn_by_kind_name("pipeline", name).is_none() {
            return validation(&format!(
                "Could not find pipeline with PipelineName {name}."
            ));
        }
        let arn = format!(
            "arn:aws:sagemaker:{r}:{a}:pipeline/{name}/execution/{uuid}",
            r = self.region(),
            a = self.account,
            uuid = Uuid::new_v4()
        );
        self.resources.insert(
            SagemakerState::key("pipeline-execution", &arn),
            Entry {
                kind: "pipeline-execution",
                name: arn.clone(),
                arn: arn.clone(),
                created: now_ts(),
                modified: None,
                status: Some("Executing"),
                props: json!({"PipelineName": name}),
            },
        );
        ok(json!({"PipelineExecutionArn": arn}))
    }
}

/// helpers attached after impl block
fn family_camel(kind: &str) -> String {
    kind_camel(kind)
}

fn camelize_special(kind: &str) -> String {
    match kind {
        "auto-ml-job" => "AutoMLJob".to_string(),
        "auto-ml-job-v2" => "AutoMLJobV2".to_string(),
        other => family_camel(other),
    }
}

/// Start<Kind> ops that behave like pipeline executions.
fn update_name_field(op: &str, f: &Family) -> &'static str {
    match op {
        "UpdateDomain" | "DeleteDomain" => "DomainId",
        _ => f.name_param,
    }
}

fn arn_ok_response(_kind: &str) -> bool {
    true
}

impl SagemakerState {
    /// Update<Kind> and one-off mutation ops. Returns None when `op` is not
    /// one of them (route falls through to family handling).
    fn route_update(&mut self, op: &str, params: &Value) -> Option<Response> {
        // associations: moto returns the literal placeholder pair
        if op == "AddAssociation" || op == "DeleteAssociation" {
            // behave like moto: echoes of the request via tagging service
            return Some(ok(json!({
                "SourceArn": "source_arn",
                "DestinationArn": "destination_arn",
            })));
        }
        if op == "AddTags" || op == "DeleteTags" || op == "ListTags" || op == "DescribeTags" {
            return Self::noop();
        }
        if op == "UpdatePipelineExecution" || op == "UpdatePipelineVersion" {
            let key = if op == "UpdatePipelineExecution" {
                "PipelineExecutionArn"
            } else {
                "PipelineArn"
            };
            return Some(ok(json!({ key: "_arn" })));
        }
        if op == "UpdateHubContent" || op == "UpdateHubContentReference" {
            return Some(ok(json!({"HubArn": "_arn", "HubContentArn": "_arn"})));
        }
        if op == "UpdateModelPackage" {
            let arn = get(params, "ModelPackageArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            if let Some(e) = self
                .resources
                .values_mut()
                .find(|e| e.kind == "model-package" && e.arn == arn)
            {
                if let Some(status) = get(params, "ModelApprovalStatus") {
                    e.props["ModelApprovalStatus"] = status.clone();
                }
                return Some(ok(json!({"ModelPackageArn": arn})));
            }
            return Some(validation(&format!("Model package {arn} not found")));
        }
        if op == "UpdatePipelineExecutionDetail" {
            return Some(empty());
        }
        if op == "StartMlflowTrackingServer" || op == "StopMlflowTrackingServer" {
            return Some(ok(json!({"TrackingServerArn": "_arn"})));
        }
        if op == "UpdateMonitoringAlert" {
            let name = get(params, "MonitoringScheduleName")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let arn = self
                .get_arn_by_kind_name("monitoring-schedule", name)
                .unwrap_or_else(|| {
                    format!(
                        "arn:aws:sagemaker:{r}:{a}:monitoring-schedule/{n}",
                        r = self.region(),
                        a = self.account,
                        n = name
                    )
                });
            return Some(ok(json!({"MonitoringScheduleArn": arn})));
        }
        if op == "StopPipelineExecution" {
            return Some(ok(json!({"PipelineExecutionArn": "_arn"})));
        }
        if op == "UpdateTrial" {
            return Some(ok(json!({"TrialArn": "_arn"})));
        }
        if op == "UpdateTrialComponent" {
            return Some(ok(json!({"TrialComponentArn": "_arn"})));
        }
        if op == "UpdateComputeQuota" {
            return Some(ok(json!({"ComputeQuotaArn": "_arn"})));
        }
        if op == "DescribePipelineExecution" {
            let arn = get(params, "PipelineExecutionArn")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if let Some(e) = self
                .resources
                .values()
                .find(|e| e.kind == "pipeline-execution" && e.arn == arn)
            {
                return Some(ok(json!({
                    "PipelineExecutionArn": arn,
                    "PipelineExecutionStatus": "Executing",
                    "PipelineName": e.props.get("PipelineName"),
                    "CreationTime": e.created,
                    "StartTime": e.created
                })));
            }
            // moto parses the pipeline name portion out of the bogus ARN
            let extracted = arn
                .split("pipeline/")
                .nth(1)
                .and_then(|s| s.split('/').next())
                .unwrap_or(arn);
            return Some(validation(&format!(
                "Could not find pipeline with PipelineName {extracted}."
            )));
        }
        if op == "DescribePipelineDefinitionForExecution" {
            let arn = get(params, "PipelineExecutionArn")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if let Some(e) = self
                .resources
                .values()
                .find(|e| e.kind == "pipeline-execution" && e.arn == arn)
            {
                let pname = e
                    .props
                    .get("PipelineName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if let Some(p) = self
                    .resources
                    .values()
                    .find(|e2| e2.kind == "pipeline" && e2.name == pname)
                {
                    let definition = p
                        .props
                        .get("PipelineDefinition")
                        .cloned()
                        .unwrap_or(json!({}));
                    return Some(ok(json!({"PipelineDefinition": definition})));
                }
            }
            let extracted = arn
                .split("pipeline/")
                .nth(1)
                .and_then(|s| s.split('/').next())
                .unwrap_or(arn);
            return Some(validation(&format!(
                "Could not find pipeline with PipelineName {extracted}."
            )));
        }
        if op == "ListPipelineExecutions" {
            let name = get(params, "PipelineName")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let items: Vec<Value> = self
                .resources
                .values()
                .filter(|e| e.kind == "pipeline-execution")
                .filter(|e| e.props.get("PipelineName").and_then(Value::as_str) == Some(name))
                .map(|e| {
                    json!({
                        "PipelineExecutionArn": e.arn,
                        "PipelineName": e.props.get("PipelineName"),
                        "CreationTime": e.created,
                        "PipelineExecutionStatus": "Executing",
                    })
                })
                .collect();
            return Some(ok(json!({"PipelineExecutionSummaries": items})));
        }
        if op == "DescribeEdgePackagingJob" {
            let name = get(params, "EdgePackagingJobName")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if let Some(e) = self
                .resources
                .values()
                .find(|e| e.kind == "edge-packaging-job" && e.name == name)
            {
                return Some(ok(json!({
                    "EdgePackagingJobArn": e.arn,
                    "EdgePackagingJobName": e.name,
                    "EdgePackagingJobStatus": "Started"
                })));
            }
            return Some(ok(json!({
                "EdgePackagingJobArn": "_arn",
                "EdgePackagingJobName": name,
                "EdgePackagingJobStatus": "Completed"
            })));
        }
        if op == "DescribeInferenceRecommendationsJob" {
            let name = get(params, "JobName")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if let Some(e) = self
                .resources
                .values()
                .find(|e| e.kind == "inference-recommendations-job" && e.name == name)
            {
                return Some(ok(json!({
                    "JobName": name, "Status": "COMPLETED", "JobArn": e.arn
                })));
            }
            return Some(ok(json!({
                "JobName": name, "JobType": "Default", "JobArn": "_arn",
                "RoleArn": "arn:aws:iam::role/r", "Status": "COMPLETED",
                "CreationTime": 1704067200.0, "InputConfig": {}
            })));
        }
        if op == "DescribeAutoMLJob" {
            let name = get(params, "AutoMLJobName")
                .and_then(Value::as_str)
                .unwrap_or_default();
            return Some(ok(json!({
                "AutoMLJobName": name,
                "AutoMLJobArn": format!("arn:aws:sagemaker{}{}:automl-job/{name}", self.region(), self.account),
                "AutoMLJobStatus": "InProgress",
                "AutoMLJobSecondaryStatus": "Starting",
                "CreationTime": now_ts(),
                "LastModifiedTime": now_ts()
            })));
        }
        if op == "UpdateClusterSoftware" {
            return Some(ok(json!({"ClusterArn": "_arn"})));
        }
        if op == "ListStageDevices" {
            return Some(ok(json!({"DeviceDeploymentSummaries": []})));
        }
        if op == "UpdateMonitoringAlert" {
            return Some(ok(json!({"MonitoringScheduleArn": "_arn"})));
        }
        if op == "UpdateModelCard" {
            let name = get(params, "ModelCardName")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if let Some(e) = self
                .resources
                .values_mut()
                .find(|e| e.kind == "model-card" && e.name == name)
            {
                if let Some(status) = get(params, "ModelCardStatus") {
                    e.props["ModelCardStatus"] = status.clone();
                    e.modified = Some(now_ts());
                }
                let arn = e.arn.clone();
                return Some(ok(json!({"ModelCardArn": arn})));
            }
            return Some(resource_not_found_plain(&format!(
                "Modelcard {name} does not exist."
            )));
        }
        if op == "UpdateClusterSchedulerConfig" {
            return Some(ok(json!({"ClusterSchedulerConfigArn": "_arn"})));
        }
        if op == "UpdateImageVersion" || op == "CreateImageVersion" {
            let key = if op == "UpdateImageVersion" {
                "ImageVersionArn"
            } else {
                "ImageVersionArn"
            };
            let _ = key;
            return Some(ok(json!({"ImageVersionArn": "_arn"})));
        }
        if op == "UpdateEndpointWeightsAndCapacities" {
            let name = get(params, "EndpointName")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let arn = format!(
                "arn:aws:sagemaker:{r}:{a}:endpoint/{name}",
                r = self.region(),
                a = self.account
            );
            if self.get_arn_by_kind_name("endpoint", name).is_none() {
                // moto hurls the ARN embedded in quotes and a trailing period
                return Some(validation(&format!("Could not find endpoint \"{arn}\".")));
            }
            return Some(ok(json!({"EndpointArn": arn})));
        }
        if op == "CreatePartnerApp" {
            let name = get(params, "Name")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let arn = format!(
                "arn:aws:sagemaker:{r}:{a}:partner-app/{name}",
                r = self.region(),
                a = self.account
            );
            self.resources.insert(
                SagemakerState::key("partner-app", name),
                Entry {
                    kind: "partner-app",
                    name: name.to_string(),
                    arn: arn.clone(),
                    created: now_ts(),
                    modified: None,
                    status: Some("Active"),
                    props: json!({}),
                },
            );
            return Some(ok(json!({"Arn": arn})));
        }
        if op == "DescribePartnerApp" {
            let arn = get(params, "Arn").and_then(Value::as_str).unwrap_or("");
            let name = arn.rsplit('/').next().unwrap_or("");
            if self.get_arn_by_kind_name("partner-app", name).is_some() {
                return Some(ok(json!({"Arn": arn, "Status": "Active"})));
            }
            return Some(validation("Could not find partner app None"));
        }
        if op == "ListPartnerApps" {
            return Some(ok(json!({"Summaries": []})));
        }
        if op == "UpdatePartnerApp" {
            let arn = get(params, "Arn").and_then(Value::as_str).unwrap_or("");
            let name = arn.rsplit('/').next().unwrap_or("");
            if self.get_arn_by_kind_name("partner-app", name).is_some() {
                return Some(ok(json!({"Arn": arn})));
            }
            return Some(validation("Could not find partner app None"));
        }
        // generic update of a once-existing family element
        let update_family = FAMILIES.iter().find(|f| {
            op == format!("Update{}", family_camel(f.kind))
                || op == format!("Update{}", camelize_special(f.kind))
        })?;
        let uf: &Family = update_family;
        let name_field = match op {
            "UpdateDomain" => "DomainId",
            "UpdateMlflowApp" | "UpdatePartnerApp" => "Arn",
            _ => uf.name_param,
        };
        let raw = get(params, name_field)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let name: String = match uf.kind {
            "domain" | "user-profile" | "space" | "app" => strip_d_prefix(&raw).to_string(),
            "mlflow-app" => raw.rsplit('/').next().unwrap_or("").to_string(),
            _ => raw.clone(),
        };
        if let Some(arn) = self.get_arn_by_kind_name(uf.kind, &name) {
            if uf.kind == "pipeline" {
                // apply the requested changes into the pipeline entry
                if let Some(e) = self
                    .resources
                    .get_mut(&SagemakerState::key("pipeline", &name))
                {
                    for (k, dest) in [
                        ("PipelineDescription", "PipelineDescription"),
                        ("PipelineDisplayName", "PipelineDisplayName"),
                        ("RoleArn", "RoleArn"),
                        ("PipelineDefinition", "PipelineDefinition"),
                    ] {
                        if let Some(v) = get(params, k) {
                            e.props[dest] = v.clone();
                        }
                    }
                    e.modified = Some(now_ts());
                }
                return Some(ok(json!({"PipelineArn": arn})));
            }
            if matches!(
                uf.kind,
                "training-job"
                    | "trial"
                    | "experiment"
                    | "mlflow-app"
                    | "partner-app"
                    | "image-version"
                    | "pipeline-execution"
            ) {
                // moto returns the null-key variant for these
                return Some(ok(json!({ uf.arn_key: "_arn" })));
            }
            let key = match uf.kind {
                "mlflow-app" | "app" | "partner-app" => "Arn",
                _ => uf.arn_key,
            };
            // apply modification passthrough into props? keep simple: no change
            return Some(ok(json!({ key: arn })));
        }
        if matches!(
            uf.kind,
            "action"
                | "context"
                | "flow-definition"
                | "hub"
                | "code-repository"
                | "image"
                | "workforce"
                | "workteam"
                | "project"
                | "app"
                | "space"
                | "user-profile"
                | "device-fleet"
                | "monitoring-schedule"
                | "inference-experiment"
                | "inference-component"
                | "human-task-ui"
                | "model-card"
                | "endpoint-config"
                | "experiment-trial-component"
                | "artifact"
                | "model-package"
                | "app-image-config"
                | "mlflow-tracking-server"
                | "mlflow-app"
        ) {
            if uf.kind == "mlflow-app" {
                return Some(validation("Could not find mlflow app None"));
            }
            if uf.kind == "model-package" {
                return Some(validation(&format!("Model package {}/None not found", raw)));
            }
            if uf.kind == "model-card" {
                return Some(resource_not_found_plain(&format!(
                    "Modelcard {raw} does not exist."
                )));
            }
            let synthetic_arn = format!(
                "arn:aws:sagemaker:{r}:{a}:{path}/{name}",
                r = self.region(),
                a = self.account,
                path = uf.arn_path
            );
            return Some(Response::json_status(
                400,
                &missing_body(uf, &raw, &synthetic_arn),
            ));
        }
        Some(ok(json!({ uf.arn_key: "_arn" })))
    }

    fn noop() -> Option<Response> {
        None
    }
}
