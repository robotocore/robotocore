//! AWS Bedrock backend — moto-equivalent (rest-json).
//!
//! Replaces the moto sidecar for `bedrock`. Request URIs are the botocore
//! rest-json templates (e.g. `GET /custom-models/{modelIdentifier}`); the
//! gateway keeps no REST op table for bedrock, so this module resolves the
//! operation from the HTTP method + requestUri, exactly like module `ram`.
//!
//! State is per-(account, region), mirroring moto's `BedrockBackend`
//! (moto/bedrock): model customization jobs and their derived custom models,
//! guardrails with versioned snapshots, provisioned model throughput,
//! evaluation jobs (plus batch delete), inference profiles, model import
//! jobs and imported models, model copy jobs, model invocation jobs, the
//! static foundation-model catalog, marketplace model endpoints, prompt
//! routers, custom model deployments, foundation model agreements, enforced
//! guardrail configuration, use-case forms, and automated reasoning policies
//! with build workflows, test cases, and test results.
//!
//! Contract: probes/bedrock.json + tests/compatibility/test_bedrock_compat.py,
//! source `moto/bedrock/{models,responses}.py`. One documented deviation from
//! stock moto: Get/StopModelCustomizationJob also resolve the job *ARN* (moto
//! keys by job name only) — the compat suite's by-ARN assertions and moto's
//! own `_find` helpers for every sibling job resource require it.
//! Identifiers: percent-encoded path segments are split on `/` and only then
//! URL-decoded (botocore fully percent-encodes URI params, so ARNs arrive as
//! one segment).

use crate::proto::{json_error, parse_json_body, query_map, url_decode};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use chrono::Utc;
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// botocore rest-json requestUri table (method, template, operation)
// ---------------------------------------------------------------------------

static RULES: &[(&str, &str, &str)] = &[
    // POST
    ("POST", "/evaluation-jobs/batch-delete", "BatchDeleteEvaluationJob"),
    ("POST", "/automated-reasoning-policies/{policyArn}/build-workflows/{buildWorkflowId}/cancel", "CancelAutomatedReasoningPolicyBuildWorkflow"),
    ("POST", "/automated-reasoning-policies", "CreateAutomatedReasoningPolicy"),
    ("POST", "/automated-reasoning-policies/{policyArn}/test-cases", "CreateAutomatedReasoningPolicyTestCase"),
    ("POST", "/automated-reasoning-policies/{policyArn}/versions", "CreateAutomatedReasoningPolicyVersion"),
    ("POST", "/custom-models/create-custom-model", "CreateCustomModel"),
    ("POST", "/model-customization/custom-model-deployments", "CreateCustomModelDeployment"),
    ("POST", "/evaluation-jobs", "CreateEvaluationJob"),
    ("POST", "/create-foundation-model-agreement", "CreateFoundationModelAgreement"),
    ("POST", "/guardrails", "CreateGuardrail"),
    ("POST", "/guardrails/{guardrailIdentifier}", "CreateGuardrailVersion"),
    ("POST", "/inference-profiles", "CreateInferenceProfile"),
    ("POST", "/marketplace-model/endpoints", "CreateMarketplaceModelEndpoint"),
    ("POST", "/model-copy-jobs", "CreateModelCopyJob"),
    ("POST", "/model-customization-jobs", "CreateModelCustomizationJob"),
    ("POST", "/model-import-jobs", "CreateModelImportJob"),
    ("POST", "/model-invocation-job", "CreateModelInvocationJob"),
    ("POST", "/prompt-routers", "CreatePromptRouter"),
    ("POST", "/provisioned-model-throughput", "CreateProvisionedModelThroughput"),
    ("POST", "/listTagsForResource", "ListTagsForResource"),
    ("POST", "/use-case-for-model-access", "PutUseCaseForModelAccess"),
    ("POST", "/marketplace-model/endpoints/{endpointIdentifier}/registration", "RegisterMarketplaceModelEndpoint"),
    ("POST", "/automated-reasoning-policies/{policyArn}/build-workflows/{buildWorkflowType}/start", "StartAutomatedReasoningPolicyBuildWorkflow"),
    ("POST", "/automated-reasoning-policies/{policyArn}/build-workflows/{buildWorkflowId}/test-workflows", "StartAutomatedReasoningPolicyTestWorkflow"),
    ("POST", "/evaluation-job/{jobIdentifier}/stop", "StopEvaluationJob"),
    ("POST", "/model-customization-jobs/{jobIdentifier}/stop", "StopModelCustomizationJob"),
    ("POST", "/model-invocation-job/{jobIdentifier}/stop", "StopModelInvocationJob"),
    ("POST", "/tagResource", "TagResource"),
    ("POST", "/untagResource", "UntagResource"),
    // GET
    ("GET", "/automated-reasoning-policies/{policyArn}/export", "ExportAutomatedReasoningPolicyVersion"),
    ("GET", "/automated-reasoning-policies/{policyArn}", "GetAutomatedReasoningPolicy"),
    ("GET", "/automated-reasoning-policies/{policyArn}/build-workflows/{buildWorkflowId}/annotations", "GetAutomatedReasoningPolicyAnnotations"),
    ("GET", "/automated-reasoning-policies/{policyArn}/build-workflows/{buildWorkflowId}", "GetAutomatedReasoningPolicyBuildWorkflow"),
    ("GET", "/automated-reasoning-policies/{policyArn}/build-workflows/{buildWorkflowId}/result-assets", "GetAutomatedReasoningPolicyBuildWorkflowResultAssets"),
    ("GET", "/automated-reasoning-policies/{policyArn}/build-workflows/{buildWorkflowId}/scenarios", "GetAutomatedReasoningPolicyNextScenario"),
    ("GET", "/automated-reasoning-policies/{policyArn}/test-cases/{testCaseId}", "GetAutomatedReasoningPolicyTestCase"),
    ("GET", "/automated-reasoning-policies/{policyArn}/build-workflows/{buildWorkflowId}/test-cases/{testCaseId}/test-results", "GetAutomatedReasoningPolicyTestResult"),
    ("GET", "/custom-models/{modelIdentifier}", "GetCustomModel"),
    ("GET", "/model-customization/custom-model-deployments/{customModelDeploymentIdentifier}", "GetCustomModelDeployment"),
    ("GET", "/evaluation-jobs/{jobIdentifier}", "GetEvaluationJob"),
    ("GET", "/foundation-models/{modelIdentifier}", "GetFoundationModel"),
    ("GET", "/foundation-model-availability/{modelId}", "GetFoundationModelAvailability"),
    ("GET", "/guardrails/{guardrailIdentifier}", "GetGuardrail"),
    ("GET", "/imported-models/{modelIdentifier}", "GetImportedModel"),
    ("GET", "/inference-profiles/{inferenceProfileIdentifier}", "GetInferenceProfile"),
    ("GET", "/marketplace-model/endpoints/{endpointArn}", "GetMarketplaceModelEndpoint"),
    ("GET", "/model-copy-jobs/{jobArn}", "GetModelCopyJob"),
    ("GET", "/model-customization-jobs/{jobIdentifier}", "GetModelCustomizationJob"),
    ("GET", "/model-import-jobs/{jobIdentifier}", "GetModelImportJob"),
    ("GET", "/model-invocation-job/{jobIdentifier}", "GetModelInvocationJob"),
    ("GET", "/logging/modelinvocations", "GetModelInvocationLoggingConfiguration"),
    ("GET", "/prompt-routers/{promptRouterArn}", "GetPromptRouter"),
    ("GET", "/provisioned-model-throughput/{provisionedModelId}", "GetProvisionedModelThroughput"),
    ("GET", "/use-case-for-model-access", "GetUseCaseForModelAccess"),
    ("GET", "/automated-reasoning-policies", "ListAutomatedReasoningPolicies"),
    ("GET", "/automated-reasoning-policies/{policyArn}/build-workflows", "ListAutomatedReasoningPolicyBuildWorkflows"),
    ("GET", "/automated-reasoning-policies/{policyArn}/test-cases", "ListAutomatedReasoningPolicyTestCases"),
    ("GET", "/automated-reasoning-policies/{policyArn}/build-workflows/{buildWorkflowId}/test-results", "ListAutomatedReasoningPolicyTestResults"),
    ("GET", "/model-customization/custom-model-deployments", "ListCustomModelDeployments"),
    ("GET", "/custom-models", "ListCustomModels"),
    ("GET", "/enforcedGuardrailsConfiguration", "ListEnforcedGuardrailsConfiguration"),
    ("GET", "/evaluation-jobs", "ListEvaluationJobs"),
    ("GET", "/list-foundation-model-agreement-offers/{modelId}", "ListFoundationModelAgreementOffers"),
    ("GET", "/foundation-models", "ListFoundationModels"),
    ("GET", "/guardrails", "ListGuardrails"),
    ("GET", "/imported-models", "ListImportedModels"),
    ("GET", "/inference-profiles", "ListInferenceProfiles"),
    ("GET", "/marketplace-model/endpoints", "ListMarketplaceModelEndpoints"),
    ("GET", "/model-copy-jobs", "ListModelCopyJobs"),
    ("GET", "/model-customization-jobs", "ListModelCustomizationJobs"),
    ("GET", "/model-import-jobs", "ListModelImportJobs"),
    ("GET", "/model-invocation-jobs", "ListModelInvocationJobs"),
    ("GET", "/prompt-routers", "ListPromptRouters"),
    ("GET", "/provisioned-model-throughputs", "ListProvisionedModelThroughputs"),
    // PUT / PATCH
    ("PUT", "/enforcedGuardrailsConfiguration", "PutEnforcedGuardrailConfiguration"),
    ("PUT", "/logging/modelinvocations", "PutModelInvocationLoggingConfiguration"),
    ("PUT", "/guardrails/{guardrailIdentifier}", "UpdateGuardrail"),
    ("PATCH", "/automated-reasoning-policies/{policyArn}", "UpdateAutomatedReasoningPolicy"),
    ("PATCH", "/automated-reasoning-policies/{policyArn}/build-workflows/{buildWorkflowId}/annotations", "UpdateAutomatedReasoningPolicyAnnotations"),
    ("PATCH", "/automated-reasoning-policies/{policyArn}/test-cases/{testCaseId}", "UpdateAutomatedReasoningPolicyTestCase"),
    ("PATCH", "/model-customization/custom-model-deployments/{customModelDeploymentIdentifier}", "UpdateCustomModelDeployment"),
    ("PATCH", "/marketplace-model/endpoints/{endpointArn}", "UpdateMarketplaceModelEndpoint"),
    ("PATCH", "/provisioned-model-throughput/{provisionedModelId}", "UpdateProvisionedModelThroughput"),
    // DELETE
    ("DELETE", "/automated-reasoning-policies/{policyArn}", "DeleteAutomatedReasoningPolicy"),
    ("DELETE", "/automated-reasoning-policies/{policyArn}/build-workflows/{buildWorkflowId}", "DeleteAutomatedReasoningPolicyBuildWorkflow"),
    ("DELETE", "/automated-reasoning-policies/{policyArn}/test-cases/{testCaseId}", "DeleteAutomatedReasoningPolicyTestCase"),
    ("POST", "/delete-foundation-model-agreement", "DeleteFoundationModelAgreement"),
    ("DELETE", "/custom-models/{modelIdentifier}", "DeleteCustomModel"),
    ("DELETE", "/model-customization/custom-model-deployments/{customModelDeploymentIdentifier}", "DeleteCustomModelDeployment"),
    ("DELETE", "/enforcedGuardrailsConfiguration/{configId}", "DeleteEnforcedGuardrailConfiguration"),
    ("DELETE", "/guardrails/{guardrailIdentifier}", "DeleteGuardrail"),
    ("DELETE", "/imported-models/{modelIdentifier}", "DeleteImportedModel"),
    ("DELETE", "/inference-profiles/{inferenceProfileIdentifier}", "DeleteInferenceProfile"),
    ("DELETE", "/marketplace-model/endpoints/{endpointArn}", "DeleteMarketplaceModelEndpoint"),
    ("DELETE", "/logging/modelinvocations", "DeleteModelInvocationLoggingConfiguration"),
    ("DELETE", "/prompt-routers/{promptRouterArn}", "DeletePromptRouter"),
    ("DELETE", "/provisioned-model-throughput/{provisionedModelId}", "DeleteProvisionedModelThroughput"),
    ("DELETE", "/marketplace-model/endpoints/{endpointArn}/registration", "DeregisterMarketplaceModelEndpoint"),
];

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, BedrockStore>>>;

fn stores() -> Stores {
    store::slot::<BedrockStore>("bedrock")
}

/// Per-(account, region) in-memory state for AWS Bedrock.
#[derive(Default)]
struct BedrockStore {
    /// job name
    model_customization_jobs: BTreeMap<String, ModelCustomizationJob>,
    /// model name (moto keys custom models by name, not ARN)
    custom_models: BTreeMap<String, CustomModel>,
    /// 12-char guardrail id
    guardrails: BTreeMap<String, Guardrail>,
    /// provisioned model id
    provisioned_model_throughputs: BTreeMap<String, ProvisionedThroughput>,
    /// job ARN (moto keys evaluation jobs by ARN)
    evaluation_jobs: BTreeMap<String, EvaluationJob>,
    /// profile id
    inference_profiles: BTreeMap<String, InferenceProfile>,
    /// job ARN
    model_import_jobs: BTreeMap<String, ModelImportJob>,
    /// imported model name
    imported_models: BTreeMap<String, ImportedModel>,
    /// job ARN
    model_copy_jobs: BTreeMap<String, ModelCopyJob>,
    /// job ARN
    model_invocation_jobs: BTreeMap<String, ModelInvocationJob>,
    /// endpoint ARN
    marketplace_endpoints: BTreeMap<String, MarketplaceModelEndpoint>,
    /// router ARN
    prompt_routers: BTreeMap<String, PromptRouter>,
    /// deployment id
    custom_model_deployments: BTreeMap<String, CustomModelDeployment>,
    /// policy ARN
    automated_reasoning_policies: BTreeMap<String, AutomatedReasoningPolicy>,
    enforced_guardrail_configs: BTreeMap<String, Value>,
    /// arn -> {tag key: value} — moto's TaggingService
    tags: BTreeMap<String, BTreeMap<String, Option<String>>>,
    logging_config: Value,
    use_case_for_model_access: Option<Value>,
}

/// moto's `str(uuid.uuid4())[:12]` identifier quirk — the first 12 chars of
/// the hyphenated form (8 hex + `-` + 2 hex).
fn short_id() -> String {
    Uuid::new_v4().hyphenated().to_string()[..12].to_string()
}

/// moto's `_iso_now()`.
fn iso_now() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// moto's job-record timestamp format.
fn space_now() -> String {
    Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

/// moto's `get_partition`.
fn partition(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else if region == "us-iso-east-1" || region == "us-iso-west-1" {
        "aws-iso"
    } else if region == "us-isob-east-1" {
        "aws-iso-b"
    } else if region.starts_with("eusc-") {
        "aws-eusc"
    } else {
        "aws"
    }
}

// ---------------------------------------------------------------------------
// Records (moto's model classes)
// ---------------------------------------------------------------------------

/// moto's `{k: v for k, v in dct.items() if v}` — falsy members drop.
fn drop_falsy(v: Value) -> Value {
    match &v {
        Value::Object(map) => {
            let out: Map<String, Value> = map
                .iter()
                .filter(|(_, val)| match val {
                    Value::Null => false,
                    Value::String(s) => !s.is_empty(),
                    Value::Array(a) => !a.is_empty(),
                    Value::Object(o) => !o.is_empty(),
                    Value::Number(n) => n.as_f64() != Some(0.0),
                    Value::Bool(b) => *b,
                })
                .map(|(k, val)| (k.clone(), val.clone()))
                .collect();
            Value::Object(out)
        }
        _ => v,
    }
}

/// moto's `ModelCustomizationJob`.
struct ModelCustomizationJob {
    job_name: String,
    custom_model_name: String,
    role_arn: String,
    client_request_token: Option<String>,
    customization_type: Option<String>,
    job_arn: String,
    output_model_name: String,
    output_model_arn: String,
    status: String,
    failure_message: String,
    creation_time: String,
    last_modified_time: String,
    end_time: String,
    base_model_arn: String,
    output_model_kms_key_arn: String,
    training_metrics: Value,
    validation_metrics: Value,
    training_data_config: Value,
    validation_data_config: Option<Value>,
    output_data_config: Value,
    hyper_parameters: Value,
    vpc_config: Option<Value>,
}

impl ModelCustomizationJob {
    /// moto's `to_dict()` — falsy members drop off the wire.
    fn to_dict(&self) -> Value {
        drop_falsy(json!({
            "baseModelArn": self.base_model_arn,
            "clientRequestToken": self.client_request_token,
            "creationTime": self.creation_time,
            "customizationType": self.customization_type,
            "endTime": self.end_time,
            "failureMessage": self.failure_message,
            "hyperParameters": self.hyper_parameters,
            "jobArn": self.job_arn,
            "jobName": self.job_name,
            "lastModifiedTime": self.last_modified_time,
            "outputDataConfig": self.output_data_config,
            "outputModelArn": self.output_model_arn,
            "outputModelKmsKeyArn": self.output_model_kms_key_arn,
            "outputModelName": self.output_model_name,
            "roleArn": self.role_arn,
            "status": self.status,
            "trainingDataConfig": self.training_data_config,
            "trainingMetrics": self.training_metrics,
            "validationDataConfig": self.validation_data_config,
            "validationMetrics": self.validation_metrics,
            "vpcConfig": self.vpc_config,
        }))
    }

    /// moto's list summary — keeps nulls (plain `json.dumps`).
    fn summary(&self) -> Value {
        json!({
            "jobArn": self.job_arn,
            "baseModelArn": self.base_model_arn,
            "jobName": self.job_name,
            "status": self.status,
            "lastModifiedTime": self.last_modified_time,
            "creationTime": self.creation_time,
            "endTime": self.end_time,
            "customModelArn": self.output_model_arn,
            "customModelName": self.custom_model_name,
            "customizationType": self.customization_type,
        })
    }
}

/// moto's `CustomModel`.
#[derive(Default)]
struct CustomModel {
    model_name: String,
    job_name: String,
    job_arn: String,
    base_model_arn: String,
    base_model_name: String,
    hyper_parameters: Value,
    output_data_config: Value,
    training_data_config: Value,
    training_metrics: Value,
    customization_type: Option<String>,
    model_kms_key_arn: Option<String>,
    validation_data_config: Option<Value>,
    validation_metrics: Option<Value>,
    model_arn: String,
    creation_time: String,
}

impl CustomModel {
    /// moto's `to_dict()` — falsy members drop (`CreateCustomModel` models
    /// therefore expose only modelArn/modeName/creationTime).
    fn to_dict(&self) -> Value {
        drop_falsy(json!({
            "baseModelArn": self.base_model_arn.clone(),
            "creationTime": self.creation_time.clone(),
            "customizationType": self.customization_type.clone(),
            "hyperParameters": self.hyper_parameters.clone(),
            "jobArn": self.job_arn.clone(),
            "jobName": self.job_name.clone(),
            "modelArn": self.model_arn.clone(),
            "modelKmsKeyArn": self.model_kms_key_arn.clone(),
            "modelName": self.model_name.clone(),
            "outputDataConfig": self.output_data_config.clone(),
            "trainingDataConfig": self.training_data_config.clone(),
            "trainingMetrics": self.training_metrics.clone(),
            "validationDataConfig": self.validation_data_config.clone(),
            "validationMetrics": self.validation_metrics.clone(),
        }))
    }

    /// moto's list summary — keeps blanks (no falsy-dropping here).
    fn summary(&self) -> Value {
        json!({
            "modelArn": self.model_arn,
            "modelName": self.model_name,
            "creationTime": self.creation_time,
            "baseModelArn": self.base_model_arn,
            "baseModelName": self.base_model_name,
            "jobArn": self.job_arn,
            "customizationType": self.customization_type,
        })
    }
}

/// moto's `Guardrail` plus its versioned snapshots.
#[derive(Default)]
struct Guardrail {
    name: String,
    description: String,
    guardrail_id: String,
    guardrail_arn: String,
    status: String,
    topic_policy: Option<Value>,
    content_policy: Option<Value>,
    word_policy: Option<Value>,
    sensitive_information_policy: Option<Value>,
    contextual_grounding_policy: Option<Value>,
    automated_reasoning_policy: Option<Value>,
    cross_region_config: Option<Value>,
    blocked_input_messaging: String,
    blocked_outputs_messaging: String,
    kms_key_id: Option<String>,
    created_at: String,
    updated_at: String,
    next_version: u32,
    versions: BTreeMap<String, Value>,
}

impl Guardrail {
    fn new(account: u64, region: &str, name: &str, blocked_in: &str, blocked_out: &str) -> Self {
        let guardrail_id = short_id();
        Guardrail {
            name: name.to_string(),
            description: String::new(),
            guardrail_id: guardrail_id.clone(),
            guardrail_arn: format!(
                "arn:{}:bedrock:{region}:{account}:guardrail/{guardrail_id}",
                partition(region)
            ),
            status: "READY".to_string(),
            blocked_input_messaging: blocked_in.to_string(),
            blocked_outputs_messaging: blocked_out.to_string(),
            created_at: iso_now(),
            updated_at: iso_now(),
            next_version: 1,
            ..Default::default()
        }
    }

    /// moto's `to_dict()` — the DRAFT rendition.
    fn to_dict(&self) -> Value {
        let mut d = json!({
            "name": self.name,
            "description": self.description,
            "guardrailId": self.guardrail_id,
            "guardrailArn": self.guardrail_arn,
            "version": "DRAFT",
            "status": self.status,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "blockedInputMessaging": self.blocked_input_messaging,
            "blockedOutputsMessaging": self.blocked_outputs_messaging,
        });
        if let Some(cfg) = &self.topic_policy {
            d["topicPolicy"] = cfg.clone();
        }
        if let Some(cfg) = &self.content_policy {
            d["contentPolicy"] = cfg.clone();
        }
        if let Some(cfg) = &self.word_policy {
            d["wordPolicy"] = cfg.clone();
        }
        if let Some(cfg) = &self.sensitive_information_policy {
            d["sensitiveInformationPolicy"] = cfg.clone();
        }
        if let Some(cfg) = &self.contextual_grounding_policy {
            d["contextualGroundingPolicy"] = cfg.clone();
        }
        if let Some(cfg) = &self.automated_reasoning_policy {
            d["automatedReasoningPolicy"] = cfg.clone();
        }
        if let Some(cfg) = &self.cross_region_config {
            d["crossRegionDetails"] = cfg.clone();
        }
        if let Some(kms) = &self.kms_key_id {
            d["kmsKeyArn"] = json!(kms);
        }
        d
    }

    fn summary(&self) -> Value {
        json!({
            "id": self.guardrail_id,
            "arn": self.guardrail_arn,
            "status": self.status,
            "name": self.name,
            "description": self.description,
            "version": "DRAFT",
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
        })
    }
}

/// moto's `ProvisionedModelThroughput`.
#[derive(Default)]
struct ProvisionedThroughput {
    provisioned_model_name: String,
    provisioned_model_id: String,
    provisioned_model_arn: String,
    model_arn: String,
    desired_model_arn: String,
    foundation_model_arn: String,
    model_units: i64,
    desired_model_units: i64,
    status: String,
    creation_time: String,
    last_modified_time: String,
    commitment_duration: Option<String>,
}

impl ProvisionedThroughput {
    fn new(
        account: u64,
        region: &str,
        name: &str,
        model_id: &str,
        units: i64,
        commitment: Option<String>,
    ) -> Self {
        let id = short_id();
        let model_arn = format!(
            "arn:{}:bedrock:{region}::foundation-model/{model_id}",
            partition(region)
        );
        ProvisionedThroughput {
            provisioned_model_name: name.to_string(),
            provisioned_model_id: id.clone(),
            provisioned_model_arn: format!(
                "arn:{}:bedrock:{region}:{account}:provisioned-model/{id}",
                partition(region)
            ),
            desired_model_arn: model_arn.clone(),
            foundation_model_arn: model_arn.clone(),
            model_arn,
            model_units: units,
            desired_model_units: units,
            status: "InService".to_string(),
            creation_time: iso_now(),
            last_modified_time: iso_now(),
            commitment_duration: commitment,
        }
    }

    fn to_dict(&self) -> Value {
        let mut d = json!({
            "modelUnits": self.model_units,
            "desiredModelUnits": self.desired_model_units,
            "provisionedModelName": self.provisioned_model_name,
            "provisionedModelArn": self.provisioned_model_arn,
            "modelArn": self.model_arn,
            "desiredModelArn": self.desired_model_arn,
            "foundationModelArn": self.foundation_model_arn,
            "status": self.status,
            "creationTime": self.creation_time,
            "lastModifiedTime": self.last_modified_time,
        });
        if let Some(cd) = &self.commitment_duration {
            d["commitmentDuration"] = json!(cd);
        }
        d
    }

    fn summary(&self) -> Value {
        self.to_dict()
    }
}

/// moto's `EvaluationJob`.
#[derive(Default)]
struct EvaluationJob {
    job_name: String,
    job_id: String,
    job_arn: String,
    role_arn: String,
    evaluation_config: Value,
    inference_config: Value,
    output_data_config: Value,
    job_description: String,
    customer_encryption_key_id: Option<String>,
    application_type: String,
    job_type: String,
    status: String,
    creation_time: String,
    last_modified_time: String,
}

impl EvaluationJob {
    /// moto stores the job by ARN; evaluationConfig decides `job_type`.
    fn new(
        region: &str,
        account: u64,
        name: &str,
        role_arn: &str,
        evaluation_config: &Value,
    ) -> Self {
        let job_id = short_id();
        let job_type = if evaluation_config.get("automated").is_some() {
            "Automated"
        } else {
            "Human"
        };
        let job_arn = format!(
            "arn:{}:bedrock:{region}:{account}:evaluation-job/{job_id}",
            partition(region)
        );
        EvaluationJob {
            job_name: name.to_string(),
            job_id,
            job_arn,
            role_arn: role_arn.to_string(),
            evaluation_config: evaluation_config.clone(),
            inference_config: json!({}),
            output_data_config: json!({}),
            job_description: String::new(),
            customer_encryption_key_id: None,
            application_type: "ModelEvaluation".to_string(),
            job_type: job_type.to_string(),
            status: "InProgress".to_string(),
            creation_time: iso_now(),
            last_modified_time: iso_now(),
        }
    }

    fn to_dict(&self) -> Value {
        json!({
            "jobName": self.job_name,
            "status": self.status,
            "jobArn": self.job_arn,
            "roleArn": self.role_arn,
            "jobType": self.job_type,
            "evaluationConfig": self.evaluation_config,
            "inferenceConfig": self.inference_config,
            "outputDataConfig": self.output_data_config,
            "creationTime": self.creation_time,
            "lastModifiedTime": self.last_modified_time,
            "jobDescription": self.job_description.clone(),
            "applicationType": self.application_type.clone(),
        })
    }

    fn summary(&self) -> Value {
        json!({
            "jobArn": self.job_arn,
            "jobName": self.job_name,
            "status": self.status,
            "creationTime": self.creation_time,
            "jobType": self.job_type,
            "applicationType": self.application_type,
        })
    }
}

/// moto's `InferenceProfile`.
#[derive(Default)]
struct InferenceProfile {
    inference_profile_name: String,
    description: String,
    inference_profile_id: String,
    inference_profile_arn: String,
    status: String,
    profile_type: String,
    created_at: String,
    updated_at: String,
    models: Vec<Value>,
}

impl InferenceProfile {
    fn new(
        account: u64,
        region: &str,
        name: &str,
        model_source: &Value,
        description: Option<&str>,
    ) -> Self {
        let id = short_id();
        let mut models = Vec::new();
        match model_source.get("copyFrom") {
            Some(Value::String(arn)) => models.push(json!({"modelArn": arn})),
            Some(Value::Array(items)) => {
                for m in items {
                    models.push(json!({"modelArn": m}));
                }
            }
            _ => {}
        }
        InferenceProfile {
            inference_profile_name: name.to_string(),
            description: description.unwrap_or("").to_string(),
            inference_profile_id: id.clone(),
            inference_profile_arn: format!(
                "arn:{}:bedrock:{region}:{account}:inference-profile/{id}",
                partition(region)
            ),
            status: "ACTIVE".to_string(),
            profile_type: "APPLICATION".to_string(),
            created_at: iso_now(),
            updated_at: iso_now(),
            models,
        }
    }

    fn to_dict(&self) -> Value {
        json!({
            "inferenceProfileName": self.inference_profile_name,
            "description": self.description,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "inferenceProfileArn": self.inference_profile_arn,
            "models": self.models,
            "inferenceProfileId": self.inference_profile_id,
            "status": self.status,
            "type": self.profile_type,
        })
    }

    fn summary(&self) -> Value {
        self.to_dict()
    }
}

/// moto's `ModelImportJob`.
#[derive(Default)]
struct ModelImportJob {
    job_name: String,
    job_id: String,
    job_arn: String,
    imported_model_name: String,
    imported_model_arn: String,
    role_arn: String,
    model_data_source: Value,
    vpc_config: Option<Value>,
    imported_model_kms_key_arn: String,
    status: String,
    creation_time: String,
    last_modified_time: String,
    end_time: String,
}

impl ModelImportJob {
    fn to_dict(&self) -> Value {
        drop_falsy(json!({
            "jobArn": self.job_arn,
            "jobName": self.job_name,
            "importedModelName": self.imported_model_name,
            "importedModelArn": self.imported_model_arn,
            "roleArn": self.role_arn,
            "modelDataSource": self.model_data_source,
            "status": self.status,
            "creationTime": self.creation_time,
            "lastModifiedTime": self.last_modified_time,
            "endTime": self.end_time,
            "vpcConfig": self.vpc_config,
            "importedModelKmsKeyArn": self.imported_model_kms_key_arn,
        }))
    }

    fn summary(&self) -> Value {
        json!({
            "jobArn": self.job_arn,
            "jobName": self.job_name,
            "status": self.status,
            "importedModelName": self.imported_model_name,
            "importedModelArn": self.imported_model_arn,
            "creationTime": self.creation_time,
            "lastModifiedTime": self.last_modified_time,
            "endTime": self.end_time,
        })
    }
}

/// moto's `ImportedModel`.
#[derive(Default)]
struct ImportedModel {
    model_name: String,
    model_arn: String,
    job_name: String,
    job_arn: String,
    model_data_source: Value,
    model_kms_key_arn: Option<String>,
    creation_time: String,
}

impl ImportedModel {
    fn to_dict(&self) -> Value {
        let mut d = json!({
            "modelArn": self.model_arn,
            "modelName": self.model_name,
            "jobName": self.job_name,
            "jobArn": self.job_arn,
            "modelDataSource": self.model_data_source,
            "creationTime": self.creation_time,
        });
        if let Some(kms) = &self.model_kms_key_arn {
            if !kms.is_empty() {
                d["modelKmsKeyArn"] = json!(kms);
            }
        }
        // moto leaves modelArchitecture empty (dropped) and customModelUnits = 1
        d["customModelUnits"] = json!(1);
        d
    }

    fn summary(&self) -> Value {
        json!({
            "modelArn": self.model_arn,
            "modelName": self.model_name,
            "creationTime": self.creation_time,
        })
    }
}

/// moto's `ModelCopyJob`.
#[derive(Default)]
struct ModelCopyJob {
    job_arn: String,
    source_model_arn: String,
    source_model_name: String,
    source_account_id: String,
    target_model_name: String,
    target_model_arn: String,
    target_model_kms_key_arn: String,
    target_model_tags: Option<Value>,
    status: String,
    creation_time: String,
}

impl ModelCopyJob {
    fn to_dict(&self) -> Value {
        let mut d = json!({
            "jobArn": self.job_arn,
            "status": self.status,
            "creationTime": self.creation_time,
            "targetModelArn": self.target_model_arn,
            "targetModelName": self.target_model_name,
            "sourceAccountId": self.source_account_id,
            "sourceModelArn": self.source_model_arn,
            "sourceModelName": self.source_model_name,
        });
        if !self.target_model_kms_key_arn.is_empty() {
            d["targetModelKmsKeyArn"] = json!(self.target_model_kms_key_arn);
        }
        if let Some(tags) = &self.target_model_tags {
            d["targetModelTags"] = tags.clone();
        }
        d
    }

    fn summary(&self) -> Value {
        json!({
            "jobArn": self.job_arn,
            "status": self.status,
            "creationTime": self.creation_time,
            "targetModelArn": self.target_model_arn,
            "sourceAccountId": self.source_account_id,
            "sourceModelArn": self.source_model_arn,
        })
    }
}

/// moto's `ModelInvocationJob` (batch inference).
#[derive(Default)]
struct ModelInvocationJob {
    job_name: String,
    job_id: String,
    job_arn: String,
    role_arn: String,
    model_id: String,
    input_data_config: Value,
    output_data_config: Value,
    client_request_token: Option<String>,
    vpc_config: Option<Value>,
    timeout_duration_in_hours: i64,
    model_invocation_type: Option<String>,
    status: String,
    submit_time: String,
    last_modified_time: String,
    end_time: String,
}

impl ModelInvocationJob {
    fn to_dict(&self) -> Value {
        drop_falsy(json!({
            "jobArn": self.job_arn,
            "jobName": self.job_name,
            "modelId": self.model_id,
            "roleArn": self.role_arn,
            "status": self.status,
            "submitTime": self.submit_time,
            "lastModifiedTime": self.last_modified_time,
            "inputDataConfig": self.input_data_config,
            "outputDataConfig": self.output_data_config,
            "timeoutDurationInHours": self.timeout_duration_in_hours,
            "clientRequestToken": self.client_request_token,
            "endTime": self.end_time,
            "vpcConfig": self.vpc_config,
            "modelInvocationType": self.model_invocation_type,
        }))
    }

    fn summary(&self) -> Value {
        let mut d = json!({
            "jobArn": self.job_arn,
            "jobName": self.job_name,
            "modelId": self.model_id,
            "status": self.status,
            "submitTime": self.submit_time,
            "lastModifiedTime": self.last_modified_time,
            "inputDataConfig": self.input_data_config,
            "outputDataConfig": self.output_data_config,
            "timeoutDurationInHours": self.timeout_duration_in_hours,
        });
        if !self.end_time.is_empty() {
            d["endTime"] = json!(self.end_time);
        }
        d
    }
}

/// moto's `FoundationModelInfo` — static catalog entry.
struct FoundationModel {
    model_id: String,
    model_name: String,
    provider: String,
    model_arn: String,
}

impl FoundationModel {
    fn to_dict(&self) -> Value {
        json!({
            "modelArn": self.model_arn,
            "modelId": self.model_id,
            "modelName": self.model_name,
            "providerName": self.provider,
            "inputModalities": ["TEXT"],
            "outputModalities": ["TEXT"],
            "responseStreamingSupported": true,
            "customizationsSupported": [],
            "inferenceTypesSupported": ["ON_DEMAND"],
            "modelLifecycle": {"status": "ACTIVE"},
        })
    }
}

/// moto's `MarketplaceModelEndpoint`.
#[derive(Default)]
struct MarketplaceModelEndpoint {
    model_source_identifier: String,
    endpoint_config: Value,
    endpoint_arn: String,
    status: String,
    created_at: String,
    updated_at: String,
    #[allow(dead_code)]
    registered: bool,
}

impl MarketplaceModelEndpoint {
    fn to_dict(&self) -> Value {
        json!({
            "endpointArn": self.endpoint_arn,
            "modelSourceIdentifier": self.model_source_identifier,
            "endpointConfig": self.endpoint_config,
            "endpointStatus": self.status,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
        })
    }
}

/// moto's `PromptRouter`.
#[derive(Default)]
struct PromptRouter {
    prompt_router_name: String,
    prompt_router_arn: String,
    models: Vec<Value>,
    fallback_model: Value,
    routing_criteria: Value,
    description: String,
    status: String,
    router_type: String,
    created_at: String,
    updated_at: String,
}

impl PromptRouter {
    fn to_dict(&self) -> Value {
        json!({
            "promptRouterName": self.prompt_router_name,
            "routingCriteria": self.routing_criteria,
            "description": self.description,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "promptRouterArn": self.prompt_router_arn,
            "models": self.models,
            "fallbackModel": self.fallback_model,
            "status": self.status,
            "type": self.router_type,
        })
    }

    fn summary(&self) -> Value {
        self.to_dict()
    }
}

/// moto's `CustomModelDeployment`.
#[derive(Default)]
struct CustomModelDeployment {
    model_deployment_name: String,
    model_id: String,
    deployment_id: String,
    deployment_arn: String,
    status: String,
    created_at: String,
    last_modified_time: String,
}

impl CustomModelDeployment {
    fn to_dict(&self) -> Value {
        json!({
            "customModelDeploymentIdentifier": self.deployment_id,
            "customModelDeploymentArn": self.deployment_arn,
            "modelDeploymentName": self.model_deployment_name,
            "modelId": self.model_id,
            "status": self.status,
            "createdAt": self.created_at,
            "lastModifiedTime": self.last_modified_time,
        })
    }
}

/// moto's `AutomatedReasoningPolicy`.
#[derive(Default)]
struct AutomatedReasoningPolicy {
    name: String,
    policy_type: String,
    description: String,
    rules: Value,
    policy_id: String,
    policy_arn: String,
    status: String,
    version: i64,
    created_at: String,
    updated_at: String,
    build_workflows: BTreeMap<String, Value>,
    test_cases: BTreeMap<String, Value>,
}

impl AutomatedReasoningPolicy {
    fn to_dict(&self) -> Value {
        json!({
            "policyArn": self.policy_arn,
            "name": self.name,
            "description": self.description,
            "policyType": self.policy_type,
            "status": self.status,
            "version": self.version,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "rules": self.rules,
        })
    }

    fn summary(&self) -> Value {
        json!({
            "policyArn": self.policy_arn,
            "name": self.name,
            "status": self.status,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
        })
    }
}

// ---------------------------------------------------------------------------
// Foundation model catalog (moto's _FOUNDATION_MODELS)
// ---------------------------------------------------------------------------

fn foundation_models(region: &str) -> Vec<FoundationModel> {
    const CATALOG: &[(&str, &str, &str)] = &[
        (
            "amazon.titan-text-express-v1",
            "Titan Text Express",
            "Amazon",
        ),
        ("amazon.titan-text-lite-v1", "Titan Text Lite", "Amazon"),
        (
            "amazon.titan-embed-text-v1",
            "Titan Embeddings G1 - Text",
            "Amazon",
        ),
        (
            "amazon.titan-embed-text-v2:0",
            "Titan Text Embeddings V2",
            "Amazon",
        ),
        (
            "amazon.titan-embed-image-v1",
            "Titan Multimodal Embeddings G1",
            "Amazon",
        ),
        (
            "amazon.titan-image-generator-v1",
            "Titan Image Generator G1",
            "Amazon",
        ),
        ("anthropic.claude-v2", "Claude", "Anthropic"),
        ("anthropic.claude-v2:1", "Claude", "Anthropic"),
        (
            "anthropic.claude-3-sonnet-20240229-v1:0",
            "Claude 3 Sonnet",
            "Anthropic",
        ),
        (
            "anthropic.claude-3-haiku-20240307-v1:0",
            "Claude 3 Haiku",
            "Anthropic",
        ),
        (
            "anthropic.claude-3-opus-20240229-v1:0",
            "Claude 3 Opus",
            "Anthropic",
        ),
        ("anthropic.claude-instant-v1", "Claude Instant", "Anthropic"),
        ("ai21.j2-mid-v1", "Jurassic-2 Mid", "AI21 Labs"),
        ("ai21.j2-ultra-v1", "Jurassic-2 Ultra", "AI21 Labs"),
        ("cohere.command-text-v14", "Command", "Cohere"),
        ("cohere.command-light-text-v14", "Command Light", "Cohere"),
        ("cohere.embed-english-v3", "Embed English", "Cohere"),
        (
            "cohere.embed-multilingual-v3",
            "Embed Multilingual",
            "Cohere",
        ),
        ("meta.llama2-13b-chat-v1", "Llama 2 Chat 13B", "Meta"),
        ("meta.llama2-70b-chat-v1", "Llama 2 Chat 70B", "Meta"),
        (
            "meta.llama3-8b-instruct-v1:0",
            "Llama 3 8B Instruct",
            "Meta",
        ),
        (
            "meta.llama3-70b-instruct-v1:0",
            "Llama 3 70B Instruct",
            "Meta",
        ),
        (
            "mistral.mistral-7b-instruct-v0:2",
            "Mistral 7B Instruct",
            "Mistral AI",
        ),
        (
            "mistral.mixtral-8x7b-instruct-v0:1",
            "Mixtral 8x7B Instruct",
            "Mistral AI",
        ),
        (
            "stability.stable-diffusion-xl-v1",
            "Stable Diffusion XL",
            "Stability AI",
        ),
    ];
    CATALOG
        .iter()
        .map(|(id, name, provider)| FoundationModel {
            model_id: id.to_string(),
            model_name: name.to_string(),
            provider: provider.to_string(),
            model_arn: format!(
                "arn:{}:bedrock:{region}::foundation-model/{id}",
                partition(region)
            ),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "bedrock" {
        return None;
    }
    let Some(op) = wire_operation(req) else {
        return Some(json_error(
            501,
            "NotImplemented",
            &format!("The {} action has not been implemented", req.operation),
        ));
    };
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    let params = collect_params(req);
    Some(route(req, op, &params, st))
}

fn wire_operation(req: &Request) -> Option<&'static str> {
    let path = req.path.trim_start_matches('/').trim_end_matches('/');
    let segs: Vec<&str> = if path.is_empty() {
        Vec::new()
    } else {
        path.split('/').collect()
    };
    RULES
        .iter()
        .find(|(method, uri, _)| *method == req.method && template_matches(uri, &segs))
        .map(|(_, _, op)| *op)
}

fn template_matches(uri: &str, segs: &[&str]) -> bool {
    let tpl: Vec<&str> = uri
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    tpl.len() == segs.len()
        && tpl
            .iter()
            .zip(segs)
            .all(|(t, s)| (t.starts_with('{') && t.ends_with('}')) || *t == *s)
}

/// Denormalized parameters: query-string members, then the JSON body —
/// POST/PUT/PATCH bodies carry the operation params while GET/DELETE only have
/// the query members; a shared key prefers the body value (moto reads either).
fn collect_params(req: &Request) -> Value {
    let body = parse_json_body(&req.body);
    let mut out = Map::new();
    for (k, vals) in query_map(&req.query_string) {
        if vals.len() == 1 {
            out.insert(k, json!(vals[0]));
        } else {
            out.insert(k, json!(vals));
        }
    }
    if let Some(obj) = body.as_object() {
        for (k, v) in obj {
            out.insert(k.clone(), v.clone());
        }
    }
    Value::Object(out)
}

/// Path segment `from_end` positions from the tail, URL-decoded
/// (moto's `unquote(self.path.split("/")[-N])`).
fn path_arg(path: &str, from_end: usize) -> String {
    url_decode(path.split('/').nth_back(from_end).unwrap_or(""))
}

/// Same position, kept raw — moto's `self.path.split("/")[-N]`.
fn path_raw(path: &str, from_end: usize) -> String {
    path.split('/').nth_back(from_end).unwrap_or("").to_string()
}

// ---------------------------------------------------------------------------
// wire helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

fn ok_empty() -> Response {
    ok(json!({}))
}

fn not_found(message: &str) -> Response {
    json_error(400, "ResourceNotFoundException", message)
}

fn in_use(message: &str) -> Response {
    json_error(400, "ResourceInUseException", message)
}

fn validation(_message: &str) -> Response {
    // moto's Bedrock ValidationException message constant (the detail is a
    // misused template arg and never reaches the wire).
    json_error(
        400,
        "ValidationException",
        "Input validation failed. Check your request parameters and retry the request.",
    )
}

fn as_str<'a>(v: &'a Value, k: &str) -> &'a str {
    v.get(k).and_then(Value::as_str).unwrap_or("")
}

// ---------------------------------------------------------------------------
// tags (moto's TaggingService)
// ---------------------------------------------------------------------------

fn tag_resource(st: &mut BedrockStore, arn: &str, tags: Option<&Vec<Value>>) {
    let Some(tags) = tags else {
        return;
    };
    let row = st.tags.entry(arn.to_string()).or_default();
    for tag in tags {
        let Some(key) = tag.get("key").and_then(Value::as_str) else {
            continue;
        };
        row.insert(
            key.to_string(),
            tag.get("value").and_then(Value::as_str).map(String::from),
        );
    }
}

fn untag_resource(st: &mut BedrockStore, arn: &str, keys: Option<&Vec<Value>>) {
    let Some(keys) = keys else {
        return;
    };
    if let Some(row) = st.tags.get_mut(arn) {
        for k in keys {
            if let Some(key) = k.as_str() {
                row.remove(key);
            }
        }
    }
}

fn list_tags(st: &BedrockStore, arn: &str) -> Vec<Value> {
    st.tags
        .get(arn)
        .map(|row| {
            row.iter()
                .map(|(k, v)| json!({"key": k, "value": v.as_deref()}))
                .collect()
        })
        .unwrap_or_default()
}

/// moto's `_list_arns()` — every resource ARN in this store.
fn all_arns(st: &BedrockStore) -> Vec<String> {
    let mut arns = Vec::new();
    arns.extend(
        st.model_customization_jobs
            .values()
            .map(|j| j.job_arn.clone()),
    );
    arns.extend(st.custom_models.values().map(|m| m.model_arn.clone()));
    arns.extend(st.guardrails.values().map(|g| g.guardrail_arn.clone()));
    arns.extend(
        st.provisioned_model_throughputs
            .values()
            .map(|p| p.provisioned_model_arn.clone()),
    );
    arns.extend(st.evaluation_jobs.values().map(|j| j.job_arn.clone()));
    arns.extend(
        st.inference_profiles
            .values()
            .map(|i| i.inference_profile_arn.clone()),
    );
    arns.extend(st.model_import_jobs.values().map(|j| j.job_arn.clone()));
    arns.extend(st.imported_models.values().map(|m| m.model_arn.clone()));
    arns.extend(st.model_copy_jobs.values().map(|j| j.job_arn.clone()));
    arns.extend(st.model_invocation_jobs.values().map(|j| j.job_arn.clone()));
    arns.extend(
        st.marketplace_endpoints
            .values()
            .map(|e| e.endpoint_arn.clone()),
    );
    arns.extend(
        st.prompt_routers
            .values()
            .map(|r| r.prompt_router_arn.clone()),
    );
    arns.extend(
        st.custom_model_deployments
            .values()
            .map(|d| d.deployment_arn.clone()),
    );
    arns.extend(
        st.automated_reasoning_policies
            .values()
            .map(|p| p.policy_arn.clone()),
    );
    arns
}

// ---------------------------------------------------------------------------
// pagination (moto's @paginate: token = base64 JSON of the next page's
// first item's unique attribute; round-trip opaque within this server)
// ---------------------------------------------------------------------------

fn make_token(unique: &str) -> Value {
    let payload = serde_json::to_vec(&json!({"uniqueAttributes": unique})).unwrap_or_default();
    json!(B64.encode(payload))
}

/// moto's token payload: base64 JSON `{"uniqueAttributes": "<attr>"}`.
fn decode_token(token: Option<&str>) -> Option<String> {
    let bytes = B64.decode(token?).ok()?;
    let v: Value = serde_json::from_slice(&bytes).ok()?;
    v.get("uniqueAttributes")?.as_str().map(String::from)
}

/// Split `results` per moto's `Paginator.paginate`.
fn paginate(
    results: Vec<Value>,
    max_results: Option<usize>,
    next_token: Option<&Value>,
    unique_of: impl Fn(&Value) -> String,
) -> (Vec<Value>, Value) {
    let offset = next_token
        .and_then(Value::as_str)
        .and_then(|t| decode_token(Some(t)))
        .and_then(|unique| results.iter().position(|r| unique_of(r) == unique))
        .unwrap_or(0);
    let limit = max_results.unwrap_or(100);
    let end = (offset + limit).min(results.len());
    let page = results[offset.min(end)..end].to_vec();
    if !page.is_empty() && end < results.len() {
        (page, make_token(&unique_of(&results[end])))
    } else {
        (page, Value::Null)
    }
}

fn int_query(params: &Value, key: &str) -> Option<usize> {
    match params.get(key) {
        Some(Value::Number(n)) => n.as_u64().map(|v| v as usize),
        Some(Value::String(s)) => s.parse().ok(),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// handlers — model customization jobs
// ---------------------------------------------------------------------------

fn create_customization_job(
    st: &mut BedrockStore,
    account: u64,
    region: &str,
    params: &Value,
) -> Response {
    let job_name = as_str(params, "jobName");
    let custom_model_name = as_str(params, "customModelName");
    if st.model_customization_jobs.contains_key(job_name) {
        return in_use(&format!(
            "Model customization job {job_name} already exists"
        ));
    }
    if st.custom_models.contains_key(custom_model_name) {
        return in_use(&format!("Custom model {custom_model_name} already exists"));
    }
    let training_data_config = params
        .get("trainingDataConfig")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let output_data_config = params
        .get("outputDataConfig")
        .cloned()
        .unwrap_or_else(|| json!({}));
    // moto's constructor validation, in moto's order.
    for cfg in [&training_data_config, &output_data_config] {
        let valid = cfg
            .get("s3Uri")
            .and_then(Value::as_str)
            .map(|u| u.starts_with("s3://"))
            .unwrap_or(false);
        if !valid {
            return validation(&format!(
                "Validation error detected: Value '{cfg}' failed to satisfy constraint: Member must satisfy regular expression pattern: s3://.*"
            ));
        }
    }
    if let Some(validators) = params
        .get("validationDataConfig")
        .and_then(|v| v.get("validators"))
        .and_then(Value::as_array)
    {
        let ok_all = validators.iter().all(|v| {
            v.get("s3Uri")
                .and_then(Value::as_str)
                .map(|u| u.starts_with("s3://"))
                .unwrap_or(false)
        });
        if !ok_all {
            return validation("Validation error detected");
        }
    }
    let base_model_identifier = as_str(params, "baseModelIdentifier");
    let job = ModelCustomizationJob {
        job_name: job_name.to_string(),
        custom_model_name: custom_model_name.to_string(),
        role_arn: as_str(params, "roleArn").to_string(),
        client_request_token: params
            .get("clientRequestToken")
            .and_then(Value::as_str)
            .map(String::from),
        customization_type: params
            .get("customizationType")
            .and_then(Value::as_str)
            .map(String::from),
        job_arn: format!(
            "arn:{}:bedrock:{region}:{account}:model-customization-job/{job_name}",
            partition(region)
        ),
        output_model_name: format!("{custom_model_name}-{job_name}"),
        output_model_arn: format!(
            "arn:{}:bedrock:{region}:{account}:custom-model/{custom_model_name}-{job_name}",
            partition(region)
        ),
        status: "InProgress".to_string(),
        failure_message: "Failure Message".to_string(),
        creation_time: space_now(),
        last_modified_time: space_now(),
        end_time: space_now(),
        base_model_arn: format!(
            "arn:{}:bedrock:{region}::foundation-model/{base_model_identifier}",
            partition(region)
        ),
        output_model_kms_key_arn: format!(
            "arn:{}:kms:{region}:{account}:key/{custom_model_name}-{job_name}-kms-key",
            partition(region)
        ),
        training_metrics: json!({"trainingLoss": 0.0}),
        validation_metrics: json!([{"validationLoss": 0.0}]),
        training_data_config,
        validation_data_config: params.get("validationDataConfig").cloned(),
        output_data_config,
        hyper_parameters: params
            .get("hyperParameters")
            .cloned()
            .unwrap_or(Value::Null),
        vpc_config: params.get("vpcConfig").cloned(),
    };
    let job_arn = job.job_arn.clone();
    let base_model_arn = job.base_model_arn.clone();
    let hyper_parameters = job.hyper_parameters.clone();
    let out_model_kms = job.output_model_kms_key_arn.clone();
    st.model_customization_jobs
        .insert(job_name.to_string(), job);
    if let Some(tags) = params.get("jobTags").and_then(Value::as_array) {
        if !tags.is_empty() {
            let snapshot: Vec<Value> = tags.clone();
            let arn = job_arn.clone();
            let row = st.tags.entry(arn).or_default();
            for tag in snapshot {
                let Some(key) = tag.get("key").and_then(Value::as_str) else {
                    continue;
                };
                row.insert(
                    key.to_string(),
                    tag.get("value").and_then(Value::as_str).map(String::from),
                );
            }
        }
    }
    // moto derives the custom model straight off the job.
    let custom_model = CustomModel {
        model_name: custom_model_name.to_string(),
        job_name: job_name.to_string(),
        job_arn: job_arn.clone(),
        base_model_arn: base_model_arn.clone(),
        base_model_name: as_str(params, "baseModelIdentifier").to_string(),
        hyper_parameters,
        output_data_config: params
            .get("outputDataConfig")
            .cloned()
            .unwrap_or(Value::Null),
        training_data_config: params
            .get("trainingDataConfig")
            .cloned()
            .unwrap_or(Value::Null),
        training_metrics: json!({"trainingLoss": 0.0}),
        customization_type: params
            .get("customizationType")
            .and_then(Value::as_str)
            .map(String::from),
        model_kms_key_arn: Some(out_model_kms.clone()),
        validation_data_config: params.get("validationDataConfig").cloned(),
        validation_metrics: Some(json!([{"validationLoss": 0.0}])),
        model_arn: format!(
            "arn:{}:bedrock:{region}:{account}:custom-model/{custom_model_name}",
            partition(region)
        ),
        creation_time: space_now(),
    };
    let custom_model_arn = custom_model.model_arn.clone();
    st.custom_models
        .insert(custom_model_name.to_string(), custom_model);
    if let Some(tags) = params.get("customModelTags").and_then(Value::as_array) {
        if !tags.is_empty() {
            tag_resource(st, &custom_model_arn, Some(tags));
        }
    }
    ok(json!({"jobArn": job_arn}))
}

/// Resolves a customization job by name OR ARN — stock moto keys by name
/// only; the compat suite's by-ARN assertions require the ARN fallback.
fn find_customization_job<'a>(
    st: &'a BedrockStore,
    job_identifier: &str,
) -> Option<&'a ModelCustomizationJob> {
    if let Some(job) = st.model_customization_jobs.get(job_identifier) {
        return Some(job);
    }
    st.model_customization_jobs
        .values()
        .find(|j| j.job_arn == job_identifier)
}

fn get_customization_job(st: &BedrockStore, job_identifier: &str) -> Response {
    match find_customization_job(st, job_identifier) {
        Some(job) => ok(job.to_dict()),
        None => not_found(&format!(
            "Model customization job {job_identifier} not found"
        )),
    }
}

fn stop_customization_job(st: &mut BedrockStore, job_identifier: &str) -> Response {
    if let Some(job) = find_customization_job(st, job_identifier) {
        let borrow = st
            .model_customization_jobs
            .get_mut(&job.job_name.clone())
            .unwrap();
        borrow.status = "Stopped".to_string();
        return ok_empty();
    }
    not_found(&format!(
        "Model customization job {job_identifier} not found"
    ))
}

/// moto's list filter + sort, per `list_model_customization_jobs`. Time
/// filters compare iso strings, as moto does; only `CreationTime` sorts.
fn list_customization_jobs(st: &BedrockStore, params: &Value) -> Response {
    let mut found: Vec<&ModelCustomizationJob> = st.model_customization_jobs.values().collect();
    if let Some(contains) = str_opt(params.get("nameContains")) {
        found.retain(|j| j.job_name.contains(contains));
    }
    if let Some(status) = str_opt(params.get("statusEquals")) {
        found.retain(|j| j.status == status);
    }
    if let Some(after) = str_opt(params.get("creationTimeAfter")) {
        found.retain(|j| j.creation_time.as_str() > after);
    }
    if let Some(before) = str_opt(params.get("creationTimeBefore")) {
        found.retain(|j| j.creation_time.as_str() < before);
    }
    if let Some(sort_by) = str_opt(params.get("sortBy")) {
        if sort_by != "CreationTime" {
            return validation(&format!("Invalid sort by field: {sort_by}"));
        }
        match str_opt(params.get("sortOrder")) {
            Some("Ascending") => found.sort_by_key(|j| j.creation_time.clone()),
            Some("Descending") => {
                found.sort_by_key(|j| j.creation_time.clone());
                found.reverse();
            }
            _ => {
                return validation(&format!(
                    "Invalid sort order: {:?}",
                    params.get("sortOrder")
                ))
            }
        }
    }
    let (page, next) = paginate(
        found.into_iter().map(|j| j.summary()).collect(),
        int_query(params, "maxResults"),
        params.get("nextToken"),
        |s| {
            s.get("jobArn")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        },
    );
    ok(json!({
        "nextToken": next,
        "modelCustomizationJobSummaries": page,
    }))
}

/// Non-blank string member — moto's truthy `params.get(x)` read.
fn str_opt(v: Option<&Value>) -> Option<&str> {
    v.and_then(Value::as_str).filter(|s| !s.is_empty())
}

// ---------------------------------------------------------------------------
// handlers — logging configuration
// ---------------------------------------------------------------------------

// (in route() inline)

// ---------------------------------------------------------------------------
// handlers — custom models
// ---------------------------------------------------------------------------

fn create_custom_model(
    st: &mut BedrockStore,
    account: u64,
    region: &str,
    params: &Value,
) -> Response {
    let model_name = as_str(params, "modelName");
    if st.custom_models.contains_key(model_name) {
        return in_use(&format!("Custom model {model_name} already exists"));
    }
    let model_arn = format!(
        "arn:{}:bedrock:{region}:{account}:custom-model/{model_name}",
        partition(region)
    );
    st.custom_models.insert(
        model_name.to_string(),
        CustomModel {
            model_name: model_name.to_string(),
            model_arn,
            creation_time: space_now(),
            model_kms_key_arn: params
                .get("modelKmsKeyId")
                .and_then(Value::as_str)
                .map(String::from),
            ..Default::default()
        },
    );
    ok(
        json!({"modelArn": format!("arn:{}:bedrock:{region}:{account}:custom-model/{model_name}", partition(region))}),
    )
}

fn get_custom_model(st: &BedrockStore, model_identifier: &str) -> Response {
    // moto: r ARNs scan by ARN; names hit the key.
    if model_identifier.starts_with("arn") {
        for m in st.custom_models.values() {
            if m.model_arn == model_identifier {
                return ok(m.to_dict());
            }
        }
    } else if let Some(m) = st.custom_models.get(model_identifier) {
        return ok(m.to_dict());
    }
    not_found(&format!("Custom model {model_identifier} not found"))
}

fn delete_custom_model(st: &mut BedrockStore, model_identifier: &str) -> Response {
    if st.custom_models.remove(model_identifier).is_some() {
        return ok_empty();
    }
    not_found(&format!("Custom model {model_identifier} not found"))
}

fn list_custom_models(st: &BedrockStore, params: &Value) -> Response {
    let mut items: Vec<&CustomModel> = st.custom_models.values().collect();
    if let Some(contains) = str_opt(params.get("nameContains")) {
        // moto quirk: filters against job_name, not model_name.
        items.retain(|m| m.job_name.contains(contains));
    }
    if let Some(base) = str_opt(params.get("baseModelArnEquals")) {
        items.retain(|m| m.base_model_arn == base);
    }
    // foundationModelArnEquals: moto documents it as not-yet-supported.
    if let Some(after) = str_opt(params.get("creationTimeAfter")) {
        items.retain(|m| m.creation_time.as_str() > after);
    }
    if let Some(before) = str_opt(params.get("creationTimeBefore")) {
        items.retain(|m| m.creation_time.as_str() < before);
    }
    if let Some(sort_by) = str_opt(params.get("sortBy")) {
        if sort_by != "CreationTime" {
            return validation(&format!("Invalid sort by field: {sort_by}"));
        }
        match str_opt(params.get("sortOrder")) {
            Some("Ascending") => items.sort_by_key(|m| m.creation_time.clone()),
            Some("Descending") => {
                items.sort_by_key(|m| m.creation_time.clone());
                items.reverse();
            }
            _ => return validation("Invalid sort order"),
        }
    }
    let (page, next) = paginate(
        items.into_iter().map(|m| m.summary()).collect(),
        int_query(params, "maxResults"),
        params.get("nextToken"),
        |s| {
            s.get("modelArn")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        },
    );
    ok(json!({
        "nextToken": next,
        "modelSummaries": page,
    }))
}

// ---------------------------------------------------------------------------
// handlers — tagging ops
// ---------------------------------------------------------------------------

fn tag_resource_op(st: &mut BedrockStore, params: &Value) -> Response {
    let arn = as_str(params, "resourceARN");
    if !all_arns(st).iter().any(|a| a == arn) {
        return not_found(&format!("Resource {arn} not found"));
    }
    // moto: `if tags:` — an absent/empty tag list never reaches the tagger.
    let Some(tags) = params
        .get("tags")
        .and_then(Value::as_array)
        .filter(|t| !t.is_empty())
    else {
        return ok_empty();
    };
    // moto's TooManyTags check counts the resource's existing tags.
    let existing = st.tags.get(arn).map(|m| m.len()).unwrap_or(0);
    if tags.len() + existing > 50 {
        return json_error(
            400,
            "TooManyTagsException",
            "Member must have length less than or equal to 50",
        );
    }
    tag_resource(st, arn, Some(tags));
    ok_empty()
}

fn untag_resource_op(st: &mut BedrockStore, params: &Value) -> Response {
    let arn = as_str(params, "resourceARN");
    if !all_arns(st).iter().any(|a| a == arn) {
        return not_found(&format!("Resource {arn} not found"));
    }
    untag_resource(st, arn, params.get("tagKeys").and_then(Value::as_array));
    ok(json!({}))
}

fn list_tags_op(st: &mut BedrockStore, params: &Value) -> Response {
    let arn = as_str(params, "resourceARN");
    if !all_arns(st).iter().any(|a| a == arn) {
        return not_found(&format!("Resource {arn} not found"));
    }
    ok(json!({"tags": list_tags(st, arn)}))
}

// ---------------------------------------------------------------------------
// handlers — guardrails
// ---------------------------------------------------------------------------

fn create_guardrail(st: &mut BedrockStore, account: u64, region: &str, params: &Value) -> Response {
    let mut g = Guardrail::new(
        account,
        region,
        as_str(params, "name"),
        as_str(params, "blockedInputMessaging"),
        as_str(params, "blockedOutputsMessaging"),
    );
    g.description = as_str(params, "description").to_string();
    g.topic_policy = params
        .get("topicPolicyConfig")
        .cloned()
        .filter(|v| !v.is_null());
    g.content_policy = params
        .get("contentPolicyConfig")
        .cloned()
        .filter(|v| !v.is_null());
    g.word_policy = params
        .get("wordPolicyConfig")
        .cloned()
        .filter(|v| !v.is_null());
    g.sensitive_information_policy = params
        .get("sensitiveInformationPolicyConfig")
        .cloned()
        .filter(|v| !v.is_null());
    g.contextual_grounding_policy = params
        .get("contextualGroundingPolicyConfig")
        .cloned()
        .filter(|v| !v.is_null());
    g.automated_reasoning_policy = params
        .get("automatedReasoningPolicyConfig")
        .cloned()
        .filter(|v| !v.is_null());
    g.cross_region_config = params
        .get("crossRegionConfig")
        .cloned()
        .filter(|v| !v.is_null());
    let g_arn = g.guardrail_arn.clone();
    let created = g.created_at.clone();
    let g_id = g.guardrail_id.clone();
    st.guardrails.insert(g_id.clone(), g);
    if let Some(tags) = params.get("tags").and_then(Value::as_array) {
        if !tags.is_empty() {
            tag_resource(st, &g_arn, Some(tags));
        }
    }
    ok(json!({
        "guardrailId": g_id,
        "guardrailArn": g_arn,
        "version": "DRAFT",
        "createdAt": created,
    }))
}

/// Accepts both the 12-char id and the ARN (moto's `_find_guardrail`).
fn find_guardrail<'a>(
    st: &'a mut BedrockStore,
    identifier: &str,
) -> Result<&'a mut Guardrail, Response> {
    let found_id = if st.guardrails.contains_key(identifier) {
        Some(identifier.to_string())
    } else {
        st.guardrails
            .values()
            .find(|g| g.guardrail_arn == identifier)
            .map(|g| g.guardrail_id.clone())
    };
    match found_id {
        Some(id) => Ok(st.guardrails.get_mut(&id).unwrap()),
        None => Err(not_found(&format!("Could not find guardrail {identifier}"))),
    }
}

fn get_guardrail(st: &mut BedrockStore, identifier: &str, version: Option<&str>) -> Response {
    let g = match find_guardrail(st, identifier) {
        Ok(g) => g,
        Err(r) => return r,
    };
    if let Some(v) = version.filter(|v| !v.is_empty() && *v != "DRAFT") {
        return match g.versions.get(v) {
            Some(snapshot) => ok(snapshot.clone()),
            None => not_found(&format!("Could not find guardrail version {v}")),
        };
    }
    ok(g.to_dict())
}

fn update_guardrail(st: &mut BedrockStore, identifier: &str, params: &Value) -> Response {
    let g = match find_guardrail(st, identifier) {
        Ok(g) => g,
        Err(r) => return r,
    };
    g.name = as_str(params, "name").to_string();
    g.blocked_input_messaging = as_str(params, "blockedInputMessaging").to_string();
    g.blocked_outputs_messaging = as_str(params, "blockedOutputsMessaging").to_string();
    if let Some(d) = params.get("description").and_then(Value::as_str) {
        g.description = d.to_string();
    }
    // moto assigns the optional policy blobs unconditionally (None clears).
    g.topic_policy = params
        .get("topicPolicyConfig")
        .cloned()
        .filter(|v| !v.is_null());
    g.content_policy = params
        .get("contentPolicyConfig")
        .cloned()
        .filter(|v| !v.is_null());
    g.word_policy = params
        .get("wordPolicyConfig")
        .cloned()
        .filter(|v| !v.is_null());
    g.sensitive_information_policy = params
        .get("sensitiveInformationPolicyConfig")
        .cloned()
        .filter(|v| !v.is_null());
    g.contextual_grounding_policy = params
        .get("contextualGroundingPolicyConfig")
        .cloned()
        .filter(|v| !v.is_null());
    g.automated_reasoning_policy = params
        .get("automatedReasoningPolicyConfig")
        .cloned()
        .filter(|v| !v.is_null());
    g.cross_region_config = params
        .get("crossRegionConfig")
        .cloned()
        .filter(|v| !v.is_null());
    if let Some(kms) = params.get("kmsKeyId").and_then(Value::as_str) {
        g.kms_key_id = Some(kms.to_string());
    }
    g.updated_at = iso_now();
    ok(json!({
        "guardrailId": g.guardrail_id,
        "guardrailArn": g.guardrail_arn,
        "version": "DRAFT",
        "updatedAt": g.updated_at,
    }))
}

fn delete_guardrail(st: &mut BedrockStore, identifier: &str, version: Option<&str>) -> Response {
    let g_id = match find_guardrail(st, identifier) {
        Ok(g) => g.guardrail_id.clone(),
        Err(r) => return r,
    };
    if let Some(v) = version.filter(|v| !v.is_empty() && *v != "DRAFT") {
        let g = st.guardrails.get_mut(&g_id).unwrap();
        if g.versions.remove(v).is_some() {
            return ok_empty();
        }
        return not_found(&format!("Could not find guardrail version {v}"));
    }
    st.guardrails.remove(&g_id);
    ok_empty()
}

fn list_guardrails(st: &mut BedrockStore, params: &Value) -> Response {
    let mut out: Vec<Value> = Vec::new();
    match str_opt(params.get("guardrailIdentifier")) {
        Some(identifier) => match find_guardrail(st, identifier) {
            Ok(g) => out.push(g.summary()),
            Err(r) => return r,
        },
        None => {
            for g in st.guardrails.values() {
                out.push(g.summary());
            }
        }
    }
    let (page, next) = paginate(
        out,
        int_query(params, "maxResults"),
        params.get("nextToken"),
        |s| {
            s.get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        },
    );
    ok(json!({"guardrails": page, "nextToken": next}))
}

fn create_guardrail_version(st: &mut BedrockStore, identifier: &str, params: &Value) -> Response {
    let g = match find_guardrail(st, identifier) {
        Ok(g) => g,
        Err(r) => return r,
    };
    let version_num = g.next_version.to_string();
    g.next_version += 1;
    let mut snapshot = g.to_dict();
    snapshot["version"] = json!(version_num);
    if let Some(d) = params.get("description").and_then(Value::as_str) {
        snapshot["description"] = json!(d);
    }
    g.versions.insert(version_num.clone(), snapshot);
    ok(json!({"guardrailId": g.guardrail_id, "version": version_num}))
}

// ---------------------------------------------------------------------------
// handlers — provisioned model throughput
// ---------------------------------------------------------------------------

fn create_provisioned_model_throughput(
    st: &mut BedrockStore,
    account: u64,
    region: &str,
    params: &Value,
) -> Response {
    let pmt = ProvisionedThroughput::new(
        account,
        region,
        as_str(params, "provisionedModelName"),
        as_str(params, "modelId"),
        params
            .get("modelUnits")
            .and_then(Value::as_i64)
            .unwrap_or(1),
        params
            .get("commitmentDuration")
            .and_then(Value::as_str)
            .map(String::from),
    );
    let arn = pmt.provisioned_model_arn.clone();
    st.provisioned_model_throughputs
        .insert(pmt.provisioned_model_id.clone(), pmt);
    if let Some(tags) = params.get("tags").and_then(Value::as_array) {
        if !tags.is_empty() {
            tag_resource(st, &arn, Some(tags));
        }
    }
    ok(json!({"provisionedModelArn": arn}))
}

fn find_pmt<'a>(
    st: &'a mut BedrockStore,
    id: &str,
) -> Result<&'a mut ProvisionedThroughput, Response> {
    if st.provisioned_model_throughputs.contains_key(id) {
        return Ok(st.provisioned_model_throughputs.get_mut(id).unwrap());
    }
    // moto matches ARN or provisioned name too.
    let found_id = st
        .provisioned_model_throughputs
        .values()
        .find(|p| p.provisioned_model_arn == id || p.provisioned_model_name == id)
        .map(|p| p.provisioned_model_id.clone());
    match found_id {
        Some(f) => Ok(st.provisioned_model_throughputs.get_mut(&f).unwrap()),
        None => Err(not_found(&format!("Could not find provisioned model {id}"))),
    }
}

fn get_provisioned_model_throughput(st: &mut BedrockStore, id: &str) -> Response {
    match find_pmt(st, id) {
        Ok(p) => ok(p.to_dict()),
        Err(r) => r,
    }
}

fn update_provisioned_model_throughput(
    st: &mut BedrockStore,
    region: &str,
    id: &str,
    params: &Value,
) -> Response {
    let name = params
        .get("desiredProvisionedModelName")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    let desired = params
        .get("desiredModelId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    match find_pmt(st, id) {
        Ok(p) => {
            if let Some(n) = name {
                p.provisioned_model_name = n.to_string();
            }
            if let Some(model_id) = desired {
                p.desired_model_arn = format!(
                    "arn:{}:bedrock:{region}::foundation-model/{model_id}",
                    partition(region),
                );
            }
            p.last_modified_time = iso_now();
            ok_empty()
        }
        Err(r) => r,
    }
}

fn delete_provisioned_model_throughput(st: &mut BedrockStore, id: &str) -> Response {
    let not_found = not_found(&format!("Could not find provisioned model {id}"));
    let found_id = st
        .provisioned_model_throughputs
        .get(id)
        .map(|p| p.provisioned_model_id.clone())
        .or_else(|| {
            st.provisioned_model_throughputs
                .values()
                .find(|p| p.provisioned_model_arn == id || p.provisioned_model_name == id)
                .map(|p| p.provisioned_model_id.clone())
        });
    match found_id {
        Some(key) => {
            st.provisioned_model_throughputs.remove(&key);
            ok_empty()
        }
        None => not_found,
    }
}

fn list_provisioned_model_throughputs(st: &BedrockStore, params: &Value) -> Response {
    let mut out: Vec<Value> = st
        .provisioned_model_throughputs
        .values()
        .map(|p| p.summary())
        .collect();
    if let Some(contains) = str_opt(params.get("nameContains")) {
        out.retain(|p| {
            p.get("provisionedModelName")
                .and_then(Value::as_str)
                .map(|n| n.contains(contains))
                .unwrap_or(false)
        });
    }
    if let Some(status) = str_opt(params.get("statusEquals")) {
        out.retain(|p| p.get("status").and_then(Value::as_str) == Some(status));
    }
    if let Some(arn) = str_opt(params.get("modelArnEquals")) {
        out.retain(|p| p.get("modelArn").and_then(Value::as_str) == Some(arn));
    }
    let (page, next) = paginate(
        out,
        int_query(params, "maxResults"),
        params.get("nextToken"),
        |s| {
            s.get("provisionedModelArn")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        },
    );
    ok(json!({"nextToken": next, "provisionedModelSummaries": page}))
}

// ---------------------------------------------------------------------------
// handlers — evaluation jobs
// ---------------------------------------------------------------------------

fn create_evaluation_job(
    st: &mut BedrockStore,
    account: u64,
    region: &str,
    params: &Value,
) -> Response {
    let cfg = params.get("evaluationConfig").cloned().unwrap_or(json!({}));
    let mut job = EvaluationJob::new(
        region,
        account,
        as_str(params, "jobName"),
        as_str(params, "roleArn"),
        &cfg,
    );
    let inference = params.get("inferenceConfig").cloned();
    let output = params.get("outputDataConfig").cloned();
    job.inference_config = inference.unwrap_or(Value::Null);
    job.output_data_config = output.unwrap_or(Value::Null);
    job.job_description = as_str(params, "jobDescription").to_string();
    job.customer_encryption_key_id = params
        .get("customerEncryptionKeyId")
        .and_then(Value::as_str)
        .map(String::from);
    let application = params
        .get("applicationType")
        .and_then(Value::as_str)
        .map(String::from)
        .unwrap_or_else(|| "ModelEvaluation".to_string());
    job.application_type = application;
    let arn = job.job_arn.clone();
    st.evaluation_jobs.insert(job.job_arn.clone(), job);
    if let Some(tags) = params.get("jobTags").and_then(Value::as_array) {
        if !tags.is_empty() {
            tag_resource(st, &arn, Some(tags));
        }
    }
    ok(json!({"jobArn": arn}))
}

fn find_evaluation_job<'a>(st: &'a BedrockStore, id: &str) -> Option<&'a EvaluationJob> {
    if let Some(j) = st.evaluation_jobs.get(id) {
        return Some(j);
    }
    st.evaluation_jobs
        .values()
        .find(|j| j.job_name == id || j.job_id == id)
}

fn get_evaluation_job(st: &BedrockStore, id: &str) -> Response {
    match find_evaluation_job(st, id) {
        Some(j) => ok(j.to_dict()),
        None => not_found(&format!("Could not find evaluation job {id}")),
    }
}

fn stop_evaluation_job(st: &mut BedrockStore, id: &str) -> Response {
    let arn = match find_evaluation_job(st, id) {
        Some(j) => j.job_arn.clone(),
        None => return not_found(&format!("Could not find evaluation job {id}")),
    };
    let j = st.evaluation_jobs.get_mut(&arn).unwrap();
    j.status = "Stopped".to_string();
    j.last_modified_time = iso_now();
    ok_empty()
}

fn batch_delete_evaluation_job(st: &mut BedrockStore, params: &Value) -> Response {
    let ids: Vec<String> = params
        .get("jobIdentifiers")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();
    let mut errors = Vec::new();
    let mut deleted = Vec::new();
    for id in ids {
        let arn = find_evaluation_job(st, &id).map(|j| j.job_arn.clone());
        match arn {
            Some(arn) => {
                let (status, arn) = {
                    let j = st.evaluation_jobs.get(&arn).unwrap();
                    (j.status.clone(), j.job_arn.clone())
                };
                st.evaluation_jobs.remove(&arn);
                deleted.push(json!({"jobArn": arn, "status": status}));
            }
            None => errors.push(json!({
                "jobIdentifier": id,
                "code": "ResourceNotFoundException",
                "message": format!("Evaluation job {id} not found"),
            })),
        }
    }
    ok(json!({"errors": errors, "evaluationJobs": deleted}))
}

fn list_evaluation_jobs(st: &BedrockStore, params: &Value) -> Response {
    let mut out: Vec<Value> = st.evaluation_jobs.values().map(|j| j.summary()).collect();
    if let Some(contains) = str_opt(params.get("nameContains")) {
        out.retain(|j| {
            j.get("jobName")
                .and_then(Value::as_str)
                .map(|n| n.contains(contains))
                .unwrap_or(false)
        });
    }
    if let Some(status) = str_opt(params.get("statusEquals")) {
        out.retain(|j| j.get("status").and_then(Value::as_str) == Some(status));
    }
    if let Some(app) = str_opt(params.get("applicationTypeEquals")) {
        out.retain(|j| j.get("applicationType").and_then(Value::as_str) == Some(app));
    }
    let (page, next) = paginate(
        out,
        int_query(params, "maxResults"),
        params.get("nextToken"),
        |s| {
            s.get("jobArn")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        },
    );
    ok(json!({"nextToken": next, "jobSummaries": page}))
}

// ---------------------------------------------------------------------------
// handlers — inference profiles
// ---------------------------------------------------------------------------

fn create_inference_profile(
    st: &mut BedrockStore,
    account: u64,
    region: &str,
    params: &Value,
) -> Response {
    let p = InferenceProfile::new(
        account,
        region,
        as_str(params, "inferenceProfileName"),
        params.get("modelSource").unwrap_or(&json!({})),
        params.get("description").and_then(Value::as_str),
    );
    let arn = p.inference_profile_arn.clone();
    let status = p.status.clone();
    st.inference_profiles
        .insert(p.inference_profile_id.clone(), p);
    if let Some(tags) = params.get("tags").and_then(Value::as_array) {
        if !tags.is_empty() {
            tag_resource(st, &arn, Some(tags));
        }
    }
    ok(json!({"inferenceProfileArn": arn, "status": status}))
}

fn find_inference_profile<'a>(
    st: &'a BedrockStore,
    id: &str,
) -> Result<&'a InferenceProfile, Response> {
    if let Some(p) = st.inference_profiles.get(id) {
        return Ok(p);
    }
    st.inference_profiles
        .values()
        .find(|p| p.inference_profile_arn == id || p.inference_profile_name == id)
        .ok_or_else(|| not_found(&format!("Could not find inference profile {id}")))
}

fn get_inference_profile(st: &mut BedrockStore, id: &str) -> Response {
    match find_inference_profile(st, id) {
        Ok(p) => ok(p.to_dict()),
        Err(r) => r,
    }
}

fn delete_inference_profile(st: &mut BedrockStore, id: &str) -> Response {
    match find_inference_profile(st, id) {
        Ok(p) => {
            st.inference_profiles
                .remove(&p.inference_profile_id.clone());
            ok_empty()
        }
        Err(r) => r,
    }
}

fn list_inference_profiles(st: &BedrockStore, params: &Value) -> Response {
    let mut out: Vec<Value> = st
        .inference_profiles
        .values()
        .map(|p| p.summary())
        .collect();
    if let Some(t) = str_opt(params.get("type")) {
        out.retain(|p| p.get("type").and_then(Value::as_str) == Some(t));
    }
    let (page, next) = paginate(
        out,
        int_query(params, "maxResults"),
        params.get("nextToken"),
        |s| {
            s.get("inferenceProfileId")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        },
    );
    ok(json!({"inferenceProfileSummaries": page, "nextToken": next}))
}

// ---------------------------------------------------------------------------
// handlers — model import jobs / imported models
// ---------------------------------------------------------------------------

fn create_model_import_job(
    st: &mut BedrockStore,
    account: u64,
    region: &str,
    params: &Value,
) -> Response {
    let job_name = as_str(params, "jobName");
    let imported_model_name = as_str(params, "importedModelName");
    let import_kms = params
        .get("importedModelKmsKeyId")
        .and_then(Value::as_str)
        .map(String::from);
    let job_id = short_id();
    let job_arn = format!(
        "arn:{}:bedrock:{region}:{account}:model-import-job/{job_id}",
        partition(region)
    );
    let imported_model_arn = format!(
        "arn:{}:bedrock:{region}:{account}:imported-model/{imported_model_name}",
        partition(region)
    );
    let imported_model_kms_key_arn: String = import_kms
        .clone()
        .map(|k| format!("arn:{}:kms:{region}:{account}:key/{k}", partition(region)))
        .unwrap_or_default();
    let job = ModelImportJob {
        job_name: job_name.to_string(),
        job_id,
        job_arn: job_arn.clone(),
        imported_model_name: imported_model_name.to_string(),
        imported_model_arn: imported_model_arn.clone(),
        role_arn: as_str(params, "roleArn").to_string(),
        model_data_source: params.get("modelDataSource").cloned().unwrap_or(json!({})),
        vpc_config: params.get("vpcConfig").cloned(),
        imported_model_kms_key_arn: imported_model_kms_key_arn.clone(),
        status: "Completed".to_string(),
        creation_time: iso_now(),
        last_modified_time: iso_now(),
        end_time: iso_now(),
    };
    st.model_import_jobs.insert(job_arn.clone(), job);
    if let Some(tags) = params.get("jobTags").and_then(Value::as_array) {
        if !tags.is_empty() {
            tag_resource(st, &job_arn, Some(tags));
        }
    }
    let model = ImportedModel {
        model_name: imported_model_name.to_string(),
        model_arn: imported_model_arn.clone(),
        job_name: job_name.to_string(),
        job_arn: job_arn.clone(),
        model_data_source: params.get("modelDataSource").cloned().unwrap_or(json!({})),
        model_kms_key_arn: if imported_model_kms_key_arn.is_empty() {
            None
        } else {
            Some(imported_model_kms_key_arn.clone())
        },
        creation_time: iso_now(),
    };
    st.imported_models
        .insert(imported_model_name.to_string(), model);
    if let Some(tags) = params.get("importedModelTags").and_then(Value::as_array) {
        if !tags.is_empty() {
            tag_resource(st, &imported_model_arn, Some(tags));
        }
    }
    ok(json!({"jobArn": job_arn}))
}

fn find_model_import_job<'a>(
    st: &'a BedrockStore,
    id: &str,
) -> Result<&'a ModelImportJob, Response> {
    if let Some(j) = st.model_import_jobs.get(id) {
        return Ok(j);
    }
    st.model_import_jobs
        .values()
        .find(|j| j.job_name == id || j.job_id == id)
        .ok_or_else(|| not_found(&format!("Could not find model import job {id}")))
}

fn get_model_import_job(st: &BedrockStore, id: &str) -> Response {
    match find_model_import_job(st, id) {
        Ok(j) => ok(j.to_dict()),
        Err(r) => r,
    }
}

fn list_model_import_jobs(st: &BedrockStore, params: &Value) -> Response {
    let mut out: Vec<Value> = st.model_import_jobs.values().map(|j| j.summary()).collect();
    if let Some(contains) = str_opt(params.get("nameContains")) {
        out.retain(|j| {
            j.get("jobName")
                .and_then(Value::as_str)
                .map(|n| n.contains(contains))
                .unwrap_or(false)
        });
    }
    if let Some(status) = str_opt(params.get("statusEquals")) {
        out.retain(|j| j.get("status").and_then(Value::as_str) == Some(status));
    }
    let (page, next) = paginate(
        out,
        int_query(params, "maxResults"),
        params.get("nextToken"),
        |s| {
            s.get("jobArn")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        },
    );
    ok(json!({"nextToken": next, "modelImportJobSummaries": page}))
}

fn find_imported_model<'a>(st: &'a BedrockStore, id: &str) -> Result<&'a ImportedModel, Response> {
    if let Some(m) = st.imported_models.get(id) {
        return Ok(m);
    }
    st.imported_models
        .values()
        .find(|m| m.model_arn == id)
        .ok_or_else(|| not_found(&format!("Could not find imported model {id}")))
}

fn get_imported_model(st: &BedrockStore, id: &str) -> Response {
    match find_imported_model(st, id) {
        Ok(m) => ok(m.to_dict()),
        Err(r) => r,
    }
}

fn delete_imported_model(st: &mut BedrockStore, id: &str) -> Response {
    match find_imported_model(st, id) {
        Ok(m) => {
            st.imported_models.remove(&m.model_name.clone());
            ok_empty()
        }
        Err(r) => r,
    }
}

fn list_imported_models(st: &BedrockStore, params: &Value) -> Response {
    let mut out: Vec<Value> = st.imported_models.values().map(|m| m.summary()).collect();
    if let Some(contains) = str_opt(params.get("nameContains")) {
        out.retain(|m| {
            m.get("modelName")
                .and_then(Value::as_str)
                .map(|n| n.contains(contains))
                .unwrap_or(false)
        });
    }
    let (page, next) = paginate(
        out,
        int_query(params, "maxResults"),
        params.get("nextToken"),
        |s| {
            s.get("modelArn")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        },
    );
    ok(json!({"nextToken": next, "modelSummaries": page}))
}

// ---------------------------------------------------------------------------
// handlers — model copy jobs
// ---------------------------------------------------------------------------

fn create_model_copy_job(
    st: &mut BedrockStore,
    account: u64,
    region: &str,
    params: &Value,
) -> Response {
    let source_model_arn = as_str(params, "sourceModelArn");
    let target_model_name = as_str(params, "targetModelName");
    let model_kms_key_id = params
        .get("modelKmsKeyId")
        .and_then(Value::as_str)
        .map(String::from);
    let job_id = short_id();
    let job_arn = format!(
        "arn:{}:bedrock:{region}:{account}:model-copy-job/{job_id}",
        partition(region)
    );
    let parts: Vec<&str> = source_model_arn.split(':').collect();
    let source_account_id = if parts.len() > 4 {
        parts[4].to_string()
    } else {
        account.to_string()
    };
    let source_model_name = if source_model_arn.contains('/') {
        source_model_arn
            .split('/')
            .next_back()
            .unwrap_or("")
            .to_string()
    } else {
        String::new()
    };
    let job = ModelCopyJob {
        job_arn: job_arn.clone(),
        source_model_arn: source_model_arn.to_string(),
        source_model_name,
        source_account_id,
        target_model_name: target_model_name.to_string(),
        target_model_arn: format!(
            "arn:{}:bedrock:{region}:{account}:custom-model/{target_model_name}",
            partition(region)
        ),
        target_model_kms_key_arn: model_kms_key_id
            .clone()
            .map(|k| format!("arn:{}:kms:{region}:{account}:key/{k}", partition(region)))
            .unwrap_or_default(),
        target_model_tags: params.get("targetModelTags").cloned(),
        status: "Completed".to_string(),
        creation_time: iso_now(),
    };
    st.model_copy_jobs.insert(job_arn.clone(), job);
    ok(json!({"jobArn": job_arn}))
}

fn get_model_copy_job(st: &BedrockStore, job_arn: &str) -> Response {
    match st.model_copy_jobs.get(job_arn) {
        Some(j) => ok(j.to_dict()),
        None => not_found(&format!("Could not find model copy job {job_arn}")),
    }
}

fn list_model_copy_jobs(st: &BedrockStore, params: &Value) -> Response {
    let mut out: Vec<Value> = st.model_copy_jobs.values().map(|j| j.summary()).collect();
    if let Some(status) = str_opt(params.get("statusEquals")) {
        out.retain(|j| j.get("status").and_then(Value::as_str) == Some(status));
    }
    if let Some(acct) = str_opt(params.get("sourceAccountEquals")) {
        out.retain(|j| j.get("sourceAccountId").and_then(Value::as_str) == Some(acct));
    }
    if let Some(arn) = str_opt(params.get("sourceModelArnEquals")) {
        out.retain(|j| j.get("sourceModelArn").and_then(Value::as_str) == Some(arn));
    }
    // botocore serializes this member as `outputModelNameContains`.
    if let Some(contains) = str_opt(params.get("outputModelNameContains")) {
        out.retain(|j| {
            j.get("targetModelName")
                .and_then(Value::as_str)
                .map(|n| n.contains(contains))
                .unwrap_or(false)
        });
    }
    let (page, next) = paginate(
        out,
        int_query(params, "maxResults"),
        params.get("nextToken"),
        |s| {
            s.get("jobArn")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        },
    );
    ok(json!({"nextToken": next, "modelCopyJobSummaries": page}))
}

// ---------------------------------------------------------------------------
// handlers — model invocation jobs
// ---------------------------------------------------------------------------

fn create_model_invocation_job(
    st: &mut BedrockStore,
    account: u64,
    region: &str,
    params: &Value,
) -> Response {
    let job_id = short_id();
    let job_arn = format!(
        "arn:{}:bedrock:{region}:{account}:model-invocation-job/{job_id}",
        partition(region)
    );
    let job = ModelInvocationJob {
        job_name: as_str(params, "jobName").to_string(),
        job_id,
        job_arn: job_arn.clone(),
        role_arn: as_str(params, "roleArn").to_string(),
        model_id: as_str(params, "modelId").to_string(),
        input_data_config: params.get("inputDataConfig").cloned().unwrap_or(json!({})),
        output_data_config: params.get("outputDataConfig").cloned().unwrap_or(json!({})),
        client_request_token: params
            .get("clientRequestToken")
            .and_then(Value::as_str)
            .map(String::from),
        vpc_config: params.get("vpcConfig").cloned(),
        timeout_duration_in_hours: params
            .get("timeoutDurationInHours")
            .and_then(Value::as_i64)
            .unwrap_or(24),
        model_invocation_type: params
            .get("modelInvocationType")
            .and_then(Value::as_str)
            .map(String::from),
        status: "InProgress".to_string(),
        submit_time: iso_now(),
        last_modified_time: iso_now(),
        end_time: String::new(),
    };
    st.model_invocation_jobs.insert(job_arn.clone(), job);
    if let Some(tags) = params.get("tags").and_then(Value::as_array) {
        if !tags.is_empty() {
            tag_resource(st, &job_arn, Some(tags));
        }
    }
    ok(json!({"jobArn": job_arn}))
}

fn find_model_invocation_job<'a>(
    st: &'a BedrockStore,
    id: &str,
) -> Result<&'a ModelInvocationJob, Response> {
    if let Some(j) = st.model_invocation_jobs.get(id) {
        return Ok(j);
    }
    st.model_invocation_jobs
        .values()
        .find(|j| j.job_name == id || j.job_id == id)
        .ok_or_else(|| not_found(&format!("Could not find model invocation job {id}")))
}

fn get_model_invocation_job(st: &BedrockStore, id: &str) -> Response {
    match find_model_invocation_job(st, id) {
        Ok(j) => ok(j.to_dict()),
        Err(r) => r,
    }
}

fn stop_model_invocation_job(st: &mut BedrockStore, id: &str) -> Response {
    let arn = match find_model_invocation_job(st, id) {
        Ok(j) => j.job_arn.clone(),
        Err(r) => return r,
    };
    let j = st.model_invocation_jobs.get_mut(&arn).unwrap();
    j.status = "Stopped".to_string();
    j.last_modified_time = iso_now();
    ok_empty()
}

fn list_model_invocation_jobs(st: &BedrockStore, params: &Value) -> Response {
    let mut out: Vec<Value> = st
        .model_invocation_jobs
        .values()
        .map(|j| j.summary())
        .collect();
    if let Some(contains) = str_opt(params.get("nameContains")) {
        out.retain(|j| {
            j.get("jobName")
                .and_then(Value::as_str)
                .map(|n| n.contains(contains))
                .unwrap_or(false)
        });
    }
    if let Some(status) = str_opt(params.get("statusEquals")) {
        out.retain(|j| j.get("status").and_then(Value::as_str) == Some(status));
    }
    let (page, next) = paginate(
        out,
        int_query(params, "maxResults"),
        params.get("nextToken"),
        |s| {
            s.get("jobArn")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        },
    );
    ok(json!({"nextToken": next, "invocationJobSummaries": page}))
}

// ---------------------------------------------------------------------------
// handlers — foundation models (read-only catalog)
// ---------------------------------------------------------------------------

fn get_foundation_model(region: &str, model_identifier: &str) -> Response {
    for fm in foundation_models(region) {
        if fm.model_id == model_identifier || fm.model_arn == model_identifier {
            return ok(json!({"modelDetails": fm.to_dict()}));
        }
    }
    not_found(&format!(
        "Could not find foundation model {model_identifier}"
    ))
}

fn list_foundation_models(region: &str, params: &Value) -> Response {
    let mut out: Vec<Value> = Vec::new();
    for fm in foundation_models(region) {
        if let Some(provider) = str_opt(params.get("byProvider")) {
            if fm.provider != provider {
                continue;
            }
        }
        if let Some(modality) = str_opt(params.get("byOutputModality")) {
            // catalog is TEXT-only, mirroring moto.
            if !fm
                .to_dict()
                .get("outputModalities")
                .and_then(Value::as_array)
                .map(|a| a.iter().any(|m| m.as_str() == Some(modality)))
                .unwrap_or(false)
            {
                continue;
            }
        }
        if let Some(itype) = str_opt(params.get("byInferenceType")) {
            if !fm
                .to_dict()
                .get("inferenceTypesSupported")
                .and_then(Value::as_array)
                .map(|a| a.iter().any(|m| m.as_str() == Some(itype)))
                .unwrap_or(false)
            {
                continue;
            }
        }
        out.push(fm.to_dict());
    }
    ok(json!({"modelSummaries": out}))
}

// ---------------------------------------------------------------------------
// handlers — marketplace model endpoints
// ---------------------------------------------------------------------------

fn create_marketplace_endpoint(
    st: &mut BedrockStore,
    account: u64,
    region: &str,
    params: &Value,
) -> Response {
    let endpoint_name = params
        .get("endpointName")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .unwrap_or_else(|| format!("endpoint-{}", &Uuid::new_v4().hyphenated().to_string()[..8]));
    let ep = MarketplaceModelEndpoint {
        model_source_identifier: as_str(params, "modelSourceIdentifier").to_string(),
        endpoint_config: params.get("endpointConfig").cloned().unwrap_or(json!({})),
        endpoint_arn: format!(
            "arn:{}:bedrock:{region}:{account}:marketplace-model-endpoint/{endpoint_name}",
            partition(region)
        ),
        status: "ACTIVE".to_string(),
        created_at: iso_now(),
        updated_at: iso_now(),
        registered: false,
    };
    let arn = ep.endpoint_arn.clone();
    let dict = ep.to_dict();
    st.marketplace_endpoints.insert(arn.clone(), ep);
    if let Some(tags) = params.get("tags").and_then(Value::as_array) {
        if !tags.is_empty() {
            tag_resource(st, &arn, Some(tags));
        }
    }
    ok(json!({"marketplaceModelEndpoint": dict}))
}

fn find_marketplace_endpoint<'a>(
    st: &'a mut BedrockStore,
    id: &str,
) -> Result<&'a mut MarketplaceModelEndpoint, Response> {
    if st.marketplace_endpoints.contains_key(id) {
        return Ok(st.marketplace_endpoints.get_mut(id).unwrap());
    }
    let found = st
        .marketplace_endpoints
        .values()
        .find(|e| e.endpoint_arn.ends_with(&format!("/{id}")) || e.endpoint_arn == id)
        .map(|e| e.endpoint_arn.clone());
    match found {
        Some(f) => Ok(st.marketplace_endpoints.get_mut(&f).unwrap()),
        None => Err(not_found(&format!(
            "Could not find marketplace model endpoint {id}"
        ))),
    }
}

fn get_marketplace_endpoint(st: &mut BedrockStore, arn: &str) -> Response {
    match find_marketplace_endpoint(st, arn) {
        Ok(ep) => {
            let dict = ep.to_dict();
            ok(json!({"marketplaceModelEndpoint": dict}))
        }
        Err(r) => r,
    }
}

fn update_marketplace_endpoint(st: &mut BedrockStore, arn: &str, params: &Value) -> Response {
    match find_marketplace_endpoint(st, arn) {
        Ok(ep) => {
            ep.endpoint_config = params.get("endpointConfig").cloned().unwrap_or(json!({}));
            ep.updated_at = iso_now();
            ok(json!({"marketplaceModelEndpoint": ep.to_dict()}))
        }
        Err(r) => r,
    }
}

fn delete_marketplace_endpoint(st: &mut BedrockStore, arn: &str) -> Response {
    match find_marketplace_endpoint(st, arn) {
        Ok(ep) => {
            let key = ep.endpoint_arn.clone();
            st.marketplace_endpoints.remove(&key);
            ok_empty()
        }
        Err(r) => r,
    }
}

fn register_marketplace_endpoint(st: &mut BedrockStore, id: &str, params: &Value) -> Response {
    match find_marketplace_endpoint(st, id) {
        Ok(ep) => {
            ep.registered = true;
            ep.model_source_identifier = as_str(params, "modelSourceIdentifier").to_string();
            let dict = ep.to_dict();
            ok(json!({"marketplaceModelEndpoint": dict}))
        }
        Err(r) => r,
    }
}

fn deregister_marketplace_endpoint(st: &mut BedrockStore, arn: &str) -> Response {
    match find_marketplace_endpoint(st, arn) {
        Ok(ep) => {
            ep.registered = false;
            ok_empty()
        }
        Err(r) => r,
    }
}

fn list_marketplace_endpoints(st: &BedrockStore, params: &Value) -> Response {
    let mut out: Vec<Value> = st
        .marketplace_endpoints
        .values()
        .map(|e| e.to_dict())
        .collect();
    if let Some(src) = str_opt(params.get("modelSourceIdentifier")) {
        out.retain(|e| e.get("modelSourceIdentifier").and_then(Value::as_str) == Some(src));
    }
    let (page, next) = paginate(
        out,
        int_query(params, "maxResults"),
        params.get("nextToken"),
        |s| {
            s.get("endpointArn")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        },
    );
    ok(json!({"marketplaceModelEndpoints": page, "nextToken": next}))
}

// ---------------------------------------------------------------------------
// handlers — prompt routers
// ---------------------------------------------------------------------------

fn create_prompt_router(
    st: &mut BedrockStore,
    account: u64,
    region: &str,
    params: &Value,
) -> Response {
    let id = short_id();
    let pr = PromptRouter {
        prompt_router_name: as_str(params, "promptRouterName").to_string(),
        prompt_router_arn: format!(
            "arn:{}:bedrock:{region}:{account}:prompt-router/{id}",
            partition(region)
        ),
        models: params
            .get("models")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        fallback_model: params.get("fallbackModel").cloned().unwrap_or(json!({})),
        routing_criteria: params.get("routingCriteria").cloned().unwrap_or(json!({})),
        description: as_str(params, "description").to_string(),
        status: "ACTIVE".to_string(),
        router_type: "custom".to_string(),
        created_at: iso_now(),
        updated_at: iso_now(),
    };
    let arn = pr.prompt_router_arn.clone();
    st.prompt_routers.insert(arn.clone(), pr);
    if let Some(tags) = params.get("tags").and_then(Value::as_array) {
        if !tags.is_empty() {
            tag_resource(st, &arn, Some(tags));
        }
    }
    ok(json!({"promptRouterArn": arn}))
}

fn find_prompt_router<'a>(st: &'a BedrockStore, arn: &str) -> Result<&'a PromptRouter, Response> {
    st.prompt_routers
        .get(arn)
        .ok_or_else(|| not_found(&format!("Could not find prompt router {arn}")))
}

fn get_prompt_router(st: &BedrockStore, arn: &str) -> Response {
    match find_prompt_router(st, arn) {
        Ok(pr) => ok(pr.to_dict()),
        Err(r) => r,
    }
}

fn delete_prompt_router(st: &mut BedrockStore, arn: &str) -> Response {
    if !st.prompt_routers.contains_key(arn) {
        return not_found(&format!("Could not find prompt router {arn}"));
    }
    st.prompt_routers.remove(arn);
    ok_empty()
}

fn list_prompt_routers(st: &BedrockStore, params: &Value) -> Response {
    let mut out: Vec<Value> = st.prompt_routers.values().map(|r| r.summary()).collect();
    if let Some(t) = str_opt(params.get("type")) {
        out.retain(|r| r.get("type").and_then(Value::as_str) == Some(t));
    }
    let (page, next) = paginate(
        out,
        int_query(params, "maxResults"),
        params.get("nextToken"),
        |s| {
            s.get("promptRouterArn")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        },
    );
    ok(json!({"promptRouterSummaries": page, "nextToken": next}))
}

// ---------------------------------------------------------------------------
// handlers — custom model deployments
// ---------------------------------------------------------------------------

fn create_custom_model_deployment(
    st: &mut BedrockStore,
    account: u64,
    region: &str,
    params: &Value,
) -> Response {
    let deployment_id = short_id();
    let dep = CustomModelDeployment {
        model_deployment_name: as_str(params, "modelDeploymentName").to_string(),
        model_id: as_str(params, "modelId").to_string(),
        deployment_id: deployment_id.clone(),
        deployment_arn: format!(
            "arn:{}:bedrock:{region}:{account}:custom-model-deployment/{deployment_id}",
            partition(region)
        ),
        status: "ACTIVE".to_string(),
        created_at: iso_now(),
        last_modified_time: iso_now(),
    };
    let arn = dep.deployment_arn.clone();
    st.custom_model_deployments
        .insert(deployment_id.clone(), dep);
    ok(json!({"customModelDeploymentArn": arn}))
}

fn find_deployment<'a>(
    st: &'a BedrockStore,
    id: &str,
) -> Result<&'a CustomModelDeployment, Response> {
    if let Some(d) = st.custom_model_deployments.get(id) {
        return Ok(d);
    }
    st.custom_model_deployments
        .values()
        .find(|d| d.deployment_arn == id)
        .ok_or_else(|| not_found(&format!("Could not find custom model deployment {id}")))
}

fn get_custom_model_deployment(st: &BedrockStore, id: &str) -> Response {
    match find_deployment(st, id) {
        Ok(d) => ok(d.to_dict()),
        Err(r) => r,
    }
}

fn update_custom_model_deployment(st: &mut BedrockStore, id: &str) -> Response {
    let found_id = match find_deployment(st, id) {
        Ok(d) => d.deployment_id.clone(),
        Err(r) => return r,
    };
    let dep = st.custom_model_deployments.get_mut(&found_id).unwrap();
    dep.last_modified_time = iso_now();
    ok(json!({"customModelDeploymentArn": dep.deployment_arn}))
}

fn delete_custom_model_deployment(st: &mut BedrockStore, id: &str) -> Response {
    match find_deployment(st, id) {
        Ok(d) => {
            st.custom_model_deployments.remove(&d.deployment_id.clone());
            ok_empty()
        }
        Err(r) => r,
    }
}

fn list_custom_model_deployments(st: &BedrockStore, params: &Value) -> Response {
    let out: Vec<Value> = st
        .custom_model_deployments
        .values()
        .map(|d| d.to_dict())
        .collect();
    let (page, next) = paginate(
        out,
        int_query(params, "maxResults"),
        params.get("nextToken"),
        |s| {
            s.get("customModelDeploymentIdentifier")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        },
    );
    ok(json!({"customModelDeploymentSummaries": page, "nextToken": next}))
}

// ---------------------------------------------------------------------------
// handlers — automated reasoning policies
// ---------------------------------------------------------------------------

fn create_ar_policy(st: &mut BedrockStore, account: u64, region: &str, params: &Value) -> Response {
    let id = short_id();
    let mut p = AutomatedReasoningPolicy {
        name: as_str(params, "name").to_string(),
        policy_type: as_str(params, "policyType").to_string(),
        description: as_str(params, "description").to_string(),
        rules: params.get("rules").cloned().unwrap_or(json!([])),
        policy_id: id.clone(),
        policy_arn: format!(
            "arn:{}:bedrock:{region}:{account}:automated-reasoning-policy/{id}",
            partition(region)
        ),
        status: "ACTIVE".to_string(),
        version: 1,
        created_at: iso_now(),
        updated_at: iso_now(),
        test_cases: BTreeMap::new(),
        build_workflows: BTreeMap::new(),
    };
    p.policy_id = id.clone();
    let arn = p.policy_arn.clone();
    let dict = p.to_dict();
    st.automated_reasoning_policies.insert(arn.clone(), p);
    if let Some(tags) = params.get("tags").and_then(Value::as_array) {
        if !tags.is_empty() {
            tag_resource(st, &arn, Some(tags));
        }
    }
    ok(dict)
}

fn find_ar_policy<'a>(
    st: &'a BedrockStore,
    arn: &str,
) -> Result<&'a AutomatedReasoningPolicy, Response> {
    st.automated_reasoning_policies
        .get(arn)
        .ok_or_else(|| not_found(&format!("Could not find automated reasoning policy {arn}")))
}

fn get_ar_policy(st: &BedrockStore, arn: &str) -> Response {
    match find_ar_policy(st, arn) {
        Ok(p) => ok(p.to_dict()),
        Err(r) => r,
    }
}

fn update_ar_policy(st: &mut BedrockStore, arn: &str, params: &Value) -> Response {
    let p = match st.automated_reasoning_policies.get_mut(arn) {
        Some(p) => p,
        None => return not_found(&format!("Could not find automated reasoning policy {arn}")),
    };
    if let Some(n) = str_opt(params.get("name")) {
        p.name = n.to_string();
    }
    if let Some(d) = params.get("description").and_then(Value::as_str) {
        p.description = d.to_string();
    }
    if let Some(rules) = params.get("rules").filter(|r| !r.is_null()) {
        p.rules = rules.clone();
    }
    p.updated_at = iso_now();
    ok(p.to_dict())
}

fn delete_ar_policy(st: &mut BedrockStore, arn: &str) -> Response {
    if st.automated_reasoning_policies.remove(arn).is_none() {
        return not_found(&format!("Could not find automated reasoning policy {arn}"));
    }
    ok_empty()
}

fn list_ar_policies(st: &BedrockStore, params: &Value) -> Response {
    let out: Vec<Value> = st
        .automated_reasoning_policies
        .values()
        .map(|p| p.summary())
        .collect();
    let (page, next) = paginate(
        out,
        int_query(params, "maxResults"),
        params.get("nextToken"),
        |s| {
            s.get("policyArn")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        },
    );
    ok(json!({"policies": page, "nextToken": next}))
}

fn create_ar_policy_version(st: &mut BedrockStore, arn: &str) -> Response {
    match st.automated_reasoning_policies.get_mut(arn) {
        Some(p) => {
            p.version += 1;
            p.updated_at = iso_now();
            ok(json!({"policyArn": p.policy_arn, "version": p.version, "status": p.status}))
        }
        None => not_found(&format!("Could not find automated reasoning policy {arn}")),
    }
}

fn export_ar_policy_version(st: &BedrockStore, arn: &str) -> Response {
    match find_ar_policy(st, arn) {
        Ok(p) => ok(json!({"policyArn": p.policy_arn, "version": p.version, "rules": p.rules})),
        Err(r) => r,
    }
}

fn create_ar_test_case(st: &mut BedrockStore, arn: &str, params: &Value) -> Response {
    match st.automated_reasoning_policies.get_mut(arn) {
        Some(p) => {
            let id = short_id();
            let tc = json!({
                "testCaseId": id.clone(),
                "policyArn": arn,
                "name": params
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or(&format!("test-{id}")),
                "description": params.get("description").and_then(Value::as_str).unwrap_or(""),
                "inputText": params.get("inputText").and_then(Value::as_str).unwrap_or(""),
                "expectedResult": params.get("expectedResult").and_then(Value::as_str).unwrap_or(""),
                "createdAt": iso_now(),
            });
            p.test_cases.insert(id, tc.clone());
            ok(tc)
        }
        None => not_found(&format!("Could not find automated reasoning policy {arn}")),
    }
}

fn get_ar_test_case(st: &BedrockStore, arn: &str, case_id: &str) -> Response {
    match st.automated_reasoning_policies.get(arn) {
        Some(p) => match p.test_cases.get(case_id) {
            Some(tc) => ok(tc.clone()),
            None => not_found(&format!("Test case {case_id} not found")),
        },
        None => not_found(&format!("Could not find automated reasoning policy {arn}")),
    }
}

fn update_ar_test_case(
    st: &mut BedrockStore,
    arn: &str,
    case_id: &str,
    params: &Value,
) -> Response {
    match st.automated_reasoning_policies.get_mut(arn) {
        Some(p) => match p.test_cases.get_mut(case_id) {
            Some(tc) => {
                if let Some(n) = params.get("name").and_then(Value::as_str) {
                    tc["name"] = json!(n);
                }
                if let Some(d) = params.get("description") {
                    tc["description"] = d.clone();
                }
                if let Some(t) = params.get("inputText") {
                    tc["inputText"] = t.clone();
                }
                if let Some(r) = params.get("expectedResult") {
                    tc["expectedResult"] = r.clone();
                }
                ok(tc.clone())
            }
            None => not_found(&format!("Test case {case_id} not found")),
        },
        None => not_found(&format!("Could not find automated reasoning policy {arn}")),
    }
}

fn delete_ar_test_case(st: &mut BedrockStore, arn: &str, case_id: &str) -> Response {
    match st.automated_reasoning_policies.get_mut(arn) {
        Some(p) => {
            if p.test_cases.remove(case_id).is_none() {
                return not_found(&format!("Test case {case_id} not found"));
            }
            ok_empty()
        }
        None => not_found(&format!("Could not find automated reasoning policy {arn}")),
    }
}

fn list_ar_test_cases(st: &BedrockStore, arn: &str) -> Response {
    match st.automated_reasoning_policies.get(arn) {
        Some(p) => ok(json!({"testCases": p.test_cases.values().cloned().collect::<Vec<Value>>()})),
        None => not_found(&format!("Could not find automated reasoning policy {arn}")),
    }
}

fn start_ar_build_workflow(st: &mut BedrockStore, arn: &str, workflow_type: &str) -> Response {
    match st.automated_reasoning_policies.get_mut(arn) {
        Some(p) => {
            let id = short_id();
            let wf = json!({
                "buildWorkflowId": id.clone(),
                "policyArn": arn,
                "type": workflow_type,
                "status": "InProgress",
                "createdAt": iso_now(),
            });
            p.build_workflows.insert(id, wf.clone());
            ok(wf)
        }
        None => not_found(&format!("Could not find automated reasoning policy {arn}")),
    }
}

fn get_ar_build_workflow(st: &BedrockStore, arn: &str, wf_id: &str) -> Response {
    match st.automated_reasoning_policies.get(arn) {
        Some(p) => match p.build_workflows.get(wf_id) {
            Some(wf) => ok(wf.clone()),
            None => not_found(&format!("Build workflow {wf_id} not found")),
        },
        None => not_found(&format!("Could not find automated reasoning policy {arn}")),
    }
}

fn cancel_ar_build_workflow(st: &mut BedrockStore, arn: &str, wf_id: &str) -> Response {
    match st.automated_reasoning_policies.get_mut(arn) {
        Some(p) => match p.build_workflows.get_mut(wf_id) {
            Some(wf) => {
                wf["status"] = json!("Cancelled");
                ok_empty()
            }
            None => not_found(&format!("Build workflow {wf_id} not found")),
        },
        None => not_found(&format!("Could not find automated reasoning policy {arn}")),
    }
}

fn delete_ar_build_workflow(st: &mut BedrockStore, arn: &str, wf_id: &str) -> Response {
    match st.automated_reasoning_policies.get_mut(arn) {
        Some(p) => {
            if p.build_workflows.remove(wf_id).is_none() {
                return not_found(&format!("Build workflow {wf_id} not found"));
            }
            ok_empty()
        }
        None => not_found(&format!("Could not find automated reasoning policy {arn}")),
    }
}

fn list_ar_build_workflows(st: &BedrockStore, arn: &str) -> Response {
    match st.automated_reasoning_policies.get(arn) {
        Some(p) => ok(json!({
            "buildWorkflows": p.build_workflows.values().cloned().collect::<Vec<Value>>(),
        })),
        None => not_found(&format!("Could not find automated reasoning policy {arn}")),
    }
}

fn get_ar_result_assets(st: &BedrockStore, arn: &str, wf_id: &str) -> Response {
    match st.automated_reasoning_policies.get(arn) {
        Some(p) => {
            if p.build_workflows.contains_key(wf_id) {
                return not_found(&format!("Build workflow {wf_id} not found"));
            }
            ok(json!({"assets": []}))
        }
        None => not_found(&format!("Could not find automated reasoning policy {arn}")),
    }
}

fn get_ar_annotations(st: &BedrockStore, arn: &str, wf_id: &str) -> Response {
    match st.automated_reasoning_policies.get(arn) {
        Some(p) => {
            if p.build_workflows.contains_key(wf_id) {
                return not_found(&format!("Build workflow {wf_id} not found"));
            }
            ok(json!({"annotations": []}))
        }
        None => not_found(&format!("Could not find automated reasoning policy {arn}")),
    }
}

fn update_ar_annotations(
    st: &mut BedrockStore,
    arn: &str,
    wf_id: &str,
    params: &Value,
) -> Response {
    match st.automated_reasoning_policies.get_mut(arn) {
        Some(p) => {
            if p.build_workflows.contains_key(wf_id) {
                return not_found(&format!("Build workflow {wf_id} not found"));
            }
            ok(json!({
                "annotations": params.get("annotations").cloned().unwrap_or(json!([])),
            }))
        }
        None => not_found(&format!("Could not find automated reasoning policy {arn}")),
    }
}

fn get_ar_next_scenario(st: &BedrockStore, arn: &str, wf_id: &str) -> Response {
    match st.automated_reasoning_policies.get(arn) {
        Some(p) => {
            if p.build_workflows.contains_key(wf_id) {
                return not_found(&format!("Build workflow {wf_id} not found"));
            }
            ok(json!({"scenario": Value::Null}))
        }
        None => not_found(&format!("Could not find automated reasoning policy {arn}")),
    }
}

fn get_ar_test_result(st: &BedrockStore, arn: &str, wf_id: &str, case_id: &str) -> Response {
    match st.automated_reasoning_policies.get(arn) {
        Some(p) => {
            if p.build_workflows.contains_key(wf_id) {
                return not_found(&format!("Build workflow {wf_id} not found"));
            }
            ok(json!({
                "testCaseId": case_id,
                "buildWorkflowId": wf_id,
                "status": "Passed",
            }))
        }
        None => not_found(&format!("Could not find automated reasoning policy {arn}")),
    }
}

fn list_ar_test_results(st: &BedrockStore, arn: &str, wf_id: &str) -> Response {
    match st.automated_reasoning_policies.get(arn) {
        Some(p) => {
            if p.build_workflows.contains_key(wf_id) {
                return not_found(&format!("Build workflow {wf_id} not found"));
            }
            ok(json!({"testResults": []}))
        }
        None => not_found(&format!("Could not find automated reasoning policy {arn}")),
    }
}

fn start_ar_test_workflow(st: &mut BedrockStore, arn: &str, wf_id: &str) -> Response {
    match st.automated_reasoning_policies.get_mut(arn) {
        Some(p) => {
            if p.build_workflows.contains_key(wf_id) {
                return not_found(&format!("Build workflow {wf_id} not found"));
            }
            ok(json!({
                "policyArn": arn,
                "buildWorkflowId": wf_id,
                "status": "InProgress",
            }))
        }
        None => not_found(&format!("Could not find automated reasoning policy {arn}")),
    }
}

// ---------------------------------------------------------------------------
// route
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
fn route(req: &Request, op: &str, params: &Value, st: &mut BedrockStore) -> Response {
    let path = req.path.as_str();
    let account = req.account;
    let region = req.region.as_str();
    match op {
        // --- model customization jobs --------------------------------------
        "CreateModelCustomizationJob" => create_customization_job(st, account, region, params),
        "GetModelCustomizationJob" => get_customization_job(st, &path_arg(path, 0)),
        "StopModelCustomizationJob" => stop_customization_job(st, &path_arg(path, 1)),
        "ListModelCustomizationJobs" => list_customization_jobs(st, params),
        // --- logging configuration ------------------------------------------
        "GetModelInvocationLoggingConfiguration" => ok(json!({
            "loggingConfig": st.logging_config.clone(),
        })),
        "PutModelInvocationLoggingConfiguration" => {
            st.logging_config = params.get("loggingConfig").cloned().unwrap_or(Value::Null);
            ok_empty()
        }
        "DeleteModelInvocationLoggingConfiguration" => {
            st.logging_config = json!({});
            ok_empty()
        }
        // --- custom models ----------------------------------------------------
        "CreateCustomModel" => create_custom_model(st, account, region, params),
        "GetCustomModel" => get_custom_model(st, &path_arg(path, 0)),
        "DeleteCustomModel" => delete_custom_model(st, &path_raw(path, 0)),
        "ListCustomModels" => list_custom_models(st, params),
        // --- tagging ------------------------------------------------------------
        "TagResource" => tag_resource_op(st, params),
        "UntagResource" => untag_resource_op(st, params),
        "ListTagsForResource" => list_tags_op(st, params),
        // --- guardrails ------------------------------------------------------------
        "CreateGuardrail" => create_guardrail(st, account, region, params),
        "GetGuardrail" => get_guardrail(
            st,
            &path_arg(path, 0),
            params.get("guardrailVersion").and_then(Value::as_str),
        ),
        "UpdateGuardrail" => update_guardrail(st, &path_arg(path, 0), params),
        "DeleteGuardrail" => delete_guardrail(
            st,
            &path_arg(path, 0),
            params.get("guardrailVersion").and_then(Value::as_str),
        ),
        "ListGuardrails" => list_guardrails(st, params),
        "CreateGuardrailVersion" => create_guardrail_version(st, &path_arg(path, 0), params),
        // --- provisioned model throughput -------------------------------------------
        "CreateProvisionedModelThroughput" => {
            create_provisioned_model_throughput(st, account, region, params)
        }
        "GetProvisionedModelThroughput" => get_provisioned_model_throughput(st, &path_arg(path, 0)),
        "UpdateProvisionedModelThroughput" => {
            update_provisioned_model_throughput(st, region, &path_arg(path, 0), params)
        }
        "DeleteProvisionedModelThroughput" => {
            delete_provisioned_model_throughput(st, &path_arg(path, 0))
        }
        "ListProvisionedModelThroughputs" => list_provisioned_model_throughputs(st, params),
        // --- evaluation jobs ----------------------------------------------------------
        "CreateEvaluationJob" => create_evaluation_job(st, account, region, params),
        "GetEvaluationJob" => get_evaluation_job(st, &path_arg(path, 0)),
        "StopEvaluationJob" => stop_evaluation_job(st, &path_arg(path, 1)),
        "BatchDeleteEvaluationJob" => batch_delete_evaluation_job(st, params),
        "ListEvaluationJobs" => list_evaluation_jobs(st, params),
        // --- inference profiles --------------------------------------------------------
        "CreateInferenceProfile" => create_inference_profile(st, account, region, params),
        "GetInferenceProfile" => get_inference_profile(st, &path_arg(path, 0)),
        "DeleteInferenceProfile" => delete_inference_profile(st, &path_arg(path, 0)),
        "ListInferenceProfiles" => list_inference_profiles(st, params),
        // --- model import jobs / imported models ----------------------------------------
        "CreateModelImportJob" => create_model_import_job(st, account, region, params),
        "GetModelImportJob" => get_model_import_job(st, &path_arg(path, 0)),
        "ListModelImportJobs" => list_model_import_jobs(st, params),
        "GetImportedModel" => get_imported_model(st, &path_arg(path, 0)),
        "DeleteImportedModel" => delete_imported_model(st, &path_arg(path, 0)),
        "ListImportedModels" => list_imported_models(st, params),
        // --- model copy jobs ------------------------------------------------------------
        "CreateModelCopyJob" => create_model_copy_job(st, account, region, params),
        "GetModelCopyJob" => get_model_copy_job(st, &path_arg(path, 0)),
        "ListModelCopyJobs" => list_model_copy_jobs(st, params),
        // --- model invocation jobs ------------------------------------------------------
        "CreateModelInvocationJob" => create_model_invocation_job(st, account, region, params),
        "GetModelInvocationJob" => get_model_invocation_job(st, &path_arg(path, 0)),
        "StopModelInvocationJob" => stop_model_invocation_job(st, &path_arg(path, 1)),
        "ListModelInvocationJobs" => list_model_invocation_jobs(st, params),
        // --- foundation models -----------------------------------------------------------
        "GetFoundationModel" => get_foundation_model(region, &path_arg(path, 0)),
        "ListFoundationModels" => list_foundation_models(region, params),
        "GetFoundationModelAvailability" => ok(json!({
            "modelId": path_arg(path, 0),
            "agreementAvailability": {"status": "AVAILABLE"},
        })),
        // --- marketplace model endpoints ---------------------------------------------------
        "CreateMarketplaceModelEndpoint" => {
            create_marketplace_endpoint(st, account, region, params)
        }
        "GetMarketplaceModelEndpoint" => get_marketplace_endpoint(st, &path_arg(path, 0)),
        "UpdateMarketplaceModelEndpoint" => {
            update_marketplace_endpoint(st, &path_arg(path, 0), params)
        }
        "DeleteMarketplaceModelEndpoint" => delete_marketplace_endpoint(st, &path_arg(path, 0)),
        "RegisterMarketplaceModelEndpoint" => {
            register_marketplace_endpoint(st, &path_arg(path, 1), params)
        }
        "DeregisterMarketplaceModelEndpoint" => {
            deregister_marketplace_endpoint(st, &path_arg(path, 1))
        }
        "ListMarketplaceModelEndpoints" => list_marketplace_endpoints(st, params),
        // --- prompt routers -----------------------------------------------------------------
        "CreatePromptRouter" => create_prompt_router(st, account, region, params),
        "GetPromptRouter" => get_prompt_router(st, &path_arg(path, 0)),
        "DeletePromptRouter" => delete_prompt_router(st, &path_arg(path, 0)),
        "ListPromptRouters" => list_prompt_routers(st, params),
        // --- custom model deployments ---------------------------------------------------------
        "CreateCustomModelDeployment" => {
            create_custom_model_deployment(st, account, region, params)
        }
        "GetCustomModelDeployment" => get_custom_model_deployment(st, &path_arg(path, 0)),
        "UpdateCustomModelDeployment" => update_custom_model_deployment(st, &path_arg(path, 0)),
        "DeleteCustomModelDeployment" => delete_custom_model_deployment(st, &path_arg(path, 0)),
        "ListCustomModelDeployments" => list_custom_model_deployments(st, params),
        // --- foundation model agreements -------------------------------------------------------
        "CreateFoundationModelAgreement" => ok(json!({
            "modelId": as_str(params, "modelId"),
            "offerToken": as_str(params, "offerToken"),
        })),
        "DeleteFoundationModelAgreement" => ok(json!({"modelId": as_str(params, "modelId")})),
        "ListFoundationModelAgreementOffers" => {
            ok(json!({"modelId": path_arg(path, 0), "offers": []}))
        }
        // --- enforced guardrails configuration -------------------------------------------------
        "PutEnforcedGuardrailConfiguration" => {
            // moto reads the flat body keys; the botocore input nests them
            // under guardrailInferenceConfig, so also descend one level.
            let body = params;
            let guardrail_identifier = body
                .get("guardrailIdentifier")
                .and_then(Value::as_str)
                .map(String::from)
                .unwrap_or_else(|| {
                    body.get("guardrailInferenceConfig")
                        .and_then(|c| c.get("guardrailIdentifier"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string()
                });
            let guardrail_version = body
                .get("guardrailVersion")
                .and_then(Value::as_str)
                .map(String::from)
                .unwrap_or_else(|| {
                    body.get("guardrailInferenceConfig")
                        .and_then(|c| c.get("guardrailVersion"))
                        .and_then(Value::as_str)
                        .unwrap_or("DRAFT")
                        .to_string()
                })
                .to_string();
            st.enforced_guardrail_configs.insert(
                "default".to_string(),
                json!({
                    "configId": "default",
                    "guardrailIdentifier": guardrail_identifier,
                    "guardrailVersion": guardrail_version,
                }),
            );
            let entry = st
                .enforced_guardrail_configs
                .get("default")
                .cloned()
                .unwrap_or(json!({}));
            ok(entry)
        }
        "DeleteEnforcedGuardrailConfiguration" => {
            st.enforced_guardrail_configs.remove(&path_arg(path, 0));
            ok_empty()
        }
        "ListEnforcedGuardrailsConfiguration" => ok(json!({
            "configurations": st.enforced_guardrail_configs.values().cloned().collect::<Vec<Value>>(),
        })),
        // --- use case for model access ----------------------------------------------------------
        "GetUseCaseForModelAccess" => ok(st
            .use_case_for_model_access
            .clone()
            .unwrap_or_else(|| json!({}))),
        "PutUseCaseForModelAccess" => {
            let use_cases = params
                .get("useCases")
                .cloned()
                .filter(|u| !u.is_null())
                .unwrap_or_else(|| json!([]));
            st.use_case_for_model_access = Some(json!({ "useCases": use_cases }));
            ok(json!({ "useCases": use_cases }))
        }
        // --- automated reasoning policies -------------------------------------------------------
        "CreateAutomatedReasoningPolicy" => create_ar_policy(st, account, region, params),
        "GetAutomatedReasoningPolicy" => get_ar_policy(st, &path_arg(path, 0)),
        "UpdateAutomatedReasoningPolicy" => update_ar_policy(st, &path_arg(path, 0), params),
        "DeleteAutomatedReasoningPolicy" => delete_ar_policy(st, &path_arg(path, 0)),
        "ListAutomatedReasoningPolicies" => list_ar_policies(st, params),
        "CreateAutomatedReasoningPolicyVersion" => create_ar_policy_version(st, &path_arg(path, 1)),
        "ExportAutomatedReasoningPolicyVersion" => export_ar_policy_version(st, &path_arg(path, 1)),
        "CreateAutomatedReasoningPolicyTestCase" => {
            create_ar_test_case(st, &path_arg(path, 1), params)
        }
        "GetAutomatedReasoningPolicyTestCase" => {
            get_ar_test_case(st, &path_arg(path, 2), &path_arg(path, 0))
        }
        "UpdateAutomatedReasoningPolicyTestCase" => {
            update_ar_test_case(st, &path_arg(path, 2), &path_arg(path, 0), params)
        }
        "DeleteAutomatedReasoningPolicyTestCase" => {
            delete_ar_test_case(st, &path_arg(path, 2), &path_arg(path, 0))
        }
        "ListAutomatedReasoningPolicyTestCases" => list_ar_test_cases(st, &path_arg(path, 1)),
        "StartAutomatedReasoningPolicyBuildWorkflow" => {
            start_ar_build_workflow(st, &path_arg(path, 3), &path_arg(path, 1))
        }
        "GetAutomatedReasoningPolicyBuildWorkflow" => {
            get_ar_build_workflow(st, &path_arg(path, 2), &path_arg(path, 0))
        }
        "CancelAutomatedReasoningPolicyBuildWorkflow" => {
            cancel_ar_build_workflow(st, &path_arg(path, 3), &path_arg(path, 1))
        }
        "DeleteAutomatedReasoningPolicyBuildWorkflow" => {
            delete_ar_build_workflow(st, &path_arg(path, 2), &path_arg(path, 0))
        }
        "ListAutomatedReasoningPolicyBuildWorkflows" => {
            list_ar_build_workflows(st, &path_arg(path, 1))
        }
        "GetAutomatedReasoningPolicyBuildWorkflowResultAssets" => {
            get_ar_result_assets(st, &path_arg(path, 2), &path_arg(path, 0))
        }
        "GetAutomatedReasoningPolicyAnnotations" => {
            get_ar_annotations(st, &path_arg(path, 2), &path_arg(path, 0))
        }
        "UpdateAutomatedReasoningPolicyAnnotations" => {
            update_ar_annotations(st, &path_arg(path, 2), &path_arg(path, 0), params)
        }
        "GetAutomatedReasoningPolicyNextScenario" => {
            get_ar_next_scenario(st, &path_arg(path, 2), &path_arg(path, 0))
        }
        "GetAutomatedReasoningPolicyTestResult" => get_ar_test_result(
            st,
            &path_arg(path, 4),
            &path_arg(path, 2),
            &path_arg(path, 0),
        ),
        "ListAutomatedReasoningPolicyTestResults" => {
            list_ar_test_results(st, &path_arg(path, 2), &path_arg(path, 0))
        }
        "StartAutomatedReasoningPolicyTestWorkflow" => {
            start_ar_test_workflow(st, &path_arg(path, 2), &path_arg(path, 0))
        }
        other => json_error(
            501,
            "NotImplemented",
            &format!("The {other} action has not been implemented"),
        ),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn request(account: u64, method: &str, path: &str, body: Value) -> Request {
        Request {
            service: "bedrock".to_string(),
            operation: String::new(),
            method: method.to_string(),
            path: path.to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: serde_json::to_vec(&body)
                .map(Bytes::from)
                .unwrap_or_default(),
            account,
            region: "us-east-1".to_string(),
        }
    }

    fn next_account() -> u64 {
        static COUNTER: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(7_000_000_000_001);
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    fn send_req(req: &Request) -> (u16, Value) {
        let resp = dispatch(req).expect("bedrock dispatch answered");
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(Value::Null),
        )
    }

    fn unique(prefix: &str) -> String {
        format!("{prefix}-{}", Uuid::new_v4().simple())
    }

    #[test]
    fn customization_job_arn_get_and_stop() {
        let account = next_account();
        let job_name = unique("job");
        let model_name = unique("model");
        let (status, body) = send_req(&m_create_job(account, &job_name, &model_name));
        assert_eq!(status, 200, "{body}");
        let job_arn = body["jobArn"].as_str().unwrap().to_string();
        assert!(job_arn.contains("model-customization-job"));

        // by name
        let (status, body) = send_req(&request(
            account,
            "GET",
            &format!("/model-customization-jobs/{job_name}"),
            json!({}),
        ));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["jobName"], json!(job_name));
        assert_eq!(body["status"], "InProgress");

        // by full ARN (encoded by botocore)
        let encoded = job_arn.replace(':', "%3A").replace('/', "%2F");
        let (status, body) = send_req(&request(
            account,
            "GET",
            &format!("/model-customization-jobs/{encoded}"),
            json!({}),
        ));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["jobArn"], json!(job_arn));

        // stop by ARN
        let (status, _) = send_req(&request(
            account,
            "POST",
            &format!("/model-customization-jobs/{encoded}/stop"),
            json!({}),
        ));
        assert_eq!(status, 200);
        let (_, body) = send_req(&request(
            account,
            "GET",
            &format!("/model-customization-jobs/{job_name}"),
            json!({}),
        ));
        assert_eq!(body["status"], json!("Stopped"));
    }

    fn m_create_job(account: u64, job_name: &str, model_name: &str) -> Request {
        request(
            account,
            "POST",
            "/model-customization-jobs",
            json!({
                "jobName": job_name,
                "customModelName": model_name,
                "roleArn": "arn:aws:iam::123456789012:role/test",
                "baseModelIdentifier": "amazon.titan-text-express-v1",
                "trainingDataConfig": {"s3Uri": "s3://test-bucket/train.jsonl"},
                "outputDataConfig": {"s3Uri": "s3://test-bucket/output/"},
                "hyperParameters": {"epochCount": "1"},
            }),
        )
    }

    #[test]
    fn duplicate_job_name_rejected() {
        let account = next_account();
        let job_name = unique("dup");
        let (status, _) = send_req(&m_create_job(account, &job_name, &unique("m")));
        assert_eq!(status, 200);
        let (status, body) = send_req(&m_create_job(account, &job_name, &unique("m2")));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], json!("ResourceInUseException"));

        // A duplicate custom model name is refused before URI validation.
        let (status, _) = send_req(&request(
            account,
            "POST",
            "/custom-models/create-custom-model",
            json!({
                "modelName": "seeded-model",
                "modelSourceConfig": {"s3DataSource": {"s3Uri": "s3://bucket/m/"}},
            }),
        ));
        assert_eq!(status, 200);
        let (status, body) = send_req(&request(
            account,
            "POST",
            "/model-customization-jobs",
            json!({
                "jobName": unique("job"),
                "customModelName": "seeded-model",
                "roleArn": "role",
                "baseModelIdentifier": "m",
                "trainingDataConfig": {"s3Uri": "not-s3"},
                "outputDataConfig": {"s3Uri": "s3://t/out"},
            }),
        ));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], json!("ResourceInUseException"));
    }

    #[test]
    fn invalid_s3_uri_rejected() {
        let account = next_account();
        let (status, body) = send_req(&request(
            account,
            "POST",
            "/model-customization-jobs",
            json!({
                "jobName": unique("job"),
                "customModelName": unique("model"),
                "roleArn": "role",
                "baseModelIdentifier": "amazon.titan-text-express-v1",
                "trainingDataConfig": {"s3Uri": "not-s3"},
                "outputDataConfig": {"s3Uri": "s3://t/out"},
            }),
        ));
        assert_eq!(status, 400, "{body}");
        assert_eq!(body["__type"], json!("ValidationException"));
    }

    #[test]
    fn guardrail_lifecycle() {
        let account = next_account();
        let name = unique("guard");
        let (status, body) = send_req(&request(
            account,
            "POST",
            "/guardrails",
            json!({
                "name": name,
                "blockedInputMessaging": "blocked-input",
                "blockedOutputsMessaging": "blocked-output",
            }),
        ));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["version"], json!("DRAFT"));
        let id = body["guardrailId"].as_str().unwrap().to_string();

        let (status, body) = send_req(&request(
            account,
            "GET",
            &format!("/guardrails/{id}"),
            json!({}),
        ));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["name"], json!(name));

        let (status, body) = send_req(&request(
            account,
            "POST",
            &format!("/guardrails/{id}"),
            json!({}),
        ));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["version"], json!("1"));

        let (status, _) = send_req(&request(
            account,
            "DELETE",
            &format!("/guardrails/{id}"),
            json!({}),
        ));
        assert_eq!(status, 200);
        let (status, body) = send_req(&request(
            account,
            "GET",
            &format!("/guardrails/{id}"),
            json!({}),
        ));
        assert_eq!(status, 400, "{body}");
        assert_eq!(body["__type"], json!("ResourceNotFoundException"));
    }

    #[test]
    fn foundation_model_catalog() {
        let account = next_account();
        let (status, body) = send_req(&request(account, "GET", "/foundation-models", json!({})));
        assert_eq!(status, 200, "{body}");
        assert!(!body["modelSummaries"].as_array().unwrap().is_empty());

        let (status, body) = send_req(&request(
            account,
            "GET",
            "/foundation-models/anthropic.claude-v2",
            json!({}),
        ));
        assert_eq!(status, 200, "{body}");
        assert_eq!(
            body["modelDetails"]["modelId"],
            json!("anthropic.claude-v2")
        );

        let (status, _) = send_req(&request(
            account,
            "GET",
            "/foundation-models/nonexistent-model",
            json!({}),
        ));
        assert_eq!(status, 400);
    }

    #[test]
    fn tags_roundtrip() {
        let account = next_account();
        let (_, body) = send_req(&m_create_job(account, &unique("job"), &unique("model")));
        let job_arn = body["jobArn"].as_str().unwrap().to_string();

        let (status, _) = send_req(&request(
            account,
            "POST",
            "/tagResource",
            json!({
                "resourceARN": job_arn,
                "tags": [{"key": "env", "value": "test"}],
            }),
        ));
        assert_eq!(status, 200);
        let (status, body) = send_req(&request(
            account,
            "POST",
            "/listTagsForResource",
            json!({"resourceARN": job_arn}),
        ));
        assert_eq!(status, 200);
        assert_eq!(body["tags"][0]["key"], json!("env"));
        assert_eq!(body["tags"][0]["value"], json!("test"));

        let (status, _) = send_req(&request(
            account,
            "POST",
            "/untagResource",
            json!({"resourceARN": job_arn, "tagKeys": ["env"]}),
        ));
        assert_eq!(status, 200);
        let (status, body) = send_req(&request(
            account,
            "POST",
            "/listTagsForResource",
            json!({"resourceARN": job_arn}),
        ));
        assert_eq!(status, 200);
        assert_eq!(body["tags"], json!([]));

        // unknown resources are refused
        let (status, _) = send_req(&request(
            account,
            "POST",
            "/listTagsForResource",
            json!({"resourceARN": "arn:aws:bedrock:us-east-1:123456789012:custom-model/none"}),
        ));
        assert_eq!(status, 400);
    }

    #[test]
    fn logging_config_lifecycle() {
        let account = next_account();
        let (status, body) = send_req(&request(
            account,
            "PUT",
            "/logging/modelinvocations",
            json!({"loggingConfig": {"textDataDeliveryEnabled": true}}),
        ));
        assert_eq!(status, 200, "{body}");
        let (_, body) = send_req(&request(
            account,
            "GET",
            "/logging/modelinvocations",
            json!({}),
        ));
        assert_eq!(
            body["loggingConfig"]["textDataDeliveryEnabled"],
            json!(true)
        );
        let (status, _) = send_req(&request(
            account,
            "DELETE",
            "/logging/modelinvocations",
            json!({}),
        ));
        assert_eq!(status, 200);
        let (_, body) = send_req(&request(
            account,
            "GET",
            "/logging/modelinvocations",
            json!({}),
        ));
        assert_eq!(body["loggingConfig"], json!({}));
    }

    #[test]
    fn evaluation_job_crud_and_batch_delete() {
        let account = next_account();
        let job_name = unique("eval");
        let (status, body) = send_req(&request(
            account,
            "POST",
            "/evaluation-jobs",
            json!({
                "jobName": job_name,
                "roleArn": "arn:aws:iam::123456789012:role/test",
                "evaluationConfig": {"automated": {}},
                "inferenceConfig": {"models": []},
                "outputDataConfig": {"s3Uri": "s3://bucket/out/"},
            }),
        ));
        assert_eq!(status, 200, "{body}");
        let job_arn = body["jobArn"].as_str().unwrap().to_string();
        assert!(job_arn.contains("evaluation-job"));

        let (status, body) = send_req(&request(
            account,
            "GET",
            &format!("/evaluation-jobs/{job_name}"),
            json!({}),
        ));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["jobName"], json!(job_name));

        let (status, body) = send_req(&request(
            account,
            "POST",
            "/evaluation-jobs/batch-delete",
            json!({"jobIdentifiers": [job_arn]}),
        ));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["errors"], json!([]));
        assert_eq!(body["evaluationJobs"].as_array().unwrap().len(), 1);

        // after deletion the job is gone
        let (status, _) = send_req(&request(
            account,
            "GET",
            &format!("/evaluation-jobs/{job_name}"),
            json!({}),
        ));
        assert_eq!(status, 400);
    }
}
