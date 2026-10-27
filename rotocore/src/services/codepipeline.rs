//! AWS CodePipeline backend — port of moto's `codepipeline` engine.
//!
//! JSON-protocol service: ops arrive through the X-Amz-Target suffix and
//! bodies keep the spec's lowercase-first member names ("pipeline", "roleArn")
//! exactly like moto sees them. Executions, action types, webhooks and jobs
//! keep moto's statuses/ids. CreatePipeline validates the assume-role trust
//! against the IAM backend; moto reads its in-process IAM store, so this port
//! swaps in the native rotocore route — a loopback GetRole through the
//! gateway (the mirror.rs callback used by the CFN backend) into the native
//! IAM crate's role store.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::HashMap;

static SLOT: Lazy<store::Slot<CodePipelineState>> =
    Lazy::new(|| store::slot::<CodePipelineState>("codepipeline"));

#[derive(Default)]
struct CodePipelineState {
    pipelines: HashMap<String, Pipeline>,
    executions: HashMap<String, Vec<Execution>>,
    custom_action_types: HashMap<String, Value>,
    webhooks: HashMap<String, Value>,
    jobs: HashMap<String, Value>, // job_id -> job record
    third_party_jobs: HashMap<String, Value>,
    region: String,
    account: String,
}

struct Pipeline {
    /// The pipeline definition body, semantically owned (version bumped).
    pipeline: Value,
    tags: HashMap<String, String>,
    arn: String,
    created: String,
    updated: String,
    stage_transitions: HashMap<String, bool>,
}

struct Execution {
    pipeline_name: String,
    pipeline_version: i64,
    pipeline_execution_id: String,
    status: String,
    status_summary: Option<String>,
    trigger: Value,
    source_revisions: Value,
    start_time: String,
    last_update_time: String,
}

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn iso_stamp(stamp: &str) -> String {
    stamp.to_string()
}

fn pipeline_not_found(account: &str, name: &str) -> Response {
    crate::proto::json_error(
        404,
        "PipelineNotFoundException",
        &format!("Account '{account}' does not have a pipeline with name '{name}'"),
    )
}

fn resource_not_found(account: &str, name: &str) -> Response {
    crate::proto::json_error(
        404,
        "ResourceNotFoundException",
        &format!(
            "The account with id '{account}' does not include a pipeline with the name '{name}'"
        ),
    )
}

fn webhook_not_found(name: &str) -> Response {
    crate::proto::json_error(
        400,
        "WebhookNotFoundException",
        &format!("Webhook with name '{name}' does not exist"),
    )
}

fn job_not_found(job_id: &str) -> Response {
    crate::proto::json_error(
        400,
        "JobNotFoundException",
        &format!("Job with id '{job_id}' not found"),
    )
}

fn invalid_structure(message: &str) -> Response {
    crate::proto::json_error(400, "InvalidStructureException", message)
}

fn stage_not_found(stage: &str, pipeline: &str) -> Response {
    crate::proto::json_error(
        400,
        "StageNotFoundException",
        &format!("Stage '{stage}' does not exist in pipeline '{pipeline}'"),
    )
}

fn execution_not_found(id: &str, pipeline: &str) -> Response {
    crate::proto::json_error(
        404,
        "PipelineExecutionNotFoundException",
        &format!("Pipeline execution with ID '{id}' does not exist for pipeline '{pipeline}'"),
    )
}

impl Execution {
    fn to_dict(&self) -> Value {
        let mut result = json!({
            "pipelineExecutionId": self.pipeline_execution_id,
            "pipelineName": self.pipeline_name,
            "pipelineVersion": self.pipeline_version,
            "status": self.status,
            "artifactRevisions": self.source_revisions,
        });
        if let Some(summary) = &self.status_summary {
            result["statusSummary"] = json!(summary);
        }
        result["trigger"] = self.trigger.clone();
        result
    }

    fn to_summary(&self) -> Value {
        let mut result = json!({
            "pipelineExecutionId": self.pipeline_execution_id,
            "status": self.status,
            "startTime": self.start_time,
            "lastUpdateTime": self.last_update_time,
            "sourceRevisions": self.source_revisions,
            "trigger": self.trigger,
        });
        if let Some(summary) = &self.status_summary {
            result["statusSummary"] = json!(summary);
        }
        result
    }
}

fn param_str(params: &Value, key: &str) -> String {
    params
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

pub fn dispatch(req: &Request) -> Response {
    let params = crate::proto::parse_json_body(&req.body);
    let mut guard = SLOT.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    if st.region.is_empty() {
        st.region = req.region.clone();
        st.account = req.account.to_string();
    }
    match req.operation.as_str() {
        "CreatePipeline" => st.create_pipeline(&params),
        "GetPipeline" => st.get_pipeline(&params),
        "UpdatePipeline" => st.update_pipeline(&params),
        "DeletePipeline" => st.delete_pipeline(&params),
        "ListPipelines" => st.list_pipelines(),
        "ListTagsForResource" => st.list_tags_for_resource(&params),
        "TagResource" => st.tag_resource(&params),
        "UntagResource" => st.untag_resource(&params),
        "StartPipelineExecution" => st.start_pipeline_execution(&params),
        "StopPipelineExecution" => st.stop_pipeline_execution(&params),
        "GetPipelineExecution" => st.get_pipeline_execution(&params),
        "ListPipelineExecutions" => st.list_pipeline_executions(&params),
        "GetPipelineState" => st.get_pipeline_state(&params),
        "DisableStageTransition" => st.set_stage_transition(&params, false),
        "EnableStageTransition" => st.set_stage_transition(&params, true),
        "RetryStageExecution" => st.retry_stage_execution(&params),
        "RollbackStage" => st.rollback_stage(&params),
        "OverrideStageCondition" => st.override_stage_condition(&params),
        "ListActionExecutions" => st.list_action_executions(&params),
        "ListRuleExecutions" => st.list_rule_executions(&params),
        "ListDeployActionExecutionTargets" => st.list_deploy_action_execution_targets(&params),
        "CreateCustomActionType" => st.create_custom_action_type(&params),
        "DeleteCustomActionType" => st.delete_custom_action_type(&params),
        "ListActionTypes" => st.list_action_types(&params),
        "GetActionType" => st.get_action_type(&params),
        "UpdateActionType" => st.update_action_type(&params),
        "PutWebhook" => st.put_webhook(&params),
        "DeleteWebhook" => st.delete_webhook(&params),
        "ListWebhooks" => st.list_webhooks(),
        "RegisterWebhookWithThirdParty" => st.set_webhook_registration(&params, true),
        "DeregisterWebhookWithThirdParty" => st.set_webhook_registration(&params, false),
        "PollForJobs" => st.poll_for_jobs(),
        "PollForThirdPartyJobs" => st.poll_for_third_party_jobs(),
        "GetJobDetails" => st.get_job_details(&params),
        "GetThirdPartyJobDetails" => st.get_job_details(&params),
        "AcknowledgeJob" => st.acknowledge_job(&params),
        "AcknowledgeThirdPartyJob" => st.acknowledge_job(&params),
        "PutJobSuccessResult" => st.put_job_result(&params, "Succeeded"),
        "PutJobFailureResult" => st.put_job_result(&params, "Failed"),
        "PutThirdPartyJobSuccessResult" => st.put_job_result(&params, "Succeeded"),
        "PutThirdPartyJobFailureResult" => st.put_job_result(&params, "Failed"),
        "PutApprovalResult" => st.put_approval_result(&params),
        "ListRuleTypes" => Response::json_status(200, &json!({"ruleTypes": []})),
        "PutActionRevision" => st.put_action_revision(&params),
        _ => crate::proto::json_error(
            501,
            "NotImplemented",
            &format!("The {} action has not been implemented", req.operation),
        ),
    }
}

impl CodePipelineState {
    fn pipeline_mut(&mut self, name: &str) -> Option<&mut Pipeline> {
        self.pipelines.get_mut(name)
    }

    /// Route to pipeline/ARN resolution: ARNs end with the pipeline name.
    fn tags_lookup_mut(&mut self, arn: &str) -> Option<&mut Pipeline> {
        let name = arn.rsplit(':').next().unwrap_or("");
        self.pipeline_mut(name)
    }

    fn validate_trust_via_iam(&self, role_arn: &str) -> Result<(), Response> {
        // The native IAM crate owns roles: resolve through the gateway
        // loopback (mirror.rs's callback path) instead of the sidecar's
        // mirror-into-moto-IAM dance.
        let role_name = role_arn
            .split(":role/")
            .nth(1)
            .map(|r| r.split('/').next().unwrap_or(r).to_string())
            .unwrap_or_default();
        let call = crate::services::cloudformation::mirror::form_call(
            "iam",
            "GetRole",
            "2010-05-08",
            &[("RoleName", role_name.clone())],
            &self.account,
            &self.region,
        );
        if call.status != 200 {
            return Err(invalid_structure(&format!(
                "CodePipeline is not authorized to perform AssumeRole on role {role_arn}"
            )));
        }
        let doc = extract_between(
            &call.body,
            "<AssumeRolePolicyDocument>",
            "</AssumeRolePolicyDocument>",
        );
        let decoded = crate::proto::url_decode(&doc);
        let policy: Value = serde_json::from_str(&decoded).unwrap_or(json!({}));
        let statements = policy.get("Statement").cloned().unwrap_or(json!([]));
        let statements_owned = match statements {
            Value::Array(v) => v,
            other => vec![other],
        };
        for statement in statements_owned {
            let service = statement
                .get("Principal")
                .and_then(|p| p.get("Service"))
                .and_then(Value::as_str);
            let services = match service {
                Some(single) => vec![single.to_string()],
                None => statement
                    .get("Principal")
                    .and_then(|p| p.get("Service"))
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .map(String::from)
                            .collect()
                    })
                    .unwrap_or_default(),
            };
            if services.iter().any(|s| s == "codepipeline.amazonaws.com") {
                return Ok(());
            }
        }
        Err(invalid_structure(&format!(
            "CodePipeline is not authorized to perform AssumeRole on role {role_arn}"
        )))
    }

    fn add_default_values(pipeline: &mut Value) {
        let stages = &mut pipeline["stages"];
        if let Some(stage_list) = stages.as_array_mut() {
            for stage in stage_list {
                if let Some(actions) = stage.get_mut("actions").and_then(Value::as_array_mut) {
                    for action in actions {
                        if !action.is_object() {
                            continue;
                        }
                        let map = action.as_object_mut().unwrap();
                        map.entry("runOrder".to_string()).or_insert(json!(1));
                        map.entry("configuration".to_string()).or_insert(json!({}));
                        map.entry("outputArtifacts".to_string())
                            .or_insert(json!([]));
                        map.entry("inputArtifacts".to_string()).or_insert(json!([]));
                    }
                }
            }
        }
    }

    fn create_pipeline(&mut self, params: &Value) -> Response {
        let mut pipeline = params.get("pipeline").cloned().unwrap_or(json!({}));
        let name = pipeline
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if self.pipelines.contains_key(&name) {
            return invalid_structure(&format!(
                "A pipeline with the name '{name}' already exists in account '{}'",
                self.account
            ));
        }
        let role_arn = pipeline
            .get("roleArn")
            .and_then(Value::as_str)
            .unwrap_or("");
        if let Err(resp) = self.validate_trust_via_iam(role_arn) {
            return resp;
        }
        let stage_count = pipeline
            .get("stages")
            .and_then(Value::as_array)
            .map(|a| a.len())
            .unwrap_or(0);
        if stage_count < 2 {
            return invalid_structure(
                "Pipeline has only 1 stage(s). There should be a minimum of 2 stages in a pipeline",
            );
        }
        pipeline["version"] = json!(1);
        Self::add_default_values(&mut pipeline);
        let tags_in = params.get("tags").and_then(Value::as_array).cloned();
        let mut tags = HashMap::new();
        if let Some(tag_list) = &tags_in {
            for tag in tag_list {
                if let (Some(k), Some(v)) = (
                    tag.get("key").and_then(Value::as_str),
                    tag.get("value").and_then(Value::as_str),
                ) {
                    tags.insert(k.to_string(), v.to_string());
                }
            }
        }
        let record = Pipeline {
            pipeline: pipeline.clone(),
            tags,
            arn: format!(
                "arn:aws:codepipeline:{region}:{account}:{name}",
                region = self.region,
                account = self.account
            ),
            created: now_iso(),
            updated: now_iso(),
            stage_transitions: pipeline
                .get("stages")
                .and_then(Value::as_array)
                .map(|stages| {
                    stages
                        .iter()
                        .filter_map(|s| s.get("name").and_then(Value::as_str))
                        .map(|n| (n.to_string(), true))
                        .collect()
                })
                .unwrap_or_default(),
        };
        self.pipelines.insert(name.clone(), record);
        let tags_out = sorted_tag_list_from(&self.pipelines[&name].tags);
        Response::json_status(200, &json!({"pipeline": pipeline, "tags": tags_out}))
    }

    fn get_pipeline(&self, params: &Value) -> Response {
        let name = param_str(params, "name");
        let Some(p) = self.pipelines.get(&name) else {
            return pipeline_not_found(&self.account, &name);
        };
        Response::json_status(
            200,
            &json!({
                "pipeline": p.pipeline,
                "metadata": {
                    "pipelineArn": p.arn,
                    "created": p.created,
                    "updated": p.updated,
                },
            }),
        )
    }

    fn update_pipeline(&mut self, params: &Value) -> Response {
        let mut pipeline = params.get("pipeline").cloned().unwrap_or(json!({}));
        let name = pipeline
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if !self.pipelines.contains_key(&name) {
            return resource_not_found(&self.account, &name);
        }
        let existing_version = self.pipelines[&name].pipeline["version"]
            .as_i64()
            .unwrap_or(1);
        pipeline["version"] = json!(existing_version + 1);
        Self::add_default_values(&mut pipeline);
        {
            let record = self.pipeline_mut(&name).unwrap();
            record.pipeline = pipeline.clone();
            record.updated = now_iso();
            for stage in pipeline
                .get("stages")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(stage_name) = stage.get("name").and_then(Value::as_str) {
                    record
                        .stage_transitions
                        .entry(stage_name.to_string())
                        .or_insert(true);
                }
            }
        }
        Response::json_status(200, &json!({"pipeline": pipeline}))
    }

    fn delete_pipeline(&mut self, params: &Value) -> Response {
        let name = param_str(params, "name");
        self.pipelines.remove(&name);
        self.executions.remove(&name);
        Response::json_status(200, &json!({}))
    }

    fn list_pipelines(&self) -> Response {
        let mut pipelines: Vec<Value> = self
            .pipelines
            .values()
            .map(|p| {
                json!({
                    "name": p.pipeline["name"],
                    "version": p.pipeline["version"],
                    "created": iso_stamp(&p.created),
                    "updated": iso_stamp(&p.updated),
                })
            })
            .collect();
        pipelines.sort_by(|a, b| {
            let ka = a["name"].as_str().unwrap_or("");
            let kb = b["name"].as_str().unwrap_or("");
            ka.cmp(kb)
        });
        Response::json_status(200, &json!({"pipelines": pipelines}))
    }

    fn list_tags_for_resource(&self, params: &Value) -> Response {
        let arn = param_str(params, "resourceArn");
        let name = arn.rsplit(':').next().unwrap_or("");
        let Some(p) = self.pipelines.get(name) else {
            return resource_not_found(&self.account, name);
        };
        Response::json_status(200, &json!({"tags": sorted_tag_list_from(&p.tags)}))
    }

    fn tag_resource(&mut self, params: &Value) -> Response {
        let arn = param_str(params, "resourceArn");
        let name = arn.rsplit(':').next().unwrap_or("").to_string();
        if self.tags_lookup_mut(&arn).is_none() {
            return resource_not_found(&self.account, &name);
        }
        if let Some(tags) = params.get("tags").and_then(Value::as_array) {
            let record = self.pipelines.get_mut(&name).unwrap();
            for tag in tags {
                if let (Some(k), Some(v)) = (
                    tag.get("key").and_then(Value::as_str),
                    tag.get("value").and_then(Value::as_str),
                ) {
                    record.tags.insert(k.to_string(), v.to_string());
                }
            }
        }
        Response::json_status(200, &json!({}))
    }

    fn untag_resource(&mut self, params: &Value) -> Response {
        let arn = param_str(params, "resourceArn");
        let name = arn.rsplit(':').next().unwrap_or("").to_string();
        if self.tags_lookup_mut(&arn).is_none() {
            return resource_not_found(&self.account, &name);
        }
        if let Some(keys) = params.get("tagKeys").and_then(Value::as_array) {
            let record = self.pipelines.get_mut(&name).unwrap();
            for key in keys {
                if let Some(k) = key.as_str() {
                    record.tags.remove(k);
                }
            }
        }
        Response::json_status(200, &json!({}))
    }

    // ---- executions ----------------------------------------------------------

    fn start_pipeline_execution(&mut self, params: &Value) -> Response {
        let name = param_str(params, "name");
        let Some(p) = self.pipelines.get(&name) else {
            return pipeline_not_found(&self.account, &name);
        };
        let execution = Execution {
            pipeline_name: name.clone(),
            pipeline_version: p.pipeline["version"].as_i64().unwrap_or(1),
            pipeline_execution_id: uuid::Uuid::new_v4().to_string(),
            status: "InProgress".to_string(),
            status_summary: None,
            trigger: json!({"triggerType": "StartPipelineExecution", "triggerDetail": ""}),
            source_revisions: json!([]),
            start_time: now_iso(),
            last_update_time: now_iso(),
        };
        let id = execution.pipeline_execution_id.clone();
        self.executions
            .entry(name)
            .or_default()
            .insert(0, execution);
        Response::json_status(200, &json!({"pipelineExecutionId": id}))
    }

    fn stop_pipeline_execution(&mut self, params: &Value) -> Response {
        let pipeline_name = param_str(params, "pipelineName");
        let execution_id = param_str(params, "pipelineExecutionId");
        let reason = param_str(params, "reason");
        if self.pipeline_mut(&pipeline_name).is_none() {
            return pipeline_not_found(&self.account, &pipeline_name);
        }
        let Some(executions) = self.executions.get_mut(&pipeline_name) else {
            return execution_not_found(&execution_id, &pipeline_name);
        };
        let Some(execution) = executions
            .iter_mut()
            .find(|e| e.pipeline_execution_id == execution_id)
        else {
            return execution_not_found(&execution_id, &pipeline_name);
        };
        if execution.status != "InProgress" {
            return crate::proto::json_error(
                409,
                "ConflictException",
                &format!(
                    "Pipeline execution '{execution_id}' is not in a state that can be stopped"
                ),
            );
        }
        let abandon = params
            .get("abandon")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        execution.status = if abandon {
            "Abandoned".to_string()
        } else {
            "Stopping".to_string()
        };
        if !reason.is_empty() {
            execution.status_summary = Some(reason);
        }
        execution.last_update_time = now_iso();
        Response::json_status(200, &json!({"pipelineExecutionId": execution_id}))
    }

    fn get_pipeline_execution(&self, params: &Value) -> Response {
        let pipeline_name = param_str(params, "pipelineName");
        let execution_id = param_str(params, "pipelineExecutionId");
        let Some(p) = self.pipelines.get(&pipeline_name) else {
            return pipeline_not_found(&self.account, &pipeline_name);
        };
        let executions = self.executions.get(&pipeline_name);
        let Some(execution) = executions.and_then(|list| {
            list.iter()
                .find(|e| e.pipeline_execution_id == execution_id)
        }) else {
            return execution_not_found(&execution_id, &pipeline_name);
        };
        let _ = p;
        Response::json_status(200, &json!({"pipelineExecution": execution.to_dict()}))
    }

    fn list_pipeline_executions(&self, params: &Value) -> Response {
        let pipeline_name = param_str(params, "pipelineName");
        let Some(_) = self.pipelines.get(&pipeline_name) else {
            return pipeline_not_found(&self.account, &pipeline_name);
        };
        let summaries: Vec<Value> = self
            .executions
            .get(&pipeline_name)
            .map(|list| list.iter().map(|e| e.to_summary()).collect())
            .unwrap_or_default();
        Response::json_status(200, &json!({"pipelineExecutionSummaries": summaries}))
    }

    fn get_pipeline_state(&self, params: &Value) -> Response {
        let name = param_str(params, "name");
        let Some(p) = self.pipelines.get(&name) else {
            return pipeline_not_found(&self.account, &name);
        };
        let latest = self
            .executions
            .get(&name)
            .and_then(|list| list.first())
            .map(|e| {
                json!({
                    "pipelineExecutionId": e.pipeline_execution_id,
                    "status": e.status,
                })
            });
        let stage_states: Vec<Value> = p
            .pipeline
            .get("stages")
            .and_then(Value::as_array)
            .map(|stages| {
                stages
                    .iter()
                    .map(|stage| {
                        let stage_name = stage.get("name").and_then(Value::as_str).unwrap_or("");
                        let action_states: Vec<Value> = stage
                            .get("actions")
                            .and_then(Value::as_array)
                            .map(|actions| {
                                actions
                                    .iter()
                                    .map(|a| json!({"actionName": a.get("name")}))
                                    .collect()
                            })
                            .unwrap_or_default();
                        let transition =
                            p.stage_transitions.get(stage_name).copied().unwrap_or(true);
                        let mut stage_state = json!({
                            "stageName": stage_name,
                            "actionStates": action_states,
                            "inboundTransitionState": {"enabled": transition},
                        });
                        if let Some(latest) = &latest {
                            stage_state["latestExecution"] = latest.clone();
                        }
                        stage_state
                    })
                    .collect()
            })
            .unwrap_or_default();
        Response::json_status(
            200,
            &json!({
                "pipelineName": name,
                "pipelineVersion": p.pipeline["version"],
                "stageStates": stage_states,
                "created": iso_stamp(&p.created),
                "updated": iso_stamp(&p.updated),
            }),
        )
    }

    fn retry_stage_execution(&mut self, params: &Value) -> Response {
        let pipeline_name = param_str(params, "pipelineName");
        let execution_id = param_str(params, "pipelineExecutionId");
        let stage_name = param_str(params, "stageName");
        if self.pipeline_mut(&pipeline_name).is_none() {
            return pipeline_not_found(&self.account, &pipeline_name);
        }
        if let Err(resp) = self.validate_stage(&pipeline_name, &stage_name) {
            return resp;
        }
        let Some(executions) = self.executions.get_mut(&pipeline_name) else {
            return execution_not_found(&execution_id, &pipeline_name);
        };
        let Some(execution) = executions
            .iter_mut()
            .find(|e| e.pipeline_execution_id == execution_id)
        else {
            return execution_not_found(&execution_id, &pipeline_name);
        };
        if !matches!(execution.status.as_str(), "Failed" | "Stopped") {
            return crate::proto::json_error(
                400,
                "StageNotRetryableException",
                &format!(
                    "Stage '{stage_name}' in pipeline execution '{execution_id}' is not retryable"
                ),
            );
        }
        execution.status = "InProgress".to_string();
        execution.last_update_time = now_iso();
        Response::json_status(200, &json!({"pipelineExecutionId": execution_id}))
    }

    fn rollback_stage(&mut self, params: &Value) -> Response {
        let pipeline_name = param_str(params, "pipelineName");
        let stage_name = param_str(params, "stageName");
        let Some(p) = self.pipelines.get(&pipeline_name) else {
            return pipeline_not_found(&self.account, &pipeline_name);
        };
        let version = p.pipeline["version"].as_i64().unwrap_or(1);
        if let Err(resp) = self.validate_stage(&pipeline_name, &stage_name) {
            return resp;
        }
        let execution = Execution {
            pipeline_name: pipeline_name.clone(),
            pipeline_version: version,
            pipeline_execution_id: uuid::Uuid::new_v4().to_string(),
            status: "InProgress".to_string(),
            status_summary: None,
            trigger: json!({"triggerType": "RollbackStage", "triggerDetail": stage_name}),
            source_revisions: json!([]),
            start_time: now_iso(),
            last_update_time: now_iso(),
        };
        let id = execution.pipeline_execution_id.clone();
        self.executions
            .entry(pipeline_name)
            .or_default()
            .insert(0, execution);
        Response::json_status(200, &json!({"pipelineExecutionId": id}))
    }

    fn validate_stage(&self, pipeline_name: &str, stage_name: &str) -> Result<(), Response> {
        let names: Vec<&str> = self
            .pipelines
            .get(pipeline_name)
            .and_then(|p| p.pipeline.get("stages"))
            .and_then(Value::as_array)
            .map(|stages| {
                stages
                    .iter()
                    .filter_map(|s| s.get("name").and_then(Value::as_str))
                    .collect()
            })
            .unwrap_or_default();
        if names.contains(&stage_name) {
            Ok(())
        } else {
            Err(stage_not_found(stage_name, pipeline_name))
        }
    }

    fn set_stage_transition(&mut self, params: &Value, value: bool) -> Response {
        let pipeline_name = param_str(params, "pipelineName");
        let stage_name = param_str(params, "stageName");
        if self.pipeline_mut(&pipeline_name).is_none() {
            return pipeline_not_found(&self.account, &pipeline_name);
        }
        if let Err(resp) = self.validate_stage(&pipeline_name, &stage_name) {
            return resp;
        }
        if let Some(record) = self.pipeline_mut(&pipeline_name) {
            record.stage_transitions.insert(stage_name, value);
        }
        Response::json_status(200, &json!({}))
    }

    fn override_stage_condition(&mut self, params: &Value) -> Response {
        let pipeline_name = param_str(params, "pipelineName");
        let stage_name = param_str(params, "stageName");
        if self.pipeline_mut(&pipeline_name).is_none() {
            return pipeline_not_found(&self.account, &pipeline_name);
        }
        if let Err(resp) = self.validate_stage(&pipeline_name, &stage_name) {
            return resp;
        }
        Response::json_status(200, &json!({}))
    }

    fn list_action_executions(&self, params: &Value) -> Response {
        let pipeline_name = param_str(params, "pipelineName");
        let Some(p) = self.pipelines.get(&pipeline_name) else {
            return pipeline_not_found(&self.account, &pipeline_name);
        };
        let filter_execution = params
            .get("filter")
            .and_then(|f| f.get("pipelineExecutionId"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let mut details = Vec::new();
        let executions = self.executions.get(&pipeline_name);
        for execution in executions.into_iter().flatten() {
            if !filter_execution.is_empty() && execution.pipeline_execution_id != filter_execution {
                continue;
            }
            for stage in p
                .pipeline
                .get("stages")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                for action in stage
                    .get("actions")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    details.push(json!({
                        "pipelineExecutionId": execution.pipeline_execution_id,
                        "actionExecutionId": uuid::Uuid::new_v4().to_string(),
                        "pipelineVersion": execution.pipeline_version,
                        "stageName": stage.get("name"),
                        "actionName": action.get("name"),
                        "startTime": execution.start_time,
                        "status": execution.status,
                        "input": {
                            "actionTypeId": action.get("actionTypeId").cloned().unwrap_or(json!({})),
                            "configuration": action.get("configuration").cloned().unwrap_or(json!({})),
                            "region": self.region,
                            "inputArtifacts": action.get("inputArtifacts").cloned().unwrap_or(json!([])),
                        },
                        "output": {
                            "outputArtifacts": action.get("outputArtifacts").cloned().unwrap_or(json!([])),
                            "outputVariables": {},
                        },
                    }));
                }
            }
        }
        Response::json_status(200, &json!({"actionExecutionDetails": details}))
    }

    fn list_rule_executions(&self, params: &Value) -> Response {
        let pipeline_name = param_str(params, "pipelineName");
        if self.pipeline_mut_check(&pipeline_name).is_none() {
            return pipeline_not_found(&self.account, &pipeline_name);
        }
        Response::json_status(200, &json!({"ruleExecutionDetails": []}))
    }

    fn list_deploy_action_execution_targets(&self, params: &Value) -> Response {
        let pipeline_name = param_str(params, "pipelineName");
        if !pipeline_name.is_empty() && self.pipeline_mut_check(&pipeline_name).is_none() {
            return pipeline_not_found(&self.account, &pipeline_name);
        }
        Response::json_status(200, &json!({"targets": []}))
    }

    // ---- custom action types ---------------------------------------------------

    fn create_custom_action_type(&mut self, params: &Value) -> Response {
        let category = param_str(params, "category");
        let provider = param_str(params, "provider");
        let version = param_str(params, "version");
        let action_type = json!({
            "id": {
                "category": category,
                "owner": "Custom",
                "provider": provider,
                "version": version,
            },
            "settings": params.get("settings").cloned().unwrap_or(json!({})),
            "inputArtifactDetails": params.get("inputArtifactDetails").cloned()
                .unwrap_or(json!({"minimumCount": 0, "maximumCount": 5})),
            "outputArtifactDetails": params.get("outputArtifactDetails").cloned()
                .unwrap_or(json!({"minimumCount": 0, "maximumCount": 5})),
        });
        let key = format!("{category}:{provider}");
        self.custom_action_types
            .insert(format!("{key}:{version}"), action_type.clone());
        Response::json_status(200, &json!({"actionType": action_type}))
    }

    fn delete_custom_action_type(&mut self, params: &Value) -> Response {
        let key = format!(
            "{}:{}",
            param_str(params, "category"),
            param_str(params, "provider")
        ) + ":"
            + &param_str(params, "version");
        if self.custom_action_types.remove(&key).is_none() {
            return crate::proto::json_error(
                400,
                "ActionTypeNotFoundException",
                &format!("No action type found for: category '{}', owner 'Custom', provider '{}', version '{}'",
                    param_str(params, "category"), param_str(params, "provider"), param_str(params, "version")),
            );
        }
        Response::json_status(200, &json!({}))
    }

    fn list_action_types(&self, params: &Value) -> Response {
        let owner_filter = params.get("actionOwnerFilter").and_then(Value::as_str);
        let mut result = Vec::new();
        let include = |owner: &str| owner_filter.is_none_or(|f| f == owner);
        let builtin = vec![
            ("Source", "S3", 0u16, 1u16),
            ("Source", "CodeCommit", 0, 1),
            ("Source", "CodeStarSourceConnection", 0, 1),
            ("Source", "ECR", 0, 1),
            ("Build", "CodeBuild", 1, 5),
            ("Deploy", "S3", 1, 0),
            ("Deploy", "CloudFormation", 0, 1),
            ("Deploy", "CodeDeploy", 1, 0),
            ("Deploy", "ECS", 1, 0),
            ("Invoke", "Lambda", 0, 5),
            ("Approval", "Manual", 0, 0),
            ("Test", "CodeBuild", 1, 5),
        ];
        if include("AWS") {
            for (category, provider, min_in, max_in) in builtin {
                result.push(json!({
                    "id": {"category": category, "owner": "AWS", "provider": provider, "version": "1"},
                    "inputArtifactDetails": {"minimumCount": min_in, "maximumCount": max_in},
                    "outputArtifactDetails": {"minimumCount": max_in, "maximumCount": max_in},
                }));
            }
        }
        if include("ThirdParty") {
            result.push(json!({
                "id": {"category": "Source", "owner": "ThirdParty", "provider": "GitHub", "version": "1"},
                "inputArtifactDetails": {"minimumCount": 0, "maximumCount": 0},
                "outputArtifactDetails": {"minimumCount": 1, "maximumCount": 1},
            }));
        }
        if include("Custom") {
            result.extend(self.custom_action_types.values().cloned());
        }
        Response::json_status(200, &json!({"actionTypes": result}))
    }

    fn get_action_type(&self, params: &Value) -> Response {
        let owner = param_str(params, "owner");
        if owner == "Custom" {
            let key = format!(
                "{}:{}:{}",
                param_str(params, "category"),
                param_str(params, "provider"),
                param_str(params, "version")
            );
            return match self.custom_action_types.get(&key) {
                Some(at) => Response::json_status(200, &json!({"actionType": at})),
                None => crate::proto::json_error(
                    400,
                    "ActionTypeNotFoundException",
                    &format!(
                        "No action type found for: category '{}', owner '{owner}', provider '{}', version '{}'",
                        param_str(params, "category"),
                        param_str(params, "provider"),
                        param_str(params, "version")
                    ),
                ),
            };
        }
        Response::json_status(
            200,
            &json!({"actionType": {
                "id": {
                    "category": param_str(params, "category"),
                    "owner": owner,
                    "provider": param_str(params, "provider"),
                    "version": param_str(params, "version"),
                },
                "inputArtifactDetails": {"minimumCount": 0, "maximumCount": 5},
                "outputArtifactDetails": {"minimumCount": 0, "maximumCount": 5},
            }}),
        )
    }

    fn update_action_type(&mut self, params: &Value) -> Response {
        let at = params.get("actionType").cloned().unwrap_or(json!({}));
        let id = at.get("id").cloned().unwrap_or(json!({}));
        if id.get("owner").and_then(Value::as_str) != Some("Custom") {
            return crate::proto::json_error(
                400,
                "ValidationException",
                "Only custom action types can be updated",
            );
        }
        let key = format!(
            "{}:{}:{}",
            id.get("category").and_then(Value::as_str).unwrap_or(""),
            id.get("provider").and_then(Value::as_str).unwrap_or(""),
            id.get("version").and_then(Value::as_str).unwrap_or("")
        );
        let Some(existing) = self.custom_action_types.get_mut(&key) else {
            return crate::proto::json_error(
                400,
                "ActionTypeNotFoundException",
                &format!("No action type found for: category '{}', owner 'Custom', provider '{}', version '{}'",
                    id.get("category").and_then(Value::as_str).unwrap_or(""),
                    id.get("provider").and_then(Value::as_str).unwrap_or(""),
                    id.get("version").and_then(Value::as_str).unwrap_or("")),
            );
        };
        if let Some(v) = null_if_absent(at.get("inputArtifactDetails").cloned()) {
            existing["inputArtifactDetails"] = v;
        }
        if let Some(v) = null_if_absent(at.get("outputArtifactDetails").cloned()) {
            existing["outputArtifactDetails"] = v;
        }
        if let Some(v) = null_if_absent(at.get("description").cloned()) {
            existing["description"] = v;
        }
        Response::json_status(200, &json!({}))
    }

    // ---- webhooks ----------------------------------------------------------------

    fn put_webhook(&mut self, params: &Value) -> Response {
        let webhook = params.get("webhook").cloned().unwrap_or(json!({}));
        let name = webhook
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let target = webhook
            .get("targetPipeline")
            .and_then(Value::as_str)
            .unwrap_or("");
        // moto validates the target pipeline before storing the webhook.
        if !target.is_empty() && !self.pipelines.contains_key(target) {
            return pipeline_not_found(&self.account, target);
        }
        let record = json!({
            "definition": webhook.clone(),
            "url": format!("https://webhooks.{region}.codepipeline.amazonaws.com/{name}", region = self.region),
            "arn": format!("arn:aws:codepipeline:{region}:{account}:webhook:{name}",
                region = self.region,
                account = self.account),
        });
        self.webhooks.insert(name.clone(), record.clone());
        Response::json_status(200, &json!({"webhook": record}))
    }

    fn delete_webhook(&mut self, params: &Value) -> Response {
        let name = param_str(params, "name");
        match self.webhooks.remove(&name) {
            Some(_) => Response::json_status(200, &json!({})),
            None => webhook_not_found(&name),
        }
    }

    fn list_webhooks(&self) -> Response {
        Response::json_status(
            200,
            &json!({"webhooks": self.webhooks.values().cloned().collect::<Vec<_>>()}),
        )
    }

    fn set_webhook_registration(&mut self, params: &Value, _register: bool) -> Response {
        let name = param_str(params, "webhookName");
        if !self.webhooks.contains_key(&name) {
            return webhook_not_found(&name);
        }
        Response::json_status(200, &json!({}))
    }

    // ---- jobs ---------------------------------------------------------------------

    fn poll_for_jobs(&self) -> Response {
        let jobs: Vec<Value> = self
            .jobs
            .values()
            .map(|job| job_to_dict_json(job.clone()))
            .collect();
        Response::json_status(200, &json!({"jobs": jobs}))
    }

    fn poll_for_third_party_jobs(&self) -> Response {
        let jobs: Vec<Value> = self
            .third_party_jobs
            .values()
            .map(|job| json!({"clientId": self.account, "jobId": job["id"]}))
            .collect();
        Response::json_status(200, &json!({"jobs": jobs}))
    }

    fn get_job_details(&self, params: &Value) -> Response {
        let job_id = param_str(params, "jobId");
        match self.jobs.get(&job_id).cloned() {
            Some(job) => Response::json_status(200, &json!({"jobDetails": job_to_dict_json(job)})),
            None => job_not_found(&job_id),
        }
    }

    fn acknowledge_job(&self, params: &Value) -> Response {
        let job_id = param_str(params, "jobId");
        match self.jobs.get(&job_id) {
            Some(_) => Response::json_status(200, &json!({"status": "InProgress"})),
            None => job_not_found(&job_id),
        }
    }

    fn put_job_result(&mut self, params: &Value, _status: &str) -> Response {
        let _ = _status;
        let job_id = param_str(params, "jobId");
        if !self.jobs.contains_key(&job_id) && !self.third_party_jobs.contains_key(&job_id) {
            return job_not_found(&job_id);
        }
        Response::json_status(200, &json!({}))
    }

    fn put_approval_result(&self, params: &Value) -> Response {
        let pipeline_name = param_str(params, "pipelineName");
        if self.pipeline_mut_check(&pipeline_name).is_none() {
            return pipeline_not_found(&self.account, &pipeline_name);
        }
        Response::json_status(200, &json!({"approvedAt": now_iso()}))
    }

    fn put_action_revision(&mut self, params: &Value) -> Response {
        let pipeline_name = param_str(params, "pipelineName");
        let out = {
            let Some(p) = self.pipelines.get(&pipeline_name) else {
                return pipeline_not_found(&self.account, &pipeline_name);
            };
            let stage_name = param_str(params, "stageName");
            if let Err(resp) = self.validate_stage(&pipeline_name, &stage_name) {
                return resp;
            }
            let action_name = param_str(params, "actionName");
            let stage = p
                .pipeline
                .get("stages")
                .and_then(Value::as_array)
                .and_then(|stages| {
                    stages.iter().find(|s| {
                        s.get("name").and_then(Value::as_str) == Some(stage_name.as_str())
                    })
                })
                .cloned()
                .unwrap_or(json!({}));
            let action_names: Vec<&str> = stage
                .get("actions")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|a| a.get("name").and_then(Value::as_str))
                        .collect()
                })
                .unwrap_or_default();
            if !action_names.contains(&action_name.as_str()) {
                return crate::proto::json_error(
                    400,
                    "ActionNotFoundException",
                    &format!("Action '{action_name}' does not exist in stage '{stage_name}' of pipeline '{pipeline_name}'"),
                );
            }
            let revision = params.get("actionRevision").cloned().unwrap_or(json!({}));
            let source_revisions = json!([{
                "actionName": action_name,
                "revisionId": revision.get("revisionId").cloned().unwrap_or(json!("")),
                "revisionSummary": revision.get("revisionChangeId").cloned().unwrap_or(json!("")),
            }]);
            let execution = Execution {
                pipeline_name: pipeline_name.clone(),
                pipeline_version: p.pipeline["version"].as_i64().unwrap_or(1),
                pipeline_execution_id: uuid::Uuid::new_v4().to_string(),
                status: "InProgress".to_string(),
                status_summary: None,
                trigger: json!({"triggerType": "StartPipelineExecution", "triggerDetail": ""}),
                source_revisions,
                start_time: now_iso(),
                last_update_time: now_iso(),
            };
            (true, execution.pipeline_execution_id.clone(), execution)
        };
        let (new_revision, id, execution) = out;
        self.executions
            .entry(pipeline_name)
            .or_default()
            .insert(0, execution);
        Response::json_status(
            200,
            &json!({
                "newRevision": new_revision,
                "pipelineExecutionId": id,
            }),
        )
    }

    fn pipeline_mut_check(&self, name: &str) -> Option<&Pipeline> {
        self.pipelines.get(name)
    }
}

fn sorted_tag_list_from(tags: &HashMap<String, String>) -> Value {
    let mut list: Vec<Value> = tags
        .iter()
        .map(|(k, v)| json!({"key": k, "value": v}))
        .collect();
    list.sort_by(|a, b| {
        let ka = a["key"].as_str().unwrap_or("");
        let kb = b["key"].as_str().unwrap_or("");
        ka.cmp(kb)
    });
    Value::Array(list)
}

fn job_to_dict_json(job: Value) -> Value {
    let record = json!({
        "id": job["id"],
        "nonce": job["nonce"],
        "accountId": job["accountId"],
        "data": {
            "actionTypeId": job["actionTypeId"],
            "actionConfiguration": {"configuration": job["actionConfiguration"]},
            "pipelineContext": {
                "pipelineName": job["pipelineName"],
                "stage": {"name": job["stageName"]},
                "action": {"name": job["actionName"]},
            },
            "inputArtifacts": job["inputArtifacts"],
            "outputArtifacts": job["outputArtifacts"],
        },
    });
    record
}

fn extract_between(text: &str, start: &str, end: &str) -> String {
    match (text.find(start), text.find(end)) {
        (Some(s), Some(e)) if s + start.len() <= e => text[s + start.len()..e].to_string(),
        _ => String::new(),
    }
}

fn null_if_absent(v: Option<Value>) -> Option<Value> {
    v.filter(|x| !x.is_null())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn req(op: &str, body: Value) -> Request {
        Request {
            service: "codepipeline".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account: 123456789001,
            region: "us-east-1".into(),
        }
    }

    fn json_body(resp: Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    fn pipeline_def() -> Value {
        json!({
            "name": "pipe1",
            "roleArn": "arn:aws:iam::123456789012:role/cp-role",
            "artifactStore": {"type": "S3", "location": "artifacts"},
            "stages": [
                {"name": "Source", "actions": [{"name": "SourceAction", "actionTypeId": {
                    "category": "Source", "owner": "AWS", "provider": "S3", "version": "1"}}]},
                {"name": "Deploy", "actions": [{"name": "DeployAction", "actionTypeId": {
                    "category": "Deploy", "owner": "AWS", "provider": "S3", "version": "1"}}]},
            ],
        })
    }

    // The trust probe goes through the gateway loopback; in unit tests there
    // is no server, so CreatePipeline reports the unauthorized-structure
    // error. Everything else exercises pure state.
    #[test]
    fn update_list_and_tags() {
        // Pipeline misses the trust lookup and stays absent — exercise the
        // non-IAM paths via a directly inserted record.
        let pipeline = pipeline_def();
        let mut record = Pipeline {
            pipeline: pipeline.clone(),
            tags: HashMap::new(),
            arn: "arn:aws:codepipeline:us-east-1:123456789001:pipe1".into(),
            created: now_iso(),
            updated: now_iso(),
            stage_transitions: HashMap::new(),
        };
        record.stage_transitions.insert("Source".into(), true);
        record.stage_transitions.insert("Deploy".into(), true);
        SLOT.write()
            .entry((123456789001, "us-east-1".into()))
            .or_default();
        // insert via the state map entry
        {
            let mut guard = SLOT.write();
            let st = guard
                .entry((123456789001, "us-east-1".to_string()))
                .or_default();
            st.pipelines.insert("pipe1".into(), record);
        }

        let got = json_body(dispatch(&req("GetPipeline", json!({"name": "pipe1"}))));
        assert_eq!(got["pipeline"]["name"], "pipe1");
        assert!(got["metadata"]["pipelineArn"].is_string());

        let list = json_body(dispatch(&req("ListPipelines", json!({}))));
        assert!(list["pipelines"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["name"] == "pipe1"));

        let arn = "arn:aws:codepipeline:us-east-1:123456789001:pipe1";
        dispatch(&req(
            "TagResource",
            json!({"resourceArn": arn, "tags": [{"key": "env", "value": "test"}]}),
        ));
        let tags = json_body(dispatch(&req(
            "ListTagsForResource",
            json!({"resourceArn": arn}),
        )));
        assert_eq!(tags["tags"][0]["key"], "env");
        dispatch(&req(
            "UntagResource",
            json!({"resourceArn": arn, "tagKeys": ["env"]}),
        ));
        let after = json_body(dispatch(&req(
            "ListTagsForResource",
            json!({"resourceArn": arn}),
        )));
        assert_eq!(after["tags"].as_array().unwrap().len(), 0);

        let started = json_body(dispatch(&req(
            "StartPipelineExecution",
            json!({"name": "pipe1"}),
        )));
        let exec_id = started["pipelineExecutionId"].as_str().unwrap().to_string();
        let exec = json_body(dispatch(&req(
            "GetPipelineExecution",
            json!({"pipelineName": "pipe1", "pipelineExecutionId": exec_id}),
        )));
        assert_eq!(exec["pipelineExecution"]["pipelineExecutionId"], exec_id);

        let state = json_body(dispatch(&req("GetPipelineState", json!({"name": "pipe1"}))));
        assert_eq!(state["pipelineName"], "pipe1");
        assert_eq!(state["stageStates"].as_array().unwrap().len(), 2);

        let unknown = dispatch(&req("GetPipeline", json!({"name": "does-not-exist"})));
        let err: Value = serde_json::from_slice(&unknown.body).unwrap();
        assert_eq!(err["__type"], "PipelineNotFoundException");
        assert_eq!(unknown.status, 404);
    }

    #[test]
    fn webhooks_action_types_and_jobs() {
        SLOT.write()
            .entry((123456789001, "us-east-1".into()))
            .or_default();
        let json_types = json_body(dispatch(&req(
            "ListActionTypes",
            json!({"actionOwnerFilter": "AWS"}),
        )));
        assert!(json_types["actionTypes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["id"]["provider"] == "CodeBuild"));

        let created = json_body(dispatch(&req(
            "CreateCustomActionType",
            json!({"category": "Test", "provider": "Upd", "version": "1",
                   "inputArtifactDetails": {"minimumCount": 0, "maximumCount": 1},
                   "outputArtifactDetails": {"minimumCount": 0, "maximumCount": 1}}),
        )));
        assert_eq!(created["actionType"]["id"]["owner"], "Custom");

        let got = json_body(dispatch(&req(
            "GetActionType",
            json!({"category": "Test", "owner": "Custom", "provider": "Upd", "version": "1"}),
        )));
        assert_eq!(got["actionType"]["id"]["provider"], "Upd");

        assert_eq!(dispatch(&req("UpdateActionType", json!({"actionType": {"id": {"category": "Test", "owner": "Custom", "provider": "Upd", "version": "1"}, "inputArtifactDetails": {"minimumCount": 0, "maximumCount": 2}}}))).status, 200);

        let missing = dispatch(&req(
            "GetJobDetails",
            json!({"jobId": "00000000-0000-0000-0000-000000000000"}),
        ));
        let err: Value = serde_json::from_slice(&missing.body).unwrap();
        assert_eq!(err["__type"], "JobNotFoundException");

        let hooks = json_body(dispatch(&req("ListWebhooks", json!({}))));
        assert!(hooks["webhooks"].is_array());

        let rules = json_body(dispatch(&req("ListRuleTypes", json!({}))));
        assert_eq!(rules["ruleTypes"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn delete_webhook_not_found() {
        SLOT.write()
            .entry((123456789001, "us-east-1".into()))
            .or_default();
        let resp = dispatch(&req("DeleteWebhook", json!({"name": "no-such-hook"})));
        let err: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(err["__type"], "WebhookNotFoundException");
    }
}
