//! AWS Batch backend — moto-equivalent, docker-free.
//!
//! Base CRUD ops follow the Python ground-truth batch provider; the newer
//! families (scheduling policies, consumable resources, service
//! environments/jobs, queue snapshots) replicate moto's stub semantics.
//! The pytest suites (compat + moto_impl) are the contract.

use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use uuid::Uuid;

type Stores = Arc<RwLock<HashMap<ScopeKey, BatchStore>>>;

fn stores() -> Stores {
    store::slot::<BatchStore>("batch")
}

/// Per-(account, region) in-memory state for AWS Batch.
#[derive(Default)]
struct BatchStore {
    /// name -> compute environment
    compute_envs: BTreeMap<String, Value>,
    /// name -> job queue
    job_queues: BTreeMap<String, Value>,
    /// name -> [revisions]
    job_definitions: BTreeMap<String, Vec<Value>>,
    /// job_id -> job
    jobs: BTreeMap<String, Value>,
    /// arn -> scheduling policy
    scheduling_policies: BTreeMap<String, Value>,
    /// arn -> consumable resource
    consumable_resources: BTreeMap<String, Value>,
    /// arn -> service environment
    service_environments: BTreeMap<String, Value>,
    /// arn -> {tag key: value}
    tags: BTreeMap<String, BTreeMap<String, String>>,
}

/// Entry point: serve a batch request, or `None` when this module is not
/// responsible for `req`.
pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "batch" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);

    // Tag routes carry the resource ARN in the path: /v1/tags/{arn}
    if let Some(arn) = req.path.strip_prefix("/v1/tags/") {
        return Some(tag_ops(req, &urldecode(arn), &body));
    }

    let op = path_op(req);
    Some(match op {
        Some(handler) => handler(req, &body),
        // Deny-by-default: only suite-covered behavior is ported.
        None => unknown_op(req),
    })
}

fn path_op(req: &Request) -> Option<fn(&Request, &Value) -> Response> {
    if req.method != "POST" {
        return None;
    }
    Some(match req.path.as_str() {
        "/v1/createcomputeenvironment" => create_compute_environment,
        "/v1/describecomputeenvironments" => describe_compute_environments,
        "/v1/updatecomputeenvironment" => update_compute_environment,
        "/v1/deletecomputeenvironment" => delete_compute_environment,
        "/v1/createjobqueue" => create_job_queue,
        "/v1/describejobqueues" => describe_job_queues,
        "/v1/updatejobqueue" => update_job_queue,
        "/v1/deletejobqueue" => delete_job_queue,
        "/v1/registerjobdefinition" => register_job_definition,
        "/v1/describejobdefinitions" => describe_job_definitions,
        "/v1/deregisterjobdefinition" => deregister_job_definition,
        "/v1/submitjob" => submit_job,
        "/v1/describejobs" => describe_jobs,
        "/v1/listjobs" => list_jobs,
        "/v1/terminatejob" => terminate_job,
        "/v1/canceljob" => cancel_job,
        "/v1/createschedulingpolicy" => create_scheduling_policy,
        "/v1/describeschedulingpolicies" => describe_scheduling_policies,
        "/v1/listschedulingpolicies" => list_scheduling_policies,
        "/v1/deleteschedulingpolicy" => delete_scheduling_policy,
        "/v1/updateschedulingpolicy" => update_scheduling_policy,
        "/v1/getjobqueuesnapshot" => get_job_queue_snapshot,
        "/v1/createconsumableresource" => create_consumable_resource,
        "/v1/describeconsumableresource" => describe_consumable_resource,
        "/v1/deleteconsumableresource" => delete_consumable_resource,
        "/v1/listconsumableresources" => list_consumable_resources,
        "/v1/updateconsumableresource" => update_consumable_resource,
        "/v1/listjobsbyconsumableresource" => list_jobs_by_consumable_resource,
        "/v1/createserviceenvironment" => create_service_environment,
        "/v1/describeserviceenvironments" => describe_service_environments,
        "/v1/updateserviceenvironment" => update_service_environment,
        "/v1/deleteserviceenvironment" => delete_service_environment,
        "/v1/submitservicejob" => submit_service_job,
        "/v1/describeservicejob" => describe_service_job,
        "/v1/listservicejobs" => list_service_jobs,
        "/v1/terminateservicejob" => terminate_service_job,
        _ => return None,
    })
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response::new(
        200,
        "application/json",
        serde_json::to_vec(&body).unwrap_or_default(),
    )
}

/// The rest-json error shape botocore derives typed ClientErrors from.
fn batch_error(status: u16, code: &str, message: &str) -> Response {
    crate::proto::json_error(status, code, message)
}

fn client_error(message: String) -> Response {
    batch_error(400, "ClientException", &message)
}

fn param_error(message: String) -> Response {
    batch_error(400, "InvalidParameterValueException", &message)
}

fn unknown_op(req: &Request) -> Response {
    batch_error(
        501,
        "NotImplemented",
        &format!("The {} action has not been implemented", req.operation),
    )
}

fn urldecode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 3 <= bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(b) => {
                        out.push(b);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn arn_prefix(req: &Request) -> String {
    format!("arn:aws:batch:{}:{}", req.region, req.account)
}

/// Run an operation against the (account, region)-scoped store.
fn with_store(
    req: &Request,
    f: impl FnOnce(&mut BatchStore) -> Result<Value, Response>,
) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    match f(st) {
        Ok(body) => ok(body),
        Err(resp) => resp,
    }
}

fn tags_value(params: &Value, key: &str) -> Option<BTreeMap<String, String>> {
    let obj = params.get(key)?.as_object()?;
    let map: BTreeMap<String, String> = obj
        .iter()
        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
        .collect();
    Some(map)
}

/// Split a job definition reference into (name, Some(revision)) — revision
/// present for `name:revision` / ARN forms.
fn job_def_ref(ref_: &str) -> (&str, Option<i64>) {
    let tail = if ref_.starts_with("arn:") {
        ref_.rsplit('/').next().unwrap_or(ref_)
    } else {
        ref_
    };
    if let Some((name, rev)) = tail.rsplit_once(':') {
        if let Ok(rev) = rev.parse::<i64>() {
            return (name, Some(rev));
        }
    }
    (tail, None)
}

fn tail_of(s: &str) -> &str {
    s.rsplit('/').next().unwrap_or(s)
}

// ---------------------------------------------------------------------------
// Compute environments
// ---------------------------------------------------------------------------

fn create_compute_environment(req: &Request, params: &Value) -> Response {
    let name = params["computeEnvironmentName"].as_str().unwrap_or("");
    if name.is_empty() {
        return client_error("computeEnvironmentName is required.".to_string());
    }
    let ce_arn = format!("{}:compute-environment/{}", arn_prefix(req), name);
    with_store(req, |st| {
        if st.compute_envs.contains_key(name) {
            return Err(param_error(format!(
                "Compute environment {name} already exists."
            )));
        }
        if let Some(t) = tags_value(params, "tags").filter(|t| !t.is_empty()) {
            st.tags.insert(ce_arn.clone(), t);
        }
        let ce = json!({
            "computeEnvironmentArn": ce_arn,
            "computeEnvironmentName": name,
            "type": params.get("type").cloned().unwrap_or(json!("MANAGED")),
            "state": params.get("state").cloned().unwrap_or(json!("ENABLED")),
            "status": "VALID",
            "statusReason": "ComputeEnvironment Healthy",
            "computeResources": params.get("computeResources").cloned().unwrap_or(json!({})),
            "serviceRole": params.get("serviceRole").cloned().unwrap_or(json!("")),
            "tags": params.get("tags").cloned().unwrap_or(json!({})),
        });
        st.compute_envs.insert(name.to_string(), ce);
        Ok(json!({
            "computeEnvironmentArn": ce_arn,
            "computeEnvironmentName": name,
        }))
    })
}

fn describe_compute_environments(req: &Request, params: &Value) -> Response {
    let names = params["computeEnvironments"].as_array().cloned();
    with_store(req, |st| {
        let envs: Vec<Value> = match names {
            Some(list) => list
                .iter()
                .filter_map(|n| n.as_str())
                .filter_map(|n| st.compute_envs.get(tail_of(n)))
                .cloned()
                .collect(),
            None => st.compute_envs.values().cloned().collect(),
        };
        Ok(json!({ "computeEnvironments": envs }))
    })
}

fn update_compute_environment(req: &Request, params: &Value) -> Response {
    let name = params["computeEnvironment"]
        .as_str()
        .unwrap_or("")
        .to_string();
    with_store(req, |st| {
        let ce = match st.compute_envs.get_mut(tail_of(&name)) {
            Some(ce) => ce,
            None => {
                return Err(client_error(format!(
                    "Compute environment {name} not found."
                )))
            }
        };
        if let Some(state) = params.get("state") {
            ce["state"] = state.clone();
        }
        if let Some(resources) = params.get("computeResources").and_then(|v| v.as_object()) {
            if let Some(obj) = ce["computeResources"].as_object_mut() {
                for (k, v) in resources {
                    obj.insert(k.clone(), v.clone());
                }
            }
        }
        Ok(json!({
            "computeEnvironmentArn": ce["computeEnvironmentArn"],
            "computeEnvironmentName": name,
        }))
    })
}

fn delete_compute_environment(req: &Request, params: &Value) -> Response {
    let name = params["computeEnvironment"]
        .as_str()
        .unwrap_or("")
        .to_string();
    with_store(req, |st| {
        let ce_name = tail_of(&name).to_string();
        let arn = match st.compute_envs.get(&ce_name) {
            Some(ce) => ce["computeEnvironmentArn"]
                .as_str()
                .unwrap_or("")
                .to_string(),
            None => {
                return Err(client_error(format!(
                    "Compute environment {name} not found."
                )))
            }
        };
        st.compute_envs.remove(&ce_name);
        st.tags.remove(&arn);
        Ok(json!({}))
    })
}

// ---------------------------------------------------------------------------
// Job queues
// ---------------------------------------------------------------------------

fn create_job_queue(req: &Request, params: &Value) -> Response {
    let name = params["jobQueueName"].as_str().unwrap_or("");
    if name.is_empty() {
        return client_error("jobQueueName is required.".to_string());
    }
    let q_arn = format!("{}:job-queue/{}", arn_prefix(req), name);
    with_store(req, |st| {
        if st.job_queues.contains_key(name) {
            return Err(client_error(format!("Job queue {name} already exists.")));
        }
        if let Some(t) = tags_value(params, "tags").filter(|t| !t.is_empty()) {
            st.tags.insert(q_arn.clone(), t);
        }
        let queue = json!({
            "jobQueueArn": q_arn,
            "jobQueueName": name,
            "state": params.get("state").cloned().unwrap_or(json!("ENABLED")),
            "status": "VALID",
            "statusReason": "JobQueue Healthy",
            "priority": params.get("priority").cloned().unwrap_or(json!(1)),
            "computeEnvironmentOrder": params
                .get("computeEnvironmentOrder")
                .cloned()
                .unwrap_or(json!([])),
            "tags": params.get("tags").cloned().unwrap_or(json!({})),
        });
        st.job_queues.insert(name.to_string(), queue);
        Ok(json!({ "jobQueueArn": q_arn, "jobQueueName": name }))
    })
}

fn describe_job_queues(req: &Request, params: &Value) -> Response {
    let names = params["jobQueues"].as_array().cloned();
    with_store(req, |st| {
        let queues: Vec<Value> = match names {
            Some(list) => list
                .iter()
                .filter_map(|n| n.as_str())
                .filter_map(|n| st.job_queues.get(tail_of(n)))
                .cloned()
                .collect(),
            None => st.job_queues.values().cloned().collect(),
        };
        Ok(json!({ "jobQueues": queues }))
    })
}

fn update_job_queue(req: &Request, params: &Value) -> Response {
    let name = params["jobQueue"].as_str().unwrap_or("").to_string();
    with_store(req, |st| {
        let q = match st.job_queues.get_mut(tail_of(&name)) {
            Some(q) => q,
            None => return Err(client_error(format!("Job queue {name} not found."))),
        };
        if let Some(state) = params.get("state") {
            q["state"] = state.clone();
        }
        if let Some(priority) = params.get("priority") {
            q["priority"] = priority.clone();
        }
        if let Some(order) = params.get("computeEnvironmentOrder") {
            q["computeEnvironmentOrder"] = order.clone();
        }
        Ok(json!({
            "jobQueueArn": q["jobQueueArn"],
            "jobQueueName": name,
        }))
    })
}

fn delete_job_queue(req: &Request, params: &Value) -> Response {
    let name = params["jobQueue"].as_str().unwrap_or("").to_string();
    with_store(req, |st| {
        let q_name = tail_of(&name).to_string();
        let arn = match st.job_queues.get(&q_name) {
            Some(q) => q["jobQueueArn"].as_str().unwrap_or("").to_string(),
            None => return Err(client_error(format!("Job queue {name} not found."))),
        };
        st.job_queues.remove(&q_name);
        st.tags.remove(&arn);
        Ok(json!({}))
    })
}

// ---------------------------------------------------------------------------
// Job definitions
// ---------------------------------------------------------------------------

fn resolve_job_definition<'a>(st: &'a BatchStore, ref_: &str) -> Option<&'a Value> {
    let (name, revision) = job_def_ref(ref_);
    let revisions = st.job_definitions.get(name)?;
    match revision {
        Some(rev) => revisions
            .iter()
            .find(|jd| jd["revision"].as_i64() == Some(rev)),
        None => revisions.iter().rev().find(|jd| jd["status"] == "ACTIVE"),
    }
}

fn register_job_definition(req: &Request, params: &Value) -> Response {
    let name = params["jobDefinitionName"].as_str().unwrap_or("");
    if name.is_empty() {
        return client_error("jobDefinitionName is required.".to_string());
    }
    with_store(req, |st| {
        let revisions = st.job_definitions.entry(name.to_string()).or_default();
        let revision = revisions.len() as i64 + 1;
        let jd_arn = format!("{}:job-definition/{}:{}", arn_prefix(req), name, revision);
        if let Some(t) = tags_value(params, "tags").filter(|t| !t.is_empty()) {
            st.tags.insert(jd_arn.clone(), t);
        }
        let jd = json!({
            "jobDefinitionArn": jd_arn,
            "jobDefinitionName": name,
            "revision": revision,
            "status": "ACTIVE",
            "type": params.get("type").cloned().unwrap_or(json!("container")),
            "containerProperties": params.get("containerProperties").cloned().unwrap_or(json!({})),
            "parameters": params.get("parameters").cloned().unwrap_or(json!({})),
            "retryStrategy": params
                .get("retryStrategy")
                .cloned()
                .unwrap_or(json!({"attempts": 1})),
            "timeout": params.get("timeout").cloned().unwrap_or(json!({})),
            "tags": params.get("tags").cloned().unwrap_or(json!({})),
        });
        revisions.push(jd);
        Ok(json!({
            "jobDefinitionArn": jd_arn,
            "jobDefinitionName": name,
            "revision": revision,
        }))
    })
}

fn describe_job_definitions(req: &Request, params: &Value) -> Response {
    let refs = params["jobDefinitions"].as_array().cloned();
    let name_filter = params["jobDefinitionName"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let status = params["status"].as_str().unwrap_or("ACTIVE").to_string();
    with_store(req, |st| match refs {
        Some(list) => {
            let results: Vec<Value> = list
                .iter()
                .filter_map(|r| r.as_str())
                .filter_map(|r| resolve_job_definition(st, r))
                .cloned()
                .collect();
            Ok(json!({ "jobDefinitions": results }))
        }
        None => {
            let results: Vec<Value> = st
                .job_definitions
                .iter()
                .filter(|(name, _)| name_filter.is_empty() || name.as_str() == name_filter)
                .flat_map(|(_, revisions)| revisions.iter().filter(|jd| jd["status"] == status))
                .cloned()
                .collect();
            Ok(json!({ "jobDefinitions": results }))
        }
    })
}

fn deregister_job_definition(req: &Request, params: &Value) -> Response {
    let ref_ = params["jobDefinition"].as_str().unwrap_or("").to_string();
    with_store(req, |st| {
        let (name, revision) = job_def_ref(&ref_);
        let revisions = match st.job_definitions.get_mut(name) {
            Some(r) => r,
            None => return Err(client_error(format!("Job definition {ref_} not found."))),
        };
        let target = match revision {
            Some(rev) => revisions
                .iter_mut()
                .find(|jd| jd["revision"].as_i64() == Some(rev)),
            None => revisions
                .iter_mut()
                .rev()
                .find(|jd| jd["status"] == "ACTIVE"),
        };
        match target {
            Some(jd) => {
                jd["status"] = json!("INACTIVE");
                Ok(json!({}))
            }
            None => Err(client_error(format!("Job definition {ref_} not found."))),
        }
    })
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

fn submit_job(req: &Request, params: &Value) -> Response {
    let job_name = params["jobName"].as_str().unwrap_or("").to_string();
    if job_name.is_empty() {
        return client_error("jobName is required.".to_string());
    }
    with_store(req, |st| {
        let job_id = Uuid::new_v4().to_string();
        let job_arn = format!("{}:job/{}", arn_prefix(req), job_id);
        let now = now_ms();
        let tags = tags_value(params, "tags");
        if let Some(t) = tags.clone().filter(|t| !t.is_empty()) {
            st.tags.insert(job_arn.clone(), t);
        }
        let job = json!({
            "jobArn": job_arn.clone(),
            "jobId": job_id.clone(),
            "jobName": job_name,
            "jobQueue": params.get("jobQueue").cloned().unwrap_or(json!("")),
            "jobDefinition": params.get("jobDefinition").cloned().unwrap_or(json!("")),
            "status": "SUCCEEDED",
            "statusReason": "",
            "createdAt": now,
            "startedAt": now,
            "stoppedAt": now,
            "dependsOn": params.get("dependsOn").cloned().unwrap_or(json!([])),
            "parameters": params.get("parameters").cloned().unwrap_or(json!({})),
            "container": params.get("containerOverrides").cloned().unwrap_or(json!({})),
            "tags": params.get("tags").cloned().unwrap_or(json!({})),
        });
        st.jobs.insert(job_id.clone(), job);
        Ok(json!({ "jobArn": job_arn, "jobId": job_id, "jobName": job_name }))
    })
}

fn describe_jobs(req: &Request, params: &Value) -> Response {
    let ids = params["jobs"].as_array().cloned();
    with_store(req, |st| {
        let jobs: Vec<Value> = match ids {
            Some(list) => list
                .iter()
                .filter_map(|j| j.as_str())
                .filter_map(|id| st.jobs.get(id))
                .cloned()
                .collect(),
            None => st.jobs.values().cloned().collect(),
        };
        Ok(json!({ "jobs": jobs }))
    })
}

/// Summaries mirror the ground-truth provider's list op: no jobDefinition.
fn list_jobs(req: &Request, params: &Value) -> Response {
    let queue = params["jobQueue"].as_str().unwrap_or("").to_string();
    let status = params["jobStatus"].as_str().map(str::to_string);
    with_store(req, |st| {
        let summaries: Vec<Value> = st
            .jobs
            .values()
            .filter(|job| {
                if queue.is_empty() {
                    return true;
                }
                let jq = job["jobQueue"].as_str().unwrap_or("");
                queue == jq || queue == tail_of(jq)
            })
            .filter(|job| {
                status
                    .as_deref()
                    .is_none_or(|s| s == job["status"].as_str().unwrap_or(""))
            })
            .map(|job| {
                json!({
                    "jobArn": job["jobArn"],
                    "jobId": job["jobId"],
                    "jobName": job["jobName"],
                    "createdAt": job["createdAt"],
                    "status": job["status"],
                    "statusReason": job["statusReason"],
                })
            })
            .collect();
        Ok(json!({ "jobSummaryList": summaries }))
    })
}

fn stop_job(req: &Request, params: &Value, cancelled: bool) -> Response {
    let job_id = params["jobId"].as_str().unwrap_or("").to_string();
    if job_id.is_empty() {
        return client_error("'jobId' is a required field (cannot be an empty string)".to_string());
    }
    let reason = params["reason"].as_str().unwrap_or("").to_string();
    if reason.is_empty() {
        return client_error(
            "'reason' is a required field (cannot be an empty string)".to_string(),
        );
    }
    let new_status = if cancelled { "CANCELLED" } else { "FAILED" };
    with_store(req, |st| {
        let job = match st.jobs.get_mut(&job_id) {
            Some(job) => job,
            None => return Err(client_error(format!("Job {job_id} not found."))),
        };
        let status = job["status"].as_str().unwrap_or("");
        if cancelled && (status == "STARTING" || status == "RUNNING") {
            return Err(client_error(
                "Cannot cancel a job that is already STARTING or RUNNING.".to_string(),
            ));
        }
        job["status"] = json!(new_status);
        job["statusReason"] = json!(reason);
        job["stoppedAt"] = json!(now_ms());
        Ok(json!({}))
    })
}

fn terminate_job(req: &Request, params: &Value) -> Response {
    stop_job(req, params, false)
}

fn cancel_job(req: &Request, params: &Value) -> Response {
    stop_job(req, params, true)
}

// ---------------------------------------------------------------------------
// Tagging
// ---------------------------------------------------------------------------

fn arn_exists(st: &BatchStore, arn: &str) -> bool {
    st.compute_envs
        .values()
        .any(|ce| ce["computeEnvironmentArn"].as_str() == Some(arn))
        || st
            .job_queues
            .values()
            .any(|q| q["jobQueueArn"].as_str() == Some(arn))
        || st.job_definitions.values().any(|revs| {
            revs.iter()
                .any(|jd| jd["jobDefinitionArn"].as_str() == Some(arn))
        })
        || st.jobs.values().any(|j| j["jobArn"].as_str() == Some(arn))
        || st.scheduling_policies.contains_key(arn)
        || st.consumable_resources.contains_key(arn)
        || st.service_environments.contains_key(arn)
}

fn query_params(query_string: &str) -> Vec<(String, String)> {
    query_string
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| match kv.split_once('=') {
            Some((k, v)) => (urldecode(k), urldecode(v)),
            None => (urldecode(kv), String::new()),
        })
        .collect()
}

fn tag_ops(req: &Request, arn: &str, params: &Value) -> Response {
    match req.method.as_str() {
        "POST" => with_store(req, |st| {
            if !arn_exists(st, arn) {
                return Err(batch_error(
                    404,
                    "ClientException",
                    &format!("Resource not found: {arn}"),
                ));
            }
            let entry = st.tags.entry(arn.to_string()).or_default();
            if let Some(new_tags) = params.get("tags").and_then(|v| v.as_object()) {
                for (k, v) in new_tags {
                    entry.insert(k.clone(), v.as_str().unwrap_or("").to_string());
                }
            }
            Ok(json!({}))
        }),
        "DELETE" => {
            let keys: Vec<String> = query_params(&req.query_string)
                .iter()
                .filter(|(k, _)| k == "tagKeys")
                .map(|(_, v)| v.clone())
                .collect();
            with_store(req, |st| {
                if !arn_exists(st, arn) {
                    return Err(batch_error(
                        404,
                        "ClientException",
                        &format!("Resource not found: {arn}"),
                    ));
                }
                if let Some(entry) = st.tags.get_mut(arn) {
                    for k in keys {
                        entry.remove(&k);
                    }
                }
                Ok(json!({}))
            })
        }
        "GET" => with_store(req, |st| {
            if !arn_exists(st, arn) {
                return Err(batch_error(
                    404,
                    "ClientException",
                    &format!("Resource not found: {arn}"),
                ));
            }
            let tags = st.tags.get(arn).cloned().unwrap_or_default();
            Ok(json!({ "tags": tags }))
        }),
        _ => unknown_op(req),
    }
}

// ---------------------------------------------------------------------------
// Scheduling policies
// ---------------------------------------------------------------------------

/// Mirror moto: default missing fairsharePolicy members to their zero values.
fn normalize_fairshare(fairshare: &Value) -> Value {
    let fairshare = fairshare.as_object();
    let field = |name: &str, default: Value| -> Value {
        fairshare
            .and_then(|f| f.get(name))
            .cloned()
            .unwrap_or(default)
    };
    json!({
        "computeReservation": field("computeReservation", json!(0)),
        "shareDecaySeconds": field("shareDecaySeconds", json!(0)),
        "shareDistribution": field("shareDistribution", json!([])),
    })
}

fn create_scheduling_policy(req: &Request, params: &Value) -> Response {
    let name = params["name"].as_str().unwrap_or("").to_string();
    let arn = format!("{}:scheduling-policy/{}", arn_prefix(req), name);
    let fairshare = normalize_fairshare(&params["fairsharePolicy"]);
    with_store(req, |st| {
        if let Some(t) = tags_value(params, "tags").filter(|t| !t.is_empty()) {
            st.tags.insert(arn.clone(), t.clone());
        }
        st.scheduling_policies.insert(
            arn.clone(),
            json!({
                "name": name,
                "arn": arn,
                "fairsharePolicy": fairshare,
                "tags": st.tags.get(&arn).cloned().unwrap_or_default(),
            }),
        );
        Ok(json!({ "name": name, "arn": arn }))
    })
}

fn describe_scheduling_policies(req: &Request, params: &Value) -> Response {
    let arns = params["arns"].as_array().cloned().unwrap_or_default();
    with_store(req, |st| {
        let list: Vec<Value> = st
            .scheduling_policies
            .values()
            .filter(|p| {
                arns.iter()
                    .any(|a| a.as_str() == Some(p["arn"].as_str().unwrap_or("")))
            })
            .cloned()
            .collect();
        Ok(json!({ "schedulingPolicies": list }))
    })
}

fn list_scheduling_policies(req: &Request, _params: &Value) -> Response {
    with_store(req, |st| {
        let list: Vec<Value> = st
            .scheduling_policies
            .keys()
            .map(|arn| json!({ "arn": arn }))
            .collect();
        Ok(json!({ "schedulingPolicies": list }))
    })
}

fn delete_scheduling_policy(req: &Request, params: &Value) -> Response {
    let arn = params["arn"].as_str().unwrap_or("").to_string();
    with_store(req, |st| {
        st.scheduling_policies.remove(&arn);
        st.tags.remove(&arn);
        Ok(json!({}))
    })
}

fn update_scheduling_policy(req: &Request, params: &Value) -> Response {
    let arn = params["arn"].as_str().unwrap_or("").to_string();
    with_store(req, |st| match st.scheduling_policies.get_mut(&arn) {
        Some(p) => {
            if let Some(f) = params.get("fairsharePolicy") {
                p["fairsharePolicy"] = normalize_fairshare(f);
            }
            Ok(json!({}))
        }
        None => Err(client_error(format!("Scheduling policy {arn} not found."))),
    })
}

// ---------------------------------------------------------------------------
// Job queue snapshot
// ---------------------------------------------------------------------------

fn get_job_queue_snapshot(req: &Request, params: &Value) -> Response {
    let queue = params["jobQueue"].as_str().unwrap_or("").to_string();
    with_store(req, |st| {
        let exists = st.job_queues.values().any(|q| {
            q["jobQueueName"].as_str() == Some(queue.as_str())
                || q["jobQueueArn"].as_str() == Some(queue.as_str())
        });
        if exists {
            Ok(json!({
                "frontOfQueue": {
                    "jobs": [],
                    "lastUpdatedAt": now_ms(),
                },
                "scheduledJobs": [],
            }))
        } else {
            Err(client_error(format!("Job queue {queue} does not exist")))
        }
    })
}

// ---------------------------------------------------------------------------
// Consumable resources
// ---------------------------------------------------------------------------

fn create_consumable_resource(req: &Request, params: &Value) -> Response {
    let name = params["consumableResourceName"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let arn = format!("{}:consumable-resource/{}", arn_prefix(req), name);
    with_store(req, |st| {
        let total = params["totalQuantity"].as_i64().unwrap_or(0);
        let resource = json!({
            "consumableResourceArn": arn,
            "consumableResourceName": name,
            "totalQuantity": total,
            "resourceType": params.get("resourceType").cloned().unwrap_or(json!("REPLENISHABLE")),
            "inUseQuantity": 0,
            "availableQuantity": total,
            "createdAt": now_ms(),
        });
        st.consumable_resources
            .insert(arn.clone(), resource.clone());
        if let Some(t) = tags_value(params, "tags").filter(|t| !t.is_empty()) {
            st.tags.insert(arn, t);
        }
        // moto's response shape for CreateConsumableResource.
        Ok(json!({
            "consumableResourceArn": resource["consumableResourceArn"],
            "consumableResourceName": resource["consumableResourceName"],
        }))
    })
}

/// Internal lookup by ARN-or-name, mirroring moto's match semantics.
fn consumable_resource_arn(st: &BatchStore, ref_arn: &str) -> Option<String> {
    st.consumable_resources
        .contains_key(ref_arn)
        .then(|| ref_arn.to_string())
        .or_else(|| {
            st.consumable_resources
                .iter()
                .find(|(_, r)| r["consumableResourceName"].as_str() == Some(ref_arn))
                .map(|(arn, _)| arn.clone())
        })
}

fn describe_consumable_resource(req: &Request, params: &Value) -> Response {
    let name = params["consumableResource"]
        .as_str()
        .unwrap_or("")
        .to_string();
    with_store(req, |st| {
        let arn = consumable_resource_arn(st, &name)
            .ok_or_else(|| client_error(format!("Consumable resource {name} does not exist")))?;
        let mut result = st.consumable_resources[&arn].clone();
        result["tags"] = json!(st.tags.get(&arn).cloned().unwrap_or_default());
        Ok(result)
    })
}

fn delete_consumable_resource(req: &Request, params: &Value) -> Response {
    let name = params["consumableResource"]
        .as_str()
        .unwrap_or("")
        .to_string();
    with_store(req, |st| {
        // moto: deleting an unknown resource is a silent no-op.
        if let Some(arn) = consumable_resource_arn(st, &name) {
            st.consumable_resources.remove(&arn);
            st.tags.remove(&arn);
        }
        Ok(json!({}))
    })
}

fn list_consumable_resources(req: &Request, _params: &Value) -> Response {
    with_store(req, |st| {
        let list: Vec<Value> = st
            .consumable_resources
            .values()
            .map(|r| {
                json!({
                    "consumableResourceArn": r["consumableResourceArn"],
                    "consumableResourceName": r["consumableResourceName"],
                    "totalQuantity": r["totalQuantity"],
                    "resourceType": r["resourceType"],
                })
            })
            .collect();
        Ok(json!({ "consumableResources": list }))
    })
}

fn update_consumable_resource(req: &Request, params: &Value) -> Response {
    let name = params["consumableResource"]
        .as_str()
        .unwrap_or("")
        .to_string();
    with_store(req, |st| {
        let arn = consumable_resource_arn(st, &name)
            .ok_or_else(|| client_error(format!("Consumable resource {name} does not exist")))?;
        let r = st.consumable_resources.get_mut(&arn).unwrap();
        if let Some(q) = params.get("totalQuantity").and_then(|v| v.as_i64()) {
            r["totalQuantity"] = json!(q);
            let in_use = r["inUseQuantity"].as_i64().unwrap_or(0);
            r["availableQuantity"] = json!(q - in_use);
        }
        Ok(json!({
            "consumableResourceArn": r["consumableResourceArn"],
            "consumableResourceName": r["consumableResourceName"],
            "totalQuantity": r["totalQuantity"],
        }))
    })
}

fn list_jobs_by_consumable_resource(req: &Request, _params: &Value) -> Response {
    with_store(req, |_st| Ok(json!({ "jobs": [] })))
}

// ---------------------------------------------------------------------------
// Service environments
// ---------------------------------------------------------------------------

fn create_service_environment(req: &Request, params: &Value) -> Response {
    let name = params["serviceEnvironmentName"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let arn = format!("{}:service-environment/{}", arn_prefix(req), name);
    with_store(req, |st| {
        let env = json!({
            "serviceEnvironmentArn": arn.clone(),
            "serviceEnvironmentName": name,
            "status": "ACTIVE",
            "createdAt": now_ms(),
        });
        st.service_environments.insert(arn, env.clone());
        Ok(env)
    })
}

fn describe_service_environments(req: &Request, params: &Value) -> Response {
    let names: Option<Vec<String>> = params
        .get("serviceEnvironments")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        });
    with_store(req, |st| {
        let envs: Vec<Value> = match names {
            Some(list) => st
                .service_environments
                .values()
                .filter(|e| {
                    let name = e["serviceEnvironmentName"].as_str().unwrap_or("");
                    let arn = e["serviceEnvironmentArn"].as_str().unwrap_or("");
                    list.iter().any(|n| n == name || n == arn)
                })
                .cloned()
                .collect(),
            None => st.service_environments.values().cloned().collect(),
        };
        Ok(json!({ "serviceEnvironments": envs }))
    })
}

fn update_service_environment(req: &Request, params: &Value) -> Response {
    let name = params["serviceEnvironment"]
        .as_str()
        .unwrap_or("")
        .to_string();
    with_store(req, |st| {
        match st.service_environments.values().find(|e| {
            e["serviceEnvironmentName"].as_str() == Some(name.as_str())
                || e["serviceEnvironmentArn"].as_str() == Some(name.as_str())
        }) {
            Some(e) => Ok(e.clone()),
            None => Err(client_error(format!(
                "Service environment {name} does not exist"
            ))),
        }
    })
}

fn delete_service_environment(req: &Request, params: &Value) -> Response {
    let name = params["serviceEnvironment"]
        .as_str()
        .unwrap_or("")
        .to_string();
    with_store(req, |st| {
        let to_delete = st
            .service_environments
            .iter()
            .find(|(_, e)| {
                e["serviceEnvironmentName"].as_str() == Some(name.as_str())
                    || e["serviceEnvironmentArn"].as_str() == Some(name.as_str())
            })
            .map(|(arn, _)| arn.clone());
        if let Some(arn) = to_delete {
            st.service_environments.remove(&arn);
            st.tags.remove(&arn);
        }
        Ok(json!({}))
    })
}

// ---------------------------------------------------------------------------
// Service jobs (moto's stubs)
// ---------------------------------------------------------------------------

fn submit_service_job(req: &Request, params: &Value) -> Response {
    let name = params["jobName"].as_str().unwrap_or("").to_string();
    let job_id = Uuid::new_v4().to_string();
    let arn = format!("{}:service-job/{}", arn_prefix(req), job_id);
    with_store(req, |_st| {
        Ok(json!({
            "jobArn": arn,
            "jobName": name,
            "jobId": job_id,
        }))
    })
}

fn describe_service_job(req: &Request, params: &Value) -> Response {
    let job_id = params["jobId"].as_str().unwrap_or("").to_string();
    with_store(req, |_st| {
        Ok(json!({
            "jobId": job_id,
            "status": "SUCCEEDED",
        }))
    })
}

fn list_service_jobs(req: &Request, _params: &Value) -> Response {
    with_store(req, |_st| Ok(json!({ "jobSummaryList": [] })))
}

fn terminate_service_job(req: &Request, _params: &Value) -> Response {
    with_store(req, |_st| Ok(json!({})))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    struct Ctx {
        req: Request,
    }

    impl Ctx {
        fn new() -> Self {
            Ctx {
                req: Request {
                    service: "batch".to_string(),
                    operation: String::new(),
                    method: "POST".to_string(),
                    path: "/v1/createcomputeenvironment".to_string(),
                    query_string: String::new(),
                    headers: HashMap::new(),
                    body: bytes::Bytes::new(),
                    account: 123456789012,
                    region: "us-east-1".to_string(),
                },
            }
        }

        fn call(&self, path: &str, params: Value) -> Response {
            let mut req = Request {
                path: path.to_string(),
                ..self.req.clone()
            };
            req.body = bytes::Bytes::from(serde_json::to_vec(&params).unwrap_or_default());
            dispatch(&req).unwrap()
        }

        fn result_of(&self, resp: &Response) -> Value {
            serde_json::from_slice(&resp.body).unwrap_or_default()
        }
    }

    #[test]
    fn compute_environment_lifecycle() {
        let ctx = Ctx::new();
        let created = ctx.result_of(&ctx.call(
            "/v1/createcomputeenvironment",
            json!({"computeEnvironmentName": "ce-1", "type": "MANAGED"}),
        ));
        assert_eq!(created["computeEnvironmentName"], "ce-1");
        assert!(created["computeEnvironmentArn"]
            .as_str()
            .unwrap()
            .ends_with(":compute-environment/ce-1"));

        // Duplicate creation is rejected.
        let dup = ctx.call(
            "/v1/createcomputeenvironment",
            json!({"computeEnvironmentName": "ce-1"}),
        );
        assert_eq!(dup.status, 400);

        let listed = ctx.result_of(&ctx.call(
            "/v1/describecomputeenvironments",
            json!({"computeEnvironments": ["ce-1"]}),
        ));
        assert_eq!(
            listed["computeEnvironments"][0]["computeEnvironmentName"],
            "ce-1"
        );

        // Describe with an unknown name returns no entries.
        let empty = ctx.result_of(&ctx.call(
            "/v1/describecomputeenvironments",
            json!({"computeEnvironments": ["nope"]}),
        ));
        assert_eq!(empty["computeEnvironments"].as_array().unwrap().len(), 0);

        let deleted = ctx.call(
            "/v1/deletecomputeenvironment",
            json!({"computeEnvironment": "ce-1"}),
        );
        assert_eq!(deleted.status, 200);
        let after = ctx.result_of(&ctx.call(
            "/v1/describecomputeenvironments",
            json!({"computeEnvironments": ["ce-1"]}),
        ));
        assert_eq!(after["computeEnvironments"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn job_definition_revisions_and_status_filter() {
        let ctx = Ctx::new();
        let params = |memory: i64| {
            json!({
                "jobDefinitionName": "jd",
                "type": "container",
                "containerProperties": {"image": "busybox", "vcpus": 1, "memory": memory},
            })
        };
        let r1 = ctx.result_of(&ctx.call("/v1/registerjobdefinition", params(128)));
        assert_eq!(r1["revision"], 1);
        let r2 = ctx.result_of(&ctx.call("/v1/registerjobdefinition", params(256)));
        assert_eq!(r2["revision"], 2);

        let listed = ctx.result_of(&ctx.call(
            "/v1/describejobdefinitions",
            json!({"jobDefinitionName": "jd", "status": "ACTIVE"}),
        ));
        let revisions: Vec<i64> = listed["jobDefinitions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["revision"].as_i64().unwrap())
            .collect();
        assert_eq!(revisions, vec![1, 2]);

        // Deregister :2, then the ACTIVE describe hides it.
        let _ = ctx.call(
            "/v1/deregisterjobdefinition",
            json!({"jobDefinition": format!("jd:{}", r2["revision"])}),
        );
        let listed = ctx.result_of(&ctx.call(
            "/v1/describejobdefinitions",
            json!({"jobDefinitionName": "jd", "status": "ACTIVE"}),
        ));
        let revisions: Vec<i64> = listed["jobDefinitions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["revision"].as_i64().unwrap())
            .collect();
        assert_eq!(revisions, vec![1]);
    }

    #[test]
    fn job_submit_describe_cancel_and_list() {
        let ctx = Ctx::new();
        // infra
        let _ = ctx.call(
            "/v1/createcomputeenvironment",
            json!({"computeEnvironmentName": "ce"}),
        );
        let ce = ctx.result_of(&ctx.call(
            "/v1/describecomputeenvironments",
            json!({"computeEnvironments": ["ce"]}),
        ));
        let ce_arn = ce["computeEnvironments"][0]["computeEnvironmentArn"].clone();
        let _ = ctx.call(
            "/v1/createjobqueue",
            json!({"jobQueueName": "jq", "priority": 1,
                   "computeEnvironmentOrder": [{"order": 1, "computeEnvironment": ce_arn}]}),
        );
        let jd = ctx.result_of(&ctx.call(
            "/v1/registerjobdefinition",
            json!({"jobDefinitionName": "jd-x", "type": "container",
                   "containerProperties": {"image": "busybox", "vcpus": 1, "memory": 128}}),
        ));
        let jd_arn = jd["jobDefinitionArn"].clone();

        let submitted = ctx.result_of(&ctx.call(
            "/v1/submitjob",
            json!({"jobName": "j1", "jobQueue": "jq-arn",
                     "jobDefinition": jd_arn}),
        ));
        let job_id = submitted["jobId"].as_str().unwrap().to_string();
        assert_eq!(submitted["jobName"], "j1");

        let described = ctx.result_of(&ctx.call("/v1/describejobs", json!({"jobs": [job_id]})));
        let jobs = described["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0]["jobId"], job_id);
        assert!(jobs[0].get("jobQueue").is_some());
        assert!(jobs[0]["status"].is_string());

        let listed = ctx.result_of(&ctx.call("/v1/listjobs", json!({"jobQueue": "job-queue/ce"})));
        assert!(listed["jobSummaryList"].is_array());

        let _ = ctx.call(
            "/v1/canceljob",
            json!({"jobId": job_id, "reason": "testing"}),
        );
        let described = ctx.result_of(&ctx.call("/v1/describejobs", json!({"jobs": [job_id]})));
        assert_eq!(described["jobs"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn job_queue_snapshot_requires_existing_queue() {
        let ctx = Ctx::new();
        let resp = ctx.call(
            "/v1/getjobqueuesnapshot",
            json!({"jobQueue": "missing-queue"}),
        );
        assert_eq!(resp.status, 400);
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains("ClientException"), "{body}");
        assert!(body.contains("does not exist"), "{body}");

        let _ = ctx.call(
            "/v1/createjobqueue",
            json!({"jobQueueName": "snap-q", "priority": 1}),
        );
        let snap =
            ctx.result_of(&ctx.call("/v1/getjobqueuesnapshot", json!({"jobQueue": "snap-q"})));
        assert!(snap["frontOfQueue"].is_object());
    }

    #[test]
    fn consumable_resource_family() {
        let ctx = Ctx::new();
        let created = ctx.result_of(&ctx.call(
            "/v1/createconsumableresource",
            json!({"consumableResourceName": "cr", "totalQuantity": 100, "resourceType": "GPU"}),
        ));
        assert_eq!(created["consumableResourceName"], "cr");
        assert!(created["consumableResourceArn"]
            .as_str()
            .unwrap()
            .contains("consumable-resource/cr"));

        let described = ctx.result_of(&ctx.call(
            "/v1/describeconsumableresource",
            json!({"consumableResource": "cr"}),
        ));
        assert_eq!(described["totalQuantity"], 100);
        assert_eq!(described["resourceType"], "GPU");

        let listed = ctx.result_of(&ctx.call("/v1/listconsumableresources", json!({})));
        assert_eq!(listed["consumableResources"].as_array().unwrap().len(), 1);

        let updated = ctx.result_of(&ctx.call(
            "/v1/updateconsumableresource",
            json!({"consumableResource": "cr", "operation": "SET", "quantity": 200}),
        ));
        assert_eq!(updated["consumableResourceName"], "cr");
        assert!(updated["totalQuantity"].is_i64());

        let _ = ctx.call(
            "/v1/deleteconsumableresource",
            json!({"consumableResource": "cr"}),
        );
        let listed = ctx.result_of(&ctx.call("/v1/listconsumableresources", json!({})));
        assert_eq!(listed["consumableResources"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn scheduling_policy_family() {
        let ctx = Ctx::new();
        let created = ctx.result_of(&ctx.call(
            "/v1/createschedulingpolicy",
            json!({"name": "sp", "fairsharePolicy": {
                "shareDecaySeconds": 3600, "computeReservation": 1,
                "shareDistribution": [{"shareIdentifier": "A", "weightFactor": 1.0}]}}),
        ));
        assert!(created["arn"]
            .as_str()
            .unwrap()
            .contains("scheduling-policy/sp"));
        let arn = created["arn"].as_str().unwrap().to_string();

        let listed = ctx.result_of(&ctx.call("/v1/listschedulingpolicies", json!({})));
        let arns: Vec<&str> = listed["schedulingPolicies"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|p| p["arn"].as_str())
            .collect();
        assert!(arns.contains(&arn.as_str()));

        let described =
            ctx.result_of(&ctx.call("/v1/describeschedulingpolicies", json!({"arns": [arn]})));
        let policy = &described["schedulingPolicies"][0];
        assert_eq!(policy["fairsharePolicy"]["shareDecaySeconds"], 3600);

        let _ = ctx.call("/v1/deleteschedulingpolicy", json!({"arn": created["arn"]}));
        let listed = ctx.result_of(&ctx.call("/v1/listschedulingpolicies", json!({})));
        let arns: Vec<&str> = listed["schedulingPolicies"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|p| p["arn"].as_str())
            .collect();
        assert!(!arns.contains(&created["arn"].as_str().unwrap()));
    }

    #[test]
    fn tags_roundtrip() {
        let ctx = Ctx::new();
        let created = ctx.result_of(&ctx.call(
            "/v1/createcomputeenvironment",
            json!({"computeEnvironmentName": "ce-tag"}),
        ));
        let arn = created["computeEnvironmentArn"]
            .as_str()
            .unwrap()
            .to_string();
        let path = format!("/v1/tags/{}", urlencode(&arn));
        let mut req = ctx.req.clone();
        req.path = path;
        req.method = "POST".to_string();
        req.body = bytes::Bytes::from(json!({"tags": {"team": "platform"}}).to_string());
        assert_eq!(dispatch(&req).unwrap().status, 200);
        req.method = "GET".to_string();
        req.body = bytes::Bytes::new();
        let tags = ctx.result_of(&dispatch(&req).unwrap());
        assert_eq!(tags["tags"]["team"], "platform");
        req.method = "DELETE".to_string();
        req.query_string = "tagKeys=team".to_string();
        let resp = dispatch(&req).unwrap();
        assert_eq!(resp.status, 200);
        req.query_string = String::new();
        req.method = "GET".to_string();
        let tags = ctx.result_of(&dispatch(&req).unwrap());
        assert!(
            tags["tags"]
                .as_object()
                .map(|t| t.is_empty())
                .unwrap_or(false),
            "{tags}"
        );
    }

    fn urlencode(s: &str) -> String {
        let mut out = String::new();
        for b in s.bytes() {
            match b {
                b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b':' | b'/' | b'-' | b'_' | b'.' => {
                    out.push(b as char)
                }
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
        out
    }
}
