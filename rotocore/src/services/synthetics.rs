//! CloudWatch Synthetics backend (REST-JSON, one URI per operation).
//!
//! Port of moto's `synthetics` backend: canaries (create/get/update/delete,
//! start/stop/dry-run, runs), groups and their resource associations,
//! runtime versions, describe lists and the tag routes. Wire shapes mirror
//! moto's UpperCamel JSON keys; every moto error renders as HTTP 400 with
//! `__type` + `message` (moto's `JsonRESTError`), `ResourceNotFoundException`
//! for unknown canary/group identifiers.
//!
//! Requests enter with the synthetics sigv4 credential scope (the gateway
//! carries no REST op table for this service), so the operation is resolved
//! from the HTTP method + botocore requestUri segments; parameters are path
//! members. State is per-(account, region), keyed by canary/group name like
//! moto's backend dicts, plus a tagger map keyed by ARN.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Run {
    id: String,
    name: String,
    started: String,
    completed: String,
}

impl Run {
    /// moto's `CanaryRun.to_dict()`.
    fn to_wire(&self) -> Value {
        json!({
            "Id": self.id,
            "Name": self.name,
            "Status": {"State": "PASSED", "StateReason": "", "StateReasonCode": ""},
            "Timeline": {"Started": self.started, "Completed": self.completed},
            "ArtifactS3Location": "s3://cw-syn-results/canary",
        })
    }
}

struct Canary {
    name: String,
    id: String,
    code: Value,
    artifact_s3_location: String,
    execution_role_arn: String,
    schedule: Value,
    run_config: Value,
    success_retention: i64,
    failure_retention: i64,
    runtime_version: String,
    vpc_config: Option<Value>,
    resources_to_replicate_tags: Option<Value>,
    provisioned_resource_cleanup: Option<String>,
    browser_configs: Option<Value>,
    tags: BTreeMap<String, String>,
    artifact_config: Option<Value>,
    state: &'static str,
    arn: String,
    runs: Vec<Run>,
    last_run: Option<Run>,
    created: String,
    last_modified: String,
    last_started: Option<String>,
    last_stopped: Option<String>,
}

impl Canary {
    /// moto's create-time defaults: artifact location "s3://dummy", role
    /// fallback, `rate(5 minutes)` schedule, 60s run timeout, 31-day
    /// retention, `syn-nodejs-puppeteer-3.8` runtime, state READY.
    fn create(name: &str, p: &Value, region: &str, account: u64) -> Canary {
        Canary {
            name: name.to_string(),
            id: uuid::Uuid::new_v4().to_string(),
            code: p.get("Code").cloned().unwrap_or_else(|| json!({})),
            artifact_s3_location: param_str(p, "ArtifactS3Location")
                .unwrap_or("s3://dummy")
                .to_string(),
            execution_role_arn: param_str(p, "ExecutionRoleArn")
                .unwrap_or("arn:aws:iam::123:role/service-role")
                .to_string(),
            schedule: p
                .get("Schedule")
                .cloned()
                .unwrap_or_else(|| json!({"Expression": "rate(5 minutes)"})),
            run_config: p
                .get("RunConfig")
                .cloned()
                .unwrap_or_else(|| json!({"TimeoutInSeconds": 60})),
            success_retention: p
                .get("SuccessRetentionPeriodInDays")
                .and_then(Value::as_i64)
                .unwrap_or(31),
            failure_retention: p
                .get("FailureRetentionPeriodInDays")
                .and_then(Value::as_i64)
                .unwrap_or(31),
            runtime_version: param_str(p, "RuntimeVersion")
                .unwrap_or("syn-nodejs-puppeteer-3.8")
                .to_string(),
            vpc_config: p.get("VpcConfig").cloned(),
            resources_to_replicate_tags: p.get("ResourcesToReplicateTags").cloned(),
            provisioned_resource_cleanup: p
                .get("ProvisionedResourceCleanup")
                .and_then(Value::as_str)
                .map(str::to_string),
            browser_configs: p.get("BrowserConfigs").cloned(),
            tags: p
                .get("Tags")
                .map(|t| tags_from(t.clone()))
                .unwrap_or_default(),
            artifact_config: p.get("ArtifactConfig").cloned(),
            state: "READY",
            arn: canary_arn(region, account, name),
            runs: Vec::new(),
            last_run: None,
            created: crate::proto::now_iso(),
            last_modified: crate::proto::now_iso(),
            last_started: None,
            last_stopped: None,
        }
    }

    /// moto's `Canary.to_dict()`: provided members rendered verbatim
    /// (`BrowserConfigs` empty list when absent, `VpcConfig` and
    /// `ArtifactConfig` null when absent).
    fn to_wire(&self) -> Value {
        json!({
            "Id": self.id,
            "Name": self.name,
            "Code": self.code,
            "ArtifactS3Location": self.artifact_s3_location,
            "ExecutionRoleArn": self.execution_role_arn,
            "Schedule": self.schedule,
            "RunConfig": self.run_config,
            "SuccessRetentionPeriodInDays": self.success_retention,
            "FailureRetentionPeriodInDays": self.failure_retention,
            "RuntimeVersion": self.runtime_version,
            "VpcConfig": self.vpc_config.clone().unwrap_or(Value::Null),
            "ResourcesToReplicateTags": self
                .resources_to_replicate_tags
                .clone()
                .unwrap_or(Value::Null),
            "ProvisionedResourceCleanup": json!(self.provisioned_resource_cleanup),
            "BrowserConfigs": self.browser_configs.clone().unwrap_or_else(|| json!([])),
            "Tags": tags_wire(&self.tags),
            "ArtifactConfig": self.artifact_config.clone().unwrap_or(Value::Null),
            "Status": {
                "State": self.state,
                "StateReason": "Created by Moto",
                "StateReasonCode": "CREATE_COMPLETE",
            },
            "Timeline": {
                "Created": self.created,
                "LastModified": self.last_modified,
                "LastStarted": json!(self.last_started),
                "LastStopped": json!(self.last_stopped),
            },
        })
    }
}

struct Group {
    name: String,
    id: String,
    arn: String,
    tags: BTreeMap<String, String>,
    resource_arns: Vec<String>,
    created: String,
    last_modified: String,
}

impl Group {
    fn create(name: &str, p: &Value, region: &str, account: u64) -> Group {
        Group {
            name: name.to_string(),
            id: uuid::Uuid::new_v4().to_string(),
            arn: group_arn(region, account, name),
            tags: p
                .get("Tags")
                .map(|t| tags_from(t.clone()))
                .unwrap_or_default(),
            resource_arns: Vec::new(),
            created: crate::proto::now_iso(),
            last_modified: crate::proto::now_iso(),
        }
    }

    /// moto's `Group.to_dict()`.
    fn to_wire(&self) -> Value {
        json!({
            "Id": self.id,
            "Name": self.name,
            "Arn": self.arn,
            "Tags": tags_wire(&self.tags),
            "CreatedTime": self.created,
            "LastModifiedTime": self.last_modified,
        })
    }

    /// moto's `Group.summary_dict()`.
    fn summary(&self) -> Value {
        json!({"Id": self.id, "Name": self.name, "Arn": self.arn})
    }
}

#[derive(Default)]
struct State {
    canaries: BTreeMap<String, Canary>,
    groups: BTreeMap<String, Group>,
    /// moto's TaggingService: resource ARN -> {tag key: value}.
    tagger: BTreeMap<String, BTreeMap<String, String>>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("synthetics"));
    SLOT.clone()
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn not_found(message: String) -> Response {
    crate::proto::json_error(400, "ResourceNotFoundException", &message)
}

fn not_implemented(op: &str) -> Response {
    crate::proto::json_error(
        501,
        "NotImplemented",
        &format!("The {op} action has not been implemented"),
    )
}

fn tags_from(v: Value) -> BTreeMap<String, String> {
    v.as_object()
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn tags_wire(tags: &BTreeMap<String, String>) -> Value {
    tags.iter()
        .map(|(k, v)| (k.clone(), json!(v)))
        .collect::<serde_json::Map<String, Value>>()
        .into()
}

fn param_str<'a>(p: &'a Value, key: &str) -> Option<&'a str> {
    p.get(key).and_then(Value::as_str)
}

/// The `Names` describe filter (absent = no filter; empty = filter to
/// nothing, per moto's truthiness on `params.get("Names")`).
fn param_names(p: &Value) -> Option<Vec<String>> {
    p.get("Names").and_then(Value::as_array).map(|a| {
        a.iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect()
    })
}

fn canary_arn(region: &str, account: u64, name: &str) -> String {
    format!(
        "arn:{}:synthetics:{region}:{account}:canary:{name}",
        partition(region)
    )
}

fn group_arn(region: &str, account: u64, name: &str) -> String {
    format!(
        "arn:{}:synthetics:{region}:{account}:group:{name}",
        partition(region)
    )
}

fn partition(region: &str) -> &'static str {
    for (prefix, name) in [
        ("cn-", "aws-cn"),
        ("us-gov-", "aws-us-gov"),
        ("us-iso-", "aws-iso"),
        ("us-isob-", "aws-iso-b"),
        ("eusc-", "aws-eusc"),
    ] {
        if region.starts_with(prefix) {
            return name;
        }
    }
    "aws"
}

/// Runtime versions, from moto's `models.RUNTIME_VERSIONS`: (VersionName,
/// Description, ReleaseDate, DeprecationDate).
const RUNTIME_VERSIONS: &[(&str, &str, &str, Option<&str>)] = &[
    (
        "syn-nodejs-puppeteer-9.1",
        "Node.js Puppeteer 9.1",
        "2024-01-01T00:00:00Z",
        None,
    ),
    (
        "syn-nodejs-puppeteer-7.0",
        "Node.js Puppeteer 7.0",
        "2023-06-01T00:00:00Z",
        None,
    ),
    (
        "syn-nodejs-puppeteer-6.2",
        "Node.js Puppeteer 6.2",
        "2023-01-01T00:00:00Z",
        None,
    ),
    (
        "syn-nodejs-puppeteer-3.8",
        "Node.js Puppeteer 3.8",
        "2022-01-01T00:00:00Z",
        Some("2024-06-01T00:00:00Z"),
    ),
    (
        "syn-python-selenium-3.0",
        "Python Selenium 3.0",
        "2023-03-01T00:00:00Z",
        None,
    ),
];

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "synthetics" {
        return None;
    }
    let slot = state();
    let mut st = slot.write();
    let s = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(s, req))
}

fn handle(st: &mut State, req: &Request) -> Response {
    let path = req.path.trim_end_matches('/');
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let method = req.method.as_str();
    let p = crate::proto::parse_json_body(&req.body);
    let region = req.region.clone();
    let account = req.account;

    match (method, parts.as_slice(), p) {
        // Canary CRUD -------------------------------------------------------
        ("POST", ["canary"], p) => {
            let name = param_str(&p, "Name").unwrap_or_default().to_string();
            let canary = Canary::create(&name, &p, &region, account);
            if !canary.tags.is_empty() {
                st.tagger.insert(canary.arn.clone(), canary.tags.clone());
            }
            st.canaries.insert(name.clone(), canary);
            ok(&json!({"Canary": st.canaries[&name].to_wire()}))
        }
        ("GET", ["canary", name], _) => {
            let name = crate::proto::url_decode(name);
            match st.canaries.get(&name) {
                Some(c) => ok(&json!({"Canary": c.to_wire()})),
                None => not_found(format!("Canary {name} not found")),
            }
        }
        ("DELETE", ["canary", name], _) => {
            let name = crate::proto::url_decode(name);
            match st.canaries.get(&name).map(|c| c.arn.clone()) {
                Some(arn) => {
                    st.tagger.remove(&arn);
                    st.canaries.remove(&name);
                    ok(&json!({}))
                }
                None => not_found(format!("Canary {name} not found")),
            }
        }
        ("PATCH", ["canary", name], p) => {
            let name = crate::proto::url_decode(name);
            match st.canaries.get_mut(&name) {
                Some(c) => {
                    if let Some(v) = p.get("Code") {
                        c.code = v.clone();
                    }
                    if let Some(s) = param_str(&p, "ExecutionRoleArn") {
                        c.execution_role_arn = s.to_string();
                    }
                    if let Some(s) = param_str(&p, "RuntimeVersion") {
                        c.runtime_version = s.to_string();
                    }
                    if let Some(v) = p.get("Schedule") {
                        c.schedule = v.clone();
                    }
                    if let Some(v) = p.get("RunConfig") {
                        c.run_config = v.clone();
                    }
                    if let Some(v) = p
                        .get("SuccessRetentionPeriodInDays")
                        .and_then(Value::as_i64)
                    {
                        c.success_retention = v;
                    }
                    if let Some(v) = p
                        .get("FailureRetentionPeriodInDays")
                        .and_then(Value::as_i64)
                    {
                        c.failure_retention = v;
                    }
                    if let Some(v) = p.get("VpcConfig") {
                        c.vpc_config = Some(v.clone());
                    }
                    if let Some(s) = param_str(&p, "ArtifactS3Location") {
                        c.artifact_s3_location = s.to_string();
                    }
                    if let Some(v) = p.get("ArtifactConfig") {
                        c.artifact_config = Some(v.clone());
                    }
                    if let Some(s) = param_str(&p, "ProvisionedResourceCleanup") {
                        c.provisioned_resource_cleanup = Some(s.to_string());
                    }
                    if let Some(v) = p.get("BrowserConfigs") {
                        c.browser_configs = Some(v.clone());
                    }
                    c.last_modified = crate::proto::now_iso();
                    ok(&json!({}))
                }
                None => not_found(format!("Canary {name} not found")),
            }
        }

        // Canary lifecycle ---------------------------------------------------
        ("POST", ["canary", name, "start"], _) => {
            let name = crate::proto::url_decode(name);
            match st.canaries.get_mut(&name) {
                Some(c) => {
                    c.state = "RUNNING";
                    c.last_started = Some(crate::proto::now_iso());
                    let run = Run {
                        id: uuid::Uuid::new_v4().to_string(),
                        name: c.name.clone(),
                        started: crate::proto::now_iso(),
                        completed: crate::proto::now_iso(),
                    };
                    c.runs.push(run.clone());
                    c.last_run = Some(run);
                    ok(&json!({}))
                }
                None => not_found(format!("Canary {name} not found")),
            }
        }
        ("POST", ["canary", name, "stop"], _) => {
            let name = crate::proto::url_decode(name);
            match st.canaries.get_mut(&name) {
                Some(c) => {
                    c.state = "STOPPED";
                    c.last_stopped = Some(crate::proto::now_iso());
                    ok(&json!({}))
                }
                None => not_found(format!("Canary {name} not found")),
            }
        }
        ("POST", ["canary", name, "dry-run", "start"], _) => {
            let name = crate::proto::url_decode(name);
            match st.canaries.get(&name) {
                Some(_) => {
                    ok(&json!({"DryRunConfig": {"DryRunId": uuid::Uuid::new_v4().to_string()}}))
                }
                None => not_found(format!("Canary {name} not found")),
            }
        }
        ("POST", ["canary", name, "runs"], _) => {
            let name = crate::proto::url_decode(name);
            match st.canaries.get(&name) {
                Some(c) => ok(&json!({
                    "CanaryRuns": c.runs.iter().map(|r| r.to_wire()).collect::<Vec<_>>()
                })),
                None => not_found(format!("Canary {name} not found")),
            }
        }

        // Describes ----------------------------------------------------------
        ("POST", ["canaries"], p) => {
            let names = param_names(&p);
            ok(&json!({"Canaries": st
                .canaries
                .values()
                .filter(|c| names.as_ref().is_none_or(|n| n.is_empty() || n.contains(&c.name)))
                .map(|c| c.to_wire())
                .collect::<Vec<_>>()}))
        }
        ("POST", ["canaries", "last-run"], p) => {
            let names = param_names(&p);
            ok(&json!({"CanariesLastRun": st
                .canaries
                .values()
                .filter(|c| names.as_ref().is_none_or(|n| n.is_empty() || n.contains(&c.name)))
                .map(|c| {
                    let mut entry = json!({"CanaryName": c.name});
                    if let Some(run) = &c.last_run {
                        entry["LastRun"] = run.to_wire();
                    }
                    entry
                })
                .collect::<Vec<_>>()}))
        }
        ("POST", ["runtime-versions"], _) => ok(&json!({"RuntimeVersions": RUNTIME_VERSIONS
            .iter()
            .map(|(name, desc, release, deprecation)| {
                json!({
                    "VersionName": json!(&name),
                    "Description": json!(&desc),
                    "ReleaseDate": json!(&release),
                    "DeprecationDate": json!(&deprecation),
                })
            })
            .collect::<Vec<_>>()})),

        // Groups ---------------------------------------------------------------
        ("POST", ["group"], p) => {
            let name = param_str(&p, "Name").unwrap_or_default().to_string();
            let group = Group::create(&name, &p, &region, account);
            if !group.tags.is_empty() {
                st.tagger.insert(group.arn.clone(), group.tags.clone());
            }
            st.groups.insert(name.clone(), group);
            ok(&json!({"Group": st.groups[&name].to_wire()}))
        }
        ("GET", ["group", id], _) => {
            let id = crate::proto::url_decode(id);
            match find_group(st, &id) {
                Some(g) => ok(&json!({"Group": g.to_wire()})),
                None => not_found(format!("Group {id} not found")),
            }
        }
        ("DELETE", ["group", id], _) => {
            let id = crate::proto::url_decode(id);
            match find_group(st, &id).map(|g| (g.name.clone(), g.arn.clone())) {
                Some((name, arn)) => {
                    st.tagger.remove(&arn);
                    st.groups.remove(&name);
                    ok(&json!({}))
                }
                None => not_found(format!("Group {id} not found")),
            }
        }
        ("PATCH", ["group", id, "associate"], p) => {
            let arn = param_str(&p, "ResourceArn").unwrap_or_default().to_string();
            let id = crate::proto::url_decode(id);
            match find_group_mut(st, &id) {
                Some(g) => {
                    if !g.resource_arns.contains(&arn) {
                        g.resource_arns.push(arn);
                    }
                    g.last_modified = crate::proto::now_iso();
                    ok(&json!({}))
                }
                None => not_found(format!("Group {id} not found")),
            }
        }
        ("PATCH", ["group", id, "disassociate"], p) => {
            let arn = param_str(&p, "ResourceArn").unwrap_or_default().to_string();
            let id = crate::proto::url_decode(id);
            match find_group_mut(st, &id) {
                Some(g) => {
                    g.resource_arns.retain(|a| a != &arn);
                    g.last_modified = crate::proto::now_iso();
                    ok(&json!({}))
                }
                None => not_found(format!("Group {id} not found")),
            }
        }
        ("POST", ["group", id, "resources"], _) => {
            let id = crate::proto::url_decode(id);
            match find_group(st, &id) {
                Some(g) => ok(&json!({"Resources": g.resource_arns})),
                None => not_found(format!("Group {id} not found")),
            }
        }
        ("POST", ["groups"], _) => ok(&json!({"Groups": st
            .groups
            .values()
            .map(|g| g.summary())
            .collect::<Vec<_>>()})),
        ("POST", ["resource", arn, "groups"], _) => {
            let arn = crate::proto::url_decode(arn);
            ok(&json!({"Groups": st
                .groups
                .values()
                .filter(|g| g.resource_arns.contains(&arn))
                .map(|g| g.summary())
                .collect::<Vec<_>>()}))
        }

        // Tags -----------------------------------------------------------------
        ("GET", ["tags", arn], _) => {
            let arn = crate::proto::url_decode(arn);
            let tags = st
                .tagger
                .get(&arn)
                .map(tags_wire)
                .unwrap_or_else(|| json!({}));
            ok(&json!({"Tags": tags}))
        }
        ("POST", ["tags", arn], p) => {
            let arn = crate::proto::url_decode(arn);
            let tags = tags_from(p.get("Tags").cloned().unwrap_or_else(|| json!({})));
            let entry = st.tagger.entry(arn.clone()).or_default();
            for (k, v) in &tags {
                entry.insert(k.clone(), v.clone());
            }
            // Also update the inline tags on canaries/groups.
            if let Some(c) = st.canaries.values_mut().find(|c| c.arn == arn) {
                for (k, v) in tags {
                    c.tags.insert(k, v);
                }
            } else if let Some(g) = st.groups.values_mut().find(|g| g.arn == arn) {
                for (k, v) in tags {
                    g.tags.insert(k, v);
                }
            }
            ok(&json!({}))
        }
        ("DELETE", ["tags", arn], _) => {
            let arn = crate::proto::url_decode(arn);
            let keys: Vec<String> = crate::proto::query_map(&req.query_string)
                .get("tagKeys")
                .cloned()
                .unwrap_or_default();
            if let Some(entry) = st.tagger.get_mut(&arn) {
                for k in &keys {
                    entry.remove(k);
                }
            }
            if let Some(c) = st.canaries.values_mut().find(|c| c.arn == arn) {
                for k in &keys {
                    c.tags.remove(k);
                }
            } else if let Some(g) = st.groups.values_mut().find(|g| g.arn == arn) {
                for k in &keys {
                    g.tags.remove(k);
                }
            }
            ok(&json!({}))
        }

        // Unknown route => moto's dispatch miss: the wire NotImplemented.
        _ => not_implemented(&req.operation),
    }
}

/// Find a group by name or ARN (moto's `get_group`).
fn find_group<'a>(st: &'a State, id: &str) -> Option<&'a Group> {
    st.groups.values().find(|g| g.name == id || g.arn == id)
}

fn find_group_mut<'a>(st: &'a mut State, id: &'a str) -> Option<&'a mut Group> {
    st.groups.values_mut().find(|g| g.name == id || g.arn == id)
}

// ---------------------------------------------------------------------------
// Tests (unique account per test for isolation)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn req(method: &str, path: &str, body: Value) -> Request {
        Request {
            service: "synthetics".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    fn create_canary(name: &str) -> Request {
        req(
            "POST",
            "/canary",
            json!({
                "Name": name,
                "Code": {"S3Bucket": "b", "S3Key": "k", "Handler": "index.handler"},
                "ArtifactS3Location": "s3://b/artifacts",
                "ExecutionRoleArn": "arn:aws:iam::123456789012:role/canary-role",
                "Schedule": {"Expression": "rate(5 minutes)"},
                "RuntimeVersion": "syn-python-selenium-3.0",
            }),
        )
    }

    /// Assert the dispatch response for `r` returned `status` and return
    /// its parsed body.
    fn expect(req: Request, status: u16) -> Value {
        let resp = dispatch(&req).unwrap();
        assert_eq!(
            resp.status, status,
            "bad status for {} {}",
            req.method, req.path
        );
        body(&resp)
    }

    #[test]
    fn create_and_get_canary() {
        let canary = body(&dispatch(&create_canary("c1")).unwrap())["Canary"].clone();
        assert_eq!(canary["Name"], "c1");
        assert_eq!(canary["Status"]["State"], "READY");
        assert_eq!(canary["RuntimeVersion"], "syn-python-selenium-3.0");
        assert_eq!(canary["Code"]["Handler"], "index.handler");
        assert_eq!(
            canary["ExecutionRoleArn"],
            "arn:aws:iam::123456789012:role/canary-role"
        );
        assert!(canary["Id"].as_str().is_some());
        assert!(canary["Timeline"]["Created"].as_str().is_some());
        assert!(canary["Timeline"]["LastStarted"].is_null());

        let wire = body(&dispatch(&req("GET", "/canary/c1", json!({}))).unwrap())["Canary"].clone();
        assert_eq!(wire["Name"], "c1");
        assert_eq!(wire["RunConfig"]["TimeoutInSeconds"], 60);
        assert_eq!(wire["SuccessRetentionPeriodInDays"], 31);
        assert_eq!(wire["FailureRetentionPeriodInDays"], 31);
    }

    #[test]
    fn canary_not_found_and_delete() {
        let resp = expect(req("GET", "/canary/nope", json!({})), 400);
        assert_eq!(resp["__type"], "ResourceNotFoundException");
        assert_eq!(resp["message"], "Canary nope not found");

        expect(create_canary("c2"), 200);
        expect(req("DELETE", "/canary/c2", json!({})), 200);
        expect(req("GET", "/canary/c2", json!({})), 400);
        expect(req("DELETE", "/canary/c2", json!({})), 400);
    }

    #[test]
    fn lifecycle_start_stop_update_runs() {
        expect(create_canary("c3"), 200);
        expect(req("POST", "/canary/c3/start", json!({})), 200);

        let runs = body(&dispatch(&req("POST", "/canary/c3/runs", json!({}))).unwrap());
        assert_eq!(runs["CanaryRuns"].as_array().unwrap().len(), 1);
        assert_eq!(runs["CanaryRuns"][0]["Status"]["State"], "PASSED");
        assert!(runs["CanaryRuns"][0]["Id"].as_str().is_some());

        let last = body(&dispatch(&req("POST", "/canaries/last-run", json!({}))).unwrap());
        let entry = last["CanariesLastRun"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["CanaryName"] == "c3")
            .cloned()
            .unwrap();
        assert!(entry["LastRun"]["Id"].as_str().is_some());

        expect(req("POST", "/canary/c3/stop", json!({})), 200);

        let up = expect(
            req(
                "PATCH",
                "/canary/c3",
                json!({"SuccessRetentionPeriodInDays": 7}),
            ),
            200,
        );
        assert!(up.as_object().unwrap().is_empty());
        let wire = body(&dispatch(&req("GET", "/canary/c3", json!({}))).unwrap())["Canary"].clone();
        assert_eq!(wire["SuccessRetentionPeriodInDays"], 7);

        let dry = expect(req("POST", "/canary/c3/dry-run/start", json!({})), 200);
        assert!(dry["DryRunConfig"]["DryRunId"].as_str().is_some());

        for (method, path) in [
            ("POST", "/canary/none/start"),
            ("POST", "/canary/none/stop"),
            ("POST", "/canary/none/dry-run/start"),
            ("POST", "/canary/none/runs"),
            ("PATCH", "/canary/none"),
        ] {
            expect(req(method, path, json!({})), 400);
        }
    }

    #[test]
    fn describes() {
        expect(create_canary("c4"), 200);
        let listed = body(&dispatch(&req("POST", "/canaries", json!({}))).unwrap());
        assert!(listed["Canaries"].as_array().is_some());
        // An empty Names array is falsy in moto: no filter.
        let listed = body(&dispatch(&req("POST", "/canaries", json!({"Names": ["c4"]}))).unwrap());
        assert_eq!(listed["Canaries"].as_array().unwrap().len(), 1);
        let listed = body(&dispatch(&req("POST", "/canaries", json!({"Names": ["zzz"]}))).unwrap());
        assert!(listed["Canaries"].as_array().unwrap().is_empty());

        let versions = body(&dispatch(&req("POST", "/runtime-versions", json!({}))).unwrap());
        assert_eq!(versions["RuntimeVersions"].as_array().unwrap().len(), 5);
        assert_eq!(
            versions["RuntimeVersions"][0]["VersionName"],
            "syn-nodejs-puppeteer-9.1"
        );
        assert!(versions["RuntimeVersions"][0]["DeprecationDate"].is_null());
        assert_eq!(
            versions["RuntimeVersions"][3]["DeprecationDate"],
            "2024-06-01T00:00:00Z"
        );
    }

    #[test]
    fn groups_and_associations() {
        let group = expect(req("POST", "/group", json!({"Name": "g1"})), 200)["Group"].clone();
        assert_eq!(group["Name"], "g1");
        let arn = group["Arn"].as_str().unwrap().to_string();

        // Get by name and by (URL-encoded) ARN.
        let got = body(&dispatch(&req("GET", "/group/g1", json!({}))).unwrap())["Group"].clone();
        assert_eq!(got["Id"], group["Id"]);
        let encoded = arn.replace(':', "%3A");
        let got = body(&dispatch(&req("GET", &format!("/group/{encoded}"), json!({}))).unwrap())
            ["Group"]
            .clone();
        assert_eq!(got["Id"], group["Id"]);

        expect(req("GET", "/group/none", json!({})), 400);

        // Associate a canary and list both directions.
        expect(create_canary("g1canary"), 200);
        let canary_arn = "arn:aws:synthetics:us-east-1:123456789012:canary:g1canary";
        let enc_arn = canary_arn.replace(':', "%3A");
        expect(
            req(
                "PATCH",
                "/group/g1/associate",
                json!({"ResourceArn": canary_arn}),
            ),
            200,
        );
        let resources = body(&dispatch(&req("POST", "/group/g1/resources", json!({}))).unwrap());
        assert_eq!(resources["Resources"].as_array().unwrap().len(), 1);
        let groups = body(
            &dispatch(&req(
                "POST",
                &format!("/resource/{enc_arn}/groups"),
                json!({}),
            ))
            .unwrap(),
        );
        assert_eq!(groups["Groups"][0]["Name"], "g1");
        assert!(groups["Groups"][0]["Id"].as_str().is_some());

        // Empty association list for a fresh group.
        expect(req("POST", "/group", json!({"Name": "g2"})), 200);
        let resources = body(&dispatch(&req("POST", "/group/g2/resources", json!({}))).unwrap());
        assert!(resources["Resources"].as_array().unwrap().is_empty());

        // Disassociate empties the listing.
        expect(
            req(
                "PATCH",
                "/group/g1/disassociate",
                json!({"ResourceArn": canary_arn}),
            ),
            200,
        );
        let groups = body(
            &dispatch(&req(
                "POST",
                &format!("/resource/{enc_arn}/groups"),
                json!({}),
            ))
            .unwrap(),
        );
        assert!(groups["Groups"].as_array().unwrap().is_empty());

        for (method, path) in [
            ("DELETE", "/group/none"),
            ("PATCH", "/group/none/associate"),
            ("PATCH", "/group/none/disassociate"),
            ("POST", "/group/none/resources"),
        ] {
            expect(req(method, path, json!({})), 400);
        }

        expect(req("DELETE", "/group/g2", json!({})), 200);
        expect(req("GET", "/group/g2", json!({})), 400);
    }

    #[test]
    fn tags_round_trip() {
        expect(create_canary("t1"), 200);
        let arn = canary_arn("us-east-1", 123456789012, "t1");
        let enc = arn.replace(':', "%3A");

        expect(
            req(
                "POST",
                &format!("/tags/{enc}"),
                json!({"Tags": {"team": "dev"}}),
            ),
            200,
        );
        let tags = body(&dispatch(&req("GET", &format!("/tags/{enc}"), json!({}))).unwrap());
        assert_eq!(tags["Tags"]["team"], "dev");
        // Inline tags are updated too.
        let wire = body(&dispatch(&req("GET", "/canary/t1", json!({}))).unwrap())["Canary"].clone();
        assert_eq!(wire["Tags"]["team"], "dev");

        let mut untag = req("DELETE", &format!("/tags/{enc}"), json!({}));
        untag.query_string = "tagKeys=team".into();
        expect(untag, 200);
        let tags = body(&dispatch(&req("GET", &format!("/tags/{enc}"), json!({}))).unwrap());
        assert!(tags["Tags"].get("team").is_none());
        let wire = body(&dispatch(&req("GET", "/canary/t1", json!({}))).unwrap())["Canary"].clone();
        assert!(wire["Tags"].get("team").is_none());
    }

    #[test]
    fn create_canary_with_tags() {
        let r = req(
            "POST",
            "/canary",
            json!({
                "Name": "tagged",
                "Tags": {"env": "test", "project": "demo"},
            }),
        );
        let canary = body(&dispatch(&r).unwrap())["Canary"].clone();
        assert_eq!(canary["Tags"]["env"], "test");
        assert_eq!(canary["Tags"]["project"], "demo");

        let arn = canary_arn("us-east-1", 123456789012, "tagged");
        let enc = arn.replace(':', "%3A");
        let tags = body(&dispatch(&req("GET", &format!("/tags/{enc}"), json!({}))).unwrap());
        assert_eq!(tags["Tags"]["project"], "demo");
    }

    #[test]
    fn unknown_route_is_not_implemented() {
        expect(req("POST", "/unknown", json!({})), 501);
        let resp = body(&dispatch(&req("POST", "/unknown", json!({}))).unwrap());
        assert_eq!(resp["__type"], "NotImplemented");
    }
}
