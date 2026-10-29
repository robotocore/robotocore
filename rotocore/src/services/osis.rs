//! OpenSearch Ingestion backend (REST-JSON; `/2022-01-01/osis/...`).
//!
//! Port of moto's osis: pipeline CRUD with ACTIVE/STOPPED lifecycle,
//! destinations parsed from the configuration body, ingest endpoint URLs
//! and a tag store. `resourcePolicy` remains sidecar-backed.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};

#[derive(Default)]
struct State {
    /// PipelineName -> pipeline dict.
    pipelines: Vec<Value>,
    /// Pipeline arn -> stored resource policy.
    policies: Vec<(String, Value)>,
    /// arn -> {Key: Value}
    tags: Vec<(String, Value)>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("osis"));
    SLOT.clone()
}

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn active_reason() -> &'static str {
    "The pipeline is ready to ingest data."
}

fn stopped_reason() -> &'static str {
    "The pipeline is stopped"
}

fn start_conflict(status: &str) -> Response {
    conflict(&format!(
        "Only pipelines with one of the following statuses are eligible for starting: ['START_FAILED', 'STOPPED']. The current status is {status}."
    ))
}

fn conflict(message: &str) -> Response {
    let e = json!({"__type": "ConflictException", "message": message});
    Response::json(409, serde_json::to_vec(&e).unwrap_or_default())
}

fn not_found(name: &str) -> Response {
    let e = json!({
        "__type": "ResourceNotFoundException",
        "message": format!("Pipeline {name} not found"),
    });
    Response::json(404, serde_json::to_vec(&e).unwrap_or_default())
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "osis" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(req, b, &body, &req.region, req.account))
}

impl State {
    fn find(&self, name: &str) -> Option<&Value> {
        self.pipelines
            .iter()
            .find(|p| p["PipelineName"] == json!(name))
    }

    fn find_mut(&mut self, name: &str) -> Option<&mut Value> {
        self.pipelines
            .iter_mut()
            .find(|p| p["PipelineName"] == json!(name))
    }
}

/// Destinations parsed from the configuration body, mirroring moto's
/// `_update_destinations` (OpenSearch/OpenSearch_Serverless/S3 sinks).
fn destinations(config: &Value) -> Vec<Value> {
    let mut out = Vec::new();
    if let Some(subs) = config.as_object() {
        for (sub_name, sub_value) in subs {
            if sub_name == "version" {
                continue;
            }
            let Some(sinks) = sub_value.get("sink").and_then(Value::as_array) else {
                continue;
            };
            for sink in sinks {
                for (sink_type, cfg) in sink.as_object().unwrap_or(&serde_json::Map::new()).iter() {
                    let (service, endpoint) = match sink_type.as_str() {
                        "opensearch" => {
                            let serverless = cfg
                                .get("aws")
                                .and_then(|aws| aws.get("serverless"))
                                .and_then(Value::as_bool)
                                .unwrap_or(false);
                            (
                                if serverless {
                                    "OpenSearch_Serverless"
                                } else {
                                    "OpenSearch"
                                },
                                cfg.get("hosts")
                                    .and_then(Value::as_array)
                                    .and_then(|h| h.first())
                                    .and_then(Value::as_str)
                                    .unwrap_or("")
                                    .to_string(),
                            )
                        }
                        "s3" => (
                            "S3",
                            cfg.get("bucket")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string(),
                        ),
                        _ => continue,
                    };
                    out.push(json!({"ServiceName": service, "Endpoint": endpoint}));
                }
            }
        }
    }
    out
}

fn pipeline_json(
    name: &str,
    arn: &str,
    p: &Value,
    status: &str,
    region: &str,
    account: u64,
    stored: Option<&Value>,
) -> Value {
    let config = p
        .get("PipelineConfigurationBody")
        .and_then(Value::as_str)
        .unwrap_or("{}");
    let parsed: Value = serde_json::from_str(config).unwrap_or_else(|_| json!({}));
    let ingests = stored
        .and_then(|s| s.get("IngestEndpointUrls"))
        .cloned()
        .unwrap_or(json!([format!(
            "{name}-{}.{region}.osis.amazonaws.com",
            uuid::Uuid::new_v4().simple()
        )]));
    json!({
        "PipelineName": name,
        "PipelineArn": arn,
        "MinUnits": p.get("MinUnits").cloned().unwrap_or(json!(1)),
        "MaxUnits": p.get("MaxUnits").cloned().unwrap_or(json!(1)),
        "Status": status,
        "StatusReason": {"Description": reason_for(status)},
        "PipelineConfigurationBody": config,
        "CreatedAt": now_iso(),
        "LastUpdatedAt": now_iso(),
        "IngestEndpointUrls": ingests,
        "Destinations": destinations(&parsed),
        "Tags": stored
            .and_then(|s| s.get("Tags"))
            .cloned()
            .unwrap_or(json!([])),
        "account": account,
    })
}

fn reason_for(status: &str) -> &str {
    match status {
        "ACTIVE" | "STARTING" => active_reason(),
        "STOPPED" => stopped_reason(),
        _ => "The pipeline is being created. It is not able to ingest data.",
    }
}

fn now_iso() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S%.6f+00:00")
        .to_string()
}

fn arn_of(region: &str, account: u64, name: &str) -> String {
    format!("arn:aws:osis:{region}:{account}:pipeline/{name}")
}

fn handle(req: &Request, st: &mut State, p: &Value, region: &str, account: u64) -> Response {
    let path = req.path.as_str();
    let tail = path.split("/2022-01-01/osis/").nth(1).unwrap_or("");

    if tail == "createPipeline" && req.method == "POST" {
        let name = p.get("PipelineName").and_then(Value::as_str).unwrap_or("");
        let arn = arn_of(region, account, name);
        let mut pipe = json!({
            "PipelineName": name,
            "PipelineArn": &arn,
        });
        pipe["PipelineConfigurationBody"] = p
            .get("PipelineConfigurationBody")
            .cloned()
            .unwrap_or(json!(null));
        let mut full = pipeline_json(name, &arn, p, "ACTIVE", region, account, None);
        full["PipelineConfigurationBody"] = pipe["PipelineConfigurationBody"].clone();
        st.pipelines.push(full.clone());
        if let Some(tags) = p.get("Tags") {
            store_tags(st, &arn, tags);
        }
        return ok(&json!({"Pipeline": full}));
    }

    if tail == "listPipelines" {
        let list: Vec<Value> = st
            .pipelines
            .iter()
            .map(|v| {
                // to_short_dict: drop config body + endpoint details
                let mut out = v.clone();
                if let Some(o) = out.as_object_mut() {
                    o.remove("PipelineConfigurationBody");
                    o.remove("IngestEndpointUrls");
                }
                out
            })
            .collect();
        return ok(&json!({"nextToken": Value::Null, "Pipelines": list}));
    }

    if tail == "listTagsForResource/" {
        let arn = p.get("Arn").and_then(Value::as_str).unwrap_or("");
        let tags = tag_list(st, arn);
        return ok(&json!({"Tags": tags}));
    }

    if tail == "tagResource/" {
        let arn = p.get("Arn").and_then(Value::as_str).unwrap_or("");
        if let Some(tags) = p.get("Tags") {
            store_tags(st, arn, tags);
        }
        return ok(&json!({}));
    }

    if tail == "untagResource/" {
        let arn = p.get("Arn").and_then(Value::as_str).unwrap_or("");
        let keys: Vec<String> = p
            .get("TagKeys")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();
        for (a, v) in st.tags.iter_mut() {
            if a == arn {
                if let Some(obj) = v.as_object_mut() {
                    for k in &keys {
                        obj.remove(k);
                    }
                }
            }
        }
        return ok(&json!({}));
    }

    // Path-suffix operations: getPipeline/{name}, deletePipeline/{name},
    // startPipeline/{name}, stopPipeline/{name}, updatePipeline/{name},
    // resourcePolicy/{arn}.
    if let Some(rest) = tail.strip_suffix('/') {
        let _ = rest;
    }
    if let Some((resource, op)) = split_tail(tail) {
        let _ = resource;
        match (op, req.method.as_str()) {
            ("getPipeline", _) => {
                let name = op_name(tail, "getPipeline");
                return match st.find(&name) {
                    Some(v) => ok(&json!({"Pipeline": v})),
                    None => not_found(&name),
                };
            }
            ("deletePipeline", _) => {
                let name = op_name(tail, "deletePipeline");
                if st.find(&name).is_none() {
                    return not_found(&name);
                }
                st.pipelines.retain(|v| v["PipelineName"] != json!(name));
                return ok(&json!({}));
            }
            ("startPipeline", _) => {
                let name = op_name(tail, "startPipeline");
                match st.find(&name) {
                    Some(found) => {
                        let status = found["Status"].as_str().unwrap_or("ACTIVE");
                        if status != "STOPPED" && status != "START_FAILED" {
                            return start_conflict(status);
                        }
                        if let Some(v) = st.find_mut(&name) {
                            v["Status"] = json!("ACTIVE");
                            return ok(&json!({"Pipeline": v.clone()}));
                        }
                        return not_found(&name);
                    }
                    None => return not_found(&name),
                }
            }
            ("stopPipeline", _) => {
                let name = op_name(tail, "stopPipeline");
                match st.find(&name) {
                    Some(found) => {
                        let status = found["Status"].as_str().unwrap_or("ACTIVE");
                        if status != "ACTIVE" && status != "UPDATE_FAILED" {
                            return conflict(&format!(
                                "Only pipelines with one of the following statuses are eligible for stopping: ['UPDATE_FAILED', 'ACTIVE']. The current status is {status}."
                            ));
                        }
                        if let Some(v) = st.find_mut(&name) {
                            return ok(
                                &json!({"Pipeline": out_set_status(v, "STOPPED", json!({}))}),
                            );
                        }
                        return not_found(&name);
                    }
                    None => return not_found(&name),
                }
            }
            ("updatePipeline", _) => {
                let name = op_name(tail, "updatePipeline");
                if st.find(&name).is_none() {
                    return not_found(&name);
                }
                if let Some(v) = st.find_mut(&name) {
                    if let Some(m) = p.get("MinUnits") {
                        v["MinUnits"] = m.clone();
                    }
                    if let Some(m) = p.get("MaxUnits") {
                        v["MaxUnits"] = m.clone();
                    }
                    if let Some(c) = p.get("PipelineConfigurationBody") {
                        v["PipelineConfigurationBody"] = c.clone();
                    }
                    return ok(&json!({"Pipeline": v.clone()}));
                }
                return not_found(&name);
            }
            ("resourcePolicy", "PUT") => {
                let arn =
                    crate::proto::url_decode(tail.strip_prefix("resourcePolicy/").unwrap_or(""));
                if st_find_policy(st, &arn).is_some() {
                    return conflict("The policy is already set for that arn");
                }
                let policy = p.get("Policy").and_then(Value::as_str).unwrap_or("");
                store_policy(st, &arn, policy);
                return ok(&json!({}));
            }
            ("resourcePolicy", "GET") => {
                let arn =
                    crate::proto::url_decode(tail.strip_prefix("resourcePolicy/").unwrap_or(""));
                let Some(policy) = st_find_policy(st, &arn) else {
                    return not_found(&arn);
                };
                return ok(&json!({"ResourceArn": arn, "Policy": policy}));
            }
            ("resourcePolicy", "DELETE") => {
                let arn =
                    crate::proto::url_decode(tail.strip_prefix("resourcePolicy/").unwrap_or(""));
                if st_find_policy(st, &arn).is_none() {
                    return not_found(&arn);
                }
                remove_policy(st, &arn);
                return ok(&json!({}));
            }
            _ => {}
        }
    }

    unknown_operation(&req.operation)
}

fn out_set_status(v: &mut Value, status: &str, _out: Value) -> Value {
    v["Status"] = json!(status);
    v["StatusReason"] = json!({"Description": reason_for(status)});
    v.clone()
}

fn split_tail(tail: &str) -> Option<(&str, &str)> {
    const OPS: [&str; 7] = [
        "getPipeline",
        "deletePipeline",
        "startPipeline",
        "stopPipeline",
        "updatePipeline",
        "resourcePolicy",
        "getPipelineBlueprint",
    ];
    for op in OPS {
        if let Some(rest) = tail.strip_prefix(op) {
            return Some((rest, op));
        }
    }
    None
}

fn op_name(tail: &str, op: &str) -> String {
    tail.strip_prefix(op)
        .unwrap_or("")
        .trim_matches('/')
        .to_string()
}

fn store_policy(st: &mut State, arn: &str, policy: &str) {
    st.tags.push((arn.to_string(), json!({})));
    st.tags.push((format!("policy:{arn}"), json!(policy)));
    let _ = arn;
}

fn st_find_policy(st: &mut State, arn: &str) -> Option<String> {
    st.policies
        .iter()
        .find(|(a, _)| a == arn)
        .map(|(_, v)| v.as_str().unwrap_or("").to_string())
}

fn remove_policy(st: &mut State, arn: &str) {
    st.policies.retain(|(a, _)| a != arn);
}

fn unknown_operation(op: &str) -> Response {
    let e = json!({
        "__type": "NotImplemented",
        "message": format!("The {op} action has not been implemented"),
    });
    Response::json(501, serde_json::to_vec(&e).unwrap_or_default())
}

fn tag_list(st: &State, arn: &str) -> Vec<Value> {
    st.tags
        .iter()
        .find(|(a, _)| a == arn)
        .map(|(_, v)| v.clone())
        .unwrap_or(json!({}))
        .as_object()
        .map(|m| {
            m.iter()
                .map(|(k, v)| json!({"Key": k, "Value": v}))
                .collect()
        })
        .unwrap_or_default()
}

fn store_tags(st: &mut State, arn: &str, tags: &Value) {
    let entry = match st.tags.iter_mut().find(|(a, _)| a == arn) {
        Some((_, v)) => v,
        None => {
            st.tags.push((arn.to_string(), json!({})));
            let i = st.tags.len() - 1;
            &mut st.tags[i].1
        }
    };
    for tag in tags.as_array().unwrap_or(&vec![]) {
        let key = tag.get("Key").and_then(Value::as_str).unwrap_or("");
        if key.is_empty() {
            continue;
        }
        entry.as_object_mut().unwrap().insert(
            key.to_string(),
            tag.get("Value").cloned().unwrap_or(Value::Null),
        );
    }
}
