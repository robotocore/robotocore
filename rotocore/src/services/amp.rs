//! AMP backend — Amazon Managed Service for Prometheus (REST-JSON).
//!
//! Replaces the moto sidecar for `amp` (paths under `/workspaces`,
//! `/scrapers`, `/tags/...`). State is per-(account, region), mirroring
//! moto's `PrometheusServiceBackend` (moto/amp). Not-found errors are 404
//! with an `x-amzn-ErrorType` header and `{"resourceId","message",
//! "resourceType"}` body — botocore's RestJSONParser reads the error code
//! from that header.
//!
//! Route table mirrors moto/amp/urls.py; the operation comes from
//! method + path. Unknown paths fall through to 501 NotImplemented.

use crate::store;
use crate::{Request, Response};
use serde_json::{json, Value};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn json_resp(status: u16, body: &Value) -> Response {
    Response::new(
        status,
        "application/json",
        serde_json::to_vec(body).unwrap_or_default(),
    )
}

fn not_found(message: &str, resource_id: &str, resource_type: &str) -> Response {
    let mut resp = json_resp(
        404,
        &json!({
            "resourceId": resource_id,
            "message": message,
            "resourceType": resource_type,
        }),
    );
    resp.headers.push((
        "x-amzn-ErrorType".to_string(),
        "ResourceNotFoundException".to_string(),
    ));
    resp
}

fn workspace_missing(id: &str) -> Response {
    not_found("Workspace not found", id, "AWS::APS::Workspace")
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// Per-(account, region) AMP state.
#[derive(Default)]
pub struct AmpState {
    /// workspace id → Workspace
    workspaces: BTreeMap<String, Workspace>,
    /// scraper ids are keyed with their arn: id→(arn, Scraper)
    scrapers: BTreeMap<String, (String, Scraper)>,
    /// resource ARN → tag map (moto TaggingService dict semantics)
    tags: BTreeMap<String, BTreeMap<String, Option<String>>>,
}

struct RuleGroupNamespace {
    name: String,
    data: String,
    arn: String,
    created_at: i64,
    modified_at: i64,
}

struct AlertManagerDefinition {
    data: String,
    created_at: i64,
    modified_at: i64,
}

struct Scraper {
    alias: String,
    scrape_configuration: Value,
    source: Value,
    destination: Value,
    role_configuration: Option<Value>,
    created_at: i64,
    last_modified_at: i64,
    logging_destination: Option<Value>,
    scraper_components: Option<Value>,
}

struct AnomalyDetector {
    alias: String,
    evaluation_interval: i64,
    missing_data_action: Value,
    configuration: Value,
    labels: Value,
    created_at: i64,
    modified_at: i64,
}

#[derive(Default)]
struct Workspace {
    alias: String,
    arn: String,
    created_at: i64,
    rule_groups: BTreeMap<String, RuleGroupNamespace>,
    logging_config: Option<Value>,
    alert_manager: Option<AlertManagerDefinition>,
    anomaly_detectors: BTreeMap<String, AnomalyDetector>,
    query_logging: Option<Value>,
    resource_policy: Option<Value>,
    workspace_configuration: Option<Value>,
}

fn with_state<T>(account: u64, region: &str, f: impl FnOnce(&mut AmpState) -> T) -> T {
    let slot = store::slot::<AmpState>("amp");
    let mut map = slot.write();
    f(map.entry((account, region.to_string())).or_default())
}

fn now_epoch() -> i64 {
    chrono::Utc::now().timestamp()
}

fn partition(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else {
        "aws"
    }
}

// ---------------------------------------------------------------------------
// Tags (moto TaggingService dict semantics)
// ---------------------------------------------------------------------------

fn tag_map(state: &AmpState, arn: &str) -> Value {
    match state.tags.get(arn) {
        Some(map) => {
            let obj: serde_json::Map<String, Value> = map
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        v.clone().map(Value::String).unwrap_or(Value::Null),
                    )
                })
                .collect();
            Value::Object(obj)
        }
        None => json!({}),
    }
}

fn apply_tags(state: &mut AmpState, arn: &str, tags: Option<&Value>) {
    let Some(Value::Object(map)) = tags else {
        return;
    };
    let entry = state.tags.entry(arn.to_string()).or_default();
    for (k, v) in map {
        entry.insert(k.clone(), v.as_str().map(String::from));
    }
}

// ---------------------------------------------------------------------------
// Path / query parsing
// ---------------------------------------------------------------------------

/// Percent-decode a path or query component.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                if let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                    out.push(h * 16 + l);
                    i += 3;
                    continue;
                }
                out.push(b'%');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Multi-value query params (`tagKeys` can repeat).
fn query_multi(query_string: &str, key: &str) -> Vec<String> {
    let mut out = Vec::new();
    for pair in query_string.split('&') {
        let (k, v) = match pair.find('=') {
            Some(i) => (&pair[..i], &pair[i + 1..]),
            None => (pair, ""),
        };
        if k == key && !v.is_empty() {
            out.push(percent_decode(v));
        }
    }
    out
}

fn query_single(query_string: &str, key: &str) -> Option<String> {
    query_multi(query_string, key).into_iter().next()
}

/// Everything after the last `tags/` occurrence, percent-decoded (the ARN).
fn arn_from_tags_path(path: &str) -> String {
    match path.rfind("tags/") {
        Some(idx) => percent_decode(&path[idx + 5..]),
        None => String::new(),
    }
}

// ---------------------------------------------------------------------------
// Shapes
// ---------------------------------------------------------------------------

fn workspace_dict(ws: &Workspace, tags: Value, region: &str) -> Value {
    let id = ws.arn.rsplit('/').next().unwrap_or_default();
    json!({
        "alias": ws.alias,
        "arn": ws.arn,
        "workspaceId": id,
        "status": { "statusCode": "ACTIVE" },
        "createdAt": ws.created_at,
        "prometheusEndpoint": format!(
            "https://aps-workspaces.{region}.amazonaws.com/workspaces/{id}/"
        ),
        "tags": tags,
    })
}

fn rule_group_dict(ns: &RuleGroupNamespace, tags: Value) -> Value {
    json!({
        "name": ns.name,
        "arn": ns.arn,
        "status": { "statusCode": "ACTIVE" },
        "createdAt": ns.created_at,
        "modifiedAt": ns.modified_at,
        "data": ns.data,
        "tags": tags,
    })
}

impl AlertManagerDefinition {
    fn log_status(&self) -> Value {
        json!({ "statusCode": "ACTIVE" })
    }
}

fn alert_manager_dict(defn: &AlertManagerDefinition) -> Value {
    json!({
        "data": defn.data,
        "createdAt": defn.created_at,
        "modifiedAt": defn.modified_at,
        "status": { "statusCode": "ACTIVE" },
    })
}

fn scraper_dict(id: &str, arn: &str, s: &Scraper, tags: Value) -> Value {
    json!({
        "scraperId": id,
        "alias": s.alias,
        "arn": arn,
        "scrapeConfiguration": s.scrape_configuration,
        "source": s.source,
        "destination": s.destination,
        "roleConfiguration": s.role_configuration,
        "status": { "statusCode": "ACTIVE" },
        "createdAt": s.created_at,
        "lastModifiedAt": s.last_modified_at,
        "tags": tags,
    })
}

fn scraper_summary(id: &str, arn: &str, s: &Scraper, tags: Value) -> Value {
    json!({
        "scraperId": id,
        "alias": s.alias,
        "arn": arn,
        "source": s.source,
        "destination": s.destination,
        "roleConfiguration": s.role_configuration,
        "status": { "statusCode": "ACTIVE" },
        "createdAt": s.created_at,
        "lastModifiedAt": s.last_modified_at,
        "tags": tags,
    })
}

fn anomaly_dict(id: &str, arn: &str, ad: &AnomalyDetector, tags: Value) -> Value {
    json!({
        "anomalyDetectorId": id,
        "alias": ad.alias,
        "arn": arn,
        "evaluationIntervalInSeconds": ad.evaluation_interval,
        "missingDataAction": ad.missing_data_action,
        "configuration": ad.configuration,
        "labels": ad.labels,
        "status": { "statusCode": "ACTIVE" },
        "createdAt": ad.created_at,
        "modifiedAt": ad.modified_at,
        "tags": tags,
    })
}

// ---------------------------------------------------------------------------
// Op handlers — plain fns returning responses (no Result plumbing)
// ---------------------------------------------------------------------------

// (op implementations appended below)

// --- workspaces -----------------------------------------------------------

fn op_workspaces(
    state: &mut AmpState,
    method: &str,
    workspace_id: Option<&str>,
    body: &Value,
    query: &str,
    account: u64,
    region: &str,
) -> Response {
    match (method, workspace_id) {
        ("POST", _) => {
            let alias = body
                .get("alias")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let id = format!("ws-{}", uuid::Uuid::new_v4());
            let arn = format!(
                "arn:{}:aps:{region}:{account}:workspace/{id}",
                partition(region)
            );
            let ws = Workspace {
                alias: alias.to_string(),
                arn: arn.clone(),
                created_at: now_epoch(),
                ..Workspace::default()
            };
            state.workspaces.insert(id, ws);
            apply_tags(state, &arn, body.get("tags"));
            let flat = state
                .workspaces
                .get(arn.rsplit('/').next().unwrap_or(""))
                .expect("inserted");
            json_resp(200, &workspace_dict(flat, tag_map(state, &arn), region))
        }
        ("GET", None) => {
            let alias = query_single(query, "alias");
            let max_results =
                query_single(query, "maxResults").and_then(|v| v.parse::<usize>().ok());
            let mut ids: Vec<String> = state
                .workspaces
                .values()
                .filter(|w| alias.as_deref().is_none_or(|a| w.alias == a))
                .map(|w| w.arn.rsplit('/').next().unwrap_or_default().to_string())
                .collect();
            let mut next_token: Value = Value::Null;
            if let Some(max) = max_results {
                if ids.len() > max {
                    next_token = json!(state
                        .workspaces
                        .get(&ids[max])
                        .map(|w| w.arn.clone())
                        .unwrap_or_default());
                    ids.truncate(max);
                }
            }
            let workspaces: Vec<Value> = ids
                .iter()
                .filter_map(|id| state.workspaces.get(id))
                .map(|w| workspace_dict(w, tag_map(state, &w.arn), region))
                .collect();
            json_resp(
                200,
                &json!({ "nextToken": next_token, "workspaces": workspaces }),
            )
        }
        ("GET", Some(id)) => match state.workspaces.get(id) {
            Some(ws) => json_resp(
                200,
                &json!({ "workspace": workspace_dict(ws, tag_map(state, &ws.arn), region) }),
            ),
            None => workspace_missing(id),
        },
        ("DELETE", Some(id)) => match state.workspaces.remove(id) {
            Some(ws) => {
                state.tags.remove(&ws.arn);
                json_resp(200, &json!({}))
            }
            None => workspace_missing(id),
        },
        _ => match workspace_id {
            Some(id) => workspace_missing(id),
            // moto's amp handlers answer unsupported verbs with a bare 400
            // and an empty body (moto/amp/responses.py `_r("{}", 400)`).
            None => json_resp(400, &json!({})),
        },
    }
}

// --- workspace alias (PUT /workspaces/{id}/alias) --------------------------

fn op_workspace_alias(state: &mut AmpState, workspace_id: &str, body: &Value) -> Response {
    match state.workspaces.get_mut(workspace_id) {
        Some(ws) => {
            ws.alias = body
                .get("alias")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            json_resp(200, &json!({}))
        }
        None => workspace_missing(workspace_id),
    }
}

// --- rule groups namespaces ----------------------------------------------

fn rg_arn(account: u64, region: &str, wsid: &str, name: &str) -> String {
    format!(
        "arn:{}:aps:{region}:{account}:rulegroupsnamespace/{wsid}/{name}",
        partition(region)
    )
}

#[allow(clippy::too_many_arguments)]
fn op_rule_group_namespaces(
    state: &mut AmpState,
    method: &str,
    workspace_id: &str,
    name: Option<&str>,
    body: &Value,
    query: &str,
    account: u64,
    region: &str,
) -> Response {
    if !state.workspaces.contains_key(workspace_id) {
        return workspace_missing(workspace_id);
    }
    match (method, name) {
        ("POST", None) => {
            let name = body.get("name").and_then(Value::as_str).unwrap_or_default();
            let data = body.get("data").and_then(Value::as_str).unwrap_or_default();
            let created = now_epoch();
            let arn = rg_arn(account, region, workspace_id, name);
            let ns = RuleGroupNamespace {
                name: name.to_string(),
                data: data.to_string(),
                arn: arn.clone(),
                created_at: created,
                modified_at: created,
            };
            apply_tags(state, &arn, body.get("tags"));
            let tags = tag_map(state, &arn);
            state
                .workspaces
                .get_mut(workspace_id)
                .expect("checked")
                .rule_groups
                .insert(name.to_string(), ns);
            let ns = state
                .workspaces
                .get(workspace_id)
                .and_then(|w| w.rule_groups.get(name))
                .expect("inserted");
            json_resp(202, &rule_group_dict(ns, tags))
        }
        ("GET", None) => {
            let name_filter = query_single(query, "name");
            let max_results =
                query_single(query, "maxResults").and_then(|v| v.parse::<usize>().ok());
            let list: Vec<Value> = state
                .workspaces
                .get(workspace_id)
                .expect("checked")
                .rule_groups
                .values()
                .filter(|ns| {
                    name_filter
                        .as_deref()
                        .is_none_or(|f| ns.name.starts_with(f))
                })
                .map(|ns| rule_group_dict(ns, tag_map(state, &ns.arn)))
                .collect();
            let list = match max_results {
                Some(max) if list.len() > max => {
                    let token = list
                        .get(max)
                        .map(|ns| ns["name"].as_str().unwrap_or_default().to_string());
                    let mut body = json!({
                        "nextToken": token,
                        "ruleGroupsNamespaces": list.into_iter().take(max).collect::<Vec<_>>(),
                    });
                    // truncate underlying
                    if let Some(arr) = body
                        .get_mut("ruleGroupsNamespaces")
                        .and_then(Value::as_array_mut)
                    {
                        arr.truncate(max);
                    }
                    return json_resp(200, &body);
                }
                _ => list,
            };
            json_resp(
                200,
                &json!({ "nextToken": Value::Null, "ruleGroupsNamespaces": list }),
            )
        }
        ("GET", Some(name)) => match state
            .workspaces
            .get(workspace_id)
            .and_then(|w| w.rule_groups.get(name))
        {
            Some(ns) => json_resp(
                200,
                &json!({ "ruleGroupsNamespace": rule_group_dict(ns, tag_map(state, &ns.arn)) }),
            ),
            None => not_found(
                "RuleGroupNamespace not found",
                name,
                "AWS::APS::RuleGroupNamespace",
            ),
        },
        ("PUT", Some(name)) => {
            let data = body.get("data").and_then(Value::as_str).unwrap_or_default();
            let Some(arn) = state
                .workspaces
                .get_mut(workspace_id)
                .and_then(|w| w.rule_groups.get_mut(name))
                .map(|ns| {
                    ns.data = data.to_string();
                    ns.modified_at = now_epoch();
                    ns.arn.clone()
                })
            else {
                return not_found(
                    "RuleGroupNamespace not found",
                    name,
                    "AWS::APS::RuleGroupNamespace",
                );
            };
            let ns = state
                .workspaces
                .get(workspace_id)
                .and_then(|w| w.rule_groups.get(name))
                .expect("present");
            json_resp(202, &rule_group_dict(ns, tag_map(state, &arn)))
        }
        ("DELETE", Some(name)) => {
            match state
                .workspaces
                .get_mut(workspace_id)
                .and_then(|w| w.rule_groups.remove(name))
            {
                Some(ns) => {
                    state.tags.remove(&ns.arn);
                    json_resp(202, &json!({}))
                }
                None => json_resp(202, &json!({})),
            }
        }
        _ => not_found(
            "RuleGroupNamespace not found",
            name.unwrap_or_default(),
            "AWS::APS::RuleGroupNamespace",
        ),
    }
}

// --- alert manager definition --------------------------------------------

fn op_alert_manager_definition(
    state: &mut AmpState,
    method: &str,
    workspace_id: &str,
    body: &Value,
) -> Response {
    if !state.workspaces.contains_key(workspace_id) {
        return workspace_missing(workspace_id);
    }
    let status_code = match method {
        "POST" | "PUT" => 202,
        "DELETE" => 202,
        _ => 200,
    };
    let ws = state.workspaces.get_mut(workspace_id).expect("checked");
    match method {
        "POST" => {
            let now = now_epoch();
            let defn = AlertManagerDefinition {
                data: body
                    .get("data")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                created_at: now,
                modified_at: now,
            };
            let status = defn.log_status();
            ws.alert_manager = Some(defn);
            json_resp(status_code, &json!({ "status": status }))
        }
        "GET" => match &ws.alert_manager {
            Some(defn) => json_resp(
                200,
                &json!({ "alertManagerDefinition": alert_manager_dict(defn) }),
            ),
            None => not_found(
                "AlertManagerDefinition not found",
                workspace_id,
                "AWS::APS::AlertManagerDefinition",
            ),
        },
        "PUT" => match &mut ws.alert_manager {
            Some(defn) => {
                defn.data = body
                    .get("data")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                defn.modified_at = now_epoch();
                let status = defn.log_status();
                json_resp(status_code, &json!({ "status": status }))
            }
            None => {
                let now = now_epoch();
                let defn = AlertManagerDefinition {
                    data: body
                        .get("data")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    created_at: now,
                    modified_at: now,
                };
                let status = defn.log_status();
                ws.alert_manager = Some(defn);
                json_resp(status_code, &json!({ "status": status }))
            }
        },
        "DELETE" => {
            ws.alert_manager = None;
            json_resp(status_code, &json!({}))
        }
        _ => not_found(
            "AlertManagerDefinition not found",
            workspace_id,
            "AWS::APS::AlertManagerDefinition",
        ),
    }
}

// --- workspace / scraper logging configurations ---------------------------

fn op_workspace_logging(
    state: &mut AmpState,
    method: &str,
    workspace_id: &str,
    body: &Value,
) -> Response {
    if !state.workspaces.contains_key(workspace_id) {
        return workspace_missing(workspace_id);
    }
    let ws = state.workspaces.get_mut(workspace_id).expect("checked");
    match method {
        "POST" => {
            let log_group_arn = body
                .get("logGroupArn")
                .and_then(Value::as_str)
                .unwrap_or_default();
            ws.logging_config = Some(json!({
                "logGroupArn": log_group_arn,
                "createdAt": now_epoch(),
                "status": { "statusCode": "ACTIVE" },
                "workspace": workspace_id,
            }));
            json_resp(202, &json!({ "status": { "statusCode": "ACTIVE" } }))
        }
        "GET" => match &ws.logging_config {
            Some(config) => json_resp(200, &json!({ "loggingConfiguration": config })),
            None => json_resp(200, &json!({ "loggingConfiguration": {} })),
        },
        "PUT" => {
            let log_group_arn = body
                .get("logGroupArn")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if ws.logging_config.is_some() {
                if let Some(config) = ws.logging_config.as_mut().and_then(Value::as_object_mut) {
                    config.insert("logGroupArn".to_string(), json!(log_group_arn));
                    config.insert("modifiedAt".to_string(), json!(now_epoch()));
                }
            } else {
                ws.logging_config = Some(json!({
                    "logGroupArn": log_group_arn,
                    "createdAt": now_epoch(),
                    "status": { "statusCode": "ACTIVE" },
                    "workspace": workspace_id,
                }));
            }
            json_resp(202, &json!({ "status": { "statusCode": "ACTIVE" } }))
        }
        "DELETE" => {
            ws.logging_config = None;
            json_resp(202, &json!({}))
        }
        // moto's fallthrough for unsupported verbs: plain 400, empty body.
        _ => json_resp(400, &json!({})),
    }
}

fn op_query_logging(
    state: &mut AmpState,
    method: &str,
    workspace_id: &str,
    body: &Value,
) -> Response {
    if !state.workspaces.contains_key(workspace_id) {
        return workspace_missing(workspace_id);
    }
    let ws = state.workspaces.get_mut(workspace_id).expect("checked");
    match method {
        "POST" => {
            ws.query_logging = Some(json!({
                "destinations": body.get("destinations").cloned().unwrap_or(json!([])),
                "createdAt": now_epoch(),
                "modifiedAt": now_epoch(),
                "status": { "statusCode": "ACTIVE" },
                "workspace": workspace_id,
            }));
            json_resp(202, &json!({ "status": { "statusCode": "ACTIVE" } }))
        }
        "GET" => match &ws.query_logging {
            Some(config) => json_resp(200, &json!({ "queryLoggingConfiguration": config })),
            None => not_found(
                "QueryLoggingConfiguration not found",
                workspace_id,
                "AWS::APS::QueryLoggingConfiguration",
            ),
        },
        "PUT" => match &mut ws.query_logging {
            Some(config) => {
                if let Some(obj) = config.as_object_mut() {
                    obj.insert(
                        "destinations".to_string(),
                        body.get("destinations").cloned().unwrap_or(json!([])),
                    );
                    obj.insert("modifiedAt".to_string(), json!(now_epoch()));
                }
                json_resp(202, &json!({ "status": { "statusCode": "ACTIVE" } }))
            }
            None => not_found(
                "QueryLoggingConfiguration not found",
                workspace_id,
                "AWS::APS::QueryLoggingConfiguration",
            ),
        },
        "DELETE" => {
            ws.query_logging = None;
            json_resp(202, &json!({}))
        }
        // moto's amp handlers answer unsupported verbs with a bare 400 and
        // an empty "{}" body (moto/amp/responses.py `_r("{}", 400)`)
        _ => json_resp(400, &json!({})),
    }
}

fn op_workspace_policy(
    state: &mut AmpState,
    method: &str,
    workspace_id: &str,
    body: &Value,
) -> Response {
    if !state.workspaces.contains_key(workspace_id) {
        return workspace_missing(workspace_id);
    }
    let ws = state.workspaces.get_mut(workspace_id).expect("checked");
    match method {
        "PUT" => {
            let revision = uuid::Uuid::new_v4().to_string();
            let policy = json!({
                "policyDocument": body.get("policyDocument").and_then(Value::as_str).unwrap_or_default(),
                "revisionId": revision.clone(),
                "policyStatus": { "statusCode": "ACTIVE" },
            });
            ws.resource_policy = Some(policy);
            json_resp(
                202,
                &json!({
                    "policyStatus": { "statusCode": "ACTIVE" },
                    "revisionId": revision,
                }),
            )
        }
        "GET" => match &ws.resource_policy {
            Some(policy) => json_resp(200, policy),
            None => not_found(
                "ResourcePolicy not found",
                workspace_id,
                "AWS::APS::ResourcePolicy",
            ),
        },
        "DELETE" => {
            ws.resource_policy = None;
            json_resp(202, &json!({}))
        }
        // moto's amp handlers answer unsupported verbs with a bare 400 and
        // an empty "{}" body (moto/amp/responses.py `_r("{}", 400)`)
        _ => json_resp(400, &json!({})),
    }
}

// --- workspace configuration (retention/limits) ----------------------------

fn op_workspace_configuration(
    state: &mut AmpState,
    method: &str,
    workspace_id: &str,
    body: &Value,
) -> Response {
    if !state.workspaces.contains_key(workspace_id) {
        return workspace_missing(workspace_id);
    }
    let ws = state.workspaces.get_mut(workspace_id).expect("checked");
    match method {
        "GET" => {
            let config = ws.workspace_configuration.clone().unwrap_or_else(|| {
                json!({
                    "status": { "statusCode": "ACTIVE" },
                    "retentionPeriodInDays": 150,
                    "limitsPerLabelSet": [],
                })
            });
            json_resp(200, &json!({ "workspaceConfiguration": config }))
        }
        "PATCH" | "PUT" => {
            let mut config = json!({ "status": { "statusCode": "ACTIVE" } });
            if let Some(obj) = config.as_object_mut() {
                if let Some(days) = body.get("retentionPeriodInDays").and_then(Value::as_i64) {
                    obj.insert("retentionPeriodInDays".to_string(), json!(days));
                }
                if let Some(limits) = body.get("limitsPerLabelSet") {
                    obj.insert("limitsPerLabelSet".to_string(), limits.clone());
                }
            }
            ws.workspace_configuration = Some(config);
            json_resp(202, &json!({ "status": { "statusCode": "ACTIVE" } }))
        }
        // moto's amp handlers answer unsupported verbs with a bare 400 and
        // an empty "{}" body (moto/amp/responses.py `_r("{}", 400)`)
        _ => json_resp(400, &json!({})),
    }
}

// --- anomaly detectors -----------------------------------------------------

fn ad_arn(account: u64, region: &str, wsid: &str, adid: &str) -> String {
    format!(
        "arn:{}:aps:{region}:{account}:workspace/{wsid}/anomalydetector/{adid}",
        partition(region)
    )
}

#[allow(clippy::too_many_arguments)]
fn op_anomaly_detectors(
    state: &mut AmpState,
    method: &str,
    workspace_id: &str,
    ad_id: Option<&str>,
    body: &Value,
    query: &str,
    account: u64,
    region: &str,
) -> Response {
    if !state.workspaces.contains_key(workspace_id) {
        return workspace_missing(workspace_id);
    }
    match (method, ad_id) {
        ("POST", None) => {
            let id = format!("ad-{}", uuid::Uuid::new_v4());
            let arn = ad_arn(account, region, workspace_id, &id);
            let now = now_epoch();
            let ad = AnomalyDetector {
                alias: body
                    .get("alias")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                evaluation_interval: body
                    .get("evaluationIntervalInSeconds")
                    .and_then(Value::as_i64)
                    .unwrap_or(300),
                missing_data_action: body
                    .get("missingDataAction")
                    .cloned()
                    .unwrap_or_else(|| json!({ "skip": {} })),
                configuration: body
                    .get("configuration")
                    .cloned()
                    .unwrap_or_else(|| json!({})),
                labels: body.get("labels").cloned().unwrap_or_else(|| json!({})),
                created_at: now,
                modified_at: now,
            };
            apply_tags(state, &arn, body.get("tags"));
            let tags = tag_map(state, &arn);
            state
                .workspaces
                .get_mut(workspace_id)
                .expect("checked")
                .anomaly_detectors
                .insert(id.clone(), ad);
            json_resp(
                202,
                &json!({
                    "anomalyDetectorId": id,
                    "arn": arn,
                    "status": { "statusCode": "ACTIVE" },
                    "tags": tags,
                }),
            )
        }
        ("GET", None) => {
            let alias_filter = query_single(query, "alias");
            let list: Vec<Value> = state
                .workspaces
                .get(workspace_id)
                .expect("checked")
                .anomaly_detectors
                .iter()
                .filter(|(_, ad)| alias_filter.as_deref().is_none_or(|a| ad.alias == a))
                .map(|(id, ad)| {
                    anomaly_dict(
                        id,
                        &ad_arn(account, region, workspace_id, id),
                        ad,
                        tag_map(state, &ad_arn(account, region, workspace_id, id)),
                    )
                })
                .collect();
            json_resp(
                200,
                &json!({ "nextToken": Value::Null, "anomalyDetectors": list }),
            )
        }
        ("GET", Some(id)) => match state
            .workspaces
            .get(workspace_id)
            .and_then(|w| w.anomaly_detectors.get(id))
        {
            Some(ad) => {
                let arn = ad_arn(account, region, workspace_id, id);
                json_resp(
                    200,
                    &json!({ "anomalyDetector": anomaly_dict(id, &arn, ad, tag_map(state, &arn)) }),
                )
            }
            None => not_found("AnomalyDetector not found", id, "AWS::APS::AnomalyDetector"),
        },
        ("PUT", Some(id)) => {
            let ws = state.workspaces.get_mut(workspace_id).expect("checked");
            match ws.anomaly_detectors.get_mut(id) {
                Some(ad) => {
                    if let Some(interval) = body
                        .get("evaluationIntervalInSeconds")
                        .and_then(Value::as_i64)
                    {
                        ad.evaluation_interval = interval;
                    }
                    if let Some(action) = body.get("missingDataAction") {
                        ad.missing_data_action = action.clone();
                    }
                    if let Some(config) = body.get("configuration") {
                        ad.configuration = config.clone();
                    }
                    if let Some(labels) = body.get("labels") {
                        ad.labels = labels.clone();
                    }
                    ad.modified_at = now_epoch();
                    let arn = ad_arn(account, region, workspace_id, id);
                    let tags = tag_map(state, &arn);
                    json_resp(
                        202,
                        &json!({
                            "anomalyDetectorId": id,
                            "arn": arn,
                            "status": { "statusCode": "ACTIVE" },
                            "tags": tags,
                        }),
                    )
                }
                None => not_found("AnomalyDetector not found", id, "AWS::APS::AnomalyDetector"),
            }
        }
        ("DELETE", Some(id)) => {
            let ws = state.workspaces.get_mut(workspace_id).expect("checked");
            if ws.anomaly_detectors.remove(id).is_some() {
                state
                    .tags
                    .remove(&ad_arn(account, region, workspace_id, id));
            }
            json_resp(202, &json!({}))
        }
        _ => not_found(
            "AnomalyDetector not found",
            ad_id.unwrap_or_default(),
            "AWS::APS::AnomalyDetector",
        ),
    }
}

// --- scrapers ----------------------------------------------------------------

fn op_scrapers(
    state: &mut AmpState,
    method: &str,
    scraper_id: Option<&str>,
    body: &Value,
    _query: &str,
    account: u64,
    region: &str,
) -> Response {
    match (method, scraper_id) {
        ("POST", None) => {
            let id = format!("s-{}", uuid::Uuid::new_v4());
            let arn = format!(
                "arn:{}:aps:{region}:{account}:scraper/{id}",
                partition(region)
            );
            let now = now_epoch();
            let scraper = Scraper {
                alias: body
                    .get("alias")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                scrape_configuration: body
                    .get("scrapeConfiguration")
                    .cloned()
                    .unwrap_or_else(|| json!({})),
                source: body.get("source").cloned().unwrap_or_else(|| json!({})),
                destination: body
                    .get("destination")
                    .cloned()
                    .unwrap_or_else(|| json!({})),
                role_configuration: body.get("roleConfiguration").cloned(),
                created_at: now,
                last_modified_at: now,
                logging_destination: None,
                scraper_components: None,
            };
            apply_tags(state, &arn, body.get("tags"));
            state.scrapers.insert(id.clone(), (arn.clone(), scraper));
            json_resp(
                202,
                &json!({
                    "scraperId": id,
                    "arn": arn,
                    "status": { "statusCode": "ACTIVE" },
                    "tags": body.get("tags").cloned().unwrap_or_else(|| json!({})),
                }),
            )
        }
        ("GET", None) => {
            let list: Vec<Value> = state
                .scrapers
                .iter()
                .map(|(id, (arn, s))| scraper_summary(id, arn, s, tag_map(state, arn)))
                .collect();
            json_resp(200, &json!({ "nextToken": Value::Null, "scrapers": list }))
        }
        ("GET", Some(id)) => match state.scrapers.get(id) {
            Some((arn, s)) => json_resp(
                200,
                &json!({ "scraper": scraper_dict(id, arn, s, tag_map(state, arn)) }),
            ),
            None => not_found("Scraper not found", id, "AWS::APS::Scraper"),
        },
        ("PUT", Some(id)) => match state.scrapers.get_mut(id) {
            Some((_, s)) => {
                if let Some(alias) = body.get("alias") {
                    s.alias = alias.as_str().unwrap_or_default().to_string();
                }
                if let Some(config) = body.get("scrapeConfiguration") {
                    s.scrape_configuration = config.clone();
                }
                if let Some(destination) = body.get("destination") {
                    s.destination = destination.clone();
                }
                if let Some(role) = body.get("roleConfiguration") {
                    s.role_configuration = Some(role.clone());
                }
                s.last_modified_at = now_epoch();
                let arn = state
                    .scrapers
                    .get(id)
                    .map(|(a, _)| a.clone())
                    .unwrap_or_default();
                json_resp(
                    202,
                    &json!({
                        "scraperId": id,
                        "arn": arn,
                        "status": { "statusCode": "ACTIVE" },
                        "tags": tag_map(state, &arn),
                    }),
                )
            }
            None => not_found("Scraper not found", id, "AWS::APS::Scraper"),
        },
        ("DELETE", Some(id)) => {
            if state.scrapers.remove(id).is_some() {
                json_resp(
                    202,
                    &json!({
                        "scraperId": id,
                        "status": { "statusCode": "DELETING" },
                    }),
                )
            } else {
                not_found("Scraper not found", id, "AWS::APS::Scraper")
            }
        }
        // moto's amp handlers answer unsupported verbs with a bare 400 and
        // an empty "{}" body (moto/amp/responses.py `_r("{}", 400)`)
        _ => json_resp(400, &json!({})),
    }
}

fn op_scraper_logging(
    state: &mut AmpState,
    method: &str,
    scraper_id: &str,
    body: &Value,
) -> Response {
    let entry = match state.scrapers.get_mut(scraper_id) {
        Some(entry) => entry,
        None => return not_found("Scraper not found", scraper_id, "AWS::APS::Scraper"),
    };
    let (_, scraper) = entry;
    match method {
        "GET" => {
            let mut out = json!({
                "scraperId": scraper_id,
                "status": { "statusCode": "ACTIVE" },
                "modifiedAt": scraper.last_modified_at,
            });
            if let Some(dest) = &scraper.logging_destination {
                out.as_object_mut()
                    .expect("out obj")
                    .insert("loggingDestination".to_string(), dest.clone());
            }
            if let Some(components) = &scraper.scraper_components {
                out.as_object_mut()
                    .expect("out obj")
                    .insert("scraperComponents".to_string(), components.clone());
            }
            json_resp(200, &out)
        }
        "PUT" => {
            scraper.logging_destination = body.get("loggingDestination").cloned();
            scraper.scraper_components = body.get("scraperComponents").cloned();
            scraper.last_modified_at = now_epoch();
            json_resp(202, &json!({ "status": { "statusCode": "ACTIVE" } }))
        }
        "DELETE" => {
            scraper.logging_destination = None;
            scraper.scraper_components = None;
            scraper.last_modified_at = now_epoch();
            json_resp(202, &json!({}))
        }
        // moto's amp handlers answer unsupported verbs with a bare 400 and
        // an empty "{}" body (moto/amp/responses.py `_r("{}", 400)`)
        _ => json_resp(400, &json!({})),
    }
}

const DEFAULT_SCRAPER_CONFIG: &str = "global:\n  scrape_interval: 30s\nscrape_configs:\n  - job_name: default\n    static_configs:\n      - targets: ['localhost:9090']\n";

// --- tags -------------------------------------------------------------------

fn op_tags(state: &mut AmpState, method: &str, path: &str, body: &Value, query: &str) -> Response {
    let arn = arn_from_tags_path(path);
    if arn.is_empty() {
        // Unreachable through the route table (the ARN segment is required);
        // keep the moto `tags()` fallthrough shape instead of an invented
        // error envelope.
        return json_resp(400, &json!({}));
    }
    match method {
        "GET" => json_resp(200, &json!({ "tags": tag_map(state, &arn) })),
        "POST" => {
            apply_tags(state, &arn, body.get("tags"));
            json_resp(200, &json!({}))
        }
        "DELETE" => {
            let keys = query_multi(query, "tagKeys");
            if let Some(entry) = state.tags.get_mut(&arn) {
                for key in keys {
                    entry.remove(&key);
                }
                if entry.is_empty() {
                    state.tags.remove(&arn);
                }
            }
            json_resp(200, &json!({}))
        }
        // moto's amp handlers answer unsupported verbs with a bare 400 and
        // an empty "{}" body (moto/amp/responses.py `_r("{}", 400)`)
        _ => json_resp(400, &json!({})),
    }
}

fn route(req: &Request) -> Response {
    // The gateway passes `uri.path()` (no query); be defensive if a query tail
    // sneaks in so tag-ARN extraction and slice routing stay stable.
    let path_only = req.path.split('?').next().unwrap_or("").to_string();
    if path_only.contains("/tags/") {
        return with_state(req.account, &req.region, |state| {
            op_tags(
                state,
                req.method.as_str(),
                &path_only,
                &parse_body(&req.body),
                &req.query_string,
            )
        });
    }
    let segments_owned: Vec<String> = path_only
        .split('/')
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();
    let segments = segments_owned
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    match segments.as_slice() {
        ["workspaces"] | ["workspaces", ""] => with_state(req.account, &req.region, |state| {
            op_workspaces(
                state,
                &req.method,
                None,
                &parse_body(&req.body),
                &req.query_string,
                req.account,
                &req.region,
            )
        }),
        ["scraperconfiguration"] if req.method == "GET" => json_resp(
            200,
            &json!({
                "configuration": base64_encode(DEFAULT_SCRAPER_CONFIG.as_bytes()),
            }),
        ),
        ["scrapers"] => with_state(req.account, &req.region, |state| {
            op_scrapers(
                state,
                &req.method,
                None,
                &parse_body(&req.body),
                &req.query_string,
                req.account,
                &req.region,
            )
        }),
        ["scrapers", id] => with_state(req.account, &req.region, |state| {
            op_scrapers(
                state,
                &req.method,
                Some(id),
                &parse_body(&req.body),
                &req.query_string,
                req.account,
                &req.region,
            )
        }),
        ["scrapers", id, "logging-configuration"] => {
            with_state(req.account, &req.region, |state| {
                op_scraper_logging(state, &req.method, id, &parse_body(&req.body))
            })
        }
        ["workspaces", wsid, "alias"] => with_state(req.account, &req.region, |state| {
            op_workspace_alias(state, wsid, &parse_body(&req.body))
        }),
        ["workspaces", wsid] => with_state(req.account, &req.region, |state| {
            op_workspaces(
                state,
                &req.method,
                Some(wsid),
                &parse_body(&req.body),
                &req.query_string,
                req.account,
                &req.region,
            )
        }),
        ["workspaces", wsid, "logging"] => with_state(req.account, &req.region, |state| {
            op_workspace_logging(state, &req.method, wsid, &parse_body(&req.body))
        }),
        ["workspaces", wsid, "logging", "query"] => with_state(req.account, &req.region, |state| {
            op_query_logging(state, &req.method, wsid, &parse_body(&req.body))
        }),
        ["workspaces", wsid, "rulegroupsnamespaces"] => {
            with_state(req.account, &req.region, |state| {
                op_rule_group_namespaces(
                    state,
                    &req.method,
                    wsid,
                    None,
                    &parse_body(&req.body),
                    &req.query_string,
                    req.account,
                    &req.region,
                )
            })
        }
        ["workspaces", wsid, "rulegroupsnamespaces", name] => {
            with_state(req.account, &req.region, |state| {
                op_rule_group_namespaces(
                    state,
                    &req.method,
                    wsid,
                    Some(name),
                    &parse_body(&req.body),
                    &req.query_string,
                    req.account,
                    &req.region,
                )
            })
        }
        ["workspaces", wsid, "alertmanager", "definition"] => {
            with_state(req.account, &req.region, |state| {
                op_alert_manager_definition(state, &req.method, wsid, &parse_body(&req.body))
            })
        }
        ["workspaces", wsid, "anomalydetectors"] => with_state(req.account, &req.region, |state| {
            op_anomaly_detectors(
                state,
                &req.method,
                wsid,
                None,
                &parse_body(&req.body),
                &req.query_string,
                req.account,
                &req.region,
            )
        }),
        ["workspaces", wsid, "anomalydetectors", ad_id] => {
            with_state(req.account, &req.region, |state| {
                op_anomaly_detectors(
                    state,
                    &req.method,
                    wsid,
                    Some(ad_id),
                    &parse_body(&req.body),
                    &req.query_string,
                    req.account,
                    &req.region,
                )
            })
        }
        ["workspaces", wsid, "policy"] => with_state(req.account, &req.region, |state| {
            op_workspace_policy(state, &req.method, wsid, &parse_body(&req.body))
        }),
        ["workspaces", wsid, "configuration"] => with_state(req.account, &req.region, |state| {
            op_workspace_configuration(state, &req.method, wsid, &parse_body(&req.body))
        }),
        // [wsid, "<sub>"] arms handled above via segment lists
        _ => json_resp(
            400,
            &json!({
                "__type": "NotImplemented",
                "message": format!("amp path {}", req.path),
            }),
        ),
    }
}

pub fn dispatch(req: &Request) -> Response {
    route(req)
}

/// JSON body (empty bodies parsed as {}).
fn parse_body(body: &[u8]) -> Value {
    crate::proto::parse_json_body(body)
}

fn base64_encode(data: &[u8]) -> String {
    const ALPHA: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let n = (chunk[0] as u32) << 16
            | (chunk.get(1).copied().unwrap_or(0) as u32) << 8
            | (chunk.get(2).copied().unwrap_or(0) as u32);
        out.push(ALPHA[(n >> 18) as usize & 63] as char);
        out.push(ALPHA[(n >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            out.push(ALPHA[(n >> 6) as usize & 63] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(ALPHA[n as usize & 63] as char);
        } else {
            out.push('=');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const ACCOUNT: u64 = 123456789012;
    const REGION: &str = "us-east-1";

    fn run(method: &str, path: &str, body: Value) -> Response {
        let (path_only, query) = match path.split_once('?') {
            Some((p, q)) => (p.to_string(), q.to_string()),
            None => (path.to_string(), String::new()),
        };
        dispatch(&Request {
            service: "amp".to_string(),
            operation: String::new(),
            method: method.to_string(),
            path: path_only,
            query_string: query,
            headers: std::collections::HashMap::new(),
            body: serde_json::to_vec(&body).unwrap_or_default().into(),
            account: ACCOUNT,
            region: REGION.to_string(),
        })
    }

    fn body_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    #[test]
    fn workspace_lifecycle() {
        let created = run(
            "POST",
            "/workspaces",
            serde_json::json!({"alias": "ws-amp-unit"}),
        );
        assert_eq!(created.status, 200);
        let created_body = body_of(&created);
        let ws_id = created_body["workspaceId"].as_str().unwrap().to_string();
        assert!(created_body["arn"]
            .as_str()
            .unwrap()
            .contains("arn:aws:aps:"));
        assert!(created_body["tags"].is_object());

        let listed = body_of(&run("GET", "/workspaces", serde_json::json!({})));
        assert!(listed["workspaces"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w["workspaceId"] == ws_id.as_str()));

        let described = body_of(&run(
            "GET",
            &format!("/workspaces/{ws_id}"),
            serde_json::json!({}),
        ));
        assert_eq!(described["workspace"]["workspaceId"], ws_id.as_str());

        run(
            "DELETE",
            &format!("/workspaces/{ws_id}"),
            serde_json::json!({}),
        );
        let _gone = run(
            "GET",
            &format!("/workspaces/{ws_id}"),
            serde_json::json!({}),
        );
        assert_eq!(gone_status(&format!("/workspaces/{ws_id}")), 404);
    }

    fn gone_status(path: &str) -> u16 {
        run("GET", path, serde_json::json!({})).status
    }

    #[test]
    fn workspace_missing_gives_rnfe_with_header() {
        let resp = run(
            "GET",
            "/workspaces/ws-does-not-exist",
            serde_json::json!({}),
        );
        assert_eq!(resp.status, 404);
        assert!(resp
            .headers
            .iter()
            .any(|(k, v)| k == "x-amzn-ErrorType" && v.contains("ResourceNotFoundException")));
    }

    #[test]
    fn rule_group_namespace_lifecycle() {
        let created_ws = body_of(&run(
            "POST",
            "/workspaces",
            serde_json::json!({"alias": "ws-rgn"}),
        ));
        let ws_id = created_ws["workspaceId"].as_str().unwrap().to_string();
        let path = format!("/workspaces/{ws_id}/rulegroupsnamespaces");
        let created = body_of(&run(
            "POST",
            &path,
            serde_json::json!({"name": "rg1", "data": "groups: []"}),
        ));
        assert_eq!(created["name"], "rg1");
        let listed = body_of(&run("GET", &path, serde_json::json!({})));
        assert_eq!(listed["ruleGroupsNamespaces"].as_array().unwrap().len(), 1);

        let described = body_of(&run("GET", &format!("{path}/rg1"), serde_json::json!({})));
        assert_eq!(described["ruleGroupsNamespace"]["name"], "rg1");

        run(
            "PUT",
            &format!("{path}/rg1"),
            serde_json::json!({"data": "groups: []v2"}),
        );
        let described = body_of(&run("GET", &format!("{path}/rg1"), serde_json::json!({})));
        assert_eq!(described["ruleGroupsNamespace"]["data"], "groups: []v2");

        run("DELETE", &format!("{path}/rg1"), serde_json::json!({}));
        let listed = body_of(&run("GET", &path, serde_json::json!({})));
        assert!(listed["ruleGroupsNamespaces"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn tag_resource_roundtrip() {
        let created_ws = body_of(&run(
            "POST",
            "/workspaces",
            serde_json::json!({"alias": "ws-tag"}),
        ));
        let arn = created_ws["arn"].as_str().unwrap().to_string();
        let encoded = arn.replace(':', "%3A").replace('/', "%2F");
        let tags_path = format!("/tags/{encoded}");
        run(
            "POST",
            &tags_path,
            serde_json::json!({"tags": {"team": "platform"}}),
        );
        let listed = body_of(&run("GET", &tags_path, serde_json::json!({})));
        assert_eq!(listed["tags"]["team"], "platform");
        run(
            "DELETE",
            &format!("{tags_path}?tagKeys=team"),
            serde_json::json!({}),
        );
        let listed = body_of(&run("GET", &tags_path, serde_json::json!({})));
        assert!(listed["tags"].as_object().unwrap().is_empty());
    }

    #[test]
    fn scraper_lifecycle() {
        let created_ws = body_of(&run(
            "POST",
            "/workspaces",
            serde_json::json!({"alias": "ws-scr"}),
        ));
        let ws_arn = created_ws["arn"].as_str().unwrap().to_string();
        let created = body_of(&run(
            "POST",
            "/scrapers",
            serde_json::json!({
                "alias": "scraper-1",
                "scrapeConfiguration": {"configurationBlob": "QUJD"},
                "source": {"eksConfiguration": {"clusterArn": "arn:aws:eks:us-east-1:123456789012:cluster/c", "subnetIds": ["subnet-1"]}},
                "destination": {"ampConfiguration": {"workspaceArn": ws_arn}},
            }),
        ));
        let scraper_id = created["scraperId"].as_str().unwrap().to_string();
        let described = body_of(&run(
            "GET",
            &format!("/scrapers/{scraper_id}"),
            serde_json::json!({}),
        ));
        assert_eq!(described["scraper"]["alias"], "scraper-1");

        let updated = run(
            "PUT",
            &format!("/scrapers/{scraper_id}"),
            serde_json::json!({"alias": "scraper-2"}),
        );
        assert_eq!(updated.status, 202);
        run(
            "DELETE",
            &format!("/scrapers/{scraper_id}"),
            serde_json::json!({}),
        );
        let missing = run(
            "GET",
            &format!("/scrapers/{scraper_id}"),
            serde_json::json!({}),
        );
        assert_eq!(missing.status, 404);
    }

    #[test]
    fn default_scraper_configuration() {
        let resp = run("GET", "/scraperconfiguration", serde_json::json!({}));
        let body = body_of(&resp);
        assert!(body["configuration"].is_string());
    }

    #[test]
    fn alertmanager_definition_statuses() {
        let created_ws = body_of(&run(
            "POST",
            "/workspaces",
            serde_json::json!({"alias": "ws-amd"}),
        ));
        let ws_id = created_ws["workspaceId"].as_str().unwrap().to_string();
        let path = format!("/workspaces/{ws_id}/alertmanager/definition");
        let created = run("POST", &path, serde_json::json!({"data": "config"}));
        assert_eq!(created.status, 202);
        let described = body_of(&run("GET", &path, serde_json::json!({})));
        assert_eq!(described["alertManagerDefinition"]["data"], "config");
        let deleted = run("DELETE", &path, serde_json::json!({}));
        assert_eq!(deleted.status, 202);
    }
}
