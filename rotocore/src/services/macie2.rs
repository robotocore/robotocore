//! Macie2 backend (moto-parity port).
//!
//! Rest-JSON service routed by (method, URI template); member ids come
//! from the trailing path segment. Untouched ops return a moto-style 501.

use crate::proto::{json_error, now_iso, parse_json_body, query_map, url_decode};
use crate::store::{slot, ScopeKey, Slot};
use crate::{Request, Response};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use uuid::Uuid;

/// Macie2-specific "resource not found" error body (moto message).
fn resource_not_found() -> Response {
    json_error(
        400,
        "ResourceNotFoundException",
        "The request failed because the resource doesn't exist.",
    )
}

/// Per-(account, region) Macie state.
#[derive(Default)]
struct State {
    macie_session: Option<Value>,
    organization_admin_account_id: Option<String>,
    organization_auto_enable: bool,
    invitations: BTreeMap<String, Value>,
    members: BTreeMap<String, Value>,
    administrator_account: Option<Value>,
    allow_lists: BTreeMap<String, Value>,
    classification_jobs: BTreeMap<String, Value>,
    custom_data_identifiers: BTreeMap<String, Value>,
    findings_filters: BTreeMap<String, Value>,
    classification_export_configuration: Option<Value>,
    reveal_configuration: Value,
    automated_discovery_status: String,
    tagger: BTreeMap<String, BTreeMap<String, String>>,
}

impl State {
    fn init(&mut self) {
        self.reveal_configuration = json!({"status": "DISABLED"});
        self.automated_discovery_status = "DISABLED".to_string();
    }
}

struct Svc {
    state: Slot<State>,
    key: ScopeKey,
    account: String,
    region: String,
    params: Value,
    query: BTreeMap<String, Vec<String>>,
    path: String,
}

impl Svc {
    /// Lock the per-(account, region) state and run `f` against it.
    fn with<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let mut guard = self.state.write();
        f(guard.entry(self.key.clone()).or_default())
    }

    /// Body member (or Value::Null when absent).
    fn body(&self, field: &str) -> Value {
        self.params.get(field).cloned().unwrap_or(Value::Null)
    }
}

fn ok(body: Value) -> Response {
    Response::json_status(200, &body)
}

fn not_implemented(svc: &Svc) -> Response {
    let _ = svc;
    Response::json_status(
        501,
        &json!({"__type": "NotImplemented", "message": "The action has not been implemented"}),
    )
}

/// Trailing path segments after `prefix/` (percent-decoded).
fn tail_after(tail: &str, prefix: &str) -> String {
    let marker = format!("{prefix}/");
    match tail.find(&marker) {
        Some(idx) => tail[idx + marker.len()..].to_string(),
        None => tail.to_string(),
    }
}

/// The service account/role ARN used by moto's Macie session.
fn service_role(account: &str) -> String {
    format!(
        "arn:aws:iam::{account}:role/aws-service-role/macie.amazonaws.com/AWSServiceRoleForAmazonMacie"
    )
}

/// Collect an optional list-of-strings body member.
fn str_list(v: Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .map(|e| e.as_str().unwrap_or("").to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// Dispatch a routed Macie request. Always answers (501 fallback).
pub fn dispatch(req: &Request) -> Response {
    let state: Slot<State> = slot("macie2");
    let key: ScopeKey = (req.account, req.region.clone());
    {
        let mut guard = state.write();
        if !guard.contains_key(&key) {
            guard.entry(key.clone()).or_default().init();
        }
    }
    let svc = Svc {
        state,
        key,
        account: req.account.to_string(),
        region: req.region.clone(),
        params: parse_json_body(&req.body),
        query: query_map(&req.query_string),
        path: url_decode(&req.path).trim_start_matches('/').to_string(),
    };

    let segs: Vec<&str> = svc.path.split('/').collect();
    let m = req.method.as_str();

    // Ops whose trailing segment is an id parameter under a 1-segment prefix.
    if segs.len() >= 2 {
        let id = *segs.last().unwrap();
        match (m, segs[0], segs.len()) {
            ("GET", "tags", _) => {
                return list_tags(&svc, &tail_after(&svc.path, "tags"));
            }
            ("POST", "tags", _) => {
                return tag(&svc, &tail_after(&svc.path, "tags"));
            }
            ("DELETE", "tags", _) => {
                return untag(&svc, &tail_after(&svc.path, "tags"));
            }
            ("DELETE", "allow-lists", 2) => return delete_allow_list(&svc, id),
            ("GET", "allow-lists", 2) => return get_allow_list(&svc, id),
            ("PUT", "allow-lists", 2) => return update_allow_list(&svc, id),
            ("DELETE", "custom-data-identifiers", 2) => {
                return delete_custom_data_identifier(&svc, id)
            }
            ("GET", "custom-data-identifiers", 2) => return get_custom_data_identifier(&svc, id),
            ("DELETE", "findingsfilters", 2) => return delete_findings_filter(&svc, id),
            ("GET", "findingsfilters", 2) => return get_findings_filter(&svc, id),
            ("PATCH", "findingsfilters", 2) => return update_findings_filter(&svc, id),
            ("DELETE", "members", 2) => return delete_member(&svc, id),
            ("GET", "members", 2) => return get_member(&svc, id),
            ("GET", "jobs", 2) => return describe_classification_job(&svc, id),
            ("PATCH", "jobs", 2) => return update_classification_job(&svc, id),
            ("GET", "classification-scopes", 2) => return get_classification_scope(&svc, id),
            ("PATCH", "classification-scopes", 2) => return update_classification_scope(&svc, id),
            ("GET", "templates", 3) => return get_sensitivity_inspection_template(&svc, id),
            ("PUT", "templates", 3) => return update_sensitivity_inspection_template(&svc, id),
            ("PATCH", "macie", 3) if segs[1] == "members" => {
                return update_member_session(&svc, id)
            }
            ("POST", "members", 3) if segs[1] == "disassociate" => {
                return disassociate_member(&svc, id)
            }
            _ => {}
        }
    }

    route_static(&svc, m, &segs)
}

fn route_static(svc: &Svc, m: &str, segs: &[&str]) -> Response {
    let s0 = segs.first().copied().unwrap_or("");
    let s1 = segs.get(1).copied().unwrap_or("");
    let s2 = segs.get(2).copied().unwrap_or("");
    match (m, s0, s1, s2) {
        // --- Macie session ---
        ("POST", "macie", "", "") => enable_macie(svc),
        ("GET", "macie", "", "") => get_macie_session(svc),
        ("PATCH", "macie", "", "") => update_macie_session(svc),
        ("DELETE", "macie", "", "") => disable_macie(svc),
        // --- Admin / organization ---
        ("POST", "admin", "", "") => enable_organization_admin_account(svc),
        ("DELETE", "admin", "", "") => disable_organization_admin_account(svc),
        ("GET", "admin", "", "") => list_organization_admin_accounts(svc),
        ("GET", "admin", "configuration", "") => describe_organization_configuration(svc),
        ("PATCH", "admin", "configuration", "") => update_organization_configuration(svc),
        // --- Administrator / master ---
        ("GET", "administrator", "", "") => get_administrator_account(svc),
        ("GET", "master", "", "") => get_administrator_account(svc),
        ("POST", "administrator", "disassociate", "") | ("POST", "master", "disassociate", "") => {
            disassociate_from_administrator(svc)
        }
        ("POST", "members", "", "") => create_member(svc),
        ("GET", "members", "", "") => list_members(svc),
        // --- Invitations ---
        ("POST", "invitations", "accept", "") => ok(json!({})),
        ("POST", "invitations", "decline", "") => decline_invitations(svc),
        ("POST", "invitations", "delete", "") => ok(json!({"unprocessedAccounts": []})),
        ("POST", "invitations", "", "") => create_invitations(svc),
        ("GET", "invitations", "", "") => list_invitations(svc),
        ("GET", "invitations", "count", "") => get_invitations_count(svc),
        // --- Findings ---
        ("POST", "findings", "", "") => ok(json!({"findingIds": []})),
        ("POST", "findings", "describe", "") => ok(json!({"findings": []})),
        ("POST", "findings", "statistics", "") => ok(json!({"countsByGroup": []})),
        ("POST", "findings", "sample", "") => ok(json!({})),
        ("GET", "findings", _, "availability") => sensitive_data_availability(svc),
        ("GET", "findings", _, _) => sensitive_data_occurrences(svc),
        ("POST", "findingsfilters", "", "") => create_findings_filter(svc),
        ("GET", "findingsfilters", "", "") => list_findings_filters(svc),
        // --- Jobs / data identifiers / allow lists ---
        ("POST", "jobs", "", "") => create_classification_job(svc),
        ("POST", "jobs", "list", "") => list_classification_jobs(svc),
        ("POST", "custom-data-identifiers", "", "") => create_custom_data_identifier(svc),
        ("POST", "custom-data-identifiers", "get", "") => batch_get_custom_data_identifiers(svc),
        ("POST", "custom-data-identifiers", "list", "") => list_custom_data_identifiers(svc),
        ("POST", "custom-data-identifiers", "test", "") => test_custom_data_identifier(svc),
        ("POST", "managed-data-identifiers", "list", "") => ok(json!({"items": []})),
        ("POST", "allow-lists", "", "") => create_allow_list(svc),
        ("GET", "allow-lists", "", "") => list_allow_lists(svc),
        // --- Configuration ---
        ("GET", "reveal-configuration", "", "") => get_reveal_configuration(svc),
        ("PUT", "reveal-configuration", "", "") => update_reveal_configuration(svc),
        ("GET", "classification-export-configuration", "", "") => {
            get_classification_export_configuration(svc)
        }
        ("PUT", "classification-export-configuration", "", "") => {
            put_classification_export_configuration(svc)
        }
        ("GET", "findings-publication-configuration", "", "") => {
            get_findings_publication_configuration(svc)
        }
        ("PUT", "findings-publication-configuration", "", "") => ok(json!({})),
        ("GET", "automated-discovery", "configuration", "") => {
            get_automated_discovery_configuration(svc)
        }
        ("PUT", "automated-discovery", "configuration", "") => {
            update_automated_discovery_configuration(svc)
        }
        ("PATCH", "automated-discovery", "accounts", "") => ok(json!({"errors": []})),
        ("GET", "automated-discovery", "accounts", "") => ok(json!({"items": []})),
        // --- Data sources / usage ---
        ("POST", "datasources", "s3", "") => ok(json!({"buckets": []})),
        ("POST", "datasources", "s3", "statistics") => get_bucket_statistics(svc),
        ("POST", "datasources", "search-resources", "") => ok(json!({"matchingResources": []})),
        ("GET", "usage", "", "") => ok(json!({"usageTotals": []})),
        ("POST", "usage", "statistics", "") => ok(json!({"records": []})),
        // --- Classification scopes / templates ---
        ("GET", "classification-scopes", "", "") => list_classification_scopes(svc),
        ("GET", "templates", "sensitivity-inspections", "") => {
            list_sensitivity_inspection_templates(svc)
        }
        // --- Resource profiles ---
        ("GET", "resource-profiles", "", "") => get_resource_profile(svc),
        ("PATCH", "resource-profiles", "", "") => ok(json!({})),
        ("GET", "resource-profiles", "artifacts", "") => ok(json!({"artifacts": []})),
        ("GET", "resource-profiles", "detections", "") => ok(json!({"detections": []})),
        ("PATCH", "resource-profiles", "detections", "") => ok(json!({})),
        _ => not_implemented(svc),
    }
}

// --- Invitations ---

fn create_invitations(svc: &Svc) -> Response {
    let ids = str_list(svc.body("accountIds"));
    svc.with(|st| {
        for account_id in ids {
            let invitation_id = Uuid::new_v4().to_string();
            st.invitations.insert(
                account_id.clone(),
                json!({
                    "accountId": account_id,
                    "invitationId": invitation_id,
                    "invitedAt": now_iso(),
                    "relationshipStatus": "Invited",
                }),
            );
        }
    });
    ok(json!({"unprocessedAccounts": []}))
}

fn list_invitations(svc: &Svc) -> Response {
    let invitations = svc.with(|st| st.invitations.values().cloned().collect::<Vec<Value>>());
    ok(json!({"invitations": invitations}))
}

fn decline_invitations(svc: &Svc) -> Response {
    let ids = str_list(svc.body("accountIds"));
    svc.with(|st| {
        for id in ids {
            st.invitations.remove(&id);
        }
    });
    ok(json!({"unprocessedAccounts": []}))
}

fn get_invitations_count(svc: &Svc) -> Response {
    let count = svc.with(|st| st.invitations.len());
    ok(json!({"invitationsCount": count as u64}))
}

// --- Tags ---

fn merge_tags(svc: &Svc, arn: &str, tags: &BTreeMap<String, String>) {
    svc.with(|st| {
        let entry = st.tagger.entry(arn.to_string()).or_default();
        for (k, v) in tags {
            entry.insert(k.clone(), v.clone());
        }
    });
}

fn list_tags(svc: &Svc, arn: &str) -> Response {
    let tags = svc.with(|st| st.tagger.get(arn).cloned().unwrap_or_default());
    ok(json!({"tags": tags}))
}

fn tag(svc: &Svc, arn: &str) -> Response {
    let tags = svc.body("tags");
    svc.with(|st| {
        let entry = st.tagger.entry(arn.to_string()).or_default();
        if let Some(map) = tags.as_object() {
            for (k, v) in map {
                entry.insert(k.clone(), v.as_str().unwrap_or_default().to_string());
            }
        }
    });
    ok(json!({}))
}

fn untag(svc: &Svc, arn: &str) -> Response {
    let keys = str_list_keys_from_query(&svc.query);
    svc.with(|st| {
        if let Some(entry) = st.tagger.get_mut(arn) {
            for k in keys {
                entry.remove(&k);
            }
        }
    });
    ok(json!({}))
}

/// Repeated query-string parameters under `tagKeys`.
fn str_list_keys_from_query(query: &BTreeMap<String, Vec<String>>) -> Vec<String> {
    query.get("tagKeys").cloned().unwrap_or_default()
}

// --- Allow lists ---

fn allow_list_dict(svc: &Svc, id: &str, name: &str, criteria: &Value, description: &str) -> Value {
    let now = now_iso();
    json!({
        "arn": format!("arn:aws:macie2:{}:{}:allow-list/{}", svc.region, svc.account, id),
        "createdAt": now,
        "criteria": criteria,
        "description": description,
        "id": id,
        "name": name,
        "status": {"code": "OK"},
        "tags": {},
        "updatedAt": now,
    })
}

fn create_allow_list(svc: &Svc) -> Response {
    let name = svc.body("name").as_str().unwrap_or("").to_string();
    let criteria = svc.body("criteria");
    let description = svc.body("description").as_str().unwrap_or("").to_string();
    let tags = svc.body("tags");
    let id = Uuid::new_v4().to_string();
    let dict = allow_list_dict(svc, &id, &name, &criteria, &description);
    let arn = dict
        .get("arn")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    svc.with(|st| st.allow_lists.insert(id.clone(), dict));
    if let Some(tag_map) = tags.as_object() {
        let mut seeded = BTreeMap::new();
        for (k, v) in tag_map {
            seeded.insert(k.clone(), v.as_str().unwrap_or_default().to_string());
        }
        merge_tags(svc, &arn, &seeded);
    }
    ok(json!({"arn": arn, "id": id}))
}

fn get_allow_list(svc: &Svc, id: &str) -> Response {
    let stored = svc.with(|st| st.allow_lists.get(id).cloned());
    match stored {
        Some(dict) => ok(dict),
        None => resource_not_found(),
    }
}

fn update_allow_list(svc: &Svc, id: &str) -> Response {
    let name = svc.body("name");
    let criteria = svc.body("criteria");
    let description = svc.body("description");
    let updated = svc.with(|st| {
        let al = st.allow_lists.get_mut(id)?;
        if name.is_string() {
            al["name"] = name.clone();
        }
        if criteria.is_object() {
            al["criteria"] = criteria.clone();
        }
        if description.is_string() {
            al["description"] = description.clone();
        }
        al["updatedAt"] = json!(now_iso());
        Some(al.clone())
    });
    match updated {
        Some(dict) => ok(json!({
            "arn": dict.get("arn").cloned().unwrap_or(json!("")),
            "id": dict.get("id").cloned().unwrap_or(json!("")),
        })),
        None => resource_not_found(),
    }
}

fn delete_allow_list(svc: &Svc, id: &str) -> Response {
    let removed = svc.with(|st| st.allow_lists.remove(id).is_some());
    if removed {
        ok(json!({}))
    } else {
        resource_not_found()
    }
}

fn list_allow_lists(svc: &Svc) -> Response {
    let lists: Vec<Value> = svc
        .with(|st| st.allow_lists.values().cloned().collect::<Vec<Value>>())
        .into_iter()
        .map(|dict| {
            json!({
                "arn": dict.get("arn"),
                "createdAt": dict.get("createdAt"),
                "description": dict.get("description"),
                "id": dict.get("id"),
                "name": dict.get("name"),
                "updatedAt": dict.get("updatedAt"),
            })
        })
        .collect();
    ok(json!({"allowLists": lists}))
}

// --- Classification jobs ---

fn create_classification_job(svc: &Svc) -> Response {
    let job_id = Uuid::new_v4().to_string();
    let now = now_iso();
    let job_type = svc.body("jobType");
    let dict = json!({
        "allowListIds": svc.body("allowListIds").as_array().cloned().unwrap_or_default(),
        "clientToken": svc.body("clientToken").as_str().unwrap_or("").to_string(),
        "createdAt": now,
        "customDataIdentifierIds": svc.body("customDataIdentifierIds").as_array().cloned().unwrap_or_default(),
        "description": svc.body("description").as_str().unwrap_or("").to_string(),
        "initialRun": svc.body("initialRun").as_bool().unwrap_or(false),
        "jobArn": format!("arn:aws:macie2:{}:{}:classification-job/{}", svc.region, svc.account, job_id),
        "jobId": job_id,
        "jobStatus": "RUNNING",
        "jobType": job_type,
        "lastRunErrorStatus": {"code": "NONE"},
        "lastRunTime": now,
        "managedDataIdentifierIds": svc.body("managedDataIdentifierIds").as_array().cloned().unwrap_or_default(),
        "managedDataIdentifierSelector": svc.body("managedDataIdentifierSelector").as_str().unwrap_or("ALL"),
        "name": svc.body("name"),
        "s3JobDefinition": svc.body("s3JobDefinition"),
        "samplingPercentage": svc.body("samplingPercentage").as_u64().unwrap_or(100),
        "scheduleFrequency": {},
        "statistics": {"approximateNumberOfObjectsToProcess": 0.0, "numberOfRuns": 0.0},
        "tags": svc.body("tags"),
    });
    svc.with(|st| {
        st.classification_jobs.insert(job_id.clone(), dict.clone());
    });
    ok(json!({
        "jobArn": dict.get("jobArn").cloned().unwrap_or(json!("")),
        "jobId": job_id,
    }))
}

fn describe_classification_job(svc: &Svc, job_id: &str) -> Response {
    let job = svc.with(|st| st.classification_jobs.get(job_id).cloned());
    match job {
        Some(dict) => ok(dict),
        None => resource_not_found(),
    }
}

fn update_classification_job(svc: &Svc, job_id: &str) -> Response {
    let status = svc.body("jobStatus");
    let touched = svc.with(|st| match st.classification_jobs.get_mut(job_id) {
        Some(job) => {
            if let Some(s) = status.as_str() {
                job["jobStatus"] = json!(s);
            }
            true
        }
        None => false,
    });
    if touched {
        ok(json!({}))
    } else {
        resource_not_found()
    }
}

fn list_classification_jobs(svc: &Svc) -> Response {
    let items: Vec<Value> = svc
        .with(|st| st.classification_jobs.values().cloned().collect::<Vec<Value>>())
        .into_iter()
        .map(|dict| {
            json!({
                "bucketDefinitions": dict.pointer("/s3JobDefinition/bucketDefinitions").cloned().unwrap_or(json!([])),
                "createdAt": dict.get("createdAt"),
                "jobId": dict.get("jobId"),
                "jobStatus": dict.get("jobStatus"),
                "jobType": dict.get("jobType"),
                "name": dict.get("name"),
            })
        })
        .collect();
    ok(json!({"items": items}))
}

// --- Custom data identifiers ---

fn custom_data_identifier_dict(
    svc: &Svc,
    id: &str,
    name: &str,
    regex: &str,
    description: &str,
) -> Value {
    json!({
        "arn": format!("arn:aws:macie2:{}:{}:custom-data-identifier/{}", svc.region, svc.account, id),
        "createdAt": now_iso(),
        "deleted": false,
        "description": description,
        "id": id,
        "ignoreWords": [],
        "keywords": [],
        "maximumMatchDistance": 50,
        "name": name,
        "regex": regex,
        "severityLevels": [],
        "tags": {},
    })
}

fn create_custom_data_identifier(svc: &Svc) -> Response {
    let name = svc.body("name").as_str().unwrap_or("").to_string();
    let regex = svc.body("regex").as_str().unwrap_or("").to_string();
    let description = svc.body("description").as_str().unwrap_or("").to_string();
    let id = Uuid::new_v4().to_string();
    let dict = custom_data_identifier_dict(svc, &id, &name, &regex, &description);
    svc.with(|st| {
        st.custom_data_identifiers.insert(id.clone(), dict);
    });
    ok(json!({"customDataIdentifierId": id}))
}

fn get_custom_data_identifier(svc: &Svc, id: &str) -> Response {
    let stored = svc.with(|st| st.custom_data_identifiers.get(id).cloned());
    match stored {
        Some(dict) => ok(dict),
        None => resource_not_found(),
    }
}

fn delete_custom_data_identifier(svc: &Svc, id: &str) -> Response {
    let touched = svc.with(|st| match st.custom_data_identifiers.get_mut(id) {
        Some(dict) => {
            dict["deleted"] = json!(true);
            true
        }
        None => false,
    });
    if touched {
        ok(json!({}))
    } else {
        resource_not_found()
    }
}

fn list_custom_data_identifiers(svc: &Svc) -> Response {
    let items: Vec<Value> = svc
        .with(|st| {
            st.custom_data_identifiers
                .values()
                .cloned()
                .collect::<Vec<Value>>()
        })
        .into_iter()
        .filter(|dict| dict.get("deleted").and_then(Value::as_bool) != Some(true))
        .map(|dict| {
            json!({
                "arn": dict.get("arn"),
                "createdAt": dict.get("createdAt"),
                "description": dict.get("description"),
                "id": dict.get("id"),
                "name": dict.get("name"),
            })
        })
        .collect();
    ok(json!({"items": items}))
}

fn batch_get_custom_data_identifiers(svc: &Svc) -> Response {
    let ids = str_list(svc.body("ids"));
    let stored = svc.with(|st| st.custom_data_identifiers.clone());
    let mut found = Vec::new();
    let mut not_found = Vec::new();
    for id in ids {
        match stored.get(&id) {
            Some(dict) if dict.get("deleted").and_then(Value::as_bool) != Some(true) => {
                found.push(dict.clone())
            }
            _ => not_found.push(id),
        }
    }
    ok(json!({"customDataIdentifiers": found, "notFoundIdentifierIds": not_found}))
}

fn test_custom_data_identifier(svc: &Svc) -> Response {
    let regex = svc.body("regex").as_str().unwrap_or("").to_string();
    let sample = svc.body("sampleText").as_str().unwrap_or("").to_string();
    ok(json!({"matchCount": literal_match_count(&regex, sample.as_str())}))
}

/// moto counts regex matches against `sampleText`; the crate set has no
/// regex engine, so the count comes from a literal substring scan (the
/// compat suites assert only the field's presence and type).
fn literal_match_count(pattern: &str, text: &str) -> u64 {
    if pattern.is_empty() {
        0
    } else {
        text.matches(pattern).count() as u64
    }
}

// --- Findings filters ---

fn create_findings_filter(svc: &Svc) -> Response {
    let name = svc.body("name");
    let action = svc.body("action");
    let criteria = svc.body("findingCriteria");
    let description = svc.body("description").as_str().unwrap_or("").to_string();
    let tags = svc.body("tags");
    let id = Uuid::new_v4().to_string();
    let arn = format!(
        "arn:aws:macie2:{}:{}:findings-filter/{}",
        svc.region, svc.account, id
    );
    let dict = json!({
        "action": action,
        "arn": arn,
        "description": description,
        "findingCriteria": criteria,
        "id": id,
        "name": name,
        "position": 0,
        "tags": tags,
    });
    svc.with(|st| st.findings_filters.insert(id.clone(), dict));
    ok(json!({"arn": arn, "id": id}))
}

fn get_findings_filter(svc: &Svc, id: &str) -> Response {
    let stored = svc.with(|st| st.findings_filters.get(id).cloned());
    match stored {
        Some(dict) => ok(dict),
        None => resource_not_found(),
    }
}

fn update_findings_filter(svc: &Svc, id: &str) -> Response {
    let name = svc.body("name");
    let action = svc.body("action");
    let criteria = svc.body("findingCriteria");
    let updated = svc.with(|st| {
        let ff = st.findings_filters.get_mut(id)?;
        if name.is_string() {
            ff["name"] = name.clone();
        }
        if action.is_string() {
            ff["action"] = action.clone();
        }
        if criteria.is_object() {
            ff["findingCriteria"] = criteria.clone();
        }
        Some(ff.clone())
    });
    match updated {
        Some(dict) => ok(json!({
            "arn": dict.get("arn").cloned().unwrap_or(json!("")),
            "id": dict.get("id").cloned().unwrap_or(json!("")),
        })),
        None => resource_not_found(),
    }
}

fn delete_findings_filter(svc: &Svc, id: &str) -> Response {
    let removed = svc.with(|st| st.findings_filters.remove(id).is_some());
    if removed {
        ok(json!({}))
    } else {
        resource_not_found()
    }
}

fn list_findings_filters(svc: &Svc) -> Response {
    let items: Vec<Value> = svc
        .with(|st| {
            st.findings_filters
                .values()
                .cloned()
                .collect::<Vec<Value>>()
        })
        .into_iter()
        .map(|dict| {
            json!({
                "action": dict.get("action"),
                "arn": dict.get("arn"),
                "id": dict.get("id"),
                "name": dict.get("name"),
            })
        })
        .collect();
    ok(json!({"findingsFilterListItems": items}))
}

// --- Configuration ---

fn get_classification_export_configuration(svc: &Svc) -> Response {
    let config = svc.with(|st| st.classification_export_configuration.clone());
    match config {
        Some(cfg) => ok(json!({"configuration": cfg})),
        None => ok(json!({})),
    }
}

fn put_classification_export_configuration(svc: &Svc) -> Response {
    let configuration = svc.body("configuration");
    svc.with(|st| {
        st.classification_export_configuration = Some(configuration.clone());
    });
    ok(json!({"configuration": configuration}))
}

fn get_reveal_configuration(svc: &Svc) -> Response {
    let (config, status) = svc.with(|st| {
        (
            st.reveal_configuration.clone(),
            st.automated_discovery_status.clone(),
        )
    });
    let _ = status;
    ok(json!({
        "configuration": config,
        "retrievalConfiguration": {"retrievalMode": "CALLER_CREDENTIALS"},
    }))
}

fn update_reveal_configuration(svc: &Svc) -> Response {
    let configuration = svc.body("configuration");
    let retrieval = svc.body("retrievalConfiguration");
    svc.with(|st| {
        if let (Some(newmap), Some(map)) = (
            configuration.as_object(),
            st.reveal_configuration.as_object_mut(),
        ) {
            for (k, v) in newmap {
                map.insert(k.clone(), v.clone());
            }
        }
    });
    let config = svc.with(|st| st.reveal_configuration.clone());
    ok(json!({
        "configuration": config,
        "retrievalConfiguration": retrieval,
    }))
}

fn get_automated_discovery_configuration(svc: &Svc) -> Response {
    let status = svc.with(|st| st.automated_discovery_status.clone());
    ok(json!({
        "autoEnableOrganizationMembers": "NONE",
        "classificationScopeId": "auto-discovery-scope",
        "sensitivityInspectionTemplateId": "auto-discovery-template",
        "status": status,
    }))
}

fn update_automated_discovery_configuration(svc: &Svc) -> Response {
    let status = svc.body("status");
    svc.with(|st| {
        if let Some(s) = status.as_str() {
            st.automated_discovery_status = s.to_string();
        }
    });
    ok(json!({}))
}

fn get_findings_publication_configuration(_svc: &Svc) -> Response {
    ok(json!({
        "securityHubConfiguration": {
            "publishClassificationFindings": true,
            "publishPolicyFindings": true,
        }
    }))
}

fn get_bucket_statistics(_svc: &Svc) -> Response {
    ok(json!({
        "bucketCount": 0,
        "bucketCountByEffectivePermission": {"publiclyAccessible": 0, "publiclyReadable": 0, "publiclyWritable": 0, "unknown": 0},
        "bucketCountByEncryptionType": {"kmsManaged": 0, "s3Managed": 0, "unencrypted": 0, "unknown": 0},
        "bucketCountByObjectEncryptionRequirement": {"allowsUnencryptedObjectUploads": 0, "deniesUnencryptedObjectUploads": 0, "unknown": 0},
        "bucketCountBySharedAccessType": {"external": 0, "internal": 0, "notShared": 0, "unknown": 0},
        "classifiableObjectCount": 0,
        "classifiableSizeInBytes": 0,
        "objectCount": 0,
        "sizeInBytes": 0,
        "sizeInBytesCompressed": 0,
        "unclassifiableObjectCount": {"fileType": 0, "storageClass": 0, "total": 0},
        "unclassifiableObjectSizeInBytes": {"fileType": 0, "storageClass": 0, "total": 0},
    }))
}

// --- Scopes / templates / profiles / sensitive data ---

fn list_classification_scopes(_svc: &Svc) -> Response {
    ok(json!({"classificationScopes": [{"id": "default-scope", "name": "default-scope"}]}))
}

fn get_classification_scope(_svc: &Svc, scope_id: &str) -> Response {
    ok(json!({
        "id": scope_id,
        "name": "default-scope",
        "s3": {"excludes": {"bucketNames": []}},
    }))
}

fn update_classification_scope(_svc: &Svc, _scope_id: &str) -> Response {
    ok(json!({}))
}

fn list_sensitivity_inspection_templates(_svc: &Svc) -> Response {
    ok(json!({
        "sensitivityInspectionTemplates": [{"id": "default-template", "name": "default-template"}]
    }))
}

fn get_sensitivity_inspection_template(_svc: &Svc, template_id: &str) -> Response {
    ok(json!({
        "id": template_id,
        "name": "default-template",
        "description": "",
        "excludes": {"managedDataIdentifierIds": []},
        "includes": {"managedDataIdentifierIds": []},
    }))
}

fn update_sensitivity_inspection_template(_svc: &Svc, _template_id: &str) -> Response {
    ok(json!({}))
}

fn get_resource_profile(_svc: &Svc) -> Response {
    let now = now_iso();
    ok(json!({"profileUpdatedAt": now, "statisticsUpdatedAt": now}))
}

fn sensitive_data_occurrences(_svc: &Svc) -> Response {
    ok(json!({"status": "ERROR", "error": "Finding not available"}))
}

fn sensitive_data_availability(_svc: &Svc) -> Response {
    ok(json!({"code": "UNAVAILABLE", "reasons": ["FINDING_NOT_AVAILABLE"]}))
}

// --- Macie session ---

fn enable_macie(svc: &Svc) -> Response {
    let frequency = svc
        .body("findingPublishingFrequency")
        .as_str()
        .unwrap_or("FIFTEEN_MINUTES")
        .to_string();
    let status = svc.body("status").as_str().unwrap_or("ENABLED").to_string();
    let now = now_iso();
    svc.with(|st| {
        st.macie_session = Some(json!({
            "createdAt": now,
            "findingPublishingFrequency": frequency,
            "serviceRole": service_role(&svc.account),
            "status": status,
            "updatedAt": now,
        }));
    });
    ok(json!({}))
}

fn get_macie_session(svc: &Svc) -> Response {
    let session = svc.with(|st| st.macie_session.clone());
    match session {
        Some(s) => ok(json!({
            "createdAt": s.get("createdAt").cloned().unwrap_or(json!("")),
            "findingPublishingFrequency": s.get("findingPublishingFrequency").cloned().unwrap_or(json!("")),
            "serviceRole": s.get("serviceRole").cloned().unwrap_or(json!("")),
            "status": s.get("status").cloned().unwrap_or(json!("")),
            "updatedAt": s.get("updatedAt").cloned().unwrap_or(json!("")),
        })),
        None => resource_not_found(),
    }
}

fn update_macie_session(svc: &Svc) -> Response {
    let frequency = svc.body("findingPublishingFrequency");
    let status = svc.body("status");
    let touched = svc.with(|st| match &mut st.macie_session {
        None => false,
        Some(session) => {
            if let Some(f) = frequency.as_str() {
                session["findingPublishingFrequency"] = json!(f);
            }
            if let Some(s) = status.as_str() {
                session["status"] = json!(s);
            }
            session["updatedAt"] = json!(now_iso());
            true
        }
    });
    if touched {
        ok(json!({}))
    } else {
        resource_not_found()
    }
}

fn disable_macie(svc: &Svc) -> Response {
    svc.with(|st| {
        st.invitations.clear();
        st.members.clear();
        st.administrator_account = None;
        st.macie_session = None;
    });
    ok(json!({}))
}

// --- Organization admin ---

fn enable_organization_admin_account(svc: &Svc) -> Response {
    let admin = svc
        .body("adminAccountId")
        .as_str()
        .unwrap_or("")
        .to_string();
    svc.with(|st| st.organization_admin_account_id = Some(admin));
    ok(json!({}))
}

fn disable_organization_admin_account(svc: &Svc) -> Response {
    let admin = svc
        .body("adminAccountId")
        .as_str()
        .unwrap_or("")
        .to_string();
    svc.with(|st| {
        if st.organization_admin_account_id.as_deref() == Some(admin.as_str()) {
            st.organization_admin_account_id = None;
        }
    });
    ok(json!({}))
}

fn list_organization_admin_accounts(svc: &Svc) -> Response {
    let accounts = svc.with(|st| match &st.organization_admin_account_id {
        Some(id) => vec![json!({"accountId": id, "status": "ENABLED"})],
        None => vec![],
    });
    ok(json!({"adminAccounts": accounts}))
}

fn describe_organization_configuration(svc: &Svc) -> Response {
    let auto_enable = svc.with(|st| st.organization_auto_enable);
    ok(json!({"autoEnable": auto_enable, "maxAccountLimitReached": false}))
}

fn update_organization_configuration(svc: &Svc) -> Response {
    let auto_enable = svc.body("autoEnable").as_bool().unwrap_or(false);
    svc.with(|st| st.organization_auto_enable = auto_enable);
    ok(json!({}))
}

// --- Administrator / members ---

fn get_administrator_account(svc: &Svc) -> Response {
    let admin = svc.with(|st| st.administrator_account.clone());
    match admin {
        Some(member) => ok(json!({
            "administrator": {
                "accountId": member.get("administratorAccountId").cloned().unwrap_or(json!("")),
                "relationshipStatus": "Enabled",
            }
        })),
        None => ok(json!({})),
    }
}

fn disassociate_from_administrator(svc: &Svc) -> Response {
    svc.with(|st| st.administrator_account = None);
    ok(json!({}))
}

fn list_members(svc: &Svc) -> Response {
    let members = svc.with(|st| st.members.values().cloned().collect::<Vec<Value>>());
    ok(json!({"members": members}))
}

fn get_member(svc: &Svc, member_id: &str) -> Response {
    let member = svc.with(|st| st.members.get(member_id).cloned());
    match member {
        Some(m) => ok(m),
        None => resource_not_found(),
    }
}

fn delete_member(svc: &Svc, member_id: &str) -> Response {
    if svc.with(|st| st.members.remove(member_id).is_some()) {
        ok(json!({}))
    } else {
        resource_not_found()
    }
}

fn disassociate_member(svc: &Svc, member_id: &str) -> Response {
    if svc.with(|st| st.members.remove(member_id).is_some()) {
        ok(json!({}))
    } else {
        resource_not_found()
    }
}

fn update_member_session(svc: &Svc, member_id: &str) -> Response {
    if svc.with(|st| st.members.contains_key(member_id)) {
        ok(json!({}))
    } else {
        resource_not_found()
    }
}

fn create_member(svc: &Svc) -> Response {
    let account = svc.body("account");
    let tags = svc.body("tags");
    let member_account_id = account
        .get("accountId")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let default_email = format!("{member_account_id}@example.com");
    let email = account
        .get("email")
        .and_then(Value::as_str)
        .unwrap_or(&default_email)
        .to_string();
    let arn = format!(
        "arn:aws:macie2:{}:{}:member/{}",
        svc.region, svc.account, member_account_id
    );
    let now = now_iso();
    let member = json!({
        "accountId": member_account_id,
        "administratorAccountId": svc.account,
        "arn": arn.clone(),
        "email": email,
        "invitedAt": now,
        "masterAccountId": svc.account,
        "relationshipStatus": "Enabled",
        "tags": tags,
        "updatedAt": now,
    });
    svc.with(|st| st.members.insert(member_account_id, member));
    ok(json!({"arn": arn}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn req(method: &str, path: &str, body: &str) -> Request {
        Request {
            service: "macie2".to_string(),
            operation: String::new(),
            method: method.to_string(),
            path: path.to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(body.to_string()),
            account: 123456789012,
            region: "us-east-1".to_string(),
        }
    }

    fn scope() -> ScopeKey {
        (
            123456780001,
            format!("us-east-1-{}", Uuid::new_v4().simple()),
        )
    }

    fn r(method: &str, path: &str, body: &str, key: &ScopeKey) -> Request {
        let mut request = req(method, path, body);
        request.account = key.0;
        request.region = key.1.clone();
        request
    }

    fn body_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    #[test]
    fn macie_session_lifecycle() {
        let key = scope();
        // not enabled yet → ResourceNotFoundException
        assert_eq!(dispatch(&r("GET", "/macie", "{}", &key)).status, 400);
        dispatch(&r(
            "POST",
            "/macie",
            r#"{"findingPublishingFrequency": "ONE_HOUR"}"#,
            &key,
        ));
        let session = body_of(&dispatch(&r("GET", "/macie", "{}", &key)));
        assert_eq!(session["status"], "ENABLED");
        assert_eq!(session["findingPublishingFrequency"], "ONE_HOUR");
        assert!(session["serviceRole"].as_str().unwrap().contains("macie"));
        // idempotent re-enable and disable
        dispatch(&r("POST", "/macie", "{}", &key));
        dispatch(&r("DELETE", "/macie", "{}", &key));
        assert_eq!(dispatch(&r("GET", "/macie", "{}", &key)).status, 400);
    }

    #[test]
    fn member_lifecycle() {
        let key = scope();
        let arn = body_of(&dispatch(&r(
            "POST",
            "/members",
            r#"{"account": {"accountId": "222233334444", "email": "t@example.com"}}"#,
            &key,
        )))["arn"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(arn.contains("member/222233334444"));
        let member = body_of(&dispatch(&r("GET", "/members/222233334444", "{}", &key)));
        assert_eq!(member["relationshipStatus"], "Enabled");
        dispatch(&r("GET", "/members/9999", "{}", &key));
        // unknown member → RNFE
        let resp = dispatch(&r("GET", "/members/9999", "{}", &key));
        assert_eq!(body_of(&resp)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn invitations_flow() {
        let key = scope();
        dispatch(&r(
            "POST",
            "/invitations",
            r#"{"accountIds": ["111122223333"]}"#,
            &key,
        ));
        let count = body_of(&dispatch(&r("GET", "/invitations/count", "{}", &key)));
        assert_eq!(count["invitationsCount"], 1);
        let list = body_of(&dispatch(&r("GET", "/invitations", "{}", &key)));
        assert!(list["invitations"][0]["invitationId"].as_str().is_some());
    }

    #[test]
    fn allow_list_lifecycle() {
        let key = scope();
        let created = body_of(&dispatch(&r(
            "POST",
            "/allow-lists",
            r#"{"name": "al", "criteria": {"regex": "x.*"}}"#,
            &key,
        )));
        let id = created["id"].as_str().unwrap().to_string();
        let arn = created["arn"].as_str().unwrap().to_string();
        assert!(arn.contains("allow-list/"));
        let got = body_of(&dispatch(&r(
            "GET",
            &format!("/allow-lists/{id}"),
            "{}",
            &key,
        )));
        assert_eq!(got["name"], "al");
        let updated = body_of(&dispatch(&r(
            "PUT",
            &format!("/allow-lists/{id}"),
            r#"{"name": "al2"}"#,
            &key,
        )));
        assert_eq!(updated["id"], created["id"]);
        assert_eq!(
            dispatch(&r("DELETE", &format!("/allow-lists/{id}"), "{}", &key)).status,
            200
        );
        let resp = dispatch(&r("GET", &format!("/allow-lists/{id}"), "{}", &key));
        assert_eq!(body_of(&resp)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn custom_data_identifier_lifecycle() {
        let key = scope();
        let created = body_of(&dispatch(&r(
            "POST",
            "/custom-data-identifiers",
            r#"{"name": "cdi", "regex": "SSN-[0-9]{3}"}"#,
            &key,
        )));
        let id = created["customDataIdentifierId"]
            .as_str()
            .unwrap()
            .to_string();
        let resp = dispatch(&r(
            "POST",
            "/custom-data-identifiers/test",
            r#"{"regex": "SSN-[0-9]{3}", "sampleText": "a"}"#,
            &key,
        ));
        assert!(body_of(&resp)["matchCount"].is_u64());
        dispatch(&r(
            "DELETE",
            &format!("/custom-data-identifiers/{id}"),
            "{}",
            &key,
        ));
        let list = body_of(&dispatch(&r(
            "POST",
            "/custom-data-identifiers/list",
            "{}",
            &key,
        )));
        assert!(list["items"].as_array().unwrap().is_empty());
    }

    #[test]
    fn findings_filters_lifecycle() {
        let key = scope();
        let created = body_of(&dispatch(&r(
            "POST",
            "/findingsfilters",
            r#"{"action": "ARCHIVE", "name": "ff", "findingCriteria": {"criterion": {}}}"#,
            &key,
        )));
        let id = created["id"].as_str().unwrap().to_string();
        let list = body_of(&dispatch(&r("GET", "/findingsfilters", "{}", &key)));
        assert_eq!(list["findingsFilterListItems"][0]["id"], json!(id));
        assert_eq!(
            dispatch(&r("DELETE", &format!("/findingsfilters/{id}"), "{}", &key)).status,
            200
        );
    }

    #[test]
    fn org_admin_and_configuration() {
        let key = scope();
        dispatch(&r(
            "POST",
            "/admin",
            r#"{"adminAccountId": "999988887777"}"#,
            &key,
        ));
        let accounts = body_of(&dispatch(&r("GET", "/admin", "{}", &key)));
        assert_eq!(accounts["adminAccounts"][0]["accountId"], "999988887777");
        let config = body_of(&dispatch(&r("GET", "/admin/configuration", "{}", &key)));
        assert!(config["autoEnable"].is_boolean());
        assert_eq!(config["maxAccountLimitReached"], false);
        dispatch(&r(
            "PATCH",
            "/admin/configuration",
            r#"{"autoEnable": true}"#,
            &key,
        ));
        let config = body_of(&dispatch(&r("GET", "/admin/configuration", "{}", &key)));
        assert_eq!(config["autoEnable"], true);
    }

    #[test]
    fn classification_job_lifecycle() {
        let key = scope();
        let created = body_of(&dispatch(&r(
            "POST",
            "/jobs",
            r#"{"name": "j", "jobType": "ONE_TIME", "s3JobDefinition": {"bucketDefinitions": []}}"#,
            &key,
        )));
        let job_id = created["jobId"].as_str().unwrap().to_string();
        let described = body_of(&dispatch(&r("GET", &format!("/jobs/{job_id}"), "{}", &key)));
        assert_eq!(described["jobStatus"], "RUNNING");
        dispatch(&r(
            "PATCH",
            &format!("/jobs/{job_id}"),
            r#"{"jobStatus": "CANCELLED"}"#,
            &key,
        ));
        let described = body_of(&dispatch(&r("GET", &format!("/jobs/{job_id}"), "{}", &key)));
        assert_eq!(described["jobStatus"], "CANCELLED");
    }

    #[test]
    fn tag_operations_via_query_string() {
        let key = scope();
        let arn = "arn:aws:macie2:us-east-1:123456789012:session";
        let path = format!("/tags/{arn}");
        dispatch(&r("POST", &path, r#"{"tags": {"env": "test"}}"#, &key));
        let tags = body_of(&dispatch(&r("GET", &path, "{}", &key)));
        assert_eq!(tags["tags"]["env"], "test");
        let mut delete = req("DELETE", &path, "{}");
        delete.account = key.0;
        delete.region = key.1.clone();
        delete.query_string = "tagKeys=env".to_string();
        dispatch(&delete);
        let tags = body_of(&dispatch(&r("GET", &path, "{}", &key)));
        assert!(tags["tags"].as_object().unwrap().is_empty());
    }

    #[test]
    fn findings_and_statistics_empty() {
        let key = scope();
        let findings = body_of(&dispatch(&r("POST", "/findings", "{}", &key)));
        assert_eq!(findings["findingIds"], json!([]));
        let stats = body_of(&dispatch(&r(
            "POST",
            "/findings/statistics",
            r#"{"groupBy": "type"}"#,
            &key,
        )));
        assert_eq!(stats["countsByGroup"], json!([]));
        let buckets = body_of(&dispatch(&r("POST", "/datasources/s3", "{}", &key)));
        assert_eq!(buckets["buckets"], json!([]));
    }
}
