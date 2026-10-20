//! EMR Serverless backend (REST-JSON, URI-template routes).
//!
//! Port of moto's emrserverless: applications and job runs per
//! (account, region), routed by method + path the way moto's `urls.py`
//! does. Wire shapes mirror moto exactly — including its quirks: the
//! application `type` is capitalized at creation (`SPARK` → `Spark`),
//! list shapes use `id` where get shapes use `applicationId`, job runs
//! are born in the SUCCESS state, `createdAt` sits at midnight UTC, and
//! missing resources raise `ResourceNotFoundException` with HTTP 400.

use crate::proto::{json_error, parse_json_body, query_first, query_map, url_decode};
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

const DEFAULT_MAX_RESULTS: usize = 100;
const INITIAL_STATE: &str = "STARTED";
const INITIAL_JOB_STATE: &str = "SUCCESS";

#[derive(Default)]
struct State {
    /// Insertion-ordered applications (moto keeps a dict keyed by id).
    apps: Vec<App>,
    /// (application_id, job runs) pairs, inserted at first `start_job_run`
    /// (moto keeps `job_runs: {application_id: [run, ...]}`).
    runs: Vec<(String, Vec<JobRun>)>,
}

struct App {
    id: String,
    arn: String,
    /// Raw request field — `Null` when the request omitted `name`.
    name: Value,
    release_label: String,
    /// Application type as moto stores it: capitalized (`SPARK` → `Spark`).
    app_type: String,
    /// `None` == param absent/null (moto's falsy placeholder).
    initial_capacity: Option<Value>,
    maximum_capacity: Option<Value>,
    /// Always an object (moto: `tags or {}`).
    tags: Value,
    auto_start: Value,
    auto_stop: Value,
    network_configuration: Option<Value>,
    state: String,
    created_at: String,
    updated_at: String,
}

struct JobRun {
    id: String,
    arn: String,
    name: Value,
    application_id: String,
    execution_role_arn: String,
    job_driver: Value,
    configuration_overrides: Value,
    /// Null or the request's object, like moto's `tags: Optional[dict]`.
    tags: Value,
    /// Copied from the application at start time (null when it had none).
    network_configuration: Value,
    release_label: String,
    app_type: String,
    state: String,
    created_at: String,
    updated_at: String,
    timeout_minutes: i64,
    duration_seconds: i64,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("emrserverless"));
    SLOT.clone()
}

fn ok_json(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn ok_empty() -> Response {
    Response::new(200, "application/json", Vec::new())
}

/// moto's JsonRESTError wire shape: `{"__type": code, "message": msg}`.
/// Every exception moto's emrserverless raises carries `code = 400`.
fn err(status: u16, code: &str, message: String) -> Response {
    json_error(status, code, &message)
}

fn resource_not_found(kind: &str, token: &str) -> Response {
    err(
        400,
        "ResourceNotFoundException",
        format!("{kind} {token} does not exist"),
    )
}

fn validation(message: String) -> Response {
    err(400, "ValidationException", message)
}

fn access_denied(message: &str) -> Response {
    err(400, "AccessDeniedException", message.to_string())
}

/// moto's `random_id(16)`: lowercase letters + digits.
fn random_id() -> String {
    const ALPHA: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let uuid = uuid::Uuid::new_v4();
    let bytes = uuid.as_bytes();
    (0..16)
        .map(|i| ALPHA[bytes[i % bytes.len()] as usize % ALPHA.len()] as char)
        .collect()
}

/// moto zeroes the time of day when stamping createdAt/updatedAt
/// (`datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)`).
fn midnight_iso() -> String {
    chrono::Utc::now()
        .date_naive()
        .format("%Y-%m-%dT00:00:00Z")
        .to_string()
}

/// Python's `str.capitalize()`: first char uppercased, rest lowercased.
fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) => c.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase(),
        None => String::new(),
    }
}

/// The literal regex in moto's `create_application` spans two adjacent
/// string literals that concatenate to
/// `emr-[0-9]{1}\.[0-9]{1,2}\.0(|-[0-9]{8})` — the `|` makes the group's
/// first branch empty, so any suffix after the required prefix matches.
/// Keep the minor-version backtracking semantics (two minor digits then a
/// literal `.0` must match for `emr-6.15.0`).
fn valid_release_label(label: &str) -> bool {
    let Some(rest) = label.strip_prefix("emr-") else {
        return false;
    };
    let b = rest.as_bytes();
    let digit = |i: usize| b.get(i).is_some_and(|c| c.is_ascii_digit());
    if !digit(0) || b.get(1) != Some(&b'.') {
        return false;
    }
    for minor in 1..=2 {
        if !digit(2 + minor - 1) {
            break;
        }
        // Minor digits b[2..2+minor], then a literal ".0".
        let j = 2 + minor;
        if b.get(j) == Some(&b'.') && b.get(j + 1) == Some(&b'0') {
            return true;
        }
    }
    false
}

/// Python truthiness for the capacity/config params — `{}` is falsy, an
/// object with keys is truthy.
fn truthy(v: &Value) -> bool {
    match v {
        Value::Object(m) => !m.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::String(s) => !s.is_empty(),
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0),
    }
}

/// Param → `Option<Value>`: absent or null become `None`, mirroring moto's
/// `_get_param` returning None.
fn opt_field(v: &Value) -> Option<Value> {
    if v.is_null() {
        None
    } else {
        Some(v.clone())
    }
}

/// moto's `x or default` for JSON params: falsy values fall back.
fn or_default(v: &Value, default: &Value) -> Value {
    if truthy(v) {
        v.clone()
    } else {
        default.clone()
    }
}

/// Query params for a list field arrive as repeated keys (`states=a&states=b`)
/// and, under some botocore versions, as member-suffixed indices
/// (`states.member.1=a`). Collect both spellings in order.
fn query_values<'a>(map: &'a BTreeMap<String, Vec<String>>, base: &str) -> Vec<&'a str> {
    let mut out = Vec::new();
    let dot = format!("{base}.");
    for (k, vals) in map {
        if k == base || k.strip_prefix(&dot).is_some() {
            for v in vals {
                out.push(v.as_str());
            }
        }
    }
    out
}

/// Emulate moto's `paginated_list`: sort by key, slice at the numeric
/// offset, emit a next token only when the slice is full.
fn paginate(
    mut values: Vec<Value>,
    sort_key: &str,
    max_results: usize,
    next_token: Option<&str>,
) -> (Vec<Value>, Option<Value>) {
    // moto's `paginated_list` re-sorts the whole list on every call —
    // the sorted slice is taken again even when a next token arrives.
    values.sort_by(|a, b| {
        a[sort_key]
            .as_str()
            .unwrap_or_default()
            .cmp(b[sort_key].as_str().unwrap_or_default())
    });
    let start: usize = next_token.and_then(|t| t.parse().ok()).unwrap_or(0);
    let slice: Vec<Value> = values.into_iter().skip(start).take(max_results).collect();
    let token = if slice.len() == max_results {
        Some(Value::from((start + max_results).to_string()))
    } else {
        None
    };
    (slice, token)
}

/// The capacity/config fields moto appends (conditionally — omit the falsy
/// ones) to both summary and detail dicts.
fn add_operational(m: &mut Map<String, Value>, app: &App) {
    if truthy_opt(app.initial_capacity.as_ref()) {
        m.insert(
            "initialCapacity".into(),
            app.initial_capacity.clone().unwrap_or(Value::Null),
        );
    }
    if truthy_opt(app.maximum_capacity.as_ref()) {
        m.insert(
            "maximumCapacity".into(),
            app.maximum_capacity.clone().unwrap_or(Value::Null),
        );
    }
    if truthy_opt(app.network_configuration.as_ref()) {
        m.insert(
            "networkConfiguration".into(),
            app.network_configuration.clone().unwrap_or(Value::Null),
        );
    }
}

fn truthy_opt(v: Option<&Value>) -> bool {
    v.map(truthy).unwrap_or(false)
}

impl App {
    /// moto's `FakeApplication.__iter__` shape — `create_application`
    /// serializes exactly this, with the abbreviated config key names.
    fn create_response(&self) -> Value {
        json!({
            "applicationId": self.id,
            "name": self.name,
            "arn": self.arn,
            "autoStartConfig": self.auto_start,
            "autoStopConfig": self.auto_stop,
        })
    }

    /// List-shape dict (`list_applications`): note the plain `id` key and
    /// the absence of tags + auto-start/stop configurations.
    fn summary(&self) -> Value {
        let mut m = match json!({
            "id": self.id,
            "name": self.name,
            "arn": self.arn,
            "releaseLabel": self.release_label,
            "type": self.app_type,
            "state": self.state,
            "stateDetails": "",
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
        })
        .take_object()
        {
            Some(m) => m,
            None => return Value::Null,
        };
        add_operational(&mut m, self);
        Value::Object(m)
    }

    /// Detail shape (`get`/`update` callers): full application record
    /// with tags and the auto configurations.
    fn detail(&self) -> Value {
        let mut m = match json!({
            "applicationId": self.id,
            "name": self.name
                .as_str()
                .map_or(Value::Null, |s| Value::String(s.to_string())),
            "arn": self.arn,
            "releaseLabel": self.release_label,
            "type": self.app_type,
            "state": self.state,
            "stateDetails": "",
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "autoStartConfiguration": self.auto_start,
            "autoStopConfiguration": self.auto_stop,
            "tags": self.tags,
        })
        .take_object()
        {
            Some(m) => m,
            None => return Value::Null,
        };
        add_operational(&mut m, self);
        Value::Object(m)
    }
}

impl JobRun {
    /// Detail shape (`to_dict("get")`) — carries `executionRole` and the
    /// run configuration as stored at start time.
    fn detail(&self) -> Value {
        json!({
            "applicationId": self.application_id,
            "jobRunId": self.id,
            "name": self.name,
            "arn": self.arn,
            "createdBy": Value::Null,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "executionRole": self.execution_role_arn,
            "state": self.state,
            "stateDetails": Value::Null,
            "releaseLabel": self.release_label,
            "configurationOverrides": self.configuration_overrides,
            "jobDriver": self.job_driver,
            "tags": self.tags,
            "networkConfiguration": self.network_configuration,
            "totalExecutionDurationSeconds": self.duration_seconds,
            "executionTimeoutMinutes": self.timeout_minutes,
            "billedResourceUtilization": {
                "vCPUHour": 0.0,
                "memoryGBHour": 0.0,
                "storageGBHour": 0.0,
            },
        })
    }

    /// List shape (`to_dict("list")`): plain `id` key plus the run `type`.
    fn summary(&self) -> Value {
        json!({
            "applicationId": self.application_id,
            "id": self.id,
            "name": self.name,
            "arn": self.arn,
            "createdBy": Value::Null,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "executionRole": self.execution_role_arn,
            "state": self.state,
            "stateDetails": Value::Null,
            "releaseLabel": self.release_label,
            "type": self.app_type,
        })
    }
}

trait TakeObject {
    fn take_object(self) -> Option<Map<String, Value>>;
}

impl TakeObject for Value {
    fn take_object(self) -> Option<Map<String, Value>> {
        match self {
            Value::Object(m) => Some(m),
            _ => None,
        }
    }
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "emrserverless" {
        return None;
    }
    // moto's urls.py routes exactly these six path families; anything
    // else (the tags ops at /tags/{arn}, GetDashboardForJobRun,
    // ListJobRunAttempts) is un-routed moto-side and answers the
    // gateway's generic 501 NotImplemented.
    let segs: Vec<String> = req
        .path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(url_decode)
        .collect();
    let parts: Vec<&str> = segs.iter().map(String::as_str).collect();
    let q = query_map(&req.query_string);
    let body = parse_json_body(&req.body);

    let slot = state();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();

    Some(match parts.as_slice() {
        ["applications"] if req.method == "GET" => list_applications(st, &q),
        ["applications"] if req.method == "POST" => {
            create_application(st, &body, &req.region, req.account)
        }
        ["applications", id] if req.method == "GET" => get_application(st, id),
        ["applications", id] if req.method == "PATCH" => update_application(st, id, &body),
        ["applications", id] if req.method == "DELETE" => delete_application(st, id),
        ["applications", id, "start"] if req.method == "POST" => start_application(st, id),
        ["applications", id, "stop"] if req.method == "POST" => stop_application(st, id),
        ["applications", id, "jobruns"] if req.method == "POST" => {
            start_job_run(st, id, &body, &req.region, req.account)
        }
        ["applications", id, "jobruns"] if req.method == "GET" => list_job_runs(st, id, &q),
        ["applications", id, "jobruns", run_id] if req.method == "GET" => {
            get_job_run(st, id, run_id)
        }
        ["applications", id, "jobruns", run_id] if req.method == "DELETE" => {
            cancel_job_run(st, id, run_id)
        }
        _ => return None,
    })
}

fn create_application(st: &mut State, p: &Value, region: &str, account: u64) -> Response {
    // moto: `if application_type not in ["HIVE", "SPARK"]`.
    let app_type = p["type"].as_str().unwrap_or("None");
    if app_type != "HIVE" && app_type != "SPARK" {
        return validation(format!("Unsupported engine {app_type}"));
    }
    let release_label = p["releaseLabel"].as_str().unwrap_or_default();
    if !valid_release_label(release_label) {
        return validation(format!(
            "Type '{app_type}' is not supported for release label '{release_label}' \
             or release label does not exist"
        ));
    }
    let id = random_id();
    let arn = format!("arn:aws:emr-serverless:{region}:{account}:/applications/{id}");
    let now = midnight_iso();
    let app = App {
        id,
        arn,
        name: p["name"].clone(),
        release_label: release_label.to_string(),
        app_type: capitalize(app_type),
        initial_capacity: opt_field(&p["initialCapacity"]),
        maximum_capacity: opt_field(&p["maximumCapacity"]),
        // moto: `tags or {}`.
        tags: if truthy(&p["tags"]) {
            p["tags"].clone()
        } else {
            json!({})
        },
        // moto: `auto_start_configuration or default_auto_start_configuration()`.
        auto_start: or_default(&p["autoStartConfiguration"], &json!({"enabled": true})),
        auto_stop: or_default(
            &p["autoStopConfiguration"],
            &json!({"enabled": true, "idleTimeoutMinutes": 15}),
        ),
        network_configuration: opt_field(&p["networkConfiguration"]),
        state: INITIAL_STATE.to_string(),
        created_at: now.clone(),
        updated_at: now,
    };
    let resp = app.create_response();
    st.apps.push(app);
    ok_json(&resp)
}

fn find_app<'a>(st: &'a State, id: &str) -> Option<&'a App> {
    st.apps.iter().find(|a| a.id == id)
}

fn find_app_mut<'a>(st: &'a mut State, id: &str) -> Option<&'a mut App> {
    st.apps.iter_mut().find(|a| a.id == id)
}

fn get_application(st: &State, id: &str) -> Response {
    match find_app(st, id) {
        Some(app) => ok_json(&json!({ "application": app.detail() })),
        None => resource_not_found("Application", id),
    }
}

fn list_applications(st: &State, q: &BTreeMap<String, Vec<String>>) -> Response {
    let states = query_values(q, "states");
    let mut summaries: Vec<Value> = st.apps.iter().map(App::summary).collect();
    if !states.is_empty() {
        summaries.retain(|a| states.contains(&a["state"].as_str().unwrap_or_default()));
    }
    let max_results = q
        .get("maxResults")
        .and_then(|v| v.first())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_RESULTS);
    let next_token = query_first(q, "nextToken").filter(|s| !s.is_empty());
    let (items, token) = paginate(summaries, "name", max_results, next_token);
    ok_json(&json!({ "applications": items, "nextToken": token }))
}

fn start_application(st: &mut State, id: &str) -> Response {
    match find_app_mut(st, id) {
        // moto just flips the state, no precondition checks.
        Some(app) => {
            app.state = "STARTED".to_string();
            ok_empty()
        }
        None => resource_not_found("Application", id),
    }
}

fn stop_application(st: &mut State, id: &str) -> Response {
    match find_app_mut(st, id) {
        Some(app) => {
            app.state = "STOPPED".to_string();
            ok_empty()
        }
        None => resource_not_found("Application", id),
    }
}

fn delete_application(st: &mut State, id: &str) -> Response {
    let Some(idx) = st.apps.iter().position(|a| a.id == id) else {
        return resource_not_found("Application", id);
    };
    let state = st.apps[idx].state.clone();
    // moto: delete only makes sense from CREATED/STOPPED.
    if state != "CREATED" && state != "STOPPED" {
        return validation(format!(
            "Application {id} must be in one of the following statuses [CREATED, STOPPED]. \
             Current status: {state}"
        ));
    }
    st.apps[idx].state = "TERMINATED".to_string();
    ok_empty()
}

fn update_application(st: &mut State, id: &str, p: &Value) -> Response {
    let Some(idx) = st.apps.iter().position(|a| a.id == id) else {
        return resource_not_found("Application", id);
    };
    let state = st.apps[idx].state.clone();
    if state != "CREATED" && state != "STOPPED" {
        return validation(format!(
            "Application {id} must be in one of the following statuses [CREATED, STOPPED]. \
             Current status: {state}"
        ));
    }
    let app = &mut st.apps[idx];
    // moto updates each field `if <param>:` — falsy params leave the
    // stored value untouched.
    if truthy(&p["initialCapacity"]) {
        app.initial_capacity = opt_field(&p["initialCapacity"]);
    }
    if truthy(&p["maximumCapacity"]) {
        app.maximum_capacity = opt_field(&p["maximumCapacity"]);
    }
    if truthy(&p["autoStartConfiguration"]) {
        app.auto_start = p["autoStartConfiguration"].clone();
    }
    if truthy(&p["autoStopConfiguration"]) {
        app.auto_stop = p["autoStopConfiguration"].clone();
    }
    if truthy(&p["networkConfiguration"]) {
        app.network_configuration = opt_field(&p["networkConfiguration"]);
    }
    app.updated_at = midnight_iso();
    ok_json(&json!({ "application": app.detail() }))
}

/// Locate the job-run list for an application; moto raises
/// `ResourceNotFoundException(application_id, "Application")` when the
/// application id is not a key — so an application that never had a job
/// run reports "does not exist" for job-run reads too.
fn runs_entry<'a>(st: &'a State, app_id: &str) -> Option<&'a Vec<JobRun>> {
    st.runs.iter().find(|(id, _)| id == app_id).map(|(_, r)| r)
}

fn runs_entry_mut<'a>(st: &'a mut State, app_id: &str) -> Option<&'a mut Vec<JobRun>> {
    st.runs
        .iter_mut()
        .find(|(id, _)| id == app_id)
        .map(|(_, r)| r)
}

fn start_job_run(st: &mut State, app_id: &str, p: &Value, region: &str, account: u64) -> Response {
    // moto checks the pass role BEFORE any application lookup.
    let role_arn = p["executionRoleArn"].as_str().unwrap_or_default();
    let role_account = role_arn.split(':').nth(4).unwrap_or_default();
    if role_account != account.to_string() {
        return access_denied("Cross-account pass role is not allowed.");
    }
    let timeout = p["executionTimeoutMinutes"].as_i64();
    if let Some(t) = timeout {
        // moto: `if execution_timeout_minutes and execution_timeout_minutes < 5`
        // (0 stays falsy and falls back to the default below).
        if t != 0 && t < 5 {
            return validation("RunTimeout must be at least 5 minutes.".to_string());
        }
    }
    let Some(app) = find_app(st, app_id) else {
        return resource_not_found("Application", app_id);
    };
    if app.state == "TERMINATED" {
        return validation(format!(
            "Application {app_id} is terminated. Cannot start job run."
        ));
    }
    let id = random_id();
    let arn =
        format!("arn:aws:emr-serverless:{region}:{account}:/applications/{app_id}/jobruns/{id}");
    let now = midnight_iso();
    let run = JobRun {
        id,
        arn,
        name: p["name"].clone(),
        application_id: app_id.to_string(),
        execution_role_arn: role_arn.to_string(),
        job_driver: p["jobDriver"].clone(),
        configuration_overrides: p["configurationOverrides"].clone(),
        tags: p["tags"].clone(),
        network_configuration: app.network_configuration.clone().unwrap_or(Value::Null),
        release_label: app.release_label.clone(),
        app_type: app.app_type.clone(),
        state: INITIAL_JOB_STATE.to_string(),
        created_at: now.clone(),
        updated_at: now,
        // moto: `execution_timeout_minutes or 720`.
        timeout_minutes: timeout.filter(|t| *t != 0).unwrap_or(720),
        duration_seconds: 0,
    };
    let resp = json!({
        "applicationId": run.application_id,
        "jobRunId": run.id,
        "arn": run.arn,
    });
    match st.runs.iter_mut().find(|(id, _)| id == app_id) {
        Some((_, runs)) => runs.push(run),
        None => st.runs.push((app_id.to_string(), vec![run])),
    }
    ok_json(&resp)
}

fn get_job_run(st: &State, app_id: &str, run_id: &str) -> Response {
    let Some(runs) = runs_entry(st, app_id) else {
        return resource_not_found("Application", app_id);
    };
    match runs.iter().find(|r| r.id == run_id) {
        Some(run) => ok_json(&json!({ "jobRun": run.detail() })),
        None => resource_not_found("JobRun", run_id),
    }
}

fn cancel_job_run(st: &mut State, app_id: &str, run_id: &str) -> Response {
    let Some(runs) = runs_entry_mut(st, app_id) else {
        return resource_not_found("Application", app_id);
    };
    match runs.iter_mut().find(|r| r.id == run_id) {
        Some(run) => {
            run.state = "CANCELLED".to_string();
            ok_json(&json!({ "applicationId": app_id, "jobRunId": run_id }))
        }
        None => resource_not_found("JobRun", run_id),
    }
}

fn list_job_runs(st: &State, app_id: &str, q: &BTreeMap<String, Vec<String>>) -> Response {
    let Some(runs) = runs_entry(st, app_id) else {
        return resource_not_found("Application", app_id);
    };
    let states = query_values(q, "states");
    // moto parses createdAtAfter/Before with the same fixed format the
    // records carry, so plain string compare orders them the same way.
    let after = query_first(q, "createdAtAfter").filter(|s| !s.is_empty());
    let before = query_first(q, "createdAtBefore").filter(|s| !s.is_empty());
    let summaries: Vec<Value> = runs
        .iter()
        .filter(|r| states.is_empty() || states.contains(&r.state.as_str()))
        .filter(|r| after.is_none_or(|a| r.created_at.as_str() > a))
        .filter(|r| before.is_none_or(|b| r.created_at.as_str() < b))
        .map(JobRun::summary)
        .collect();
    let max_results = q
        .get("maxResults")
        .and_then(|v| v.first())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_RESULTS);
    let next_token = query_first(q, "nextToken").filter(|s| !s.is_empty());
    let (items, token) = paginate(summaries, "createdAt", max_results, next_token);
    ok_json(&json!({ "jobRuns": items, "nextToken": token }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn req(method: &str, path: &str, body: Value, query: &str, account: u64) -> Request {
        Request {
            service: "emrserverless".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: query.into(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
            account,
            region: "us-east-1".into(),
        }
    }

    fn body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    fn uuid_account() -> u64 {
        uuid::Uuid::new_v4().as_u128() as u64
    }

    fn create_app(account: u64, extra: Value) -> Value {
        let mut p = json!({"type": "SPARK", "releaseLabel": "emr-6.15.0", "name": "app"});
        if let (Some(dst), Some(src)) = (p.as_object_mut(), extra.as_object()) {
            for (k, v) in src {
                dst.insert(k.clone(), v.clone());
            }
        }
        body(&dispatch(&req("POST", "/applications", p, "", account)).unwrap())
    }

    #[test]
    fn create_mirrors_moto_iter_shape() {
        let acct = uuid_account();
        let r = create_app(acct, json!({"name": "my-app"}));
        assert_eq!(r["name"], "my-app");
        let arn = r["arn"].as_str().unwrap();
        assert!(arn.starts_with(&format!(
            "arn:aws:emr-serverless:us-east-1:{acct}:/applications/"
        )));
        let app_id = r["applicationId"].as_str().unwrap();
        assert_eq!(app_id.len(), 16, "moto's random_id(16)");
        // the abbreviated config keys carry moto's defaults
        assert_eq!(r["autoStartConfig"]["enabled"], true);
        assert_eq!(r["autoStopConfig"]["idleTimeoutMinutes"], 15);
        // and no detail fields leak into create responses
        assert!(r.get("releaseLabel").is_none());
        assert!(r.get("tags").is_none());
    }

    #[test]
    fn get_list_shapes_differ_like_moto() {
        let acct = uuid_account();
        let app_id = create_app(acct, json!({"name": "b"}))["applicationId"]
            .as_str()
            .unwrap()
            .to_string();

        // get: full detail with applicationId, capitalized type, tags
        let g = body(
            &dispatch(&req(
                "GET",
                &format!("/applications/{app_id}"),
                json!({}),
                "",
                acct,
            ))
            .unwrap(),
        );
        let app = &g["application"];
        assert_eq!(app["applicationId"], app_id.clone());
        assert_eq!(app["type"], "Spark");
        assert_eq!(app["releaseLabel"], "emr-6.15.0");
        assert_eq!(app["state"], "STARTED");
        assert_eq!(app["tags"], json!({}));
        assert_eq!(app["autoStartConfiguration"]["enabled"], true);

        // list: summary with a plain "id" key and no tags
        let l = body(&dispatch(&req("GET", "/applications", json!({}), "", acct)).unwrap());
        let item = &l["applications"].as_array().unwrap()[0];
        assert_eq!(item["id"], app_id.clone());
        assert!(item.get("applicationId").is_none());
        assert!(item.get("tags").is_none());
        assert!(l.get("nextToken").is_some());
    }

    #[test]
    fn full_lifecycle_states() {
        let acct = uuid_account();
        let app_id = create_app(acct, json!({}))["applicationId"]
            .as_str()
            .unwrap()
            .to_string();
        let path = format!("/applications/{app_id}");
        let state = |acct| {
            body(&dispatch(&req("GET", &path.clone(), json!({}), "", acct)).unwrap())["application"]
                ["state"]
                .as_str()
                .unwrap()
                .to_string()
        };
        assert_eq!(state(acct), "STARTED");
        dispatch(&req("POST", &format!("{path}/stop"), json!({}), "", acct)).unwrap();
        assert_eq!(state(acct), "STOPPED");
        dispatch(&req("POST", &format!("{path}/start"), json!({}), "", acct)).unwrap();
        assert_eq!(state(acct), "STARTED");
        // delete from STARTED is rejected by moto (only CREATED/STOPPED)...
        let err = dispatch(&req("DELETE", &path.clone(), json!({}), "", acct)).unwrap();
        assert_eq!(err.status, 400);
        assert_eq!(body(&err)["__type"], "ValidationException");
        // ...allowed from STOPPED, and it leaves a terminated record
        // (get keeps answering 200, never a 404)
        dispatch(&req("POST", &format!("{path}/stop"), json!({}), "", acct)).unwrap();
        assert_eq!(
            dispatch(&req("DELETE", &path.clone(), json!({}), "", acct))
                .unwrap()
                .status,
            200
        );
        assert_eq!(state(acct), "TERMINATED");
    }

    #[test]
    fn update_requires_stopped_state_and_updates_fields() {
        let acct = uuid_account();
        let app_id = create_app(acct, json!({}))["applicationId"]
            .as_str()
            .unwrap()
            .to_string();
        let path = format!("/applications/{app_id}");
        // update in STARTED is rejected
        let err = dispatch(&req(
            "PATCH",
            &path.clone(),
            json!({"maximumCapacity": {"cpu": "4"}}),
            "",
            acct,
        ))
        .unwrap();
        assert_eq!(body(&err)["__type"], "ValidationException");
        // stop, update, verify the capacity round-trips into the detail
        dispatch(&req("POST", &format!("{path}/stop"), json!({}), "", acct)).unwrap();
        let resp = dispatch(&req(
            "PATCH",
            &path.clone(),
            json!({"maximumCapacity": {"cpu": "4 vCPU", "memory": "16 GB", "disk": "100 GB"}}),
            "",
            acct,
        ))
        .unwrap();
        assert_eq!(body(&resp)["application"]["applicationId"], app_id);
        assert_eq!(
            body(&resp)["application"]["maximumCapacity"]["memory"],
            "16 GB"
        );
    }

    #[test]
    fn validation_errors_mirroring_moto() {
        let acct = uuid_account();
        // unsupported engine
        let r = dispatch(&req(
            "POST",
            "/applications",
            json!({"type": "FLINK", "releaseLabel": "emr-6.15.0", "name": "x"}),
            "",
            acct,
        ))
        .unwrap();
        assert_eq!(body(&r)["__type"], "ValidationException");
        assert_eq!(r.status, 400);
        // bad release label
        let r = dispatch(&req(
            "POST",
            "/applications",
            json!({"type": "SPARK", "releaseLabel": "emr-7.9.9-late", "name": "x"}),
            "",
            acct,
        ))
        .unwrap();
        assert_eq!(body(&r)["message"].as_str().unwrap(),
            "Type 'SPARK' is not supported for release label 'emr-7.9.9-late' or release label does not exist");
        // missing application on get
        let r = dispatch(&req("GET", "/applications/missing123", json!({}), "", acct)).unwrap();
        assert_eq!(body(&r)["__type"], "ResourceNotFoundException");
        assert_eq!(body(&r)["message"], "Application missing123 does not exist");
    }

    #[test]
    fn job_run_roundtrip() {
        let acct = uuid_account();
        let app_id = create_app(acct, json!({"name": "jobs"}))["applicationId"]
            .as_str()
            .unwrap()
            .to_string();
        let role = format!("arn:aws:iam::{acct}:role/test-role");

        // create
        let r = body(
            &dispatch(&req(
                "POST",
                &format!("/applications/{app_id}/jobruns"),
                json!({
                    "executionRoleArn": role,
                    "jobDriver": {"sparkSubmit": {"entryPoint": "s3://bucket/script.py"}},
                    "name": "first"
                }),
                "",
                acct,
            ))
            .unwrap(),
        );
        let run_id = r["jobRunId"].as_str().unwrap().to_string();
        assert_eq!(r["applicationId"], app_id.clone());
        assert!(r["arn"].as_str().unwrap().contains("/jobruns/"));

        // get: summary asserts the suite makes
        let r = body(
            &dispatch(&req(
                "GET",
                &format!("/applications/{app_id}/jobruns/{run_id}"),
                json!({}),
                "",
                acct,
            ))
            .unwrap(),
        );
        let jr = &r["jobRun"];
        assert_eq!(jr["jobRunId"], run_id.clone());
        assert_eq!(jr["applicationId"], app_id.clone());
        assert!(jr.get("state").is_some());
        assert_eq!(
            jr["executionRole"],
            json!(format!("arn:aws:iam::{acct}:role/test-role"))
        );
        assert_eq!(jr["state"], "SUCCESS"); // moto creates runs in SUCCESS
        assert_eq!(jr["executionTimeoutMinutes"], 720);

        // cancel
        let r = body(
            &dispatch(&req(
                "DELETE",
                &format!("/applications/{app_id}/jobruns/{run_id}"),
                json!({}),
                "",
                acct,
            ))
            .unwrap(),
        );
        assert_eq!(r["applicationId"], app_id.clone());
        assert_eq!(r["jobRunId"], run_id.clone());
        let r = body(
            &dispatch(&req(
                "GET",
                &format!("/applications/{app_id}/jobruns/{run_id}"),
                json!({}),
                "",
                acct,
            ))
            .unwrap(),
        );
        assert_eq!(r["jobRun"]["state"], "CANCELLED");

        // list: jobRunId→id, contains our run
        let r = body(
            &dispatch(&req(
                "GET",
                &format!("/applications/{app_id}/jobruns"),
                json!({}),
                "",
                acct,
            ))
            .unwrap(),
        );
        assert!(r["jobRuns"]
            .as_array()
            .unwrap()
            .iter()
            .any(|j| j["id"] == run_id.clone()));
    }

    #[test]
    fn job_run_guards() {
        let acct = uuid_account();
        let app_id = create_app(acct, json!({}))["applicationId"]
            .as_str()
            .unwrap()
            .to_string();
        let own_role = format!("arn:aws:iam::{acct}:role/r");
        // cross-account pass role raises before the app lookup
        let r = body(
            &dispatch(&req(
                "POST",
                &format!("/applications/{app_id}/jobruns"),
                json!({"executionRoleArn": "arn:aws:iam::999999999999:role/r"}),
                "",
                acct,
            ))
            .unwrap(),
        );
        assert_eq!(r["__type"], "AccessDeniedException");
        // short timeout
        let r = body(
            &dispatch(&req(
                "POST",
                &format!("/applications/{app_id}/jobruns"),
                json!({"executionRoleArn": own_role.clone(), "executionTimeoutMinutes": 1}),
                "",
                acct,
            ))
            .unwrap(),
        );
        assert_eq!(r["__type"], "ValidationException");
        assert_eq!(r["message"], "RunTimeout must be at least 5 minutes.");
        // missing app: app lookup error
        let r = body(
            &dispatch(&req(
                "POST",
                "/applications/nope12345/jobruns",
                json!({"executionRoleArn": own_role.clone()}),
                "",
                acct,
            ))
            .unwrap(),
        );
        assert_eq!(r["__type"], "ResourceNotFoundException");
        assert_eq!(
            body(
                &dispatch(&req(
                    "GET",
                    "/applications/app123/jobruns/missing123",
                    json!({}),
                    "",
                    acct
                ))
                .unwrap()
            )["__type"],
            "ResourceNotFoundException"
        );
        // job run on a terminated app
        dispatch(&req(
            "POST",
            &format!("/applications/{app_id}/stop"),
            json!({}),
            "",
            acct,
        ))
        .unwrap();
        dispatch(&req(
            "DELETE",
            &format!("/applications/{app_id}"),
            json!({}),
            "",
            acct,
        ))
        .unwrap();
        let r = body(
            &dispatch(&req(
                "POST",
                &format!("/applications/{app_id}/jobruns"),
                json!({"executionRoleArn": own_role.clone()}),
                "",
                acct,
            ))
            .unwrap(),
        );
        assert_eq!(
            r["message"],
            format!("Application {app_id} is terminated. Cannot start job run.")
        );
        // job-run read on an app that never had a run: moto reports the app
        let other = create_app(acct, json!({}))["applicationId"]
            .as_str()
            .unwrap()
            .to_string();
        let r = body(
            &dispatch(&req(
                "GET",
                &format!("/applications/{other}/jobruns"),
                json!({}),
                "",
                acct,
            ))
            .unwrap(),
        );
        assert_eq!(r["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn list_pagination_uses_offset_tokens() {
        let acct = uuid_account();
        let mut ids = Vec::new();
        for i in 0..3 {
            ids.push(
                create_app(acct, json!({"name": format!("zzz{i}")}))["applicationId"]
                    .as_str()
                    .unwrap()
                    .to_string(),
            );
        }
        let r = body(
            &dispatch(&req(
                "GET",
                "/applications",
                json!({}),
                "maxResults=2",
                acct,
            ))
            .unwrap(),
        );
        let items = r["applications"].as_array().unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(r["nextToken"], "2"); // zzz are sorted first
        let r2 = body(
            &dispatch(&req(
                "GET",
                "/applications",
                json!({}),
                "maxResults=2&nextToken=2",
                acct,
            ))
            .unwrap(),
        );
        assert!(r2["nextToken"].is_null()); // short page ends the chain
        assert!(r2["applications"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| { ids.iter().any(|id| id == i["id"].as_str().unwrap()) }));
        // states filter
        let r = body(
            &dispatch(&req(
                "GET",
                "/applications",
                json!({}),
                "states=STOPPED",
                acct,
            ))
            .unwrap(),
        );
        assert_eq!(r["applications"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn unrouted_paths_stay_absent() {
        // moto routes neither /tags ops, the dashboard, nor run attempts —
        // dispatch must answer None so the gateway serves 501 verbatim.
        let acct = uuid_account();
        assert!(dispatch(&req(
            "POST",
            "/tags/arn%3Aaws%3Aemr-serverless",
            json!({"tags": {}}),
            "",
            acct
        ))
        .is_none());
        assert!(dispatch(&req("GET", "/tags/arn%3Ax", json!({}), "", acct)).is_none());
        assert!(dispatch(&req(
            "GET",
            "/applications/a/jobruns/j/dashboard",
            json!({}),
            "",
            acct
        ))
        .is_none());
        assert!(dispatch(&req(
            "GET",
            "/applications/a/jobruns/j/attempts",
            json!({}),
            "",
            acct
        ))
        .is_none());
        // wrong service label
        let mut wrong = req("POST", "/applications", json!({}), "", acct);
        wrong.service = "athena".into();
        assert!(dispatch(&wrong).is_none());
    }
}
