//! EMR Containers (EMR on EKS) backend (REST-JSON, path-per-operation).
//!
//! Port of moto's `emrcontainers` service: virtual clusters and their job
//! runs. Wire bodies mirror moto's raw `json.dumps(...)` payloads (the
//! compat suite pins them), including moto's quirks:
//!
//! - `createdAt` fields are today's midnight UTC (`%Y-%m-%dT%H:%M:%SZ`), in
//!   moto's `datetime.today().replace(hour=0, ...)` shape.
//! - `StartJobRun`/`CancelJobRun` render through moto's `FakeJob.__iter__`,
//!   which serializes `configurationOverrides` as the *release label* (a moto
//!   bug kept verbatim); `DescribeJobRun`/`ListJobRuns` use `to_dict`, which
//!   carries the real value.
//! - `ListJobRuns`'s `name` filter keeps jobs whose name is a *substring of*
//!   the requested name (moto compares `job["name"] in name`).
//! - One virtual cluster per EKS namespace (400 `ValidationException` on a
//!   repeat), ids are 25/19 lowercase-alnum chars, missing virtual-cluster
//!   paths answer moto's `ValidationException` / `ResourceNotFoundException`
//!   JSON documents, and misshapen `containerProvider` bodies escape moto's
//!   `KeyError` as a 500 `InternalError`.
//! - Botocore surface outside moto's module (job templates, managed
//!   endpoints, security configurations, `ListTagsForResource`) answers
//!   moto's `The <snake_op> action has not been implemented` 501 document.
//!
//! State is per-(account, region) via [`crate::store`].

use crate::proto::{json_error, parse_json_body, query_map, url_decode};
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::BTreeMap;

const DEFAULT_MAX_RESULTS: i64 = 100;
/// moto's response default for the `containerProviderType` listing filter.
const DEFAULT_CONTAINER_PROVIDER_TYPE: &str = "EKS";
const VIRTUAL_CLUSTER_STATUS: &str = "RUNNING";
const JOB_STATUS: &str = "RUNNING";

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// moto `FakeCluster` — `__iter__` and `to_dict` render identical keys here.
struct VirtualCluster {
    id: String,
    name: Value,
    arn: String,
    state: String,
    container_provider: Value,
    created_at: String,
    tags: Value,
}

impl VirtualCluster {
    /// moto: `id, name, arn, state, containerProvider, createdAt, tags`.
    fn to_wire(&self) -> Value {
        json!({
            "id": self.id,
            "name": self.name,
            "arn": self.arn,
            "state": self.state,
            "containerProvider": self.container_provider,
            "createdAt": self.created_at,
            "tags": self.tags,
        })
    }
}

/// moto `FakeJob`. Two serializations: `__iter__` (StartJobRun / CancelJobRun
/// responses — `configurationOverrides` is the release label) and `to_dict`
/// (DescribeJobRun / ListJobRuns — the real request value).
struct JobRun {
    id: String,
    name: Value,
    virtual_cluster_id: String,
    arn: String,
    state: String,
    client_token: Value,
    execution_role_arn: Value,
    release_label: Value,
    configuration_overrides: Value,
    job_driver: Value,
    created_at: String,
    /// moto hardcodes `created_by = None`; always null on the wire.
    created_by: Value,
    finished_at: Value,
    state_details: Value,
    failure_reason: Value,
    tags: Value,
}

impl JobRun {
    /// moto `to_dict` (DescribeJobRun payload; ListJobRuns rows).
    fn to_dict(&self) -> Value {
        json!({
            "id": self.id,
            "name": self.name,
            "virtualClusterId": self.virtual_cluster_id,
            "arn": self.arn,
            "state": self.state,
            "clientToken": self.client_token,
            "executionRoleArn": self.execution_role_arn,
            "releaseLabel": self.release_label,
            "configurationOverrides": self.configuration_overrides,
            "jobDriver": self.job_driver,
            "createdAt": self.created_at,
            "createdBy": self.created_by,
            "finishedAt": self.finished_at,
            "stateDetails": self.state_details,
            "failureReason": self.failure_reason,
            "tags": self.tags,
        })
    }

    /// moto `__iter__` (StartJobRun / CancelJobRun payloads): the
    /// `configurationOverrides` slot carries the release label.
    fn to_wire(&self) -> Value {
        let mut v = self.to_dict();
        if let Some(obj) = v.as_object_mut() {
            obj.insert(
                "configurationOverrides".to_string(),
                self.release_label.clone(),
            );
        }
        v
    }
}

/// Insertion-ordered records (moto iterates its python dicts).
#[derive(Default)]
struct PartitionState {
    virtual_clusters: Vec<VirtualCluster>,
    jobs: Vec<JobRun>,
}

fn with_state<T>(account: u64, region: &str, f: impl FnOnce(&mut PartitionState) -> T) -> T {
    static SLOT: Lazy<store::Slot<PartitionState>> = Lazy::new(|| store::slot("emrcontainers"));
    let mut map = SLOT.write();
    f(map.entry((account, region.to_string())).or_default())
}

// ---------------------------------------------------------------------------
// Wire helpers (moto core error shapes)
// ---------------------------------------------------------------------------

fn ok(v: Value) -> Response {
    Response::json_status(200, &v)
}

/// moto's KeyError escapes surface as a 500 (werkzeug internal error).
fn internal_error(message: String) -> Response {
    Response::json_status(
        500,
        &json!({ "__type": "InternalError", "message": message }),
    )
}

/// moto's shared `ValidationException` (config.exceptions): 400 JSON doc.
fn validation(message: String) -> Response {
    json_error(400, "ValidationException", &message)
}

/// moto's `ResourceNotFoundException`: 400 JSON doc (not 404).
fn resource_not_found(message: String) -> Response {
    json_error(400, "ResourceNotFoundException", &message)
}

/// moto's 501 for an action its response class does not implement.
fn not_implemented(snake_op: &str) -> Response {
    Response::json_status(
        501,
        &json!({
            "__type": "NotImplemented",
            "message": format!("The {snake_op} action has not been implemented"),
        }),
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

// ---------------------------------------------------------------------------
// IDs / time (moto utils)
// ---------------------------------------------------------------------------

/// moto's `random_id`: digits + lowercase letters alphabet.
fn random_id(len: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
    (0..len)
        .map(|i| ALPHABET[bytes[i % bytes.len()] as usize % ALPHABET.len()] as char)
        .collect()
}

/// moto: `iso_8601_datetime_without_milliseconds(datetime.today().replace(
/// hour=0, minute=0, second=0, microsecond=0))` — midnight today, wire format
/// `%Y-%m-%dT%H:%M:%SZ`.
fn today_midnight() -> String {
    format!("{}T00:00:00Z", chrono::Utc::now().date_naive())
}

/// moto's cancel timestamp: today's midnight + one minute.
fn today_midnight_plus_minute() -> String {
    format!("{}T00:01:00Z", chrono::Utc::now().date_naive())
}

// ---------------------------------------------------------------------------
// Request params
// ---------------------------------------------------------------------------

fn int_param(q: &BTreeMap<String, Vec<String>>, key: &str, default: i64) -> Result<i64, Response> {
    match q.get(key).and_then(|v| v.first()) {
        None => Ok(default),
        Some(raw) => raw.parse::<i64>().map_err(|_| {
            internal_error(format!("invalid literal for int() with base 10: '{raw}'"))
        }),
    }
}

fn opt_str_param(q: &BTreeMap<String, Vec<String>>, key: &str) -> Option<String> {
    q.get(key).and_then(|v| v.first().cloned())
}

fn str_list_param(q: &BTreeMap<String, Vec<String>>, key: &str) -> Vec<String> {
    q.get(key).cloned().unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Pagination (moto emrcontainers.utils.paginated_list)
// ---------------------------------------------------------------------------

/// Sorted slice `full_list[next_token : next_token + max_results]` with the
/// offset token bound when the page is fully consumed (moto's
/// `paginated_list`: token = `str(next_token + max_results)`, so tokens ride
/// the wire as JSON strings — including the degenerate `maxResults=0` case,
/// whose token moves by zero). Python list-slice semantics cover negative
/// indices (botocore validates `maxResults >= 1` client-side; moto never
/// sees them through boto3).
fn paginated_list(
    full_list: Vec<Value>,
    sort_key: &str,
    max_results: i64,
    token: Option<&str>,
) -> Result<(Vec<Value>, Value), Response> {
    let start: i64 = match token {
        None | Some("") => 0,
        Some(t) => t.parse::<i64>().map_err(|_| {
            internal_error(format!("invalid literal for int() with base 10: '{t}'"))
        })?,
    };
    let mut sorted = full_list;
    sorted.sort_by(|a, b| {
        let ka = a[sort_key].as_str().unwrap_or_default();
        let kb = b[sort_key].as_str().unwrap_or_default();
        ka.cmp(kb)
    });
    let (s, e) = py_range(
        sorted.len() as i64,
        start,
        start.saturating_add(max_results),
    );
    let page: Vec<Value> = sorted[s..e].to_vec();
    let next_token = if page.len() as i64 == max_results {
        json!(start.saturating_add(max_results).to_string())
    } else {
        Value::Null
    };
    Ok((page, next_token))
}

/// Python `list[a:b]` index normalization (step 1): negative indices count
/// back from the end; out-of-range clamps; `b <= a` yields an empty slice.
fn py_range(len: i64, a: i64, b: i64) -> (usize, usize) {
    let norm = |v: i64| -> usize {
        let v = if v < 0 { v + len } else { v };
        v.clamp(0, len) as usize
    };
    let (s, e) = (norm(a), norm(b));
    (s, e.max(s))
}

// ---------------------------------------------------------------------------
// Ops: virtual clusters
// ---------------------------------------------------------------------------

fn find_cluster<'a>(st: &'a PartitionState, cluster_id: &str) -> Option<&'a VirtualCluster> {
    st.virtual_clusters.iter().find(|vc| vc.id == cluster_id)
}

fn find_cluster_mut<'a>(
    st: &'a mut PartitionState,
    cluster_id: &str,
) -> Option<&'a mut VirtualCluster> {
    st.virtual_clusters
        .iter_mut()
        .find(|vc| vc.id == cluster_id)
}

/// CreateVirtualCluster — POST /virtualclusters.
fn create_virtual_cluster(
    st: &mut PartitionState,
    account: u64,
    region: &str,
    body: &Value,
) -> Result<Response, Response> {
    let provider = match body.get("containerProvider") {
        Some(cp @ Value::Object(_)) => cp.clone(),
        // moto's backend reads container_provider["info"]["eksInfo"] first
        // (occupied-namespace check) — a non-object provider escapes as
        // KeyError('info') before FakeCluster reads ["id"].
        _ => return Err(internal_error("'info'".to_string())),
    };

    // moto FakeCluster.__init__ + backend namespace check read, in order:
    // ["info"]["eksInfo"]["namespace"], then ["id"].
    let namespace = provider
        .get("info")
        .ok_or_else(|| internal_error("'info'".to_string()))?
        .get("eksInfo")
        .ok_or_else(|| internal_error("'eksInfo'".to_string()))?
        .get("namespace")
        .ok_or_else(|| internal_error("'namespace'".to_string()))?
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| internal_error("'namespace'".to_string()))?;
    provider
        .get("id")
        .ok_or_else(|| internal_error("'id'".to_string()))?;

    let occupied: Vec<String> = st
        .virtual_clusters
        .iter()
        .filter_map(|vc| {
            vc.container_provider
                .get("info")?
                .get("eksInfo")?
                .get("namespace")?
                .as_str()
                .map(str::to_string)
        })
        .collect();
    if occupied.contains(&namespace) {
        return Err(validation(
            "A virtual cluster already exists in the given namespace".to_string(),
        ));
    }

    let id = random_id(25);
    let arn = format!(
        "arn:{}:emr-containers:{}:{account}:/virtualclusters/{id}",
        partition(region),
        region
    );
    let vc = VirtualCluster {
        id: id.clone(),
        name: body.get("name").cloned().unwrap_or(Value::Null),
        arn,
        state: VIRTUAL_CLUSTER_STATUS.to_string(),
        container_provider: provider,
        created_at: today_midnight(),
        tags: match body.get("tags") {
            Some(t @ Value::Object(_)) => t.clone(),
            // moto stores whatever dict was passed; absent/partial → null
            _ => Value::Null,
        },
    };
    let wire = vc.to_wire();
    st.virtual_clusters.push(vc);
    Ok(ok(wire))
}

/// DescribeVirtualCluster — GET /virtualclusters/{id}.
fn describe_virtual_cluster(st: &PartitionState, id: &str) -> Result<Response, Response> {
    let vc = find_cluster(st, id)
        .ok_or_else(|| validation(format!("Virtual cluster {id} doesn't exist.")))?;
    Ok(ok(json!({ "virtualCluster": vc.to_wire() })))
}

/// DeleteVirtualCluster — DELETE /virtualclusters/{id}.
fn delete_virtual_cluster(st: &mut PartitionState, id: &str) -> Result<Response, Response> {
    let vc = find_cluster_mut(st, id)
        .ok_or_else(|| validation("VirtualCluster does not exist".to_string()))?;
    vc.state = "TERMINATED".to_string();
    Ok(ok(vc.to_wire()))
}

/// ListVirtualClusters — GET /virtualclusters.
fn list_virtual_clusters(
    st: &PartitionState,
    q: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let provider_id = opt_str_param(q, "containerProviderId").unwrap_or_default();
    let provider_type = opt_str_param(q, "containerProviderType")
        .unwrap_or_else(|| DEFAULT_CONTAINER_PROVIDER_TYPE.to_string());
    let created_after = opt_str_param(q, "createdAfter").unwrap_or_default();
    let created_before = opt_str_param(q, "createdBefore").unwrap_or_default();
    let states = str_list_param(q, "states");
    let max_results = int_param(q, "maxResults", DEFAULT_MAX_RESULTS)?;
    let token = opt_str_param(q, "nextToken");

    let mut clusters: Vec<Value> = st.virtual_clusters.iter().map(|vc| vc.to_wire()).collect();
    if !provider_id.is_empty() {
        clusters.retain(|c| c["containerProvider"]["id"].as_str() == Some(provider_id.as_str()));
    }
    if !provider_type.is_empty() {
        clusters.retain(|c| {
            c["containerProvider"]["type"]
                .as_str()
                .map(|t| t == provider_type)
                .unwrap_or(false)
        });
    }
    if !created_after.is_empty() {
        clusters.retain(|c| {
            c["createdAt"]
                .as_str()
                .map(|t| t >= created_after.as_str())
                .unwrap_or(false)
        });
    }
    if !created_before.is_empty() {
        clusters.retain(|c| {
            c["createdAt"]
                .as_str()
                .map(|t| t <= created_before.as_str())
                .unwrap_or(false)
        });
    }
    if !states.is_empty() {
        clusters.retain(|c| {
            c["state"]
                .as_str()
                .map(|s| states.iter().any(|w| w == s))
                .unwrap_or(false)
        });
    }
    let (page, token) = paginated_list(clusters, "name", max_results, token.as_deref())?;
    Ok(ok(json!({ "virtualClusters": page, "nextToken": token })))
}

// ---------------------------------------------------------------------------
// Ops: job runs
// ---------------------------------------------------------------------------

/// Hand-rolled `re.match` for moto's
/// `emr-[0-9]{1}\.[0-9]{1,2}\.0-(latest|[0-9]{8})` (prefix match).
fn release_label_matches(label: Option<&str>) -> bool {
    let Some(rest) = label.and_then(|l| l.strip_prefix("emr-")) else {
        return false;
    };
    let b = rest.as_bytes();
    let mut i = 0usize;
    // [0-9]{1}
    if !b.first().copied().is_some_and(|c| c.is_ascii_digit()) {
        return false;
    }
    i += 1;
    // \.
    if b.get(i) != Some(&b'.') {
        return false;
    }
    i += 1;
    // [0-9]{1,2}
    let start = i;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    let minor = i - start;
    if !(1..=2).contains(&minor) {
        return false;
    }
    // \.0-
    if b.get(i) != Some(&b'.') || b.get(i + 1) != Some(&b'0') || b.get(i + 2) != Some(&b'-') {
        return false;
    }
    i += 3;
    // (latest|[0-9]{8}) — re.match consumes a prefix; trailing bytes are free.
    let after = &rest[i..];
    let digits = after.chars().take_while(|c| c.is_ascii_digit()).count();
    after.starts_with("latest") || digits >= 8
}

/// moto's fake job-run ids (`random_id(19)`).
fn random_job_id() -> String {
    random_id(19)
}

/// moto: `re.match(r"[a-z,A-Z,0-9]{19}", job_id)` — the first 19 bytes must
/// all be alphanumeric (or the accidental literal comma); length may exceed.
fn job_id_shape_ok(job_id: &str) -> bool {
    let bytes = job_id.as_bytes();
    bytes.len() >= 19
        && bytes[..19]
            .iter()
            .all(|c| c.is_ascii_alphanumeric() || *c == b',')
}

/// StartJobRun — POST /virtualclusters/{virtualClusterId}/jobruns.
fn start_job_run(
    st: &mut PartitionState,
    account: u64,
    region: &str,
    virtual_cluster_id: &str,
    body: &Value,
) -> Result<Response, Response> {
    if find_cluster(st, virtual_cluster_id).is_none() {
        return Err(resource_not_found(format!(
            "Virtual cluster {virtual_cluster_id} doesn't exist."
        )));
    }

    let release_label = body.get("releaseLabel").and_then(Value::as_str);
    if !release_label_matches(release_label) {
        return Err(resource_not_found(format!(
            "Release {} doesn't exist.",
            release_label.unwrap_or("")
        )));
    }

    let id = random_job_id();
    let arn = format!(
        "arn:{}:emr-containers:{}:{account}:/virtualclusters/{virtual_cluster_id}/jobruns/{id}",
        partition(region),
        region
    );
    let job = JobRun {
        id: id.clone(),
        name: body.get("name").cloned().unwrap_or(Value::Null),
        virtual_cluster_id: virtual_cluster_id.to_string(),
        arn,
        state: JOB_STATUS.to_string(),
        client_token: body.get("clientToken").cloned().unwrap_or(Value::Null),
        execution_role_arn: body.get("executionRoleArn").cloned().unwrap_or(Value::Null),
        release_label: release_label.map(Value::from).unwrap_or(Value::Null),
        configuration_overrides: body
            .get("configurationOverrides")
            .cloned()
            .unwrap_or(Value::Null),
        job_driver: body.get("jobDriver").cloned().unwrap_or(Value::Null),
        created_at: today_midnight(),
        created_by: Value::Null,
        finished_at: Value::Null,
        state_details: Value::Null,
        failure_reason: Value::Null,
        tags: match body.get("tags") {
            Some(t @ Value::Object(_)) => t.clone(),
            _ => Value::Null,
        },
    };
    let wire = job.to_wire();
    st.jobs.push(job);
    Ok(ok(wire))
}

/// DescribeJobRun — GET /virtualclusters/{vc}/jobruns/{job}.
fn describe_job_run(
    st: &PartitionState,
    virtual_cluster_id: &str,
    job_id: &str,
) -> Result<Response, Response> {
    if !job_id_shape_ok(job_id) {
        return Err(validation("Invalid job run short id".to_string()));
    }
    let job = st
        .jobs
        .iter()
        .find(|j| j.id == job_id)
        .ok_or_else(|| resource_not_found(format!("Job run {job_id} doesn't exist.")))?;
    if job.virtual_cluster_id != virtual_cluster_id {
        return Err(resource_not_found(format!(
            "Job run {job_id} doesn't exist."
        )));
    }
    Ok(ok(json!({ "jobRun": job.to_dict() })))
}

/// CancelJobRun — DELETE /virtualclusters/{vc}/jobruns/{job}.
fn cancel_job_run(
    st: &mut PartitionState,
    virtual_cluster_id: &str,
    job_id: &str,
) -> Result<Response, Response> {
    if !job_id_shape_ok(job_id) {
        return Err(validation("Invalid job run short id".to_string()));
    }
    let job = st
        .jobs
        .iter_mut()
        .find(|j| j.id == job_id)
        .ok_or_else(|| resource_not_found(format!("Job run {job_id} doesn't exist.")))?;
    if job.virtual_cluster_id != virtual_cluster_id {
        return Err(resource_not_found(format!(
            "Job run {job_id} doesn't exist."
        )));
    }
    if ["FAILED", "CANCELLED", "CANCEL_PENDING", "COMPLETED"]
        .iter()
        .any(|s| *s == job.state)
    {
        return Err(validation(format!(
            "Job run {job_id} is not in a cancellable state"
        )));
    }
    job.state = "CANCELLED".to_string();
    job.finished_at = json!(today_midnight_plus_minute());
    job.state_details = json!("JobRun CANCELLED successfully.");
    Ok(ok(job.to_wire()))
}

/// ListJobRuns — GET /virtualclusters/{vc}/jobruns.
fn list_job_runs(
    st: &PartitionState,
    virtual_cluster_id: &str,
    q: &BTreeMap<String, Vec<String>>,
) -> Result<Response, Response> {
    let created_before = opt_str_param(q, "createdBefore").unwrap_or_default();
    let created_after = opt_str_param(q, "createdAfter").unwrap_or_default();
    let name = opt_str_param(q, "name").unwrap_or_default();
    let states = str_list_param(q, "states");
    let max_results = int_param(q, "maxResults", DEFAULT_MAX_RESULTS)?;
    let token = opt_str_param(q, "nextToken");

    let mut jobs: Vec<Value> = st
        .jobs
        .iter()
        .filter(|j| j.virtual_cluster_id == virtual_cluster_id)
        .map(|j| j.to_dict())
        .collect();
    if !created_after.is_empty() {
        jobs.retain(|j| {
            j["createdAt"]
                .as_str()
                .map(|t| t >= created_after.as_str())
                .unwrap_or(false)
        });
    }
    if !created_before.is_empty() {
        jobs.retain(|j| {
            j["createdAt"]
                .as_str()
                .map(|t| t <= created_before.as_str())
                .unwrap_or(false)
        });
    }
    if !states.is_empty() {
        jobs.retain(|j| {
            j["state"]
                .as_str()
                .map(|s| states.iter().any(|w| w == s))
                .unwrap_or(false)
        });
    }
    // moto compares `job["name"] in name` — a substring check against the
    // filter param (kept verbatim from the moto source).
    if !name.is_empty() {
        jobs.retain(|j| {
            j["name"]
                .as_str()
                .map(|n| name.contains(n))
                .unwrap_or(false)
        });
    }
    let (page, token) = paginated_list(jobs, "id", max_results, token.as_deref())?;
    Ok(ok(json!({ "jobRuns": page, "nextToken": token })))
}

// ---------------------------------------------------------------------------
// REST routing (operation per method + requestUri)
// ---------------------------------------------------------------------------

/// Dispatch a single emr-containers request. The gateway routes the whole
/// `emr-containers` signing scope here, so unrouted method+path pairs answer
/// the moto wire shapes (404 empty / 501 NotImplemented) directly — this
/// dispatch never returns `None`.
pub fn dispatch(req: &Request) -> Option<Response> {
    // moto anchors its url regexes on the raw path; percent-encoded path
    // parameters are decoded once, after the arm resolves.
    let decoded: Vec<String> = req
        .path
        .trim_start_matches('/')
        .split('/')
        .map(url_decode)
        .collect();
    let segs: Vec<&str> = decoded.iter().map(String::as_str).collect();
    let q = query_map(&req.query_string);
    let body = parse_json_body(&req.body);

    let result = with_state(req.account, &req.region, |st| {
        match (req.method.as_str(), segs.as_slice()) {
            // --- implemented surface (moto emrcontainers) ---
            ("GET", ["virtualclusters"]) => list_virtual_clusters(st, &q),
            ("POST", ["virtualclusters"]) => {
                create_virtual_cluster(st, req.account, &req.region, &body)
            }
            ("GET", ["virtualclusters", vc]) => describe_virtual_cluster(st, vc),
            ("DELETE", ["virtualclusters", vc]) => delete_virtual_cluster(st, vc),
            ("GET", ["virtualclusters", vc, "jobruns"]) => list_job_runs(st, vc, &q),
            ("POST", ["virtualclusters", vc, "jobruns"]) => {
                start_job_run(st, req.account, &req.region, vc, &body)
            }
            // botocore surface moto's module does not implement — these
            // literal-segment arms must precede the job-run wildcard arms,
            // or "endpoints" would be eaten as a job-run short id.
            ("POST", ["virtualclusters", _, "endpoints"]) => {
                Err(not_implemented("create_managed_endpoint"))
            }
            ("GET", ["virtualclusters", _, "endpoints"]) => {
                Err(not_implemented("list_managed_endpoints"))
            }
            ("GET", ["virtualclusters", _, "endpoints", _]) => {
                Err(not_implemented("describe_managed_endpoint"))
            }
            ("DELETE", ["virtualclusters", _, "endpoints", _]) => {
                Err(not_implemented("delete_managed_endpoint"))
            }
            ("GET", ["virtualclusters", vc, "jobruns", job]) => describe_job_run(st, vc, job),
            ("DELETE", ["virtualclusters", vc, "jobruns", job]) => cancel_job_run(st, vc, job),
            // --- more botocore surface moto's module does not implement ---
            ("POST", ["jobtemplates"]) => Err(not_implemented("create_job_template")),
            ("GET", ["jobtemplates"]) => Err(not_implemented("list_job_templates")),
            ("GET", ["jobtemplates", _]) => Err(not_implemented("describe_job_template")),
            ("DELETE", ["jobtemplates", _]) => Err(not_implemented("delete_job_template")),
            ("POST", ["securityconfigurations"]) => {
                Err(not_implemented("create_security_configuration"))
            }
            ("GET", ["securityconfigurations"]) => {
                Err(not_implemented("list_security_configurations"))
            }
            ("GET", ["securityconfigurations", _]) => {
                Err(not_implemented("describe_security_configuration"))
            }
            ("GET", ["tags", _]) => Err(not_implemented("list_tags_for_resource")),
            // moto: `if not action: return 404, headers, ""`.
            _ => Ok(Response::new(404, "application/json", Vec::new())),
        }
    });
    Some(result.unwrap_or_else(|resp| resp))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn scope() -> u64 {
        // unique account per test for state isolation
        uuid::Uuid::new_v4().as_u128() as u64
    }

    fn req(method: &str, path: &str) -> Request {
        let (path, query) = match path.split_once('?') {
            Some((p, q)) => (p.to_string(), q.to_string()),
            None => (path.to_string(), String::new()),
        };
        Request {
            service: "emrcontainers".into(),
            operation: String::new(),
            method: method.into(),
            path,
            query_string: query,
            headers: HashMap::new(),
            body: Bytes::new(),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn req_for(acct: u64, method: &str, path: &str, body: Value) -> Request {
        Request {
            body: Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
            account: acct,
            ..req(method, path)
        }
    }

    fn body_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    fn provider(ns: &str) -> Value {
        json!({"id": "eks-1", "type": "EKS", "info": {"eksInfo": {"namespace": ns}}})
    }

    fn make_cluster(acct: u64, name: &str, ns: &str) -> Value {
        let r = req_for(
            acct,
            "POST",
            "/virtualclusters",
            json!({"name": name, "containerProvider": provider(ns), "clientToken": "ct"}),
        );
        let resp = body_of(&dispatch(&r).unwrap());
        resp.as_object().cloned().map(Value::Object).unwrap()
    }

    #[test]
    fn create_virtual_cluster_wire_shape() {
        let acct = scope();
        let vc = make_cluster(acct, "vc", "ns-create");
        assert_eq!(vc["name"], "vc");
        assert_eq!(vc["state"], "RUNNING");
        assert_eq!(vc["tags"], Value::Null);
        let arn = vc["arn"].as_str().unwrap();
        assert_eq!(
            arn,
            format!(
                "arn:aws:emr-containers:us-east-1:{acct}:/virtualclusters/{}",
                vc["id"].as_str().unwrap()
            )
        );
        assert!(vc["id"].as_str().unwrap().len() == 25);
        let today = format!("{}T00:00:00Z", chrono::Utc::now().date_naive());
        assert_eq!(vc["createdAt"], today);
    }

    #[test]
    fn create_carries_container_provider_and_tags() {
        let acct = scope();
        let r = req_for(
            acct,
            "POST",
            "/virtualclusters",
            json!({
                "name": "vc",
                "containerProvider": provider("ns-cp"),
                "clientToken": "ct",
                "tags": {"team": "data"}
            }),
        );
        let vc = body_of(&dispatch(&r).unwrap());
        assert_eq!(vc["containerProvider"], provider("ns-cp"));
        assert_eq!(vc["tags"], json!({"team": "data"}));
    }

    #[test]
    fn duplicate_namespace_is_validation_exception() {
        let acct = scope();
        make_cluster(acct, "one", "ns-dup");
        let r = req_for(
            acct,
            "POST",
            "/virtualclusters",
            json!({"name": "two", "containerProvider": provider("ns-dup"), "clientToken": "ct"}),
        );
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 400);
        let b = body_of(&resp);
        assert_eq!(b["__type"], "ValidationException");
        assert_eq!(
            b["message"],
            "A virtual cluster already exists in the given namespace"
        );
    }

    #[test]
    fn describe_and_delete_round_trip() {
        let acct = scope();
        let vc = make_cluster(acct, "vc", "ns-dd");
        let id = vc["id"].as_str().unwrap().to_string();

        let r = req_for(acct, "GET", &format!("/virtualclusters/{id}"), json!({}));
        let described = body_of(&dispatch(&r).unwrap())["virtualCluster"]
            .as_object()
            .cloned()
            .unwrap();
        assert_eq!(described["id"], id);
        assert_eq!(described["name"], "vc");
        assert_eq!(described["arn"], vc["arn"]);
        assert!(described.get("containerProvider").is_some());

        let r = req_for(acct, "DELETE", &format!("/virtualclusters/{id}"), json!({}));
        let deleted = body_of(&dispatch(&r).unwrap());
        assert_eq!(deleted["id"], id);
        assert_eq!(deleted["state"], "TERMINATED");
    }

    #[test]
    fn describe_unknown_is_validation_exception() {
        let acct = scope();
        let r = req_for(acct, "GET", "/virtualclusters/does-not-exist", json!({}));
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 400);
        let b = body_of(&resp);
        assert_eq!(b["__type"], "ValidationException");
        assert_eq!(
            b["message"],
            "Virtual cluster does-not-exist doesn't exist."
        );
    }

    #[test]
    fn delete_unknown_uses_motos_message() {
        let acct = scope();
        let r = req_for(acct, "DELETE", "/virtualclusters/nope", json!({}));
        let b = body_of(&dispatch(&r).unwrap());
        assert_eq!(b["__type"], "ValidationException");
        assert_eq!(b["message"], "VirtualCluster does not exist");
    }

    #[test]
    fn list_virtual_clusters_and_filters() {
        let acct = scope();
        make_cluster(acct, "vc-a", "ns-list-a");
        make_cluster(acct, "vc-b", "ns-list-b");

        let r = req_for(acct, "GET", "/virtualclusters", json!({}));
        let resp = body_of(&dispatch(&r).unwrap());
        let ids: Vec<&str> = resp["virtualClusters"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids.len(), 2);
        assert_eq!(resp["nextToken"], Value::Null);

        // name sort (moto's listing key)
        let names: Vec<&str> = resp["virtualClusters"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap())
            .collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);

        // containerProviderType defaults to moto's "EKS"
        let r = req_for(
            acct,
            "GET",
            "/virtualclusters?states=RUNNING&maxResults=1",
            json!({}),
        );
        let resp = body_of(&dispatch(&r).unwrap());
        assert_eq!(resp["virtualClusters"].as_array().unwrap().len(), 1);
        assert_eq!(resp["nextToken"], json!("1"));
    }

    #[test]
    fn start_job_run_and_iter_bug() {
        let acct = scope();
        let vc = make_cluster(acct, "vc", "ns-sjr");
        let vcid = vc["id"].as_str().unwrap().to_string();

        let r = req_for(
            acct,
            "POST",
            &format!("/virtualclusters/{vcid}/jobruns"),
            json!({
                "name": "job",
                "clientToken": "ct",
                "executionRoleArn": "arn:aws:iam::1:role/r",
                "releaseLabel": "emr-7.1.0-latest",
                "jobDriver": {"sparkSubmitJobDriver": {"entryPoint": "s3://e.py"}},
                "configurationOverrides": {
                    "applicationConfiguration": [],
                    "monitoringConfiguration": {}
                }
            }),
        );
        let wire = body_of(&dispatch(&r).unwrap());
        assert_eq!(
            wire["createdAt"],
            format!("{}T00:00:00Z", chrono::Utc::now().date_naive())
        );
        assert_eq!(wire["releaseLabel"], "emr-7.1.0-latest");
        // moto `__iter__` bug: configurationOverrides rides the release label
        // on StartJobRun (and CancelJobRun) payloads.
        assert_eq!(wire["configurationOverrides"], "emr-7.1.0-latest");
        assert_eq!(wire["virtualClusterId"], vcid);
        assert!(wire["id"].as_str().unwrap().len() == 19);
        assert_eq!(
            wire["arn"],
            format!(
                "arn:aws:emr-containers:us-east-1:{acct}:/virtualclusters/{vcid}/jobruns/{}",
                wire["id"].as_str().unwrap()
            )
        );
    }

    #[test]
    fn describe_job_run_uses_to_dict() {
        let acct = scope();
        let vc = make_cluster(acct, "vc", "ns-djr");
        let vcid = vc["id"].as_str().unwrap().to_string();
        let r = req_for(
            acct,
            "POST",
            &format!("/virtualclusters/{vcid}/jobruns"),
            json!({
                "name": "job",
                "clientToken": "ct",
                "executionRoleArn": "arn:role",
                "releaseLabel": "emr-6.10.0-latest",
                "jobDriver": {"sparkSubmitJobDriver": {"entryPoint": "ep"}},
                "configurationOverrides": {"monitoringConfiguration": {}}
            }),
        );
        let created = body_of(&dispatch(&r).unwrap());
        let job_id = created["id"].as_str().unwrap().to_string();

        let r = req_for(
            acct,
            "GET",
            &format!("/virtualclusters/{vcid}/jobruns/{job_id}"),
            json!({}),
        );
        let run = body_of(&dispatch(&r).unwrap())["jobRun"].clone();
        // to_dict carries the real value (no iter bug here)
        assert_eq!(
            run["configurationOverrides"],
            json!({"monitoringConfiguration": {}})
        );
        assert_eq!(run["state"], "RUNNING");
        assert_eq!(run["createdBy"], Value::Null);
        assert_eq!(run["finishedAt"], Value::Null);
    }

    #[test]
    fn start_job_run_unknown_cluster() {
        let acct = scope();
        let r = req_for(
            acct,
            "POST",
            "/virtualclusters/missing/jobruns",
            json!({"name": "j", "releaseLabel": "emr-7.1.0-latest"}),
        );
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 400);
        let b = body_of(&resp);
        assert_eq!(b["__type"], "ResourceNotFoundException");
        assert_eq!(b["message"], "Virtual cluster missing doesn't exist.");
    }

    #[test]
    fn bad_release_labels() {
        let acct = scope();
        let vc = make_cluster(acct, "vc", "ns-label");
        let vcid = vc["id"].as_str().unwrap().to_string();
        for label in [
            "emr-9.9.9-latest",
            "bogus",
            "emr-71.0-latest",
            "emr-7.123.0-latest",
        ] {
            let r = req_for(
                acct,
                "POST",
                &format!("/virtualclusters/{vcid}/jobruns"),
                json!({"name": "j", "releaseLabel": label}),
            );
            let resp = dispatch(&r).unwrap();
            assert_eq!(resp.status, 400, "{label}");
            let b = body_of(&resp);
            assert_eq!(b["__type"], "ResourceNotFoundException");
            assert_eq!(b["message"], format!("Release {label} doesn't exist."));
        }
    }

    #[test]
    fn cancel_job_run_lifecycle() {
        let acct = scope();
        let vc = make_cluster(acct, "vc", "ns-cancel");
        let vcid = vc["id"].as_str().unwrap().to_string();
        let r = req_for(
            acct,
            "POST",
            &format!("/virtualclusters/{vcid}/jobruns"),
            json!({
                "name": "j",
                "releaseLabel": "emr-7.1.0-latest",
                "configurationOverrides": {"monitoringConfiguration": {}}
            }),
        );
        let job_id = body_of(&dispatch(&r).unwrap())["id"]
            .as_str()
            .unwrap()
            .to_string();

        // first cancel succeeds (iter render again)
        let r = req_for(
            acct,
            "DELETE",
            &format!("/virtualclusters/{vcid}/jobruns/{job_id}"),
            json!({}),
        );
        let wire = body_of(&dispatch(&r).unwrap());
        assert_eq!(wire["state"], "CANCELLED");
        // iter bug carries through cancel too
        assert_eq!(wire["configurationOverrides"], "emr-7.1.0-latest");
        assert_eq!(wire["stateDetails"], "JobRun CANCELLED successfully.");
        assert_eq!(
            wire["finishedAt"],
            format!("{}T00:01:00Z", chrono::Utc::now().date_naive())
        );

        // second cancel: not in a cancellable state
        let r = req_for(
            acct,
            "DELETE",
            &format!("/virtualclusters/{vcid}/jobruns/{job_id}"),
            json!({}),
        );
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 400);
        let b = body_of(&resp);
        assert_eq!(b["__type"], "ValidationException");
        assert_eq!(
            b["message"],
            format!("Job run {job_id} is not in a cancellable state")
        );
    }

    #[test]
    fn cancel_unknown_job() {
        let acct = scope();
        let r = req_for(
            acct,
            "DELETE",
            "/virtualclusters/none/jobruns/aaaaaaaaaaaaaaaaaaa",
            json!({}),
        );
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 400);
        let b = body_of(&resp);
        assert_eq!(b["__type"], "ResourceNotFoundException");
        assert_eq!(b["message"], "Job run aaaaaaaaaaaaaaaaaaa doesn't exist.");
    }

    #[test]
    fn invalid_job_run_short_id() {
        let acct = scope();
        // <19 alphanumeric chars → moto ValidationException
        for bad in ["short", "has-dash-000000000000000"] {
            let r = req_for(
                acct,
                "GET",
                &format!("/virtualclusters/none/jobruns/{bad}"),
                json!({}),
            );
            let resp = dispatch(&r).unwrap();
            assert_eq!(resp.status, 400, "{bad}");
            assert_eq!(body_of(&resp)["message"], "Invalid job run short id");
        }
    }

    #[test]
    fn list_job_runs_filters_and_pagination() {
        let acct = scope();
        let vc = make_cluster(acct, "vc", "ns-listjobs");
        let vcid = vc["id"].as_str().unwrap().to_string();
        for name in ["b", "a", "c"] {
            let r = req_for(
                acct,
                "POST",
                &format!("/virtualclusters/{vcid}/jobruns"),
                json!({"name": name, "releaseLabel": "emr-7.1.0-latest"}),
            );
            dispatch(&r).unwrap();
        }
        // other cluster's jobs stay out
        let other = make_cluster(acct, "other", "ns-listjobs-other");
        let other_id = other["id"].as_str().unwrap().to_string();
        let r = req_for(
            acct,
            "POST",
            &format!("/virtualclusters/{other_id}/jobruns"),
            json!({"name": "other", "releaseLabel": "emr-7.1.0-latest"}),
        );
        dispatch(&r).unwrap();

        // sorted by id, page of 2 → token "2"
        let url = format!("/virtualclusters/{vcid}/jobruns?maxResults=2");
        let r = req_for(acct, "GET", &url, json!({}));
        let resp = body_of(&dispatch(&r).unwrap());
        let runs = resp["jobRuns"].as_array().unwrap();
        assert_eq!(runs.len(), 2);
        assert_eq!(resp["nextToken"], json!("2"));
        let ids: Vec<&str> = runs.iter().map(|j| j["id"].as_str().unwrap()).collect();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted);

        let r = req_for(acct, "GET", &format!("{url}&nextToken=2"), json!({}));
        let resp = body_of(&dispatch(&r).unwrap());
        assert_eq!(resp["jobRuns"].as_array().unwrap().len(), 1);
        assert_eq!(resp["nextToken"], Value::Null);

        // moto's name filter: substring of the filter param (`job in name`).
        // "a", "b" and "c" are each substrings of "abcba" (the "other" job is
        // not); rows sort by random id, so compare as a sorted set.
        let r = req_for(
            acct,
            "GET",
            &format!("/virtualclusters/{vcid}/jobruns?name=abcba"),
            json!({}),
        );
        let resp = body_of(&dispatch(&r).unwrap());
        let mut names: Vec<&str> = resp["jobRuns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|j| j["name"].as_str().unwrap())
            .collect();
        names.sort_unstable();
        assert_eq!(names, vec!["a", "b", "c"]);

        // state filter
        let r = req_for(
            acct,
            "GET",
            &format!("/virtualclusters/{vcid}/jobruns?states=RUNNING&states=CANCELLED"),
            json!({}),
        );
        let resp = body_of(&dispatch(&r).unwrap());
        assert_eq!(resp["jobRuns"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn cluster_namespace_released_only_by_account() {
        let acct = scope();
        let other_acct = scope();
        make_cluster(acct, "vc", "ns-iso");
        // same namespace in a different account partition is fine
        let r = req_for(
            other_acct,
            "POST",
            "/virtualclusters",
            json!({"name": "vc", "containerProvider": provider("ns-iso"), "clientToken": "ct"}),
        );
        assert_eq!(dispatch(&r).unwrap().status, 200);
    }

    #[test]
    fn missing_container_provider_escapes_as_key_error() {
        let acct = scope();
        let r = req_for(
            acct,
            "POST",
            "/virtualclusters",
            json!({"name": "vc", "containerProvider": {"id": "eks"}}),
        );
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 500);
        let b = body_of(&resp);
        assert_eq!(b["__type"], "InternalError");
        assert_eq!(b["message"], "'info'");
    }

    #[test]
    fn unimplemented_botocore_surface_answers_501() {
        let acct = scope();
        let cases: &[(&str, &str, &str)] = &[
            ("POST", "/jobtemplates", "create_job_template"),
            ("GET", "/jobtemplates", "list_job_templates"),
            ("GET", "/jobtemplates/t-1", "describe_job_template"),
            ("DELETE", "/jobtemplates/t-1", "delete_job_template"),
            (
                "POST",
                "/securityconfigurations",
                "create_security_configuration",
            ),
            (
                "GET",
                "/securityconfigurations",
                "list_security_configurations",
            ),
            (
                "GET",
                "/securityconfigurations/sc-1",
                "describe_security_configuration",
            ),
            (
                "POST",
                "/virtualclusters/vc/endpoints",
                "create_managed_endpoint",
            ),
            (
                "GET",
                "/virtualclusters/vc/endpoints",
                "list_managed_endpoints",
            ),
            (
                "GET",
                "/virtualclusters/vc/endpoints/ep-1",
                "describe_managed_endpoint",
            ),
            (
                "DELETE",
                "/virtualclusters/vc/endpoints/ep-1",
                "delete_managed_endpoint",
            ),
            ("GET", "/tags/arn:aws:x", "list_tags_for_resource"),
        ];
        for (method, path, snake) in cases {
            let r = req_for(
                acct,
                method,
                path,
                if *method == "POST" {
                    json!({"name": "x"})
                } else {
                    json!({})
                },
            );
            let resp = dispatch(&r).unwrap();
            assert_eq!(resp.status, 501, "{method} {path}");
            let b = body_of(&resp);
            assert_eq!(b["__type"], "NotImplemented", "{method} {path}");
            assert_eq!(
                b["message"],
                format!("The {snake} action has not been implemented")
            );
        }
    }

    #[test]
    fn unhandled_method_path_answers_404() {
        let acct = scope();
        let r = req_for(acct, "PATCH", "/virtualclusters", json!({}));
        assert_eq!(dispatch(&r).unwrap().status, 404);
        let r = req_for(acct, "GET", "/virtualclusters/vc/unknown-child", json!({}));
        assert_eq!(dispatch(&r).unwrap().status, 404);
    }

    #[test]
    fn release_label_matcher_matches_moto() {
        for good in [
            "emr-6.10.0-latest",
            "emr-7.1.0-latest",
            "emr-7.0.0-20240101",
            "emr-6.9.0-20230101suffix",
            "emr-1.2.0-latestx",
        ] {
            assert!(release_label_matches(Some(good)), "{good}");
        }
        for bad in [
            None,
            Some(""),
            Some("latest"),
            Some("emr-"),
            Some("emr-71.0-latest"),    // first group is 2 digits
            Some("emr-7.123.0-latest"), // minor is 3 digits
            Some("emr-7.1.2-latest"),   // literal .0- missing
            Some("emr-7.1.0-2024010"),  // only 7 digits
            Some("emr-7.1.0-toofast"),
        ] {
            assert!(!release_label_matches(bad), "{bad:?}");
        }
    }

    #[test]
    fn paginated_list_boundaries() {
        let mk = |n: u32| {
            let mut v = Vec::new();
            for i in 0..n {
                v.push(json!({"id": format!("j{i:017}"), "name": "n"}));
            }
            v
        };
        // exact page → token equals the next offset (string on the wire)
        let (page, tok) = paginated_list(mk(3), "id", 2, None).unwrap();
        assert_eq!(page.len(), 2);
        assert_eq!(tok, json!("2"));
        // partial page → null token
        let (page, tok) = paginated_list(mk(3), "id", 2, Some("2")).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(tok, Value::Null);
        // token past the end → empty page, null token
        let (_, tok) = paginated_list(mk(3), "id", 2, Some("9")).unwrap();
        assert_eq!(tok, Value::Null);
        // empty list
        let (page, tok) = paginated_list(Vec::new(), "id", 100, None).unwrap();
        assert!(page.is_empty());
        assert_eq!(tok, Value::Null);
        // degenerate maxResults=0: moto's `len == maxResults` rule applies
        let (page, tok) = paginated_list(mk(2), "id", 0, None).unwrap();
        assert!(page.is_empty());
        assert_eq!(tok, json!("0"));
    }
}
