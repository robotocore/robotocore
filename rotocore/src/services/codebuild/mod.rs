//! AWS CodeBuild backend (JSON protocol, target `CodeBuild_20161006`).
//!
//! Port of the deployed moto fork's `codebuild` service
//! (`JackDanger/moto@2016563fd`, the wave's ground truth — no probe file
//! exists for this service): project CRUD, builds, build batches, webhooks,
//! source credentials, report groups/reports, fleets, resource policies,
//! curated images and the sandbox/command-execution stubs.
//!
//! Deliberate fork quirks kept:
//! - build metadata carries the fork's hardcoded CODECOMMIT source block and
//!   its `eu-west-2` deepLink (the fork never wires the project's source
//!   through);
//! - `DeleteProject` leaves build history behind — builds outlive their
//!   project on the fork too;
//! - reports have no producers, so report-management ops answer empties
//!   exactly like the fork;
//! - `DeleteBuildBatch` reports `statusCode` as the string `"200"`;
//! - a start-build `artifactsOverride` is stored verbatim (a JSON null
//!   otherwise rides along, like moto's None).

use crate::proto::{json_error, parse_json_body};
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use uuid::Uuid;

#[derive(Default)]
struct State {
    /// project name -> Project
    projects: Vec<(String, Project)>,
    /// project name -> [build ids] (insertion order, like the fork's dict)
    build_history: Vec<(String, Vec<String>)>,
    /// project name -> [build metadata dicts] (the authoritative build store)
    build_metadata_history: Vec<(String, Vec<Value>)>,
    /// project name -> webhook value
    webhooks: Vec<(String, Value)>,
    /// server type -> source credential
    source_credentials: Vec<(String, Value)>,
    /// report group name -> report group
    report_groups: Vec<(String, Value)>,
    /// report arn -> report (no producer in the fork: stays empty)
    reports: Vec<(String, Value)>,
    /// fleet arn -> fleet
    fleets: Vec<(String, Value)>,
    /// resource arn -> policy string
    resource_policies: Vec<(String, String)>,
    /// build batch id -> build batch
    build_batches: Vec<(String, Value)>,
    /// project name -> [build batch ids]
    build_batch_history: Vec<(String, Vec<String>)>,
    /// sandbox id -> sandbox
    sandboxes: Vec<(String, Value)>,
    /// project name -> [sandbox ids]
    sandbox_history: Vec<(String, Vec<String>)>,
    /// command execution id -> execution
    command_executions: Vec<(String, Value)>,
}

/// A stored project: the serialized metadata plus the fork's out-of-band
/// fields (the raw service role builds reuse, the visibility state).
struct Project {
    /// moto's `project_metadata` dict, serialized verbatim.
    md: Value,
    /// raw `serviceRole` input as given by the caller.
    service_role: String,
    /// PRIVATE or PUBLIC_READ (UpdateProjectVisibility).
    visibility: String,
    resource_access_role: Option<String>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("codebuild"));
    SLOT.clone()
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "codebuild" {
        return None;
    }
    let body = parse_json_body(&req.body);
    let slot = state();
    let mut guard = slot.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    Some(handle(req, st, &body))
}

fn handle(req: &Request, st: &mut State, p: &Value) -> Response {
    match req.operation.as_str() {
        // ---- Project operations ----
        "CreateProject" => op_create_project(st, req, p),
        "UpdateProject" => op_update_project(st, req, p),
        "UpdateProjectVisibility" => op_update_project_visibility(st, req, p),
        "DeleteProject" => op_delete_project(st, p),
        "InvalidateProjectCache" => op_invalidate_project_cache(st, req, p),
        "ListProjects" => op_list_projects(st),
        "BatchGetProjects" => op_batch_get_projects(st, p),
        "ListSharedProjects" => op_list_shared_projects(),

        // ---- Build operations ----
        "StartBuild" => op_start_build(st, req, p),
        "RetryBuild" => op_retry_build(st, req, p),
        "BatchGetBuilds" => op_batch_get_builds(st, p),
        "BatchDeleteBuilds" => op_batch_delete_builds(st, p),
        "ListBuilds" => op_list_builds(st),
        "ListBuildsForProject" => op_list_builds_for_project(st, req, p),
        "StopBuild" => op_stop_build(st, p),

        // ---- Build batch operations ----
        "StartBuildBatch" => op_start_build_batch(st, req, p),
        "StopBuildBatch" => op_stop_build_batch(st, p),
        "DeleteBuildBatch" => op_delete_build_batch(st, p),
        "RetryBuildBatch" => op_retry_build_batch(st, req, p),
        "ListBuildBatches" => op_list_build_batches(st),
        "ListBuildBatchesForProject" => op_list_build_batches_for_project(st, req, p),
        "BatchGetBuildBatches" => op_batch_get_build_batches(st, p),

        // ---- Webhook operations ----
        "CreateWebhook" => op_create_webhook(st, req, p),
        "UpdateWebhook" => op_update_webhook(st, p),
        "DeleteWebhook" => op_delete_webhook(st, p),

        // ---- Source credential operations ----
        "ImportSourceCredentials" => op_import_source_credentials(st, req, p),
        "DeleteSourceCredentials" => op_delete_source_credentials(st, p),
        "ListSourceCredentials" => op_list_source_credentials(st),

        // ---- Report group operations ----
        "CreateReportGroup" => op_create_report_group(st, req, p),
        "UpdateReportGroup" => op_update_report_group(st, p),
        "DeleteReportGroup" => op_delete_report_group(st, p),
        "BatchGetReportGroups" => op_batch_get_report_groups(st, p),
        "ListReportGroups" => op_list_report_groups(st),
        "ListSharedReportGroups" => op_list_shared_report_groups(),

        // ---- Report operations ----
        "ListReports" => op_list_reports(st),
        "ListReportsForReportGroup" => op_list_reports_for_report_group(st, p),
        "DeleteReport" => op_delete_report(st, p),
        "BatchGetReports" => op_batch_get_reports(st, p),
        "DescribeTestCases" => op_describe_test_cases(p),
        "DescribeCodeCoverages" => op_describe_code_coverages(p),
        "GetReportGroupTrend" => op_get_report_group_trend(p),

        // ---- Fleet operations ----
        "CreateFleet" => op_create_fleet(st, req, p),
        "UpdateFleet" => op_update_fleet(st, p),
        "DeleteFleet" => op_delete_fleet(st, p),
        "BatchGetFleets" => op_batch_get_fleets(st, p),
        "ListFleets" => op_list_fleets(st),

        // ---- Resource policy operations ----
        "PutResourcePolicy" => op_put_resource_policy(st, p),
        "GetResourcePolicy" => op_get_resource_policy(st, p),
        "DeleteResourcePolicy" => op_delete_resource_policy(st, p),

        // ---- Curated environment images ----
        "ListCuratedEnvironmentImages" => op_list_curated_environment_images(),

        // ---- Sandbox operations ----
        "StartSandbox" => op_start_sandbox(st, req, p),
        "StopSandbox" => op_stop_sandbox(st, p),
        "BatchGetSandboxes" => op_batch_get_sandboxes(st, p),
        "ListSandboxes" => op_list_sandboxes(st),
        "ListSandboxesForProject" => op_list_sandboxes_for_project(st, p),
        "StartSandboxConnection" => op_start_sandbox_connection(st, req, p),

        // ---- Command execution operations ----
        "StartCommandExecution" => op_start_command_execution(st, p),
        "ListCommandExecutionsForSandbox" => op_list_command_executions_for_sandbox(st, p),
        "BatchGetCommandExecutions" => op_batch_get_command_executions(st, p),

        // Deny-by-default: only the fork's surface is ported.
        _ => not_implemented(req),
    }
}

// --- Shared helpers ---------------------------------------------------------

/// moto's `get_partition`.
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

/// moto's `iso_8601_datetime_with_milliseconds`: `%Y-%m-%dT%H:%M:%S.%f`
/// truncated to milliseconds + `Z` (e.g. `2026-10-19T12:00:00.123Z`).
fn iso_ms() -> String {
    let now = chrono::Utc::now();
    format!(
        "{}.{:03}Z",
        now.format("%Y-%m-%dT%H:%M:%S"),
        now.timestamp_subsec_millis()
    )
}

/// moto's `unix_time`: float seconds.
fn unix_now() -> f64 {
    (chrono::Utc::now().timestamp_millis() as f64) / 1000.0
}

/// Poor-man's `mock_random.randint(1, n)` (nanos-driven, inclusive on 1..=n).
fn rand_int_inclusive(n: u32) -> u32 {
    let nanos = chrono::Utc::now().timestamp_subsec_nanos();
    (nanos % n) + 1
}

/// moto's `mock_random.get_random_hex(n)`: n lowercase hex chars.
fn random_hex(n: usize) -> String {
    let mut s = String::new();
    while s.len() < n {
        s.push_str(&hex::encode(Uuid::new_v4().as_bytes()));
    }
    s.truncate(n);
    s
}

/// moto's `iso_8601_datetime_with_milliseconds(... + timedelta(minutes=1..5))`.
fn later_time_minutes(start: &str) -> String {
    let parsed = start
        .trim_end_matches('Z')
        .parse::<chrono::NaiveDateTime>()
        .ok()
        .map(|dt| dt.and_utc() + chrono::Duration::minutes(rand_int_inclusive(5) as i64));
    match parsed {
        Some(dt) => format!(
            "{}.{:03}Z",
            dt.format("%Y-%m-%dT%H:%M:%S"),
            dt.timestamp_subsec_millis()
        ),
        None => iso_ms(),
    }
}

/// Python truthiness for the values moto's handlers branch on.
fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// `_get_param(key)` for string params: `None` covers absent and JSON null.
fn s_opt<'a>(p: &'a Value, key: &str) -> Option<&'a str> {
    match p.get(key) {
        Some(Value::String(s)) => Some(s.as_str()),
        _ => None,
    }
}

/// `_get_param(key)` for array params: `None` covers absent/null/non-array.
fn array_opt<'a>(p: &'a Value, key: &str) -> Option<&'a Vec<Value>> {
    p.get(key).and_then(Value::as_array)
}

/// Python-dict-semantics map helpers over insertion-ordered `(key, value)`.
fn map_get<'a, T>(m: &'a [(String, T)], k: &str) -> Option<&'a T> {
    m.iter().find(|(key, _)| key == k).map(|(_, v)| v)
}

fn map_get_mut<'a, T>(m: &'a mut [(String, T)], k: &str) -> Option<&'a mut T> {
    m.iter_mut().find(|(key, _)| key == k).map(|(_, v)| v)
}

/// Python-dict semantics: overwriting keeps the original insertion position.
fn map_put<T>(m: &mut Vec<(String, T)>, k: String, v: T) {
    match m.iter_mut().find(|(key, _)| *key == k) {
        Some((_, slot)) => *slot = v,
        None => m.push((k, v)),
    }
}

fn map_remove<T>(m: &mut Vec<(String, T)>, k: &str) -> Option<T> {
    let pos = m.iter().position(|(key, _)| key == k)?;
    Some(m.remove(pos).1)
}

// --- Response helpers -------------------------------------------------------

fn ok(v: &Value) -> Response {
    Response::json_status(200, v)
}

fn empty_ok() -> Response {
    Response::json(200, b"{}".to_vec())
}

fn invalid_input(message: &str) -> Response {
    json_error(400, "InvalidInputException", message)
}

fn not_found(message: &str) -> Response {
    json_error(400, "ResourceNotFoundException", message)
}

fn already_exists(message: &str) -> Response {
    json_error(400, "ResourceAlreadyExistsException", message)
}

fn not_implemented(req: &Request) -> Response {
    json_error(
        501,
        "NotImplemented",
        &format!("The {} action has not been implemented", req.operation),
    )
}

// --- Validators (the fork's responses.py module functions) -------------------

/// `_validate_required_params_source`.
fn validate_source(source: Option<&Value>) -> Option<Response> {
    let src = source?;
    let kind = src.get("type").and_then(Value::as_str).unwrap_or("");
    if ![
        "BITBUCKET",
        "CODECOMMIT",
        "CODEPIPELINE",
        "GITHUB",
        "GITHUB_ENTERPRISE",
        "NO_SOURCE",
        "S3",
    ]
    .contains(&kind)
    {
        return Some(invalid_input("Invalid type provided: Project source type"));
    }
    match src.get("location") {
        None => Some(invalid_input("Project source location is required")),
        Some(Value::String(loc)) if loc.is_empty() => {
            Some(invalid_input("Project source location is required"))
        }
        Some(_) => None,
    }
}

/// `_validate_required_params_service_role`.
fn validate_service_role(account: &str, region: &str, service_role: &str) -> Option<Response> {
    let prefix = format!("arn:{}:iam::{account}:role/", partition(region));
    if !service_role.starts_with(&prefix) {
        return Some(invalid_input(
            "Invalid service role: Service role account ID does not match caller's account",
        ));
    }
    None
}

/// `_validate_required_params_artifacts`.
fn validate_artifacts(artifacts: Option<&Value>) -> Option<Response> {
    let artifacts = artifacts?;
    let kind = artifacts.get("type").and_then(Value::as_str).unwrap_or("");
    if !["CODEPIPELINE", "S3", "NO_ARTIFACTS"].contains(&kind) {
        return Some(invalid_input("Invalid type provided: Artifact type"));
    }
    if kind == "NO_ARTIFACTS" {
        // `if "location" in artifacts` — a present key of any shape counts.
        if artifacts
            .as_object()
            .map(|m| m.contains_key("location"))
            .unwrap_or(false)
        {
            return Some(invalid_input(
                "Invalid artifacts: artifact type NO_ARTIFACTS should have null location",
            ));
        }
        return None;
    }
    match artifacts.get("location") {
        None => Some(invalid_input("Project source location is required")),
        Some(Value::String(loc)) if loc.is_empty() => {
            Some(invalid_input("Project source location is required"))
        }
        Some(_) => None,
    }
}

/// `_validate_required_params_environment`.
fn validate_environment(environment: Option<&Value>) -> Option<Response> {
    let environment = environment?;
    let kind = environment
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("");
    if ![
        "WINDOWS_CONTAINER",
        "LINUX_CONTAINER",
        "LINUX_GPU_CONTAINER",
        "ARM_CONTAINER",
    ]
    .contains(&kind)
    {
        return Some(invalid_input(&format!("Invalid type provided: {kind}")));
    }
    let compute = environment
        .get("computeType")
        .and_then(Value::as_str)
        .unwrap_or("");
    if ![
        "BUILD_GENERAL1_SMALL",
        "BUILD_GENERAL1_MEDIUM",
        "BUILD_GENERAL1_LARGE",
        "BUILD_GENERAL1_2XLARGE",
    ]
    .contains(&compute)
    {
        return Some(invalid_input(&format!(
            "Invalid compute type provided: {compute}"
        )));
    }
    None
}

/// The punctuation set excluded from a project name's final character
/// (moto's `[^!£$%^&*()+=|?`¬{}@~#:;<>\\/\[\]]` class).
const FORBIDDEN_LAST: &[char] = &[
    '!', '£', '$', '%', '^', '&', '*', '(', ')', '+', '=', '|', '?', '`', '¬', '{', '}', '@', '~',
    '#', ':', ';', '<', '>', '\\', '/', '[', ']',
];

/// `_validate_required_params_project_name`: length guard plus regex
/// `^[A-Za-z]{1}.*[^!£$%^&*()+=|?`¬{}@~#:;<>\\/\[\]]$`.
fn validate_project_name(name: &str) -> Option<Response> {
    if name.chars().count() >= 150 {
        return Some(invalid_input(
            "Only alphanumeric characters, dash, and underscore are supported",
        ));
    }
    let chars: Vec<char> = name.chars().collect();
    // The regex needs two characters at minimum (a leading letter plus the
    // final class): a single-char name does not match and is rejected.
    let valid = chars.len() >= 2
        && chars[0].is_ascii_alphabetic()
        && !FORBIDDEN_LAST.contains(&chars[chars.len() - 1]);
    if !valid {
        return Some(invalid_input(
            "Only alphanumeric characters, dash, and underscore are supported",
        ));
    }
    None
}

/// `_validate_required_params_id` against the ids `ListBuilds` would return.
fn validate_build_id(build_id: Option<&str>, all_build_ids: &[String]) -> Option<Response> {
    // botocore enforces the required member, so a missing id never reaches
    // the server through the AWS SDK; answer the modeled shape anyway.
    let Some(id) = build_id else {
        return Some(invalid_input("Invalid build ID provided"));
    };
    if !id.contains(':') {
        return Some(invalid_input("Invalid build ID provided"));
    }
    if !all_build_ids.iter().any(|b| b == id) {
        return Some(not_found(&format!("Build {id} does not exist")));
    }
    None
}

fn project_arn(req: &Request, name: &str) -> String {
    format!(
        "arn:{}:codebuild:{}:{}:project/{name}",
        partition(&req.region),
        req.region,
        req.account
    )
}

// --- Metadata builders -------------------------------------------------------

/// moto's `CodeBuild.__init__` metadata construction.
fn project_metadata(req: &Request, p: &Value, name: &str, service_role: &str) -> Value {
    let region = &req.region;
    let account = req.account.to_string();
    let part = partition(region);
    let now = unix_now();
    let mut md = json!({
        "name": name,
        "arn": project_arn(req, name),
        "encryptionKey": format!("arn:{part}:kms:{region}:{account}:alias/aws/s3"),
        "serviceRole": if service_role.starts_with("arn:") {
            service_role.to_string()
        } else {
            format!("arn:{part}:iam::{account}:role/service-role/{service_role}")
        },
        "lastModifiedDate": now,
        "created": now,
        "badge": {"badgeEnabled": false},
        "environment": p.get("environment").cloned().unwrap_or(Value::Null),
        "artifacts": p.get("artifacts").cloned().unwrap_or(Value::Null),
        "source": p.get("source").cloned().unwrap_or(Value::Null),
        "cache": {"type": "NO_CACHE"},
        "timeoutInMinutes": 0,
        "queuedTimeoutInMinutes": 0,
        "tags": Value::Null,
    });
    if let Some(description) = s_opt(p, "description").filter(|d| !d.is_empty()) {
        md["description"] = json!(description);
    }
    md["timeoutInMinutes"] = json!(p
        .get("timeoutInMinutes")
        .and_then(Value::as_i64)
        .unwrap_or(0));
    md["queuedTimeoutInMinutes"] = json!(p
        .get("queuedTimeoutInMinutes")
        .and_then(Value::as_i64)
        .unwrap_or(0));
    if let Some(cache) = p.get("cache").filter(|v| truthy(v)) {
        md["cache"] = cache.clone();
    }
    if let Some(source_version) = s_opt(p, "sourceVersion").filter(|v| !v.is_empty()) {
        md["sourceVersion"] = json!(source_version);
    }
    if let Some(logs_config) = p.get("logsConfig").filter(|v| truthy(v)) {
        md["logsConfig"] = logs_config.clone();
    }
    if let Some(vpc_config) = p.get("vpcConfig").filter(|v| truthy(v)) {
        md["vpcConfig"] = vpc_config.clone();
    }
    // The fork always assigns tags (JSON null when the caller omits them).
    md["tags"] = p.get("tags").cloned().unwrap_or(Value::Null);
    md
}

/// moto's `CodeBuildProjectMetadata` construction.
fn build_metadata(
    req: &Request,
    project_name: &str,
    source_version: Option<&str>,
    artifacts_override: Option<&Value>,
    service_role: &str,
) -> (String, Value) {
    let region = &req.region;
    let account = req.account.to_string();
    let part = partition(region);
    let build_id = format!("{project_name}:{}", Uuid::new_v4());
    let now = iso_ms();
    let build = json!({
        "id": build_id,
        "arn": format!("arn:{part}:codebuild:{region}:{account}:build/{build_id}"),
        "buildNumber": rand_int_inclusive(100),
        "startTime": now,
        "currentPhase": "QUEUED",
        "buildStatus": "IN_PROGRESS",
        "sourceVersion": source_version.unwrap_or("refs/heads/main"),
        "projectName": project_name,
        "phases": [
            {
                "phaseType": "SUBMITTED",
                "phaseStatus": "SUCCEEDED",
                "startTime": now,
                "endTime": now,
                "durationInSeconds": 0,
            },
            {"phaseType": "QUEUED", "startTime": now},
        ],
        // moto hardcodes this source block in build metadata.
        "source": {
            "type": "CODECOMMIT",
            "location": "https://git-codecommit.eu-west-2.amazonaws.com/v1/repos/testing",
            "gitCloneDepth": 1,
            "gitSubmodulesConfig": {"fetchSubmodules": false},
            "buildspec": "buildspec/stuff.yaml",
            "insecureSsl": false,
        },
        "secondarySources": [],
        "secondarySourceVersions": [],
        "artifacts": artifacts_override.cloned().unwrap_or(Value::Null),
        "secondaryArtifacts": [],
        "cache": {"type": "NO_CACHE"},
        "environment": {
            "type": "LINUX_CONTAINER",
            "image": "aws/codebuild/amazonlinux2-x86_64-standard:3.0",
            "computeType": "BUILD_GENERAL1_SMALL",
            "environmentVariables": [],
            "privilegedMode": false,
            "imagePullCredentialsType": "CODEBUILD",
        },
        "serviceRole": service_role,
        "logs": {
            // moto hardcodes the deepLink region (eu-west-2).
            "deepLink": "https://console.aws.amazon.com/cloudwatch/home?region=eu-west-2#logEvent:group=null;stream=null",
            "cloudWatchLogsArn": format!(
                "arn:{part}:logs:{region}:{account}:log-group:null:log-stream:null"
            ),
            "cloudWatchLogs": {"status": "ENABLED"},
            "s3Logs": {"status": "DISABLED", "encryptionDisabled": false},
        },
        "timeoutInMinutes": 45,
        "queuedTimeoutInMinutes": 480,
        "buildComplete": false,
        "initiator": "rootme",
        "encryptionKey": format!("arn:{part}:kms:{region}:{account}:alias/aws/s3"),
    });
    (build_id, build)
}

/// moto's `_set_phases`: mark an existing QUEUED phase SUCCEEDED, then
/// append the remaining phase sequence with per-phase random durations.
fn extend_phases(phases: &mut Vec<Value>) {
    let now = iso_ms();
    for status in [
        "PROVISIONING",
        "DOWNLOAD_SOURCE",
        "INSTALL",
        "PRE_BUILD",
        "BUILD",
        "POST_BUILD",
        "UPLOAD_ARTIFACTS",
        "FINALIZING",
        "COMPLETED",
    ] {
        phases.push(json!({
            "phaseType": status,
            "phaseStatus": "SUCCEEDED",
            "startTime": now,
            "endTime": now,
            "durationInSeconds": rand_int_inclusive(91) + 9,
        }));
    }
}

/// The shared completion mutation used by `StopBuild` and `BatchGetBuilds`.
fn complete_build(build: &mut Value, new_status: &str) {
    if let Some(phases) = build.get_mut("phases").and_then(Value::as_array_mut) {
        for phase in phases.iter_mut() {
            if phase.get("phaseType").and_then(Value::as_str) == Some("QUEUED") {
                phase["phaseStatus"] = json!("SUCCEEDED");
            }
        }
        extend_phases(phases);
    }
    let start = build.get("startTime").and_then(Value::as_str).unwrap_or("");
    build["endTime"] = json!(later_time_minutes(start));
    build["currentPhase"] = json!("COMPLETED");
    build["buildStatus"] = json!(new_status);
}

/// moto's `BuildBatch.metadata` construction.
#[allow(clippy::too_many_arguments)]
fn build_batch_value(
    req: &Request,
    project_name: &str,
    build_batch_id: &str,
    service_role: &str,
    source_version: Option<&str>,
) -> Value {
    let region = &req.region;
    let account = req.account.to_string();
    let part = partition(region);
    let now = iso_ms();
    json!({
        "id": build_batch_id,
        "arn": format!("arn:{part}:codebuild:{region}:{account}:build-batch/{build_batch_id}"),
        "startTime": now,
        "currentPhase": "SUBMITTED",
        "buildBatchStatus": "IN_PROGRESS",
        "projectName": project_name,
        "sourceVersion": source_version.unwrap_or("refs/heads/main"),
        "phases": [{
            "phaseType": "SUBMITTED",
            "phaseStatus": "SUCCEEDED",
            "startTime": now,
            "endTime": now,
            "durationInSeconds": 0,
        }],
        "source": {"type": "NO_SOURCE"},
        "artifacts": {"type": "NO_ARTIFACTS"},
        "cache": {"type": "NO_CACHE"},
        "environment": {
            "type": "LINUX_CONTAINER",
            "image": "aws/codebuild/amazonlinux2-x86_64-standard:3.0",
            "computeType": "BUILD_GENERAL1_SMALL",
            "environmentVariables": [],
            "privilegedMode": false,
            "imagePullCredentialsType": "CODEBUILD",
        },
        "serviceRole": service_role,
        "encryptionKey": format!("arn:{part}:kms:{region}:{account}:alias/aws/s3"),
        "buildGroups": [],
        "buildBatchNumber": rand_int_inclusive(100),
        "complete": false,
        "initiator": "rootme",
    })
}

/// moto's `Webhook.to_dict`.
fn webhook_value(req: &Request, project_name: &str, p: &Value) -> Value {
    json!({
        "url": format!(
            "https://codebuild.{}.amazonaws.com/webhooks/{project_name}",
            req.region
        ),
        "payloadUrl": format!(
            "https://codebuild.{}.amazonaws.com/webhook-trigger/{}",
            req.region,
            Uuid::new_v4()
        ),
        "secret": random_hex(40),
        "branchFilter": p.get("branchFilter").and_then(Value::as_str).unwrap_or(""),
        "filterGroups": p.get("filterGroups").cloned().unwrap_or(json!([])),
        "buildType": p.get("buildType").and_then(Value::as_str).unwrap_or("BUILD"),
        "lastModifiedSecret": unix_now(),
    })
}

/// moto's `ReportGroup` construction (`to_dict` shape).
fn report_group_value(
    req: &Request,
    name: &str,
    report_type: &str,
    export_config: &Value,
    tags: Option<&Value>,
) -> Value {
    json!({
        "arn": format!(
            "arn:{}:codebuild:{}:{}:report-group/{name}",
            partition(&req.region),
            req.region,
            req.account
        ),
        "name": name,
        "type": report_type,
        "exportConfig": export_config,
        "created": unix_now(),
        "lastModified": unix_now(),
        "tags": tags.cloned().unwrap_or(json!([])),
        "status": "ACTIVE",
    })
}

/// moto's `Fleet` construction (`to_dict` shape).
fn fleet_value(req: &Request, p: &Value, name: &str) -> Value {
    let mut fleet = json!({
        "arn": format!(
            "arn:{}:codebuild:{}:{}:fleet/{name}:{}",
            partition(&req.region),
            req.region,
            req.account,
            Uuid::new_v4()
        ),
        "name": name,
        "id": Uuid::new_v4().to_string(),
        "created": unix_now(),
        "lastModified": unix_now(),
        "status": {"statusCode": "ACTIVE", "context": "FLEET_ACTIVE"},
        "baseCapacity": p.get("baseCapacity").cloned().unwrap_or(Value::Null),
        "environmentType": p.get("environmentType").cloned().unwrap_or(Value::Null),
        "computeType": p.get("computeType").cloned().unwrap_or(Value::Null),
        "overflowBehavior": p.get("overflowBehavior").cloned().unwrap_or(json!("QUEUE")),
        "tags": p.get("tags").cloned().unwrap_or(json!([])),
    });
    for key in [
        "scalingConfiguration",
        "computeConfiguration",
        "vpcConfig",
        "proxyConfiguration",
        "imageId",
        "fleetServiceRole",
    ] {
        if let Some(v) = p.get(key).filter(|v| !v.is_null()) {
            fleet[key] = v.clone();
        }
    }
    fleet
}

/// moto's `Sandbox.metadata` construction.
fn sandbox_value(req: &Request, project_name: Option<&str>, sandbox_id: &str) -> Value {
    json!({
        "id": sandbox_id,
        "arn": format!(
            "arn:{}:codebuild:{}:{}:sandbox/{sandbox_id}",
            partition(&req.region),
            req.region,
            req.account
        ),
        "projectName": project_name.unwrap_or(""),
        "startTime": iso_ms(),
        "status": "RUNNING",
    })
}

/// moto's `list_curated_environment_images` static list.
fn op_list_curated_environment_images() -> Response {
    ok(&json!({"platforms": [{
        "languageName": "DOCKER",
        "images": [
            {
                "name": "aws/codebuild/amazonlinux2-x86_64-standard:3.0",
                "description": "Amazon Linux 2 standard image 3.0",
            },
            {
                "name": "aws/codebuild/amazonlinux2-x86_64-standard:4.0",
                "description": "Amazon Linux 2 standard image 4.0",
            },
        ],
    }]}))
}

// --- Project operations ------------------------------------------------------

fn op_create_project(st: &mut State, req: &Request, p: &Value) -> Response {
    if let Some(err) = validate_source(p.get("source")) {
        return err;
    }
    let account = req.account.to_string();
    let service_role = p
        .get("serviceRole")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if let Some(err) = validate_service_role(&account, &req.region, service_role) {
        return err;
    }
    if let Some(err) = validate_artifacts(p.get("artifacts")) {
        return err;
    }
    if let Some(err) = validate_environment(p.get("environment")) {
        return err;
    }
    let name = p.get("name").and_then(Value::as_str).unwrap_or("");
    if let Some(err) = validate_project_name(name) {
        return err;
    }
    if map_get(&st.projects, name).is_some() {
        return already_exists(&format!(
            "Project already exists: arn:{}:codebuild:{}:{account}:project/{name}",
            partition(&req.region),
            req.region
        ));
    }
    let project = Project {
        md: project_metadata(req, p, name, service_role),
        service_role: service_role.to_string(),
        visibility: "PRIVATE".into(),
        resource_access_role: None,
    };
    let md = project.md.clone();
    map_put(&mut st.projects, name.to_string(), project);
    map_put(&mut st.build_history, name.to_string(), vec![]);
    map_put(&mut st.build_metadata_history, name.to_string(), vec![]);
    ok(&json!({"project": md}))
}

fn op_update_project(st: &mut State, req: &Request, p: &Value) -> Response {
    let name = s_opt(p, "name").unwrap_or("");
    if name.is_empty() {
        return invalid_input("Project name is required");
    }
    if let Some(err) = validate_project_name(name) {
        return err;
    }
    // Field validation runs only when the caller passed the field (python
    // truthiness); the backend then applies `is not None` fields.
    if let Some(source) = p.get("source").filter(|v| truthy(v)) {
        if let Some(err) = validate_source(Some(source)) {
            return err;
        }
    }
    if let Some(service_role) = p
        .get("serviceRole")
        .filter(|v| truthy(v))
        .and_then(Value::as_str)
    {
        if let Some(err) =
            validate_service_role(&req.account.to_string(), &req.region, service_role)
        {
            return err;
        }
    }
    if let Some(err) = validate_artifacts(p.get("artifacts").filter(|v| truthy(v))) {
        return err;
    }
    if let Some(err) = validate_environment(p.get("environment").filter(|v| truthy(v))) {
        return err;
    }
    let Some(project) = map_get_mut(&mut st.projects, name) else {
        return not_found(&format!(
            "The provided project arn:{}:codebuild:{}:{}:project/{name} does not exist",
            partition(&req.region),
            req.region,
            req.account
        ));
    };
    let md = &mut project.md;
    if let Some(description) = p.get("description").filter(|v| !v.is_null()) {
        md["description"] = description.clone();
    }
    if let Some(source) = p.get("source").filter(|v| !v.is_null()) {
        md["source"] = source.clone();
    }
    if let Some(artifacts) = p.get("artifacts").filter(|v| !v.is_null()) {
        md["artifacts"] = artifacts.clone();
    }
    if let Some(environment) = p.get("environment").filter(|v| !v.is_null()) {
        md["environment"] = environment.clone();
    }
    if let Some(service_role) = p.get("serviceRole").filter(|v| !v.is_null()) {
        let raw = service_role.as_str().unwrap_or("").to_string();
        project.service_role = raw.clone();
        md["serviceRole"] = if raw.starts_with("arn:") {
            json!(raw)
        } else {
            json!(format!(
                "arn:{}:iam::{}:role/service-role/{raw}",
                partition(&req.region),
                req.account
            ))
        };
    }
    for key in [
        "tags",
        "cache",
        "timeoutInMinutes",
        "queuedTimeoutInMinutes",
        "sourceVersion",
        "logsConfig",
        "vpcConfig",
        "encryptionKey",
    ] {
        if let Some(v) = p.get(key).filter(|v| !v.is_null()) {
            md[key] = v.clone();
        }
    }
    if let Some(badge) = p.get("badgeEnabled").filter(|v| !v.is_null()) {
        md["badge"]["badgeEnabled"] = badge.clone();
    }
    md["lastModifiedDate"] = json!(unix_now());
    let md = md.clone();
    ok(&json!({"project": md}))
}

fn op_update_project_visibility(st: &mut State, req: &Request, p: &Value) -> Response {
    let arn = s_opt(p, "projectArn").unwrap_or("");
    if arn.is_empty() {
        return invalid_input("Project ARN is required");
    }
    let visibility = s_opt(p, "projectVisibility").unwrap_or("");
    if visibility.is_empty() {
        return invalid_input("Project visibility is required");
    }
    let resource_access_role = s_opt(p, "resourceAccessRole").map(String::from);
    let Some(project) = st
        .projects
        .iter_mut()
        .find(|(_, prj)| prj.md.get("arn").and_then(Value::as_str) == Some(arn))
        .map(|(_, prj)| prj)
    else {
        return not_found(&format!("Project {arn} does not exist"));
    };
    project.visibility = visibility.to_string();
    if let Some(role) = resource_access_role {
        project.resource_access_role = Some(role);
    }
    let public_alias = if visibility == "PUBLIC_READ" {
        format!(
            "{}/{}",
            req.account,
            project.md["name"].as_str().unwrap_or_default()
        )
    } else {
        String::new()
    };
    ok(&json!({
        "projectArn": arn,
        "publicProjectAlias": public_alias,
        "projectVisibility": visibility,
    }))
}

fn op_delete_project(st: &mut State, p: &Value) -> Response {
    let name = p.get("name").and_then(Value::as_str).unwrap_or("");
    if let Some(err) = validate_project_name(name) {
        return err;
    }
    map_remove(&mut st.projects, name);
    map_remove(&mut st.webhooks, name);
    // The fork leaves build history behind — builds outlive their project.
    empty_ok()
}

fn op_invalidate_project_cache(st: &State, req: &Request, p: &Value) -> Response {
    let project_name = p.get("projectName").and_then(Value::as_str).unwrap_or("");
    if project_name.is_empty() {
        return invalid_input("Project name is required");
    }
    if let Some(err) = validate_project_name(project_name) {
        return err;
    }
    if map_get(&st.projects, project_name).is_none() {
        return not_found(&format!(
            "The provided project arn:{}:codebuild:{}:{}:project/{project_name} does not exist",
            partition(&req.region),
            req.region,
            req.account
        ));
    }
    empty_ok()
}

fn op_list_projects(st: &State) -> Response {
    let names: Vec<&str> = st.projects.iter().map(|(n, _)| n.as_str()).collect();
    ok(&json!({"projects": names}))
}

fn op_batch_get_projects(st: &State, p: &Value) -> Response {
    let mut found: Vec<Value> = Vec::new();
    for name in array_opt(p, "names").into_iter().flatten() {
        let Some(name) = name.as_str() else {
            continue;
        };
        if let Some(project) = map_get(&st.projects, name) {
            found.push(project.md.clone());
        } else if name.starts_with("arn:") {
            for (_, project) in st.projects.iter() {
                if project.md.get("arn").and_then(Value::as_str) == Some(name) {
                    found.push(project.md.clone());
                }
            }
        }
    }
    ok(&json!({"projects": found}))
}

fn op_list_shared_projects() -> Response {
    // Shared projects require RAM sharing, which the fork does not implement.
    ok(&json!({"projects": []}))
}

/// `ListSharedReportGroups`: no RAM sharing on the fork — empty list.
fn op_list_shared_report_groups() -> Response {
    ok(&json!({"reportGroups": []}))
}

// --- Build operations --------------------------------------------------------

fn op_start_build(st: &mut State, req: &Request, p: &Value) -> Response {
    let project_name = p.get("projectName").and_then(Value::as_str).unwrap_or("");
    if let Some(err) = validate_project_name(project_name) {
        return err;
    }
    start_build_inner(
        st,
        req,
        project_name,
        s_opt(p, "sourceVersion"),
        p.get("artifactsOverride").filter(|v| !v.is_null()),
    )
}

fn op_retry_build(st: &mut State, req: &Request, p: &Value) -> Response {
    let Some(build_id) = s_opt(p, "id") else {
        return invalid_input("Build ID is required");
    };
    let mut original: Option<Value> = None;
    for (_, list) in st.build_metadata_history.iter_mut() {
        for build in list.iter() {
            if build.get("id").and_then(Value::as_str) == Some(build_id) {
                original = Some(build.clone());
                break;
            }
        }
        if original.is_some() {
            break;
        }
    }
    let Some(original) = original else {
        return not_found(&format!("Build {build_id} does not exist"));
    };
    let project_name = original["projectName"].as_str().unwrap_or("").to_string();
    let source_version = original
        .get("sourceVersion")
        .and_then(Value::as_str)
        .map(String::from);
    start_build_inner(st, req, &project_name, source_version.as_deref(), None)
}

/// The `start_build` backend shared by `StartBuild` and `RetryBuild`.
fn start_build_inner(
    st: &mut State,
    req: &Request,
    project_name: &str,
    source_version: Option<&str>,
    artifacts_override: Option<&Value>,
) -> Response {
    let service_role = match map_get(&st.projects, project_name) {
        Some(project) => project.service_role.clone(),
        None => {
            return not_found(&format!(
                "Project cannot be found: arn:{}:codebuild:{}:{}:project/{project_name}",
                partition(&req.region),
                req.region,
                req.account
            ))
        }
    };
    let (build_id, build) = build_metadata(
        req,
        project_name,
        source_version,
        artifacts_override,
        &service_role,
    );
    if let Some(history) = map_get_mut(&mut st.build_history, project_name) {
        history.push(build_id);
    }
    if let Some(history) = map_get_mut(&mut st.build_metadata_history, project_name) {
        history.push(build.clone());
    }
    ok(&json!({"build": build}))
}

fn op_batch_get_builds(st: &mut State, p: &Value) -> Response {
    let ids: Vec<String> = array_opt(p, "ids")
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(String::from)
        .collect();
    // The response handler rejects every id without the `name:uuid` shape.
    if ids.iter().any(|id| !id.contains(':')) {
        return invalid_input("Invalid build ID provided");
    }
    let mut found: Vec<Value> = Vec::new();
    for (_, list) in st.build_metadata_history.iter_mut() {
        for build in list.iter_mut() {
            if let Some(id) = build.get("id").and_then(Value::as_str) {
                if ids.iter().any(|i| i == id) {
                    // The fork mutates the stored build in place; mirror it.
                    complete_build(build, "SUCCEEDED");
                    found.push(build.clone());
                }
            }
        }
    }
    ok(&json!({"builds": found}))
}

fn op_batch_delete_builds(st: &mut State, p: &Value) -> Response {
    let Some(ids) = array_opt(p, "ids").filter(|ids| !ids.is_empty()) else {
        return invalid_input("Build IDs are required");
    };
    let mut deleted: Vec<String> = Vec::new();
    let mut not_deleted: Vec<Value> = Vec::new();
    for id in ids {
        let Some(build_id) = id.as_str() else {
            not_deleted.push(json!({"id": id, "statusCode": "BUILD_NOT_FOUND"}));
            continue;
        };
        let mut found = false;
        for (project_name, list) in st.build_metadata_history.iter_mut() {
            if let Some(pos) = list
                .iter()
                .position(|b| b.get("id").and_then(Value::as_str) == Some(build_id))
            {
                list.remove(pos);
                if let Some(history) = map_get_mut(&mut st.build_history, project_name) {
                    if let Some(i) = history.iter().position(|b| b == build_id) {
                        history.remove(i);
                    }
                }
                deleted.push(build_id.to_string());
                found = true;
                break;
            }
        }
        if !found {
            not_deleted.push(json!({"id": build_id, "statusCode": "BUILD_NOT_FOUND"}));
        }
    }
    ok(&json!({"buildsDeleted": deleted, "buildsNotDeleted": not_deleted}))
}

fn op_list_builds(st: &State) -> Response {
    let ids: Vec<String> = st
        .build_history
        .iter()
        .flat_map(|(_, list)| list.iter().cloned())
        .collect();
    ok(&json!({"ids": ids}))
}

fn op_list_builds_for_project(st: &State, req: &Request, p: &Value) -> Response {
    let project_name = p.get("projectName").and_then(Value::as_str).unwrap_or("");
    if let Some(err) = validate_project_name(project_name) {
        return err;
    }
    if map_get(&st.projects, project_name).is_none() {
        return not_found(&format!(
            "The provided project arn:{}:codebuild:{}:{}:project/{project_name} does not exist",
            partition(&req.region),
            req.region,
            req.account
        ));
    }
    let ids = map_get(&st.build_history, project_name)
        .cloned()
        .unwrap_or_default();
    ok(&json!({"ids": ids}))
}

fn op_stop_build(st: &mut State, p: &Value) -> Response {
    let all_build_ids: Vec<String> = st
        .build_history
        .iter()
        .flat_map(|(_, list)| list.iter().cloned())
        .collect();
    if let Some(err) = validate_build_id(s_opt(p, "id"), &all_build_ids) {
        return err;
    }
    let build_id = s_opt(p, "id").unwrap_or_default();
    for (_, list) in st.build_metadata_history.iter_mut() {
        for build in list.iter_mut() {
            if build.get("id").and_then(Value::as_str) == Some(build_id) {
                complete_build(build, "STOPPED");
                return ok(&json!({"build": build}));
            }
        }
    }
    not_found(&format!("Build {build_id} does not exist"))
}

// --- Build batch operations ---------------------------------------------------

fn op_start_build_batch(st: &mut State, req: &Request, p: &Value) -> Response {
    let project_name = p.get("projectName").and_then(Value::as_str).unwrap_or("");
    if project_name.is_empty() {
        return invalid_input("Project name is required");
    }
    if let Some(err) = validate_project_name(project_name) {
        return err;
    }
    start_build_batch_inner(st, req, project_name, s_opt(p, "sourceVersion"))
}

/// The `start_build_batch` backend shared by `StartBuildBatch` and
/// `RetryBuildBatch` (which passes no sourceVersion, like the fork).
fn start_build_batch_inner(
    st: &mut State,
    req: &Request,
    project_name: &str,
    source_version: Option<&str>,
) -> Response {
    let service_role = match map_get(&st.projects, project_name) {
        Some(project) => project.service_role.clone(),
        None => {
            return not_found(&format!(
                "Project cannot be found: arn:{}:codebuild:{}:{}:project/{project_name}",
                partition(&req.region),
                req.region,
                req.account
            ))
        }
    };
    let batch_id = format!("{project_name}:{}", Uuid::new_v4());
    let metadata = build_batch_value(req, project_name, &batch_id, &service_role, source_version);
    map_put(&mut st.build_batches, batch_id.clone(), metadata.clone());
    match map_get_mut(&mut st.build_batch_history, project_name) {
        Some(history) => history.push(batch_id),
        None => map_put(
            &mut st.build_batch_history,
            project_name.to_string(),
            vec![batch_id],
        ),
    }
    ok(&json!({"buildBatch": metadata}))
}

fn op_stop_build_batch(st: &mut State, p: &Value) -> Response {
    let Some(batch_id) = s_opt(p, "id") else {
        return invalid_input("Build batch ID is required");
    };
    let Some(batch) = map_get_mut(&mut st.build_batches, batch_id) else {
        return not_found(&format!("Build batch {batch_id} does not exist"));
    };
    batch["buildBatchStatus"] = json!("STOPPED");
    batch["currentPhase"] = json!("STOPPED");
    batch["endTime"] = json!(iso_ms());
    batch["complete"] = json!(true);
    ok(&json!({"buildBatch": batch}))
}

fn op_delete_build_batch(st: &mut State, p: &Value) -> Response {
    let Some(batch_id) = s_opt(p, "id") else {
        return invalid_input("Build batch ID is required");
    };
    let Some(batch) = map_remove(&mut st.build_batches, batch_id) else {
        return not_found(&format!("Build batch {batch_id} does not exist"));
    };
    let project_name = batch["projectName"].as_str().unwrap_or("").to_string();
    if let Some(history) = map_get_mut(&mut st.build_batch_history, &project_name) {
        if let Some(i) = history.iter().position(|b| b == batch_id) {
            history.remove(i);
        }
    }
    ok(&json!({
        "statusCode": "200",
        "buildsDeleted": [batch_id],
        "buildsNotDeleted": [],
    }))
}

fn op_retry_build_batch(st: &mut State, req: &Request, p: &Value) -> Response {
    let Some(batch_id) = s_opt(p, "id") else {
        return invalid_input("Build batch ID is required");
    };
    let Some(batch) = map_get(&st.build_batches, batch_id) else {
        return not_found(&format!("Build batch {batch_id} does not exist"));
    };
    let project_name = batch["projectName"].as_str().unwrap_or("").to_string();
    start_build_batch_inner(st, req, &project_name, None)
}

fn op_list_build_batches(st: &State) -> Response {
    let ids: Vec<&str> = st.build_batches.iter().map(|(id, _)| id.as_str()).collect();
    ok(&json!({"ids": ids}))
}

fn op_list_build_batches_for_project(st: &State, req: &Request, p: &Value) -> Response {
    let ids: Vec<String> = match s_opt(p, "projectName") {
        Some(name) => {
            if map_get(&st.projects, name).is_none() {
                return not_found(&format!(
                    "The provided project arn:{}:codebuild:{}:{}:project/{name} does not exist",
                    partition(&req.region),
                    req.region,
                    req.account
                ));
            }
            map_get(&st.build_batch_history, name)
                .cloned()
                .unwrap_or_default()
        }
        None => st.build_batches.iter().map(|(id, _)| id.clone()).collect(),
    };
    ok(&json!({"ids": ids}))
}

fn op_batch_get_build_batches(st: &State, p: &Value) -> Response {
    let Some(ids) = array_opt(p, "ids").filter(|ids| !ids.is_empty()) else {
        return invalid_input("Build batch IDs are required");
    };
    let mut found: Vec<Value> = Vec::new();
    let mut missing: Vec<&str> = Vec::new();
    for id in ids {
        let Some(id) = id.as_str() else {
            continue;
        };
        match map_get(&st.build_batches, id) {
            Some(batch) => found.push(batch.clone()),
            None => missing.push(id),
        }
    }
    ok(&json!({"buildBatches": found, "buildBatchesNotFound": missing}))
}

// --- Webhook operations -------------------------------------------------------

fn op_create_webhook(st: &mut State, req: &Request, p: &Value) -> Response {
    let project_name = p.get("projectName").and_then(Value::as_str).unwrap_or("");
    if project_name.is_empty() {
        return invalid_input("Project name is required");
    }
    if map_get(&st.projects, project_name).is_none() {
        return not_found(&format!(
            "The provided project arn:{}:codebuild:{}:{}:project/{project_name} does not exist",
            partition(&req.region),
            req.region,
            req.account
        ));
    }
    if map_get(&st.webhooks, project_name).is_some() {
        return already_exists(&format!(
            "Webhook already exists for project: {project_name}"
        ));
    }
    let webhook = webhook_value(req, project_name, p);
    map_put(&mut st.webhooks, project_name.to_string(), webhook.clone());
    ok(&json!({"webhook": webhook}))
}

fn op_update_webhook(st: &mut State, p: &Value) -> Response {
    let project_name = p.get("projectName").and_then(Value::as_str).unwrap_or("");
    if project_name.is_empty() {
        return invalid_input("Project name is required");
    }
    let Some(webhook) = map_get_mut(&mut st.webhooks, project_name) else {
        return not_found(&format!("No webhook found for project: {project_name}"));
    };
    for key in ["branchFilter", "filterGroups", "buildType"] {
        if let Some(v) = p.get(key).filter(|v| !v.is_null()) {
            webhook[key] = v.clone();
        }
    }
    if p.get("rotateSecret")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        webhook["secret"] = json!(random_hex(40));
        webhook["lastModifiedSecret"] = json!(unix_now());
    }
    ok(&json!({"webhook": webhook}))
}

fn op_delete_webhook(st: &mut State, p: &Value) -> Response {
    let project_name = p.get("projectName").and_then(Value::as_str).unwrap_or("");
    if project_name.is_empty() {
        return invalid_input("Project name is required");
    }
    if map_remove(&mut st.webhooks, project_name).is_none() {
        return not_found(&format!("No webhook found for project: {project_name}"));
    }
    empty_ok()
}

// --- Source credential operations ----------------------------------------------

fn op_import_source_credentials(st: &mut State, req: &Request, p: &Value) -> Response {
    let Some(token) = s_opt(p, "token") else {
        return invalid_input("Token is required");
    };
    let Some(server_type) = s_opt(p, "serverType") else {
        return invalid_input("Server type is required");
    };
    let Some(auth_type) = s_opt(p, "authType") else {
        return invalid_input("Auth type is required");
    };
    let username = s_opt(p, "username").map(String::from);
    let should_overwrite = p
        .get("shouldOverwrite")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if map_get(&st.source_credentials, server_type).is_some() && !should_overwrite {
        return already_exists(&format!(
            "Source credentials for type {server_type} already exist"
        ));
    }
    let cred = json!({
        "arn": format!(
            "arn:{}:codebuild:{}:{}:token/{server_type}",
            partition(&req.region),
            req.region,
            req.account
        ),
        "token": token,
        "serverType": server_type,
        "authType": auth_type,
        "username": username,
    });
    map_put(
        &mut st.source_credentials,
        server_type.to_string(),
        cred.clone(),
    );
    ok(&json!({"arn": cred["arn"]}))
}

fn op_delete_source_credentials(st: &mut State, p: &Value) -> Response {
    let Some(arn) = s_opt(p, "arn") else {
        return invalid_input("ARN is required");
    };
    let pos = st
        .source_credentials
        .iter()
        .position(|(_, cred)| cred.get("arn").and_then(Value::as_str) == Some(arn));
    match pos {
        Some(i) => {
            st.source_credentials.remove(i);
            ok(&json!({"arn": arn}))
        }
        None => not_found(&format!("Source credentials {arn} not found")),
    }
}

fn op_list_source_credentials(st: &State) -> Response {
    let infos: Vec<Value> = st
        .source_credentials
        .iter()
        .map(|(_, cred)| {
            let mut info = json!({
                "arn": cred["arn"],
                "serverType": cred["serverType"],
                "authType": cred["authType"],
            });
            if let Some(username) = cred.get("username").filter(|v| !v.is_null()) {
                info["username"] = username.clone();
            }
            info
        })
        .collect();
    ok(&json!({"sourceCredentialsInfos": infos}))
}

// --- Report group operations ----------------------------------------------------

fn op_create_report_group(st: &mut State, req: &Request, p: &Value) -> Response {
    let name = s_opt(p, "name").unwrap_or("");
    if name.is_empty() {
        return invalid_input("Report group name is required");
    }
    let Some(report_type) = s_opt(p, "type") else {
        return invalid_input("Report group type is required");
    };
    if p.get("exportConfig").filter(|v| !v.is_null()).is_none() {
        return invalid_input("Export config is required");
    }
    if map_get(&st.report_groups, name).is_some() {
        return already_exists(&format!("Report group already exists: {name}"));
    }
    let export_config = p.get("exportConfig").cloned().unwrap_or(Value::Null);
    let rg = report_group_value(req, name, report_type, &export_config, p.get("tags"));
    map_put(&mut st.report_groups, name.to_string(), rg.clone());
    ok(&json!({"reportGroup": rg}))
}

fn op_update_report_group(st: &mut State, p: &Value) -> Response {
    let arn = s_opt(p, "arn").unwrap_or("");
    if arn.is_empty() {
        return invalid_input("Report group ARN is required");
    }
    let Some(rg) = st
        .report_groups
        .iter_mut()
        .find(|(_, rg)| rg.get("arn").and_then(Value::as_str) == Some(arn))
        .map(|(_, rg)| rg)
    else {
        return not_found(&format!("Report group {arn} does not exist"));
    };
    for key in ["exportConfig", "tags"] {
        if let Some(v) = p.get(key).filter(|v| !v.is_null()) {
            rg[key] = v.clone();
        }
    }
    rg["lastModified"] = json!(unix_now());
    let rg = rg.clone();
    ok(&json!({"reportGroup": rg}))
}

fn op_delete_report_group(st: &mut State, p: &Value) -> Response {
    let arn = s_opt(p, "arn").unwrap_or("");
    if arn.is_empty() {
        return invalid_input("Report group ARN is required");
    }
    let pos = st
        .report_groups
        .iter()
        .position(|(_, rg)| rg.get("arn").and_then(Value::as_str) == Some(arn));
    let Some(i) = pos else {
        return not_found(&format!("Report group {arn} does not exist"));
    };
    st.report_groups.remove(i);
    // Cascade: drop the group's reports too (none exist in this port).
    st.reports
        .retain(|(_, r)| r.get("reportGroupArn").and_then(Value::as_str) != Some(arn));
    empty_ok()
}

fn op_batch_get_report_groups(st: &State, p: &Value) -> Response {
    let Some(arns) = array_opt(p, "reportGroupArns").filter(|a| !a.is_empty()) else {
        return invalid_input("Report group ARNs are required");
    };
    let mut found: Vec<Value> = Vec::new();
    let mut missing: Vec<&str> = Vec::new();
    for arn in arns {
        let Some(arn) = arn.as_str() else {
            continue;
        };
        match st
            .report_groups
            .iter()
            .find(|(_, rg)| rg.get("arn").and_then(Value::as_str) == Some(arn))
        {
            Some((_, rg)) => found.push(rg.clone()),
            None => missing.push(arn),
        }
    }
    ok(&json!({"reportGroups": found, "reportGroupsNotFound": missing}))
}

fn op_list_report_groups(st: &State) -> Response {
    let arns: Vec<&str> = st
        .report_groups
        .iter()
        .filter_map(|(_, rg)| rg.get("arn").and_then(Value::as_str))
        .collect();
    ok(&json!({"reportGroups": arns}))
}

// --- Report operations -----------------------------------------------------------

fn op_list_reports(st: &State) -> Response {
    let arns: Vec<&str> = st.reports.iter().map(|(arn, _)| arn.as_str()).collect();
    ok(&json!({"reports": arns}))
}

fn op_list_reports_for_report_group(st: &State, p: &Value) -> Response {
    let Some(group_arn) = s_opt(p, "reportGroupArn") else {
        return invalid_input("Report group ARN is required");
    };
    let matches: Vec<&str> = st
        .reports
        .iter()
        .filter(|(_, r)| r.get("reportGroupArn").and_then(Value::as_str) == Some(group_arn))
        .map(|(arn, _)| arn.as_str())
        .collect();
    ok(&json!({"reports": matches}))
}

fn op_delete_report(_st: &State, p: &Value) -> Response {
    if s_opt(p, "arn").is_none() {
        return invalid_input("Report ARN is required");
    }
    // The fork pops without an existence check; this port has no report
    // producers, so there is nothing else to remove.
    empty_ok()
}

fn op_batch_get_reports(st: &State, p: &Value) -> Response {
    let Some(arns) = array_opt(p, "reportArns").filter(|a| !a.is_empty()) else {
        return invalid_input("Report ARNs are required");
    };
    let mut found: Vec<Value> = Vec::new();
    let mut missing: Vec<&str> = Vec::new();
    for arn in arns {
        let Some(arn) = arn.as_str() else { continue };
        match map_get(&st.reports, arn) {
            Some(report) => found.push(report.clone()),
            None => missing.push(arn),
        }
    }
    ok(&json!({"reports": found, "reportsNotFound": missing}))
}

fn op_describe_test_cases(p: &Value) -> Response {
    if s_opt(p, "reportArn").is_none() {
        return invalid_input("Report ARN is required");
    }
    ok(&json!({"testCases": []}))
}

fn op_describe_code_coverages(p: &Value) -> Response {
    if s_opt(p, "reportArn").is_none() {
        return invalid_input("Report ARN is required");
    }
    ok(&json!({"codeCoverages": []}))
}

fn op_get_report_group_trend(p: &Value) -> Response {
    if s_opt(p, "reportGroupArn").is_none() {
        return invalid_input("Report group ARN is required");
    }
    if s_opt(p, "trendField").is_none() {
        return invalid_input("Trend field is required");
    }
    ok(&json!({"stats": {}, "rawData": []}))
}

// --- Fleet operations -------------------------------------------------------------

fn op_create_fleet(st: &mut State, req: &Request, p: &Value) -> Response {
    let name = p.get("name").and_then(Value::as_str).unwrap_or("");
    if name.is_empty() {
        return invalid_input("Fleet name is required");
    }
    if p.get("baseCapacity").and_then(Value::as_i64).is_none() {
        return invalid_input("Base capacity is required");
    }
    if p.get("environmentType").and_then(Value::as_str).is_none() {
        return invalid_input("Environment type is required");
    }
    if p.get("computeType").and_then(Value::as_str).is_none() {
        return invalid_input("Compute type is required");
    }
    let fleet = fleet_value(req, p, name);
    map_put(
        &mut st.fleets,
        fleet["arn"].as_str().unwrap_or("").to_string(),
        fleet.clone(),
    );
    ok(&json!({"fleet": fleet}))
}

fn op_update_fleet(st: &mut State, p: &Value) -> Response {
    let arn = s_opt(p, "arn").unwrap_or("");
    if arn.is_empty() {
        return invalid_input("Fleet ARN is required");
    }
    let Some(fleet) = map_get_mut(&mut st.fleets, arn) else {
        return not_found(&format!("Fleet {arn} does not exist"));
    };
    for key in [
        "baseCapacity",
        "environmentType",
        "computeType",
        "computeConfiguration",
        "scalingConfiguration",
        "overflowBehavior",
        "vpcConfig",
        "proxyConfiguration",
        "imageId",
        "fleetServiceRole",
        "tags",
    ] {
        if let Some(v) = p.get(key).filter(|v| !v.is_null()) {
            fleet[key] = v.clone();
        }
    }
    fleet["lastModified"] = json!(unix_now());
    let fleet = fleet.clone();
    ok(&json!({"fleet": fleet}))
}

fn op_delete_fleet(st: &mut State, p: &Value) -> Response {
    let arn = s_opt(p, "arn").unwrap_or("");
    if arn.is_empty() {
        return invalid_input("Fleet ARN is required");
    }
    if map_remove(&mut st.fleets, arn).is_none() {
        return not_found(&format!("Fleet {arn} does not exist"));
    }
    empty_ok()
}

fn op_batch_get_fleets(st: &State, p: &Value) -> Response {
    let Some(names) = array_opt(p, "names").filter(|a| !a.is_empty()) else {
        return invalid_input("Fleet names are required");
    };
    let mut found: Vec<Value> = Vec::new();
    let mut missing: Vec<&str> = Vec::new();
    for name in names {
        let Some(name) = name.as_str() else { continue };
        if let Some((_, fleet)) = st.fleets.iter().find(|(k, _)| k == name) {
            found.push(fleet.clone());
            continue;
        }
        // The fork also matches by the fleet's name field.
        if let Some((_, fleet)) = st
            .fleets
            .iter()
            .find(|(_, f)| f.get("name").and_then(Value::as_str) == Some(name))
        {
            found.push(fleet.clone());
        } else {
            missing.push(name);
        }
    }
    ok(&json!({"fleets": found, "fleetsNotFound": missing}))
}

fn op_list_fleets(st: &State) -> Response {
    let arns: Vec<&str> = st.fleets.iter().map(|(arn, _)| arn.as_str()).collect();
    ok(&json!({"fleets": arns}))
}

// --- Resource policy operations -----------------------------------------------------

fn op_put_resource_policy(st: &mut State, p: &Value) -> Response {
    if s_opt(p, "policy").is_none() {
        return invalid_input("Policy is required");
    }
    let Some(resource_arn) = s_opt(p, "resourceArn") else {
        return invalid_input("Resource ARN is required");
    };
    let policy = p["policy"].as_str().unwrap_or("").to_string();
    map_put(&mut st.resource_policies, resource_arn.to_string(), policy);
    ok(&json!({"resourceArn": resource_arn}))
}

fn op_get_resource_policy(st: &State, p: &Value) -> Response {
    let Some(resource_arn) = s_opt(p, "resourceArn") else {
        return invalid_input("Resource ARN is required");
    };
    match map_get(&st.resource_policies, resource_arn) {
        Some(policy) => ok(&json!({"policy": policy})),
        None => not_found(&format!(
            "Resource policy for {resource_arn} does not exist"
        )),
    }
}

fn op_delete_resource_policy(st: &mut State, p: &Value) -> Response {
    let Some(resource_arn) = s_opt(p, "resourceArn") else {
        return invalid_input("Resource ARN is required");
    };
    if map_remove(&mut st.resource_policies, resource_arn).is_none() {
        return not_found(&format!(
            "Resource policy for {resource_arn} does not exist"
        ));
    }
    empty_ok()
}

// --- Sandboxes -------------------------------------------------------------------------

fn op_start_sandbox(st: &mut State, req: &Request, p: &Value) -> Response {
    let sandbox_id = Uuid::new_v4().to_string();
    let project_name = s_opt(p, "projectName").map(String::from);
    let sandbox = sandbox_value(req, project_name.clone().as_deref(), &sandbox_id);
    map_put(&mut st.sandboxes, sandbox_id.clone(), sandbox.clone());
    if let Some(project) = project_name {
        match map_get_mut(&mut st.sandbox_history, &project) {
            Some(history) => history.push(sandbox_id),
            None => map_put(&mut st.sandbox_history, project, vec![sandbox_id]),
        }
    }
    ok(&json!({"sandbox": sandbox}))
}

fn op_stop_sandbox(st: &mut State, p: &Value) -> Response {
    let Some(sandbox_id) = s_opt(p, "id") else {
        return invalid_input("Sandbox ID is required");
    };
    let Some(sandbox) = map_get_mut(&mut st.sandboxes, sandbox_id) else {
        return not_found(&format!("Sandbox {sandbox_id} does not exist"));
    };
    sandbox["status"] = json!("STOPPED");
    sandbox["endTime"] = json!(iso_ms());
    ok(&json!({"sandbox": sandbox}))
}

fn op_batch_get_sandboxes(st: &State, p: &Value) -> Response {
    let Some(ids) = array_opt(p, "ids").filter(|a| !a.is_empty()) else {
        return invalid_input("Sandbox IDs are required");
    };
    let mut found: Vec<Value> = Vec::new();
    let mut missing: Vec<&str> = Vec::new();
    for id in ids {
        let Some(id) = id.as_str() else { continue };
        match map_get(&st.sandboxes, id) {
            Some(sandbox) => found.push(sandbox.clone()),
            None => missing.push(id),
        }
    }
    ok(&json!({"sandboxes": found, "sandboxesNotFound": missing}))
}

fn op_list_sandboxes(st: &State) -> Response {
    let ids: Vec<&str> = st.sandboxes.iter().map(|(id, _)| id.as_str()).collect();
    ok(&json!({"ids": ids}))
}

fn op_list_sandboxes_for_project(st: &State, p: &Value) -> Response {
    let Some(project_name) = s_opt(p, "projectName") else {
        return invalid_input("Project name is required");
    };
    let ids = map_get(&st.sandbox_history, project_name)
        .cloned()
        .unwrap_or_default();
    ok(&json!({"ids": ids}))
}

fn op_start_sandbox_connection(st: &State, req: &Request, p: &Value) -> Response {
    let Some(sandbox_id) = s_opt(p, "sandboxId") else {
        return invalid_input("Sandbox ID is required");
    };
    if map_get(&st.sandboxes, sandbox_id).is_none() {
        return not_found(&format!("Sandbox {sandbox_id} does not exist"));
    }
    ok(&json!({"ssmSession": {
        "sessionId": Uuid::new_v4().to_string(),
        "streamUrl": format!(
            "wss://ssm.{}.amazonaws.com/ssm-session/{sandbox_id}",
            req.region
        ),
        "tokenValue": random_hex(64),
    }}))
}

// --- Command execution operations ---------------------------------------------------------

fn op_start_command_execution(st: &mut State, p: &Value) -> Response {
    let Some(sandbox_id) = s_opt(p, "sandboxId") else {
        return invalid_input("Sandbox ID is required");
    };
    let Some(command) = s_opt(p, "command") else {
        return invalid_input("Command is required");
    };
    if map_get(&st.sandboxes, sandbox_id).is_none() {
        return not_found(&format!("Sandbox {sandbox_id} does not exist"));
    }
    let exec_id = Uuid::new_v4().to_string();
    let execution = json!({
        "id": exec_id,
        "sandboxId": sandbox_id,
        "command": command,
        "type": s_opt(p, "type").unwrap_or("SHELL"),
        "status": "IN_PROGRESS",
        "submitTime": iso_ms(),
    });
    map_put(
        &mut st.command_executions,
        exec_id.clone(),
        execution.clone(),
    );
    ok(&json!({"commandExecution": execution}))
}

fn op_list_command_executions_for_sandbox(st: &State, p: &Value) -> Response {
    let Some(sandbox_id) = s_opt(p, "sandboxId") else {
        return invalid_input("Sandbox ID is required");
    };
    let executions: Vec<Value> = st
        .command_executions
        .iter()
        .filter(|(_, ex)| ex.get("sandboxId").and_then(Value::as_str) == Some(sandbox_id))
        .map(|(_, ex)| ex.clone())
        .collect();
    ok(&json!({"commandExecutions": executions}))
}

fn op_batch_get_command_executions(st: &State, p: &Value) -> Response {
    let Some(sandbox_id) = s_opt(p, "sandboxId") else {
        return invalid_input("Sandbox ID is required");
    };
    let Some(exec_ids) = array_opt(p, "commandExecutionIds").filter(|a| !a.is_empty()) else {
        return invalid_input("Command execution IDs are required");
    };
    let mut found: Vec<Value> = Vec::new();
    let mut missing: Vec<&str> = Vec::new();
    for id in exec_ids {
        let Some(id) = id.as_str() else { continue };
        match st.command_executions.iter().find(|(_, ex)| {
            ex.get("id").and_then(Value::as_str) == Some(id)
                && ex.get("sandboxId").and_then(Value::as_str) == Some(sandbox_id)
        }) {
            Some((_, ex)) => found.push(ex.clone()),
            None => missing.push(id),
        }
    }
    ok(&json!({"commandExecutions": found, "commandExecutionsNotFound": missing}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ACCOUNT: u64 = 432100000001;

    fn req(op: &str, body: Value) -> Request {
        Request {
            service: "codebuild".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: Default::default(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            account: ACCOUNT,
            region: "us-east-1".into(),
        }
    }

    fn send(st: &mut State, op: &str, body: Value) -> Value {
        let request = req(op, body);
        let response = handle(&request, st, &parse_json_body(&request.body));
        assert_eq!(response.status, 200, "{op} failed: {:?}", response.body);
        serde_json::from_slice(&response.body).unwrap()
    }

    fn send_err(st: &mut State, op: &str, body: Value) -> Response {
        let request = req(op, body);
        handle(&request, st, &parse_json_body(&request.body))
    }

    fn create_project(st: &mut State, name: &str) -> Value {
        send(
            st,
            "CreateProject",
            json!({
                "name": name,
                "source": {"type": "S3", "location": "my-bucket/source.zip"},
                "artifacts": {"type": "NO_ARTIFACTS"},
                "environment": {
                    "type": "LINUX_CONTAINER",
                    "image": "aws/codebuild/standard:5.0",
                    "computeType": "BUILD_GENERAL1_SMALL",
                },
                "serviceRole": "arn:aws:iam::432100000001:role/codebuild-role",
            }),
        )["project"]
            .clone()
    }

    #[test]
    fn project_lifecycle() {
        let slot = state();
        let mut guard = slot.write();
        let st = guard.entry((ACCOUNT, "us-east-1".into())).or_default();

        let project = create_project(st, "proj-a");
        assert_eq!(project["name"], "proj-a");
        assert!(project["arn"]
            .as_str()
            .unwrap()
            .ends_with(":project/proj-a"));
        assert!(project["created"].is_number());

        // duplicate -> ResourceAlreadyExistsException (after full validation)
        let dup = send_err(
            st,
            "CreateProject",
            json!({
                "name": "proj-a",
                "source": {"type": "S3", "location": "my-bucket/source.zip"},
                "artifacts": {"type": "NO_ARTIFACTS"},
                "environment": {
                    "type": "LINUX_CONTAINER",
                    "image": "aws/codebuild/standard:5.0",
                    "computeType": "BUILD_GENERAL1_SMALL",
                },
                "serviceRole": "arn:aws:iam::432100000001:role/codebuild-role",
            }),
        );
        assert_eq!(dup.status, 400);
        assert!(String::from_utf8_lossy(&dup.body).contains("ResourceAlreadyExistsException"));

        // visibility update
        let arn = project["arn"].as_str().unwrap().to_string();
        let vis = send(
            st,
            "UpdateProjectVisibility",
            json!({
                "projectArn": arn, "projectVisibility": "PRIVATE"
            }),
        );
        assert_eq!(vis["projectVisibility"], "PRIVATE");

        // list + batch get + delete
        assert!(send(st, "ListProjects", json!({}))["projects"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n == "proj-a"));
        let got = send(st, "BatchGetProjects", json!({"names": ["proj-a", "nope"]}));
        assert_eq!(got["projects"].as_array().unwrap().len(), 1);
        send(st, "DeleteProject", json!({"name": "proj-a"}));
        let gone = send(st, "BatchGetProjects", json!({"names": ["proj-a"]}));
        assert_eq!(gone["projects"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn build_lifecycle() {
        let slot = state();
        let mut guard = slot.write();
        let st = guard.entry((ACCOUNT, "eu-west-2".into())).or_default();
        create_project(st, "proj-b");

        let build = send(st, "StartBuild", json!({"projectName": "proj-b"}))["build"].clone();
        let build_id = build["id"].as_str().unwrap().to_string();
        assert!(build_id.starts_with("proj-b:"));
        assert_eq!(build["buildStatus"], "IN_PROGRESS");
        assert!(build["buildComplete"].is_boolean());

        // stop -> STOPPED / COMPLETED with full phase list
        let stopped = send(st, "StopBuild", json!({"id": build_id}))["build"].clone();
        assert_eq!(stopped["buildStatus"], "STOPPED");
        assert_eq!(stopped["currentPhase"], "COMPLETED");
        assert!(stopped["endTime"].is_string());
        assert_eq!(stopped["phases"].as_array().unwrap().len(), 11);

        // batch_get_builds on a fresh build -> SUCCEEDED and COMPLETED
        let b2 = send(st, "StartBuild", json!({"projectName": "proj-b"}))["build"]["id"]
            .as_str()
            .unwrap()
            .to_string();
        let got = send(st, "BatchGetBuilds", json!({"ids": [b2]}));
        let builds = got["builds"].as_array().unwrap();
        assert_eq!(builds.len(), 1);
        assert_eq!(builds[0]["buildStatus"], "SUCCEEDED");
        assert_eq!(builds[0]["currentPhase"], "COMPLETED");

        // batch_delete_builds round-trips deleted/not-deleted
        let del = send(st, "BatchDeleteBuilds", json!({"ids": [b2, "proj-b:nope"]}));
        assert_eq!(del["buildsDeleted"].as_array().unwrap().len(), 1);
        let missing = del["buildsNotDeleted"].as_array().unwrap();
        assert_eq!(missing[0]["statusCode"], "BUILD_NOT_FOUND");

        // malformed id -> InvalidInputException
        let bad = send_err(st, "BatchGetBuilds", json!({"ids": ["nocolon"]}));
        assert_eq!(bad.status, 400);
    }

    #[test]
    fn report_group_and_fleet_lifecycle() {
        let slot = state();
        let mut guard = slot.write();
        let st = guard.entry((ACCOUNT, "us-east-1".into())).or_default();

        let rg = send(
            st,
            "CreateReportGroup",
            json!({"name": "rg-1", "type": "TEST", "exportConfig": {"exportConfigType": "NO_EXPORT"}}),
        )["reportGroup"].clone();
        let rg_arn = rg["arn"].as_str().unwrap().to_string();
        assert!(rg_arn.ends_with("report-group/rg-1"));
        assert!(rg["tags"].is_array());
        assert!(rg["tags"].as_array().unwrap().is_empty());

        let updated = send(st, "UpdateReportGroup", json!({"arn": rg_arn, "tags": []}));
        assert_eq!(updated["reportGroup"]["arn"], json!(rg_arn));

        let listed = send(st, "ListReportGroups", json!({}));
        assert_eq!(listed["reportGroups"].as_array().unwrap().len(), 1);

        // batch_get with missing arn -> both lists
        let got = send(
            st,
            "BatchGetReportGroups",
            json!({"reportGroupArns": [rg_arn, "arn:missing"]}),
        );
        assert_eq!(got["reportGroups"].as_array().unwrap().len(), 1);
        assert_eq!(got["reportGroupsNotFound"].as_array().unwrap().len(), 1);

        send(st, "DeleteReportGroup", json!({"arn": rg_arn}));
        assert!(send(st, "ListReportGroups", json!({}))["reportGroups"]
            .as_array()
            .unwrap()
            .is_empty());

        // fleet
        let fleet = send(
            st,
            "CreateFleet",
            json!({"name": "f-1", "baseCapacity": 1, "environmentType": "LINUX_CONTAINER",
                   "computeType": "BUILD_GENERAL1_SMALL"}),
        )["fleet"]
            .clone();
        let f_arn = fleet["arn"].as_str().unwrap().to_string();
        assert_eq!(fleet["overflowBehavior"], "QUEUE");
        let updated = send(st, "UpdateFleet", json!({"arn": f_arn, "baseCapacity": 2}));
        assert_eq!(updated["fleet"]["baseCapacity"], 2);

        let got = send(st, "BatchGetFleets", json!({"names": ["f-1", "gone"]}));
        assert_eq!(got["fleets"].as_array().unwrap().len(), 1);
        assert_eq!(got["fleetsNotFound"].as_array().unwrap()[0], json!("gone"));

        send(st, "DeleteFleet", json!({"arn": f_arn}));
        let del_missing = send_err(st, "DeleteFleet", json!({"arn": f_arn}));
        assert_eq!(del_missing.status, 400);

        // resource policy
        let policy_arn = "arn:aws:codebuild:us-east-1:432100000001:report-group/rg-1";
        send(
            st,
            "PutResourcePolicy",
            json!({"policy": "{\"Version\":\"2012-10-17\"}", "resourceArn": policy_arn}),
        );
        assert!(
            send(st, "GetResourcePolicy", json!({"resourceArn": policy_arn}))["policy"].is_string()
        );
        send(
            st,
            "DeleteResourcePolicy",
            json!({"resourceArn": policy_arn}),
        );
        let gone = send_err(st, "GetResourcePolicy", json!({"resourceArn": policy_arn}));
        assert_eq!(gone.status, 400);
    }

    #[test]
    fn webhook_and_credentials() {
        let slot = state();
        let mut guard = slot.write();
        let st = guard.entry((ACCOUNT, "us-east-1".into())).or_default();
        create_project(st, "proj-c");

        let wh = send(
            st,
            "CreateWebhook",
            json!({"projectName": "proj-c", "buildType": "BUILD"}),
        )["webhook"]
            .clone();
        assert!(wh["payloadUrl"]
            .as_str()
            .unwrap()
            .contains("webhook-trigger/"));
        assert_eq!(wh["buildType"], "BUILD");

        let dup = send_err(st, "CreateWebhook", json!({"projectName": "proj-c"}));
        assert!(String::from_utf8_lossy(&dup.body).contains("ResourceAlreadyExistsException"));

        let updated = send(
            st,
            "UpdateWebhook",
            json!({"projectName": "proj-c", "rotateSecret": true}),
        );
        assert_eq!(updated["webhook"]["branchFilter"], "");

        send(st, "DeleteWebhook", json!({"projectName": "proj-c"}));
        let gone = send_err(st, "DeleteWebhook", json!({"projectName": "proj-c"}));
        assert!(String::from_utf8_lossy(&gone.body).contains("ResourceNotFoundException"));

        // credentials keyed by server type; second import without overwrite fails
        let cred = send(
            st,
            "ImportSourceCredentials",
            json!({"token": "t", "serverType": "GITHUB", "authType": "PERSONAL_ACCESS_TOKEN"}),
        );
        assert!(cred["arn"].as_str().unwrap().ends_with("token/GITHUB"));
        let dup = send_err(
            st,
            "ImportSourceCredentials",
            json!({"token": "t", "serverType": "GITHUB", "authType": "PERSONAL_ACCESS_TOKEN"}),
        );
        assert!(String::from_utf8_lossy(&dup.body).contains("ResourceAlreadyExistsException"));
        let overwrite = send(
            st,
            "ImportSourceCredentials",
            json!({"token": "t2", "serverType": "GITHUB", "authType": "PERSONAL_ACCESS_TOKEN", "shouldOverwrite": true}),
        );
        assert_eq!(overwrite["arn"], cred["arn"]);

        let listed = send(st, "ListSourceCredentials", json!({}));
        assert_eq!(
            listed["sourceCredentialsInfos"].as_array().unwrap().len(),
            1
        );
        send(st, "DeleteSourceCredentials", json!({"arn": cred["arn"]}));
    }

    #[test]
    fn sandbox_and_command_executions() {
        let slot = state();
        let mut guard = slot.write();
        let st = guard.entry((ACCOUNT, "us-east-1".into())).or_default();

        // no projectName -> sandbox with empty projectName (fork behavior)
        let sbx = send(st, "StartSandbox", json!({}))["sandbox"].clone();
        let sid = sbx["id"].as_str().unwrap().to_string();
        assert_eq!(sbx["projectName"], "");
        assert_eq!(sbx["status"], "RUNNING");

        let conn = send(
            st,
            "StartSandboxConnection",
            json!({
                "sandboxId": sid, "session": {}
            }),
        )["ssmSession"]
            .clone();
        assert!(conn["streamUrl"]
            .as_str()
            .unwrap()
            .starts_with("wss://ssm.us-east-1."));

        let exec = send(
            st,
            "StartCommandExecution",
            json!({"sandboxId": sid, "command": "echo hi", "type": "SHELL"}),
        )["commandExecution"]
            .clone();
        let exec_id = exec["id"].as_str().unwrap().to_string();
        assert_eq!(exec["sandboxId"], json!(sid));

        let listed = send(
            st,
            "ListCommandExecutionsForSandbox",
            json!({"sandboxId": sid}),
        );
        assert_eq!(listed["commandExecutions"].as_array().unwrap().len(), 1);

        let got = send(
            st,
            "BatchGetCommandExecutions",
            json!({"sandboxId": sid, "commandExecutionIds": [exec_id, "nope"]}),
        );
        assert_eq!(got["commandExecutions"].as_array().unwrap().len(), 1);
        assert_eq!(
            got["commandExecutionsNotFound"].as_array().unwrap()[0],
            json!("nope")
        );

        // nonexistent sandbox -> ResourceNotFoundException on the exec path
        let err = send_err(
            st,
            "StartCommandExecution",
            json!({"sandboxId": "nope", "command": "echo hi"}),
        );
        assert!(String::from_utf8_lossy(&err.body).contains("ResourceNotFoundException"));

        send(st, "StopSandbox", json!({"id": sid}));
        let stopped = send(st, "BatchGetSandboxes", json!({"ids": [sid]}));
        assert_eq!(stopped["sandboxes"][0]["status"], "STOPPED");
    }

    #[test]
    fn project_name_validation() {
        assert!(validate_project_name("ab").is_none());
        assert!(validate_project_name("project-1_2").is_none());
        assert!(validate_project_name("a").is_some()); // regex needs two chars
        assert!(validate_project_name("b!").is_some());
        // Only the LAST character is checked against the punctuation set.
        assert!(validate_project_name("Ab/c").is_none());
        assert!(validate_project_name("aC#").is_some());
        let long = "a".repeat(150);
        assert!(validate_project_name(&long).is_some());
    }

    #[test]
    fn unknown_operation_is_501() {
        let slot = state();
        let mut guard = slot.write();
        let st = guard.entry((ACCOUNT, "us-east-1".into())).or_default();
        let response = send_err(st, "BogusOperation", json!({}));
        assert_eq!(response.status, 501);
        assert!(String::from_utf8_lossy(&response.body).contains("NotImplemented"));
    }
}
