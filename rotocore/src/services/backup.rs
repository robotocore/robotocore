//! AWS Backup backend — moto-equivalent (rest-json).
//!
//! Mirrors moto's backup for the suite-covered surface: vaults, plans,
//! selections, jobs, frameworks, legal holds, recovery points, restore
//! testing plans/selections, report plans, vault policies/notifications,
//! settings, and the fake scan/tiering families. The pytest suites
//! (tests/compatibility/test_backup_compat.py, tests/moto_impl/
//! test_backup_lifecycle.py) are the contract.

use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

const SUPPORTED_RESOURCE_TYPES: &[&str] = &[
    "Aurora",
    "CloudFormation",
    "DocumentDB",
    "DynamoDB",
    "EBS",
    "EC2",
    "EFS",
    "FSx",
    "Neptune",
    "RDS",
    "Redshift",
    "S3",
    "SAP HANA on Amazon EC2",
    "Storage Gateway",
    "Timestream",
    "VirtualMachine",
];

const BACKUP_PLAN_TEMPLATES: &[(&str, &str)] = &[
    (
        "87c0c1ef-254d-4a46-945e-a37e8a17891c",
        "Daily-35day-Retention",
    ),
    (
        "d3e5f678-7ab2-4c56-8a1b-234567890abc",
        "Daily-Monthly-1yr-Retention",
    ),
];

fn backup_plan_template_details(template_id: &str) -> Option<Value> {
    match template_id {
        "87c0c1ef-254d-4a46-945e-a37e8a17891c" => Some(json!({
            "BackupPlanName": "Daily-35day-Retention",
            "Rules": [{
                "RuleName": "DailyBackups",
                "ScheduleExpression": "cron(0 5 ? * * *)",
                "StartWindowMinutes": 480,
                "CompletionWindowMinutes": 10080,
                "Lifecycle": {"DeleteAfterDays": 35},
                "TargetBackupVaultName": "Default",
            }],
        })),
        "d3e5f678-7ab2-4c56-8a1b-234567890abc" => Some(json!({
            "BackupPlanName": "Daily-Monthly-1yr-Retention",
            "Rules": [
                {
                    "RuleName": "DailyBackups",
                    "ScheduleExpression": "cron(0 5 ? * * *)",
                    "StartWindowMinutes": 480,
                    "CompletionWindowMinutes": 10080,
                    "Lifecycle": {"DeleteAfterDays": 35},
                    "TargetBackupVaultName": "Default",
                },
                {
                    "RuleName": "MonthlyBackups",
                    "ScheduleExpression": "cron(0 5 1 * ? *)",
                    "StartWindowMinutes": 480,
                    "CompletionWindowMinutes": 10080,
                    "Lifecycle": {"DeleteAfterDays": 365},
                    "TargetBackupVaultName": "Default",
                },
            ],
        })),
        _ => None,
    }
}

type Stores = Arc<RwLock<std::collections::HashMap<ScopeKey, BackupStore>>>;

fn stores() -> Stores {
    store::slot::<BackupStore>("backup")
}

/// Per-(account, region) in-memory state for AWS Backup.
#[derive(Default)]
struct BackupStore {
    /// name -> vault
    vaults: BTreeMap<String, Value>,
    /// plan id -> plan
    plans: BTreeMap<String, Value>,
    /// plan id -> {selection id -> selection}
    selections: BTreeMap<String, BTreeMap<String, Value>>,
    /// job id -> backup job
    backup_jobs: BTreeMap<String, Value>,
    /// copy job id -> copy job
    copy_jobs: BTreeMap<String, Value>,
    /// restore job id -> restore job
    restore_jobs: BTreeMap<String, Value>,
    /// framework name -> framework
    frameworks: BTreeMap<String, Value>,
    /// legal hold id -> hold
    legal_holds: BTreeMap<String, Value>,
    /// vault name -> {recovery point arn -> recovery point} (stays empty:
    /// StartBackupJob does not materialize recovery points in moto).
    recovery_points: BTreeMap<String, BTreeMap<String, Value>>,
    /// vault name -> access policy
    vault_access_policies: BTreeMap<String, Value>,
    /// vault name -> notification configuration
    vault_notifications: BTreeMap<String, Value>,
    /// report plan name -> plan
    report_plans: BTreeMap<String, Value>,
    /// report job id -> job
    report_jobs: BTreeMap<String, Value>,
    /// restore testing plan name -> plan
    restore_testing_plans: BTreeMap<String, Value>,
    /// restore testing plan name -> {selection name -> selection}
    restore_testing_selections: BTreeMap<String, BTreeMap<String, Value>>,
    /// vault name -> logically air-gapped vault
    logically_air_gapped_vaults: BTreeMap<String, Value>,
    /// scan job id -> scan job
    scan_jobs: BTreeMap<String, Value>,
    /// tiering configuration name -> configuration
    tiering_configurations: BTreeMap<String, Value>,
    /// DescribeGlobalSettings payload
    global_settings: Value,
    /// DescribeRegionSettings payload
    region_settings: Value,
    /// arn -> {tag key: value}
    tags: BTreeMap<String, BTreeMap<String, String>>,
}

fn fresh_settings() -> BackupStore {
    BackupStore {
        global_settings: json!({ "isCrossAccountBackupEnabled": "false" }),
        region_settings: json!({
            "EBS": true, "EC2": true, "EFS": true, "DynamoDB": true,
            "RDS": true, "Aurora": true, "Storage Gateway": true,
            "DocumentDB": true, "Neptune": true, "S3": true,
            "CloudFormation": true, "FSx": true, "VirtualMachine": true,
            "Redshift": true, "SAP HANA on Amazon EC2": true, "Timestream": true,
        }),
        ..BackupStore::default()
    }
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "backup" {
        return None;
    }
    Some(with_store(req, |st| route(req, st)))
}

fn with_store(req: &Request, f: impl FnOnce(&mut BackupStore) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map
        .entry((req.account, req.region.clone()))
        .or_insert_with(fresh_settings);
    f(st)
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response {
        status: 200,
        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
        body: serde_json::to_vec(&body).unwrap_or_default(),
    }
}

fn ok_with_status(status: u16, body: Value) -> Response {
    Response {
        status,
        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
        body: serde_json::to_vec(&body).unwrap_or_default(),
    }
}

fn not_found(message: String) -> Response {
    crate::proto::json_error(400, "ResourceNotFoundException", &message)
}

fn already_exists(message: String) -> Response {
    crate::proto::json_error(400, "AlreadyExistsException", &message)
}

fn invalid_request(message: String) -> Response {
    crate::proto::json_error(400, "InvalidRequestException", &message)
}

/// Validate well-known plan rule members the way moto does (defaults only).
fn fill_rule_defaults(rule: &mut Value) {
    let obj = match rule.as_object_mut() {
        Some(o) => o,
        None => return,
    };
    obj.entry("ScheduleExpression".to_string())
        .or_insert(json!("cron(0 5 ? * * *)"));
    obj.entry("StartWindowMinutes".to_string())
        .or_insert(json!(480));
    obj.entry("CompletionWindowMinutes".to_string())
        .or_insert(json!(10080));
    obj.entry("ScheduleExpressionTimezone".to_string())
        .or_insert(json!("Etc/UTC"));
    obj.entry("RuleId".to_string())
        .or_insert(json!(Uuid::new_v4().to_string()));
}

fn invalid_param(message: String) -> Response {
    crate::proto::json_error(400, "InvalidParameterValueException", &message)
}

fn body_of(req: &Request) -> Value {
    crate::proto::parse_json_body(&req.body)
}

fn now() -> f64 {
    now_ms() as f64 / 1000.0
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 3 <= bytes.len() => {
                match u8::from_str_radix(
                    std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""),
                    16,
                ) {
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
            b'+' => {
                out.push(b' ');
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

fn query_param(req: &Request, key: &str) -> Option<String> {
    req.query_string.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
        (percent_decode(k) == key).then(|| percent_decode(v))
    })
}

/// A path parameter occupying one-or-more segments (collect and decode).
fn path_param(rest: &[&str]) -> String {
    percent_decode(&rest.join("/"))
}

fn random_string(len: usize) -> String {
    let base = Uuid::new_v4().simple().to_string();
    base.repeat((len / base.len()) + 1)
        .chars()
        .take(len)
        .collect()
}

fn arn_prefix(req: &Request) -> String {
    format!("arn:aws:backup:{}:{}", req.region, req.account)
}

fn vault_arn(req: &Request, name: &str) -> String {
    format!("{}:backup-vault:{}", arn_prefix(req), name)
}

fn recovery_point_arn(req: &Request) -> String {
    format!("{}:recovery-point:{}", arn_prefix(req), Uuid::new_v4())
}

/// Maps the service segment of a resource ARN onto Backup's resource type.
fn derive_resource_type(resource_arn: &str) -> String {
    match resource_arn.split(':').nth(2) {
        Some("dynamodb") => "DynamoDB".to_string(),
        Some("ec2") => "EC2".to_string(),
        Some("rds") => "RDS".to_string(),
        Some("s3") => "S3".to_string(),
        Some("efs") => "EFS".to_string(),
        Some("ebs") => "EBS".to_string(),
        Some(other) => other.to_string(),
        None => "Unknown".to_string(),
    }
}

impl BackupStore {
    fn tag_resource(&mut self, arn: &str, tags: Option<&Value>) {
        let Some(tags) = tags.and_then(Value::as_object) else {
            return;
        };
        let entry = self.tags.entry(arn.to_string()).or_default();
        for (k, v) in tags {
            if let Some(s) = v.as_str() {
                entry.insert(k.clone(), s.to_string());
            }
        }
    }

    fn untag_resource(&mut self, arn: &str, keys: Option<&Value>) {
        let Some(keys) = keys.and_then(Value::as_array) else {
            return;
        };
        if let Some(entry) = self.tags.get_mut(arn) {
            for k in keys.iter().filter_map(Value::as_str) {
                entry.remove(k);
            }
        }
    }

    fn list_tags(&self, arn: &str) -> Value {
        self.tags
            .get(arn)
            .map(|m| {
                Value::Object(
                    m.iter()
                        .map(|(k, v)| (k.clone(), json!(v.clone())))
                        .collect(),
                )
            })
            .unwrap_or_else(|| Value::Object(serde_json::Map::new()))
    }
}

/// Whether an ARN belongs to a resource known to this store.
fn resource_arn_known(st: &BackupStore, arn: &str) -> bool {
    st.tags.contains_key(arn)
        || st
            .vaults
            .values()
            .any(|v| v["backup_vault_arn"].as_str() == Some(arn))
        || st
            .plans
            .values()
            .any(|p| p["backup_plan_arn"].as_str() == Some(arn))
        || st
            .frameworks
            .values()
            .any(|f| f["framework_arn"].as_str() == Some(arn))
        || st
            .legal_holds
            .values()
            .any(|h| h["legal_hold_arn"].as_str() == Some(arn))
        || st
            .restore_testing_plans
            .values()
            .any(|p| p["restore_testing_plan_arn"].as_str() == Some(arn))
        || st
            .report_plans
            .values()
            .any(|p| p["report_plan_arn"].as_str() == Some(arn))
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

fn route(req: &Request, st: &mut BackupStore) -> Response {
    let parts: Vec<&str> = req.path.split('/').filter(|s| !s.is_empty()).collect();
    let method = req.method.as_str();
    let body = body_of(req);

    if let Some(rest) = strip(&parts, &["backup", "plans"]) {
        return plans_routes(req, st, method, rest, &body);
    }
    if let Some(rest) = strip(&parts, &["backup-vaults"]) {
        return vaults_routes(req, st, method, rest, &body);
    }
    if let Some(rest) = strip(&parts, &["tags"]) {
        return tags_routes(req, st, method, rest, &body);
    }
    if let Some(rest) = strip(&parts, &["untag"]) {
        let arn = path_param(rest);
        if method == "POST" {
            if !resource_arn_known(st, &arn) {
                return invalid_param(format!("Unknown resource {arn}"));
            }
            st.untag_resource(&arn, body.get("TagKeyList"));
            return ok(json!({}));
        }
        return not_supported(req);
    }
    if let Some(rest) = strip(&parts, &["backup-jobs"]) {
        return backup_jobs_routes(req, st, method, rest, &body);
    }
    if let Some(rest) = strip(&parts, &["copy-jobs"]) {
        return copy_jobs_routes(req, st, method, rest, &body);
    }
    if let Some(rest) = strip(&parts, &["restore-jobs"]) {
        return restore_jobs_routes(req, st, method, rest, &body);
    }
    if let Some(rest) = strip(&parts, &["backup", "template"]) {
        return template_routes(req, st, method, rest, &body);
    }
    if let Some(rest) = strip(&parts, &["audit", "frameworks"]) {
        return frameworks_routes(req, st, method, rest, &body);
    }
    if let Some(rest) = strip(&parts, &["audit", "report-plans"]) {
        return report_plans_routes(req, st, method, rest, &body);
    }
    if let Some(rest) = strip(&parts, &["audit", "report-jobs"]) {
        return report_jobs_routes(req, st, method, rest);
    }
    if let Some(rest) = strip(&parts, &["audit"]) {
        if let [family, ..] = rest {
            if let Some(key) = summary_key(family) {
                return ok(json!({ key: summaries(req, family, st) }));
            }
        }
        return not_supported(req);
    }
    if parts == ["global-settings"] {
        return match method {
            "GET" => ok(json!({ "GlobalSettings": st.global_settings.clone() })),
            "PUT" => {
                // moto: params.get("GlobalSettings", {}) — an empty body is a no-op.
                if let Some(settings) = body.get("GlobalSettings").and_then(Value::as_object) {
                    for (k, v) in settings {
                        st.global_settings[k] = v.clone();
                    }
                }
                ok(json!({}))
            }
            _ => not_supported(req),
        };
    }
    if parts == ["account-settings"] {
        return match method {
            "GET" => ok(json!({ "ResourceTypeOptInPreference": st.region_settings.clone() })),
            "PUT" => {
                // moto: a missing ResourceTypeOptInPreference is a no-op.
                if let Some(settings) = body
                    .get("ResourceTypeOptInPreference")
                    .and_then(Value::as_object)
                {
                    for (k, v) in settings {
                        st.region_settings[k] = v.clone();
                    }
                }
                ok(json!({}))
            }
            _ => not_supported(req),
        };
    }
    if parts == ["supported-resource-types"] {
        return ok(json!({"ResourceTypes": SUPPORTED_RESOURCE_TYPES}));
    }
    if let Some(rest) = strip(&parts, &["legal-holds"]) {
        return legal_holds_routes(req, st, method, rest, &body);
    }
    if let Some(rest) = strip(&parts, &["resources"]) {
        return protected_resources_routes(req, st, method, rest, &body);
    }
    if let Some(rest) = strip(&parts, &["restore-testing", "plans"]) {
        return restore_testing_routes(req, st, method, rest, &body);
    }
    if let Some(rest) = strip(&parts, &["logically-air-gapped-backup-vaults"]) {
        return air_gapped_routes(req, st, method, rest, &body);
    }
    if let Some(rest) = strip(&parts, &["scan"]) {
        // StartScanJob uses /scan/job; list/describe live under /scan/jobs/.
        if method == "PUT" && rest == ["job"] {
            return scan_job_routes(req, st, "PUT", &[], &body);
        }
        return scan_job_routes(req, st, method, rest, &body);
    }
    if let Some(rest) = strip(&parts, &["tiering-configurations"]) {
        return tiering_routes(req, st, method, rest, &body);
    }

    not_supported(req)
}

/// Route `parts` after removing exactly `prefix` from the front.
fn strip<'a>(parts: &'a [&'a str], prefix: &[&str]) -> Option<&'a [&'a str]> {
    if parts.len() < prefix.len() {
        return None;
    }
    for (idx, p) in prefix.iter().enumerate() {
        if parts[idx] != *p {
            return None;
        }
    }
    Some(&parts[prefix.len()..])
}

fn not_supported(req: &Request) -> Response {
    crate::proto::json_error(
        501,
        "NotImplemented",
        &format!("The {} action has not been implemented", req.operation),
    )
}

// ---------------------------------------------------------------------------
// Backup plans
// ---------------------------------------------------------------------------

fn plans_routes(
    req: &Request,
    st: &mut BackupStore,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    if rest.is_empty() {
        return match method {
            "PUT" => create_backup_plan(req, st, body),
            "GET" => list_backup_plans(req, st),
            _ => not_supported(req),
        };
    }
    let plan_id = rest[0];
    match (method, rest.get(1)) {
        ("GET", None) => get_backup_plan(st, plan_id),
        ("POST", None) => update_backup_plan(st, plan_id, body),
        ("DELETE", None) => delete_backup_plan(st, plan_id),
        ("PUT", Some(&"selections")) => create_backup_selection(req, st, plan_id, body),
        ("GET", Some(&"selections")) if rest.len() == 2 => list_backup_selections(st, plan_id),
        ("GET", Some(&"selections")) => get_backup_selection(st, plan_id, rest[2]),
        ("GET", Some(&"versions")) => match st.plans.get(plan_id) {
            Some(plan) => ok(json!({ "BackupPlanVersionsList": [plan_list_entry(plan)] })),
            None => plan_not_found(),
        },
        ("GET", Some(&"toTemplate")) => match st.plans.get(plan_id) {
            Some(plan) => {
                let template = serde_json::to_string(&plan["backup_plan"]).unwrap_or_default();
                ok(json!({ "BackupPlanTemplateJson": template }))
            }
            None => plan_not_found(),
        },
        ("DELETE", Some(&"selections")) if rest.len() == 3 => {
            if let Some(map) = st.selections.get_mut(plan_id) {
                map.remove(rest[2]);
            }
            ok(json!({}))
        }
        _ => not_supported(req),
    }
}

fn plan_not_found() -> Response {
    not_found("Failed reading Backup plan with provided version".to_string())
}

fn new_plan(backup_plan: &Value, req: &Request) -> Value {
    let plan_id = Uuid::new_v4().to_string();
    let mut marker = backup_plan.clone();
    if let Some(rules) = marker.get_mut("Rules").and_then(Value::as_array_mut) {
        rules.iter_mut().for_each(fill_rule_defaults);
    }
    json!({
        "backup_plan_id": plan_id,
        "backup_plan_arn": format!("{}:backup-plan:{}", arn_prefix(req), plan_id),
        "creation_date": now(),
        "version_id": random_string(48),
        "creator_request_id": Value::Null,
        "backup_plan": marker,
        "advanced_backup_settings": backup_plan
            .get("AdvancedBackupSettings")
            .cloned()
            .unwrap_or(json!([])),
        "deletion_date": Value::Null,
        "last_execution_date": Value::Null,
    })
}

fn plan_to_dict(plan: &Value) -> Value {
    let mut out = json!({
        "BackupPlanId": plan["backup_plan_id"],
        "BackupPlanArn": plan["backup_plan_arn"],
        "CreationDate": plan["creation_date"],
        "VersionId": plan["version_id"],
    });
    let advanced = plan["advanced_backup_settings"]
        .as_array()
        .unwrap_or(&Vec::new())
        .clone();
    if !advanced.is_empty() {
        out["AdvancedBackupSettings"] = json!(advanced);
    }
    out
}

fn plan_to_get_dict(plan: &Value) -> Value {
    let mut out = plan_to_dict(plan);
    out["BackupPlan"] = plan["backup_plan"].clone();
    if let Some(c) = plan["creator_request_id"]
        .as_str()
        .filter(|c| !c.is_empty())
    {
        out["CreatorRequestId"] = json!(c);
    }
    if let Some(d) = plan["deletion_date"].as_f64() {
        out["DeletionDate"] = json!(d);
    }
    if let Some(d) = plan["last_execution_date"].as_f64() {
        out["LastExecutionDate"] = json!(d);
    }
    out
}

fn plan_list_entry(plan: &Value) -> Value {
    let mut out = plan_to_get_dict(plan);
    out.as_object_mut().unwrap().remove("BackupPlan");
    out["BackupPlanName"] = plan["backup_plan"]["BackupPlanName"].clone();
    out
}

fn create_backup_plan(req: &Request, st: &mut BackupStore, body: &Value) -> Response {
    let backup_plan = body.get("BackupPlan").cloned().unwrap_or(json!({}));
    let name = backup_plan
        .get("BackupPlanName")
        .and_then(Value::as_str)
        .unwrap_or("");
    if st.plans.values().any(|p| {
        p["backup_plan"]
            .get("BackupPlanName")
            .and_then(Value::as_str)
            == Some(name)
    }) {
        return already_exists(
            "Backup plan with the same plan document already exists".to_string(),
        );
    }
    let plan = new_plan(&backup_plan, req);
    if let Some(tags) = body.get("BackupPlanTags") {
        st.tag_resource(plan["backup_plan_arn"].as_str().unwrap_or(""), Some(tags));
    }
    let plan_id = plan["backup_plan_id"].as_str().unwrap_or("").to_string();
    st.plans.insert(plan_id, plan.clone());
    ok(plan_to_dict(&plan))
}

fn get_backup_plan(st: &mut BackupStore, plan_id: &str) -> Response {
    match st.plans.get(plan_id) {
        Some(plan) => ok(plan_to_get_dict(plan)),
        None => plan_not_found(),
    }
}

fn delete_backup_plan(st: &mut BackupStore, plan_id: &str) -> Response {
    let Some(plan) = st.plans.get_mut(plan_id) else {
        return plan_not_found();
    };
    let arn = plan["backup_plan_arn"].as_str().unwrap_or("").to_string();
    let version_id = plan["version_id"].as_str().unwrap_or("").to_string();
    let deletion_date = now();
    plan["deletion_date"] = json!(deletion_date);
    ok(json!({
        "BackupPlanId": plan_id,
        "BackupPlanArn": arn,
        "DeletionDate": deletion_date,
        "VersionId": version_id,
    }))
}

fn list_backup_plans(req: &Request, st: &mut BackupStore) -> Response {
    let include_deleted =
        query_param(req, "includeDeleted").is_some_and(|v| v == "true" || v == "True");
    let list: Vec<Value> = st
        .plans
        .values()
        .filter(|p| include_deleted || p["deletion_date"].as_f64().is_none())
        .map(plan_list_entry)
        .collect();
    ok(json!({ "BackupPlansList": list }))
}

fn update_backup_plan(st: &mut BackupStore, plan_id: &str, body: &Value) -> Response {
    let Some(plan) = st.plans.get_mut(plan_id) else {
        return plan_not_found();
    };
    if let Some(bp) = body.get("BackupPlan") {
        let mut marker = bp.clone();
        if let Some(rules) = marker.get_mut("Rules").and_then(Value::as_array_mut) {
            rules.iter_mut().for_each(fill_rule_defaults);
        }
        plan["backup_plan"] = marker;
        plan["advanced_backup_settings"] = bp
            .get("AdvancedBackupSettings")
            .cloned()
            .unwrap_or(json!([]));
    }
    plan["creation_date"] = json!(now());
    plan["version_id"] = json!(random_string(48));
    ok(plan_to_dict(plan))
}

// ---------------------------------------------------------------------------
// Selections
// ---------------------------------------------------------------------------

fn create_backup_selection(
    _req: &Request,
    st: &mut BackupStore,
    plan_id: &str,
    body: &Value,
) -> Response {
    if !st.plans.contains_key(plan_id) {
        return plan_not_found();
    }
    let selection = body.get("BackupSelection").cloned().unwrap_or(json!({}));
    let creator = body
        .get("CreatorRequestId")
        .and_then(Value::as_str)
        .unwrap_or("");
    let sel_id = Uuid::new_v4().to_string();
    st.selections
        .entry(plan_id.to_string())
        .or_default()
        .insert(
            sel_id.clone(),
            json!({
                "selection_id": sel_id,
                "backup_plan_id": plan_id,
                "backup_selection": selection,
                "creator_request_id": creator,
                "creation_date": now(),
            }),
        );
    ok(json!({
        "SelectionId": sel_id,
        "BackupPlanId": plan_id,
        "CreationDate": now(),
    }))
}

fn get_backup_selection(st: &mut BackupStore, plan_id: &str, sel_id: &str) -> Response {
    if !st.plans.contains_key(plan_id) {
        return plan_not_found();
    }
    match st.selections.get(plan_id).and_then(|m| m.get(sel_id)) {
        Some(sel) => {
            let mut out = json!({
                "SelectionId": sel["selection_id"],
                "BackupPlanId": sel["backup_plan_id"],
                "CreationDate": sel["creation_date"],
                "BackupSelection": sel["backup_selection"],
            });
            if let Some(c) = sel["creator_request_id"].as_str().filter(|c| !c.is_empty()) {
                out["CreatorRequestId"] = json!(c);
            }
            ok(out)
        }
        None => not_found(format!("Backup Selection {sel_id} not found")),
    }
}

fn list_backup_selections(st: &mut BackupStore, plan_id: &str) -> Response {
    if !st.plans.contains_key(plan_id) {
        return plan_not_found();
    }
    let list: Vec<Value> = st
        .selections
        .get(plan_id)
        .map(|m| {
            m.values()
                .map(|sel| {
                    json!({
                        "SelectionId": sel["selection_id"],
                        "SelectionName": sel["backup_selection"]["SelectionName"],
                        "BackupPlanId": sel["backup_plan_id"],
                        "CreationDate": sel["creation_date"],
                        "IamRoleArn": sel["backup_selection"]["IamRoleArn"],
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    ok(json!({ "BackupSelectionsList": list }))
}

// ---------------------------------------------------------------------------
// Vault family
// ---------------------------------------------------------------------------

fn vaults_routes(
    req: &Request,
    st: &mut BackupStore,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    let Some(name) = rest.first() else {
        let list: Vec<Value> = st.vaults.values().map(vault_list_entry).collect();
        return ok(json!({ "BackupVaultList": list }));
    };
    let name = percent_decode(name);
    if rest.len() == 1 {
        return match method {
            "PUT" => create_backup_vault(req, st, &name, body),
            "DELETE" => {
                st.vaults.remove(&name);
                ok(json!({}))
            }
            "GET" => describe_backup_vault(st, &name),
            _ => not_supported(req),
        };
    }
    match (method, rest.get(1)) {
        ("PUT", Some(&"access-policy")) => {
            if !st.vaults.contains_key(&name) {
                return not_found(format!("Backup vault {name} not found"));
            }
            st.vault_access_policies.insert(
                name.clone(),
                json!({
                    "BackupVaultName": name,
                    "BackupVaultArn": vault_arn(req, &name),
                    "Policy": body.get("Policy").cloned().unwrap_or(json!("")),
                }),
            );
            ok(json!({}))
        }
        ("GET", Some(&"access-policy")) => {
            if !st.vaults.contains_key(&name) {
                return not_found(format!("Backup vault {name} not found"));
            }
            match st.vault_access_policies.get(&name) {
                Some(p) => ok(p.clone()),
                None => not_found(format!("Access policy for vault {name} not found")),
            }
        }
        ("DELETE", Some(&"access-policy")) => {
            if !st.vaults.contains_key(&name) {
                return not_found(format!("Backup vault {name} not found"));
            }
            st.vault_access_policies.remove(&name);
            ok(json!({}))
        }
        ("PUT", Some(&"notification-configuration")) => {
            if !st.vaults.contains_key(&name) {
                return not_found(format!("Backup vault {name} not found"));
            }
            st.vault_notifications.insert(
                name.clone(),
                json!({
                    "BackupVaultName": name,
                    "BackupVaultArn": vault_arn(req, &name),
                    "SNSTopicArn": body.get("SNSTopicArn").cloned().unwrap_or(json!("")),
                    "BackupVaultEvents": body.get("BackupVaultEvents").cloned().unwrap_or(json!([])),
                }),
            );
            ok(json!({}))
        }
        ("GET", Some(&"notification-configuration")) => {
            if !st.vaults.contains_key(&name) {
                return not_found(format!("Backup vault {name} not found"));
            }
            match st.vault_notifications.get(&name) {
                Some(n) => ok(n.clone()),
                None => not_found(format!(
                    "Failed reading notifications for Backup vault {name}"
                )),
            }
        }
        ("DELETE", Some(&"notification-configuration")) => {
            if !st.vaults.contains_key(&name) {
                return not_found(format!("Backup vault {name} not found"));
            }
            st.vault_notifications.remove(&name);
            ok(json!({}))
        }
        ("PUT", Some(&"vault-lock")) => vault_lock_put(st, &name, body),
        ("DELETE", Some(&"vault-lock")) => vault_lock_delete(st, &name),
        ("GET", Some(&"recovery-points")) if rest.len() == 2 => {
            if !st.vaults.contains_key(&name) {
                return not_found(format!("Backup vault {name} not found"));
            }
            ok(json!({"RecoveryPoints": []}))
        }
        (_, Some(&"recovery-points")) if rest.len() >= 3 => {
            recovery_point_routes(req, st, method, &name, &path_param(&rest[2..]), body)
        }
        (_, Some(&"resources")) => {
            if !st.vaults.contains_key(&name) {
                return not_found(format!("Backup vault {name} not found"));
            }
            ok(json!({"Results": []}))
        }
        _ => not_supported(req),
    }
}

fn vault_list_entry(vault: &Value) -> Value {
    let mut out = json!({
        "BackupVaultName": vault["backup_vault_name"],
        "BackupVaultArn": vault["backup_vault_arn"],
        "CreationDate": vault["creation_date"],
        "NumberOfRecoveryPoints": vault["num_of_recovery_points"],
        "Locked": vault["locked"],
    });
    merge_optional_vault_fields(&mut out, vault);
    out
}

fn merge_optional_vault_fields(out: &mut Value, vault: &Value) {
    if let Some(e) = vault["encryption_key_arn"]
        .as_str()
        .filter(|k| !k.is_empty())
    {
        out["EncryptionKeyArn"] = json!(e);
    }
    if let Some(id) = vault["creator_request_id"]
        .as_str()
        .filter(|k| !k.is_empty())
    {
        out["CreatorRequestId"] = json!(id);
    }
    if let Some(d) = vault["min_retention_days"].as_i64() {
        out["MinRetentionDays"] = json!(d);
    }
    if let Some(d) = vault["max_retention_days"].as_i64() {
        out["MaxRetentionDays"] = json!(d);
    }
    if let Some(d) = vault["lock_date"].as_f64() {
        out["LockDate"] = json!(d);
    }
}

fn create_backup_vault(req: &Request, st: &mut BackupStore, name: &str, body: &Value) -> Response {
    if st.vaults.contains_key(name) {
        return already_exists("Backup vault with the same name already exists".to_string());
    }
    let vault = json!({
        "backup_vault_name": name,
        "backup_vault_arn": vault_arn(req, name),
        "creation_date": now(),
        "encryption_key_arn": body.get("EncryptionKeyArn").and_then(Value::as_str).unwrap_or(""),
        "creator_request_id": body.get("CreatorRequestId").and_then(Value::as_str).unwrap_or(""),
        "num_of_recovery_points": 0,
        "locked": false,
        "min_retention_days": Value::Null,
        "max_retention_days": Value::Null,
        "lock_date": Value::Null,
    });
    if let Some(t) = body.get("BackupVaultTags") {
        st.tag_resource(&vault_arn(req, name), Some(t));
    }
    st.vaults.insert(name.to_string(), vault.clone());
    ok(json!({
        "BackupVaultName": name,
        "BackupVaultArn": vault["backup_vault_arn"],
        "CreationDate": vault["creation_date"],
    }))
}

fn describe_backup_vault(st: &mut BackupStore, name: &str) -> Response {
    let Some(vault) = st.vaults.get(name) else {
        return not_found(format!("Backup vault {name} not found"));
    };
    let mut out = vault_list_entry(vault);
    merge_optional_vault_fields(&mut out, vault);
    ok(out)
}

fn vault_lock_put(st: &mut BackupStore, name: &str, body: &Value) -> Response {
    let Some(vault) = st.vaults.get_mut(name) else {
        return not_found(format!("Backup vault {name} not found"));
    };
    if vault["lock_date"].as_f64().is_some_and(|ld| ld <= now()) {
        return invalid_request(
            "Vault Lock configuration is immutable and cannot be modified".to_string(),
        );
    }
    let min = body.get("MinRetentionDays").and_then(Value::as_i64);
    let max = body.get("MaxRetentionDays").and_then(Value::as_i64);
    let changeable = body.get("ChangeableForDays").and_then(Value::as_i64);
    if min.is_some_and(|m| m < 1) {
        return invalid_param("MinRetentionDays must be at least 1 day".to_string());
    }
    if max.is_some_and(|m| m > 36500) {
        return invalid_param("MaxRetentionDays cannot exceed 36500 days".to_string());
    }
    if let (Some(mi), Some(ma)) = (min, max) {
        if mi > ma {
            return invalid_param(
                "MinRetentionDays cannot be greater than MaxRetentionDays".to_string(),
            );
        }
    }
    if changeable.is_some_and(|c| c < 3) {
        return invalid_param("ChangeableForDays must be at least 3 days".to_string());
    }
    vault["locked"] = json!(true);
    vault["min_retention_days"] = min.map(|m| json!(m)).unwrap_or(Value::Null);
    vault["max_retention_days"] = max.map(|m| json!(m)).unwrap_or(Value::Null);
    vault["lock_date"] = changeable
        .map(|c| json!(now() + (c as f64 * 86400.0)))
        .unwrap_or(Value::Null);
    ok(json!({}))
}

fn vault_lock_delete(st: &mut BackupStore, name: &str) -> Response {
    let Some(vault) = st.vaults.get_mut(name) else {
        return not_found(format!("Backup vault {name} not found"));
    };
    vault["locked"] = json!(false);
    vault["min_retention_days"] = Value::Null;
    vault["max_retention_days"] = Value::Null;
    vault["lock_date"] = Value::Null;
    ok(json!({}))
}

fn recovery_point_routes(
    req: &Request,
    st: &mut BackupStore,
    method: &str,
    name: &str,
    arn_and_suffix: &str,
    _body: &Value,
) -> Response {
    // moto's route label for rpArn is greedy: the arn may span several
    // segments. Only a recognized terminal segment is a suffix; "index" has
    // no moto handler and stays a faithful 501.
    let (rp_arn, suffix) = {
        let segs: Vec<&str> = arn_and_suffix.split('/').collect();
        match segs.last().copied().filter(|la| {
            matches!(
                *la,
                "restore-metadata" | "disassociate" | "parentAssociation" | "index"
            )
        }) {
            Some(last) if segs.len() > 1 => {
                let rp = segs[..segs.len() - 1].join("/");
                (percent_decode(&rp), Some(last))
            }
            _ => (percent_decode(arn_and_suffix), None),
        }
    };
    let vault_missing = || not_found(format!("Backup vault {name} not found"));
    let rp_missing = || {
        if !st.vaults.contains_key(name) {
            return vault_missing();
        }
        rp_not_found(&rp_arn)
    };
    match (method, suffix) {
        ("GET", Some("restore-metadata")) => rp_missing(),
        (_, Some("disassociate")) | ("DELETE", Some("parentAssociation")) => rp_missing(),
        ("GET", Some("index")) | ("POST", Some("index")) => {
            // GetRecoveryPointIndexDetails / UpdateRecoveryPointIndexSettings:
            // moto has no handler — faithfully absent.
            not_supported(req)
        }
        ("GET", None) | ("POST", None) | ("DELETE", None) => rp_missing(),
        _ => not_supported(req),
    }
}

fn rp_not_found(rp_arn: &str) -> Response {
    not_found(format!("Recovery point {rp_arn} not found"))
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

fn tags_routes(
    req: &Request,
    st: &mut BackupStore,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    let arn = path_param(rest);
    match method {
        // GET /tags/{arn}/ -> ListTags
        "GET" => ok(json!({ "Tags": st.list_tags(&arn) })),
        "POST" => {
            if !resource_arn_known(st, &arn) {
                return invalid_param(format!("Unknown resource {arn}"));
            }
            st.tag_resource(&arn, body.get("Tags"));
            ok(json!({}))
        }
        _ => not_supported(req),
    }
}

// ---------------------------------------------------------------------------
// Backup jobs
// ---------------------------------------------------------------------------

fn backup_jobs_routes(
    req: &Request,
    st: &mut BackupStore,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    match (method, rest) {
        ("PUT", []) => start_backup_job(req, st, body),
        ("GET", []) => list_backup_jobs(req, st),
        ("GET", [job_id]) => match st.backup_jobs.get(*job_id) {
            Some(job) => ok(backup_job_public(job)),
            None => not_found(format!("Backup job {job_id} not found")),
        },
        ("POST", [job_id]) => {
            let Some(job) = st.backup_jobs.get_mut(*job_id) else {
                return not_found(format!("Backup job {job_id} not found"));
            };
            let state = job["state"].as_str().unwrap_or("");
            if state != "CREATED" && state != "RUNNING" {
                return invalid_request(format!(
                    "Backup job {job_id} is not in a cancelable state"
                ));
            }
            job["state"] = json!("ABORTED");
            job["completion_date"] = json!(now());
            job["status_message"] = json!("Backup job was stopped by user");
            ok(json!({}))
        }
        _ => not_supported(req),
    }
}

fn start_backup_job(req: &Request, st: &mut BackupStore, body: &Value) -> Response {
    let vault_name = body
        .get("BackupVaultName")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    if !st.vaults.contains_key(&vault_name) {
        return not_found(format!("Backup vault {vault_name} not found"));
    }
    let job_id = Uuid::new_v4().to_string();
    let resource_arn = body
        .get("ResourceArn")
        .and_then(Value::as_str)
        .unwrap_or("");
    let job = json!({
        "backup_job_id": job_id,
        "backup_vault_name": vault_name,
        "backup_vault_arn": vault_arn(req, &vault_name),
        "resource_arn": resource_arn,
        "iam_role_arn": body.get("IamRoleArn").and_then(Value::as_str).unwrap_or(""),
        "creation_date": now(),
        "completion_date": now(),
        "state": "COMPLETED",
        "status_message": "",
        "percent_done": "100.0",
        "backup_size_in_bytes": 0,
        "resource_type": derive_resource_type(resource_arn),
        "recovery_point_arn": recovery_point_arn(req),
        "start_window_minutes": body.get("StartWindowMinutes").and_then(Value::as_i64).unwrap_or(480),
        "complete_window_minutes": body.get("CompleteWindowMinutes").and_then(Value::as_i64).unwrap_or(10080),
        "account_id": req.account.to_string(),
        "lifecycle": body.get("Lifecycle").cloned().unwrap_or(json!({})),
        "backup_options": body.get("BackupOptions").cloned().unwrap_or(json!({})),
    });
    st.backup_jobs.insert(job_id.clone(), job.clone());
    ok(json!({
        "BackupJobId": job_id,
        "RecoveryPointArn": job["recovery_point_arn"],
        "CreationDate": job["creation_date"],
    }))
}

/// Public restore-job body (mirrors Moto's RestoreJob.to_dict).
fn restore_job_public(job: &Value) -> Value {
    let mut out = json!({
        "RestoreJobId": job["restore_job_id"],
        "RecoveryPointArn": job["recovery_point_arn"],
        "IamRoleArn": job["iam_role_arn"],
        "CreationDate": job["creation_date"],
        "Status": job["status"],
        "StatusMessage": job["status_message"],
        "PercentDone": job["percent_done"],
        "BackupSizeInBytes": job["backup_size_in_bytes"],
        "CreatedResourceArn": job["created_resource_arn"],
        "AccountId": job["account_id"],
        "ResourceType": job["resource_type"],
    });
    if let Some(c) = job["completion_date"].as_f64() {
        out["CompletionDate"] = json!(c);
    }
    if let Some(v) = job["validation_status"].as_str().filter(|v| !v.is_empty()) {
        out["ValidationStatus"] = json!(v);
    }
    if let Some(v) = job["validation_status_message"]
        .as_str()
        .filter(|v| !v.is_empty())
    {
        out["ValidationStatusMessage"] = json!(v);
    }
    out
}

/// Public backup-job body (mirrors Moto's BackupJob.to_dict).
fn backup_job_public(job: &Value) -> Value {
    let mut out = json!({
        "BackupJobId": job["backup_job_id"],
        "BackupVaultName": job["backup_vault_name"],
        "BackupVaultArn": job["backup_vault_arn"],
        "ResourceArn": job["resource_arn"],
        "IamRoleArn": job["iam_role_arn"],
        "CreationDate": job["creation_date"],
        "State": job["state"],
        "StatusMessage": job["status_message"],
        "PercentDone": job["percent_done"],
        "BackupSizeInBytes": job["backup_size_in_bytes"],
        "ResourceType": job["resource_type"],
        "RecoveryPointArn": job["recovery_point_arn"],
        "StartWindowMinutes": job["start_window_minutes"],
        "CompleteWindowMinutes": job["complete_window_minutes"],
        "AccountId": job["account_id"],
    });
    if let Some(c) = job["completion_date"].as_f64().filter(|c| *c != 0.0) {
        out["CompletionDate"] = json!(c);
    }
    let empty_obj = |v: &Value| v.as_object().is_some_and(|o| o.is_empty());
    if !empty_obj(&job["lifecycle"]) {
        out["RecoveryPointLifecycle"] = job["lifecycle"].clone();
    }
    if !empty_obj(&job["backup_options"]) {
        out["BackupOptions"] = job["backup_options"].clone();
    }
    out
}

fn list_backup_jobs(req: &Request, st: &mut BackupStore) -> Response {
    let vault = query_param(req, "backupVaultName");
    let state = query_param(req, "state");
    let resource_arn = query_param(req, "resourceArn");
    let resource_type = query_param(req, "resourceType");
    let account = query_param(req, "accountId");
    let list: Vec<Value> = st
        .backup_jobs
        .values()
        .filter(|j| {
            let matches = |filter: &Option<String>, value: Option<&str>| {
                filter.as_deref().is_none_or(|f| f == value.unwrap_or(""))
            };
            matches(&vault, j["backup_vault_name"].as_str())
                && matches(&state, j["state"].as_str())
                && matches(&resource_arn, j["resource_arn"].as_str())
                && matches(&resource_type, j["resource_type"].as_str())
                && matches(&account, j["account_id"].as_str())
        })
        .map(backup_job_public)
        .collect();
    ok(json!({ "BackupJobs": list }))
}

// ---------------------------------------------------------------------------
// Copy / restore jobs
// ---------------------------------------------------------------------------

fn copy_jobs_routes(
    req: &Request,
    st: &mut BackupStore,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    match (method, rest) {
        ("PUT", []) => {
            let job_id = Uuid::new_v4().to_string();
            let source = body
                .get("SourceBackupVaultName")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let job = json!({
                "copy_job_id": job_id,
                "source_backup_vault_arn": vault_arn(req, &source),
                "source_recovery_point_arn": body.get("RecoveryPointArn").and_then(Value::as_str).unwrap_or(""),
                "destination_backup_vault_arn": body.get("DestinationBackupVaultArn").and_then(Value::as_str).unwrap_or(""),
                "destination_recovery_point_arn": recovery_point_arn(req),
                "iam_role_arn": body.get("IamRoleArn").and_then(Value::as_str).unwrap_or(""),
                "creation_date": now(),
                "completion_date": now(),
                "state": "COMPLETED",
                "status_message": "",
                "backup_size_in_bytes": 0,
                "account_id": req.account.to_string(),
                "resource_type": "EBS",
                "resource_arn": body.get("RecoveryPointArn").and_then(Value::as_str).unwrap_or(""),
                "lifecycle": body.get("Lifecycle").cloned().unwrap_or(json!({})),
            });
            st.copy_jobs.insert(job_id.clone(), job.clone());
            ok(json!({ "CopyJobId": job_id, "CreationDate": job["creation_date"] }))
        }
        ("GET", [id]) => match st.copy_jobs.get(*id) {
            Some(job) => ok(json!({ "CopyJob": job.clone() })),
            None => not_found(format!("Copy job {id} not found")),
        },
        ("GET", []) => {
            let state = query_param(req, "state");
            let list: Vec<Value> = st
                .copy_jobs
                .values()
                .filter(|j| {
                    state
                        .as_deref()
                        .is_none_or(|s| j["state"].as_str() == Some(s))
                })
                .cloned()
                .collect();
            ok(json!({ "CopyJobs": list }))
        }
        _ => not_supported(req),
    }
}

fn restore_jobs_routes(
    req: &Request,
    st: &mut BackupStore,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    match (method, rest) {
        ("PUT", []) => {
            let job_id = Uuid::new_v4().to_string();
            let job = json!({
                "restore_job_id": job_id,
                "recovery_point_arn": body.get("RecoveryPointArn").and_then(Value::as_str).unwrap_or(""),
                "iam_role_arn": body.get("IamRoleArn").and_then(Value::as_str).unwrap_or(""),
                "metadata": body.get("Metadata").cloned().unwrap_or(json!({})),
                "resource_type": body.get("ResourceType").and_then(Value::as_str).unwrap_or(""),
                "creation_date": now(),
                "completion_date": now(),
                "status": "COMPLETED",
                "status_message": "",
                "percent_done": "100.0",
                "backup_size_in_bytes": 0,
                "account_id": req.account.to_string(),
                "created_resource_arn": format!(
                    "{}:restored-resource:{}",
                    arn_prefix(req),
                    Uuid::new_v4()
                ),
                "validation_status": Value::Null,
                "validation_status_message": Value::Null,
            });
            st.restore_jobs.insert(job_id.clone(), job.clone());
            ok(json!({ "RestoreJobId": job_id }))
        }
        ("GET", [id]) => {
            let Some(job) = st.restore_jobs.get(*id) else {
                return not_found(format!("Restore job {id} not found"));
            };
            ok(restore_job_public(job))
        }
        ("GET", [id, "metadata"]) => {
            let Some(job) = st.restore_jobs.get(*id) else {
                return not_found(format!("Restore job {id} not found"));
            };
            ok(json!({
                "RestoreJobId": job["restore_job_id"],
                "Metadata": job["metadata"],
            }))
        }
        ("PUT", [id, "validations"]) => {
            let Some(job) = st.restore_jobs.get_mut(*id) else {
                return not_found(format!("Restore job {id} not found"));
            };
            job["validation_status"] = body.get("ValidationStatus").cloned().unwrap_or(json!(""));
            if let Some(m) = body.get("ValidationStatusMessage") {
                job["validation_status_message"] = m.clone();
            }
            ok_with_status(204, json!({}))
        }
        ("GET", []) => {
            let status = query_param(req, "status");
            let resource_type = query_param(req, "resourceType");
            let account = query_param(req, "accountId");
            let list: Vec<Value> = st
                .restore_jobs
                .values()
                .filter(|j| {
                    let matches = |filter: &Option<String>, value: Option<&str>| {
                        filter.as_deref().is_none_or(|f| f == value.unwrap_or(""))
                    };
                    matches(&status, j["status"].as_str())
                        && matches(&resource_type, j["resource_type"].as_str())
                        && matches(&account, j["account_id"].as_str())
                })
                .map(|j| {
                    json!({
                        "RestoreJobId": j["restore_job_id"],
                        "RecoveryPointArn": j["recovery_point_arn"],
                        "CreationDate": j["creation_date"],
                        "Status": j["status"],
                        "AccountId": j["account_id"],
                        "ResourceType": j["resource_type"],
                        "CreatedResourceArn": j["created_resource_arn"],
                    })
                })
                .collect();
            ok(json!({ "RestoreJobs": list }))
        }
        _ => not_supported(req),
    }
}

// ---------------------------------------------------------------------------
// Frameworks
// ---------------------------------------------------------------------------

fn frameworks_routes(
    req: &Request,
    st: &mut BackupStore,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    match (method, rest) {
        ("POST", []) => {
            let name = body
                .get("FrameworkName")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if st.frameworks.contains_key(&name) {
                return already_exists(format!("Framework {name} already exists"));
            }
            let controls = body.get("FrameworkControls").cloned().unwrap_or(json!([]));
            let arn = format!(
                "{}:framework:{}-{}",
                arn_prefix(req),
                name,
                &Uuid::new_v4().to_string()[..8]
            );
            if let Some(t) = body.get("FrameworkTags") {
                st.tag_resource(&arn, Some(t));
            }
            st.frameworks.insert(
                name.clone(),
                json!({
                    "framework_name": name.clone(),
                    "framework_arn": arn.clone(),
                    "framework_description": body
                        .get("FrameworkDescription")
                        .and_then(Value::as_str)
                        .unwrap_or(""),
                    "framework_controls": controls.clone(),
                    "number_of_controls": controls.as_array().map(|c| c.len()).unwrap_or(0),
                    "creation_time": now(),
                    "deployment_status": "COMPLETED",
                }),
            );
            ok(json!({ "FrameworkName": name, "FrameworkArn": arn }))
        }
        ("GET", []) => {
            let list: Vec<Value> = st
                .frameworks
                .values()
                .map(|f| {
                    json!({
                        "FrameworkName": f["framework_name"],
                        "FrameworkArn": f["framework_arn"],
                        "FrameworkDescription": f["framework_description"],
                        "NumberOfControls": f["number_of_controls"],
                        "CreationTime": f["creation_time"],
                        "DeploymentStatus": f["deployment_status"],
                    })
                })
                .collect();
            ok(json!({ "Frameworks": list }))
        }
        ("GET", [name]) => match st.frameworks.get(*name) {
            Some(f) => ok(json!({
                "FrameworkName": f["framework_name"],
                "FrameworkArn": f["framework_arn"],
                "FrameworkDescription": f["framework_description"],
                "FrameworkControls": f["framework_controls"],
                "CreationTime": f["creation_time"],
                "DeploymentStatus": f["deployment_status"],
            })),
            None => not_found(format!("Framework {name} not found")),
        },
        ("PUT", [name]) => {
            let Some(f) = st.frameworks.get_mut(*name) else {
                return not_found(format!("Framework {name} not found"));
            };
            if let Some(d) = body.get("FrameworkDescription") {
                f["framework_description"] = d.clone();
            }
            if let Some(c) = body.get("FrameworkControls") {
                f["framework_controls"] = c.clone();
                f["number_of_controls"] = json!(c.as_array().map(|c| c.len()).unwrap_or(0));
            }
            ok(json!({
                "FrameworkName": f["framework_name"],
                "FrameworkArn": f["framework_arn"],
            }))
        }
        ("DELETE", [name]) => {
            if st.frameworks.remove(*name).is_none() {
                return not_found(format!("Framework {name} not found"));
            }
            ok(json!({}))
        }
        _ => not_supported(req),
    }
}

// ---------------------------------------------------------------------------
// Report plans / jobs
// ---------------------------------------------------------------------------

fn report_plans_routes(
    req: &Request,
    st: &mut BackupStore,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    match (method, rest) {
        ("POST", []) => {
            let name = body
                .get("ReportPlanName")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let arn = format!("{}:report-plan:{}", arn_prefix(req), name);
            let plan = json!({
                "report_plan_name": name.clone(),
                "report_plan_arn": arn.clone(),
                "report_plan_description": body
                    .get("ReportPlanDescription")
                    .and_then(Value::as_str)
                    .unwrap_or(""),
                "report_setting": body.get("ReportSetting").cloned().unwrap_or(json!({})),
                "report_delivery_channel": body
                    .get("ReportDeliveryChannel")
                    .cloned()
                    .unwrap_or(json!({})),
                "deployment_status": "COMPLETED",
                "creation_time": now(),
            });
            st.report_plans.insert(name.clone(), plan.clone());
            ok(json!({
                "ReportPlanName": name,
                "ReportPlanArn": arn,
                "CreationTime": plan["creation_time"],
            }))
        }
        ("GET", []) => {
            let plans: Vec<Value> = st.report_plans.values().map(report_plan_public).collect();
            ok(json!({ "ReportPlans": plans }))
        }
        ("GET", [name]) => match st.report_plans.get(*name) {
            Some(p) => ok(json!({ "ReportPlan": report_plan_public(p) })),
            None => not_found(format!("Report Plan {name} not found")),
        },
        ("DELETE", [name]) => {
            st.report_plans.remove(*name);
            ok(json!({}))
        }
        ("PUT", [name]) => {
            let Some(p) = st.report_plans.get_mut(*name) else {
                return not_found(format!("Report plan {name} not found"));
            };
            if let Some(d) = body.get("ReportPlanDescription") {
                p["report_plan_description"] = d.clone();
            }
            if let Some(d) = body.get("ReportDeliveryChannel") {
                p["report_delivery_channel"] = d.clone();
            }
            if let Some(d) = body.get("ReportSetting") {
                p["report_setting"] = d.clone();
            }
            ok(json!({
                "ReportPlanName": p["report_plan_name"],
                "ReportPlanArn": p["report_plan_arn"],
                "CreationTime": p["creation_time"],
            }))
        }
        _ => not_supported(req),
    }
}

/// Public report-plan body (a serializer would skip empty descriptions).
fn report_plan_public(p: &Value) -> Value {
    let mut out = json!({
        "ReportPlanArn": p["report_plan_arn"],
        "ReportPlanName": p["report_plan_name"],
        "ReportSetting": p["report_setting"],
        "ReportDeliveryChannel": p["report_delivery_channel"],
        "DeploymentStatus": p["deployment_status"],
        "CreationTime": p["creation_time"],
    });
    if let Some(d) = p["report_plan_description"]
        .as_str()
        .filter(|d| !d.is_empty())
    {
        out["ReportPlanDescription"] = json!(d);
    }
    out
}

fn report_jobs_routes(
    req: &Request,
    st: &mut BackupStore,
    method: &str,
    rest: &[&str],
) -> Response {
    // StartReportJob is POST /audit/report-jobs/{reportPlanName}.
    match (method, rest) {
        ("POST", [plan_name]) => start_report_job(st, plan_name),
        ("GET", []) => {
            let plan_filter = query_param(req, "reportPlanName");
            let status = query_param(req, "status");
            let list: Vec<Value> = st
                .report_jobs
                .values()
                .filter(|j| {
                    plan_filter
                        .as_deref()
                        .is_none_or(|p| j["ReportPlanArn"].as_str().unwrap_or("").ends_with(p))
                        && status
                            .as_deref()
                            .is_none_or(|s| j["Status"].as_str() == Some(s))
                })
                .cloned()
                .collect();
            ok(json!({ "ReportJobs": list }))
        }
        ("GET", [id]) => {
            let Some(job) = st.report_jobs.get(*id) else {
                return not_found(format!("Report job {id} not found"));
            };
            ok(json!({ "ReportJob": job }))
        }
        _ => not_supported(req),
    }
}

/// moto: the job is created already COMPLETED, its destination is the plan's
/// delivery channel as posted.
fn start_report_job(st: &mut BackupStore, plan_name: &str) -> Response {
    let Some(plan) = st.report_plans.get(plan_name) else {
        return not_found(format!("Report plan {plan_name} not found"));
    };
    let job_id = Uuid::new_v4().to_string();
    let arn = plan["report_plan_arn"].clone();
    let destination = plan["report_delivery_channel"].clone();
    st.report_jobs.insert(
        job_id.clone(),
        json!({
            "ReportJobId": job_id,
            "ReportPlanArn": arn,
            "CreationTime": now(),
            "CompletionTime": now(),
            "Status": "COMPLETED",
            "StatusMessage": "",
            "ReportDestination": destination,
        }),
    );
    ok(json!({ "ReportJobId": job_id }))
}

/// The response key for a job-summaries URL family, if recognized.
fn summary_key(family: &str) -> Option<&'static str> {
    if family.contains("scan-job-summaries") {
        Some("ScanJobSummaries")
    } else if family.starts_with("backup") {
        Some("BackupJobSummaries")
    } else if family.starts_with("copy") {
        Some("CopyJobSummaries")
    } else if family.starts_with("restore") {
        Some("RestoreJobSummaries")
    } else {
        None
    }
}

/// Job summaries aggregate by state the way moto does.
fn summaries(req: &Request, family: &str, st: &BackupStore) -> Value {
    let (jobs, status_key): (Vec<Value>, &str) = if family.starts_with("backup") {
        (st.backup_jobs.values().cloned().collect(), "state")
    } else if family.starts_with("copy") {
        (st.copy_jobs.values().cloned().collect(), "state")
    } else {
        (st.restore_jobs.values().cloned().collect(), "status")
    };
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    for j in &jobs {
        let key = j[status_key].as_str().unwrap_or("").to_string();
        *counts.entry(key).or_insert(0) += 1;
    }
    Value::Array(
        counts
            .into_iter()
            .map(|(state, count)| {
                json!({
                    "Region": req.region,
                    "AccountId": req.account.to_string(),
                    "State": state,
                    "Count": count,
                })
            })
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// Legal holds
// ---------------------------------------------------------------------------

fn legal_holds_routes(
    req: &Request,
    st: &mut BackupStore,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    match (method, rest) {
        ("POST", []) => {
            let id = Uuid::new_v4().to_string();
            let arn = format!("{}:legal-hold:{}", arn_prefix(req), id);
            let hold = json!({
                "legal_hold_id": id.clone(),
                "legal_hold_arn": arn.clone(),
                "title": body.get("Title").and_then(Value::as_str).unwrap_or(""),
                "description": body.get("Description").and_then(Value::as_str).unwrap_or(""),
                "status": "ACTIVE",
                "creation_date": now(),
                "cancellation_date": Value::Null,
                "recovery_point_selection": body
                    .get("RecoveryPointSelection")
                    .cloned()
                    .unwrap_or(json!({})),
                "retain_record_until": Value::Null,
            });
            st.legal_holds.insert(id.clone(), hold.clone());
            ok(json!({
                "LegalHoldId": id,
                "LegalHoldArn": arn,
                "Title": hold["title"],
                "Description": hold["description"],
                "Status": hold["status"],
                "CreationDate": hold["creation_date"],
                "RecoveryPointSelection": hold["recovery_point_selection"],
            }))
        }
        ("GET", []) => {
            let holds: Vec<Value> = st
                .legal_holds
                .values()
                .map(|h| {
                    let mut out = json!({
                        "LegalHoldId": h["legal_hold_id"],
                        "LegalHoldArn": h["legal_hold_arn"],
                        "Title": h["title"],
                        "Description": h["description"],
                        "Status": h["status"],
                        "CreationDate": h["creation_date"],
                    });
                    if let Some(c) = h["cancellation_date"].as_f64() {
                        out["CancellationDate"] = json!(c);
                    }
                    out
                })
                .collect();
            ok(json!({"LegalHolds": holds}))
        }
        ("GET", [id]) => {
            let Some(h) = st.legal_holds.get(*id) else {
                return not_found(format!("Legal hold {id} not found"));
            };
            let mut out = json!({
                "LegalHoldId": h["legal_hold_id"],
                "LegalHoldArn": h["legal_hold_arn"],
                "Title": h["title"],
                "Description": h["description"],
                "Status": h["status"],
                "CreationDate": h["creation_date"],
                "RecoveryPointSelection": h["recovery_point_selection"],
            });
            if let Some(c) = h["cancellation_date"].as_f64() {
                out["CancellationDate"] = json!(c);
            }
            ok(out)
        }
        ("DELETE", [id]) => {
            if !st.legal_holds.contains_key(*id) {
                return not_found(format!("Legal hold {id} not found"));
            }
            let hold = st.legal_holds.get_mut(*id).unwrap();
            if hold["status"].as_str() == Some("CANCELED") {
                return invalid_request(format!("Legal hold {id} is already canceled"));
            }
            hold["status"] = json!("CANCELED");
            hold["cancellation_date"] = json!(now());
            if let Some(days) =
                query_param(req, "retainRecordInDays").and_then(|d| d.parse::<i64>().ok())
            {
                hold["retain_record_until"] = json!(now() + (days as f64 * 86400.0));
            }
            ok(json!({}))
        }
        ("GET", [id, "recovery-points"]) => {
            if !st.legal_holds.contains_key(*id) {
                return not_found(format!("Legal hold {id} not found"));
            }
            let list: Vec<Value> = st
                .recovery_points
                .values()
                .flat_map(|m| m.values().cloned().collect::<Vec<_>>())
                .collect();
            ok(json!({"RecoveryPoints": list}))
        }
        _ => not_supported(req),
    }
}

// ---------------------------------------------------------------------------
// Protected resources
// ---------------------------------------------------------------------------

fn protected_resources_routes(
    req: &Request,
    st: &mut BackupStore,
    method: &str,
    _rest: &[&str],
    _body: &Value,
) -> Response {
    if method != "GET" {
        return not_supported(req);
    }
    // moto splits the raw path: /resources/{arn}[/suffix]
    let Some(after) = req.path.split("/resources/").nth(1) else {
        return not_supported(req);
    };
    let after = after.trim_matches('/');
    if after.is_empty() {
        return ok(json!({"Results": []}));
    }
    if let Some((arn, _)) = after.split_once("/recovery-points") {
        let _ = arn;
        return ok(json!({"RecoveryPoints": []}));
    }
    if let Some((arn, _)) = after.split_once("/restore-jobs") {
        let resource_arn = percent_decode(arn);
        let list: Vec<Value> = st
            .restore_jobs
            .values()
            .filter(|j| {
                j["recovery_point_arn"].as_str() == Some(resource_arn.as_str())
                    || j["created_resource_arn"].as_str() == Some(resource_arn.as_str())
            })
            .map(|j| {
                json!({
                    "RestoreJobId": j["restore_job_id"],
                    "RecoveryPointArn": j["recovery_point_arn"],
                    "CreationDate": j["creation_date"],
                    "Status": j["status"],
                    "AccountId": j["account_id"],
                    "ResourceType": j["resource_type"],
                    "CreatedResourceArn": j["created_resource_arn"],
                })
            })
            .collect();
        return ok(json!({"RestoreJobs": list}));
    }
    not_found(format!(
        "Resource {} is not protected",
        percent_decode(after)
    ))
}

// ---------------------------------------------------------------------------
// Restore testing plans / selections
// ---------------------------------------------------------------------------

fn rtp_public(p: &Value, with_selection: bool) -> Value {
    let mut out = json!({
        "CreationTime": p["creation_time"],
        "LastUpdatedTime": p["last_updated_time"],
        "RestoreTestingPlanArn": p["restore_testing_plan_arn"],
        "RestoreTestingPlanName": p["restore_testing_plan_name"],
        "ScheduleExpression": p["schedule_expression"],
        "ScheduleExpressionTimezone": p["schedule_expression_timezone"],
        "StartWindowHours": p["start_window_hours"],
    });
    if with_selection {
        out["RecoveryPointSelection"] = p["recovery_point_selection"].clone();
    }
    out
}

fn restore_testing_routes(
    req: &Request,
    st: &mut BackupStore,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    match (method, rest) {
        ("PUT", []) => {
            let plan_body = body.get("RestoreTestingPlan").cloned().unwrap_or(json!({}));
            let name = plan_body
                .get("RestoreTestingPlanName")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if st.restore_testing_plans.contains_key(&name) {
                return already_exists(format!("Restore testing plan {name} already exists"));
            }
            let arn = format!("{}:restore-testing-plan:{}", arn_prefix(req), name);
            let plan = json!({
                "restore_testing_plan_arn": arn.clone(),
                "restore_testing_plan_name": name.clone(),
                "schedule_expression": plan_body.get("ScheduleExpression").and_then(Value::as_str).unwrap_or(""),
                "schedule_expression_timezone": plan_body
                    .get("ScheduleExpressionTimezone")
                    .and_then(Value::as_str)
                    .unwrap_or("Etc/UTC"),
                "start_window_hours": plan_body
                    .get("StartWindowHours")
                    .and_then(Value::as_i64)
                    .unwrap_or(24),
                "recovery_point_selection": plan_body
                    .get("RecoveryPointSelection")
                    .cloned()
                    .unwrap_or(json!({})),
                "creation_time": now(),
                "last_updated_time": now(),
            });
            if let Some(t) = body.get("Tags") {
                st.tag_resource(&arn, Some(t));
            }
            st.restore_testing_plans.insert(name.clone(), plan.clone());
            ok_with_status(
                201,
                json!({
                    "CreationTime": plan["creation_time"],
                    "RestoreTestingPlanArn": arn,
                    "RestoreTestingPlanName": name,
                }),
            )
        }
        ("GET", []) => {
            let plans: Vec<Value> = st
                .restore_testing_plans
                .values()
                .map(|p| rtp_public(p, false))
                .collect();
            ok(json!({"RestoreTestingPlans": plans}))
        }
        ("GET", [name]) => match st.restore_testing_plans.get(*name) {
            Some(p) => ok(json!({ "RestoreTestingPlan": rtp_public(p, true) })),
            None => not_found(format!("Restore testing plan {name} not found")),
        },
        ("DELETE", [name]) => {
            st.restore_testing_plans.remove(*name);
            st.restore_testing_selections.remove(*name);
            ok_with_status(204, json!({}))
        }
        ("PUT", [name]) => {
            let Some(p) = st.restore_testing_plans.get_mut(*name) else {
                return not_found(format!("Restore testing plan {name} not found"));
            };
            let plan_body = body.get("RestoreTestingPlan").cloned().unwrap_or(json!({}));
            if let Some(v) = plan_body.get("ScheduleExpression") {
                p["schedule_expression"] = v.clone();
            }
            if let Some(v) = plan_body.get("RecoveryPointSelection") {
                p["recovery_point_selection"] = v.clone();
            }
            if let Some(v) = plan_body.get("ScheduleExpressionTimezone") {
                p["schedule_expression_timezone"] = v.clone();
            }
            if let Some(v) = plan_body.get("StartWindowHours") {
                p["start_window_hours"] = v.clone();
            }
            p["last_updated_time"] = json!(now());
            ok(json!({
                "CreationTime": p["creation_time"],
                "RestoreTestingPlanArn": p["restore_testing_plan_arn"],
                "RestoreTestingPlanName": p["restore_testing_plan_name"],
                "UpdateTime": p["last_updated_time"],
            }))
        }
        ("PUT", [name, "selections"]) => {
            let Some(plan_arn) = st
                .restore_testing_plans
                .get(*name)
                .map(|p| p["restore_testing_plan_arn"].clone())
            else {
                return not_found(format!("Restore testing plan {name} not found"));
            };
            let sel_body = body
                .get("RestoreTestingSelection")
                .cloned()
                .unwrap_or(json!({}));
            let sel_name = sel_body_name(&sel_body);
            let selection = json!({
                "creation_time": now(),
                "iam_role_arn": sel_body.get("IamRoleArn").cloned().unwrap_or(json!("")),
                "protected_resource_arns": sel_body
                    .get("ProtectedResourceArns")
                    .cloned()
                    .unwrap_or(json!([])),
                "protected_resource_conditions": sel_body
                    .get("ProtectedResourceConditions")
                    .cloned()
                    .unwrap_or(json!({})),
                "restore_metadata_overrides": sel_body
                    .get("RestoreMetadataOverrides")
                    .cloned()
                    .unwrap_or(json!({})),
                "restore_testing_selection_name": sel_name.clone(),
                "validation_window_hours": sel_body
                    .get("ValidationWindowHours")
                    .and_then(Value::as_i64)
                    .unwrap_or(168),
            });
            st.restore_testing_selections
                .entry(name.to_string())
                .or_default()
                .insert(sel_name.clone(), selection.clone());
            ok_with_status(
                201,
                json!({
                    "CreationTime": now(),
                    "RestoreTestingPlanArn": plan_arn,
                    "RestoreTestingPlanName": name,
                    "RestoreTestingSelectionName": sel_name,
                }),
            )
        }
        ("GET", [name, "selections"]) => {
            if !st.restore_testing_plans.contains_key(*name) {
                return not_found(format!("Restore testing plan {name} not found"));
            }
            ok(json!({
                "RestoreTestingSelections": st
                    .restore_testing_selections
                    .get(*name)
                    .map(|m| m.values().cloned().collect::<Vec<_>>())
                    .unwrap_or_default()
            }))
        }
        ("GET", [name, "selections", sel]) => {
            if !st.restore_testing_plans.contains_key(*name) {
                return not_found(format!("Restore testing plan {name} not found"));
            }
            match st
                .restore_testing_selections
                .get(*name)
                .and_then(|m| m.get(*sel))
            {
                Some(sel_value) => ok(json!({"RestoreTestingSelection": sel_value.clone()})),
                None => not_found(format!("Restore testing selection {sel} not found")),
            }
        }
        ("DELETE", [name, "selections", sel]) => {
            if let Some(map) = st.restore_testing_selections.get_mut(*name) {
                map.remove(*sel);
            }
            ok_with_status(204, json!({}))
        }
        ("PUT", [name, "selections", sel]) => {
            let Some(map) = st.restore_testing_selections.get_mut(*name) else {
                return not_found(format!("Restore testing plan {name} not found"));
            };
            let Some(s) = map.get_mut(*sel) else {
                return not_found(format!("Restore testing selection {sel} not found"));
            };
            let sel_body = body
                .get("RestoreTestingSelection")
                .cloned()
                .unwrap_or(json!({}));
            if let Some(v) = sel_body.get("IamRoleArn") {
                s["iam_role_arn"] = v.clone();
            }
            if let Some(v) = sel_body.get("ProtectedResourceArns") {
                s["protected_resource_arns"] = v.clone();
            }
            if let Some(v) = sel_body.get("ProtectedResourceConditions") {
                s["protected_resource_conditions"] = v.clone();
            }
            if let Some(v) = sel_body.get("RestoreMetadataOverrides") {
                s["restore_metadata_overrides"] = v.clone();
            }
            if let Some(v) = sel_body.get("ValidationWindowHours") {
                s["validation_window_hours"] = v.clone();
            }
            ok_with_status(
                201,
                json!({
                    "CreationTime": s["creation_time"],
                    "RestoreTestingPlanArn": st
                        .restore_testing_plans
                        .get(*name)
                        .map(|p| p["restore_testing_plan_arn"].clone())
                        .unwrap_or(json!("")),
                    "RestoreTestingPlanName": name,
                    "RestoreTestingSelectionName": s["restore_testing_selection_name"],
                    "UpdateTime": s["creation_time"],
                }),
            )
        }
        _ => not_supported(req),
    }
}

fn sel_body_name(sel_body: &Value) -> String {
    sel_body
        .get("RestoreTestingSelectionName")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

// ---------------------------------------------------------------------------
// Logically air-gapped vaults
// ---------------------------------------------------------------------------

fn air_gapped_routes(
    req: &Request,
    st: &mut BackupStore,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    match (method, rest) {
        ("PUT", [name]) => {
            if st.logically_air_gapped_vaults.contains_key(*name) {
                return already_exists(format!("Logically air-gapped vault {name} already exists"));
            }
            let vault = json!({
                "backup_vault_arn": vault_arn(req, name),
                "backup_vault_name": name,
                "creation_date": now(),
                "max_retention_days": body.get("MaxRetentionDays").and_then(Value::as_i64).unwrap_or(36500),
                "min_retention_days": body.get("MinRetentionDays").and_then(Value::as_i64).unwrap_or(7),
                "vault_state": "AVAILABLE",
                "vault_type": "LOGICALLY_AIR_GAPPED_BACKUP_VAULT",
            });
            st.logically_air_gapped_vaults
                .insert(name.to_string(), vault.clone());
            ok(json!({
                "BackupVaultArn": vault["backup_vault_arn"],
                "BackupVaultName": name,
                "CreationDate": vault["creation_date"],
                "MaxRetentionDays": vault["max_retention_days"],
                "MinRetentionDays": vault["min_retention_days"],
                "VaultState": vault["vault_state"],
                "VaultType": vault["vault_type"],
            }))
        }
        ("GET", []) => {
            let list: Vec<Value> = st.logically_air_gapped_vaults.values().cloned().collect();
            ok(json!({"LogicallyAirGappedBackupVaults": list}))
        }
        ("GET", [name]) => match st.logically_air_gapped_vaults.get(*name) {
            Some(v) => ok(v.clone()),
            None => not_found(format!("Logically air-gapped vault {name} not found")),
        },
        ("DELETE", [name]) => {
            if st.logically_air_gapped_vaults.remove(*name).is_none() {
                return not_found(format!("Logically air-gapped vault {name} not found"));
            }
            ok(json!({}))
        }
        _ => not_supported(req),
    }
}

// ---------------------------------------------------------------------------
// Templates
// ---------------------------------------------------------------------------

fn template_routes(
    req: &Request,
    _st: &mut BackupStore,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    match (method, rest) {
        ("GET", ["plans"]) => ok(json!({
            "BackupPlanTemplatesList": BACKUP_PLAN_TEMPLATES
                .iter()
                .map(|(id, name)| json!({
                    "BackupPlanTemplateId": id,
                    "BackupPlanTemplateName": name,
                }))
                .collect::<Vec<_>>()
        })),
        ("GET", ["plans", template_id, "toPlan"]) => {
            match backup_plan_template_details(template_id) {
                Some(details) => ok(json!({ "BackupPlanDocument": details })),
                None => not_found(format!("Backup plan template {template_id} not found")),
            }
        }
        ("POST", ["json", "toPlan"]) => {
            // /backup/template/json/toPlan
            let template_json = body
                .get("BackupPlanTemplateJson")
                .and_then(Value::as_str)
                .unwrap_or("{}");
            let plan: Value = serde_json::from_str(template_json).unwrap_or(json!({}));
            ok(json!({ "BackupPlan": plan }))
        }
        _ => not_supported(req),
    }
}

// ---------------------------------------------------------------------------
// Scan jobs / tiering (fakes)
// ---------------------------------------------------------------------------

fn scan_job_routes(
    req: &Request,
    st: &mut BackupStore,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    match (method, rest) {
        ("PUT", []) => {
            let vault_name = body
                .get("BackupVaultName")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if !st.vaults.contains_key(&vault_name) {
                return not_found(format!("Backup vault {vault_name} not found"));
            }
            let job_id = Uuid::new_v4().to_string();
            let mode = body
                .get("ScanMode")
                .and_then(Value::as_str)
                .unwrap_or("FULL_SCAN");
            let iam = body.get("IamRoleArn").and_then(Value::as_str).unwrap_or("");
            st.scan_jobs.insert(
                job_id.clone(),
                json!({
                    "scan_job_id": job_id.clone(),
                    "backup_vault_name": vault_name.clone(),
                    "backup_vault_arn": vault_arn(req, &vault_name),
                    "recovery_point_arn": body.get("RecoveryPointArn").and_then(Value::as_str).unwrap_or(""),
                    "iam_role_arn": iam,
                    "scanner_role_arn": body.get("ScannerRoleArn").and_then(Value::as_str).unwrap_or(iam),
                    "scan_mode": mode,
                    "state": "RUNNING",
                    "status_message": "",
                    "creation_date": now(),
                    "account_id": req.account.to_string(),
                    "malware_scanner": "GUARDDUTY",
                }),
            );
            ok(json!({
                "ScanJobId": job_id,
                "BackupVaultArn": vault_arn(req, &vault_name),
                "CreationDate": now(),
            }))
        }
        ("GET", ["jobs"]) => {
            let jobs: Vec<Value> = st.scan_jobs.values().map(scan_job_public).collect();
            ok(json!({ "ScanJobs": jobs }))
        }
        ("GET", ["jobs", id]) => match st.scan_jobs.get(*id) {
            Some(job) => ok(scan_job_public(job)),
            None => not_found(format!("Scan job {id} not found")),
        },
        _ => not_supported(req),
    }
}

/// Public scan-job body (mirrors Moto's FakeScanJob.to_dict).
fn scan_job_public(job: &Value) -> Value {
    json!({
        "ScanJobId": job["scan_job_id"],
        "BackupVaultName": job["backup_vault_name"],
        "BackupVaultArn": job["backup_vault_arn"],
        "RecoveryPointArn": job["recovery_point_arn"],
        "IamRoleArn": job["iam_role_arn"],
        "ScanMode": job["scan_mode"],
        "State": job["state"],
        "StatusMessage": job["status_message"],
        "CreationDate": job["creation_date"],
        "AccountId": job["account_id"],
        "MalwareScanner": job["malware_scanner"],
    })
}

fn tiering_routes(
    req: &Request,
    st: &mut BackupStore,
    method: &str,
    rest: &[&str],
    body: &Value,
) -> Response {
    match (method, rest) {
        ("PUT", []) => {
            let cfg_body = body
                .get("TieringConfiguration")
                .cloned()
                .unwrap_or(json!({}));
            let name = cfg_body
                .get("TieringConfigurationName")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if st.tiering_configurations.contains_key(&name) {
                return already_exists(format!("Tiering configuration {name} already exists"));
            }
            let arn = format!("{}:tiering-configuration:{}", arn_prefix(req), name);
            let config = json!({
                "tiering_configuration_name": name.clone(),
                "tiering_configuration_arn": arn.clone(),
                "backup_vault_name": cfg_body.get("BackupVaultName").and_then(Value::as_str).unwrap_or(""),
                "resource_selection": cfg_body.get("ResourceSelection").cloned().unwrap_or(json!([])),
                "creator_request_id": body.get("CreatorRequestId").and_then(Value::as_str).unwrap_or(""),
                "creation_time": now(),
                "last_updated_time": now(),
            });
            st.tiering_configurations
                .insert(name.clone(), config.clone());
            ok(tiering_public(&config))
        }
        ("GET", []) => {
            let vault_filter = query_param(req, "backupVaultName");
            let list: Vec<Value> = st
                .tiering_configurations
                .values()
                .filter(|c| {
                    vault_filter
                        .as_deref()
                        .is_none_or(|v| c["backup_vault_name"].as_str() == Some(v))
                })
                .map(tiering_public)
                .collect();
            ok(json!({"TieringConfigurations": list}))
        }
        (_, [name]) => match (method, rest) {
            ("GET", _) => match st.tiering_configurations.get(*name) {
                Some(c) => ok(json!({"TieringConfiguration": tiering_public(c)})),
                None => not_found(format!("Tiering configuration {name} not found")),
            },
            ("PUT", _) => {
                let Some(c) = st.tiering_configurations.get_mut(*name) else {
                    return not_found(format!("Tiering configuration {name} not found"));
                };
                let cfg_body = body
                    .get("TieringConfiguration")
                    .cloned()
                    .unwrap_or(json!({}));
                if let Some(v) = cfg_body.get("BackupVaultName") {
                    c["backup_vault_name"] = v.clone();
                }
                if let Some(v) = cfg_body.get("ResourceSelection") {
                    c["resource_selection"] = v.clone();
                }
                c["last_updated_time"] = json!(now());
                ok(tiering_public(c))
            }
            ("DELETE", _) => {
                if st.tiering_configurations.remove(*name).is_none() {
                    return not_found(format!("Tiering configuration {name} not found"));
                }
                ok(json!({}))
            }
            _ => not_supported(req),
        },
        _ => not_supported(req),
    }
}

/// Public tiering-configuration body (mirrors Moto's FakeTieringConfiguration.to_dict).
fn tiering_public(config: &Value) -> Value {
    let mut out = json!({
        "TieringConfigurationName": config["tiering_configuration_name"],
        "TieringConfigurationArn": config["tiering_configuration_arn"],
        "BackupVaultName": config["backup_vault_name"],
        "ResourceSelection": config["resource_selection"],
        "CreationTime": config["creation_time"],
        "LastUpdatedTime": config["last_updated_time"],
    });
    if let Some(c) = config["creator_request_id"]
        .as_str()
        .filter(|c| !c.is_empty())
    {
        out["CreatorRequestId"] = json!(c);
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    struct Ctx {
        base: Request,
    }

    impl Ctx {
        fn new(region: &str) -> Self {
            Self {
                base: Request {
                    service: "backup".to_string(),
                    operation: String::new(),
                    method: "GET".to_string(),
                    path: "/".to_string(),
                    query_string: String::new(),
                    headers: std::collections::HashMap::new(),
                    body: bytes::Bytes::new(),
                    account: 123456789012,
                    region: region.to_string(),
                },
            }
        }

        fn call(&self, method: &str, path: &str, params: Value) -> Response {
            let mut req = self.base.clone();
            req.method = method.to_string();
            match path.split_once('?') {
                Some((p, query)) => {
                    req.path = p.to_string();
                    req.query_string = query.to_string();
                }
                None => req.path = path.to_string(),
            }
            req.body = bytes::Bytes::from(serde_json::to_vec(&params).unwrap_or_default());
            dispatch(&req).unwrap()
        }

        fn result(&self, resp: &Response) -> Value {
            serde_json::from_slice(&resp.body).unwrap_or_default()
        }

        fn vault(&self) -> String {
            let name = "vault-1";
            self.call("PUT", "/backup-vaults/vault-1", json!({}));
            name.to_string()
        }
    }

    #[test]
    fn vault_crud_and_lock() {
        let ctx = Ctx::new("us-east-1");
        let created = ctx.call("PUT", "/backup-vaults/v1", json!({}));
        let created = ctx.result(&created);
        assert_eq!(created["BackupVaultName"], "v1");
        assert!(created["BackupVaultArn"]
            .as_str()
            .unwrap()
            .ends_with(":backup-vault:v1"));

        let desc = ctx.result(&ctx.call("GET", "/backup-vaults/v1", json!({})));
        assert_eq!(desc["BackupVaultName"], "v1");
        assert_eq!(desc["Locked"], false);
        assert!(desc.get("CreationDate").is_some());

        // tags round trip
        let arn = desc["BackupVaultArn"].as_str().unwrap().to_string();
        ctx.call(
            "POST",
            &format!("/tags/{}", urlencode(&arn)),
            json!({"Tags": {"env": "test"}}),
        );
        let tags = ctx.result(&ctx.call("GET", &format!("/tags/{}", urlencode(&arn)), json!({})));
        assert_eq!(tags["Tags"]["env"], "test");
        ctx.call(
            "POST",
            &format!("/untag/{}", urlencode(&arn)),
            json!({"TagKeyList": ["env"]}),
        );
        let tags = ctx.result(&ctx.call("GET", &format!("/tags/{}", urlencode(&arn)), json!({})));
        assert!(tags["Tags"]
            .as_object()
            .map(|t| t.is_empty())
            .unwrap_or(false));

        // lock
        ctx.call(
            "PUT",
            "/backup-vaults/v1/vault-lock",
            json!({"MinRetentionDays": 7, "MaxRetentionDays": 365}),
        );
        let desc = ctx.result(&ctx.call("GET", "/backup-vaults/v1", json!({})));
        assert_eq!(desc["Locked"], true);
        assert_eq!(desc["MinRetentionDays"], 7);
        ctx.call("DELETE", "/backup-vaults/v1/vault-lock", json!({}));
        let desc = ctx.result(&ctx.call("GET", "/backup-vaults/v1", json!({})));
        assert_eq!(desc["Locked"], false);
        assert!(desc.get("MinRetentionDays").is_none());

        // unknown vault is a ResourceNotFound
        let missing = ctx.call("GET", "/backup-vaults/nope", json!({}));
        assert_eq!(missing.status, 400);
        let listed = ctx.result(&ctx.call("GET", "/backup-vaults/", json!({})));
        assert_eq!(listed["BackupVaultList"][0]["BackupVaultName"], "v1");
    }

    #[test]
    fn plan_lifecycle_and_version_changes() {
        let ctx = Ctx::new("us-east-1");
        ctx.vault();
        let plan_body = |name: &str| {
            json!({
                "BackupPlan": {
                    "BackupPlanName": name,
                    "Rules": [
                        {"RuleName": "daily", "TargetBackupVaultName": "vault-1",
                         "ScheduleExpression": "cron(0 12 * * ? *)"},
                    ],
                },
                "BackupPlanTags": {"env": "staging"},
            })
        };
        let created = ctx.result(&ctx.call("PUT", "/backup/plans/", plan_body("plan-a")));
        let plan_id = created["BackupPlanId"].as_str().unwrap().to_string();
        assert!(created["BackupPlanArn"]
            .as_str()
            .unwrap()
            .contains("backup-plan"));
        assert!(created["VersionId"].is_string());

        // tags visible
        let tags = ctx.result(&ctx.call(
            "GET",
            &format!(
                "/tags/{}",
                urlencode(created["BackupPlanArn"].as_str().unwrap())
            ),
            json!({}),
        ));
        assert_eq!(tags["Tags"]["env"], "staging");

        // list
        let listed = ctx.result(&ctx.call("GET", "/backup/plans/", json!({})));
        let found = listed["BackupPlansList"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["BackupPlanId"] == plan_id)
            .unwrap()
            .clone();
        assert_eq!(found["BackupPlanName"], "plan-a");

        // update -> new VersionId
        let updated = ctx.result(&ctx.call(
            "POST",
            &format!("/backup/plans/{plan_id}"),
            plan_body("updated-plan"),
        ));
        assert_eq!(updated["BackupPlanId"], plan_id);
        assert_ne!(updated["VersionId"], created["VersionId"]);
        let listed = ctx.result(&ctx.call("GET", "/backup/plans/", json!({})));
        let found = listed["BackupPlansList"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["BackupPlanId"] == plan_id)
            .unwrap()
            .clone();
        assert_eq!(found["BackupPlanName"], "updated-plan");

        // delete then list excludes it
        let deleted =
            ctx.result(&ctx.call("DELETE", &format!("/backup/plans/{plan_id}"), json!({})));
        assert_eq!(deleted["BackupPlanId"], plan_id);
        assert!(deleted.get("DeletionDate").is_some());
        // nonexistent delete is a ResourceNotFound
        let missing = ctx.call("DELETE", "/backup/plans/0000", json!({}));
        assert_eq!(missing.status, 400);
        // nonexistent update also 400
        let listed = ctx.result(&ctx.call("GET", "/backup/plans/", json!({})));
        assert!(
            listed["BackupPlansList"]
                .as_array()
                .unwrap()
                .iter()
                .all(|p| p["BackupPlanId"] != plan_id),
            "deleted plan should not be listed"
        );
    }

    #[test]
    fn backup_job_start_describe_stop() {
        let ctx = Ctx::new("us-east-1");
        ctx.vault();
        let started = ctx.result(&ctx.call(
            "PUT",
            "/backup-jobs",
            json!({
                "BackupVaultName": "vault-1",
                "ResourceArn": "arn:aws:dynamodb:us-east-1:123456789012:table/t1",
                "IamRoleArn": "arn:aws:iam::123456789012:role/backup-role",
            }),
        ));
        assert!(started["BackupJobId"].is_string());
        assert!(started["CreationDate"].is_f64());
        assert!(started["RecoveryPointArn"]
            .as_str()
            .unwrap()
            .contains("recovery-point"));

        let job_id = started["BackupJobId"].as_str().unwrap().to_string();
        let desc = ctx.result(&ctx.call("GET", &format!("/backup-jobs/{job_id}"), json!({})));
        assert_eq!(desc["BackupJobId"], job_id);
        assert_eq!(desc["BackupVaultName"], "vault-1");
        assert_eq!(desc["State"], "COMPLETED");
        assert!(desc.get("ResourceArn").is_some());
        assert!(desc.get("IamRoleArn").is_some());

        let listed = ctx.result(&ctx.call("GET", "/backup-jobs/", json!({})));
        assert!(listed["BackupJobs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|j| j["BackupJobId"] == job_id));
        let listed =
            ctx.result(&ctx.call("GET", "/backup-jobs/?backupVaultName=vault-1", json!({})));
        assert!(listed["BackupJobs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|j| j["BackupJobId"] == job_id));

        // stop on a completed job is rejected
        let resp = ctx.call("POST", &format!("/backup-jobs/{job_id}"), json!({}));
        assert_eq!(resp.status, 400);
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains("InvalidRequestException"), "{body}");

        // nonexistent job describe -> not found
        let resp = ctx.call(
            "GET",
            "/backup-jobs/00000000-0000-0000-0000-000000000000",
            json!({}),
        );
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn framework_crud() {
        let ctx = Ctx::new("us-east-1");
        let name = "fw1";
        let created = ctx.result(&ctx.call("POST", "/audit/frameworks", json!({
            "FrameworkName": name,
            "FrameworkDescription": "desc",
            "FrameworkControls": [{"ControlName": "BACKUP_PLAN_MIN_FREQUENCY_AND_MIN_RETENTION_CHECK"}],
            "FrameworkTags": {"project": "robotocore"},
        })));
        assert_eq!(created["FrameworkName"], name);
        assert!(created["FrameworkArn"].is_string());

        let desc = ctx.result(&ctx.call("GET", "/audit/frameworks/fw1", json!({})));
        assert_eq!(desc["FrameworkDescription"], "desc");
        assert_eq!(desc["DeploymentStatus"], "COMPLETED");
        assert_eq!(desc["FrameworkControls"].as_array().unwrap().len(), 1);

        let listed = ctx.result(&ctx.call("GET", "/audit/frameworks", json!({})));
        assert!(
            listed["Frameworks"]
                .as_array()
                .unwrap()
                .iter()
                .any(|f| f["FrameworkName"] == name),
            "{listed}"
        );

        let _ = ctx.call(
            "PUT",
            "/audit/frameworks/fw1",
            json!({"FrameworkControls": [
                {"ControlName": "BACKUP_PLAN_MIN_FREQUENCY_AND_MIN_RETENTION_CHECK"},
                {"ControlName": "BACKUP_RESOURCES_PROTECTED_BY_BACKUP_PLAN"},
            ]}),
        );
        let desc = ctx.result(&ctx.call("GET", "/audit/frameworks/fw1", json!({})));
        assert_eq!(desc["FrameworkControls"].as_array().unwrap().len(), 2);

        let _ = ctx.call("DELETE", "/audit/frameworks/fw1", json!({}));
        let resp = ctx.call("GET", "/audit/frameworks/fw1", json!({}));
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn report_plan_lifecycle() {
        let ctx = Ctx::new("us-east-1");
        let body = json!({
            "ReportPlanName": "rp1",
            "ReportPlanDescription": "field check",
            "ReportDeliveryChannel": {"S3BucketName": "bucket", "S3KeyPrefix": "pfx/", "Formats": ["CSV"]},
            "ReportSetting": {"ReportTemplate": "BACKUP_JOB_REPORT"},
        });
        let created = ctx.result(&ctx.call("POST", "/audit/report-plans", body.clone()));
        assert_eq!(created["ReportPlanName"], "rp1");
        assert!(created["ReportPlanArn"].is_string());

        let desc = ctx.result(&ctx.call("GET", "/audit/report-plans/rp1", json!({})));
        let rp = &desc["ReportPlan"];
        assert_eq!(rp["ReportPlanDescription"], "field check");
        assert_eq!(rp["ReportDeliveryChannel"]["S3KeyPrefix"], "pfx/");
        assert_eq!(rp["ReportSetting"]["ReportTemplate"], "BACKUP_JOB_REPORT");
        assert!(rp.get("CreationTime").is_some());

        let listed = ctx.result(&ctx.call("GET", "/audit/report-plans", json!({})));
        assert!(listed["ReportPlans"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["ReportPlanName"] == "rp1"));

        let updated = ctx.result(&ctx.call(
            "PUT",
            "/audit/report-plans/rp1",
            json!({"ReportPlanDescription": "u2"}),
        ));
        assert_eq!(updated["ReportPlanName"], "rp1");
        assert!(updated["ReportPlanArn"].is_string());

        let resp = ctx.call("GET", "/audit/report-plans/nada", json!({}));
        assert_eq!(resp.status, 400);
        let _ = ctx.call("DELETE", "/audit/report-plans/rp1", json!({}));
        let resp = ctx.call("GET", "/audit/report-plans/rp1", json!({}));
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn legal_hold_lifecycle() {
        let ctx = Ctx::new("us-east-1");
        let created = ctx.result(&ctx.call(
            "POST",
            "/legal-holds/",
            json!({
                "Title": "hold-1",
                "Description": "test hold",
                "RecoveryPointSelection": {"ResourceIdentifiers": ["*"], "VaultNames": ["*"]},
            }),
        ));
        let hold_id = created["LegalHoldId"].as_str().unwrap().to_string();
        assert_eq!(created["Title"], "hold-1");
        assert_eq!(created["Status"], "ACTIVE");
        assert!(created["LegalHoldArn"].is_string());

        let listed = ctx.result(&ctx.call("GET", "/legal-holds/", json!({})));
        assert!(listed["LegalHolds"]
            .as_array()
            .unwrap()
            .iter()
            .any(|h| h["LegalHoldId"] == hold_id));

        let resp = ctx.call("DELETE", &format!("/legal-holds/{hold_id}"), json!({}));
        assert_eq!(resp.status, 200);
        let listed = ctx.result(&ctx.call("GET", "/legal-holds/", json!({})));
        let hold = listed["LegalHolds"]
            .as_array()
            .unwrap()
            .iter()
            .find(|h| h["LegalHoldId"] == hold_id)
            .unwrap();
        assert_eq!(hold["Status"], "CANCELED");

        let rps = ctx.result(&ctx.call(
            "GET",
            &format!("/legal-holds/{hold_id}/recovery-points"),
            json!({}),
        ));
        assert!(rps["RecoveryPoints"].is_array());
    }

    #[test]
    fn restore_testing_plan_and_selection() {
        let ctx = Ctx::new("us-east-1");
        let created = ctx.result(&ctx.call(
            "PUT",
            "/restore-testing/plans",
            json!({
                "RestoreTestingPlan": {
                    "RestoreTestingPlanName": "rtp1",
                    "ScheduleExpression": "cron(0 12 * * ? *)",
                    "RecoveryPointSelection": {"Algorithm": "LATEST_WITHIN_WINDOW",
                        "RecoveryPointTypes": ["CONTINUOUS"], "IncludeVaults": ["*"]},
                    "StartWindowHours": 1,
                },
            }),
        ));
        assert_eq!(created["RestoreTestingPlanName"], "rtp1");
        assert_eq!(created["RestoreTestingPlanName"], "rtp1");
        assert!(created["RestoreTestingPlanArn"].is_string());

        let plan = ctx.result(&ctx.call("GET", "/restore-testing/plans/rtp1", json!({})))
            ["RestoreTestingPlan"]
            .clone();
        assert_eq!(plan["RestoreTestingPlanName"], "rtp1");
        assert_eq!(plan["ScheduleExpressionTimezone"], "Etc/UTC");
        assert_eq!(plan["StartWindowHours"], 1);

        let sel = ctx.result(&ctx.call(
            "PUT",
            "/restore-testing/plans/rtp1/selections",
            json!({
                "RestoreTestingSelection": {
                    "RestoreTestingSelectionName": "sel1",
                    "ProtectedResourceType": "DynamoDB",
                    "IamRoleArn": "arn:aws:iam::123456789012:role/t",
                },
            }),
        ));
        assert_eq!(sel["RestoreTestingSelectionName"], "sel1");

        let listed =
            ctx.result(&ctx.call("GET", "/restore-testing/plans/rtp1/selections", json!({})));
        assert!(listed["RestoreTestingSelections"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["restore_testing_selection_name"] == "sel1"));

        let updated = ctx.result(&ctx.call(
            "PUT",
            "/restore-testing/plans/rtp1/selections/sel1",
            json!({"RestoreTestingSelection": {"IamRoleArn": "arn:aws:iam::123456789012:role/u"}}),
        ));
        assert_eq!(updated["RestoreTestingSelectionName"], "sel1");
        assert!(updated["UpdateTime"].is_f64());
        // update plan
        let updated_plan = ctx.result(&ctx.call(
            "PUT",
            "/restore-testing/plans/rtp1",
            json!({
                "RestoreTestingPlan": {"ScheduleExpression": "cron(0 6 * * ? *)"},
            }),
        ));
        assert_eq!(updated_plan["RestoreTestingPlanName"], "rtp1");
        assert!(updated_plan["UpdateTime"].is_f64());

        // delete plan + selection lifecycle
        let resp = ctx.call("DELETE", "/restore-testing/plans/rtp1", json!({}));
        assert_eq!(resp.status, 204);
        // deleting the (now absent) plan still leaves selection lookups lonely;
        // a fake selection delete is a 204
        let resp = ctx.call(
            "DELETE",
            "/restore-testing/plans/fake/selections/fakesel",
            json!({}),
        );
        assert_eq!(resp.status, 204);
    }

    #[test]
    fn settings_resources_and_misc() {
        let ctx = Ctx::new("us-east-1");
        let global = ctx.result(&ctx.call("GET", "/global-settings", json!({})));
        assert_eq!(
            global["GlobalSettings"]["isCrossAccountBackupEnabled"],
            "false"
        );
        let region = ctx.result(&ctx.call("GET", "/account-settings", json!({})));
        assert_eq!(region["ResourceTypeOptInPreference"]["EBS"], true);
        assert_eq!(region["ResourceTypeOptInPreference"]["S3"], true);
        let types = ctx.result(&ctx.call("GET", "/supported-resource-types", json!({})));
        assert!(types["ResourceTypes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t == "EBS"));

        // protected resources + recovery points on stray arn
        assert_eq!(ctx.call("GET", "/resources/", json!({})).status, 200);
        let resp = ctx.call(
            "GET",
            "/resources/arn:aws:backup:us-east-1:123456789012:recovery-point:fake",
            json!({}),
        );
        assert_eq!(resp.status, 400);
        let listed = ctx.result(&ctx.call(
            "GET",
            "/resources/arn:aws:dynamodb:us-east-1:123456789012:table/x/recovery-points",
            json!({}),
        ));
        assert!(listed["RecoveryPoints"].as_array().unwrap().is_empty());

        // summaries and scan/tiering fakes
        let resp = ctx.call("GET", "/audit/backup-job-summaries", json!({}));
        assert_eq!(resp.status, 200);
        let scan = ctx.result(&ctx.call("GET", "/scan/jobs", json!({})));
        assert!(scan["ScanJobs"].is_array());
        let resp = ctx.call("GET", "/scan/jobs/fake", json!({}));
        assert_eq!(resp.status, 400);
        let tiering = ctx.result(&ctx.call("GET", "/tiering-configurations", json!({})));
        assert!(tiering["TieringConfigurations"].is_array());
        let resp = ctx.call("GET", "/tiering-configurations/fake", json!({}));
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn templates_and_plan_json() {
        let ctx = Ctx::new("us-east-1");
        let listed = ctx.result(&ctx.call("GET", "/backup/template/plans", json!({})));
        assert!(!listed["BackupPlanTemplatesList"]
            .as_array()
            .unwrap()
            .is_empty());
        let tid = listed["BackupPlanTemplatesList"][0]["BackupPlanTemplateId"]
            .as_str()
            .unwrap()
            .to_string();
        let doc = ctx.result(&ctx.call(
            "GET",
            &format!("/backup/template/plans/{tid}/toPlan"),
            json!({}),
        ));
        assert!(doc["BackupPlanDocument"].get("BackupPlanName").is_some());

        let plan_json = r#"{"BackupPlanName": "json-plan", "Rules": [{"RuleName": "daily"}]}"#;
        let from_json = ctx.result(&ctx.call(
            "POST",
            "/backup/template/json/toPlan",
            json!({"BackupPlanTemplateJson": plan_json}),
        ));
        assert_eq!(from_json["BackupPlan"]["BackupPlanName"], "json-plan");
        assert_eq!(
            from_json["BackupPlan"]["Rules"].as_array().unwrap().len(),
            1
        );
    }

    #[test]
    fn recovery_point_ops_report_jobs_and_empty_settings() {
        let ctx = Ctx::new("us-east-1");
        ctx.vault();

        // Recovery points never materialize (StartBackupJob leaves none), so
        // the whole family answers vault-check-then-RP-not-found like moto.
        let base = "/backup-vaults/vault-1/recovery-points/%61rp";
        for (method, suffix, op) in [
            ("GET", None, "DescribeRecoveryPoint"),
            ("DELETE", None, "DeleteRecoveryPoint"),
            ("POST", None, "UpdateRecoveryPointLifecycle"),
            ("GET", Some("restore-metadata"), "GetRestoreMetadata"),
            ("POST", Some("disassociate"), "Disassociate"),
            ("DELETE", Some("parentAssociation"), "DissocFromParent"),
        ] {
            let path = match suffix {
                Some(s) => format!("{base}/{s}"),
                None => base.to_string(),
            };
            let resp = ctx.call(method, &path, json!({}));
            assert_eq!(resp.status, 400, "{op}");
            let body = ctx.result(&resp);
            assert_eq!(body["__type"], "ResourceNotFoundException", "{op} body");
            assert!(
                body["message"]
                    .as_str()
                    .unwrap()
                    .starts_with("Recovery point "),
                "{op} message"
            );
        }
        // Unknown vault wins over unknown recovery point.
        let resp = ctx.call("GET", "/backup-vaults/ghost/recovery-points/x", json!({}));
        let body = ctx.result(&resp);
        assert_eq!(resp.status, 400);
        assert_eq!(body["message"], "Backup vault ghost not found");

        // Greedy rpArn: a recovery-point arn containing slashes (as the probe
        // filler generates) is matched whole, like moto's greedy route label.
        let resp = ctx.call(
            "GET",
            "/backup-vaults/vault-1/recovery-points/arn:aws:backup:us-east-1:123456789012:thing/probe-test",
            json!({}),
        );
        assert_eq!(resp.status, 400);
        assert_eq!(
            ctx.result(&resp)["message"],
            "Recovery point arn:aws:backup:us-east-1:123456789012:thing/probe-test not found"
        );
        // ...and with a suffix riding on top of a slashed arn.
        let resp = ctx.call(
            "GET",
            "/backup-vaults/vault-1/recovery-points/arn:aws:backup:us-east-1:123456789012:thing/probe-test/restore-metadata",
            json!({}),
        );
        assert_eq!(resp.status, 400);
        assert!(ctx.result(&resp)["message"]
            .as_str()
            .unwrap()
            .ends_with("thing/probe-test not found"));

        // Report jobs: unknown plan errors, live plan starts a COMPLETED job.
        ctx.call(
            "POST",
            "/audit/report-plans/",
            json!({
                "ReportPlanName": "rp1",
                "ReportDeliveryChannel": {"S3BucketName": "bkt"},
            }),
        );
        let resp = ctx.call("POST", "/audit/report-jobs/ghost", json!({}));
        assert_eq!(resp.status, 400);
        assert_eq!(ctx.result(&resp)["message"], "Report plan ghost not found");
        let started = ctx.result(&ctx.call("POST", "/audit/report-jobs/rp1", json!({})));
        assert!(started["ReportJobId"].as_str().unwrap().len() == 36);
        let jobs = ctx.result(&ctx.call(
            "GET",
            "/audit/report-jobs?reportPlanName=rp1&status=COMPLETED",
            json!({}),
        ));
        assert_eq!(jobs["ReportJobs"].as_array().unwrap().len(), 1);
        assert_eq!(jobs["ReportJobs"][0]["Status"], "COMPLETED");
        let none = ctx.result(&ctx.call(
            "GET",
            "/audit/report-jobs?reportPlanName=stranger",
            json!({}),
        ));
        assert!(none["ReportJobs"].as_array().unwrap().is_empty());

        // Settings PUT with an empty body is a moto no-op success.
        assert_eq!(ctx.call("PUT", "/global-settings", json!({})).status, 200);
        assert_eq!(ctx.call("PUT", "/account-settings", json!({})).status, 200);
        let merged = ctx.result(&ctx.call(
            "PUT",
            "/global-settings",
            json!({"GlobalSettings": {"isCrossAccountBackupEnabled": "true"}}),
        ));
        assert_eq!(merged, json!({}));
        let global = ctx.result(&ctx.call("GET", "/global-settings", json!({})));
        assert_eq!(
            global["GlobalSettings"]["isCrossAccountBackupEnabled"],
            "true"
        );
    }

    fn urlencode(s: &str) -> String {
        let mut out = String::new();
        for b in s.bytes() {
            match b {
                b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    out.push(b as char)
                }
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
        out
    }
}
