//! Inspector2 backend (moto-parity port).
//!
//! Rest-JSON service routed by (method, URI template) — every op is a
//! static path except the tags trio. Untouched ops return a moto-style 501.

use crate::proto::{now_unix, parse_json_body, query_map, unix_in, url_decode};
use crate::store::{slot, ScopeKey, Slot};
use crate::{Request, Response};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use uuid::Uuid;

/// Per-(account, region) Inspector2 state.
#[derive(Default)]
struct State {
    filters: BTreeMap<String, Value>,
    admin_accounts: BTreeMap<String, String>,
    account_status: BTreeMap<String, Value>, // accountId -> AccountStatus
    members: BTreeMap<String, Value>,
    org_config: Value,
    tagger: BTreeMap<String, BTreeMap<String, String>>,
    cis_scan_configurations: BTreeMap<String, Value>,
    findings_reports: BTreeMap<String, Value>,
    sbom_exports: BTreeMap<String, Value>,
    code_security_integrations: BTreeMap<String, Value>,
    code_security_scan_configurations: BTreeMap<String, Value>,
    code_security_scans: BTreeMap<String, Value>,
    ec2_deep_inspection_config: Value,
    encryption_keys: BTreeMap<String, String>,
    configuration: Value,
}

impl State {
    fn init(&mut self) {
        self.org_config = json!({"ec2": false, "ecr": false, "lambda": false, "lambdaCode": false});
        self.ec2_deep_inspection_config =
            json!({"packagePaths": [], "orgPackagePaths": [], "status": "DISABLED"});
        self.configuration = json!({"ecrConfiguration": {"rescanDuration": "LIFETIME"}});
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

    /// Query-string scalar (GET encrypt-key ops carry params in the query).
    fn query1(&self, name: &str) -> Option<String> {
        self.query.get(name).and_then(|v| v.first()).cloned()
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

fn status_to_json(account_id: &str, status: &Value) -> Value {
    let ec2 = status.get("ec2").and_then(Value::as_str).unwrap_or("DISABLED");
    let ecr = status.get("ecr").and_then(Value::as_str).unwrap_or("DISABLED");
    let lambda = status.get("lambda").and_then(Value::as_str).unwrap_or("DISABLED");
    let lambda_code = status
        .get("lambdaCode")
        .and_then(Value::as_str)
        .unwrap_or("DISABLED");
    let overall = [ec2, ecr, lambda, lambda_code]
        .iter()
        .any(|s| *s == "ENABLED");
    json!({
        "accountId": account_id,
        "resourceStatus": {"ec2": ec2, "ecr": ecr, "lambda": lambda, "lambdaCode": lambda_code},
        "status": if overall { "ENABLED" } else { "DISABLED" },
    })
}

fn status_to_batch_json(account_id: &str, status: &Value) -> Value {
    let pick = |field: &str| status.get(field).and_then(Value::as_str).unwrap_or("DISABLED");
    let overall = ["ec2", "ecr", "lambda", "lambdaCode"]
        .iter()
        .any(|f| pick(f) == "ENABLED");
    json!({
        "accountId": account_id,
        "resourceState": {
            "ec2": {"status": pick("ec2")},
            "ecr": {"status": pick("ecr")},
            "lambda": {"status": pick("lambda")},
            "lambdaCode": {"status": pick("lambdaCode")},
        },
        "state": {"status": if overall { "ENABLED" } else { "DISABLED" }},
    })
}

/// Toggle `resource_types` for `account_ids` and return the affected statuses.
fn toggle_accounts(
    st: &mut State,
    account_ids: &[String],
    resource_types: &[String],
    enable: bool,
) -> Vec<Value> {
    for account_id in account_ids {
        let entry = st
            .account_status
            .entry(account_id.clone())
            .or_insert_with(|| {
                json!({"ec2": "DISABLED", "ecr": "DISABLED", "lambda": "DISABLED", "lambdaCode": "DISABLED"})
            });
        let value = if enable { "ENABLED" } else { "DISABLED" };
        if resource_types_matches(resource_types, "EC2") {
            entry["ec2"] = json!(value);
        }
        if resource_types_matches(resource_types, "ECR") {
            entry["ecr"] = json!(value);
        }
        if resource_types_matches(resource_types, "LAMBDA") {
            entry["lambda"] = json!(value);
        }
        if resource_types_matches(resource_types, "LAMBDA_CODE") {
            entry["lambdaCode"] = json!(value);
        }
    }
    st.account_status
        .iter()
        .filter(|(id, _)| account_ids.iter().any(|a| a == id.as_str()))
        .map(|(id, s)| status_to_json(id, s))
        .collect::<Vec<Value>>()
}

fn resource_types_matches(types: &[String], name: &str) -> bool {
    types.iter().any(|t| t == name)
}

/// Fish an arn out of `/tags/{arn}` (botocore encodes the ARN in one segment).
fn tags_arn(svc: &Svc) -> String {
    let marker = "tags/";
    match svc.path.find(marker) {
        Some(idx) => svc.path[idx + marker.len()..].to_string(),
        None => String::new(),
    }
}

/// Dispatch a routed Inspector2 request. Always answers (501 fallback).
pub fn dispatch(req: &Request) -> Response {
    let state: Slot<State> = slot("inspector2");
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

    if segs.len() >= 2 && segs[0] == "tags" {
        let arn = tags_arn(&svc);
        return match m {
            "GET" => list_tags(&svc, &arn),
            "POST" => tag(&svc, &arn),
            _ => untag(&svc, &arn),
        };
    }

    route_static(&svc, m, &segs)
}

fn route_static(svc: &Svc, m: &str, segs: &[&str]) -> Response {
    let tail = segs.join("/");
    match (m, tail.as_str()) {
        // --- Filters ---
        ("POST", "filters/create") => create_filter(svc),
        ("POST", "filters/delete") => delete_filter(svc),
        ("POST", "filters/list") => list_filters(svc),
        ("POST", "filters/update") => update_filter(svc),
        // --- Findings / aggregations / coverage ---
        ("POST", "findings/list") => ok(json!({"findings": []})),
        ("POST", "findings/aggregation/list") => {
            let agg = svc.body("aggregationType");
            ok(json!({"aggregationType": agg.as_str().unwrap_or(""), "responses": []}))
        }
        ("POST", "findings/details/batch/get") => batch_get_finding_details(svc),
        ("POST", "codesnippet/batchget") => batch_get_code_snippet(svc),
        ("POST", "coverage/list") => ok(json!({"coveredResources": []})),
        ("POST", "coverage/statistics/list") => ok(json!({"countsByGroup": [], "totalCounts": 0})),
        ("POST", "vulnerabilities/search") => ok(json!({"vulnerabilities": []})),
        ("POST", "cluster/get") => ok(json!({"cluster": []})),
        // --- Members ---
        ("POST", "members/associate") => associate_member(svc),
        ("POST", "members/disassociate") => disassociate_member(svc),
        ("POST", "members/get") => get_member(svc),
        ("POST", "members/list") => list_members(svc),
        // --- Delegated admin ---
        ("POST", "delegatedadminaccounts/enable") => enable_delegated_admin(svc, true),
        ("POST", "delegatedadminaccounts/disable") => enable_delegated_admin(svc, false),
        ("POST", "delegatedadminaccounts/list") => list_delegated_admins(svc),
        ("POST", "delegatedadminaccounts/get") => get_delegated_admin(svc),
        // --- Account enablement ---
        ("POST", "enable") => enable_disable(svc, true),
        ("POST", "disable") => enable_disable(svc, false),
        ("POST", "status/batch/get") => batch_get_account_status(svc),
        ("POST", "freetrialinfo/batchget") => batch_get_free_trial_info(svc),
        ("POST", "ec2deepinspectionstatus/member/batch/get") => {
            batch_get_member_deep_inspection(svc)
        }
        ("POST", "ec2deepinspectionstatus/member/batch/update") => {
            batch_update_member_deep_inspection(svc)
        }
        // --- Organization ---
        ("POST", "organizationconfiguration/describe") => describe_organization_configuration(svc),
        ("POST", "organizationconfiguration/update") => update_organization_configuration(svc),
        // --- CIS ---
        ("POST", "cis/scan-configuration/create") => create_cis_scan_configuration(svc),
        ("POST", "cis/scan-configuration/delete") => delete_cis_scan_configuration(svc),
        ("POST", "cis/scan-configuration/update") => update_cis_scan_configuration(svc),
        ("POST", "cis/scan-configuration/list") => list_cis_scan_configurations(svc),
        ("POST", "cis/scan/list") => ok(json!({"scans": []})),
        ("POST", "cis/scan/report/get") => ok(json!({"url": "", "status": "NO_FINDINGS_FOUND"})),
        ("POST", "cis/scan-result/details/get") => ok(json!({"scanResultDetails": []})),
        ("POST", "cis/scan-result/check/list") => ok(json!({"checkAggregations": []})),
        ("POST", "cis/scan-result/resource/list") => ok(json!({"targetResourceAggregations": []})),
        ("PUT", "cissession/start") => ok(json!({})),
        ("PUT", "cissession/stop") => ok(json!({})),
        ("PUT", "cissession/health/send") => ok(json!({})),
        ("PUT", "cissession/telemetry/send") => ok(json!({})),
        // --- Reports & SBOM ---
        ("POST", "reporting/create") => create_findings_report(svc),
        ("POST", "reporting/cancel") => cancel_findings_report(svc),
        ("POST", "reporting/status/get") => get_findings_report_status(svc),
        ("POST", "sbomexport/create") => create_sbom_export(svc),
        ("POST", "sbomexport/cancel") => cancel_sbom_export(svc),
        ("POST", "sbomexport/get") => get_sbom_export(svc),
        // --- Configurations ---
        ("POST", "configuration/get") => get_configuration(svc),
        ("POST", "configuration/update") => update_configuration(svc),
        ("GET", "encryptionkey/get") => get_encryption_key(svc),
        ("PUT", "encryptionkey/update") => update_encryption_key(svc, true),
        ("PUT", "encryptionkey/reset") => update_encryption_key(svc, false),
        ("POST", "ec2deepinspectionconfiguration/get") => get_ec2_deep_inspection_config(svc),
        ("POST", "ec2deepinspectionconfiguration/update") => update_ec2_deep_inspection_config(svc),
        ("POST", "ec2deepinspectionconfiguration/org/update") => {
            update_org_ec2_deep_inspection(svc)
        }
        ("POST", "accountpermissions/list") => ok(json!({
            "permissions": [{"service": "inspector2", "operation": "ENABLE_SCANNING"}]
        })),
        ("POST", "usage/list") => ok(json!({"totals": []})),
        // --- Code security ---
        ("POST", "codesecurity/integration/create") => create_code_security_integration(svc),
        ("POST", "codesecurity/integration/delete") => delete_code_security_integration(svc),
        ("POST", "codesecurity/integration/get") => get_code_security_integration(svc),
        ("POST", "codesecurity/integration/update") => update_code_security_integration(svc),
        ("POST", "codesecurity/integration/list") => list_code_security_integrations(svc),
        ("POST", "codesecurity/scan-configuration/create") => {
            create_code_security_scan_configuration(svc)
        }
        ("POST", "codesecurity/scan-configuration/delete") => {
            delete_code_security_scan_configuration(svc)
        }
        ("POST", "codesecurity/scan-configuration/get") => get_code_security_scan_configuration(svc),
        ("POST", "codesecurity/scan-configuration/update") => {
            update_code_security_scan_configuration(svc)
        }
        ("POST", "codesecurity/scan-configuration/list") => {
            list_code_security_scan_configurations(svc)
        }
        ("POST", "codesecurity/scan-configurationAssociations/list")
        | ("POST", "codesecurity/scan-configuration/associations/list") => {
            list_scan_configuration_associations(svc)
        }
        ("POST", "codesecurity/scan-configuration/batch/associate") => {
            batch_associate(svc, true)
        }
        ("POST", "codesecurity/scan-configuration/batch/disassociate") => {
            batch_associate(svc, false)
        }
        ("POST", "codesecurity/scan/start") => start_code_security_scan(svc),
        ("POST", "codesecurity/scan/get") => get_code_security_scan(svc),
        _ => not_implemented(svc),
    }
}

// --- Filters ---

fn filter_dict(svc: &Svc, id: &str, name: &str, action: &str, criteria: &Value, description: &str, reason: &str) -> Value {
    json!({
        "action": action,
        "arn": format!(
            "arn:aws:inspector2:{}:{}:owner/{}/filter/{}",
            svc.region, svc.account, svc.account, id
        ),
        "createdAt": now_unix(),
        "criteria": criteria,
        "description": description,
        "name": name,
        "ownerId": svc.account,
        "reason": reason,
    })
}

fn create_filter(svc: &Svc) -> Response {
    let name = svc.body("name").as_str().unwrap_or("").to_string();
    let action = svc.body("action").as_str().unwrap_or("").to_string();
    let criteria = svc.body("filterCriteria");
    let description = svc.body("description").as_str().unwrap_or("").to_string();
    let reason = svc.body("reason").as_str().unwrap_or("").to_string();
    let tags = svc.body("tags");
    let id = Uuid::new_v4().to_string();
    let mut dict = filter_dict(svc, &id, &name, &action, &criteria, &description, &reason);
    if let Some(tag_map) = tags.as_object() {
        let mut seeded = BTreeMap::new();
        for (k, v) in tag_map {
            seeded.insert(k.clone(), v.as_str().unwrap_or_default().to_string());
        }
        let arn = dict.get("arn").and_then(Value::as_str).unwrap_or("").to_string();
        merge_tags(svc, &arn, &seeded);
        dict["tags"] = json!(seeded);
    }
    let arn = dict.get("arn").and_then(Value::as_str).unwrap_or("").to_string();
    svc.with(|st| st.filters.insert(arn, dict.clone()));
    ok(json!({"arn": dict.get("arn").cloned().unwrap_or(json!(""))}))
}

fn delete_filter(svc: &Svc) -> Response {
    let arn = svc.body("arn").as_str().unwrap_or("").to_string();
    svc.with(|st| st.filters.remove(&arn));
    ok(json!({"arn": arn}))
}

fn list_filters(svc: &Svc) -> Response {
    let all = svc.with(|st| st.filters.values().cloned().collect::<Vec<Value>>());
    let mut out = Vec::new();
    for mut dict in all {
        let arn = dict.get("arn").and_then(Value::as_str).unwrap_or("").to_string();
        let tags = svc.with(|st| st.tagger.get(&arn).cloned().unwrap_or_default());
        dict["tags"] = json!(tags);
        out.push(dict);
    }
    ok(json!({"filters": out}))
}

fn update_filter(svc: &Svc) -> Response {
    let arn = svc.body("filterArn").as_str().unwrap_or("").to_string();
    let action = svc.body("action");
    let name = svc.body("name");
    let description = svc.body("description");
    let reason = svc.body("reason");
    let criteria = svc.body("filterCriteria");
    let result = svc.with(|st| {
        match st.filters.get_mut(&arn) {
            Some(filter) => {
                if action.is_string() {
                    filter["action"] = action.clone();
                }
                if name.is_string() {
                    filter["name"] = name.clone();
                }
                if description.is_string() {
                    filter["description"] = description.clone();
                }
                if reason.is_string() {
                    filter["reason"] = reason.clone();
                }
                if criteria.is_object() {
                    filter["criteria"] = criteria.clone();
                }
                Some(arn.clone())
            }
            None => None,
        }
    });
    match result {
        Some(arn) => ok(json!({"arn": arn})),
        None => json_resource_not_found(),
    }
}

fn json_resource_not_found() -> Response {
    Response::json_status(
        400,
        &json!({"__type": "ResourceNotFoundException", "message": "The resource was not found."}),
    )
}

fn batch_get_finding_details(svc: &Svc) -> Response {
    let arns = str_list(svc.body("findingArns"));
    let errors: Vec<Value> = arns
        .into_iter()
        .map(|arn| {
            json!({
                "findingArn": arn,
                "errorCode": "INTERNAL_ERROR",
                "errorMessage": "Finding not found",
            })
        })
        .collect();
    ok(json!({"findingDetails": [], "errors": errors}))
}

fn batch_get_code_snippet(svc: &Svc) -> Response {
    let arns = str_list(svc.body("findingArns"));
    let errors: Vec<Value> = arns
        .into_iter()
        .map(|arn| {
            json!({
                "findingArn": arn,
                "errorCode": "INTERNAL_ERROR",
                "errorMessage": "Finding not found",
            })
        })
        .collect();
    ok(json!({"codeSnippetResults": [], "errors": errors}))
}

fn associate_member(svc: &Svc) -> Response {
    let account_id = svc.body("accountId").as_str().unwrap_or("").to_string();
    svc.with(|st| {
        st.members.insert(
            account_id.clone(),
            json!({
                "accountId": account_id,
                "delegatedAdminAccountId": svc.account,
                "relationshipStatus": "ENABLED",
                "updatedAt": now_unix(),
            }),
        );
    });
    ok(json!({"accountId": svc.account}))
}
fn disassociate_member(svc: &Svc) -> Response {
    let account_id = svc.body("accountId").as_str().unwrap_or("").to_string();
    let found = svc.with(|st| {
        match st.members.get_mut(&account_id) {
            Some(member) => {
                member["relationshipStatus"] = json!("DISABLED");
                true
            }
            None => false,
        }
    });
    if found {
        ok(json!({"accountId": account_id}))
    } else {
        json_resource_not_found()
    }
}

fn get_member(svc: &Svc) -> Response {
    let account_id = svc.body("accountId").as_str().unwrap_or("").to_string();
    let member = svc.with(|st| st.members.get(&account_id).cloned());
    match member {
        Some(m) => ok(json!({"member": m})),
        None => json_resource_not_found(),
    }
}

fn list_members(svc: &Svc) -> Response {
    let members = svc.with(|st| st.members.values().cloned().collect::<Vec<Value>>());
    ok(json!({"members": members}))
}

// --- Delegated admin ---

fn enable_delegated_admin(svc: &Svc, enable: bool) -> Response {
    let account_id = svc
        .body("delegatedAdminAccountId")
        .as_str()
        .unwrap_or("")
        .to_string();
    svc.with(|st| {
        st.admin_accounts
            .insert(account_id.clone(), if enable { "ENABLED" } else { "DISABLED" }.to_string());
    });
    ok(json!({"delegatedAdminAccountId": account_id}))
}

fn list_delegated_admins(svc: &Svc) -> Response {
    let accounts = svc
        .with(|st| {
            st.admin_accounts
                .iter()
                .map(|(id, status)| json!({"accountId": id, "status": status}))
                .collect::<Vec<Value>>()
        });
    ok(json!({"delegatedAdminAccounts": accounts}))
}

fn get_delegated_admin(svc: &Svc) -> Response {
    let found = svc
        .with(|st| {
            st.admin_accounts
                .iter()
                .find(|(_, status)| status.as_str() == "ENABLED")
                .map(|(id, status)| json!({"accountId": id, "status": status}))
        });
    match found {
        Some(d) => ok(json!({"delegatedAdmin": d})),
        None => ok(json!({})),
    }
}

// --- Enable / disable ---

fn enable_disable(svc: &Svc, enable: bool) -> Response {
    let account_ids = str_list(svc.body("accountIds"));
    let resource_types = str_list(svc.body("resourceTypes"));
    let accounts = svc
        .with(|st| toggle_accounts(st, &account_ids, &resource_types, enable));
    ok(json!({"accounts": accounts, "failedAccounts": []}))
}

fn batch_get_account_status(svc: &Svc) -> Response {
    let account_ids = str_list(svc.body("accountIds"));
    let accounts = svc
        .with(|st| {
            st.account_status
                .iter()
                .filter(|(id, _)| account_ids.iter().any(|a| a == id.as_str()))
                .map(|(id, status)| status_to_batch_json(id, status))
                .collect::<Vec<Value>>()
        });
    ok(json!({"accounts": accounts, "failedAccounts": []}))
}

fn batch_get_free_trial_info(svc: &Svc) -> Response {
    let account_ids = str_list(svc.body("accountIds"));
    let mut accounts = Vec::new();
    for account_id in account_ids {
        let now = now_unix();
        let trial = |t: &str| {
            json!({"end": unix_in(86400.0 * 15.0), "start": now, "status": "ACTIVE", "type": t})
        };
        accounts.push(json!({
            "accountId": account_id,
            "freeTrialInfo": [trial("EC2"), trial("ECR"), trial("LAMBDA")],
        }));
    }
    ok(json!({"accounts": accounts, "failedAccounts": []}))
}

fn batch_get_member_deep_inspection(svc: &Svc) -> Response {
    let account_ids = str_list(svc.body("accountIds"));
    let results: Vec<Value> = if account_ids.is_empty() {
        vec![]
    } else {
        account_ids
            .into_iter()
            .map(|id| json!({"accountId": id, "status": "DISABLED"}))
            .collect()
    };
    ok(json!({"accountIds": results, "failedAccountIds": []}))
}

fn batch_update_member_deep_inspection(svc: &Svc) -> Response {
    let ids = svc
        .body("accountIds")
        .as_array()
        .cloned()
        .unwrap_or_default();
    let results: Vec<Value> = ids
        .into_iter()
        .map(|item| {
            json!({
                "accountId": item.get("accountId").cloned().unwrap_or(json!("")),
                "status": "SUCCESSFUL",
            })
        })
        .collect();
    ok(json!({"accountIds": results, "failedAccountIds": []}))
}

// --- Organization ---

fn describe_organization_configuration(svc: &Svc) -> Response {
    let config = svc.with(|st| st.org_config.clone());
    ok(json!({"autoEnable": config, "maxAccountLimitReached": false}))
}

fn update_organization_configuration(svc: &Svc) -> Response {
    let auto_enable = svc.body("autoEnable");
    let merged = svc.with(|st| {
        if let (Some(newmap), Some(map)) = (auto_enable.as_object(), st.org_config.as_object_mut()) {
            for (k, v) in newmap {
                map.insert(k.clone(), v.clone());
            }
        }
        st.org_config.clone()
    });
    ok(json!({"autoEnable": merged}))
}
// end re-added block

// --- CIS scan configurations ---

fn create_cis_scan_configuration(svc: &Svc) -> Response {
    let id = Uuid::new_v4().to_string();
    let arn = format!(
        "arn:aws:inspector2:{}:{}:owner/{}/cis-configuration/{}",
        svc.region, svc.account, svc.account, id
    );
    let dict = json!({
        "scanConfigurationArn": arn,
        "scanName": svc.body("scanName"),
        "securityLevel": svc.body("securityLevel"),
        "schedule": svc.body("schedule"),
        "targets": svc.body("targets"),
        "ownerId": svc.account,
        "tags": {},
    });
    svc.with(|st| st.cis_scan_configurations.insert(arn, dict.clone()));
    ok(json!({"scanConfigurationArn": dict.get("scanConfigurationArn").cloned().unwrap_or(json!(""))}))
}

fn delete_cis_scan_configuration(svc: &Svc) -> Response {
    let arn = svc.body("scanConfigurationArn").as_str().unwrap_or("").to_string();
    svc.with(|st| st.cis_scan_configurations.remove(&arn));
    ok(json!({"scanConfigurationArn": arn}))
}

fn update_cis_scan_configuration(svc: &Svc) -> Response {
    let arn = svc.body("scanConfigurationArn").as_str().unwrap_or("").to_string();
    for field in ["scanName", "securityLevel", "schedule", "targets"] {
        let v = svc.body(field);
        svc.with(|st| {
            if let Some(config) = st.cis_scan_configurations.get_mut(&arn) {
                if !v.is_null() {
                    config[field] = v.clone();
                }
            }
        });
    }
    ok(json!({"scanConfigurationArn": arn}))
}

fn list_cis_scan_configurations(svc: &Svc) -> Response {
    let configs = svc.with(|st| st.cis_scan_configurations.values().cloned().collect::<Vec<Value>>());
    ok(json!({"scanConfigurations": configs}))
}

// --- Reports & SBOM ---

fn create_findings_report(svc: &Svc) -> Response {
    let report_id = Uuid::new_v4().to_string();
    let dict = json!({
        "reportId": report_id,
        "status": "COMPLETE",
        "destination": {"s3Destination": svc.body("s3Destination")},
    });
    svc.with(|st| st.findings_reports.insert(report_id.clone(), dict));
    ok(json!({"reportId": report_id}))
}

fn cancel_findings_report(svc: &Svc) -> Response {
    let report_id = svc.body("reportId").as_str().unwrap_or("").to_string();
    svc.with(|st| {
        if let Some(report) = st.findings_reports.get_mut(&report_id) {
            report["status"] = json!("CANCELLED");
        }
    });
    ok(json!({"reportId": report_id}))
}

fn get_findings_report_status(svc: &Svc) -> Response {
    let report_id = svc.body("reportId").as_str().unwrap_or("").to_string();
    let found = svc.with(|st| st.findings_reports.get(&report_id).cloned());
    match found {
        Some(dict) => ok(dict),
        None => ok(json!({"reportId": report_id, "status": "NOT_FOUND"})),
    }
}

fn create_sbom_export(svc: &Svc) -> Response {
    let report_id = Uuid::new_v4().to_string();
    let dict = json!({
        "reportId": report_id,
        "format": svc.body("reportFormat"),
        "status": "COMPLETE",
        "s3Destination": svc.body("s3Destination"),
    });
    svc.with(|st| st.sbom_exports.insert(report_id.clone(), dict));
    ok(json!({"reportId": report_id}))
}

fn cancel_sbom_export(svc: &Svc) -> Response {
    let report_id = svc.body("reportId").as_str().unwrap_or("").to_string();
    svc.with(|st| {
        if let Some(export) = st.sbom_exports.get_mut(&report_id) {
            export["status"] = json!("CANCELLED");
        }
    });
    ok(json!({"reportId": report_id}))
}

fn get_sbom_export(svc: &Svc) -> Response {
    let report_id = svc.body("reportId").as_str().unwrap_or("").to_string();
    let found = svc.with(|st| st.sbom_exports.get(&report_id).cloned());
    match found {
        Some(dict) => ok(dict),
        None => ok(json!({"reportId": report_id, "status": "NOT_FOUND"})),
    }
}

// --- Configuration ---

fn get_configuration(svc: &Svc) -> Response {
    let config = svc.with(|st| st.configuration.clone());
    ok(config)
}

fn update_configuration(svc: &Svc) -> Response {
    let ecr = svc.body("ecrConfiguration");
    let ec2 = svc.body("ec2Configuration");
    svc.with(|st| {
        if ecr.is_object() {
            st.configuration["ecrConfiguration"] = ecr;
        }
        if let Value::Object(map) = &mut st.configuration {
            if let Value::Object(ec2_map) = &ec2 {
                if !ec2_map.is_empty() || map.contains_key("ec2Configuration") {
                    map.insert("ec2Configuration".to_string(), ec2.clone());
                }
            }
        }
    });
    ok(json!({}))
}

fn get_encryption_key(svc: &Svc) -> Response {
    let scan_type = svc
        .body("scanType")
        .as_str()
        .map(String::from)
        .or_else(|| svc.query1("scanType"))
        .unwrap_or_default();
    let resource_type = svc
        .body("resourceType")
        .as_str()
        .map(String::from)
        .or_else(|| svc.query1("resourceType"))
        .unwrap_or_default();
    let key = format!("{scan_type}:{resource_type}");
    let kms_key_id = svc.with(|st| st.encryption_keys.get(&key).cloned().unwrap_or_default());
    ok(json!({"kmsKeyId": kms_key_id}))
}

fn update_encryption_key(svc: &Svc, set: bool) -> Response {
    let scan_type = svc.body("scanType").as_str().unwrap_or("").to_string();
    let resource_type = svc.body("resourceType").as_str().unwrap_or("").to_string();
    let key = format!("{scan_type}:{resource_type}");
    svc.with(|st| {
        if set {
            let kms_key_id = svc.body("kmsKeyId").as_str().unwrap_or("").to_string();
            st.encryption_keys.insert(key, kms_key_id);
        } else {
            st.encryption_keys.remove(&key);
        }
    });
    ok(json!({}))
}

fn get_ec2_deep_inspection_config(svc: &Svc) -> Response {
    let config = svc.with(|st| st.ec2_deep_inspection_config.clone());
    ok(config)
}

fn update_ec2_deep_inspection_config(svc: &Svc) -> Response {
    let activate = svc.body("activateDeepInspection");
    let package_paths = svc.body("packagePaths");
    let config = svc.with(|st| {
        if let Some(v) = activate.as_bool() {
            st.ec2_deep_inspection_config["status"] =
                json!(if v { "ACTIVATED" } else { "DEACTIVATED" });
        }
        if package_paths.is_array() {
            st.ec2_deep_inspection_config["packagePaths"] = package_paths;
        }
        st.ec2_deep_inspection_config.clone()
    });
    ok(config)
}

fn update_org_ec2_deep_inspection(svc: &Svc) -> Response {
    let org_package_paths = svc.body("orgPackagePaths");
    svc.with(|st| {
        if org_package_paths.is_array() {
            st.ec2_deep_inspection_config["orgPackagePaths"] = org_package_paths;
        }
    });
    ok(json!({}))
}

// --- Code security ---

fn create_code_security_integration(svc: &Svc) -> Response {
    let id = Uuid::new_v4().to_string();
    let arn = format!(
        "arn:aws:inspector2:{}:{}:owner/{}/code-security-integration/{}",
        svc.region, svc.account, svc.account, id
    );
    let dict = json!({
        "integrationArn": arn,
        "name": svc.body("name"),
        "type": svc.body("type"),
        "status": "PENDING",
        "statusReason": "",
        "createdOn": now_unix(),
        "lastUpdateOn": now_unix(),
        "tags": {},
    });
    svc.with(|st| st.code_security_integrations.insert(arn, dict.clone()));
    ok(json!({
        "integrationArn": dict.get("integrationArn").cloned().unwrap_or(json!("")),
        "status": "PENDING",
    }))
}

fn delete_code_security_integration(svc: &Svc) -> Response {
    let arn = svc.body("integrationArn").as_str().unwrap_or("").to_string();
    svc.with(|st| st.code_security_integrations.remove(&arn));
    ok(json!({"integrationArn": arn}))
}

fn get_code_security_integration(svc: &Svc) -> Response {
    let arn = svc.body("integrationArn").as_str().unwrap_or("").to_string();
    let found = svc.with(|st| st.code_security_integrations.get(&arn).cloned());
    ok(found.unwrap_or(json!({})))
}

fn update_code_security_integration(svc: &Svc) -> Response {
    let arn = svc.body("integrationArn").as_str().unwrap_or("").to_string();
    let details = svc.body("details");
    let (returned_arn, status) = svc.with(|st| {
        match st.code_security_integrations.get_mut(&arn) {
            Some(integration) => {
                if details.is_object() {
                    integration["details"] = details;
                }
                integration["lastUpdateOn"] = json!(now_unix());
                (arn.clone(), integration.get("status").cloned().unwrap_or(json!("PENDING")))
            }
            None => (arn.clone(), json!("NOT_FOUND")),
        }
    });
    ok(json!({"integrationArn": returned_arn, "status": status}))
}

fn list_code_security_integrations(svc: &Svc) -> Response {
    let items = svc
        .with(|st| st.code_security_integrations.values().cloned().collect::<Vec<Value>>());
    ok(json!({"integrations": items}))
}

fn create_code_security_scan_configuration(svc: &Svc) -> Response {
    let id = Uuid::new_v4().to_string();
    let arn = format!(
        "arn:aws:inspector2:{}:{}:owner/{}/code-security-scan-configuration/{}",
        svc.region, svc.account, svc.account, id
    );
    let dict = json!({
        "scanConfigurationArn": arn,
        "name": svc.body("name"),
        "configuration": svc.body("configuration"),
        "level": svc.body("level"),
        "scopeSettings": svc.body("scopeSettings"),
        "createdAt": now_unix(),
        "lastUpdatedAt": now_unix(),
        "tags": {},
        "associations": [],
    });
    let arn_map = arn.clone();
        svc.with(|st| st.code_security_scan_configurations.insert(arn_map, dict));
    ok(json!({"scanConfigurationArn": arn}))
}

fn delete_code_security_scan_configuration(svc: &Svc) -> Response {
    let arn = svc.body("scanConfigurationArn").as_str().unwrap_or("").to_string();
    svc.with(|st| st.code_security_scan_configurations.remove(&arn));
    ok(json!({"scanConfigurationArn": arn}))
}

fn get_code_security_scan_configuration(svc: &Svc) -> Response {
    let arn = svc.body("scanConfigurationArn").as_str().unwrap_or("").to_string();
    let found = svc.with(|st| st.code_security_scan_configurations.get(&arn).cloned());
    ok(found.unwrap_or(json!({})))
}

fn update_code_security_scan_configuration(svc: &Svc) -> Response {
    let arn = svc.body("scanConfigurationArn").as_str().unwrap_or("").to_string();
    let configuration = svc.body("configuration");
    svc.with(|st| {
        if let Some(config) = st.code_security_scan_configurations.get_mut(&arn) {
            if configuration.is_object() {
                config["configuration"] = configuration;
            }
            config["lastUpdatedAt"] = json!(now_unix());
        }
    });
    ok(json!({"scanConfigurationArn": arn}))
}

fn list_code_security_scan_configurations(svc: &Svc) -> Response {
    let configs = svc
        .with(|st| st.code_security_scan_configurations.values().cloned().collect::<Vec<Value>>())
        .into_iter()
        .map(|dict| {
            json!({
                "scanConfigurationArn": dict.get("scanConfigurationArn"),
                "name": dict.get("name"),
                "level": dict.get("level"),
            })
        })
        .collect::<Vec<Value>>();
    ok(json!({"configurations": configs}))
}

fn list_scan_configuration_associations(svc: &Svc) -> Response {
    let arn = svc
        .body("scanConfigurationArn")
        .as_str()
        .unwrap_or("")
        .to_string();
    let associations = svc
        .with(|st| {
            match st.code_security_scan_configurations.get(&arn) {
                Some(config) => config
                    .get("associations")
                    .cloned()
                    .unwrap_or(json!([])),
                None => json!([]),
            }
        });
    ok(json!({"associations": associations}))
}

fn batch_associate(svc: &Svc, associate: bool) -> Response {
    let field = if associate {
        "associateConfigurationRequests"
    } else {
        "disassociateConfigurationRequests"
    };
    let requests = svc.body(field).as_array().cloned().unwrap_or_default();
    let mut successful = Vec::new();
    let mut failed = Vec::new();
    let known: Vec<bool> = svc
        .with(|st| {
            requests
                .iter()
                .map(|req| {
                    let arn = req
                        .get("scanConfigurationArn")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    st.code_security_scan_configurations.contains_key(&arn)
                })
                .collect()
        });
    for (i, req) in requests.into_iter().enumerate() {
        let arn = req
            .get("scanConfigurationArn")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let resource = req.get("resource").cloned().unwrap_or(json!({}));
        if known.get(i).copied().unwrap_or(false) {
            successful.push(json!({"scanConfigurationArn": arn, "resource": resource}));
        } else {
            failed.push(json!({
                "scanConfigurationArn": arn,
                "resource": resource,
                "errorCode": "INTERNAL_ERROR",
                "errorMessage": "Configuration not found",
            }));
        }
    }
    ok(json!({"failedAssociations": failed, "successfulAssociations": successful}))
}

fn start_code_security_scan(svc: &Svc) -> Response {
    let scan_id = Uuid::new_v4().to_string();
    let resource = svc.body("resource");
    let dict = json!({
        "scanId": scan_id,
        "resource": resource,
        "accountId": svc.account,
        "status": "IN_PROGRESS",
        "createdAt": now_unix(),
        "updatedAt": now_unix(),
    });
    svc.with(|st| st.code_security_scans.insert(scan_id.clone(), dict));
    ok(json!({"scanId": scan_id, "status": "IN_PROGRESS"}))
}

fn get_code_security_scan(svc: &Svc) -> Response {
    let scan_id = svc.body("scanId").as_str().unwrap_or("").to_string();
    let resource = svc.body("resource");
    let found = svc.with(|st| st.code_security_scans.get(&scan_id).cloned());
    match found {
        Some(dict) => ok(dict),
        None => ok(json!({
            "scanId": scan_id,
            "resource": resource,
            "accountId": svc.account,
            "status": "NOT_FOUND",
            "createdAt": now_unix(),
            "updatedAt": now_unix(),
        })),
    }
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
    let keys = svc.query.get("tagKeys").cloned().unwrap_or_default();
    svc.with(|st| {
        if let Some(entry) = st.tagger.get_mut(arn) {
            for k in keys {
                entry.remove(&k);
            }
        }
    });
    ok(json!({}))
}

/// Body list-of-strings helper (same shape as the other ports).
fn str_list(v: Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .map(|e| e.as_str().unwrap_or("").to_string())
                .collect()
        })
        .unwrap_or_default()
}
