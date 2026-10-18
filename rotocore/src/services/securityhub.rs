//! Security Hub backend (moto-parity port).
//!
//! Rest-JSON service: operations route by (method, URI template) from the
//! botocore spec; ARN path parameters are the decoded remainder after the
//! matched prefix. Untouched ops fall through to a deny-by-default 501,
//! matching moto's response for unimplemented actions.

use crate::proto::{json_error, now_iso, parse_json_body, query_map, url_decode};
use crate::store::{slot, ScopeKey, Slot};
use crate::{Request, Response};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use uuid::Uuid;

type Tags = BTreeMap<String, String>;

/// Per-(account, region) Security Hub state.
#[derive(Default)]
struct State {
    enabled: bool,
    subscribed_at: Option<String>,
    auto_enable_controls: bool,
    control_finding_generator: String,
    findings: Vec<Value>,
    members: BTreeMap<String, Value>,
    tags: BTreeMap<String, Tags>,
    action_targets: BTreeMap<String, Value>,
    insights: BTreeMap<String, Value>,
    standards_subs: BTreeMap<String, Value>,
    standards_controls: BTreeMap<String, Value>,
    automation_rules: BTreeMap<String, Value>,
    finding_aggregators: BTreeMap<String, Value>,
    configuration_policies: BTreeMap<String, Value>,
    policy_associations: BTreeMap<String, Value>,
    security_controls: BTreeMap<String, Value>,
    product_subscriptions: BTreeMap<String, String>,
    hub_v2_arn: Option<String>,
    automation_rules_v2: BTreeMap<String, Value>,
    aggregators_v2: BTreeMap<String, Value>,
    connectors_v2: BTreeMap<String, Value>,
}

impl State {
    fn init(&mut self) {
        self.auto_enable_controls = true;
        self.control_finding_generator = "SECURITY_CONTROL".to_string();
    }
}

/// Built-in standards moto advertises.
const BUILTIN_STANDARDS: &[(&str, &str, &str, bool)] = &[
    (
        "arn:aws:securityhub:::ruleset/cis-aws-foundations-benchmark/v/1.2.0",
        "CIS AWS Foundations Benchmark v1.2.0",
        "The Center for Internet Security (CIS) AWS Foundations Benchmark v1.2.0",
        true,
    ),
    (
        "arn:aws:securityhub::{region}::standards/aws-foundational-security-best-practices/v/1.0.0",
        "AWS Foundational Security Best Practices",
        "AWS Foundational Security Best Practices",
        true,
    ),
    (
        "arn:aws:securityhub:::ruleset/cis-aws-foundations-benchmark/v/1.4.0",
        "CIS AWS Foundations Benchmark v1.4.0",
        "The Center for Internet Security (CIS) AWS Foundations Benchmark v1.4.0",
        false,
    ),
    (
        "arn:aws:securityhub:::standards/pci-dss/v/3.2.1",
        "PCI DSS v3.2.1",
        "Payment Card Industry Data Security Standard (PCI DSS) v3.2.1",
        false,
    ),
];

/// Built-in product integrations moto advertises: (arn template, name, company).
const BUILTIN_PRODUCTS: &[(&str, &str, &str)] = &[
    (
        "arn:aws:securityhub:{region}::product/aws/guardduty",
        "GuardDuty",
        "Amazon",
    ),
    (
        "arn:aws:securityhub:{region}::product/aws/inspector",
        "Inspector",
        "Amazon",
    ),
    (
        "arn:aws:securityhub:{region}::product/aws/macie",
        "Macie",
        "Amazon",
    ),
    (
        "arn:aws:securityhub:{region}::product/aws/config",
        "Config",
        "Amazon",
    ),
    (
        "arn:aws:securityhub:{region}::product/aws/firewall-manager",
        "Firewall Manager",
        "Amazon",
    ),
    (
        "arn:aws:securityhub:{region}::product/aws/access-analyzer",
        "IAM Access Analyzer",
        "Amazon",
    ),
    (
        "arn:aws:securityhub:{region}::product/aws/systems-manager-patch-manager",
        "Systems Manager Patch Manager",
        "Amazon",
    ),
];

fn ok(body: Value) -> Response {
    Response::json_status(200, &body)
}

fn not_implemented(_svc: &Svc) -> Response {
    err(501, "NotImplemented", "The action has not been implemented")
}

fn err(status: u16, code: &str, message: &str) -> Response {
    json_error(status, code, message)
}

fn not_found(where_: &str, id: &str) -> Response {
    err(
        400,
        "ResourceNotFoundException",
        &format!("The {where_} {id} was not found."),
    )
}

/// Everything after `prefix/` in the decoded path tail (ARN / id params).
fn tail_after(tail: &str, prefix: &str) -> String {
    let marker = format!("{prefix}/");
    match tail.find(&marker) {
        Some(idx) => tail[idx + marker.len()..].to_string(),
        None => tail.to_string(),
    }
}

/// Dispatch a routed Security Hub request. Always answers (501 fallback).
pub fn dispatch(req: &Request) -> Response {
    let state: Slot<State> = slot("securityhub");
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

    // Ops whose last path segment is the request's ARN / identifier parameter.
    if segs.len() >= 2 {
        match (m, segs[0], segs[1]) {
            ("GET", "tags", _) => {
                return list_tags_for_resource(&svc, &tail_after(&svc.path, "tags"))
            }
            ("POST", "tags", _) => return tag_resource(&svc, &tail_after(&svc.path, "tags")),
            ("DELETE", "tags", _) => return untag_resource(&svc, &tail_after(&svc.path, "tags")),
            ("DELETE", "actionTargets", _) => {
                return delete_action_target(&svc, &tail_after(&svc.path, "actionTargets"))
            }
            ("PATCH", "actionTargets", _) => {
                return update_action_target(&svc, &tail_after(&svc.path, "actionTargets"))
            }
            ("DELETE", "insights", _) => {
                return delete_insight(&svc, &tail_after(&svc.path, "insights"))
            }
            ("PATCH", "insights", _) => {
                return update_insight(&svc, &tail_after(&svc.path, "insights"))
            }
            ("GET", "insights", "results") => {
                return get_insight_results(&svc, &tail_after(&svc.path, "insights/results"))
            }
            ("DELETE", "aggregatorv2", "delete") => {
                return delete_aggregator_v2(&svc, &tail_after(&svc.path, "aggregatorv2/delete"))
            }
            ("GET", "aggregatorv2", "get") => {
                return get_aggregator_v2(&svc, &tail_after(&svc.path, "aggregatorv2/get"))
            }
            ("PATCH", "aggregatorv2", "update") => {
                return update_aggregator_v2(&svc, &tail_after(&svc.path, "aggregatorv2/update"))
            }
            ("DELETE", "findingAggregator", "delete") => {
                return delete_finding_aggregator(
                    &svc,
                    &tail_after(&svc.path, "findingAggregator/delete"),
                )
            }
            ("GET", "findingAggregator", "get") => {
                return get_finding_aggregator(
                    &svc,
                    &tail_after(&svc.path, "findingAggregator/get"),
                )
            }
            ("GET", "automationrulesv2", _) => {
                return get_automation_rule_v2(&svc, &tail_after(&svc.path, "automationrulesv2"))
            }
            ("PATCH", "automationrulesv2", _) => {
                return update_automation_rule_v2(&svc, &tail_after(&svc.path, "automationrulesv2"))
            }
            ("DELETE", "automationrulesv2", _) => {
                return delete_automation_rule_v2(&svc, &tail_after(&svc.path, "automationrulesv2"))
            }
            ("GET", "configurationPolicy", "get") => {
                return get_configuration_policy(
                    &svc,
                    &tail_after(&svc.path, "configurationPolicy/get"),
                )
            }
            ("PATCH", "configurationPolicy", _) => {
                return update_configuration_policy(
                    &svc,
                    &tail_after(&svc.path, "configurationPolicy"),
                )
            }
            ("DELETE", "configurationPolicy", _) => {
                return delete_configuration_policy(
                    &svc,
                    &tail_after(&svc.path, "configurationPolicy"),
                )
            }
            ("GET", "connectorsv2", _) => {
                return get_connector_v2(&svc, &tail_after(&svc.path, "connectorsv2"))
            }
            ("PATCH", "connectorsv2", _) => {
                return update_connector_v2(&svc, &tail_after(&svc.path, "connectorsv2"))
            }
            ("DELETE", "connectorsv2", _) => {
                return delete_connector_v2(&svc, &tail_after(&svc.path, "connectorsv2"))
            }
            ("DELETE", "productSubscriptions", _) => {
                return disable_import_findings_for_product(
                    &svc,
                    &tail_after(&svc.path, "productSubscriptions"),
                )
            }
            ("GET", "standards", "controls") => {
                return describe_standards_controls(
                    &svc,
                    &tail_after(&svc.path, "standards/controls"),
                )
            }
            ("PATCH", "standards", "control") => {
                return update_standards_control(&svc, &tail_after(&svc.path, "standards/control"))
            }
            _ => {}
        }
    }

    route_static(&svc, m, &segs)
}

/// Routes for ops without a path parameter.
fn route_static(svc: &Svc, m: &str, segs: &[&str]) -> Response {
    let s0 = segs.first().copied().unwrap_or("");
    let s1 = segs.get(1).copied().unwrap_or("");
    match (m, s0, s1) {
        ("GET", "accounts", _) => describe_hub(svc),
        ("POST", "accounts", _) => enable_security_hub(svc),
        ("DELETE", "accounts", _) => disable_security_hub(svc),
        ("PATCH", "accounts", _) => update_security_hub_configuration(svc),
        ("GET", "hubv2", _) => describe_security_hub_v2(svc),
        ("POST", "hubv2", _) => enable_security_hub_v2(svc),
        ("DELETE", "hubv2", _) => disable_security_hub_v2(svc),
        ("POST", "findings", "import") => batch_import_findings(svc),
        ("POST", "findings", _) => get_findings(svc),
        ("PATCH", "findings", "batchupdate") => batch_update_findings(svc),
        ("PATCH", "findings", _) => update_findings(svc),
        ("POST", "findingHistory", _) => ok(json!({"Records": [], "NextToken": null})),
        ("POST", "findingsv2", "statistics") | ("POST", "resourcesv2", "statistics") => {
            ok(json!({"GroupByResults": []}))
        }
        ("POST", "findingsTrendsv2", _) | ("POST", "resourcesTrendsv2", _) => {
            ok(json!({"Granularity": "DAILY", "TrendsMetrics": [], "NextToken": null}))
        }
        ("POST", "findingsv2", _) => get_findings_v2(svc),
        ("PATCH", "findingsv2", _) => batch_update_findings_v2(svc),
        ("POST", "resourcesv2", _) => ok(json!({"Resources": []})),
        ("GET", "administrator", _) => ok(json!({})),
        ("GET", "master", _) => ok(json!({})),
        ("POST", "administrator", "disassociate") | ("POST", "master", "disassociate") => {
            ok(json!({}))
        }
        ("POST", "administrator", _) | ("POST", "master", _) => ok(json!({})),
        ("POST", "members", "delete") => delete_members(svc),
        ("POST", "members", "get") => get_members(svc),
        ("POST", "members", "disassociate") => disassociate_members(svc),
        ("POST", "members", "invite") => invite_members(svc),
        ("POST", "members", _) => create_members(svc),
        ("GET", "members", _) => list_members(svc),
        ("POST", "invitations", "decline") | ("POST", "invitations", "delete") => {
            ok(json!({"UnprocessedAccounts": []}))
        }
        ("GET", "invitations", "count") => ok(json!({"InvitationsCount": 0})),
        ("GET", "invitations", _) => ok(json!({"Invitations": []})),
        ("POST", "actionTargets", "get") => describe_action_targets(svc),
        ("POST", "actionTargets", _) => create_action_target(svc),
        ("POST", "insights", "get") => get_insights(svc),
        ("POST", "insights", _) => create_insight(svc),
        ("POST", "standards", "register") => batch_enable_standards(svc),
        ("POST", "standards", "deregister") => batch_disable_standards(svc),
        ("POST", "standards", "get") => get_enabled_standards(svc),
        ("GET", "standards", "") => describe_standards(svc),
        ("POST", "associations", "batchGet") => batch_get_control_associations(svc),
        ("PATCH", "associations", _) => ok(json!({"UnprocessedAssociationUpdates": []})),
        ("GET", "associations", _) => list_control_associations(svc),
        ("POST", "automationrules", "create") => create_automation_rule(svc),
        ("POST", "automationrules", "get") => batch_get_automation_rules(svc),
        ("POST", "automationrules", "delete") => batch_delete_automation_rules(svc),
        ("PATCH", "automationrules", "update") => batch_update_automation_rules(svc),
        ("GET", "automationrules", "list") => list_automation_rules(svc),
        ("POST", "findingAggregator", "create") => create_finding_aggregator(svc),
        ("PATCH", "findingAggregator", "update") => update_finding_aggregator_body(svc),
        ("GET", "findingAggregator", "list") => list_finding_aggregators(svc),
        ("POST", "configurationPolicy", "create") => create_configuration_policy(svc),
        ("GET", "configurationPolicy", "list") => list_configuration_policies(svc),
        ("POST", "configurationPolicyAssociation", "associate") => start_policy_association(svc),
        ("POST", "configurationPolicyAssociation", "disassociate") => {
            start_policy_disassociation(svc)
        }
        ("POST", "configurationPolicyAssociation", "get") => get_policy_association(svc),
        ("POST", "configurationPolicyAssociation", "batchget") => {
            batch_get_policy_associations(svc)
        }
        ("POST", "configurationPolicyAssociation", _) => list_policy_associations(svc),
        ("POST", "securityControls", "batchGet") => batch_get_security_controls(svc),
        ("PATCH", "securityControl", "update") => update_security_control(svc),
        ("GET", "securityControl", "definition") => get_security_control_definition(svc),
        ("GET", "securityControls", "definitions") => ok(json!({"SecurityControlDefinitions": []})),
        ("GET", "products", _) => describe_products(svc),
        ("GET", "productsV2", _) => describe_products_v2(svc),
        ("POST", "productSubscriptions", _) => enable_import_findings_for_product(svc),
        ("GET", "productSubscriptions", _) => list_enabled_products_for_import(svc),
        ("GET", "organization", "configuration") => describe_organization_configuration(svc),
        ("POST", "organization", "configuration") => update_organization_configuration(svc),
        ("GET", "organization", "admin") => list_organization_admin_accounts(svc),
        ("POST", "organization", "admin") if segs.len() >= 3 => org_not_in_use(),
        ("POST", "organization", "enable") | ("POST", "organization", "disable") => {
            org_not_in_use()
        }
        ("GET", "connectorsv2", _) => list_connectors_v2(svc),
        ("GET", "automationrulesv2", "list") => list_automation_rules_v2(svc),
        ("GET", "aggregatorv2", "list") => list_aggregators_v2(svc),
        ("POST", "aggregatorv2", "create") => create_aggregator_v2(svc),
        ("POST", "automationrulesv2", "create") => create_automation_rule_v2(svc),
        ("POST", "connectorsv2", "register") => register_connector_v2(svc),
        ("POST", "connectorsv2", _) => create_connector_v2(svc),
        ("POST", "ticketsv2", _) => create_ticket_v2(svc),
        _ => not_implemented(svc),
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

    /// Query-string scalar (GET list ops carry params in the query).
    fn query1(&self, name: &str) -> Option<String> {
        self.query.get(name).and_then(|v| v.first()).cloned()
    }

    fn hub_arn(&self) -> String {
        format!(
            "arn:aws:securityhub:{}:{}:hub/default",
            self.region, self.account
        )
    }
}

// --- Hub lifecycle ---

fn enable_security_hub(svc: &Svc) -> Response {
    let tags = svc.body("Tags");
    svc.with(|st| {
        st.enabled = true;
        st.subscribed_at = Some(now_iso());
        if let Some(map) = tags.as_object() {
            let mut seeded = BTreeMap::new();
            for (k, v) in map {
                seeded.insert(k.clone(), v.as_str().unwrap_or_default().to_string());
            }
            let hub_arn = svc.hub_arn();
            merge_tags(st, &hub_arn, &seeded);
        }
    });
    ok(json!({}))
}

fn disable_security_hub(svc: &Svc) -> Response {
    let subscribed = svc.with(|st| st.enabled);
    if !subscribed {
        return err(
            400,
            "InvalidAccessException",
            "Account is not subscribed to AWS Security Hub",
        );
    }
    svc.with(|st| {
        st.enabled = false;
        st.subscribed_at = None;
        st.findings.clear();
    });
    ok(json!({}))
}

fn describe_hub(svc: &Svc) -> Response {
    let (enabled, subscribed_at, auto, cfg) = svc.with(|st| {
        (
            st.enabled,
            st.subscribed_at.clone(),
            st.auto_enable_controls,
            st.control_finding_generator.clone(),
        )
    });
    if !enabled {
        return err(
            400,
            "InvalidAccessException",
            "Account is not subscribed to AWS Security Hub",
        );
    }
    let tags = svc.with(|st| st.tags.get(&svc.hub_arn()).cloned().unwrap_or_default());
    ok(json!({
        "HubArn": svc.hub_arn(),
        "SubscribedAt": subscribed_at,
        "AutoEnableControls": auto,
        "ControlFindingGenerator": cfg,
        "Tags": tags,
    }))
}

fn update_security_hub_configuration(svc: &Svc) -> Response {
    let auto_enable = svc.body("AutoEnableControls");
    let generator = svc.body("ControlFindingGenerator");
    svc.with(|st| {
        if !st.enabled {
            return;
        }
        if let Some(v) = auto_enable.as_bool() {
            st.auto_enable_controls = v;
        }
        if let Some(v) = generator.as_str() {
            st.control_finding_generator = v.to_string();
        }
    });
    ok(json!({}))
}

// --- Findings ---

/// Evaluate the EQUALS filter members moto callers use on GetFindings.
fn finding_matches(finding: &Value, filters: Option<&serde_json::Map<String, Value>>) -> bool {
    let Some(list) = filters else { return true };
    for (field, entries) in list {
        let Some(entries) = entries.as_array() else {
            continue;
        };
        let actual: Vec<String> = match field.as_str() {
            "Id" => vec![finding
                .get("Id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()],
            "Title" => vec![finding
                .get("Title")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()],
            "SeverityLabel" => vec![finding
                .pointer("/Severity/Label")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()],
            "ResourceType" => finding
                .get("Resources")
                .and_then(Value::as_array)
                .map(|rs| {
                    rs.iter()
                        .map(|r| {
                            r.get("Type")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string()
                        })
                        .collect()
                })
                .unwrap_or_default(),
            _ => vec![], // other filter fields are ignored, like moto
        };
        for cond in entries {
            let want = cond.get("Value").and_then(Value::as_str).unwrap_or("");
            let equals = cond
                .get("Comparison")
                .and_then(Value::as_str)
                .unwrap_or("EQUALS");
            if equals == "EQUALS" && !actual.iter().any(|a| a == want) {
                return false;
            }
        }
    }
    true
}

fn get_findings(svc: &Svc) -> Response {
    let filters = svc.body("Filters");
    let max_results = svc.body("MaxResults").as_u64();
    if let Some(max) = max_results {
        if !(1..=100).contains(&max) {
            return err(
                400,
                "InvalidInputException",
                "MaxResults must be a number between 1 and 100",
            );
        }
    }
    let mut findings: Vec<Value> = svc
        .with(|st| st.findings.clone())
        .into_iter()
        .filter(|f| finding_matches(f, filters.as_object()))
        .collect();
    if let Some(max) = max_results {
        findings.truncate(max as usize);
    }
    ok(json!({"Findings": findings, "NextToken": null}))
}

/// Add or update a tag map entry on `arn` (moto's TaggingService semantics).
fn merge_tags(st: &mut State, arn: &str, tags: &BTreeMap<String, String>) {
    let entry = st.tags.entry(arn.to_string()).or_default();
    for (k, v) in tags {
        entry.insert(k.clone(), v.clone());
    }
}

fn batch_import_findings(svc: &Svc) -> Response {
    let imported_list: Vec<Value> = svc.body("Findings").as_array().cloned().unwrap_or_default();
    let mut success = 0u64;
    let mut failed: Vec<Value> = Vec::new();
    for finding_data in imported_list {
        let ok_import = finding_data
            .get("Resources")
            .and_then(Value::as_array)
            .map(|r| !r.is_empty())
            .unwrap_or(false);
        if !ok_import {
            failed.push(json!({
                "Id": finding_data.get("Id"),
                "ErrorCode": "InvalidInput",
                "ErrorMessage": "Finding must contain at least one resource in the Resources array",
            }));
            continue;
        }
        let fid = finding_data
            .get("Id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        success += 1;
        entries_insert(svc, fid, finding_data);
    }
    ok(json!({
        "FailedCount": failed.len() as u64,
        "FailedFindings": failed,
        "SuccessCount": success,
    }))
}

fn batch_update_findings(svc: &Svc) -> Response {
    let mut processed: Vec<Value> = Vec::new();
    let mut unprocessed: Vec<Value> = Vec::new();
    let note = svc.body("Note");
    let severity = svc.body("Severity");
    let verification_state = svc.body("VerificationState");
    let confidence = svc.body("Confidence");
    let criticality = svc.body("Criticality");
    let types = svc.body("Types");
    let user_defined = svc.body("UserDefinedFields");
    let workflow = svc.body("Workflow");
    let related = svc.body("RelatedFindings");
    let identifiers = svc
        .body("FindingIdentifiers")
        .as_array()
        .cloned()
        .unwrap_or_default();
    svc.with(|st| {
        for idf in identifiers {
            let fid = idf
                .get("Id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let existing = st
                .findings
                .iter_mut()
                .find(|f| f.get("Id").and_then(Value::as_str) == Some(fid.as_str()));
            let Some(existing) = existing else {
                unprocessed.push(json!({
                    "FindingIdentifier": idf,
                    "ErrorCode": "FindingNotFound",
                    "ErrorMessage": format!("Finding with Id {fid} not found"),
                }));
                continue;
            };
            if !note.is_null() {
                existing["Note"] = note.clone();
            }
            if severity.is_object() {
                let map = existing
                    .as_object_mut()
                    .and_then(|o| o.entry("Severity").or_insert(json!({})).as_object_mut());
                if let Some(sev) = severity.as_object() {
                    if let Some(m) = map {
                        for (k, v) in sev {
                            m.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
            if !verification_state.is_null() {
                existing["VerificationState"] = verification_state.clone();
            }
            if let Some(c) = confidence.as_u64() {
                existing["Confidence"] = json!(c);
            }
            if let Some(c) = criticality.as_u64() {
                existing["Criticality"] = json!(c);
            }
            if types.is_array() {
                existing["Types"] = types.clone();
            }
            if let Some(newmap) = user_defined.as_object() {
                if let Some(m) = existing.as_object_mut().and_then(|o| {
                    o.entry("UserDefinedFields")
                        .or_insert(json!({}))
                        .as_object_mut()
                }) {
                    for (k, v) in newmap {
                        m.insert(k.clone(), v.clone());
                    }
                }
            }
            if let Some(wf) = workflow.as_object() {
                let map = existing
                    .as_object_mut()
                    .map(|o| o.entry("Workflow").or_insert(json!({})));
                if let Some(Value::Object(m)) = map {
                    for (k, v) in wf {
                        m.insert(k.clone(), v.clone());
                    }
                }
            }
            if related.is_array() {
                existing["RelatedFindings"] = related.clone();
            }
            existing["UpdatedAt"] = json!(now_iso());
            let prod = idf.get("ProductArn").cloned().unwrap_or(Value::Null);
            processed.push(json!({"Id": fid, "ProductArn": prod}));
        }
    });
    ok(json!({
        "ProcessedFindings": processed,
        "UnprocessedFindings": unprocessed,
    }))
}

fn entries_insert(svc: &Svc, fid: String, finding: Value) {
    svc.with(move |st| {
        if let Some(existing) = st
            .findings
            .iter_mut()
            .find(|f| f.get("Id").and_then(Value::as_str) == Some(fid.as_str()))
        {
            if let (Some(newmap), Some(map)) = (finding.as_object(), existing.as_object_mut()) {
                for (k, v) in newmap {
                    map.insert(k.clone(), v.clone());
                }
            }
        } else {
            st.findings.push(finding);
        }
    });
}

fn update_findings(svc: &Svc) -> Response {
    let note = svc.body("Note");
    let record_state = svc.body("RecordState");
    svc.with(|st| {
        for finding in &mut st.findings {
            if !note.is_null() {
                finding["Note"] = note.clone();
            }
            if !record_state.is_null() {
                finding["RecordState"] = record_state.clone();
            }
            finding["UpdatedAt"] = json!(now_iso());
        }
    });
    ok(json!({}))
}

fn get_findings_v2(svc: &Svc) -> Response {
    let findings = svc.with(|st| st.findings.clone());
    ok(json!({"Findings": findings, "NextToken": null}))
}

fn batch_update_findings_v2(svc: &Svc) -> Response {
    let comment = svc.body("Comment");
    let severity_id = svc.body("SeverityId");
    let status_id = svc.body("StatusId");
    let mut processed: Vec<Value> = Vec::new();
    let mut unprocessed: Vec<Value> = Vec::new();
    let mut identifiers = svc
        .body("FindingIdentifiers")
        .as_array()
        .cloned()
        .unwrap_or_default();
    for uid in svc
        .body("MetadataUids")
        .as_array()
        .cloned()
        .unwrap_or_default()
    {
        identifiers.push(json!({"Id": uid, "ProductArn": ""}));
    }
    svc.with(|st| {
        for idf in identifiers {
            let fid = idf
                .get("Id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let existing = st
                .findings
                .iter_mut()
                .find(|f| f.get("Id").and_then(Value::as_str) == Some(fid.as_str()));
            let Some(existing) = existing else {
                unprocessed.push(json!({
                    "FindingIdentifier": idf,
                    "ErrorCode": "FindingNotFound",
                    "ErrorMessage": format!("Finding {fid} not found"),
                }));
                continue;
            };
            if let Some(text) = comment.as_str() {
                let map = existing
                    .as_object_mut()
                    .and_then(|o| o.entry("Note").or_insert(json!({})).as_object_mut());
                if let Some(m) = map {
                    m.insert("Text".to_string(), json!(text));
                }
            }
            if let Some(sid) = severity_id.as_i64() {
                existing["SeverityId"] = json!(sid);
            }
            if let Some(sid) = status_id.as_i64() {
                existing["StatusId"] = json!(sid);
            }
            existing["UpdatedAt"] = json!(now_iso());
            let prod = idf.get("ProductArn").cloned().unwrap_or(json!(""));
            processed.push(json!({"Id": fid, "ProductArn": prod}));
        }
    });
    ok(json!({
        "ProcessedFindings": processed,
        "UnprocessedFindings": unprocessed,
    }))
}

// --- Members & invitations ---

fn member_with_defaults(account_id: &str, email: &str) -> Value {
    let now = now_iso();
    json!({
        "AccountId": account_id,
        "Email": email,
        "MemberStatus": "ENABLED",
        "InvitedAt": now,
        "UpdatedAt": now,
    })
}

fn create_members(svc: &Svc) -> Response {
    let details = svc
        .body("AccountDetails")
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut unprocessed: Vec<Value> = Vec::new();
    let created: Vec<(String, String)> = details
        .into_iter()
        .map(|d| {
            (
                d.get("AccountId")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                d.get("Email")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            )
        })
        .collect();
    let mut new_members: Vec<(String, Value)> = Vec::new();
    for (account_id, email) in created {
        if account_id.is_empty() {
            unprocessed.push(json!({
                "AccountId": "",
                "ProcessingResult": "Invalid input: AccountId is required",
            }));
            continue;
        }
        new_members.push((
            account_id.clone(),
            member_with_defaults(&account_id, &email),
        ));
    }
    svc.with(|st| {
        for (account_id, member) in new_members {
            if st.members.contains_key(&account_id) {
                unprocessed.push(json!({
                    "AccountId": account_id,
                    "ProcessingResult": format!("Account {account_id} is already a member"),
                }));
                continue;
            }
            st.members.insert(account_id, member);
        }
    });
    ok(json!({"UnprocessedAccounts": unprocessed}))
}

fn get_members(svc: &Svc) -> Response {
    let ids = svc
        .body("AccountIds")
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut members: Vec<Value> = Vec::new();
    let mut unprocessed: Vec<Value> = Vec::new();
    let state_members = svc.with(|st| st.members.clone());
    for id in ids {
        let account_id = id.as_str().unwrap_or("");
        match state_members.get(account_id) {
            Some(m) => members.push(m.clone()),
            None => unprocessed.push(json!({
                "AccountId": account_id,
                "ProcessingResult": format!("Account {account_id} is not a member"),
            })),
        }
    }
    ok(json!({"Members": members, "UnprocessedAccounts": unprocessed}))
}

fn list_members(svc: &Svc) -> Response {
    let members: Vec<Value> = svc
        .with(|st| st.members.values().cloned().collect::<Vec<Value>>())
        .into_iter()
        .filter(|m| m.get("MemberStatus").and_then(Value::as_str) == Some("ENABLED"))
        .collect();
    ok(json!({"Members": members}))
}

fn delete_members(svc: &Svc) -> Response {
    let ids = str_list(svc.body("AccountIds"));
    let mut unprocessed: Vec<Value> = Vec::new();
    svc.with(|st| {
        for account_id in ids {
            if st.members.remove(&account_id).is_none() {
                unprocessed.push(json!({
                    "AccountId": account_id,
                    "ProcessingResult": format!("Account {account_id} is not a member"),
                }));
            }
        }
    });
    ok(json!({"UnprocessedAccounts": unprocessed}))
}

fn disassociate_members(svc: &Svc) -> Response {
    let ids = str_list(svc.body("AccountIds"));
    svc.with(|st| {
        for account_id in ids {
            if let Some(m) = st.members.get_mut(&account_id) {
                m["MemberStatus"] = json!("DISASSOCIATED");
            }
        }
    });
    ok(json!({}))
}

fn invite_members(svc: &Svc) -> Response {
    let ids = str_list(svc.body("AccountIds"));
    let mut unprocessed: Vec<Value> = Vec::new();
    svc.with(|st| {
        for account_id in ids {
            match st.members.get_mut(&account_id) {
                Some(m) => {
                    m["MemberStatus"] = json!("INVITED");
                }
                None => unprocessed.push(json!({
                    "AccountId": account_id,
                    "ProcessingResult": format!("Account {account_id} is not a member"),
                })),
            }
        }
    });
    ok(json!({"UnprocessedAccounts": unprocessed}))
}

fn org_not_in_use() -> Response {
    err(
        400,
        "AWSOrganizationsNotInUseException",
        "AWS Organizations is not in use.",
    )
}

fn describe_organization_configuration(_svc: &Svc) -> Response {
    err(
        400,
        "AccessDeniedException",
        "You do not have sufficient access to perform this action.",
    )
}

fn update_organization_configuration(_svc: &Svc) -> Response {
    err(
        400,
        "ResourceNotFoundException",
        "The request was rejected because AWS Organizations is not in use or not configured for this account.",
    )
}

fn list_organization_admin_accounts(_svc: &Svc) -> Response {
    ok(json!({"AdminAccounts": []}))
}

fn enable_import_findings_for_product(svc: &Svc) -> Response {
    let product_arn = svc.body("ProductArn").as_str().unwrap_or("").to_string();
    let sub_arn = format!(
        "arn:aws:securityhub:{}:{}:product-subscription/{}",
        svc.region,
        svc.account,
        product_arn.split("::product/").nth(1).unwrap_or("")
    );
    let conflict = svc.with(|st| st.product_subscriptions.contains_key(&product_arn));
    if conflict {
        return err(
            400,
            "ResourceConflictException",
            &format!("The product subscription for {product_arn} already exists."),
        );
    }
    svc.with(|st| {
        st.product_subscriptions
            .insert(product_arn, sub_arn.clone());
    });
    ok(json!({"ProductSubscriptionArn": sub_arn}))
}

fn disable_import_findings_for_product(svc: &Svc, sub_arn: &str) -> Response {
    let found = svc.with(|st| {
        let hit = st
            .product_subscriptions
            .iter()
            .find(|(_, v)| v.as_str() == sub_arn)
            .map(|(k, _)| k.clone());
        let existed = hit.is_some();
        if let Some(product) = hit {
            st.product_subscriptions.remove(&product);
        }
        existed
    });
    if found {
        ok(json!({}))
    } else {
        not_found("product subscription", sub_arn)
    }
}

fn list_enabled_products_for_import(svc: &Svc) -> Response {
    let subs = svc.with(|st| {
        st.product_subscriptions
            .values()
            .cloned()
            .collect::<Vec<String>>()
    });
    ok(json!({"ProductSubscriptions": subs}))
}

// --- Tags ---

fn list_tags_for_resource(svc: &Svc, arn: &str) -> Response {
    let tags = svc.with(|st| st.tags.get(arn).cloned().unwrap_or_default());
    ok(json!({"Tags": tags}))
}

fn tag_resource(svc: &Svc, arn: &str) -> Response {
    let tags = svc.body("Tags");
    svc.with(|st| {
        let mut map = BTreeMap::new();
        if let Some(obj) = tags.as_object() {
            for (k, v) in obj {
                map.insert(k.clone(), v.as_str().unwrap_or_default().to_string());
            }
        }
        merge_tags(st, arn, &map);
    });
    ok(json!({}))
}

fn untag_resource(svc: &Svc, arn: &str) -> Response {
    let keys = svc.query.get("tagKeys").cloned().unwrap_or_default();
    svc.with(|st| {
        if let Some(entry) = st.tags.get_mut(arn) {
            for k in keys {
                entry.remove(&k);
            }
        }
    });
    ok(json!({}))
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

// --- Action targets ---

fn create_action_target(svc: &Svc) -> Response {
    let name = svc.body("Name").as_str().unwrap_or("").to_string();
    let description = svc.body("Description").as_str().unwrap_or("").to_string();
    let target_id = svc.body("Id").as_str().unwrap_or("").to_string();
    let arn = format!(
        "arn:aws:securityhub:{}:{}:action/custom/{}",
        svc.region, svc.account, target_id
    );
    let record = json!({"ActionTargetArn": arn, "Name": name, "Description": description});
    let conflict = svc.with(|st| st.action_targets.contains_key(&arn));
    if conflict {
        return err(
            400,
            "ResourceConflictException",
            &format!("The action target {arn} already exists."),
        );
    }
    let arn_for_map = arn.clone();
    svc.with(|st| {
        st.action_targets.insert(arn_for_map, record.clone());
    });
    ok(json!({"ActionTargetArn": arn}))
}

fn describe_action_targets(svc: &Svc) -> Response {
    let filters = str_list(svc.body("ActionTargetArns"));
    let all: Vec<Value> =
        svc.with(|st| st.action_targets.values().cloned().collect::<Vec<Value>>());
    let mut targets = Vec::new();
    if !filters.is_empty() {
        let stored = svc.with(|st| st.action_targets.clone());
        for arn in &filters {
            match stored.get(arn) {
                Some(t) => targets.push(t.clone()),
                None => return not_found("action target", arn),
            }
        }
    } else {
        targets = all;
    }
    ok(json!({"ActionTargets": targets}))
}

fn update_action_target(svc: &Svc, arn: &str) -> Response {
    let name = svc.body("Name");
    let description = svc.body("Description");
    let hit = svc.with(|st| {
        let Some(t) = st.action_targets.get_mut(arn) else {
            return false;
        };
        if let Some(n) = name.as_str() {
            t["Name"] = json!(n);
        }
        if let Some(d) = description.as_str() {
            t["Description"] = json!(d);
        }
        true
    });
    if hit {
        ok(json!({}))
    } else {
        not_found("action target", arn)
    }
}

fn delete_action_target(svc: &Svc, arn: &str) -> Response {
    let removed = svc.with(|st| st.action_targets.remove(arn).is_some());
    if removed {
        ok(json!({"ActionTargetArn": arn}))
    } else {
        not_found("action target", arn)
    }
}

// --- Insights ---

fn create_insight(svc: &Svc) -> Response {
    let name = svc.body("Name").as_str().unwrap_or("").to_string();
    let filters = svc.body("Filters");
    let group_by = svc.body("GroupByAttribute");
    let arn = format!(
        "arn:aws:securityhub:{}:{}:insight/{}",
        svc.region,
        svc.account,
        Uuid::new_v4()
    );
    let record = json!({
        "InsightArn": arn,
        "Name": name,
        "Filters": filters,
        "GroupByAttribute": group_by,
    });
    svc.with(|st| {
        st.insights.insert(arn.clone(), record);
    });
    ok(json!({"InsightArn": arn}))
}

fn get_insights(svc: &Svc) -> Response {
    let asked = str_list(svc.body("InsightArns"));
    if asked.is_empty() {
        let insights = svc.with(|st| st.insights.values().cloned().collect::<Vec<Value>>());
        ok(json!({"Insights": insights}))
    } else {
        let insights = svc.with(|st| {
            asked
                .iter()
                .filter_map(|arn| st.insights.get(arn).cloned())
                .collect::<Vec<Value>>()
        });
        ok(json!({"Insights": insights}))
    }
}

fn get_insight_results(svc: &Svc, arn: &str) -> Response {
    let stored = svc.with(|st| st.insights.get(arn).cloned());
    match stored {
        Some(insight) => ok(json!({
            "InsightResults": {
                "InsightArn": arn,
                "GroupByAttribute": insight.get("GroupByAttribute"),
                "ResultValues": [],
            }
        })),
        None => not_found("insight", arn),
    }
}

fn update_insight(svc: &Svc, arn: &str) -> Response {
    let name = svc.body("Name");
    let filters = svc.body("Filters");
    let group_by = svc.body("GroupByAttribute");
    let hit = svc.with(|st| {
        let Some(i) = st.insights.get_mut(arn) else {
            return false;
        };
        if let Some(n) = name.as_str() {
            i["Name"] = json!(n);
        }
        if filters.is_object() {
            i["Filters"] = filters.clone();
        }
        if let Some(g) = group_by.as_str() {
            i["GroupByAttribute"] = json!(g);
        }
        true
    });
    if hit {
        ok(json!({}))
    } else {
        not_found("insight", arn)
    }
}

fn delete_insight(svc: &Svc, arn: &str) -> Response {
    let removed = svc.with(|st| st.insights.remove(arn).is_some());
    if removed {
        ok(json!({"InsightArn": arn}))
    } else {
        not_found("insight", arn)
    }
}

// --- Standards ---

fn batch_enable_standards(svc: &Svc) -> Response {
    let requests = svc
        .body("StandardsSubscriptionRequests")
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut results: Vec<Value> = Vec::new();
    svc.with(|st| {
        for req in requests {
            let standards_arn = req
                .get("StandardsArn")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let tail = standards_arn
                .split('/')
                .next_back()
                .unwrap_or("")
                .to_string();
            let sub_arn = format!("arn:aws:securityhub:::subscription/{tail}");
            let input = req.get("StandardsInput").cloned().unwrap_or(json!({}));
            let dict = json!({
                "StandardsArn": standards_arn,
                "StandardsSubscriptionArn": sub_arn.clone(),
                "StandardsInput": input,
                "StandardsStatus": "READY",
            });
            st.standards_subs.insert(sub_arn, dict.clone());
            results.push(dict);
        }
    });
    ok(json!({"StandardsSubscriptions": results}))
}
fn batch_disable_standards(svc: &Svc) -> Response {
    let arns = str_list(svc.body("StandardsSubscriptionArns"));
    let mut results: Vec<Value> = Vec::new();
    let mut missing: Option<String> = None;
    svc.with(|st| {
        for arn in arns {
            match st.standards_subs.remove(&arn) {
                Some(mut dict) => {
                    dict["StandardsStatus"] = json!("INCOMPLETE");
                    results.push(dict);
                }
                None => missing = Some(arn.clone()),
            }
        }
    });
    if let Some(arn) = missing {
        return not_found("standards subscription", &arn);
    }
    ok(json!({"StandardsSubscriptions": results}))
}

fn get_enabled_standards(svc: &Svc) -> Response {
    let asked = str_list(svc.body("StandardsSubscriptionArns"));
    let subs: Vec<Value> = svc.with(|st| {
        if asked.is_empty() {
            st.standards_subs.values().cloned().collect()
        } else {
            asked
                .iter()
                .filter_map(|a| st.standards_subs.get(a).cloned())
                .collect()
        }
    });
    ok(json!({"StandardsSubscriptions": subs}))
}

fn describe_standards(svc: &Svc) -> Response {
    let standards: Vec<Value> = BUILTIN_STANDARDS
        .iter()
        .map(|(arn, name, desc, enabled_default)| {
            json!({
                "StandardsArn": arn.replace("{region}", &svc.region),
                "Name": name,
                "Description": desc,
                "EnabledByDefault": enabled_default,
            })
        })
        .collect();
    ok(json!({"Standards": standards}))
}

fn describe_standards_controls(svc: &Svc, sub_arn: &str) -> Response {
    let exists = svc.with(|st| st.standards_subs.contains_key(sub_arn));
    if !exists {
        return not_found("standards subscription", sub_arn);
    }
    let controls: Vec<Value> = svc
        .with(|st| {
            st.standards_controls
                .values()
                .cloned()
                .collect::<Vec<Value>>()
        })
        .into_iter()
        .filter(|c| {
            c.get("StandardsControlArn")
                .and_then(Value::as_str)
                .map(|a| a.starts_with(sub_arn))
                .unwrap_or(false)
        })
        .collect();
    ok(json!({"Controls": controls}))
}

fn control_default(svc: &Svc, control_id: &str) -> Value {
    json!({
        "SecurityControlId": control_id,
        "SecurityControlArn": format!(
            "arn:aws:securityhub:{}:{}:security-control/{}",
            svc.region, svc.account, control_id
        ),
        "Title": control_id,
        "Description": format!("Control {control_id}"),
        "RemediationUrl": "",
        "SeverityRating": "MEDIUM",
        "SecurityControlStatus": "ENABLED",
        "Parameters": {},
        "LastUpdateReason": "",
    })
}

fn update_standards_control(svc: &Svc, arn: &str) -> Response {
    let control_status = svc.body("ControlStatus");
    let reason = svc.body("DisabledReason");
    svc.with(|st| {
        let entry = st
            .standards_controls
            .entry(arn.to_string())
            .or_insert_with(|| {
                let control_id = arn.split('/').next_back().unwrap_or("").to_string();
                json!({
                    "StandardsControlArn": arn,
                    "ControlStatus": "ENABLED",
                    "ControlId": control_id,
                    "Title": control_id,
                    "Description": format!("Control {control_id}"),
                    "SeverityRating": "MEDIUM",
                    "RelatedRequirements": [],
                    "RemediationUrl": "",
                })
            });
        if let Some(status) = control_status.as_str() {
            entry["ControlStatus"] = json!(status);
        }
        if let Some(r) = reason.as_str() {
            entry["DisabledReason"] = json!(r);
        }
    });
    ok(json!({}))
}

fn batch_get_control_associations(svc: &Svc) -> Response {
    let ids = svc
        .body("StandardsControlAssociationIds")
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut details = Vec::new();
    for id in ids {
        let control_id = id
            .get("SecurityControlId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let mut summary = control_association_summary(svc, &control_id);
        summary["StandardsArn"] = id.get("StandardsArn").cloned().unwrap_or(json!(""));
        details.push(summary);
    }
    ok(json!({
        "StandardsControlAssociationDetails": details,
        "UnprocessedAssociations": [],
    }))
}

fn control_association_summary(svc: &Svc, control_id: &str) -> Value {
    json!({
        "StandardsArn": "",
        "SecurityControlId": control_id,
        "SecurityControlArn": format!(
            "arn:aws:securityhub:{}:{}:security-control/{}",
            svc.region, svc.account, control_id
        ),
        "AssociationStatus": "ENABLED",
        "RelatedRequirements": [],
        "UpdatedAt": now_iso(),
        "UpdatedReason": "",
        "StandardsControlTitle": control_id,
        "StandardsControlDescription": format!("Control {control_id}"),
    })
}

fn list_control_associations(svc: &Svc) -> Response {
    let control_id = svc.query1("SecurityControlId").unwrap_or_default();
    let summaries: Vec<Value> = svc
        .with(|st| st.standards_subs.values().cloned().collect::<Vec<Value>>())
        .into_iter()
        .map(|sub| {
            let mut s = control_association_summary(svc, &control_id);
            s["StandardsArn"] = sub.get("StandardsArn").cloned().unwrap_or(json!(""));
            s
        })
        .collect();
    ok(json!({"StandardsControlAssociationSummaries": summaries}))
}

// --- Automation rules (v1) ---

fn create_automation_rule(svc: &Svc) -> Response {
    let now = now_iso();
    let rule_arn = format!(
        "arn:aws:securityhub:{}:{}:automation-rule/{}",
        svc.region,
        svc.account,
        Uuid::new_v4()
    );
    let record = json!({
        "RuleArn": rule_arn,
        "RuleName": svc.body("RuleName"),
        "RuleOrder": svc.body("RuleOrder"),
        "RuleStatus": svc.body("RuleStatus"),
        "Description": svc.body("Description"),
        "IsTerminal": svc.body("IsTerminal"),
        "Criteria": svc.body("Criteria"),
        "Actions": svc.body("Actions"),
        "CreatedAt": now,
        "UpdatedAt": now,
    });
    svc.with(|st| {
        st.automation_rules.insert(rule_arn.clone(), record);
    });
    ok(json!({"RuleArn": rule_arn}))
}

fn batch_get_automation_rules(svc: &Svc) -> Response {
    let arns = str_list(svc.body("AutomationRulesArns"));
    let mut rules = Vec::new();
    let mut unprocessed = Vec::new();
    let stored = svc.with(|st| st.automation_rules.clone());
    for arn in arns {
        match stored.get(&arn) {
            Some(rule) => rules.push(rule.clone()),
            None => unprocessed.push(json!({
                "RuleArn": arn,
                "ErrorCode": 404,
                "ErrorMessage": format!("Rule {arn} not found"),
            })),
        }
    }
    ok(json!({"Rules": rules, "UnprocessedAutomationRules": unprocessed}))
}

fn batch_delete_automation_rules(svc: &Svc) -> Response {
    let arns = str_list(svc.body("AutomationRulesArns"));
    let mut processed = Vec::new();
    let mut unprocessed = Vec::new();
    svc.with(|st| {
        for arn in arns {
            if st.automation_rules.remove(&arn).is_some() {
                processed.push(arn);
            } else {
                unprocessed.push(json!({
                    "RuleArn": arn,
                    "ErrorCode": 404,
                    "ErrorMessage": format!("Rule {arn} not found"),
                }));
            }
        }
    });
    ok(json!({
        "ProcessedAutomationRules": processed,
        "UnprocessedAutomationRules": unprocessed,
    }))
}

fn batch_update_automation_rules(svc: &Svc) -> Response {
    let items = svc
        .body("UpdateAutomationRulesRequestItems")
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut processed = Vec::new();
    let mut unprocessed = Vec::new();
    svc.with(|st| {
        for item in items {
            let arn = item
                .get("RuleArn")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let Some(rule) = st.automation_rules.get_mut(&arn) else {
                unprocessed.push(json!({
                    "RuleArn": arn,
                    "ErrorCode": 404,
                    "ErrorMessage": format!("Rule {arn} not found"),
                }));
                continue;
            };
            if let Some(v) = item.get("RuleStatus") {
                rule["RuleStatus"] = v.clone();
            }
            if let Some(v) = item.get("RuleOrder") {
                rule["RuleOrder"] = v.clone();
            }
            if let Some(v) = item.get("Description") {
                rule["Description"] = v.clone();
            }
            if let Some(v) = item.get("RuleName") {
                rule["RuleName"] = v.clone();
            }
            if let Some(v) = item.get("IsTerminal") {
                rule["IsTerminal"] = v.clone();
            }
            if let Some(v) = item.get("Criteria") {
                rule["Criteria"] = v.clone();
            }
            if let Some(v) = item.get("Actions") {
                rule["Actions"] = v.clone();
            }
            rule["UpdatedAt"] = json!(now_iso());
            processed.push(arn.clone());
        }
    });
    ok(json!({
        "ProcessedAutomationRules": processed,
        "UnprocessedAutomationRules": unprocessed,
    }))
}

fn list_automation_rules(svc: &Svc) -> Response {
    let metadata: Vec<Value> = svc
        .with(|st| {
            st.automation_rules
                .values()
                .cloned()
                .collect::<Vec<Value>>()
        })
        .into_iter()
        .map(|mut rule| {
            if let Some(obj) = rule.as_object_mut() {
                obj.remove("Criteria");
                obj.remove("Actions");
            }
            rule
        })
        .collect();
    ok(json!({"AutomationRulesMetadata": metadata}))
}

// --- Finding aggregator ---

fn create_finding_aggregator(svc: &Svc) -> Response {
    let mode = svc.body("RegionLinkingMode");
    let regions = svc.body("Regions");
    let arn = format!(
        "arn:aws:securityhub:{}:{}:finding-aggregator/{}",
        svc.region,
        svc.account,
        Uuid::new_v4()
    );
    let dict = json!({
        "FindingAggregatorArn": arn,
        "FindingAggregationRegion": svc.region,
        "RegionLinkingMode": mode,
        "Regions": regions,
    });
    svc.with(|st| {
        st.finding_aggregators.insert(arn.clone(), dict.clone());
    });
    ok(dict)
}

fn get_finding_aggregator(svc: &Svc, arn: &str) -> Response {
    let stored = svc.with(|st| st.finding_aggregators.get(arn).cloned());
    match stored {
        Some(dict) => ok(dict),
        None => not_found("finding aggregator", arn),
    }
}

fn update_finding_aggregator_body(svc: &Svc) -> Response {
    let arn = svc
        .body("FindingAggregatorArn")
        .as_str()
        .unwrap_or("")
        .to_string();
    let mode = svc.body("RegionLinkingMode");
    let regions = svc.body("Regions");
    let updated = svc.with(|st| {
        let agg = st.finding_aggregators.get_mut(&arn)?;
        if !mode.is_null() {
            agg["RegionLinkingMode"] = mode.clone();
        }
        if !regions.is_null() {
            agg["Regions"] = regions.clone();
        }
        Some(agg.clone())
    });
    match updated {
        Some(dict) => ok(dict),
        None => not_found("finding aggregator", &arn),
    }
}

fn delete_finding_aggregator(svc: &Svc, arn: &str) -> Response {
    let removed = svc.with(|st| st.finding_aggregators.remove(arn).is_some());
    if removed {
        ok(json!({}))
    } else {
        not_found("finding aggregator", arn)
    }
}

fn list_finding_aggregators(svc: &Svc) -> Response {
    let aggs = svc.with(|st| {
        st.finding_aggregators
            .values()
            .map(|a| {
                json!({
                    "FindingAggregatorArn": a.get("FindingAggregatorArn"),
                    "FindingAggregationRegion": a.get("FindingAggregationRegion"),
                })
            })
            .collect::<Vec<Value>>()
    });
    ok(json!({"FindingAggregators": aggs}))
}

// --- Configuration policies & associations ---

fn policy_dict(
    account: &str,
    region: &str,
    name: &str,
    description: &str,
    config: Value,
    created: &str,
) -> (String, String, Value) {
    let policy_id = Uuid::new_v4().to_string();
    let arn = format!("arn:aws:securityhub:{region}:{account}:configuration-policy/{policy_id}");
    (
        policy_id.clone(),
        arn.clone(),
        json!({
            "Arn": arn,
            "Id": policy_id,
            "Name": name,
            "Description": description,
            "ConfigurationPolicy": config,
            "CreatedAt": created,
            "UpdatedAt": created,
        }),
    )
}

fn create_configuration_policy(svc: &Svc) -> Response {
    let name = svc.body("Name").as_str().unwrap_or("").to_string();
    let description = svc.body("Description").as_str().unwrap_or("").to_string();
    let config = svc.body("ConfigurationPolicy");
    let now = now_iso();
    let (_id, _arn, dict) =
        policy_dict(&svc.account, &svc.region, &name, &description, config, &now);
    svc.with(|st| {
        let pid = dict
            .get("Id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        st.configuration_policies.insert(pid, dict.clone());
    });
    ok(dict)
}

/// Find a policy stored by id whose name matches the identifier, by id, or
/// by ARN (moto supports id and ARN lookup).
fn find_policy(st: &State, identifier: &str) -> Option<Value> {
    if let Some(p) = st.configuration_policies.get(identifier) {
        return Some(p.clone());
    }
    st.configuration_policies
        .values()
        .find(|p| p.get("Arn").and_then(Value::as_str) == Some(identifier))
        .cloned()
}

fn get_configuration_policy(svc: &Svc, identifier: &str) -> Response {
    let found = svc.with(|st| find_policy(st, identifier));
    match found {
        Some(dict) => ok(dict),
        None => not_found("configuration policy", identifier),
    }
}

fn update_configuration_policy(svc: &Svc, identifier: &str) -> Response {
    let name = svc.body("Name");
    let description = svc.body("Description");
    let config = svc.body("ConfigurationPolicy");
    let found = svc.with(|st| {
        let pid = st
            .configuration_policies
            .iter()
            .find(|(id, p)| {
                id.as_str() == identifier
                    || p.get("Arn").and_then(Value::as_str) == Some(identifier)
            })
            .map(|(id, _)| id.clone());
        let pid = pid?;
        let existing = st
            .configuration_policies
            .iter_mut()
            .find(|(id, _)| id.as_str() == pid)
            .map(|(_, p)| p);
        if let Some(policy) = existing {
            if let Some(n) = name.as_str() {
                policy["Name"] = json!(n);
            }
            if let Some(d) = description.as_str() {
                policy["Description"] = json!(d);
            }
            if config.is_object() {
                policy["ConfigurationPolicy"] = config.clone();
            }
            policy["UpdatedAt"] = json!(now_iso());
            return Some(policy.clone());
        }
        None
    });
    match found {
        Some(dict) => ok(dict),
        None => not_found("configuration policy", identifier),
    }
}

fn delete_configuration_policy(svc: &Svc, identifier: &str) -> Response {
    let removed = svc.with(|st| {
        let pid = st
            .configuration_policies
            .iter()
            .find(|(id, p)| {
                id.as_str() == identifier
                    || p.get("Arn").and_then(Value::as_str) == Some(identifier)
            })
            .map(|(id, _)| id.clone());
        match pid {
            Some(pid) => st.configuration_policies.remove(&pid).is_some(),
            None => false,
        }
    });
    if removed {
        ok(json!({}))
    } else {
        not_found("configuration policy", identifier)
    }
}

fn list_configuration_policies(svc: &Svc) -> Response {
    let summaries: Vec<Value> = svc
        .with(|st| {
            st.configuration_policies
                .values()
                .cloned()
                .collect::<Vec<Value>>()
        })
        .into_iter()
        .map(|policy| {
            json!({
                "Arn": policy.get("Arn"),
                "Id": policy.get("Id"),
                "Name": policy.get("Name"),
                "Description": policy.get("Description"),
                "UpdatedAt": policy.get("UpdatedAt"),
                "ServiceEnabled": policy
                    .pointer("/ConfigurationPolicy/SecurityHub/ServiceEnabled")
                    .cloned()
                    .unwrap_or(json!(false)),
            })
        })
        .collect();
    ok(json!({"ConfigurationPolicySummaries": summaries}))
}

// --- Configuration policy associations ---

/// Target id string from a Target dict (AccountId | OrganizationalUnitId | RootId).
fn target_id(target: &Value) -> String {
    for key in ["AccountId", "OrganizationalUnitId", "RootId"] {
        if let Some(v) = target.get(key).and_then(Value::as_str) {
            return v.to_string();
        }
    }
    String::new()
}

fn association_dict(policy_id: &str, target_id: &str, target: &Value) -> Value {
    json!({
        "ConfigurationPolicyId": policy_id,
        "TargetId": target_id,
        "TargetType": target_type(target),
        "AssociationType": "APPLIED",
        "UpdatedAt": now_iso(),
        "AssociationStatus": "SUCCESS",
        "AssociationStatusMessage": "",
    })
}

fn target_type(target: &Value) -> &str {
    if target.get("OrganizationalUnitId").is_some() {
        "ORGANIZATIONAL_UNIT"
    } else if target.get("RootId").is_some() {
        "ROOT"
    } else {
        "ACCOUNT"
    }
}

fn start_policy_association(svc: &Svc) -> Response {
    let policy_id = svc
        .body("ConfigurationPolicyIdentifier")
        .as_str()
        .unwrap_or("")
        .to_string();
    let target = svc.body("Target");
    let tid = target_id(&target);
    let dict = association_dict(&policy_id, &tid, &target);
    svc.with(|st| {
        st.policy_associations.insert(tid, dict.clone());
    });
    ok(dict)
}

fn start_policy_disassociation(svc: &Svc) -> Response {
    let target = svc.body("Target");
    let tid = target_id(&target);
    svc.with(|st| {
        st.policy_associations.remove(&tid);
    });
    ok(json!({}))
}

fn get_policy_association(svc: &Svc) -> Response {
    let target = svc.body("Target");
    let tid = target_id(&target);
    let stored = svc.with(|st| st.policy_associations.get(&tid).cloned());
    match stored {
        Some(dict) => ok(dict),
        None => not_found("configuration policy association", &tid),
    }
}

fn batch_get_policy_associations(svc: &Svc) -> Response {
    let identifiers = svc
        .body("ConfigurationPolicyAssociationIdentifiers")
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut results = Vec::new();
    let mut unprocessed = Vec::new();
    let stored = svc.with(|st| st.policy_associations.clone());
    for identifier in identifiers {
        let tid = target_id(identifier.get("Target").unwrap_or(&Value::Null));
        match stored.get(&tid) {
            Some(d) => results.push(d.clone()),
            None => unprocessed.push(json!({
                "ConfigurationPolicyAssociationIdentifiers": identifier,
                "ErrorCode": "404",
                "ErrorReason": "Not found",
            })),
        }
    }
    ok(json!({
        "ConfigurationPolicyAssociations": results,
        "UnprocessedConfigurationPolicyAssociations": unprocessed,
    }))
}

fn list_policy_associations(svc: &Svc) -> Response {
    let results = svc.with(|st| {
        st.policy_associations
            .values()
            .cloned()
            .collect::<Vec<Value>>()
    });
    ok(json!({"ConfigurationPolicyAssociationSummaries": results}))
}

// --- Security controls ---

fn batch_get_security_controls(svc: &Svc) -> Response {
    let ids = str_list(svc.body("SecurityControlIds"));
    let mut controls = Vec::new();
    let stored = svc.with(|st| st.security_controls.clone());
    for id in ids {
        match stored.get(&id) {
            Some(c) => controls.push(c.clone()),
            None => controls.push(control_default(svc, &id)),
        }
    }
    ok(json!({"SecurityControls": controls, "UnprocessedIds": []}))
}

fn update_security_control(svc: &Svc) -> Response {
    let id = svc
        .body("SecurityControlId")
        .as_str()
        .unwrap_or("")
        .to_string();
    let parameters = svc.body("Parameters");
    let reason = svc.body("LastUpdateReason");
    svc.with(|st| {
        let existing = st.security_controls.entry(id.clone()).or_insert_with(|| {
            json!({
                "SecurityControlId": id,
                "SecurityControlArn": "",
                "Title": id,
                "Description": format!("Control {id}"),
                "RemediationUrl": "",
                "SeverityRating": "MEDIUM",
                "SecurityControlStatus": "ENABLED",
                "Parameters": {},
                "LastUpdateReason": "",
            })
        });
        existing["Parameters"] = parameters;
        if let Some(r) = reason.as_str() {
            existing["LastUpdateReason"] = json!(r);
        }
    });
    ok(json!({}))
}

fn get_security_control_definition(svc: &Svc) -> Response {
    let id = svc.query1("SecurityControlId").unwrap_or_default();
    ok(json!({
        "SecurityControlDefinition": {
            "SecurityControlId": id,
            "Title": id,
            "Description": format!("Control {id}"),
            "RemediationUrl": "",
            "SeverityRating": "MEDIUM",
            "CurrentRegionAvailability": "AVAILABLE",
            "CustomizableProperties": ["Parameters"],
            "ParameterDefinitions": {},
        }
    }))
}

// --- Products (v1 + v2) ---

fn describe_products(svc: &Svc) -> Response {
    let filter = svc.query1("ProductArn");
    let products: Vec<Value> = BUILTIN_PRODUCTS
        .iter()
        .filter(|(arn, _, _)| match &filter {
            Some(f) => arn.replace("{region}", &svc.region) == *f,
            None => true,
        })
        .map(|(arn_template, name, company)| {
            let arn = arn_template.replace("{region}", &svc.region);
            json!({
                "ProductArn": arn,
                "ProductName": name,
                "CompanyName": company,
                "Description": format!("{name} integration"),
                "Categories": ["Software and Configuration Checks"],
                "IntegrationTypes": ["SEND_FINDINGS_TO_SECURITY_HUB"],
                "MarketplaceUrl": "",
                "ActivationUrl": "",
                "ProductSubscriptionResourcePolicy": "",
            })
        })
        .collect();
    ok(json!({"Products": products}))
}

fn describe_products_v2(svc: &Svc) -> Response {
    let products: Vec<Value> = BUILTIN_PRODUCTS
        .iter()
        .map(|(arn_template, name, company)| {
            json!({
                "ProductArn": arn_template.replace("{region}", &svc.region),
                "ProductName": name,
                "CompanyName": company,
            })
        })
        .collect();
    ok(json!({"ProductsV2": products}))
}

// --- V2 hub / rules / aggregators / connectors / tickets ---

fn describe_security_hub_v2(svc: &Svc) -> Response {
    let state = svc.with(|st| (st.hub_v2_arn.clone(), st.subscribed_at.clone()));
    match state {
        (Some(hub_arn), Some(subscribed_at)) => ok(json!({
            "HubV2Arn": hub_arn,
            "SubscribedAt": subscribed_at,
        })),
        _ => err(
            400,
            "InvalidAccessException",
            "Account is not subscribed to AWS Security Hub V2",
        ),
    }
}

fn enable_security_hub_v2(svc: &Svc) -> Response {
    let hub_arn = format!(
        "arn:aws:securityhub:{}:{}:hubv2/default",
        svc.region, svc.account
    );
    svc.with(|st| {
        st.hub_v2_arn = Some(hub_arn.clone());
        st.subscribed_at = Some(now_iso());
    });
    ok(json!({"HubV2Arn": hub_arn}))
}

fn disable_security_hub_v2(svc: &Svc) -> Response {
    svc.with(|st| {
        st.hub_v2_arn = None;
    });
    ok(json!({}))
}

fn create_automation_rule_v2(svc: &Svc) -> Response {
    let rule_id = Uuid::new_v4().to_string();
    let arn = format!(
        "arn:aws:securityhub:{}:{}:automation-rule-v2/{}",
        svc.region, svc.account, rule_id
    );
    let now = now_iso();
    let record = json!({
        "RuleArn": arn,
        "RuleId": rule_id,
        "RuleOrder": svc.body("RuleOrder"),
        "RuleName": svc.body("RuleName"),
        "RuleStatus": svc.body("RuleStatus"),
        "Description": svc.body("Description"),
        "Criteria": svc.body("Criteria"),
        "Actions": svc.body("Actions"),
        "CreatedAt": now,
        "UpdatedAt": now,
    });
    svc.with(|st| {
        st.automation_rules_v2.insert(rule_id.clone(), record);
    });
    ok(json!({"RuleArn": arn, "RuleId": rule_id}))
}

fn find_rule_v2_id(st: &State, id: &str) -> Option<String> {
    if st.automation_rules_v2.contains_key(id) {
        return Some(id.to_string());
    }
    st.automation_rules_v2
        .iter()
        .find(|(_, rule)| rule.get("RuleArn").and_then(Value::as_str) == Some(id))
        .map(|(rid, _)| rid.clone())
}

fn get_automation_rule_v2(svc: &Svc, id: &str) -> Response {
    let found = svc.with(|st| find_rule_v2_id(st, id));
    match found {
        Some(rid) => {
            let rule = svc.with(|st| st.automation_rules_v2.get(&rid).cloned());
            match rule {
                Some(r) => ok(r),
                None => not_found("automation rule", id),
            }
        }
        None => not_found("automation rule", id),
    }
}

fn update_automation_rule_v2(svc: &Svc, id: &str) -> Response {
    let status = svc.body("RuleStatus");
    let order = svc.body("RuleOrder");
    let description = svc.body("Description");
    let name = svc.body("RuleName");
    let criteria = svc.body("Criteria");
    let actions = svc.body("Actions");
    let found = svc.with(|st| {
        let rid = find_rule_v2_id(st, id);
        match rid {
            Some(rid) => {
                if let Some(rule) = st.automation_rules_v2.get_mut(&rid) {
                    if !status.is_null() {
                        rule["RuleStatus"] = status.clone();
                    }
                    if !order.is_null() {
                        rule["RuleOrder"] = order.clone();
                    }
                    if !description.is_null() {
                        rule["Description"] = description.clone();
                    }
                    if !name.is_null() {
                        rule["RuleName"] = name.clone();
                    }
                    if !criteria.is_null() {
                        rule["Criteria"] = criteria.clone();
                    }
                    if !actions.is_null() {
                        rule["Actions"] = actions.clone();
                    }
                    rule["UpdatedAt"] = json!(now_iso());
                }
                true
            }
            None => false,
        }
    });
    if found {
        ok(json!({}))
    } else {
        not_found("automation rule", id)
    }
}

fn delete_automation_rule_v2(svc: &Svc, id: &str) -> Response {
    let found = svc.with(|st| {
        let rid = find_rule_v2_id(st, id);
        match rid {
            Some(rid) => st.automation_rules_v2.remove(&rid).is_some(),
            None => false,
        }
    });
    if found {
        ok(json!({}))
    } else {
        not_found("automation rule", id)
    }
}

fn list_automation_rules_v2(svc: &Svc) -> Response {
    let rules: Vec<Value> = svc
        .with(|st| {
            st.automation_rules_v2
                .values()
                .cloned()
                .collect::<Vec<Value>>()
        })
        .into_iter()
        .map(|mut r| {
            if let Some(obj) = r.as_object_mut() {
                obj.remove("Criteria");
                obj.remove("Actions");
            }
            r
        })
        .collect();
    ok(json!({"Rules": rules}))
}

fn create_aggregator_v2(svc: &Svc) -> Response {
    let mode = svc.body("RegionLinkingMode");
    let linked = svc.body("LinkedRegions");
    let arn = format!(
        "arn:aws:securityhub:{}:{}:aggregator-v2/{}",
        svc.region,
        svc.account,
        Uuid::new_v4()
    );
    let dict = json!({
        "AggregatorV2Arn": arn,
        "AggregationRegion": svc.region,
        "RegionLinkingMode": mode,
        "LinkedRegions": linked,
    });
    svc.with(|st| {
        st.aggregators_v2.insert(arn.clone(), dict.clone());
    });
    ok(dict)
}

fn get_aggregator_v2(svc: &Svc, arn: &str) -> Response {
    let stored = svc.with(|st| st.aggregators_v2.get(arn).cloned());
    match stored {
        Some(dict) => ok(dict),
        None => not_found("aggregator", arn),
    }
}

fn update_aggregator_v2(svc: &Svc, arn: &str) -> Response {
    let mode = svc.body("RegionLinkingMode");
    let linked = svc.body("LinkedRegions");
    let updated = svc.with(|st| {
        let agg = st.aggregators_v2.get_mut(arn)?;
        if !mode.is_null() {
            agg["RegionLinkingMode"] = mode.clone();
        }
        if !linked.is_null() {
            agg["LinkedRegions"] = linked.clone();
        }
        Some(agg.clone())
    });
    match updated {
        Some(dict) => ok(dict),
        None => not_found("aggregator", arn),
    }
}

fn delete_aggregator_v2(svc: &Svc, arn: &str) -> Response {
    let removed = svc.with(|st| st.aggregators_v2.remove(arn).is_some());
    if removed {
        ok(json!({}))
    } else {
        not_found("aggregator", arn)
    }
}

fn list_aggregators_v2(svc: &Svc) -> Response {
    let aggs = svc.with(|st| st.aggregators_v2.values().cloned().collect::<Vec<Value>>());
    ok(json!({"AggregatorsV2": aggs}))
}

fn create_connector_v2(svc: &Svc) -> Response {
    let connector_id = Uuid::new_v4().to_string();
    let arn = format!(
        "arn:aws:securityhub:{}:{}:connector-v2/{}",
        svc.region, svc.account, connector_id
    );
    let now = now_iso();
    let record = json!({
        "ConnectorArn": arn,
        "ConnectorId": connector_id,
        "Name": svc.body("Name"),
        "Description": svc.body("Description"),
        "ProviderDetail": svc.body("Provider"),
        "KmsKeyArn": svc.body("KmsKeyArn"),
        "CreatedAt": now,
        "LastUpdatedAt": now,
        "ConnectorStatus": "CONNECTED",
        "Health": {"ConnectorStatus": "CONNECTED", "Message": ""},
    });
    svc.with(|st| {
        st.connectors_v2
            .insert(connector_id.clone(), record.clone());
    });
    ok(json!({
        "ConnectorArn": arn,
        "ConnectorId": connector_id,
        "AuthUrl": "",
        "ConnectorStatus": "CONNECTED",
    }))
}

fn find_connector_v2(st: &State, id: &str) -> Option<String> {
    if st.connectors_v2.contains_key(id) {
        return Some(id.to_string());
    }
    st.connectors_v2
        .iter()
        .find(|(_, c)| c.get("ConnectorArn").and_then(Value::as_str) == Some(id))
        .map(|(cid, _)| cid.clone())
}

fn get_connector_v2(svc: &Svc, id: &str) -> Response {
    let found = svc.with(|st| find_connector_v2(st, id));
    match found {
        Some(cid) => {
            let connector = svc.with(|st| st.connectors_v2.get(&cid).cloned());
            match connector {
                Some(r) => ok(r),
                None => not_found("connector", id),
            }
        }
        None => not_found("connector", id),
    }
}

fn update_connector_v2(svc: &Svc, id: &str) -> Response {
    let description = svc.body("Description");
    let provider = svc.body("Provider");
    let found = svc.with(|st| {
        let cid = find_connector_v2(st, id);
        match cid {
            Some(cid) => {
                if let Some(connector) = st.connectors_v2.get_mut(&cid) {
                    if !description.is_null() {
                        connector["Description"] = description.clone();
                    }
                    if provider.is_object() {
                        connector["ProviderDetail"] = provider.clone();
                    }
                    connector["LastUpdatedAt"] = json!(now_iso());
                }
                true
            }
            None => false,
        }
    });
    if found {
        ok(json!({}))
    } else {
        not_found("connector", id)
    }
}

fn delete_connector_v2(svc: &Svc, id: &str) -> Response {
    let found = svc.with(|st| {
        let cid = find_connector_v2(st, id);
        match cid {
            Some(cid) => st.connectors_v2.remove(&cid).is_some(),
            None => false,
        }
    });
    if found {
        ok(json!({}))
    } else {
        not_found("connector", id)
    }
}

fn list_connectors_v2(svc: &Svc) -> Response {
    let connectors: Vec<Value> = svc
        .with(|st| st.connectors_v2.values().cloned().collect::<Vec<Value>>())
        .into_iter()
        .map(|connector| {
            json!({
                "ConnectorArn": connector.get("ConnectorArn"),
                "ConnectorId": connector.get("ConnectorId"),
                "Name": connector.get("Name"),
                "Description": connector.get("Description"),
                "ConnectorStatus": connector.get("ConnectorStatus"),
                "CreatedAt": connector.get("CreatedAt"),
            })
        })
        .collect();
    ok(json!({"Connectors": connectors}))
}

fn register_connector_v2(svc: &Svc) -> Response {
    let connector_id = Uuid::new_v4().to_string();
    ok(json!({
        "ConnectorArn": format!(
            "arn:aws:securityhub:{}:{}:connector-v2/{}",
            svc.region, svc.account, connector_id
        ),
        "ConnectorId": connector_id,
    }))
}

fn create_ticket_v2(_svc: &Svc) -> Response {
    let ticket_id = Uuid::new_v4().to_string();
    ok(json!({
        "TicketId": ticket_id,
        "TicketSrcUrl": format!("https://ticket.example.com/{ticket_id}"),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn req(method: &str, path: &str, body: &str) -> Request {
        Request {
            service: "securityhub".to_string(),
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

    fn body_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    fn unique_scope() -> ScopeKey {
        // unique (account, region) per test for state isolation
        (
            fastrand_id(),
            format!("us-east-1-{}", Uuid::new_v4().simple()),
        )
    }

    fn fastrand_id() -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        Uuid::new_v4().simple().to_string().hash(&mut h);
        h.finish() % 9_000_000_000_000 + 100_000_000_000
    }

    fn dispatch_scoped(method: &str, path: &str, body: &str, key: ScopeKey) -> Response {
        let mut r = req(method, path, body);
        r.account = key.0;
        r.region = key.1.clone();
        dispatch(&r)
    }

    #[test]
    fn hub_enable_describe_disable() {
        let key = unique_scope();
        dispatch_scoped("POST", "/accounts", "{}", key.clone());
        let describe = body_of(&dispatch_scoped("GET", "/accounts", "{}", key.clone()));
        assert!(describe["HubArn"].as_str().unwrap().contains("hub/default"));
        assert!(describe["SubscribedAt"].as_str().is_some());
        assert_eq!(describe["ControlFindingGenerator"], "SECURITY_CONTROL");
        let disable = dispatch_scoped("DELETE", "/accounts", "{}", key.clone());
        assert_eq!(disable.status, 200);
    }

    #[test]
    fn describe_hub_when_disabled_is_access_denied() {
        let key = unique_scope();
        let resp = dispatch_scoped("GET", "/accounts", "{}", key);
        assert_eq!(resp.status, 400);
        let body = body_of(&resp);
        assert_eq!(body["__type"], "InvalidAccessException");
    }

    #[test]
    fn import_and_filter_findings() {
        let key = unique_scope();
        let finding = json!({
            "Id": "f-1",
            "Severity": {"Label": "CRITICAL"},
            "Title": "Critical Test",
            "Resources": [{"Type": "AwsEc2Instance"}],
        });
        let resp = dispatch_scoped(
            "POST",
            "/findings/import",
            &json!({"Findings": [finding]}).to_string(),
            key.clone(),
        );
        let body = body_of(&resp);
        assert_eq!(body["SuccessCount"], 1);
        assert_eq!(body["FailedCount"], 0);
        assert_eq!(body["FailedFindings"], json!([]));

        let filter =
            json!({"Filters": {"SeverityLabel": [{"Value": "CRITICAL", "Comparison": "EQUALS"}]}});
        let resp = dispatch_scoped("POST", "/findings", &filter.to_string(), key.clone());
        let body = body_of(&resp);
        let titles: Vec<&str> = body
            .get("Findings")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|f| f.get("Title").and_then(Value::as_str))
                    .collect()
            })
            .unwrap_or_default();
        assert!(titles.contains(&"Critical Test"));

        // MaxResults caps the page
        let resp = dispatch_scoped("POST", "/findings", r#"{"MaxResults": 1}"#, key.clone());
        assert_eq!(body_of(&resp)["Findings"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn import_requires_resources() {
        let key = unique_scope();
        let finding = json!({"Id": "f-2", "Title": "no resources", "Resources": []});
        let resp = dispatch_scoped(
            "POST",
            "/findings/import",
            &json!({"Findings": [finding]}).to_string(),
            key.clone(),
        );
        let body = body_of(&resp);
        assert_eq!(body["FailedCount"], 1);
        assert_eq!(body["FailedFindings"][0]["ErrorCode"], "InvalidInput");
    }

    #[test]
    fn batch_update_findings_processes_existing() {
        let key = unique_scope();
        let finding = json!({
            "Id": "f-3",
            "Severity": {"Label": "MEDIUM"},
            "Resources": [{"Type": "AwsEc2Instance", "Id": "i-1"}],
        });
        dispatch_scoped(
            "POST",
            "/findings/import",
            &json!({"Findings": [finding]}).to_string(),
            key.clone(),
        );
        let resp = dispatch_scoped(
            "PATCH",
            "/findings/batchupdate",
            &json!({
                "FindingIdentifiers": [{"Id": "f-3", "ProductArn": "arn:prod"}],
                "Severity": {"Label": "CRITICAL"},
            })
            .to_string(),
            key.clone(),
        );
        let body = body_of(&resp);
        assert_eq!(body["ProcessedFindings"][0]["Id"], "f-3");
        assert_eq!(body["UnprocessedFindings"], json!([]));
    }

    #[test]
    fn members_create_get_list() {
        let key = unique_scope();
        let create = dispatch_scoped(
            "POST",
            "/members",
            r#"{"AccountDetails": [{"AccountId": "111122223333", "Email": "a@example.com"}]}"#,
            key.clone(),
        );
        assert_eq!(body_of(&create)["UnprocessedAccounts"], json!([]));

        let list = body_of(&dispatch_scoped("GET", "/members", "{}", key.clone()));
        let ids: Vec<&str> = list["Members"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|m| m.get("AccountId").and_then(Value::as_str))
            .collect();
        assert!(ids.contains(&"111122223333"));

        let get = body_of(&dispatch_scoped(
            "POST",
            "/members/get",
            r#"{"AccountIds": ["111122223333"]}"#,
            key.clone(),
        ));
        assert_eq!(get["Members"][0]["Email"], "a@example.com");

        let get_unknown = body_of(&dispatch_scoped(
            "POST",
            "/members/get",
            r#"{"AccountIds": ["999999999999"]}"#,
            key,
        ));
        assert_eq!(
            get_unknown["UnprocessedAccounts"][0]["AccountId"],
            "999999999999"
        );
    }

    #[test]
    fn action_target_lifecycle() {
        let key = unique_scope();
        let create = body_of(&dispatch_scoped(
            "POST",
            "/actionTargets",
            r#"{"Name": "n", "Description": "d", "Id": "t-1"}"#,
            key.clone(),
        ));
        let arn = create["ActionTargetArn"].as_str().unwrap().to_string();
        assert!(arn.contains("action/custom/t-1"));

        // botocore sends the ARN percent-encoded in one URI segment
        let encoded = arn.replace('/', "%2F");
        dispatch_scoped(
            "PATCH",
            &format!("/actionTargets/{encoded}"),
            r#"{"Name": "updated"}"#,
            key.clone(),
        );
        let describe = body_of(&dispatch_scoped(
            "POST",
            "/actionTargets/get",
            "{}",
            key.clone(),
        ));
        assert_eq!(describe["ActionTargets"][0]["Name"], "updated");

        let del = body_of(&dispatch_scoped(
            "DELETE",
            &format!("/actionTargets/{encoded}"),
            "{}",
            key,
        ));
        assert_eq!(del["ActionTargetArn"], json!(arn));
    }

    #[test]
    fn standards_enable_get_disable() {
        let key = unique_scope();
        let enable = body_of(&dispatch_scoped(
            "POST",
            "/standards/register",
            r#"{"StandardsSubscriptionRequests": [{"StandardsArn": "arn:aws:securityhub:::standards/aws-foundational-security-best-practices/v/1.0.0"}]}"#,
            key.clone(),
        ));
        let sub_arn = enable["StandardsSubscriptions"][0]["StandardsSubscriptionArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(!sub_arn.is_empty());

        let disable = body_of(&dispatch_scoped(
            "POST",
            "/standards/deregister",
            &json!({"StandardsSubscriptionArns": [sub_arn]}).to_string(),
            key,
        ));
        assert_eq!(
            disable["StandardsSubscriptions"][0]["StandardsStatus"],
            "INCOMPLETE"
        );
    }

    #[test]
    fn standards_controls_require_subscription() {
        let key = unique_scope();
        let resp = dispatch_scoped("GET", "/standards/controls/nonexistent", "{}", key);
        assert_eq!(body_of(&resp)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn automation_rule_create_and_get() {
        let key = unique_scope();
        let create = body_of(&dispatch_scoped(
            "POST",
            "/automationrules/create",
            r#"{"RuleName": "r1", "RuleOrder": 1, "RuleStatus": "ENABLED", "Criteria": {}, "Actions": []}"#,
            key.clone(),
        ));
        let arn = create["RuleArn"].as_str().unwrap().to_string();
        assert!(arn.contains("automation-rule"));

        let get = body_of(&dispatch_scoped(
            "POST",
            "/automationrules/get",
            &json!({"AutomationRulesArns": [arn.clone()]}).to_string(),
            key.clone(),
        ));
        assert_eq!(get["Rules"][0]["RuleName"], "r1");
        assert_eq!(get["UnprocessedAutomationRules"], json!([]));

        let delete = body_of(&dispatch_scoped(
            "POST",
            "/automationrules/delete",
            &json!({"AutomationRulesArns": [arn]}).to_string(),
            key,
        ));
        assert_eq!(
            delete["ProcessedAutomationRules"].as_array().unwrap().len(),
            1
        );
    }

    #[test]
    fn insight_lifecycle() {
        let key = unique_scope();
        let create = body_of(&dispatch_scoped(
            "POST",
            "/insights",
            r#"{"Name": "i-1", "Filters": {}, "GroupByAttribute": "SeverityLabel"}"#,
            key.clone(),
        ));
        let arn = create["InsightArn"].as_str().unwrap().to_string();

        let del = body_of(&dispatch_scoped(
            "DELETE",
            &format!("/insights/{arn}"),
            "{}",
            key.clone(),
        ));
        assert_eq!(del["InsightArn"], json!(arn));

        let resp = dispatch_scoped("DELETE", "/insights/arn:missing", "{}", key);
        assert_eq!(body_of(&resp)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn tags_roundtrip_on_hub() {
        let key = unique_scope();
        dispatch_scoped("POST", "/accounts", "{}", key.clone());
        let hub_arn = format!("arn:aws:securityhub:us-east-1:{}:hub/default", key.0);
        let hdr = format!("/tags/{hub_arn}");
        dispatch_scoped("POST", &hdr, r#"{"Tags": {"env": "test"}}"#, key.clone());
        let tags = body_of(&dispatch_scoped("GET", &hdr, "{}", key.clone()));
        assert_eq!(tags["Tags"]["env"], "test");

        // untag via query-string keys, as botocore does
        let mut r = req("DELETE", &hdr, "{}");
        r.account = key.0;
        r.region = key.1.clone();
        let key = key.clone();
        r.query_string = "tagKeys=env".to_string();
        let resp = dispatch(&r);
        assert_eq!(resp.status, 200);
        let tags = body_of(&dispatch_scoped("GET", &hdr, "{}", key));
        assert!(tags["Tags"].as_object().unwrap().is_empty());
    }

    #[test]
    fn org_ops_fail_without_organizations() {
        let key = unique_scope();
        let resp = dispatch_scoped("POST", "/organization/admin/enable", "{}", key.clone());
        assert_eq!(
            body_of(&resp)["__type"],
            "AWSOrganizationsNotInUseException"
        );
        let resp = dispatch_scoped("GET", "/organization/configuration", "{}", key);
        assert_eq!(body_of(&resp)["__type"], "AccessDeniedException");
    }

    #[test]
    fn v2_hub_lifecycle() {
        let key = unique_scope();
        let enable = body_of(&dispatch_scoped("POST", "/hubv2", "{}", key.clone()));
        assert!(enable["HubV2Arn"].as_str().is_some());
        let describe = body_of(&dispatch_scoped("GET", "/hubv2", "{}", key.clone()));
        assert_eq!(describe["HubV2Arn"], enable["HubV2Arn"]);
        dispatch_scoped("DELETE", "/hubv2", "{}", key.clone());
        let resp = dispatch_scoped("GET", "/hubv2", "{}", key);
        assert_eq!(body_of(&resp)["__type"], "InvalidAccessException");
    }

    #[test]
    fn products_listed() {
        let key = unique_scope();
        let products = body_of(&dispatch_scoped("GET", "/products", "{}", key.clone()));
        assert!(products["Products"].as_array().unwrap().len() >= 7);
        let v2 = body_of(&dispatch_scoped("GET", "/productsV2", "{}", key));
        assert!(v2["ProductsV2"].as_array().unwrap().len() >= 7);
    }

    #[test]
    fn unknown_op_returns_501() {
        let key = unique_scope();
        let r = req("PUT", "/unknown/operation", "{}");
        let resp = dispatch(&Request {
            account: key.0,
            region: key.1,
            ..r
        });
        assert_eq!(resp.status, 501);
    }
}
