//! AWS Config backend (JSON protocol, target `StarlingDoveService`).
//!
//! Port of moto's `config` covering the ops its compat suite exercises:
//! rules, recorders + statuses, delivery channels, compliance queries,
//! aggregators/authorizations, conformance packs, organization rules/packs,
//! stored queries, remediation configs/exceptions, retention, resource
//! config and the stub gap ops. Resource history reads the s3 crate's state
//! bridge for `AWS::S3::Bucket` (moto records moto-side S3 resources; here
//! the twin's bucket store plays that role).

use std::collections::HashMap;

use crate::proto::{json_error, parse_json_body};
use crate::store::{self, Slot};
use crate::{Request, Response};
use serde_json::{json, Value};

fn now_stamp() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S%:z")
        .to_string()
}

fn rand_lower(len: usize) -> String {
    const ALPHA: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
    let mut out = String::with_capacity(len);
    while out.len() < len {
        for b in uuid::Uuid::new_v4().as_bytes() {
            if out.len() == len {
                break;
            }
            out.push(ALPHA[(*b as usize) % ALPHA.len()] as char);
        }
    }
    out
}

fn json_err(status: u16, code: &str, message: String) -> Response {
    json_error(status, code, &message)
}

fn ok_json(value: Value) -> Response {
    Response::json_status(200, &value)
}

fn with_scope<T>(
    slot: &Slot<ConfigState>,
    req: &Request,
    f: impl FnOnce(&mut ConfigState) -> T,
) -> T {
    let mut map = slot.write();
    f(map.entry((req.account, req.region.clone())).or_default())
}

type RuleEntry = (String, Value, Vec<(String, String)>);

#[derive(Default)]
pub struct ConfigState {
    /// (name, stored rule echo, tags)
    config_rules: Vec<RuleEntry>,
    recorders: Vec<(String, Value)>,
    recorder_recording: Vec<String>,
    recorder_started: Vec<String>,
    /// Timestamps parallel to recorder_started.
    recorder_started_ts: Vec<String>,
    /// "name<US>stamp" entries for stopped recorders.
    recorder_stopped: Vec<String>,
    delivery_channels: Vec<(String, Value)>,
    aggregators: Vec<(String, Value)>,
    aggregation_authorizations: Vec<(String, String)>,
    conformance_packs: Vec<(String, Value)>,
    org_conformance_packs: Vec<(String, Value)>,
    org_config_rules: Vec<(String, Value)>,
    stored_queries: Vec<(String, Value)>,
    remediation_configurations: Vec<(String, Value)>,
    /// rule name → list of {ResourceType, ResourceId}
    remediation_exceptions: HashMap<String, Vec<Value>>,
    retention: Option<Value>,
    custom_resources: Vec<(String, String, String)>,
    tags: HashMap<String, Vec<(String, String)>>,
}

/// Rule narrowed from the stored echo: names snapshot for lookups.
fn rule_field(rule: &Value, key: &str) -> String {
    rule.get(key)
        .and_then(|k| k.as_str())
        .unwrap_or_default()
        .to_string()
}

/// ConfigRule description mirrored moto's ConfigEmptyDictable output (noted
/// against the moto-bridge in this lane's probes).
fn rule_to_dict(name: &str, rule: &Value, account: u64, region: &str) -> Value {
    let rule_id = rule_field(rule, "__ruleId");
    let mut out = json!({
        "ConfigRuleName": name,
        "ConfigRuleArn": rule.get("__ruleArn"),
        "ConfigRuleId": rule_id,
        "Source": rule.get("Source"),
        "ConfigRuleState": rule.get("ConfigRuleState").unwrap_or(&json!("ACTIVE")),
    });
    if !rule_id.is_empty() {
        out["ConfigRuleArn"] = json!(format!(
            "arn:aws:config:{region}:{account}:config-rule/{rule_id}"
        ));
    }
    if let Some(d) = rule.get("Description") {
        if !d.is_null() && !d.as_str().unwrap_or_default().is_empty() {
            out["Description"] = d.clone();
        }
    }
    if let Some(s) = rule.get("Scope") {
        out["Scope"] = s.clone();
    }
    if let Some(ip) = rule.get("InputParameters") {
        out["InputParameters"] = ip.clone();
    }
    if let Some(f) = rule.get("MaximumExecutionFrequency") {
        out["MaximumExecutionFrequency"] = f.clone();
    }
    out
}

fn recorder_view(rec: &Value, region: &str, account: u64) -> Value {
    let mut out = json!({
        "name": rec.get("name").cloned().unwrap_or(serde_json::json!("default")),
        "roleARN": rec.get("roleARN").cloned().unwrap_or(serde_json::json!("")),
        "recordingGroup": rec.get("recordingGroup").cloned().unwrap_or(serde_json::json!({})),
    });
    // moto fills the default recordingGroup members.
    let rg_defaults = json!({
        "allSupported": false,
        "includeGlobalResourceTypes": false,
        "resourceTypes": [],
        "exclusionByResourceTypes": {"resourceTypes": []},
        "recordingStrategy": {"useOnly": "ALL_SUPPORTED_RESOURCE_TYPES"},
        "recordingFrequency": "CONTINUOUS",
    });
    let group = out.get_mut("recordingGroup").unwrap();
    if let (Some(mine), Some(base)) = (group.as_object_mut(), rg_defaults.as_object()) {
        for (k, v) in base {
            mine.entry(k.clone()).or_insert(v.clone());
        }
        let _ = region;
        let _ = account;
    }
    out
}

fn recorder_status_view(
    name: &str,
    recording: bool,
    started: Option<&String>,
    stopped: Option<&String>,
) -> Value {
    let mut out = json!({ "name": name, "recording": recording, "lastStatus": "PENDING" });
    if let Some(started) = started {
        out["lastStartTime"] = Value::String(started.clone());
        out["lastStatusChangeTime"] = Value::String(started.clone());
    }
    if let Some(stopped) = stopped {
        out["lastStopTime"] = Value::String(stopped.clone());
        out["lastStatusChangeTime"] = Value::String(stopped.clone());
    }
    out
}

fn err_no_such_rule(name: &str) -> Response {
    json_err(
        400,
        "NoSuchConfigRuleException",
        format!("Config rule {name} does not exist."),
    )
}

pub fn dispatch(req: &Request) -> Response {
    let body = parse_json_body(&req.body);
    handle(req, &body).unwrap_or_else(|e| e)
}

fn handle(req: &Request, body: &Value) -> Result<Response, Response> {
    let slot = store::slot::<ConfigState>("config");
    let region = req.region.clone();
    let account = req.account;

    match req.operation.as_str() {
        // --- Config rules ---------------------------------------------------
        "PutConfigRule" => {
            let rule = body.get("ConfigRule").cloned().ok_or_else(|| {
                json_err(
                    400,
                    "InvalidParameterValueException",
                    "Missing ConfigRule".to_string(),
                )
            })?;
            let name = rule_field(&rule, "ConfigRuleName");
            if name.is_empty() {
                return Err(json_err(
                    400,
                    "InvalidParameterValueException",
                    "One or more identifiers needs to be required. Provide name or ID or ARN"
                        .to_string(),
                ));
            }
            let tags = tag_pairs(body, "Tags");
            let tags_arn = format!("arn:aws:config:{region}:{account}:config-rule/{}", "TBD");
            let _ = tags_arn;
            let owner = rule["Source"]["Owner"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            if owner != "AWS" && owner != "CUSTOM_LAMBDA" {
                return Err(json_err(
                    400,
                    "ValidationException",
                    format!("Value '{owner}' at 'configRule.source.owner' failed to satisfy constraint: Member must satisfy enum value set: {{AWS, CUSTOM_LAMBDA}}"),
                ));
            }
            with_scope(&slot, req, |st| {
                let id = format!("config-rule-{}", rand_lower(8));
                let mut stored = rule.clone();
                stored["__ruleId"] = json!(id);
                let arn = format!("arn:aws:config:{region}:{account}:config-rule/{id}");
                stored["__ruleArn"] = json!(arn);
                if let Some(pos) = st.config_rules.iter_mut().position(|(n, ..)| *n == name) {
                    stored["__ruleId"] = st.config_rules[pos].1["__ruleId"].clone();
                    stored["__ruleArn"] = st.config_rules[pos].1["__ruleArn"].clone();
                    st.config_rules[pos].1 = stored;
                    if !tags.is_empty() {
                        st.config_rules[pos].2 = tags;
                    }
                } else {
                    st.config_rules.push((name, stored, tags));
                }
            });
            Ok(ok_json(json!({})))
        }
        "DescribeConfigRules" => {
            let names: Vec<String> = str_list(body, "ConfigRuleNames");
            let found = with_scope(&slot, req, |st| {
                let mut all: Vec<Value> = Vec::new();
                if !names.is_empty() {
                    for n in &names {
                        let missing = !st.config_rules.iter().any(|(rn, _, _)| rn == n);
                        if missing {
                            return Err(json_err(
                                400,
                                "NoSuchConfigRuleException",
                                format!("Config rule {n} does not exist"),
                            ));
                        }
                    }
                }
                let mut keys: Vec<String> =
                    st.config_rules.iter().map(|(n, _, _)| n.clone()).collect();
                keys.sort();
                for key in keys {
                    if !names.is_empty() && !names.contains(&key) {
                        continue;
                    }
                    if let Some((_, rule, _)) = st.config_rules.iter().find(|(n, _, _)| *n == key) {
                        all.push(rule_to_dict(&key, rule, account, &region));
                    }
                }
                Ok(all)
            })?;
            Ok(ok_json(json!({ "ConfigRules": found })))
        }
        "DeleteConfigRule" => {
            let name = body_field(body, "ConfigRuleName");
            let removed = with_scope(&slot, req, |st| {
                st.config_rules
                    .iter()
                    .position(|(n, _, _)| *n == name)
                    .map(|i| {
                        st.config_rules.remove(i);
                        true
                    })
                    .unwrap_or(false)
            });
            if removed {
                Ok(ok_json(json!({})))
            } else {
                Err(err_no_such_rule(&name))
            }
        }
        "DeleteEvaluationResults" => {
            let name = body_field(body, "ConfigRuleName");
            let exists = with_scope(&slot, req, |st| {
                st.config_rules.iter().any(|(n, _, _)| *n == name)
            });
            if exists {
                Ok(ok_json(json!({})))
            } else {
                Err(err_no_such_rule(&name))
            }
        }
        "PutExternalEvaluation" => {
            let name = body_field(body, "ConfigRuleName");
            let exists = with_scope(&slot, req, |st| {
                st.config_rules.iter().any(|(n, _, _)| *n == name)
            });
            if exists {
                Ok(ok_json(json!({})))
            } else {
                Err(err_no_such_rule(&name))
            }
        }
        // --- Recorders and delivery channels ---------------------------------
        "PutConfigurationRecorder" => {
            let rec = body
                .get("ConfigurationRecorder")
                .cloned()
                .unwrap_or(json!({}));
            let name = rec
                .get("name")
                .and_then(|k| k.as_str())
                .unwrap_or_default()
                .to_string();
            with_scope(&slot, req, |st| {
                if let Some(pos) = st.recorders.iter().position(|(n, _)| *n == name) {
                    st.recorders[pos].1 = rec;
                } else {
                    st.recorders.push((name.clone(), rec));
                }
            });
            Ok(ok_json(json!({})))
        }
        "DescribeConfigurationRecorders" => {
            let names: Vec<String> = str_list(body, "ConfigurationRecorderNames");
            let mut list: Vec<Value> = with_scope(&slot, req, |st| {
                st.recorders
                    .iter()
                    .filter(|(n, _)| names.is_empty() || names.contains(n))
                    .map(|(_, r)| r.clone())
                    .collect()
            });
            list = list
                .into_iter()
                .map(|r| recorder_view(&r, &region, account))
                .collect();
            Ok(ok_json(json!({ "ConfigurationRecorders": list })))
        }
        "DeleteConfigurationRecorder" => {
            let name = body_field(body, "ConfigurationRecorderName");
            with_scope(&slot, req, |st| {
                st.recorders.retain(|(n, _)| *n != name);
                st.recorder_recording.retain(|n| *n != name);
                st.recorder_started.retain(|n| *n != name);
                st.recorder_stopped.retain(|n| *n != name);
            });
            Ok(ok_json(json!({})))
        }
        "StartConfigurationRecorder" => {
            let name = body_field(body, "ConfigurationRecorderName");
            let has_recorder = with_scope(&slot, req, |st| {
                st.recorders.iter().any(|(n, _)| *n == name)
            });
            if !has_recorder {
                return Err(json_err(
                    400,
                    "NoSuchConfigurationRecorderException",
                    format!("Configuration recorder {name} does not exist."),
                ));
            }
            let has_dc = with_scope(&slot, req, |st| !st.delivery_channels.is_empty());
            if !has_dc {
                return Err(json_err(
                    400,
                    "NoAvailableDeliveryChannelException",
                    "Delivery channel is not available to start configuration recorder."
                        .to_string(),
                ));
            }
            with_scope(&slot, req, |st| {
                let stamp = now_stamp();
                if !st.recorder_started.iter().any(|n| n == &name) {
                    st.recorder_started
                        .push((name.clone(), stamp.clone()).0.clone());
                    st.recorder_started_ts.push(stamp);
                }
                if !st.recorder_recording.iter().any(|n| n == &name) {
                    st.recorder_recording.push(name.clone());
                }
            });
            Ok(ok_json(json!({})))
        }
        "StopConfigurationRecorder" => {
            let name = body_field(body, "ConfigurationRecorderName");
            with_scope(&slot, req, |st| {
                let stamp = now_stamp();
                if let Some(pos) = st.recorder_started.iter().position(|n| n == &name) {
                    st.recorder_stopped.push(name.clone() + "\u{1}" + &stamp);
                    let _ = pos;
                }
                st.recorder_recording.retain(|n| *n != name);
            });
            Ok(ok_json(json!({})))
        }
        "DescribeConfigurationRecorderStatus" => {
            let names: Vec<String> = str_list(body, "ConfigurationRecorderNames");
            let statuses: Vec<Value> = with_scope(&slot, req, |st| {
                st.recorders
                    .iter()
                    .filter(|(n, _)| names.is_empty() || names.contains(n))
                    .map(|(n, _)| {
                        let recording = st.recorder_recording.iter().any(|r| r == n);
                        let started = st
                            .recorder_started_ts
                            .get(
                                st.recorder_started
                                    .iter()
                                    .position(|s| s == n)
                                    .unwrap_or(usize::MAX),
                            )
                            .cloned();
                        let stopped = st
                            .recorder_stopped
                            .iter()
                            .find(|s| s.starts_with(n))
                            .map(|s| s.split('\u{1}').nth(1).unwrap_or_default().to_string());
                        recorder_status_view(n, recording, started.as_ref(), stopped.as_ref())
                    })
                    .collect()
            });
            Ok(ok_json(json!({ "ConfigurationRecordersStatus": statuses })))
        }
        _ => handle_2(req, body, slot),
    }
}

/// Rest of the op table (delivery channels, compliance, aggregation, packs,
/// queries, remediation, retention, resource config, stub ops).
fn handle_2(req: &Request, body: &Value, slot: Slot<ConfigState>) -> Result<Response, Response> {
    let region = req.region.clone();
    let account = req.account;

    match req.operation.as_str() {
        "PutDeliveryChannel" => {
            let dc = body.get("DeliveryChannel").cloned().unwrap_or(json!({}));
            let name = dc
                .get("name")
                .and_then(|k| k.as_str())
                .unwrap_or_default()
                .to_string();
            with_scope(&slot, req, |st| {
                if let Some(pos) = st.delivery_channels.iter().position(|(n, _)| *n == name) {
                    st.delivery_channels[pos].1 = dc;
                } else {
                    st.delivery_channels.push((name, dc));
                }
            });
            Ok(ok_json(json!({})))
        }
        "DescribeDeliveryChannels" => {
            let names: Vec<String> = str_list(body, "DeliveryChannelNames");
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.delivery_channels
                    .iter()
                    .filter(|(n, _)| names.is_empty() || names.contains(n))
                    .map(|(_, d)| d.clone())
                    .collect()
            });
            Ok(ok_json(json!({ "DeliveryChannels": list })))
        }
        "DeleteDeliveryChannel" => {
            let name = body_field(body, "DeliveryChannelName");
            with_scope(&slot, req, |st| {
                st.delivery_channels.retain(|(n, _)| *n != name)
            });
            Ok(ok_json(json!({})))
        }
        "DescribeDeliveryChannelStatus" => Ok(ok_json(json!({ "DeliveryChannelsStatus": [] }))),
        "DeliverConfigSnapshot" => Ok(ok_json(
            json!({ "configSnapshotId": format!("s-{}", rand_lower(16)) }),
        )),
        // --- Compliance -----------------------------------------------
        "PutEvaluations" => Ok(ok_json(json!({ "FailedEvaluations": [] }))),
        "DescribeComplianceByConfigRule" => Ok(ok_json(json!({ "ComplianceByConfigRules": [] }))),
        "DescribeConfigRuleEvaluationStatus" => {
            Ok(ok_json(json!({ "ConfigRulesEvaluationStatus": [] })))
        }
        "GetComplianceDetailsByConfigRule" => Ok(ok_json(json!({ "EvaluationResults": [] }))),
        "GetComplianceDetailsByResource" => Ok(ok_json(json!({ "EvaluationResults": [] }))),
        "DescribeComplianceByResource" => Ok(ok_json(json!({ "ComplianceByResources": [] }))),
        "GetComplianceSummaryByConfigRule" => Ok(ok_json(
            json!({ "ComplianceSummary": { "CompliantResourceCount": { "CappedCount": false, "CapExceeded": false }, "NonCompliantResourceCount": { "CappedCount": false, "CapExceeded": false }, "CompliantRuleCount": 0, "NonCompliantRuleCount": 0 } }),
        )),
        "GetComplianceSummaryByResourceType" => {
            Ok(ok_json(json!({ "ComplianceSummariesByResourceType": [] })))
        }
        // --- Aggregation ---------------------------------------------------
        "PutAggregationAuthorization" => {
            let auth_account = body_field(body, "AuthorizedAccountId");
            let auth_region = body_field(body, "AuthorizedAwsRegion");
            let arn = format!(
                "arn:aws:config:{region}:{account}:aggregation-authorization/{auth_account}/{auth_region}"
            );
            with_scope(&slot, req, |st| {
                if !st
                    .aggregation_authorizations
                    .iter()
                    .any(|(a2, r2)| *a2 == auth_account && *r2 == auth_region)
                {
                    st.aggregation_authorizations
                        .push((auth_account.clone(), auth_region.clone()));
                }
            });
            Ok(ok_json(json!({
                "AggregationAuthorization": {
                    "AggregationAuthorizationArn": arn,
                    "AuthorizedAccountId": auth_account,
                    "AuthorizedAwsRegion": auth_region,
                    "CreationTime": now_stamp(),
                }
            })))
        }
        "DescribeAggregationAuthorizations" => {
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.aggregation_authorizations
                    .iter()
                    .map(|(a, r)| {
                        json!({
                            "AggregationAuthorizationArn":
                                format!("arn:aws:config:{region}:{account}:aggregation-authorization/{a}/{r}"),
                            "AuthorizedAccountId": a,
                            "AuthorizedAwsRegion": r,
                            "CreationTime": now_stamp(),
                        })
                    })
                    .collect()
            });
            Ok(ok_json(json!({ "AggregationAuthorizations": list })))
        }
        "DeleteAggregationAuthorization" => {
            let a = body_field(body, "AuthorizedAccountId");
            let r = body_field(body, "AuthorizedAwsRegion");
            with_scope(&slot, req, |st| {
                st.aggregation_authorizations
                    .retain(|(a2, r2)| !(*a2 == a && *r2 == r))
            });
            Ok(ok_json(json!({})))
        }
        "PutConfigurationAggregator" => {
            let name = body_field(body, "ConfigurationAggregatorName");
            let sources = body
                .get("AccountAggregationSources")
                .cloned()
                .unwrap_or(json!([]));
            let id = format!("config-aggregator-{}", rand_lower(8));
            let arn = format!("arn:aws:config:{region}:{account}:config-aggregator/{id}");
            let stamp = now_stamp();
            let aggregator = json!({
                "ConfigurationAggregatorName": name,
                "ConfigurationAggregatorArn": arn,
                "AccountAggregationSources": sources,
                "CreationTime": stamp,
                "LastUpdatedTime": stamp,
            });
            with_scope(&slot, req, |st| {
                if let Some(pos) = st.aggregators.iter().position(|(n, _)| *n == name) {
                    st.aggregators[pos].1 = aggregator.clone();
                } else {
                    st.aggregators.push((name, aggregator.clone()));
                }
            });
            Ok(ok_json(json!({ "ConfigurationAggregator": aggregator })))
        }
        "DescribeConfigurationAggregators" => {
            let names: Vec<String> = str_list(body, "ConfigurationAggregatorNames");
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                if !names.is_empty() {
                    for n in &names {
                        if !st.aggregators.iter().any(|(a, _)| a == n) {
                            return Err(json_err(
                                400,
                                "NoSuchConfigurationAggregatorException",
                                format!(
                                    "The configuration aggregator with name {n} does not exist."
                                ),
                            ));
                        }
                    }
                }
                let list: Vec<Value> = st
                    .aggregators
                    .iter()
                    .filter(|(n, _)| names.is_empty() || names.contains(n))
                    .map(|(_, a)| a.clone())
                    .collect();
                Ok(list)
            })?;
            Ok(ok_json(json!({ "ConfigurationAggregators": list })))
        }
        "DeleteConfigurationAggregator" => {
            let name = body_field(body, "ConfigurationAggregatorName");
            with_scope(&slot, req, |st| st.aggregators.retain(|(n, _)| *n != name));
            Ok(ok_json(json!({})))
        }
        "DescribeConfigurationAggregatorSourcesStatus" => {
            let name = body_field(body, "ConfigurationAggregatorName");
            let exists = with_scope(&slot, req, |st| {
                st.aggregators.iter().any(|(n, _)| *n == name)
            });
            if !exists {
                return Err(json_err(
                    400,
                    "NoSuchConfigurationAggregatorException",
                    format!("The configuration aggregator with name {name} does not exist."),
                ));
            }
            Ok(ok_json(json!({ "AggregationSourceStatusList": [] })))
        }
        _ => handle_3(req, body, slot),
    }
}

/// Third segment: conformance packs, organization resources, stored queries,
/// remediation, retention, resource config + stubs.
fn handle_3(req: &Request, body: &Value, slot: Slot<ConfigState>) -> Result<Response, Response> {
    let region = req.region.clone();
    let account = req.account;

    match req.operation.as_str() {
        // --- Conformance packs ------------------------------------------------
        "PutConformancePack" => {
            let name = body_field(body, "ConformancePackName");
            let arn = format!(
                "arn:aws:config:{region}:{account}:conformance-pack/{name}/{}",
                rand_lower(8)
            );
            let pack = json!({
                "ConformancePackName": name,
                "ConformancePackArn": arn.clone(),
                "ConformancePackId": rand_lower(8),
                "DeliveryS3Bucket": body_field(body, "DeliveryS3Bucket"),
                "DeliveryS3KeyPrefix": body_field(body, "DeliveryS3KeyPrefix"),
                "ConformancePackInputParameters": body.get("ConformancePackInputParameters").cloned().unwrap_or(json!([])),
                "LastUpdateRequestedTime": now_stamp(),
            });
            with_scope(&slot, req, |st| {
                if let Some(pos) = st.conformance_packs.iter().position(|(n, _)| *n == name) {
                    st.conformance_packs[pos].1 = pack.clone();
                } else {
                    st.conformance_packs.push((name, pack));
                }
            });
            Ok(ok_json(json!({ "ConformancePackArn": arn })))
        }
        "DescribeConformancePacks" => {
            let names: Vec<String> = str_list(body, "ConformancePackNames");
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                if !names.is_empty() {
                    for n in &names {
                        if !st.conformance_packs.iter().any(|(p, _)| p == n) {
                            return Err(json_err(
                                400,
                                "NoSuchConformancePackException",
                                format!("Conformance pack {n} does not exist."),
                            ));
                        }
                    }
                }
                let list: Vec<Value> = st
                    .conformance_packs
                    .iter()
                    .filter(|(p, _)| names.is_empty() || names.contains(p))
                    .map(|(_, d)| d.clone())
                    .collect();
                Ok(list)
            })?;
            Ok(ok_json(json!({ "ConformancePackDetails": list })))
        }
        "DeleteConformancePack" => {
            let name = body_field(body, "ConformancePackName");
            let exists = with_scope(&slot, req, |st| {
                st.conformance_packs.iter().any(|(p, _)| *p == name)
            });
            if !exists {
                return Err(json_err(
                    400,
                    "NoSuchConformancePackException",
                    format!("Conformance pack {name} does not exist"),
                ));
            }
            with_scope(&slot, req, |st| {
                st.conformance_packs.retain(|(p, _)| *p != name)
            });
            Ok(ok_json(json!({})))
        }
        "DescribeConformancePackStatus" => {
            Ok(ok_json(json!({ "ConformancePackStatusDetails": [] })))
        }
        "DescribeConformancePackCompliance" => {
            let name = body_field(body, "ConformancePackName");
            let exists = with_scope(&slot, req, |st| {
                st.conformance_packs.iter().any(|(p, _)| *p == name)
            });
            if !exists {
                return Err(json_err(
                    400,
                    "NoSuchConformancePackException",
                    format!("Conformance pack {name} does not exist"),
                ));
            }
            Ok(ok_json(json!({ "ConformancePackRules": [] })))
        }
        "GetConformancePackComplianceDetails" => {
            let name = body_field(body, "ConformancePackName");
            let exists = with_scope(&slot, req, |st| {
                st.conformance_packs.iter().any(|(p, _)| *p == name)
            });
            if !exists {
                return Err(json_err(
                    400,
                    "NoSuchConformancePackException",
                    format!("Conformance pack {name} does not exist"),
                ));
            }
            Ok(ok_json(json!({ "EvaluationResults": [] })))
        }
        "GetConformancePackComplianceSummary" => {
            let names: Vec<String> = str_list(body, "ConformancePackNames");
            let missing = with_scope(&slot, req, |st| {
                names
                    .iter()
                    .any(|n| !st.conformance_packs.iter().any(|(p, _)| p == n))
            });
            if missing {
                return Err(json_err(
                    400,
                    "NoSuchConformancePackException",
                    format!("Conformance pack {:?} does not exist", names.first()),
                ));
            }
            Ok(ok_json(json!({ "ConformancePackComplianceSummaries": [] })))
        }
        "ListConformancePackComplianceScores" => {
            Ok(ok_json(json!({ "ConformancePackComplianceScores": [] })))
        }
        // --- Organization conformance packs -----------------------------------
        "PutOrganizationConformancePack" => {
            let name = body_field(body, "OrganizationConformancePackName");
            let arn = format!(
                "arn:aws:config:{region}:{account}:organization-conformance-pack/{name}-{}",
                rand_lower(8)
            );
            let pack = json!({
                "OrganizationConformancePackName": name.clone(),
                "OrganizationConformancePackArn": arn.clone(),
                "ConformancePackInputParameters": body.get("ConformancePackInputParameters").cloned().unwrap_or(json!([])),
                "ExcludedAccounts": body.get("ExcludedAccounts").cloned().unwrap_or(json!([])),
                "LastUpdateTime": now_stamp(),
            });
            with_scope(&slot, req, |st| st.org_conformance_packs.push((name, pack)));
            Ok(ok_json(json!({ "OrganizationConformancePackArn": arn })))
        }
        "DescribeOrganizationConformancePacks" => {
            let names: Vec<String> = str_list(body, "OrganizationConformancePackNames");
            let missing = with_scope(&slot, req, |st| {
                names
                    .iter()
                    .any(|n| !st.org_conformance_packs.iter().any(|(p, _)| p == n))
            });
            if missing {
                return Err(json_err(
                    400,
                    "NoSuchOrganizationConformancePackException",
                    "One or more organization conformance packs does not exist".to_string(),
                ));
            }
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.org_conformance_packs
                    .iter()
                    .filter(|(p, _)| names.is_empty() || names.contains(p))
                    .map(|(_, d)| d.clone())
                    .collect()
            });
            Ok(ok_json(json!({ "OrganizationConformancePacks": list })))
        }
        "DescribeOrganizationConformancePackStatuses" => Ok(ok_json(
            json!({ "OrganizationConformancePackStatuses": [] }),
        )),
        "GetOrganizationConformancePackDetailedStatus" => Err(json_err(
            400,
            "NoSuchOrganizationConformancePackException",
            format!(
                "Organization conformance pack {} does not exist",
                body_field(body, "OrganizationConformancePackName")
            ),
        )),
        "DeleteOrganizationConformancePack" => {
            let name = body_field(body, "OrganizationConformancePackName");
            let removed = with_scope(&slot, req, |st| {
                st.org_conformance_packs
                    .iter()
                    .position(|(p, _)| *p == name)
                    .map(|i| {
                        st.org_conformance_packs.remove(i);
                        true
                    })
                    .unwrap_or(false)
            });
            if !removed {
                return Err(json_err(
                    400,
                    "NoSuchOrganizationConformancePackException",
                    format!("Organization conformance pack {name} does not exist"),
                ));
            }
            Ok(ok_json(json!({})))
        }
        // --- Organization config rules --------------------------------------------
        "PutOrganizationConfigRule" => {
            let name = body_field(body, "OrganizationConfigRuleName");
            let arn = format!(
                "arn:aws:config:{region}:{account}:organization-config-rule/o-{}",
                rand_lower(8)
            );
            let rule = json!({
                "OrganizationConfigRuleName": name.clone(),
                "OrganizationConfigRuleArn": arn.clone(),
                "OrganizationManagedRuleMetadata": body.get("OrganizationManagedRuleMetadata").cloned().unwrap_or(json!({})),
                "OrganizationCustomRuleMetadata": body.get("OrganizationCustomRuleMetadata").cloned().unwrap_or(json!(null)),
                "ExcludedAccounts": body.get("ExcludedAccounts").cloned().unwrap_or(json!([])),
                "LastUpdateTime": now_stamp(),
            });
            with_scope(&slot, req, |st| {
                if let Some(pos) = st.org_config_rules.iter().position(|(n, _)| *n == name) {
                    st.org_config_rules[pos].1 = rule;
                } else {
                    st.org_config_rules.push((name, rule));
                }
            });
            Ok(ok_json(json!({ "OrganizationConfigRuleArn": arn })))
        }
        "DescribeOrganizationConfigRules" => {
            let names: Vec<String> = str_list(body, "OrganizationConfigRuleNames");
            let missing = with_scope(&slot, req, |st| {
                names
                    .iter()
                    .any(|n| !st.org_config_rules.iter().any(|(r, _)| r == n))
            });
            if missing {
                return Err(json_err(
                    400,
                    "NoSuchOrganizationConfigRuleException",
                    "One or more organization config rules does not exist".to_string(),
                ));
            }
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.org_config_rules
                    .iter()
                    .filter(|(r, _)| names.is_empty() || names.contains(r))
                    .map(|(_, d)| d.clone())
                    .collect()
            });
            Ok(ok_json(json!({ "OrganizationConfigRules": list })))
        }
        "DescribeOrganizationConfigRuleStatuses" => {
            let names: Vec<String> = str_list(body, "OrganizationConfigRuleNames");
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.org_config_rules
                    .iter()
                    .filter(|(r, _)| names.is_empty() || names.contains(r))
                    .map(|(r, _)| {
                        json!({
                            "OrganizationConfigRuleName": r,
                            "OrganizationRuleStatus": "CREATE_SUCCESSFUL",
                            "LastUpdateTime": now_stamp(),
                        })
                    })
                    .collect()
            });
            Ok(ok_json(json!({ "OrganizationConfigRuleStatuses": list })))
        }
        "GetOrganizationConfigRuleDetailedStatus" => Err(json_err(
            400,
            "NoSuchOrganizationConfigRuleException",
            format!(
                "Organization config rule {} does not exist",
                body_field(body, "OrganizationConfigRuleName")
            ),
        )),
        "GetOrganizationCustomRulePolicy" => Ok(ok_json(
            json!({ "PolicyText": r#"{"Version":"2012-10-17","Statement":[]}"# }),
        )),
        "DeleteOrganizationConfigRule" => {
            let name = body_field(body, "OrganizationConfigRuleName");
            let removed = with_scope(&slot, req, |st| {
                st.org_config_rules
                    .iter()
                    .position(|(r, _)| *r == name)
                    .map(|i| {
                        st.org_config_rules.remove(i);
                        true
                    })
                    .unwrap_or(false)
            });
            if !removed {
                return Err(json_err(
                    400,
                    "NoSuchOrganizationConfigRuleException",
                    format!("Organization config rule {name} does not exist"),
                ));
            }
            Ok(ok_json(json!({})))
        }
        // --- Stored queries --------------------------------------------------------
        "PutStoredQuery" => {
            let q = body.get("StoredQuery").cloned().unwrap_or(json!({}));
            let name = q
                .get("QueryName")
                .and_then(|k| k.as_str())
                .unwrap_or_default()
                .to_string();
            let id = rand_lower(8);
            let arn = format!("arn:aws:config:{region}:{account}:stored-query/{name}/{id}");
            let stored = json!({
                "QueryId": id,
                "QueryArn": arn.clone(),
                "QueryName": name.clone(),
                "Description": q.get("Description").cloned().unwrap_or(json!("")),
                "Expression": q.get("Expression").cloned().unwrap_or(json!("")),
            });
            let arn = f_arn(&arn);
            with_scope(&slot, req, |st| {
                if let Some(pos) = st.stored_queries.iter().position(|(n, _)| *n == name) {
                    st.stored_queries[pos].1 = stored.clone();
                } else {
                    st.stored_queries.push((name, stored.clone()));
                }
            });
            Ok(ok_json(json!({ "QueryArn": arn })))
        }
        "GetStoredQuery" => {
            let name = body_field(body, "QueryName");
            let stored = with_scope(&slot, req, |st| {
                st.stored_queries
                    .iter()
                    .find(|(n, _)| *n == name)
                    .map(|(_, q)| q.clone())
            })
            .ok_or_else(|| {
                json_err(
                    400,
                    "ResourceNotFoundException",
                    format!("Stored query {name} not found"),
                )
            })?;
            Ok(ok_json(json!({ "StoredQuery": stored })))
        }
        "ListStoredQueries" => {
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.stored_queries
                    .iter()
                    .map(|(_, q)| {
                        json!({
                            "QueryId": q.get("QueryId"),
                            "QueryArn": q.get("QueryArn"),
                            "QueryName": q.get("QueryName"),
                            "Description": q.get("Description"),
                        })
                    })
                    .collect()
            });
            Ok(ok_json(json!({ "StoredQueryMetadata": list })))
        }
        "DeleteStoredQuery" => {
            let name = body_field(body, "QueryName");
            with_scope(&slot, req, |st| {
                st.stored_queries.retain(|(n, _)| *n != name)
            });
            Ok(ok_json(json!({})))
        }
        // --- Remediation ---------------------------------------------------------------
        "PutRemediationConfigurations" => {
            let configs: Vec<Value> = body
                .get("RemediationConfigurations")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            with_scope(&slot, req, |st| {
                for c in configs {
                    let rule = c
                        .get("ConfigRuleName")
                        .and_then(|k| k.as_str())
                        .unwrap_or_default()
                        .to_string();
                    if let Some(pos) = st
                        .remediation_configurations
                        .iter()
                        .position(|(r, _)| *r == rule)
                    {
                        st.remediation_configurations[pos].1 = c;
                    } else {
                        st.remediation_configurations.push((rule, c));
                    }
                }
            });
            Ok(ok_json(json!({ "FailedBatches": [] })))
        }
        "DescribeRemediationConfigurations" => {
            let names: Vec<String> = str_list(body, "ConfigRuleNames");
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.remediation_configurations
                    .iter()
                    .filter(|(r, _)| names.contains(r))
                    .map(|(_, c)| c.clone())
                    .collect()
            });
            Ok(ok_json(json!({ "RemediationConfigurations": list })))
        }
        "DeleteRemediationConfiguration" => {
            let rule = body_field(body, "ConfigRuleName");
            with_scope(&slot, req, |st| {
                st.remediation_configurations.retain(|(r, _)| *r != rule)
            });
            Ok(ok_json(json!({})))
        }
        "PutRemediationExceptions" => {
            let rule = body_field(body, "ConfigRuleName");
            let keys = body.get("ResourceKeys").cloned().unwrap_or(json!([]));
            with_scope(&slot, req, |st| {
                let entry = st.remediation_exceptions.entry(rule.clone()).or_default();
                entry.retain(|k| {
                    !keys
                        .as_array()
                        .map(|arr| {
                            arr.iter().any(|new| {
                                new.get("ResourceId") == k.get("ResourceId")
                                    && new.get("ResourceType") == k.get("ResourceType")
                            })
                        })
                        .unwrap_or(false)
                });
                if let Some(arr) = keys.as_array() {
                    for k in arr {
                        entry.push(k.clone());
                    }
                }
            });
            Ok(ok_json(json!({ "FailedBatches": [] })))
        }
        "DescribeRemediationExceptions" => {
            let rule = body_field(body, "ConfigRuleName");
            let exceptions = with_scope(&slot, req, |st| {
                st.remediation_exceptions
                    .get(&rule)
                    .cloned()
                    .unwrap_or_default()
            });
            let list: Vec<Value> = exceptions
                .iter()
                .map(|k| {
                    json!({
                        "ConfigRuleName": rule,
                        "ResourceKey": k,
                        "Message": "",
                        "ExpirationTime": Value::Null,
                    })
                })
                .collect();
            Ok(ok_json(json!({ "RemediationExceptions": list })))
        }
        "DeleteRemediationExceptions" => {
            let rule = body_field(body, "ConfigRuleName");
            let keys: Vec<Value> = body
                .get("ResourceKeys")
                .and_then(|k| k.as_array())
                .cloned()
                .unwrap_or_default();
            with_scope(&slot, req, |st| {
                if let Some(entry) = st.remediation_exceptions.get_mut(&rule) {
                    entry.retain(|k| {
                        !keys.iter().any(|rm| {
                            k.get("ResourceId") == rm.get("ResourceId")
                                && k.get("ResourceType") == rm.get("ResourceType")
                        })
                    });
                }
            });
            Ok(ok_json(json!({ "FailedBatches": [] })))
        }
        "StartRemediationExecution" => {
            let rule = body_field(body, "ConfigRuleName");
            let exists = with_scope(&slot, req, |st| {
                st.remediation_configurations
                    .iter()
                    .any(|(r, _)| *r == rule)
            });
            if exists {
                Ok(ok_json(json!({ "FailedItems": [] })))
            } else {
                Err(json_err(
                    400,
                    "NoSuchRemediationConfigurationException",
                    format!("No remediation configuration exists for rule {rule}"),
                ))
            }
        }
        "DescribeRemediationExecutionStatus" => {
            Ok(ok_json(json!({ "RemediationExecutionStatuses": [] })))
        }
        _ => handle_4(req, body, slot),
    }
}

fn f_arn(arn: &str) -> String {
    arn.to_string()
}

/// Fourth slice: retention, resource config, aggregation reads, stubs.
fn handle_4(req: &Request, body: &Value, slot: Slot<ConfigState>) -> Result<Response, Response> {
    let region = req.region.clone();
    let account = req.account;

    match req.operation.as_str() {
        "PutRetentionConfiguration" => {
            let days = body
                .get("RetentionPeriodInDays")
                .and_then(|k| k.as_i64())
                .unwrap_or(30);
            let ret = json!({ "Name": "default", "RetentionPeriodInDays": days });
            with_scope(&slot, req, |st| st.retention = Some(ret.clone()));
            Ok(ok_json(json!({ "RetentionConfiguration": ret })))
        }
        "DescribeRetentionConfigurations" => {
            let list: Vec<Value> =
                with_scope(&slot, req, |st| st.retention.iter().cloned().collect());
            Ok(ok_json(json!({ "RetentionConfigurations": list })))
        }
        "DeleteRetentionConfiguration" => {
            let name = body_field(body, "RetentionConfigurationName");
            with_scope(&slot, req, |st| {
                if st
                    .retention
                    .as_ref()
                    .map(|r| r.get("Name").and_then(|k| k.as_str()).unwrap_or("") == name)
                    .unwrap_or(false)
                {
                    st.retention = None;
                }
            });
            Ok(ok_json(json!({})))
        }
        // --- Resource config ------------------------------------------------
        "PutResourceConfig" => {
            let rtype = body_field(body, "ResourceType");
            let rid = body_field(body, "ResourceId");
            let configuration = body_field(body, "Configuration");
            with_scope(&slot, req, |st| {
                if let Some(pos) = st
                    .custom_resources
                    .iter()
                    .position(|(t, i, _)| *t == rtype && *i == rid)
                {
                    st.custom_resources[pos].2 = configuration;
                } else {
                    st.custom_resources.push((rtype, rid, configuration));
                }
            });
            Ok(ok_json(json!({})))
        }
        "DeleteResourceConfig" => {
            let rtype = body_field(body, "ResourceType");
            let rid = body_field(body, "ResourceId");
            with_scope(&slot, req, |st| {
                st.custom_resources
                    .retain(|(t, i, _)| !(*t == rtype && *i == rid))
            });
            Ok(ok_json(json!({})))
        }
        "ListDiscoveredResources" => {
            let rtype = body_field(body, "resourceType");
            let items = with_scope(&slot, req, |st| {
                st.custom_resources
                    .iter()
                    .filter(|(t, _, _)| *t == rtype)
                    .map(|(_, i, _)| {
                        json!({
                            "resourceType": rtype,
                            "resourceId": i,
                        })
                    })
                    .collect::<Vec<_>>()
            });
            Ok(ok_json(json!({ "resourceIdentifiers": items })))
        }
        "GetResourceConfigHistory" => {
            let rtype = body_field(body, "resourceType");
            let rid = body_field(body, "resourceId");
            // Recorded custom resources and real S3 buckets have history.
            let mut exists = with_scope(&slot, req, |st| {
                st.custom_resources
                    .iter()
                    .any(|(t, i, _)| *t == rtype && *i == rid)
            });
            if !exists && rtype == "AWS::S3::Bucket" {
                exists = s3::bridge::bucket_exists(account, &region, &rid);
            }
            if exists {
                return Ok(ok_json(json!({ "configurationItems": [json!({
                    "resourceType": rtype,
                    "resourceId": rid,
                    "configurationStateId": now_stamp(),
                })] })));
            }
            Err(json_err(
                400,
                "ResourceNotDiscoveredException",
                format!(
                    "Resource {rid} of resourceType:{rtype} is unknown or has not been discovered"
                ),
            ))
        }
        "BatchGetResourceConfig" => Ok(ok_json(json!({
            "baseConfigurationItems": [],
            "unprocessedResourceKeys": [],
        }))),
        "SelectResourceConfig" => Ok(ok_json(json!({
            "Results": [],
            "QueryInfo": { "SelectClause": "", "Expression": body_field(body, "Expression"), "GroupByKeys": [] },
        }))),
        "StartResourceEvaluation" => Ok(ok_json(json!({
            "ResourceEvaluationId": format!("test-resourceevaluation-{}", rand_lower(12)),
        }))),
        "ListResourceEvaluations" => Ok(ok_json(json!({ "ResourceEvaluations": [] }))),
        "GetResourceEvaluationSummary" => Err(json_err(
            400,
            "ResourceNotFoundException",
            format!(
                "Resource evaluation {} not found",
                body_field(body, "ResourceEvaluationId")
            ),
        )),
        // --- Aggregator reads -------------------------------------------------
        "BatchGetAggregateResourceConfig" => Ok(ok_json(json!({
            "BaseConfigurationItems": [],
            "UnprocessedResourceIdentifiers": [],
        }))),
        "GetAggregateResourceConfig" => Ok(ok_json(json!({ "ConfigurationItem": {} }))),
        "ListAggregateDiscoveredResources" => Ok(ok_json(json!({ "ResourceIdentifiers": [] }))),
        "GetAggregateDiscoveredResourceCounts" => Ok(ok_json(json!({
            "TotalDiscoveredResources": 0,
            "resourceTypes": [],
        }))),
        "GetDiscoveredResourceCounts" => Ok(ok_json(json!({
            "totalDiscoveredResources": 0,
            "resourceTypes": [],
        }))),
        "DescribeAggregateComplianceByConfigRules" => {
            Ok(ok_json(json!({ "AggregateComplianceByConfigRules": [] })))
        }
        "DescribeAggregateComplianceByConformancePacks" => Ok(ok_json(
            json!({ "AggregateComplianceByConformancePacks": [] }),
        )),
        "GetAggregateConfigRuleComplianceSummary" => {
            Ok(ok_json(json!({ "AggregateComplianceCounts": [] })))
        }
        "GetAggregateConformancePackComplianceSummary" => Ok(ok_json(
            json!({ "AggregateConformancePackComplianceSummaries": [] }),
        )),
        "GetAggregateComplianceDetailsByConfigRule" => {
            Ok(ok_json(json!({ "AggregateEvaluationResults": [] })))
        }
        "SelectAggregateResourceConfig" => Ok(ok_json(json!({
            "Results": [],
            "QueryInfo": { "SelectClause": "", "Expression": body_field(body, "Expression"), "GroupByKeys": [] },
        }))),
        // --- Rule tags ---------------------------------------------------------------
        "TagResource" | "UntagResource" | "ListTagsForResource" => {
            let arn = body_field(body, "ResourceArn");
            if req.operation == "TagResource" {
                let tags = tag_pairs(body, "Tags");
                with_scope(&slot, req, |st| store_tags(st, &arn, &tags));
                Ok(ok_json(json!({})))
            } else if req.operation == "UntagResource" {
                let keys: Vec<String> = str_list(body, "TagKeys");
                with_scope(&slot, req, |st| {
                    if let Some(entry) = st.tags.get_mut(&arn) {
                        entry.retain(|(k, _)| !keys.contains(k));
                    }
                });
                Ok(ok_json(json!({})))
            } else {
                let tags = tags_view(&slot, req, &arn);
                Ok(ok_json(json!({ "Tags": tags })))
            }
        }
        // --- Misc gap stubs ------------------------------------------------------
        "ListConfigurationRecorders" => Ok(ok_json(json!({}))),
        "StartConfigRulesEvaluation" => Ok(ok_json(json!({}))),
        "GetCustomRulePolicy" => Ok(ok_json(json!({ "PolicyText": "{}" }))),
        "DeletePendingAggregationRequest" => Ok(ok_json(json!({}))),
        "PutServiceLinkedConfigurationRecorder" => Ok(ok_json(
            json!({ "Arn": format!("arn:aws:config:{region}:{account}:config-recorder/sl-{}", rand_lower(6)), "Name": format!("sl-{}", rand_lower(6)) }),
        )),
        "DeleteServiceLinkedConfigurationRecorder" => Ok(ok_json(
            json!({ "Arn": format!("arn:aws:config:{region}:{account}:config-recorder/sl-{}", rand_lower(6)), "Name": format!("sl-{}", rand_lower(6)) }),
        )),
        "AssociateResourceTypes" | "DisassociateResourceTypes" => {
            Ok(ok_json(json!({ "ConfigurationRecorder": {} })))
        }
        "DescribePendingAggregationRequests" => {
            Ok(ok_json(json!({ "PendingAggregationRequests": [] })))
        }
        _ => Err(json_err(
            400,
            "UnknownOperationException",
            format!("The {} action is not implemented", req.operation),
        )),
    }
}

fn str_list(v: &Value, key: &str) -> Vec<String> {
    v.get(key)
        .and_then(|k| k.as_array())
        .and_then(|a| {
            a.iter()
                .map(|x| x.as_str().map(String::from))
                .collect::<Option<Vec<_>>>()
        })
        .unwrap_or_default()
}

fn body_field(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(|k| k.as_str().map(String::from))
        .unwrap_or_default()
}

fn store_tags(st: &mut ConfigState, arn: &str, tags: &[(String, String)]) {
    let entry = st.tags.entry(arn.to_string()).or_default();
    for (k, v) in tags {
        entry.retain(|(ek, _)| ek != k);
        entry.push((k.clone(), v.clone()));
    }
}

fn tags_view(slot: &Slot<ConfigState>, req: &Request, arn: &str) -> Vec<Value> {
    let map = slot.read();
    map.get(&(req.account, req.region.clone()))
        .and_then(|st| st.tags.get(arn))
        .map(|m| {
            m.iter()
                .map(|(k, v)| json!({ "Key": k, "Value": v }))
                .collect()
        })
        .unwrap_or_default()
}

fn tag_pairs(v: &Value, key: &str) -> Vec<(String, String)> {
    v.get(key)
        .and_then(|k| k.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|t| {
                    let k = t.get("Key")?.as_str()?;
                    let val = t.get("Value")?.as_str()?;
                    Some((k.to_string(), val.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    struct Ctx(u64);

    impl Ctx {
        fn req(&self, op: &str, body: Value) -> Request {
            Request {
                service: "config".into(),
                operation: op.into(),
                method: "POST".into(),
                path: "/".into(),
                query_string: String::new(),
                headers: HashMap::from([(
                    "x-amz-target".to_string(),
                    format!("StarlingDoveService.{op}"),
                )]),
                body: Bytes::from(serde_json::to_vec(&body).unwrap()),
                account: self.0,
                region: "us-east-1".into(),
            }
        }
    }

    fn fresh_ctx() -> Ctx {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(171111111110);
        Ctx(NEXT.fetch_add(1, Ordering::SeqCst))
    }

    fn parsed(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    fn aws_rule(name: &str) -> Value {
        json!({
            "ConfigRuleName": name,
            "Source": {"Owner": "AWS", "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED"},
        })
    }

    #[test]
    fn rule_describe_delete_not_found() {
        let ctx = fresh_ctx();
        dispatch(&ctx.req("PutConfigRule", json!({ "ConfigRule": aws_rule("r1") })));
        let got = parsed(&dispatch(
            &ctx.req("DescribeConfigRules", json!({ "ConfigRuleNames": ["r1"] })),
        ));
        let rule = &got["ConfigRules"][0];
        assert_eq!(rule["ConfigRuleName"], "r1");
        assert!(rule["ConfigRuleArn"]
            .as_str()
            .unwrap()
            .contains("config-rule/"));

        // Missing rule name triggers NoSuchConfigRuleException.
        let missing = dispatch(&ctx.req(
            "DescribeConfigRules",
            json!({ "ConfigRuleNames": ["never"] }),
        ));
        assert_eq!(parsed(&missing)["__type"], "NoSuchConfigRuleException");

        dispatch(&ctx.req("DeleteConfigRule", json!({ "ConfigRuleName": "r1" })));
        let gone = dispatch(&ctx.req("DeleteConfigRule", json!({ "ConfigRuleName": "r1" })));
        assert_eq!(parsed(&gone)["__type"], "NoSuchConfigRuleException");
    }

    #[test]
    fn recorder_lifecycle_requires_delivery_channel() {
        let ctx = fresh_ctx();
        dispatch(&ctx.req(
            "PutConfigurationRecorder",
            json!({ "ConfigurationRecorder": {"name": "default", "roleARN": "arn:aws:iam::1:role/r", "recordingGroup": {"allSupported": true}} }),
        ));
        // No delivery channel -> NoAvailableDeliveryChannelException
        let err = dispatch(&ctx.req(
            "StartConfigurationRecorder",
            json!({ "ConfigurationRecorderName": "default" }),
        ));
        assert_eq!(
            parsed(&err)["__type"],
            "NoAvailableDeliveryChannelException"
        );

        dispatch(&ctx.req(
            "PutDeliveryChannel",
            json!({ "DeliveryChannel": {"name": "default", "s3BucketName": "bkt"} }),
        ));
        dispatch(&ctx.req(
            "StartConfigurationRecorder",
            json!({ "ConfigurationRecorderName": "default" }),
        ));
        let st = parsed(&dispatch(&ctx.req(
            "DescribeConfigurationRecorderStatus",
            json!({ "ConfigurationRecorderNames": ["default"] }),
        )));
        assert_eq!(st["ConfigurationRecordersStatus"][0]["recording"], true);

        let recs = parsed(&dispatch(&ctx.req(
            "DescribeConfigurationRecorders",
            json!({ "ConfigurationRecorderNames": ["default"] }),
        )));
        assert_eq!(recs["ConfigurationRecorders"][0]["name"], "default");
        assert!(recs["ConfigurationRecorders"][0]["roleARN"]
            .as_str()
            .is_some());

        dispatch(&ctx.req(
            "StopConfigurationRecorder",
            json!({ "ConfigurationRecorderName": "default" }),
        ));
        let st = parsed(&dispatch(&ctx.req(
            "DescribeConfigurationRecorderStatus",
            json!({ "ConfigurationRecorderNames": ["default"] }),
        )));
        assert_eq!(st["ConfigurationRecordersStatus"][0]["recording"], false);

        dispatch(&ctx.req(
            "DeleteConfigurationRecorder",
            json!({ "ConfigurationRecorderName": "default" }),
        ));
        let gone = parsed(&dispatch(
            &ctx.req("DescribeConfigurationRecorders", json!({})),
        ));
        assert_eq!(gone["ConfigurationRecorders"], json!([]));
    }

    #[test]
    fn retention_aggregator_stored_query() {
        let ctx = fresh_ctx();
        let ret = parsed(&dispatch(&ctx.req(
            "PutRetentionConfiguration",
            json!({ "RetentionPeriodInDays": 365 }),
        )));
        assert_eq!(ret["RetentionConfiguration"]["RetentionPeriodInDays"], 365);
        assert_eq!(ret["RetentionConfiguration"]["Name"], "default");
        dispatch(&ctx.req(
            "DeleteRetentionConfiguration",
            json!({ "RetentionConfigurationName": "default" }),
        ));
        let list = parsed(&dispatch(
            &ctx.req("DescribeRetentionConfigurations", json!({})),
        ));
        assert_eq!(list["RetentionConfigurations"], json!([]));

        let agg = parsed(&dispatch(&ctx.req(
            "PutConfigurationAggregator",
            json!({ "ConfigurationAggregatorName": "agg", "AccountAggregationSources": [{"AccountIds": ["123456789012"], "AllAwsRegions": true}] }),
        )));
        assert_eq!(
            agg["ConfigurationAggregator"]["ConfigurationAggregatorName"],
            "agg"
        );
        let describe = parsed(&dispatch(&ctx.req(
            "DescribeConfigurationAggregators",
            json!({ "ConfigurationAggregatorNames": ["agg"] }),
        )));
        assert_eq!(
            describe["ConfigurationAggregators"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        dispatch(&ctx.req(
            "PutStoredQuery",
            json!({ "StoredQuery": {"QueryName": "sq", "Expression": "SELECT 1"} }),
        ));
        let got = parsed(&dispatch(
            &ctx.req("GetStoredQuery", json!({ "QueryName": "sq" })),
        ));
        assert_eq!(got["StoredQuery"]["QueryName"], "sq");
        assert_eq!(got["StoredQuery"]["Expression"], "SELECT 1");
        dispatch(&ctx.req("DeleteStoredQuery", json!({ "QueryName": "sq" })));
        let gone = dispatch(&ctx.req("GetStoredQuery", json!({ "QueryName": "sq" })));
        assert_eq!(parsed(&gone)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn gap_stubs_return_empty_lists() {
        let ctx = fresh_ctx();
        assert!(
            parsed(&dispatch(&ctx.req("DescribeConformancePacks", json!({}))))
                ["ConformancePackDetails"]
                .is_array()
        );
        assert!(parsed(&dispatch(
            &ctx.req("DescribeOrganizationConfigRules", json!({}))
        ))["OrganizationConfigRules"]
            .is_array());
        assert!(parsed(&dispatch(
            &ctx.req("DescribePendingAggregationRequests", json!({}))
        ))["PendingAggregationRequests"]
            .is_array());
        assert!(
            parsed(&dispatch(&ctx.req("ListStoredQueries", json!({}))))["StoredQueryMetadata"]
                .is_array()
        );
        assert!(parsed(&dispatch(
            &ctx.req("SelectResourceConfig", json!({ "Expression": "SELECT 1" }))
        ))["Results"]
            .is_array());
        assert!(parsed(&dispatch(&ctx.req(
            "BatchGetResourceConfig",
            json!({ "resourceKeys": [{"resourceType": "AWS::S3::Bucket", "resourceId": "x"}] }),
        )))["baseConfigurationItems"]
            .is_array());
        let missing = dispatch(&ctx.req(
            "GetResourceConfigHistory",
            json!({ "resourceType": "AWS::S3::Bucket", "resourceId": "no-such-bucket" }),
        ));
        assert_eq!(parsed(&missing)["__type"], "ResourceNotDiscoveredException");
    }
}
