//! Network Firewall backend (JSON protocol, target
//! `NetworkFirewall_20201112.<Op>`).
//!
//! Port of moto's networkfirewall: firewall CRUD with moto's fixed
//! update-token and READY/IN_SYNC status shape, firewall policies,
//! logging configurations and a tag store.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};

#[derive(Default)]
struct State {
    /// FirewallArn -> firewall record.
    firewalls: Vec<Value>,
    /// arn -> {Key: Value}
    tags: Vec<(String, Value)>,
    /// arn -> LoggingConfiguration
    logging: Vec<(String, Value)>,
    /// FirewallPolicyName -> policy response dict.
    policies: Vec<Value>,
    /// RuleGroupResponse records plus their `Rules` string / `RuleGroup` body
    /// (`rules_string` / `rule_group_body`, dropped on the wire render).
    rule_groups: Vec<Value>,
    /// TLSInspectionConfigurationResponse records plus their configuration
    /// body (`tls_body`).
    tls_configs: Vec<Value>,
    /// VpcEndpointAssociation records.
    vpc_endpoint_associations: Vec<Value>,
    /// Proxy records keyed to their NatGatewayId / ProxyName / ProxyArn.
    proxies: Vec<Value>,
    /// ProxyConfiguration records; `rule_group_rows` carries the attached
    /// ProxyConfigRuleGroup list.
    proxy_configurations: Vec<Value>,
    /// ProxyRuleGroup records with their per-phase `Rules`.
    proxy_rule_groups: Vec<Value>,
    /// (TransitGatewayAttachmentId, status) rows. VPC associations feed
    /// CreateFirewall, which is moto-ported and never creates one; rows are
    /// only born here when a caller works with a real-looking id.
    transit_gateways: Vec<(String, String)>,
    /// Flow capture/flush operations (StartFlowCapture / StartFlowFlush).
    flow_operations: Vec<Value>,
    /// Analysis reports launched with StartAnalysisReport.
    analysis_reports: Vec<Value>,
    /// arn -> resource policy (PutResourcePolicy).
    resource_policies: Vec<(String, String)>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("networkfirewall"));
    SLOT.clone()
}

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

const UPDATE_TOKEN: &str = "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d";

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "networkfirewall" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(req, b, &body))
}

/// First matching firewall by ARN, else by name.
impl State {
    fn find(&self, arn: &str, name: &str) -> Option<&Value> {
        self.firewalls.iter().find(|fw| {
            fw["FirewallArn"].as_str() == Some(arn) || fw["FirewallName"].as_str() == Some(name)
        })
    }

    fn find_mut(&mut self, arn: &str, name: &str) -> Option<&mut Value> {
        self.firewalls.iter_mut().find(|fw| {
            fw["FirewallArn"].as_str() == Some(arn) || fw["FirewallName"].as_str() == Some(name)
        })
    }
}

fn firewall_status() -> Value {
    json!({"Status": "READY", "ConfigurationSyncStateSummary": "IN_SYNC"})
}

fn not_found(msg: &str) -> Response {
    let e = json!({"__type": "ResourceNotFoundException", "message": msg});
    Response::json(400, serde_json::to_vec(&e).unwrap_or_default())
}

fn invalid_request(msg: &str) -> Response {
    let e = json!({"__type": "InvalidRequestException", "message": msg});
    Response::json(400, serde_json::to_vec(&e).unwrap_or_default())
}

/// Seconds-precision epoch float (JSON protocol timestamps are epoch).
fn now_epoch() -> f64 {
    chrono::Utc::now().timestamp() as f64
}

/// Short deployed-style id (hex, no hyphens) for generated resource ids.
fn short_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..16].to_string()
}

fn uuid_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

impl State {
    fn find_rule_group(&self, arn: &str, name: &str) -> Option<&Value> {
        self.rule_groups.iter().find(|rg| {
            rg["RuleGroupArn"].as_str() == Some(arn) || rg["RuleGroupName"].as_str() == Some(name)
        })
    }

    fn find_rule_group_mut(&mut self, arn: &str, name: &str) -> Option<&mut Value> {
        self.rule_groups.iter_mut().find(|rg| {
            rg["RuleGroupArn"].as_str() == Some(arn) || rg["RuleGroupName"].as_str() == Some(name)
        })
    }

    fn find_tls(&self, arn: &str, name: &str) -> Option<&Value> {
        self.tls_configs.iter().find(|v| {
            v["TLSInspectionConfigurationArn"].as_str() == Some(arn)
                || v["TLSInspectionConfigurationName"].as_str() == Some(name)
        })
    }

    fn find_tls_mut(&mut self, arn: &str, name: &str) -> Option<&mut Value> {
        self.tls_configs.iter_mut().find(|v| {
            v["TLSInspectionConfigurationArn"].as_str() == Some(arn)
                || v["TLSInspectionConfigurationName"].as_str() == Some(name)
        })
    }

    fn find_vpc_assoc(&self, arn: &str) -> Option<&Value> {
        self.vpc_endpoint_associations
            .iter()
            .find(|v| v["VpcEndpointAssociationArn"].as_str() == Some(arn))
    }

    fn find_proxy(&self, arn: &str, name: &str, nat: &str) -> Option<&Value> {
        self.proxies.iter().find(|v| {
            v["ProxyArn"].as_str() == Some(arn)
                || v["ProxyName"].as_str() == Some(name)
                || v["NatGatewayId"].as_str() == Some(nat)
        })
    }

    fn find_proxies_mut(&mut self, arn: &str, name: &str, nat: &str) -> Option<&mut Value> {
        self.proxies.iter_mut().find(|v| {
            v["ProxyArn"].as_str() == Some(arn)
                || v["ProxyName"].as_str() == Some(name)
                || v["NatGatewayId"].as_str() == Some(nat)
        })
    }

    fn find_proxy_config(&self, arn: &str, name: &str) -> Option<&Value> {
        self.proxy_configurations.iter().find(|v| {
            v["ProxyConfigurationArn"].as_str() == Some(arn)
                || v["ProxyConfigurationName"].as_str() == Some(name)
        })
    }

    fn find_proxy_config_mut(&mut self, arn: &str, name: &str) -> Option<&mut Value> {
        self.proxy_configurations.iter_mut().find(|v| {
            v["ProxyConfigurationArn"].as_str() == Some(arn)
                || v["ProxyConfigurationName"].as_str() == Some(name)
        })
    }

    fn find_proxy_rule_group(&self, arn: &str, name: &str) -> Option<&Value> {
        self.proxy_rule_groups.iter().find(|v| {
            v["ProxyRuleGroupArn"].as_str() == Some(arn)
                || v["ProxyRuleGroupName"].as_str() == Some(name)
        })
    }

    fn find_proxy_rule_group_mut(&mut self, arn: &str, name: &str) -> Option<&mut Value> {
        self.proxy_rule_groups.iter_mut().find(|v| {
            v["ProxyRuleGroupArn"].as_str() == Some(arn)
                || v["ProxyRuleGroupName"].as_str() == Some(name)
        })
    }

    fn find_flow_operation(&self, arn: &str, op_id: &str) -> Option<&Value> {
        self.flow_operations.iter().find(|v| {
            v["FirewallArn"].as_str() == Some(arn) && v["FlowOperationId"].as_str() == Some(op_id)
        })
    }

    fn find_report(&self, arn: &str, report_id: &str) -> Option<&Value> {
        self.analysis_reports.iter().find(|v| {
            v["FirewallArn"].as_str() == Some(arn)
                && v["AnalysisReportId"].as_str() == Some(report_id)
        })
    }
}

fn handle(req: &Request, st: &mut State, p: &Value) -> Response {
    let region = req.region.as_str();
    let account = req.account;
    let name = p.get("FirewallName").and_then(Value::as_str).unwrap_or("");
    let arn_param = p.get("FirewallArn").and_then(Value::as_str).unwrap_or("");
    match req.operation.as_str() {
        "CreateFirewall" => {
            let arn = format!("arn:aws:network-firewall:{region}:{account}:firewall/{name}");
            let fw = json!({
                "FirewallName": name,
                "FirewallArn": arn,
                "FirewallPolicyArn": p.get("FirewallPolicyArn").cloned().unwrap_or(Value::Null),
                "VpcId": p.get("VpcId").cloned().unwrap_or(Value::Null),
                "SubnetMappings": p.get("SubnetMappings").cloned().unwrap_or(json!([])),
                // moto defaults all three change-protection flags to true
                // (moto/networkfirewall/models.py — delete/subnet/policy).
                "DeleteProtection": p.get("DeleteProtection").cloned().unwrap_or(json!(true)),
                "SubnetChangeProtection": p.get("SubnetChangeProtection").cloned().unwrap_or(json!(true)),
                "FirewallPolicyChangeProtection": p.get("FirewallPolicyChangeProtection").cloned().unwrap_or(json!(true)),
                "Description": p.get("Description").cloned().unwrap_or(Value::Null),
                "Tags": p.get("Tags").cloned().unwrap_or(json!([])),
                "EncryptionConfiguration": p.get("EncryptionConfiguration").cloned().unwrap_or(Value::Null),
                "EnabledAnalysisTypes": p.get("EnabledAnalysisTypes").cloned().unwrap_or(json!([])),
            });
            st.firewalls.push(fw.clone());
            if let Some(tags) = p.get("Tags") {
                store_tags(st, &arn, tags);
            }
            ok(&json!({"Firewall": fw, "FirewallStatus": firewall_status()}))
        }
        "DescribeFirewall" => match st.find(arn_param, name) {
            Some(fw) => ok(&json!({
                "UpdateToken": UPDATE_TOKEN,
                "Firewall": fw.clone(),
                "FirewallStatus": firewall_status(),
            })),
            None => not_found(&format!(
                "Firewall with name {name} or Arn {arn_param} could not be found"
            )),
        },
        "DeleteFirewall" => {
            st.firewalls.retain(|fw| {
                !(fw["FirewallName"].as_str() == Some(name)
                    || fw["FirewallArn"].as_str() == Some(arn_param))
            });
            ok(&json!({}))
        }
        "ListFirewalls" => ok(&json!({
            "Firewalls": st.firewalls.clone(),
            "nextToken": Value::Null,
        })),
        "UpdateFirewallDeleteProtection"
        | "UpdateSubnetChangeProtection"
        | "UpdateFirewallPolicyChangeProtection" => {
            let key = match req.operation.as_str() {
                "UpdateFirewallDeleteProtection" => "DeleteProtection",
                "UpdateSubnetChangeProtection" => "SubnetChangeProtection",
                _ => "FirewallPolicyChangeProtection",
            };
            match st.find_mut(arn_param, name) {
                Some(fw) => {
                    if let Some(v) = p.get(key) {
                        fw[key] = v.clone();
                    }
                    ok(&json!({
                        "Firewall": fw.clone(),
                        "FirewallStatus": firewall_status(),
                        "UpdateToken": UPDATE_TOKEN,
                    }))
                }
                None => not_found(&format!("Firewall {name} not found")),
            }
        }
        "DescribeLoggingConfiguration" => {
            let Some(fw) = st.find(arn_param, name) else {
                return not_found(&format!("Firewall {name} not found"));
            };
            let arn = fw["FirewallArn"].as_str().unwrap_or("").to_string();
            let cfg = last_logging(st, &arn);
            ok(&json!({"FirewallArn": arn, "LoggingConfiguration": cfg}))
        }
        "UpdateLoggingConfiguration" => {
            let Some(fw) = st.find(arn_param, name) else {
                return not_found(&format!("Firewall {name} not found"));
            };
            let arn = fw["FirewallArn"].as_str().unwrap_or("").to_string();
            if let Some(cfg) = p.get("LoggingConfiguration") {
                st.logging.push((arn.clone(), cfg.clone()));
            }
            ok(&json!({
                "FirewallArn": arn,
                "FirewallName": name,
                "LoggingConfiguration": last_logging(st, &arn),
            }))
        }
        "CreateFirewallPolicy" => {
            let policy_name = p
                .get("FirewallPolicyName")
                .and_then(Value::as_str)
                .unwrap_or("");
            let arn = format!(
                "arn:aws:network-firewall:{region}:{account}:firewall-policy/{policy_name}"
            );
            let policy = json!({
                "FirewallPolicyName": policy_name,
                "FirewallPolicyArn": arn,
                "FirewallPolicyId": uuid_id(),
                "FirewallPolicyStatus": "ACTIVE",
                "FirewallPolicy": p.get("FirewallPolicy").cloned().unwrap_or(json!({})),
                "Description": p.get("Description").cloned().unwrap_or(Value::Null),
                "LastModifiedTime": now_epoch(),
            });
            if let Some(tags) = p.get("Tags") {
                store_tags(st, &arn, tags);
            }
            st.policies.push(policy.clone());
            ok(&json!({
                "FirewallPolicyResponse": policy_response(&policy, st),
                "UpdateToken": UPDATE_TOKEN,
            }))
        }
        "DeleteFirewallPolicy" => {
            let pn = p
                .get("FirewallPolicyName")
                .and_then(Value::as_str)
                .unwrap_or("");
            st.policies.retain(|v| v["FirewallPolicyName"] != json!(pn));
            ok(&json!({}))
        }
        "DescribeFirewallPolicy" => {
            let pn = p
                .get("FirewallPolicyName")
                .and_then(Value::as_str)
                .unwrap_or("");
            match st
                .policies
                .iter()
                .find(|v| v["FirewallPolicyName"] == json!(pn))
            {
                Some(v) => ok(&json!({
                    "FirewallPolicyResponse": policy_response(v, st),
                    "FirewallPolicy": v["FirewallPolicy"].clone(),
                    "UpdateToken": UPDATE_TOKEN,
                })),
                None => not_found(&format!("FirewallPolicy {pn} not found")),
            }
        }
        "UpdateFirewallPolicy" => {
            let (parn, pn) = (policy_arn(p), policy_name(p));
            match st.policies.iter_mut().find(|v| {
                v["FirewallPolicyArn"].as_str() == Some(parn)
                    || v["FirewallPolicyName"].as_str() == Some(pn)
            }) {
                Some(pl) => {
                    if let Some(v) = p.get("FirewallPolicy") {
                        pl["FirewallPolicy"] = v.clone();
                    }
                    if let Some(v) = p.get("Description") {
                        pl["Description"] = v.clone();
                    }
                    if let Some(v) = p.get("EncryptionConfiguration") {
                        pl["EncryptionConfiguration"] = v.clone();
                    }
                    pl["LastModifiedTime"] = json!(now_epoch());
                    let updated = pl.clone();
                    ok(&json!({
                        "UpdateToken": UPDATE_TOKEN,
                        "FirewallPolicyResponse": policy_response(&updated, st),
                    }))
                }
                None => not_found(&format!("FirewallPolicy {pn} not found")),
            }
        }
        "ListFirewallPolicies" => ok(&json!({
            "FirewallPolicies": st
                .policies
                .iter()
                .map(|pl| json!({"Name": pl["FirewallPolicyName"], "Arn": pl["FirewallPolicyArn"]}))
                .collect::<Vec<Value>>(),
            "NextToken": next_token_of(p),
        })),
        "ListTagsForResource" => {
            let tags = tag_map(st, arn_param);
            ok(&json!({"Tags": tags}))
        }
        "TagResource" => {
            if let Some(tags) = p.get("Tags") {
                store_tags(st, arn_param, tags);
            }
            ok(&json!({}))
        }
        "UntagResource" => {
            for (a, v) in st.tags.iter_mut() {
                if a == arn_param {
                    if let Some(obj) = v.as_object_mut() {
                        for k in tag_keys(p) {
                            obj.remove(&k);
                        }
                    }
                }
            }
            ok(&json!({}))
        }
        // ------------------------------------------------------------------
        // Rule groups (modeled per botocore; moto has no handler).
        // ------------------------------------------------------------------
        "CreateRuleGroup" => {
            let rn = p
                .get("RuleGroupName")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let rules_string = p.get("Rules").and_then(Value::as_str).unwrap_or("");
            let group_body = p
                .get("RuleGroup")
                .filter(|v| v.is_object() && !v.as_object().unwrap().is_empty());
            if group_body.is_some() == !rules_string.is_empty() {
                return invalid_request("Please specify one of: Rules or RuleGroup");
            }
            let id = uuid_id();
            // moto/AWS wire ARN: <kind>/<name> — no id suffix. The unique
            // uuid stays in RuleGroupId; name|uuid lives on only as the
            // internal `rule_group_key` state key (never rendered).
            let arn = format!("arn:aws:network-firewall:{region}:{account}:rulegroup/{rn}");
            let capacity = p.get("Capacity").cloned().unwrap_or(json!(0));
            let rg = json!({
                "RuleGroupArn": arn.clone(),
                "RuleGroupName": rn,
                "RuleGroupId": id,
                "Description": p.get("Description").cloned().unwrap_or(Value::Null),
                "Type": p.get("Type").cloned().unwrap_or(Value::Null),
                "Capacity": capacity,
                "RuleGroupStatus": "ACTIVE",
                "EncryptionConfiguration": p.get("EncryptionConfiguration").cloned().unwrap_or(Value::Null),
                "SourceMetadata": p.get("SourceMetadata").cloned().unwrap_or(Value::Null),
                "LastModifiedTime": now_epoch(),
                "rules_string": rules_string,
                "rule_group_body": group_body.cloned().unwrap_or(Value::Null),
                "rule_group_key": format!("{rn}|{id}"),
            });
            if let Some(tags) = p.get("Tags") {
                store_tags(st, &arn, tags);
            }
            st.rule_groups.push(rg.clone());
            ok(
                &json!({"UpdateToken": UPDATE_TOKEN, "RuleGroupResponse": rule_group_response(&rg, st)}),
            )
        }
        "DescribeRuleGroup" => {
            let Some(rg) = st.find_rule_group(&rule_arn(p), &rule_name(p)) else {
                return not_found(&format!(
                    "RuleGroup with name {} or Arn {} could not be found",
                    p.get("RuleGroupName").and_then(Value::as_str).unwrap_or(""),
                    p.get("RuleGroupArn").and_then(Value::as_str).unwrap_or("")
                ));
            };
            ok(&json!({
                "UpdateToken": UPDATE_TOKEN,
                "RuleGroup": rule_group_body(rg),
                "RuleGroupResponse": rule_group_response(rg, st),
            }))
        }
        "UpdateRuleGroup" => {
            let rules_string = p.get("Rules").and_then(Value::as_str).unwrap_or("");
            let group_body = p
                .get("RuleGroup")
                .filter(|v| v.is_object() && !v.as_object().unwrap().is_empty());
            if group_body.is_some() && !rules_string.is_empty() {
                return invalid_request("Please specify one of: Rules or RuleGroup");
            }
            let Some(rg) = st.find_rule_group_mut(&rule_arn(p), &rule_name(p)) else {
                return not_found(&format!(
                    "RuleGroup with name {} or Arn {} could not be found",
                    p.get("RuleGroupName").and_then(Value::as_str).unwrap_or(""),
                    p.get("RuleGroupArn").and_then(Value::as_str).unwrap_or("")
                ));
            };
            if let Some(v) = p.get("Description") {
                rg["Description"] = v.clone();
            }
            if let Some(v) = p.get("EncryptionConfiguration") {
                rg["EncryptionConfiguration"] = v.clone();
            }
            if let Some(group) = group_body {
                rg["rule_group_body"] = group.clone();
            } else if !rules_string.is_empty() {
                rg["rules_string"] = json!(rules_string);
                rg["rule_group_body"] = Value::Null;
            }
            rg["LastModifiedTime"] = json!(now_epoch());
            let updated = rg.clone();
            let rendered = rule_group_response(&updated, st);
            ok(&json!({"UpdateToken": UPDATE_TOKEN, "RuleGroupResponse": rendered}))
        }
        "DeleteRuleGroup" => {
            let (arn, nm) = (rule_arn(p), rule_name(p));
            let Some(rg) = st.find_rule_group(&arn, &nm).cloned() else {
                return not_found(&format!(
                    "RuleGroup with name {nm} or Arn {arn} could not be found"
                ));
            };
            let mut out = rule_group_response(&rg, st);
            out["RuleGroupStatus"] = json!("DELETING");
            st.rule_groups.retain(|v| v != &rg);
            st.tags
                .retain(|(a, _)| a != rg["RuleGroupArn"].as_str().unwrap_or(""));
            ok(&json!({ "RuleGroupResponse": out }))
        }
        "ListRuleGroups" => {
            let want_type = p.get("Type").and_then(Value::as_str).unwrap_or("");
            let groups: Vec<Value> = st
                .rule_groups
                .iter()
                .filter(|rg| want_type.is_empty() || rg["Type"].as_str() == Some(want_type))
                .map(|rg| {
                    json!({
                        "Name": rg["RuleGroupName"],
                        "Arn": rg["RuleGroupArn"],
                    })
                })
                .collect();
            ok(&json!({ "RuleGroups": groups, "NextToken": next_token_of(p) }))
        }
        "DescribeRuleGroupMetadata" => {
            let (arn, nm) = (rule_arn(p), rule_name(p));
            match st.find_rule_group(&arn, &nm) {
                Some(rg) => ok(&json!({
                    "RuleGroupArn": rg["RuleGroupArn"],
                    "RuleGroupName": rg["RuleGroupName"],
                    "Description": rg["Description"],
                    "Type": rg["Type"],
                    "Capacity": rg["Capacity"],
                    "StatefulRuleOptions": rule_group_body(rg)["StatefulRuleOptions"],
                    "LastModifiedTime": rg["LastModifiedTime"],
                })),
                None => not_found(&format!(
                    "RuleGroup with name {nm} or Arn {arn} could not be found"
                )),
            }
        }
        "DescribeRuleGroupSummary" => {
            let (arn, nm) = (rule_arn(p), rule_name(p));
            match st.find_rule_group(&arn, &nm) {
                Some(rg) => ok(&json!({
                    "RuleGroupName": rg["RuleGroupName"],
                    "Description": rg["Description"],
                    "Summary": { "RuleSummaries": [] },
                })),
                None => not_found(&format!(
                    "RuleGroup with name {nm} or Arn {arn} could not be found"
                )),
            }
        }
        // ------------------------------------------------------------------
        // TLS inspection configurations (modeled per botocore).
        // ------------------------------------------------------------------
        "CreateTLSInspectionConfiguration" => {
            let tn = p
                .get("TLSInspectionConfigurationName")
                .and_then(Value::as_str)
                .unwrap_or("");
            let id = uuid_id();
            let arn =
                format!("arn:aws:network-firewall:{region}:{account}:tls-inspection-configuration/{tn}|{id}");
            let record = json!({
                "TLSInspectionConfigurationArn": arn,
                "TLSInspectionConfigurationName": tn,
                "TLSInspectionConfigurationId": id,
                "TLSInspectionConfigurationStatus": "ACTIVE",
                "Description": p.get("Description").cloned().unwrap_or(Value::Null),
                "NumberOfAssociations": 0,
                "EncryptionConfiguration": p.get("EncryptionConfiguration").cloned().unwrap_or(Value::Null),
                "LastModifiedTime": now_epoch(),
                "tls_body": p.get("TLSInspectionConfiguration").cloned().unwrap_or(json!({})),
            });
            if let Some(tags) = p.get("Tags") {
                store_tags(st, &arn, tags);
            }
            st.tls_configs.push(record.clone());
            ok(
                &json!({"UpdateToken": UPDATE_TOKEN, "TLSInspectionConfigurationResponse": tls_response(&record, st)}),
            )
        }
        "DescribeTLSInspectionConfiguration" => {
            let (arn, nm) = (tls_arn(p), tls_name(p));
            match st.find_tls(&arn, &nm) {
                Some(t) => ok(&json!({
                    "UpdateToken": UPDATE_TOKEN,
                    "TLSInspectionConfiguration": t["tls_body"].clone(),
                    "TLSInspectionConfigurationResponse": tls_response(t, st),
                })),
                None => not_found(&format!(
                    "TLSInspectionConfiguration with name {nm} or Arn {arn} could not be found"
                )),
            }
        }
        "UpdateTLSInspectionConfiguration" => {
            let (arn, nm) = (tls_arn(p), tls_name(p));
            match st.find_tls_mut(&arn, &nm) {
                Some(t) => {
                    if let Some(v) = p.get("TLSInspectionConfiguration") {
                        t["tls_body"] = v.clone();
                    }
                    if let Some(v) = p.get("Description") {
                        t["Description"] = v.clone();
                    }
                    if let Some(v) = p.get("EncryptionConfiguration") {
                        t["EncryptionConfiguration"] = v.clone();
                    }
                    t["LastModifiedTime"] = json!(now_epoch());
                    let updated = t.clone();
                    let rendered = tls_response(&updated, st);
                    ok(
                        &json!({"UpdateToken": UPDATE_TOKEN, "TLSInspectionConfigurationResponse": rendered}),
                    )
                }
                None => not_found(&format!(
                    "TLSInspectionConfiguration with name {nm} or Arn {arn} could not be found"
                )),
            }
        }
        "DeleteTLSInspectionConfiguration" => {
            let (arn, nm) = (tls_arn(p), tls_name(p));
            let Some(t) = st.find_tls(&arn, &nm).cloned() else {
                return not_found(&format!(
                    "TLSInspectionConfiguration with name {nm} or Arn {arn} could not be found"
                ));
            };
            let mut out = tls_response(&t, st);
            out["TLSInspectionConfigurationStatus"] = json!("DELETING");
            let recorded_arn = t
                .get("TLSInspectionConfigurationArn")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            st.tls_configs.retain(|v| v != &t);
            st.tags.retain(|(ka, _)| ka.as_str() != recorded_arn);
            ok(&json!({ "TLSInspectionConfigurationResponse": out }))
        }
        "ListTLSInspectionConfigurations" => {
            let configs: Vec<Value> = st
                .tls_configs
                .iter()
                .map(|t| json!({ "Name": t["TLSInspectionConfigurationName"], "Arn": t["TLSInspectionConfigurationArn"] }))
                .collect();
            ok(&json!({ "TLSInspectionConfigurations": configs, "NextToken": next_token_of(p) }))
        }
        // ------------------------------------------------------------------
        // VPC endpoint associations (modeled per botocore).
        // ------------------------------------------------------------------
        "CreateVpcEndpointAssociation" => {
            let fire_arn = p.get("FirewallArn").and_then(Value::as_str).unwrap_or("");
            if st
                .firewalls
                .iter()
                .all(|f| f["FirewallArn"].as_str() != Some(fire_arn))
            {
                return not_found(&format!("Firewall {fire_arn} could not be found"));
            }
            let id = format!("vpcea-{}", short_id());
            let arn = format!(
                "arn:aws:network-firewall:{region}:{account}:vpc-endpoint-association/{id}"
            );
            let assoc = json!({
                "VpcEndpointAssociationId": id,
                "VpcEndpointAssociationArn": arn,
                "FirewallArn": fire_arn,
                "VpcId": p.get("VpcId").cloned().unwrap_or(Value::Null),
                "SubnetMapping": p.get("SubnetMapping").cloned().unwrap_or(Value::Null),
                "Description": p.get("Description").cloned().unwrap_or(Value::Null),
                "creation": now_epoch(),
            });
            if let Some(tags) = p.get("Tags") {
                store_tags(st, &arn, tags);
            }
            st.vpc_endpoint_associations.push(assoc.clone());
            ok(&json!({
                "VpcEndpointAssociation": stored_vpc_assoc(&assoc, st),
                "VpcEndpointAssociationStatus": { "Status": "READY" },
            }))
        }
        "DescribeVpcEndpointAssociation" => {
            match st.find_vpc_assoc(
                p.get("VpcEndpointAssociationArn")
                    .and_then(Value::as_str)
                    .unwrap_or(""),
            ) {
                Some(a) => ok(&json!({
                    "VpcEndpointAssociation": stored_vpc_assoc(a, st),
                    "VpcEndpointAssociationStatus": { "Status": "READY" },
                })),
                None => not_found(&format!(
                    "VpcEndpointAssociation with Arn {} could not be found",
                    p.get("VpcEndpointAssociationArn")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                )),
            }
        }
        "DeleteVpcEndpointAssociation" => {
            let arn = p
                .get("VpcEndpointAssociationArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            let Some(a) = st.find_vpc_assoc(arn).cloned() else {
                return not_found(&format!(
                    "VpcEndpointAssociation with Arn {arn} could not be found"
                ));
            };
            let mut out = stored_vpc_assoc(&a, st);
            out["VpcEndpointAssociationArn"] = a["VpcEndpointAssociationArn"].clone();
            st.vpc_endpoint_associations.retain(|v| v != &a);
            st.tags.retain(|(ka, _)| ka != arn);
            ok(&json!({
                "VpcEndpointAssociation": out,
                "VpcEndpointAssociationStatus": { "Status": "DELETING" },
            }))
        }
        "ListVpcEndpointAssociations" => {
            let fw = p.get("FirewallArn").and_then(Value::as_str).unwrap_or("");
            let rows: Vec<Value> = st
                .vpc_endpoint_associations
                .iter()
                .filter(|a| fw.is_empty() || a["FirewallArn"].as_str() == Some(fw))
                .map(|a| json!({ "VpcEndpointAssociationArn": a["VpcEndpointAssociationArn"] }))
                .collect();
            ok(&json!({ "VpcEndpointAssociations": rows, "NextToken": next_token_of(p) }))
        }
        // ------------------------------------------------------------------
        // Proxies, proxy configurations and proxy rule groups (modeled per
        // botocore).
        // ------------------------------------------------------------------
        "CreateProxy" => {
            let pn = p.get("ProxyName").and_then(Value::as_str).unwrap_or("");
            let nat = p.get("NatGatewayId").and_then(Value::as_str).unwrap_or("");
            let arn = format!(
                "arn:aws:network-firewall:{region}:{account}:proxy/{pn}|{}",
                short_id()
            );
            let proxy = json!({
                "ProxyName": pn,
                "ProxyArn": arn,
                "NatGatewayId": nat,
                "ProxyConfigurationName": p.get("ProxyConfigurationName").cloned().unwrap_or(Value::Null),
                "ProxyConfigurationArn": p.get("ProxyConfigurationArn").cloned().unwrap_or(Value::Null),
                "ProxyState": "ATTACHED",
                "ProxyModifyState": "COMPLETED",
                "ListenerProperties": p.get("ListenerProperties").cloned().unwrap_or(json!([])),
                "TlsInterceptProperties": p.get("TlsInterceptProperties").cloned().unwrap_or(Value::Null),
                "CreateTime": now_epoch(),
            });
            if let Some(tags) = p.get("Tags") {
                store_tags(st, &arn, tags);
            }
            st.proxies.push(proxy.clone());
            ok(&json!({ "Proxy": proxy, "UpdateToken": UPDATE_TOKEN }))
        }
        "DescribeProxy" => {
            let (arn, nm) = (proxy_arn(p), proxy_name(p));
            match st.find_proxy(&arn, &nm, "") {
                Some(px) => ok(&json!({ "Proxy": px.clone(), "UpdateToken": UPDATE_TOKEN })),
                None => not_found(&format!("Proxy {nm} not found")),
            }
        }
        "UpdateProxy" => {
            let (arn, nm, nat) = (proxy_arn(p), proxy_name(p), nat_id(p));
            match st.find_proxies_mut(&arn, &nm, &nat) {
                Some(px) => {
                    if let Some(add) = p.get("ListenerPropertiesToAdd") {
                        for item in add.as_array().unwrap_or(&vec![]) {
                            px["ListenerProperties"]
                                .as_array_mut()
                                .unwrap()
                                .push(item.clone());
                        }
                    }
                    if let Some(remove) = p.get("ListenerPropertiesToRemove") {
                        let drops: Vec<Value> = remove.as_array().unwrap_or(&vec![]).to_vec();
                        if let Some(list) = px["ListenerProperties"].as_array_mut() {
                            list.retain(|l| !drops.contains(l));
                        }
                    }
                    if let Some(tls) = p.get("TlsInterceptProperties") {
                        px["TlsInterceptProperties"] = tls.clone();
                    }
                    px["UpdateTime"] = json!(now_epoch());
                    px["ProxyModifyState"] = json!("COMPLETED");
                    ok(&json!({ "Proxy": px.clone(), "UpdateToken": UPDATE_TOKEN }))
                }
                None => not_found(&format!("Proxy {nm} not found")),
            }
        }
        "DeleteProxy" => {
            let (arn, nm, nat) = (proxy_arn(p), proxy_name(p), nat_id(p));
            let Some(px) = st.find_proxy(&arn, &nm, &nat).cloned() else {
                return not_found(&format!("Proxy {nm} not found"));
            };
            let out = json!({
                "NatGatewayId": px["NatGatewayId"].clone(),
                "ProxyName": px["ProxyName"].clone(),
                "ProxyArn": px["ProxyArn"].clone(),
            });
            st.proxies.retain(|v| v != &px);
            st.tags
                .retain(|(ka, _)| ka != px.get("ProxyArn").and_then(Value::as_str).unwrap_or(""));
            ok(&out)
        }
        "ListProxies" => ok(&json!({
            "Proxies": st.proxies.iter().map(|px| json!({"Name": px["ProxyName"], "Arn": px["ProxyArn"]})).collect::<Vec<Value>>(),
            "NextToken": next_token_of(p),
        })),
        "CreateProxyConfiguration" => {
            let name = p
                .get("ProxyConfigurationName")
                .and_then(Value::as_str)
                .unwrap_or("");
            let arn = format!(
                "arn:aws:network-firewall:{region}:{account}:proxy-configuration/{name}|{}",
                short_id()
            );
            let mut rows = Vec::new();
            let mut priority = 1;
            for name_key in ["RuleGroupNames", "RuleGroupArns"] {
                if let Some(list) = p.get(name_key).and_then(Value::as_array) {
                    for want in list {
                        let want = want.as_str().unwrap_or("");
                        let Some(rgrg) = st.find_proxy_rule_group("", want) else {
                            return not_found(&format!("Proxy rule group {want} not found"));
                        };
                        rows.push(json!({
                            "ProxyRuleGroupName": rgrg["ProxyRuleGroupName"],
                            "ProxyRuleGroupArn": rgrg["ProxyRuleGroupArn"],
                            "Priority": priority,
                        }));
                        priority += 1;
                    }
                }
            }
            let cfg = json!({
                "ProxyConfigurationName": name,
                "ProxyConfigurationArn": arn,
                "Description": p.get("Description").cloned().unwrap_or(Value::Null),
                "CreateTime": now_epoch(),
                "RuleGroups": rows,
                "DefaultRulePhaseActions": p.get("DefaultRulePhaseActions").cloned().unwrap_or(Value::Null),
            });
            if let Some(tags) = p.get("Tags") {
                store_tags(st, &arn, tags);
            }
            st.proxy_configurations.push(cfg.clone());
            ok(&json!({ "ProxyConfiguration": cfg, "UpdateToken": UPDATE_TOKEN }))
        }
        "DescribeProxyConfiguration" => {
            let (arn, nm) = (pc_arn(p), pc_name(p));
            match st.find_proxy_config(&arn, &nm) {
                Some(c) => {
                    ok(&json!({ "ProxyConfiguration": c.clone(), "UpdateToken": UPDATE_TOKEN }))
                }
                None => not_found(&format!("Proxy configuration {nm} not found")),
            }
        }
        "UpdateProxyConfiguration" => {
            let (arn, nm) = (pc_arn(p), pc_name(p));
            match st.find_proxy_config_mut(&arn, &nm) {
                Some(c) => {
                    if let Some(v) = p.get("DefaultRulePhaseActions") {
                        c["DefaultRulePhaseActions"] = v.clone();
                    }
                    ok(&json!({ "ProxyConfiguration": c.clone(), "UpdateToken": UPDATE_TOKEN }))
                }
                None => not_found(&format!("Proxy configuration {nm} not found")),
            }
        }
        "DeleteProxyConfiguration" => {
            let (arn, nm) = (pc_arn(p), pc_name(p));
            let Some(c) = st.find_proxy_config(&arn, &nm).cloned() else {
                return not_found(&format!("Proxy configuration {nm} not found"));
            };
            let out = json!({
                "ProxyConfigurationName": c["ProxyConfigurationName"].clone(),
                "ProxyConfigurationArn": c["ProxyConfigurationArn"].clone(),
            });
            st.proxy_configurations.retain(|v| v != &c);
            let dead_arn = c
                .get("ProxyConfigurationArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            st.tags.retain(|(ka, _)| ka.as_str() != dead_arn);
            ok(&out)
        }
        "ListProxyConfigurations" => ok(&json!({
            "ProxyConfigurations": st.proxy_configurations.iter().map(|c| json!({"Name": c["ProxyConfigurationName"], "Arn": c["ProxyConfigurationArn"]})).collect::<Vec<Value>>(),
            "NextToken": next_token_of(p),
        })),
        "CreateProxyRuleGroup" => {
            let nm = p
                .get("ProxyRuleGroupName")
                .and_then(Value::as_str)
                .unwrap_or("");
            let arn = format!(
                "arn:aws:network-firewall:{region}:{account}:proxy-rule-group/{nm}|{}",
                short_id()
            );
            let rgrg = json!({
                "ProxyRuleGroupName": nm,
                "ProxyRuleGroupArn": arn,
                "Description": p.get("Description").cloned().unwrap_or(Value::Null),
                "Rules": p.get("Rules").cloned().unwrap_or(json!({})),
                "CreateTime": now_epoch(),
            });
            if let Some(tags) = p.get("Tags") {
                store_tags(st, &arn, tags);
            }
            st.proxy_rule_groups.push(rgrg.clone());
            ok(&json!({ "ProxyRuleGroup": rgrg, "UpdateToken": UPDATE_TOKEN }))
        }
        "DescribeProxyRuleGroup" => {
            let (arn, nm) = (prg_arn(p), prg_name(p));
            match st.find_proxy_rule_group(&arn, &nm) {
                Some(g) => ok(&json!({ "ProxyRuleGroup": g.clone(), "UpdateToken": UPDATE_TOKEN })),
                None => not_found(&format!("Proxy rule group {nm} not found")),
            }
        }
        "DeleteProxyRuleGroup" => {
            let (arn, nm) = (prg_arn(p), prg_name(p));
            let Some(g) = st.find_proxy_rule_group(&arn, &nm).cloned() else {
                return not_found(&format!("Proxy rule group {nm} not found"));
            };
            let out = json!({
                "ProxyRuleGroupName": g["ProxyRuleGroupName"].clone(),
                "ProxyRuleGroupArn": g["ProxyRuleGroupArn"].clone(),
            });
            let dead_arn = g
                .get("ProxyRuleGroupArn")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            st.proxy_rule_groups.retain(|v| v != &g);
            st.tags.retain(|(ka, _)| ka.as_str() != dead_arn);
            // Attachments hand out this arn; drop dead references too.
            for c in st.proxy_configurations.iter_mut() {
                if let Some(list) = c["RuleGroups"].as_array_mut() {
                    list.retain(|row| {
                        row.get("ProxyRuleGroupArn").and_then(Value::as_str)
                            != Some(dead_arn.as_str())
                    });
                }
            }
            ok(&out)
        }
        "ListProxyRuleGroups" => ok(&json!({
            "ProxyRuleGroups": st.proxy_rule_groups.iter().map(|g| json!({"Name": g["ProxyRuleGroupName"], "Arn": g["ProxyRuleGroupArn"]})).collect::<Vec<Value>>(),
            "NextToken": next_token_of(p),
        })),
        "CreateProxyRules" => {
            let (arn, nm) = (prg_arn(p), prg_name(p));
            let Some(group) = st.find_proxy_rule_group_mut(&arn, &nm) else {
                return not_found(&format!("Proxy rule group {nm} not found"));
            };
            let rules = p.get("Rules").cloned().unwrap_or(json!({}));
            for phase in ["PreDNS", "PreREQUEST", "PostRESPONSE"] {
                if let Some(list) = rules.get(phase).and_then(Value::as_array) {
                    let converted: Vec<Value> = list
                        .iter()
                        .map(|r| {
                            let mut rule = r.clone();
                            if let Some(obj) = rule.as_object_mut() {
                                obj.remove("InsertPosition");
                            }
                            rule
                        })
                        .collect();
                    if let Some(target) =
                        group["Rules"].get_mut(phase).and_then(Value::as_array_mut)
                    {
                        let positions: Vec<i64> = list
                            .iter()
                            .map(|r| r.get("InsertPosition").and_then(Value::as_i64).unwrap_or(0))
                            .collect();
                        for (i, pos) in positions.iter().enumerate() {
                            let idx = (*pos as usize).min(target.len());
                            target.insert(idx, converted[i].clone());
                        }
                    } else {
                        if !converted.is_empty() {
                            if let Some(obj) = group["Rules"].as_object_mut() {
                                obj.insert(phase.to_string(), Value::Array(converted.clone()));
                            }
                        }
                    }
                }
            }
            let rendered = group.clone();
            ok(&json!({ "ProxyRuleGroup": rendered, "UpdateToken": UPDATE_TOKEN }))
        }
        "DeleteProxyRules" => {
            let (arn, nm) = (prg_arn(p), prg_name(p));
            let drops: Vec<String> = p
                .get("Rules")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            let Some(group) = st.find_proxy_rule_group_mut(&arn, &nm) else {
                return not_found(&format!("Proxy rule group {nm} not found"));
            };
            for phase in ["PreDNS", "PreREQUEST", "PostRESPONSE"] {
                if let Some(list) = group["Rules"].get_mut(phase).and_then(Value::as_array_mut) {
                    list.retain(|r| {
                        let name = r.get("ProxyRuleName").and_then(Value::as_str).unwrap_or("");
                        !drops.iter().any(|d| d == name)
                    });
                }
            }
            ok(&json!({ "ProxyRuleGroup": group.clone() }))
        }
        "DescribeProxyRule" => {
            let (garn, gnm) = (prg_arn(p), prg_name(p));
            let rule_name = p.get("ProxyRuleName").and_then(Value::as_str).unwrap_or("");
            match st.find_proxy_rule_group(&garn, &gnm) {
                Some(g) => match find_proxy_rule(g, rule_name) {
                    Some(rule) => {
                        ok(&json!({ "ProxyRule": rule.clone(), "UpdateToken": UPDATE_TOKEN }))
                    }
                    None => not_found(&format!("Proxy rule {rule_name} not found")),
                },
                None => not_found(&format!("Proxy rule group {gnm} not found")),
            }
        }
        "UpdateProxyRule" => {
            let (garn, gnm) = (prg_arn(p), prg_name(p));
            let rule_name = p.get("ProxyRuleName").and_then(Value::as_str).unwrap_or("");
            let Some(group) = st.find_proxy_rule_group_mut(&garn, &gnm) else {
                return not_found(&format!("Proxy rule group {gnm} not found"));
            };
            match find_proxy_rule_mut(group, rule_name) {
                Some(rule) => {
                    if let Some(v) = p.get("Action") {
                        rule["Action"] = v.clone();
                    }
                    if let Some(v) = p.get("Description") {
                        rule["Description"] = v.clone();
                    }
                    if p.get("AddConditions").is_some() && !rule["Conditions"].is_array() {
                        rule["Conditions"] = json!([]);
                    }
                    for key in ["AddConditions", "RemoveConditions"] {
                        if let Some(list) = p.get(key).and_then(Value::as_array) {
                            let values: Vec<Value> = list.to_vec();
                            let Some(conditions) = rule["Conditions"].as_array_mut() else {
                                continue;
                            };
                            if key == "AddConditions" {
                                for v in values {
                                    conditions.push(v);
                                }
                            } else {
                                conditions.retain(|c| !values.contains(c));
                            }
                        }
                    }
                    let now = json!(UPDATE_TOKEN);
                    ok(&json!({ "ProxyRule": rule.clone(), "UpdateToken": now }))
                }
                None => not_found(&format!("Proxy rule {rule_name} not found")),
            }
        }
        "UpdateProxyRuleGroupPriorities" => {
            let (arn, nm) = (pc_arn(p), pc_name(p));
            let Some(cfg) = st.find_proxy_config_mut(&arn, &nm) else {
                return not_found(&format!("Proxy configuration {nm} not found"));
            };
            let wanted = p.get("RuleGroups").cloned().unwrap_or(json!([]));
            if let Some(rows) = cfg["RuleGroups"].as_array_mut() {
                for want in wanted.as_array().unwrap_or(&vec![]) {
                    let want_name = want
                        .get("ProxyRuleGroupName")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let new_position =
                        want.get("NewPosition").and_then(Value::as_i64).unwrap_or(0) as usize;
                    if let Some(i) = rows.iter().position(|row| {
                        row.get("ProxyRuleGroupName").and_then(Value::as_str) == Some(want_name)
                    }) {
                        let row = rows.remove(i);
                        let idx = new_position.min(rows.len());
                        rows.insert(idx, row.clone());
                    }
                    // Priorities are dense positions: renumber after the move.
                    for (pos, row) in rows.iter_mut().enumerate() {
                        if let Some(obj) = row.as_object_mut() {
                            obj.insert("Priority".to_string(), json!(pos + 1));
                        }
                    }
                }
            }
            let rendered: Vec<Value> = cfg["RuleGroups"]
                .as_array()
                .map(|rows| {
                    rows.iter()
                        .map(|row| {
                            json!({
                                "ProxyRuleGroupName": row.get("ProxyRuleGroupName").cloned().unwrap_or(Value::Null),
                                "Priority": row.get("Priority").cloned().unwrap_or(Value::Null),
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            ok(&json!({ "ProxyRuleGroups": rendered, "UpdateToken": UPDATE_TOKEN }))
        }
        "UpdateProxyRulePriorities" => {
            let (arn, nm) = (prg_arn(p), prg_name(p));
            let Some(group) = st.find_proxy_rule_group_mut(&arn, &nm) else {
                return not_found(&format!("Proxy rule group {nm} not found"));
            };
            let phase = p
                .get("RuleGroupRequestPhase")
                .and_then(Value::as_str)
                .unwrap_or("");
            let wanted = p.get("Rules").cloned().unwrap_or(json!([]));
            if let Some(rows) = group["Rules"].get_mut(phase).and_then(Value::as_array_mut) {
                for want in wanted.as_array().unwrap_or(&vec![]) {
                    let want_name = want
                        .get("ProxyRuleName")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let new_position =
                        want.get("NewPosition").and_then(Value::as_i64).unwrap_or(0) as usize;
                    if let Some(i) = rows.iter().position(|row| {
                        row.get("ProxyRuleName").and_then(Value::as_str) == Some(want_name)
                    }) {
                        let row = rows.remove(i);
                        let idx = new_position.min(rows.len());
                        rows.insert(idx, row.clone());
                    }
                }
            }
            ok(&json!({
                "ProxyRuleGroupName": group["ProxyRuleGroupName"],
                "ProxyRuleGroupArn": group["ProxyRuleGroupArn"],
                "RuleGroupRequestPhase": phase,
                "Rules": wanted,
                "UpdateToken": UPDATE_TOKEN,
            }))
        }
        "AttachRuleGroupsToProxyConfiguration" => {
            let (arn, nm) = (pc_arn(p), pc_name(p));
            let wanted = p
                .get("RuleGroups")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            // Validate every referenced rule group first so a bad name fails
            // the whole call (modeled RFNF).
            for want in &wanted {
                let want_name = want
                    .get("ProxyRuleGroupName")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if st.find_proxy_rule_group("", want_name).is_none() {
                    return not_found(&format!("Proxy rule group {want_name} not found"));
                }
            }
            let Some(mut cfg) = st.find_proxy_config(&arn, &nm).cloned() else {
                return not_found(&format!("Proxy configuration {nm} not found"));
            };
            let mut max_priority = cfg["RuleGroups"].as_array().map_or(0, |rows| {
                rows.iter()
                    .filter_map(|r| r.get("Priority").and_then(Value::as_i64))
                    .max()
                    .unwrap_or(0)
            });
            for want in &wanted {
                let want_name = want
                    .get("ProxyRuleGroupName")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let Some(rg) = st.find_proxy_rule_group("", want_name) else {
                    continue;
                };
                max_priority += 1;
                if let Some(rows) = cfg["RuleGroups"].as_array_mut() {
                    rows.push(json!({
                        "ProxyRuleGroupName": rg["ProxyRuleGroupName"].clone(),
                        "ProxyRuleGroupArn": rg["ProxyRuleGroupArn"].clone(),
                        "Priority": max_priority,
                    }));
                }
            }
            if let Some(idx) = st
                .proxy_configurations
                .iter()
                .position(|v| v["ProxyConfigurationArn"] == cfg["ProxyConfigurationArn"])
            {
                st.proxy_configurations[idx] = cfg.clone();
            }
            ok(&json!({ "ProxyConfiguration": cfg, "UpdateToken": UPDATE_TOKEN }))
        }
        "DetachRuleGroupsFromProxyConfiguration" => {
            let (arn, nm) = (pc_arn(p), pc_name(p));
            let Some(mut cfg) = st.find_proxy_config(&arn, &nm).cloned() else {
                return not_found(&format!("Proxy configuration {nm} not found"));
            };
            let drop_names: Vec<String> = p
                .get("RuleGroupNames")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            let drop_arns: Vec<String> = p
                .get("RuleGroupArns")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            if let Some(rows) = cfg["RuleGroups"].as_array_mut() {
                rows.retain(|row| {
                    let name = row
                        .get("ProxyRuleGroupName")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let row_arn = row
                        .get("ProxyRuleGroupArn")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    !drop_names.iter().any(|d| d == name) && !drop_arns.iter().any(|d| d == row_arn)
                });
            }
            if let Some(idx) = st
                .proxy_configurations
                .iter()
                .position(|v| v["ProxyConfigurationArn"] == cfg["ProxyConfigurationArn"])
            {
                st.proxy_configurations[idx] = cfg.clone();
            }
            ok(&json!({ "ProxyConfiguration": cfg, "UpdateToken": UPDATE_TOKEN }))
        }
        // ------------------------------------------------------------------
        // Firewall-side updates and associations that extend the ported
        // firewall mutations (modeled per botocore).
        // ------------------------------------------------------------------
        "AssociateFirewallPolicy" => {
            let policy_arn = p
                .get("FirewallPolicyArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            if st
                .policies
                .iter()
                .all(|pl| pl["FirewallPolicyArn"].as_str() != Some(policy_arn))
            {
                return not_found(&format!("FirewallPolicy {policy_arn} not found"));
            }
            match st.find_mut(arn_param, name) {
                Some(fw) => {
                    fw["FirewallPolicyArn"] = json!(policy_arn);
                    ok(&json!({
                        "FirewallArn": fw["FirewallArn"],
                        "FirewallName": fw["FirewallName"],
                        "FirewallPolicyArn": policy_arn,
                        "UpdateToken": UPDATE_TOKEN,
                    }))
                }
                None => not_found(&format!("Firewall {name} not found")),
            }
        }
        "AssociateSubnets" | "DisassociateSubnets" => {
            let Some(fw) = st.find_mut(arn_param, name) else {
                return not_found(&format!("Firewall {name} not found"));
            };
            if req.operation == "AssociateSubnets" {
                fw["SubnetMappings"] = p.get("SubnetMappings").cloned().unwrap_or(json!([]));
            } else {
                let drop: Vec<String> = p
                    .get("SubnetIds")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .map(String::from)
                            .collect()
                    })
                    .unwrap_or_default();
                if let Some(list) = fw["SubnetMappings"].as_array_mut() {
                    list.retain(|m| {
                        let sid = m.get("SubnetId").and_then(Value::as_str).unwrap_or("");
                        !drop.iter().any(|d| d == sid)
                    });
                }
            }
            ok(&json!({
                "FirewallArn": fw["FirewallArn"],
                "FirewallName": fw["FirewallName"],
                "SubnetMappings": fw["SubnetMappings"].clone(),
                "UpdateToken": UPDATE_TOKEN,
            }))
        }
        "AssociateAvailabilityZones" | "DisassociateAvailabilityZones" => {
            let Some(fw) = st.find_mut(arn_param, name) else {
                return not_found(&format!("Firewall {name} not found"));
            };
            if req.operation == "AssociateAvailabilityZones" {
                fw["AvailabilityZoneMappings"] = p
                    .get("AvailabilityZoneMappings")
                    .cloned()
                    .unwrap_or(json!([]));
            } else {
                let drop: Vec<String> = p
                    .get("AvailabilityZoneMappings")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(|m| m.get("AvailabilityZone").and_then(Value::as_str))
                            .map(String::from)
                            .collect()
                    })
                    .unwrap_or_default();
                if let Some(list) = fw["AvailabilityZoneMappings"].as_array_mut() {
                    list.retain(|m| {
                        let az = m
                            .get("AvailabilityZone")
                            .and_then(Value::as_str)
                            .unwrap_or("");
                        !drop.iter().any(|d| d == az)
                    });
                }
            }
            ok(&json!({
                "FirewallArn": fw["FirewallArn"],
                "FirewallName": fw["FirewallName"],
                "AvailabilityZoneMappings": fw["AvailabilityZoneMappings"].clone(),
                "UpdateToken": UPDATE_TOKEN,
            }))
        }
        "UpdateAvailabilityZoneChangeProtection" => match st.find_mut(arn_param, name) {
            Some(fw) => {
                if let Some(v) = p.get("AvailabilityZoneChangeProtection") {
                    fw["AvailabilityZoneChangeProtection"] = v.clone();
                }
                ok(&json!({
                    "FirewallArn": fw["FirewallArn"],
                    "FirewallName": fw["FirewallName"],
                    "AvailabilityZoneChangeProtection": fw["AvailabilityZoneChangeProtection"].clone(),
                    "UpdateToken": UPDATE_TOKEN,
                }))
            }
            None => not_found(&format!("Firewall {name} not found")),
        },
        "UpdateFirewallDescription" => match st.find_mut(arn_param, name) {
            Some(fw) => {
                if let Some(v) = p.get("Description") {
                    fw["Description"] = v.clone();
                }
                ok(&json!({
                    "FirewallArn": fw["FirewallArn"],
                    "FirewallName": fw["FirewallName"],
                    "Description": fw["Description"].clone(),
                    "UpdateToken": UPDATE_TOKEN,
                }))
            }
            None => not_found(&format!("Firewall {name} not found")),
        },
        "UpdateFirewallEncryptionConfiguration" => match st.find_mut(arn_param, name) {
            Some(fw) => {
                if let Some(v) = p.get("EncryptionConfiguration") {
                    fw["EncryptionConfiguration"] = v.clone();
                }
                ok(&json!({
                    "FirewallArn": fw["FirewallArn"],
                    "FirewallName": fw["FirewallName"],
                    "EncryptionConfiguration": fw["EncryptionConfiguration"].clone(),
                    "UpdateToken": UPDATE_TOKEN,
                }))
            }
            None => not_found(&format!("Firewall {name} not found")),
        },
        "UpdateFirewallAnalysisSettings" => match st.find_mut(arn_param, name) {
            Some(fw) => {
                if let Some(v) = p.get("EnabledAnalysisTypes") {
                    fw["EnabledAnalysisTypes"] = v.clone();
                }
                ok(&json!({
                    "FirewallArn": fw["FirewallArn"],
                    "FirewallName": fw["FirewallName"],
                    "EnabledAnalysisTypes": fw["EnabledAnalysisTypes"].clone(),
                    "UpdateToken": UPDATE_TOKEN,
                }))
            }
            None => not_found(&format!("Firewall {name} not found")),
        },
        "DescribeFirewallMetadata" => match st.find(arn_param, name) {
            Some(fw) => {
                let mut zones = serde_json::Map::new();
                if let Some(rows) = fw["AvailabilityZoneMappings"].as_array() {
                    for m in rows {
                        if let Some(az) = m.get("AvailabilityZone").and_then(Value::as_str) {
                            zones.insert(az.to_string(), json!({}));
                        }
                    }
                }
                let mut out = json!({
                    "FirewallArn": fw["FirewallArn"],
                    "FirewallPolicyArn": fw["FirewallPolicyArn"],
                    "Description": fw["Description"],
                    "Status": "READY",
                    "SupportedAvailabilityZones": zones,
                });
                if let Some(tg) = fw.get("TransitGatewayId") {
                    if !tg.is_null() {
                        out["TransitGatewayAttachmentId"] = tg.clone();
                    }
                }
                ok(&out)
            }
            None => not_found(&format!("Firewall {name} not found")),
        },
        // ------------------------------------------------------------------
        // Flow operations and analysis reports (modeled per botocore).
        // ------------------------------------------------------------------
        "StartFlowCapture" | "StartFlowFlush" => {
            let fire_arn = p.get("FirewallArn").and_then(Value::as_str).unwrap_or("");
            if st
                .firewalls
                .iter()
                .all(|f| f["FirewallArn"].as_str() != Some(fire_arn))
            {
                return not_found(&format!("Firewall {fire_arn} not found"));
            }
            let op_type = if req.operation == "StartFlowCapture" {
                "FLOW_CAPTURE"
            } else {
                "FLOW_FLUSH"
            };
            let op_id = uuid_id();
            let rec = json!({
                "FirewallArn": fire_arn,
                "AvailabilityZone": p.get("AvailabilityZone").cloned().unwrap_or(Value::Null),
                "VpcEndpointAssociationArn": p.get("VpcEndpointAssociationArn").cloned().unwrap_or(Value::Null),
                "VpcEndpointId": p.get("VpcEndpointId").cloned().unwrap_or(Value::Null),
                "FlowOperationId": op_id,
                "FlowOperationType": op_type,
                "FlowOperationStatus": "COMPLETED",
                "FlowRequestTimestamp": now_epoch(),
                "FlowFilters": p.get("FlowFilters").cloned().unwrap_or(json!([])),
                "MinimumFlowAgeInSeconds": p.get("MinimumFlowAgeInSeconds").cloned().unwrap_or(Value::Null),
            });
            st.flow_operations.push(rec.clone());
            ok(&json!({
                "FirewallArn": fire_arn,
                "FlowOperationId": op_id,
                "FlowOperationStatus": "COMPLETED",
            }))
        }
        "DescribeFlowOperation" => {
            let fire_arn = p.get("FirewallArn").and_then(Value::as_str).unwrap_or("");
            let op_id = p
                .get("FlowOperationId")
                .and_then(Value::as_str)
                .unwrap_or("");
            match st.find_flow_operation(fire_arn, op_id) {
                Some(r) => ok(r),
                None => not_found(&format!("FlowOperation {op_id} not found")),
            }
        }
        "ListFlowOperations" => {
            let fire_arn = p.get("FirewallArn").and_then(Value::as_str).unwrap_or("");
            if st
                .firewalls
                .iter()
                .all(|f| f["FirewallArn"].as_str() != Some(fire_arn))
            {
                return not_found(&format!("Firewall {fire_arn} not found"));
            }
            let ops: Vec<Value> = st
                .flow_operations
                .iter()
                .filter(|r| r["FirewallArn"].as_str() == Some(fire_arn))
                .map(|r| {
                    json!({
                        "FlowOperationId": r["FlowOperationId"],
                        "FlowOperationType": r["FlowOperationType"],
                        "FlowRequestTimestamp": r["FlowRequestTimestamp"],
                        "FlowOperationStatus": r["FlowOperationStatus"],
                    })
                })
                .collect();
            ok(&json!({ "FlowOperations": ops, "NextToken": next_token_of(p) }))
        }
        "ListFlowOperationResults" => {
            let fire_arn = p.get("FirewallArn").and_then(Value::as_str).unwrap_or("");
            let op_id = p
                .get("FlowOperationId")
                .and_then(Value::as_str)
                .unwrap_or("");
            match st.find_flow_operation(fire_arn, op_id) {
                Some(r) => ok(&json!({
                    "FirewallArn": r["FirewallArn"],
                    "AvailabilityZone": r["AvailabilityZone"],
                    "VpcEndpointAssociationArn": r["VpcEndpointAssociationArn"],
                    "VpcEndpointId": r["VpcEndpointId"],
                    "FlowOperationId": r["FlowOperationId"],
                    "FlowOperationStatus": r["FlowOperationStatus"],
                    "FlowRequestTimestamp": r["FlowRequestTimestamp"],
                    "Flows": [],
                    "NextToken": next_token_of(p),
                })),
                None => not_found(&format!("FlowOperation {op_id} not found")),
            }
        }
        "StartAnalysisReport" => {
            let fw = match st.find(arn_param, name) {
                Some(f) => f.clone(),
                None => return not_found(&format!("Firewall {name} not found")),
            };
            let fire_arn = fw.get("FirewallArn").cloned().unwrap_or(Value::Null);
            let analysis_type = p
                .get("AnalysisType")
                .and_then(Value::as_str)
                .unwrap_or("TLS_SNI");
            let report_id = uuid_id();
            let rec = json!({
                "FirewallArn": fire_arn,
                "AnalysisReportId": report_id,
                "AnalysisType": analysis_type,
                "Status": "COMPLETED",
                "StartTime": now_epoch(),
                "ReportTime": now_epoch(),
            });
            st.analysis_reports.push(rec.clone());
            ok(&json!({ "AnalysisReportId": report_id }))
        }
        "GetAnalysisReportResults" => {
            let (fire_arn, found) = fw_arn_for_reports(p, st);
            let report_id = p
                .get("AnalysisReportId")
                .and_then(Value::as_str)
                .unwrap_or("");
            if !found {
                return not_found(&format!("Firewall with Arn {fire_arn} could not be found"));
            }
            match st.find_report(&fire_arn, report_id) {
                Some(r) => ok(&json!({
                    "Status": r["Status"],
                    "StartTime": r["StartTime"],
                    "ReportTime": r["ReportTime"],
                    "AnalysisType": r["AnalysisType"],
                    "AnalysisReportResults": [],
                    "NextToken": Value::Null,
                })),
                None => not_found(&format!("Analysis report {report_id} not found")),
            }
        }
        "ListAnalysisReports" => {
            let fire_arn = p.get("FirewallArn").and_then(Value::as_str).unwrap_or("");
            let fire_name = p.get("FirewallName").and_then(Value::as_str).unwrap_or("");
            let resolved = if fire_arn.is_empty() {
                st.find("", fire_name)
                    .map(|f| f["FirewallArn"].as_str().unwrap_or("").to_string())
            } else {
                Some(fire_arn.to_string())
            };
            let Some(fire_arn) = resolved else {
                return not_found(&format!("Firewall {fire_name} not found"));
            };
            let reports: Vec<Value> = st
                .analysis_reports
                .iter()
                .filter(|r| r["FirewallArn"].as_str() == Some(fire_arn.as_str()))
                .map(|r| {
                    json!({
                        "AnalysisReportId": r["AnalysisReportId"],
                        "AnalysisType": r["AnalysisType"],
                        "ReportTime": r["ReportTime"],
                        "Status": r["Status"],
                    })
                })
                .collect();
            ok(&json!({ "AnalysisReports": reports, "NextToken": next_token_of(p) }))
        }
        // ------------------------------------------------------------------
        // Resource policy (modeled per botocore; arn-keyed, like tags).
        // ------------------------------------------------------------------
        "PutResourcePolicy" => {
            let arn = p.get("ResourceArn").and_then(Value::as_str).unwrap_or("");
            let policy = p.get("Policy").and_then(Value::as_str).unwrap_or_default();
            match st.resource_policies.iter_mut().find(|(a, _)| a == arn) {
                Some((_, stored)) => *stored = policy.to_string(),
                None => st
                    .resource_policies
                    .push((arn.to_string(), policy.to_string())),
            }
            ok(&json!({}))
        }
        "DescribeResourcePolicy" => {
            let arn = p.get("ResourceArn").and_then(Value::as_str).unwrap_or("");
            match st.resource_policies.iter().find(|(a, _)| a == arn) {
                Some((_, policy)) => ok(&json!({ "Policy": policy })),
                None => not_found(&format!("Resource policy {arn} not found")),
            }
        }
        "DeleteResourcePolicy" => {
            let arn = p.get("ResourceArn").and_then(Value::as_str).unwrap_or("");
            let before = st.resource_policies.len();
            st.resource_policies.retain(|(a, _)| a != arn);
            if st.resource_policies.len() == before {
                return not_found(&format!("Resource policy {arn} not found"));
            }
            ok(&json!({}))
        }
        // Transit gateway attachment decisions (modeled: unknown ids are
        // RFNF; known rows transition their stored status).
        "AcceptNetworkFirewallTransitGatewayAttachment"
        | "RejectNetworkFirewallTransitGatewayAttachment"
        | "DeleteNetworkFirewallTransitGatewayAttachment" => {
            let tg = p
                .get("TransitGatewayAttachmentId")
                .and_then(Value::as_str)
                .unwrap_or("");
            if !st.transit_gateways.iter().any(|(a, _)| a == tg) {
                return not_found(&format!("TransitGatewayAttachment {tg} not found"));
            }
            let new_status = match req.operation.as_str() {
                "AcceptNetworkFirewallTransitGatewayAttachment" => "READY",
                "RejectNetworkFirewallTransitGatewayAttachment" => "REJECTED",
                _ => "DELETED",
            };
            if let Some((_, status)) = st.transit_gateways.iter_mut().find(|(a, _)| a == tg) {
                *status = new_status.to_string();
            }
            ok(&json!({
                "TransitGatewayAttachmentId": tg,
                "TransitGatewayAttachmentStatus": new_status,
            }))
        }
        _ => {
            let e = json!({
                "__type": "NotImplemented",
                "message": format!("The {} action has not been implemented", req.operation),
            });
            Response::json(501, serde_json::to_vec(&e).unwrap_or_default())
        }
    }
}

fn last_logging(st: &State, arn: &str) -> Value {
    st.logging
        .iter()
        .find(|(a, _)| a == arn)
        .map(|(_, v)| v.clone())
        .unwrap_or(json!({}))
}

// ---------------------------------------------------------------------------
// family render helpers (modeled per botocore output shapes)
// ---------------------------------------------------------------------------

fn rule_name(p: &Value) -> String {
    p.get("RuleGroupName")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn rule_arn(p: &Value) -> String {
    p.get("RuleGroupArn")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn tls_arn(p: &Value) -> String {
    p.get("TLSInspectionConfigurationArn")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn tls_name(p: &Value) -> String {
    p.get("TLSInspectionConfigurationName")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn proxy_arn(p: &Value) -> String {
    p.get("ProxyArn")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn proxy_name(p: &Value) -> String {
    p.get("ProxyName")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn nat_id(p: &Value) -> String {
    p.get("NatGatewayId")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn pc_arn(p: &Value) -> String {
    p.get("ProxyConfigurationArn")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn pc_name(p: &Value) -> String {
    p.get("ProxyConfigurationName")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn prg_arn(p: &Value) -> String {
    p.get("ProxyRuleGroupArn")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn prg_name(p: &Value) -> String {
    p.get("ProxyRuleGroupName")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn fw_arn_for_reports(p: &Value, st: &State) -> (String, bool) {
    let arn = p.get("FirewallArn").and_then(Value::as_str).unwrap_or("");
    if !arn.is_empty() {
        let exists = st
            .firewalls
            .iter()
            .any(|f| f["FirewallArn"].as_str() == Some(arn));
        return (arn.to_string(), exists);
    }
    let name = p.get("FirewallName").and_then(Value::as_str).unwrap_or("");
    match st.find("", name) {
        Some(fw) => (
            fw.get("FirewallArn")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            true,
        ),
        None => (String::new(), false),
    }
}

fn policy_name(p: &Value) -> &str {
    p.get("FirewallPolicyName")
        .and_then(Value::as_str)
        .unwrap_or("")
}

fn policy_arn(p: &Value) -> &str {
    p.get("FirewallPolicyArn")
        .and_then(Value::as_str)
        .unwrap_or("")
}

/// The response slice of a stored firewall policy record (drops the
/// policy body fields; renders the botocore FirewallPolicyResponse shape).
fn policy_response(pl: &Value, st: &State) -> Value {
    let arn = pl
        .get("FirewallPolicyArn")
        .and_then(Value::as_str)
        .unwrap_or("");
    json!({
        "FirewallPolicyName": pl["FirewallPolicyName"].clone(),
        "FirewallPolicyArn": pl["FirewallPolicyArn"].clone(),
        "FirewallPolicyId": pl.get("FirewallPolicyId").cloned().unwrap_or(json!("")),
        "FirewallPolicyStatus": pl.get("FirewallPolicyStatus").cloned().unwrap_or(json!("ACTIVE")),
        "Description": pl.get("Description").cloned().unwrap_or(Value::Null),
        "Tags": tag_map(st, arn),
        "NumberOfAssociations": 0,
        "LastModifiedTime": pl.get("LastModifiedTime").cloned().unwrap_or(Value::Null),
    })
}

/// The response slice of a stored RuleGroup record (drops private members).
fn rule_group_response(rg: &Value, st: &State) -> Value {
    let arn = rg.get("RuleGroupArn").and_then(Value::as_str).unwrap_or("");
    json!({
        "RuleGroupArn": rg["RuleGroupArn"].clone(),
        "RuleGroupName": rg["RuleGroupName"].clone(),
        "RuleGroupId": rg["RuleGroupId"].clone(),
        "Description": rg["Description"].clone(),
        "Type": rg["Type"].clone(),
        "Capacity": rg["Capacity"].clone(),
        "RuleGroupStatus": rg["RuleGroupStatus"].clone(),
        "Tags": tag_map(st, arn),
        "NumberOfAssociations": 0,
        "LastModifiedTime": rg["LastModifiedTime"].clone(),
    })
}

/// The stored RuleGroup body as the DescribeRuleGroup wire shape: either the
/// full RuleGroup structure the caller supplied, or a RulesString wrapper.
fn rule_group_body(rg: &Value) -> Value {
    let body = &rg["rule_group_body"];
    if body.is_object() && !body.as_object().unwrap().is_empty() {
        return body.clone();
    }
    let rules = rg["rules_string"].as_str().unwrap_or_default();
    if rules.is_empty() {
        json!({ "RulesSource": { "RulesString": "" } })
    } else {
        json!({ "RulesSource": { "RulesString": rules } })
    }
}

/// The response slice of a stored TLS inspection configuration.
fn tls_response(t: &Value, st: &State) -> Value {
    let arn = t
        .get("TLSInspectionConfigurationArn")
        .and_then(Value::as_str)
        .unwrap_or("");
    json!({
        "TLSInspectionConfigurationArn": t["TLSInspectionConfigurationArn"].clone(),
        "TLSInspectionConfigurationName": t["TLSInspectionConfigurationName"].clone(),
        "TLSInspectionConfigurationId": t["TLSInspectionConfigurationId"].clone(),
        "TLSInspectionConfigurationStatus": t["TLSInspectionConfigurationStatus"].clone(),
        "Description": t["Description"].clone(),
        "Tags": tag_map(st, arn),
        "LastModifiedTime": t["LastModifiedTime"].clone(),
        "NumberOfAssociations": t["NumberOfAssociations"].clone(),
    })
}

/// The association wire shape (drops the private `creation` stamp).
fn stored_vpc_assoc(a: &Value, _st: &State) -> Value {
    let mut out = a.clone();
    if let Some(obj) = out.as_object_mut() {
        obj.remove("creation");
    }
    out
}

/// List paging stays trivially single-page across modeled families: every
/// row is returned and no token is minted.
fn next_token_of(_p: &Value) -> Value {
    json!(null)
}

const PROXY_PHASES: [&str; 3] = ["PreDNS", "PreREQUEST", "PostRESPONSE"];

/// First proxy rule named `name` across the group's request phases.
fn find_proxy_rule<'a>(group: &'a Value, name: &str) -> Option<&'a Value> {
    for phase in PROXY_PHASES {
        if let Some(list) = group["Rules"].get(phase).and_then(Value::as_array) {
            if let Some(rule) = list
                .iter()
                .find(|r| r.get("ProxyRuleName").and_then(Value::as_str) == Some(name))
            {
                return Some(rule);
            }
        }
    }
    None
}

fn find_proxy_rule_mut<'a>(group: &'a mut Value, name: &str) -> Option<&'a mut Value> {
    // Pass 1 reads out where the rule lives; pass 2 walks the index mutably
    // (one borrow at a time).
    let (phase, item) = {
        let rules_ref = group.get("Rules")?;
        let mut hit = None;
        for phase in PROXY_PHASES {
            if let Some(list) = rules_ref.get(phase).and_then(Value::as_array) {
                if let Some(i) = list
                    .iter()
                    .position(|r| r.get("ProxyRuleName").and_then(Value::as_str) == Some(name))
                {
                    hit = Some((phase, i));
                    break;
                }
            }
        }
        hit?
    };
    let rules = group.get_mut("Rules")?;
    let list = rules.get_mut(phase).and_then(Value::as_array_mut)?;
    list.get_mut(item)
}

fn tag_keys(p: &Value) -> Vec<String> {
    p.get("TagKeys")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

fn tag_map(st: &State, arn: &str) -> Vec<Value> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(op: &str, p: Value) -> Request {
        Request {
            service: "networkfirewall".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: Default::default(),
            body: bytes::Bytes::from(serde_json::to_vec(&p).unwrap_or_default()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn call(b: &mut State, op: &str, p: Value) -> (u16, Value) {
        let r = req(op, p);
        let parsed = serde_json::from_slice(&r.body).unwrap();
        let resp = handle(&r, b, &parsed);
        let v = serde_json::from_slice::<Value>(&resp.body).unwrap_or(Value::Null);
        (resp.status, v)
    }

    fn seed(b: &mut State) {
        b.firewalls.clear();
        b.tags.clear();
        b.logging.clear();
        b.policies.clear();
        b.rule_groups.clear();
        b.tls_configs.clear();
        b.vpc_endpoint_associations.clear();
        b.proxies.clear();
        b.proxy_configurations.clear();
        b.proxy_rule_groups.clear();
        b.transit_gateways.clear();
        b.flow_operations.clear();
        b.analysis_reports.clear();
        b.resource_policies.clear();
    }

    #[test]
    fn firewall_crud() {
        let slot = state();
        let mut guard = slot.write();
        let b = guard.entry((123456789012, "us-east-1".into())).or_default();
        seed(b);
        let create = req(
            "CreateFirewall",
            json!({
                "FirewallName": "fw-test",
                "FirewallPolicyArn": "arn:aws:network-firewall:us-east-1:123456789012:firewall-policy/t",
                "SubnetMappings": [{"SubnetId": "subnet-12345"}],
                "VpcId": "vpc-12345",
            }),
        );
        let p = serde_json::from_slice(&create.body).unwrap();
        let resp = handle(&create, b, &p);
        let v = serde_json::from_slice::<Value>(&resp.body).unwrap();
        assert_eq!(v["Firewall"]["FirewallName"], "fw-test");
        // moto defaults all three protection flags to true when omitted.
        assert_eq!(v["Firewall"]["DeleteProtection"], true);
        assert_eq!(v["Firewall"]["SubnetChangeProtection"], true);
        assert_eq!(v["Firewall"]["FirewallPolicyChangeProtection"], true);
        assert_eq!(v["FirewallStatus"]["Status"], "READY");

        // An explicit false still wins over the default.
        let explicit = req(
            "CreateFirewall",
            json!({
                "FirewallName": "fw-explicit",
                "FirewallPolicyArn": "arn:aws:network-firewall:us-east-1:123456789012:firewall-policy/t",
                "SubnetMappings": [{"SubnetId": "subnet-12345"}],
                "VpcId": "vpc-12345",
                "DeleteProtection": false,
            }),
        );
        let p = serde_json::from_slice(&explicit.body).unwrap();
        let v = serde_json::from_slice::<Value>(&handle(&explicit, b, &p).body).unwrap();
        assert_eq!(v["Firewall"]["DeleteProtection"], false);
        assert_eq!(v["Firewall"]["SubnetChangeProtection"], true);

        let describe = req("DescribeFirewall", json!({"FirewallName": "fw-test"}));
        let p = serde_json::from_slice(&describe.body).unwrap();
        let v = serde_json::from_slice::<Value>(&handle(&describe, b, &p).body).unwrap();
        assert_eq!(v["Firewall"]["VpcId"], "vpc-12345");
        assert!(v["UpdateToken"].as_str().is_some());

        let list = req("ListFirewalls", json!({}));
        let p = serde_json::from_slice(&list.body).unwrap();
        let v = serde_json::from_slice::<Value>(&handle(&list, b, &p).body).unwrap();
        assert_eq!(v["Firewalls"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn rule_group_crud() {
        let slot = state();
        let mut guard = slot.write();
        let b = guard.entry((123456789013, "us-east-1".into())).or_default();
        seed(b);
        let (status, v) = call(
            b,
            "CreateRuleGroup",
            json!({
                "RuleGroupName": "rg-crud",
                "Rules": "alert tcp 10.0.0.0/8 any -> 10.0.0.0/8 80",
                "Type": "STATEFUL",
                "Capacity": 10,
                "Tags": [{"Key": "k", "Value": "v"}],
            }),
        );
        assert_eq!(status, 200);
        let arn = v["RuleGroupResponse"]["RuleGroupArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(v["RuleGroupResponse"]["RuleGroupName"], "rg-crud");
        assert_eq!(v["RuleGroupResponse"]["RuleGroupStatus"], "ACTIVE");
        // moto/AWS wire shape: rulegroup/<name>, no |uuid suffix.
        assert!(arn.ends_with(":rulegroup/rg-crud"), "{arn}");
        assert!(!arn.contains('|'), "{arn}");
        assert!(v["UpdateToken"].is_string());
        // The composite name|uuid survives as the internal state key only.
        let rg_id = v["RuleGroupResponse"]["RuleGroupId"].as_str().unwrap();
        assert_eq!(
            b.rule_groups[0]["rule_group_key"].as_str().unwrap(),
            format!("rg-crud|{rg_id}")
        );

        let (status, v) = call(b, "DescribeRuleGroup", json!({"RuleGroupName": "rg-crud"}));
        assert_eq!(status, 200);
        assert!(v["RuleGroup"]["RulesSource"]["RulesString"]
            .as_str()
            .unwrap()
            .contains("alert"));
        assert_eq!(v["RuleGroupResponse"]["RuleGroupName"], "rg-crud");

        let (status, v) = call(
            b,
            "UpdateRuleGroup",
            json!({
                "RuleGroupName": "rg-crud",
                "Rules": "alert udp 10.0.0.0/8 any -> 10.0.0.0/8 53",
                "UpdateToken": "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d",
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(v["RuleGroupResponse"]["RuleGroupName"], "rg-crud");
        let (_, v) = call(b, "DescribeRuleGroup", json!({"RuleGroupName": "rg-crud"}));
        assert!(v["RuleGroup"]["RulesSource"]["RulesString"]
            .as_str()
            .unwrap()
            .contains("udp"));

        let (status, v) = call(b, "ListRuleGroups", json!({"Type": "STATEFUL"}));
        assert_eq!(status, 200);
        assert_eq!(v["RuleGroups"].as_array().unwrap().len(), 1);
        let (status, _) = call(b, "ListRuleGroups", json!({"Type": "STATELESS"}));
        assert_eq!(status, 200);
        let (_, v) = call(b, "ListRuleGroups", json!({"Type": "STATELESS"}));
        assert_eq!(v["RuleGroups"].as_array().unwrap().len(), 0);

        let (_, v) = call(
            b,
            "DescribeRuleGroupMetadata",
            json!({"RuleGroupName": "rg-crud"}),
        );
        assert_eq!(v["Type"], "STATEFUL");
        assert_eq!(v["Capacity"], 10);
        let (_, v) = call(
            b,
            "DescribeRuleGroupSummary",
            json!({"RuleGroupName": "rg-crud"}),
        );
        assert!(v["Summary"]["RuleSummaries"].is_array());

        let (status, v) = call(b, "DeleteRuleGroup", json!({"RuleGroupName": "rg-crud"}));
        assert_eq!(status, 200);
        assert_eq!(v["RuleGroupResponse"]["RuleGroupStatus"], "DELETING");
        let (_, v) = call(b, "DescribeRuleGroup", json!({"RuleGroupName": "rg-crud"}));
        assert_eq!(v["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn rule_group_create_validation() {
        let slot = state();
        let mut guard = slot.write();
        let b = guard.entry((123456789014, "us-east-1".into())).or_default();
        seed(b);
        // Real AWS: exactly one of Rules / RuleGroup must be present.
        let (status, _) = call(
            b,
            "CreateRuleGroup",
            json!({"RuleGroupName": "rg-both", "Rules": "a", "RuleGroup": {"RulesSource": {"RulesString": "b"}}, "Capacity": 1, "Type": "STATEFUL"}),
        );
        assert_eq!(status, 400);
        let (status, v) = call(
            b,
            "CreateRuleGroup",
            json!({"RuleGroupName": "rg-neither", "Capacity": 1, "Type": "STATEFUL"}),
        );
        assert_eq!(status, 400);
        assert_eq!(v["__type"], "InvalidRequestException");
    }

    #[test]
    fn tls_inspection_crud() {
        let slot = state();
        let mut guard = slot.write();
        let b = guard.entry((123456789015, "us-east-1".into())).or_default();
        seed(b);
        let (status, v) = call(
            b,
            "CreateTLSInspectionConfiguration",
            json!({
                "TLSInspectionConfigurationName": "tls-crud",
                "TLSInspectionConfiguration": {
                    "ServerCertificateConfigurations": [
                        {"ServerCertificates": [{"ServerCertificateArn": "arn:aws:acm:us-east-1:123456789012:certificate/abc"}]}
                    ]
                },
                "Description": "test config",
            }),
        );
        assert_eq!(status, 200);
        let arn = v["TLSInspectionConfigurationResponse"]["TLSInspectionConfigurationArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(arn.contains("tls-inspection-configuration/tls-crud"));
        assert_eq!(
            v["TLSInspectionConfigurationResponse"]["TLSInspectionConfigurationStatus"],
            "ACTIVE"
        );

        let (_, v) = call(
            b,
            "DescribeTLSInspectionConfiguration",
            json!({"TLSInspectionConfigurationName": "tls-crud"}),
        );
        assert_eq!(
            v["TLSInspectionConfiguration"]["ServerCertificateConfigurations"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        let (status, _) = call(
            b,
            "UpdateTLSInspectionConfiguration",
            json!({
                "TLSInspectionConfigurationName": "tls-crud",
                "TLSInspectionConfiguration": {"ServerCertificateConfigurations": []},
                "UpdateToken": "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d",
            }),
        );
        assert_eq!(status, 200);

        let (_, v) = call(b, "ListTLSInspectionConfigurations", json!({}));
        assert_eq!(
            v["TLSInspectionConfigurations"].as_array().unwrap().len(),
            1
        );

        let (_, v) = call(
            b,
            "DeleteTLSInspectionConfiguration",
            json!({"TLSInspectionConfigurationName": "tls-crud"}),
        );
        assert_eq!(
            v["TLSInspectionConfigurationResponse"]["TLSInspectionConfigurationStatus"],
            "DELETING"
        );
        let (_, v) = call(
            b,
            "DescribeTLSInspectionConfiguration",
            json!({"TLSInspectionConfigurationName": "tls-crud"}),
        );
        assert_eq!(v["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn vpc_association_crud() {
        let slot = state();
        let mut guard = slot.write();
        let b = guard.entry((123456789016, "us-east-1".into())).or_default();
        seed(b);
        let (_, fw) = call(
            b,
            "CreateFirewall",
            json!({
                "FirewallName": "fw-vpcea",
                "FirewallPolicyArn": "arn:aws:network-firewall:us-east-1:123456789012:firewall-policy/t",
                "VpcId": "vpc-1",
            }),
        );
        let fire_arn = fw["Firewall"]["FirewallArn"].as_str().unwrap().to_string();

        let (status, v) = call(
            b,
            "CreateVpcEndpointAssociation",
            json!({
                "FirewallArn": fire_arn.clone(),
                "VpcId": "vpc-2",
                "SubnetMapping": {"SubnetId": "subnet-999"},
            }),
        );
        assert_eq!(status, 200);
        let assoc_arn = v["VpcEndpointAssociation"]["VpcEndpointAssociationArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(assoc_arn.contains("vpc-endpoint-association/"));
        assert_eq!(v["VpcEndpointAssociationStatus"]["Status"], "READY");

        // Unknown firewall -> RFNF (modeled).
        let (status, _) = call(
            b,
            "CreateVpcEndpointAssociation",
            json!({
                "FirewallArn": "arn:aws:network-firewall:us-east-1:123456789012:firewall/noperes",
                "VpcId": "vpc-3",
                "SubnetMapping": {"SubnetId": "subnet-888"},
            }),
        );
        assert_eq!(status, 400);

        let (_, v) = call(
            b,
            "ListVpcEndpointAssociations",
            json!({"FirewallArn": fire_arn}),
        );
        assert_eq!(v["VpcEndpointAssociations"].as_array().unwrap().len(), 1);

        let (_, v) = call(
            b,
            "DescribeVpcEndpointAssociation",
            json!({"VpcEndpointAssociationArn": assoc_arn}),
        );
        assert_eq!(v["VpcEndpointAssociation"]["VpcId"], "vpc-2");

        let (_, v) = call(
            b,
            "DeleteVpcEndpointAssociation",
            json!({"VpcEndpointAssociationArn": assoc_arn}),
        );
        assert_eq!(v["VpcEndpointAssociationStatus"]["Status"], "DELETING");
        let (status, v) = call(
            b,
            "DescribeVpcEndpointAssociation",
            json!({"VpcEndpointAssociationArn": assoc_arn}),
        );
        assert_eq!(status, 400);
        assert_eq!(v["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn proxy_lifecycle() {
        let slot = state();
        let mut guard = slot.write();
        let b = guard.entry((123456789017, "us-east-1".into())).or_default();
        seed(b);
        let (status, v) = call(
            b,
            "CreateProxy",
            json!({
                "ProxyName": "pr-lc",
                "NatGatewayId": "nat-1234",
                "ListenerProperties": [{"Port": 8443, "Type": "HTTPS"}],
                "TlsInterceptProperties": {"TlsInterceptMode": "ENABLED"},
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(v["Proxy"]["ProxyName"], "pr-lc");
        assert_eq!(v["Proxy"]["ProxyState"], "ATTACHED");

        let (_, v) = call(b, "DescribeProxy", json!({"ProxyName": "pr-lc"}));
        assert_eq!(v["Proxy"]["NatGatewayId"], "nat-1234");

        let (status, v) = call(
            b,
            "UpdateProxy",
            json!({
                "NatGatewayId": "nat-1234",
                "ListenerPropertiesToAdd": [{"Port": 8080, "Type": "HTTP"}],
                "UpdateToken": "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d",
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(
            v["Proxy"]["ListenerProperties"].as_array().unwrap().len(),
            2
        );

        let (_, v) = call(b, "ListProxies", json!({}));
        assert_eq!(v["Proxies"].as_array().unwrap().len(), 1);

        let (_, v) = call(b, "DeleteProxy", json!({"NatGatewayId": "nat-1234"}));
        assert!(v["ProxyArn"].as_str().unwrap().contains("proxy/pr-lc"));
        let (status, _) = call(b, "DescribeProxy", json!({"ProxyName": "pr-lc"}));
        assert_eq!(status, 400);
    }

    #[test]
    fn proxy_configuration_and_rule_groups() {
        let slot = state();
        let mut guard = slot.write();
        let b = guard.entry((123456789018, "us-east-1".into())).or_default();
        seed(b);

        let (_, g1) = call(
            b,
            "CreateProxyRuleGroup",
            json!({
                "ProxyRuleGroupName": "prg-a",
                "Rules": {"PreDNS": [{"ProxyRuleName": "r1", "Action": "DENY"}]},
            }),
        );
        assert_eq!(g1["ProxyRuleGroup"]["ProxyRuleGroupName"], "prg-a");
        let g_arn = g1["ProxyRuleGroup"]["ProxyRuleGroupArn"]
            .as_str()
            .unwrap()
            .to_string();

        let (status, _) = call(
            b,
            "CreateProxyRules",
            json!({
                "ProxyRuleGroupArn": g_arn.clone(),
                "Rules": {"PreDNS": [{"ProxyRuleName": "r2", "Action": "ALLOW"}]},
            }),
        );
        assert_eq!(status, 200);
        let (_, v) = call(
            b,
            "DescribeProxyRuleGroup",
            json!({"ProxyRuleGroupName": "prg-a"}),
        );
        assert_eq!(
            v["ProxyRuleGroup"]["Rules"]["PreDNS"]
                .as_array()
                .unwrap()
                .len(),
            2
        );

        let (status, v) = call(
            b,
            "DescribeProxyRule",
            json!({"ProxyRuleGroupArn": g_arn.clone(), "ProxyRuleName": "r1"}),
        );
        assert_eq!(status, 200);
        assert_eq!(v["ProxyRule"]["Action"], "DENY");

        // Unknown referenced rule group -> RFNF.
        let (status, _) = call(
            b,
            "CreateProxyConfiguration",
            json!({
                "ProxyConfigurationName": "pc-1",
                "RuleGroupNames": ["no-such-group"],
                "DefaultRulePhaseActions": {"PreDNS": "ALLOW"},
            }),
        );
        assert_eq!(status, 400);

        let (_, c) = call(
            b,
            "CreateProxyConfiguration",
            json!({
                "ProxyConfigurationName": "pc-1",
                "RuleGroupNames": ["prg-a"],
                "DefaultRulePhaseActions": {"PreDNS": "ALLOW"},
            }),
        );
        assert_eq!(
            c["ProxyConfiguration"]["RuleGroups"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let c_arn = c["ProxyConfiguration"]["ProxyConfigurationArn"]
            .as_str()
            .unwrap()
            .to_string();

        let (status, _) = call(
            b,
            "UpdateProxyConfiguration",
            json!({
                "ProxyConfigurationArn": c_arn,
                "DefaultRulePhaseActions": {"PreDNS": "DENY"},
                "UpdateToken": "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d",
            }),
        );
        assert_eq!(status, 200);

        let (status, _) = call(
            b,
            "UpdateProxyRule",
            json!({
                "ProxyRuleGroupArn": g_arn.clone(),
                "ProxyRuleName": "r1",
                "Action": "ALERT",
                "AddConditions": [{"ConditionKey": "t", "ConditionOperator": "eq", "ConditionValues": ["example.com"]}],
                "UpdateToken": "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d",
            }),
        );
        assert_eq!(status, 200);
        let (_, v) = call(
            b,
            "DescribeProxyRule",
            json!({"ProxyRuleGroupArn": g_arn, "ProxyRuleName": "r1"}),
        );
        assert_eq!(v["ProxyRule"]["Action"], "ALERT");
        assert_eq!(v["ProxyRule"]["Conditions"].as_array().unwrap().len(), 1);

        let (_, g2) = call(
            b,
            "CreateProxyRuleGroup",
            json!({"ProxyRuleGroupName": "prg-b"}),
        );
        assert_eq!(g2["ProxyRuleGroup"]["ProxyRuleGroupName"], "prg-b");
        let (status, _) = call(
            b,
            "AttachRuleGroupsToProxyConfiguration",
            json!({
                "ProxyConfigurationName": "pc-1",
                "RuleGroups": [{"ProxyRuleGroupName": "prg-b"}],
                "UpdateToken": "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d",
            }),
        );
        assert_eq!(status, 200);
        let (status, _) = call(
            b,
            "AttachRuleGroupsToProxyConfiguration",
            json!({
                "ProxyConfigurationName": "pc-1",
                "RuleGroups": [{"ProxyRuleGroupName": "prg-missing"}],
                "UpdateToken": "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d",
            }),
        );
        assert_eq!(status, 400);

        let (status, v) = call(
            b,
            "UpdateProxyRuleGroupPriorities",
            json!({
                "ProxyConfigurationName": "pc-1",
                "RuleGroups": [{"ProxyRuleGroupName": "prg-b", "NewPosition": 0}],
                "UpdateToken": "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d",
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(
            v["ProxyRuleGroups"].as_array().unwrap()[0]["ProxyRuleGroupName"],
            "prg-b"
        );

        let (status, _) = call(
            b,
            "DetachRuleGroupsFromProxyConfiguration",
            json!({
                "ProxyConfigurationName": "pc-1",
                "RuleGroupNames": ["prg-a"],
                "UpdateToken": "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d",
            }),
        );
        assert_eq!(status, 200);
        let (_, v) = call(
            b,
            "DescribeProxyConfiguration",
            json!({"ProxyConfigurationArn": c_arn}),
        );
        let rows = v["ProxyConfiguration"]["RuleGroups"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["ProxyRuleGroupName"], "prg-b");

        let (status, _) = call(
            b,
            "UpdateProxyRulePriorities",
            json!({
                "ProxyRuleGroupArn": g_arn.clone(),
                "RuleGroupRequestPhase": "PRE_DNS",
                "Rules": [{"ProxyRuleName": "r2", "NewPosition": 0}],
                "UpdateToken": "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d",
            }),
        );
        assert_eq!(status, 200);

        let (status, _) = call(
            b,
            "DeleteProxyRules",
            json!({"ProxyRuleGroupArn": g_arn.clone(), "Rules": ["r2"]}),
        );
        assert_eq!(status, 200);
        let (_, v) = call(
            b,
            "DescribeProxyRuleGroup",
            json!({"ProxyRuleGroupArn": g_arn}),
        );
        assert_eq!(
            v["ProxyRuleGroup"]["Rules"]["PreDNS"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        let (status, _) = call(
            b,
            "DeleteProxyRuleGroup",
            json!({"ProxyRuleGroupName": "prg-a"}),
        );
        assert_eq!(status, 200);
        let (status, _) = call(
            b,
            "DeleteProxyConfiguration",
            json!({"ProxyConfigurationName": "pc-1"}),
        );
        assert_eq!(status, 200);
        let (status, _) = call(
            b,
            "DescribeProxyConfiguration",
            json!({"ProxyConfigurationName": "pc-1"}),
        );
        assert_eq!(status, 400);
    }

    #[test]
    fn firewall_associations_and_updates() {
        let slot = state();
        let mut guard = slot.write();
        let b = guard.entry((123456789019, "us-east-1".into())).or_default();
        seed(b);

        let (_, pl) = call(
            b,
            "CreateFirewallPolicy",
            json!({
                "FirewallPolicyName": "pl-1",
                "FirewallPolicy": {
                    "StatelessDefaultActions": ["aws:forward_to_s3"],
                    "StatelessFragmentDefaultActions": ["aws:forward_to_s3"],
                },
            }),
        );
        let policy_arn = pl["FirewallPolicyResponse"]["FirewallPolicyArn"]
            .as_str()
            .unwrap()
            .to_string();
        let (_, fw) = call(
            b,
            "CreateFirewall",
            json!({
                "FirewallName": "fw-assoc",
                "FirewallPolicyArn": policy_arn.clone(),
                "VpcId": "vpc-a",
                "SubnetMappings": [{"SubnetId": "subnet-1"}],
                "AvailabilityZoneMappings": [{"AvailabilityZone": "us-east-1a"}],
            }),
        );
        assert_eq!(fw["Firewall"]["FirewallName"], "fw-assoc");

        let (status, v) = call(
            b,
            "AssociateFirewallPolicy",
            json!({"FirewallName": "fw-assoc", "FirewallPolicyArn": policy_arn}),
        );
        assert_eq!(status, 200);
        assert_eq!(v["FirewallPolicyArn"], policy_arn);

        let (status, _) = call(
            b,
            "AssociateFirewallPolicy",
            json!({
                "FirewallName": "fw-assoc",
                "FirewallPolicyArn": "arn:aws:network-firewall:us-east-1:123456789012:firewall-policy/ghost",
            }),
        );
        assert_eq!(status, 400);

        let (status, _) = call(
            b,
            "AssociateSubnets",
            json!({
                "FirewallName": "fw-assoc",
                "SubnetMappings": [{"SubnetId": "subnet-2"}, {"SubnetId": "subnet-3"}],
            }),
        );
        assert_eq!(status, 200);
        let (_, v) = call(
            b,
            "DisassociateSubnets",
            json!({"FirewallName": "fw-assoc", "SubnetIds": ["subnet-2"]}),
        );
        let ids: Vec<&str> = v["SubnetMappings"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["SubnetId"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["subnet-3"]);

        let (status, _) = call(
            b,
            "AssociateAvailabilityZones",
            json!({
                "FirewallName": "fw-assoc",
                "AvailabilityZoneMappings": [{"AvailabilityZone": "us-east-1b"}],
            }),
        );
        assert_eq!(status, 200);
        // Associate replaces the AZ mapping set (mirrors AssociateSubnets).
        let (_, v) = call(b, "DescribeFirewall", json!({"FirewallName": "fw-assoc"}));
        assert_eq!(
            v["Firewall"]["AvailabilityZoneMappings"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let (status, _) = call(
            b,
            "DisassociateAvailabilityZones",
            json!({
                "FirewallName": "fw-assoc",
                "AvailabilityZoneMappings": [{"AvailabilityZone": "us-east-1b"}],
            }),
        );
        assert_eq!(status, 200);

        let (status, _) = call(
            b,
            "UpdateAvailabilityZoneChangeProtection",
            json!({"FirewallName": "fw-assoc", "AvailabilityZoneChangeProtection": true}),
        );
        assert_eq!(status, 200);
        let (status, _) = call(
            b,
            "UpdateFirewallDescription",
            json!({"FirewallName": "fw-assoc", "Description": "new text"}),
        );
        assert_eq!(status, 200);
        let (status, _) = call(
            b,
            "UpdateFirewallEncryptionConfiguration",
            json!({
                "FirewallName": "fw-assoc",
                "EncryptionConfiguration": {"Type": "AWS_OWNED_KMS_KEY"},
            }),
        );
        assert_eq!(status, 200);
        let (status, _) = call(
            b,
            "UpdateFirewallAnalysisSettings",
            json!({"FirewallName": "fw-assoc", "EnabledAnalysisTypes": ["TLS_SNI"]}),
        );
        assert_eq!(status, 200);
        let (_, v) = call(b, "DescribeFirewall", json!({"FirewallName": "fw-assoc"}));
        assert_eq!(v["Firewall"]["AvailabilityZoneChangeProtection"], true);
        assert_eq!(v["Firewall"]["Description"], "new text");
        assert_eq!(
            v["Firewall"]["EnabledAnalysisTypes"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            v["Firewall"]["EncryptionConfiguration"]["Type"],
            "AWS_OWNED_KMS_KEY"
        );

        let (_, v) = call(
            b,
            "DescribeFirewallMetadata",
            json!({"FirewallName": "fw-assoc"}),
        );
        assert_eq!(v["Status"], "READY");
        assert!(v["SupportedAvailabilityZones"].is_object());

        let (status, _) = call(
            b,
            "UpdateFirewallDeleteProtection",
            json!({"FirewallName": "missing-fw", "DeleteProtection": true}),
        );
        assert_eq!(status, 400);
    }

    #[test]
    fn flow_reports_and_transit_gateways() {
        let slot = state();
        let mut guard = slot.write();
        let b = guard.entry((123456789020, "us-east-1".into())).or_default();
        seed(b);
        let (_, fw) = call(
            b,
            "CreateFirewall",
            json!({
                "FirewallName": "fw-flow",
                "FirewallPolicyArn": "arn:aws:network-firewall:us-east-1:123456789012:firewall-policy/t",
                "VpcId": "vpc-f",
            }),
        );
        let fire_arn = fw["Firewall"]["FirewallArn"].as_str().unwrap().to_string();

        let (status, v) = call(
            b,
            "StartFlowCapture",
            json!({"FirewallArn": fire_arn.clone(), "FlowFilters": []}),
        );
        assert_eq!(status, 200);
        assert_eq!(v["FlowOperationStatus"], "COMPLETED");
        let op_id = v["FlowOperationId"].as_str().unwrap().to_string();

        let (status, _) = call(
            b,
            "DescribeFlowOperation",
            json!({"FirewallArn": fire_arn.clone(), "FlowOperationId": op_id.clone()}),
        );
        assert_eq!(status, 200);
        let (status, _) = call(
            b,
            "ListFlowOperations",
            json!({"FirewallArn": fire_arn.clone()}),
        );
        assert_eq!(status, 200);
        let (status, _) = call(
            b,
            "ListFlowOperationResults",
            json!({
                "FirewallArn": fire_arn.clone(), "FlowOperationId": op_id.clone(),
            }),
        );
        assert_eq!(status, 200);
        let (_, v) = call(
            b,
            "ListFlowOperations",
            json!({"FirewallArn": fire_arn.clone()}),
        );
        assert_eq!(v["FlowOperations"].as_array().unwrap().len(), 1);
        let (_, v) = call(
            b,
            "ListFlowOperationResults",
            json!({"FirewallArn": fire_arn.clone(), "FlowOperationId": op_id}),
        );
        assert!(v["Flows"].as_array().unwrap().is_empty());

        let (status, _) = call(
            b,
            "StartFlowFlush",
            json!({"FirewallArn": fire_arn.clone(), "FlowFilters": []}),
        );
        assert_eq!(status, 200);

        // Unknown firewall -> RFNF.
        let (status, _) = call(
            b,
            "StartFlowCapture",
            json!({
                "FirewallArn": "arn:aws:network-firewall:us-east-1:123456789012:firewall/nose",
                "FlowFilters": [{"SourceAddress": {"AddressDefinition": "10.0.0.1/32"}}],
            }),
        );
        assert_eq!(status, 400);

        let (status, v) = call(
            b,
            "StartAnalysisReport",
            json!({"FirewallName": "fw-flow", "AnalysisType": "TLS_SNI"}),
        );
        assert_eq!(status, 200);
        let report_id = v["AnalysisReportId"].as_str().unwrap().to_string();

        let (status, _) = call(
            b,
            "GetAnalysisReportResults",
            json!({"FirewallArn": fire_arn.clone(), "AnalysisReportId": report_id.clone()}),
        );
        assert_eq!(status, 200);
        let (status, _) = call(b, "ListAnalysisReports", json!({"FirewallName": "fw-flow"}));
        assert_eq!(status, 200);
        let (_, v) = call(b, "ListAnalysisReports", json!({"FirewallName": "fw-flow"}));
        assert_eq!(v["AnalysisReports"].as_array().unwrap().len(), 1);

        // Transit gateway attachments: unknown id RFNF; known id transitions.
        let (status, _) = call(
            b,
            "AcceptNetworkFirewallTransitGatewayAttachment",
            json!({"TransitGatewayAttachmentId": "tg-attach-1234"}),
        );
        assert_eq!(status, 400);
        b.transit_gateways.push((
            "tg-attach-1234".to_string(),
            "PENDING_ACCEPTANCE".to_string(),
        ));
        let (status, v) = call(
            b,
            "AcceptNetworkFirewallTransitGatewayAttachment",
            json!({"TransitGatewayAttachmentId": "tg-attach-1234"}),
        );
        assert_eq!(status, 200);
        assert_eq!(v["TransitGatewayAttachmentStatus"], "READY");
        let (status, _) = call(
            b,
            "RejectNetworkFirewallTransitGatewayAttachment",
            json!({"TransitGatewayAttachmentId": "tg-attach-1234"}),
        );
        assert_eq!(status, 200);
        let (status, v) = call(
            b,
            "DeleteNetworkFirewallTransitGatewayAttachment",
            json!({"TransitGatewayAttachmentId": "tg-attach-1234"}),
        );
        assert_eq!(status, 200);
        assert_eq!(v["TransitGatewayAttachmentStatus"], "DELETED");

        let (status, _) = call(
            b,
            "GetAnalysisReportResults",
            json!({"FirewallArn": fire_arn, "AnalysisReportId": report_id}),
        );
        assert_eq!(status, 200);
    }

    #[test]
    fn resource_policy_roundtrip() {
        let slot = state();
        let mut guard = slot.write();
        let b = guard.entry((123456789021, "us-east-1".into())).or_default();
        seed(b);
        let arn = "arn:aws:network-firewall:us-east-1:123456789012:stateful-rulegroup/rg-pol";
        let (status, _) = call(
            b,
            "PutResourcePolicy",
            json!({"ResourceArn": arn, "Policy": "{\"Version\":\"2012-10-17\"}"}),
        );
        assert_eq!(status, 200);
        let (status, v) = call(b, "DescribeResourcePolicy", json!({"ResourceArn": arn}));
        assert_eq!(status, 200);
        assert!(v["Policy"].as_str().unwrap().contains("2012-10-17"));
        // Upsert replaces the stored policy.
        call(
            b,
            "PutResourcePolicy",
            json!({"ResourceArn": arn, "Policy": "{\"Version\":\"2023\"}"}),
        );
        let (_, v) = call(b, "DescribeResourcePolicy", json!({"ResourceArn": arn}));
        assert!(v["Policy"].as_str().unwrap().contains("2023"));
        let (status, _) = call(b, "DeleteResourcePolicy", json!({"ResourceArn": arn}));
        assert_eq!(status, 200);
        let (status, _) = call(b, "DeleteResourcePolicy", json!({"ResourceArn": arn}));
        assert_eq!(status, 400);
        let (status, _) = call(b, "DescribeResourcePolicy", json!({"ResourceArn": arn}));
        assert_eq!(status, 400);
    }

    #[test]
    fn list_envelopes_are_shapeful() {
        let slot = state();
        let mut guard = slot.write();
        let b = guard.entry((123456789022, "us-east-1".into())).or_default();
        seed(b);
        for op in [
            "ListProxies",
            "ListProxyConfigurations",
            "ListProxyRuleGroups",
            "ListRuleGroups",
            "ListTLSInspectionConfigurations",
            "ListVpcEndpointAssociations",
        ] {
            let (status, v) = call(b, op, json!({}));
            assert_eq!(status, 200, "{}", op);
            assert!(v.is_object(), "{}: {}", op, v);
        }
    }
}
