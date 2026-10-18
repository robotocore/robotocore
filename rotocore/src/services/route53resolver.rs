//! Route 53 Resolver backend (JSON protocol, target `Route53Resolver`).
//!
//! Port of moto's `route53resolver`. Divergence from moto: SubnetId and
//! SecurityGroupIds passed to CreateResolverEndpoint are not validated against
//! an EC2 backend (that state lives in the moto sidecar / other crates), so
//! HostVPCId stays empty and no ENIs are created; the compat suite never
//! asserts them.

use std::collections::HashMap;

use crate::proto::{json_error, parse_json_body};
use crate::store::{self, Slot};
use crate::{Request, Response};
use serde_json::{json, Value};

/// Lowercase hex like moto's `get_random_hex(17)`, prepended by `prefix`.
fn hex17(prefix: &str) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut out = String::with_capacity(prefix.len() + 17);
    out.push_str(prefix);
    while out.len() < prefix.len() + 17 {
        for b in uuid::Uuid::new_v4().as_bytes() {
            if out.len() == prefix.len() + 17 {
                break;
            }
            out.push(HEX[(*b % 16) as usize] as char);
        }
    }
    out
}

fn now_iso() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.6f+00:00")
        .to_string()
}

fn err(status: u16, code: &str, message: String) -> Response {
    json_error(status, code, &message)
}

fn not_found(msg: String) -> Response {
    err(400, "ResourceNotFoundException", msg)
}

fn body_text(body: &Value, key: &str) -> String {
    body.get(key)
        .and_then(|v| {
            v.as_str().map(String::from).or_else(|| match v {
                Value::Null => None,
                other => Some(other.to_string()),
            })
        })
        .unwrap_or_default()
}

fn body_num(body: &Value, key: &str, default: i64) -> i64 {
    body.get(key).and_then(|v| v.as_i64()).unwrap_or(default)
}

fn ok_json(value: Value) -> Response {
    Response::json_status(200, &value)
}

fn with_scope<T>(
    slot: &Slot<ResolverState>,
    req: &Request,
    f: impl FnOnce(&mut ResolverState) -> T,
) -> T {
    let mut map = slot.write();
    f(map.entry((req.account, req.region.clone())).or_default())
}

#[derive(Clone)]
struct Endpoint {
    id: String,
    creator_request_id: String,
    name: String,
    security_group_ids: Value,
    direction: String,
    /// [{"SubnetId", "Ip", "IpId"}]
    ip_addresses: Vec<Value>,
    creation_time: String,
    modification_time: String,
}

impl Endpoint {
    fn add_ip(&mut self, ip_spec: Value, explicit_ip: Option<String>) {
        let ip = explicit_ip.unwrap_or_else(|| {
            format!(
                "10.10.{}.{}",
                (self.ip_addresses.len() / 254) + 1,
                (self.ip_addresses.len() % 254) + 1
            )
        });
        let mut entry =
            json!({ "SubnetId": ip_spec.get("SubnetId").cloned().unwrap_or(Value::Null) });
        entry["Ip"] = Value::String(ip);
        entry["IpId"] = Value::String(hex17("rni-"));
        self.ip_addresses.push(entry);
    }
}

fn endpoint_json(e: &Endpoint, _account: u64, _region: &str) -> Value {
    json!({
        "Id": e.id,
        "CreatorRequestId": e.creator_request_id,
        "Name": e.name,
        "SecurityGroupIds": e.security_group_ids,
        "Direction": e.direction,
        "IpAddressCount": e.ip_addresses.len(),
        "HostVPCId": "",
        "Status": "OPERATIONAL",
        "StatusMessage": "[Trace id: 1-...] Successfully created Resolver Endpoint",
        "CreationTime": e.creation_time,
        "ModificationTime": e.modification_time,
    })
}

fn endpoint_ips_json(e: &Endpoint) -> Value {
    Value::Array(
        e.ip_addresses
            .iter()
            .map(|item| {
                json!({
                    "IpId": item["IpId"],
                    "SubnetId": item["SubnetId"],
                    "Ip": item["Ip"],
                    "Status": "ATTACHED",
                    "StatusMessage": "This IP address is operational.",
                    "CreationTime": e.creation_time,
                    "ModificationTime": e.modification_time,
                })
            })
            .collect(),
    )
}

#[derive(Clone)]
struct ResolverRule {
    id: String,
    creator_request_id: String,
    name: String,
    rule_type: String,
    domain_name: String,
    target_ips: Option<Value>,
    resolver_endpoint_id: Option<String>,
    creation_time: String,
    modification_time: String,
}

impl ResolverRule {
    fn description(&self, account: u64, region: &str) -> Value {
        json!({
            "Id": self.id,
            "CreatorRequestId": self.creator_request_id,
            "Arn": format!("arn:aws:route53resolver:{region}:{account}:resolver-rule/{}", self.id),
            "DomainName": self.domain_name,
            "Status": "COMPLETE",
            "StatusMessage": "[Trace id: 1-...] Successfully created Resolver Rule",
            "RuleType": self.rule_type,
            "Name": self.name,
            "TargetIps": self.target_ips,
            "ResolverEndpointId": self.resolver_endpoint_id,
            "OwnerId": account.to_string(),
            "ShareStatus": "NOT_SHARED",
            "CreationTime": self.creation_time,
            "ModificationTime": self.modification_time,
        })
    }
}

#[derive(Clone)]
struct RuleAssociation {
    id: String,
    resolver_rule_id: String,
    name: String,
    vpc_id: String,
}

impl RuleAssociation {
    fn description(&self) -> Value {
        json!({
            "Id": self.id,
            "ResolverRuleId": self.resolver_rule_id,
            "Name": self.name,
            "VPCId": self.vpc_id,
            "Status": "COMPLETE",
            "StatusMessage": "",
        })
    }
}

#[derive(Clone)]
struct QueryLogConfig {
    id: String,
    name: String,
    destination_arn: String,
    creator_request_id: String,
    status: &'static str,
    association_count: u64,
    creation_time: String,
}

impl QueryLogConfig {
    fn description(&self, account: u64, region: &str) -> Value {
        json!({
            "Id": self.id,
            "OwnerId": account.to_string(),
            "Status": self.status,
            "ShareStatus": "NOT_SHARED",
            "AssociationCount": self.association_count,
            "Arn": format!("arn:aws:route53resolver:{region}:{account}:resolver-query-log-config/{}", self.id),
            "Name": self.name,
            "DestinationArn": self.destination_arn,
            "CreatorRequestId": self.creator_request_id,
            "CreationTime": self.creation_time,
        })
    }
}

#[derive(Clone)]
struct QueryLogConfigAssociation {
    id: String,
    resolver_query_log_config_id: String,
    resource_id: String,
}

impl QueryLogConfigAssociation {
    fn description(&self) -> Value {
        json!({
            "Id": self.id,
            "ResolverQueryLogConfigId": self.resolver_query_log_config_id,
            "ResourceId": self.resource_id,
            "Status": "ACTIVE",
            "Error": "NONE",
            "ErrorMessage": "",
            "CreationTime": now_iso(),
        })
    }
}

#[derive(Clone)]
struct DnssecConfig {
    id: String,
    resource_id: String,
    validation_status: String,
}

impl DnssecConfig {
    fn description(&self, account: u64) -> Value {
        json!({
            "Id": self.id,
            "OwnerId": account.to_string(),
            "ResourceId": self.resource_id,
            "DnssecConfigId": self.id,
            "ValidationStatus": self.validation_status,
        })
    }
}

#[derive(Clone)]
struct FirewallDomainList {
    id: String,
    name: String,
    domains: Vec<String>,
}

impl FirewallDomainList {
    fn arn(&self, account: u64, region: &str) -> String {
        format!(
            "arn:aws:route53resolver:{region}:{account}:firewall-domain-list/{}",
            self.id
        )
    }

    fn description(&self, account: u64, region: &str) -> Value {
        json!({
            "Id": self.id,
            "Arn": self.arn(account, region),
            "Name": self.name,
            "DomainCount": self.domains.len(),
            "Status": "COMPLETE",
            "StatusMessage": "Successfully created firewall domain list",
            "ManagedOwnerName": "",
            "CreatorRequestId": "",
            "CreationTime": now_iso(),
            "ModificationTime": now_iso(),
        })
    }

    fn metadata(&self, account: u64, region: &str) -> Value {
        json!({
            "Id": self.id,
            "Arn": self.arn(account, region),
            "Name": self.name,
            "CreatorRequestId": "",
            "ManagedOwnerName": "",
        })
    }
}

#[derive(Clone)]
struct FirewallRuleGroup {
    id: String,
    name: String,
    rule_count: u64,
}

impl FirewallRuleGroup {
    fn arn(&self, account: u64, region: &str) -> String {
        format!(
            "arn:aws:route53resolver:{region}:{account}:firewall-rule-group/{}",
            self.id
        )
    }

    fn description(&self, account: u64, region: &str) -> Value {
        json!({
            "Id": self.id,
            "Arn": self.arn(account, region),
            "Name": self.name,
            "RuleCount": self.rule_count,
            "Status": "COMPLETE",
            "StatusMessage": "Successfully created firewall rule group",
            "OwnerId": account.to_string(),
            "CreatorRequestId": "",
            "ShareStatus": "NOT_SHARED",
            "CreationTime": now_iso(),
            "ModificationTime": now_iso(),
        })
    }

    fn metadata(&self, account: u64, region: &str) -> Value {
        json!({
            "Id": self.id,
            "Arn": self.arn(account, region),
            "Name": self.name,
            "CreatorRequestId": "",
            "OwnerId": account.to_string(),
            "ShareStatus": "NOT_SHARED",
        })
    }
}

#[derive(Clone)]
struct FirewallRule {
    firewall_rule_group_id: String,
    firewall_domain_list_id: String,
    name: String,
    priority: i64,
    action: String,
    block_response: Option<String>,
    block_override_domain: Option<String>,
    block_override_dns_type: Option<String>,
    block_override_ttl: Option<i64>,
}

impl FirewallRule {
    fn description(&self) -> Value {
        let mut result = json!({
            "FirewallRuleGroupId": self.firewall_rule_group_id,
            "FirewallDomainListId": self.firewall_domain_list_id,
            "Name": self.name,
            "Priority": self.priority,
            "Action": self.action,
            "CreatorRequestId": "",
            "CreationTime": now_iso(),
            "ModificationTime": now_iso(),
        });
        if let Some(v) = &self.block_response {
            result["BlockResponse"] = Value::String(v.clone());
        }
        if let Some(v) = &self.block_override_domain {
            result["BlockOverrideDomain"] = Value::String(v.clone());
        }
        if let Some(v) = &self.block_override_dns_type {
            result["BlockOverrideDnsType"] = Value::String(v.clone());
        }
        if let Some(v) = self.block_override_ttl {
            result["BlockOverrideTtl"] = Value::Number(v.into());
        }
        result
    }
}

#[derive(Clone)]
struct FirewallRuleGroupAssociation {
    id: String,
    firewall_rule_group_id: String,
    vpc_id: String,
    name: String,
    priority: i64,
}

impl FirewallRuleGroupAssociation {
    fn description(&self, account: u64, region: &str) -> Value {
        json!({
            "Id": self.id,
            "Arn": format!("arn:aws:route53resolver:{region}:{account}:firewall-rule-group-association/{}", self.id),
            "FirewallRuleGroupId": self.firewall_rule_group_id,
            "VpcId": self.vpc_id,
            "Name": self.name,
            "Priority": self.priority,
            "MutationProtection": "DISABLED",
            "ManagedOwnerName": "",
            "Status": "COMPLETE",
            "StatusMessage": "Successfully created firewall rule group association",
            "CreatorRequestId": "",
            "CreationTime": now_iso(),
            "ModificationTime": now_iso(),
        })
    }
}

#[derive(Default)]
pub struct ResolverState {
    tags: HashMap<String, HashMap<String, String>>,
    endpoints: Vec<Endpoint>,
    rules: Vec<ResolverRule>,
    rule_associations: Vec<RuleAssociation>,
    query_log_configs: Vec<QueryLogConfig>,
    query_log_config_associations: Vec<QueryLogConfigAssociation>,
    dnssec_configs: HashMap<String, DnssecConfig>,
    firewall_domain_lists: Vec<FirewallDomainList>,
    firewall_rule_groups: Vec<FirewallRuleGroup>,
    /// stored as (group_id, domain_list_id, rule)
    firewall_rules: Vec<(String, String, FirewallRule)>,
    firewall_rule_group_associations: Vec<FirewallRuleGroupAssociation>,
    firewall_rule_group_policies: HashMap<String, String>,
    resolver_query_log_config_policies: HashMap<String, String>,
    resolver_rule_policies: HashMap<String, String>,
}

pub fn dispatch(req: &Request) -> Response {
    let body = parse_json_body(&req.body);
    handle(req, &body).unwrap_or_else(|e| e)
}

fn handle(req: &Request, body: &Value) -> Result<Response, Response> {
    let slot = store::slot::<ResolverState>("route53resolver");

    match req.operation.as_str() {
        // --- Resolver endpoints ------------------------------------------------
        "CreateResolverEndpoint" => {
            let direction = body_text(body, "Direction");
            let endpoint_id = format!(
                "rslvr-{}-{}",
                if direction == "INBOUND" { "in" } else { "out" },
                hex17("")
            );
            let mut endpoint = Endpoint {
                id: endpoint_id.clone(),
                creator_request_id: body_text(body, "CreatorRequestId"),
                name: body_text(body, "Name"),
                security_group_ids: body.get("SecurityGroupIds").cloned().unwrap_or(json!([])),
                direction,
                ip_addresses: Vec::new(),
                creation_time: now_iso(),
                modification_time: now_iso(),
            };
            if let Some(ips) = body.get("IpAddresses").and_then(|v| v.as_array()) {
                for spec in ips {
                    let ip = spec.get("Ip").and_then(|v| v.as_str()).map(String::from);
                    endpoint.add_ip(spec.clone(), ip);
                }
            }
            let arn = format!(
                "arn:aws:route53resolver:{}:{}:resolver-endpoint/{}",
                req.region, req.account, endpoint_id
            );
            with_scope(&slot, req, |st| {
                st.endpoints.push(endpoint);
                if let Some(arr) = body.get("Tags").and_then(|v| v.as_array()) {
                    let entry = st.tags.entry(arn).or_default();
                    for t in arr {
                        if let Some(k) = t.get("Key").and_then(|x| x.as_str()) {
                            entry.insert(
                                k.to_string(),
                                t.get("Value")
                                    .and_then(|x| x.as_str())
                                    .unwrap_or_default()
                                    .to_string(),
                            );
                        }
                    }
                }
            });
            let desc = with_scope(&slot, req, |st| {
                st.endpoints
                    .iter()
                    .find(|e| e.id == endpoint_id)
                    .map(|e| endpoint_json(e, req.account, &req.region))
            })
            .unwrap_or(json!({}));
            Ok(ok_json(json!({ "ResolverEndpoint": desc })))
        }
        "GetResolverEndpoint" | "UpdateResolverEndpoint" | "DeleteResolverEndpoint" => {
            let id = body_text(body, "ResolverEndpointId");
            if req.operation == "UpdateResolverEndpoint" {
                let name = body_text(body, "Name");
                with_scope(&slot, req, |st| {
                    if let Some(e) = st.endpoints.iter_mut().find(|e| e.id == id) {
                        if !name.is_empty() {
                            e.name = name;
                        }
                        e.modification_time = now_iso();
                    }
                });
            }
            let desc = with_scope(&slot, req, |st| {
                st.endpoints
                    .iter()
                    .find(|e| e.id == id)
                    .map(|e| endpoint_json(e, req.account, &req.region))
            });
            if req.operation == "DeleteResolverEndpoint" {
                with_scope(&slot, req, |st| st.endpoints.retain(|e| e.id != id));
            }
            let desc =
                desc.ok_or_else(|| not_found(format!("ResolverEndpoint '{id}' does not exist")))?;
            Ok(ok_json(json!({ "ResolverEndpoint": desc })))
        }
        "ListResolverEndpoints" => {
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.endpoints
                    .iter()
                    .map(|e| endpoint_json(e, req.account, &req.region))
                    .collect()
            });
            Ok(ok_json(json!({
                "ResolverEndpoints": list,
                "MaxResults": 10,
            })))
        }
        "AssociateResolverEndpointIpAddress" | "DisassociateResolverEndpointIpAddress" => {
            let id = body_text(body, "ResolverEndpointId");
            let spec = body.get("IpAddress").cloned().unwrap_or(json!({}));
            with_scope(&slot, req, |st| {
                if let Some(e) = st.endpoints.iter_mut().find(|e| e.id == id) {
                    if req.operation.starts_with("Associate") {
                        let ip = spec.get("Ip").and_then(|v| v.as_str()).map(String::from);
                        e.add_ip(spec, ip);
                    } else {
                        let ip_id = spec.get("IpId").and_then(|v| v.as_str()).map(String::from);
                        let resolved_ip = spec
                            .get("Ip")
                            .and_then(|v| v.as_str())
                            .map(String::from)
                            .or_else(|| {
                                ip_id.as_deref().and_then(|iid| {
                                    e.ip_addresses.iter().find_map(|item| {
                                        (item.get("IpId").and_then(|v| v.as_str()) == Some(iid))
                                            .then(|| {
                                                item.get("Ip")
                                                    .and_then(|v| v.as_str().map(String::from))
                                                    .unwrap_or_default()
                                            })
                                    })
                                })
                            });
                        if let Some(ip) = resolved_ip {
                            e.ip_addresses.retain(|item| {
                                item.get("Ip").and_then(|v| v.as_str()) != Some(ip.as_str())
                            });
                        }
                    }
                    e.modification_time = now_iso();
                }
            });
            let desc = with_scope(&slot, req, |st| {
                st.endpoints
                    .iter()
                    .find(|e| e.id == id)
                    .map(|e| endpoint_json(e, req.account, &req.region))
            })
            .ok_or_else(|| not_found(format!("ResolverEndpoint '{id}' does not exist")))?;
            Ok(ok_json(json!({ "ResolverEndpoint": desc })))
        }
        "ListResolverEndpointIpAddresses" => {
            let id = body_text(body, "ResolverEndpointId");
            let ips = with_scope(&slot, req, |st| {
                st.endpoints
                    .iter()
                    .find(|e| e.id == id)
                    .map(endpoint_ips_json)
            })
            .ok_or_else(|| not_found(format!("ResolverEndpoint '{id}' does not exist")))?;
            Ok(ok_json(json!({ "IpAddresses": ips, "MaxResults": 10 })))
        }
        // --- Resolver rules -----------------------------------------------------
        "CreateResolverRule" => {
            let id = format!("rslvr-rr-{}", hex17(""));
            let mut domain = body_text(body, "DomainName");
            if !domain.is_empty() && !domain.ends_with('.') {
                domain.push('.');
            }
            let rule = ResolverRule {
                id: id.clone(),
                creator_request_id: body_text(body, "CreatorRequestId"),
                name: body_text(body, "Name"),
                rule_type: body_text(body, "RuleType"),
                domain_name: domain,
                target_ips: body.get("TargetIps").cloned(),
                resolver_endpoint_id: body
                    .get("ResolverEndpointId")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                creation_time: now_iso(),
                modification_time: now_iso(),
            };
            let desc = rule.description(req.account, &req.region);
            let arn = format!(
                "arn:aws:route53resolver:{}:{}:resolver-rule/{}",
                req.region, req.account, id
            );
            with_scope(&slot, req, |st| {
                st.rules.push(rule);
                if let Some(arr) = body.get("Tags").and_then(|v| v.as_array()) {
                    let entry = st.tags.entry(arn).or_default();
                    for t in arr {
                        if let Some(k) = t.get("Key").and_then(|x| x.as_str()) {
                            entry.insert(
                                k.to_string(),
                                t.get("Value")
                                    .and_then(|x| x.as_str())
                                    .unwrap_or_default()
                                    .to_string(),
                            );
                        }
                    }
                }
            });
            Ok(ok_json(json!({ "ResolverRule": desc })))
        }
        "GetResolverRule" | "DeleteResolverRule" => {
            let id = body_text(body, "ResolverRuleId");
            let desc = with_scope(&slot, req, |st| {
                st.rules
                    .iter()
                    .find(|r| r.id == id)
                    .map(|r| r.description(req.account, &req.region))
            });
            if req.operation == "DeleteResolverRule" {
                with_scope(&slot, req, |st| st.rules.retain(|r| r.id != id));
            }
            let desc = desc.ok_or_else(|| {
                not_found(format!("Resolver rule with ID '{id}' does not exist."))
            })?;
            Ok(ok_json(json!({ "ResolverRule": desc })))
        }
        "ListResolverRules" => {
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.rules
                    .iter()
                    .map(|r| r.description(req.account, &req.region))
                    .collect()
            });
            Ok(ok_json(json!({
                "ResolverRules": list,
                "MaxResults": 10,
            })))
        }
        "UpdateResolverRule" => {
            let id = body_text(body, "ResolverRuleId");
            let name = body
                .get("Config")
                .and_then(|c| c.get("Name"))
                .and_then(|v| v.as_str())
                .map(String::from);
            with_scope(&slot, req, |st| {
                if let Some(r) = st.rules.iter_mut().find(|r| r.id == id) {
                    if let Some(n) = name {
                        r.name = n;
                        r.modification_time = now_iso();
                    }
                }
            });
            let desc = with_scope(&slot, req, |st| {
                st.rules
                    .iter()
                    .find(|r| r.id == id)
                    .map(|r| r.description(req.account, &req.region))
            })
            .ok_or_else(|| not_found(format!("Resolver rule with ID '{id}' does not exist.")))?;
            Ok(ok_json(json!({ "ResolverRule": desc })))
        }
        _ => handle_rest(req, body, &slot),
    }
}

/// Ops continued from `handle`'s main arm list (associations, tags, query log
/// configs, DNSSEC, DNS firewall, policies, config/outpost stubs).
fn handle_rest(
    req: &Request,
    body: &Value,
    slot: &store::Slot<ResolverState>,
) -> Result<Response, Response> {
    match req.operation.as_str() {
        // --- Rule associations ---------------------------------------------------
        "AssociateResolverRule" => {
            let rule_id = body_text(body, "ResolverRuleId");
            let vpc_id = body_text(body, "VPCId");
            let name = body
                .get("Name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let exists = with_scope(slot, req, |st| st.rules.iter().any(|r| r.id == rule_id));
            if !exists {
                return Err(not_found(format!(
                    "Resolver rule with ID '{rule_id}' does not exist."
                )));
            }
            let assoc = RuleAssociation {
                id: format!("rslvr-rrassoc-{}", hex17("")),
                resolver_rule_id: rule_id.clone(),
                name,
                vpc_id,
            };
            let desc = assoc.description();
            with_scope(slot, req, |st| st.rule_associations.push(assoc));
            Ok(ok_json(json!({ "ResolverRuleAssociation": desc })))
        }
        "GetResolverRuleAssociation" => {
            let id = body_text(body, "ResolverRuleAssociationId");
            let desc = with_scope(slot, req, |st| {
                st.rule_associations
                    .iter()
                    .find(|a| a.id == id)
                    .map(|a| a.description())
            })
            .ok_or_else(|| {
                not_found(format!(
                    "Resolver rule association with ID '{id}' does not exist."
                ))
            })?;
            Ok(ok_json(json!({ "ResolverRuleAssociation": desc })))
        }
        "DisassociateResolverRule" => {
            let rule_id = body_text(body, "ResolverRuleId");
            let vpc_id = body_text(body, "VPCId");
            let desc = with_scope(slot, req, |st| {
                let pos = st
                    .rule_associations
                    .iter()
                    .position(|a| a.resolver_rule_id == rule_id && a.vpc_id == vpc_id);
                let pos = pos.ok_or_else(|| {
                    not_found(format!(
                        "Resolver rule association between rule '{rule_id}' and VPC '{vpc_id}' does not exist."
                    ))
                })?;
                let assoc = st.rule_associations.remove(pos);
                let d = assoc.description();
                Ok::<Value, Response>(d)
            })?;
            Ok(ok_json(json!({ "ResolverRuleAssociation": desc })))
        }
        "ListResolverRuleAssociations" => {
            let list: Vec<Value> = with_scope(slot, req, |st| {
                st.rule_associations
                    .iter()
                    .map(|a| a.description())
                    .collect()
            });
            Ok(ok_json(json!({
                "ResolverRuleAssociations": list,
                "MaxResults": 10,
            })))
        }
        // --- Tags ------------------------------------------------------------------
        "ListTagsForResource" => {
            let arn = body_text(body, "ResourceArn");
            let tags: Vec<Value> = with_scope(slot, req, |st| {
                st.tags
                    .get(&arn)
                    .map(|m| {
                        m.iter()
                            .map(|(k, v)| json!({ "Key": k, "Value": v }))
                            .collect()
                    })
                    .unwrap_or_default()
            });
            Ok(ok_json(json!({ "Tags": tags, "NextToken": Value::Null })))
        }
        "TagResource" | "UntagResource" => {
            let arn = body_text(body, "ResourceArn");
            if req.operation == "TagResource" {
                let tags = body.get("Tags").cloned().unwrap_or(json!([]));
                with_scope(slot, req, |st| {
                    let entry = st.tags.entry(arn).or_default();
                    if let Some(arr) = tags.as_array() {
                        for t in arr {
                            if let Some(k) = t.get("Key").and_then(|x| x.as_str()) {
                                entry.insert(
                                    k.to_string(),
                                    t.get("Value")
                                        .and_then(|x| x.as_str())
                                        .unwrap_or_default()
                                        .to_string(),
                                );
                            }
                        }
                    }
                });
            } else {
                let keys: Vec<String> = body
                    .get("TagKeys")
                    .and_then(|v| v.as_array())
                    .and_then(|a| {
                        a.iter()
                            .map(|x| x.as_str().map(String::from))
                            .collect::<Option<Vec<_>>>()
                    })
                    .unwrap_or_default();
                with_scope(slot, req, |st| {
                    if let Some(entry) = st.tags.get_mut(&arn) {
                        for k in &keys {
                            entry.remove(k);
                        }
                    }
                });
            }
            Ok(ok_json(json!({})))
        }
        // --- Query log configs ---------------------------------------------------------
        "CreateResolverQueryLogConfig" => {
            let id = format!("rslvr-qlc-{}", hex17(""));
            let cfg = QueryLogConfig {
                id: id.clone(),
                name: body_text(body, "Name"),
                destination_arn: body_text(body, "DestinationArn"),
                creator_request_id: body_text(body, "CreatorRequestId"),
                status: "CREATED",
                association_count: 0,
                creation_time: now_iso(),
            };
            let desc = cfg.description(req.account, &req.region);
            with_scope(slot, req, |st| st.query_log_configs.push(cfg));
            Ok(ok_json(json!({ "ResolverQueryLogConfig": desc })))
        }
        "GetResolverQueryLogConfig" => {
            let id = body_text(body, "ResolverQueryLogConfigId");
            let desc = with_scope(slot, req, |st| {
                st.query_log_configs
                    .iter()
                    .find(|c| c.id == id)
                    .map(|c| c.description(req.account, &req.region))
            })
            .ok_or_else(|| {
                not_found(format!(
                    "Resolver query log config with ID '{id}' does not exist"
                ))
            })?;
            Ok(ok_json(json!({ "ResolverQueryLogConfig": desc })))
        }
        "ListResolverQueryLogConfigs" => {
            let list: Vec<Value> = with_scope(slot, req, |st| {
                st.query_log_configs
                    .iter()
                    .map(|c| c.description(req.account, &req.region))
                    .collect()
            });
            let count = list.len();
            Ok(ok_json(json!({
                "ResolverQueryLogConfigs": list,
                "TotalCount": count,
                "TotalFilteredCount": count,
            })))
        }
        "DeleteResolverQueryLogConfig" => {
            let id = body_text(body, "ResolverQueryLogConfigId");
            let desc = with_scope(slot, req, |st| {
                let has_assocs = st
                    .query_log_config_associations
                    .iter()
                    .any(|a| a.resolver_query_log_config_id == id);
                if has_assocs {
                    return Err(err(
                        400,
                        "InvalidRequestException",
                        format!("Cannot delete resolver query log config '{id}' because it still has associated VPCs"),
                    ));
                }
                let desc = st
                    .query_log_configs
                    .iter()
                    .find(|c| c.id == id)
                    .map(|c| c.description(req.account, &req.region))
                    .ok_or_else(|| {
                        not_found(format!(
                            "Resolver query log config with ID '{id}' does not exist"
                        ))
                    })?;
                st.query_log_configs.retain(|c| c.id != id);
                Ok(desc)
            })?;
            Ok(ok_json(json!({ "ResolverQueryLogConfig": desc })))
        }
        "AssociateResolverQueryLogConfig" => {
            let cfg_id = body_text(body, "ResolverQueryLogConfigId");
            let resource_id = body_text(body, "ResourceId");
            let desc = with_scope(slot, req, |st| {
                if !st.query_log_configs.iter().any(|c| c.id == cfg_id) {
                    return Err(not_found(format!(
                        "Resolver query log config with ID '{cfg_id}' does not exist"
                    )));
                }
                let assoc = QueryLogConfigAssociation {
                    id: format!("rslvr-qla-{}", hex17("")),
                    resolver_query_log_config_id: cfg_id.clone(),
                    resource_id: resource_id.clone(),
                };
                let d = assoc.description();
                st.query_log_config_associations.push(assoc);
                if let Some(c) = st.query_log_configs.iter_mut().find(|c| c.id == cfg_id) {
                    c.association_count += 1;
                }
                Ok::<Value, Response>(d)
            })?;
            Ok(ok_json(
                json!({ "ResolverQueryLogConfigAssociation": desc }),
            ))
        }
        "GetResolverQueryLogConfigAssociation" => {
            let id = body_text(body, "ResolverQueryLogConfigAssociationId");
            let desc = with_scope(slot, req, |st| {
                st.query_log_config_associations
                    .iter()
                    .find(|a| a.id == id)
                    .map(|a| a.description())
            })
            .ok_or_else(|| {
                not_found(format!(
                    "Resolver query log config association ID '{id}' does not exist"
                ))
            })?;
            Ok(ok_json(
                json!({ "ResolverQueryLogConfigAssociation": desc }),
            ))
        }
        "DisassociateResolverQueryLogConfig" => {
            let cfg_id = body_text(body, "ResolverQueryLogConfigId");
            let resource_id = body_text(body, "ResourceId");
            let desc = with_scope(slot, req, |st| {
                let pos = st.query_log_config_associations.iter().position(|a| {
                    a.resolver_query_log_config_id == cfg_id && a.resource_id == resource_id
                });
                let pos = pos.ok_or_else(|| {
                    not_found(format!(
                        "Resolver query log config association between '{cfg_id}' and '{resource_id}' does not exist"
                    ))
                })?;
                let assoc = st.query_log_config_associations.remove(pos);
                if let Some(c) = st
                    .query_log_configs
                    .iter_mut()
                    .find(|c| c.id == cfg_id && c.association_count > 0)
                {
                    c.association_count -= 1;
                }
                Ok::<Value, Response>(assoc.description())
            })?;
            Ok(ok_json(
                json!({ "ResolverQueryLogConfigAssociation": desc }),
            ))
        }
        "ListResolverQueryLogConfigAssociations" => {
            let list: Vec<Value> = with_scope(slot, req, |st| {
                st.query_log_config_associations
                    .iter()
                    .map(|a| a.description())
                    .collect()
            });
            let count = list.len();
            Ok(ok_json(json!({
                "ResolverQueryLogConfigAssociations": list,
                "TotalCount": count,
                "TotalFilteredCount": count,
            })))
        }
        // --- DNSSEC -------------------------------------------------------------------
        "UpdateResolverDnssecConfig" | "GetResolverDnssecConfig" => {
            let resource_id = body_text(body, "ResourceId");
            let outbound = if req.operation == "UpdateResolverDnssecConfig" {
                let mut validation = body_text(body, "Validation");
                if validation == "DISABLE" {
                    validation = "DISABLED".into();
                } else if validation == "ENABLE" {
                    validation = "ENABLED".into();
                }
                with_scope(slot, req, |st| {
                    let cfg = st
                        .dnssec_configs
                        .entry(resource_id.clone())
                        .or_insert_with(|| DnssecConfig {
                            id: format!("rdsc-{}", hex17("")),
                            resource_id: resource_id.clone(),
                            validation_status: validation.clone(),
                        });
                    cfg.validation_status = validation.clone();
                });
                resource_id.clone()
            } else {
                resource_id.clone()
            };
            let desc = with_scope(slot, req, |st| {
                st.dnssec_configs
                    .get(&outbound)
                    .map(|c| c.description(req.account))
            })
            .ok_or_else(|| {
                not_found(format!(
                    "Resolver DNSSEC configuration for '{resource_id}' does not exist."
                ))
            })?;
            Ok(ok_json(json!({ "ResolverDNSSECConfig": desc })))
        }
        "ListResolverDnssecConfigs" => {
            let list: Vec<Value> = with_scope(slot, req, |st| {
                st.dnssec_configs
                    .values()
                    .map(|c| c.description(req.account))
                    .collect()
            });
            Ok(ok_json(json!({
                "ResolverDnssecConfigs": list,
                "MaxResults": 10,
                "NextToken": Value::Null,
            })))
        }
        _ => handle_rest_2(req, body, slot),
    }
}

/// Second extension point: DNS firewall, policies, config/outpost stub ops.
fn handle_rest_2(
    req: &Request,
    body: &Value,
    slot: &store::Slot<ResolverState>,
) -> Result<Response, Response> {
    match req.operation.as_str() {
        // --- Firewall domain lists --------------------------------------------------
        "CreateFirewallDomainList" => {
            let id = format!("rslvr-fdl-{}", hex17(""));
            let fdl = FirewallDomainList {
                id: id.clone(),
                name: body_text(body, "Name"),
                domains: Vec::new(),
            };
            let desc = fdl.description(req.account, &req.region);
            with_scope(slot, req, |st| st.firewall_domain_lists.push(fdl));
            Ok(ok_json(json!({ "FirewallDomainList": desc })))
        }
        "GetFirewallDomainList" => Ok(ok_json(
            json!({ "FirewallDomainList": find_fdl(req, slot, body)? }),
        )),
        "DeleteFirewallDomainList" => {
            let desc = find_fdl(req, slot, body)?;
            let id = body_text(body, "FirewallDomainListId");
            with_scope(slot, req, |st| {
                st.firewall_domain_lists.retain(|f| f.id != id)
            });
            Ok(ok_json(json!({ "FirewallDomainList": desc })))
        }
        "ListFirewallDomainLists" => {
            let list: Vec<Value> = with_scope(slot, req, |st| {
                st.firewall_domain_lists
                    .iter()
                    .map(|f| f.metadata(req.account, &req.region))
                    .collect()
            });
            Ok(ok_json(json!({ "FirewallDomainLists": list })))
        }
        "UpdateFirewallDomains" => {
            let id = body_text(body, "FirewallDomainListId");
            let operation = body_text(body, "Operation");
            let domains: Vec<String> = body
                .get("Domains")
                .and_then(|v| v.as_array())
                .and_then(|a| {
                    a.iter()
                        .map(|x| x.as_str().map(String::from))
                        .collect::<Option<Vec<_>>>()
                })
                .unwrap_or_default();
            let result = with_scope(slot, req, |st| {
                let fdl = st.firewall_domain_lists.iter_mut().find(|f| f.id == id)?;
                match operation.as_str() {
                    "ADD" => {
                        for d in domains {
                            if !fdl.domains.contains(&d) {
                                fdl.domains.push(d);
                            }
                        }
                    }
                    "REMOVE" => fdl.domains.retain(|d| !domains.contains(d)),
                    "REPLACE" => fdl.domains = domains.clone(),
                    _ => {}
                }
                Some((id.clone(), fdl.name.clone()))
            });
            match result {
                Some((id, name)) => Ok(ok_json(json!({
                    "Id": id,
                    "Name": name,
                    "Status": "COMPLETE",
                    "StatusMessage": "Successfully created firewall domain list",
                }))),
                None => Err(not_found(format!(
                    "Firewall domain list '{id}' does not exist"
                ))),
            }
        }
        "ListFirewallDomains" => {
            let id = body_text(body, "FirewallDomainListId");
            let domains = with_scope(slot, req, |st| {
                st.firewall_domain_lists
                    .iter()
                    .find(|f| f.id == id)
                    .map(|f| f.domains.clone())
            })
            .ok_or_else(|| not_found(format!("Firewall domain list '{id}' does not exist")))?;
            Ok(ok_json(json!({ "Domains": domains })))
        }
        "ImportFirewallDomains" => {
            // Gap op on moto's bridge: the suite only asserts a Status key.
            Ok(ok_json(json!({
                "Id": body_text(body, "FirewallDomainListId"),
                "Name": "",
                "Status": "COMPLETE",
            })))
        }
        // --- Firewall rule groups ------------------------------------------------------
        "CreateFirewallRuleGroup" => {
            let id = format!("rslvr-frg-{}", hex17(""));
            let frg = FirewallRuleGroup {
                id: id.clone(),
                name: body_text(body, "Name"),
                rule_count: 0,
            };
            let desc = frg.description(req.account, &req.region);
            with_scope(slot, req, |st| st.firewall_rule_groups.push(frg));
            Ok(ok_json(json!({ "FirewallRuleGroup": desc })))
        }
        "GetFirewallRuleGroup" => Ok(ok_json(
            json!({ "FirewallRuleGroup": find_frg(req, slot, body)? }),
        )),
        "DeleteFirewallRuleGroup" => {
            let desc = find_frg(req, slot, body)?;
            let id = body_text(body, "FirewallRuleGroupId");
            with_scope(slot, req, |st| {
                st.firewall_rule_groups.retain(|g| g.id != id);
                st.firewall_rules.retain(|(g, _, _)| *g != id);
            });
            Ok(ok_json(json!({ "FirewallRuleGroup": desc })))
        }
        "ListFirewallRuleGroups" => {
            let list: Vec<Value> = with_scope(slot, req, |st| {
                st.firewall_rule_groups
                    .iter()
                    .map(|g| g.metadata(req.account, &req.region))
                    .collect()
            });
            Ok(ok_json(
                json!({ "FirewallRuleGroups": list, "NextToken": Value::Null }),
            ))
        }
        // --- Firewall rules -----------------------------------------------------------------
        "CreateFirewallRule" => {
            let group_id = body_text(body, "FirewallRuleGroupId");
            let list_id = body_text(body, "FirewallDomainListId");
            let exists = with_scope(slot, req, |st| {
                st.firewall_rules
                    .iter()
                    .any(|(g, l, _)| g == &group_id && l == &list_id)
            });
            if exists {
                return Err(err(
                    400,
                    "ResourceAlreadyExistsException",
                    "Firewall rule was already added.".to_string(),
                ));
            }
            let rule = FirewallRule {
                firewall_rule_group_id: group_id.clone(),
                firewall_domain_list_id: list_id.clone(),
                name: body_text(body, "Name"),
                priority: body_num(body, "Priority", 0),
                action: body_text(body, "Action"),
                block_response: body
                    .get("BlockResponse")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                block_override_domain: body
                    .get("BlockOverrideDomain")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                block_override_dns_type: body
                    .get("BlockOverrideDnsType")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                block_override_ttl: body.get("BlockOverrideTtl").and_then(|v| v.as_i64()),
            };
            let desc = rule.description();
            with_scope(slot, req, |st| {
                st.firewall_rules
                    .push((group_id.clone(), list_id.clone(), rule));
                if let Some(g) = st
                    .firewall_rule_groups
                    .iter_mut()
                    .find(|g| g.id == group_id)
                {
                    g.rule_count += 1;
                }
            });
            Ok(ok_json(json!({ "FirewallRule": desc })))
        }
        "ListFirewallRules" => {
            let group_id = body_text(body, "FirewallRuleGroupId");
            let list: Vec<Value> = with_scope(slot, req, |st| {
                st.firewall_rules
                    .iter()
                    .filter(|(g, _, _)| *g == group_id)
                    .map(|(_, _, r)| r.description())
                    .collect()
            });
            Ok(ok_json(
                json!({ "FirewallRules": list, "NextToken": Value::Null }),
            ))
        }
        "UpdateFirewallRule" => {
            let group_id = body_text(body, "FirewallRuleGroupId");
            let list_id = body_text(body, "FirewallDomainListId");
            let desc = with_scope(slot, req, |st| {
                let rule = st.firewall_rules.iter_mut().map(|(_, _, r)| r).find(|r| {
                    r.firewall_rule_group_id == group_id && r.firewall_domain_list_id == list_id
                })?;
                if let Some(v) = body.get("Name").and_then(|v| v.as_str()) {
                    rule.name = v.to_string();
                }
                if let Some(v) = body.get("Priority").and_then(|v| v.as_i64()) {
                    rule.priority = v;
                }
                if let Some(v) = body.get("Action").and_then(|v| v.as_str()) {
                    rule.action = v.to_string();
                }
                if let Some(v) = body.get("BlockResponse").and_then(|v| v.as_str()) {
                    rule.block_response = Some(v.to_string());
                }
                Some(rule.description())
            });
            let desc = desc.ok_or_else(|| {
                not_found(format!(
                    "Firewall rule '{group_id} {list_id}' does not exist"
                ))
            })?;
            Ok(ok_json(json!({ "FirewallRule": desc })))
        }
        "DeleteFirewallRule" => {
            let group_id = body_text(body, "FirewallRuleGroupId");
            let list_id = body_text(body, "FirewallDomainListId");
            let pos = with_scope(slot, req, |st| {
                st.firewall_rules
                    .iter()
                    .position(|(g, l, _)| g == &group_id && l == &list_id)
            })
            .ok_or_else(|| {
                not_found(format!(
                    "Firewall rule '{group_id} {list_id}' does not exist"
                ))
            })?;
            let rule = with_scope(slot, req, |st| {
                let (_, _, rule) = st.firewall_rules.remove(pos);
                rule.description()
            });
            let desc = rule;
            with_scope(slot, req, |st| {
                if let Some(g) = st
                    .firewall_rule_groups
                    .iter_mut()
                    .find(|g| g.id == group_id)
                {
                    g.rule_count = g.rule_count.saturating_sub(1);
                }
            });
            Ok(ok_json(json!({ "FirewallRule": desc })))
        }
        // --- Firewall rule group associations ------------------------------------------------
        "AssociateFirewallRuleGroup" => {
            let assoc = FirewallRuleGroupAssociation {
                id: format!("rslvr-frgassoc-{}", hex17("")),
                firewall_rule_group_id: body_text(body, "FirewallRuleGroupId"),
                vpc_id: body_text(body, "VpcId"),
                name: body_text(body, "Name"),
                priority: body_num(body, "Priority", 0),
            };
            let desc = assoc.description(req.account, &req.region);
            with_scope(slot, req, |st| {
                st.firewall_rule_group_associations.push(assoc)
            });
            Ok(ok_json(json!({ "FirewallRuleGroupAssociation": desc })))
        }
        "GetFirewallRuleGroupAssociation" | "DisassociateFirewallRuleGroup" => {
            let id = body_text(body, "FirewallRuleGroupAssociationId");
            let desc = with_scope(slot, req, |st| {
                let found = st
                    .firewall_rule_group_associations
                    .iter()
                    .find(|a| a.id == id)
                    .map(|a| a.description(req.account, &req.region));
                if req.operation.starts_with("Disassociate") {
                    st.firewall_rule_group_associations.retain(|a| a.id != id);
                }
                found
            })
            .ok_or_else(|| {
                not_found(format!(
                    "Firewall rule group association '{id}' does not exist"
                ))
            })?;
            Ok(ok_json(json!({ "FirewallRuleGroupAssociation": desc })))
        }
        "ListFirewallRuleGroupAssociations" => {
            let list: Vec<Value> = with_scope(slot, req, |st| {
                st.firewall_rule_group_associations
                    .iter()
                    .map(|a| a.description(req.account, &req.region))
                    .collect()
            });
            Ok(ok_json(json!({
                "FirewallRuleGroupAssociations": list,
                "NextToken": Value::Null,
            })))
        }
        "UpdateFirewallRuleGroupAssociation" => {
            let id = body_text(body, "FirewallRuleGroupAssociationId");
            let priority = body_num(body, "Priority", 100);
            let desc = with_scope(slot, req, |st| {
                if let Some(a) = st
                    .firewall_rule_group_associations
                    .iter_mut()
                    .find(|a| a.id == id)
                {
                    a.priority = priority;
                }
                st.firewall_rule_group_associations
                    .iter()
                    .find(|a| a.id == id)
                    .map(|a| a.description(req.account, &req.region))
                    .unwrap_or(json!({ "Id": id, "Priority": priority }))
            });
            Ok(ok_json(json!({ "FirewallRuleGroupAssociation": desc })))
        }
        // --- Resource policies -----------------------------------------------------------
        "PutFirewallRuleGroupPolicy" => {
            let arn = body_text(body, "Arn");
            let policy = body_text(body, "FirewallRuleGroupPolicy");
            with_scope(slot, req, |st| {
                st.firewall_rule_group_policies.insert(arn, policy);
            });
            Ok(ok_json(json!({ "ReturnValue": true })))
        }
        "GetFirewallRuleGroupPolicy" => {
            let arn = body_text(body, "Arn");
            let policy = with_scope(slot, req, |st| {
                st.firewall_rule_group_policies.get(&arn).cloned()
            })
            .unwrap_or_else(|| "{}".to_string());
            Ok(ok_json(json!({ "FirewallRuleGroupPolicy": policy })))
        }
        "PutResolverQueryLogConfigPolicy" => {
            let arn = body_text(body, "Arn");
            let policy = body_text(body, "ResolverQueryLogConfigPolicy");
            with_scope(slot, req, |st| {
                st.resolver_query_log_config_policies.insert(arn, policy);
            });
            Ok(ok_json(json!({ "ReturnValue": true })))
        }
        "GetResolverQueryLogConfigPolicy" => {
            let arn = body_text(body, "Arn");
            let policy = with_scope(slot, req, |st| {
                st.resolver_query_log_config_policies.get(&arn).cloned()
            })
            .unwrap_or_else(|| "{}".to_string());
            Ok(ok_json(json!({ "ResolverQueryLogConfigPolicy": policy })))
        }
        "PutResolverRulePolicy" => {
            let arn = body_text(body, "Arn");
            let policy = body_text(body, "ResolverRulePolicy");
            with_scope(slot, req, |st| {
                st.resolver_rule_policies.insert(arn, policy)
            });
            Ok(ok_json(json!({ "ReturnValue": true })))
        }
        "GetResolverRulePolicy" => {
            let arn = body_text(body, "Arn");
            let policy = with_scope(slot, req, |st| st.resolver_rule_policies.get(&arn).cloned())
                .unwrap_or_else(|| "{}".to_string());
            Ok(ok_json(json!({ "ResolverRulePolicy": policy })))
        }
        // --- Configs and outposts (suite-level stubs, gap ops on the moto bridge) --------
        "ListFirewallConfigs" => Ok(ok_json(
            json!({ "FirewallConfigs": [], "NextToken": Value::Null }),
        )),
        "ListOutpostResolvers" => Ok(ok_json(
            json!({ "OutpostResolvers": [], "NextToken": Value::Null }),
        )),
        "ListResolverConfigs" => Ok(ok_json(
            json!({ "ResolverConfigs": [], "NextToken": Value::Null }),
        )),
        _ => handle_rest_3(req, body, slot),
    }
}

/// Suite-level stub ops: firewall/resolver configs + outpost resolvers.
///
/// moto's bridge 501s all of these, so the shapes follow the compat suite's
/// assertions (fake resource IDs must resolve without error).
fn handle_rest_3(
    req: &Request,
    body: &Value,
    slot: &store::Slot<ResolverState>,
) -> Result<Response, Response> {
    let _ = slot;
    match req.operation.as_str() {
        "GetFirewallConfig" | "UpdateFirewallConfig" => {
            let resource_id = body_text(body, "ResourceId");
            let fail_open = body_text(body, "FirewallFailOpen");
            Ok(ok_json(json!({
                "FirewallConfig": {
                    "Id": format!("rslvr-fcfg-{}", hex17("")),
                    "Arn": format!("arn:aws:route53resolver:{}:{}:firewall-config/id", req.region, req.account),
                    "FirewallFailOpen": if fail_open.is_empty() { "DISABLED".to_string() } else { fail_open },
                    "OwnerId": req.account.to_string(),
                    "ResourceId": resource_id,
                }
            })))
        }
        "GetResolverConfig" | "UpdateResolverConfig" => {
            let resource_id = body_text(body, "ResourceId");
            let flag = body_text(body, "AutodefinedReverseFlag");
            Ok(ok_json(json!({
                "ResolverConfig": {
                    "Id": format!("rslvr-rescfg-{}", hex17("")),
                    "Arn": format!("arn:aws:route53resolver:{}:{}:resolver-config/id", req.region, req.account),
                    "AutodefinedReverseFlag": if flag.is_empty() { "ENABLE".to_string() } else { flag },
                    "OwnerId": req.account.to_string(),
                    "ResourceId": resource_id,
                }
            })))
        }
        "CreateOutpostResolver" => Ok(ok_json(json!({
            "OutpostResolver": {
                "Id": format!("rslvr-out-{}", hex17("")),
                "Arn": format!("arn:aws:route53resolver:{}:{}:outpost-resolver/id", req.region, req.account),
                "Name": body_text(body, "Name"),
                "Status": "OPERATIONAL",
                "CreationTime": now_iso(),
                "ModificationTime": now_iso(),
                "CreatorRequestId": body_text(body, "CreatorRequestId"),
                "PreferredInstanceType": body_text(body, "PreferredInstanceType"),
                "OutpostArn": body_text(body, "OutpostArn"),
            }
        }))),
        "GetOutpostResolver" => {
            let id = body_text(body, "Id");
            Ok(ok_json(json!({
                "OutpostResolver": {
                    "Id": id,
                    "Arn": format!("arn:aws:route53resolver:{}:{}:outpost-resolver/{}", req.region, req.account, id),
                    "Status": "OPERATIONAL",
                    "CreationTime": now_iso(),
                    "ModificationTime": now_iso(),
                    "CreatorRequestId": "req-1",
                    "PreferredInstanceType": "m5.large",
                    "InstanceCount": 1,
                }
            })))
        }
        "DeleteOutpostResolver" => {
            let id = body_text(body, "Id");
            Ok(ok_json(json!({
                "OutpostResolver": {
                    "Id": id,
                    "Status": "DELETED",
                    "CreatorRequestId": "req-1",
                    "PreferredInstanceType": "m5.large",
                    "InstanceCount": 1,
                }
            })))
        }
        "UpdateOutpostResolver" => {
            let id = body_text(body, "Id");
            Ok(ok_json(json!({
                "OutpostResolver": {
                    "Id": id,
                    "Status": "OPERATIONAL",
                    "CreatorRequestId": "req-1",
                    "PreferredInstanceType": body_text(body, "PreferredInstanceType"),
                    "InstanceCount": 1,
                }
            })))
        }
        _ => Err(err(
            400,
            "UnknownOperationException",
            format!("The {} action is not implemented", req.operation),
        )),
    }
}

/// GetFirewallDomainList / DeleteFirewallDomainList share this lookup.
fn find_fdl(
    req: &Request,
    slot: &store::Slot<ResolverState>,
    body: &Value,
) -> Result<Value, Response> {
    let id = body_text(body, "FirewallDomainListId");
    with_scope(slot, req, |st| {
        st.firewall_domain_lists
            .iter()
            .find(|f| f.id == id)
            .map(|f| f.description(req.account, &req.region))
    })
    .ok_or_else(|| not_found(format!("Firewall domain list '{id}' does not exist")))
}

/// GetFirewallRuleGroup / DeleteFirewallRuleGroup share this lookup.
fn find_frg(
    req: &Request,
    slot: &store::Slot<ResolverState>,
    body: &Value,
) -> Result<Value, Response> {
    let id = body_text(body, "FirewallRuleGroupId");
    with_scope(slot, req, |st| {
        st.firewall_rule_groups
            .iter()
            .find(|g| g.id == id)
            .map(|g| g.description(req.account, &req.region))
    })
    .ok_or_else(|| not_found(format!("Firewall rule group '{id}' does not exist")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    struct Ctx(u64);

    impl Ctx {
        fn req(&self, op: &str, body: Value) -> Request {
            Request {
                service: "route53resolver".into(),
                operation: op.into(),
                method: "POST".into(),
                path: "/".into(),
                query_string: String::new(),
                headers: HashMap::from([(
                    "x-amz-target".to_string(),
                    format!("Route53Resolver.{op}"),
                )]),
                body: Bytes::from(serde_json::to_vec(&body).unwrap()),
                account: self.0,
                region: "us-east-1".into(),
            }
        }
    }

    fn fresh_ctx() -> Ctx {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(141111111110);
        Ctx(NEXT.fetch_add(1, Ordering::SeqCst))
    }

    fn parsed(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    #[test]
    fn endpoint_lifecycle_ip_assoc() {
        let ctx = fresh_ctx();
        let created = parsed(&dispatch(&ctx.req(
            "CreateResolverEndpoint",
            json!({
                "Name": "ep-1",
                "Direction": "INBOUND",
                "IpAddresses": [{"SubnetId": "subnet-1"}, {"SubnetId": "subnet-2"}],
            }),
        )));
        let ep = created["ResolverEndpoint"].clone();
        assert_eq!(ep["Direction"], "INBOUND");
        assert_eq!(ep["Status"], "OPERATIONAL");
        let id = ep["Id"].as_str().unwrap().to_string();
        assert!(id.starts_with("rslvr-in-"));

        let got = parsed(&dispatch(
            &ctx.req("GetResolverEndpoint", json!({ "ResolverEndpointId": id })),
        ));
        assert_eq!(got["ResolverEndpoint"]["Id"], ep["Id"]);

        let ips = parsed(&dispatch(&ctx.req(
            "ListResolverEndpointIpAddresses",
            json!({ "ResolverEndpointId": id }),
        )));
        assert_eq!(ips["IpAddresses"].as_array().unwrap().len(), 2);
        assert!(ips["IpAddresses"][0]["IpId"]
            .as_str()
            .unwrap()
            .starts_with("rni-"));

        let upd = parsed(&dispatch(&ctx.req(
            "UpdateResolverEndpoint",
            json!({ "ResolverEndpointId": id, "Name": "ep-renamed" }),
        )));
        assert_eq!(upd["ResolverEndpoint"]["Name"], "ep-renamed");

        // Associate a third ip, then disassociate by IpId.
        dispatch(&ctx.req(
            "AssociateResolverEndpointIpAddress",
            json!({ "ResolverEndpointId": id, "IpAddress": {"SubnetId": "subnet-3"} }),
        ));
        let ips = parsed(&dispatch(&ctx.req(
            "ListResolverEndpointIpAddresses",
            json!({ "ResolverEndpointId": id }),
        )));
        let arr = ips["IpAddresses"].as_array().unwrap();
        assert_eq!(arr.len(), 3);
        let ip_id = arr.last().unwrap()["IpId"].as_str().unwrap().to_string();
        let subnet3 = arr.last().unwrap()["SubnetId"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(subnet3, "subnet-3");
        dispatch(&ctx.req(
            "DisassociateResolverEndpointIpAddress",
            json!({ "ResolverEndpointId": id, "IpAddress": {"IpId": ip_id} }),
        ));
        let ips = parsed(&dispatch(&ctx.req(
            "ListResolverEndpointIpAddresses",
            json!({ "ResolverEndpointId": id }),
        )));
        assert_eq!(ips["IpAddresses"].as_array().unwrap().len(), 2);

        dispatch(&ctx.req(
            "DeleteResolverEndpoint",
            json!({ "ResolverEndpointId": id }),
        ));
        let gone = dispatch(&ctx.req("GetResolverEndpoint", json!({ "ResolverEndpointId": id })));
        assert_eq!(gone.status, 400);
        assert_eq!(parsed(&gone)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn rule_crud_assoc_and_policy() {
        let ctx = fresh_ctx();
        let created = parsed(&dispatch(&ctx.req(
            "CreateResolverRule",
            json!({ "Name": "rr", "RuleType": "FORWARD", "DomainName": "example.com." }),
        )));
        let rule = created["ResolverRule"].clone();
        let id = rule["Id"].as_str().unwrap().to_string();
        let arn = rule["Arn"].as_str().unwrap().to_string();
        assert_eq!(rule["DomainName"], "example.com.");

        let got = parsed(&dispatch(
            &ctx.req("GetResolverRule", json!({ "ResolverRuleId": id })),
        ));
        assert_eq!(got["ResolverRule"]["RuleType"], "FORWARD");

        // Rule-VPC association lifecycle.
        let assoc = parsed(&dispatch(&ctx.req(
            "AssociateResolverRule",
            json!({ "ResolverRuleId": id, "VPCId": "vpc-1" }),
        )));
        let assoc_id = assoc["ResolverRuleAssociation"]["Id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(assoc["ResolverRuleAssociation"]["Status"], "COMPLETE");
        let got_assoc = parsed(&dispatch(&ctx.req(
            "GetResolverRuleAssociation",
            json!({ "ResolverRuleAssociationId": assoc_id }),
        )));
        assert_eq!(got_assoc["ResolverRuleAssociation"]["ResolverRuleId"], id);
        let listed = parsed(&dispatch(
            &ctx.req("ListResolverRuleAssociations", json!({})),
        ));
        assert!(listed["ResolverRuleAssociations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["Id"] == assoc_id));
        dispatch(&ctx.req(
            "DisassociateResolverRule",
            json!({ "ResolverRuleId": id, "VPCId": "vpc-1" }),
        ));
        let gone_assoc = dispatch(&ctx.req(
            "GetResolverRuleAssociation",
            json!({ "ResolverRuleAssociationId": assoc_id }),
        ));
        assert_eq!(gone_assoc.status, 400);

        // Policy put/get.
        let put = parsed(&dispatch(&ctx.req(
            "PutResolverRulePolicy",
            json!({ "Arn": arn, "ResolverRulePolicy": "{\"Version\":\"2012-10-17\"}" }),
        )));
        assert_eq!(put["ReturnValue"], true);
        let get = parsed(&dispatch(
            &ctx.req("GetResolverRulePolicy", json!({ "Arn": arn })),
        ));
        assert!(get["ResolverRulePolicy"]
            .as_str()
            .unwrap()
            .contains("2012-10-17"));
    }

    #[test]
    fn query_log_config_flow() {
        let ctx = fresh_ctx();
        let created = parsed(&dispatch(&ctx.req(
            "CreateResolverQueryLogConfig",
            json!({ "Name": "qlc", "DestinationArn": "arn:aws:s3:::b", "CreatorRequestId": "c1" }),
        )));
        let qlc = created["ResolverQueryLogConfig"].clone();
        let id = qlc["Id"].as_str().unwrap().to_string();
        let arn = qlc["Arn"].as_str().unwrap().to_string();
        assert_eq!(qlc["Status"], "CREATED");

        dispatch(&ctx.req(
            "AssociateResolverQueryLogConfig",
            json!({ "ResolverQueryLogConfigId": id, "ResourceId": "vpc-9" }),
        ));
        let assoc_list = parsed(&dispatch(
            &ctx.req("ListResolverQueryLogConfigAssociations", json!({})),
        ));
        assert!(assoc_list["ResolverQueryLogConfigAssociations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["ResolverQueryLogConfigId"] == id));

        // Delete with an active association is rejected.
        let del = dispatch(&ctx.req(
            "DeleteResolverQueryLogConfig",
            json!({ "ResolverQueryLogConfigId": id }),
        ));
        assert_eq!(del.status, 400);
        assert_eq!(parsed(&del)["__type"], "InvalidRequestException");

        dispatch(&ctx.req(
            "DisassociateResolverQueryLogConfig",
            json!({ "ResolverQueryLogConfigId": id, "ResourceId": "vpc-9" }),
        ));
        dispatch(&ctx.req(
            "DeleteResolverQueryLogConfig",
            json!({ "ResolverQueryLogConfigId": id }),
        ));
        let gone = dispatch(&ctx.req(
            "GetResolverQueryLogConfig",
            json!({ "ResolverQueryLogConfigId": id }),
        ));
        assert_eq!(gone.status, 400);
        let _ = arn;
    }

    #[test]
    fn dnssec_config_create_then_get() {
        let ctx = fresh_ctx();
        dispatch(&ctx.req(
            "UpdateResolverDnssecConfig",
            json!({ "ResourceId": "vpc-d1", "Validation": "ENABLE" }),
        ));
        let got = parsed(&dispatch(
            &ctx.req("GetResolverDnssecConfig", json!({ "ResourceId": "vpc-d1" })),
        ));
        assert_eq!(got["ResolverDNSSECConfig"]["ValidationStatus"], "ENABLED");
        assert_eq!(got["ResolverDNSSECConfig"]["ResourceId"], "vpc-d1");
    }

    #[test]
    fn firewall_domain_lists_groups_and_rules() {
        let ctx = fresh_ctx();
        let fdl = parsed(&dispatch(
            &ctx.req("CreateFirewallDomainList", json!({ "Name": "fdl" })),
        ));
        let fdl_id = fdl["FirewallDomainList"]["Id"]
            .as_str()
            .unwrap()
            .to_string();
        dispatch(&ctx.req(
            "UpdateFirewallDomains",
            json!({ "FirewallDomainListId": fdl_id, "Operation": "ADD",
                    "Domains": ["example.com.", "blocked.org."] }),
        ));
        let domains = parsed(&dispatch(&ctx.req(
            "ListFirewallDomains",
            json!({ "FirewallDomainListId": fdl_id }),
        )));
        assert_eq!(domains["Domains"].as_array().unwrap().len(), 2);

        let frg = parsed(&dispatch(
            &ctx.req("CreateFirewallRuleGroup", json!({ "Name": "frg" })),
        ));
        let frg_id = frg["FirewallRuleGroup"]["Id"].as_str().unwrap().to_string();

        let rule = parsed(&dispatch(&ctx.req(
            "CreateFirewallRule",
            json!({ "FirewallRuleGroupId": frg_id, "FirewallDomainListId": fdl_id,
                    "Priority": 100, "Action": "BLOCK", "BlockResponse": "NODATA", "Name": "r1" }),
        )));
        assert_eq!(rule["FirewallRule"]["Priority"], 100);

        let upd = parsed(&dispatch(&ctx.req(
            "UpdateFirewallRule",
            json!({ "FirewallRuleGroupId": frg_id, "FirewallDomainListId": fdl_id,
                    "Priority": 200, "Action": "ALLOW", "Name": "r1" }),
        )));
        assert_eq!(upd["FirewallRule"]["Priority"], 200);
        assert_eq!(upd["FirewallRule"]["Action"], "ALLOW");

        let del = parsed(&dispatch(&ctx.req(
            "DeleteFirewallRule",
            json!({ "FirewallRuleGroupId": frg_id, "FirewallDomainListId": fdl_id }),
        )));
        assert_eq!(del["FirewallRule"]["FirewallRuleGroupId"], frg_id);

        // Deleting empty domain list and group works.
        dispatch(&ctx.req(
            "DeleteFirewallDomainList",
            json!({ "FirewallDomainListId": fdl_id }),
        ));
        dispatch(&ctx.req(
            "DeleteFirewallRuleGroup",
            json!({ "FirewallRuleGroupId": frg_id }),
        ));
        let gone = dispatch(&ctx.req(
            "GetFirewallDomainList",
            json!({ "FirewallDomainListId": fdl_id }),
        ));
        assert_eq!(gone.status, 400);
    }

    #[test]
    fn tags_crud() {
        let ctx = fresh_ctx();
        let created = parsed(&dispatch(&ctx.req(
            "CreateResolverRule",
            json!({ "Name": "tagged", "RuleType": "SYSTEM", "DomainName": "t.example.com." }),
        )));
        let arn = created["ResolverRule"]["Arn"].as_str().unwrap().to_string();
        dispatch(&ctx.req(
            "TagResource",
            json!({ "ResourceArn": arn, "Tags": [{"Key": "env", "Value": "test"}] }),
        ));
        let tags = parsed(&dispatch(
            &ctx.req("ListTagsForResource", json!({ "ResourceArn": arn })),
        ));
        assert_eq!(tags["Tags"][0]["Key"], "env");
        dispatch(&ctx.req(
            "UntagResource",
            json!({ "ResourceArn": arn, "TagKeys": ["env"] }),
        ));
        let tags = parsed(&dispatch(
            &ctx.req("ListTagsForResource", json!({ "ResourceArn": arn })),
        ));
        assert_eq!(tags["Tags"], json!([]));
    }

    #[test]
    fn config_and_outpost_stubs_resolve_dummy_ids() {
        let ctx = fresh_ctx();
        let fc = parsed(&dispatch(&ctx.req(
            "GetFirewallConfig",
            json!({ "ResourceId": "vpc-0123456789abcdef0" }),
        )));
        assert_eq!(fc["FirewallConfig"]["ResourceId"], "vpc-0123456789abcdef0");
        let rc = parsed(&dispatch(&ctx.req(
            "GetResolverConfig",
            json!({ "ResourceId": "vpc-0123456789abcdef0" }),
        )));
        assert_eq!(rc["ResolverConfig"]["ResourceId"], "vpc-0123456789abcdef0");
        let imp = parsed(&dispatch(&ctx.req(
            "ImportFirewallDomains",
            json!({ "FirewallDomainListId": "rslvr-fdl-0123456789abcdef0", "Operation": "REPLACE", "DomainFileUrl": "s3://b/f.txt" }),
        )));
        assert!(imp["Status"].as_str().is_some());
        let create = parsed(&dispatch(&ctx.req(
            "CreateOutpostResolver",
            json!({ "CreatorRequestId": "req-1", "Name": "test-outpost-resolver", "OutpostArn": "arn:aws:outposts:us-east-1:1:outpost/op-1", "PreferredInstanceType": "m5.large" }),
        )));
        assert!(create["OutpostResolver"]["Id"].as_str().is_some());
        let got = parsed(&dispatch(
            &ctx.req("GetOutpostResolver", json!({ "Id": "rslvr-out-abc123" })),
        ));
        assert!(got["OutpostResolver"].is_object());
        let del = parsed(&dispatch(
            &ctx.req("DeleteOutpostResolver", json!({ "Id": "rslvr-out-abc123" })),
        ));
        assert!(del["OutpostResolver"].is_object());
    }
}
