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
                "DeleteProtection": p.get("DeleteProtection").cloned().unwrap_or(json!(false)),
                "SubnetChangeProtection": p.get("SubnetChangeProtection").cloned().unwrap_or(json!(false)),
                "FirewallPolicyChangeProtection": p.get("FirewallPolicyChangeProtection").cloned().unwrap_or(json!(false)),
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
                "FirewallPolicy": p.get("FirewallPolicy").cloned().unwrap_or(json!({})),
                "Description": p.get("Description").cloned().unwrap_or(Value::Null),
            });
            st.policies.push(policy.clone());
            ok(&json!({"FirewallPolicyResponse": policy, "UpdateToken": UPDATE_TOKEN}))
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
                    "FirewallPolicyResponse": v,
                    "FirewallPolicy": v["FirewallPolicy"].clone(),
                    "UpdateToken": UPDATE_TOKEN,
                })),
                None => not_found(&format!("FirewallPolicy {pn} not found")),
            }
        }
        "ListFirewallPolicies" => ok(&json!({
            "FirewallPolicies": st.policies.clone(),
            "nextToken": Value::Null,
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

    #[test]
    fn firewall_crud() {
        let slot = state();
        let mut guard = slot.write();
        let b = guard.entry((123456789012, "us-east-1".into())).or_default();
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
        assert_eq!(v["FirewallStatus"]["Status"], "READY");

        let describe = req("DescribeFirewall", json!({"FirewallName": "fw-test"}));
        let p = serde_json::from_slice(&describe.body).unwrap();
        let v = serde_json::from_slice::<Value>(&handle(&describe, b, &p).body).unwrap();
        assert_eq!(v["Firewall"]["VpcId"], "vpc-12345");
        assert!(v["UpdateToken"].as_str().is_some());

        let list = req("ListFirewalls", json!({}));
        let p = serde_json::from_slice(&list.body).unwrap();
        let v = serde_json::from_slice::<Value>(&handle(&list, b, &p).body).unwrap();
        assert_eq!(v["Firewalls"].as_array().unwrap().len(), 1);
    }
}
