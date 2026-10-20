//! Timestream InfluxDB backend (JSON protocol, target
//! `TimestreamInfluxDB.<Op>`).
//!
//! Port of moto's timestreaminfluxdb: DB instances, clusters and parameter
//! groups. List calls return `{"items": [summaries]}` with the module's
//! lower-case camel keys; CRUD mirrors moto's instance dict.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};

#[derive(Default)]
struct State {
    /// DB instance id -> instance dict.
    instances: Vec<Value>,
    /// Cluster id -> cluster dict.
    clusters: Vec<Value>,
    /// Parameter group id -> group dict.
    param_groups: Vec<Value>,
    /// resource ARN -> tags (moto's tagger.dao: the ARN is not validated).
    tags: Vec<(String, Vec<(String, String)>)>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("timestreaminfluxdb"));
    SLOT.clone()
}

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn arn_of(region: &str, account: u64, kind: &str, id: &str) -> String {
    format!("arn:aws:timestream-influxdb:{region}:{account}:{kind}/{id}")
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "timestreaminfluxdb" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(req, b, &body, &req.region, req.account))
}

fn instance_summary(inst: &Value) -> Value {
    // moto's to_summary_dict(): id/name/arn/status/endpoint.
    json!({
        "id": inst["id"].clone(),
        "name": inst["name"].clone(),
        "arn": inst["arn"].clone(),
        "status": inst["status"].clone(),
        "endpoint": inst["endpoint"].clone(),
    })
}

fn instance_dict(
    st: &State,
    p: &Value,
    region: &str,
    account: u64,
    existing_id: Option<&str>,
) -> Value {
    let id = existing_id.map(String::from).unwrap_or_else(random_id);
    json!({
        "id": id,
        "name": p["name"],
        "arn": arn_of(region, account, "db-instance", &id),
        "status": "RUNNING",
        "endpoint": p["port"],
        "readerEndpoint": "",
        "port": p.get("port").cloned().unwrap_or(json!(8086)),
        "deploymentType": p.get("deploymentType").cloned().unwrap_or(json!("SINGLE_AZ")),
        "dbInstanceType": p.get("dbInstanceType").cloned().unwrap_or(Value::Null),
        "networkType": p.get("networkType").cloned().unwrap_or(json!("IPV4")),
        "dbStorageType": p.get("dbStorageType").cloned().unwrap_or(json!("InfluxIOIncludedT1")),
        "allocatedStorage": p.get("allocatedStorage"),
        "publiclyAccessible": p.get("publiclyAccessible").cloned().unwrap_or(json!(false)),
        "dbParameterGroupIdentifier": p.get("dbParameterGroupIdentifier"),
        "logDeliveryConfiguration": p.get("logDeliveryConfiguration").cloned().unwrap_or(json!({})),
        "region": region,
        "availabilityZone": format!("{region}a"),
        "vpcSubnetIds": p.get("vpcSubnetIds").cloned().unwrap_or(json!([])),
        "vpcSecurityGroupIds": p.get("vpcSecurityGroupIds"),
    })
    .with_state_defaults(st)
}

fn random_id() -> String {
    let raw = uuid::Uuid::new_v4().simple().to_string();
    raw.chars().take(11).collect()
}

trait StateDefaults {
    fn with_state_defaults(self, st: &State) -> Value;
}

impl StateDefaults for Value {
    fn with_state_defaults(self, st: &State) -> Value {
        let _ = st;
        self
    }
}

fn find<'a>(list: &'a [Value], id: &str) -> Option<&'a Value> {
    list.iter().find(|v| v["id"] == json!(id))
}

/// moto's `Tagger.validate_tags`: each tag dict entry {"key","value"} yields
/// one message per bad key, one per bad value, joined as
/// "N validation error(s) detected: ...".
fn validate_tags(tags: Option<&serde_json::Map<String, Value>>) -> Option<String> {
    // The key pattern's leading `(?!aws:)` look-ahead is unsupported by the
    // `regex` crate; reject `aws:`-prefixed keys via the guard below instead.
    static KEY_RE: Lazy<regex::Regex> =
        Lazy::new(|| regex::Regex::new(r"^([\w\s\d_.:/=+\-@]*)$").unwrap());
    static VALUE_RE: Lazy<regex::Regex> =
        Lazy::new(|| regex::Regex::new(r"^([\w\s\d_.:/=+\-@]*)$").unwrap());

    let map = tags?;
    let mut errors: Vec<String> = Vec::new();
    for (idx, (key_s, value)) in map.iter().enumerate() {
        let idx = idx + 1;
        let key_s = key_s.clone();
        let value_s = value.as_str().unwrap_or_default();
        // len(key) validations come from botocore; moto checks the value side
        // of both members.
        if key_s.len() > 128 {
            errors.push(format!(
                "Value '{key_s}' at 'tags.{idx}.member.key' failed to satisfy constraint: Member must have length less than or equal to 128"
            ));
        }
        if !(!key_s.starts_with("aws:") && KEY_RE.is_match(&key_s)) {
            errors.push(format!(
                "Value '{key_s}' at 'tags.{idx}.member.key' failed to satisfy constraint: Member must satisfy regular expression pattern: ^(?!aws:)[{{a-zA-Z0-9 }}_.://=+-@%]*$"
            ));
        }
        if value_s.len() > 256 {
            errors.push(format!(
                "Value '{value_s}' at 'tags.{idx}.member.value' failed to satisfy constraint: Member must have length less than or equal to 256"
            ));
        }
        if !VALUE_RE.is_match(value_s) {
            errors.push(format!(
                "Value '{value_s}' at 'tags.{idx}.member.value' failed to satisfy constraint: Member must satisfy regular expression pattern: ^[{{a-zA-Z0-9 }}_.://=+-@%]*$"
            ));
        }
    }
    if errors.is_empty() {
        None
    } else {
        Some(format!(
            "{} validation error{} detected: {}",
            errors.len(),
            if errors.len() > 1 { "s" } else { "" },
            errors.join("; ")
        ))
    }
}

fn not_found(id: &str) -> Response {
    let e = json!({
        "__type": "ResourceNotFoundException",
        "message": format!("ResourceNotFoundException {id}"),
    });
    Response::json(404, serde_json::to_vec(&e).unwrap_or_default())
}

fn handle(req: &Request, st: &mut State, p: &Value, region: &str, account: u64) -> Response {
    match req.operation.as_str() {
        "CreateDbInstance" => {
            let dict = instance_dict(st, p, region, account, None);
            st.instances.push(dict.clone());
            ok(&dict)
        }
        "GetDbInstance" => {
            let id = p.get("identifier").and_then(Value::as_str).unwrap_or("");
            match find(&st.instances, id) {
                Some(inst) => ok(inst),
                None => not_found(id),
            }
        }
        "DeleteDbInstance" => {
            let id = p.get("identifier").and_then(Value::as_str).unwrap_or("");
            match find(&st.instances, id).cloned() {
                Some(mut inst) => {
                    inst["status"] = json!("DELETING");
                    st.instances.retain(|v| v["id"] != json!(id));
                    ok(&inst)
                }
                None => not_found(id),
            }
        }
        "ListDbInstances" => {
            let items: Vec<Value> = st
                .instances
                .iter()
                .map(|v| {
                    let mut summary = instance_summary(v);
                    summary["region"] = json!(region);
                    summary
                })
                .collect();
            ok(&json!({"items": items}))
        }
        "CreateDbCluster" => {
            let mut dict = instance_dict(st, p, region, account, None);
            let id = dict["id"].as_str().unwrap_or_default().to_string();
            dict["arn"] = json!(arn_of(region, account, "db-cluster", &id));
            st.clusters.push(dict.clone());
            ok(&dict)
        }
        "GetDbCluster" => {
            let id = p.get("dbClusterId").and_then(Value::as_str).unwrap_or("");
            match find(&st.clusters, id) {
                Some(c) => ok(c),
                None => not_found(id),
            }
        }
        "ListDbClusters" => {
            let items: Vec<Value> = st.clusters.iter().map(instance_summary).collect();
            ok(&json!({"items": items}))
        }
        "DeleteDbCluster" => {
            let id = p.get("dbClusterId").and_then(Value::as_str).unwrap_or("");
            match find(&st.clusters, id).cloned() {
                Some(mut c) => {
                    c["status"] = json!("DELETING");
                    st.clusters.retain(|v| v["id"] != json!(id));
                    ok(&c)
                }
                None => not_found(id),
            }
        }
        "CreateDbParameterGroup" => {
            let id = random_id();
            let dict = json!({
                "id": id,
                "name": p.get("name").cloned().unwrap_or(json!("")),
                "arn": arn_of(region, account, "db-parameter-group", &id),
                "description": p.get("description").cloned().unwrap_or(json!("")),
                "parameters": p.get("parameters").cloned().unwrap_or(json!({})),
            });
            st.param_groups.push(dict.clone());
            ok(&dict)
        }
        "ListDbParameterGroups" => {
            let items: Vec<Value> = st
                .param_groups
                .iter()
                .map(|g| {
                    json!({
                        "id": g["id"], "name": g["name"], "arn": g["arn"],
                        "description": g["description"],
                    })
                })
                .collect();
            ok(&json!({"items": items}))
        }
        "GetDbParameterGroup" => {
            let identifier = p.get("identifier").and_then(Value::as_str).unwrap_or("");
            match find(&st.param_groups, identifier) {
                Some(group) => ok(group),
                None => {
                    let e = json!({
                        "__type": "ResourceNotFoundException",
                        "message": format!(
                            "DB parameter group with identifier {identifier} not found"
                        ),
                    });
                    Response::json(400, serde_json::to_vec(&e).unwrap_or_default())
                }
            }
        }
        "TagResource" => {
            let arn = p.get("resourceArn").and_then(Value::as_str).unwrap_or("");
            if let Some(errmsg) = validate_tags(p.get("tags").and_then(|t| t.as_object())) {
                let e = json!({ "__type": "ValidationException", "message": errmsg });
                return Response::json(400, serde_json::to_vec(&e).unwrap_or_default());
            }
            let mut new_tags: Vec<(String, String)> = p
                .get("tags")
                .and_then(Value::as_object)
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                        .collect()
                })
                .unwrap_or_default();
            match st.tags.iter_mut().find(|(a, _)| a == arn) {
                Some((_, existing)) => {
                    for (k, v) in new_tags.drain(..) {
                        existing.retain(|(ek, _)| *ek != k);
                        existing.push((k, v));
                    }
                }
                None => st.tags.push((arn.to_string(), new_tags)),
            }
            ok(&json!({}))
        }
        "ListTagsForResource" => {
            let arn = p.get("resourceArn").and_then(Value::as_str).unwrap_or("");
            let tags: Vec<(String, String)> = st
                .tags
                .iter()
                .find(|(a, _)| a == arn)
                .map(|(_, v)| v.clone())
                .unwrap_or_default();
            let map: serde_json::Map<String, Value> = tags
                .into_iter()
                .map(|(k, v)| (k, Value::String(v)))
                .collect();
            ok(&json!({ "tags": Value::Object(map) }))
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(op: &str) -> Request {
        Request {
            service: "timestreaminfluxdb".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: Default::default(),
            body: bytes::Bytes::new(),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    #[test]
    fn create_get_delete_instance() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();
        let created = handle(
            &req("CreateDbInstance"),
            b,
            &json!({"name": "i1", "password": "pw", "username": "admin"}),
            "us-east-1",
            123456789012,
        );
        assert_eq!(created.status, 200);
        let v: Value = serde_json::from_slice(&created.body).unwrap();
        let id = v["id"].as_str().unwrap().to_string();
        assert!(v["arn"].as_str().is_some());

        let got = handle(
            &req("GetDbInstance"),
            b,
            &json!({"identifier": &id}),
            "us-east-1",
            123456789012,
        );
        assert_eq!(got.status, 200);

        let listed = handle(
            &req("ListDbInstances"),
            b,
            &json!({}),
            "us-east-1",
            123456789012,
        );
        let listed = serde_json::from_slice::<Value>(&listed.body).unwrap();
        assert_eq!(listed["items"].as_array().unwrap().len(), 1);

        handle(
            &req("DeleteDbInstance"),
            b,
            &json!({"identifier": &id}),
            "us-east-1",
            123456789012,
        );
        let missing = handle(
            &req("GetDbInstance"),
            b,
            &json!({"identifier": &id}),
            "us-east-1",
            123456789012,
        );
        assert_eq!(missing.status, 404);
    }
    #[test]
    fn param_group_tags_and_unknown_arns() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((223456789112, "us-east-1".into())).or_default();

        // GetDbParameterGroup: unknown identifier -> moto's 400 RNFE text.
        let resp = handle(
            &req("GetDbParameterGroup"),
            b,
            &json!({"identifier": "nope"}),
            "us-east-1",
            223456789112,
        );
        assert_eq!(resp.status, 400);
        let v: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["__type"], "ResourceNotFoundException");
        assert_eq!(
            v["message"],
            "DB parameter group with identifier nope not found"
        );

        // TagResource / ListTagsForResource: the ARN is not validated, tags
        // follow moto's arn-keyed store (unknown arn -> accepted).
        let arn = "arn:aws:timestream-influxdb:us-east-1:223456789112:db-instance/none";
        let resp = handle(
            &req("TagResource"),
            b,
            &json!({"resourceArn": arn, "tags": {"a": "b"}}),
            "us-east-1",
            223456789112,
        );
        assert_eq!(resp.status, 200);
        let resp = handle(
            &req("ListTagsForResource"),
            b,
            &json!({"resourceArn": arn}),
            "us-east-1",
            223456789112,
        );
        let v: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["tags"]["a"], "b");

        // aws:-prefixed keys are rejected with moto's ValidationException.
        let resp = handle(
            &req("TagResource"),
            b,
            &json!({"resourceArn": arn, "tags": {"aws:tag": "blocked"}}),
            "us-east-1",
            223456789112,
        );
        assert_eq!(resp.status, 400);
        let v: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["__type"], "ValidationException");
        assert!(v["message"]
            .as_str()
            .unwrap()
            .starts_with("1 validation error detected: Value 'aws:tag' at 'tags.1.member.key'"));

        // A parameter group created through the module answers cleanly.
        let created = handle(
            &req("CreateDbParameterGroup"),
            b,
            &json!({"name": "pg", "description": "d", "parameters": {}}),
            "us-east-1",
            223456789112,
        );
        let created: Value = serde_json::from_slice(&created.body).unwrap();
        let pid = created["id"].as_str().unwrap().to_string();
        let got = handle(
            &req("GetDbParameterGroup"),
            b,
            &json!({"identifier": &pid}),
            "us-east-1",
            223456789112,
        );
        assert_eq!(got.status, 200);
        let got: Value = serde_json::from_slice(&got.body).unwrap();
        assert_eq!(got["name"], "pg");
    }
}
