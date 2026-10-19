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
}
