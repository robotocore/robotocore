//! Amazon Personalize backend (JSON protocol, target
//! `AmazonPersonalize.<Op>`).
//!
//! Port of moto's personalize: schema CRUD with lowercase camel JSON keys.
//! Remaining operations stay sidecar-backed.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};

#[derive(Default)]
struct State {
    /// schemaArn -> {name, schemaArn, domain, creationDateTime, schema?}
    schemas: Vec<Value>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("personalize"));
    SLOT.clone()
}

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "personalize" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(b, &body, &req.region, req.account, req))
}

fn schema_arn(region: &str, account: u64, name: &str) -> String {
    format!("arn:aws:personalize:{region}:{account}:schema/{name}")
}

fn handle(st: &mut State, p: &Value, region: &str, account: u64, req: &Request) -> Response {
    match req.operation.as_str() {
        "CreateSchema" => {
            let name = p
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("null")
                .to_string();
            let arn = schema_arn(region, account, &name);
            let mut entry = json!({
                "name": name,
                "schemaArn": &arn,
                "domain": p.get("domain").cloned().unwrap_or(Value::Null),
                "creationDateTime": crate::proto::now_iso(),
                "lastUpdatedDateTime": crate::proto::now_iso(),
            });
            if let Some(s) = p.get("schema") {
                entry["schema"] = s.clone();
            }
            st.schemas.push(entry);
            ok(&json!({"schemaArn": &arn}))
        }
        "DescribeSchema" => {
            let arn = p.get("schemaArn").and_then(Value::as_str).unwrap_or("");
            match st.schemas.iter().find(|s| s["schemaArn"] == json!(arn)) {
                Some(s) => {
                    let mut full = s.clone();
                    full["lastUpdatedDateTime"] = s["creationDateTime"].clone();
                    ok(&json!({"schema": full}))
                }
                None => {
                    let e = json!({
                        "__type": "ResourceNotFoundException",
                        "message": format!("ResourceNotFoundException {arn}"),
                    });
                    Response::json(400, serde_json::to_vec(&e).unwrap_or_default())
                }
            }
        }
        "ListSchemas" => {
            let schemas: Vec<Value> = st
                .schemas
                .iter()
                .map(|s| {
                    let mut summary = s.clone();
                    summary.as_object_mut().unwrap().remove("schema");
                    summary
                })
                .collect();
            ok(&json!({"schemas": schemas}))
        }
        "DeleteSchema" => {
            let arn = p.get("schemaArn").and_then(Value::as_str).unwrap_or("");
            let before = st.schemas.len();
            st.schemas.retain(|s| s["schemaArn"] != json!(arn));
            match before == st.schemas.len() {
                true => {
                    let e = json!({
                        "__type": "ResourceNotFoundException",
                        "message": format!("ResourceNotFoundException {arn}"),
                    });
                    Response::json(400, serde_json::to_vec(&e).unwrap_or_default())
                }
                false => ok(&json!({})),
            }
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
            service: "personalize".into(),
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
    fn create_list_describe() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();
        let created = handle(
            b,
            &json!({"name": "s1", "schema": {"type": "record"}, "domain": "ECOMMERCE"}),
            "us-east-1",
            123456789012,
            &req("CreateSchema"),
        );
        let arn = serde_json::from_slice::<Value>(&created.body).unwrap()["schemaArn"]
            .as_str()
            .unwrap()
            .to_string();

        let listed = handle(
            b,
            &json!({}),
            "us-east-1",
            123456789012,
            &req("ListSchemas"),
        );
        let listed = serde_json::from_slice::<Value>(&listed.body).unwrap();
        assert!(listed["schemas"].as_array().unwrap().len() == 1);
        assert!(listed["schemas"][0].get("schema").is_none());

        let described = handle(
            b,
            &json!({"schemaArn": &arn}),
            "us-east-1",
            123456789012,
            &req("DescribeSchema"),
        );
        let described = serde_json::from_slice::<Value>(&described.body).unwrap();
        assert_eq!(described["schema"]["schemaArn"], json!(arn));

        let missing = handle(
            b,
            &json!({"schemaArn": "arn:aws:personalize:us-east-1:123456789012:schema/nope"}),
            "us-east-1",
            123456789012,
            &req("DescribeSchema"),
        );
        assert_eq!(missing.status, 400);
    }
}
