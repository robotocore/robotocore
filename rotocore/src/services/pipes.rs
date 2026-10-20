#![allow(dead_code)]

//! AWS EventBridge Pipes backend — ported from the py provider's pipes
//! engine (moto does not implement UpdatePipe; the native gateway does).
//!
//! Pipe state is (account, region)-scoped, name-keyed, and tags live
//! off-pipe (`_tags` excluded from describe bodies), matching moto's
//! shape. Create/Update/Start/Stop emit the six-key envelope plus
//! pipe-specific members pruned of nulls; Delete flips DELETING/DELETED.

use crate::store::{self};
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::HashMap;

static SLOT: Lazy<store::Slot<PipesState>> = Lazy::new(|| store::slot::<PipesState>("pipes"));

#[derive(Default)]
struct PipesState {
    pipes: HashMap<String, Value>,
    tags: HashMap<String, Value>,
    region: String,
    account: String,
}

pub fn dispatch(req: &Request) -> Response {
    let params = crate::proto::parse_json_body(&req.body);
    let slot = store::slot::<PipesState>("pipes");
    let mut guard = slot.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    if st.region.is_empty() {
        st.region = req.region.clone();
        st.account = req.account.to_string();
    }
    let path = req.path.trim_end_matches('/');
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    // /v1/pipes/{name}[/start|/stop], /v1/pipes, /tags/{arn}
    match (req.method.as_str(), segments.as_slice()) {
        ("POST", ["v1", "pipes"]) => st.op_create(&params, req.operation.as_str(), path),
        ("POST", ["v1", "pipes", _name]) => st.op_create(&params, req.operation.as_str(), path),
        ("POST", ["v1", "pipes", name, "start"]) => st.op_toggle(name, "start"),
        ("POST", ["v1", "pipes", name, "stop"]) => st.op_toggle(name, "stop"),
        ("GET", ["v1", "pipes"]) => st.op_list(req),
        ("GET", ["v1", "pipes", name]) => st.op_describe(name),
        ("PUT", ["v1", "pipes", name]) => st.op_update(name, &params),
        ("DELETE", ["v1", "pipes", name]) => st.op_delete(name),
        ("GET", ["tags", ..]) => st.op_list_tags(path),
        ("POST", ["tags", ..]) => st.op_tag(path, &params),
        ("DELETE", ["tags", ..]) => st.op_untag(req, path),
        _ => crate::proto::json_error(
            501,
            "NotImplemented",
            &format!("The {} action has not been implemented", req.operation),
        ),
    }
}

fn bad(status: u16, code: &str, message: &str) -> Response {
    Response::json_status(status, &json!({ "__type": code, "message": message }))
}

fn now_secs() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

fn six_time() -> f64 {
    // creation/last-modified echo, epoch floats like the py provider
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

impl PipesState {
    fn op_create(&mut self, p: &Value, op: &str, path: &str) -> Response {
        let name = p
            .get("Name")
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or_else(|| path.rsplit('/').next().unwrap_or("").to_string());
        let source = p.get("Source").cloned().unwrap_or(Value::Null);
        let target = p.get("Target").cloned().unwrap_or(Value::Null);
        let role = p.get("RoleArn").cloned().unwrap_or(Value::Null);
        if source.is_null() {
            return bad(400, "ValidationException", "Source is a required parameter");
        }
        if target.is_null() {
            return bad(400, "ValidationException", "Target is a required parameter");
        }
        if role.is_null() {
            return bad(
                400,
                "ValidationException",
                "RoleArn is a required parameter",
            );
        }
        let arn = format!(
            "arn:aws:pipes:{r}:{a}:pipe/{name}",
            r = self.region,
            a = self.account
        );
        let desired_state = p.get("DesiredState").cloned().unwrap_or(json!("RUNNING"));
        let mut pipe = json!({
            "Arn": arn,
            "Name": name.clone(),
            "DesiredState": desired_state,
            "CurrentState": "STARTING",
            "CreationTime": six_time(),
            "LastModifiedTime": six_time(),
            "Source": source,
            "Target": target,
            "RoleArn": role,
        });
        for (k, key) in [
            ("Description", "Description"),
            ("SourceParameters", "SourceParameters"),
            ("Enrichment", "Enrichment"),
            ("EnrichmentParameters", "EnrichmentParameters"),
            ("TargetParameters", "TargetParameters"),
            ("Tags", "Tags"),
            ("LogConfiguration", "LogConfiguration"),
            ("KmsKeyIdentifier", "KmsKeyIdentifier"),
        ] {
            if let Some(v) = p.get(k).filter(|v| !v.is_null()) {
                pipe[key] = v.clone();
            }
        }
        self.pipes.insert(name.clone(), pipe.clone());
        let _ = op;
        let _ = Map::new();
        Response::json_status(
            200,
            &json!({
                "Arn": pipe["Arn"],
                "Name": pipe["Name"],
                "DesiredState": pipe["DesiredState"],
                "CurrentState": pipe["CurrentState"],
                "CreationTime": pipe["CreationTime"],
                "LastModifiedTime": pipe["LastModifiedTime"],
            }),
        )
    }

    fn op_describe(&mut self, name: &str) -> Response {
        match self.pipes.get(name) {
            Some(pipe) => Response::json_status(200, &without_nulls_entries(pipe.clone())),
            None => bad(
                404,
                "NotFoundException",
                &format!("Pipe {name} does not exist."),
            ),
        }
    }

    fn op_update(&mut self, name: &str, p: &Value) -> Response {
        let Some(pipe) = self.pipes.get_mut(name) else {
            return bad(
                404,
                "NotFoundException",
                &format!("Pipe {name} does not exist."),
            );
        };
        for key in [
            "SourceParameters",
            "Enrichment",
            "EnrichmentParameters",
            "Target",
            "TargetParameters",
            "RoleArn",
            "Description",
            "DesiredState",
        ] {
            if let Some(v) = p.get(key).filter(|v| !v.is_null()) {
                pipe[key] = v.clone();
            }
        }
        pipe["LastModifiedTime"] = json!(now_secs());
        // state machine from the py provider
        let desired = pipe["DesiredState"]
            .as_str()
            .unwrap_or("RUNNING")
            .to_string();
        let current = pipe["CurrentState"]
            .as_str()
            .unwrap_or("STOPPED")
            .to_string();
        if desired == "RUNNING" && current != "RUNNING" {
            pipe["CurrentState"] = json!("RUNNING");
        } else if desired == "STOPPED" && current == "RUNNING" {
            pipe["CurrentState"] = json!("STOPPED");
        }
        Response::json_status(
            200,
            &json!({
                "Name": pipe["Name"],
                "Arn": pipe["Arn"],
                "DesiredState": pipe["DesiredState"],
                "CurrentState": pipe["CurrentState"],
                "CreationTime": pipe["CreationTime"],
                "LastModifiedTime": pipe["LastModifiedTime"],
            }),
        )
    }

    fn op_toggle(&mut self, name: &str, dir: &str) -> Response {
        let Some(pipe) = self.pipes.get_mut(name) else {
            return bad(
                404,
                "NotFoundException",
                &format!("Pipe {name} does not exist."),
            );
        };
        if dir == "start" {
            pipe["DesiredState"] = json!("RUNNING");
            pipe["CurrentState"] = json!("RUNNING");
        } else {
            pipe["DesiredState"] = json!("STOPPED");
            pipe["CurrentState"] = json!("STOPPED");
        }
        pipe["LastModifiedTime"] = json!(now_secs());
        Response::json_status(
            200,
            &json!({
                "Arn": pipe["Arn"],
                "Name": pipe["Name"],
                "DesiredState": pipe["DesiredState"],
                "CurrentState": pipe["CurrentState"],
                "CreationTime": pipe["CreationTime"],
                "LastModifiedTime": pipe["LastModifiedTime"],
            }),
        )
    }

    fn op_delete(&mut self, name: &str) -> Response {
        let Some(mut pipe) = self.pipes.remove(name) else {
            return bad(
                404,
                "NotFoundException",
                &format!("Pipe {name} does not exist."),
            );
        };
        pipe["CurrentState"] = json!("DELETING");
        Response::json_status(
            200,
            &json!({
                "Name": pipe["Name"],
                "Arn": pipe["Arn"],
                "DesiredState": "DELETED",
                "CurrentState": "DELETING",
                "CreationTime": pipe["CreationTime"],
                "LastModifiedTime": pipe["LastModifiedTime"],
            }),
        )
    }

    fn op_list(&mut self, req: &Request) -> Response {
        // list_pipes filters arrive as rest-json POST/GET; accept a JSON body
        // first, then query params, mirroring moto's caster order.
        let mut params = IndexMap::new();
        if !req.body.is_empty() {
            if let Ok(Value::Object(body)) = serde_json::from_slice::<Value>(&req.body) {
                for (k, v) in body {
                    if let Some(s) = v.as_str() {
                        params.insert(&k, s.to_string());
                    }
                }
            }
        }
        for kv in req.query_string.split('&') {
            if let Some((k, v)) = kv.split_once('=') {
                if !k.is_empty() && !params.contains(k) {
                    params.insert(k, urldecode(v));
                }
            }
        }
        let wanted = |prefix_key: &str, field: &str, pipe: &Value| -> bool {
            if let Some(expect) = params.get(prefix_key) {
                return prefix_matches(
                    pipe[field].as_str().unwrap_or_default(),
                    expect.as_str(),
                    prefix_key,
                );
            }
            true
        };
        let mut items: Vec<Value> = self
            .pipes
            .values()
            .filter(|pipe| {
                let _name = pipe["Name"].as_str().unwrap_or_default().to_string();
                // `wanted` returns true when its key is absent, so the two
                // casings compose with `&&` — an `||` short-circuits the
                // filter to always-pass.
                if !(wanted("NamePrefix", "Name", pipe) && wanted("namePrefix", "Name", pipe)) {
                    return false;
                }
                wanted("DesiredState", "DesiredState", pipe)
                    && wanted("desiredState", "DesiredState", pipe)
                    && wanted("CurrentState", "CurrentState", pipe)
                    && wanted("currentState", "CurrentState", pipe)
                    && wanted("SourcePrefix", "Source", pipe)
                    && wanted("TargetPrefix", "Target", pipe)
            })
            .map(|pipe| {
                json!({
                    "Arn": pipe["Arn"],
                    "Name": pipe["Name"],
                    "DesiredState": pipe["DesiredState"],
                    "CurrentState": pipe["CurrentState"],
                    "CreationTime": pipe["CreationTime"],
                    "LastModifiedTime": pipe["LastModifiedTime"],
                })
            })
            .collect();
        items.sort_by(|a, b| {
            let an = a["Name"].as_str().unwrap_or_default();
            let bn = b["Name"].as_str().unwrap_or_default();
            an.cmp(bn)
        });
        Response::json_status(200, &json!({"Pipes": items}))
    }

    fn op_list_tags(&mut self, path: &str) -> Response {
        let arn = urldecode(path.strip_prefix("/tags/").unwrap_or(path));
        let Some(pipe) = self
            .pipes
            .values()
            .find(|p| p["Arn"].as_str() == Some(&arn))
        else {
            return bad(
                404,
                "NotFoundException",
                &format!("Pipe does not exist: {arn}"),
            );
        };
        Response::json_status(
            200,
            &json!({"tags": pipe.get("Tags").cloned().unwrap_or(json!({}))}),
        )
    }

    fn op_tag(&mut self, path: &str, p: &Value) -> Response {
        let arn = urldecode(path.strip_prefix("/tags/").unwrap_or(path));
        let Some(pipe) = self
            .pipes
            .values_mut()
            .find(|p| p["Arn"].as_str() == Some(&arn))
        else {
            return bad(
                404,
                "NotFoundException",
                &format!("Pipe does not exist: {arn}"),
            );
        };
        let Some(tags) = p.get("Tags").or_else(|| p.get("tags")) else {
            return bad(400, "ValidationException", "Tags is a required parameter");
        };
        if tags.as_object().is_some_and(|o| o.is_empty()) {
            return bad(400, "ValidationException", "Tags is a required parameter");
        }
        pipe["Tags"] = tags.clone();
        Response::json_status(200, &json!({}))
    }

    fn op_untag(&mut self, req: &Request, path: &str) -> Response {
        let arn = urldecode(
            path.strip_prefix("/tags/")
                .unwrap_or(path)
                .split('?')
                .next()
                .unwrap_or(""),
        );
        let keys: Vec<String> = req
            .query_string
            .split('&')
            .filter_map(|kv| kv.strip_prefix("tagKeys="))
            .map(|v| urldecode(v).trim_matches('"').to_string())
            .collect();
        let Some(pipe) = self
            .pipes
            .values_mut()
            .find(|p| p["Arn"].as_str() == Some(&arn))
        else {
            return bad(
                404,
                "NotFoundException",
                &format!("Pipe does not exist: {arn}"),
            );
        };
        if let Some(tags) = pipe["Tags"].as_object_mut() {
            for k in keys {
                tags.remove(&k);
            }
        }
        Response::json_status(200, &json!({}))
    }
}

fn crate_json_body_field<'a>(_req: &'a Request, _key: &str) -> Option<&'a str> {
    None
}

fn urldecode(input: &str) -> String {
    let mut out = String::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '%' if i + 2 < chars.len() => {
                let hex: String = chars[i + 1..i + 3].iter().collect();
                if let Ok(v) = u8::from_str_radix(&hex, 16) {
                    out.push(v as char);
                    i += 3;
                } else {
                    out.push('%');
                    i += 1;
                }
            }
            '+' => {
                out.push(' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    out
}

fn without_nulls_entries(pipe: Value) -> Value {
    match pipe {
        Value::Object(map) => {
            let mut out = Map::new();
            for (k, v) in map {
                let keep = match &v {
                    Value::Null => false,
                    Value::String(s) => !s.is_empty(),
                    _ => true,
                };
                if keep {
                    out.insert(k, v);
                }
            }
            Value::Object(out)
        }
        o => o,
    }
}

/// Prefix params use startswith; state filters use equality (the py
/// provider's semantics).
fn prefix_matches(value: &str, expect: &str, key: &str) -> bool {
    if key.ends_with("Prefix") {
        value.starts_with(expect)
    } else {
        value == expect
    }
}

/// BTreeMap-backed ordered string map for query params.
struct IndexMap(std::collections::BTreeMap<String, String>);

impl IndexMap {
    fn new() -> Self {
        Self(std::collections::BTreeMap::new())
    }

    fn insert(&mut self, k: &str, v: String) {
        if let std::collections::btree_map::Entry::Vacant(e) = self.0.entry(k.to_string()) {
            e.insert(v);
        }
    }

    fn contains(&self, k: &str) -> bool {
        self.0.contains_key(k)
    }

    fn get(&self, k: &str) -> Option<String> {
        self.0.get(k).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as StdMap;

    fn request(method: &str, path: &str, body: Value, query: &str) -> Request {
        Request {
            service: "pipes".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: query.into(),
            headers: StdMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    #[test]
    fn create_describe_update_start_stop_delete() {
        let r = dispatch(&request(
            "POST",
            "/v1/pipes/p1",
            json!({"Name": "p1", "Source": "sqs", "Target": "fn", "RoleArn": "arn:role"}),
            "",
        ));
        assert_eq!(r.status, 200);
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        assert!(v["Arn"].as_str().is_some());
        assert_eq!(v["DesiredState"], "RUNNING");

        let r = dispatch(&request("GET", "/v1/pipes/p1", json!({}), ""));
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(v["Target"], "fn");

        let r = dispatch(&request(
            "PUT",
            "/v1/pipes/p1",
            json!({"Description": "upd", "DesiredState": "STOPPED"}),
            "",
        ));
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(v["DesiredState"], "STOPPED");

        let r = dispatch(&request("POST", "/v1/pipes/p1/start", json!({}), ""));
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(v["CurrentState"], "RUNNING");

        let r = dispatch(&request("GET", "/v1/pipes", json!({}), ""));
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(v["Pipes"].as_array().unwrap().len(), 1);

        let r = dispatch(&request("DELETE", "/v1/pipes/p1", json!({}), ""));
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(v["DesiredState"], "DELETED");

        let r = dispatch(&request("GET", "/v1/pipes/p1", json!({}), ""));
        assert_eq!(r.status, 404);
    }

    #[test]
    fn unknown_route_is_not_implemented() {
        let r = dispatch(&request("POST", "/v1/unknown", json!({}), ""));
        assert_eq!(r.status, 501);
    }
}
