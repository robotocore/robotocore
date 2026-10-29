//! Aurora DSQL backend (REST-JSON; path-per-op).
//!
//! Port of moto's dsql: cluster CRUD (26-hex identifier), VPC endpoint
//! names, cluster policies and tags. Wire shapes captured from moto's
//! `dsql` responses (lower-case camel JSON keys, 404 ResourceNotFound with
//! `resourceId`/`resourceType`).

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};

#[derive(Default)]
struct State {
    /// identifier -> cluster record; tags and policy ride along as values.
    clusters: Vec<(String, Value)>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("dsql"));
    SLOT.clone()
}

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

/// Cluster dict as moto renders it over the wire.
fn cluster_json(
    identifier: &str,
    arn: &str,
    region: &str,
    status: &str,
    stable: Option<&Value>,
) -> Value {
    let creation = stable
        .and_then(|c| c.get("creationTime"))
        .cloned()
        .unwrap_or_else(|| json!(now_iso()));
    json!({
        "identifier": identifier,
        "arn": arn,
        "status": status,
        "creationTime": creation,
        "deletionProtectionEnabled": stable
            .and_then(|c| c.get("deletionProtectionEnabled"))
            .cloned()
            .unwrap_or(json!(false)),
        "endpoint": format!("{identifier}.{region}.on.aws"),
        "encryptionDetails": {"encryptionType": "AWS_OWNED_KMS_KEY", "encryptionStatus": "ENABLED"},
    })
}

fn now_iso() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S%.6f+00:00")
        .to_string()
}

fn identifier_of(arn: &str) -> &str {
    arn.rsplit('/').next().unwrap_or("")
}

fn arn_for(region: &str, account: u64, id: &str) -> String {
    format!("arn:aws:dsql:{region}:{account}:cluster/{id}")
}

impl State {
    fn find(&self, id: &str) -> Option<&Value> {
        self.clusters.iter().find(|(i, _)| i == id).map(|(_, v)| v)
    }

    fn find_mut(&mut self, id: &str) -> Option<&mut Value> {
        self.clusters
            .iter_mut()
            .find(|(i, _)| i == id)
            .map(|(_, v)| v)
    }
}

fn not_found(arn: &str, cluster_id: &str) -> Response {
    let e = json!({
        "message": format!(
            "The resource with ARN {arn} doesn't exist. Verify the ARN and try again."
        ),
        "resourceId": cluster_id,
        "resourceType": "cluster",
    });
    let mut resp = Response::json(404, serde_json::to_vec(&e).unwrap_or_default());
    resp.headers.push((
        "x-amzn-ErrorType".to_string(),
        "ResourceNotFoundException:".to_string(),
    ));
    resp
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "dsql" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(req, b, &body))
}

fn handle(req: &Request, st: &mut State, p: &Value) -> Response {
    let region = req.region.as_str();
    let account = req.account;
    // /tags/{resourceArn} (GET list / POST tag / DELETE untag).
    if let Some(raw_arn) = req.path.strip_prefix("/tags/").filter(|s| !s.is_empty()) {
        let arn = crate::proto::url_decode(raw_arn);
        let id = identifier_of(&arn).to_string();
        if st.find(&id).is_none() {
            return not_found(&arn, &id);
        }
        return match req.method.as_str() {
            "GET" => {
                let tags = st
                    .find(&id)
                    .and_then(|c| c.get("tags"))
                    .cloned()
                    .unwrap_or(json!({}));
                ok(&json!({"tags": tags}))
            }
            "POST" => {
                if let Some(Value::Object(tags)) = p.get("tags") {
                    if let Some(c) = st.find_mut(&id) {
                        if !c.get("tags").map(Value::is_object).unwrap_or(false) {
                            c["tags"] = json!({});
                        }
                        for (k, v) in tags {
                            c["tags"][k.clone()] = v.clone();
                        }
                    }
                }
                ok(&json!({}))
            }
            "DELETE" => {
                let keys = tag_keys_from_query(&req.query_string);
                if let Some(c) = st.find_mut(&id) {
                    for k in keys {
                        c["tags"].as_object_mut().unwrap().remove(&k);
                    }
                }
                ok(&json!({}))
            }
            _ => ok(&json!({})),
        };
    }

    // Everything else lives under /cluster (or /clusters for vpc-endpoint
    // + list). Identify the remainder after the leading segment.
    let rest = req
        .path
        .trim_start_matches('/')
        .strip_prefix("cluster")
        .map(|r| r.trim_start_matches('s'))
        .unwrap_or("")
        .trim_start_matches('/')
        .to_string();
    let rest_path = rest.as_str();
    let method = req.method.as_str();

    // POST /cluster (Create)
    if rest_path.is_empty() && method == "POST" {
        let identifier = uuid::Uuid::new_v4().simple().to_string();
        let arn = arn_for(region, account, &identifier);
        let mut cluster = cluster_json(&identifier, &arn, region, "CREATING", None);
        if p.get("deletionProtectionEnabled").is_some() {
            cluster["deletionProtectionEnabled"] = p["deletionProtectionEnabled"].clone();
        }
        if p.get("tags").is_some() {
            cluster["tags"] = p["tags"].clone();
        }
        st.clusters.push((identifier.clone(), cluster.clone()));
        return ok(&cluster);
    }

    // GET /clusters => list all clusters.
    if rest_path.is_empty() && method != "POST" {
        let list: Vec<Value> = st
            .clusters
            .iter()
            .map(|(_, c)| {
                cluster_json(
                    c["identifier"].as_str().unwrap_or(""),
                    c["arn"].as_str().unwrap_or(""),
                    region,
                    "ACTIVE",
                    Some(c),
                )
            })
            .collect();
        return ok(&json!({"clusters": list, "nextToken": Value::Null}));
    }

    // {id}/vpc-endpoint-service-name
    if let Some(id) = rest_path.strip_suffix("/vpc-endpoint-service-name") {
        if st.find(id).is_none() {
            return not_found(&arn_for(region, account, id), id);
        }
        let endpoint = st
            .find(id)
            .and_then(|c| c.get("endpoint").and_then(Value::as_str))
            .unwrap_or("")
            .to_string();
        let svc = format!("com.amazonaws.{region}.dsql-7cwu");
        return ok(&json!({"serviceName": svc, "clusterVpcEndpoint": endpoint}));
    }

    // {id}/policy
    if let Some(id) = rest_path.strip_suffix("/policy") {
        if st.find(id).is_none() {
            return not_found(&arn_for(region, account, id), id);
        }
        return match method {
            "GET" => {
                let policy = st
                    .find(id)
                    .and_then(|c| c.get("policy").and_then(Value::as_str))
                    .unwrap_or("")
                    .to_string();
                ok(&json!({"policy": policy}))
            }
            "POST" => {
                let policy = p
                    .get("policy")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                if let Some(c) = st.find_mut(id) {
                    c["policy"] = json!(policy.clone());
                }
                ok(&json!({"policy": policy}))
            }
            _ => {
                if let Some(c) = st.find_mut(id) {
                    c["policy"] = json!("");
                }
                ok(&json!({"policy": ""}))
            }
        };
    }

    // {id} => get / delete / update cluster.
    if !rest_path.is_empty() {
        let id = rest_path;
        if st.find(id).is_none() {
            return not_found(&arn_for(region, account, id), id);
        }
        return match method {
            "GET" => {
                let c = st.find(id).unwrap().clone();
                ok(&cluster_json(
                    id,
                    c["arn"].as_str().unwrap_or(""),
                    region,
                    "ACTIVE",
                    Some(&c),
                ))
            }
            "DELETE" => {
                let c = st.find(id).unwrap().clone();
                st.clusters.retain(|(i, _)| i != id);
                let mut out = cluster_json(
                    id,
                    c["arn"].as_str().unwrap_or(""),
                    region,
                    "DELETING",
                    Some(&c),
                );
                // Delete presupposes ACTIVE state over the wire anyway.
                out["status"] = json!("DELETING");
                ok(&out)
            }
            "PATCH" | "POST" => {
                if let Some(d) = p.get("deletionProtectionEnabled") {
                    if let Some(c) = st.find_mut(id) {
                        c["deletionProtectionEnabled"] = d.clone();
                    }
                }
                let c = st.find(id).unwrap().clone();
                ok(&cluster_json(
                    id,
                    c["arn"].as_str().unwrap_or(""),
                    region,
                    "ACTIVE",
                    Some(&c),
                ))
            }
            _ => ok(&json!({})),
        };
    }

    ok(&json!({}))
}

fn tag_keys_from_query(query: &str) -> Vec<String> {
    query
        .split('&')
        .filter_map(|kv| kv.strip_prefix("tagKeys="))
        .map(crate::proto::url_decode)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(method: &str, path: &str, op: &str) -> Request {
        let (path, query) = match path.split_once('?') {
            Some((p, q)) => (p.to_string(), q.to_string()),
            None => (path.to_string(), String::new()),
        };
        Request {
            service: "dsql".into(),
            operation: op.into(),
            method: method.into(),
            path: path.clone(),
            query_string: query,
            headers: Default::default(),
            body: bytes::Bytes::new(),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    #[test]
    fn cluster_lifecycle() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();

        let created = handle(&req("POST", "/cluster", ""), b, &json!({}));
        let created = serde_json::from_slice::<Value>(&created.body).unwrap();
        let id = created["identifier"].as_str().unwrap().to_string();
        assert_eq!(created["status"], "CREATING");

        let got = handle(&req("GET", &format!("/cluster/{id}"), ""), b, &json!({}));
        let got = serde_json::from_slice::<Value>(&got.body).unwrap();
        assert_eq!(got["status"], "ACTIVE");
        assert_eq!(got["identifier"], json!(id));

        handle(&req("DELETE", &format!("/cluster/{id}"), ""), b, &json!({}));
        let gone = handle(&req("GET", &format!("/cluster/{id}"), ""), b, &json!({}));
        assert_eq!(gone.status, 404);
    }

    #[test]
    fn tag_roundtrip() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();
        let created = handle(
            &req("POST", "/cluster", ""),
            b,
            &json!({"tags": {"a": "b"}}),
        );
        let created = serde_json::from_slice::<Value>(&created.body).unwrap();
        let arn = created["arn"].as_str().unwrap().to_string();
        let id = created["identifier"].as_str().unwrap().to_string();

        handle(
            &req("POST", &format!("/tags/{arn}"), ""),
            b,
            &json!({"tags": {"c": "d"}}),
        );
        let listed = handle(&req("GET", &format!("/tags/{arn}"), ""), b, &json!({}));
        let listed = serde_json::from_slice::<Value>(&listed.body).unwrap();
        assert_eq!(listed["tags"]["a"], "b");
        assert_eq!(listed["tags"]["c"], "d");

        handle(
            &req("DELETE", &format!("/tags/{arn}?tagKeys=c"), ""),
            b,
            &json!({}),
        );
        let listed = handle(&req("GET", &format!("/tags/{arn}"), ""), b, &json!({}));
        let listed = serde_json::from_slice::<Value>(&listed.body).unwrap();
        assert!(listed["tags"].get("c").is_none());
        let _ = &id;
    }
}
