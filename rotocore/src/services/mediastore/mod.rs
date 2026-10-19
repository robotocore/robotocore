//! AWS Elemental MediaStore backend — moto-equivalent (json protocol).
//!
//! Replaces the moto sidecar for `mediastore` (`X-Amz-Target:
//! MediaStore_20170901.<Op>` on `application/x-amz-json-1.1`). State is
//! per-(account, region), mirroring moto's `MediaStoreBackend`
//! (moto/mediastore): containers keyed by name, each carrying its container
//! policy, lifecycle policy, CORS policy, metric policy, access-logging
//! flag and the tag list.
//!
//! Contract: probes/mediastore.json and tests/compatibility/
//! test_mediastore_compat.py. moto's quirks are kept verbatim:
//! - `DescribeContainer` flips the stored status CREATING → ACTIVE.
//! - `PutMetricPolicy` returns the metric policy itself as the bare body
//!   (moto's `json.dumps(metric_policy)`), not `{}`.
//! - `CreationTime` is a `%m/%d/%Y, 00:00:00` string — moto formats a bare
//!   `date`, so the time part is always zero.
//! - Policy lookups resolve by name or ARN, and raise
//!   `ResourceNotFoundException` on the policy put/get paths but
//!   `ContainerNotFoundException` everywhere else.
//! - `ListContainers` always answers `{"Containers": [...], "NextToken": null}`
//!   (moto does not implement pagination).

use crate::proto::{json_error, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, MediaStoreStore>>>;

fn stores() -> Stores {
    store::slot::<MediaStoreStore>("mediastore")
}

/// Per-(account, region) in-memory state for AWS Elemental MediaStore.
#[derive(Default)]
struct MediaStoreStore {
    /// container name -> container
    containers: BTreeMap<String, Container>,
}

struct Container {
    arn: String,
    name: String,
    endpoint: String,
    status: String,
    creation_time: String,
    policy: Option<String>,
    lifecycle_policy: Option<String>,
    metric_policy: Option<String>,
    cors_policy: Option<Vec<Value>>,
    access_logging_enabled: bool,
    tags: Vec<Value>,
}

impl Container {
    /// moto's `create_container` stub: `CREATING` with a formatted `date`
    /// and an endpoint of `/{name}`.
    fn new(account: u64, region: &str, name: &str, tags: Vec<Value>) -> Self {
        Container {
            arn: format!(
                "arn:{}:mediastore:{region}:{account}:container/{name}",
                partition(region),
            ),
            name: name.to_string(),
            endpoint: format!("/{name}"),
            status: "CREATING".to_string(),
            creation_time: moto_date_today(),
            policy: None,
            lifecycle_policy: None,
            metric_policy: None,
            cors_policy: None,
            access_logging_enabled: false,
            tags,
        }
    }

    /// moto's `Container.to_dict()`: ARN, Name, Endpoint, Status,
    /// CreationTime and the raw tags — no policy members, no
    /// access-logging flag.
    fn to_dict(&self) -> Value {
        json!({
            "ARN": self.arn,
            "Name": self.name,
            "Endpoint": self.endpoint,
            "Status": self.status,
            "CreationTime": self.creation_time,
            "Tags": self.tags,
        })
    }
}

/// moto's `get_partition` for ARN partitions.
fn partition(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else if region.starts_with("us-iso-") {
        "aws-iso"
    } else if region.starts_with("us-isob-") {
        "aws-iso-b"
    } else if region.starts_with("eusc-") {
        "aws-eusc"
    } else {
        "aws"
    }
}

/// moto stores `date.today().strftime("%m/%d/%Y, %H:%M:%S")` — a bare date
/// formats the time part as zeros, so the wire value ends in `, 00:00:00`.
fn moto_date_today() -> String {
    format!("{}, 00:00:00", chrono::Local::now().format("%m/%d/%Y"))
}

impl MediaStoreStore {
    /// Resolve a container by name or ARN (moto's `_resolve_resource`).
    fn resolve(&mut self, name_or_arn: &str) -> Option<&mut Container> {
        if self.containers.contains_key(name_or_arn) {
            return self.containers.get_mut(name_or_arn);
        }
        self.containers.values_mut().find(|c| c.arn == name_or_arn)
    }
}

fn with_store(req: &Request, f: impl FnOnce(&mut MediaStoreStore) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    f(st)
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "mediastore" {
        return None;
    }
    Some(with_store(req, |st| route(req, st)))
}

fn route(req: &Request, st: &mut MediaStoreStore) -> Response {
    let body = parse_json_body(&req.body);
    let name = as_str(&body, "ContainerName");
    match req.operation.as_str() {
        "CreateContainer" => create_container(req, st, &body),
        "DeleteContainer" => {
            if st.containers.remove(name).is_some() {
                ok_empty()
            } else {
                container_not_found()
            }
        }
        "DescribeContainer" => describe_container(st, name),
        "ListContainers" => {
            let list: Vec<Value> = st.containers.values().map(|c| c.to_dict()).collect();
            ok(json!({ "Containers": list, "NextToken": null }))
        }
        // --- policies ---------------------------------------------------------
        "PutContainerPolicy" => with_policy_container(st, name, |c| {
            c.policy = policy(body.get("Policy"));
            ok_empty()
        }),
        "GetContainerPolicy" => with_policy_container(st, name, |c| match &c.policy {
            Some(p) => ok(json!({ "Policy": p })),
            None => policy_not_found(),
        }),
        "PutLifecyclePolicy" => with_policy_container(st, name, |c| {
            c.lifecycle_policy = policy(body.get("LifecyclePolicy"));
            ok_empty()
        }),
        "GetLifecyclePolicy" => with_policy_container(st, name, |c| match &c.lifecycle_policy {
            Some(p) => ok(json!({ "LifecyclePolicy": p })),
            None => policy_not_found(),
        }),
        "PutMetricPolicy" => with_policy_container(st, name, |c| {
            let metric_policy = body.get("MetricPolicy").cloned().unwrap_or(Value::Null);
            // moto stores the json-dumped policy and answers the put with
            // `json.dumps(metric_policy)` — the policy as the bare body.
            c.metric_policy = Some(metric_policy.to_string());
            ok(metric_policy)
        }),
        "GetMetricPolicy" => with_policy_container(st, name, |c| match &c.metric_policy {
            Some(p) => ok(json!({ "MetricPolicy": json_from_str(p) })),
            None => policy_not_found(),
        }),
        "DeleteContainerPolicy" => with_container(st, name, |c| {
            c.policy = None;
            ok_empty()
        }),
        "DeleteLifecyclePolicy" => with_container(st, name, |c| {
            c.lifecycle_policy = None;
            ok_empty()
        }),
        "DeleteMetricPolicy" => with_container(st, name, |c| {
            c.metric_policy = None;
            ok_empty()
        }),
        // --- CORS -------------------------------------------------------------
        "PutCorsPolicy" => with_container(st, name, |c| {
            c.cors_policy = Some(
                body.get("CorsPolicy")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default(),
            );
            ok_empty()
        }),
        "GetCorsPolicy" => with_container(st, name, |c| match &c.cors_policy {
            Some(list) => ok(json!({ "CorsPolicy": list })),
            None => policy_not_found(),
        }),
        "DeleteCorsPolicy" => with_container(st, name, |c| {
            c.cors_policy = None;
            ok_empty()
        }),
        // --- access logging -----------------------------------------------------
        "StartAccessLogging" => with_container(st, name, |c| {
            c.access_logging_enabled = true;
            ok_empty()
        }),
        "StopAccessLogging" => with_container(st, name, |c| {
            c.access_logging_enabled = false;
            ok_empty()
        }),
        // --- tags -----------------------------------------------------------------
        "ListTagsForResource" => match st.resolve(as_str(&body, "Resource")) {
            Some(c) => ok(json!({ "Tags": c.tags })),
            None => container_not_found(),
        },
        "TagResource" => match st.resolve(as_str(&body, "Resource")) {
            Some(c) => {
                tag_resource(
                    c,
                    body.get("Tags")
                        .and_then(Value::as_array)
                        .map(Vec::as_slice)
                        .unwrap_or(&[]),
                );
                ok_empty()
            }
            None => container_not_found(),
        },
        "UntagResource" => match st.resolve(as_str(&body, "Resource")) {
            Some(c) => {
                untag_resource(
                    c,
                    body.get("TagKeys")
                        .and_then(Value::as_array)
                        .map(Vec::as_slice)
                        .unwrap_or(&[]),
                );
                ok_empty()
            }
            None => container_not_found(),
        },
        other => json_error(
            501,
            "NotImplemented",
            &format!("The {other} action has not been implemented"),
        ),
    }
}

// ---------------------------------------------------------------------------
// Operation helpers
// ---------------------------------------------------------------------------

fn create_container(req: &Request, st: &mut MediaStoreStore, body: &Value) -> Response {
    let name = as_str(body, "ContainerName").to_string();
    // moto: `tags = kwargs.get("tags") or []`.
    let tags = body
        .get("Tags")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let container = Container::new(req.account, &req.region, &name, tags);
    let dict = container.to_dict();
    st.containers.insert(name.to_string(), container);
    ok(json!({ "Container": dict }))
}

/// `DescribeContainer`: moto sets the container `ACTIVE` for good.
fn describe_container(st: &mut MediaStoreStore, name: &str) -> Response {
    match st.containers.get_mut(name) {
        Some(c) => {
            c.status = "ACTIVE".to_string();
            let dict = c.to_dict();
            ok(json!({ "Container": dict }))
        }
        None => resource_not_found(),
    }
}

/// Policy get/put paths: moto raises `ResourceNotFoundException` for a
/// missing container.
fn with_policy_container(
    st: &mut MediaStoreStore,
    name: &str,
    f: impl FnOnce(&mut Container) -> Response,
) -> Response {
    match st.containers.get_mut(name) {
        Some(c) => f(c),
        None => resource_not_found(),
    }
}

/// Delete/logging/tags routes: moto raises `ContainerNotFoundException`.
fn with_container(
    st: &mut MediaStoreStore,
    name_or_arn: &str,
    f: impl FnOnce(&mut Container) -> Response,
) -> Response {
    match st.resolve(name_or_arn) {
        Some(c) => f(c),
        None => container_not_found(),
    }
}

/// Accept a policy string as sent, falling back to the JSON encoding for
/// anything a client sent that is not a string.
fn policy(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(s)) => Some(s.clone()),
        Some(other) => Some(other.to_string()),
        None => None,
    }
}

fn json_from_str(s: &str) -> Value {
    serde_json::from_str(s).unwrap_or(Value::Null)
}

/// moto's tag merge: keys keep their first-insertion position; a repeated
/// key updates its value in place.
fn tag_resource(container: &mut Container, tags: &[Value]) {
    let mut pairs: Vec<(String, Value)> = Vec::new();
    for tag in container.tags.iter().chain(tags.iter()) {
        let key = tag.get("Key").and_then(Value::as_str);
        let Some(key) = key else { continue };
        let value = tag.get("Value").cloned().unwrap_or(Value::Null);
        if let Some(pos) = pairs.iter().position(|(k, _)| k == key) {
            pairs[pos].1 = value;
        } else {
            pairs.push((key.to_string(), value));
        }
    }
    container.tags = pairs
        .into_iter()
        .map(|(k, v)| json!({ "Key": k, "Value": v }))
        .collect();
}

fn untag_resource(container: &mut Container, keys: &[Value]) {
    let drop: Vec<&str> = keys.iter().filter_map(Value::as_str).collect();
    container.tags.retain(|t| {
        !t.get("Key")
            .and_then(Value::as_str)
            .map(|k| drop.contains(&k))
            .unwrap_or(false)
    });
}

fn as_str<'a>(body: &'a Value, key: &str) -> &'a str {
    body.get(key).and_then(Value::as_str).unwrap_or("")
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

fn ok_empty() -> Response {
    ok(json!({}))
}

/// moto's `ContainerNotFoundException` (delete / logging / tag routes).
fn container_not_found() -> Response {
    json_error(
        400,
        "ContainerNotFoundException",
        "The specified container does not exist",
    )
}

/// moto's `ResourceNotFoundException` (policy put/get paths, describe).
fn resource_not_found() -> Response {
    json_error(
        400,
        "ResourceNotFoundException",
        "The specified container does not exist",
    )
}

/// moto's `PolicyNotFoundException` (including its `specfied` typo).
fn policy_not_found() -> Response {
    json_error(
        400,
        "PolicyNotFoundException",
        "The policy does not exist within the specfied container",
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU64, Ordering};

    static ACCOUNTS: AtomicU64 = AtomicU64::new(200_000_000_001);

    /// Every test takes a fresh account so the shared store stays isolated.
    fn next_account() -> u64 {
        ACCOUNTS.fetch_add(1, Ordering::SeqCst)
    }

    fn send(account: u64, operation: &str, body: Value) -> (u16, Value) {
        let request = Request {
            service: "mediastore".to_string(),
            operation: operation.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&request).expect("mediastore dispatch");
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(json!({})),
        )
    }

    fn create(account: u64, name: &str) -> Value {
        let (status, body) = send(account, "CreateContainer", json!({ "ContainerName": name }));
        assert_eq!(status, 200, "create_container failed: {body}");
        body["Container"].clone()
    }

    #[test]
    fn container_lifecycle() {
        let account = next_account();
        let container = create(account, "c-1");

        assert_eq!(container["Name"], "c-1");
        assert_eq!(container["Status"], "CREATING");
        assert!(container["ARN"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:mediastore:us-east-1:"));
        assert!(container["ARN"].as_str().unwrap().ends_with("/c-1"));
        assert_eq!(container["Endpoint"], "/c-1");
        assert_eq!(container["CreationTime"].as_str().unwrap(), {
            format!("{}, 00:00:00", chrono::Local::now().format("%m/%d/%Y"))
        });
        assert_eq!(container["Tags"], json!([]));

        // Describe flips the status to ACTIVE for good.
        let (status, body) = send(
            account,
            "DescribeContainer",
            json!({ "ContainerName": "c-1" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Container"]["Status"], "ACTIVE");

        let (status, body) = send(account, "ListContainers", json!({}));
        assert_eq!(status, 200);
        let names: Vec<&str> = body["Containers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["Name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"c-1"));
        assert!(body.get("NextToken").unwrap().is_null());

        let (status, _body) = send(
            account,
            "DeleteContainer",
            json!({ "ContainerName": "c-1" }),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "DescribeContainer",
            json!({ "ContainerName": "c-1" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn missing_containers_raise_two_distinct_errors() {
        let account = next_account();
        // Describe/policy get+put use ResourceNotFoundException …
        let (_status, body) = send(
            account,
            "DescribeContainer",
            json!({ "ContainerName": "gone" }),
        );
        assert_eq!(body["__type"], "ResourceNotFoundException");
        assert_eq!(body["message"], "The specified container does not exist");
        let (status, _) = send(
            account,
            "PutContainerPolicy",
            json!({ "ContainerName": "gone", "Policy": "{}" }),
        );
        assert_eq!(status, 400);
        // … while delete/logging/tag routes use ContainerNotFoundException.
        for (operation, params) in [
            ("DeleteContainer", json!({ "ContainerName": "gone" })),
            ("DeleteContainerPolicy", json!({ "ContainerName": "gone" })),
            ("DeleteLifecyclePolicy", json!({ "ContainerName": "gone" })),
            ("DeleteMetricPolicy", json!({ "ContainerName": "gone" })),
            ("StartAccessLogging", json!({ "ContainerName": "gone" })),
            ("StopAccessLogging", json!({ "ContainerName": "gone" })),
            ("ListTagsForResource", json!({ "Resource": "gone" })),
            (
                "ListTagsForResource",
                json!({ "Resource": "arn:aws:mediastore:us-east-1:1:container/gone" }),
            ),
        ] {
            let (status, body) = send(account, operation, params);
            assert_eq!(status, 400, "{operation}: {body}");
            assert_eq!(body["__type"], "ContainerNotFoundException", "{operation}");
        }
    }

    #[test]
    fn container_policy_round_trip_and_policy_not_found() {
        let account = next_account();
        create(account, "c-1");

        let (status, body) = send(
            account,
            "GetContainerPolicy",
            json!({ "ContainerName": "c-1" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "PolicyNotFoundException");
        assert_eq!(
            body["message"],
            "The policy does not exist within the specfied container"
        );

        let (status, _) = send(
            account,
            "PutContainerPolicy",
            json!({ "ContainerName": "c-1", "Policy": "{\"Version\":\"2012-10-17\"}" }),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "GetContainerPolicy",
            json!({ "ContainerName": "c-1" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Policy"], "{\"Version\":\"2012-10-17\"}");

        // Delete clears the policy (no error with a real container — as in moto).
        let (status, _) = send(
            account,
            "DeleteContainerPolicy",
            json!({ "ContainerName": "c-1" }),
        );
        assert_eq!(status, 200);
        let (_status, body) = send(
            account,
            "GetContainerPolicy",
            json!({ "ContainerName": "c-1" }),
        );
        assert_eq!(body["__type"], "PolicyNotFoundException");
    }

    #[test]
    fn lifecycle_and_metric_policy_round_trips() {
        let account = next_account();
        create(account, "c-1");

        let (status, _) = send(
            account,
            "PutLifecyclePolicy",
            json!({ "ContainerName": "c-1", "LifecyclePolicy": "{\"rules\":[{\"action\":\"EXPIRE\"}]}" }),
        );
        assert_eq!(status, 200);
        let (_status, body) = send(
            account,
            "GetLifecyclePolicy",
            json!({ "ContainerName": "c-1" }),
        );
        assert_eq!(
            body["LifecyclePolicy"],
            "{\"rules\":[{\"action\":\"EXPIRE\"}]}"
        );

        // moto echoes the metric policy as the bare response body.
        let (status, body) = send(
            account,
            "PutMetricPolicy",
            json!({ "ContainerName": "c-1", "MetricPolicy": { "ContainerLevelMetrics": "ENABLED" } }),
        );
        assert_eq!(status, 200);
        assert_eq!(body, json!({ "ContainerLevelMetrics": "ENABLED" }));
        let (_status, body) = send(
            account,
            "GetMetricPolicy",
            json!({ "ContainerName": "c-1" }),
        );
        assert_eq!(body["MetricPolicy"]["ContainerLevelMetrics"], "ENABLED");

        // Getting another container's policies 404s with PolicyNotFound once
        // the container exists but has no policy.
        create(account, "c-2");
        let (status, body) = send(
            account,
            "GetLifecyclePolicy",
            json!({ "ContainerName": "c-2" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "PolicyNotFoundException");
        let (_status, body) = send(
            account,
            "GetMetricPolicy",
            json!({ "ContainerName": "c-2" }),
        );
        assert_eq!(body["__type"], "PolicyNotFoundException");
    }

    #[test]
    fn cors_policy_round_trip() {
        let account = next_account();
        create(account, "c-1");

        let cors = json!([{
            "AllowedOrigins": ["https://example.com"],
            "AllowedMethods": ["GET", "HEAD"],
            "MaxAgeSeconds": 3000,
        }]);
        let (status, _) = send(
            account,
            "PutCorsPolicy",
            json!({ "ContainerName": "c-1", "CorsPolicy": cors }),
        );
        assert_eq!(status, 200);
        let (status, body) = send(account, "GetCorsPolicy", json!({ "ContainerName": "c-1" }));
        assert_eq!(status, 200);
        assert_eq!(
            body["CorsPolicy"][0]["AllowedMethods"],
            json!(["GET", "HEAD"])
        );

        let (status, _) = send(
            account,
            "DeleteCorsPolicy",
            json!({ "ContainerName": "c-1" }),
        );
        assert_eq!(status, 200);
        let (_status, body) = send(account, "GetCorsPolicy", json!({ "ContainerName": "c-1" }));
        assert_eq!(body["__type"], "PolicyNotFoundException");

        // Unknown container: ContainerNotFoundException, not PolicyNotFound.
        let (_status, body) = send(account, "GetCorsPolicy", json!({ "ContainerName": "gone" }));
        assert_eq!(body["__type"], "ContainerNotFoundException");
    }

    #[test]
    fn tags_round_trip_with_merge_and_untag() {
        let account = next_account();
        let container = create(account, "t-1");
        assert_eq!(container["Tags"], json!([]));
        let arn = container["ARN"].as_str().unwrap().to_string();

        // Tags by name and by ARN behave the same.
        let (status, _) = send(
            account,
            "TagResource",
            json!({ "Resource": arn, "Tags": [{"Key": "env", "Value": "test"}] }),
        );
        assert_eq!(status, 200);
        let (status, _) = send(
            account,
            "TagResource",
            json!({ "Resource": "t-1", "Tags": [{"Key": "project", "Value": "compat"}, {"Key": "env", "Value": "prod"}] }),
        );
        assert_eq!(status, 200);

        let (status, body) = send(account, "ListTagsForResource", json!({ "Resource": arn }));
        assert_eq!(status, 200);
        assert_eq!(
            body["Tags"],
            json!([{"Key": "env", "Value": "prod"}, {"Key": "project", "Value": "compat"}]),
        );

        let (status, _) = send(
            account,
            "UntagResource",
            json!({ "Resource": arn, "TagKeys": ["env"] }),
        );
        assert_eq!(status, 200);
        let (_status, body) = send(account, "ListTagsForResource", json!({ "Resource": arn }));
        assert_eq!(body["Tags"], json!([{"Key": "project", "Value": "compat"}]));

        create(account, "t-2");
        let (status, body) = send(
            account,
            "CreateContainer",
            json!({ "ContainerName": "t-3", "Tags": [{"Key": "from-create", "Value": "yes"}] }),
        );
        assert_eq!(status, 200);
        assert_eq!(
            body["Container"]["Tags"],
            json!([{"Key": "from-create", "Value": "yes"}])
        );
    }

    #[test]
    fn access_logging_flips() {
        let account = next_account();
        create(account, "c-1");
        let (status, _) = send(
            account,
            "StartAccessLogging",
            json!({ "ContainerName": "c-1" }),
        );
        assert_eq!(status, 200);
        let (status, _) = send(
            account,
            "StopAccessLogging",
            json!({ "ContainerName": "c-1" }),
        );
        assert_eq!(status, 200);
        // On a fake container the flag flip errors like every other non-policy op.
        let (status, body) = send(
            account,
            "StartAccessLogging",
            json!({ "ContainerName": "gone" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ContainerNotFoundException");
    }

    #[test]
    fn partition_follows_moto_region_prefixes() {
        assert_eq!(partition("us-east-1"), "aws");
        assert_eq!(partition("cn-north-1"), "aws-cn");
        assert_eq!(partition("us-gov-west-1"), "aws-us-gov");
        assert_eq!(partition("us-iso-east-1"), "aws-iso");
        assert_eq!(partition("us-isob-east-1"), "aws-iso-b");
        assert_eq!(partition("eusc-de-east-1"), "aws-eusc");
        assert_eq!(partition(""), "aws");
    }

    #[test]
    fn unknown_operations_answer_not_implemented() {
        let account = next_account();
        let request = Request {
            service: "mediastore".to_string(),
            operation: "NoSuchOperation".to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::new(),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&request).expect("dispatch");
        assert_eq!(resp.status, 501);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["message"],
            "The NoSuchOperation action has not been implemented"
        );
    }

    #[test]
    fn foreign_service_is_untouched() {
        let request = Request {
            service: "s3".to_string(),
            operation: "ListContainers".to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::new(),
            account: 1,
            region: "us-east-1".to_string(),
        };
        assert!(dispatch(&request).is_none());
    }
}
